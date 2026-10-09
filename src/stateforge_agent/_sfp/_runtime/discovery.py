"""Monitor the local Provider endpoint discovery directory."""

from __future__ import annotations

import asyncio
import os
import stat
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from watchfiles import awatch

from stateforge_agent._sfp._runtime.descriptor import (
    PROVIDER_DESCRIPTOR_SUFFIX,
)

_DISCOVERY_DIRECTORY_MODE = 0o750
_WATCH_DEBOUNCE_MILLISECONDS = 100
_WATCH_STEP_MILLISECONDS = 25
_WATCH_TIMEOUT_MILLISECONDS = 1_000
_POLL_DELAY_MILLISECONDS = 300
_ENTRY_TYPES = {
    stat.S_IFREG: "file",
    stat.S_IFSOCK: "socket",
    stat.S_IFDIR: "directory",
    stat.S_IFLNK: "symlink",
    stat.S_IFIFO: "fifo",
}


class ProviderDirectoryError(RuntimeError):
    """Report an unusable Provider discovery directory."""


class ProviderDirectoryEventKind(StrEnum):
    """Describe a change observed in the discovery directory."""

    DISCOVERED = "discovered"
    UPDATED = "updated"
    REMOVED = "removed"


@dataclass(frozen=True, slots=True)
class ProviderDirectoryEntry:
    """Filesystem identity used to detect endpoint-entry changes."""

    path: Path
    entry_type: str
    device: int
    inode: int
    mode: int
    size: int
    modified_nanoseconds: int


@dataclass(frozen=True, slots=True)
class ProviderDirectoryEvent:
    """One discovered, updated, or removed directory entry."""

    kind: ProviderDirectoryEventKind
    entry: ProviderDirectoryEntry


class ProviderDirectoryMonitor:
    """Create, validate, and watch the Provider discovery directory."""

    def __init__(self, directory: Path) -> None:
        """Create a monitor without accessing the filesystem yet."""
        self.directory = directory
        self._entries: dict[str, ProviderDirectoryEntry] = {}

    def prepare(self) -> None:
        """Create or validate the protected discovery directory."""
        try:
            self.directory.mkdir(
                mode=_DISCOVERY_DIRECTORY_MODE,
                parents=True,
                exist_ok=True,
            )
            directory_status = self.directory.lstat()
        except OSError as error:
            raise ProviderDirectoryError(
                f"Cannot prepare Provider discovery directory "
                f"{self.directory}: {error}"
            ) from error

        if stat.S_ISLNK(directory_status.st_mode):
            raise ProviderDirectoryError(
                "Provider discovery directory must not be a symbolic link: "
                f"{self.directory}"
            )
        if not stat.S_ISDIR(directory_status.st_mode):
            raise ProviderDirectoryError(
                f"Provider discovery path is not a directory: {self.directory}"
            )
        if directory_status.st_uid != os.geteuid():
            raise ProviderDirectoryError(
                "Provider discovery directory must be owned by the Agent "
                f"user: {self.directory}"
            )
        if directory_status.st_mode & stat.S_IWOTH:
            raise ProviderDirectoryError(
                "Provider discovery directory must not be world-writable: "
                f"{self.directory}"
            )

    def scan(self) -> tuple[ProviderDirectoryEvent, ...]:
        """Return changes since the previous complete directory scan."""
        current_entries = self._read_entries()
        events = [
            ProviderDirectoryEvent(
                kind=ProviderDirectoryEventKind.DISCOVERED,
                entry=current_entries[name],
            )
            for name in sorted(current_entries.keys() - self._entries.keys())
        ]
        events.extend(
            ProviderDirectoryEvent(
                kind=ProviderDirectoryEventKind.UPDATED,
                entry=current_entries[name],
            )
            for name in sorted(current_entries.keys() & self._entries.keys())
            if current_entries[name] != self._entries[name]
        )
        events.extend(
            ProviderDirectoryEvent(
                kind=ProviderDirectoryEventKind.REMOVED,
                entry=self._entries[name],
            )
            for name in sorted(self._entries.keys() - current_entries.keys())
        )

        self._entries = current_entries
        return tuple(events)

    async def watch(
        self,
        stop_event: asyncio.Event,
    ) -> AsyncIterator[tuple[ProviderDirectoryEvent, ...]]:
        """Yield changes from notifications and periodic reconciliation."""
        initial_events = self.scan()
        if initial_events:
            yield initial_events

        try:
            async for _ in awatch(
                self.directory,
                debounce=_WATCH_DEBOUNCE_MILLISECONDS,
                step=_WATCH_STEP_MILLISECONDS,
                stop_event=stop_event,
                rust_timeout=_WATCH_TIMEOUT_MILLISECONDS,
                yield_on_timeout=True,
                force_polling=False,
                poll_delay_ms=_POLL_DELAY_MILLISECONDS,
                recursive=False,
                ignore_permission_denied=False,
            ):
                events = self.scan()
                if events:
                    yield events
        except ProviderDirectoryError:
            raise
        except (OSError, RuntimeError) as error:
            raise ProviderDirectoryError(
                "Provider discovery filesystem watcher failed for "
                f"{self.directory}: {error}"
            ) from error

    def _read_entries(self) -> dict[str, ProviderDirectoryEntry]:
        """Build one complete non-following filesystem snapshot."""
        try:
            paths = tuple(
                path
                for path in self.directory.iterdir()
                if path.name.endswith(PROVIDER_DESCRIPTOR_SUFFIX)
            )
        except OSError as error:
            raise ProviderDirectoryError(
                f"Cannot scan Provider discovery directory "
                f"{self.directory}: {error}"
            ) from error

        entries: dict[str, ProviderDirectoryEntry] = {}
        for path in paths:
            try:
                entry_status = path.lstat()
            except FileNotFoundError:
                continue
            except OSError as error:
                raise ProviderDirectoryError(
                    f"Cannot inspect Provider discovery entry {path}: {error}"
                ) from error

            entries[path.name] = ProviderDirectoryEntry(
                path=path,
                entry_type=_ENTRY_TYPES.get(
                    stat.S_IFMT(entry_status.st_mode),
                    "other",
                ),
                device=entry_status.st_dev,
                inode=entry_status.st_ino,
                mode=stat.S_IMODE(entry_status.st_mode),
                size=entry_status.st_size,
                modified_nanoseconds=entry_status.st_mtime_ns,
            )
        return entries
