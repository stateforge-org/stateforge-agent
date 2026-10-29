"""Run serialized SQLite journal transactions outside the event loop."""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

_Result = TypeVar("_Result")
_BUSY_TIMEOUT_MILLISECONDS = 5000


class SqliteJournalError(RuntimeError):
    """Report that the Agent journal could not complete an operation."""


class SqliteJournal:
    """Own one bounded SQLite transaction boundary for Agent state."""

    def __init__(self, path: Path) -> None:
        """Bind the journal to its platform-owned persistent path."""
        self._path = path
        self._lock = asyncio.Lock()
        self._opened = False

    @property
    def path(self) -> Path:
        """Return the platform-owned database path."""
        return self._path

    async def open(self) -> None:
        """Create and configure the journal without replacing existing data."""
        async with self._lock:
            if self._opened:
                return
            try:
                await asyncio.to_thread(self._prepare)
            except (OSError, sqlite3.Error) as error:
                raise SqliteJournalError(
                    "Agent journal could not be opened"
                ) from error
            self._opened = True

    async def transaction(
        self,
        operation: Callable[[sqlite3.Connection], _Result],
    ) -> _Result:
        """Run one immediate transaction with serialized local access."""
        async with self._lock:
            if not self._opened:
                raise SqliteJournalError("Agent journal is not open")
            try:
                return await asyncio.to_thread(self._transact, operation)
            except (OSError, sqlite3.Error) as error:
                raise SqliteJournalError(
                    "Agent journal transaction failed"
                ) from error

    async def close(self) -> None:
        """Reject future transactions after current work has finished."""
        async with self._lock:
            self._opened = False

    def _prepare(self) -> None:
        """Prepare the directory and durable SQLite mode synchronously."""
        self._path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
        finally:
            connection.close()
        os.chmod(self._path, 0o640)

    def _transact(
        self,
        operation: Callable[[sqlite3.Connection], _Result],
    ) -> _Result:
        """Execute and durably commit one synchronous transaction."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = operation(connection)
            connection.commit()
            return result
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        """Open one fully configured local connection."""
        connection = sqlite3.connect(
            self._path,
            timeout=_BUSY_TIMEOUT_MILLISECONDS / 1000,
            isolation_level=None,
        )
        connection.execute(
            f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MILLISECONDS}"
        )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection
