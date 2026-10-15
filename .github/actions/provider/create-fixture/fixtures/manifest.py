"""Run a minimal Provider that declares a valid manifest."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from pathlib import Path

from stateforge_provider import HealthStatus, Platform, Provider


class ManifestFixtureProvider(Provider):
    """Expose a valid manifest and deterministic public Health controls."""

    identity = os.environ["STATEFORGE_TEST_PROVIDER_IDENTITY"]
    display_name = os.environ.get(
        "STATEFORGE_TEST_PROVIDER_DISPLAY_NAME",
        "StateForge manifest fixture",
    )
    version = "1.0.0"
    platforms = (Platform("linux", os.uname().machine),)

    def __init__(
        self,
        *,
        discovery_directory: Path,
        state_directory: Path,
    ) -> None:
        """Bind one fixture to isolated installation and discovery state."""
        super().__init__(
            discovery_directory=discovery_directory,
            state_directory=state_directory,
        )
        self._health_status = HealthStatus.healthy()
        self._fail_health = False
        self._health_tasks: set[asyncio.Task[None]] = set()

    async def on_startup(self) -> None:
        """Install deterministic health transitions for system tests."""
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGUSR1,
            self._schedule_health,
            HealthStatus.maintenance(),
            False,
        )
        loop.add_signal_handler(
            signal.SIGUSR2,
            self._schedule_health,
            HealthStatus.healthy(),
            False,
        )
        loop.add_signal_handler(
            signal.SIGHUP,
            self._schedule_health,
            HealthStatus.not_serving(),
            True,
        )

    async def on_shutdown(self) -> None:
        """Remove fixture signal handlers and finish scheduled refreshes."""
        loop = asyncio.get_running_loop()
        for signal_number in (signal.SIGUSR1, signal.SIGUSR2, signal.SIGHUP):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(signal_number)
        if self._health_tasks:
            await asyncio.gather(*self._health_tasks, return_exceptions=True)

    async def health(self) -> HealthStatus:
        """Return the scenario-controlled Provider-wide status."""
        if self._fail_health:
            raise RuntimeError("fixture health hook failed")
        return self._health_status

    def _schedule_health(
        self,
        status: HealthStatus,
        fail_health: bool,
    ) -> None:
        """Schedule a public Health refresh from a process signal."""
        task = asyncio.create_task(
            self._publish_health(status, fail_health),
        )
        self._health_tasks.add(task)
        task.add_done_callback(self._health_tasks.discard)

    async def _publish_health(
        self,
        status: HealthStatus,
        fail_health: bool,
    ) -> None:
        """Publish one deterministic status through the Provider API."""
        self._health_status = status
        self._fail_health = fail_health
        await self.refresh_health()


def main() -> None:
    """Run the Provider fixture until its service manager stops it."""
    ManifestFixtureProvider(
        discovery_directory=Path(
            os.environ["STATEFORGE_TEST_DISCOVERY_DIRECTORY"]
        ),
        state_directory=Path(os.environ["STATEFORGE_TEST_STATE_DIRECTORY"]),
    ).run()


if __name__ == "__main__":
    main()
