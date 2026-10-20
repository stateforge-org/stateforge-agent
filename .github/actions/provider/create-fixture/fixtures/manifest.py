"""Run a minimal Provider that declares a valid manifest."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from pathlib import Path

from stateforge_provider import (
    HealthStatus,
    Platform,
    Provider,
    ProviderModel,
    RequirementCheckResult,
    operation,
    requirement_check,
)


class FixtureInput(ProviderModel):
    """Describe operation input without invoking it during assessment."""

    value: str


class FixtureResult(ProviderModel):
    """Describe operation output without invoking it during assessment."""

    value: str


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
        self._event_log = Path(os.environ["STATEFORGE_TEST_EVENT_LOG"])
        self._target_path = Path(os.environ["STATEFORGE_TEST_TARGET_PATH"])

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

    @requirement_check(
        "runtime.target.readable",
        "Fixture target is readable",
    )
    async def target_readable(self) -> RequirementCheckResult:
        """Inspect the target without changing its contents or metadata."""
        self._record("requirement:runtime.target.readable")
        if os.access(self._target_path, os.R_OK):
            return RequirementCheckResult.satisfied()
        return RequirementCheckResult.unsatisfied()

    @requirement_check("runtime.pending", "Pending fixture prerequisite")
    async def pending(self) -> RequirementCheckResult:
        """Expose one deterministic unmet prerequisite."""
        self._record("requirement:runtime.pending")
        return RequirementCheckResult.unsatisfied()

    @requirement_check("runtime.incompatible", "Incompatible fixture runtime")
    async def incompatible(self) -> RequirementCheckResult:
        """Expose conclusive negative runtime evidence."""
        self._record("requirement:runtime.incompatible")
        return RequirementCheckResult.incompatible("Fixture incompatibility")

    @requirement_check("runtime.unknown", "Unobservable fixture prerequisite")
    async def unknown(self) -> RequirementCheckResult:
        """Expose an unresolved prerequisite without guessing."""
        self._record("requirement:runtime.unknown")
        return RequirementCheckResult.unknown("Fixture evidence unavailable")

    @operation(
        "stateforge.test/inspect@1",
        requires=("runtime.target.readable",),
    )
    async def inspect(self, request: FixtureInput) -> FixtureResult:
        """Declare a generic supported operation that is never dispatched."""
        self._record("operation:inspect")
        return FixtureResult(value=request.value)

    @operation(
        "stateforge.test/snapshot@1",
        implements=("stateforge.backup.v1.prepare",),
        requires=("runtime.target.readable", "runtime.pending"),
    )
    async def snapshot(self, request: FixtureInput) -> FixtureResult:
        """Declare a profile-related operation with an unmet prerequisite."""
        self._record("operation:snapshot")
        return FixtureResult(value=request.value)

    @operation(
        "stateforge.test/unsupported@1",
        requires=("runtime.incompatible",),
    )
    async def unsupported(self, request: FixtureInput) -> FixtureResult:
        """Declare one operation with conclusive negative evidence."""
        self._record("operation:unsupported")
        return FixtureResult(value=request.value)

    @operation(
        "stateforge.test/indeterminate@1",
        requires=("runtime.unknown",),
    )
    async def indeterminate(self, request: FixtureInput) -> FixtureResult:
        """Declare one operation whose current support is unresolved."""
        self._record("operation:indeterminate")
        return FixtureResult(value=request.value)

    def _record(self, event: str) -> None:
        """Record fixture-only evidence outside the inspected target."""
        with self._event_log.open("a", encoding="utf-8") as stream:
            stream.write(f"{event}\n")

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
