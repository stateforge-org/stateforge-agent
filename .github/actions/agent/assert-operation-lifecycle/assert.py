"""Exercise lifecycle observation through the RPM-installed Agent package."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from uuid import UUID, uuid4

from stateforge_protocol.sfp.operation_lifecycle.v1 import (
    operation_lifecycle_pb2,
)

from stateforge_agent._journal import SqliteJournal
from stateforge_agent._sfp import (
    OperationAdmissionRequest,
    ProviderAvailabilityState,
    ProviderSessionManager,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.descriptor import load_provider_descriptor
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCoordinator,
)
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAttempt,
)
from stateforge_agent._sfp._services.operation_lifecycle.coordinator import (
    OperationLifecycleCoordinator,
)
from stateforge_agent._sfp._services.operation_lifecycle.models import (
    OperationStatus,
)

_AGENT_JOURNAL = Path("/var/lib/stateforge-agent/journal/agent.db")
_DISCOVERY_DIRECTORY = Path("/run/stateforge-agent/providers")
_STATE_DIRECTORY = Path("/var/lib/stateforge-provider-fixtures")
_OPERATION = "stateforge.test/lifecycle@1"
_UNCANCELLABLE = "stateforge.test/lifecycle.uncancellable@1"
_TERMINAL = frozenset(
    {
        "OPERATION_STATE_SUCCEEDED",
        "OPERATION_STATE_FAILED",
        "OPERATION_STATE_CANCELLED",
    }
)
_COMPLETE_PERCENTAGE = 100
_TERMINAL_NUMBERS = frozenset(
    {
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        operation_lifecycle_pb2.OPERATION_STATE_FAILED,
        operation_lifecycle_pb2.OPERATION_STATE_CANCELLED,
    }
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider-instance-id", type=UUID, required=True)
    parser.add_argument("--provider-state-name", required=True)
    return parser.parse_args()


def _events(path: Path) -> tuple[str, ...]:
    return tuple(path.read_text(encoding="utf-8").splitlines())


async def _wait_status(
    manager: ProviderSessionManager,
    provider_id: UUID,
    operation_id: UUID,
    wanted: set[str] | frozenset[str],
) -> OperationStatus:
    async with asyncio.timeout(20):
        while True:
            status = await manager.operation_status(provider_id, operation_id)
            if status is not None and status.state in wanted:
                return status
            await asyncio.sleep(0.05)


async def _wait_journal_terminal(
    journal: SqliteJournal,
    provider_id: UUID,
    operation_id: UUID,
) -> OperationAttempt:
    store = journal.operation_admissions(maximum_unresolved_attempts=32)
    async with asyncio.timeout(20):
        while True:
            attempt = await store.get(provider_id, operation_id)
            if attempt is not None and attempt.lifecycle_snapshot is not None:
                state = attempt.lifecycle_snapshot.state
                if state in _TERMINAL_NUMBERS:
                    return attempt
            await asyncio.sleep(0.05)


async def _restart_agent() -> None:
    process = await asyncio.create_subprocess_exec(
        "/usr/bin/systemctl", "restart", "stateforge-agent.service"
    )
    if await process.wait() != 0:
        raise AssertionError("Installed Agent service restart failed")


async def _manager(
    journal: SqliteJournal,
    provider_id: UUID,
) -> ProviderSessionManager:
    store = journal.operation_admissions(maximum_unresolved_attempts=32)
    await store.initialize()
    manager = ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version=version("stateforge-agent"),
        supported_sfp_versions=(SfpVersion(major=1, minor=0),),
        capability_assessment_retry_initial_delay_seconds=0.05,
        operation_admission=OperationAdmissionCoordinator(store),
        operation_lifecycle=OperationLifecycleCoordinator(
            store, retry_initial_delay_seconds=0.05
        ),
    )
    await manager.establish(
        load_provider_descriptor(
            _DISCOVERY_DIRECTORY / f"{provider_id}.sfp.json",
            discovery_directory=_DISCOVERY_DIRECTORY,
        )
    )
    async with asyncio.timeout(15):
        while True:
            availability = manager.availability_status(provider_id)
            if (
                availability is not None
                and availability.state is ProviderAvailabilityState.AVAILABLE
            ):
                return manager
            await asyncio.sleep(0.05)


async def _admit(
    manager: ProviderSessionManager,
    provider_id: UUID,
    value: str,
    *,
    identifier: str = _OPERATION,
) -> UUID:
    result = await manager.admit_operation(
        OperationAdmissionRequest(
            provider_instance_id=provider_id,
            operation_identifier=identifier,
            payload={"value": value},
            execution_deadline=datetime.now(UTC) + timedelta(minutes=3),
            execution_context={"scenario": "installed-operation-lifecycle"},
        )
    )
    return result.operation_id


async def _run(  # noqa: C901, PLR0912, PLR0915 - scenario assertions
    arguments: argparse.Namespace,
) -> dict[str, object]:
    provider_id: UUID = arguments.provider_instance_id
    state_directory = _STATE_DIRECTORY / arguments.provider_state_name
    event_log = state_directory / "events.log"
    journal = SqliteJournal(_AGENT_JOURNAL)
    await journal.open()
    manager = await _manager(journal, provider_id)
    seen_ids: list[UUID] = []
    try:
        success_id = await _admit(manager, provider_id, "success")
        seen_ids.append(success_id)
        success = await _wait_status(
            manager, provider_id, success_id, _TERMINAL
        )
        if success.state != "OPERATION_STATE_SUCCEEDED":
            raise AssertionError("Installed Provider success was not observed")
        if success.result != {"value": "success"}:
            raise AssertionError("Typed result was not normalized")
        if success.progress_percentage != _COMPLETE_PERCENTAGE:
            raise AssertionError("Latest progress was not retained")

        held_id = await _admit(manager, provider_id, "held")
        seen_ids.append(held_id)
        await _wait_status(
            manager, provider_id, held_id, {"OPERATION_STATE_RUNNING"}
        )
    finally:
        await manager.close()

    # The operation stays in the Provider while the observer and Agent restart.
    await _restart_agent()
    (state_directory / "lifecycle-release").touch()
    persisted = await _wait_journal_terminal(journal, provider_id, held_id)
    if (
        persisted.lifecycle_snapshot is None
        or persisted.lifecycle_snapshot.state
        != operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED
    ):
        raise AssertionError("Daemon did not resume outstanding observation")

    manager = await _manager(journal, provider_id)
    try:
        cancel_id = await _admit(manager, provider_id, "cancel")
        seen_ids.append(cancel_id)
        await _wait_status(
            manager, provider_id, cancel_id, {"OPERATION_STATE_RUNNING"}
        )
        requested = await manager.cancel_operation(provider_id, cancel_id)
        if requested.state != "OPERATION_STATE_CANCELLATION_REQUESTED":
            raise AssertionError("Cancellation request fabricated an outcome")
        if requested.cancellation_disposition != (
            "CANCELLATION_DISPOSITION_REQUESTED"
        ):
            raise AssertionError("Provider cancellation decision was lost")
        (state_directory / "cancellation-release").touch()
        cancelled = await _wait_status(
            manager, provider_id, cancel_id, _TERMINAL
        )
        if cancelled.state != "OPERATION_STATE_CANCELLED":
            raise AssertionError("Confirmed cancellation was not retained")
        if cancelled.side_effects != "SIDE_EFFECT_STATE_PRESENT":
            raise AssertionError("Cancellation effects were not retained")

        failure_id = await _admit(manager, provider_id, "failure")
        seen_ids.append(failure_id)
        failed = await _wait_status(
            manager, provider_id, failure_id, _TERMINAL
        )
        if failed.state != "OPERATION_STATE_FAILED" or failed.failure_code != (
            "stateforge.test.fixture_failure"
        ):
            raise AssertionError("Structured failure was not retained")
        if failed.cleanup != "CLEANUP_STATE_INCOMPLETE":
            raise AssertionError("Partial cleanup evidence was lost")

        invalid_id = await _admit(manager, provider_id, "invalid-result")
        seen_ids.append(invalid_id)
        invalid = await _wait_status(
            manager, provider_id, invalid_id, _TERMINAL
        )
        if (
            invalid.state != "OPERATION_STATE_FAILED"
            or invalid.failure_code != ("stateforge.operation.invalid_result")
        ):
            raise AssertionError(
                "Invalid typed result was accepted as success"
            )

        unsupported_id = await _admit(
            manager, provider_id, "unsupported", identifier=_UNCANCELLABLE
        )
        seen_ids.append(unsupported_id)
        await _wait_status(
            manager, provider_id, unsupported_id, {"OPERATION_STATE_RUNNING"}
        )
        unsupported = await manager.cancel_operation(
            provider_id, unsupported_id
        )
        if unsupported.cancellation_disposition != (
            "CANCELLATION_DISPOSITION_UNSUPPORTED"
        ):
            raise AssertionError("Unsupported cancellation was misclassified")
        (state_directory / "uncancellable-release").touch()
        await _wait_status(manager, provider_id, unsupported_id, _TERMINAL)

        already_terminal = await manager.cancel_operation(
            provider_id, success_id
        )
        if already_terminal.cancellation_disposition != (
            "CANCELLATION_DISPOSITION_ALREADY_TERMINAL"
        ):
            raise AssertionError("Terminal cancellation was misclassified")
        if already_terminal.state != "OPERATION_STATE_SUCCEEDED":
            raise AssertionError("Terminal success was replaced")
    finally:
        await manager.close()
        await journal.close()

    events = _events(event_log)
    for operation_id in seen_ids:
        dispatches = sum(
            event.endswith(str(operation_id))
            and event.startswith("operation:")
            for event in events
        )
        if dispatches != 1:
            raise AssertionError("Watch or Cancel redispatched business work")
    return {
        "agent_version": version("stateforge-agent"),
        "protocol_version": version("stateforge-protocol"),
        "provider_version": version("stateforge-provider"),
        "operation_ids": [str(item) for item in seen_ids],
        "daemon_resumed_after_restart": True,
        "typed_success_and_progress": True,
        "requested_then_confirmed_cancelled": True,
        "partial_failure_evidence": True,
        "invalid_result_rejected": True,
        "unsupported_and_already_terminal": True,
        "no_redispatch": True,
    }


def main() -> None:
    """Emit sanitized installed-scenario evidence as JSON."""
    sys.stdout.write(
        json.dumps(asyncio.run(_run(_arguments())), indent=2, sort_keys=True)
        + "\n"
    )


if __name__ == "__main__":
    main()
