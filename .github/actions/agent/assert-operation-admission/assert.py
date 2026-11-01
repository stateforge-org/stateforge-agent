"""Exercise Operation Admission from the RPM-installed Agent package."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from uuid import UUID, uuid4

from stateforge_agent._journal import SqliteJournal
from stateforge_agent._sfp import (
    OperationAdmissionDisposition,
    OperationAdmissionPreconditionError,
    OperationAdmissionRequest,
    ProviderAvailabilityState,
    ProviderSessionManager,
    SfpCapabilityAssessmentRequestError,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.descriptor import load_provider_descriptor
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCoordinator,
)

_AGENT_JOURNAL = Path("/var/lib/stateforge-agent/journal/agent.db")
_AGENT_UNIT = "stateforge-agent.service"
_DISCOVERY_DIRECTORY = Path("/run/stateforge-agent/providers")
_OPERATION = "stateforge.test/inspect@1"
_OPERATION_ATTEMPT_PREFIX = "rpc:operation-admission:"
_EXPECTED_ATTEMPTS = 2


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider-instance-id", type=UUID, required=True)
    parser.add_argument("--state-directory", type=Path, required=True)
    return parser.parse_args()


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _events(path: Path) -> tuple[str, ...]:
    try:
        return tuple(path.read_text(encoding="utf-8").splitlines())
    except FileNotFoundError:
        return ()


def _wait_for_operation_event(path: Path) -> None:
    """Wait outside the event loop for one deterministic fixture dispatch."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _events(path).count("operation:inspect") == 1:
            return
        time.sleep(0.05)
    raise AssertionError("Provider did not dispatch exactly one operation")


async def _restart_agent() -> None:
    """Restart the exact packaged Agent unit under test."""
    process = await asyncio.create_subprocess_exec(
        "/usr/bin/systemctl",
        "restart",
        _AGENT_UNIT,
    )
    if await process.wait() != 0:
        raise AssertionError("Installed Agent service restart failed")


async def _wait_until_available(
    manager: ProviderSessionManager,
    provider_instance_id: UUID,
) -> None:
    async with asyncio.timeout(15):
        while True:
            status = manager.availability_status(provider_instance_id)
            if (
                status is not None
                and status.state is ProviderAvailabilityState.AVAILABLE
            ):
                return
            await asyncio.sleep(0.05)


async def _manager(
    journal: SqliteJournal,
) -> tuple[ProviderSessionManager, OperationAdmissionCoordinator]:
    store = journal.operation_admissions(maximum_unresolved_attempts=32)
    await store.initialize()
    coordinator = OperationAdmissionCoordinator(
        store,
        attempts=_EXPECTED_ATTEMPTS,
        retry_initial_delay_seconds=0.05,
    )
    manager = ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version=version("stateforge-agent"),
        supported_sfp_versions=(SfpVersion(major=1, minor=0),),
        capability_assessment_retry_initial_delay_seconds=0.05,
        operation_admission=coordinator,
    )
    return manager, coordinator


async def _require_local_rejections(
    manager: ProviderSessionManager,
    provider_instance_id: UUID,
    event_log: Path,
) -> None:
    attempts_before = tuple(
        event
        for event in _events(event_log)
        if event.startswith(_OPERATION_ATTEMPT_PREFIX)
    )
    requests = (
        OperationAdmissionRequest(
            provider_instance_id=provider_instance_id,
            operation_identifier="stateforge.test/not-declared@1",
            payload={"value": "rejected"},
            execution_deadline=datetime.now(UTC) + timedelta(minutes=5),
            execution_context={"scenario": "installed-agent"},
        ),
        OperationAdmissionRequest(
            provider_instance_id=provider_instance_id,
            operation_identifier=_OPERATION,
            payload={"unexpected": "field"},
            execution_deadline=datetime.now(UTC) + timedelta(minutes=5),
            execution_context={"scenario": "installed-agent"},
        ),
        OperationAdmissionRequest(
            provider_instance_id=provider_instance_id,
            operation_identifier=_OPERATION,
            payload={"value": "rejected"},
            execution_deadline=datetime.now(UTC) + timedelta(minutes=5),
            execution_context={"scenario": "installed-agent"},
            required_implementation_roles=(
                "stateforge.test.profile.v1.not_declared",
            ),
        ),
    )
    for request in requests:
        try:
            await manager.admit_operation(request)
        except (
            OperationAdmissionPreconditionError,
            SfpCapabilityAssessmentRequestError,
        ):
            continue
        raise AssertionError("Agent accepted an invalid local operation")
    attempts_after = tuple(
        event
        for event in _events(event_log)
        if event.startswith(_OPERATION_ATTEMPT_PREFIX)
    )
    if attempts_after != attempts_before:
        raise AssertionError("Locally rejected operation reached Provider")


async def _run(arguments: argparse.Namespace) -> dict[str, object]:
    provider_instance_id: UUID = arguments.provider_instance_id
    descriptor = load_provider_descriptor(
        _DISCOVERY_DIRECTORY / f"{provider_instance_id}.sfp.json",
        discovery_directory=_DISCOVERY_DIRECTORY,
    )
    event_log = arguments.state_directory / "events.log"
    target = arguments.state_directory / "target-state.txt"
    target_before = _digest(target)

    journal = SqliteJournal(_AGENT_JOURNAL)
    await journal.open()
    manager, coordinator = await _manager(journal)
    try:
        await manager.establish(descriptor)
        await _wait_until_available(manager, provider_instance_id)
        await _require_local_rejections(
            manager,
            provider_instance_id,
            event_log,
        )
        result = await manager.admit_operation(
            OperationAdmissionRequest(
                provider_instance_id=provider_instance_id,
                operation_identifier=_OPERATION,
                payload={"value": "admitted-once"},
                execution_deadline=datetime.now(UTC) + timedelta(minutes=5),
                execution_context={"scenario": "installed-agent"},
            )
        )
        attempt = await coordinator.attempt(
            provider_instance_id,
            result.operation_id,
        )
    finally:
        await manager.close()
        await journal.close()

    if result.disposition is not (
        OperationAdmissionDisposition.RESOLVED_EXISTING
    ):
        raise AssertionError("Lost response was not resolved idempotently")
    if attempt is None or attempt.result != result:
        raise AssertionError("Accepted operation was not durably resolved")
    if attempt.attempt_count != _EXPECTED_ATTEMPTS:
        raise AssertionError("Admission did not use the expected retry count")

    await asyncio.to_thread(_wait_for_operation_event, event_log)
    events = _events(event_log)
    request_digests = tuple(
        event.removeprefix(_OPERATION_ATTEMPT_PREFIX)
        for event in events
        if event.startswith(_OPERATION_ATTEMPT_PREFIX)
    )
    if (
        len(request_digests) != _EXPECTED_ATTEMPTS
        or len(set(request_digests)) != 1
    ):
        raise AssertionError("Retry changed the immutable wire request")
    expected_responses = {
        "operation-admission-response:dropped:accepted_new",
        "operation-admission-response:returned:resolved_existing",
    }
    if not expected_responses.issubset(events):
        raise AssertionError("Provider did not expose both admission outcomes")
    if _digest(target) != target_before:
        raise AssertionError("Generic fixture operation mutated its target")

    await _restart_agent()
    reopened = SqliteJournal(_AGENT_JOURNAL)
    await reopened.open()
    reopened_store = reopened.operation_admissions(
        maximum_unresolved_attempts=32
    )
    await reopened_store.initialize()
    try:
        persisted = await reopened_store.get(
            provider_instance_id,
            result.operation_id,
        )
    finally:
        await reopened.close()
    if persisted is None or persisted.result != result:
        raise AssertionError("Agent restart lost the admission record")

    return {
        "agent_restart_preserved_record": True,
        "agent_version": version("stateforge-agent"),
        "attempt_count": attempt.attempt_count,
        "disposition": result.disposition.value,
        "endpoint_generation": result.endpoint_generation,
        "fingerprint": result.request_fingerprint,
        "manifest_revision": result.manifest_revision,
        "operation_id": str(result.operation_id),
        "operation_identifier": _OPERATION,
        "provider_instance_id": str(result.provider_instance_id),
        "provider_version": version("stateforge-provider"),
        "protocol_version": version("stateforge-protocol"),
        "request_identity_preserved": True,
        "target_sha256": target_before,
    }


def main() -> None:
    """Run the installed-package assertion and emit sanitized evidence."""
    sys.stdout.write(
        json.dumps(asyncio.run(_run(_arguments())), indent=2, sort_keys=True)
        + "\n"
    )


if __name__ == "__main__":
    main()
