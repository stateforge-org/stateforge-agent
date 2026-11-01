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
    OperationAdmissionUnresolvedError,
    ProviderAvailabilityState,
    ProviderSessionManager,
    SfpCapabilityAssessmentRequestError,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
    load_provider_descriptor,
)
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCoordinator,
)

_AGENT_JOURNAL = Path("/var/lib/stateforge-agent/journal/agent.db")
_AGENT_UNIT = "stateforge-agent.service"
_DISCOVERY_DIRECTORY = Path("/run/stateforge-agent/providers")
_OPERATION = "stateforge.test/inspect@1"
_OPERATION_ATTEMPT_PREFIX = "rpc:operation-admission:"
_EXPECTED_RETRY_ATTEMPTS = 2
_EXPECTED_PRIMARY_DISPATCHES = 2
_SENSITIVE_VALUE = "stateforge-a5-secret-must-not-leak"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider-instance-id", type=UUID, required=True)
    parser.add_argument("--state-directory", type=Path, required=True)
    parser.add_argument("--provider-unit-name", required=True)
    parser.add_argument(
        "--secondary-provider-instance-id",
        type=UUID,
        required=True,
    )
    parser.add_argument(
        "--secondary-state-directory",
        type=Path,
        required=True,
    )
    return parser.parse_args()


def _descriptor(provider_instance_id: UUID) -> ProviderEndpointDescriptor:
    return load_provider_descriptor(
        _DISCOVERY_DIRECTORY / f"{provider_instance_id}.sfp.json",
        discovery_directory=_DISCOVERY_DIRECTORY,
    )


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _events(path: Path) -> tuple[str, ...]:
    try:
        return tuple(path.read_text(encoding="utf-8").splitlines())
    except FileNotFoundError:
        return ()


def _wait_for_operation_count(path: Path, expected: int) -> None:
    """Wait outside the event loop for an exact dispatch count."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _events(path).count("operation:inspect") == expected:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"Provider did not dispatch exactly {expected} operations"
    )


async def _restart_unit(unit_name: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "/usr/bin/systemctl",
        "restart",
        unit_name,
    )
    if await process.wait() != 0:
        raise AssertionError(f"systemd restart failed for {unit_name}")


async def _wait_for_replacement(
    provider_instance_id: UUID,
    previous_generation: int,
) -> ProviderEndpointDescriptor:
    async with asyncio.timeout(15):
        while True:
            try:
                descriptor = _descriptor(provider_instance_id)
            except (FileNotFoundError, ValueError):
                await asyncio.sleep(0.05)
                continue
            if descriptor.endpoint_generation > previous_generation:
                return descriptor
            await asyncio.sleep(0.05)


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
    *,
    attempts: int,
) -> tuple[ProviderSessionManager, OperationAdmissionCoordinator]:
    store = journal.operation_admissions(maximum_unresolved_attempts=32)
    await store.initialize()
    coordinator = OperationAdmissionCoordinator(
        store,
        attempts=attempts,
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


def _request(
    provider_instance_id: UUID,
    value: str,
    *,
    scenario: str,
) -> OperationAdmissionRequest:
    return OperationAdmissionRequest(
        provider_instance_id=provider_instance_id,
        operation_identifier=_OPERATION,
        payload={"value": value},
        execution_deadline=datetime.now(UTC) + timedelta(minutes=5),
        execution_context={"scenario": scenario},
    )


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


async def _run(  # noqa: C901, PLR0912, PLR0915
    arguments: argparse.Namespace,
) -> dict[str, object]:
    primary_id: UUID = arguments.provider_instance_id
    secondary_id: UUID = arguments.secondary_provider_instance_id
    primary_descriptor = _descriptor(primary_id)
    secondary_descriptor = _descriptor(secondary_id)
    primary_events_path = arguments.state_directory / "events.log"
    secondary_events_path = arguments.secondary_state_directory / "events.log"
    primary_target = arguments.state_directory / "target-state.txt"
    secondary_target = arguments.secondary_state_directory / "target-state.txt"
    primary_target_before = _digest(primary_target)
    secondary_target_before = _digest(secondary_target)

    journal = SqliteJournal(_AGENT_JOURNAL)
    await journal.open()
    manager, coordinator = await _manager(
        journal,
        attempts=_EXPECTED_RETRY_ATTEMPTS,
    )
    try:
        await manager.establish(primary_descriptor)
        await manager.establish(secondary_descriptor)
        await asyncio.gather(
            _wait_until_available(manager, primary_id),
            _wait_until_available(manager, secondary_id),
        )
        await _require_local_rejections(
            manager,
            primary_id,
            primary_events_path,
        )
        primary_result, secondary_result = await asyncio.gather(
            manager.admit_operation(
                _request(
                    primary_id,
                    _SENSITIVE_VALUE,
                    scenario="multi-provider-primary",
                )
            ),
            manager.admit_operation(
                _request(
                    secondary_id,
                    "secondary-operation",
                    scenario="multi-provider-secondary",
                )
            ),
        )
        primary_attempt = await coordinator.attempt(
            primary_id,
            primary_result.operation_id,
        )
        secondary_attempt = await coordinator.attempt(
            secondary_id,
            secondary_result.operation_id,
        )
    finally:
        await manager.close()

    if (
        primary_result.disposition
        is not OperationAdmissionDisposition.ACCEPTED_NEW
    ):
        raise AssertionError("Primary Provider did not admit new work")
    if secondary_result.disposition is not (
        OperationAdmissionDisposition.RESOLVED_EXISTING
    ):
        raise AssertionError("Lost response was not resolved idempotently")
    if primary_attempt is None or primary_attempt.result != primary_result:
        raise AssertionError("Primary admission was not durably resolved")
    if (
        secondary_attempt is None
        or secondary_attempt.result != secondary_result
    ):
        raise AssertionError("Secondary admission was not durably resolved")
    if primary_attempt.attempt_count != 1:
        raise AssertionError("Primary Provider received a duplicate attempt")
    if secondary_attempt.attempt_count != _EXPECTED_RETRY_ATTEMPTS:
        raise AssertionError("Secondary retry count is incorrect")
    if primary_result.operation_id == secondary_result.operation_id:
        raise AssertionError("Provider journals shared an operation identity")

    await asyncio.gather(
        asyncio.to_thread(_wait_for_operation_count, primary_events_path, 1),
        asyncio.to_thread(_wait_for_operation_count, secondary_events_path, 1),
    )
    secondary_events = _events(secondary_events_path)
    secondary_digests = tuple(
        event.removeprefix(_OPERATION_ATTEMPT_PREFIX)
        for event in secondary_events
        if event.startswith(_OPERATION_ATTEMPT_PREFIX)
    )
    if (
        len(secondary_digests) != _EXPECTED_RETRY_ATTEMPTS
        or len(set(secondary_digests)) != 1
    ):
        raise AssertionError("Retry changed the immutable wire request")

    drop_marker = arguments.state_directory / "drop-next-operation-response"
    drop_marker.touch()
    replacement_manager, replacement_coordinator = await _manager(
        journal,
        attempts=1,
    )
    try:
        await replacement_manager.establish(primary_descriptor)
        await _wait_until_available(replacement_manager, primary_id)
        try:
            await replacement_manager.admit_operation(
                _request(
                    primary_id,
                    "generation-replacement",
                    scenario="generation-replacement",
                )
            )
        except OperationAdmissionUnresolvedError as error:
            unresolved_operation_id = error.operation_id
        else:
            raise AssertionError("Injected response loss was not unresolved")
        unresolved_attempt = await replacement_coordinator.attempt(
            primary_id,
            unresolved_operation_id,
        )
    finally:
        await replacement_manager.close()

    if unresolved_attempt is None or unresolved_attempt.attempt_count != 1:
        raise AssertionError("Uncertain operation identity was not durable")
    await asyncio.to_thread(_wait_for_operation_count, primary_events_path, 2)

    await _restart_unit(arguments.provider_unit_name)
    replacement_descriptor = await _wait_for_replacement(
        primary_id,
        primary_descriptor.endpoint_generation,
    )
    recovery_manager, recovery_coordinator = await _manager(
        journal,
        attempts=1,
    )
    try:
        await recovery_manager.establish(replacement_descriptor)
        await _wait_until_available(recovery_manager, primary_id)
        try:
            await recovery_manager.resolve_operation(
                primary_id,
                unresolved_operation_id,
            )
        except OperationAdmissionUnresolvedError as error:
            if error.operation_id != unresolved_operation_id:
                raise AssertionError(
                    "Replacement changed the unresolved operation identity"
                ) from error
        else:
            raise AssertionError(
                "Replacement generation claimed a missing retained record"
            )
        recovered_attempt = await recovery_coordinator.attempt(
            primary_id,
            unresolved_operation_id,
        )
    finally:
        await recovery_manager.close()
        await journal.close()

    if recovered_attempt is None or recovered_attempt.result is not None:
        raise AssertionError("Unprovable replacement outcome was not retained")
    if recovered_attempt.operation_id != unresolved_operation_id:
        raise AssertionError("Replacement changed the operation identity")
    if recovered_attempt.last_endpoint_generation != (
        replacement_descriptor.endpoint_generation
    ):
        raise AssertionError("Resolution attempt used the wrong generation")

    primary_events = _events(primary_events_path)
    if (
        primary_events.count("operation:inspect")
        != _EXPECTED_PRIMARY_DISPATCHES
    ):
        raise AssertionError("Replacement generation redispatched prior work")
    if "operation-admission-mode:resolve_only" not in primary_events:
        raise AssertionError("Replacement recovery did not use RESOLVE_ONLY")
    if (
        "operation-admission-response:dropped:accepted_new"
        not in primary_events
    ):
        raise AssertionError(
            "Generation test did not lose an accepted response"
        )
    if _SENSITIVE_VALUE in "\n".join(primary_events + secondary_events):
        raise AssertionError(
            "Sensitive operation content reached fixture logs"
        )
    if _digest(primary_target) != primary_target_before:
        raise AssertionError("Primary fixture operation mutated its target")
    if _digest(secondary_target) != secondary_target_before:
        raise AssertionError("Secondary fixture operation mutated its target")

    await _restart_unit(_AGENT_UNIT)
    reopened = SqliteJournal(_AGENT_JOURNAL)
    await reopened.open()
    reopened_store = reopened.operation_admissions(
        maximum_unresolved_attempts=32
    )
    await reopened_store.initialize()
    try:
        persisted_primary = await reopened_store.get(
            primary_id,
            primary_result.operation_id,
        )
        persisted_secondary = await reopened_store.get(
            secondary_id,
            secondary_result.operation_id,
        )
        persisted_recovered = await reopened_store.get(
            primary_id,
            unresolved_operation_id,
        )
    finally:
        await reopened.close()
    if persisted_primary is None or persisted_primary.result != primary_result:
        raise AssertionError("Agent restart lost the primary admission record")
    if (
        persisted_secondary is None
        or persisted_secondary.result != secondary_result
    ):
        raise AssertionError(
            "Agent restart lost the secondary admission record"
        )
    if persisted_recovered is None or persisted_recovered.result is not None:
        raise AssertionError(
            "Agent restart lost the unresolved admission record"
        )

    return {
        "agent_restart_preserved_records": True,
        "agent_version": version("stateforge-agent"),
        "generation_replacement": {
            "from": primary_descriptor.endpoint_generation,
            "operation_id": str(unresolved_operation_id),
            "remained_unresolved": True,
            "request_identity_preserved": True,
            "to": replacement_descriptor.endpoint_generation,
            "used_resolve_only": True,
        },
        "multi_provider_isolation": {
            "primary_provider_instance_id": str(primary_id),
            "secondary_provider_instance_id": str(secondary_id),
        },
        "primary_disposition": primary_result.disposition.value,
        "protocol_version": version("stateforge-protocol"),
        "provider_version": version("stateforge-provider"),
        "secondary_attempt_count": secondary_attempt.attempt_count,
        "secondary_disposition": secondary_result.disposition.value,
        "secret_safe_evidence": True,
    }


def main() -> None:
    """Run the installed-package assertion and emit sanitized evidence."""
    sys.stdout.write(
        json.dumps(asyncio.run(_run(_arguments())), indent=2, sort_keys=True)
        + "\n"
    )


if __name__ == "__main__":
    main()
