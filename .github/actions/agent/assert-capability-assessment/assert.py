"""Exercise Capability Assessment from the RPM-installed Agent package."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import stat
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from uuid import UUID, uuid4

import grpc
from stateforge_protocol.sfp.capability_assessment.v1 import (
    capability_assessment_pb2,
)

from stateforge_agent._sfp import (
    CapabilityAssessment,
    CapabilityRequirementState,
    CapabilitySupportState,
    ProviderAvailabilityState,
    ProviderSessionManager,
    SfpCapabilityAssessmentUnavailableError,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.descriptor import load_provider_descriptor
from stateforge_agent._sfp._services.capability_assessment.client import (
    CapabilityAssessmentClient,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessmentScope,
)

_DISCOVERY_DIRECTORY = Path("/run/stateforge-agent/providers")
_OPERATIONS = (
    "stateforge.test/inspect@1",
    "stateforge.test/snapshot@1",
    "stateforge.test/unsupported@1",
    "stateforge.test/indeterminate@1",
)
_EXPECTED_STATES = (
    CapabilitySupportState.SUPPORTED,
    CapabilitySupportState.SUPPORTED_WITH_REQUIREMENTS,
    CapabilitySupportState.UNSUPPORTED,
    CapabilitySupportState.INDETERMINATE,
)
_EXPECTED_RETRY_ATTEMPTS = 2


class _UnavailableOnceRequester:
    """Inject one transport failure before delegating to the real Provider."""

    def __init__(self) -> None:
        self._delegate = CapabilityAssessmentClient()
        self.serialized_requests: list[bytes] = []

    async def assess(
        self,
        channel: grpc.aio.Channel,
        request: capability_assessment_pb2.AssessCapabilitiesRequest,
        *,
        scope: CapabilityAssessmentScope,
    ) -> CapabilityAssessment:
        """Fail once, then execute the installed client's real RPC path."""
        self.serialized_requests.append(request.SerializeToString())
        if len(self.serialized_requests) == 1:
            raise SfpCapabilityAssessmentUnavailableError(
                "Injected one-shot transport unavailability"
            )
        return await self._delegate.assess(channel, request, scope=scope)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider-instance-id", type=UUID, required=True)
    parser.add_argument("--state-directory", type=Path, required=True)
    parser.add_argument("--unit-name", required=True)
    return parser.parse_args()


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def _wait_until_state(
    manager: ProviderSessionManager,
    provider_instance_id: UUID,
    state: ProviderAvailabilityState,
) -> None:
    async with asyncio.timeout(15):
        while True:
            status = manager.availability_status(provider_instance_id)
            if status is not None and status.state is state:
                return
            await asyncio.sleep(0.05)


def _signal_provider(unit_name: str, signal_name: str) -> None:
    """Request one deterministic Provider health transition."""
    subprocess.run(  # noqa: S603 - fixed executable and argument vector
        ["/usr/bin/systemctl", "kill", "--signal", signal_name, unit_name],
        check=True,
    )


def _support_states(
    assessment: CapabilityAssessment,
) -> tuple[CapabilitySupportState, ...]:
    """Return operation support in request order."""
    return tuple(item.support_state for item in assessment.findings)


async def _require_initial_assessment(
    manager: ProviderSessionManager,
    provider_instance_id: UUID,
    requester: _UnavailableOnceRequester,
) -> CapabilityAssessment:
    """Prove bounded retry, normalized evidence, and exact cache scope."""
    assessment = await manager.assess_capabilities(
        provider_instance_id,
        _OPERATIONS,
        execution_context={"scenario": "installed-agent"},
    )
    retry_requests = tuple(requester.serialized_requests)
    if len(retry_requests) != _EXPECTED_RETRY_ATTEMPTS or (
        retry_requests[0] != retry_requests[1]
    ):
        raise AssertionError(
            "Capability Assessment retry did not preserve the request"
        )
    cached = await manager.cached_capability_assessment(
        provider_instance_id,
        _OPERATIONS,
        execution_context={"scenario": "installed-agent"},
    )
    changed_context = await manager.cached_capability_assessment(
        provider_instance_id,
        _OPERATIONS,
        execution_context={"scenario": "different-context"},
    )
    if cached is not assessment or changed_context is not None:
        raise AssertionError("Assessment cache escaped its exact context")
    if _support_states(assessment) != _EXPECTED_STATES:
        raise AssertionError("Initial assessment returned unexpected states")
    if assessment.findings[1].requirements[1].state is not (
        CapabilityRequirementState.UNSATISFIED
    ):
        raise AssertionError("Unmet requirement was not preserved")
    if assessment.findings[3].requirements[0].state is not (
        CapabilityRequirementState.UNKNOWN
    ):
        raise AssertionError("Unknown requirement was not preserved")
    return assessment


async def _require_environment_refresh(
    manager: ProviderSessionManager,
    provider_instance_id: UUID,
    target: Path,
) -> CapabilityAssessment:
    """Prove operation evidence changes without changing A2 availability."""
    os.chmod(target, 0)
    restricted = await manager.assess_capabilities(
        provider_instance_id,
        _OPERATIONS,
        execution_context={"scenario": "restricted-environment"},
    )
    status = manager.availability_status(provider_instance_id)
    if status is None or not status.eligible:
        raise AssertionError(
            "An operation-specific requirement changed A2 availability"
        )
    if restricted.findings[0].support_state is not (
        CapabilitySupportState.SUPPORTED_WITH_REQUIREMENTS
    ) or restricted.findings[0].requirements[0].state is not (
        CapabilityRequirementState.UNSATISFIED
    ):
        raise AssertionError(
            "Changed runtime environment did not refresh A3 evidence"
        )
    return restricted


async def _require_availability_cycle(
    manager: ProviderSessionManager,
    provider_instance_id: UUID,
    unit_name: str,
    initial: CapabilityAssessment,
) -> CapabilityAssessment:
    """Prove A2 loss blocks A3, clears evidence, and allows recovery."""
    _signal_provider(unit_name, "SIGUSR1")
    await _wait_until_state(
        manager,
        provider_instance_id,
        ProviderAvailabilityState.NOT_SERVING,
    )
    try:
        await manager.assess_capabilities(
            provider_instance_id,
            _OPERATIONS,
            execution_context={"scenario": "provider-not-serving"},
        )
    except SfpCapabilityAssessmentUnavailableError:
        pass
    else:
        raise AssertionError(
            "Capability Assessment ran while Provider was unavailable"
        )

    _signal_provider(unit_name, "SIGUSR2")
    await _wait_until_state(
        manager,
        provider_instance_id,
        ProviderAvailabilityState.AVAILABLE,
    )
    cached = await manager.cached_capability_assessment(
        provider_instance_id,
        _OPERATIONS,
        execution_context={"scenario": "installed-agent"},
    )
    if cached is not None:
        raise AssertionError(
            "Availability transition did not invalidate A3 evidence"
        )
    recovered = await manager.assess_capabilities(
        provider_instance_id,
        _OPERATIONS,
        execution_context={"scenario": "recovered-environment"},
    )
    if _support_states(recovered) != _EXPECTED_STATES:
        raise AssertionError(
            "Recovered Provider did not produce fresh evidence"
        )
    if recovered.assessment_id == initial.assessment_id:
        raise AssertionError("Recovery reused the pre-transition assessment")
    return recovered


async def _run(arguments: argparse.Namespace) -> dict[str, object]:
    provider_instance_id: UUID = arguments.provider_instance_id
    state_directory: Path = arguments.state_directory
    descriptor = load_provider_descriptor(
        _DISCOVERY_DIRECTORY / f"{provider_instance_id}.sfp.json",
        discovery_directory=_DISCOVERY_DIRECTORY,
    )
    target = state_directory / "target-state.txt"
    event_log = state_directory / "events.log"
    before = _digest(target)
    original_mode = stat.S_IMODE(target.stat().st_mode)
    requester = _UnavailableOnceRequester()
    manager = ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version=version("stateforge-agent"),
        supported_sfp_versions=(SfpVersion(major=1, minor=0),),
        capability_assessment_retry_initial_delay_seconds=0.05,
        capability_assessment_client=requester,
    )
    try:
        await manager.establish(descriptor)
        await _wait_until_state(
            manager,
            provider_instance_id,
            ProviderAvailabilityState.AVAILABLE,
        )
        assessment = await _require_initial_assessment(
            manager,
            provider_instance_id,
            requester,
        )
        restricted = await _require_environment_refresh(
            manager,
            provider_instance_id,
            target,
        )
        os.chmod(target, original_mode)
        recovered = await _require_availability_cycle(
            manager,
            provider_instance_id,
            arguments.unit_name,
            assessment,
        )
    finally:
        os.chmod(target, original_mode)
        with contextlib.suppress(subprocess.CalledProcessError):
            _signal_provider(arguments.unit_name, "SIGUSR2")
        await manager.close()

    after = _digest(target)
    if before != after:
        raise AssertionError("Capability Assessment mutated the target")
    events = event_log.read_text().splitlines()
    if any(event.startswith("operation:") for event in events):
        raise AssertionError("Capability Assessment dispatched an operation")

    return {
        "agent_version": version("stateforge-agent"),
        "assessment_id": str(assessment.assessment_id),
        "assessment_request_id": str(assessment.request_id),
        "availability_transitions": [
            ProviderAvailabilityState.AVAILABLE.value,
            ProviderAvailabilityState.NOT_SERVING.value,
            ProviderAvailabilityState.AVAILABLE.value,
        ],
        "endpoint_generation": assessment.scope.endpoint_generation,
        "findings": [
            {
                "operation_identifier": finding.operation_identifier,
                "requirement_states": [
                    requirement.state.value
                    for requirement in finding.requirements
                ],
                "support_state": finding.support_state.value,
            }
            for finding in assessment.findings
        ],
        "manifest_revision": assessment.scope.manifest_revision,
        "protocol_version": version("stateforge-protocol"),
        "provider_instance_id": str(assessment.scope.provider_instance_id),
        "provider_version": version("stateforge-provider"),
        "recovered_assessment_id": str(recovered.assessment_id),
        "restricted_environment": {
            "requirement_state": (
                restricted.findings[0].requirements[0].state.value
            ),
            "support_state": restricted.findings[0].support_state.value,
        },
        "retry_attempts": _EXPECTED_RETRY_ATTEMPTS,
        "retry_request_preserved": True,
        "target_sha256": after,
    }


def main() -> None:
    """Run the installed-package assertion and emit its evidence."""
    sys.stdout.write(
        json.dumps(asyncio.run(_run(_arguments())), indent=2, sort_keys=True)
        + "\n"
    )


if __name__ == "__main__":
    main()
