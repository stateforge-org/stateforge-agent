"""Verify Provider restart cannot redispatch work whose state was lost."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from uuid import UUID, uuid4

from stateforge_agent._journal import SqliteJournal
from stateforge_agent._sfp import (
    OperationAdmissionRequest,
    ProviderAvailabilityState,
    ProviderSessionManager,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
    load_provider_descriptor,
)
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCoordinator,
)
from stateforge_agent._sfp._services.operation_lifecycle.coordinator import (
    OperationLifecycleCoordinator,
)

_AGENT_JOURNAL = Path("/var/lib/stateforge-agent/journal/agent.db")
_DISCOVERY_DIRECTORY = Path("/run/stateforge-agent/providers")
_STATE_DIRECTORY = Path("/var/lib/stateforge-provider-fixtures")
_OPERATION = "stateforge.test/lifecycle@1"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider-instance-id", type=UUID, required=True)
    parser.add_argument("--provider-state-name", required=True)
    parser.add_argument("--provider-unit-name", required=True)
    return parser.parse_args()


async def _run(  # noqa: PLR0915 - one focused installed recovery scenario
    arguments: argparse.Namespace,
) -> dict[str, object]:
    provider_id: UUID = arguments.provider_instance_id
    descriptor_path = _DISCOVERY_DIRECTORY / f"{provider_id}.sfp.json"

    def descriptor_loader(
        requested_provider_id: UUID,
    ) -> ProviderEndpointDescriptor:
        if requested_provider_id != provider_id:
            raise AssertionError("Reconnect requested a different Provider")
        return load_provider_descriptor(
            descriptor_path,
            discovery_directory=_DISCOVERY_DIRECTORY,
        )

    journal = SqliteJournal(_AGENT_JOURNAL)
    await journal.open()
    store = journal.operation_admissions(maximum_unresolved_attempts=32)
    await store.initialize()
    manager = ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version=version("stateforge-agent"),
        supported_sfp_versions=(SfpVersion(major=1, minor=0),),
        descriptor_loader=descriptor_loader,
        reconnect_initial_delay_seconds=0.05,
        reconnect_max_delay_seconds=0.2,
        health_retry_max_delay_seconds=0.2,
        capability_assessment_retry_initial_delay_seconds=0.05,
        operation_admission=OperationAdmissionCoordinator(store),
        operation_lifecycle=OperationLifecycleCoordinator(
            store,
            retry_initial_delay_seconds=0.05,
            retry_max_delay_seconds=0.2,
        ),
    )
    state_directory = _STATE_DIRECTORY / arguments.provider_state_name
    event_log = state_directory / "events.log"
    try:
        await manager.establish(descriptor_loader(provider_id))
        async with asyncio.timeout(15):
            # Availability is observed through the asynchronous Health watcher.
            while not manager.is_eligible(provider_id):  # noqa: ASYNC110
                await asyncio.sleep(0.05)

        previous_session = manager.get(provider_id)
        if previous_session is None:
            raise AssertionError("Initial Provider session is absent")
        operation = await manager.admit_operation(
            OperationAdmissionRequest(
                provider_instance_id=provider_id,
                operation_identifier=_OPERATION,
                payload={"value": "held"},
                execution_deadline=datetime.now(UTC) + timedelta(minutes=2),
                execution_context={"scenario": "provider-process-restart"},
            )
        )
        async with asyncio.timeout(15):
            while True:
                status = await manager.operation_status(
                    provider_id,
                    operation.operation_id,
                )
                if status is not None and status.state == (
                    "OPERATION_STATE_RUNNING"
                ):
                    break
                await asyncio.sleep(0.05)

        restart = await asyncio.create_subprocess_exec(
            "/usr/bin/systemctl",
            "restart",
            arguments.provider_unit_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, restart_error = await restart.communicate()
        if restart.returncode != 0:
            raise AssertionError(
                "Provider systemd restart failed: "
                f"{restart_error.decode(errors='replace')}"
            )

        async with asyncio.timeout(20):
            while True:
                current_session = manager.get(provider_id)
                availability = manager.availability_status(provider_id)
                if (
                    current_session is not None
                    and current_session.endpoint_generation
                    > previous_session.endpoint_generation
                    and current_session.provider_runtime_id
                    != previous_session.provider_runtime_id
                    and availability is not None
                    and availability.state
                    is ProviderAvailabilityState.AVAILABLE
                ):
                    break
                await asyncio.sleep(0.05)

        await asyncio.sleep(0.25)
        final_status = await manager.operation_status(
            provider_id,
            operation.operation_id,
        )
        dispatches = sum(
            line.endswith(str(operation.operation_id))
            and line.startswith("operation:lifecycle:")
            for line in event_log.read_text(encoding="utf-8").splitlines()
        )
        if final_status is None or final_status.state != (
            "OPERATION_STATE_RUNNING"
        ):
            raise AssertionError(
                "Agent did not preserve the last proven running state after "
                "Provider state loss"
            )
        if dispatches != 1:
            raise AssertionError(
                "Provider replacement redispatched previously admitted work"
            )
        current_session = manager.get(provider_id)
        if current_session is None:
            raise AssertionError("Replacement Provider session disappeared")
        return {
            "agent_version": version("stateforge-agent"),
            "protocol_version": version("stateforge-protocol"),
            "provider_version": version("stateforge-provider"),
            "operation_id": str(operation.operation_id),
            "operation_state_after_provider_restart": final_status.state,
            "provider_instance_id": str(provider_id),
            "previous_endpoint_generation": (
                previous_session.endpoint_generation
            ),
            "replacement_endpoint_generation": (
                current_session.endpoint_generation
            ),
            "provider_runtime_changed": True,
            "dispatch_count": dispatches,
            "provider_state_retained": False,
            "no_redispatch": True,
        }
    finally:
        await manager.close()
        await journal.close()


def main() -> None:
    """Emit sanitized evidence from the installed packages."""
    sys.stdout.write(
        json.dumps(asyncio.run(_run(_arguments())), indent=2, sort_keys=True)
        + "\n"
    )


if __name__ == "__main__":
    main()
