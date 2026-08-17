"""End-to-end tests for the local Provider endpoint lifecycle."""

import asyncio
import json
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import pytest
from stateforge_protocol.sfp.bootstrap.v1 import (
    bootstrap_pb2,
    bootstrap_pb2_grpc,
)

from stateforge_agent._runtime.daemon import _process_events
from stateforge_agent._runtime.provider_descriptor import (
    PROVIDER_DESCRIPTOR_SUFFIX,
    load_provider_descriptor,
)
from stateforge_agent._runtime.provider_monitor import (
    ProviderDirectoryEventKind,
    ProviderDirectoryMonitor,
)
from stateforge_agent._sfp import (
    ProviderConnectionState,
    ProviderSessionManager,
    SfpVersion,
)

_PROVIDER_INSTANCE_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_SFP_VERSION = SfpVersion(major=1, minor=0)
_INCOMPATIBLE_SFP_VERSION = SfpVersion(major=2, minor=0)
_REPLACEMENT_ENDPOINT_GENERATION = 2
_DESCRIPTOR_MODE = 0o640
_SOCKET_MODE = 0o660


@pytest.fixture
def discovery_directory() -> Iterator[Path]:
    """Provide a short Unix-socket-safe discovery directory."""
    directory = Path(tempfile.mkdtemp(prefix="sfp-life-", dir="/tmp"))
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


class _LifecycleBootstrapService(bootstrap_pb2_grpc.BootstrapServiceServicer):
    """Serve the minimum handshake required by a lifecycle test."""

    def __init__(
        self,
        *,
        endpoint_generation: int,
        supported_version: SfpVersion,
    ) -> None:
        self.endpoint_generation = endpoint_generation
        self.supported_version = supported_version
        self.runtime_id = uuid4()

    async def Handshake(self, request, context):  # noqa: N802
        offered_versions = {
            SfpVersion(major=value.major, minor=value.minor)
            for value in request.supported_sfp_versions
        }
        if self.supported_version not in offered_versions:
            await context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "No mutually supported SFP wire version",
            )
        return bootstrap_pb2.HandshakeResponse(
            provider_instance_id=str(_PROVIDER_INSTANCE_ID),
            endpoint_generation=self.endpoint_generation,
            selected_sfp_version=bootstrap_pb2.ProtocolVersion(
                major=self.supported_version.major,
                minor=self.supported_version.minor,
            ),
            provider_runtime_id=str(self.runtime_id),
            session_id=str(uuid4()),
        )


class _LifecycleProvider:
    """Represent one independently running Provider endpoint generation."""

    def __init__(
        self,
        directory: Path,
        *,
        endpoint_generation: int,
        supported_version: SfpVersion = _SFP_VERSION,
    ) -> None:
        self.directory = directory
        self.endpoint_generation = endpoint_generation
        self.socket_path = directory / (
            f"{_PROVIDER_INSTANCE_ID}.{endpoint_generation}.sock"
        )
        self.descriptor_path = directory / (
            f"{_PROVIDER_INSTANCE_ID}{PROVIDER_DESCRIPTOR_SUFFIX}"
        )
        self.server = grpc.aio.server()
        bootstrap_pb2_grpc.add_BootstrapServiceServicer_to_server(
            _LifecycleBootstrapService(
                endpoint_generation=endpoint_generation,
                supported_version=supported_version,
            ),
            self.server,
        )

    async def start(self) -> None:
        """Start the endpoint without publishing its descriptor."""
        self.socket_path.unlink(missing_ok=True)
        self.server.add_insecure_port(f"unix:{self.socket_path}")
        await self.server.start()
        self.socket_path.chmod(_SOCKET_MODE)

    def publish(self) -> None:
        """Atomically advertise this ready endpoint generation."""
        document = {
            "schema_version": "1",
            "provider_instance_id": str(_PROVIDER_INSTANCE_ID),
            "endpoint_generation": self.endpoint_generation,
            "transport": {
                "type": "unix",
                "socket_path": str(self.socket_path),
            },
        }
        temporary_path = self.descriptor_path.with_suffix(".tmp")
        temporary_path.write_text(
            json.dumps(document),
            encoding="utf-8",
        )
        temporary_path.chmod(_DESCRIPTOR_MODE)
        temporary_path.replace(self.descriptor_path)

    async def stop(self) -> None:
        """Stop this generation while leaving discovery under test control."""
        await self.server.stop(grace=None)


def _session_manager(directory: Path) -> ProviderSessionManager:
    """Build a real session manager that reloads validated descriptors."""

    def reload_descriptor(provider_instance_id: UUID):
        return load_provider_descriptor(
            directory / f"{provider_instance_id}{PROVIDER_DESCRIPTOR_SUFFIX}",
            discovery_directory=directory,
        )

    return ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version="0.1.0",
        supported_sfp_versions=(_SFP_VERSION,),
        channel_ready_timeout_seconds=1,
        handshake_timeout_seconds=1,
        reconnect_initial_delay_seconds=0.2,
        reconnect_max_delay_seconds=0.2,
        descriptor_loader=reload_descriptor,
    )


async def _wait_for_state(
    manager: ProviderSessionManager,
    expected_state: ProviderConnectionState,
) -> None:
    """Wait for one background channel transition."""
    async with asyncio.timeout(2):
        while True:
            status = manager.connection_status(_PROVIDER_INSTANCE_ID)
            if status is not None and status.state is expected_state:
                return
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_complete_discovery_replacement_and_removal_lifecycle(
    discovery_directory: Path,
) -> None:
    """Exercise the normal Agent-side endpoint lifecycle end to end."""
    monitor = ProviderDirectoryMonitor(discovery_directory)
    monitor.prepare()
    manager = _session_manager(discovery_directory)
    first_provider = _LifecycleProvider(
        discovery_directory,
        endpoint_generation=1,
    )
    replacement_provider = _LifecycleProvider(
        discovery_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
    )

    try:
        await first_provider.start()
        first_provider.publish()
        discovered = monitor.scan()
        assert discovered[0].kind is ProviderDirectoryEventKind.DISCOVERED
        await _process_events(monitor, manager, discovered)

        first_session = manager.get(_PROVIDER_INSTANCE_ID)
        assert first_session is not None
        assert first_session.endpoint_generation == 1
        assert monitor.scan() == ()

        await replacement_provider.start()
        replacement_provider.publish()
        updated = monitor.scan()
        assert updated[0].kind is ProviderDirectoryEventKind.UPDATED
        await _process_events(monitor, manager, updated)

        replacement_session = manager.get(_PROVIDER_INSTANCE_ID)
        assert replacement_session is not None
        assert replacement_session is not first_session
        assert (
            replacement_session.endpoint_generation
            == _REPLACEMENT_ENDPOINT_GENERATION
        )

        await first_provider.stop()
        assert manager.get(_PROVIDER_INSTANCE_ID) is replacement_session

        replacement_provider.descriptor_path.unlink()
        removed = monitor.scan()
        assert removed[0].kind is ProviderDirectoryEventKind.REMOVED
        await _process_events(monitor, manager, removed)

        assert manager.get(_PROVIDER_INSTANCE_ID) is None
        status = manager.connection_status(_PROVIDER_INSTANCE_ID)
        assert status is not None
        assert status.state is ProviderConnectionState.UNAVAILABLE
    finally:
        await manager.close()
        await first_provider.stop()
        await replacement_provider.stop()


@pytest.mark.asyncio
async def test_failure_update_channel_loss_and_recovery_lifecycle(
    discovery_directory: Path,
) -> None:
    """Exercise failed negotiation, immediate update, and reconnection."""
    monitor = ProviderDirectoryMonitor(discovery_directory)
    monitor.prepare()
    manager = _session_manager(discovery_directory)
    incompatible_provider = _LifecycleProvider(
        discovery_directory,
        endpoint_generation=1,
        supported_version=_INCOMPATIBLE_SFP_VERSION,
    )
    replacement_provider = _LifecycleProvider(
        discovery_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
    )
    restarted_provider = _LifecycleProvider(
        discovery_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
    )

    try:
        await incompatible_provider.start()
        incompatible_provider.publish()
        await _process_events(monitor, manager, monitor.scan())
        assert manager.get(_PROVIDER_INSTANCE_ID) is None

        await replacement_provider.start()
        replacement_provider.publish()
        await _process_events(monitor, manager, monitor.scan())
        connected = manager.get(_PROVIDER_INSTANCE_ID)
        assert connected is not None
        assert (
            connected.endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
        )

        await replacement_provider.stop()
        await _wait_for_state(
            manager,
            ProviderConnectionState.UNAVAILABLE,
        )

        await restarted_provider.start()
        await _wait_for_state(manager, ProviderConnectionState.AVAILABLE)
        recovered = manager.get(_PROVIDER_INSTANCE_ID)
        assert recovered is not None
        assert (
            recovered.endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
        )
        assert recovered.provider_runtime_id != connected.provider_runtime_id

        restarted_provider.descriptor_path.unlink()
        await _process_events(monitor, manager, monitor.scan())
        assert manager.get(_PROVIDER_INSTANCE_ID) is None
    finally:
        await manager.close()
        await incompatible_provider.stop()
        await replacement_provider.stop()
        await restarted_provider.stop()
