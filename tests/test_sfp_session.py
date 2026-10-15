"""Integration tests for Agent-side SFP session establishment."""

import asyncio
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc
from stateforge_protocol.sfp.bootstrap.v1 import (
    bootstrap_pb2,
    bootstrap_pb2_grpc,
)
from stateforge_protocol.sfp.bootstrap.v1 import (
    contract as bootstrap_contract,
)

from stateforge_agent._sfp import (
    ProviderAvailabilityState,
    ProviderConnectionState,
    ProviderSession,
    ProviderSessionManager,
    SfpConnectionError,
    SfpIncompatibleError,
    SfpManifestError,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.channel import create_provider_channel
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._services.bootstrap.client import perform_handshake
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestCacheKey,
    ManifestDeclaration,
)

_PROVIDER_INSTANCE_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_DIFFERENT_PROVIDER_INSTANCE_ID = "00000000-0000-4000-8000-000000000000"
_SFP_VERSION = SfpVersion(major=1, minor=0)
_REPLACEMENT_ENDPOINT_GENERATION = 2
_MANIFEST_REVISION = "sha256:" + ("0" * 64)
_VARINT_DATA_MASK = 0x7F
_VARINT_CONTINUATION_BIT = 0x80
_END_HEALTH_WATCH = object()


def _manifest_for(
    provider_instance_id: UUID = _PROVIDER_INSTANCE_ID,
) -> ManifestDeclaration:
    return ManifestDeclaration(
        key=ManifestCacheKey(
            provider_instance_id=provider_instance_id,
            sfp_version=_SFP_VERSION,
            schema_version=1,
            revision=_MANIFEST_REVISION,
        ),
        document=b"{}",
    )


class _StaticManifestClient:
    async def retrieve(
        self,
        channel,
        descriptor: ProviderEndpointDescriptor,
        *,
        sfp_version: SfpVersion,
        session_id: str,
    ) -> ManifestDeclaration:
        del channel, sfp_version, session_id
        return _manifest_for(descriptor.provider_instance_id)


class _BlockingManifestClient(_StaticManifestClient):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def retrieve(
        self,
        channel,
        descriptor: ProviderEndpointDescriptor,
        *,
        sfp_version: SfpVersion,
        session_id: str,
    ) -> ManifestDeclaration:
        self.started.set()
        await self.release.wait()
        return await super().retrieve(
            channel,
            descriptor,
            sfp_version=sfp_version,
            session_id=session_id,
        )


class _RejectingManifestClient:
    async def retrieve(
        self,
        channel,
        descriptor: ProviderEndpointDescriptor,
        *,
        sfp_version: SfpVersion,
        session_id: str,
    ) -> ManifestDeclaration:
        del channel, descriptor, sfp_version, session_id
        raise SfpManifestError("invalid declaration")


class _HealthServicer(health_pb2_grpc.HealthServicer):
    """Expose controllable Provider-wide Health behavior for sessions."""

    def __init__(self, initial_status: int | None) -> None:
        self.status = initial_status
        self.watch_calls = 0
        self.watch_started = asyncio.Event()
        self._observers: set[asyncio.Queue[int | object]] = set()

    async def Check(self, request, context):  # noqa: N802
        del request
        if self.status is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Provider-wide Health status is not registered",
            )
        return health_pb2.HealthCheckResponse(status=self.status)

    async def Watch(self, request, context):  # noqa: N802
        assert request.service == ""
        queue: asyncio.Queue[int | object] = asyncio.Queue()
        self._observers.add(queue)
        self.watch_calls += 1
        self.watch_started.set()
        try:
            if self.status is not None:
                yield health_pb2.HealthCheckResponse(status=self.status)
            while True:
                status = await queue.get()
                if status is _END_HEALTH_WATCH:
                    return
                assert isinstance(status, int)
                yield health_pb2.HealthCheckResponse(status=status)
        finally:
            self._observers.discard(queue)

    def publish(self, status: int) -> None:
        """Publish one ordered status to every active Watch."""
        self.status = status
        for observer in tuple(self._observers):
            observer.put_nowait(status)

    def finish_watch(self) -> None:
        """End every active Watch without closing the shared channel."""
        self.status = None
        for observer in tuple(self._observers):
            observer.put_nowait(_END_HEALTH_WATCH)


@dataclass(frozen=True, slots=True)
class _HandshakeResponseOverrides:
    """Override selected fields in a test Provider handshake response."""

    provider_instance_id: str | None = None
    endpoint_generation: int | None = None
    selected_sfp_version: tuple[int, int] | None = None
    omit_selected_sfp_version: bool = False
    provider_runtime_id: str | None = None
    session_id: str | None = None
    serialized_size: int | None = None


def _encode_varint(value: int) -> bytes:
    """Encode one nonnegative integer for a test-only unknown field."""
    encoded = bytearray()
    while value > _VARINT_DATA_MASK:
        encoded.append((value & _VARINT_DATA_MASK) | _VARINT_CONTINUATION_BIT)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _pad_handshake_response(
    response: bootstrap_pb2.HandshakeResponse,
    target_size: int,
) -> bootstrap_pb2.HandshakeResponse:
    """Retain a valid response while padding its unknown wire fields."""
    serialized = response.SerializeToString()
    unknown_field_tag = _encode_varint((2_047 << 3) | 2)
    padding_size = target_size - len(serialized) - len(unknown_field_tag) - 1
    for _ in range(8):
        wire = (
            serialized
            + unknown_field_tag
            + _encode_varint(padding_size)
            + bytes(padding_size)
        )
        difference = target_size - len(wire)
        if difference == 0:
            return bootstrap_pb2.HandshakeResponse.FromString(wire)
        padding_size += difference
    raise AssertionError(f"Could not construct a {target_size}-byte response")


def _software_version_for_request_size(
    *,
    agent_runtime_id: UUID,
    descriptor: ProviderEndpointDescriptor,
    target_size: int,
) -> str:
    """Return a software version producing an exact-size request."""
    request = bootstrap_pb2.HandshakeRequest(
        agent_runtime=bootstrap_pb2.AgentRuntime(
            runtime_id=str(agent_runtime_id),
        ),
        expected_provider_instance_id=str(descriptor.provider_instance_id),
        expected_endpoint_generation=descriptor.endpoint_generation,
        supported_sfp_versions=[
            bootstrap_pb2.ProtocolVersion(
                major=_SFP_VERSION.major,
                minor=_SFP_VERSION.minor,
            )
        ],
    )
    software_version_length = target_size
    for _ in range(8):
        request.agent_runtime.software_version = "x" * software_version_length
        difference = target_size - request.ByteSize()
        if difference == 0:
            return request.agent_runtime.software_version
        software_version_length += difference
    raise AssertionError(f"Could not construct a {target_size}-byte request")


@pytest.fixture
def socket_directory() -> Iterator[Path]:
    directory = Path(tempfile.mkdtemp(prefix="sfp-", dir="/tmp"))
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


class _BootstrapServicer(bootstrap_pb2_grpc.BootstrapServiceServicer):
    def __init__(
        self,
        *,
        provider_instance_id: UUID,
        endpoint_generation: int,
        supported_versions: tuple[SfpVersion, ...],
        handshake_started: asyncio.Event | None = None,
        allow_handshake: asyncio.Event | None = None,
        response_overrides: _HandshakeResponseOverrides | None = None,
        handshake_error_code: grpc.StatusCode | None = None,
    ) -> None:
        self.provider_instance_id = provider_instance_id
        self.endpoint_generation = endpoint_generation
        self.supported_versions = supported_versions
        self.runtime_id = uuid4()
        self.handshake_started = handshake_started
        self.allow_handshake = allow_handshake
        self.response_overrides = (
            response_overrides or _HandshakeResponseOverrides()
        )
        self.handshake_calls = 0
        self.handshake_error_code = handshake_error_code

    async def Handshake(self, request, context):  # noqa: N802
        self.handshake_calls += 1
        if request.expected_provider_instance_id != str(
            self.provider_instance_id
        ):
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Provider identity mismatch",
            )
        if self.handshake_error_code is not None:
            await context.abort(
                self.handshake_error_code,
                "Injected transient handshake failure",
            )
        offered = {
            SfpVersion(major=value.major, minor=value.minor)
            for value in request.supported_sfp_versions
        }
        compatible = offered.intersection(self.supported_versions)
        if not compatible:
            await context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "No mutually supported SFP wire version",
            )
        if self.handshake_started is not None:
            self.handshake_started.set()
        if self.allow_handshake is not None:
            await self.allow_handshake.wait()
        selected = max(compatible)
        overrides = self.response_overrides
        selected_major, selected_minor = overrides.selected_sfp_version or (
            selected.major,
            selected.minor,
        )
        response = bootstrap_pb2.HandshakeResponse(
            provider_instance_id=(
                overrides.provider_instance_id
                or str(self.provider_instance_id)
            ),
            endpoint_generation=(
                overrides.endpoint_generation
                if overrides.endpoint_generation is not None
                else self.endpoint_generation
            ),
            provider_runtime_id=(
                overrides.provider_runtime_id or str(self.runtime_id)
            ),
            session_id=(
                overrides.session_id
                if overrides.session_id is not None
                else str(uuid4())
            ),
        )
        if not overrides.omit_selected_sfp_version:
            response.selected_sfp_version.CopyFrom(
                bootstrap_pb2.ProtocolVersion(
                    major=selected_major,
                    minor=selected_minor,
                )
            )
        if overrides.serialized_size is not None:
            response = _pad_handshake_response(
                response,
                overrides.serialized_size,
            )
        return response


class _BootstrapServer:
    def __init__(
        self,
        directory: Path,
        *,
        provider_instance_id: UUID = _PROVIDER_INSTANCE_ID,
        endpoint_generation: int,
        supported_versions: tuple[SfpVersion, ...] = (_SFP_VERSION,),
        handshake_started: asyncio.Event | None = None,
        allow_handshake: asyncio.Event | None = None,
        response_overrides: _HandshakeResponseOverrides | None = None,
        handshake_error_code: grpc.StatusCode | None = None,
        health_status: int | None = (health_pb2.HealthCheckResponse.SERVING),
        health_implemented: bool = True,
    ) -> None:
        self.provider_instance_id = provider_instance_id
        self.endpoint_generation = endpoint_generation
        self.socket_path = directory / (
            f"{provider_instance_id}.{endpoint_generation}.sock"
        )
        self.server = grpc.aio.server()
        self.servicer = _BootstrapServicer(
            provider_instance_id=provider_instance_id,
            endpoint_generation=endpoint_generation,
            supported_versions=supported_versions,
            handshake_started=handshake_started,
            allow_handshake=allow_handshake,
            response_overrides=response_overrides,
            handshake_error_code=handshake_error_code,
        )
        bootstrap_pb2_grpc.add_BootstrapServiceServicer_to_server(
            self.servicer,
            self.server,
        )
        self.health_servicer = _HealthServicer(health_status)
        if health_implemented:
            health_pb2_grpc.add_HealthServicer_to_server(
                self.health_servicer,
                self.server,
            )

    @property
    def descriptor(self) -> ProviderEndpointDescriptor:
        return ProviderEndpointDescriptor(
            schema_version="1",
            provider_instance_id=self.provider_instance_id,
            endpoint_generation=self.endpoint_generation,
            socket_path=self.socket_path,
        )

    async def start(self) -> None:
        self.socket_path.unlink(missing_ok=True)
        self.server.add_insecure_port(f"unix:{self.socket_path}")
        await self.server.start()

    async def stop(self) -> None:
        await self.server.stop(grace=None)


class _ControllableChannel:
    def __init__(self) -> None:
        self.state = grpc.ChannelConnectivity.READY
        self.state_changed = asyncio.Event()
        self.reconnect_requested = asyncio.Event()

    def get_state(self, try_to_connect: bool = False):
        if try_to_connect:
            self.reconnect_requested.set()
        return self.state

    async def wait_for_state_change(self, last_observed_state) -> None:
        while self.state is last_observed_state:
            self.state_changed.clear()
            if self.state is not last_observed_state:
                return
            await self.state_changed.wait()

    def transition_to(self, state: grpc.ChannelConnectivity) -> None:
        self.state = state
        self.state_changed.set()

    async def close(self) -> None:
        self.transition_to(grpc.ChannelConnectivity.SHUTDOWN)


def _manager(
    *,
    descriptor_loader=None,
    manifest_client=None,
    reconnect_initial_delay_seconds: float = 0.05,
    reconnect_max_delay_seconds: float = 0.2,
) -> ProviderSessionManager:
    return ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version="0.1.0",
        supported_sfp_versions=(_SFP_VERSION,),
        channel_ready_timeout_seconds=2,
        handshake_timeout_seconds=2,
        reconnect_initial_delay_seconds=reconnect_initial_delay_seconds,
        reconnect_max_delay_seconds=reconnect_max_delay_seconds,
        descriptor_loader=descriptor_loader,
        manifest_client=manifest_client or _StaticManifestClient(),
    )


async def _wait_for_state(
    manager: ProviderSessionManager,
    expected: ProviderConnectionState,
) -> None:
    """Wait briefly for one asynchronous state transition."""
    async with asyncio.timeout(3):
        while True:
            status = manager.connection_status(_PROVIDER_INSTANCE_ID)
            if status is not None and status.state is expected:
                return
            await asyncio.sleep(0)


async def _wait_for_availability(
    manager: ProviderSessionManager,
    expected: ProviderAvailabilityState,
    provider_instance_id: UUID = _PROVIDER_INSTANCE_ID,
) -> None:
    """Wait briefly for one asynchronous availability transition."""
    async with asyncio.timeout(3):
        while True:
            status = manager.availability_status(provider_instance_id)
            if status is not None and status.state is expected:
                return
            await asyncio.sleep(0)


def test_rejects_invalid_session_manager_configuration() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        ProviderSessionManager(
            agent_runtime_id=uuid4(),
            agent_software_version="",
            supported_sfp_versions=(_SFP_VERSION,),
        )
    with pytest.raises(ValueError, match="At least one"):
        ProviderSessionManager(
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(),
        )
    with pytest.raises(ValueError, match="duplicates"):
        ProviderSessionManager(
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(_SFP_VERSION, _SFP_VERSION),
        )
    with pytest.raises(ValueError, match="must be positive"):
        ProviderSessionManager(
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(_SFP_VERSION,),
            channel_ready_timeout_seconds=0,
        )
    with pytest.raises(ValueError, match="reconnect_initial"):
        ProviderSessionManager(
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(_SFP_VERSION,),
            reconnect_initial_delay_seconds=0,
        )
    with pytest.raises(ValueError, match="greater than or equal"):
        ProviderSessionManager(
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(_SFP_VERSION,),
            reconnect_initial_delay_seconds=2,
            reconnect_max_delay_seconds=1,
        )


def test_rejects_invalid_sfp_versions() -> None:
    with pytest.raises(ValueError, match="major version"):
        SfpVersion(major=0, minor=0)
    with pytest.raises(ValueError, match="minor version"):
        SfpVersion(major=1, minor=-1)


@pytest.mark.asyncio
async def test_establishes_and_reuses_negotiated_session(
    socket_directory: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager()
    await server.start()

    session = await manager.establish(server.descriptor)
    repeated_session = await manager.establish(server.descriptor)

    assert repeated_session is session
    assert session.provider_instance_id == _PROVIDER_INSTANCE_ID
    assert session.endpoint_generation == 1
    assert session.selected_sfp_version == _SFP_VERSION
    assert manager.get(_PROVIDER_INSTANCE_ID) is session
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED

    await manager.remove(_PROVIDER_INSTANCE_ID)
    await server.stop()
    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.UNAVAILABLE
    assert status.active_endpoint_generation is None
    assert status.candidate_endpoint_generation is None
    availability = manager.availability_status(_PROVIDER_INSTANCE_ID)
    assert availability is not None
    assert availability.state is ProviderAvailabilityState.UNAVAILABLE
    assert availability.endpoint_generation == 1
    assert availability.session_id == session.session_id
    assert "state=unavailable" in caplog.text
    assert "reason=Provider endpoint was removed" in caplog.text


@pytest.mark.asyncio
async def test_health_watch_gates_new_operation_eligibility(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        health_status=None,
    )
    manager = _manager()
    await server.start()
    try:
        session = await manager.establish(server.descriptor)
        await asyncio.wait_for(
            server.health_servicer.watch_started.wait(),
            timeout=1,
        )

        status = manager.availability_status(_PROVIDER_INSTANCE_ID)
        assert status is not None
        assert status.state is ProviderAvailabilityState.WATCHING
        assert not manager.is_eligible(_PROVIDER_INSTANCE_ID)
        assert status.endpoint_generation == session.endpoint_generation
        assert status.session_id == session.session_id
        assert status.manifest_revision == _MANIFEST_REVISION

        server.health_servicer.publish(health_pb2.HealthCheckResponse.SERVING)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )
        assert manager.is_eligible(_PROVIDER_INSTANCE_ID)

        server.health_servicer.publish(health_pb2.HealthCheckResponse.UNKNOWN)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.UNKNOWN,
        )
        assert not manager.is_eligible(_PROVIDER_INSTANCE_ID)

        server.health_servicer.publish(
            health_pb2.HealthCheckResponse.NOT_SERVING
        )
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.NOT_SERVING,
        )
        assert not manager.is_eligible(_PROVIDER_INSTANCE_ID)
        assert manager.get(_PROVIDER_INSTANCE_ID) is session

        server.health_servicer.publish(
            health_pb2.HealthCheckResponse.SERVICE_UNKNOWN
        )
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.SERVICE_UNKNOWN,
        )

        server.health_servicer.publish(health_pb2.HealthCheckResponse.SERVING)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )
    finally:
        await manager.close()
        await server.stop()


@pytest.mark.asyncio
async def test_ended_health_watch_fails_closed_and_retries(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager(
        reconnect_initial_delay_seconds=0.1,
        reconnect_max_delay_seconds=0.1,
    )
    await server.start()
    try:
        expected_watch_calls_after_retry = 2
        await manager.establish(server.descriptor)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )

        server.health_servicer.watch_started.clear()
        server.health_servicer.finish_watch()
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.UNAVAILABLE,
        )
        assert not manager.is_eligible(_PROVIDER_INSTANCE_ID)

        await asyncio.wait_for(
            server.health_servicer.watch_started.wait(),
            timeout=1,
        )
        assert (
            server.health_servicer.watch_calls
            == expected_watch_calls_after_retry
        )
        server.health_servicer.publish(health_pb2.HealthCheckResponse.SERVING)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )
    finally:
        await manager.close()
        await server.stop()


@pytest.mark.asyncio
async def test_health_watch_unimplemented_is_terminal_for_generation(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        health_implemented=False,
    )
    manager = _manager(
        reconnect_initial_delay_seconds=0.01,
        reconnect_max_delay_seconds=0.02,
    )
    await server.start()
    try:
        await manager.establish(server.descriptor)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.WATCH_UNSUPPORTED,
        )
        await asyncio.sleep(0.05)

        status = manager.availability_status(_PROVIDER_INSTANCE_ID)
        assert status is not None
        assert status.state is ProviderAvailabilityState.WATCH_UNSUPPORTED
        assert not status.eligible
    finally:
        await manager.close()
        await server.stop()


@pytest.mark.asyncio
async def test_rejected_manifest_never_opens_health_watch(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager(manifest_client=_RejectingManifestClient())
    await server.start()
    try:
        with pytest.raises(SfpManifestError, match="invalid declaration"):
            await manager.establish(server.descriptor)

        assert server.health_servicer.watch_calls == 0
        status = manager.availability_status(_PROVIDER_INSTANCE_ID)
        assert status is not None
        assert status.state is ProviderAvailabilityState.UNAVAILABLE
        assert not status.eligible
    finally:
        await manager.close()
        await server.stop()


@pytest.mark.asyncio
async def test_replacement_fences_stale_health_updates(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
    )
    second_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
        health_status=None,
    )
    manager = _manager()
    await first_server.start()
    await second_server.start()
    try:
        first_session = await manager.establish(first_server.descriptor)
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )

        second_session = await manager.establish(second_server.descriptor)
        await asyncio.wait_for(
            second_server.health_servicer.watch_started.wait(),
            timeout=1,
        )
        status = manager.availability_status(_PROVIDER_INSTANCE_ID)
        assert status is not None
        assert status.state is ProviderAvailabilityState.WATCHING
        assert status.endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION

        applied = await manager._apply_health_status(
            first_session,
            health_pb2.HealthCheckResponse.SERVING,
        )
        assert not applied
        assert not manager.is_eligible(_PROVIDER_INSTANCE_ID)

        second_server.health_servicer.publish(
            health_pb2.HealthCheckResponse.SERVING
        )
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )
        assert manager.get(_PROVIDER_INSTANCE_ID) is second_session
    finally:
        await manager.close()
        await first_server.stop()
        await second_server.stop()


@pytest.mark.asyncio
async def test_concurrent_health_streams_are_provider_scoped(
    socket_directory: Path,
) -> None:
    second_provider_id = uuid4()
    first_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        health_status=health_pb2.HealthCheckResponse.SERVING,
    )
    second_server = _BootstrapServer(
        socket_directory,
        provider_instance_id=second_provider_id,
        endpoint_generation=1,
        health_status=health_pb2.HealthCheckResponse.NOT_SERVING,
    )
    manager = _manager()
    await first_server.start()
    await second_server.start()
    try:
        await asyncio.gather(
            manager.establish(first_server.descriptor),
            manager.establish(second_server.descriptor),
        )
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
        )
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.NOT_SERVING,
            second_provider_id,
        )

        assert manager.is_eligible(_PROVIDER_INSTANCE_ID)
        assert not manager.is_eligible(second_provider_id)
        second_server.health_servicer.publish(
            health_pb2.HealthCheckResponse.SERVING
        )
        await _wait_for_availability(
            manager,
            ProviderAvailabilityState.AVAILABLE,
            second_provider_id,
        )
        assert manager.is_eligible(second_provider_id)
    finally:
        await manager.close()
        await first_server.stop()
        await second_server.stop()


@pytest.mark.parametrize(
    ("request_size", "accepted"),
    [
        (bootstrap_contract.MAX_REQUEST_BYTES, True),
        (bootstrap_contract.MAX_REQUEST_BYTES + 1, False),
    ],
)
@pytest.mark.asyncio
async def test_enforces_canonical_bootstrap_request_limit(
    socket_directory: Path,
    request_size: int,
    accepted: bool,
) -> None:
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    agent_runtime_id = uuid4()
    software_version = _software_version_for_request_size(
        agent_runtime_id=agent_runtime_id,
        descriptor=server.descriptor,
        target_size=request_size,
    )
    await server.start()
    channel = create_provider_channel(f"unix:{server.socket_path}")
    try:
        await channel.channel_ready()
        handshake = perform_handshake(
            channel,
            server.descriptor,
            agent_runtime_id=agent_runtime_id,
            agent_software_version=software_version,
            supported_sfp_versions=(_SFP_VERSION,),
            timeout_seconds=2,
        )
        if accepted:
            result = await handshake
            assert result.selected_sfp_version == _SFP_VERSION
            assert server.servicer.handshake_calls == 1
        else:
            with pytest.raises(
                SfpConnectionError,
                match="request exceeds the canonical Bootstrap limit",
            ):
                await handshake
            assert server.servicer.handshake_calls == 0
    finally:
        await channel.close()
        await server.stop()


@pytest.mark.parametrize(
    ("response_size", "accepted"),
    [
        (bootstrap_contract.MAX_RESPONSE_BYTES, True),
        (bootstrap_contract.MAX_RESPONSE_BYTES + 1, False),
    ],
)
@pytest.mark.asyncio
async def test_enforces_canonical_bootstrap_response_limit(
    socket_directory: Path,
    response_size: int,
    accepted: bool,
) -> None:
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        response_overrides=_HandshakeResponseOverrides(
            serialized_size=response_size
        ),
    )
    await server.start()
    channel = create_provider_channel(f"unix:{server.socket_path}")
    try:
        await channel.channel_ready()
        handshake = perform_handshake(
            channel,
            server.descriptor,
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(_SFP_VERSION,),
            timeout_seconds=2,
        )
        if accepted:
            result = await handshake
            assert result.selected_sfp_version == _SFP_VERSION
        else:
            with pytest.raises(
                SfpConnectionError,
                match="response exceeds the canonical Bootstrap limit",
            ):
                await handshake
        assert server.servicer.handshake_calls == 1
    finally:
        await channel.close()
        await server.stop()


@pytest.mark.asyncio
async def test_reports_connecting_before_channel_is_ready(
    socket_directory: Path,
) -> None:
    manager = _manager()
    descriptor = ProviderEndpointDescriptor(
        schema_version="1",
        provider_instance_id=_PROVIDER_INSTANCE_ID,
        endpoint_generation=1,
        socket_path=socket_directory / f"{_PROVIDER_INSTANCE_ID}.1.sock",
    )

    attempt = asyncio.create_task(manager.establish(descriptor))
    await _wait_for_state(manager, ProviderConnectionState.CONNECTING)
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.active_endpoint_generation is None
    assert status.candidate_endpoint_generation == 1

    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt

    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.UNAVAILABLE


@pytest.mark.asyncio
async def test_reports_handshaking_before_initial_promotion(
    socket_directory: Path,
) -> None:
    handshake_started = asyncio.Event()
    allow_handshake = asyncio.Event()
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        handshake_started=handshake_started,
        allow_handshake=allow_handshake,
    )
    manager = _manager()
    await server.start()

    attempt = asyncio.create_task(manager.establish(server.descriptor))
    await asyncio.wait_for(handshake_started.wait(), timeout=1)
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.HANDSHAKING
    assert status.active_endpoint_generation is None
    assert status.candidate_endpoint_generation == 1

    allow_handshake.set()
    await attempt
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED
    assert status.active_endpoint_generation == 1
    assert status.candidate_endpoint_generation is None

    await manager.close()
    await server.stop()


@pytest.mark.asyncio
async def test_retrieves_manifest_before_promoting_session(
    socket_directory: Path,
) -> None:
    manifest_client = _BlockingManifestClient()
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager(manifest_client=manifest_client)
    await server.start()

    attempt = asyncio.create_task(manager.establish(server.descriptor))
    await asyncio.wait_for(manifest_client.started.wait(), timeout=1)

    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.RETRIEVING_MANIFEST

    manifest_client.release.set()
    session = await attempt
    assert session.manifest == _manifest_for()
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED

    await manager.close()
    await server.stop()


@pytest.mark.asyncio
async def test_rejected_manifest_blocks_session_promotion(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager(manifest_client=_RejectingManifestClient())
    await server.start()

    with pytest.raises(SfpManifestError, match="invalid declaration"):
        await manager.establish(server.descriptor)

    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.MANIFEST_REJECTED
    handshake_calls = server.servicer.handshake_calls
    await asyncio.sleep(0.1)
    assert server.servicer.handshake_calls == handshake_calls

    await manager.close()
    await server.stop()


@pytest.mark.asyncio
async def test_preserves_opaque_handshake_session_identifier(
    socket_directory: Path,
) -> None:
    session_id = "manifest-session/A β"
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        response_overrides=_HandshakeResponseOverrides(session_id=session_id),
    )
    manager = _manager()
    await server.start()

    session = await manager.establish(server.descriptor)

    assert session.session_id == session_id
    await manager.close()
    await server.stop()


@pytest.mark.asyncio
async def test_detects_loss_of_established_channel(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager()
    await server.start()
    await manager.establish(server.descriptor)

    await server.stop()
    await _wait_for_state(manager, ProviderConnectionState.UNAVAILABLE)

    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.active_endpoint_generation is None
    assert status.candidate_endpoint_generation is None

    await manager.close()


@pytest.mark.asyncio
async def test_idle_channel_reconnects_without_invalidating_session() -> None:
    channel = _ControllableChannel()
    manager = _manager()
    session = ProviderSession(
        provider_instance_id=_PROVIDER_INSTANCE_ID,
        endpoint_generation=1,
        socket_path=(
            Path(tempfile.gettempdir()) / f"{_PROVIDER_INSTANCE_ID}.1.sock"
        ),
        selected_sfp_version=_SFP_VERSION,
        provider_runtime_id=uuid4(),
        session_id="opaque-session",
        manifest=_manifest_for(),
        _channel=channel,
    )
    watcher = asyncio.create_task(manager._watch_session(session))

    channel.transition_to(grpc.ChannelConnectivity.IDLE)
    await asyncio.wait_for(channel.reconnect_requested.wait(), timeout=1)

    channel.transition_to(grpc.ChannelConnectivity.READY)
    await asyncio.sleep(0)
    assert not watcher.done()

    channel.transition_to(grpc.ChannelConnectivity.SHUTDOWN)
    await asyncio.wait_for(watcher, timeout=1)
    await manager.close()


@pytest.mark.asyncio
async def test_reconnects_after_established_channel_loss(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager()
    await first_server.start()
    first_session = await manager.establish(first_server.descriptor)

    await first_server.stop()
    await _wait_for_state(manager, ProviderConnectionState.UNAVAILABLE)
    replacement_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
    )
    await replacement_server.start()
    await _wait_for_state(manager, ProviderConnectionState.CONNECTED)

    replacement_session = manager.get(_PROVIDER_INSTANCE_ID)
    assert replacement_session is not None
    assert replacement_session is not first_session
    assert replacement_session.provider_runtime_id != (
        first_session.provider_runtime_id
    )

    await manager.close()
    await replacement_server.stop()


@pytest.mark.asyncio
async def test_retry_reloads_descriptor_before_reconnecting(
    socket_directory: Path,
) -> None:
    unavailable_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        handshake_error_code=grpc.StatusCode.UNAVAILABLE,
    )
    descriptor = unavailable_server.descriptor
    reloads: list[UUID] = []

    def reload_descriptor(provider_instance_id: UUID):
        reloads.append(provider_instance_id)
        return descriptor

    manager = _manager(descriptor_loader=reload_descriptor)
    await unavailable_server.start()

    with pytest.raises(SfpConnectionError, match="UNAVAILABLE"):
        await manager.establish(descriptor)

    await unavailable_server.stop()
    compatible_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
    )
    await compatible_server.start()
    await _wait_for_state(manager, ProviderConnectionState.CONNECTED)

    assert reloads
    assert set(reloads) == {_PROVIDER_INSTANCE_ID}

    await manager.close()
    await compatible_server.stop()


@pytest.mark.asyncio
async def test_descriptor_removal_cancels_reconnect(
    socket_directory: Path,
) -> None:
    unavailable_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        handshake_error_code=grpc.StatusCode.UNAVAILABLE,
    )
    manager = _manager(
        reconnect_initial_delay_seconds=0.1,
        reconnect_max_delay_seconds=0.1,
    )
    await unavailable_server.start()

    with pytest.raises(SfpConnectionError, match="UNAVAILABLE"):
        await manager.establish(unavailable_server.descriptor)
    await manager.remove(_PROVIDER_INSTANCE_ID)
    await unavailable_server.stop()

    compatible_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
    )
    await compatible_server.start()
    await asyncio.sleep(0.15)

    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.UNAVAILABLE

    await manager.close()
    await compatible_server.stop()


def test_reconnect_delay_is_positive_and_capped() -> None:
    initial_ceiling = 2
    maximum_ceiling = 8
    manager = _manager(
        reconnect_initial_delay_seconds=initial_ceiling,
        reconnect_max_delay_seconds=maximum_ceiling,
    )

    initial_delay = manager._retry_delay(0)
    capped_delay = manager._retry_delay(100)

    assert initial_ceiling / 2 <= initial_delay <= initial_ceiling
    assert maximum_ceiling / 2 <= capped_delay <= maximum_ceiling


@pytest.mark.asyncio
async def test_keeps_old_session_available_while_replacement_connects(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(socket_directory, endpoint_generation=1)
    manager = _manager()
    await first_server.start()
    first_session = await manager.establish(first_server.descriptor)
    replacement_descriptor = ProviderEndpointDescriptor(
        schema_version="1",
        provider_instance_id=_PROVIDER_INSTANCE_ID,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
        socket_path=socket_directory
        / (f"{_PROVIDER_INSTANCE_ID}.{_REPLACEMENT_ENDPOINT_GENERATION}.sock"),
    )

    replacement = asyncio.create_task(
        manager.establish(replacement_descriptor)
    )
    await _wait_for_state(
        manager,
        ProviderConnectionState.REPLACING_CONNECTING,
    )
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.active_endpoint_generation == 1
    assert (
        status.candidate_endpoint_generation
        == _REPLACEMENT_ENDPOINT_GENERATION
    )
    assert manager.get(_PROVIDER_INSTANCE_ID) is first_session

    replacement.cancel()
    with pytest.raises(asyncio.CancelledError):
        await replacement

    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED
    assert status.active_endpoint_generation == 1
    assert status.candidate_endpoint_generation is None

    await manager.close()
    await first_server.stop()


@pytest.mark.asyncio
async def test_promotes_new_generation_before_retiring_old_session(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(socket_directory, endpoint_generation=1)
    second_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
    )
    manager = _manager()

    await first_server.start()
    first_session = await manager.establish(first_server.descriptor)
    await second_server.start()
    second_session = await manager.establish(second_server.descriptor)

    assert second_session is not first_session
    assert (
        second_session.endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
    )
    assert manager.get(_PROVIDER_INSTANCE_ID) is second_session

    await manager.close()
    await first_server.stop()
    await second_server.stop()


@pytest.mark.asyncio
async def test_keeps_old_session_available_during_replacement_handshake(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(socket_directory, endpoint_generation=1)
    handshake_started = asyncio.Event()
    allow_handshake = asyncio.Event()
    second_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
        handshake_started=handshake_started,
        allow_handshake=allow_handshake,
    )
    manager = _manager()
    await first_server.start()
    first_session = await manager.establish(first_server.descriptor)
    await second_server.start()

    replacement = asyncio.create_task(
        manager.establish(second_server.descriptor)
    )
    await asyncio.wait_for(handshake_started.wait(), timeout=1)
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.REPLACING_HANDSHAKING
    assert status.active_endpoint_generation == 1
    assert (
        status.candidate_endpoint_generation
        == _REPLACEMENT_ENDPOINT_GENERATION
    )
    assert manager.get(_PROVIDER_INSTANCE_ID) is first_session

    allow_handshake.set()
    second_session = await replacement
    assert second_session is not first_session
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED
    assert (
        status.active_endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
    )
    assert status.candidate_endpoint_generation is None

    await manager.close()
    await first_server.stop()
    await second_server.stop()


@pytest.mark.asyncio
async def test_active_loss_does_not_cancel_replacement_handshake(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(socket_directory, endpoint_generation=1)
    handshake_started = asyncio.Event()
    allow_handshake = asyncio.Event()
    second_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
        handshake_started=handshake_started,
        allow_handshake=allow_handshake,
    )
    manager = _manager()
    await first_server.start()
    await manager.establish(first_server.descriptor)
    await second_server.start()

    replacement = asyncio.create_task(
        manager.establish(second_server.descriptor)
    )
    await asyncio.wait_for(handshake_started.wait(), timeout=1)
    await first_server.stop()
    await _wait_for_state(manager, ProviderConnectionState.HANDSHAKING)

    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.active_endpoint_generation is None
    assert (
        status.candidate_endpoint_generation
        == _REPLACEMENT_ENDPOINT_GENERATION
    )
    assert manager.get(_PROVIDER_INSTANCE_ID) is None

    allow_handshake.set()
    recovered_session = await replacement
    assert (
        recovered_session.endpoint_generation
        == _REPLACEMENT_ENDPOINT_GENERATION
    )
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED
    assert (
        status.active_endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
    )

    await manager.close()
    await second_server.stop()


@pytest.mark.asyncio
async def test_failed_replacement_preserves_available_session(
    socket_directory: Path,
) -> None:
    first_server = _BootstrapServer(socket_directory, endpoint_generation=1)
    unavailable_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
        handshake_error_code=grpc.StatusCode.UNAVAILABLE,
    )
    manager = _manager()
    await first_server.start()
    first_session = await manager.establish(first_server.descriptor)
    await unavailable_server.start()

    with pytest.raises(SfpConnectionError, match="UNAVAILABLE"):
        await manager.establish(unavailable_server.descriptor)

    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED
    assert status.active_endpoint_generation == 1
    assert status.candidate_endpoint_generation is None
    assert manager.get(_PROVIDER_INSTANCE_ID) is first_session

    await manager.close()
    await first_server.stop()
    await unavailable_server.stop()


@pytest.mark.asyncio
async def test_removal_during_handshake_prevents_session_promotion(
    socket_directory: Path,
) -> None:
    handshake_started = asyncio.Event()
    allow_handshake = asyncio.Event()
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        handshake_started=handshake_started,
        allow_handshake=allow_handshake,
    )
    manager = _manager()
    await server.start()

    attempt = asyncio.create_task(manager.establish(server.descriptor))
    await asyncio.wait_for(handshake_started.wait(), timeout=1)
    await manager.remove(_PROVIDER_INSTANCE_ID)
    allow_handshake.set()

    with pytest.raises(SfpConnectionError, match="superseded or removed"):
        await attempt
    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.UNAVAILABLE

    await manager.close()
    await server.stop()


@pytest.mark.asyncio
async def test_new_generation_supersedes_older_in_flight_handshake(
    socket_directory: Path,
) -> None:
    first_handshake_started = asyncio.Event()
    allow_first_handshake = asyncio.Event()
    first_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        handshake_started=first_handshake_started,
        allow_handshake=allow_first_handshake,
    )
    second_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
    )
    manager = _manager()
    await first_server.start()
    await second_server.start()

    first_attempt = asyncio.create_task(
        manager.establish(first_server.descriptor)
    )
    await asyncio.wait_for(first_handshake_started.wait(), timeout=1)
    second_session = await manager.establish(second_server.descriptor)
    allow_first_handshake.set()
    first_result = await first_attempt

    assert first_result is second_session
    assert (
        second_session.endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
    )
    assert manager.get(_PROVIDER_INSTANCE_ID) is second_session

    await manager.close()
    await first_server.stop()
    await second_server.stop()


@pytest.mark.asyncio
async def test_rejects_incompatible_provider_version(
    socket_directory: Path,
) -> None:
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        supported_versions=(SfpVersion(major=2, minor=0),),
    )
    manager = _manager()
    await server.start()

    with pytest.raises(SfpIncompatibleError, match="no supported SFP"):
        await manager.establish(server.descriptor)

    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.INCOMPATIBLE
    assert server.servicer.handshake_calls == 1
    assert manager.get(_PROVIDER_INSTANCE_ID) is None

    await asyncio.sleep(0.25)
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.INCOMPATIBLE
    assert server.servicer.handshake_calls == 1

    await manager.close()
    await server.stop()


@pytest.mark.asyncio
async def test_new_generation_can_resolve_version_incompatibility(
    socket_directory: Path,
) -> None:
    incompatible_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        supported_versions=(SfpVersion(major=2, minor=0),),
    )
    compatible_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
    )
    manager = _manager()
    await incompatible_server.start()

    with pytest.raises(SfpIncompatibleError):
        await manager.establish(incompatible_server.descriptor)
    await compatible_server.start()
    session = await manager.establish(compatible_server.descriptor)

    assert session.endpoint_generation == _REPLACEMENT_ENDPOINT_GENERATION
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED

    await manager.close()
    await incompatible_server.stop()
    await compatible_server.stop()


@pytest.mark.asyncio
async def test_incompatible_replacement_preserves_older_session(
    socket_directory: Path,
) -> None:
    compatible_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
    )
    incompatible_server = _BootstrapServer(
        socket_directory,
        endpoint_generation=_REPLACEMENT_ENDPOINT_GENERATION,
        supported_versions=(SfpVersion(major=2, minor=0),),
    )
    manager = _manager()
    await compatible_server.start()
    existing_session = await manager.establish(compatible_server.descriptor)
    await incompatible_server.start()

    with pytest.raises(SfpIncompatibleError):
        await manager.establish(incompatible_server.descriptor)

    assert manager.get(_PROVIDER_INSTANCE_ID) is existing_session
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.CONNECTED
    assert status.active_endpoint_generation == 1

    await compatible_server.stop()
    await _wait_for_state(manager, ProviderConnectionState.INCOMPATIBLE)
    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    handshake_calls = incompatible_server.servicer.handshake_calls
    await asyncio.sleep(0.25)
    assert incompatible_server.servicer.handshake_calls == handshake_calls

    await manager.close()
    await incompatible_server.stop()


@pytest.mark.asyncio
async def test_provider_rejects_request_identity_mismatch(
    socket_directory: Path,
) -> None:
    different_provider = UUID("00000000-0000-4000-8000-000000000000")
    server = _BootstrapServer(
        socket_directory,
        provider_instance_id=different_provider,
        endpoint_generation=1,
    )
    manager = _manager()
    await server.start()
    descriptor = ProviderEndpointDescriptor(
        schema_version="1",
        provider_instance_id=_PROVIDER_INSTANCE_ID,
        endpoint_generation=1,
        socket_path=server.socket_path,
    )

    with pytest.raises(SfpConnectionError, match="FAILED_PRECONDITION"):
        await manager.establish(descriptor)

    await manager.close()
    await server.stop()


@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        (
            _HandshakeResponseOverrides(
                provider_instance_id=_DIFFERENT_PROVIDER_INSTANCE_ID
            ),
            "different Provider instance identity",
        ),
        (
            _HandshakeResponseOverrides(endpoint_generation=2),
            "different endpoint generation",
        ),
        (
            _HandshakeResponseOverrides(omit_selected_sfp_version=True),
            "selected_sfp_version is required",
        ),
        (
            _HandshakeResponseOverrides(selected_sfp_version=(0, 0)),
            "SFP major version must be a positive uint32",
        ),
        (
            _HandshakeResponseOverrides(selected_sfp_version=(2, 0)),
            "selected an SFP version the Agent did not offer",
        ),
        (
            _HandshakeResponseOverrides(provider_runtime_id="not-a-uuid"),
            "provider_runtime_id must be a canonical UUIDv4",
        ),
        (
            _HandshakeResponseOverrides(session_id=""),
            "session_id must contain 1-"
            f"{bootstrap_contract.MAX_SESSION_ID_CODE_POINTS} Unicode code "
            "points",
        ),
        (
            _HandshakeResponseOverrides(session_id="x" * 256),
            "session_id must contain 1-"
            f"{bootstrap_contract.MAX_SESSION_ID_CODE_POINTS} Unicode code "
            "points",
        ),
    ],
)
@pytest.mark.asyncio
async def test_agent_rejects_invalid_handshake_response(
    socket_directory: Path,
    overrides: _HandshakeResponseOverrides,
    expected_error: str,
) -> None:
    server = _BootstrapServer(
        socket_directory,
        endpoint_generation=1,
        response_overrides=overrides,
    )
    manager = _manager()
    await server.start()

    with pytest.raises(SfpConnectionError, match=expected_error):
        await manager.establish(server.descriptor)

    assert manager.get(_PROVIDER_INSTANCE_ID) is None
    status = manager.connection_status(_PROVIDER_INSTANCE_ID)
    assert status is not None
    assert status.state is ProviderConnectionState.UNAVAILABLE

    await manager.close()
    await server.stop()
