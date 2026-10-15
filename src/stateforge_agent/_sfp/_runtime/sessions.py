"""Establish and retain authenticated local SFP sessions."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from uuid import UUID

import grpc
from stateforge_protocol.sfp import primitives

from stateforge_agent._sfp._runtime.channel import create_provider_channel
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import (
    SfpConnectionError,
    SfpIncompatibleError,
    SfpManifestError,
)
from stateforge_agent._sfp._services.bootstrap.client import perform_handshake
from stateforge_agent._sfp._services.manifest.client import (
    ManifestClient,
    ManifestRetriever,
)
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestDeclaration,
)
from stateforge_agent._sfp.version import SfpVersion

_LOGGER = logging.getLogger(__name__)
_FAILED_CHANNEL_STATES = {
    grpc.ChannelConnectivity.TRANSIENT_FAILURE,
    grpc.ChannelConnectivity.SHUTDOWN,
}
_JITTER_STEPS = 1_000_000

ProviderDescriptorLoader = Callable[[UUID], ProviderEndpointDescriptor]


class ProviderConnectionState(StrEnum):
    """Describe the Agent's local connection state for one Provider."""

    UNAVAILABLE = "unavailable"
    CONNECTING = "connecting"
    HANDSHAKING = "handshaking"
    RETRIEVING_MANIFEST = "retrieving_manifest"
    CONNECTED = "connected"
    INCOMPATIBLE = "incompatible"
    MANIFEST_REJECTED = "manifest_rejected"
    REPLACING_CONNECTING = "replacing_connecting"
    REPLACING_HANDSHAKING = "replacing_handshaking"
    REPLACING_RETRIEVING_MANIFEST = "replacing_retrieving_manifest"


@dataclass(frozen=True, slots=True)
class ProviderSession:
    """One negotiated SFP session bound to a Provider endpoint generation."""

    provider_instance_id: UUID
    endpoint_generation: int
    socket_path: Path
    selected_sfp_version: SfpVersion
    provider_runtime_id: UUID
    session_id: str
    manifest: ManifestDeclaration
    _channel: grpc.aio.Channel = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class ProviderConnectionStatus:
    """Expose one immutable snapshot of local Provider connection state."""

    provider_instance_id: UUID
    state: ProviderConnectionState
    active_endpoint_generation: int | None
    candidate_endpoint_generation: int | None


@dataclass(slots=True)
class _ProviderRuntimeState:
    """Keep all mutable connection state for one Provider together."""

    desired_descriptor: ProviderEndpointDescriptor | None = None
    active_session: ProviderSession | None = None
    connection_status: ProviderConnectionStatus | None = None
    attempt_token: object | None = None
    channel_watcher: asyncio.Task[None] | None = None
    retry_task: asyncio.Task[None] | None = None


class ProviderSessionManager:
    """Promote validated channels and replace them without a blind gap."""

    def __init__(
        self,
        *,
        agent_runtime_id: UUID,
        agent_software_version: str,
        supported_sfp_versions: tuple[SfpVersion, ...],
        channel_ready_timeout_seconds: float = 5.0,
        handshake_timeout_seconds: float = 5.0,
        manifest_timeout_seconds: float = 5.0,
        manifest_read_attempts: int = 3,
        reconnect_initial_delay_seconds: float = 1.0,
        reconnect_max_delay_seconds: float = 30.0,
        descriptor_loader: ProviderDescriptorLoader | None = None,
        manifest_client: ManifestRetriever | None = None,
    ) -> None:
        """Create an empty Provider session registry."""
        if not isinstance(agent_runtime_id, UUID):
            raise ValueError("agent_runtime_id must be a UUIDv4")
        try:
            primitives.parse_canonical_uuid4(str(agent_runtime_id))
        except primitives.ProtocolScalarError as error:
            raise ValueError("agent_runtime_id must be a UUIDv4") from error
        if not agent_software_version:
            raise ValueError("agent_software_version must not be empty")
        if not supported_sfp_versions:
            raise ValueError("At least one SFP version must be supported")
        if len(set(supported_sfp_versions)) != len(supported_sfp_versions):
            raise ValueError(
                "supported_sfp_versions must not contain duplicates"
            )
        if channel_ready_timeout_seconds <= 0:
            raise ValueError("channel_ready_timeout_seconds must be positive")
        if handshake_timeout_seconds <= 0:
            raise ValueError("handshake_timeout_seconds must be positive")
        if manifest_timeout_seconds <= 0:
            raise ValueError("manifest_timeout_seconds must be positive")
        if manifest_read_attempts <= 0:
            raise ValueError("manifest_read_attempts must be positive")
        if reconnect_initial_delay_seconds <= 0:
            raise ValueError(
                "reconnect_initial_delay_seconds must be positive"
            )
        if reconnect_max_delay_seconds < reconnect_initial_delay_seconds:
            raise ValueError(
                "reconnect_max_delay_seconds must be greater than or equal "
                "to reconnect_initial_delay_seconds"
            )

        self._agent_runtime_id = agent_runtime_id
        self._agent_software_version = agent_software_version
        self._supported_sfp_versions = supported_sfp_versions
        self._channel_ready_timeout_seconds = channel_ready_timeout_seconds
        self._handshake_timeout_seconds = handshake_timeout_seconds
        self._manifest_client = (
            manifest_client
            if manifest_client is not None
            else ManifestClient(
                timeout_seconds=manifest_timeout_seconds,
                read_attempts=manifest_read_attempts,
            )
        )
        self._reconnect_initial_delay_seconds = reconnect_initial_delay_seconds
        self._reconnect_max_delay_seconds = reconnect_max_delay_seconds
        self._descriptor_loader = descriptor_loader
        self._providers: dict[UUID, _ProviderRuntimeState] = {}
        self._closed = False
        self._lock = asyncio.Lock()

    def get(self, provider_instance_id: UUID) -> ProviderSession | None:
        """Return the active session for one Provider, when present."""
        provider = self._providers.get(provider_instance_id)
        return provider.active_session if provider is not None else None

    def connection_status(
        self,
        provider_instance_id: UUID,
    ) -> ProviderConnectionStatus | None:
        """Return the latest local connection-state snapshot."""
        provider = self._providers.get(provider_instance_id)
        return provider.connection_status if provider is not None else None

    async def establish(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> ProviderSession:
        """Immediately connect one endpoint and retry it after failure."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("Provider session manager is closed")
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                _ProviderRuntimeState(),
            )
            desired = provider.desired_descriptor
            descriptor_is_stale = (
                desired is not None
                and desired.endpoint_generation
                > descriptor.endpoint_generation
            )
            if not descriptor_is_stale:
                provider.desired_descriptor = descriptor
            retry_task = provider.retry_task
            provider.retry_task = None

        if descriptor_is_stale:
            current_session = self.get(descriptor.provider_instance_id)
            if current_session is not None:
                return current_session
            raise SfpConnectionError(
                "Provider descriptor endpoint generation is stale"
            )
        await _cancel_task(retry_task)
        try:
            return await self._establish_once(descriptor)
        except SfpIncompatibleError:
            await self._mark_incompatible(descriptor)
            raise
        except SfpManifestError:
            await self._mark_manifest_rejected(descriptor)
            raise
        except SfpConnectionError:
            await self._schedule_retry(descriptor.provider_instance_id)
            raise

    async def _establish_once(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> ProviderSession:
        """Connect, handshake, and atomically promote one endpoint."""
        current_session, attempt = await self._begin_attempt(descriptor)
        if current_session is not None:
            return current_session
        if attempt is None:
            raise RuntimeError("Connection attempt was not initialized")

        channel = create_provider_channel(f"unix:{descriptor.socket_path}")
        try:
            candidate = await self._connect_candidate(
                channel=channel,
                descriptor=descriptor,
                attempt=attempt,
            )
            if candidate is None:
                await channel.close()
                async with self._lock:
                    provider = self._providers.get(
                        descriptor.provider_instance_id,
                    )
                    surviving_session = (
                        provider.active_session
                        if provider is not None
                        else None
                    )
                if surviving_session is None:
                    raise SfpConnectionError(
                        "Provider connection attempt was superseded or removed"
                    )
                return surviving_session
            (
                result,
                replaced_session,
                candidate_was_promoted,
            ) = await self._promote_candidate(
                candidate=candidate,
                descriptor=descriptor,
                attempt=attempt,
            )
        except BaseException:
            await channel.close()
            async with self._lock:
                provider = self._providers.get(
                    descriptor.provider_instance_id,
                )
                if provider is not None and provider.attempt_token is attempt:
                    provider.attempt_token = None
                    current_session = provider.active_session
                    if current_session is None:
                        provider.connection_status = ProviderConnectionStatus(
                            provider_instance_id=descriptor.provider_instance_id,
                            state=ProviderConnectionState.UNAVAILABLE,
                            active_endpoint_generation=None,
                            candidate_endpoint_generation=None,
                        )
                    else:
                        provider.connection_status = _connected_status(
                            current_session
                        )
            raise

        if not candidate_was_promoted:
            await channel.close()
        replaced_watcher: asyncio.Task[None] | None = None
        if candidate_was_promoted:
            async with self._lock:
                provider = self._providers.get(result.provider_instance_id)
                if provider is not None and provider.active_session is result:
                    replaced_watcher = provider.channel_watcher
                    provider.channel_watcher = asyncio.create_task(
                        self._watch_session(result),
                        name=(
                            "stateforge-sfp-channel-"
                            f"{result.provider_instance_id}"
                        ),
                    )
        await _cancel_task(replaced_watcher)
        if replaced_session is not None:
            await replaced_session._channel.close()
        return result

    async def _connect_candidate(
        self,
        *,
        channel: grpc.aio.Channel,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> ProviderSession | None:
        """Open and handshake one provisional Provider channel."""
        try:
            await asyncio.wait_for(
                channel.channel_ready(),
                timeout=self._channel_ready_timeout_seconds,
            )
        except TimeoutError as error:
            raise SfpConnectionError(
                "Provider gRPC channel did not become ready before the "
                f"deadline: {descriptor.socket_path}"
            ) from error

        if not await self._mark_handshaking(descriptor, attempt):
            return None

        result = await perform_handshake(
            channel,
            descriptor,
            agent_runtime_id=self._agent_runtime_id,
            agent_software_version=self._agent_software_version,
            supported_sfp_versions=self._supported_sfp_versions,
            timeout_seconds=self._handshake_timeout_seconds,
        )
        if not await self._mark_retrieving_manifest(descriptor, attempt):
            return None
        manifest = await self._manifest_client.retrieve(
            channel,
            descriptor,
            sfp_version=result.selected_sfp_version,
            session_id=result.session_id,
        )
        return ProviderSession(
            provider_instance_id=descriptor.provider_instance_id,
            endpoint_generation=descriptor.endpoint_generation,
            socket_path=descriptor.socket_path,
            selected_sfp_version=result.selected_sfp_version,
            provider_runtime_id=result.provider_runtime_id,
            session_id=result.session_id,
            manifest=manifest,
            _channel=channel,
        )

    async def _promote_candidate(
        self,
        *,
        candidate: ProviderSession,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> tuple[ProviderSession, ProviderSession | None, bool]:
        """Atomically promote a candidate or return the surviving session."""
        replaced_session: ProviderSession | None = None
        superseded_session: ProviderSession | None = None
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                _ProviderRuntimeState(),
            )
            attempt_is_current = provider.attempt_token is attempt
            if not attempt_is_current:
                superseded_session = provider.active_session
            else:
                provider.attempt_token = None
            current_session = provider.active_session
            if attempt_is_current and current_session is not None:
                if _matches(current_session, descriptor) or (
                    current_session.endpoint_generation
                    > descriptor.endpoint_generation
                ):
                    superseded_session = current_session
                else:
                    replaced_session = current_session
            if attempt_is_current and superseded_session is None:
                provider.active_session = candidate
                provider.connection_status = _connected_status(candidate)

        if not attempt_is_current:
            if superseded_session is None:
                raise SfpConnectionError(
                    "Provider connection attempt was superseded or removed"
                )
            return superseded_session, None, False
        if superseded_session is not None:
            return superseded_session, None, False
        return candidate, replaced_session, True

    async def remove(self, provider_instance_id: UUID) -> None:
        """Remove and close the active session for one Provider."""
        async with self._lock:
            provider = self._providers.setdefault(
                provider_instance_id,
                _ProviderRuntimeState(),
            )
            session = provider.active_session
            watcher = provider.channel_watcher
            retry_task = provider.retry_task
            provider.desired_descriptor = None
            provider.active_session = None
            provider.attempt_token = None
            provider.channel_watcher = None
            provider.retry_task = None
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=provider_instance_id,
                state=ProviderConnectionState.UNAVAILABLE,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )
        await _cancel_task(retry_task)
        await _cancel_task(watcher)
        if session is not None:
            await session._channel.close()

    async def close(self) -> None:
        """Close every active Provider channel."""
        async with self._lock:
            self._closed = True
            sessions = tuple(
                provider.active_session
                for provider in self._providers.values()
                if provider.active_session is not None
            )
            watchers = tuple(
                provider.channel_watcher
                for provider in self._providers.values()
                if provider.channel_watcher is not None
            )
            retry_tasks = tuple(
                provider.retry_task
                for provider in self._providers.values()
                if provider.retry_task is not None
            )
            for provider_instance_id, provider in self._providers.items():
                provider.desired_descriptor = None
                provider.active_session = None
                provider.attempt_token = None
                provider.channel_watcher = None
                provider.retry_task = None
                provider.connection_status = ProviderConnectionStatus(
                    provider_instance_id=provider_instance_id,
                    state=ProviderConnectionState.UNAVAILABLE,
                    active_endpoint_generation=None,
                    candidate_endpoint_generation=None,
                )
        for retry_task in retry_tasks:
            retry_task.cancel()
        for watcher in watchers:
            watcher.cancel()
        if retry_tasks:
            await asyncio.gather(*retry_tasks, return_exceptions=True)
        if watchers:
            await asyncio.gather(*watchers, return_exceptions=True)
        for session in sessions:
            await session._channel.close()

    async def _schedule_retry(self, provider_instance_id: UUID) -> None:
        """Ensure exactly one bounded retry loop exists for a Provider."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                self._closed
                or provider is None
                or provider.desired_descriptor is None
            ):
                return
            if provider.attempt_token is not None:
                return
            existing = provider.retry_task
            if existing is not None and not existing.done():
                return
            provider.retry_task = asyncio.create_task(
                self._retry_loop(provider_instance_id),
                name=f"stateforge-sfp-retry-{provider_instance_id}",
            )

    async def _retry_loop(self, provider_instance_id: UUID) -> None:
        """Retry a desired endpoint with capped exponential delay."""
        attempt_number = 0
        connected = False
        try:
            while True:
                delay = self._retry_delay(attempt_number)
                _LOGGER.info(
                    "Scheduling Provider reconnect: provider_instance_id=%s "
                    "attempt=%s delay_seconds=%.3f",
                    provider_instance_id,
                    attempt_number + 1,
                    delay,
                )
                await asyncio.sleep(delay)
                try:
                    descriptor = await self._load_retry_descriptor(
                        provider_instance_id
                    )
                except Exception as error:
                    _LOGGER.warning(
                        "Cannot reload Provider descriptor for reconnect: "
                        "provider_instance_id=%s error=%s",
                        provider_instance_id,
                        error,
                    )
                    attempt_number += 1
                    continue
                if descriptor is None:
                    return
                try:
                    await self._establish_once(descriptor)
                except SfpIncompatibleError as error:
                    _LOGGER.warning(
                        "Provider is SFP-incompatible: "
                        "provider_instance_id=%s endpoint_generation=%s "
                        "error=%s",
                        provider_instance_id,
                        descriptor.endpoint_generation,
                        error,
                    )
                    await self._mark_incompatible(descriptor)
                    return
                except SfpManifestError as error:
                    _LOGGER.warning(
                        "Provider manifest rejected: "
                        "provider_instance_id=%s endpoint_generation=%s "
                        "error=%s",
                        provider_instance_id,
                        descriptor.endpoint_generation,
                        error,
                    )
                    await self._mark_manifest_rejected(descriptor)
                    return
                except SfpConnectionError as error:
                    _LOGGER.warning(
                        "Provider reconnect failed: provider_instance_id=%s "
                        "endpoint_generation=%s error=%s",
                        provider_instance_id,
                        descriptor.endpoint_generation,
                        error,
                    )
                    attempt_number += 1
                    continue
                connected = True
                return
        finally:
            reschedule = False
            async with self._lock:
                provider = self._providers.get(provider_instance_id)
                if (
                    provider is not None
                    and provider.retry_task is asyncio.current_task()
                ):
                    provider.retry_task = None
                    desired = provider.desired_descriptor
                    active = provider.active_session
                    reschedule = (
                        connected
                        and not self._closed
                        and desired is not None
                        and (
                            active is None
                            or active.endpoint_generation
                            < desired.endpoint_generation
                        )
                    )
            if reschedule:
                await self._schedule_retry(provider_instance_id)

    async def _load_retry_descriptor(
        self,
        provider_instance_id: UUID,
    ) -> ProviderEndpointDescriptor | None:
        """Reload and retain the current validated retry target."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                self._closed
                or provider is None
                or provider.desired_descriptor is None
            ):
                return None
            desired = provider.desired_descriptor
            loader = self._descriptor_loader
        if loader is None:
            return desired

        loaded = loader(provider_instance_id)
        if loaded.provider_instance_id != provider_instance_id:
            raise SfpConnectionError(
                "Reloaded descriptor returned a different Provider identity"
            )
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                self._closed
                or provider is None
                or provider.desired_descriptor is None
            ):
                return None
            current = provider.desired_descriptor
            if loaded.endpoint_generation < current.endpoint_generation:
                raise SfpConnectionError(
                    "Reloaded descriptor endpoint generation moved backwards"
                )
            provider.desired_descriptor = loaded
        return loaded

    def _retry_delay(self, attempt_number: int) -> float:
        """Return capped exponential backoff with equal positive jitter."""
        ceiling = self._reconnect_initial_delay_seconds
        for _ in range(attempt_number):
            if ceiling >= self._reconnect_max_delay_seconds:
                break
            ceiling = min(
                ceiling * 2,
                self._reconnect_max_delay_seconds,
            )
        half_ceiling = ceiling / 2
        jitter_fraction = secrets.randbelow(_JITTER_STEPS + 1) / _JITTER_STEPS
        return half_ceiling + (half_ceiling * jitter_fraction)

    async def _mark_incompatible(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> None:
        """Record incompatibility unless an older session still survives."""
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                _ProviderRuntimeState(),
            )
            current_session = provider.active_session
            if current_session is not None:
                provider.connection_status = _connected_status(current_session)
                return
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=ProviderConnectionState.INCOMPATIBLE,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )

    async def _mark_manifest_rejected(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> None:
        """Record rejection unless an older validated session survives."""
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                _ProviderRuntimeState(),
            )
            current_session = provider.active_session
            if current_session is not None:
                provider.connection_status = _connected_status(current_session)
                return
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=ProviderConnectionState.MANIFEST_REJECTED,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )

    async def _watch_session(self, session: ProviderSession) -> None:
        """Invalidate one exact session when its gRPC channel is lost."""
        try:
            state = session._channel.get_state(try_to_connect=False)
            while state not in _FAILED_CHANNEL_STATES:
                if state is grpc.ChannelConnectivity.IDLE:
                    state = session._channel.get_state(try_to_connect=True)
                    if state in _FAILED_CHANNEL_STATES:
                        break
                await session._channel.wait_for_state_change(state)
                state = session._channel.get_state(try_to_connect=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception(
                "Provider channel watcher failed: provider_instance_id=%s "
                "endpoint_generation=%s",
                session.provider_instance_id,
                session.endpoint_generation,
            )
        await self._invalidate_lost_session(session)

    async def _invalidate_lost_session(
        self,
        session: ProviderSession,
    ) -> None:
        """Remove a failed active session without disturbing replacements."""
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if provider is None or provider.active_session is not session:
                return
            provider.active_session = None
            if provider.channel_watcher is asyncio.current_task():
                provider.channel_watcher = None

            status = provider.connection_status
            if (
                status is not None
                and status.state
                is ProviderConnectionState.REPLACING_CONNECTING
            ):
                state = ProviderConnectionState.CONNECTING
                candidate_generation = status.candidate_endpoint_generation
            elif (
                status is not None
                and status.state
                is ProviderConnectionState.REPLACING_HANDSHAKING
            ):
                state = ProviderConnectionState.HANDSHAKING
                candidate_generation = status.candidate_endpoint_generation
            elif (
                status is not None
                and status.state
                is ProviderConnectionState.REPLACING_RETRIEVING_MANIFEST
            ):
                state = ProviderConnectionState.RETRIEVING_MANIFEST
                candidate_generation = status.candidate_endpoint_generation
            else:
                state = ProviderConnectionState.UNAVAILABLE
                candidate_generation = None

            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=session.provider_instance_id,
                state=state,
                active_endpoint_generation=None,
                candidate_endpoint_generation=candidate_generation,
            )
        await session._channel.close()
        await self._schedule_retry(session.provider_instance_id)

    async def _begin_attempt(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> tuple[ProviderSession | None, object | None]:
        """Record a provisional connection without hiding an active session."""
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                _ProviderRuntimeState(),
            )
            current_session = provider.active_session
            if current_session is not None and (
                _matches(current_session, descriptor)
                or current_session.endpoint_generation
                > descriptor.endpoint_generation
            ):
                return current_session, None

            attempt = object()
            provider.attempt_token = attempt
            state = (
                ProviderConnectionState.REPLACING_CONNECTING
                if current_session is not None
                else ProviderConnectionState.CONNECTING
            )
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=state,
                active_endpoint_generation=(
                    current_session.endpoint_generation
                    if current_session is not None
                    else None
                ),
                candidate_endpoint_generation=descriptor.endpoint_generation,
            )
            return None, attempt

    async def _mark_handshaking(
        self,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> bool:
        """Advance the current provisional endpoint to SFP handshaking."""
        async with self._lock:
            provider = self._providers.get(descriptor.provider_instance_id)
            if provider is None or provider.attempt_token is not attempt:
                return False
            current_session = provider.active_session
            state = (
                ProviderConnectionState.REPLACING_HANDSHAKING
                if current_session is not None
                else ProviderConnectionState.HANDSHAKING
            )
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=state,
                active_endpoint_generation=(
                    current_session.endpoint_generation
                    if current_session is not None
                    else None
                ),
                candidate_endpoint_generation=descriptor.endpoint_generation,
            )
            return True

    async def _mark_retrieving_manifest(
        self,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> bool:
        """Advance the current provisional endpoint to manifest retrieval."""
        async with self._lock:
            provider = self._providers.get(descriptor.provider_instance_id)
            if provider is None or provider.attempt_token is not attempt:
                return False
            current_session = provider.active_session
            state = (
                ProviderConnectionState.REPLACING_RETRIEVING_MANIFEST
                if current_session is not None
                else ProviderConnectionState.RETRIEVING_MANIFEST
            )
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=state,
                active_endpoint_generation=(
                    current_session.endpoint_generation
                    if current_session is not None
                    else None
                ),
                candidate_endpoint_generation=descriptor.endpoint_generation,
            )
            return True


def _matches(
    session: ProviderSession,
    descriptor: ProviderEndpointDescriptor,
) -> bool:
    """Return whether a session already represents one descriptor."""
    return (
        session.endpoint_generation == descriptor.endpoint_generation
        and session.socket_path == descriptor.socket_path
    )


def _connected_status(session: ProviderSession) -> ProviderConnectionStatus:
    """Build transport status for one manifest-accepted session."""
    return ProviderConnectionStatus(
        provider_instance_id=session.provider_instance_id,
        state=ProviderConnectionState.CONNECTED,
        active_endpoint_generation=session.endpoint_generation,
        candidate_endpoint_generation=None,
    )


async def _cancel_task(task: asyncio.Task[None] | None) -> None:
    """Cancel and join one background lifecycle task."""
    if task is None or task is asyncio.current_task():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
