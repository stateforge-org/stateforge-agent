"""Establish and retain authenticated local SFP sessions."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from uuid import UUID

import grpc
from grpc_health.v1 import health_pb2
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
from stateforge_agent._sfp._services.capability_assessment.client import (
    CapabilityAssessmentRequester,
    SfpCapabilityAssessmentStaleError,
    SfpCapabilityAssessmentUnavailableError,
)
from stateforge_agent._sfp._services.capability_assessment.coordinator import (
    CapabilityAssessmentCoordinator,
    CapabilityAssessmentLease,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
)
from stateforge_agent._sfp._services.health.client import (
    HealthClient,
    HealthWatcher,
    SfpHealthError,
    SfpHealthWatchError,
)
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


class ProviderAvailabilityState(StrEnum):
    """Describe whether one current Provider may accept new operations."""

    UNAVAILABLE = "unavailable"
    WATCHING = "watching"
    AVAILABLE = "available"
    UNKNOWN = "unknown"
    NOT_SERVING = "not_serving"
    SERVICE_UNKNOWN = "service_unknown"
    WATCH_UNSUPPORTED = "watch_unsupported"


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


@dataclass(frozen=True, slots=True)
class ProviderAvailabilityStatus:
    """Expose one immutable operational-eligibility snapshot."""

    provider_instance_id: UUID
    state: ProviderAvailabilityState
    endpoint_generation: int | None
    session_id: str | None
    manifest_revision: str | None
    reason: str

    @property
    def eligible(self) -> bool:
        """Return whether new operation admission is currently allowed."""
        return self.state is ProviderAvailabilityState.AVAILABLE


@dataclass(slots=True)
class _ProviderRuntimeState:
    """Keep all mutable connection state for one Provider together."""

    desired_descriptor: ProviderEndpointDescriptor | None = None
    active_session: ProviderSession | None = None
    connection_status: ProviderConnectionStatus | None = None
    availability_status: ProviderAvailabilityStatus | None = None
    attempt_token: object | None = None
    channel_watcher: asyncio.Task[None] | None = None
    health_watcher: asyncio.Task[None] | None = None
    retry_task: asyncio.Task[None] | None = None
    availability_generation: int = 0


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
        health_retry_max_delay_seconds: float = 15.0,
        capability_assessment_timeout_seconds: float = 5.0,
        capability_assessment_attempts: int = 2,
        capability_assessment_retry_initial_delay_seconds: float = 0.25,
        capability_assessment_cache_entries: int = 128,
        descriptor_loader: ProviderDescriptorLoader | None = None,
        manifest_client: ManifestRetriever | None = None,
        health_client: HealthWatcher | None = None,
        capability_assessment_client: (
            CapabilityAssessmentRequester | None
        ) = None,
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
        if health_retry_max_delay_seconds <= 0:
            raise ValueError("health_retry_max_delay_seconds must be positive")
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
        self._health_retry_max_delay_seconds = health_retry_max_delay_seconds
        self._descriptor_loader = descriptor_loader
        self._health_client = (
            health_client if health_client is not None else HealthClient()
        )
        self._capability_assessment = CapabilityAssessmentCoordinator(
            timeout_seconds=capability_assessment_timeout_seconds,
            attempts=capability_assessment_attempts,
            retry_initial_delay_seconds=(
                capability_assessment_retry_initial_delay_seconds
            ),
            cache_entries=capability_assessment_cache_entries,
            client=capability_assessment_client,
        )
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

    def availability_status(
        self,
        provider_instance_id: UUID,
    ) -> ProviderAvailabilityStatus | None:
        """Return the latest eligibility snapshot for one Provider."""
        provider = self._providers.get(provider_instance_id)
        return provider.availability_status if provider is not None else None

    def is_eligible(self, provider_instance_id: UUID) -> bool:
        """Return whether one Provider currently accepts new operations."""
        status = self.availability_status(provider_instance_id)
        return status is not None and status.eligible

    async def assess_capabilities(
        self,
        provider_instance_id: UUID,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None = None,
    ) -> CapabilityAssessment:
        """Read current Provider evidence without authorizing execution."""
        lease = await self._capability_assessment_lease(provider_instance_id)
        return await self._capability_assessment.assess(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=execution_context,
            assert_current=self._assert_capability_assessment_is_current,
        )

    async def cached_capability_assessment(
        self,
        provider_instance_id: UUID,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None = None,
    ) -> CapabilityAssessment | None:
        """Return exact-scope prior evidence, never an eligibility decision."""
        lease = await self._capability_assessment_lease(provider_instance_id)
        return await self._capability_assessment.cached(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=execution_context,
            assert_current=self._assert_capability_assessment_is_current,
        )

    async def _capability_assessment_lease(
        self,
        provider_instance_id: UUID,
    ) -> CapabilityAssessmentLease:
        """Snapshot one active session and its availability fence."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if provider is None or provider.active_session is None:
                raise SfpCapabilityAssessmentUnavailableError(
                    "Provider has no active SFP session"
                )
            session = provider.active_session
            return CapabilityAssessmentLease(
                provider_instance_id=session.provider_instance_id,
                endpoint_generation=session.endpoint_generation,
                session_id=session.session_id,
                manifest_revision=session.manifest.key.revision,
                availability_generation=provider.availability_generation,
                operation_identifiers=(session.manifest.operation_identifiers),
                channel=session._channel,
                session_token=session,
            )

    async def _assert_capability_assessment_is_current(
        self,
        lease: CapabilityAssessmentLease,
    ) -> None:
        """Fence one procedure lease to current session and availability."""
        async with self._lock:
            provider = self._providers.get(lease.provider_instance_id)
            if (
                provider is None
                or provider.active_session is not lease.session_token
            ):
                raise SfpCapabilityAssessmentStaleError(
                    "Capability Assessment session is no longer current"
                )
            if (
                provider.availability_generation
                != lease.availability_generation
            ):
                raise SfpCapabilityAssessmentStaleError(
                    "Provider availability changed during assessment"
                )
            status = provider.availability_status
            if status is None or not status.eligible:
                raise SfpCapabilityAssessmentUnavailableError(
                    "Provider is not currently available for assessment"
                )

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
                        self._record_availability(
                            provider,
                            _unavailable_status(
                                descriptor.provider_instance_id,
                                "Provider connection attempt failed",
                            ),
                        )
                    else:
                        provider.connection_status = _connected_status(
                            current_session
                        )
            raise

        if not candidate_was_promoted:
            await channel.close()
        replaced_watcher: asyncio.Task[None] | None = None
        replaced_health_watcher: asyncio.Task[None] | None = None
        if candidate_was_promoted:
            async with self._lock:
                provider = self._providers.get(result.provider_instance_id)
                if provider is not None and provider.active_session is result:
                    replaced_watcher = provider.channel_watcher
                    replaced_health_watcher = provider.health_watcher
                    provider.channel_watcher = asyncio.create_task(
                        self._watch_session(result),
                        name=(
                            "stateforge-sfp-channel-"
                            f"{result.provider_instance_id}"
                        ),
                    )
                    provider.health_watcher = asyncio.create_task(
                        self._watch_health(result),
                        name=(
                            "stateforge-sfp-health-"
                            f"{result.provider_instance_id}"
                        ),
                    )
        await _cancel_task(replaced_watcher)
        await _cancel_task(replaced_health_watcher)
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
                self._record_availability(
                    provider,
                    _availability_status(
                        candidate,
                        ProviderAvailabilityState.WATCHING,
                        "awaiting initial Provider Health status",
                    ),
                )

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
            health_watcher = provider.health_watcher
            retry_task = provider.retry_task
            provider.desired_descriptor = None
            provider.active_session = None
            provider.attempt_token = None
            provider.channel_watcher = None
            provider.health_watcher = None
            provider.retry_task = None
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=provider_instance_id,
                state=ProviderConnectionState.UNAVAILABLE,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )
            self._record_availability(
                provider,
                (
                    _availability_status(
                        session,
                        ProviderAvailabilityState.UNAVAILABLE,
                        "Provider endpoint was removed",
                    )
                    if session is not None
                    else _unavailable_after(
                        provider.availability_status,
                        provider_instance_id,
                        "Provider endpoint was removed",
                    )
                ),
            )
        await _cancel_task(retry_task)
        await _cancel_task(watcher)
        await _cancel_task(health_watcher)
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
            health_watchers = tuple(
                provider.health_watcher
                for provider in self._providers.values()
                if provider.health_watcher is not None
            )
            retry_tasks = tuple(
                provider.retry_task
                for provider in self._providers.values()
                if provider.retry_task is not None
            )
            for provider_instance_id, provider in self._providers.items():
                session = provider.active_session
                provider.desired_descriptor = None
                provider.active_session = None
                provider.attempt_token = None
                provider.channel_watcher = None
                provider.health_watcher = None
                provider.retry_task = None
                provider.connection_status = ProviderConnectionStatus(
                    provider_instance_id=provider_instance_id,
                    state=ProviderConnectionState.UNAVAILABLE,
                    active_endpoint_generation=None,
                    candidate_endpoint_generation=None,
                )
                self._record_availability(
                    provider,
                    (
                        _availability_status(
                            session,
                            ProviderAvailabilityState.UNAVAILABLE,
                            "Agent is shutting down",
                        )
                        if session is not None
                        else _unavailable_after(
                            provider.availability_status,
                            provider_instance_id,
                            "Agent is shutting down",
                        )
                    ),
                )
        for retry_task in retry_tasks:
            retry_task.cancel()
        for watcher in watchers:
            watcher.cancel()
        for health_watcher in health_watchers:
            health_watcher.cancel()
        if retry_tasks:
            await asyncio.gather(*retry_tasks, return_exceptions=True)
        if watchers:
            await asyncio.gather(*watchers, return_exceptions=True)
        if health_watchers:
            await asyncio.gather(*health_watchers, return_exceptions=True)
        for session in sessions:
            await session._channel.close()
        self._capability_assessment.clear()

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
            self._record_availability(
                provider,
                _unavailable_status(
                    descriptor.provider_instance_id,
                    "Provider is SFP-incompatible",
                ),
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
            self._record_availability(
                provider,
                _unavailable_status(
                    descriptor.provider_instance_id,
                    "Provider manifest was rejected",
                ),
            )

    async def _watch_health(self, session: ProviderSession) -> None:
        """Maintain one fenced Provider-wide Health Watch with backoff."""
        attempt_number = 0
        try:
            while await self._mark_health_watching(session):
                try:
                    async for status in self._health_client.watch(
                        session._channel
                    ):
                        attempt_number = 0
                        if not await self._apply_health_status(
                            session, status
                        ):
                            return
                    reason = "Provider Health Watch ended"
                except asyncio.CancelledError:
                    raise
                except SfpHealthWatchError as error:
                    code = error.code
                    reason = f"Provider Health Watch failed: {code.name}"
                    if code is grpc.StatusCode.UNIMPLEMENTED:
                        await self._set_availability(
                            session,
                            ProviderAvailabilityState.WATCH_UNSUPPORTED,
                            reason,
                        )
                        return
                except SfpHealthError:
                    reason = "Provider Health Watch returned invalid data"
                except Exception:
                    _LOGGER.exception(
                        "Provider Health Watch failed locally: "
                        "provider_instance_id=%s endpoint_generation=%s",
                        session.provider_instance_id,
                        session.endpoint_generation,
                    )
                    reason = "Provider Health Watch failed locally"

                if not await self._set_availability(
                    session,
                    ProviderAvailabilityState.UNAVAILABLE,
                    reason,
                ):
                    return
                delay = self._health_retry_delay(attempt_number)
                attempt_number += 1
                _LOGGER.info(
                    "Scheduling Provider Health Watch retry: "
                    "provider_instance_id=%s endpoint_generation=%s "
                    "attempt=%s delay_seconds=%.3f",
                    session.provider_instance_id,
                    session.endpoint_generation,
                    attempt_number,
                    delay,
                )
                await asyncio.sleep(delay)
        finally:
            async with self._lock:
                provider = self._providers.get(session.provider_instance_id)
                if (
                    provider is not None
                    and provider.health_watcher is asyncio.current_task()
                ):
                    provider.health_watcher = None

    async def _mark_health_watching(self, session: ProviderSession) -> bool:
        """Fail closed before opening or reopening a current Health Watch."""
        return await self._set_availability(
            session,
            ProviderAvailabilityState.WATCHING,
            "awaiting Provider Health status",
        )

    async def _apply_health_status(
        self,
        session: ProviderSession,
        status: int,
    ) -> bool:
        """Apply one ordered status only to its exact current session."""
        if status == health_pb2.HealthCheckResponse.SERVING:
            state = ProviderAvailabilityState.AVAILABLE
            reason = "Provider Health reports SERVING"
        elif status == health_pb2.HealthCheckResponse.NOT_SERVING:
            state = ProviderAvailabilityState.NOT_SERVING
            reason = "Provider Health reports NOT_SERVING"
        elif status == health_pb2.HealthCheckResponse.SERVICE_UNKNOWN:
            state = ProviderAvailabilityState.SERVICE_UNKNOWN
            reason = "Provider-wide Health service is unknown"
        else:
            state = ProviderAvailabilityState.UNKNOWN
            reason = "Provider Health reports UNKNOWN"
        return await self._set_availability(session, state, reason)

    async def _set_availability(
        self,
        session: ProviderSession,
        state: ProviderAvailabilityState,
        reason: str,
    ) -> bool:
        """Set health-derived eligibility when the session fence is current."""
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if provider is None or provider.active_session is not session:
                return False
            status = _availability_status(
                session,
                state,
                reason,
            )
            self._record_availability(provider, status)
            return True

    def _record_availability(
        self,
        provider: _ProviderRuntimeState,
        status: ProviderAvailabilityStatus,
    ) -> None:
        """Store and log every operator-visible eligibility transition."""
        previous = provider.availability_status
        provider.availability_status = status
        if previous != status:
            provider.availability_generation += 1
            self._capability_assessment.discard_provider(
                status.provider_instance_id
            )
            _LOGGER.info(
                "Provider availability changed: "
                "provider_instance_id=%s endpoint_generation=%s "
                "state=%s eligible=%s reason=%s",
                status.provider_instance_id,
                status.endpoint_generation,
                status.state,
                status.eligible,
                status.reason,
            )

    def _health_retry_delay(self, attempt_number: int) -> float:
        """Return a capped jittered delay for a failed Health Watch."""
        ceiling = min(
            self._reconnect_initial_delay_seconds,
            self._health_retry_max_delay_seconds,
        )
        for _ in range(attempt_number):
            if ceiling >= self._health_retry_max_delay_seconds:
                break
            ceiling = min(ceiling * 2, self._health_retry_max_delay_seconds)
        half_ceiling = ceiling / 2
        jitter_fraction = secrets.randbelow(_JITTER_STEPS + 1) / _JITTER_STEPS
        return half_ceiling + (half_ceiling * jitter_fraction)

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
        health_watcher: asyncio.Task[None] | None = None
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if provider is None or provider.active_session is not session:
                return
            provider.active_session = None
            health_watcher = provider.health_watcher
            provider.health_watcher = None
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
            self._record_availability(
                provider,
                _availability_status(
                    session,
                    ProviderAvailabilityState.UNAVAILABLE,
                    "Provider channel was lost",
                ),
            )
        await _cancel_task(health_watcher)
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
            if current_session is None:
                self._record_availability(
                    provider,
                    _unavailable_status(
                        descriptor.provider_instance_id,
                        "Provider session is not established",
                    ),
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


def _availability_status(
    session: ProviderSession,
    state: ProviderAvailabilityState,
    reason: str,
) -> ProviderAvailabilityStatus:
    """Build an eligibility snapshot bound to one exact current session."""
    return ProviderAvailabilityStatus(
        provider_instance_id=session.provider_instance_id,
        state=state,
        endpoint_generation=session.endpoint_generation,
        session_id=session.session_id,
        manifest_revision=session.manifest.key.revision,
        reason=reason,
    )


def _unavailable_status(
    provider_instance_id: UUID,
    reason: str,
) -> ProviderAvailabilityStatus:
    """Build an eligibility snapshot without an accepted current session."""
    return ProviderAvailabilityStatus(
        provider_instance_id=provider_instance_id,
        state=ProviderAvailabilityState.UNAVAILABLE,
        endpoint_generation=None,
        session_id=None,
        manifest_revision=None,
        reason=reason,
    )


def _unavailable_after(
    previous: ProviderAvailabilityStatus | None,
    provider_instance_id: UUID,
    reason: str,
) -> ProviderAvailabilityStatus:
    """Fail closed while retaining the last accepted session identity."""
    if previous is None or previous.endpoint_generation is None:
        return _unavailable_status(provider_instance_id, reason)
    return ProviderAvailabilityStatus(
        provider_instance_id=provider_instance_id,
        state=ProviderAvailabilityState.UNAVAILABLE,
        endpoint_generation=previous.endpoint_generation,
        session_id=previous.session_id,
        manifest_revision=previous.manifest_revision,
        reason=reason,
    )


async def _cancel_task(task: asyncio.Task[None] | None) -> None:
    """Cancel and join one background lifecycle task."""
    if task is None or task is asyncio.current_task():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
