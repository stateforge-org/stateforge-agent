"""Establish and retain authenticated local SFP sessions."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from uuid import UUID

from stateforge_protocol.sfp import primitives

from stateforge_agent._sfp._runtime.availability import (
    ProviderAvailabilityController,
)
from stateforge_agent._sfp._runtime.connections import (
    ProviderConnectionLifecycle,
)
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import (
    SfpConnectionError,
    SfpIncompatibleError,
    SfpManifestError,
)
from stateforge_agent._sfp._runtime.leases import ProviderSessionLeases
from stateforge_agent._sfp._runtime.models import (
    ProviderAvailabilityState,
    ProviderAvailabilityStatus,
    ProviderConnectionState,
    ProviderConnectionStatus,
    ProviderRuntimeState,
    ProviderSession,
)
from stateforge_agent._sfp._runtime.operations import ProviderOperations
from stateforge_agent._sfp._runtime.reconnect import (
    ProviderReconnectController,
)
from stateforge_agent._sfp._runtime.state import (
    _availability_status,
    _connected_status,
    _matches,
    _unavailable_after,
    _unavailable_status,
)
from stateforge_agent._sfp._runtime.watchers.connection import (
    ProviderConnectionWatcher,
)
from stateforge_agent._sfp._runtime.watchers.health import (
    ProviderHealthMonitor,
)
from stateforge_agent._sfp._services.capability_assessment.client import (
    CapabilityAssessmentRequester,
)
from stateforge_agent._sfp._services.capability_assessment.coordinator import (
    CapabilityAssessmentCoordinator,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
)
from stateforge_agent._sfp._services.health.client import (
    HealthClient,
    HealthWatcher,
)
from stateforge_agent._sfp._services.manifest.client import (
    ManifestClient,
    ManifestRetriever,
)
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCoordinator,
)
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionRequest,
    OperationAdmissionResult,
)
from stateforge_agent._sfp._services.operation_lifecycle.coordinator import (
    OperationLifecycleCoordinator,
)
from stateforge_agent._sfp._services.operation_lifecycle.models import (
    OperationStatus,
)
from stateforge_agent._sfp._services.trigger_publication.coordinator import (
    TriggerPublicationCoordinator,
)
from stateforge_agent._sfp.version import SfpVersion

ProviderDescriptorLoader = Callable[[UUID], ProviderEndpointDescriptor]


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
        trigger_publication: TriggerPublicationCoordinator | None = None,
        operation_admission: OperationAdmissionCoordinator | None = None,
        operation_lifecycle: OperationLifecycleCoordinator | None = None,
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
        self._operation_admission = operation_admission
        self._operation_lifecycle = operation_lifecycle
        self._providers: dict[UUID, ProviderRuntimeState] = {}
        self._closed = False
        self._lock = asyncio.Lock()
        self._leases = ProviderSessionLeases(
            lock=self._lock,
            providers=self._providers,
            is_closed=lambda: self._closed,
        )
        self._availability = ProviderAvailabilityController(
            lock=self._lock,
            providers=self._providers,
            capability_assessment=self._capability_assessment,
            trigger_publication=trigger_publication,
            retry_initial_delay_seconds=reconnect_initial_delay_seconds,
            retry_max_delay_seconds=health_retry_max_delay_seconds,
        )
        self._health_monitor = ProviderHealthMonitor(
            client=self._health_client,
            retry_initial_delay_seconds=reconnect_initial_delay_seconds,
            retry_max_delay_seconds=health_retry_max_delay_seconds,
            mark_watching=self._availability.mark_health_watching,
            apply_status=self._availability.apply_health_status,
            set_availability=self._availability.set_availability,
            on_finished=self._availability.finish_health_watcher,
        )
        self._operations = ProviderOperations(
            sessions=self._leases,
            capability_assessment=self._capability_assessment,
            operation_admission=self._operation_admission,
            operation_lifecycle=self._operation_lifecycle,
        )
        self._connection_lifecycle = ProviderConnectionLifecycle(
            owner=self,
            agent_runtime_id=agent_runtime_id,
            agent_software_version=agent_software_version,
            supported_sfp_versions=supported_sfp_versions,
            channel_ready_timeout_seconds=channel_ready_timeout_seconds,
            handshake_timeout_seconds=handshake_timeout_seconds,
            manifest_client=self._manifest_client,
        )
        self._reconnect = ProviderReconnectController(
            lock=self._lock,
            providers=self._providers,
            is_closed=lambda: self._closed,
            descriptor_loader=descriptor_loader,
            availability=self._availability,
        )
        self._connection_watcher = ProviderConnectionWatcher(
            retry_initial_delay_seconds=reconnect_initial_delay_seconds,
            retry_max_delay_seconds=reconnect_max_delay_seconds,
            load_retry_descriptor=self._reconnect.load_retry_descriptor,
            establish_endpoint=self._connection_lifecycle.establish,
            mark_incompatible=self._reconnect.mark_incompatible,
            mark_manifest_rejected=self._reconnect.mark_manifest_rejected,
            on_retry_finished=self._reconnect.finish_retry,
        )

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
        return await self._operations.assess_capabilities(
            provider_instance_id,
            operation_identifiers=operation_identifiers,
            execution_context=execution_context,
        )

    async def cached_capability_assessment(
        self,
        provider_instance_id: UUID,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None = None,
    ) -> CapabilityAssessment | None:
        """Return exact-scope prior evidence, never an eligibility decision."""
        return await self._operations.cached_capability_assessment(
            provider_instance_id,
            operation_identifiers=operation_identifiers,
            execution_context=execution_context,
        )

    async def admit_operation(
        self,
        request: OperationAdmissionRequest,
    ) -> OperationAdmissionResult:
        """Preflight and submit one newly authorized generic operation."""
        return await self._operations.admit_operation(request)

    async def resolve_operation(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAdmissionResult:
        """Resolve one durable uncertain identity through a current session."""
        return await self._operations.resolve_operation(
            provider_instance_id,
            operation_id,
        )

    async def operation_status(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationStatus | None:
        """Return durable local knowledge of one operation."""
        return await self._operations.operation_status(
            provider_instance_id,
            operation_id,
        )

    async def cancel_operation(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationStatus:
        """Request cancellation for one admitted operation only."""
        return await self._operations.cancel_operation(
            provider_instance_id,
            operation_id,
        )

    async def _resume_operation_observation(
        self, provider_instance_id: UUID
    ) -> None:
        await self._operations.resume_observation(provider_instance_id)

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
                ProviderRuntimeState(),
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
            return await self._connection_lifecycle.establish(descriptor)
        except SfpIncompatibleError:
            await self._reconnect.mark_incompatible(descriptor)
            raise
        except SfpManifestError:
            await self._reconnect.mark_manifest_rejected(descriptor)
            raise
        except SfpConnectionError:
            await self._reconnect.schedule_retry(
                descriptor.provider_instance_id,
                self._connection_watcher.retry,
            )
            raise

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
                ProviderRuntimeState(),
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
                self._availability.record_availability(
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

    async def _connection_attempt_failed(
        self,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> None:
        """Clear the failed attempt while preserving any active session."""
        async with self._lock:
            provider = self._providers.get(descriptor.provider_instance_id)
            if provider is None or provider.attempt_token is not attempt:
                return
            provider.attempt_token = None
            session = provider.active_session
            if session is None:
                provider.connection_status = ProviderConnectionStatus(
                    provider_instance_id=descriptor.provider_instance_id,
                    state=ProviderConnectionState.UNAVAILABLE,
                    active_endpoint_generation=None,
                    candidate_endpoint_generation=None,
                )
                self._availability.record_availability(
                    provider,
                    _unavailable_status(
                        descriptor.provider_instance_id,
                        "Provider connection attempt failed",
                    ),
                )
            else:
                provider.connection_status = _connected_status(session)

    async def _activate_candidate(
        self,
        session: ProviderSession,
        replaced_session: ProviderSession | None,
        *,
        promoted: bool,
    ) -> None:
        """Start current-session watchers and release replaced resources."""
        replaced_watcher: asyncio.Task[None] | None = None
        replaced_health_watcher: asyncio.Task[None] | None = None
        if promoted:
            async with self._lock:
                provider = self._providers.get(session.provider_instance_id)
                if provider is not None and provider.active_session is session:
                    replaced_watcher = provider.channel_watcher
                    replaced_health_watcher = provider.health_watcher
                    provider.channel_watcher = asyncio.create_task(
                        self._watch_session(session),
                        name=(
                            "stateforge-sfp-channel-"
                            f"{session.provider_instance_id}"
                        ),
                    )
                    provider.health_watcher = asyncio.create_task(
                        self._health_monitor.watch(session),
                        name=(
                            "stateforge-sfp-health-"
                            f"{session.provider_instance_id}"
                        ),
                    )
        await _cancel_task(replaced_watcher)
        await _cancel_task(replaced_health_watcher)
        if replaced_session is not None:
            await replaced_session._channel.close()
        await self._resume_operation_observation(session.provider_instance_id)

    async def remove(self, provider_instance_id: UUID) -> None:
        """Remove and close the active session for one Provider."""
        async with self._lock:
            provider = self._providers.setdefault(
                provider_instance_id,
                ProviderRuntimeState(),
            )
            session = provider.active_session
            watcher = provider.channel_watcher
            health_watcher = provider.health_watcher
            trigger_watcher = provider.trigger_watcher
            retry_task = provider.retry_task
            provider.desired_descriptor = None
            provider.active_session = None
            provider.attempt_token = None
            provider.channel_watcher = None
            provider.health_watcher = None
            provider.trigger_watcher = None
            provider.retry_task = None
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=provider_instance_id,
                state=ProviderConnectionState.UNAVAILABLE,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )
            self._availability.record_availability(
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
        await _cancel_task(trigger_watcher)
        if session is not None and self._operation_lifecycle is not None:
            await self._operation_lifecycle.suspend(
                provider_instance_id, session
            )
        if session is not None:
            await session._channel.close()

    async def close(self) -> None:
        """Close every active Provider channel."""
        async with self._lock:
            self._closed = True
            if self._operation_admission is not None:
                self._operation_admission.close()
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
            trigger_watchers = tuple(
                provider.trigger_watcher
                for provider in self._providers.values()
                if provider.trigger_watcher is not None
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
                provider.trigger_watcher = None
                provider.retry_task = None
                provider.connection_status = ProviderConnectionStatus(
                    provider_instance_id=provider_instance_id,
                    state=ProviderConnectionState.UNAVAILABLE,
                    active_endpoint_generation=None,
                    candidate_endpoint_generation=None,
                )
                self._availability.record_availability(
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
        for trigger_watcher in trigger_watchers:
            trigger_watcher.cancel()
        if retry_tasks:
            await asyncio.gather(*retry_tasks, return_exceptions=True)
        if watchers:
            await asyncio.gather(*watchers, return_exceptions=True)
        if health_watchers:
            await asyncio.gather(*health_watchers, return_exceptions=True)
        if trigger_watchers:
            await asyncio.gather(*trigger_watchers, return_exceptions=True)
        if self._operation_lifecycle is not None:
            await self._operation_lifecycle.close()
        for session in sessions:
            await session._channel.close()
        self._capability_assessment.clear()

    async def _watch_session(self, session: ProviderSession) -> None:
        """Invalidate one exact session when its gRPC channel is lost."""
        await self._connection_watcher.watch_channel(
            session,
            on_lost=self._invalidate_lost_session,
        )

    async def _invalidate_lost_session(
        self,
        session: ProviderSession,
    ) -> None:
        """Remove a failed active session without disturbing replacements."""
        health_watcher: asyncio.Task[None] | None = None
        trigger_watcher: asyncio.Task[None] | None = None
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if provider is None or provider.active_session is not session:
                return
            provider.active_session = None
            health_watcher = provider.health_watcher
            provider.health_watcher = None
            trigger_watcher = provider.trigger_watcher
            provider.trigger_watcher = None
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
            self._availability.record_availability(
                provider,
                _availability_status(
                    session,
                    ProviderAvailabilityState.UNAVAILABLE,
                    "Provider channel was lost",
                ),
            )
        await _cancel_task(health_watcher)
        await _cancel_task(trigger_watcher)
        if self._operation_lifecycle is not None:
            await self._operation_lifecycle.suspend(
                session.provider_instance_id, session
            )
        await session._channel.close()
        await self._reconnect.schedule_retry(
            session.provider_instance_id,
            self._connection_watcher.retry,
        )

    async def _begin_attempt(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> tuple[ProviderSession | None, object | None]:
        """Record a provisional connection without hiding an active session."""
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                ProviderRuntimeState(),
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
                self._availability.record_availability(
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


async def _cancel_task(task: asyncio.Task[None] | None) -> None:
    """Cancel and join one background lifecycle task."""
    if task is None or task is asyncio.current_task():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
