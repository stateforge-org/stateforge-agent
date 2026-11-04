"""Apply Health observations to manager-owned Provider availability state."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from uuid import UUID

from grpc_health.v1 import health_pb2

from stateforge_agent._sfp._runtime.models import (
    ProviderAvailabilityState,
    ProviderAvailabilityStatus,
    ProviderRuntimeState,
    ProviderSession,
)
from stateforge_agent._sfp._runtime.state import _availability_status
from stateforge_agent._sfp._runtime.watchers.triggers import (
    ProviderTriggerWatcher,
)
from stateforge_agent._sfp._services.capability_assessment.coordinator import (
    CapabilityAssessmentCoordinator,
)
from stateforge_agent._sfp._services.trigger_publication.client import (
    SfpTriggerPublicationStaleError,
)
from stateforge_agent._sfp._services.trigger_publication.coordinator import (
    TriggerPublicationCoordinator,
    TriggerPublicationLease,
)

_LOGGER = logging.getLogger(__name__)


class ProviderAvailabilityController:
    """Fence Health and Trigger observations against the active session.

    The manager supplies its one registry and lock. This controller stores no
    parallel session state; every read or transition uses that shared lock.
    """

    def __init__(
        self,
        *,
        lock: asyncio.Lock,
        providers: Mapping[UUID, ProviderRuntimeState],
        capability_assessment: CapabilityAssessmentCoordinator,
        trigger_publication: TriggerPublicationCoordinator | None,
        retry_initial_delay_seconds: float,
        retry_max_delay_seconds: float,
    ) -> None:
        """Bind availability changes to the manager-owned registry."""
        self._lock = lock
        self._providers = providers
        self._capability_assessment = capability_assessment
        self._trigger_publication = trigger_publication
        self._trigger_monitor = ProviderTriggerWatcher(
            coordinator=trigger_publication,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
            assert_current=self._assert_trigger_stream_is_current,
            on_finished=self._finish_trigger_watcher,
        )

    async def watch_triggers(
        self,
        session: ProviderSession,
        availability_generation: int,
    ) -> None:
        """Consume trigger publications for one exact available snapshot."""
        await self._trigger_monitor.watch(session, availability_generation)

    async def mark_health_watching(self, session: ProviderSession) -> bool:
        """Fail closed before opening or reopening a current Health Watch."""
        return await self.set_availability(
            session,
            ProviderAvailabilityState.WATCHING,
            "awaiting Provider Health status",
        )

    async def apply_health_status(
        self,
        session: ProviderSession,
        status: int,
    ) -> bool:
        """Apply one ordered Health status to its exact current session."""
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
        return await self.set_availability(session, state, reason)

    async def set_availability(
        self,
        session: ProviderSession,
        state: ProviderAvailabilityState,
        reason: str,
    ) -> bool:
        """Set health-derived eligibility for the current fenced session."""
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if provider is None or provider.active_session is not session:
                return False
            self.record_availability(
                provider,
                _availability_status(session, state, reason),
            )
            return True

    async def finish_health_watcher(self, session: ProviderSession) -> None:
        """Clear only the current task's Health watcher slot."""
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if (
                provider is not None
                and provider.health_watcher is asyncio.current_task()
            ):
                provider.health_watcher = None

    def record_availability(
        self,
        provider: ProviderRuntimeState,
        status: ProviderAvailabilityStatus,
    ) -> None:
        """Record one transition, invalidate evidence, and sync triggers."""
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
            self._synchronize_trigger_watcher(provider)

    def _synchronize_trigger_watcher(
        self,
        provider: ProviderRuntimeState,
    ) -> None:
        """Run Trigger Publication only when the session is eligible."""
        watcher = provider.trigger_watcher
        status = provider.availability_status
        session = provider.active_session
        if (
            self._trigger_publication is None
            or status is None
            or not status.eligible
            or session is None
            or not session.manifest.trigger_schemas
        ):
            provider.trigger_watcher = None
            if watcher is not None:
                watcher.cancel()
            return
        if watcher is not None and not watcher.done():
            return
        provider.trigger_watcher = asyncio.create_task(
            self.watch_triggers(session, provider.availability_generation),
            name=f"stateforge-sfp-triggers-{session.provider_instance_id}",
        )

    async def _finish_trigger_watcher(
        self,
        session: ProviderSession,
    ) -> None:
        """Clear only the current task's Trigger watcher slot."""
        async with self._lock:
            provider = self._providers.get(session.provider_instance_id)
            if (
                provider is not None
                and provider.trigger_watcher is asyncio.current_task()
            ):
                provider.trigger_watcher = None

    async def _assert_trigger_stream_is_current(
        self,
        lease: TriggerPublicationLease,
    ) -> None:
        """Reject events from a replaced or unavailable Trigger session."""
        async with self._lock:
            provider = self._providers.get(lease.provider_instance_id)
            if (
                provider is None
                or provider.active_session is not lease.session_token
                or provider.availability_generation
                != lease.availability_generation
            ):
                raise SfpTriggerPublicationStaleError(
                    "Trigger Publication session is no longer current"
                )
            status = provider.availability_status
            if status is None or not status.eligible:
                raise SfpTriggerPublicationStaleError(
                    "Provider is unavailable for Trigger Publication"
                )
