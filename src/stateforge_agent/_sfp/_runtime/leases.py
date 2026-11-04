"""Build and validate fenced leases over manager-owned Provider state."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from uuid import UUID

from stateforge_agent._sfp._runtime.models import (
    ProviderRuntimeState,
)
from stateforge_agent._sfp._services.capability_assessment.client import (
    SfpCapabilityAssessmentStaleError,
    SfpCapabilityAssessmentUnavailableError,
)
from stateforge_agent._sfp._services.capability_assessment.coordinator import (
    CapabilityAssessmentLease,
)
from stateforge_agent._sfp._services.operation_admission.client import (
    SfpOperationAdmissionStaleError,
)
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionLease,
    OperationAdmissionPreconditionError,
)
from stateforge_agent._sfp._services.operation_lifecycle.coordinator import (
    OperationLifecycleLease,
    OperationLifecyclePreconditionError,
)


class ProviderSessionLeases:
    """Snapshot current leases while the manager's one lock is held.

    The manager remains the owner of both the lock and Provider registry.
    This collaborator only reads those objects to create and validate fences.
    """

    def __init__(
        self,
        *,
        lock: asyncio.Lock,
        providers: Mapping[UUID, ProviderRuntimeState],
        is_closed: Callable[[], bool],
    ) -> None:
        """Bind lease operations to the manager-owned registry and lock."""
        self._lock = lock
        self._providers = providers
        self._is_closed = is_closed

    async def _operation_lifecycle_lease(
        self,
        provider_instance_id: UUID,
    ) -> OperationLifecycleLease:
        """Use the current channel even when Health blocks new admission."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if provider is None or provider.active_session is None:
                raise OperationLifecyclePreconditionError(
                    "Provider has no active session for Operation Lifecycle"
                )
            session = provider.active_session
            return OperationLifecycleLease(
                provider_instance_id=session.provider_instance_id,
                endpoint_generation=session.endpoint_generation,
                session_id=session.session_id,
                channel=session._channel,
                session_token=session,
            )

    async def _assert_operation_lifecycle_is_current(
        self,
        lease: OperationLifecycleLease,
    ) -> None:
        """Reject lifecycle work whose exact active session was replaced."""
        async with self._lock:
            provider = self._providers.get(lease.provider_instance_id)
            if (
                self._is_closed()
                or provider is None
                or provider.active_session is not lease.session_token
            ):
                raise OperationLifecyclePreconditionError(
                    "Operation Lifecycle session is no longer current"
                )

    async def _operation_admission_lease(
        self,
        provider_instance_id: UUID,
    ) -> OperationAdmissionLease:
        """Snapshot current operation declarations and availability fences."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if provider is None or provider.active_session is None:
                raise OperationAdmissionPreconditionError(
                    "Provider has no active SFP session"
                )
            status = provider.availability_status
            if status is None or not status.eligible:
                raise OperationAdmissionPreconditionError(
                    "Provider is not available for Operation Admission"
                )
            session = provider.active_session
            return OperationAdmissionLease(
                provider_instance_id=session.provider_instance_id,
                endpoint_generation=session.endpoint_generation,
                session_id=session.session_id,
                manifest_revision=session.manifest.key.revision,
                availability_generation=provider.availability_generation,
                operation_declarations=session.manifest.operation_declarations,
                channel=session._channel,
                session_token=session,
            )

    async def _assert_operation_admission_is_current(
        self,
        lease: OperationAdmissionLease,
    ) -> None:
        """Fence admission to the exact session and available generation."""
        async with self._lock:
            provider = self._providers.get(lease.provider_instance_id)
            if (
                provider is None
                or provider.active_session is not lease.session_token
                or provider.availability_generation
                != lease.availability_generation
            ):
                raise SfpOperationAdmissionStaleError(
                    "Operation Admission session is no longer current"
                )
            status = provider.availability_status
            if status is None or not status.eligible:
                raise SfpOperationAdmissionStaleError(
                    "Provider is no longer available for Operation Admission"
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
                operation_identifiers=session.manifest.operation_identifiers,
                channel=session._channel,
                session_token=session,
            )

    async def _assert_capability_assessment_is_current(
        self,
        lease: CapabilityAssessmentLease,
    ) -> None:
        """Fence assessment to the exact session and availability snapshot."""
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
