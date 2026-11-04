"""Expose operation-facing APIs over manager-owned session leases."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol
from uuid import UUID

from google.protobuf.json_format import MessageToDict

from stateforge_agent._sfp._services.capability_assessment.coordinator import (
    CapabilityAssessmentCoordinator,
    CapabilityAssessmentLease,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
)
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCoordinator,
    OperationAdmissionLease,
    OperationAdmissionPreconditionError,
)
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionRequest,
    OperationAdmissionResult,
)
from stateforge_agent._sfp._services.operation_lifecycle.coordinator import (
    OperationLifecycleCoordinator,
    OperationLifecycleLease,
    OperationLifecyclePreconditionError,
)
from stateforge_agent._sfp._services.operation_lifecycle.models import (
    OperationStatus,
)


class ProviderOperationSessions(Protocol):
    """Provide leases and fence checks owned by the session manager."""

    async def _capability_assessment_lease(
        self,
        provider_instance_id: UUID,
    ) -> CapabilityAssessmentLease: ...

    async def _assert_capability_assessment_is_current(
        self,
        lease: CapabilityAssessmentLease,
    ) -> None: ...

    async def _operation_admission_lease(
        self,
        provider_instance_id: UUID,
    ) -> OperationAdmissionLease: ...

    async def _assert_operation_admission_is_current(
        self,
        lease: OperationAdmissionLease,
    ) -> None: ...

    async def _operation_lifecycle_lease(
        self,
        provider_instance_id: UUID,
    ) -> OperationLifecycleLease: ...

    async def _assert_operation_lifecycle_is_current(
        self,
        lease: OperationLifecycleLease,
    ) -> None: ...


class ProviderOperations:
    """Delegate operation APIs while keeping session fences in the manager."""

    def __init__(
        self,
        *,
        sessions: ProviderOperationSessions,
        capability_assessment: CapabilityAssessmentCoordinator,
        operation_admission: OperationAdmissionCoordinator | None,
        operation_lifecycle: OperationLifecycleCoordinator | None,
    ) -> None:
        """Bind operation coordinators to the manager's lease boundary."""
        self._sessions = sessions
        self._capability_assessment = capability_assessment
        self._operation_admission = operation_admission
        self._operation_lifecycle = operation_lifecycle

    async def assess_capabilities(
        self,
        provider_instance_id: UUID,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None = None,
    ) -> CapabilityAssessment:
        """Read current Provider evidence without authorizing execution."""
        lease = await self._sessions._capability_assessment_lease(
            provider_instance_id
        )
        return await self._capability_assessment.assess(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=execution_context,
            assert_current=(
                self._sessions._assert_capability_assessment_is_current
            ),
        )

    async def cached_capability_assessment(
        self,
        provider_instance_id: UUID,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None = None,
    ) -> CapabilityAssessment | None:
        """Return exact-scope prior evidence, never an eligibility decision."""
        lease = await self._sessions._capability_assessment_lease(
            provider_instance_id
        )
        return await self._capability_assessment.cached(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=execution_context,
            assert_current=(
                self._sessions._assert_capability_assessment_is_current
            ),
        )

    async def admit_operation(
        self,
        request: OperationAdmissionRequest,
    ) -> OperationAdmissionResult:
        """Preflight and submit one newly authorized generic operation."""
        coordinator = self._operation_admission
        if coordinator is None:
            raise OperationAdmissionPreconditionError(
                "Operation Admission is not configured"
            )
        assessment = await self.assess_capabilities(
            request.provider_instance_id,
            (request.operation_identifier,),
            execution_context=request.execution_context,
        )
        lease = await self._sessions._operation_admission_lease(
            request.provider_instance_id
        )
        result = await coordinator.admit(
            lease,
            request,
            assessment,
            assert_current=self._sessions._assert_operation_admission_is_current,
        )
        await self.resume_observation(request.provider_instance_id)
        return result

    async def resolve_operation(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAdmissionResult:
        """Resolve one durable uncertain identity through a current session."""
        coordinator = self._operation_admission
        if coordinator is None:
            raise OperationAdmissionPreconditionError(
                "Operation Admission is not configured"
            )
        attempt = await coordinator.attempt(provider_instance_id, operation_id)
        if attempt is None:
            raise OperationAdmissionPreconditionError(
                "Operation Admission identity is not durable"
            )
        execution_context = MessageToDict(attempt.invocation.execution_context)
        assessment = await self.assess_capabilities(
            provider_instance_id,
            (attempt.invocation.operation_identifier,),
            execution_context=execution_context,
        )
        lease = await self._sessions._operation_admission_lease(
            provider_instance_id
        )
        result = await coordinator.submit(
            lease,
            operation_id,
            assessment,
            assert_current=self._sessions._assert_operation_admission_is_current,
        )
        await self.resume_observation(provider_instance_id)
        return result

    async def operation_status(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationStatus | None:
        """Return durable local knowledge of one operation."""
        coordinator = self._operation_lifecycle
        if coordinator is None:
            raise OperationLifecyclePreconditionError(
                "Operation Lifecycle is not configured"
            )
        return await coordinator.status(provider_instance_id, operation_id)

    async def cancel_operation(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationStatus:
        """Request cancellation for one admitted operation only."""
        coordinator = self._operation_lifecycle
        if coordinator is None:
            raise OperationLifecyclePreconditionError(
                "Operation Lifecycle is not configured"
            )
        lease = await self._sessions._operation_lifecycle_lease(
            provider_instance_id
        )
        return await coordinator.cancel(
            lease,
            operation_id,
            assert_current=self._sessions._assert_operation_lifecycle_is_current,
        )

    async def resume_observation(self, provider_instance_id: UUID) -> None:
        """Resume observing durable operations without resubmitting them."""
        coordinator = self._operation_lifecycle
        if coordinator is None:
            return
        try:
            lease = await self._sessions._operation_lifecycle_lease(
                provider_instance_id
            )
        except OperationLifecyclePreconditionError:
            return
        await coordinator.resume(
            lease,
            assert_current=self._sessions._assert_operation_lifecycle_is_current,
        )
