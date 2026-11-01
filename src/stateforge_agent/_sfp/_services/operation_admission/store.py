"""Define storage-neutral durable Operation Admission intent."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from stateforge_protocol.sfp.operation_admission.v1 import (
    operation_admission_pb2,
)

from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionResult,
    OperationRejection,
)


class OperationAttemptState(StrEnum):
    """Describe the Agent's durable knowledge of one admission attempt."""

    UNRESOLVED = "unresolved"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class OperationIntentDecision(StrEnum):
    """Classify an atomic immutable-intent creation request."""

    CREATED = "created"
    EXISTING = "existing"
    CONFLICT = "conflict"
    CAPACITY_EXHAUSTED = "capacity_exhausted"


@dataclass(frozen=True, slots=True)
class OperationAttempt:
    """Represent one immutable operation identity and current knowledge."""

    provider_instance_id: UUID
    operation_id: UUID
    invocation: operation_admission_pb2.OperationInvocation = field(
        repr=False,
        compare=False,
    )
    request_fingerprint: str
    state: OperationAttemptState
    created_at: datetime
    updated_at: datetime
    attempt_count: int
    last_endpoint_generation: int | None
    last_session_id: str | None
    result: OperationAdmissionResult | None = None
    rejection: OperationRejection | None = None


@dataclass(frozen=True, slots=True)
class OperationIntentResult:
    """Return an atomic creation decision and current durable attempt."""

    decision: OperationIntentDecision
    attempt: OperationAttempt | None


class OperationAdmissionStore(ABC):
    """Persist immutable Agent intent independently of a storage vendor."""

    @abstractmethod
    async def initialize(self) -> None:
        """Create or verify this procedure's journal schema."""

    @abstractmethod
    async def create_intent(
        self,
        provider_instance_id: UUID,
        invocation: operation_admission_pb2.OperationInvocation,
        request_fingerprint: str,
    ) -> OperationIntentResult:
        """Atomically create or classify one immutable operation identity."""

    @abstractmethod
    async def get(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAttempt | None:
        """Return one durable attempt when known."""

    @abstractmethod
    async def record_transport_attempt(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
        *,
        endpoint_generation: int,
        session_id: str,
    ) -> OperationAttempt:
        """Record the exact session used for one transport attempt."""

    @abstractmethod
    async def record_accepted(
        self,
        result: OperationAdmissionResult,
    ) -> OperationAttempt:
        """Resolve one durable attempt from a validated success response."""

    @abstractmethod
    async def record_rejected(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
        rejection: OperationRejection,
    ) -> OperationAttempt:
        """Record one canonical rejection without claiming execution."""

    @abstractmethod
    async def unresolved(self) -> tuple[OperationAttempt, ...]:
        """Return unresolved attempts available for restart recovery."""
