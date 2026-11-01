"""Define immutable Agent-normalized Operation Admission data."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID

JsonValue = (
    str
    | int
    | float
    | bool
    | None
    | list["JsonValue"]
    | dict[str, "JsonValue"]
)


class OperationAdmissionDisposition(StrEnum):
    """Describe how the Provider resolved one immutable identity."""

    ACCEPTED_NEW = "accepted_new"
    RESOLVED_EXISTING = "resolved_existing"


@dataclass(frozen=True, slots=True)
class OperationAdmissionRequest:
    """Describe one Agent-authorized generic operation attempt."""

    provider_instance_id: UUID
    operation_identifier: str
    payload: Mapping[str, JsonValue]
    execution_deadline: datetime
    execution_context: Mapping[str, JsonValue]
    required_implementation_roles: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OperationAdmissionResult:
    """Expose one validated Provider admission record."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    manifest_revision: str
    operation_id: UUID
    request_fingerprint: str
    disposition: OperationAdmissionDisposition
    admitted_at: datetime
    record_expires_at: datetime
    admitted_endpoint_generation: int


@dataclass(frozen=True, slots=True)
class OperationRejection:
    """Preserve one canonical sanitized Provider rejection."""

    operation_id: UUID | None
    reason: str
    retryable: bool
    retry_after: timedelta | None
    explanation: str
    grpc_status: str
