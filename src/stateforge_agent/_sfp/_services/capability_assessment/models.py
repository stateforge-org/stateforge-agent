"""Define immutable Agent-normalized Capability Assessment evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class CapabilitySupportState(StrEnum):
    """Preserve every normalized SFP support outcome without policy."""

    SUPPORTED = "supported"
    SUPPORTED_WITH_REQUIREMENTS = "supported_with_requirements"
    UNSUPPORTED = "unsupported"
    INDETERMINATE = "indeterminate"


class CapabilityRequirementState(StrEnum):
    """Preserve a Provider's evidence for one declared requirement."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CapabilityRequirement:
    """One normalized runtime-requirement finding."""

    identifier: str
    state: CapabilityRequirementState
    description: str
    version_constraint: str


@dataclass(frozen=True, slots=True)
class CapabilityReason:
    """One sanitized machine-readable reason and explanation."""

    code: str
    explanation: str


@dataclass(frozen=True, slots=True)
class CapabilityConstraint:
    """One structured limitation attached to an operation finding."""

    identifier: str
    value: str
    explanation: str


@dataclass(frozen=True, slots=True)
class CapabilityFinding:
    """Point-in-time support evidence for one declared operation."""

    operation_identifier: str
    support_state: CapabilitySupportState
    requirements: tuple[CapabilityRequirement, ...]
    reasons: tuple[CapabilityReason, ...]
    constraints: tuple[CapabilityConstraint, ...]


@dataclass(frozen=True, slots=True)
class CapabilityAssessmentScope:
    """Fence reusable evidence to its exact request and SFP session."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    manifest_revision: str
    availability_generation: int
    operation_identifiers: tuple[str, ...]
    execution_context_sha256: bytes


@dataclass(frozen=True, slots=True)
class CapabilityAssessment:
    """One validated assessment snapshot, never an authorization decision."""

    scope: CapabilityAssessmentScope
    request_id: UUID
    assessment_id: UUID
    assessed_at: datetime
    findings: tuple[CapabilityFinding, ...]
    wire_size_bytes: int

    def __post_init__(self) -> None:
        """Reject invalid retention weights from alternate requesters."""
        if (
            isinstance(self.wire_size_bytes, bool)
            or not isinstance(self.wire_size_bytes, int)
            or self.wire_size_bytes <= 0
        ):
            raise ValueError("wire_size_bytes must be a positive integer")
