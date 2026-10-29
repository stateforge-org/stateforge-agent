"""Define storage-neutral durable Trigger Publication contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from stateforge_protocol.sfp.trigger_publication.v1 import (
    trigger_publication_pb2,
)


class TriggerAcceptanceDecision(StrEnum):
    """Describe one durable idempotency decision."""

    ACCEPTED_NEW = "accepted_new"
    ACCEPTED_DUPLICATE = "accepted_duplicate"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    CAPACITY_EXHAUSTED = "capacity_exhausted"


@dataclass(frozen=True, slots=True)
class TriggerAcceptanceResult:
    """Return a durable decision without exposing publication content."""

    decision: TriggerAcceptanceDecision
    content_fingerprint: str
    pending_publications: int


@dataclass(frozen=True, slots=True)
class TriggerAcceptanceContext:
    """Identify the current session that delivered one publication."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    manifest_revision: str


class TriggerPublicationStore(ABC):
    """Persist trigger responsibility without exposing a storage vendor."""

    @abstractmethod
    async def initialize(self) -> None:
        """Prepare durable Trigger Publication state."""

    @abstractmethod
    async def accept(
        self,
        context: TriggerAcceptanceContext,
        publication: trigger_publication_pb2.TriggerPublication,
    ) -> TriggerAcceptanceResult:
        """Atomically assume responsibility or classify existing content."""

    @abstractmethod
    async def pending_count(self) -> int:
        """Return the number of events awaiting the future SF-AOP boundary."""
