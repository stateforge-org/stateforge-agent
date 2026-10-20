"""Retain bounded Capability Assessment evidence by its complete scope."""

from __future__ import annotations

from collections import OrderedDict
from uuid import UUID

from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
    CapabilityAssessmentScope,
)


class CapabilityAssessmentCache:
    """Store recent evidence without treating it as execution eligibility."""

    def __init__(self, capacity: int = 128) -> None:
        """Create a bounded least-recently-used evidence cache."""
        if capacity <= 0:
            raise ValueError(
                "Capability Assessment cache capacity must be positive"
            )
        self._capacity = capacity
        self._entries: OrderedDict[
            CapabilityAssessmentScope, CapabilityAssessment
        ] = OrderedDict()

    def get(
        self,
        scope: CapabilityAssessmentScope,
    ) -> CapabilityAssessment | None:
        """Return exact-scope evidence and refresh its retention order."""
        assessment = self._entries.get(scope)
        if assessment is not None:
            self._entries.move_to_end(scope)
        return assessment

    def put(self, assessment: CapabilityAssessment) -> None:
        """Retain one validated snapshot and evict the oldest as needed."""
        self._entries[assessment.scope] = assessment
        self._entries.move_to_end(assessment.scope)
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)

    def discard_provider(self, provider_instance_id: UUID) -> None:
        """Invalidate every snapshot for one Provider installation."""
        stale = tuple(
            scope
            for scope in self._entries
            if scope.provider_instance_id == provider_instance_id
        )
        for scope in stale:
            del self._entries[scope]

    def clear(self) -> None:
        """Invalidate all retained evidence."""
        self._entries.clear()

    def __len__(self) -> int:
        """Return the number of retained evidence snapshots."""
        return len(self._entries)
