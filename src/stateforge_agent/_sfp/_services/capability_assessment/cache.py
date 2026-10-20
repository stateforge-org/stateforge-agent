"""Retain bounded Capability Assessment evidence by its complete scope."""

from __future__ import annotations

from collections import OrderedDict
from uuid import UUID

from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
    CapabilityAssessmentScope,
)

_DEFAULT_MAX_BYTES = 32 * 1024 * 1024


class CapabilityAssessmentCache:
    """Store recent evidence without treating it as execution eligibility."""

    def __init__(
        self,
        capacity: int = 128,
        *,
        max_bytes: int = _DEFAULT_MAX_BYTES,
    ) -> None:
        """Create an entry- and byte-bounded evidence cache."""
        if capacity <= 0:
            raise ValueError(
                "Capability Assessment cache capacity must be positive"
            )
        if max_bytes <= 0:
            raise ValueError(
                "Capability Assessment cache byte limit must be positive"
            )
        self._capacity = capacity
        self._max_bytes = max_bytes
        self._retained_bytes = 0
        self._entries: OrderedDict[
            CapabilityAssessmentScope, tuple[CapabilityAssessment, int]
        ] = OrderedDict()

    def get(
        self,
        scope: CapabilityAssessmentScope,
    ) -> CapabilityAssessment | None:
        """Return exact-scope evidence and refresh its retention order."""
        entry = self._entries.get(scope)
        if entry is not None:
            self._entries.move_to_end(scope)
            return entry[0]
        return None

    def put(self, assessment: CapabilityAssessment) -> None:
        """Retain one validated snapshot and evict the oldest as needed."""
        weight = assessment.wire_size_bytes
        if weight > self._max_bytes:
            return
        previous = self._entries.pop(assessment.scope, None)
        if previous is not None:
            self._retained_bytes -= previous[1]
        self._entries[assessment.scope] = (assessment, weight)
        self._retained_bytes += weight
        self._entries.move_to_end(assessment.scope)
        while (
            len(self._entries) > self._capacity
            or self._retained_bytes > self._max_bytes
        ):
            _, (_, evicted_weight) = self._entries.popitem(last=False)
            self._retained_bytes -= evicted_weight

    def discard_provider(self, provider_instance_id: UUID) -> None:
        """Invalidate every snapshot for one Provider installation."""
        stale = tuple(
            scope
            for scope in self._entries
            if scope.provider_instance_id == provider_instance_id
        )
        for scope in stale:
            _, weight = self._entries.pop(scope)
            self._retained_bytes -= weight

    def clear(self) -> None:
        """Invalidate all retained evidence."""
        self._entries.clear()
        self._retained_bytes = 0

    def __len__(self) -> int:
        """Return the number of retained evidence snapshots."""
        return len(self._entries)
