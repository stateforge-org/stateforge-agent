"""Expose Agent-normalized operation state without protobuf types."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from google.protobuf.json_format import MessageToDict
from stateforge_protocol.sfp.operation_lifecycle.v1 import (
    operation_lifecycle_pb2,
)

from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAttempt,
)


@dataclass(frozen=True, slots=True)
class OperationStatus:
    """Describe one durable Agent observation of admitted Provider work."""

    provider_instance_id: UUID
    operation_id: UUID
    admission_state: str
    state: str | None
    sequence: int | None
    progress_message: str | None
    progress_percentage: float | None
    result: dict[str, object] | None
    failure_code: str | None
    failure_explanation: str | None
    side_effects: str | None
    cleanup: str | None
    effects_explanation: str | None
    cancellation_disposition: str | None

    @classmethod
    def from_attempt(cls, attempt: OperationAttempt) -> OperationStatus:
        """Normalize one journal record for local coordination callers."""
        snapshot = attempt.lifecycle_snapshot
        progress = (
            snapshot.progress
            if snapshot is not None and snapshot.HasField("progress")
            else None
        )
        failure = (
            snapshot.failure
            if snapshot is not None and snapshot.HasField("failure")
            else None
        )
        effects = None
        if failure is not None:
            effects = failure.effects
        elif snapshot is not None and snapshot.HasField("cancellation"):
            effects = snapshot.cancellation.effects
        result: dict[str, object] | None = None
        if snapshot is not None and snapshot.HasField("success"):
            result = MessageToDict(snapshot.success.result)
        disposition = attempt.cancellation_disposition
        return cls(
            provider_instance_id=attempt.provider_instance_id,
            operation_id=attempt.operation_id,
            admission_state=attempt.state.value,
            state=(
                operation_lifecycle_pb2.OperationState.Name(snapshot.state)
                if snapshot is not None
                else None
            ),
            sequence=snapshot.sequence if snapshot is not None else None,
            progress_message=(
                progress.message if progress is not None else None
            ),
            progress_percentage=(
                progress.percentage
                if progress is not None and progress.HasField("percentage")
                else None
            ),
            result=result,
            failure_code=failure.code if failure is not None else None,
            failure_explanation=(
                failure.explanation if failure is not None else None
            ),
            side_effects=(
                operation_lifecycle_pb2.SideEffectState.Name(
                    effects.side_effects
                )
                if effects is not None
                else None
            ),
            cleanup=(
                operation_lifecycle_pb2.CleanupState.Name(effects.cleanup)
                if effects is not None
                else None
            ),
            effects_explanation=(
                effects.explanation if effects is not None else None
            ),
            cancellation_disposition=(
                operation_lifecycle_pb2.CancellationDisposition.DESCRIPTOR.values_by_number[
                    disposition
                ].name
                if disposition is not None
                else None
            ),
        )
