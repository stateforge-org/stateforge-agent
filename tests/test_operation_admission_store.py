"""Durable Agent-owned Operation Admission intent tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from google.protobuf.json_format import ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
from stateforge_protocol.sfp.operation_admission.v1 import (
    operation_admission_pb2,
    validation,
)

from stateforge_agent._journal import SqliteJournal
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionDisposition,
    OperationAdmissionResult,
    OperationRejection,
)
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAdmissionStore,
    OperationAttemptState,
    OperationIntentDecision,
)

_PROVIDER_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_OTHER_PROVIDER_ID = UUID("1d40d0d5-ff2a-4780-bc3d-c4a7c55fc3a2")
_MANIFEST_REVISION = "sha256:" + ("a" * 64)
_ATTEMPTED_ENDPOINT_GENERATION = 4
_RESULT_SCHEMA_JSON = b'{"type":"object"}'


def _invocation(
    operation_id: UUID | None = None,
    *,
    unit: str = "nginx.service",
) -> operation_admission_pb2.OperationInvocation:
    invocation = operation_admission_pb2.OperationInvocation(
        operation_id=str(operation_id or uuid4()),
        operation_identifier="stateforge.test/restart@1",
        execution_deadline=Timestamp(
            seconds=int((datetime.now(UTC) + timedelta(minutes=5)).timestamp())
        ),
        contract=operation_admission_pb2.OperationContractContext(
            manifest_revision=_MANIFEST_REVISION,
        ),
    )
    ParseDict({"unit": unit}, invocation.payload)
    ParseDict({"requester": "test"}, invocation.execution_context)
    return invocation


async def _store(
    path: Path,
    *,
    capacity: int = 10,
) -> tuple[SqliteJournal, OperationAdmissionStore]:
    journal = SqliteJournal(path)
    await journal.open()
    store = journal.operation_admissions(
        maximum_unresolved_attempts=capacity,
    )
    await store.initialize()
    return journal, store


@pytest.mark.asyncio
async def test_intent_is_immutable_and_survives_journal_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.db"
    operation_id = uuid4()
    invocation = _invocation(operation_id)
    fingerprint = validation.compute_request_fingerprint(invocation)
    journal, store = await _store(path)

    created = await store.create_intent(
        _PROVIDER_ID,
        invocation,
        fingerprint,
        result_schema_json=_RESULT_SCHEMA_JSON,
    )
    duplicate = await store.create_intent(
        _PROVIDER_ID,
        invocation,
        fingerprint,
        result_schema_json=_RESULT_SCHEMA_JSON,
    )
    conflicting = _invocation(operation_id, unit="sshd.service")
    conflict = await store.create_intent(
        _PROVIDER_ID,
        conflicting,
        validation.compute_request_fingerprint(conflicting),
        result_schema_json=_RESULT_SCHEMA_JSON,
    )
    schema_conflict = await store.create_intent(
        _PROVIDER_ID,
        invocation,
        fingerprint,
        result_schema_json=b'{"type":"string"}',
    )
    await journal.close()

    reopened, reopened_store = await _store(path)
    try:
        persisted = await reopened_store.get(_PROVIDER_ID, operation_id)
    finally:
        await reopened.close()

    assert created.decision is OperationIntentDecision.CREATED
    assert duplicate.decision is OperationIntentDecision.EXISTING
    assert conflict.decision is OperationIntentDecision.CONFLICT
    assert schema_conflict.decision is OperationIntentDecision.CONFLICT
    assert persisted is not None
    assert persisted.request_fingerprint == fingerprint
    assert persisted.invocation == invocation


@pytest.mark.asyncio
async def test_unresolved_capacity_and_provider_identity_are_isolated(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db", capacity=2)
    first = _invocation()
    second = _invocation()
    try:
        accepted = await store.create_intent(
            _PROVIDER_ID,
            first,
            validation.compute_request_fingerprint(first),
            result_schema_json=_RESULT_SCHEMA_JSON,
        )
        same_identity_other_provider = await store.create_intent(
            _OTHER_PROVIDER_ID,
            first,
            validation.compute_request_fingerprint(first),
            result_schema_json=_RESULT_SCHEMA_JSON,
        )
        exhausted = await store.create_intent(
            _PROVIDER_ID,
            second,
            validation.compute_request_fingerprint(second),
            result_schema_json=_RESULT_SCHEMA_JSON,
        )
    finally:
        await journal.close()

    assert accepted.decision is OperationIntentDecision.CREATED
    assert (
        same_identity_other_provider.decision
        is OperationIntentDecision.CREATED
    )
    assert exhausted.decision is OperationIntentDecision.CAPACITY_EXHAUSTED


@pytest.mark.asyncio
async def test_concurrent_intent_creation_cannot_exceed_capacity(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db", capacity=1)
    first = _invocation()
    second = _invocation()
    try:
        results = await asyncio.gather(
            store.create_intent(
                _PROVIDER_ID,
                first,
                validation.compute_request_fingerprint(first),
                result_schema_json=_RESULT_SCHEMA_JSON,
            ),
            store.create_intent(
                _PROVIDER_ID,
                second,
                validation.compute_request_fingerprint(second),
                result_schema_json=_RESULT_SCHEMA_JSON,
            ),
        )
        unresolved = await store.unresolved()
    finally:
        await journal.close()

    assert {result.decision for result in results} == {
        OperationIntentDecision.CREATED,
        OperationIntentDecision.CAPACITY_EXHAUSTED,
    }
    assert len(unresolved) == 1


@pytest.mark.asyncio
async def test_transport_success_and_rejection_are_durable(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db")
    accepted_invocation = _invocation()
    rejected_invocation = _invocation()
    accepted_fingerprint = validation.compute_request_fingerprint(
        accepted_invocation
    )
    rejected_fingerprint = validation.compute_request_fingerprint(
        rejected_invocation
    )
    try:
        await store.create_intent(
            _PROVIDER_ID,
            accepted_invocation,
            accepted_fingerprint,
            result_schema_json=_RESULT_SCHEMA_JSON,
        )
        await store.create_intent(
            _PROVIDER_ID,
            rejected_invocation,
            rejected_fingerprint,
            result_schema_json=_RESULT_SCHEMA_JSON,
        )
        attempted = await store.record_transport_attempt(
            _PROVIDER_ID,
            UUID(accepted_invocation.operation_id),
            endpoint_generation=_ATTEMPTED_ENDPOINT_GENERATION,
            session_id="session-a",
        )
        now = datetime.now(UTC)
        accepted = await store.record_accepted(
            OperationAdmissionResult(
                provider_instance_id=_PROVIDER_ID,
                endpoint_generation=_ATTEMPTED_ENDPOINT_GENERATION,
                session_id="session-a",
                manifest_revision=_MANIFEST_REVISION,
                operation_id=UUID(accepted_invocation.operation_id),
                request_fingerprint=accepted_fingerprint,
                disposition=OperationAdmissionDisposition.ACCEPTED_NEW,
                admitted_at=now,
                record_expires_at=now + timedelta(hours=1),
                admitted_endpoint_generation=_ATTEMPTED_ENDPOINT_GENERATION,
            )
        )
        rejected = await store.record_rejected(
            _PROVIDER_ID,
            UUID(rejected_invocation.operation_id),
            OperationRejection(
                operation_id=UUID(rejected_invocation.operation_id),
                reason="ADMISSION_ERROR_REASON_PROVIDER_UNAVAILABLE",
                retryable=True,
                retry_after=timedelta(seconds=2),
                explanation="Provider is draining.",
                grpc_status="UNAVAILABLE",
            ),
        )
        unresolved = await store.unresolved()
    finally:
        await journal.close()

    assert attempted.attempt_count == 1
    assert attempted.last_endpoint_generation == _ATTEMPTED_ENDPOINT_GENERATION
    assert accepted.state is OperationAttemptState.ACCEPTED
    assert accepted.result is not None
    assert accepted.result.request_fingerprint == accepted_fingerprint
    assert rejected.state is OperationAttemptState.REJECTED
    assert rejected.rejection is not None
    assert rejected.rejection.retry_after == timedelta(seconds=2)
    assert unresolved == ()
