"""Agent-side Operation Admission preflight and recovery tests."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import grpc
import pytest
from google.protobuf import any_pb2, duration_pb2
from google.protobuf.json_format import ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
from google.rpc import status_pb2
from grpc_status import rpc_status
from stateforge_protocol.sfp.operation_admission.v1 import (
    contract,
    operation_admission_pb2,
    validation,
)

from stateforge_agent._journal import JournalError, SqliteJournal
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
    CapabilityAssessmentScope,
    CapabilityFinding,
    CapabilityRequirement,
    CapabilityRequirementState,
    CapabilitySupportState,
)
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestOperationDeclaration,
)
from stateforge_agent._sfp._services.operation_admission.client import (
    OperationAdmissionClient,
    OperationAdmissionRequester,
    SfpOperationAdmissionError,
    SfpOperationAdmissionLimitError,
    SfpOperationAdmissionProtocolError,
    SfpOperationAdmissionRejectedError,
    SfpOperationAdmissionStaleError,
    SfpOperationAdmissionUnavailableError,
    _decode_response,
    validate_request,
)
from stateforge_agent._sfp._services.operation_admission.coordinator import (
    OperationAdmissionCapacityError,
    OperationAdmissionCoordinator,
    OperationAdmissionLease,
    OperationAdmissionPreconditionError,
    OperationAdmissionUnresolvedError,
)
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionDisposition,
    OperationAdmissionRequest,
    OperationAdmissionResult,
    OperationRejection,
)
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAdmissionStore,
    OperationAttemptState,
)

_PROVIDER_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_MANIFEST_REVISION = "sha256:" + ("a" * 64)
_OPERATION = "stateforge.test/restart@1"
_LOST_RESPONSE_CALL_COUNT = 2
_OVERSIZED_RESPONSE_LENGTH = contract.MAX_RESPONSE_BYTES + 1
_INPUT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"unit": {"type": "string"}},
    "required": ["unit"],
    "additionalProperties": False,
}
_RESULT_SCHEMA = {"type": "object"}


class _ScriptedClient(OperationAdmissionRequester):
    """Return deterministic results while retaining exact wire requests."""

    def __init__(
        self,
        steps: list[
            OperationAdmissionDisposition | SfpOperationAdmissionError
        ],
    ) -> None:
        self.steps = steps
        self.requests: list[operation_admission_pb2.StartOperationRequest] = []

    async def start(
        self,
        channel: grpc.aio.Channel,
        request: operation_admission_pb2.StartOperationRequest,
    ) -> OperationAdmissionResult:
        del channel
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        now = datetime.now(UTC)
        return OperationAdmissionResult(
            provider_instance_id=UUID(
                request.session_binding.expected_provider_instance_id
            ),
            endpoint_generation=(
                request.session_binding.expected_endpoint_generation
            ),
            session_id=request.session_binding.expected_session_id,
            manifest_revision=request.invocation.contract.manifest_revision,
            operation_id=UUID(request.invocation.operation_id),
            request_fingerprint=validation.compute_request_fingerprint(
                request.invocation
            ),
            disposition=step,
            admitted_at=now,
            record_expires_at=now + timedelta(hours=1),
            admitted_endpoint_generation=(
                request.session_binding.expected_endpoint_generation
            ),
        )


def _lease(*, generation: int = 1) -> OperationAdmissionLease:
    return OperationAdmissionLease(
        provider_instance_id=_PROVIDER_ID,
        endpoint_generation=generation,
        session_id=f"session-{generation}",
        manifest_revision=_MANIFEST_REVISION,
        availability_generation=generation,
        operation_declarations={
            _OPERATION: ManifestOperationDeclaration(
                input_schema=_INPUT_SCHEMA,
                result_schema=_RESULT_SCHEMA,
            )
        },
        channel=cast(grpc.aio.Channel, object()),
        session_token=object(),
    )


def _request(
    *,
    unit: str = "nginx.service",
) -> OperationAdmissionRequest:
    return OperationAdmissionRequest(
        provider_instance_id=_PROVIDER_ID,
        operation_identifier=_OPERATION,
        payload={"unit": unit},
        execution_deadline=datetime.now(UTC) + timedelta(minutes=5),
        execution_context={"requester": "test"},
    )


def test_removed_request_roles_are_rejected() -> None:
    with pytest.raises(TypeError, match="required_implementation_roles"):
        replace(_request(), required_implementation_roles=("obsolete",))


def test_removed_declaration_claims_are_rejected() -> None:
    with pytest.raises(TypeError, match="implements"):
        replace(_lease().operation_declarations[_OPERATION], implements=())


def _assessment(lease: OperationAdmissionLease) -> CapabilityAssessment:
    context = operation_admission_pb2.OperationInvocation()
    ParseDict({"requester": "test"}, context.execution_context)
    return CapabilityAssessment(
        scope=CapabilityAssessmentScope(
            provider_instance_id=lease.provider_instance_id,
            endpoint_generation=lease.endpoint_generation,
            session_id=lease.session_id,
            manifest_revision=lease.manifest_revision,
            availability_generation=lease.availability_generation,
            operation_identifiers=(_OPERATION,),
            execution_context_sha256=hashlib.sha256(
                context.execution_context.SerializeToString(deterministic=True)
            ).digest(),
        ),
        request_id=uuid4(),
        assessment_id=uuid4(),
        assessed_at=datetime.now(UTC),
        findings=(
            CapabilityFinding(
                operation_identifier=_OPERATION,
                support_state=CapabilitySupportState.SUPPORTED,
                requirements=(),
                reasons=(),
                constraints=(),
            ),
        ),
        wire_size_bytes=1,
    )


async def _current(lease: OperationAdmissionLease) -> None:
    del lease


def _wire_request() -> operation_admission_pb2.StartOperationRequest:
    invocation = operation_admission_pb2.OperationInvocation(
        operation_id=str(uuid4()),
        operation_identifier=_OPERATION,
        execution_deadline=Timestamp(
            seconds=int((datetime.now(UTC) + timedelta(minutes=5)).timestamp())
        ),
        contract=operation_admission_pb2.OperationContractContext(
            manifest_revision=_MANIFEST_REVISION,
        ),
    )
    ParseDict({"unit": "nginx.service"}, invocation.payload)
    ParseDict({"requester": "test"}, invocation.execution_context)
    return operation_admission_pb2.StartOperationRequest(
        session_binding=operation_admission_pb2.OperationSessionBinding(
            expected_provider_instance_id=str(_PROVIDER_ID),
            expected_endpoint_generation=1,
            expected_session_id="session-1",
        ),
        invocation=invocation,
        admission_mode=(
            operation_admission_pb2.ADMISSION_MODE_ADMIT_OR_RESOLVE
        ),
    )


def _wire_response(
    request: operation_admission_pb2.StartOperationRequest,
) -> operation_admission_pb2.StartOperationResponse:
    now = Timestamp(seconds=int(datetime.now(UTC).timestamp()))
    expires = Timestamp(seconds=now.seconds + 3600)
    binding = request.session_binding
    return operation_admission_pb2.StartOperationResponse(
        provider_instance_id=binding.expected_provider_instance_id,
        endpoint_generation=binding.expected_endpoint_generation,
        session_id=binding.expected_session_id,
        manifest_revision=request.invocation.contract.manifest_revision,
        disposition=(
            operation_admission_pb2.ADMISSION_DISPOSITION_ACCEPTED_NEW
        ),
        record=operation_admission_pb2.OperationAdmissionRecord(
            operation_id=request.invocation.operation_id,
            request_fingerprint=validation.compute_request_fingerprint(
                request.invocation
            ),
            admitted_at=now,
            record_expires_at=expires,
            admitted_endpoint_generation=binding.expected_endpoint_generation,
        ),
    )


async def _start_admission_server(
    handler: Callable[
        [
            operation_admission_pb2.StartOperationRequest,
            grpc.aio.ServicerContext,
        ],
        Awaitable[operation_admission_pb2.StartOperationResponse | bytes],
    ],
) -> tuple[grpc.aio.Server, grpc.aio.Channel]:
    method = grpc.unary_unary_rpc_method_handler(
        handler,
        request_deserializer=(
            operation_admission_pb2.StartOperationRequest.FromString
        ),
        response_serializer=lambda value: (
            value if isinstance(value, bytes) else value.SerializeToString()
        ),
    )
    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "stateforge.sfp.operation_admission.v1."
                "OperationAdmissionService",
                {"StartOperation": method},
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    await channel.channel_ready()
    return server, channel


async def _coordinator(
    path: Path,
    client: OperationAdmissionRequester,
    *,
    attempts: int = 2,
) -> tuple[
    SqliteJournal,
    OperationAdmissionCoordinator,
    OperationAdmissionStore,
]:
    journal = SqliteJournal(path)
    await journal.open()
    store = journal.operation_admissions(maximum_unresolved_attempts=10)
    await store.initialize()
    return (
        journal,
        OperationAdmissionCoordinator(
            store,
            attempts=attempts,
            retry_initial_delay_seconds=0,
            client=client,
        ),
        store,
    )


@pytest.mark.asyncio
async def test_admits_once_and_reuses_one_identity_after_lost_response(
    tmp_path: Path,
) -> None:
    client = _ScriptedClient(
        [
            SfpOperationAdmissionUnavailableError("response lost"),
            OperationAdmissionDisposition.RESOLVED_EXISTING,
        ]
    )
    journal, coordinator, _ = await _coordinator(tmp_path / "agent.db", client)
    lease = _lease()
    try:
        result = await coordinator.admit(
            lease,
            _request(),
            _assessment(lease),
            assert_current=_current,
        )
    finally:
        await journal.close()

    assert result.disposition is (
        OperationAdmissionDisposition.RESOLVED_EXISTING
    )
    assert len(client.requests) == _LOST_RESPONSE_CALL_COUNT
    assert (
        client.requests[0].invocation.operation_id
        == client.requests[1].invocation.operation_id
    )
    assert validation.compute_request_fingerprint(
        client.requests[0].invocation
    ) == validation.compute_request_fingerprint(client.requests[1].invocation)


@pytest.mark.asyncio
async def test_cross_generation_uncertainty_uses_resolve_only(
    tmp_path: Path,
) -> None:
    client = _ScriptedClient([OperationAdmissionDisposition.RESOLVED_EXISTING])
    journal, coordinator, store = await _coordinator(
        tmp_path / "agent.db", client
    )
    first_lease = _lease(generation=1)
    replacement_lease = _lease(generation=2)
    operation_id = await coordinator.prepare(
        first_lease,
        _request(),
        _assessment(first_lease),
        assert_current=_current,
    )
    attempt = await coordinator.attempt(_PROVIDER_ID, operation_id)
    assert attempt is not None
    await store.record_transport_attempt(
        _PROVIDER_ID,
        operation_id,
        endpoint_generation=1,
        session_id="session-1",
    )
    try:
        result = await coordinator.submit(
            replacement_lease,
            operation_id,
            _assessment(replacement_lease),
            assert_current=_current,
        )
    finally:
        await journal.close()

    assert result.operation_id == operation_id
    assert client.requests[0].admission_mode == (
        operation_admission_pb2.ADMISSION_MODE_RESOLVE_ONLY
    )


@pytest.mark.asyncio
async def test_concurrent_submissions_converge_on_one_transport_attempt(
    tmp_path: Path,
) -> None:
    client = _ScriptedClient([OperationAdmissionDisposition.ACCEPTED_NEW])
    journal, coordinator, _ = await _coordinator(tmp_path / "agent.db", client)
    lease = _lease()
    assessment = _assessment(lease)
    operation_id = await coordinator.prepare(
        lease,
        _request(),
        assessment,
        assert_current=_current,
    )
    try:
        first, second = await asyncio.gather(
            coordinator.submit(
                lease,
                operation_id,
                assessment,
                assert_current=_current,
            ),
            coordinator.submit(
                lease,
                operation_id,
                assessment,
                assert_current=_current,
            ),
        )
    finally:
        await journal.close()

    assert first == second
    assert first.operation_id == operation_id
    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_storage_failure_is_sanitized_before_provider_contact() -> None:
    sensitive_value = "sensitive-operation-value"

    class _FailingStore:
        async def create_intent(self, *args, **kwargs):
            del args, kwargs
            raise JournalError(
                f"database failed while writing {sensitive_value}"
            )

    client = _ScriptedClient([])
    coordinator = OperationAdmissionCoordinator(
        cast(OperationAdmissionStore, _FailingStore()),
        client=client,
    )
    lease = _lease()
    request = replace(_request(), payload={"unit": sensitive_value})

    with pytest.raises(OperationAdmissionCapacityError) as captured:
        await coordinator.admit(
            lease,
            request,
            _assessment(lease),
            assert_current=_current,
        )

    assert sensitive_value not in str(captured.value)
    assert sensitive_value not in repr(captured.value)
    assert client.requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "admission_request",
    [
        _request(unit=""),
        _request(unit=7),
    ],
)
async def test_preflight_rejects_invalid_payload(
    tmp_path: Path,
    admission_request: OperationAdmissionRequest,
) -> None:
    client = _ScriptedClient([])
    journal, coordinator, _ = await _coordinator(tmp_path / "agent.db", client)
    lease = _lease()
    if admission_request.payload["unit"] == "":
        lease = replace(
            lease,
            operation_declarations={
                _OPERATION: ManifestOperationDeclaration(
                    input_schema={
                        **_INPUT_SCHEMA,
                        "properties": {
                            "unit": {"type": "string", "minLength": 1}
                        },
                    },
                    result_schema=_RESULT_SCHEMA,
                )
            },
        )
    try:
        with pytest.raises(OperationAdmissionPreconditionError):
            await coordinator.admit(
                lease,
                admission_request,
                _assessment(lease),
                assert_current=_current,
            )
    finally:
        await journal.close()

    assert client.requests == []


@pytest.mark.asyncio
async def test_cancellation_leaves_durable_unresolved_identity(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class _BlockingClient(OperationAdmissionRequester):
        async def start(
            self,
            channel: grpc.aio.Channel,
            request: operation_admission_pb2.StartOperationRequest,
        ) -> OperationAdmissionResult:
            del channel, request
            started.set()
            await release.wait()
            raise AssertionError("cancelled call resumed")

    journal, coordinator, store = await _coordinator(
        tmp_path / "agent.db", _BlockingClient()
    )
    lease = _lease()
    task = asyncio.create_task(
        coordinator.admit(
            lease,
            _request(),
            _assessment(lease),
            assert_current=_current,
        )
    )
    await started.wait()
    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
        unresolved = await store.unresolved()
    finally:
        release.set()
        await journal.close()

    assert len(unresolved) == 1
    assert unresolved[0].attempt_count == 1


@pytest.mark.asyncio
async def test_transport_exhaustion_reports_same_unresolved_identity(
    tmp_path: Path,
) -> None:
    client = _ScriptedClient(
        [SfpOperationAdmissionUnavailableError("unavailable")]
    )
    journal, coordinator, _ = await _coordinator(
        tmp_path / "agent.db", client, attempts=1
    )
    lease = _lease()
    try:
        with pytest.raises(OperationAdmissionUnresolvedError) as captured:
            await coordinator.admit(
                lease,
                _request(),
                _assessment(lease),
                assert_current=_current,
            )
        persisted = await coordinator.attempt(
            _PROVIDER_ID,
            captured.value.operation_id,
        )
    finally:
        await journal.close()

    assert persisted is not None
    assert persisted.operation_id == captured.value.operation_id
    assert persisted.attempt_count == 1


@pytest.mark.asyncio
async def test_retryable_rejection_reuses_identity_then_succeeds(
    tmp_path: Path,
) -> None:
    rejection = OperationRejection(
        operation_id=None,
        reason="ADMISSION_ERROR_REASON_PROVIDER_CAPACITY_EXHAUSTED",
        retryable=True,
        retry_after=None,
        explanation="Provider capacity is temporarily exhausted.",
        grpc_status=grpc.StatusCode.RESOURCE_EXHAUSTED.name,
    )
    client = _ScriptedClient(
        [
            SfpOperationAdmissionRejectedError(rejection),
            OperationAdmissionDisposition.ACCEPTED_NEW,
        ]
    )
    journal, coordinator, _ = await _coordinator(tmp_path / "agent.db", client)
    lease = _lease()
    try:
        result = await coordinator.admit(
            lease,
            _request(),
            _assessment(lease),
            assert_current=_current,
        )
    finally:
        await journal.close()

    assert result.disposition is OperationAdmissionDisposition.ACCEPTED_NEW
    assert len(client.requests) == _LOST_RESPONSE_CALL_COUNT
    assert (
        client.requests[0].invocation.operation_id
        == client.requests[1].invocation.operation_id
    )


@pytest.mark.asyncio
async def test_retry_after_beyond_execution_deadline_stops_retry(
    tmp_path: Path,
) -> None:
    rejection = OperationRejection(
        operation_id=None,
        reason="ADMISSION_ERROR_REASON_PROVIDER_CAPACITY_EXHAUSTED",
        retryable=True,
        retry_after=timedelta(minutes=1),
        explanation="Provider capacity is temporarily exhausted.",
        grpc_status=grpc.StatusCode.RESOURCE_EXHAUSTED.name,
    )
    client = _ScriptedClient([SfpOperationAdmissionRejectedError(rejection)])
    journal, coordinator, _ = await _coordinator(tmp_path / "agent.db", client)
    lease = _lease()
    request = replace(
        _request(),
        execution_deadline=datetime.now(UTC) + timedelta(seconds=1),
    )
    try:
        with pytest.raises(SfpOperationAdmissionRejectedError):
            await coordinator.admit(
                lease,
                request,
                _assessment(lease),
                assert_current=_current,
            )
    finally:
        await journal.close()

    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_non_retryable_rejection_is_durable_and_not_retried(
    tmp_path: Path,
) -> None:
    rejection = OperationRejection(
        operation_id=None,
        reason="ADMISSION_ERROR_REASON_PROVIDER_UNAVAILABLE",
        retryable=False,
        retry_after=None,
        explanation="Provider is draining.",
        grpc_status=grpc.StatusCode.FAILED_PRECONDITION.name,
    )
    client = _ScriptedClient([SfpOperationAdmissionRejectedError(rejection)])
    journal, coordinator, store = await _coordinator(
        tmp_path / "agent.db", client
    )
    lease = _lease()
    try:
        with pytest.raises(SfpOperationAdmissionRejectedError) as captured:
            await coordinator.admit(
                lease,
                _request(),
                _assessment(lease),
                assert_current=_current,
            )
        assert captured.value.rejection.operation_id is None
        operation_id = UUID(client.requests[0].invocation.operation_id)
        attempts = await store.unresolved()
        assert attempts == ()
        rejected = await store.get(_PROVIDER_ID, operation_id)
        with pytest.raises(SfpOperationAdmissionRejectedError):
            await coordinator.submit(
                lease,
                operation_id,
                _assessment(lease),
                assert_current=_current,
            )
    finally:
        await journal.close()

    assert rejected is not None
    assert rejected.state is OperationAttemptState.REJECTED
    assert rejected.rejection is not None
    assert rejected.rejection.reason == rejection.reason
    assert not rejected.rejection.retryable
    assert len(client.requests) == 1


@pytest.mark.asyncio
async def test_expired_execution_deadline_never_contacts_provider(
    tmp_path: Path,
) -> None:
    client = _ScriptedClient([])
    journal, coordinator, store = await _coordinator(
        tmp_path / "agent.db", client
    )
    lease = _lease()
    request = replace(
        _request(),
        execution_deadline=datetime.now(UTC) - timedelta(seconds=1),
    )
    try:
        with pytest.raises(SfpOperationAdmissionRejectedError):
            await coordinator.admit(
                lease,
                request,
                _assessment(lease),
                assert_current=_current,
            )
        unresolved = await store.unresolved()
    finally:
        await journal.close()

    assert unresolved == ()
    assert client.requests == []


@pytest.mark.asyncio
async def test_expired_deadline_after_transport_remains_indeterminate(
    tmp_path: Path,
) -> None:
    client = _ScriptedClient([])
    journal, coordinator, store = await _coordinator(
        tmp_path / "agent.db", client
    )
    lease = _lease()
    invocation = _wire_request().invocation
    invocation.execution_deadline.FromDatetime(
        datetime.now(UTC) - timedelta(seconds=1)
    )
    operation_id = UUID(invocation.operation_id)
    fingerprint = validation.compute_request_fingerprint(invocation)
    try:
        await store.create_intent(
            _PROVIDER_ID,
            invocation,
            fingerprint,
            result_schema_json=b'{"type":"object"}',
        )
        await store.record_transport_attempt(
            _PROVIDER_ID,
            operation_id,
            endpoint_generation=lease.endpoint_generation,
            session_id=lease.session_id,
        )

        with pytest.raises(OperationAdmissionUnresolvedError) as captured:
            await coordinator.submit(
                lease,
                operation_id,
                _assessment(lease),
                assert_current=_current,
            )
        persisted = await store.get(_PROVIDER_ID, operation_id)
    finally:
        await journal.close()

    assert captured.value.reason == (
        "OPERATION_DEADLINE_EXPIRED_WITHOUT_REMOTE_OUTCOME"
    )
    assert persisted is not None
    assert persisted.state is OperationAttemptState.UNRESOLVED
    assert persisted.rejection is None
    assert persisted.attempt_count == 1
    assert client.requests == []


@pytest.mark.asyncio
async def test_post_accept_storage_failure_leaves_identity_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ScriptedClient([OperationAdmissionDisposition.ACCEPTED_NEW])
    journal, coordinator, store = await _coordinator(
        tmp_path / "agent.db", client
    )

    async def fail_to_record(result: OperationAdmissionResult):
        del result
        raise JournalError("accepted result could not be stored")

    monkeypatch.setattr(store, "record_accepted", fail_to_record)
    lease = _lease()
    try:
        with pytest.raises(OperationAdmissionUnresolvedError) as captured:
            await coordinator.admit(
                lease,
                _request(),
                _assessment(lease),
                assert_current=_current,
            )
        attempt = await store.get(_PROVIDER_ID, captured.value.operation_id)
    finally:
        await journal.close()

    assert attempt is not None
    assert attempt.state is OperationAttemptState.UNRESOLVED
    assert attempt.result is None
    assert len(client.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stale_after_call",
    [2, 4],
    ids=["after-intent-before-transport", "after-response-before-commit"],
)
async def test_currentness_changes_cannot_become_authoritative(
    tmp_path: Path,
    stale_after_call: int,
) -> None:
    client = _ScriptedClient([OperationAdmissionDisposition.ACCEPTED_NEW])
    journal, coordinator, store = await _coordinator(
        tmp_path / f"agent-{stale_after_call}.db", client
    )
    calls = 0

    async def current(lease: OperationAdmissionLease) -> None:
        nonlocal calls
        del lease
        calls += 1
        if calls == stale_after_call:
            raise SfpOperationAdmissionStaleError("session changed")

    lease = _lease()
    try:
        with pytest.raises(SfpOperationAdmissionStaleError):
            await coordinator.admit(
                lease,
                _request(),
                _assessment(lease),
                assert_current=current,
            )
        unresolved = await store.unresolved()
    finally:
        await journal.close()

    assert len(unresolved) == 1
    assert unresolved[0].result is None
    assert len(client.requests) == (
        0 if stale_after_call == _LOST_RESPONSE_CALL_COUNT else 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evidence_case",
    [
        "unsupported",
        "indeterminate",
        "unsatisfied-requirement",
        "stale-scope",
        "wrong-operation",
        "wrong-context",
    ],
)
async def test_assessment_must_exactly_support_admission_scope(
    tmp_path: Path,
    evidence_case: str,
) -> None:
    client = _ScriptedClient([])
    journal, coordinator, store = await _coordinator(
        tmp_path / f"agent-{evidence_case}.db", client
    )
    lease = _lease()
    assessment = _assessment(lease)
    finding = assessment.findings[0]
    if evidence_case == "unsupported":
        finding = replace(
            finding,
            support_state=CapabilitySupportState.UNSUPPORTED,
        )
    elif evidence_case == "indeterminate":
        finding = replace(
            finding,
            support_state=CapabilitySupportState.INDETERMINATE,
        )
    elif evidence_case == "unsatisfied-requirement":
        finding = replace(
            finding,
            support_state=CapabilitySupportState.SUPPORTED_WITH_REQUIREMENTS,
            requirements=(
                CapabilityRequirement(
                    identifier="runtime.target.readable",
                    state=CapabilityRequirementState.UNSATISFIED,
                    description="Target is not readable.",
                    version_constraint="",
                ),
            ),
        )
    elif evidence_case == "wrong-operation":
        finding = replace(
            finding,
            operation_identifier="stateforge.test/other@1",
        )
    scope = assessment.scope
    if evidence_case == "stale-scope":
        scope = replace(
            scope, endpoint_generation=scope.endpoint_generation + 1
        )
    elif evidence_case == "wrong-context":
        scope = replace(scope, execution_context_sha256=b"\x00" * 32)
    assessment = replace(assessment, scope=scope, findings=(finding,))
    try:
        with pytest.raises(OperationAdmissionPreconditionError):
            await coordinator.admit(
                lease,
                _request(),
                assessment,
                assert_current=_current,
            )
        unresolved = await store.unresolved()
    finally:
        await journal.close()

    assert unresolved == ()
    assert client.requests == []


@pytest.mark.asyncio
async def test_wire_client_normalizes_a_canonical_success() -> None:
    async def handler(request, context):
        del context
        return _wire_response(request)

    request = _wire_request()
    server, channel = await _start_admission_server(handler)
    try:
        result = await OperationAdmissionClient().start(channel, request)
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert result.operation_id == UUID(request.invocation.operation_id)
    assert result.disposition is OperationAdmissionDisposition.ACCEPTED_NEW
    assert result.request_fingerprint == (
        validation.compute_request_fingerprint(request.invocation)
    )


@pytest.mark.asyncio
async def test_wire_client_validates_structured_rejection() -> None:
    request = _wire_request()

    async def handler(request_message, context):
        detail = operation_admission_pb2.OperationAdmissionErrorDetail(
            operation_id=request_message.invocation.operation_id,
            reason=(
                operation_admission_pb2.ADMISSION_ERROR_REASON_PROVIDER_CAPACITY_EXHAUSTED
            ),
            retryable=True,
            retry_after=duration_pb2.Duration(seconds=1),
            explanation="Provider capacity is temporarily exhausted.",
        )
        packed = any_pb2.Any()
        packed.Pack(detail)
        await context.abort_with_status(
            rpc_status.to_status(
                status_pb2.Status(
                    code=grpc.StatusCode.RESOURCE_EXHAUSTED.value[0],
                    message="Operation Admission rejected",
                    details=[packed],
                )
            )
        )
        raise AssertionError("abort returned")

    server, channel = await _start_admission_server(handler)
    try:
        with pytest.raises(SfpOperationAdmissionRejectedError) as captured:
            await OperationAdmissionClient().start(channel, request)
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert captured.value.rejection.operation_id == UUID(
        request.invocation.operation_id
    )
    assert captured.value.rejection.retryable
    assert captured.value.rejection.retry_after == timedelta(seconds=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("grpc_status", "retryable"),
    [
        (grpc.StatusCode.FAILED_PRECONDITION, True),
        (grpc.StatusCode.UNAVAILABLE, False),
    ],
    ids=["status-does-not-match-reason", "retryable-does-not-match-reason"],
)
async def test_wire_client_rejects_inconsistent_rejection_semantics(
    grpc_status: grpc.StatusCode,
    retryable: bool,
) -> None:
    request = _wire_request()

    async def handler(request_message, context):
        detail = operation_admission_pb2.OperationAdmissionErrorDetail(
            operation_id=request_message.invocation.operation_id,
            reason=(
                operation_admission_pb2.ADMISSION_ERROR_REASON_PROVIDER_UNAVAILABLE
            ),
            retryable=retryable,
            explanation="Provider is temporarily unavailable.",
        )
        packed = any_pb2.Any()
        packed.Pack(detail)
        await context.abort_with_status(
            rpc_status.to_status(
                status_pb2.Status(
                    code=grpc_status.value[0],
                    message="Operation Admission rejected",
                    details=[packed],
                )
            )
        )
        raise AssertionError("abort returned")

    server, channel = await _start_admission_server(handler)
    try:
        with pytest.raises(SfpOperationAdmissionProtocolError):
            await OperationAdmissionClient().start(channel, request)
    finally:
        await channel.close()
        await server.stop(grace=None)


@pytest.mark.asyncio
async def test_wire_client_rejects_stale_or_malformed_provider_data() -> None:
    async def stale_handler(request, context):
        del request
        await context.abort(
            grpc.StatusCode.FAILED_PRECONDITION,
            "stale generation",
        )
        raise AssertionError("abort returned")

    stale_server, stale_channel = await _start_admission_server(stale_handler)
    try:
        with pytest.raises(SfpOperationAdmissionStaleError):
            await OperationAdmissionClient().start(
                stale_channel,
                _wire_request(),
            )
    finally:
        await stale_channel.close()
        await stale_server.stop(grace=None)

    async def malformed_handler(request, context):
        del request, context
        return b"not-a-protobuf"

    malformed_server, malformed_channel = await _start_admission_server(
        malformed_handler
    )
    try:
        with pytest.raises(SfpOperationAdmissionProtocolError):
            await OperationAdmissionClient().start(
                malformed_channel,
                _wire_request(),
            )
    finally:
        await malformed_channel.close()
        await malformed_server.stop(grace=None)


@pytest.mark.asyncio
async def test_wire_client_rejects_oversized_response_before_decoding() -> (
    None
):
    assert _decode_response(b"x" * _OVERSIZED_RESPONSE_LENGTH).limit_exceeded

    async def handler(request, context):
        del request, context
        return b"x" * _OVERSIZED_RESPONSE_LENGTH

    server, channel = await _start_admission_server(handler)
    try:
        with pytest.raises(SfpOperationAdmissionLimitError):
            await OperationAdmissionClient().start(channel, _wire_request())
    finally:
        await channel.close()
        await server.stop(grace=None)


def test_wire_client_rejects_oversized_request_before_transport() -> None:
    request = _wire_request()
    request.invocation.payload["oversized"] = "x" * contract.MAX_REQUEST_BYTES

    with pytest.raises(SfpOperationAdmissionLimitError):
        validate_request(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch",
    [
        "provider-instance",
        "endpoint-generation",
        "session",
        "manifest-revision",
        "operation-id",
        "fingerprint",
    ],
)
async def test_wire_client_rejects_mismatched_success_correlation(
    mismatch: str,
) -> None:
    async def handler(request, context):
        del context
        response = _wire_response(request)
        if mismatch == "provider-instance":
            response.provider_instance_id = str(uuid4())
        elif mismatch == "endpoint-generation":
            response.endpoint_generation += 1
        elif mismatch == "session":
            response.session_id = "foreign-session"
        elif mismatch == "manifest-revision":
            response.manifest_revision = "sha256:" + ("b" * 64)
        elif mismatch == "operation-id":
            response.record.operation_id = str(uuid4())
        elif mismatch == "fingerprint":
            response.record.request_fingerprint = "sha256:" + ("b" * 64)
        return response

    server, channel = await _start_admission_server(handler)
    try:
        with pytest.raises(SfpOperationAdmissionProtocolError):
            await OperationAdmissionClient().start(channel, _wire_request())
    finally:
        await channel.close()
        await server.stop(grace=None)
