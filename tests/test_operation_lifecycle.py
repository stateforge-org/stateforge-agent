"""Agent lifecycle observation, cancellation, and durable evidence tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import pytest
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import ParseDict
from stateforge_protocol.sfp.operation_admission.v1 import (
    operation_admission_pb2,
)
from stateforge_protocol.sfp.operation_admission.v1 import (
    validation as admission_validation,
)
from stateforge_protocol.sfp.operation_lifecycle.v1 import (
    operation_lifecycle_pb2,
)

from stateforge_agent._journal import SqliteJournal
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionDisposition,
    OperationAdmissionResult,
)
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAdmissionStore,
)
from stateforge_agent._sfp._services.operation_lifecycle.client import (
    OperationLifecycleClient,
    OperationLifecycleRequester,
    SfpOperationLifecycleProtocolError,
    SfpOperationLifecycleTransportError,
)
from stateforge_agent._sfp._services.operation_lifecycle.coordinator import (
    OperationLifecycleCoordinator,
    OperationLifecycleLease,
    OperationLifecyclePreconditionError,
)

_PROVIDER_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_SHORT_SOCKET_DIRECTORY = Path("/tmp")  # noqa: S108
_OPERATION_IDENTIFIER = "stateforge.test/inspect@1"
_MANIFEST_REVISION = "sha256:" + "a" * 64
_RESULT_SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
    "additionalProperties": False,
}


def _temporary_socket_path() -> Path:
    """Use a short unique path that fits Unix socket address limits."""
    return _SHORT_SOCKET_DIRECTORY / f"sf-{uuid4().hex}.sock"


def _timestamp(value: datetime) -> timestamp_pb2.Timestamp:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(value)
    return timestamp


async def _accepted(
    path: Path,
) -> tuple[SqliteJournal, OperationAdmissionStore, OperationAdmissionResult]:
    journal = SqliteJournal(path)
    await journal.open()
    store = journal.operation_admissions(maximum_unresolved_attempts=16)
    await store.initialize()
    operation_id = uuid4()
    now = datetime.now(UTC).replace(microsecond=0)
    invocation = operation_admission_pb2.OperationInvocation(
        operation_id=str(operation_id),
        operation_identifier=_OPERATION_IDENTIFIER,
        execution_deadline=_timestamp(now + timedelta(hours=1)),
        contract=operation_admission_pb2.OperationContractContext(
            manifest_revision=_MANIFEST_REVISION,
        ),
    )
    ParseDict({}, invocation.payload)
    ParseDict({"requester": "test"}, invocation.execution_context)
    fingerprint = admission_validation.compute_request_fingerprint(invocation)
    await store.create_intent(
        _PROVIDER_ID,
        invocation,
        fingerprint,
        result_schema_json=json.dumps(_RESULT_SCHEMA).encode(),
    )
    result = OperationAdmissionResult(
        provider_instance_id=_PROVIDER_ID,
        endpoint_generation=1,
        session_id="session-1",
        manifest_revision=_MANIFEST_REVISION,
        operation_id=operation_id,
        request_fingerprint=fingerprint,
        disposition=OperationAdmissionDisposition.ACCEPTED_NEW,
        admitted_at=now,
        record_expires_at=now + timedelta(hours=2),
        admitted_endpoint_generation=1,
    )
    await store.record_accepted(result)
    return journal, store, result


def _snapshot(
    admission: OperationAdmissionResult,
    state: int,
    sequence: int,
    *,
    result: dict[str, object] | None = None,
) -> operation_lifecycle_pb2.OperationSnapshot:
    snapshot = operation_lifecycle_pb2.OperationSnapshot(
        admission_record=operation_admission_pb2.OperationAdmissionRecord(
            operation_id=str(admission.operation_id),
            request_fingerprint=admission.request_fingerprint,
            admitted_at=_timestamp(admission.admitted_at),
            record_expires_at=_timestamp(admission.record_expires_at),
            admitted_endpoint_generation=admission.admitted_endpoint_generation,
        ),
        operation_identifier=_OPERATION_IDENTIFIER,
        manifest_revision=_MANIFEST_REVISION,
        sequence=sequence,
        state=state,
        updated_at=_timestamp(
            admission.admitted_at + timedelta(seconds=sequence - 1)
        ),
    )
    if result is not None:
        ParseDict(result, snapshot.success.result)
    return snapshot


def _response(
    admission: OperationAdmissionResult,
    snapshot: operation_lifecycle_pb2.OperationSnapshot,
    *,
    generation: int = 1,
    session_id: str = "session-1",
) -> operation_lifecycle_pb2.WatchOperationResponse:
    return operation_lifecycle_pb2.WatchOperationResponse(
        provider_instance_id=str(admission.provider_instance_id),
        endpoint_generation=generation,
        session_id=session_id,
        snapshot=snapshot,
    )


class _ScriptedClient(OperationLifecycleRequester):
    def __init__(
        self,
        pages: list[
            list[
                operation_lifecycle_pb2.WatchOperationResponse
                | SfpOperationLifecycleTransportError
            ]
            | SfpOperationLifecycleTransportError
        ],
        cancel_response: operation_lifecycle_pb2.CancelOperationResponse
        | None = None,
    ) -> None:
        self.pages = pages
        self.cancel_response = cancel_response
        self.watch_requests: list[
            operation_lifecycle_pb2.WatchOperationRequest
        ] = []
        self.cancel_requests: list[
            operation_lifecycle_pb2.CancelOperationRequest
        ] = []

    async def watch(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.WatchOperationRequest,
    ) -> AsyncIterator[operation_lifecycle_pb2.WatchOperationResponse]:
        del channel
        self.watch_requests.append(request)
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        for response in page:
            if isinstance(response, Exception):
                raise response
            yield response

    async def cancel(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.CancelOperationRequest,
    ) -> operation_lifecycle_pb2.CancelOperationResponse:
        del channel
        self.cancel_requests.append(request)
        if self.cancel_response is None:
            raise AssertionError("Unexpected cancellation call")
        return self.cancel_response


def _lease(generation: int = 1) -> OperationLifecycleLease:
    return OperationLifecycleLease(
        provider_instance_id=_PROVIDER_ID,
        endpoint_generation=generation,
        session_id=f"session-{generation}",
        channel=grpc.aio.insecure_channel(
            "unix:/tmp/unused-stateforge-test.sock"
        ),
        session_token=object(),
    )


async def _current(lease: OperationLifecycleLease) -> None:
    del lease


async def _until_terminal(
    coordinator: OperationLifecycleCoordinator,
    operation_id: UUID,
) -> None:
    for _ in range(100):
        status = await coordinator.status(_PROVIDER_ID, operation_id)
        if status is not None and status.state in {
            "OPERATION_STATE_SUCCEEDED",
            "OPERATION_STATE_FAILED",
            "OPERATION_STATE_CANCELLED",
        }:
            return
        await asyncio.sleep(0)
    raise AssertionError("Operation did not reach a terminal observation")


async def _until_watch_count(client: _ScriptedClient, expected: int) -> None:
    async with asyncio.timeout(2):
        while len(client.watch_requests) < expected:  # noqa: ASYNC110
            await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_watch_retains_typed_terminal_and_does_not_dispatch(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    accepted = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_ACCEPTED, 1
    )
    running = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_RUNNING, 2
    )
    succeeded = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        3,
        result={"value": 7},
    )
    client = _ScriptedClient(
        [
            [
                _response(admitted, item)
                for item in (accepted, running, succeeded)
            ]
        ]
    )
    coordinator = OperationLifecycleCoordinator(store, client=client)
    lease = _lease()
    try:
        await coordinator.resume(lease, assert_current=_current)
        await _until_terminal(coordinator, admitted.operation_id)
        status = await coordinator.status(_PROVIDER_ID, admitted.operation_id)
        assert status is not None
        assert status.result == {"value": 7.0}
        assert status.sequence == succeeded.sequence
        assert len(client.watch_requests) == 1
        assert client.watch_requests[0].after_sequence == 0
        assert await store.accepted_nonterminal() == ()
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_invalid_terminal_result_is_not_recorded(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    invalid = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        3,
        result={"value": "not an integer"},
    )
    client = _ScriptedClient([[_response(admitted, invalid)]])
    coordinator = OperationLifecycleCoordinator(store, client=client)
    lease = _lease()
    try:
        await coordinator.resume(lease, assert_current=_current)
        await _until_watch_count(client, 1)
        status = await coordinator.status(_PROVIDER_ID, admitted.operation_id)
        assert status is not None and status.state is None
        assert len(client.watch_requests) == 1
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_cancel_request_is_not_a_cancelled_outcome(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    requested = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_CANCELLATION_REQUESTED,
        3,
    )
    cancel_response = operation_lifecycle_pb2.CancelOperationResponse(
        provider_instance_id=str(_PROVIDER_ID),
        endpoint_generation=1,
        session_id="session-1",
        disposition=operation_lifecycle_pb2.CANCELLATION_DISPOSITION_REQUESTED,
        snapshot=requested,
    )
    client = _ScriptedClient([], cancel_response)
    coordinator = OperationLifecycleCoordinator(store, client=client)
    lease = _lease()
    try:
        status = await coordinator.cancel(
            lease, admitted.operation_id, assert_current=_current
        )
        assert status.state == "OPERATION_STATE_CANCELLATION_REQUESTED"
        assert status.cancellation_disposition == (
            "CANCELLATION_DISPOSITION_REQUESTED"
        )
        assert status.state != "OPERATION_STATE_CANCELLED"
        assert len(client.cancel_requests) == 1
        assert client.cancel_requests[0].operation_id == str(
            admitted.operation_id
        )
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_unknown_cancellation_never_contacts_provider(
    tmp_path: Path,
) -> None:
    journal, store, _ = await _accepted(tmp_path / "agent.db")
    client = _ScriptedClient([])
    coordinator = OperationLifecycleCoordinator(store, client=client)
    lease = _lease()
    try:
        with pytest.raises(OperationLifecyclePreconditionError):
            await coordinator.cancel(lease, uuid4(), assert_current=_current)
        assert client.cancel_requests == []
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_terminal_outcome_cannot_be_replaced(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    success = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        3,
        result={"value": 7},
    )
    different = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        3,
        result={"value": 9},
    )
    try:
        await store.record_lifecycle_snapshot(
            _PROVIDER_ID, admitted.operation_id, success
        )
        with pytest.raises(ValueError):
            await store.record_lifecycle_snapshot(
                _PROVIDER_ID, admitted.operation_id, different
            )
        retained = await store.get(_PROVIDER_ID, admitted.operation_id)
        assert retained is not None and retained.lifecycle_snapshot == success
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_watch_retry_preserves_cursor_and_never_redispatches(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    accepted = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_ACCEPTED, 1
    )
    succeeded = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        3,
        result={"value": 7},
    )
    client = _ScriptedClient(
        [
            [
                _response(admitted, accepted),
                SfpOperationLifecycleTransportError(
                    grpc.StatusCode.UNAVAILABLE
                ),
            ],
            [_response(admitted, succeeded)],
        ]
    )
    coordinator = OperationLifecycleCoordinator(
        store, client=client, retry_initial_delay_seconds=0.001
    )
    lease = _lease()
    try:
        await coordinator.resume(lease, assert_current=_current)
        await _until_terminal(coordinator, admitted.operation_id)
        assert [
            request.after_sequence for request in client.watch_requests
        ] == [
            0,
            1,
        ]
        assert client.cancel_requests == []
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_restarted_observer_resumes_from_durable_cursor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.db"
    journal, store, admitted = await _accepted(path)
    running = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_RUNNING, 2
    )
    await store.record_lifecycle_snapshot(
        _PROVIDER_ID, admitted.operation_id, running
    )
    await journal.close()
    reopened = SqliteJournal(path)
    await reopened.open()
    reopened_store = reopened.operation_admissions(
        maximum_unresolved_attempts=16
    )
    await reopened_store.initialize()
    success = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        4,
        result={"value": 7},
    )
    client = _ScriptedClient([[_response(admitted, success)]])
    coordinator = OperationLifecycleCoordinator(reopened_store, client=client)
    lease = _lease(2)
    try:
        await coordinator.resume(lease, assert_current=_current)
        await _until_terminal(coordinator, admitted.operation_id)
        assert [
            request.after_sequence for request in client.watch_requests
        ] == [running.sequence]
        assert client.watch_requests[
            0
        ].session_binding.expected_session_id == ("session-2")
    finally:
        await coordinator.close()
        await lease.channel.close()
        await reopened.close()


@pytest.mark.asyncio
async def test_completion_wins_over_late_cancellation_response(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    success = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        5,
        result={"value": 7},
    )
    await store.record_lifecycle_snapshot(
        _PROVIDER_ID, admitted.operation_id, success
    )
    requested = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_CANCELLATION_REQUESTED,
        3,
    )
    client = _ScriptedClient(
        [],
        operation_lifecycle_pb2.CancelOperationResponse(
            provider_instance_id=str(_PROVIDER_ID),
            endpoint_generation=1,
            session_id="session-1",
            disposition=(
                operation_lifecycle_pb2.CANCELLATION_DISPOSITION_REQUESTED
            ),
            snapshot=requested,
        ),
    )
    coordinator = OperationLifecycleCoordinator(store, client=client)
    lease = _lease()
    try:
        status = await coordinator.cancel(
            lease, admitted.operation_id, assert_current=_current
        )
        assert status.state == "OPERATION_STATE_SUCCEEDED"
        assert status.sequence == success.sequence
        assert status.result == {"value": 7.0}
        assert status.cancellation_disposition == (
            "CANCELLATION_DISPOSITION_REQUESTED"
        )
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_stale_session_cannot_retain_watch_evidence(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    accepted = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_ACCEPTED, 1
    )
    client = _ScriptedClient([[_response(admitted, accepted)]])
    coordinator = OperationLifecycleCoordinator(store, client=client)
    lease = _lease()
    current_checks = 0

    async def replaced(_lease: OperationLifecycleLease) -> None:
        nonlocal current_checks
        current_checks += 1
        if current_checks > 1:
            raise OperationLifecyclePreconditionError("replaced")

    try:
        await coordinator.resume(lease, assert_current=replaced)
        await _until_watch_count(client, 1)
        for _ in range(20):
            await asyncio.sleep(0)
        status = await coordinator.status(_PROVIDER_ID, admitted.operation_id)
        assert status is not None and status.state is None
    finally:
        await coordinator.close()
        await lease.channel.close()
        await journal.close()


@pytest.mark.asyncio
async def test_wrong_admission_fingerprint_never_replaces_evidence(
    tmp_path: Path,
) -> None:
    journal, store, admitted = await _accepted(tmp_path / "agent.db")
    invalid = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_ACCEPTED, 1
    )
    invalid.admission_record.request_fingerprint = "sha256:" + "b" * 64
    try:
        with pytest.raises(ValueError):
            await store.record_lifecycle_snapshot(
                _PROVIDER_ID, admitted.operation_id, invalid
            )
        attempt = await store.get(_PROVIDER_ID, admitted.operation_id)
        assert attempt is not None and attempt.lifecycle_snapshot is None
    finally:
        await journal.close()


@pytest.mark.asyncio
async def test_real_grpc_client_validates_watch_and_cancel_correlation(
    tmp_path: Path,
) -> None:
    journal, _, admitted = await _accepted(tmp_path / "agent.db")
    accepted = _snapshot(
        admitted, operation_lifecycle_pb2.OPERATION_STATE_ACCEPTED, 1
    )
    success = _snapshot(
        admitted,
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        3,
        result={"value": 7},
    )
    seen: list[str] = []

    async def watch(request, _context):  # type: ignore[no-untyped-def]
        seen.append(f"watch:{request.operation_id}:{request.after_sequence}")
        yield _response(admitted, accepted)
        coalesced = _response(admitted, success)
        coalesced.updates_skipped = True
        yield coalesced

    async def cancel(request, _context):  # type: ignore[no-untyped-def]
        seen.append(f"cancel:{request.operation_id}")
        return operation_lifecycle_pb2.CancelOperationResponse(
            provider_instance_id=str(_PROVIDER_ID),
            endpoint_generation=1,
            session_id="session-1",
            disposition=(
                operation_lifecycle_pb2.CANCELLATION_DISPOSITION_ALREADY_TERMINAL
            ),
            snapshot=success,
        )

    service = operation_lifecycle_pb2.DESCRIPTOR.services_by_name[
        "OperationLifecycleService"
    ].full_name
    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                service,
                {
                    "WatchOperation": grpc.unary_stream_rpc_method_handler(
                        watch,
                        request_deserializer=(
                            operation_lifecycle_pb2.WatchOperationRequest.FromString
                        ),
                        response_serializer=(
                            operation_lifecycle_pb2.WatchOperationResponse.SerializeToString
                        ),
                    ),
                    "CancelOperation": grpc.unary_unary_rpc_method_handler(
                        cancel,
                        request_deserializer=(
                            operation_lifecycle_pb2.CancelOperationRequest.FromString
                        ),
                        response_serializer=(
                            operation_lifecycle_pb2.CancelOperationResponse.SerializeToString
                        ),
                    ),
                },
            ),
        )
    )
    socket_path = _temporary_socket_path()
    address = f"unix:{socket_path}"
    server.add_insecure_port(address)
    await server.start()
    channel = grpc.aio.insecure_channel(address)
    client = OperationLifecycleClient()
    binding = operation_admission_pb2.OperationSessionBinding(
        expected_provider_instance_id=str(_PROVIDER_ID),
        expected_endpoint_generation=1,
        expected_session_id="session-1",
    )
    try:
        request = operation_lifecycle_pb2.WatchOperationRequest(
            session_binding=binding,
            operation_id=str(admitted.operation_id),
            expected_request_fingerprint=admitted.request_fingerprint,
        )
        observed = [
            response async for response in client.watch(channel, request)
        ]
        assert [item.snapshot.sequence for item in observed] == [1, 3]
        decision = await client.cancel(
            channel,
            operation_lifecycle_pb2.CancelOperationRequest(
                session_binding=binding,
                operation_id=str(admitted.operation_id),
                expected_request_fingerprint=admitted.request_fingerprint,
            ),
        )
        assert decision.snapshot == success
        assert seen == [
            f"watch:{admitted.operation_id}:0",
            f"cancel:{admitted.operation_id}",
        ]
    finally:
        await channel.close()
        await server.stop(0)
        socket_path.unlink(missing_ok=True)
        await journal.close()


@pytest.mark.asyncio
async def test_real_grpc_client_rejects_foreign_watch_response(
    tmp_path: Path,
) -> None:
    journal, _, admitted = await _accepted(tmp_path / "agent.db")
    foreign = _response(
        admitted,
        _snapshot(
            admitted, operation_lifecycle_pb2.OPERATION_STATE_ACCEPTED, 1
        ),
    )
    foreign.provider_instance_id = str(uuid4())

    async def watch(_request, _context):  # type: ignore[no-untyped-def]
        yield foreign

    service = operation_lifecycle_pb2.DESCRIPTOR.services_by_name[
        "OperationLifecycleService"
    ].full_name
    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                service,
                {
                    "WatchOperation": grpc.unary_stream_rpc_method_handler(
                        watch,
                        request_deserializer=(
                            operation_lifecycle_pb2.WatchOperationRequest.FromString
                        ),
                        response_serializer=(
                            operation_lifecycle_pb2.WatchOperationResponse.SerializeToString
                        ),
                    )
                },
            ),
        )
    )
    socket_path = _temporary_socket_path()
    address = f"unix:{socket_path}"
    server.add_insecure_port(address)
    await server.start()
    channel = grpc.aio.insecure_channel(address)
    try:
        request = operation_lifecycle_pb2.WatchOperationRequest(
            session_binding=operation_admission_pb2.OperationSessionBinding(
                expected_provider_instance_id=str(_PROVIDER_ID),
                expected_endpoint_generation=1,
                expected_session_id="session-1",
            ),
            operation_id=str(admitted.operation_id),
            expected_request_fingerprint=admitted.request_fingerprint,
        )
        with pytest.raises(SfpOperationLifecycleProtocolError):
            async for _ in OperationLifecycleClient().watch(channel, request):
                pass
    finally:
        await channel.close()
        await server.stop(0)
        socket_path.unlink(missing_ok=True)
        await journal.close()
