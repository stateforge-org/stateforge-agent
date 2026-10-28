"""Agent-side Trigger Publication validation and acknowledgement tests."""

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import cast
from uuid import uuid4

import grpc
import pytest
from google.protobuf.json_format import ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
from stateforge_protocol.sfp.trigger_publication.v1 import (
    contract,
    trigger_publication_pb2,
    trigger_publication_pb2_grpc,
)

from stateforge_agent._journal import SqliteJournal
from stateforge_agent._sfp._services.trigger_publication.client import (
    SfpTriggerPublicationLimitError,
    SfpTriggerPublicationStaleError,
    SfpTriggerPublicationUnavailableError,
    TriggerPublicationClient,
    TriggerPublicationHandler,
    TriggerStreamBinding,
)
from stateforge_agent._sfp._services.trigger_publication.coordinator import (
    TriggerPublicationCoordinator,
    TriggerPublicationLease,
)
from stateforge_agent._sfp._services.trigger_publication.store import (
    TriggerPublicationStore,
)

_TRIGGER_IDENTIFIER = "example.systemd/unit.failed@1"
_STALE_BEFORE_DURABLE_COMMIT = 2
_STALE_AFTER_DURABLE_COMMIT = 3
_TRIGGER_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"unit": {"type": "string"}},
    "required": ["unit"],
    "additionalProperties": False,
}


class _TriggerPeer(
    trigger_publication_pb2_grpc.TriggerPublicationServiceServicer
):
    """Serve one publication through a real bidirectional gRPC stream."""

    def __init__(
        self,
        publication: trigger_publication_pb2.TriggerPublication,
    ) -> None:
        self.publication = publication
        self.requests: list[trigger_publication_pb2.StreamTriggersRequest] = []

    async def StreamTriggers(  # noqa: N802
        self,
        request_iterator: AsyncIterator[
            trigger_publication_pb2.StreamTriggersRequest
        ],
        context,
    ) -> AsyncIterator[trigger_publication_pb2.StreamTriggersResponse]:
        del context
        opening = await anext(request_iterator)
        self.requests.append(opening)
        binding = opening.open.session_binding
        yield trigger_publication_pb2.StreamTriggersResponse(
            ready=trigger_publication_pb2.TriggerStreamReady(
                provider_instance_id=binding.expected_provider_instance_id,
                endpoint_generation=binding.expected_endpoint_generation,
                session_id=binding.expected_session_id,
                manifest_revision=binding.expected_manifest_revision,
                maximum_in_flight_publications=(
                    opening.open.requested_max_in_flight_publications
                ),
            )
        )
        yield trigger_publication_pb2.StreamTriggersResponse(
            publication=self.publication
        )
        self.requests.append(await anext(request_iterator))


async def _start_peer(
    peer: _TriggerPeer,
) -> tuple[grpc.aio.Server, grpc.aio.Channel]:
    server = grpc.aio.server()
    trigger_publication_pb2_grpc.add_TriggerPublicationServiceServicer_to_server(
        peer,
        server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    await channel.channel_ready()
    return server, channel


class _ScriptedConsumer:
    """Deliver scripted publications and retain their acknowledgements."""

    def __init__(
        self,
        publications: list[trigger_publication_pb2.TriggerPublication],
    ) -> None:
        self.publications = publications
        self.bindings: list[TriggerStreamBinding] = []
        self.acknowledgements: list[
            trigger_publication_pb2.TriggerAcknowledgement
        ] = []

    async def consume(
        self,
        channel: grpc.aio.Channel,
        binding: TriggerStreamBinding,
        handler: TriggerPublicationHandler,
    ) -> None:
        del channel
        self.bindings.append(binding)
        for publication in self.publications:
            self.acknowledgements.append(await handler(publication))
        raise SfpTriggerPublicationStaleError("script complete")


def _publication(
    *,
    publication_id: str | None = None,
    trigger_identifier: str = _TRIGGER_IDENTIFIER,
    payload: dict[str, object] | None = None,
) -> trigger_publication_pb2.TriggerPublication:
    payload_value = {"unit": "nginx.service"} if payload is None else payload
    message = trigger_publication_pb2.TriggerPublication()
    ParseDict(payload_value, message.payload)
    return trigger_publication_pb2.TriggerPublication(
        publication_id=publication_id or str(uuid4()),
        trigger_identifier=trigger_identifier,
        observed_at=Timestamp(seconds=1),
        payload=message.payload,
    )


def _lease() -> TriggerPublicationLease:
    return TriggerPublicationLease(
        provider_instance_id=uuid4(),
        endpoint_generation=1,
        session_id="session-1",
        manifest_revision="sha256:" + ("a" * 64),
        availability_generation=2,
        trigger_schemas={_TRIGGER_IDENTIFIER: _TRIGGER_SCHEMA},
        channel=cast(grpc.aio.Channel, object()),
        session_token=object(),
    )


async def _coordinator(
    path: Path,
    consumer: _ScriptedConsumer,
    *,
    capacity: int = 10,
) -> tuple[
    SqliteJournal,
    TriggerPublicationStore,
    TriggerPublicationCoordinator,
]:
    journal = SqliteJournal(path)
    await journal.open()
    store = TriggerPublicationStore(
        journal,
        maximum_pending_publications=capacity,
    )
    await store.initialize()
    return (
        journal,
        store,
        TriggerPublicationCoordinator(
            store,
            maximum_in_flight_publications=8,
            client=consumer,
        ),
    )


async def _current(lease: TriggerPublicationLease) -> None:
    del lease


async def _run_script(
    coordinator: TriggerPublicationCoordinator,
    lease: TriggerPublicationLease,
    *,
    current: Callable[[TriggerPublicationLease], Awaitable[None]] = _current,
) -> None:
    with pytest.raises(
        SfpTriggerPublicationStaleError, match="script complete"
    ):
        await coordinator.run(lease, assert_current=current)


@pytest.mark.asyncio
async def test_client_opens_real_stream_and_returns_durable_ack() -> None:
    publication = _publication()
    peer = _TriggerPeer(publication)
    server, channel = await _start_peer(peer)
    lease = _lease()

    async def accept(
        item: trigger_publication_pb2.TriggerPublication,
    ) -> trigger_publication_pb2.TriggerAcknowledgement:
        assert item == publication
        return trigger_publication_pb2.TriggerAcknowledgement(
            publication_id=item.publication_id,
            disposition=(
                trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_NEW
            ),
        )

    try:
        async with asyncio.timeout(3):
            with pytest.raises(
                SfpTriggerPublicationUnavailableError,
                match="ended",
            ):
                await TriggerPublicationClient().consume(
                    channel,
                    TriggerStreamBinding(
                        provider_instance_id=str(lease.provider_instance_id),
                        endpoint_generation=lease.endpoint_generation,
                        session_id=lease.session_id,
                        manifest_revision=lease.manifest_revision,
                        maximum_in_flight_publications=8,
                    ),
                    accept,
                )
        assert peer.requests[0].WhichOneof("message") == "open"
        acknowledgement = peer.requests[1].acknowledgement
        assert acknowledgement.publication_id == publication.publication_id
        assert acknowledgement.disposition == (
            trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_NEW
        )
    finally:
        await channel.close()
        await server.stop(None)


@pytest.mark.asyncio
async def test_accepts_new_and_duplicate_publications(tmp_path: Path) -> None:
    publication = _publication()
    consumer = _ScriptedConsumer([publication, publication])
    journal, _, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
    )

    await _run_script(coordinator, _lease())

    assert [item.disposition for item in consumer.acknowledgements] == [
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_NEW,
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_DUPLICATE,
    ]
    await journal.close()


@pytest.mark.asyncio
async def test_rejects_undeclared_and_schema_invalid_publications(
    tmp_path: Path,
) -> None:
    consumer = _ScriptedConsumer(
        [
            _publication(trigger_identifier="example.other/event@1"),
            _publication(payload={"unit": 5}),
        ]
    )
    journal, _, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
    )

    await _run_script(coordinator, _lease())

    assert [item.rejection.reason for item in consumer.acknowledgements] == [
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_TRIGGER_NOT_DECLARED,
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_PAYLOAD_SCHEMA_VIOLATION,
    ]
    await journal.close()


@pytest.mark.asyncio
async def test_rejects_conflicting_identity_and_exhausted_capacity(
    tmp_path: Path,
) -> None:
    publication_id = str(uuid4())
    consumer = _ScriptedConsumer(
        [
            _publication(publication_id=publication_id),
            _publication(
                publication_id=publication_id,
                payload={"unit": "postgresql.service"},
            ),
            _publication(),
        ]
    )
    journal, _, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
        capacity=1,
    )

    await _run_script(coordinator, _lease())

    acknowledgements = consumer.acknowledgements
    assert acknowledgements[0].disposition == (
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_NEW
    )
    assert acknowledgements[1].rejection.reason == (
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_IDEMPOTENCY_CONFLICT
    )
    assert acknowledgements[2].rejection.reason == (
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_AGENT_CAPACITY_EXHAUSTED
    )
    assert acknowledgements[2].rejection.retryable
    await journal.close()


@pytest.mark.asyncio
async def test_commit_before_lost_ack_replays_as_duplicate(
    tmp_path: Path,
) -> None:
    publication = _publication()
    path = tmp_path / "agent.db"
    interrupted = _ScriptedConsumer([publication])
    journal, store, coordinator = await _coordinator(path, interrupted)
    calls = 0
    lease = _lease()

    async def become_stale(lease: TriggerPublicationLease) -> None:
        nonlocal calls
        del lease
        calls += 1
        if calls == _STALE_AFTER_DURABLE_COMMIT:
            raise SfpTriggerPublicationStaleError("availability changed")

    with pytest.raises(
        SfpTriggerPublicationStaleError,
        match="availability changed",
    ):
        await coordinator.run(lease, assert_current=become_stale)
    assert interrupted.acknowledgements == []

    replay = _ScriptedConsumer([publication])
    replay_coordinator = TriggerPublicationCoordinator(
        store,
        maximum_in_flight_publications=8,
        client=replay,
    )
    await _run_script(replay_coordinator, lease)
    assert replay.acknowledgements[0].disposition == (
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_DUPLICATE
    )
    await journal.close()


@pytest.mark.asyncio
async def test_rejects_invalid_publication_without_assuming_responsibility(
    tmp_path: Path,
) -> None:
    publication = _publication()
    publication.ClearField("observed_at")
    consumer = _ScriptedConsumer([publication])
    journal, store, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
    )

    await _run_script(coordinator, _lease())

    acknowledgement = consumer.acknowledgements[0]
    assert acknowledgement.disposition == (
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_REJECTED
    )
    assert acknowledgement.rejection.reason == (
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_INVALID_PUBLICATION
    )
    assert not acknowledgement.rejection.retryable
    assert await store.pending_count() == 0
    await journal.close()


@pytest.mark.asyncio
async def test_storage_failure_before_commit_is_retryable_and_not_accepted(
    tmp_path: Path,
) -> None:
    publication = _publication()
    consumer = _ScriptedConsumer([publication])
    path = tmp_path / "agent.db"
    journal, _, coordinator = await _coordinator(path, consumer)
    await journal.close()

    await _run_script(coordinator, _lease())

    acknowledgement = consumer.acknowledgements[0]
    assert acknowledgement.disposition == (
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_REJECTED
    )
    assert acknowledgement.rejection.reason == (
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_DELIVERY_RESPONSIBILITY_UNAVAILABLE
    )
    assert acknowledgement.rejection.retryable
    restarted_journal = SqliteJournal(path)
    await restarted_journal.open()
    restarted_store = TriggerPublicationStore(
        restarted_journal,
        maximum_pending_publications=10,
    )
    await restarted_store.initialize()
    assert await restarted_store.pending_count() == 0
    await restarted_journal.close()


@pytest.mark.asyncio
async def test_stale_lease_before_commit_assumes_no_responsibility(
    tmp_path: Path,
) -> None:
    publication = _publication()
    consumer = _ScriptedConsumer([publication])
    journal, store, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
    )
    checks = 0

    async def become_stale(lease: TriggerPublicationLease) -> None:
        nonlocal checks
        del lease
        checks += 1
        if checks == _STALE_BEFORE_DURABLE_COMMIT:
            raise SfpTriggerPublicationStaleError("session replaced")

    with pytest.raises(
        SfpTriggerPublicationStaleError,
        match="session replaced",
    ):
        await coordinator.run(_lease(), assert_current=become_stale)

    assert consumer.acknowledgements == []
    assert await store.pending_count() == 0
    await journal.close()


@pytest.mark.asyncio
async def test_rejection_does_not_expose_sensitive_payload(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive_value = "never-emit-this-token"
    consumer = _ScriptedConsumer(
        [
            _publication(
                payload={"unit": "nginx.service", "token": sensitive_value}
            )
        ]
    )
    journal, _, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
    )

    with caplog.at_level(logging.DEBUG):
        await _run_script(coordinator, _lease())

    acknowledgement = consumer.acknowledgements[0]
    assert acknowledgement.rejection.reason == (
        trigger_publication_pb2.TRIGGER_REJECTION_REASON_PAYLOAD_SCHEMA_VIOLATION
    )
    assert sensitive_value not in str(acknowledgement)
    assert sensitive_value not in caplog.text
    await journal.close()


@pytest.mark.asyncio
async def test_source_and_target_payload_fields_remain_opaque(
    tmp_path: Path,
) -> None:
    identifier = "example.application/change.detected@1"
    publication = _publication(
        trigger_identifier=identifier,
        payload={"source": "scheduler", "target": "database-primary"},
    )
    consumer = _ScriptedConsumer([publication])
    journal, store, coordinator = await _coordinator(
        tmp_path / "agent.db",
        consumer,
    )
    lease = replace(
        _lease(),
        trigger_schemas={
            identifier: {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "target": {"type": "string"},
                },
                "required": ["source", "target"],
                "additionalProperties": False,
            }
        },
    )

    await _run_script(coordinator, lease)

    assert consumer.acknowledgements[0].disposition == (
        trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_NEW
    )
    assert await store.pending_count() == 1
    await journal.close()


@pytest.mark.asyncio
async def test_client_rejects_request_over_canonical_limit() -> None:
    lease = _lease()
    binding = TriggerStreamBinding(
        provider_instance_id=str(lease.provider_instance_id),
        endpoint_generation=lease.endpoint_generation,
        session_id="x" * contract.MAX_REQUEST_BYTES,
        manifest_revision=lease.manifest_revision,
        maximum_in_flight_publications=8,
    )

    async def unreachable_handler(
        publication: trigger_publication_pb2.TriggerPublication,
    ) -> trigger_publication_pb2.TriggerAcknowledgement:
        raise AssertionError(publication)

    with pytest.raises(
        SfpTriggerPublicationLimitError,
        match="request limit exceeded",
    ):
        await TriggerPublicationClient().consume(
            cast(grpc.aio.Channel, object()),
            binding,
            unreachable_handler,
        )


@pytest.mark.asyncio
async def test_client_rejects_response_over_canonical_limit() -> None:
    publication = _publication(
        payload={"unit": "x" * contract.MAX_RESPONSE_BYTES}
    )
    peer = _TriggerPeer(publication)
    server, channel = await _start_peer(peer)
    lease = _lease()

    async def unreachable_handler(
        item: trigger_publication_pb2.TriggerPublication,
    ) -> trigger_publication_pb2.TriggerAcknowledgement:
        raise AssertionError(item)

    try:
        with pytest.raises(
            SfpTriggerPublicationLimitError,
            match="response limit exceeded",
        ):
            await TriggerPublicationClient().consume(
                channel,
                TriggerStreamBinding(
                    provider_instance_id=str(lease.provider_instance_id),
                    endpoint_generation=lease.endpoint_generation,
                    session_id=lease.session_id,
                    manifest_revision=lease.manifest_revision,
                    maximum_in_flight_publications=8,
                ),
                unreachable_handler,
            )
    finally:
        await channel.close()
        await server.stop(None)
