"""Tests for durable Agent-side Trigger Publication acceptance."""

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from google.protobuf.json_format import ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
from stateforge_protocol.sfp.trigger_publication.v1 import (
    trigger_publication_pb2,
)

from stateforge_agent._journal import SqliteJournal, SqliteJournalError
from stateforge_agent._sfp._services.trigger_publication import (
    TriggerAcceptanceContext,
    TriggerAcceptanceDecision,
    TriggerPublicationStore,
)

_TRIGGER_IDENTIFIER = "example.systemd/unit.failed@1"
_TWO_PUBLICATIONS = 2


def _context(
    provider_instance_id: UUID | None = None,
) -> TriggerAcceptanceContext:
    return TriggerAcceptanceContext(
        provider_instance_id=provider_instance_id or uuid4(),
        endpoint_generation=1,
        session_id="session-1",
        manifest_revision="sha256:" + ("a" * 64),
    )


def _publication(
    publication_id: UUID,
    *,
    unit: str = "nginx.service",
) -> trigger_publication_pb2.TriggerPublication:
    payload = ParseDict(
        {"unit": unit, "state": "failed"},
        trigger_publication_pb2.TriggerPublication().payload,
    )
    return trigger_publication_pb2.TriggerPublication(
        publication_id=str(publication_id),
        trigger_identifier=_TRIGGER_IDENTIFIER,
        observed_at=Timestamp(seconds=1),
        payload=payload,
    )


async def _store(
    path: Path,
    *,
    capacity: int = 10,
) -> tuple[SqliteJournal, TriggerPublicationStore]:
    journal = SqliteJournal(path)
    await journal.open()
    store = TriggerPublicationStore(
        journal,
        maximum_pending_publications=capacity,
    )
    await store.initialize()
    return journal, store


@pytest.mark.asyncio
async def test_accepts_new_publication_and_identical_replay_once(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db")
    context = _context()
    publication = _publication(uuid4())

    first = await store.accept(context, publication)
    replay = await store.accept(context, publication)

    assert first.decision is TriggerAcceptanceDecision.ACCEPTED_NEW
    assert replay.decision is TriggerAcceptanceDecision.ACCEPTED_DUPLICATE
    assert replay.content_fingerprint == first.content_fingerprint
    assert await store.pending_count() == 1
    await journal.close()


@pytest.mark.asyncio
async def test_detects_conflicting_publication_id_reuse(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db")
    context = _context()
    publication_id = uuid4()

    first = await store.accept(context, _publication(publication_id))
    conflict = await store.accept(
        context,
        _publication(publication_id, unit="postgresql.service"),
    )

    assert first.decision is TriggerAcceptanceDecision.ACCEPTED_NEW
    assert conflict.decision is TriggerAcceptanceDecision.IDEMPOTENCY_CONFLICT
    assert await store.pending_count() == 1
    await journal.close()


@pytest.mark.asyncio
async def test_persists_acceptance_across_agent_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.db"
    context = _context()
    publication = _publication(uuid4())
    journal, store = await _store(path)
    await store.accept(context, publication)
    await journal.close()

    restarted_journal, restarted_store = await _store(path)
    replay = await restarted_store.accept(context, publication)

    assert replay.decision is TriggerAcceptanceDecision.ACCEPTED_DUPLICATE
    assert await restarted_store.pending_count() == 1
    await restarted_journal.close()


@pytest.mark.asyncio
async def test_scopes_idempotency_to_provider_installation(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db")
    publication = _publication(uuid4())

    first = await store.accept(_context(), publication)
    other_provider = await store.accept(_context(), publication)

    assert first.decision is TriggerAcceptanceDecision.ACCEPTED_NEW
    assert other_provider.decision is TriggerAcceptanceDecision.ACCEPTED_NEW
    assert await store.pending_count() == _TWO_PUBLICATIONS
    await journal.close()


@pytest.mark.asyncio
async def test_capacity_rejects_only_new_responsibility(
    tmp_path: Path,
) -> None:
    journal, store = await _store(tmp_path / "agent.db", capacity=1)
    context = _context()
    accepted = _publication(uuid4())
    await store.accept(context, accepted)

    duplicate = await store.accept(context, accepted)
    capacity = await store.accept(context, _publication(uuid4()))

    assert duplicate.decision is TriggerAcceptanceDecision.ACCEPTED_DUPLICATE
    assert capacity.decision is TriggerAcceptanceDecision.CAPACITY_EXHAUSTED
    assert await store.pending_count() == 1
    await journal.close()


@pytest.mark.asyncio
async def test_closed_journal_rejects_new_transactions(tmp_path: Path) -> None:
    journal, store = await _store(tmp_path / "agent.db")
    await journal.close()

    with pytest.raises(SqliteJournalError, match="not open"):
        await store.pending_count()
