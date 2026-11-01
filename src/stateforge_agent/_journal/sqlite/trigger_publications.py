"""Persist Trigger Publication responsibility in SQLite."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol, TypeVar

from stateforge_protocol.sfp.trigger_publication.v1 import (
    trigger_publication_pb2,
    validation,
)

from stateforge_agent._sfp._services.trigger_publication.store import (
    TriggerAcceptanceContext,
    TriggerAcceptanceDecision,
    TriggerAcceptanceResult,
    TriggerPublicationStore,
)

_SCHEMA_COMPONENT = "trigger_publication"
_SCHEMA_VERSION = 1
_PENDING_STATE = "pending"
_Result = TypeVar("_Result")


class _SqliteTransactions(Protocol):
    """Expose only the SQLite transaction boundary needed by this adapter."""

    async def transaction(
        self,
        operation: Callable[[sqlite3.Connection], _Result],
    ) -> _Result:
        """Run one serialized SQLite transaction."""


class SqliteTriggerPublicationStore(TriggerPublicationStore):
    """Own SQLite trigger idempotency and pending-forward responsibility."""

    def __init__(
        self,
        journal: _SqliteTransactions,
        *,
        maximum_pending_publications: int,
    ) -> None:
        """Configure a bounded SQLite trigger responsibility store."""
        if maximum_pending_publications <= 0:
            raise ValueError("maximum_pending_publications must be positive")
        self._journal = journal
        self._maximum_pending_publications = maximum_pending_publications

    async def initialize(self) -> None:
        """Create the first trigger journal schema or verify its version."""
        await self._journal.transaction(self._initialize)

    async def accept(
        self,
        context: TriggerAcceptanceContext,
        publication: trigger_publication_pb2.TriggerPublication,
    ) -> TriggerAcceptanceResult:
        """Atomically assume responsibility or classify an existing event."""
        fingerprint = validation.compute_publication_content_fingerprint(
            publication
        )
        serialized = publication.SerializeToString(deterministic=True)
        accepted_at = datetime.now(UTC).isoformat()

        def accept_transaction(
            connection: sqlite3.Connection,
        ) -> TriggerAcceptanceResult:
            existing = connection.execute(
                """
                SELECT content_fingerprint
                FROM trigger_publications
                WHERE provider_instance_id = ? AND publication_id = ?
                """,
                (
                    str(context.provider_instance_id),
                    publication.publication_id,
                ),
            ).fetchone()
            pending = self._pending_count(connection)
            if existing is not None:
                decision = (
                    TriggerAcceptanceDecision.ACCEPTED_DUPLICATE
                    if existing[0] == fingerprint
                    else TriggerAcceptanceDecision.IDEMPOTENCY_CONFLICT
                )
                return TriggerAcceptanceResult(decision, fingerprint, pending)
            if pending >= self._maximum_pending_publications:
                return TriggerAcceptanceResult(
                    TriggerAcceptanceDecision.CAPACITY_EXHAUSTED,
                    fingerprint,
                    pending,
                )

            connection.execute(
                """
                INSERT INTO trigger_publications (
                    provider_instance_id,
                    publication_id,
                    content_fingerprint,
                    publication,
                    trigger_identifier,
                    observed_at,
                    endpoint_generation,
                    session_id,
                    manifest_revision,
                    accepted_at,
                    forwarding_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(context.provider_instance_id),
                    publication.publication_id,
                    fingerprint,
                    serialized,
                    publication.trigger_identifier,
                    publication.observed_at.ToJsonString(),
                    context.endpoint_generation,
                    context.session_id,
                    context.manifest_revision,
                    accepted_at,
                    _PENDING_STATE,
                ),
            )
            return TriggerAcceptanceResult(
                TriggerAcceptanceDecision.ACCEPTED_NEW,
                fingerprint,
                pending + 1,
            )

        return await self._journal.transaction(accept_transaction)

    async def pending_count(self) -> int:
        """Return the number of events awaiting the future SF-AOP boundary."""
        return await self._journal.transaction(self._pending_count)

    @staticmethod
    def _initialize(connection: sqlite3.Connection) -> None:
        """Create and verify the trigger-owned schema."""
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS journal_schemas (
                component TEXT PRIMARY KEY,
                version INTEGER NOT NULL CHECK (version > 0)
            ) WITHOUT ROWID
            """
        )
        row = connection.execute(
            "SELECT version FROM journal_schemas WHERE component = ?",
            (_SCHEMA_COMPONENT,),
        ).fetchone()
        if row is not None and row[0] != _SCHEMA_VERSION:
            raise sqlite3.DatabaseError(
                "Unsupported Trigger Publication journal schema"
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trigger_publications (
                provider_instance_id TEXT NOT NULL,
                publication_id TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL,
                publication BLOB NOT NULL,
                trigger_identifier TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                endpoint_generation INTEGER NOT NULL
                    CHECK (endpoint_generation > 0),
                session_id TEXT NOT NULL,
                manifest_revision TEXT NOT NULL,
                accepted_at TEXT NOT NULL,
                forwarding_state TEXT NOT NULL
                    CHECK (forwarding_state = 'pending'),
                PRIMARY KEY (provider_instance_id, publication_id)
            ) WITHOUT ROWID
            """
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO journal_schemas (component, version)
            VALUES (?, ?)
            """,
            (_SCHEMA_COMPONENT, _SCHEMA_VERSION),
        )

    @staticmethod
    def _pending_count(connection: sqlite3.Connection) -> int:
        """Count pending rows without reading their serialized content."""
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM trigger_publications
            WHERE forwarding_state = ?
            """,
            (_PENDING_STATE,),
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError("Trigger publication count is absent")
        return int(row[0])
