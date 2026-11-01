"""Persist Agent-owned Operation Admission intent in SQLite."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol, TypeVar
from uuid import UUID

from stateforge_protocol.sfp.operation_admission.v1 import (
    operation_admission_pb2,
)

from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionDisposition,
    OperationAdmissionResult,
    OperationRejection,
)
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAdmissionStore,
    OperationAttempt,
    OperationAttemptState,
    OperationIntentDecision,
    OperationIntentResult,
)

_SCHEMA_COMPONENT = "operation_admission"
_SCHEMA_VERSION = 1
_Result = TypeVar("_Result")


class _SqliteTransactions(Protocol):
    """Expose only the transaction boundary needed by this adapter."""

    async def transaction(
        self,
        operation: Callable[[sqlite3.Connection], _Result],
    ) -> _Result:
        """Run one serialized SQLite transaction."""


class SqliteOperationAdmissionStore(OperationAdmissionStore):
    """Own durable immutable operation intent and resolution state."""

    def __init__(
        self,
        journal: _SqliteTransactions,
        *,
        maximum_unresolved_attempts: int,
    ) -> None:
        """Configure one bounded operation-intent store."""
        if maximum_unresolved_attempts <= 0:
            raise ValueError("maximum_unresolved_attempts must be positive")
        self._journal = journal
        self._maximum_unresolved_attempts = maximum_unresolved_attempts

    async def initialize(self) -> None:
        """Create the first admission schema or verify its version."""
        await self._journal.transaction(self._initialize)

    async def create_intent(
        self,
        provider_instance_id: UUID,
        invocation: operation_admission_pb2.OperationInvocation,
        request_fingerprint: str,
    ) -> OperationIntentResult:
        """Atomically insert or classify one immutable invocation."""
        serialized = invocation.SerializeToString(deterministic=True)
        now = datetime.now(UTC).isoformat()

        def create(connection: sqlite3.Connection) -> OperationIntentResult:
            existing = self._select(
                connection,
                provider_instance_id,
                UUID(invocation.operation_id),
            )
            if existing is not None:
                decision = (
                    OperationIntentDecision.EXISTING
                    if existing.request_fingerprint == request_fingerprint
                    else OperationIntentDecision.CONFLICT
                )
                return OperationIntentResult(decision, existing)
            if self._unresolved_count(connection) >= (
                self._maximum_unresolved_attempts
            ):
                return OperationIntentResult(
                    OperationIntentDecision.CAPACITY_EXHAUSTED,
                    None,
                )
            connection.execute(
                """
                INSERT INTO operation_admission_attempts (
                    provider_instance_id,
                    operation_id,
                    invocation,
                    request_fingerprint,
                    state,
                    created_at,
                    updated_at,
                    attempt_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    str(provider_instance_id),
                    invocation.operation_id,
                    serialized,
                    request_fingerprint,
                    OperationAttemptState.UNRESOLVED.value,
                    now,
                    now,
                ),
            )
            inserted = self._select(
                connection,
                provider_instance_id,
                UUID(invocation.operation_id),
            )
            if inserted is None:
                raise sqlite3.DatabaseError("Operation intent was not stored")
            return OperationIntentResult(
                OperationIntentDecision.CREATED,
                inserted,
            )

        return await self._journal.transaction(create)

    async def get(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAttempt | None:
        """Return one durable attempt when known."""
        return await self._journal.transaction(
            lambda connection: self._select(
                connection,
                provider_instance_id,
                operation_id,
            )
        )

    async def record_transport_attempt(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
        *,
        endpoint_generation: int,
        session_id: str,
    ) -> OperationAttempt:
        """Record one exact session attempt and reopen retryable rejection."""
        now = datetime.now(UTC).isoformat()

        def record(connection: sqlite3.Connection) -> OperationAttempt:
            updated = connection.execute(
                """
                UPDATE operation_admission_attempts
                SET state = ?, updated_at = ?,
                    attempt_count = attempt_count + 1,
                    last_endpoint_generation = ?, last_session_id = ?,
                    rejection_reason = NULL, rejection_retryable = NULL,
                    rejection_retry_after_seconds = NULL,
                    rejection_explanation = NULL,
                    rejection_grpc_status = NULL
                WHERE provider_instance_id = ? AND operation_id = ?
                """,
                (
                    OperationAttemptState.UNRESOLVED.value,
                    now,
                    endpoint_generation,
                    session_id,
                    str(provider_instance_id),
                    str(operation_id),
                ),
            )
            if updated.rowcount != 1:
                raise sqlite3.DatabaseError("Operation intent is absent")
            attempt = self._select(
                connection,
                provider_instance_id,
                operation_id,
            )
            if attempt is None:
                raise sqlite3.DatabaseError("Operation intent is absent")
            return attempt

        return await self._journal.transaction(record)

    async def record_accepted(
        self,
        result: OperationAdmissionResult,
    ) -> OperationAttempt:
        """Resolve one durable attempt from a validated success response."""
        now = datetime.now(UTC).isoformat()

        def accept(connection: sqlite3.Connection) -> OperationAttempt:
            updated = connection.execute(
                """
                UPDATE operation_admission_attempts
                SET state = ?, updated_at = ?, accepted_disposition = ?,
                    accepted_endpoint_generation = ?,
                    accepted_session_id = ?, accepted_manifest_revision = ?,
                    admitted_at = ?, record_expires_at = ?,
                    admitted_endpoint_generation = ?
                WHERE provider_instance_id = ? AND operation_id = ?
                    AND request_fingerprint = ?
                """,
                (
                    OperationAttemptState.ACCEPTED.value,
                    now,
                    result.disposition.value,
                    result.endpoint_generation,
                    result.session_id,
                    result.manifest_revision,
                    result.admitted_at.isoformat(),
                    result.record_expires_at.isoformat(),
                    result.admitted_endpoint_generation,
                    str(result.provider_instance_id),
                    str(result.operation_id),
                    result.request_fingerprint,
                ),
            )
            if updated.rowcount != 1:
                raise sqlite3.DatabaseError(
                    "Operation success does not match durable intent"
                )
            attempt = self._select(
                connection,
                result.provider_instance_id,
                result.operation_id,
            )
            if attempt is None:
                raise sqlite3.DatabaseError("Operation intent is absent")
            return attempt

        return await self._journal.transaction(accept)

    async def record_rejected(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
        rejection: OperationRejection,
    ) -> OperationAttempt:
        """Record one canonical rejection without storing request content."""
        now = datetime.now(UTC).isoformat()
        retry_after = (
            rejection.retry_after.total_seconds()
            if rejection.retry_after is not None
            else None
        )

        def reject(connection: sqlite3.Connection) -> OperationAttempt:
            updated = connection.execute(
                """
                UPDATE operation_admission_attempts
                SET state = ?, updated_at = ?, rejection_reason = ?,
                    rejection_retryable = ?,
                    rejection_retry_after_seconds = ?,
                    rejection_explanation = ?, rejection_grpc_status = ?
                WHERE provider_instance_id = ? AND operation_id = ?
                """,
                (
                    OperationAttemptState.REJECTED.value,
                    now,
                    rejection.reason,
                    int(rejection.retryable),
                    retry_after,
                    rejection.explanation,
                    rejection.grpc_status,
                    str(provider_instance_id),
                    str(operation_id),
                ),
            )
            if updated.rowcount != 1:
                raise sqlite3.DatabaseError("Operation intent is absent")
            attempt = self._select(
                connection,
                provider_instance_id,
                operation_id,
            )
            if attempt is None:
                raise sqlite3.DatabaseError("Operation intent is absent")
            return attempt

        return await self._journal.transaction(reject)

    async def unresolved(self) -> tuple[OperationAttempt, ...]:
        """Return unresolved attempts without exposing serialized content."""

        def read(
            connection: sqlite3.Connection,
        ) -> tuple[OperationAttempt, ...]:
            rows = connection.execute(
                f"{self._SELECT} WHERE state = ? ORDER BY created_at",
                (OperationAttemptState.UNRESOLVED.value,),
            ).fetchall()
            return tuple(self._attempt_from_row(row) for row in rows)

        return await self._journal.transaction(read)

    _SELECT = """
        SELECT provider_instance_id, operation_id, invocation,
            request_fingerprint, state, created_at, updated_at,
            attempt_count, last_endpoint_generation, last_session_id,
            accepted_disposition, accepted_endpoint_generation,
            accepted_session_id, accepted_manifest_revision, admitted_at,
            record_expires_at, admitted_endpoint_generation,
            rejection_reason, rejection_retryable,
            rejection_retry_after_seconds, rejection_explanation,
            rejection_grpc_status
        FROM operation_admission_attempts
    """

    def _select(
        self,
        connection: sqlite3.Connection,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAttempt | None:
        query = (
            f"{self._SELECT} WHERE provider_instance_id = ? "
            "AND operation_id = ?"
        )
        row = connection.execute(
            query,
            (str(provider_instance_id), str(operation_id)),
        ).fetchone()
        return self._attempt_from_row(row) if row is not None else None

    @staticmethod
    def _attempt_from_row(row: tuple[object, ...]) -> OperationAttempt:
        invocation = operation_admission_pb2.OperationInvocation.FromString(
            _bytes(row[2])
        )
        state = OperationAttemptState(_string(row[4]))
        result = None
        if state is OperationAttemptState.ACCEPTED:
            result = OperationAdmissionResult(
                provider_instance_id=UUID(_string(row[0])),
                endpoint_generation=_integer(row[11]),
                session_id=_string(row[12]),
                manifest_revision=_string(row[13]),
                operation_id=UUID(_string(row[1])),
                request_fingerprint=_string(row[3]),
                disposition=OperationAdmissionDisposition(_string(row[10])),
                admitted_at=datetime.fromisoformat(_string(row[14])),
                record_expires_at=datetime.fromisoformat(_string(row[15])),
                admitted_endpoint_generation=_integer(row[16]),
            )
        rejection = None
        if state is OperationAttemptState.REJECTED:
            rejection = OperationRejection(
                operation_id=UUID(_string(row[1])),
                reason=_string(row[17]),
                retryable=bool(_integer(row[18])),
                retry_after=(
                    timedelta(seconds=_number(row[19]))
                    if row[19] is not None
                    else None
                ),
                explanation=_string(row[20]),
                grpc_status=_string(row[21]),
            )
        return OperationAttempt(
            provider_instance_id=UUID(_string(row[0])),
            operation_id=UUID(_string(row[1])),
            invocation=invocation,
            request_fingerprint=_string(row[3]),
            state=state,
            created_at=datetime.fromisoformat(_string(row[5])),
            updated_at=datetime.fromisoformat(_string(row[6])),
            attempt_count=_integer(row[7]),
            last_endpoint_generation=(
                _integer(row[8]) if row[8] is not None else None
            ),
            last_session_id=(_string(row[9]) if row[9] is not None else None),
            result=result,
            rejection=rejection,
        )

    @staticmethod
    def _unresolved_count(connection: sqlite3.Connection) -> int:
        row = connection.execute(
            """
            SELECT COUNT(*) FROM operation_admission_attempts
            WHERE state = ?
            """,
            (OperationAttemptState.UNRESOLVED.value,),
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError("Operation intent count is absent")
        return _integer(row[0])

    @staticmethod
    def _initialize(connection: sqlite3.Connection) -> None:
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
                "Unsupported Operation Admission journal schema"
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_admission_attempts (
                provider_instance_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                invocation BLOB NOT NULL,
                request_fingerprint TEXT NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN ('unresolved', 'accepted', 'rejected')
                ),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
                last_endpoint_generation INTEGER,
                last_session_id TEXT,
                accepted_disposition TEXT,
                accepted_endpoint_generation INTEGER,
                accepted_session_id TEXT,
                accepted_manifest_revision TEXT,
                admitted_at TEXT,
                record_expires_at TEXT,
                admitted_endpoint_generation INTEGER,
                rejection_reason TEXT,
                rejection_retryable INTEGER,
                rejection_retry_after_seconds REAL,
                rejection_explanation TEXT,
                rejection_grpc_status TEXT,
                PRIMARY KEY (provider_instance_id, operation_id)
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


def _string(value: object) -> str:
    if not isinstance(value, str):
        raise sqlite3.DatabaseError("Operation journal text is invalid")
    return value


def _integer(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise sqlite3.DatabaseError("Operation journal integer is invalid")
    return value


def _number(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise sqlite3.DatabaseError("Operation journal number is invalid")
    return float(value)


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise sqlite3.DatabaseError("Operation journal bytes are invalid")
    return value
