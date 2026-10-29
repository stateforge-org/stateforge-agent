"""Tests for SQLite journal failure and rollback boundaries."""

import errno
import os
import sqlite3
import stat
from pathlib import Path

import pytest

from stateforge_agent._journal import SqliteJournal, SqliteJournalError
from stateforge_agent._journal import sqlite as sqlite_journal

_LARGE_PAYLOAD_BYTES = 1024 * 1024
_SHORT_BUSY_TIMEOUT_MILLISECONDS = 1
_OWNER_ONLY_DIRECTORY_MODE = 0o700
_OWNER_ONLY_FILE_MODE = 0o600


def _file_mode(path: Path) -> int:
    """Return only the permission bits for a filesystem path."""
    return stat.S_IMODE(path.stat().st_mode)


async def _open_journal(path: Path) -> SqliteJournal:
    journal = SqliteJournal(path)
    await journal.open()
    await journal.transaction(_create_payload_table)
    return journal


def _create_payload_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE payloads (identifier INTEGER PRIMARY KEY, value BLOB)"
    )


def _payload_count(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) FROM payloads").fetchone()
    if row is None:
        raise sqlite3.DatabaseError("Payload count is absent")
    return int(row[0])


@pytest.mark.asyncio
async def test_open_creates_owner_only_journal(tmp_path: Path) -> None:
    path = tmp_path / "journal" / "agent.db"
    journal = SqliteJournal(path)

    await journal.open()

    assert _file_mode(path.parent) == _OWNER_ONLY_DIRECTORY_MODE
    assert _file_mode(path) == _OWNER_ONLY_FILE_MODE
    assert await journal.transaction(
        lambda _: (
            _file_mode(Path(f"{path}-wal")),
            _file_mode(Path(f"{path}-shm")),
        )
    ) == (_OWNER_ONLY_FILE_MODE, _OWNER_ONLY_FILE_MODE)
    await journal.close()


@pytest.mark.asyncio
async def test_open_restricts_existing_permissive_journal(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.db"
    sqlite3.connect(path).close()
    path.chmod(0o640)
    journal = SqliteJournal(path)

    await journal.open()

    assert _file_mode(path) == _OWNER_ONLY_FILE_MODE
    await journal.close()


@pytest.mark.asyncio
async def test_disk_full_rolls_back_the_complete_transaction(
    tmp_path: Path,
) -> None:
    journal = await _open_journal(tmp_path / "agent.db")

    def insert_without_available_pages(
        connection: sqlite3.Connection,
    ) -> None:
        row = connection.execute("PRAGMA page_count").fetchone()
        if row is None:
            raise sqlite3.DatabaseError("Page count is absent")
        connection.execute(f"PRAGMA max_page_count = {int(row[0])}")
        connection.execute(
            "INSERT INTO payloads (value) VALUES (?)",
            (b"x" * _LARGE_PAYLOAD_BYTES,),
        )

    with pytest.raises(SqliteJournalError) as captured:
        await journal.transaction(insert_without_available_pages)

    assert isinstance(captured.value.__cause__, sqlite3.DatabaseError)
    assert await journal.transaction(_payload_count) == 0
    await journal.close()


@pytest.mark.asyncio
async def test_read_only_journal_rejects_writes_without_partial_state(
    tmp_path: Path,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root bypasses filesystem write permission checks")
    path = tmp_path / "journal" / "agent.db"
    journal = await _open_journal(path)

    def insert_payload(connection: sqlite3.Connection) -> None:
        connection.execute("INSERT INTO payloads (value) VALUES (X'01')")

    path.chmod(0o400)
    path.parent.chmod(0o500)
    try:
        with pytest.raises(SqliteJournalError) as captured:
            await journal.transaction(insert_payload)
        assert isinstance(captured.value.__cause__, sqlite3.OperationalError)
    finally:
        path.parent.chmod(0o750)
        path.chmod(_OWNER_ONLY_FILE_MODE)

    assert await journal.transaction(_payload_count) == 0
    await journal.close()


@pytest.mark.asyncio
async def test_busy_database_times_out_without_partial_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "agent.db"
    journal = await _open_journal(path)
    monkeypatch.setattr(
        sqlite_journal,
        "_BUSY_TIMEOUT_MILLISECONDS",
        _SHORT_BUSY_TIMEOUT_MILLISECONDS,
    )
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")

    def insert_payload(connection: sqlite3.Connection) -> None:
        connection.execute("INSERT INTO payloads (value) VALUES (X'02')")

    try:
        with pytest.raises(SqliteJournalError) as captured:
            await journal.transaction(insert_payload)
        assert isinstance(captured.value.__cause__, sqlite3.OperationalError)
        assert "locked" in str(captured.value.__cause__).lower()
    finally:
        blocker.rollback()
        blocker.close()

    assert await journal.transaction(_payload_count) == 0
    await journal.close()


@pytest.mark.asyncio
async def test_filesystem_io_error_is_wrapped_as_a_journal_failure(
    tmp_path: Path,
) -> None:
    journal = await _open_journal(tmp_path / "agent.db")
    injected_error = OSError(errno.EIO, "injected durable-write failure")

    def write_then_fail(connection: sqlite3.Connection) -> None:
        connection.execute("INSERT INTO payloads (value) VALUES (X'03')")
        raise injected_error

    with pytest.raises(SqliteJournalError) as captured:
        await journal.transaction(write_then_fail)

    assert captured.value.__cause__ is injected_error
    assert await journal.transaction(_payload_count) == 0
    await journal.close()


@pytest.mark.asyncio
async def test_corrupt_database_is_rejected_during_open(
    tmp_path: Path,
) -> None:
    path = tmp_path / "agent.db"
    path.write_bytes(b"not a SQLite database")
    journal = SqliteJournal(path)

    with pytest.raises(SqliteJournalError) as captured:
        await journal.open()

    assert isinstance(captured.value.__cause__, sqlite3.DatabaseError)
