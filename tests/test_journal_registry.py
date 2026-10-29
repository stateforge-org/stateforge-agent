"""Tests for class-based journal backend selection."""

from pathlib import Path

import pytest

from stateforge_agent._config.models import JournalConfiguration
from stateforge_agent._journal import (
    JOURNAL_BACKENDS,
    Journal,
    JournalBackendRegistry,
    SqliteJournal,
    create_journal,
)
from stateforge_agent._sfp._services.trigger_publication.store import (
    TriggerPublicationStore,
)


class _AlternativeJournal(Journal):
    """Prove another backend can satisfy the class boundary."""

    backend = "alternative"

    def __init__(self, state_directory: Path) -> None:
        self.state_directory = state_directory

    @classmethod
    def from_configuration(
        cls,
        configuration: JournalConfiguration,
        *,
        state_directory: Path,
    ) -> "_AlternativeJournal":
        assert configuration.backend == cls.backend
        return cls(state_directory)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def trigger_publications(
        self,
        *,
        maximum_pending_publications: int,
    ) -> TriggerPublicationStore:
        del maximum_pending_publications
        raise NotImplementedError


def _configuration(backend: str = "sqlite") -> JournalConfiguration:
    return JournalConfiguration(
        backend=backend,
        maximum_pending_trigger_publications=10,
    )


def test_configured_sqlite_backend_resolves_to_its_journal_class(
    tmp_path: Path,
) -> None:
    journal = create_journal(
        _configuration(),
        state_directory=tmp_path,
    )

    assert isinstance(journal, SqliteJournal)
    assert journal.path == tmp_path / "journal" / "agent.db"


def test_registry_publishes_supported_backend_names() -> None:
    assert JOURNAL_BACKENDS.backend_names == frozenset({"sqlite"})


def test_registry_constructs_an_independent_backend_class(
    tmp_path: Path,
) -> None:
    registry = JournalBackendRegistry((_AlternativeJournal,))

    journal = registry.create(
        _configuration("alternative"),
        state_directory=tmp_path,
    )

    assert isinstance(journal, _AlternativeJournal)
    assert journal.state_directory == tmp_path


def test_registry_rejects_unknown_backend(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsupported journal backend"):
        create_journal(
            _configuration("unknown"),
            state_directory=tmp_path,
        )
