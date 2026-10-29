"""Resolve configured journal backend names to implementation classes."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from stateforge_agent._config.models import JournalConfiguration
from stateforge_agent._journal.base import Journal
from stateforge_agent._journal.sqlite import SqliteJournal


class JournalBackendRegistry:
    """Map stable configuration names to journal implementation classes."""

    def __init__(self, journal_classes: Iterable[type[Journal]]) -> None:
        """Register journal classes and reject ambiguous backend names."""
        self._journal_classes: dict[str, type[Journal]] = {}
        for journal_class in journal_classes:
            if journal_class.backend in self._journal_classes:
                raise ValueError(
                    f"Duplicate journal backend: {journal_class.backend}"
                )
            self._journal_classes[journal_class.backend] = journal_class

    @property
    def backend_names(self) -> frozenset[str]:
        """Return the configured names accepted by this registry."""
        return frozenset(self._journal_classes)

    def create(
        self,
        configuration: JournalConfiguration,
        *,
        state_directory: Path,
    ) -> Journal:
        """Construct the journal class selected by configuration."""
        try:
            journal_class = self._journal_classes[configuration.backend]
        except KeyError as error:
            raise ValueError(
                f"Unsupported journal backend: {configuration.backend}"
            ) from error
        return journal_class.from_configuration(
            configuration,
            state_directory=state_directory,
        )


JOURNAL_BACKENDS = JournalBackendRegistry((SqliteJournal,))


def create_journal(
    configuration: JournalConfiguration,
    *,
    state_directory: Path,
) -> Journal:
    """Construct a journal through the application backend registry."""
    return JOURNAL_BACKENDS.create(
        configuration,
        state_directory=state_directory,
    )
