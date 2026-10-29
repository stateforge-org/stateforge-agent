"""Define storage-neutral Agent journal contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from stateforge_agent._config.models import JournalConfiguration
    from stateforge_agent._sfp._services.trigger_publication.store import (
        TriggerPublicationStore,
    )


class JournalError(RuntimeError):
    """Report that durable Agent state could not complete an operation."""


class Journal(ABC):
    """Own durable Agent state without exposing a storage vendor."""

    backend: ClassVar[str]

    @classmethod
    @abstractmethod
    def from_configuration(
        cls,
        configuration: JournalConfiguration,
        *,
        state_directory: Path,
    ) -> Self:
        """Construct this backend from validated application settings."""

    @abstractmethod
    async def open(self) -> None:
        """Open the journal without replacing existing state."""

    @abstractmethod
    async def close(self) -> None:
        """Close the journal after current work has completed."""

    @abstractmethod
    def trigger_publications(
        self,
        *,
        maximum_pending_publications: int,
    ) -> TriggerPublicationStore:
        """Create the backend's durable Trigger Publication store."""
