"""Define the filesystem layout owned by the Agent package."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RuntimeLayout:
    """Describe platform-owned persistent and runtime directories."""

    state_directory: Path
    runtime_directory: Path

    @property
    def staging_directory(self) -> Path:
        """Return the SDK-managed operation staging directory."""
        return self.state_directory / "staging"

    @property
    def control_socket_path(self) -> Path:
        """Return the path of the local Agent control socket."""
        return self.runtime_directory / "control.sock"

    @property
    def provider_discovery_directory(self) -> Path:
        """Return the directory containing Provider endpoint descriptors."""
        return self.runtime_directory / "providers"


SYSTEM_RUNTIME_LAYOUT = RuntimeLayout(
    state_directory=Path("/var/lib/stateforge-agent"),
    runtime_directory=Path("/run/stateforge-agent"),
)
