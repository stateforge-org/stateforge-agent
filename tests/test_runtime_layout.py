"""Tests for the package-owned Agent filesystem layout."""

from pathlib import Path

from stateforge_agent._runtime.layout import RuntimeLayout


def test_derives_agent_owned_paths_from_platform_directories() -> None:
    layout = RuntimeLayout(
        state_directory=Path("/var/lib/stateforge-agent"),
        runtime_directory=Path("/run/stateforge-agent"),
    )

    assert layout.staging_directory == Path(
        "/var/lib/stateforge-agent/staging"
    )
    assert layout.control_socket_path == Path(
        "/run/stateforge-agent/control.sock"
    )
    assert layout.provider_discovery_directory == Path(
        "/run/stateforge-agent/providers"
    )
