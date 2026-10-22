"""Behavioral tests for the installed-CI Provider readiness boundary."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PROBE = (
    ROOT
    / ".github"
    / "actions"
    / "provider"
    / "start"
    / "resolve-ready-endpoint.sh"
)
PROVIDER_ID = "7f7db7d8-68b4-4f71-9769-065b5c4f941c"


@pytest.fixture
def endpoint_directory() -> Iterator[Path]:
    """Use a path short enough for Unix-domain sockets on every platform."""
    with tempfile.TemporaryDirectory(prefix="sf-ci-", dir="/tmp") as path:
        yield Path(path)


def _environment(tmp_path: Path, *, active: bool = True) -> dict[str, str]:
    """Install a deterministic systemctl boundary for the shell probe."""
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir(exist_ok=True)
    systemctl = binary_directory / "systemctl"
    systemctl.write_text(
        "#!/usr/bin/env bash\n"
        '[[ "${1:-}" == is-active && "${FAKE_ACTIVE:-0}" == 1 ]]\n',
        encoding="utf-8",
    )
    systemctl.chmod(0o755)
    return {
        **os.environ,
        "FAKE_ACTIVE": "1" if active else "0",
        "PATH": f"{binary_directory}:{os.environ['PATH']}",
    }


def _write_endpoint(
    directory: Path,
    *,
    identity_generation: int,
    descriptor_generation: int,
    socket_is_valid: bool = True,
) -> tuple[Path, Path]:
    """Publish independently controlled identity and descriptor snapshots."""
    identity = directory / "identity.json"
    identity.write_text(
        json.dumps(
            {
                "provider_instance_id": PROVIDER_ID,
                "last_endpoint_generation": identity_generation,
            }
        ),
        encoding="utf-8",
    )

    socket_path = directory / f"{PROVIDER_ID}.{descriptor_generation}.sock"
    if socket_is_valid:
        provider_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            provider_socket.bind(str(socket_path))
        finally:
            provider_socket.close()
    else:
        socket_path.touch()

    descriptor = directory / f"{PROVIDER_ID}.sfp.json"
    descriptor.write_text(
        json.dumps(
            {
                "schema_version": "1",
                "provider_instance_id": PROVIDER_ID,
                "endpoint_generation": descriptor_generation,
                "transport": {
                    "type": "unix",
                    "socket_path": str(socket_path),
                },
            }
        ),
        encoding="utf-8",
    )
    return identity, socket_path


def _probe(
    identity: Path,
    directory: Path,
    previous_generation: int,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run the same readiness probe copied into the system-test container."""
    return subprocess.run(  # noqa: S603 - fixed repository-owned command
        (
            "/bin/bash",
            str(PROBE),
            str(identity),
            str(directory),
            "stateforge-test-provider-a.service",
            str(previous_generation),
        ),
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )


def test_probe_rejects_mixed_identity_and_descriptor_generations(
    endpoint_directory: Path,
) -> None:
    """Never combine the prior identity with a replacement descriptor."""
    identity, _ = _write_endpoint(
        endpoint_directory,
        identity_generation=1,
        descriptor_generation=2,
    )

    result = _probe(
        identity,
        endpoint_directory,
        0,
        _environment(endpoint_directory),
    )

    assert result.returncode != 0
    assert result.stdout == ""


def test_probe_requires_a_new_live_coherent_generation(
    endpoint_directory: Path,
) -> None:
    """Accept only a live Unix endpoint newer than the captured baseline."""
    identity, socket_path = _write_endpoint(
        endpoint_directory,
        identity_generation=2,
        descriptor_generation=2,
    )
    environment = _environment(endpoint_directory)

    result = _probe(identity, endpoint_directory, 1, environment)

    assert result.returncode == 0
    assert result.stdout.strip().split("\t") == [
        PROVIDER_ID,
        "2",
        str(socket_path),
    ]
    assert _probe(identity, endpoint_directory, 2, environment).returncode != 0


def test_probe_rejects_non_socket_and_inactive_unit(
    endpoint_directory: Path,
) -> None:
    """Descriptor publication alone is not Provider readiness."""
    identity, _ = _write_endpoint(
        endpoint_directory,
        identity_generation=1,
        descriptor_generation=1,
        socket_is_valid=False,
    )
    assert (
        _probe(
            identity,
            endpoint_directory,
            0,
            _environment(endpoint_directory),
        ).returncode
        != 0
    )

    for path in endpoint_directory.glob("*.sock"):
        path.unlink()
    identity, _ = _write_endpoint(
        endpoint_directory,
        identity_generation=1,
        descriptor_generation=1,
    )
    assert (
        _probe(
            identity,
            endpoint_directory,
            0,
            _environment(endpoint_directory, active=False),
        ).returncode
        != 0
    )
