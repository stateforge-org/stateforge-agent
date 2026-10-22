"""Behavioral tests for generation-scoped manifest journal assertions."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = (
    ROOT
    / ".github"
    / "actions"
    / "provider"
    / "assert-manifest"
    / "check-journal.sh"
)
PROVIDER_ID = "7f7db7d8-68b4-4f71-9769-065b5c4f941c"
PENDING_STATUS = 3


def _check(journal: str, generation: int) -> subprocess.CompletedProcess[str]:
    """Classify journal entries through the production assertion helper."""
    return subprocess.run(  # noqa: S603 - fixed repository-owned command
        ("/bin/bash", str(CHECKER), PROVIDER_ID, str(generation)),
        check=False,
        capture_output=True,
        input=journal,
        text=True,
    )


def _accepted(generation: int) -> str:
    return (
        "SFP session established: event=created "
        f"provider_instance_id={PROVIDER_ID} "
        f"endpoint_generation={generation} sfp_version=0.1.0"
    )


def _rejected(generation: int) -> str:
    return (
        "Rejecting Provider manifest: "
        f"provider_instance_id={PROVIDER_ID} "
        f"endpoint_generation={generation} error=invalid"
    )


def test_manifest_checker_ignores_historical_generation_rejection() -> None:
    """An earlier generation rejection cannot poison a replacement."""
    current = _accepted(2)

    result = _check(f"{_rejected(1)}\n{current}\n", 2)

    assert result.returncode == 0
    assert result.stdout.strip() == current


def test_manifest_checker_ignores_historical_generation_acceptance() -> None:
    """An earlier successful session cannot satisfy a replacement."""
    current = _rejected(2)

    result = _check(f"{_accepted(1)}\n{current}\n", 2)

    assert result.returncode == 1
    assert result.stdout.strip() == current


def test_manifest_checker_reports_pending_without_exact_generation() -> None:
    """Unrelated sessions leave the current assertion waiting."""
    result = _check(f"{_accepted(1)}\n", 2)

    assert result.returncode == PENDING_STATUS
    assert result.stdout == ""
