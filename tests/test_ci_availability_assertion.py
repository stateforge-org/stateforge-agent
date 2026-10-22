"""Behavioral tests for transition-scoped availability assertions."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = (
    ROOT
    / ".github"
    / "actions"
    / "agent"
    / "assert-availability"
    / "check-journal.sh"
)
PROVIDER_ID = "7f7db7d8-68b4-4f71-9769-065b5c4f941c"
PENDING_STATUS = 3


def _transition(generation: int, state: str) -> str:
    return (
        "Provider availability changed: "
        f"provider_instance_id={PROVIDER_ID} "
        f"endpoint_generation={generation} state={state} "
        f"eligible={state == 'available'} reason=test"
    )


def _check(
    journal: str,
    generation: int,
    state: str,
) -> subprocess.CompletedProcess[str]:
    """Classify journal entries through the production assertion helper."""
    return subprocess.run(  # noqa: S603 - fixed repository-owned command
        ("/bin/bash", str(CHECKER), PROVIDER_ID, str(generation), state),
        check=False,
        capture_output=True,
        input=journal,
        text=True,
    )


def test_availability_checker_requires_exact_generation_and_state() -> None:
    """Neither an earlier generation nor a different state can satisfy it."""
    current = _transition(2, "available")
    journal = "\n".join(
        (
            _transition(1, "available"),
            _transition(2, "unavailable"),
            current,
        )
    )

    result = _check(journal, 2, "available")

    assert result.returncode == 0
    assert result.stdout.strip() == current


def test_availability_checker_does_not_confuse_state_prefixes() -> None:
    """The `available` state must not match `unavailable`."""
    result = _check(_transition(2, "unavailable"), 2, "available")

    assert result.returncode == PENDING_STATUS
    assert result.stdout == ""


def test_availability_checker_reports_pending_without_transition() -> None:
    """Unrelated journal entries leave the current assertion waiting."""
    result = _check("StateForge Agent started\n", 2, "available")

    assert result.returncode == PENDING_STATUS
    assert result.stdout == ""
