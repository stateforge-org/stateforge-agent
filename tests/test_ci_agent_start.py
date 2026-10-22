"""Behavioral tests for invocation-scoped Agent startup readiness."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = (
    ROOT
    / ".github"
    / "actions"
    / "agent"
    / "start"
    / "check-ready-invocation.sh"
)
CURRENT_INVOCATION = "1" * 32
HISTORICAL_INVOCATION = "2" * 32
PENDING_STATUS = 3
TERMINAL_STATUS = 4


def _entry(invocation_id: str, message: str) -> str:
    return json.dumps(
        {
            "_SYSTEMD_INVOCATION_ID": invocation_id,
            "MESSAGE": message,
        }
    )


def _check(
    journal: str,
    *,
    current_invocation: str = CURRENT_INVOCATION,
    state: str = "active",
) -> subprocess.CompletedProcess[str]:
    """Classify startup evidence through the production helper."""
    return subprocess.run(  # noqa: S603 - fixed repository-owned command
        (
            "/bin/bash",
            str(CHECKER),
            CURRENT_INVOCATION,
            current_invocation,
            state,
        ),
        check=False,
        capture_output=True,
        input=journal,
        text=True,
    )


def test_historical_startup_log_cannot_satisfy_current_invocation() -> None:
    """Ignore a readiness message emitted by an older service process."""
    result = _check(_entry(HISTORICAL_INVOCATION, "StateForge Agent started"))

    assert result.returncode == PENDING_STATUS
    assert result.stdout == ""


def test_current_invocation_must_emit_its_own_startup_log() -> None:
    """Accept readiness only from the invocation that remains active."""
    expected_message = "INFO stateforge: StateForge Agent started"
    result = _check(
        "\n".join(
            (
                _entry(HISTORICAL_INVOCATION, "StateForge Agent started"),
                _entry(CURRENT_INVOCATION, expected_message),
            )
        )
    )

    assert result.returncode == 0
    assert result.stdout.strip() == expected_message


def test_replaced_invocation_fails_instead_of_accepting_its_log() -> None:
    """Fail when systemd has already replaced the invocation under test."""
    result = _check(
        _entry(CURRENT_INVOCATION, "StateForge Agent started"),
        current_invocation=HISTORICAL_INVOCATION,
    )

    assert result.returncode == TERMINAL_STATUS
    assert result.stdout == ""
