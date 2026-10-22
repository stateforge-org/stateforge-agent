"""Behavioral tests for installed-runtime evidence correlation."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEXER = (
    ROOT / ".github/actions/agent/collect-evidence/build-invocation-index.sh"
)
AGENT_INVOCATION = "1" * 32
PROVIDER_INVOCATION = "2" * 32


def _entry(
    unit: str,
    invocation_id: str,
    timestamp: str,
    cursor: str,
    pid: str,
) -> str:
    return json.dumps(
        {
            "_SYSTEMD_UNIT": unit,
            "_SYSTEMD_INVOCATION_ID": invocation_id,
            "__REALTIME_TIMESTAMP": timestamp,
            "__CURSOR": cursor,
            "_PID": pid,
        }
    )


def test_invocation_index_preserves_agent_and_stopped_provider(
    tmp_path: Path,
) -> None:
    """Retain historical Provider invocation identity after unit teardown."""
    agent_journal = tmp_path / "agent.jsonl"
    provider_journal = tmp_path / "provider.jsonl"
    agent_journal.write_text(
        _entry(
            "stateforge-agent.service",
            AGENT_INVOCATION,
            "100",
            "agent-cursor",
            "10",
        )
        + "\n",
        encoding="utf-8",
    )
    provider_journal.write_text(
        "\n".join(
            (
                _entry(
                    "stateforge-test-provider-a.service",
                    PROVIDER_INVOCATION,
                    "200",
                    "provider-first",
                    "20",
                ),
                _entry(
                    "stateforge-test-provider-a.service",
                    PROVIDER_INVOCATION,
                    "300",
                    "provider-last",
                    "20",
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    result = subprocess.run(  # noqa: S603 - fixed repository-owned command
        (
            "/bin/bash",
            str(INDEXER),
            str(agent_journal),
            str(provider_journal),
        ),
        check=True,
        capture_output=True,
        text=True,
    )
    evidence = json.loads(result.stdout)

    assert evidence["schema_version"] == 1
    assert evidence["invocations"] == [
        {
            "unit": "stateforge-agent.service",
            "invocation_id": AGENT_INVOCATION,
            "pids": ["10"],
            "first_realtime_timestamp": "100",
            "last_realtime_timestamp": "100",
            "first_cursor": "agent-cursor",
            "last_cursor": "agent-cursor",
        },
        {
            "unit": "stateforge-test-provider-a.service",
            "invocation_id": PROVIDER_INVOCATION,
            "pids": ["20"],
            "first_realtime_timestamp": "200",
            "last_realtime_timestamp": "300",
            "first_cursor": "provider-first",
            "last_cursor": "provider-last",
        },
    ]
