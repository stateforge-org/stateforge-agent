"""Verify application packages import from a fresh interpreter."""

import os
import subprocess
import sys
from pathlib import Path


def test_sfp_and_configuration_import_without_a_cycle() -> None:
    repository = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repository / "src")

    result = subprocess.run(
        (
            sys.executable,
            "-c",
            "from stateforge_agent._sfp import ProviderSessionManager; "
            "from stateforge_agent._config import load_configuration",
        ),
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
