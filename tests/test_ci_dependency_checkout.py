"""Behavioral tests for immutable dependency checkout verification."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERIFY_CHECKOUT = (
    ROOT / ".github" / "actions" / "dependencies" / "verify-checkout.sh"
)
GIT = "/usr/bin/git"


def _git(
    repository: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """Run a fixed Git command against the temporary repository."""
    return subprocess.run(  # noqa: S603 - fixed test-owned command
        (GIT, "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )


def _verify(
    repository: Path,
    expected_commit: str,
) -> subprocess.CompletedProcess[str]:
    """Run the production dependency checkout verifier."""
    return subprocess.run(  # noqa: S603 - fixed repository-owned command
        ("/bin/bash", str(VERIFY_CHECKOUT), str(repository), expected_commit),
        check=False,
        capture_output=True,
        text=True,
    )


def _commit(repository: Path, contents: str, message: str) -> str:
    """Create one commit without relying on the host Git identity."""
    (repository / "dependency.txt").write_text(contents, encoding="utf-8")
    _git(repository, "add", "dependency.txt")
    _git(
        repository,
        "-c",
        "user.name=StateForge CI",
        "-c",
        "user.email=ci@stateforge.net",
        "commit",
        "-m",
        message,
    )
    return _git(repository, "rev-parse", "HEAD").stdout.strip()


def test_selected_dependency_commit_survives_moving_branch(
    tmp_path: Path,
) -> None:
    """Keep consumers on the selected SHA after the requested branch moves."""
    repository = tmp_path / "dependency"
    repository.mkdir()
    _git(repository, "init", "--initial-branch=mvp/implementation")
    selected_commit = _commit(repository, "selected\n", "Selected dependency")
    branch = _git(repository, "branch", "--show-current").stdout.strip()

    moved_commit = _commit(repository, "moved\n", "Move dependency branch")
    assert moved_commit != selected_commit

    _git(repository, "checkout", "--detach", selected_commit)
    selected = _verify(repository, selected_commit)

    assert selected.returncode == 0
    assert selected.stdout.strip() == selected_commit

    _git(repository, "checkout", branch)
    mismatch = _verify(repository, selected_commit)

    assert mismatch.returncode == 1
    expected_error = f"expected {selected_commit}, found {moved_commit}"
    assert expected_error in mismatch.stderr
