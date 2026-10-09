"""Contract tests for reusable RPM automation."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTIONS = ROOT / ".github" / "actions"
SHA_LENGTH = 40


def read(path: Path) -> str:
    """Read one automation file as UTF-8 text."""
    return path.read_text(encoding="utf-8")


def test_rpm_build_has_one_authoritative_command() -> None:
    """Keep RPM construction in one locally runnable script."""
    build_action = read(ACTIONS / "build-rpm" / "action.yml")
    build_workflow = read(WORKFLOWS / "reusable-rpm-build.yml")
    builder = read(ROOT / "packaging" / "rpm" / "build-inside-container.sh")

    assert "packaging/rpm/build-rpm.sh" in build_action
    assert "./.github/actions/build-rpm" in build_workflow
    assert "rpmbuild" in builder
    assert "rpmbuild" not in build_action
    assert "rpmbuild" not in build_workflow


def test_installed_test_consumes_uploaded_rpm() -> None:
    """The test job must download, verify, and install the built artifact."""
    workflow = read(WORKFLOWS / "reusable-rpm-test.yml")
    action = read(ACTIONS / "test-installed-rpm" / "action.yml")
    installed_test = read(
        ROOT / "packaging" / "rpm" / "test-installed-rpm-inside-container.sh"
    )

    assert "actions/download-artifact@" in workflow
    assert "sha256sum --check SHA256SUMS" in action
    assert "test-installed-rpm.sh" in action
    assert 'rpm -Uvh --nodeps "${RPM_PATH}"' in installed_test
    assert "systemctl cat stateforge-agent.service" in installed_test
    assert "systemctl start stateforge-agent.service" not in installed_test
    assert "rpm -e stateforge-agent" in installed_test


def test_triggered_workflows_are_thin_callers() -> None:
    """Triggered workflows should delegate to the reusable gate."""
    for filename in ("rpm-ci.yml", "rpm-release-candidate.yml"):
        workflow = read(WORKFLOWS / filename)
        assert "uses: ./.github/workflows/reusable-rpm-gate.yml" in workflow
        assert "run:" not in workflow


def test_external_actions_are_sha_pinned() -> None:
    """Prevent mutable action tags in executable workflows."""
    for path in WORKFLOWS.glob("*.yml"):
        for line in read(path).splitlines():
            stripped = line.strip()
            if not stripped.startswith("uses: actions/"):
                continue
            reference = stripped.split("@", maxsplit=1)[1].split()[0]
            assert len(reference) == SHA_LENGTH
            assert all(
                character in "0123456789abcdef" for character in reference
            )


def test_reusable_gate_passes_one_artifact_from_build_to_test() -> None:
    """The installed test must receive the build job's artifact identity."""
    workflow = read(WORKFLOWS / "reusable-rpm-gate.yml")

    assert "needs: build" in workflow
    assert (
        "artifact-name: ${{ needs.build.outputs.artifact-name }}" in workflow
    )
