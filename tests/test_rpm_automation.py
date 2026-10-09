"""Contract tests for reusable RPM and installed-system automation."""

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
    build_action = read(ACTIONS / "rpm" / "build" / "action.yml")
    build_workflow = read(WORKFLOWS / "rpm-build.yml")
    builder = read(ROOT / "packaging" / "rpm" / "build-inside-container.sh")

    assert "packaging/rpm/build-rpm.sh" in build_action
    assert "./.github/actions/rpm/build" in build_workflow
    assert "rpmbuild" in builder
    assert "rpmbuild" not in build_action
    assert "rpmbuild" not in build_workflow


def test_installed_test_consumes_rpm_and_packaged_provider_wheels() -> None:
    """Exercise the built RPM against an independently packaged Provider."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    action = read(ACTIONS / "rpm" / "test-installed" / "action.yml")
    install = read(ACTIONS / "agent" / "install" / "action.yml")
    fixture = read(ACTIONS / "provider" / "create-fixture" / "action.yml")

    assert "actions/download-artifact@" in workflow
    assert "./.github/actions/agent/install" in action
    assert "./.github/actions/agent/start" in action
    assert "./.github/actions/provider/start" in action
    assert "./.github/actions/provider/assert-manifest" in action
    assert "rpm -Uvh --nodeps" in install
    assert "python3 -m pip wheel --no-deps" in fixture
    assert "stateforge-provider-implemntation" in fixture
    assert "token: ${{ inputs.repository-token }}" in fixture


def test_private_sources_use_a_short_lived_github_app_token() -> None:
    """Avoid long-lived PATs when building unpublished dependency wheels."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")

    assert "actions/create-github-app-token@" in workflow
    assert "source_app_id:" in workflow
    assert "source_private_key:" in workflow
    assert (
        "repository-token: ${{ steps.source-token.outputs.token }}" in workflow
    )


def test_triggered_workflows_are_thin_callers() -> None:
    """Triggered workflows should delegate to the shared RPM pipeline."""
    for filename in ("rpm-ci.yml", "rpm-release-candidate.yml"):
        workflow = read(WORKFLOWS / filename)
        assert "uses: ./.github/workflows/rpm-build-and-test.yml" in workflow
        assert "run:" not in workflow


def test_external_actions_are_sha_pinned() -> None:
    """Prevent mutable action tags in executable workflows."""
    automation_files = [
        *WORKFLOWS.glob("*.yml"),
        *ACTIONS.glob("**/action.yml"),
    ]
    for path in automation_files:
        for line in read(path).splitlines():
            stripped = line.strip()
            if not stripped.startswith("uses: actions/"):
                continue
            reference = stripped.split("@", maxsplit=1)[1].split()[0]
            assert len(reference) == SHA_LENGTH
            assert all(
                character in "0123456789abcdef" for character in reference
            )


def test_shared_pipeline_passes_one_artifact_from_build_to_test() -> None:
    """The installed test must receive the build job's artifact identity."""
    workflow = read(WORKFLOWS / "rpm-build-and-test.yml")

    assert "needs: build" in workflow
    assert (
        "artifact-name: ${{ needs.build.outputs.artifact-name }}" in workflow
    )


def test_action_library_has_expected_domain_boundaries() -> None:
    """Keep reusable behavior organized by domain rather than filename tags."""
    expected = {
        "agent/collect-evidence",
        "agent/install",
        "agent/start",
        "provider/assert-manifest",
        "provider/create-fixture",
        "provider/start",
        "rpm/build",
        "rpm/test-installed",
    }
    observed = {
        str(path.parent.relative_to(ACTIONS))
        for path in ACTIONS.glob("**/action.yml")
    }

    assert observed == expected


def test_installed_test_always_collects_and_cleans_up() -> None:
    """Preserve failure evidence and remove the persistent test container."""
    action = read(ACTIONS / "rpm" / "test-installed" / "action.yml")
    evidence = read(ACTIONS / "agent" / "collect-evidence" / "action.yml")

    assert "if: ${{ always() }}" in action
    assert "journalctl -u stateforge-agent.service" in evidence
    assert "rpm -e stateforge-agent" in evidence
    assert 'docker rm --force "${CONTAINER_NAME}"' in evidence
