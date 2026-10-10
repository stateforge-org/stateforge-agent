"""Contract tests for reusable RPM and installed-system automation."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTIONS = ROOT / ".github" / "actions"
SHA_LENGTH = 40
DEPENDENCY_COUNT = 2


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


def test_rpm_version_has_one_source_of_truth() -> None:
    """Derive RPM and Python distribution versions from pyproject.toml."""
    host_builder = read(ROOT / "packaging" / "rpm" / "build-rpm.sh")
    container_builder = read(
        ROOT / "packaging" / "rpm" / "build-inside-container.sh"
    )
    spec = read(ROOT / "packaging" / "rpm" / "SPECS" / "stateforge-agent.spec")
    automation = "\n".join(
        read(path)
        for path in (
            ACTIONS / "rpm" / "build" / "action.yml",
            WORKFLOWS / "rpm-build.yml",
            WORKFLOWS / "rpm-build-and-test.yml",
            WORKFLOWS / "rpm-release-candidate.yml",
        )
    )

    assert 'version="$(project_version)"' in container_builder
    assert '--define "version ${version}"' in container_builder
    assert "must be defined from pyproject.toml" in spec
    assert "--version" not in host_builder
    assert "inputs.version" not in automation


def test_installed_test_consumes_rpm_and_packaged_provider_wheels() -> None:
    """Exercise the built RPM against an independently packaged Provider."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    scenario = read(
        ACTIONS
        / "rpm"
        / "test-installed"
        / "smoke"
        / "manifest-exchange"
        / "action.yml"
    )
    setup = read(ACTIONS / "rpm" / "test-installed" / "setup" / "action.yml")
    install = read(ACTIONS / "agent" / "install" / "action.yml")
    fixture = read(ACTIONS / "provider" / "create-fixture" / "action.yml")
    fixture_source = read(
        ACTIONS / "provider" / "create-fixture" / "fixtures" / "manifest.py"
    )
    provider_start = read(ACTIONS / "provider" / "start" / "action.yml")

    assert "actions/download-artifact@" in workflow
    assert (
        "./.github/actions/rpm/test-installed/smoke/manifest-exchange"
        in workflow
    )
    assert "./.github/actions/agent/install" in setup
    assert "./.github/actions/agent/start" in setup
    assert "./.github/actions/provider/start" in scenario
    assert "./.github/actions/provider/assert-manifest" in scenario
    assert "rpm -Uvh --nodeps" in install
    assert "python3 -m pip wheel --no-deps" in fixture
    assert "stateforge-provider-python" in fixture
    assert "stateforge_provider-*.whl" in fixture
    assert "stateforge_provider-*.whl" in provider_start
    assert "from stateforge_provider import" in fixture_source
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


def test_dependency_refs_are_configurable_and_traceable() -> None:
    """Bind convenient requested refs to immutable tested artifacts."""
    release = read(WORKFLOWS / "rpm-release-candidate.yml")
    fixture = read(ACTIONS / "provider" / "create-fixture" / "action.yml")
    setup = read(ACTIONS / "rpm" / "test-installed" / "setup" / "action.yml")

    for ref in ("provider-ref", "protocol-ref"):
        assert f"{ref}:\n" in release
        assert f"{ref}: ${{{{ inputs.{ref} }}}}" in release
    assert release.count("default: mvp/implementation") == DEPENDENCY_COUNT
    assert (
        "git -C .stateforge-ci/sources/protocol rev-parse --verify HEAD"
        in fixture
    )
    assert (
        "git -C .stateforge-ci/sources/provider rev-parse --verify HEAD"
        in fixture
    )
    assert '"${OUTPUT_DIRECTORY}/dependency-provenance.json"' in fixture
    assert "requested_ref:" in fixture
    assert "resolved_commit:" in fixture
    assert "wheel_sha256:" in fixture
    assert "${{ steps.fixture.outputs.provenance-path }}" in setup
    assert "dependency-provenance.json" in setup


def test_installed_python_and_rpm_identity_must_agree() -> None:
    """Reject shadow imports and disagreement between package versions."""
    install = read(ACTIONS / "agent" / "install" / "action.yml")

    assert 'distribution("stateforge-agent")' in install
    assert "stateforge_agent.__file__" in install
    assert "package.files" not in install
    assert 'package.metadata["Name"]' in install
    assert "package.version" in install
    assert "python-package.json" in install
    assert "python-module-owner.txt" in install
    assert "rpm -qf --queryformat '%{NAME}' \"${module_file}\"" in install
    assert '"${module_owner}" != "stateforge-agent"' in install
    assert "version-consistency.txt" in install
    assert '"${cli_version}" != "${python_version}"' in install
    assert '"${cli_version}" != "${rpm_version}"' in install


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
        "rpm/test-installed/adversarial/invalid-descriptor",
        "rpm/test-installed/cleanup",
        "rpm/test-installed/destructive/uninstall-reinstall",
        "rpm/test-installed/lifecycle/multiple-providers",
        "rpm/test-installed/setup",
        "rpm/test-installed/smoke/manifest-exchange",
    }
    observed = {
        str(path.parent.relative_to(ACTIONS))
        for path in ACTIONS.glob("**/action.yml")
    }

    assert observed == expected


def test_installed_test_always_collects_and_cleans_up() -> None:
    """Preserve failure evidence and remove the persistent test container."""
    action = read(
        ACTIONS
        / "rpm"
        / "test-installed"
        / "smoke"
        / "manifest-exchange"
        / "action.yml"
    )
    cleanup = read(
        ACTIONS / "rpm" / "test-installed" / "cleanup" / "action.yml"
    )
    evidence = read(ACTIONS / "agent" / "collect-evidence" / "action.yml")

    assert "if: ${{ always() }}" in action
    assert "./.github/actions/agent/collect-evidence" in cleanup
    assert "journalctl -u stateforge-agent.service" in evidence
    assert "rpm -e stateforge-agent" in evidence
    assert 'docker rm --force "${CONTAINER_NAME}"' in evidence


def test_each_installed_scenario_is_an_independent_action() -> None:
    """Keep scenario behavior outside shared environment orchestration."""
    scenario_root = ACTIONS / "rpm" / "test-installed"
    scenarios = sorted(scenario_root.glob("*/*/action.yml"))

    tiers = {
        scenario.relative_to(scenario_root).parts[0] for scenario in scenarios
    }
    assert tiers == {"smoke", "lifecycle", "adversarial", "destructive"}
    for scenario in scenarios:
        content = read(scenario)
        assert "./.github/actions/rpm/test-installed/setup" in content
        assert "./.github/actions/rpm/test-installed/cleanup" in content
        assert "if: ${{ always() }}" in content
