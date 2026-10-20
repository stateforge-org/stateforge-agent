"""Contract tests for reusable RPM and installed-system automation."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTIONS = ROOT / ".github" / "actions"
SHA_LENGTH = 40
DEPENDENCY_COUNT = 2
HEALTH_SCENARIO_PROVIDER_COUNT = 3


def read(path: Path) -> str:
    """Read one automation file as UTF-8 text."""
    return path.read_text(encoding="utf-8")


def test_rpm_build_has_one_authoritative_command() -> None:
    """Keep RPM construction in one locally runnable script."""
    build_action = read(ACTIONS / "rpm" / "build" / "action.yml")
    build_workflow = read(WORKFLOWS / "rpm-build.yml")
    builder = read(ROOT / "packaging" / "rpm" / "build-inside-container.sh")

    assert "packaging/rpm/build-rpm.sh" in build_action
    assert ".stateforge-ci/agent/.github/actions/rpm/build" in build_workflow
    assert "rpmbuild" in builder
    assert "rpmbuild" not in build_action
    assert "rpmbuild" not in build_workflow


def test_ci_runs_complete_source_validation() -> None:
    """Keep source correctness gates required beside installed RPM tests."""
    pipeline = read(WORKFLOWS / "rpm-build-and-test.yml")
    validation = read(WORKFLOWS / "source-validation.yml")

    assert "uses: ./.github/workflows/source-validation.yml" in pipeline
    assert "python -m ruff format --check ." in validation
    assert "python -m ruff check ." in validation
    assert "python -m mypy src" in validation
    assert "python -m pytest" in validation
    assert "python -m mkdocs build --strict" in validation
    assert (
        ".stateforge-ci/sources/protocol/.github/actions/python/build"
        in validation
    )


def test_rpm_builder_is_location_independent() -> None:
    """Build the Agent checkout that owns the invoked action."""
    action = read(ACTIONS / "rpm" / "build" / "action.yml")
    workflow = read(WORKFLOWS / "rpm-build.yml")
    host_builder = read(ROOT / "packaging" / "rpm" / "build-rpm.sh")
    container_builder = read(
        ROOT / "packaging" / "rpm" / "build-inside-container.sh"
    )

    assert "ACTION_PATH: ${{ github.action_path }}" in action
    assert "${ACTION_PATH}/../../../.." in action
    assert '"${source_directory}/packaging/rpm/build-rpm.sh"' in action
    assert 'output_directory="$(mkdir -p' in action
    assert "path: .stateforge-ci/agent" in workflow
    assert 'source_sha="$(git -C "${SOURCE_ROOT}"' in host_builder
    assert '"commit": os.environ["SOURCE_SHA"]' in container_builder
    assert '"ref": os.environ["SOURCE_REF"]' in container_builder


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
    assert (
        "./.stateforge-ci/sources/protocol/.github/actions/python/build"
        in fixture
    )
    assert (
        "./.stateforge-ci/sources/provider/.github/actions/package/build"
        in fixture
    )
    assert "pip wheel" not in fixture
    assert "stateforge-provider-python" in fixture
    assert (
        "PROVIDER_WHEEL: ${{ steps.provider.outputs.wheel-path }}" in fixture
    )
    assert "stateforge_provider-*.whl" in provider_start
    assert "from stateforge_provider import" in fixture_source
    assert "token: ${{ inputs.repository-token }}" in fixture


def test_private_sources_use_a_short_lived_github_app_token() -> None:
    """Keep source credentials in the one dependency-build job."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    scenario_actions = "\n".join(
        read(path)
        for path in (
            ACTIONS
            / "rpm"
            / "test-installed"
            / "smoke"
            / "manifest-exchange"
            / "action.yml",
            ACTIONS
            / "rpm"
            / "test-installed"
            / "lifecycle"
            / "multiple-providers"
            / "action.yml",
            ACTIONS
            / "rpm"
            / "test-installed"
            / "health"
            / "provider-availability"
            / "action.yml",
            ACTIONS
            / "rpm"
            / "test-installed"
            / "adversarial"
            / "invalid-descriptor"
            / "action.yml",
            ACTIONS
            / "rpm"
            / "test-installed"
            / "destructive"
            / "uninstall-reinstall"
            / "action.yml",
        )
    )

    assert "actions/create-github-app-token@" in workflow
    assert "source_app_id:" in workflow
    assert "source_private_key:" in workflow
    assert "permission-contents: read" in workflow
    assert (
        "repository-token: ${{ steps.source-token.outputs.token }}" in workflow
    )
    assert workflow.count("actions/create-github-app-token@") == 1
    assert "repository-token:" not in scenario_actions


def test_dependency_refs_are_configurable_and_traceable() -> None:
    """Bind convenient requested refs to immutable tested artifacts."""
    release = read(WORKFLOWS / "rpm-release-candidate.yml")
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
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
    assert "provider-ref: ${{ inputs.provider-ref }}" in workflow
    assert "protocol-ref: ${{ inputs.protocol-ref }}" in workflow


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
    """Transfer exact Agent and fixture artifacts to installed scenarios."""
    workflow = read(WORKFLOWS / "rpm-build-and-test.yml")
    installed = read(WORKFLOWS / "rpm-installed-test.yml")

    assert "needs: build" in workflow
    assert (
        "artifact-name: ${{ needs.build.outputs.artifact-name }}" in workflow
    )
    assert "fixture-artifact-name:" in workflow
    assert "needs: fixture" in installed
    assert "name: ${{ inputs.fixture-artifact-name }}" in installed


def test_dependency_packages_are_built_once_and_reused() -> None:
    """Do not rebuild Provider or Protocol inside each scenario."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    setup = read(ACTIONS / "rpm" / "test-installed" / "setup" / "action.yml")
    fixture = read(ACTIONS / "provider" / "create-fixture" / "action.yml")

    assert "fixture:\n    name: Build Provider fixture bundle" in workflow
    assert workflow.count("./.github/actions/provider/create-fixture") == 1
    assert "Upload immutable Provider fixture bundle" in workflow
    assert "Download exact Provider fixture bundle" in workflow
    assert "fixture-directory: dist/provider-fixture" in workflow
    assert "./.github/actions/provider/create-fixture" not in setup
    assert "sha256sum --check SHA256SUMS" in setup
    assert "sha256sum --check SHA256SUMS" in fixture


def test_action_library_has_expected_domain_boundaries() -> None:
    """Keep reusable behavior organized by domain rather than filename tags."""
    expected = {
        "agent/assert-availability",
        "agent/assert-capability-assessment",
        "agent/collect-evidence",
        "agent/install",
        "agent/start",
        "provider/assert-manifest",
        "provider/create-fixture",
        "provider/set-health",
        "provider/start",
        "provider/stop",
        "rpm/build",
        "rpm/test-installed/adversarial/invalid-descriptor",
        "rpm/test-installed/capability/assessment",
        "rpm/test-installed/cleanup",
        "rpm/test-installed/destructive/uninstall-reinstall",
        "rpm/test-installed/health/provider-availability",
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
    assert tiers == {
        "smoke",
        "lifecycle",
        "health",
        "adversarial",
        "capability",
        "destructive",
    }
    for scenario in scenarios:
        content = read(scenario)
        assert "./.github/actions/rpm/test-installed/setup" in content
        assert "./.github/actions/rpm/test-installed/cleanup" in content
        assert "if: ${{ always() }}" in content


def test_installed_health_scenario_uses_real_artifacts_and_processes() -> None:
    """Keep A2 evidence on the RPM, Provider wheel, and shared SFP channel."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    scenario = read(
        ACTIONS
        / "rpm"
        / "test-installed"
        / "health"
        / "provider-availability"
        / "action.yml"
    )
    fixture = read(
        ACTIONS / "provider" / "create-fixture" / "fixtures" / "manifest.py"
    )
    assertion = read(ACTIONS / "agent" / "assert-availability" / "action.yml")

    assert "provider-availability" in workflow
    assert "./.github/actions/rpm/test-installed/setup" in scenario
    assert (
        scenario.count("./.github/actions/provider/start")
        == HEALTH_SCENARIO_PROVIDER_COUNT
    )
    assert "provider-state-name: provider-a" in scenario
    assert "./.github/actions/provider/set-health" in scenario
    assert "./.github/actions/agent/assert-availability" in scenario
    assert "HealthStatus" in fixture
    assert "refresh_health" in fixture
    assert "Provider availability changed:" in assertion
    assert "endpoint_generation=${ENDPOINT_GENERATION}" in assertion


def test_installed_capability_scenario_uses_real_artifacts_and_processes() -> (
    None
):
    """Keep A3 evidence on exact packages and independent SFP processes."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    scenario = read(
        ACTIONS
        / "rpm"
        / "test-installed"
        / "capability"
        / "assessment"
        / "action.yml"
    )
    assertion_action = read(
        ACTIONS / "agent" / "assert-capability-assessment" / "action.yml"
    )
    assertion = read(
        ACTIONS / "agent" / "assert-capability-assessment" / "assert.py"
    )
    fixture = read(
        ACTIONS / "provider" / "create-fixture" / "fixtures" / "manifest.py"
    )

    assert "capability-assessment" in workflow
    assert "./.github/actions/rpm/test-installed/setup" in scenario
    assert "./.github/actions/provider/start" in scenario
    assert "./.github/actions/agent/assert-availability" in scenario
    assert "./.github/actions/agent/assert-capability-assessment" in scenario
    assert "/usr/bin/python3" in assertion_action
    assert "ProviderSessionManager" in assertion
    assert "assess_capabilities" in assertion
    assert "cached_capability_assessment" in assertion
    assert "_UnavailableOnceRequester" not in assertion
    assert "capability-unavailable-once" in scenario
    assert "CapabilityAssessmentService.AssessCapabilities" in fixture
    assert "grpc.StatusCode.UNAVAILABLE" in fixture
    assert "retry did not preserve the request" in assertion
    assert "ProviderAvailabilityState.NOT_SERVING" in assertion
    assert (
        "Changed runtime environment did not refresh A3 evidence" in assertion
    )
    assert (
        "Availability transition did not invalidate A3 evidence" in assertion
    )
    assert "target_sha256" in assertion
    assert 'version("stateforge-protocol")' in assertion
    assert 'version("stateforge-provider")' in assertion
    assert 'event.startswith("operation:")' in assertion
    assert "capability-events.txt" in assertion_action
    assert "target-sha256.txt" in assertion_action
    assert "@requirement_check" in fixture
    assert "@operation" in fixture
    assert 'implements=("stateforge.backup.v1.prepare",)' in fixture
    assert "RequirementCheckResult.unsatisfied" in fixture
    assert "RequirementCheckResult.incompatible" in fixture
    assert "RequirementCheckResult.unknown" in fixture
