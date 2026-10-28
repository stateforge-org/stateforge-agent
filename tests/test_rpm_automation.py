"""Contract tests for reusable RPM and installed-system automation."""

from pathlib import Path
from runpy import run_path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTIONS = ROOT / ".github" / "actions"
SHA_LENGTH = 40
DEPENDENCY_COUNT = 2
HEALTH_SCENARIO_PROVIDER_COUNT = 3
MANIFEST_ASSERTION_COUNT = 6
AVAILABILITY_ASSERTION_COUNT = 10


def read(path: Path) -> str:
    """Read one automation file as UTF-8 text."""
    return path.read_text(encoding="utf-8")


def action_invocations(content: str, action: str) -> tuple[str, ...]:
    """Return the complete YAML step for every use of a local action."""
    lines = content.splitlines()
    invocations: list[str] = []
    for index, line in enumerate(lines):
        if line.strip() != f"uses: {action}":
            continue
        start = index
        while start > 0 and not lines[start].startswith("    - "):
            start -= 1
        end = index + 1
        while end < len(lines) and not lines[end].startswith("    - "):
            end += 1
        invocations.append("\n".join(lines[start:end]))
    return tuple(invocations)


def test_capability_assertion_treats_a_missing_event_log_as_no_calls(
    tmp_path: Path,
) -> None:
    """Permit local rejections before the Provider records its first RPC."""
    assertion = run_path(
        str(ACTIONS / "agent" / "assert-capability-assessment" / "assert.py")
    )
    attempt_digests = assertion["_capability_attempt_digests"]
    event_log = tmp_path / "events.log"

    assert attempt_digests(event_log) == ()

    event_log.write_text(
        "provider-started\n"
        "rpc:capability-assessment:first\n"
        "rpc:capability-assessment:second\n",
        encoding="utf-8",
    )
    assert attempt_digests(event_log) == ("first", "second")


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


def test_packaged_service_restarts_only_recoverable_runtime_failures() -> None:
    """Do not restart-loop when operator configuration is invalid."""
    service = read(
        ROOT / "packaging" / "rpm" / "SOURCES" / "stateforge-agent.service"
    )

    assert "Restart=on-failure" in service
    assert "RestartPreventExitStatus=2" in service


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
    manifest_assertion = read(
        ACTIONS / "provider" / "assert-manifest" / "action.yml"
    )

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
    assert "previous_generation" in provider_start
    assert "resolve-ready-endpoint.sh" in provider_start
    assert "agent-journal-cursor" in provider_start
    assert '"--after-cursor=${JOURNAL_CURSOR}"' in manifest_assertion
    assert "ENDPOINT_GENERATION" in manifest_assertion
    assert "from stateforge_provider import" in fixture_source
    assert "token: ${{ inputs.repository-token }}" in fixture


def test_every_manifest_assertion_is_generation_and_cursor_scoped() -> None:
    """Bind every installed manifest check to its Provider invocation."""
    scenarios = "\n".join(
        read(path) for path in ACTIONS.glob("rpm/test-installed/**/action.yml")
    )
    invocations = action_invocations(
        scenarios,
        "./.github/actions/provider/assert-manifest",
    )

    assert len(invocations) == MANIFEST_ASSERTION_COUNT
    assert all("journal-cursor: ${{ steps." in step for step in invocations)
    assert all(
        "endpoint-generation: ${{ steps." in step for step in invocations
    )


def test_every_availability_assertion_is_transition_scoped() -> None:
    """Require one post-trigger journal boundary for every state assertion."""
    scenarios = "\n".join(
        read(path) for path in ACTIONS.glob("rpm/test-installed/**/action.yml")
    )
    assertion = read(ACTIONS / "agent" / "assert-availability" / "action.yml")
    invocations = action_invocations(
        scenarios,
        "./.github/actions/agent/assert-availability",
    )

    assert len(invocations) == AVAILABILITY_ASSERTION_COUNT
    assert all("journal-cursor: ${{ steps." in step for step in invocations)
    assert all(
        "endpoint-generation: ${{ steps." in step for step in invocations
    )
    assert "minimum-occurrences" not in scenarios
    assert "minimum-occurrences" not in assertion
    assert '"--after-cursor=${JOURNAL_CURSOR}"' in assertion


def test_agent_start_is_scoped_to_the_current_systemd_invocation() -> None:
    """Reject historical startup logs and silent service replacements."""
    action = read(ACTIONS / "agent" / "start" / "action.yml")
    start_script = read(ACTIONS / "agent" / "start" / "start.sh")

    assert '"${ACTION_PATH}/start.sh"' in action
    assert "--property=InvocationID" in start_script
    assert "check-ready-invocation.sh" in start_script
    assert "service-invocation-id" in action
    assert "grep -Fq 'StateForge Agent started'" not in start_script


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
    """Resolve convenient refs once and reuse immutable dependency commits."""
    release = read(WORKFLOWS / "rpm-release-candidate.yml")
    pipeline = read(WORKFLOWS / "rpm-build-and-test.yml")
    source = read(WORKFLOWS / "source-validation.yml")
    installed = read(WORKFLOWS / "rpm-installed-test.yml")
    resolver = read(ACTIONS / "dependencies" / "resolve" / "action.yml")
    fixture = read(ACTIONS / "provider" / "create-fixture" / "action.yml")
    setup = read(ACTIONS / "rpm" / "test-installed" / "setup" / "action.yml")

    for ref in ("provider-ref", "protocol-ref"):
        assert f"{ref}:\n" in release
        assert f"{ref}: ${{{{ inputs.{ref} }}}}" in release
    assert release.count("default: mvp/implementation") == DEPENDENCY_COUNT
    assert pipeline.count("uses: ./.github/actions/dependencies/resolve") == 1
    assert "ref: ${{ inputs.provider-ref }}" in resolver
    assert "ref: ${{ inputs.protocol-ref }}" in resolver
    assert (
        "provider-commit: ${{ needs.resolve.outputs.provider-commit }}"
        in pipeline
    )
    assert (
        "protocol-commit: ${{ needs.resolve.outputs.protocol-commit }}"
        in pipeline
    )
    assert "ref: ${{ inputs.protocol-commit }}" in source
    assert "inputs.protocol-ref" not in source
    assert "provider-commit: ${{ inputs.provider-commit }}" in installed
    assert "protocol-commit: ${{ inputs.protocol-commit }}" in installed
    assert "ref: ${{ inputs.provider-commit }}" in fixture
    assert "ref: ${{ inputs.protocol-commit }}" in fixture
    assert "ref: ${{ inputs.provider-ref }}" not in fixture
    assert "ref: ${{ inputs.protocol-ref }}" not in fixture
    assert "dependencies/verify-checkout.sh" in source
    assert "dependencies/verify-checkout.sh" in fixture
    assert '"${OUTPUT_DIRECTORY}/dependency-provenance.json"' in fixture
    assert "requested_ref:" in fixture
    assert "resolved_commit:" in fixture
    assert "wheel_sha256:" in fixture
    assert "${{ steps.fixture.outputs.provenance-path }}" in setup
    assert "dependency-provenance.json" in setup
    assert "provider-ref: ${{ inputs.provider-ref }}" in installed
    assert "protocol-ref: ${{ inputs.protocol-ref }}" in installed


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

    assert "      - build" in workflow
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
        "agent/assert-capability-boundary",
        "agent/assert-capability-assessment",
        "agent/assert-trigger-publication",
        "agent/collect-evidence",
        "agent/install",
        "agent/start",
        "dependencies/resolve",
        "provider/assert-manifest",
        "provider/create-fixture",
        "provider/set-health",
        "provider/start",
        "provider/stop",
        "rpm/build",
        "rpm/test-installed/adversarial/invalid-descriptor",
        "rpm/test-installed/capability/assessment",
        "rpm/test-installed/capability/boundary",
        "rpm/test-installed/cleanup",
        "rpm/test-installed/destructive/uninstall-reinstall",
        "rpm/test-installed/health/provider-availability",
        "rpm/test-installed/lifecycle/multiple-providers",
        "rpm/test-installed/setup",
        "rpm/test-installed/smoke/manifest-exchange",
        "rpm/test-installed/triggers/publication",
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
    diagnostics = read(
        ACTIONS / "agent" / "collect-evidence" / "collect-diagnostics.sh"
    )

    assert "if: ${{ always() }}" in action
    assert "./.github/actions/agent/collect-evidence" in cleanup
    assert "collect-diagnostics.sh" in evidence
    assert "journalctl -u stateforge-agent.service" in diagnostics
    assert "provider-identities" in diagnostics
    assert "provider-state-metadata.txt" in diagnostics
    assert "provider-state-sha256.txt" in diagnostics
    assert "systemd-invocations.json" in diagnostics
    assert "InvocationID" in diagnostics
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
        "triggers",
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
    assertion_checker = read(
        ACTIONS / "agent" / "assert-availability" / "check-journal.sh"
    )

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
    assert "Provider availability changed:" in assertion_checker
    assert "endpoint_generation=${ENDPOINT_GENERATION}" in assertion_checker


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
    for boundary in (
        "capability-timeout",
        "capability-malformed",
        "capability-mismatch",
        "capability-limits",
    ):
        assert boundary in workflow
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

    boundary_action = read(
        ACTIONS / "agent" / "assert-capability-boundary" / "action.yml"
    )
    boundary_assertion = read(
        ACTIONS / "agent" / "assert-capability-boundary" / "assert.py"
    )
    assert "/usr/bin/python3" in boundary_action
    assert "SfpCapabilityAssessmentDeadlineError" in boundary_assertion
    assert (
        "SfpCapabilityAssessmentMalformedResponseError" in boundary_assertion
    )
    assert "SfpCapabilityAssessmentLimitError" in boundary_assertion
    assert "session_id" in boundary_assertion
    assert "endpoint_generation" in boundary_assertion
    assert "manifest_revision" in boundary_assertion


def test_installed_trigger_scenario_uses_public_api_and_journal() -> None:
    """Keep A4 evidence on exact packages and the durable Agent boundary."""
    workflow = read(WORKFLOWS / "rpm-installed-test.yml")
    scenario = read(
        ACTIONS
        / "rpm"
        / "test-installed"
        / "triggers"
        / "publication"
        / "action.yml"
    )
    assertion_action = read(
        ACTIONS / "agent" / "assert-trigger-publication" / "action.yml"
    )
    assertion = read(
        ACTIONS / "agent" / "assert-trigger-publication" / "assert.py"
    )
    fixture = read(
        ACTIONS / "provider" / "create-fixture" / "fixtures" / "manifest.py"
    )

    assert "trigger-publication" in workflow
    assert "./.github/actions/rpm/test-installed/setup" in scenario
    assert "./.github/actions/provider/start" in scenario
    assert "./.github/actions/agent/assert-trigger-publication" in scenario
    assert "systemctl stop stateforge-agent.service" in scenario
    assert "./.github/actions/agent/start" in scenario
    assert "before-agent-restart" in scenario
    assert "after-agent-restart" in scenario
    assert "stateforge.test/runtime.started@1" in scenario
    assert "stateforge.test/runtime.refreshed@1" in scenario
    assert "@trigger(" in fixture
    assert "publish_trigger(" in fixture
    assert "/usr/bin/python3" in assertion_action
    assert "mode=ro" in assertion
    assert "validate_trigger_publication" in assertion
    assert "compute_publication_content_fingerprint" in assertion
    assert 'row["forwarding_state"] != "pending"' in assertion
