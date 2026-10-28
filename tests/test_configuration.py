"""Tests for strict Agent configuration loading."""

from pathlib import Path

import pytest

from stateforge_agent._config import (
    AgentConfiguration,
    ConfigurationError,
    load_configuration,
)
from stateforge_agent._process import ProcessExitCode
from stateforge_agent.cli import main

_VALID_CONFIGURATION = """
schema_version = 1

[control_plane]
url = "https://control.stateforge.example"
ca_certificate = "/etc/pki/tls/certs/ca-bundle.crt"
connect_timeout_seconds = 10
request_timeout_seconds = 30

[providers]
health_interval_seconds = 15
reconnect_initial_delay_seconds = 1
reconnect_max_delay_seconds = 30
trigger_max_in_flight_publications = 32

[journal]
backend = "sqlite"
maximum_pending_trigger_publications = 10000

[logging]
level = "info"
format = "text"
destination = "stderr"
""".strip()

_FINAL_HEALTH_INTERVAL = 45
_SUCCESS_EXIT_CODE = 0
_RUNTIME_FAILURE_EXIT_CODE = 1
_INVALID_INPUT_EXIT_CODE = 2
_TRIGGER_RECEIVE_WINDOW = 32
_TRIGGER_PENDING_CAPACITY = 10_000


def test_process_exit_codes_are_stable() -> None:
    assert int(ProcessExitCode.SUCCESS) == _SUCCESS_EXIT_CODE
    assert int(ProcessExitCode.RUNTIME_FAILURE) == _RUNTIME_FAILURE_EXIT_CODE
    assert int(ProcessExitCode.INVALID_INPUT) == _INVALID_INPUT_EXIT_CODE


def _write_configuration(directory: Path) -> Path:
    path = directory / "config.toml"
    path.write_text(_VALID_CONFIGURATION, encoding="utf-8")
    return path


def test_loads_complete_configuration(tmp_path: Path) -> None:
    config_path = _write_configuration(tmp_path)

    loaded = load_configuration(config_path)

    assert loaded.config.schema_version == 1
    assert loaded.config.control_plane.url == (
        "https://control.stateforge.example"
    )
    assert loaded.config.journal.backend == "sqlite"
    assert (
        loaded.config.providers.trigger_max_in_flight_publications
        == _TRIGGER_RECEIVE_WINDOW
    )
    assert (
        loaded.config.journal.maximum_pending_trigger_publications
        == _TRIGGER_PENDING_CAPACITY
    )
    assert loaded.sources == (config_path,)
    assert loaded.fingerprint.startswith("sha256:")


def test_merges_drop_ins_in_lexical_order(tmp_path: Path) -> None:
    config_path = _write_configuration(tmp_path)
    drop_in_directory = tmp_path / "conf.d"
    drop_in_directory.mkdir()
    (drop_in_directory / "10-health.toml").write_text(
        "[providers]\nhealth_interval_seconds = 20\n",
        encoding="utf-8",
    )
    final_override = drop_in_directory / "90-local.toml"
    final_override.write_text(
        """
[providers]
health_interval_seconds = {_FINAL_HEALTH_INTERVAL}

[logging]
level = "debug"
""".strip().format(_FINAL_HEALTH_INTERVAL=_FINAL_HEALTH_INTERVAL),
        encoding="utf-8",
    )

    loaded = load_configuration(config_path)

    assert (
        loaded.config.providers.health_interval_seconds
        == _FINAL_HEALTH_INTERVAL
    )
    assert loaded.config.logging.level == "debug"
    assert loaded.sources[-1] == final_override


def test_rejects_unknown_key_with_defining_source(tmp_path: Path) -> None:
    config_path = _write_configuration(tmp_path)
    drop_in_directory = tmp_path / "conf.d"
    drop_in_directory.mkdir()
    invalid_override = drop_in_directory / "50-invalid.toml"
    invalid_override.write_text(
        "[logging]\ncolour = true\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError) as captured:
        load_configuration(config_path)

    message = str(captured.value)
    assert "logging.colour" in message
    assert str(invalid_override) in message
    assert "unknown configuration key" in message


@pytest.mark.parametrize(
    ("old", "new", "expected_message"),
    [
        (
            'url = "https://control.stateforge.example"',
            'url = "http://control.stateforge.example"',
            "must be an absolute HTTPS URL",
        ),
        (
            "connect_timeout_seconds = 10",
            "connect_timeout_seconds = 0",
            "must be greater than zero",
        ),
        (
            'ca_certificate = "/etc/pki/tls/certs/ca-bundle.crt"',
            'ca_certificate = "etc/pki/tls/certs/ca-bundle.crt"',
            "must be an absolute path",
        ),
    ],
)
def test_rejects_invalid_values(
    tmp_path: Path,
    old: str,
    new: str,
    expected_message: str,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        _VALID_CONFIGURATION.replace(old, new),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match=expected_message):
        load_configuration(config_path)


def test_rejects_reconnect_maximum_below_initial_delay(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        _VALID_CONFIGURATION.replace(
            "reconnect_initial_delay_seconds = 1",
            "reconnect_initial_delay_seconds = 60",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="greater than or equal"):
        load_configuration(config_path)


@pytest.mark.parametrize(
    ("old", "new", "expected_field"),
    [
        (
            "[control_plane]",
            '[agent]\ndata_directory = "/tmp/stateforge"\n\n[control_plane]',
            "agent",
        ),
        (
            "[providers]",
            '[providers]\ndiscovery_directory = "/tmp/providers"',
            "providers.discovery_directory",
        ),
        (
            "[journal]",
            '[journal]\npath = "/tmp/agent.db"',
            "journal.path",
        ),
    ],
)
def test_rejects_platform_owned_path_configuration(
    tmp_path: Path,
    old: str,
    new: str,
    expected_field: str,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        _VALID_CONFIGURATION.replace(old, new, 1),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match=expected_field):
        load_configuration(config_path)


def test_rejects_invalid_toml(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text("[agent", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="Cannot parse"):
        load_configuration(config_path)


def test_rejects_missing_primary_file(tmp_path: Path) -> None:
    config_path = tmp_path / "missing.toml"

    with pytest.raises(ConfigurationError, match="does not exist"):
        load_configuration(config_path)


def test_rejects_non_directory_drop_in_path(tmp_path: Path) -> None:
    config_path = _write_configuration(tmp_path)
    drop_in_path = tmp_path / "overrides"
    drop_in_path.write_text("not a directory", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="not a directory"):
        load_configuration(
            config_path,
            drop_in_directory=drop_in_path,
        )


def test_check_config_command_succeeds(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = _write_configuration(tmp_path)

    exit_code = main(["check-config", "--config", str(config_path)])

    output = capsys.readouterr()
    assert exit_code is ProcessExitCode.SUCCESS
    assert "Configuration is valid" in output.out
    assert "sha256:" in output.out
    assert output.err == ""


def test_check_config_command_reports_errors(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "missing.toml"

    exit_code = main(["check-config", "--config", str(config_path)])

    output = capsys.readouterr()
    assert exit_code is ProcessExitCode.INVALID_INPUT
    assert output.out == ""
    assert "does not exist" in output.err


def test_run_command_loads_configuration_and_starts_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _write_configuration(tmp_path)
    observed_urls: list[str] = []

    def fake_run_agent(
        configuration: AgentConfiguration,
    ) -> ProcessExitCode:
        observed_urls.append(configuration.control_plane.url)
        return ProcessExitCode.SUCCESS

    monkeypatch.setattr("stateforge_agent.cli.run_agent", fake_run_agent)

    exit_code = main(["run", "--config", str(config_path)])

    assert exit_code is ProcessExitCode.SUCCESS
    assert observed_urls == ["https://control.stateforge.example"]


def test_root_help_uses_stateforge_cli_layout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as captured:
        main(["--help"])

    output = capsys.readouterr()
    assert captured.value.code == 0
    assert output.out.startswith("Usage: stateforge-agent [OPTIONS] COMMAND\n")
    assert (
        "A secure execution runtime for StateForge operations." in output.out
    )
    assert "Commands:" in output.out
    assert "run" in output.out
    assert "Run the StateForge Agent daemon" in output.out
    assert "check-config" in output.out
    assert "Validate the effective Agent configuration" in output.out
    assert "Global Options:" in output.out
    assert "Run 'stateforge-agent COMMAND --help'" in output.out
    assert output.err == ""


def test_version_uses_success_exit_status(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as captured:
        main(["--version"])

    output = capsys.readouterr()
    assert captured.value.code == ProcessExitCode.SUCCESS
    assert output.out.startswith("stateforge-agent ")
    assert output.err == ""


def test_invalid_invocation_uses_invalid_input_exit_status(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as captured:
        main([])

    output = capsys.readouterr()
    assert captured.value.code == ProcessExitCode.INVALID_INPUT
    assert output.out == ""
    assert "the following arguments are required: COMMAND" in output.err
