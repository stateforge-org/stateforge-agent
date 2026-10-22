"""Tests for translating discovery events into SFP sessions."""

import json
import signal
import socket
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from stateforge_agent._config.models import (
    AgentConfiguration,
    ControlPlaneConfiguration,
    JournalConfiguration,
    LoggingConfiguration,
    ProviderConfiguration,
)
from stateforge_agent._process import ProcessExitCode
from stateforge_agent._runtime import daemon
from stateforge_agent._runtime.daemon import (
    _descriptor_provider_instance_id,
    _install_signal_handlers,
    _process_events,
)
from stateforge_agent._runtime.layout import RuntimeLayout
from stateforge_agent._sfp import (
    SfpConnectionError,
    SfpManifestError,
    SfpVersion,
)
from stateforge_agent._sfp._runtime.discovery import (
    ProviderDirectoryError,
    ProviderDirectoryMonitor,
)

_PROVIDER_INSTANCE_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_EXPECTED_SIGNAL_HANDLERS = 2


def _write_descriptor(directory: Path, *, valid: bool = True) -> Path:
    socket_path = directory / f"{_PROVIDER_INSTANCE_ID}.1.sock"
    provider_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        provider_socket.bind(str(socket_path))
    finally:
        provider_socket.close()
    socket_path.chmod(0o660)

    descriptor = directory / f"{_PROVIDER_INSTANCE_ID}.sfp.json"
    document = {
        "schema_version": "1",
        "provider_instance_id": str(_PROVIDER_INSTANCE_ID),
        "endpoint_generation": 1,
        "transport": {
            "type": "unix",
            "socket_path": str(socket_path),
        },
    }
    if not valid:
        document["unknown"] = True
    descriptor.write_text(json.dumps(document), encoding="utf-8")
    descriptor.chmod(0o640)
    return descriptor


def _configuration(directory: Path) -> AgentConfiguration:
    return AgentConfiguration(
        schema_version=1,
        control_plane=ControlPlaneConfiguration(
            url="https://control.stateforge.invalid",
            ca_certificate=directory / "ca.pem",
            connect_timeout_seconds=5,
            request_timeout_seconds=30,
        ),
        providers=ProviderConfiguration(
            health_interval_seconds=10,
            reconnect_initial_delay_seconds=1,
            reconnect_max_delay_seconds=30,
        ),
        journal=JournalConfiguration(
            backend="sqlite",
        ),
        logging=LoggingConfiguration(
            level="info",
            format="text",
            destination="stderr",
        ),
    )


def _runtime_layout(directory: Path) -> RuntimeLayout:
    return RuntimeLayout(
        state_directory=directory / "state",
        runtime_directory=directory / "run",
    )


@pytest.mark.asyncio
async def test_discovered_descriptor_establishes_session(
    tmp_path: Path,
) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    _write_descriptor(tmp_path)
    session = SimpleNamespace(
        provider_instance_id=_PROVIDER_INSTANCE_ID,
        endpoint_generation=1,
        selected_sfp_version=SfpVersion(major=1, minor=0),
        provider_runtime_id=UUID("d81855f6-79ac-4b49-9f1a-c9f0985e7c58"),
        session_id="provider-session-1",
    )
    sessions = SimpleNamespace(
        establish=AsyncMock(return_value=session),
        remove=AsyncMock(),
    )

    await _process_events(monitor, sessions, monitor.scan())

    sessions.establish.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalid_descriptor_is_ignored(tmp_path: Path) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    _write_descriptor(tmp_path, valid=False)
    sessions = SimpleNamespace(
        establish=AsyncMock(),
        remove=AsyncMock(),
    )

    await _process_events(monitor, sessions, monitor.scan())

    sessions.establish.assert_not_awaited()


@pytest.mark.asyncio
async def test_connection_failure_isolated_to_provider(tmp_path: Path) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    _write_descriptor(tmp_path)
    sessions = SimpleNamespace(
        establish=AsyncMock(side_effect=SfpConnectionError("unavailable")),
        remove=AsyncMock(),
    )

    await _process_events(monitor, sessions, monitor.scan())

    sessions.establish.assert_awaited_once()


@pytest.mark.asyncio
async def test_manifest_rejection_isolated_to_provider(tmp_path: Path) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    _write_descriptor(tmp_path)
    sessions = SimpleNamespace(
        establish=AsyncMock(side_effect=SfpManifestError("invalid manifest")),
        remove=AsyncMock(),
    )

    await _process_events(monitor, sessions, monitor.scan())

    sessions.establish.assert_awaited_once()


@pytest.mark.asyncio
async def test_removed_descriptor_closes_provider_session(
    tmp_path: Path,
) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    descriptor = _write_descriptor(tmp_path)
    monitor.scan()
    descriptor.unlink()
    sessions = SimpleNamespace(
        establish=AsyncMock(),
        remove=AsyncMock(),
    )

    await _process_events(monitor, sessions, monitor.scan())

    sessions.remove.assert_awaited_once_with(_PROVIDER_INSTANCE_ID)


def test_descriptor_identity_recovery_rejects_noncanonical_names() -> None:
    assert _descriptor_provider_instance_id(Path("invalid.sfp.json")) is None
    assert _descriptor_provider_instance_id(Path("provider.json")) is None


@pytest.mark.asyncio
async def test_agent_runtime_starts_and_stops_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def stop_after_start(loop, stop_event):
        loop.call_soon(stop_event.set)
        return ()

    monkeypatch.setattr(daemon, "_install_signal_handlers", stop_after_start)

    assert (
        await daemon._run_agent(
            _configuration(tmp_path),
            _runtime_layout(tmp_path),
        )
        is ProcessExitCode.SUCCESS
    )


def test_run_agent_returns_daemon_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_result = ProcessExitCode.RUNTIME_FAILURE

    def finish_immediately(coroutine):
        coroutine.close()
        return expected_result

    monkeypatch.setattr(daemon.asyncio, "run", finish_immediately)
    monkeypatch.setattr(daemon, "configure_logging", Mock())

    assert daemon.run_agent(_configuration(tmp_path)) is expected_result


def test_run_agent_handles_keyboard_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def interrupt(coroutine):
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(daemon.asyncio, "run", interrupt)
    monkeypatch.setattr(daemon, "configure_logging", Mock())

    assert (
        daemon.run_agent(_configuration(tmp_path)) is ProcessExitCode.SUCCESS
    )


@pytest.mark.asyncio
async def test_provider_directory_failure_returns_runtime_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_to_prepare(_monitor: ProviderDirectoryMonitor) -> None:
        raise ProviderDirectoryError("unusable discovery directory")

    monkeypatch.setattr(ProviderDirectoryMonitor, "prepare", fail_to_prepare)

    result = await daemon._run_agent(
        _configuration(tmp_path),
        _runtime_layout(tmp_path),
    )

    assert result is ProcessExitCode.RUNTIME_FAILURE


def test_signal_handlers_are_registered_when_supported() -> None:
    loop = Mock()
    stop_event = Mock()

    installed = _install_signal_handlers(loop, stop_event)

    assert installed == (signal.SIGTERM, signal.SIGINT)
    assert loop.add_signal_handler.call_count == _EXPECTED_SIGNAL_HANDLERS


def test_unsupported_signal_handlers_are_skipped() -> None:
    loop = Mock()
    loop.add_signal_handler.side_effect = NotImplementedError

    assert _install_signal_handlers(loop, Mock()) == ()
