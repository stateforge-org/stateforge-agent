"""Tests for Provider discovery-directory monitoring."""

import asyncio
import json
import os
import socket
from pathlib import Path
from uuid import UUID

import pytest

from stateforge_agent._sfp._runtime.descriptor import (
    PROVIDER_DESCRIPTOR_SUFFIX,
    ProviderDescriptorError,
    load_provider_descriptor,
)
from stateforge_agent._sfp._runtime.discovery import (
    ProviderDirectoryError,
    ProviderDirectoryEventKind,
    ProviderDirectoryMonitor,
)

_DISCOVERY_DIRECTORY_MODE = 0o750
_PROVIDER_INSTANCE_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_ENDPOINT_GENERATION = 4
_MAXIMUM_ENDPOINT_GENERATION = (2**63) - 1


def _descriptor_path(directory: Path) -> Path:
    return directory / f"{_PROVIDER_INSTANCE_ID}{PROVIDER_DESCRIPTOR_SUFFIX}"


def _socket_path(
    directory: Path,
    *,
    endpoint_generation: int = _ENDPOINT_GENERATION,
) -> Path:
    return directory / (f"{_PROVIDER_INSTANCE_ID}.{endpoint_generation}.sock")


def _write_socket(directory: Path) -> Path:
    socket_path = _socket_path(directory)
    provider_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        provider_socket.bind(str(socket_path))
    finally:
        provider_socket.close()
    socket_path.chmod(0o660)
    return socket_path


def _write_descriptor(directory: Path) -> Path:
    _write_socket(directory)
    descriptor = _descriptor_path(directory)
    descriptor.write_text(
        json.dumps(
            {
                "schema_version": "1",
                "provider_instance_id": str(_PROVIDER_INSTANCE_ID),
                "endpoint_generation": _ENDPOINT_GENERATION,
                "transport": {
                    "type": "unix",
                    "socket_path": str(_socket_path(directory)),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    descriptor.chmod(0o640)
    return descriptor


def test_prepares_missing_discovery_directory(tmp_path: Path) -> None:
    directory = tmp_path / "runtime" / "providers"
    monitor = ProviderDirectoryMonitor(directory)

    monitor.prepare()

    assert directory.is_dir()
    assert directory.stat().st_mode & 0o777 == _DISCOVERY_DIRECTORY_MODE


def test_reports_discovered_updated_and_removed_entries(
    tmp_path: Path,
) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    descriptor = _write_descriptor(tmp_path)

    discovered = monitor.scan()
    document = json.loads(descriptor.read_text(encoding="utf-8"))
    document["endpoint_generation"] = 44
    descriptor.write_text(json.dumps(document), encoding="utf-8")
    updated = monitor.scan()
    descriptor.unlink()
    removed = monitor.scan()

    assert [event.kind for event in discovered] == [
        ProviderDirectoryEventKind.DISCOVERED
    ]
    assert [event.kind for event in updated] == [
        ProviderDirectoryEventKind.UPDATED
    ]
    assert [event.kind for event in removed] == [
        ProviderDirectoryEventKind.REMOVED
    ]
    assert discovered[0].entry.path == descriptor
    assert discovered[0].entry.entry_type == "file"


@pytest.mark.asyncio
async def test_watches_descriptor_creation_update_and_removal(
    tmp_path: Path,
) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    stop_event = asyncio.Event()
    event_stream = monitor.watch(stop_event)

    first_batch = asyncio.create_task(anext(event_stream))
    await asyncio.sleep(0.05)
    descriptor = _write_descriptor(tmp_path)
    discovered = await asyncio.wait_for(first_batch, timeout=5)

    updated_batch = asyncio.create_task(anext(event_stream))
    document = json.loads(descriptor.read_text(encoding="utf-8"))
    document["endpoint_generation"] = 44
    descriptor.write_text(json.dumps(document), encoding="utf-8")
    updated = await asyncio.wait_for(
        updated_batch,
        timeout=5,
    )

    removed_batch = asyncio.create_task(anext(event_stream))
    descriptor.unlink()
    removed = await asyncio.wait_for(
        removed_batch,
        timeout=5,
    )

    stop_event.set()
    await event_stream.aclose()

    assert [event.kind for event in discovered] == [
        ProviderDirectoryEventKind.DISCOVERED
    ]
    assert [event.kind for event in updated] == [
        ProviderDirectoryEventKind.UPDATED
    ]
    assert [event.kind for event in removed] == [
        ProviderDirectoryEventKind.REMOVED
    ]


@pytest.mark.asyncio
async def test_watch_reconciles_existing_descriptor_before_waiting(
    tmp_path: Path,
) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    descriptor = _write_descriptor(tmp_path)
    stop_event = asyncio.Event()
    event_stream = monitor.watch(stop_event)

    discovered = await asyncio.wait_for(anext(event_stream), timeout=2)
    stop_event.set()
    await event_stream.aclose()

    assert discovered[0].kind is ProviderDirectoryEventKind.DISCOVERED
    assert discovered[0].entry.path == descriptor


def test_ignores_entries_that_are_not_sfp_descriptors(tmp_path: Path) -> None:
    monitor = ProviderDirectoryMonitor(tmp_path)
    monitor.prepare()
    (tmp_path / "provider.sock").touch()
    (tmp_path / "provider.json").touch()
    (tmp_path / ".provider.sfp.json.tmp").touch()

    assert monitor.scan() == ()


def test_loads_valid_provider_descriptor(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)

    descriptor = load_provider_descriptor(
        descriptor_path,
        discovery_directory=tmp_path,
    )

    assert descriptor.schema_version == "1"
    assert descriptor.provider_instance_id == _PROVIDER_INSTANCE_ID
    assert descriptor.endpoint_generation == _ENDPOINT_GENERATION
    assert descriptor.socket_path == (
        tmp_path / f"{_PROVIDER_INSTANCE_ID}.4.sock"
    )


@pytest.mark.parametrize("mode", [0o600, 0o644, 0o660])
def test_rejects_noncanonical_descriptor_mode(
    tmp_path: Path,
    mode: int,
) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    descriptor_path.chmod(mode)

    with pytest.raises(ProviderDescriptorError, match="mode must be 0o640"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_missing_provider_socket(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    _socket_path(tmp_path).unlink()

    with pytest.raises(ProviderDescriptorError, match="does not exist"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_endpoint_that_is_not_a_socket(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    socket_path = _socket_path(tmp_path)
    socket_path.unlink()
    socket_path.touch(mode=0o660)

    with pytest.raises(ProviderDescriptorError, match="Unix-domain socket"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_noncanonical_socket_mode(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    _socket_path(tmp_path).chmod(0o666)

    with pytest.raises(ProviderDescriptorError, match="mode must be 0o660"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_symbolic_link_socket(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    socket_path = _socket_path(tmp_path)
    actual_socket_path = tmp_path / "actual-provider.sock"
    socket_path.rename(actual_socket_path)
    socket_path.symlink_to(actual_socket_path)

    with pytest.raises(ProviderDescriptorError, match="symbolic link"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


@pytest.mark.skipif(os.geteuid() != 0, reason="requires chown capability")
def test_rejects_socket_owned_by_another_user(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    os.chown(_socket_path(tmp_path), 12345, -1)

    with pytest.raises(ProviderDescriptorError, match="owner must match"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


@pytest.mark.skipif(os.geteuid() != 0, reason="requires chown capability")
def test_rejects_descriptor_from_another_group(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    os.chown(descriptor_path, -1, 12345)

    with pytest.raises(ProviderDescriptorError, match="discovery directory"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


@pytest.mark.skipif(os.geteuid() != 0, reason="requires chown capability")
def test_rejects_socket_from_another_group(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    os.chown(_socket_path(tmp_path), -1, 12345)

    with pytest.raises(ProviderDescriptorError, match="socket group"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


@pytest.mark.parametrize(
    "generation",
    [True, 0, _MAXIMUM_ENDPOINT_GENERATION + 1],
)
def test_rejects_invalid_endpoint_generation(
    tmp_path: Path,
    generation: object,
) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    document = json.loads(descriptor_path.read_text(encoding="utf-8"))
    document["endpoint_generation"] = generation
    descriptor_path.write_text(json.dumps(document), encoding="utf-8")
    descriptor_path.chmod(0o640)

    with pytest.raises(ProviderDescriptorError, match="endpoint_generation"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_unsupported_transport(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    document = json.loads(descriptor_path.read_text(encoding="utf-8"))
    document["transport"]["type"] = "tcp"
    descriptor_path.write_text(json.dumps(document), encoding="utf-8")
    descriptor_path.chmod(0o640)

    with pytest.raises(ProviderDescriptorError, match="transport"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_descriptor_with_unknown_field(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    document = json.loads(descriptor_path.read_text(encoding="utf-8"))
    document["unexpected_field"] = "unexpected"
    descriptor_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ProviderDescriptorError, match="unknown fields"):
        load_provider_descriptor(
            descriptor_path,
            discovery_directory=tmp_path,
        )


def test_rejects_descriptor_filename_identity_mismatch(tmp_path: Path) -> None:
    descriptor_path = _write_descriptor(tmp_path)
    wrong_path = tmp_path / (
        f"00000000-0000-4000-8000-000000000000{PROVIDER_DESCRIPTOR_SUFFIX}"
    )
    descriptor_path.rename(wrong_path)

    with pytest.raises(ProviderDescriptorError, match="filename must be"):
        load_provider_descriptor(
            wrong_path,
            discovery_directory=tmp_path,
        )


def test_rejects_symbolic_link_discovery_directory(tmp_path: Path) -> None:
    real_directory = tmp_path / "real"
    real_directory.mkdir()
    linked_directory = tmp_path / "linked"
    linked_directory.symlink_to(real_directory, target_is_directory=True)
    monitor = ProviderDirectoryMonitor(linked_directory)

    with pytest.raises(ProviderDirectoryError, match="symbolic link"):
        monitor.prepare()
