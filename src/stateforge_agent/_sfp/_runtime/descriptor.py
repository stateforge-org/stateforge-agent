"""Validate local SFP Provider endpoint descriptor files."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from stateforge_protocol.sfp.endpoint_discovery.v1 import (
    contract as discovery_contract,
)

PROVIDER_DESCRIPTOR_SUFFIX = discovery_contract.DESCRIPTOR_SUFFIX


class ProviderDescriptorError(ValueError):
    """Report an invalid or unsafe Provider endpoint descriptor."""


@dataclass(frozen=True, slots=True)
class ProviderEndpointDescriptor:
    """Validated information required to open an SFP bootstrap channel."""

    schema_version: str
    provider_instance_id: UUID
    endpoint_generation: int
    socket_path: Path


def load_provider_descriptor(
    path: Path,
    *,
    discovery_directory: Path,
) -> ProviderEndpointDescriptor:
    """Load one strict descriptor without following symbolic links."""
    content, descriptor_status = _read_descriptor(path)
    descriptor = _parse_descriptor(content, path)
    _validate_endpoint_files(
        descriptor=descriptor,
        descriptor_path=path,
        descriptor_status=descriptor_status,
        discovery_directory=discovery_directory,
    )
    return descriptor


def _read_descriptor(path: Path) -> tuple[bytes, os.stat_result]:
    """Read a bounded regular file using a non-following file descriptor."""
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ProviderDescriptorError(
            f"Cannot open Provider descriptor {path}: {error}"
        ) from error

    try:
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise ProviderDescriptorError(
                f"Provider descriptor is not a regular file: {path}"
            )
        if status.st_size > discovery_contract.MAX_DESCRIPTOR_BYTES:
            raise ProviderDescriptorError(
                "Provider descriptor exceeds "
                f"{discovery_contract.MAX_DESCRIPTOR_BYTES} "
                f"bytes: {path}"
            )
        content = os.read(
            descriptor,
            discovery_contract.MAX_DESCRIPTOR_BYTES + 1,
        )
    except OSError as error:
        raise ProviderDescriptorError(
            f"Cannot read Provider descriptor {path}: {error}"
        ) from error
    finally:
        os.close(descriptor)

    if not content:
        raise ProviderDescriptorError(f"Provider descriptor is empty: {path}")
    if len(content) > discovery_contract.MAX_DESCRIPTOR_BYTES:
        raise ProviderDescriptorError(
            "Provider descriptor exceeds "
            f"{discovery_contract.MAX_DESCRIPTOR_BYTES} "
            f"bytes: {path}"
        )
    return content, status


def _parse_descriptor(
    content: bytes,
    path: Path,
) -> ProviderEndpointDescriptor:
    """Apply the canonical data contract and adapt it for local I/O."""
    try:
        document = discovery_contract.parse_descriptor(content)
    except discovery_contract.EndpointDescriptorContractError as error:
        raise ProviderDescriptorError(
            f"Invalid Provider descriptor {path}: {error}"
        ) from error

    return ProviderEndpointDescriptor(
        schema_version=document.schema_version,
        provider_instance_id=document.provider_instance_id,
        endpoint_generation=document.endpoint_generation,
        socket_path=Path(document.socket_path),
    )


# Keep the complete filesystem trust boundary visible in one place.
def _validate_endpoint_files(  # noqa: C901, PLR0912
    *,
    descriptor: ProviderEndpointDescriptor,
    descriptor_path: Path,
    descriptor_status: os.stat_result,
    discovery_directory: Path,
) -> None:
    """Validate filesystem security and bind names to endpoint identity."""
    descriptor_mode = stat.S_IMODE(descriptor_status.st_mode)
    if descriptor_mode != discovery_contract.DESCRIPTOR_MODE:
        raise ProviderDescriptorError(
            "Provider descriptor mode must be "
            f"{discovery_contract.DESCRIPTOR_MODE:#05o}, "
            f"found {descriptor_mode:#05o}: {descriptor_path}"
        )

    if not discovery_directory.is_absolute():
        raise ProviderDescriptorError(
            "Provider discovery_directory must be absolute"
        )
    try:
        directory_status = discovery_directory.lstat()
    except OSError as error:
        raise ProviderDescriptorError(
            "Cannot inspect Provider discovery directory "
            f"{discovery_directory}: {error}"
        ) from error
    if stat.S_ISLNK(directory_status.st_mode):
        raise ProviderDescriptorError(
            "Provider discovery directory must not be a symbolic link: "
            f"{discovery_directory}"
        )
    if not stat.S_ISDIR(directory_status.st_mode):
        raise ProviderDescriptorError(
            "Provider discovery path is not a directory: "
            f"{discovery_directory}"
        )
    if descriptor_status.st_gid != directory_status.st_gid:
        raise ProviderDescriptorError(
            "Provider descriptor group must match the discovery directory: "
            f"{descriptor_path}"
        )
    if descriptor_path.parent != discovery_directory:
        raise ProviderDescriptorError(
            "Provider descriptor must be directly inside the discovery "
            "directory"
        )

    expected_descriptor_name = discovery_contract.descriptor_filename(
        descriptor.provider_instance_id
    )
    if descriptor_path.name != expected_descriptor_name:
        raise ProviderDescriptorError(
            f"Provider descriptor filename must be {expected_descriptor_name}"
        )

    socket_path = descriptor.socket_path
    if not socket_path.is_absolute():
        raise ProviderDescriptorError("Provider socket_path must be absolute")
    if socket_path.parent != discovery_directory:
        raise ProviderDescriptorError(
            "Provider socket_path must be directly inside the discovery "
            "directory"
        )

    expected_socket_name = discovery_contract.socket_filename(
        descriptor.provider_instance_id,
        descriptor.endpoint_generation,
    )
    if socket_path.name != expected_socket_name:
        raise ProviderDescriptorError(
            f"Provider socket filename must be {expected_socket_name}"
        )

    try:
        socket_status = socket_path.lstat()
    except FileNotFoundError as error:
        raise ProviderDescriptorError(
            f"Provider socket does not exist: {socket_path}"
        ) from error
    except OSError as error:
        raise ProviderDescriptorError(
            f"Cannot inspect Provider socket {socket_path}: {error}"
        ) from error

    if stat.S_ISLNK(socket_status.st_mode):
        raise ProviderDescriptorError(
            f"Provider socket must not be a symbolic link: {socket_path}"
        )
    if not stat.S_ISSOCK(socket_status.st_mode):
        raise ProviderDescriptorError(
            f"Provider endpoint is not a Unix-domain socket: {socket_path}"
        )

    socket_mode = stat.S_IMODE(socket_status.st_mode)
    if socket_mode != discovery_contract.SOCKET_MODE:
        raise ProviderDescriptorError(
            "Provider socket mode must be "
            f"{discovery_contract.SOCKET_MODE:#05o}, "
            f"found {socket_mode:#05o}: {socket_path}"
        )
    if socket_status.st_uid != descriptor_status.st_uid:
        raise ProviderDescriptorError(
            "Provider socket owner must match its descriptor owner: "
            f"{socket_path}"
        )
    if socket_status.st_gid != descriptor_status.st_gid:
        raise ProviderDescriptorError(
            "Provider socket group must match its descriptor group: "
            f"{socket_path}"
        )
