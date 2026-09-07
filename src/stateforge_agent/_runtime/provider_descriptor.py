"""Validate local SFP Provider endpoint descriptor files."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from stateforge_agent._identity import parse_canonical_uuid4

PROVIDER_DESCRIPTOR_SUFFIX = ".sfp.json"

_SCHEMA_VERSION = "1"
_TRANSPORT_TYPE = "unix"
_DESCRIPTOR_MODE = 0o640
_SOCKET_MODE = 0o660
_MAXIMUM_DESCRIPTOR_BYTES = 16 * 1024
_MAXIMUM_ENDPOINT_GENERATION = (2**63) - 1
_ROOT_FIELDS = {
    "schema_version",
    "provider_instance_id",
    "endpoint_generation",
    "transport",
}
_TRANSPORT_FIELDS = {"type", "socket_path"}


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
        if status.st_size > _MAXIMUM_DESCRIPTOR_BYTES:
            raise ProviderDescriptorError(
                f"Provider descriptor exceeds {_MAXIMUM_DESCRIPTOR_BYTES} "
                f"bytes: {path}"
            )
        content = os.read(descriptor, _MAXIMUM_DESCRIPTOR_BYTES + 1)
    except OSError as error:
        raise ProviderDescriptorError(
            f"Cannot read Provider descriptor {path}: {error}"
        ) from error
    finally:
        os.close(descriptor)

    if not content:
        raise ProviderDescriptorError(f"Provider descriptor is empty: {path}")
    if len(content) > _MAXIMUM_DESCRIPTOR_BYTES:
        raise ProviderDescriptorError(
            f"Provider descriptor exceeds {_MAXIMUM_DESCRIPTOR_BYTES} "
            f"bytes: {path}"
        )
    return content, status


# Keep the complete schema visible as one linear security review surface.
def _parse_descriptor(  # noqa: PLR0912
    content: bytes,
    path: Path,
) -> ProviderEndpointDescriptor:
    """Parse and validate the complete descriptor document."""
    try:
        document = json.loads(
            content,
            object_pairs_hook=_reject_duplicate_fields,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderDescriptorError(
            f"Cannot parse Provider descriptor {path}: {error}"
        ) from error

    if not isinstance(document, dict):
        raise ProviderDescriptorError(
            f"Provider descriptor root must be a JSON object: {path}"
        )

    fields = set(document)
    missing = sorted(_ROOT_FIELDS - fields)
    unknown = sorted(fields - _ROOT_FIELDS)
    if missing:
        raise ProviderDescriptorError(
            f"Provider descriptor is missing fields: {', '.join(missing)}"
        )
    if unknown:
        raise ProviderDescriptorError(
            "Provider descriptor contains unknown fields: "
            f"{', '.join(unknown)}"
        )

    schema_version = document.get("schema_version")
    if not isinstance(schema_version, str) or not schema_version:
        raise ProviderDescriptorError(
            "Provider descriptor field schema_version must be a non-empty "
            "string"
        )
    if schema_version != _SCHEMA_VERSION:
        raise ProviderDescriptorError(
            f"Unsupported Provider descriptor schema_version: {schema_version}"
        )

    provider_instance_value = document.get("provider_instance_id")
    if not isinstance(provider_instance_value, str):
        raise ProviderDescriptorError(
            "provider_instance_id must be a canonical UUIDv4"
        )
    try:
        provider_instance_id = parse_canonical_uuid4(provider_instance_value)
    except ValueError as error:
        raise ProviderDescriptorError(
            "provider_instance_id must be a canonical UUIDv4"
        ) from error

    endpoint_generation = document.get("endpoint_generation")
    if (
        not isinstance(endpoint_generation, int)
        or isinstance(endpoint_generation, bool)
        or endpoint_generation < 1
        or endpoint_generation > _MAXIMUM_ENDPOINT_GENERATION
    ):
        raise ProviderDescriptorError(
            "endpoint_generation must be an integer from 1 through "
            f"{_MAXIMUM_ENDPOINT_GENERATION}"
        )

    transport = document.get("transport")
    if not isinstance(transport, dict):
        raise ProviderDescriptorError(
            "Provider descriptor field transport must be an object"
        )
    transport_fields = set(transport)
    missing = sorted(_TRANSPORT_FIELDS - transport_fields)
    unknown = sorted(transport_fields - _TRANSPORT_FIELDS)
    if missing:
        raise ProviderDescriptorError(
            f"Provider transport is missing fields: {', '.join(missing)}"
        )
    if unknown:
        raise ProviderDescriptorError(
            f"Provider transport contains unknown fields: {', '.join(unknown)}"
        )

    transport_type = transport.get("type")
    if transport_type != _TRANSPORT_TYPE:
        raise ProviderDescriptorError(
            f"Unsupported Provider endpoint transport: {transport_type}"
        )
    socket_path_value = transport.get("socket_path")
    if not isinstance(socket_path_value, str) or not socket_path_value:
        raise ProviderDescriptorError(
            "Provider descriptor field socket_path must be a non-empty string"
        )

    return ProviderEndpointDescriptor(
        schema_version=schema_version,
        provider_instance_id=provider_instance_id,
        endpoint_generation=endpoint_generation,
        socket_path=Path(socket_path_value),
    )


def _reject_duplicate_fields(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    """Build an object while rejecting ambiguous duplicate names."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProviderDescriptorError(
                f"Provider descriptor contains duplicate field: {key}"
            )
        result[key] = value
    return result


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
    if descriptor_mode != _DESCRIPTOR_MODE:
        raise ProviderDescriptorError(
            f"Provider descriptor mode must be {_DESCRIPTOR_MODE:#05o}, "
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

    expected_descriptor_name = (
        f"{descriptor.provider_instance_id}{PROVIDER_DESCRIPTOR_SUFFIX}"
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

    expected_socket_name = (
        f"{descriptor.provider_instance_id}."
        f"{descriptor.endpoint_generation}.sock"
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
    if socket_mode != _SOCKET_MODE:
        raise ProviderDescriptorError(
            f"Provider socket mode must be {_SOCKET_MODE:#05o}, "
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
