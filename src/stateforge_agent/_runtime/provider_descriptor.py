"""Validate local SFP Provider endpoint descriptor files."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

PROVIDER_DESCRIPTOR_SUFFIX = ".sfp.json"

_SCHEMA_VERSION = "1"
_TRANSPORT_TYPE = "unix"
_DESCRIPTOR_MODE = 0o640
_SOCKET_MODE = 0o660
_MAXIMUM_DESCRIPTOR_BYTES = 16 * 1024
_MAXIMUM_ENDPOINT_GENERATION = (2**63) - 1
_UUID_VERSION = 4
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
    _validate_descriptor_metadata(
        path=path,
        discovery_directory=discovery_directory,
        status=descriptor_status,
    )

    document = _parse_document(content, path)
    _require_exact_fields(document, _ROOT_FIELDS, "descriptor")

    schema_version = _require_string(document, "schema_version")
    if schema_version != _SCHEMA_VERSION:
        raise ProviderDescriptorError(
            f"Unsupported Provider descriptor schema_version: {schema_version}"
        )

    provider_instance_id = _require_uuid4(
        _require_string(document, "provider_instance_id")
    )
    endpoint_generation = _require_generation(
        document.get("endpoint_generation")
    )
    transport = _require_object(document, "transport")
    _require_exact_fields(transport, _TRANSPORT_FIELDS, "transport")
    transport_type = _require_string(transport, "type")
    if transport_type != _TRANSPORT_TYPE:
        raise ProviderDescriptorError(
            f"Unsupported Provider endpoint transport: {transport_type}"
        )
    socket_path = Path(_require_string(transport, "socket_path"))

    _validate_paths(
        descriptor_path=path,
        discovery_directory=discovery_directory,
        provider_instance_id=provider_instance_id,
        endpoint_generation=endpoint_generation,
        socket_path=socket_path,
    )
    _validate_socket(
        socket_path=socket_path,
        descriptor_status=descriptor_status,
    )
    return ProviderEndpointDescriptor(
        schema_version=schema_version,
        provider_instance_id=provider_instance_id,
        endpoint_generation=endpoint_generation,
        socket_path=socket_path,
    )


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


def _validate_descriptor_metadata(
    *,
    path: Path,
    discovery_directory: Path,
    status: os.stat_result,
) -> None:
    """Validate descriptor permissions and protected-directory membership."""
    mode = stat.S_IMODE(status.st_mode)
    if mode != _DESCRIPTOR_MODE:
        raise ProviderDescriptorError(
            f"Provider descriptor mode must be {_DESCRIPTOR_MODE:#05o}, "
            f"found {mode:#05o}: {path}"
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
    if status.st_gid != directory_status.st_gid:
        raise ProviderDescriptorError(
            "Provider descriptor group must match the discovery directory: "
            f"{path}"
        )


def _parse_document(content: bytes, path: Path) -> dict[str, object]:
    """Decode one strict JSON object and reject duplicate fields."""
    try:
        parsed = json.loads(
            content,
            object_pairs_hook=_reject_duplicate_fields,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderDescriptorError(
            f"Cannot parse Provider descriptor {path}: {error}"
        ) from error

    if not isinstance(parsed, dict):
        raise ProviderDescriptorError(
            f"Provider descriptor root must be a JSON object: {path}"
        )
    return parsed


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


def _require_exact_fields(
    value: dict[str, object],
    expected: set[str],
    location: str,
) -> None:
    """Reject missing and unknown descriptor fields."""
    fields = set(value)
    missing = sorted(expected - fields)
    unknown = sorted(fields - expected)
    if missing:
        raise ProviderDescriptorError(
            f"Provider {location} is missing fields: {', '.join(missing)}"
        )
    if unknown:
        raise ProviderDescriptorError(
            f"Provider {location} contains unknown fields: "
            f"{', '.join(unknown)}"
        )


def _require_string(value: dict[str, object], field: str) -> str:
    """Read one non-empty descriptor string."""
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise ProviderDescriptorError(
            f"Provider descriptor field {field} must be a non-empty string"
        )
    return result


def _require_object(
    value: dict[str, object],
    field: str,
) -> dict[str, object]:
    """Read one descriptor object."""
    result = value.get(field)
    if not isinstance(result, dict):
        raise ProviderDescriptorError(
            f"Provider descriptor field {field} must be an object"
        )
    return result


def _require_uuid4(value: str) -> UUID:
    """Read a canonical lowercase UUIDv4 Provider instance identity."""
    try:
        result = UUID(value)
    except ValueError as error:
        raise ProviderDescriptorError(
            "provider_instance_id must be a canonical UUIDv4"
        ) from error
    if result.version != _UUID_VERSION or str(result) != value:
        raise ProviderDescriptorError(
            "provider_instance_id must be a canonical UUIDv4"
        )
    return result


def _require_generation(value: object) -> int:
    """Read a positive signed 64-bit endpoint generation."""
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > _MAXIMUM_ENDPOINT_GENERATION
    ):
        raise ProviderDescriptorError(
            "endpoint_generation must be an integer from 1 through "
            f"{_MAXIMUM_ENDPOINT_GENERATION}"
        )
    return value


def _validate_paths(
    *,
    descriptor_path: Path,
    discovery_directory: Path,
    provider_instance_id: UUID,
    endpoint_generation: int,
    socket_path: Path,
) -> None:
    """Bind descriptor and socket names to the declared endpoint identity."""
    if not discovery_directory.is_absolute():
        raise ProviderDescriptorError(
            "Provider discovery_directory must be absolute"
        )
    if descriptor_path.parent != discovery_directory:
        raise ProviderDescriptorError(
            "Provider descriptor must be directly inside the discovery "
            "directory"
        )

    expected_descriptor_name = (
        f"{provider_instance_id}{PROVIDER_DESCRIPTOR_SUFFIX}"
    )
    if descriptor_path.name != expected_descriptor_name:
        raise ProviderDescriptorError(
            f"Provider descriptor filename must be {expected_descriptor_name}"
        )

    if not socket_path.is_absolute():
        raise ProviderDescriptorError("Provider socket_path must be absolute")
    if socket_path.parent != discovery_directory:
        raise ProviderDescriptorError(
            "Provider socket_path must be directly inside the discovery "
            "directory"
        )

    expected_socket_name = f"{provider_instance_id}.{endpoint_generation}.sock"
    if socket_path.name != expected_socket_name:
        raise ProviderDescriptorError(
            f"Provider socket filename must be {expected_socket_name}"
        )


def _validate_socket(
    *,
    socket_path: Path,
    descriptor_status: os.stat_result,
) -> None:
    """Bind a descriptor to an existing protected Unix-domain socket."""
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
