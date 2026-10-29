"""Load, merge, and validate StateForge Agent configuration files."""

from __future__ import annotations

import copy
import hashlib
import json
import tomllib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TypeAlias, cast
from urllib.parse import urlsplit

from stateforge_protocol.sfp.trigger_publication.v1 import (
    contract as trigger_publication_contract,
)

from stateforge_agent._config.errors import (
    ConfigurationFileError,
    ConfigurationValidationError,
)
from stateforge_agent._config.models import (
    AgentConfiguration,
    ControlPlaneConfiguration,
    JournalConfiguration,
    LogDestination,
    LogFormat,
    LoggingConfiguration,
    LogLevel,
    ProviderConfiguration,
)
from stateforge_agent._journal.registry import JOURNAL_BACKENDS

ConfigValue: TypeAlias = (
    str | int | float | bool | list["ConfigValue"] | dict[str, "ConfigValue"]
)
ConfigTable: TypeAlias = dict[str, ConfigValue]
ConfigPath: TypeAlias = tuple[str, ...]
Provenance: TypeAlias = dict[ConfigPath, Path]

_SUPPORTED_SCHEMA_VERSION = 1
_TOP_LEVEL_KEYS = {
    "schema_version",
    "control_plane",
    "providers",
    "journal",
    "logging",
}


@dataclass(frozen=True, slots=True)
class LoadedConfiguration:
    """Validated configuration and metadata about its effective sources."""

    config: AgentConfiguration
    sources: tuple[Path, ...]
    fingerprint: str


def load_configuration(
    config_path: str | Path,
    *,
    drop_in_directory: str | Path | None = None,
) -> LoadedConfiguration:
    """Load the main file and its lexically ordered TOML drop-ins.

    Args:
        config_path: Required primary TOML configuration file.
        drop_in_directory: Optional directory containing ``*.toml``
            overrides. It defaults to ``conf.d`` beside the primary file.

    Returns:
        A complete immutable configuration and its source metadata.

    Raises:
        ConfigurationFileError: A source cannot be read or parsed.
        ConfigurationValidationError: The effective configuration is invalid.
    """
    primary_path = Path(config_path)
    overrides_path = (
        Path(drop_in_directory)
        if drop_in_directory is not None
        else primary_path.parent / "conf.d"
    )

    source_paths = (primary_path, *_find_drop_ins(overrides_path))
    effective: ConfigTable = {}
    provenance: Provenance = {}

    for source_path in source_paths:
        source = _read_toml(source_path)
        _merge_table(effective, source, source_path, provenance)

    config = _validate_configuration(effective, provenance, primary_path)
    return LoadedConfiguration(
        config=config,
        sources=source_paths,
        fingerprint=_fingerprint(config),
    )


def _find_drop_ins(directory: Path) -> tuple[Path, ...]:
    """Return regular TOML drop-ins in deterministic filename order."""
    if not directory.exists():
        return ()
    if not directory.is_dir():
        raise ConfigurationFileError(
            f"Configuration drop-in path is not a directory: {directory}"
        )

    return tuple(
        sorted(
            (path for path in directory.glob("*.toml") if path.is_file()),
            key=lambda path: path.name,
        )
    )


def _read_toml(path: Path) -> ConfigTable:
    """Read a TOML source and normalize parser failures."""
    if not path.exists():
        raise ConfigurationFileError(
            f"Configuration file does not exist: {path}"
        )
    if not path.is_file():
        raise ConfigurationFileError(
            f"Configuration source is not a regular file: {path}"
        )

    try:
        with path.open("rb") as stream:
            parsed = tomllib.load(stream)
    except OSError as error:
        raise ConfigurationFileError(
            f"Cannot read configuration file {path}: {error}"
        ) from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigurationFileError(
            f"Cannot parse configuration file {path}: {error}"
        ) from error

    return cast(ConfigTable, parsed)


def _merge_table(
    destination: ConfigTable,
    source: ConfigTable,
    source_path: Path,
    provenance: Provenance,
    prefix: ConfigPath = (),
) -> None:
    """Deep-merge a source table while recording leaf ownership."""
    for key, source_value in source.items():
        value_path = (*prefix, key)
        destination_value = destination.get(key)

        if isinstance(destination_value, dict) and isinstance(
            source_value, dict
        ):
            _merge_table(
                destination_value,
                source_value,
                source_path,
                provenance,
                value_path,
            )
            continue

        destination[key] = copy.deepcopy(source_value)
        _clear_provenance(provenance, value_path)
        _record_provenance(source_value, source_path, provenance, value_path)


def _clear_provenance(provenance: Provenance, prefix: ConfigPath) -> None:
    """Remove ownership records replaced by an override."""
    for value_path in tuple(provenance):
        if value_path[: len(prefix)] == prefix:
            del provenance[value_path]


def _record_provenance(
    value: ConfigValue,
    source_path: Path,
    provenance: Provenance,
    value_path: ConfigPath,
) -> None:
    """Associate effective leaf values with their defining source."""
    if isinstance(value, dict) and value:
        for key, child in value.items():
            _record_provenance(
                child,
                source_path,
                provenance,
                (*value_path, key),
            )
        return
    provenance[value_path] = source_path


class _Validator:
    """Accumulate strict, source-aware configuration validation issues."""

    def __init__(
        self,
        data: ConfigTable,
        provenance: Mapping[ConfigPath, Path],
        primary_path: Path,
    ) -> None:
        self._data = data
        self._provenance = provenance
        self._primary_path = primary_path
        self.issues: list[str] = []

    def table(
        self,
        path: ConfigPath,
        *,
        allowed: set[str],
        required: set[str],
    ) -> ConfigTable:
        """Read a required table and enforce its key contract."""
        value = self._value(path)
        if not isinstance(value, dict):
            if value is None:
                self.add_issue(path, "required table is missing")
            else:
                self.add_issue(path, "must be a table")
            return {}

        actual_keys = set(value)
        for key in sorted(actual_keys - allowed):
            self.add_issue((*path, key), "unknown configuration key")
        for key in sorted(required - actual_keys):
            self.add_issue((*path, key), "required value is missing")
        return value

    def string(self, path: ConfigPath) -> str:
        """Read a required string value."""
        value = self._value(path)
        if not isinstance(value, str):
            if value is not None:
                self.add_issue(path, "must be a string")
            return ""
        if not value.strip():
            self.add_issue(path, "must not be empty")
        return value

    def integer(self, path: ConfigPath) -> int:
        """Read a required integer value."""
        value = self._value(path)
        if not isinstance(value, int) or isinstance(value, bool):
            if value is not None:
                self.add_issue(path, "must be an integer")
            return 0
        return value

    def positive_integer(self, path: ConfigPath) -> int:
        """Read an integer greater than zero."""
        value = self.integer(path)
        if value <= 0 and self._value(path) is not None:
            self.add_issue(path, "must be greater than zero")
        return value

    def absolute_path(self, path: ConfigPath) -> Path:
        """Read an absolute filesystem path."""
        value = self.string(path)
        result = Path(value)
        if value and not result.is_absolute():
            self.add_issue(path, "must be an absolute path")
        return result

    def choice(self, path: ConfigPath, choices: set[str]) -> str:
        """Read a string constrained to explicit supported values."""
        value = self.string(path)
        if value and value not in choices:
            supported = ", ".join(sorted(choices))
            self.add_issue(path, f"must be one of: {supported}")
        return value

    def _value(self, path: ConfigPath) -> ConfigValue | None:
        current: ConfigValue = self._data
        for segment in path:
            if not isinstance(current, dict) or segment not in current:
                return None
            current = current[segment]
        return current

    def add_issue(self, path: ConfigPath, message: str) -> None:
        """Add a validation issue attributed to its defining source."""
        source = self._source(path)
        dotted_path = ".".join(path) if path else "<root>"
        self.issues.append(f"{dotted_path} ({source}): {message}")

    def _source(self, path: ConfigPath) -> Path:
        current_path = path
        while current_path:
            if current_path in self._provenance:
                return self._provenance[current_path]
            current_path = current_path[:-1]
        return self._primary_path


def _validate_configuration(
    data: ConfigTable,
    provenance: Mapping[ConfigPath, Path],
    primary_path: Path,
) -> AgentConfiguration:
    """Convert an effective raw table into immutable typed models."""
    validator = _Validator(data, provenance, primary_path)
    validator.table(
        (),
        allowed=_TOP_LEVEL_KEYS,
        required=_TOP_LEVEL_KEYS,
    )
    validator.table(
        ("control_plane",),
        allowed={
            "url",
            "ca_certificate",
            "connect_timeout_seconds",
            "request_timeout_seconds",
        },
        required={
            "url",
            "ca_certificate",
            "connect_timeout_seconds",
            "request_timeout_seconds",
        },
    )
    validator.table(
        ("providers",),
        allowed={
            "health_interval_seconds",
            "reconnect_initial_delay_seconds",
            "reconnect_max_delay_seconds",
            "trigger_max_in_flight_publications",
        },
        required={
            "health_interval_seconds",
            "reconnect_initial_delay_seconds",
            "reconnect_max_delay_seconds",
            "trigger_max_in_flight_publications",
        },
    )
    validator.table(
        ("journal",),
        allowed={"backend", "maximum_pending_trigger_publications"},
        required={"backend", "maximum_pending_trigger_publications"},
    )
    validator.table(
        ("logging",),
        allowed={"level", "format", "destination"},
        required={"level", "format", "destination"},
    )

    schema_version = validator.integer(("schema_version",))
    if schema_version != _SUPPORTED_SCHEMA_VERSION:
        validator.add_issue(
            ("schema_version",),
            f"unsupported version {schema_version}; expected "
            f"{_SUPPORTED_SCHEMA_VERSION}",
        )

    control_plane_url = validator.string(("control_plane", "url"))
    _validate_control_plane_url(validator, control_plane_url)
    ca_certificate = validator.absolute_path(
        ("control_plane", "ca_certificate")
    )
    connect_timeout = validator.positive_integer(
        ("control_plane", "connect_timeout_seconds")
    )
    request_timeout = validator.positive_integer(
        ("control_plane", "request_timeout_seconds")
    )

    health_interval = validator.positive_integer(
        ("providers", "health_interval_seconds")
    )
    reconnect_initial_delay = validator.positive_integer(
        ("providers", "reconnect_initial_delay_seconds")
    )
    reconnect_max_delay = validator.positive_integer(
        ("providers", "reconnect_max_delay_seconds")
    )
    trigger_max_in_flight = validator.positive_integer(
        ("providers", "trigger_max_in_flight_publications")
    )
    if (
        trigger_max_in_flight
        > trigger_publication_contract.MAX_IN_FLIGHT_PUBLICATIONS
    ):
        validator.add_issue(
            ("providers", "trigger_max_in_flight_publications"),
            "must not exceed the SFP Trigger Publication receive-window "
            "limit of "
            f"{trigger_publication_contract.MAX_IN_FLIGHT_PUBLICATIONS}",
        )
    if (
        reconnect_initial_delay > 0
        and reconnect_max_delay > 0
        and reconnect_max_delay < reconnect_initial_delay
    ):
        validator.add_issue(
            ("providers", "reconnect_max_delay_seconds"),
            "must be greater than or equal to "
            "providers.reconnect_initial_delay_seconds",
        )

    journal_backend = validator.choice(
        ("journal", "backend"),
        set(JOURNAL_BACKENDS.backend_names),
    )
    maximum_pending_trigger_publications = validator.positive_integer(
        ("journal", "maximum_pending_trigger_publications")
    )

    log_level = validator.choice(
        ("logging", "level"),
        {"debug", "info", "warning", "error", "critical"},
    )
    log_format = validator.choice(("logging", "format"), {"text", "json"})
    log_destination = validator.choice(("logging", "destination"), {"stderr"})

    if validator.issues:
        raise ConfigurationValidationError(validator.issues)

    return AgentConfiguration(
        schema_version=schema_version,
        control_plane=ControlPlaneConfiguration(
            url=control_plane_url,
            ca_certificate=ca_certificate,
            connect_timeout_seconds=connect_timeout,
            request_timeout_seconds=request_timeout,
        ),
        providers=ProviderConfiguration(
            health_interval_seconds=health_interval,
            reconnect_initial_delay_seconds=reconnect_initial_delay,
            reconnect_max_delay_seconds=reconnect_max_delay,
            trigger_max_in_flight_publications=trigger_max_in_flight,
        ),
        journal=JournalConfiguration(
            backend=journal_backend,
            maximum_pending_trigger_publications=(
                maximum_pending_trigger_publications
            ),
        ),
        logging=LoggingConfiguration(
            level=cast(LogLevel, log_level),
            format=cast(LogFormat, log_format),
            destination=cast(LogDestination, log_destination),
        ),
    )


def _validate_control_plane_url(
    validator: _Validator,
    value: str,
) -> None:
    """Enforce an HTTPS Control Plane base URL without embedded secrets."""
    if not value:
        return
    parsed = urlsplit(value)
    path = ("control_plane", "url")
    if parsed.scheme != "https" or not parsed.hostname:
        validator.add_issue(path, "must be an absolute HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        validator.add_issue(
            path,
            "must not contain embedded credentials",
        )
    if parsed.query or parsed.fragment:
        validator.add_issue(
            path,
            "must not contain a query string or fragment",
        )


def _fingerprint(config: AgentConfiguration) -> str:
    """Create a stable non-secret identity for an effective configuration."""
    serialized = json.dumps(
        asdict(config),
        default=str,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(serialized).hexdigest()
    return f"sha256:{digest}"
