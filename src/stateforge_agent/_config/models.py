"""Immutable models for the effective Agent configuration."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

LogLevel: TypeAlias = Literal[
    "debug",
    "info",
    "warning",
    "error",
    "critical",
]
LogFormat: TypeAlias = Literal["text", "json"]
LogDestination: TypeAlias = Literal["stderr"]
JournalBackend: TypeAlias = Literal["sqlite"]


@dataclass(frozen=True, slots=True)
class AgentRuntimeConfiguration:
    """Filesystem locations owned by the Agent runtime."""

    data_directory: Path
    runtime_directory: Path
    staging_directory: Path


@dataclass(frozen=True, slots=True)
class ControlPlaneConfiguration:
    """Connection settings for the StateForge Control Plane."""

    url: str
    ca_certificate: Path
    connect_timeout_seconds: int
    request_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class ProviderConfiguration:
    """Local Provider discovery and monitoring settings."""

    discovery_directory: Path
    health_interval_seconds: int
    reconnect_initial_delay_seconds: int
    reconnect_max_delay_seconds: int


@dataclass(frozen=True, slots=True)
class JournalConfiguration:
    """Durable Agent journal settings."""

    backend: JournalBackend
    path: Path


@dataclass(frozen=True, slots=True)
class LoggingConfiguration:
    """Agent process logging settings."""

    level: LogLevel
    format: LogFormat
    destination: LogDestination


@dataclass(frozen=True, slots=True)
class AgentConfiguration:
    """Complete validated configuration consumed by the Agent."""

    schema_version: int
    agent: AgentRuntimeConfiguration
    control_plane: ControlPlaneConfiguration
    providers: ProviderConfiguration
    journal: JournalConfiguration
    logging: LoggingConfiguration
