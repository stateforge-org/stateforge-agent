"""Define immutable Provider session and status snapshots."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from uuid import UUID

import grpc

from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestDeclaration,
)
from stateforge_agent._sfp.version import SfpVersion


class ProviderConnectionState(StrEnum):
    """Describe the Agent's local connection state for one Provider."""

    UNAVAILABLE = "unavailable"
    CONNECTING = "connecting"
    HANDSHAKING = "handshaking"
    RETRIEVING_MANIFEST = "retrieving_manifest"
    CONNECTED = "connected"
    INCOMPATIBLE = "incompatible"
    MANIFEST_REJECTED = "manifest_rejected"
    REPLACING_CONNECTING = "replacing_connecting"
    REPLACING_HANDSHAKING = "replacing_handshaking"
    REPLACING_RETRIEVING_MANIFEST = "replacing_retrieving_manifest"


class ProviderAvailabilityState(StrEnum):
    """Describe whether one current Provider may accept new operations."""

    UNAVAILABLE = "unavailable"
    WATCHING = "watching"
    AVAILABLE = "available"
    UNKNOWN = "unknown"
    NOT_SERVING = "not_serving"
    SERVICE_UNKNOWN = "service_unknown"
    WATCH_UNSUPPORTED = "watch_unsupported"


@dataclass(frozen=True, slots=True)
class ProviderSession:
    """Represent one negotiated session bound to an endpoint generation."""

    provider_instance_id: UUID
    endpoint_generation: int
    socket_path: Path
    selected_sfp_version: SfpVersion
    provider_runtime_id: UUID
    session_id: str
    manifest: ManifestDeclaration
    _channel: grpc.aio.Channel = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class ProviderConnectionStatus:
    """Expose an immutable snapshot of local Provider connection state."""

    provider_instance_id: UUID
    state: ProviderConnectionState
    active_endpoint_generation: int | None
    candidate_endpoint_generation: int | None


@dataclass(frozen=True, slots=True)
class ProviderAvailabilityStatus:
    """Expose an immutable snapshot of Provider operation eligibility."""

    provider_instance_id: UUID
    state: ProviderAvailabilityState
    endpoint_generation: int | None
    session_id: str | None
    manifest_revision: str | None
    reason: str

    @property
    def eligible(self) -> bool:
        """Return whether new operation admission is currently allowed."""
        return self.state is ProviderAvailabilityState.AVAILABLE


@dataclass(slots=True)
class ProviderRuntimeState:
    """Store the mutable state for one Provider registry entry."""

    desired_descriptor: ProviderEndpointDescriptor | None = None
    active_session: ProviderSession | None = None
    connection_status: ProviderConnectionStatus | None = None
    availability_status: ProviderAvailabilityStatus | None = None
    attempt_token: object | None = None
    channel_watcher: asyncio.Task[None] | None = None
    health_watcher: asyncio.Task[None] | None = None
    trigger_watcher: asyncio.Task[None] | None = None
    retry_task: asyncio.Task[None] | None = None
    availability_generation: int = 0
