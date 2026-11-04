"""Establish provisional Provider sessions before manager-owned promotion."""

from __future__ import annotations

import asyncio
from typing import Protocol
from uuid import UUID

import grpc

from stateforge_agent._sfp._runtime.channel import create_provider_channel
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import SfpConnectionError
from stateforge_agent._sfp._runtime.models import ProviderSession
from stateforge_agent._sfp._services.bootstrap.client import perform_handshake
from stateforge_agent._sfp._services.manifest.client import ManifestRetriever
from stateforge_agent._sfp.version import SfpVersion


class ProviderConnectionOwner(Protocol):
    """Expose manager-owned transitions needed during connection setup."""

    async def _begin_attempt(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> tuple[ProviderSession | None, object | None]: ...

    async def _mark_handshaking(
        self,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> bool: ...

    async def _mark_retrieving_manifest(
        self,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> bool: ...

    async def _promote_candidate(
        self,
        *,
        candidate: ProviderSession,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> tuple[ProviderSession, ProviderSession | None, bool]: ...

    async def _connection_attempt_failed(
        self,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> None: ...

    async def _activate_candidate(
        self,
        session: ProviderSession,
        replaced_session: ProviderSession | None,
        *,
        promoted: bool,
    ) -> None: ...

    def get(self, provider_instance_id: UUID) -> ProviderSession | None: ...


class ProviderConnectionLifecycle:
    """Coordinate channel setup while delegating state changes to its owner."""

    def __init__(
        self,
        *,
        owner: ProviderConnectionOwner,
        agent_runtime_id: UUID,
        agent_software_version: str,
        supported_sfp_versions: tuple[SfpVersion, ...],
        channel_ready_timeout_seconds: float,
        handshake_timeout_seconds: float,
        manifest_client: ManifestRetriever,
    ) -> None:
        """Bind connection I/O to manager-owned endpoint/session fences."""
        self._owner = owner
        self._agent_runtime_id = agent_runtime_id
        self._agent_software_version = agent_software_version
        self._supported_sfp_versions = supported_sfp_versions
        self._channel_ready_timeout_seconds = channel_ready_timeout_seconds
        self._handshake_timeout_seconds = handshake_timeout_seconds
        self._manifest_client = manifest_client

    async def establish(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> ProviderSession:
        """Promote a validated channel only if its attempt remains current."""
        current_session, attempt = await self._owner._begin_attempt(descriptor)
        if current_session is not None:
            return current_session
        if attempt is None:
            raise RuntimeError("Connection attempt was not initialized")

        channel = create_provider_channel(f"unix:{descriptor.socket_path}")
        try:
            candidate = await self._connect_candidate(
                channel=channel,
                descriptor=descriptor,
                attempt=attempt,
            )
            if candidate is None:
                await channel.close()
                surviving_session = self._owner.get(
                    descriptor.provider_instance_id
                )
                if surviving_session is None:
                    raise SfpConnectionError(
                        "Provider connection attempt was superseded or removed"
                    )
                return surviving_session
            (
                result,
                replaced_session,
                promoted,
            ) = await self._owner._promote_candidate(
                candidate=candidate,
                descriptor=descriptor,
                attempt=attempt,
            )
        except BaseException:
            await channel.close()
            await self._owner._connection_attempt_failed(
                descriptor,
                attempt,
            )
            raise

        if not promoted:
            await channel.close()
        await self._owner._activate_candidate(
            result,
            replaced_session,
            promoted=promoted,
        )
        return result

    async def _connect_candidate(
        self,
        *,
        channel: grpc.aio.Channel,
        descriptor: ProviderEndpointDescriptor,
        attempt: object,
    ) -> ProviderSession | None:
        """Handshake and retrieve the manifest on one provisional channel."""
        try:
            await asyncio.wait_for(
                channel.channel_ready(),
                timeout=self._channel_ready_timeout_seconds,
            )
        except TimeoutError as error:
            raise SfpConnectionError(
                "Provider gRPC channel did not become ready before the "
                f"deadline: {descriptor.socket_path}"
            ) from error

        if not await self._owner._mark_handshaking(descriptor, attempt):
            return None

        handshake = await perform_handshake(
            channel,
            descriptor,
            agent_runtime_id=self._agent_runtime_id,
            agent_software_version=self._agent_software_version,
            supported_sfp_versions=self._supported_sfp_versions,
            timeout_seconds=self._handshake_timeout_seconds,
        )
        if not await self._owner._mark_retrieving_manifest(
            descriptor,
            attempt,
        ):
            return None
        manifest = await self._manifest_client.retrieve(
            channel,
            descriptor,
            sfp_version=handshake.selected_sfp_version,
            session_id=handshake.session_id,
        )
        return ProviderSession(
            provider_instance_id=descriptor.provider_instance_id,
            endpoint_generation=descriptor.endpoint_generation,
            socket_path=descriptor.socket_path,
            selected_sfp_version=handshake.selected_sfp_version,
            provider_runtime_id=handshake.provider_runtime_id,
            session_id=handshake.session_id,
            manifest=manifest,
            _channel=channel,
        )
