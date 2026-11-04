"""Schedule Provider reconnects against the manager-owned registry."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, MutableMapping
from typing import Any
from uuid import UUID

from stateforge_agent._sfp._runtime.availability import (
    ProviderAvailabilityController,
)
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import SfpConnectionError
from stateforge_agent._sfp._runtime.models import (
    ProviderConnectionState,
    ProviderConnectionStatus,
    ProviderRuntimeState,
)
from stateforge_agent._sfp._runtime.state import (
    _connected_status,
    _unavailable_status,
)

ProviderDescriptorLoader = Callable[[UUID], ProviderEndpointDescriptor]
RetryLoop = Callable[[UUID], Coroutine[Any, Any, None]]


class ProviderReconnectController:
    """Own retry-task coordination without owning a second state registry."""

    def __init__(
        self,
        *,
        lock: asyncio.Lock,
        providers: MutableMapping[UUID, ProviderRuntimeState],
        is_closed: Callable[[], bool],
        descriptor_loader: ProviderDescriptorLoader | None,
        availability: ProviderAvailabilityController,
    ) -> None:
        """Bind retry coordination to the session manager's state and lock."""
        self._lock = lock
        self._providers = providers
        self._is_closed = is_closed
        self._descriptor_loader = descriptor_loader
        self._availability = availability

    async def schedule_retry(
        self,
        provider_instance_id: UUID,
        retry_loop: RetryLoop,
    ) -> None:
        """Ensure exactly one retry loop exists for a desired Provider."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                self._is_closed()
                or provider is None
                or provider.desired_descriptor is None
                or provider.attempt_token is not None
            ):
                return
            existing = provider.retry_task
            if existing is not None and not existing.done():
                return
            provider.retry_task = asyncio.create_task(
                retry_loop(provider_instance_id),
                name=f"stateforge-sfp-retry-{provider_instance_id}",
            )

    async def finish_retry(
        self,
        provider_instance_id: UUID,
        connected: bool,
        retry_loop: RetryLoop,
    ) -> None:
        """Clear its task slot and retry if a newer target superseded it."""
        reschedule = False
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                provider is not None
                and provider.retry_task is asyncio.current_task()
            ):
                provider.retry_task = None
                desired = provider.desired_descriptor
                active = provider.active_session
                reschedule = (
                    connected
                    and not self._is_closed()
                    and desired is not None
                    and (
                        active is None
                        or active.endpoint_generation
                        < desired.endpoint_generation
                    )
                )
        if reschedule:
            await self.schedule_retry(provider_instance_id, retry_loop)

    async def load_retry_descriptor(
        self,
        provider_instance_id: UUID,
    ) -> ProviderEndpointDescriptor | None:
        """Reload and retain the current validated retry target."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                self._is_closed()
                or provider is None
                or provider.desired_descriptor is None
            ):
                return None
            desired = provider.desired_descriptor
            loader = self._descriptor_loader
        if loader is None:
            return desired

        loaded = loader(provider_instance_id)
        if loaded.provider_instance_id != provider_instance_id:
            raise SfpConnectionError(
                "Reloaded descriptor returned a different Provider identity"
            )
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                self._is_closed()
                or provider is None
                or provider.desired_descriptor is None
            ):
                return None
            current = provider.desired_descriptor
            if loaded.endpoint_generation < current.endpoint_generation:
                raise SfpConnectionError(
                    "Reloaded descriptor endpoint generation moved backwards"
                )
            provider.desired_descriptor = loaded
        return loaded

    async def mark_incompatible(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> None:
        """Record incompatibility unless an older session still survives."""
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                ProviderRuntimeState(),
            )
            session = provider.active_session
            if session is not None:
                provider.connection_status = _connected_status(session)
                return
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=ProviderConnectionState.INCOMPATIBLE,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )
            self._availability.record_availability(
                provider,
                _unavailable_status(
                    descriptor.provider_instance_id,
                    "Provider is SFP-incompatible",
                ),
            )

    async def mark_manifest_rejected(
        self,
        descriptor: ProviderEndpointDescriptor,
    ) -> None:
        """Record rejection unless an older validated session survives."""
        async with self._lock:
            provider = self._providers.setdefault(
                descriptor.provider_instance_id,
                ProviderRuntimeState(),
            )
            session = provider.active_session
            if session is not None:
                provider.connection_status = _connected_status(session)
                return
            provider.connection_status = ProviderConnectionStatus(
                provider_instance_id=descriptor.provider_instance_id,
                state=ProviderConnectionState.MANIFEST_REJECTED,
                active_endpoint_generation=None,
                candidate_endpoint_generation=None,
            )
            self._availability.record_availability(
                provider,
                _unavailable_status(
                    descriptor.provider_instance_id,
                    "Provider manifest was rejected",
                ),
            )
