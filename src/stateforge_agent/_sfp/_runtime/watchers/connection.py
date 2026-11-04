"""Monitor Provider channels and retry desired endpoint connections."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any
from uuid import UUID

import grpc

from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import (
    SfpConnectionError,
    SfpIncompatibleError,
    SfpManifestError,
)
from stateforge_agent._sfp._runtime.models import ProviderSession

_LOGGER = logging.getLogger(__name__)
_FAILED_CHANNEL_STATES = {
    grpc.ChannelConnectivity.TRANSIENT_FAILURE,
    grpc.ChannelConnectivity.SHUTDOWN,
}
_JITTER_STEPS = 1_000_000

LoadRetryDescriptor = Callable[
    [UUID], Awaitable[ProviderEndpointDescriptor | None]
]
EstablishEndpoint = Callable[
    [ProviderEndpointDescriptor], Awaitable[ProviderSession]
]
MarkConnectionFailure = Callable[[ProviderEndpointDescriptor], Awaitable[None]]
ChannelLost = Callable[[ProviderSession], Awaitable[None]]
RetryLoop = Callable[[UUID], Coroutine[Any, Any, None]]


class ProviderConnectionWatcher:
    """Run channel monitoring and retry loops without owning session state."""

    def __init__(
        self,
        *,
        retry_initial_delay_seconds: float,
        retry_max_delay_seconds: float,
        load_retry_descriptor: LoadRetryDescriptor,
        establish_endpoint: EstablishEndpoint,
        mark_incompatible: MarkConnectionFailure,
        mark_manifest_rejected: MarkConnectionFailure,
        on_retry_finished: Callable[[UUID, bool, RetryLoop], Awaitable[None]],
    ) -> None:
        """Bind reconnect work to manager-owned state transitions."""
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds
        self._load_retry_descriptor = load_retry_descriptor
        self._establish_endpoint = establish_endpoint
        self._mark_incompatible = mark_incompatible
        self._mark_manifest_rejected = mark_manifest_rejected
        self._on_retry_finished = on_retry_finished

    async def watch_channel(
        self,
        session: ProviderSession,
        *,
        on_lost: ChannelLost,
    ) -> None:
        """Report loss of this exact session's gRPC channel."""
        try:
            state = session._channel.get_state(try_to_connect=False)
            while state not in _FAILED_CHANNEL_STATES:
                if state is grpc.ChannelConnectivity.IDLE:
                    state = session._channel.get_state(try_to_connect=True)
                    if state in _FAILED_CHANNEL_STATES:
                        break
                await session._channel.wait_for_state_change(state)
                state = session._channel.get_state(try_to_connect=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception(
                "Provider channel watcher failed: provider_instance_id=%s "
                "endpoint_generation=%s",
                session.provider_instance_id,
                session.endpoint_generation,
            )
        await on_lost(session)

    async def retry(self, provider_instance_id: UUID) -> None:
        """Retry the desired endpoint with capped exponential backoff."""
        attempt_number = 0
        connected = False
        try:
            while True:
                delay = self._retry_delay(attempt_number)
                _LOGGER.info(
                    "Scheduling Provider reconnect: provider_instance_id=%s "
                    "attempt=%s delay_seconds=%.3f",
                    provider_instance_id,
                    attempt_number + 1,
                    delay,
                )
                await asyncio.sleep(delay)
                try:
                    descriptor = await self._load_retry_descriptor(
                        provider_instance_id
                    )
                except Exception as error:
                    _LOGGER.warning(
                        "Cannot reload Provider descriptor for reconnect: "
                        "provider_instance_id=%s error=%s",
                        provider_instance_id,
                        error,
                    )
                    attempt_number += 1
                    continue
                if descriptor is None:
                    return
                try:
                    await self._establish_endpoint(descriptor)
                except SfpIncompatibleError as error:
                    _LOGGER.warning(
                        "Provider is SFP-incompatible: "
                        "provider_instance_id=%s endpoint_generation=%s "
                        "error=%s",
                        provider_instance_id,
                        descriptor.endpoint_generation,
                        error,
                    )
                    await self._mark_incompatible(descriptor)
                    return
                except SfpManifestError as error:
                    _LOGGER.warning(
                        "Provider manifest rejected: "
                        "provider_instance_id=%s endpoint_generation=%s "
                        "error=%s",
                        provider_instance_id,
                        descriptor.endpoint_generation,
                        error,
                    )
                    await self._mark_manifest_rejected(descriptor)
                    return
                except SfpConnectionError as error:
                    _LOGGER.warning(
                        "Provider reconnect failed: provider_instance_id=%s "
                        "endpoint_generation=%s error=%s",
                        provider_instance_id,
                        descriptor.endpoint_generation,
                        error,
                    )
                    attempt_number += 1
                    continue
                connected = True
                return
        finally:
            await self._on_retry_finished(
                provider_instance_id,
                connected,
                self.retry,
            )

    def _retry_delay(self, attempt_number: int) -> float:
        """Return capped exponential backoff with equal positive jitter."""
        ceiling = self._retry_initial_delay_seconds
        for _ in range(attempt_number):
            if ceiling >= self._retry_max_delay_seconds:
                break
            ceiling = min(ceiling * 2, self._retry_max_delay_seconds)
        half_ceiling = ceiling / 2
        jitter_fraction = secrets.randbelow(_JITTER_STEPS + 1) / _JITTER_STEPS
        return half_ceiling + half_ceiling * jitter_fraction
