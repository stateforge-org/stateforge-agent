"""Observe Provider Health and report fenced availability changes."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable

import grpc

from stateforge_agent._sfp._runtime.models import (
    ProviderAvailabilityState,
    ProviderSession,
)
from stateforge_agent._sfp._services.health.client import (
    HealthWatcher,
    SfpHealthError,
    SfpHealthWatchError,
)

_LOGGER = logging.getLogger(__name__)
_JITTER_STEPS = 1_000_000

MarkHealthWatching = Callable[[ProviderSession], Awaitable[bool]]
ApplyHealthStatus = Callable[[ProviderSession, int], Awaitable[bool]]
SetAvailability = Callable[
    [ProviderSession, ProviderAvailabilityState, str], Awaitable[bool]
]
WatcherFinished = Callable[[ProviderSession], Awaitable[None]]


class ProviderHealthMonitor:
    """Run a retrying Health Watch without owning session state."""

    def __init__(
        self,
        *,
        client: HealthWatcher,
        retry_initial_delay_seconds: float,
        retry_max_delay_seconds: float,
        mark_watching: MarkHealthWatching,
        apply_status: ApplyHealthStatus,
        set_availability: SetAvailability,
        on_finished: WatcherFinished,
    ) -> None:
        """Bind stream handling to manager-owned, fenced state callbacks."""
        self._client = client
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds
        self._mark_watching = mark_watching
        self._apply_status = apply_status
        self._set_availability = set_availability
        self._on_finished = on_finished

    async def watch(self, session: ProviderSession) -> None:
        """Maintain one fenced Provider-wide Health Watch with backoff."""
        attempt_number = 0
        try:
            while await self._mark_watching(session):
                try:
                    async for status in self._client.watch(session._channel):
                        attempt_number = 0
                        if not await self._apply_status(session, status):
                            return
                    reason = "Provider Health Watch ended"
                except asyncio.CancelledError:
                    raise
                except SfpHealthWatchError as error:
                    code = error.code
                    reason = f"Provider Health Watch failed: {code.name}"
                    if code is grpc.StatusCode.UNIMPLEMENTED:
                        await self._set_availability(
                            session,
                            ProviderAvailabilityState.WATCH_UNSUPPORTED,
                            reason,
                        )
                        return
                except SfpHealthError:
                    reason = "Provider Health Watch returned invalid data"
                except Exception:
                    _LOGGER.exception(
                        "Provider Health Watch failed locally: "
                        "provider_instance_id=%s endpoint_generation=%s",
                        session.provider_instance_id,
                        session.endpoint_generation,
                    )
                    reason = "Provider Health Watch failed locally"

                if not await self._set_availability(
                    session,
                    ProviderAvailabilityState.UNAVAILABLE,
                    reason,
                ):
                    return
                delay = self._retry_delay(attempt_number)
                attempt_number += 1
                _LOGGER.info(
                    "Scheduling Provider Health Watch retry: "
                    "provider_instance_id=%s endpoint_generation=%s "
                    "attempt=%s delay_seconds=%.3f",
                    session.provider_instance_id,
                    session.endpoint_generation,
                    attempt_number,
                    delay,
                )
                await asyncio.sleep(delay)
        finally:
            await self._on_finished(session)

    def _retry_delay(self, attempt_number: int) -> float:
        """Return a capped jittered delay for a failed Health Watch."""
        ceiling = min(
            self._retry_initial_delay_seconds,
            self._retry_max_delay_seconds,
        )
        for _ in range(attempt_number):
            if ceiling >= self._retry_max_delay_seconds:
                break
            ceiling = min(ceiling * 2, self._retry_max_delay_seconds)
        half_ceiling = ceiling / 2
        jitter_fraction = secrets.randbelow(_JITTER_STEPS + 1) / _JITTER_STEPS
        return half_ceiling + half_ceiling * jitter_fraction
