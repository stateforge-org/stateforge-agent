"""Consume Provider Trigger Publication streams for current sessions."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable

from stateforge_agent._sfp._runtime.models import ProviderSession
from stateforge_agent._sfp._services.trigger_publication.client import (
    SfpTriggerPublicationError,
    SfpTriggerPublicationStaleError,
    SfpTriggerPublicationUnavailableError,
)
from stateforge_agent._sfp._services.trigger_publication.coordinator import (
    TriggerPublicationCoordinator,
    TriggerPublicationLease,
)

_LOGGER = logging.getLogger(__name__)
_JITTER_STEPS = 1_000_000

AssertCurrent = Callable[[TriggerPublicationLease], Awaitable[None]]
WatcherFinished = Callable[[ProviderSession], Awaitable[None]]


class ProviderTriggerWatcher:
    """Maintain one retrying trigger stream without owning session state."""

    def __init__(
        self,
        *,
        coordinator: TriggerPublicationCoordinator | None,
        retry_initial_delay_seconds: float,
        retry_max_delay_seconds: float,
        assert_current: AssertCurrent,
        on_finished: WatcherFinished,
    ) -> None:
        """Bind the stream to manager-owned session fencing callbacks."""
        self._coordinator = coordinator
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._retry_max_delay_seconds = retry_max_delay_seconds
        self._assert_current = assert_current
        self._on_finished = on_finished

    async def watch(
        self,
        session: ProviderSession,
        availability_generation: int,
    ) -> None:
        """Consume one current session's Trigger Publication stream."""
        coordinator = self._coordinator
        if coordinator is None:
            return
        lease = TriggerPublicationLease(
            provider_instance_id=session.provider_instance_id,
            endpoint_generation=session.endpoint_generation,
            session_id=session.session_id,
            manifest_revision=session.manifest.key.revision,
            availability_generation=availability_generation,
            trigger_schemas=session.manifest.trigger_schemas,
            channel=session._channel,
            session_token=session,
        )
        attempt_number = 0
        try:
            while True:
                try:
                    await coordinator.run(
                        lease,
                        assert_current=self._assert_current,
                    )
                except asyncio.CancelledError:
                    raise
                except SfpTriggerPublicationStaleError:
                    return
                except SfpTriggerPublicationUnavailableError as error:
                    delay = self._retry_delay(attempt_number)
                    attempt_number += 1
                    _LOGGER.info(
                        "Scheduling Trigger Publication retry: "
                        "provider_instance_id=%s endpoint_generation=%s "
                        "attempt=%s delay_seconds=%.3f reason=%s",
                        session.provider_instance_id,
                        session.endpoint_generation,
                        attempt_number,
                        delay,
                        error,
                    )
                    await asyncio.sleep(delay)
                    continue
                except SfpTriggerPublicationError as error:
                    _LOGGER.warning(
                        "Trigger Publication stream rejected: "
                        "provider_instance_id=%s endpoint_generation=%s "
                        "error=%s",
                        session.provider_instance_id,
                        session.endpoint_generation,
                        error,
                    )
                    return
                raise SfpTriggerPublicationUnavailableError(
                    "Trigger Publication stream ended"
                )
        finally:
            await self._on_finished(session)

    def _retry_delay(self, attempt_number: int) -> float:
        """Return a capped jittered retry delay for stream failures."""
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
