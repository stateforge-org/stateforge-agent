"""Retain Agent-side observation of admitted Provider operations."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import UUID

import grpc
from google.protobuf.json_format import MessageToDict
from jsonschema import Draft202012Validator
from stateforge_protocol.sfp.operation_admission.v1 import (
    operation_admission_pb2,
)
from stateforge_protocol.sfp.operation_lifecycle.v1 import (
    operation_lifecycle_pb2,
)

from stateforge_agent._configuration_validation import (
    require_bounded_integer,
    require_finite_positive,
)
from stateforge_agent._journal.base import JournalError
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAdmissionStore,
    OperationAttempt,
    OperationAttemptState,
)
from stateforge_agent._sfp._services.operation_lifecycle.client import (
    OperationLifecycleClient,
    OperationLifecycleRequester,
    SfpOperationLifecycleError,
    SfpOperationLifecycleProtocolError,
    SfpOperationLifecycleTransportError,
)
from stateforge_agent._sfp._services.operation_lifecycle.models import (
    OperationStatus,
)

_LOGGER = logging.getLogger(__name__)
_RETRYABLE_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
)
_TERMINAL_STATES = frozenset(
    {
        operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
        operation_lifecycle_pb2.OPERATION_STATE_FAILED,
        operation_lifecycle_pb2.OPERATION_STATE_CANCELLED,
    }
)


class OperationLifecyclePreconditionError(RuntimeError):
    """Reject observation or cancellation without durable admission."""


@dataclass(frozen=True, slots=True)
class OperationLifecycleLease:
    """Identify one live Provider session used only for lifecycle RPCs."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    channel: grpc.aio.Channel = field(repr=False, compare=False)
    session_token: object = field(repr=False, compare=False)


OperationLifecycleCurrentnessCheck = Callable[
    [OperationLifecycleLease], Awaitable[None]
]


@dataclass(slots=True)
class _ProviderObservation:
    """Keep one Provider's active session and observer tasks together."""

    lease: OperationLifecycleLease
    assert_current: OperationLifecycleCurrentnessCheck
    tasks: dict[UUID, asyncio.Task[None]] = field(default_factory=dict)
    blocked: set[UUID] = field(default_factory=set)


class OperationLifecycleCoordinator:
    """Observe retained operations without initiating business execution."""

    def __init__(
        self,
        store: OperationAdmissionStore,
        *,
        maximum_active_observers: int = 16,
        retry_initial_delay_seconds: float = 0.25,
        retry_max_delay_seconds: float = 5.0,
        client: OperationLifecycleRequester | None = None,
    ) -> None:
        self._store = store
        self._maximum_active_observers = require_bounded_integer(
            maximum_active_observers,
            name="maximum active operation observers",
            minimum=1,
            maximum=1024,
        )
        self._retry_initial_delay_seconds = require_finite_positive(
            retry_initial_delay_seconds,
            name="Operation Lifecycle retry delay",
        )
        self._retry_max_delay_seconds = require_finite_positive(
            retry_max_delay_seconds,
            name="Operation Lifecycle maximum retry delay",
        )
        if retry_max_delay_seconds < retry_initial_delay_seconds:
            raise ValueError(
                "Operation Lifecycle maximum retry delay is too small"
            )
        self._client = client or OperationLifecycleClient()
        self._providers: dict[UUID, _ProviderObservation] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def resume(
        self,
        lease: OperationLifecycleLease,
        *,
        assert_current: OperationLifecycleCurrentnessCheck,
    ) -> None:
        """Resume accepted work on the current session without dispatch."""
        async with self._lock:
            if self._closed:
                return
            previous = self._providers.get(lease.provider_instance_id)
            if (
                previous is not None
                and previous.lease.session_token is not lease.session_token
            ):
                for task in previous.tasks.values():
                    task.cancel()
                previous = None
            if previous is None:
                self._providers[lease.provider_instance_id] = (
                    _ProviderObservation(lease, assert_current)
                )
        await self._fill_observer_slots()

    async def status(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationStatus | None:
        """Return current durable local knowledge without a Provider call."""
        attempt = await self._store.get(provider_instance_id, operation_id)
        return (
            OperationStatus.from_attempt(attempt)
            if attempt is not None
            else None
        )

    async def cancel(
        self,
        lease: OperationLifecycleLease,
        operation_id: UUID,
        *,
        assert_current: OperationLifecycleCurrentnessCheck,
    ) -> OperationStatus:
        """Request cooperative cancellation and retain its exact decision."""
        attempt = await self._accepted_attempt(
            lease.provider_instance_id, operation_id
        )
        request = operation_lifecycle_pb2.CancelOperationRequest(
            session_binding=_binding(lease),
            operation_id=str(operation_id),
            expected_request_fingerprint=attempt.request_fingerprint,
        )
        await assert_current(lease)
        response = await self._client.cancel(lease.channel, request)
        await assert_current(lease)
        return await self._retain(
            attempt,
            response.snapshot,
            cancellation_disposition=response.disposition,
        )

    async def close(self) -> None:
        """Stop local observers without cancelling Provider operations."""
        async with self._lock:
            self._closed = True
            tasks = tuple(
                task
                for provider in self._providers.values()
                for task in provider.tasks.values()
            )
            self._providers.clear()
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def suspend(
        self, provider_instance_id: UUID, session_token: object
    ) -> None:
        """Detach observers after session loss without changing work state."""
        async with self._lock:
            provider = self._providers.get(provider_instance_id)
            if (
                provider is None
                or provider.lease.session_token is not session_token
            ):
                return
            self._providers.pop(provider_instance_id)
            tasks = tuple(provider.tasks.values()) if provider else ()
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fill_observer_slots(self) -> None:
        outstanding = await self._store.accepted_nonterminal()
        async with self._lock:
            if self._closed:
                return
            active = sum(
                len(provider.tasks) for provider in self._providers.values()
            )
            for attempt in outstanding:
                if active >= self._maximum_active_observers:
                    break
                provider = self._providers.get(attempt.provider_instance_id)
                if (
                    provider is None
                    or attempt.operation_id in provider.tasks
                    or attempt.operation_id in provider.blocked
                ):
                    continue
                provider.tasks[attempt.operation_id] = asyncio.create_task(
                    self._observe(provider, attempt.operation_id),
                    name=f"stateforge-operation-watch-{attempt.operation_id}",
                )
                active += 1

    async def _observe(
        self,
        provider: _ProviderObservation,
        operation_id: UUID,
    ) -> None:
        lease = provider.lease
        delay = self._retry_initial_delay_seconds
        blocked = False
        try:
            while not self._closed:
                attempt = await self._accepted_attempt(
                    lease.provider_instance_id, operation_id
                )
                snapshot = attempt.lifecycle_snapshot
                if snapshot is not None and snapshot.state in _TERMINAL_STATES:
                    return
                request = operation_lifecycle_pb2.WatchOperationRequest(
                    session_binding=_binding(lease),
                    operation_id=str(operation_id),
                    expected_request_fingerprint=attempt.request_fingerprint,
                    after_sequence=(
                        snapshot.sequence if snapshot is not None else 0
                    ),
                )
                try:
                    await provider.assert_current(lease)
                    await self._watch_once(provider, attempt, request)
                    return
                except SfpOperationLifecycleTransportError as error:
                    if error.code not in _RETRYABLE_CODES:
                        _LOGGER.warning(
                            "Operation observation stopped: operation_id=%s "
                            "transport=%s",
                            operation_id,
                            error.code.name,
                        )
                        blocked = True
                        return
                except SfpOperationLifecycleError:
                    _LOGGER.warning(
                        "Operation observation rejected invalid evidence: "
                        "operation_id=%s",
                        operation_id,
                    )
                    blocked = True
                    return
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._retry_max_delay_seconds)
        except asyncio.CancelledError:
            raise
        except (JournalError, OperationLifecyclePreconditionError):
            blocked = True
            _LOGGER.exception(
                "Operation observation cannot retain evidence: "
                "operation_id=%s",
                operation_id,
            )
        finally:
            async with self._lock:
                current = self._providers.get(lease.provider_instance_id)
                if current is provider:
                    current.tasks.pop(operation_id, None)
                    if blocked:
                        current.blocked.add(operation_id)
            if not self._closed:
                await self._fill_observer_slots()

    async def _watch_once(
        self,
        provider: _ProviderObservation,
        attempt: OperationAttempt,
        request: operation_lifecycle_pb2.WatchOperationRequest,
    ) -> None:
        """Retain one stream until its terminal snapshot or failure."""
        async for response in self._client.watch(
            provider.lease.channel, request
        ):
            await provider.assert_current(provider.lease)
            await self._retain(attempt, response.snapshot)
            if response.snapshot.state in _TERMINAL_STATES:
                return
        raise SfpOperationLifecycleProtocolError(
            "Operation Lifecycle Watch ended without terminal evidence"
        )

    async def _accepted_attempt(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAttempt:
        attempt = await self._store.get(provider_instance_id, operation_id)
        if (
            attempt is None
            or attempt.state is not OperationAttemptState.ACCEPTED
            or attempt.result is None
        ):
            raise OperationLifecyclePreconditionError(
                "Operation Lifecycle requires a pinned admitted identity"
            )
        return attempt

    async def _retain(
        self,
        attempt: OperationAttempt,
        snapshot: operation_lifecycle_pb2.OperationSnapshot,
        *,
        cancellation_disposition: int | None = None,
    ) -> OperationStatus:
        if snapshot.state == operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED:
            schema = json.loads(attempt.result_schema_json)
            result = MessageToDict(snapshot.success.result)
            if not Draft202012Validator(schema).is_valid(result):
                raise SfpOperationLifecycleProtocolError(
                    "Operation result violates its pinned schema"
                )
        try:
            retained = await self._store.record_lifecycle_snapshot(
                attempt.provider_instance_id,
                attempt.operation_id,
                snapshot,
                cancellation_disposition=cancellation_disposition,
            )
        except ValueError as error:
            raise SfpOperationLifecycleProtocolError(
                "Operation evidence violates its retained contract"
            ) from error
        return OperationStatus.from_attempt(retained)


def _binding(
    lease: OperationLifecycleLease,
) -> operation_admission_pb2.OperationSessionBinding:
    return operation_admission_pb2.OperationSessionBinding(
        expected_provider_instance_id=str(lease.provider_instance_id),
        expected_endpoint_generation=lease.endpoint_generation,
        expected_session_id=lease.session_id,
    )
