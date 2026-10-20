"""Coordinate Capability Assessment without owning Provider lifecycle."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from uuid import UUID, uuid4

import grpc
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict, ParseError
from stateforge_protocol.sfp.capability_assessment.v1 import (
    capability_assessment_pb2,
)

from stateforge_agent._sfp._services.capability_assessment.cache import (
    CapabilityAssessmentCache,
)
from stateforge_agent._sfp._services.capability_assessment.client import (
    CapabilityAssessmentClient,
    CapabilityAssessmentRequester,
    SfpCapabilityAssessmentRequestError,
    SfpCapabilityAssessmentUnavailableError,
    validate_request,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
    CapabilityAssessmentScope,
)

_MAX_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class CapabilityAssessmentLease:
    """Expose only the current session facts needed by this procedure."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    manifest_revision: str
    availability_generation: int
    operation_identifiers: frozenset[str]
    channel: grpc.aio.Channel = field(repr=False, compare=False)
    session_token: object = field(repr=False, compare=False)


CapabilityAssessmentCurrentnessCheck = Callable[
    [CapabilityAssessmentLease], Awaitable[None]
]


class CapabilityAssessmentCoordinator:
    """Own request construction, retry, normalization, and evidence reuse."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        attempts: int = 2,
        retry_initial_delay_seconds: float = 0.25,
        cache_entries: int = 128,
        client: CapabilityAssessmentRequester | None = None,
    ) -> None:
        """Configure one bounded Capability Assessment procedure client."""
        _validate_configuration(
            attempts=attempts,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
        )
        self._client = (
            client
            if client is not None
            else CapabilityAssessmentClient(timeout_seconds=timeout_seconds)
        )
        self._attempts = attempts
        self._retry_initial_delay_seconds = retry_initial_delay_seconds
        self._cache = CapabilityAssessmentCache(cache_entries)

    async def assess(
        self,
        lease: CapabilityAssessmentLease,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None,
        assert_current: CapabilityAssessmentCurrentnessCheck,
    ) -> CapabilityAssessment:
        """Collect fresh evidence through one exact session lease."""
        context = _execution_context(execution_context)
        request = _request(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=context,
        )
        validate_request(request)
        _validate_declarations(lease, operation_identifiers)
        scope = _scope(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=context,
        )

        for attempt in range(self._attempts):
            await assert_current(lease)
            try:
                assessment = await self._client.assess(
                    lease.channel,
                    request,
                    scope=scope,
                )
            except SfpCapabilityAssessmentUnavailableError:
                if attempt + 1 >= self._attempts:
                    raise
                await asyncio.sleep(
                    self._retry_initial_delay_seconds * (2**attempt)
                )
                continue
            await assert_current(lease)
            self._cache.put(assessment)
            return assessment
        raise RuntimeError("Capability Assessment retry loop exhausted")

    async def cached(
        self,
        lease: CapabilityAssessmentLease,
        operation_identifiers: tuple[str, ...],
        *,
        execution_context: Mapping[str, object] | None,
        assert_current: CapabilityAssessmentCurrentnessCheck,
    ) -> CapabilityAssessment | None:
        """Return exact-scope current evidence without authorizing work."""
        context = _execution_context(execution_context)
        request = _request(
            lease,
            operation_identifiers=operation_identifiers,
            execution_context=context,
        )
        validate_request(request)
        _validate_declarations(lease, operation_identifiers)
        await assert_current(lease)
        return self._cache.get(
            _scope(
                lease,
                operation_identifiers=operation_identifiers,
                execution_context=context,
            )
        )

    def discard_provider(self, provider_instance_id: UUID) -> None:
        """Discard evidence after one Provider lifecycle transition."""
        self._cache.discard_provider(provider_instance_id)

    def clear(self) -> None:
        """Discard all evidence during Agent shutdown."""
        self._cache.clear()


def _execution_context(
    value: Mapping[str, object] | None,
) -> struct_pb2.Struct:
    """Copy JSON-compatible caller context into a protobuf Struct."""
    context = struct_pb2.Struct()
    if value is None:
        return context
    try:
        ParseDict(dict(value), context)
    except (ParseError, TypeError, ValueError) as error:
        raise SfpCapabilityAssessmentRequestError(
            "Capability Assessment execution context is not valid JSON"
        ) from error
    return context


def _request(
    lease: CapabilityAssessmentLease,
    *,
    operation_identifiers: tuple[str, ...],
    execution_context: struct_pb2.Struct,
) -> capability_assessment_pb2.AssessCapabilitiesRequest:
    """Build one request entirely from current Agent-owned fences."""
    return capability_assessment_pb2.AssessCapabilitiesRequest(
        assessment_request_id=str(uuid4()),
        expected_provider_instance_id=str(lease.provider_instance_id),
        expected_endpoint_generation=lease.endpoint_generation,
        expected_session_id=lease.session_id,
        expected_manifest_revision=lease.manifest_revision,
        execution_context=execution_context,
        operation_identifiers=operation_identifiers,
    )


def _scope(
    lease: CapabilityAssessmentLease,
    *,
    operation_identifiers: tuple[str, ...],
    execution_context: struct_pb2.Struct,
) -> CapabilityAssessmentScope:
    """Bind reusable evidence to exact context, declaration, and session."""
    return CapabilityAssessmentScope(
        provider_instance_id=lease.provider_instance_id,
        endpoint_generation=lease.endpoint_generation,
        session_id=lease.session_id,
        manifest_revision=lease.manifest_revision,
        availability_generation=lease.availability_generation,
        operation_identifiers=operation_identifiers,
        execution_context_sha256=hashlib.sha256(
            execution_context.SerializeToString(deterministic=True)
        ).digest(),
    )


def _validate_declarations(
    lease: CapabilityAssessmentLease,
    operation_identifiers: tuple[str, ...],
) -> None:
    """Reject operations absent from the accepted manifest snapshot."""
    if any(
        identifier not in lease.operation_identifiers
        for identifier in operation_identifiers
    ):
        raise SfpCapabilityAssessmentRequestError(
            "Capability Assessment operation is not declared"
        )


def _validate_configuration(
    *,
    attempts: int,
    retry_initial_delay_seconds: float,
) -> None:
    """Validate Agent-local bounded assessment retry policy."""
    if not 1 <= attempts <= _MAX_ATTEMPTS:
        raise ValueError(
            "Capability Assessment attempts must be between 1 and "
            f"{_MAX_ATTEMPTS}"
        )
    if retry_initial_delay_seconds < 0:
        raise ValueError(
            "Capability Assessment retry delay must not be negative"
        )
