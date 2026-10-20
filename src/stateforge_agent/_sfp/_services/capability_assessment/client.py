"""Call Capability Assessment and normalize only validated SFP evidence."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC
from typing import Never, Protocol
from uuid import UUID

import grpc
from google.protobuf.message import DecodeError
from stateforge_protocol.sfp.capability_assessment.v1 import (
    capability_assessment_pb2,
    contract,
    validation,
)

from stateforge_agent._configuration_validation import (
    require_finite_positive,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
    CapabilityAssessmentScope,
    CapabilityConstraint,
    CapabilityFinding,
    CapabilityReason,
    CapabilityRequirement,
    CapabilityRequirementState,
    CapabilitySupportState,
)


class SfpCapabilityAssessmentError(RuntimeError):
    """Report a sanitized Capability Assessment failure."""


class SfpCapabilityAssessmentRequestError(SfpCapabilityAssessmentError):
    """Report an invalid locally constructed assessment request."""


class SfpCapabilityAssessmentMalformedResponseError(
    SfpCapabilityAssessmentError
):
    """Report a malformed or incorrectly correlated Provider response."""


class SfpCapabilityAssessmentUnavailableError(SfpCapabilityAssessmentError):
    """Report temporary service or readiness unavailability."""


class SfpCapabilityAssessmentDeadlineError(SfpCapabilityAssessmentError):
    """Report expiry of the finite assessment deadline."""


class SfpCapabilityAssessmentTransportError(SfpCapabilityAssessmentError):
    """Report a non-retryable transport failure."""


class SfpCapabilityAssessmentStaleError(SfpCapabilityAssessmentError):
    """Report a stale session, declaration, or local assessment fence."""


class SfpCapabilityAssessmentLimitError(SfpCapabilityAssessmentError):
    """Report a request or response exceeding the canonical byte limit."""


class SfpCapabilityAssessmentProviderError(SfpCapabilityAssessmentError):
    """Report a sanitized unexpected Provider boundary failure."""


class CapabilityAssessmentRequester(Protocol):
    """Describe the finite single-attempt assessment transport."""

    async def assess(
        self,
        channel: grpc.aio.Channel,
        request: capability_assessment_pb2.AssessCapabilitiesRequest,
        *,
        scope: CapabilityAssessmentScope,
    ) -> CapabilityAssessment:
        """Return one validated and normalized assessment snapshot."""
        ...


class _CapabilityUnaryCall(Protocol):
    """Type the generated asynchronous unary call at its dynamic boundary."""

    def __call__(
        self,
        request: capability_assessment_pb2.AssessCapabilitiesRequest,
        *,
        timeout: float,
    ) -> Awaitable[_DecodedCapabilityResponse]:
        """Issue one Capability Assessment request."""
        ...


@dataclass(frozen=True, slots=True)
class _DecodedCapabilityResponse:
    """Carry a decoded response or a safe wire-rejection reason."""

    response: capability_assessment_pb2.AssessCapabilitiesResponse | None = (
        None
    )
    rejection: str | None = None
    limit_exceeded: bool = False


_SERVICE_DESCRIPTOR = capability_assessment_pb2.DESCRIPTOR.services_by_name[
    "CapabilityAssessmentService"
]
_ASSESS_DESCRIPTOR = _SERVICE_DESCRIPTOR.methods_by_name["AssessCapabilities"]
_ASSESS_METHOD_PATH = (
    f"/{_SERVICE_DESCRIPTOR.full_name}/{_ASSESS_DESCRIPTOR.name}"
)


class CapabilityAssessmentClient:
    """Perform one bounded, finite Capability Assessment RPC attempt."""

    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        """Configure the required finite per-call deadline."""
        self._timeout_seconds = require_finite_positive(
            timeout_seconds,
            name="Capability Assessment timeout",
        )

    async def assess(
        self,
        channel: grpc.aio.Channel,
        request: capability_assessment_pb2.AssessCapabilitiesRequest,
        *,
        scope: CapabilityAssessmentScope,
    ) -> CapabilityAssessment:
        """Validate, call, correlate, and normalize one response."""
        if request.ByteSize() > contract.MAX_REQUEST_BYTES:
            raise SfpCapabilityAssessmentLimitError(
                "Capability Assessment request limit exceeded"
            )
        validate_request(request)

        call: _CapabilityUnaryCall = channel.unary_unary(
            _ASSESS_METHOD_PATH,
            request_serializer=(
                capability_assessment_pb2.AssessCapabilitiesRequest.SerializeToString
            ),
            response_deserializer=_decode_response,
        )
        failure_code: grpc.StatusCode | None = None
        try:
            decoded = await call(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as error:
            failure_code = error.code()
        if failure_code is not None:
            _raise_rpc_error(failure_code)

        if decoded.limit_exceeded:
            raise SfpCapabilityAssessmentLimitError(
                "Capability Assessment response limit exceeded"
            )
        if decoded.rejection is not None or decoded.response is None:
            raise SfpCapabilityAssessmentMalformedResponseError(
                decoded.rejection
                or "Capability Assessment response protobuf is malformed"
            )
        response = decoded.response
        if response.ByteSize() > contract.MAX_RESPONSE_BYTES:
            raise SfpCapabilityAssessmentLimitError(
                "Capability Assessment response limit exceeded"
            )
        try:
            validation.validate_assessment_exchange(request, response)
        except validation.CapabilityAssessmentValidationError as error:
            raise SfpCapabilityAssessmentMalformedResponseError(
                "Capability Assessment response failed canonical validation"
            ) from error
        return _normalize(response, scope=scope)


def validate_request(
    request: capability_assessment_pb2.AssessCapabilitiesRequest,
) -> None:
    """Delegate request semantics to the public Protocol validator."""
    try:
        validation.validate_assess_capabilities_request(request)
    except validation.CapabilityAssessmentValidationError as error:
        raise SfpCapabilityAssessmentRequestError(
            "Capability Assessment request failed canonical validation"
        ) from error


def _decode_response(payload: bytes) -> _DecodedCapabilityResponse:
    """Reject oversized or malformed wire data before semantic handling."""
    if len(payload) > contract.MAX_RESPONSE_BYTES:
        return _DecodedCapabilityResponse(
            rejection="Capability Assessment response limit exceeded",
            limit_exceeded=True,
        )
    try:
        response = (
            capability_assessment_pb2.AssessCapabilitiesResponse.FromString(
                payload
            )
        )
    except DecodeError:
        return _DecodedCapabilityResponse(
            rejection="Capability Assessment response protobuf is malformed"
        )
    return _DecodedCapabilityResponse(response=response)


def _raise_rpc_error(code: grpc.StatusCode) -> Never:
    """Translate only stable gRPC status codes into sanitized Agent errors."""
    if code is grpc.StatusCode.UNAVAILABLE:
        raise SfpCapabilityAssessmentUnavailableError(
            "Capability Assessment is temporarily unavailable"
        )
    if code is grpc.StatusCode.DEADLINE_EXCEEDED:
        raise SfpCapabilityAssessmentDeadlineError(
            "Capability Assessment deadline expired"
        )
    if code is grpc.StatusCode.FAILED_PRECONDITION:
        raise SfpCapabilityAssessmentStaleError(
            "Capability Assessment session or declaration is stale"
        )
    if code is grpc.StatusCode.RESOURCE_EXHAUSTED:
        raise SfpCapabilityAssessmentLimitError(
            "Capability Assessment message limit exceeded"
        )
    if code is grpc.StatusCode.INVALID_ARGUMENT:
        raise SfpCapabilityAssessmentRequestError(
            "Provider rejected the Capability Assessment request"
        )
    if code is grpc.StatusCode.INTERNAL:
        raise SfpCapabilityAssessmentProviderError(
            "Provider could not produce Capability Assessment evidence"
        )
    raise SfpCapabilityAssessmentTransportError(
        f"Capability Assessment transport failed: {code.name}"
    )


def _normalize(
    response: capability_assessment_pb2.AssessCapabilitiesResponse,
    *,
    scope: CapabilityAssessmentScope,
) -> CapabilityAssessment:
    """Convert a validated protobuf into immutable Agent-owned data."""
    return CapabilityAssessment(
        scope=scope,
        request_id=UUID(response.assessment_request_id),
        assessment_id=UUID(response.assessment_id),
        assessed_at=response.assessment_timestamp.ToDatetime(tzinfo=UTC),
        findings=tuple(_normalize_finding(item) for item in response.findings),
        wire_size_bytes=response.ByteSize(),
    )


def _normalize_finding(
    finding: capability_assessment_pb2.CapabilityFinding,
) -> CapabilityFinding:
    """Normalize one validated operation finding without policy inference."""
    support_state = {
        capability_assessment_pb2.SUPPORT_STATE_SUPPORTED: (
            CapabilitySupportState.SUPPORTED
        ),
        capability_assessment_pb2.SUPPORT_STATE_SUPPORTED_WITH_REQUIREMENTS: (
            CapabilitySupportState.SUPPORTED_WITH_REQUIREMENTS
        ),
        capability_assessment_pb2.SUPPORT_STATE_UNSUPPORTED: (
            CapabilitySupportState.UNSUPPORTED
        ),
        capability_assessment_pb2.SUPPORT_STATE_INDETERMINATE: (
            CapabilitySupportState.INDETERMINATE
        ),
    }[finding.support_state]
    return CapabilityFinding(
        operation_identifier=finding.operation_identifier,
        support_state=support_state,
        requirements=tuple(
            CapabilityRequirement(
                identifier=item.requirement_id,
                state=_normalize_requirement_state(item.state),
                description=item.description,
                version_constraint=item.version_constraint,
            )
            for item in finding.requirement_findings
        ),
        reasons=tuple(
            CapabilityReason(
                code=item.reason_code,
                explanation=item.explanation,
            )
            for item in finding.reasons
        ),
        constraints=tuple(
            CapabilityConstraint(
                identifier=item.constraint_id,
                value=item.value,
                explanation=item.explanation,
            )
            for item in finding.constraints
        ),
    )


def _normalize_requirement_state(value: int) -> CapabilityRequirementState:
    """Map one already-validated wire enum to its Agent representation."""
    states: dict[int, CapabilityRequirementState] = {
        capability_assessment_pb2.REQUIREMENT_STATE_SATISFIED: (
            CapabilityRequirementState.SATISFIED
        ),
        capability_assessment_pb2.REQUIREMENT_STATE_UNSATISFIED: (
            CapabilityRequirementState.UNSATISFIED
        ),
        capability_assessment_pb2.REQUIREMENT_STATE_UNKNOWN: (
            CapabilityRequirementState.UNKNOWN
        ),
    }
    return states[value]
