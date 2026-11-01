"""Call SFP Operation Admission and normalize validated outcomes."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC
from typing import Never, Protocol, cast
from uuid import UUID

import grpc
from google.protobuf.message import DecodeError
from grpc_status import rpc_status
from stateforge_protocol.sfp.operation_admission.v1 import (
    contract,
    operation_admission_pb2,
    validation,
)

from stateforge_agent._configuration_validation import require_finite_positive
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionDisposition,
    OperationAdmissionResult,
    OperationRejection,
)


class SfpOperationAdmissionError(RuntimeError):
    """Report a sanitized Agent-side admission failure."""


class SfpOperationAdmissionRequestError(SfpOperationAdmissionError):
    """Report an invalid locally constructed request."""


class SfpOperationAdmissionProtocolError(SfpOperationAdmissionError):
    """Report malformed, foreign, or inconsistent Provider data."""


class SfpOperationAdmissionUnavailableError(SfpOperationAdmissionError):
    """Report transport loss without an admission conclusion."""


class SfpOperationAdmissionDeadlineError(SfpOperationAdmissionError):
    """Report expiry of one finite transport deadline."""


class SfpOperationAdmissionStaleError(SfpOperationAdmissionError):
    """Report a stale session, generation, or declaration fence."""


class SfpOperationAdmissionLimitError(SfpOperationAdmissionError):
    """Report a request or response exceeding canonical limits."""


class SfpOperationAdmissionTransportError(SfpOperationAdmissionError):
    """Report a non-retryable unstructured transport failure."""


class SfpOperationAdmissionRejectedError(SfpOperationAdmissionError):
    """Carry one validated canonical Provider rejection."""

    def __init__(self, rejection: OperationRejection) -> None:
        super().__init__(
            "Operation Admission rejected: "
            f"{rejection.reason} ({rejection.grpc_status})"
        )
        self.rejection = rejection


class OperationAdmissionRequester(Protocol):
    """Describe one finite canonical Operation Admission RPC attempt."""

    async def start(
        self,
        channel: grpc.aio.Channel,
        request: operation_admission_pb2.StartOperationRequest,
    ) -> OperationAdmissionResult:
        """Return one validated admission result."""


class _OperationUnaryCall(Protocol):
    def __call__(
        self,
        request: operation_admission_pb2.StartOperationRequest,
        *,
        timeout: float,
    ) -> Awaitable[_DecodedAdmissionResponse]:
        """Issue one unary admission request."""


@dataclass(frozen=True, slots=True)
class _DecodedAdmissionResponse:
    response: operation_admission_pb2.StartOperationResponse | None = None
    malformed: bool = False
    limit_exceeded: bool = False


_SERVICE_DESCRIPTOR = operation_admission_pb2.DESCRIPTOR.services_by_name[
    "OperationAdmissionService"
]
_START_DESCRIPTOR = _SERVICE_DESCRIPTOR.methods_by_name["StartOperation"]
_START_METHOD_PATH = (
    f"/{_SERVICE_DESCRIPTOR.full_name}/{_START_DESCRIPTOR.name}"
)


class OperationAdmissionClient:
    """Perform one bounded Operation Admission transport attempt."""

    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self._timeout_seconds = require_finite_positive(
            timeout_seconds,
            name="Operation Admission timeout",
        )

    async def start(
        self,
        channel: grpc.aio.Channel,
        request: operation_admission_pb2.StartOperationRequest,
    ) -> OperationAdmissionResult:
        """Validate, call, correlate, and normalize one response."""
        validate_request(request)
        call: _OperationUnaryCall = channel.unary_unary(
            _START_METHOD_PATH,
            request_serializer=(
                operation_admission_pb2.StartOperationRequest.SerializeToString
            ),
            response_deserializer=_decode_response,
        )
        try:
            decoded = await call(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as error:
            rejection = _canonical_rejection(
                error,
                expected_operation_id=request.invocation.operation_id,
            )
            if rejection is not None:
                raise SfpOperationAdmissionRejectedError(rejection) from None
            _raise_transport_error(error.code())

        if decoded.limit_exceeded:
            raise SfpOperationAdmissionLimitError(
                "Operation Admission response limit exceeded"
            )
        if decoded.malformed or decoded.response is None:
            raise SfpOperationAdmissionProtocolError(
                "Operation Admission response protobuf is malformed"
            )
        response = decoded.response
        try:
            validation.validate_admission_exchange(request, response)
        except validation.OperationAdmissionValidationError as error:
            raise SfpOperationAdmissionProtocolError(
                "Operation Admission response failed canonical validation"
            ) from error
        return _normalize(response)


def validate_request(
    request: operation_admission_pb2.StartOperationRequest,
) -> None:
    """Delegate context-independent request rules to Protocol."""
    if request.ByteSize() > contract.MAX_REQUEST_BYTES:
        raise SfpOperationAdmissionLimitError(
            "Operation Admission request limit exceeded"
        )
    try:
        validation.validate_start_operation_request(request)
    except validation.OperationAdmissionValidationError as error:
        raise SfpOperationAdmissionRequestError(
            "Operation Admission request failed canonical validation"
        ) from error


def _decode_response(payload: bytes) -> _DecodedAdmissionResponse:
    if len(payload) > contract.MAX_RESPONSE_BYTES:
        return _DecodedAdmissionResponse(limit_exceeded=True)
    try:
        response = operation_admission_pb2.StartOperationResponse.FromString(
            payload
        )
    except DecodeError:
        return _DecodedAdmissionResponse(malformed=True)
    return _DecodedAdmissionResponse(response=response)


def _canonical_rejection(
    error: grpc.aio.AioRpcError,
    *,
    expected_operation_id: str,
) -> OperationRejection | None:
    status = rpc_status.from_call(cast(grpc.Call, error))
    if status is None or len(status.details) != 1:
        return None
    detail = operation_admission_pb2.OperationAdmissionErrorDetail()
    if not status.details[0].Unpack(detail):
        return None
    try:
        validation.validate_admission_rejection(
            detail,
            grpc_status=error.code().name,
        )
    except validation.OperationAdmissionValidationError as validation_error:
        raise SfpOperationAdmissionProtocolError(
            "Operation Admission rejection failed canonical validation"
        ) from validation_error
    if detail.operation_id and detail.operation_id != expected_operation_id:
        raise SfpOperationAdmissionProtocolError(
            "Operation Admission rejection identity mismatch"
        )
    operation_id = UUID(detail.operation_id) if detail.operation_id else None
    retry_after = (
        detail.retry_after.ToTimedelta()
        if detail.HasField("retry_after")
        else None
    )
    return OperationRejection(
        operation_id=operation_id,
        reason=(
            operation_admission_pb2.AdmissionErrorReason.Name(detail.reason)
        ),
        retryable=detail.retryable,
        retry_after=retry_after,
        explanation=detail.explanation,
        grpc_status=error.code().name,
    )


def _raise_transport_error(code: grpc.StatusCode) -> Never:
    if code is grpc.StatusCode.UNAVAILABLE:
        raise SfpOperationAdmissionUnavailableError(
            "Operation Admission is temporarily unavailable"
        )
    if code is grpc.StatusCode.DEADLINE_EXCEEDED:
        raise SfpOperationAdmissionDeadlineError(
            "Operation Admission transport deadline expired"
        )
    if code is grpc.StatusCode.FAILED_PRECONDITION:
        raise SfpOperationAdmissionStaleError(
            "Operation Admission session or declaration is stale"
        )
    if code is grpc.StatusCode.RESOURCE_EXHAUSTED:
        raise SfpOperationAdmissionLimitError(
            "Operation Admission transport limit exceeded"
        )
    if code is grpc.StatusCode.INVALID_ARGUMENT:
        raise SfpOperationAdmissionRequestError(
            "Provider rejected the Operation Admission request"
        )
    raise SfpOperationAdmissionTransportError(
        f"Operation Admission transport failed: {code.name}"
    )


def _normalize(
    response: operation_admission_pb2.StartOperationResponse,
) -> OperationAdmissionResult:
    disposition = {
        operation_admission_pb2.ADMISSION_DISPOSITION_ACCEPTED_NEW: (
            OperationAdmissionDisposition.ACCEPTED_NEW
        ),
        operation_admission_pb2.ADMISSION_DISPOSITION_RESOLVED_EXISTING: (
            OperationAdmissionDisposition.RESOLVED_EXISTING
        ),
    }[response.disposition]
    return OperationAdmissionResult(
        provider_instance_id=UUID(response.provider_instance_id),
        endpoint_generation=response.endpoint_generation,
        session_id=response.session_id,
        manifest_revision=response.manifest_revision,
        operation_id=UUID(response.record.operation_id),
        request_fingerprint=response.record.request_fingerprint,
        disposition=disposition,
        admitted_at=response.record.admitted_at.ToDatetime(tzinfo=UTC),
        record_expires_at=response.record.record_expires_at.ToDatetime(
            tzinfo=UTC
        ),
        admitted_endpoint_generation=(
            response.record.admitted_endpoint_generation
        ),
    )
