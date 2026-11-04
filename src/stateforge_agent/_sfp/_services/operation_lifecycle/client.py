"""Call the canonical SFP Operation Lifecycle service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

import grpc
from google.protobuf.message import DecodeError
from stateforge_protocol.sfp.operation_lifecycle.v1 import (
    contract,
    operation_lifecycle_pb2,
    validation,
)

from stateforge_agent._configuration_validation import require_finite_positive


class SfpOperationLifecycleError(RuntimeError):
    """Report an invalid or failed lifecycle exchange without payload data."""


class SfpOperationLifecycleProtocolError(SfpOperationLifecycleError):
    """Report malformed, oversized, or inconsistent Provider evidence."""


class SfpOperationLifecycleTransportError(SfpOperationLifecycleError):
    """Retain the gRPC status of one inconclusive lifecycle call."""

    def __init__(self, code: grpc.StatusCode) -> None:
        super().__init__(f"Operation Lifecycle transport failed: {code.name}")
        self.code = code


class OperationLifecycleRequester(Protocol):
    """Describe the bounded lifecycle calls used by Agent coordination."""

    def watch(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.WatchOperationRequest,
    ) -> AsyncIterator[operation_lifecycle_pb2.WatchOperationResponse]:
        """Yield validated responses from one Watch call."""
        ...

    async def cancel(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.CancelOperationRequest,
    ) -> operation_lifecycle_pb2.CancelOperationResponse:
        """Return one validated cancellation decision."""
        ...


def _method_path(name: str) -> str:
    service = operation_lifecycle_pb2.DESCRIPTOR.services_by_name[
        "OperationLifecycleService"
    ]
    return f"/{service.full_name}/{service.methods_by_name[name].name}"


def _decode_watch(
    payload: bytes,
) -> operation_lifecycle_pb2.WatchOperationResponse:
    if len(payload) > contract.MAX_RESPONSE_BYTES:
        raise SfpOperationLifecycleProtocolError(
            "Operation Lifecycle Watch response exceeds its limit"
        )
    try:
        return operation_lifecycle_pb2.WatchOperationResponse.FromString(
            payload
        )
    except DecodeError as error:
        raise SfpOperationLifecycleProtocolError(
            "Operation Lifecycle Watch response is malformed"
        ) from error


def _decode_cancel(
    payload: bytes,
) -> operation_lifecycle_pb2.CancelOperationResponse:
    if len(payload) > contract.MAX_RESPONSE_BYTES:
        raise SfpOperationLifecycleProtocolError(
            "Operation Lifecycle Cancel response exceeds its limit"
        )
    try:
        return operation_lifecycle_pb2.CancelOperationResponse.FromString(
            payload
        )
    except DecodeError as error:
        raise SfpOperationLifecycleProtocolError(
            "Operation Lifecycle Cancel response is malformed"
        ) from error


class OperationLifecycleClient:
    """Use one multiplexed Provider channel without redispatching work."""

    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self._timeout_seconds = require_finite_positive(
            timeout_seconds, name="Operation Lifecycle timeout"
        )

    def watch(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.WatchOperationRequest,
    ) -> AsyncIterator[operation_lifecycle_pb2.WatchOperationResponse]:
        """Watch one already admitted identity with bounded responses."""
        return self._watch(channel, request)

    async def _watch(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.WatchOperationRequest,
    ) -> AsyncIterator[operation_lifecycle_pb2.WatchOperationResponse]:
        try:
            validation.validate_watch_operation_request(request)
        except validation.OperationLifecycleValidationError as error:
            raise SfpOperationLifecycleProtocolError(
                "Operation Lifecycle Watch request is invalid"
            ) from error
        call = channel.unary_stream(
            _method_path("WatchOperation"),
            request_serializer=(
                operation_lifecycle_pb2.WatchOperationRequest.SerializeToString
            ),
            response_deserializer=_decode_watch,
        )
        previous_sequence: int | None = None
        terminal = False
        try:
            async for response in call(request):
                try:
                    validation.validate_watch_exchange(
                        request,
                        response,
                        previous_sequence=previous_sequence,
                    )
                except validation.OperationLifecycleValidationError as error:
                    raise SfpOperationLifecycleProtocolError(
                        "Operation Lifecycle Watch exchange is invalid"
                    ) from error
                previous_sequence = response.snapshot.sequence
                terminal = response.snapshot.state in {
                    operation_lifecycle_pb2.OPERATION_STATE_SUCCEEDED,
                    operation_lifecycle_pb2.OPERATION_STATE_FAILED,
                    operation_lifecycle_pb2.OPERATION_STATE_CANCELLED,
                }
                yield response
            if not terminal:
                raise SfpOperationLifecycleProtocolError(
                    "Operation Lifecycle Watch closed without a terminal "
                    "outcome"
                )
        except grpc.aio.AioRpcError as error:
            raise SfpOperationLifecycleTransportError(error.code()) from error

    async def cancel(
        self,
        channel: grpc.aio.Channel,
        request: operation_lifecycle_pb2.CancelOperationRequest,
    ) -> operation_lifecycle_pb2.CancelOperationResponse:
        """Request cooperative cancellation without assuming completion."""
        try:
            validation.validate_cancel_operation_request(request)
        except validation.OperationLifecycleValidationError as error:
            raise SfpOperationLifecycleProtocolError(
                "Operation Lifecycle Cancel request is invalid"
            ) from error
        call = channel.unary_unary(
            _method_path("CancelOperation"),
            request_serializer=(
                operation_lifecycle_pb2.CancelOperationRequest.SerializeToString
            ),
            response_deserializer=_decode_cancel,
        )
        try:
            response = await call(request, timeout=self._timeout_seconds)
        except grpc.aio.AioRpcError as error:
            raise SfpOperationLifecycleTransportError(error.code()) from error
        try:
            validation.validate_cancel_exchange(request, response)
        except validation.OperationLifecycleValidationError as error:
            raise SfpOperationLifecycleProtocolError(
                "Operation Lifecycle Cancel exchange is invalid"
            ) from error
        return response
