"""Consume bounded standard gRPC Health responses from a Provider."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

import grpc
from google.protobuf.message import DecodeError
from grpc_health.v1 import health_pb2
from stateforge_protocol.sfp import health_contract


class SfpHealthError(RuntimeError):
    """Report an invalid standard Health response from a Provider."""


class SfpHealthWatchError(SfpHealthError):
    """Report one standard Health Watch transport failure."""

    def __init__(self, code: grpc.StatusCode) -> None:
        """Retain only the status code needed by retry policy."""
        super().__init__(f"Health Watch failed with {code.name}")
        self.code = code


class HealthWatcher(Protocol):
    """Describe the Health Watch dependency used by session coordination."""

    def watch(
        self,
        channel: grpc.aio.Channel,
    ) -> AsyncIterator[int]:
        """Yield Provider-wide Health statuses in transport order."""
        ...


class HealthClient:
    """Watch the canonical Provider-wide standard Health service."""

    def watch(
        self,
        channel: grpc.aio.Channel,
    ) -> AsyncIterator[int]:
        """Yield validated Provider-wide Health status values."""
        return self._watch(channel)

    async def _watch(
        self,
        channel: grpc.aio.Channel,
    ) -> AsyncIterator[int]:
        request = health_pb2.HealthCheckRequest(
            service=health_contract.AUTHORITATIVE_WATCH_SERVICE
        )
        if request.ByteSize() > health_contract.MAX_REQUEST_BYTES:
            raise SfpHealthError("Health Watch request limit exceeded")

        watch = channel.unary_stream(
            _health_method_path("Watch"),
            request_serializer=(
                health_pb2.HealthCheckRequest.SerializeToString
            ),
            response_deserializer=_decode_health_response,
        )
        try:
            async for response in watch(request):
                yield response.status
        except grpc.aio.AioRpcError as error:
            raise SfpHealthWatchError(error.code()) from error


def _health_method_path(method_name: str) -> str:
    """Build one standard Health RPC path from its protobuf descriptor."""
    service = health_pb2.DESCRIPTOR.services_by_name["Health"]
    method = service.methods_by_name[method_name]
    return f"/{service.full_name}/{method.name}"


def _decode_health_response(payload: bytes) -> health_pb2.HealthCheckResponse:
    """Decode one response while retaining its serialized size boundary."""
    if len(payload) > health_contract.MAX_RESPONSE_BYTES:
        raise SfpHealthError("Health Watch response limit exceeded")
    try:
        response = health_pb2.HealthCheckResponse.FromString(payload)
    except DecodeError as error:
        raise SfpHealthError("Health Watch response is malformed") from error
    if response.status not in {
        health_pb2.HealthCheckResponse.UNKNOWN,
        health_pb2.HealthCheckResponse.SERVING,
        health_pb2.HealthCheckResponse.NOT_SERVING,
        health_pb2.HealthCheckResponse.SERVICE_UNKNOWN,
    }:
        raise SfpHealthError("Health Watch response has an unknown status")
    return response
