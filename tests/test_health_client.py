"""Standard Health client contract and wire-boundary evidence for A2."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable

import pytest
from grpc_health.v1 import health_pb2
from stateforge_protocol.sfp import health_contract

from stateforge_agent._sfp._services.health.client import (
    HealthClient,
    SfpHealthError,
    _decode_health_response,
)

_VARINT_DATA_MASK = 0x7F
_VARINT_CONTINUATION_BIT = 0x80


def _encode_varint(value: int) -> bytes:
    encoded = bytearray()
    while value > _VARINT_DATA_MASK:
        encoded.append((value & _VARINT_DATA_MASK) | _VARINT_CONTINUATION_BIT)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _health_response_with_size(target_size: int) -> bytes:
    response = health_pb2.HealthCheckResponse(
        status=health_pb2.HealthCheckResponse.SERVING
    ).SerializeToString()
    unknown_field_tag = _encode_varint((2_047 << 3) | 2)
    padding_size = target_size - len(response) - len(unknown_field_tag) - 1
    for _ in range(8):
        wire = (
            response
            + unknown_field_tag
            + _encode_varint(padding_size)
            + bytes(padding_size)
        )
        difference = target_size - len(wire)
        if difference == 0:
            return wire
        padding_size += difference
    raise AssertionError(f"Could not construct a {target_size}-byte response")


class _HealthChannel:
    """Capture the dynamic standard Health call made by the client."""

    def __init__(self, response: bytes) -> None:
        self.response = response
        self.method_path: str | None = None
        self.request: health_pb2.HealthCheckRequest | None = None

    def unary_stream(
        self,
        method_path: str,
        *,
        request_serializer: Callable[[object], bytes],
        response_deserializer: Callable[[bytes], object],
    ):
        self.method_path = method_path

        def call(
            request: health_pb2.HealthCheckRequest,
        ) -> AsyncIterator[health_pb2.HealthCheckResponse]:
            self.request = request
            request_serializer(request)

            async def responses():
                yield response_deserializer(self.response)

            return responses()

        return call


@pytest.mark.asyncio
async def test_watch_uses_provider_wide_standard_health_rpc() -> None:
    channel = _HealthChannel(
        health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING
        ).SerializeToString()
    )

    statuses = [
        status
        async for status in HealthClient().watch(
            channel  # type: ignore[arg-type]
        )
    ]

    assert statuses == [health_pb2.HealthCheckResponse.SERVING]
    assert channel.method_path == "/grpc.health.v1.Health/Watch"
    assert channel.request is not None
    assert channel.request.service == (
        health_contract.AUTHORITATIVE_WATCH_SERVICE
    )


@pytest.mark.parametrize(
    ("response_size", "accepted"),
    [
        (health_contract.MAX_RESPONSE_BYTES, True),
        (health_contract.MAX_RESPONSE_BYTES + 1, False),
    ],
)
def test_health_response_wire_boundary(
    response_size: int,
    accepted: bool,
) -> None:
    payload = _health_response_with_size(response_size)
    if accepted:
        response = _decode_health_response(payload)
        assert response.status == health_pb2.HealthCheckResponse.SERVING
    else:
        with pytest.raises(SfpHealthError, match="response limit exceeded"):
            _decode_health_response(payload)


def test_malformed_health_response_is_normalized() -> None:
    with pytest.raises(SfpHealthError, match="response is malformed"):
        _decode_health_response(b"\x80")
