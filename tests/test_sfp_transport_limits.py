"""Verify canonical message ceilings on the shared Provider channel."""

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from stateforge_protocol.sfp import shared_endpoint
from stateforge_protocol.sfp.capability_assessment.v1 import (
    capability_assessment_pb2,
    capability_assessment_pb2_grpc,
)

from stateforge_agent._sfp._runtime.channel import (
    PROVIDER_CHANNEL_OPTIONS,
    create_provider_channel,
)

_METHOD = "/stateforge.test.Transport/Exchange"


class _CapabilityPeer(
    capability_assessment_pb2_grpc.CapabilityAssessmentServiceServicer
):
    """Serve one semantically valid Capability Assessment exchange."""

    async def AssessCapabilities(self, request, context):  # noqa: N802
        del context
        return capability_assessment_pb2.AssessCapabilitiesResponse(
            assessment_request_id=request.assessment_request_id,
            provider_instance_id=request.expected_provider_instance_id,
            endpoint_generation=request.expected_endpoint_generation,
            session_id=request.expected_session_id,
            manifest_revision=request.expected_manifest_revision,
            assessment_id="1d40d0d5-ff2a-4780-bc3d-c4a7c55fc3a2",
            assessment_timestamp=Timestamp(seconds=1),
            findings=[
                capability_assessment_pb2.CapabilityFinding(
                    operation_identifier=request.operation_identifiers[0],
                    support_state=(
                        capability_assessment_pb2.SUPPORT_STATE_SUPPORTED
                    ),
                )
            ],
        )


async def _start_byte_server(
    response: bytes,
) -> tuple[grpc.aio.Server, str]:
    """Start a raw byte service with ceilings above the Agent contract."""

    def handler(request: bytes, context: object) -> bytes:
        del request, context
        return response

    method = grpc.unary_unary_rpc_method_handler(
        handler,
        request_deserializer=lambda value: value,
        response_serializer=lambda value: value,
    )
    server = grpc.aio.server(
        options=(
            (
                "grpc.max_receive_message_length",
                shared_endpoint.MAX_PROVIDER_RECEIVE_BYTES + 1,
            ),
            (
                "grpc.max_send_message_length",
                shared_endpoint.MAX_PROVIDER_SEND_BYTES + 1,
            ),
        )
    )
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "stateforge.test.Transport",
                {"Exchange": method},
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


async def _exchange(target: str, request: bytes) -> bytes:
    """Exchange raw serialized message bytes over the production channel."""
    channel = create_provider_channel(target)
    try:
        await channel.channel_ready()
        exchange = channel.unary_unary(
            _METHOD,
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )
        return await exchange(request)
    finally:
        await channel.close()


def test_channel_options_follow_canonical_protocol_directions() -> None:
    assert PROVIDER_CHANNEL_OPTIONS == (
        (
            "grpc.max_send_message_length",
            shared_endpoint.MAX_PROVIDER_RECEIVE_BYTES,
        ),
        (
            "grpc.max_receive_message_length",
            shared_endpoint.MAX_PROVIDER_SEND_BYTES,
        ),
    )
    assert shared_endpoint.MAX_PROVIDER_RECEIVE_BYTES == 8 * 1024 * 1024
    assert shared_endpoint.MAX_PROVIDER_SEND_BYTES == 16 * 1024 * 1024


@pytest.mark.asyncio
async def test_channel_accepts_largest_procedure_contract() -> None:
    server = grpc.aio.server()
    capability_assessment_pb2_grpc.add_CapabilityAssessmentServiceServicer_to_server(
        _CapabilityPeer(),
        server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = create_provider_channel(f"127.0.0.1:{port}")
    request = capability_assessment_pb2.AssessCapabilitiesRequest(
        assessment_request_id="6c1ae0b8-8d70-46dc-86c0-994247e1a39b",
        expected_provider_instance_id=("7f7db7d8-68b4-4f71-9769-065b5c4f941c"),
        expected_endpoint_generation=1,
        expected_session_id="session",
        expected_manifest_revision="sha256:" + ("0" * 64),
        operation_identifiers=["stateforge.test/operation@1"],
    )
    try:
        await channel.channel_ready()
        response = await (
            capability_assessment_pb2_grpc.CapabilityAssessmentServiceStub(
                channel
            ).AssessCapabilities(request)
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert response.assessment_request_id == request.assessment_request_id
    assert (
        response.findings[0].operation_identifier
        == (request.operation_identifiers[0])
    )


@pytest.mark.asyncio
async def test_channel_accepts_largest_canonical_request() -> None:
    server, target = await _start_byte_server(b"accepted")
    try:
        response = await _exchange(
            target,
            bytes(shared_endpoint.MAX_PROVIDER_RECEIVE_BYTES),
        )
    finally:
        await server.stop(grace=None)

    assert response == b"accepted"


@pytest.mark.asyncio
async def test_channel_rejects_request_above_canonical_ceiling() -> None:
    server, target = await _start_byte_server(b"unreachable")
    try:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await _exchange(
                target,
                bytes(shared_endpoint.MAX_PROVIDER_RECEIVE_BYTES + 1),
            )
    finally:
        await server.stop(grace=None)

    assert error.value.code() is grpc.StatusCode.RESOURCE_EXHAUSTED


@pytest.mark.asyncio
async def test_channel_accepts_largest_canonical_response() -> None:
    expected_size = shared_endpoint.MAX_PROVIDER_SEND_BYTES
    server, target = await _start_byte_server(bytes(expected_size))
    try:
        response = await _exchange(target, b"request")
    finally:
        await server.stop(grace=None)

    assert len(response) == expected_size


@pytest.mark.asyncio
async def test_channel_rejects_response_above_canonical_ceiling() -> None:
    server, target = await _start_byte_server(
        bytes(shared_endpoint.MAX_PROVIDER_SEND_BYTES + 1)
    )
    try:
        with pytest.raises(grpc.aio.AioRpcError) as error:
            await _exchange(target, b"request")
    finally:
        await server.stop(grace=None)

    assert error.value.code() is grpc.StatusCode.RESOURCE_EXHAUSTED
