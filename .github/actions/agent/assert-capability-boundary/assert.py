"""Verify adversarial Capability Assessment boundaries after RPM install."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Never
from uuid import UUID

import grpc
from google.protobuf import struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from stateforge_protocol.sfp.capability_assessment.v1 import (
    capability_assessment_pb2,
    contract,
)

from stateforge_agent._sfp import (
    SfpCapabilityAssessmentDeadlineError,
    SfpCapabilityAssessmentLimitError,
    SfpCapabilityAssessmentMalformedResponseError,
)
from stateforge_agent._sfp._runtime.channel import create_provider_channel
from stateforge_agent._sfp._services.capability_assessment.client import (
    CapabilityAssessmentClient,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessmentScope,
)

_PROVIDER_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_REQUEST_ID = UUID("6c1ae0b8-8d70-46dc-86c0-994247e1a39b")
_ASSESSMENT_ID = UUID("1d40d0d5-ff2a-4780-bc3d-c4a7c55fc3a2")
_MANIFEST_REVISION = "sha256:" + ("0" * 64)
_OPERATION = "stateforge.test/inspect@1"
_METHOD = (
    "/stateforge.sfp.capability_assessment.v1."
    "CapabilityAssessmentService/AssessCapabilities"
)

_Request = capability_assessment_pb2.AssessCapabilitiesRequest
_Response = capability_assessment_pb2.AssessCapabilitiesResponse
_Context = grpc.aio.ServicerContext[_Request, _Response]

_Handler = Callable[
    [_Request, _Context],
    Awaitable[_Response | bytes],
]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=(
            "capability-timeout",
            "capability-malformed",
            "capability-mismatch",
            "capability-limits",
        ),
        required=True,
    )
    return parser.parse_args()


def _request() -> capability_assessment_pb2.AssessCapabilitiesRequest:
    return capability_assessment_pb2.AssessCapabilitiesRequest(
        assessment_request_id=str(_REQUEST_ID),
        expected_provider_instance_id=str(_PROVIDER_ID),
        expected_endpoint_generation=1,
        expected_session_id="installed-boundary-session",
        expected_manifest_revision=_MANIFEST_REVISION,
        execution_context=struct_pb2.Struct(),
        operation_identifiers=(_OPERATION,),
    )


def _scope() -> CapabilityAssessmentScope:
    return CapabilityAssessmentScope(
        provider_instance_id=_PROVIDER_ID,
        endpoint_generation=1,
        session_id="installed-boundary-session",
        manifest_revision=_MANIFEST_REVISION,
        availability_generation=1,
        operation_identifiers=(_OPERATION,),
        execution_context_sha256=hashlib.sha256(b"").digest(),
    )


def _response(
    request: capability_assessment_pb2.AssessCapabilitiesRequest,
) -> capability_assessment_pb2.AssessCapabilitiesResponse:
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime.now(UTC))
    return capability_assessment_pb2.AssessCapabilitiesResponse(
        assessment_request_id=request.assessment_request_id,
        provider_instance_id=request.expected_provider_instance_id,
        endpoint_generation=request.expected_endpoint_generation,
        session_id=request.expected_session_id,
        manifest_revision=request.expected_manifest_revision,
        assessment_id=str(_ASSESSMENT_ID),
        assessment_timestamp=timestamp,
        findings=(
            capability_assessment_pb2.CapabilityFinding(
                operation_identifier=_OPERATION,
                support_state=(
                    capability_assessment_pb2.SUPPORT_STATE_SUPPORTED
                ),
            ),
        ),
    )


async def _server(
    handler: _Handler,
) -> tuple[grpc.aio.Server, grpc.aio.Channel]:
    method = grpc.unary_unary_rpc_method_handler(
        handler,
        request_deserializer=(
            capability_assessment_pb2.AssessCapabilitiesRequest.FromString
        ),
        response_serializer=lambda value: (
            value if isinstance(value, bytes) else value.SerializeToString()
        ),
    )
    server = grpc.aio.server(
        options=(("grpc.max_send_message_length", 32 * 1024 * 1024),)
    )
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                contract.SERVICE_NAME,
                {"AssessCapabilities": method},
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = create_provider_channel(f"127.0.0.1:{port}")
    await channel.channel_ready()
    return server, channel


async def _expect(
    handler: _Handler,
    client: CapabilityAssessmentClient,
    error_type: type[Exception],
) -> None:
    server, channel = await _server(handler)
    try:
        try:
            await client.assess(channel, _request(), scope=_scope())
        except error_type:
            return
        raise AssertionError(f"Expected {error_type.__name__}")
    finally:
        await channel.close()
        await server.stop(grace=None)


async def _timeout() -> dict[str, object]:
    async def handler(request: _Request, context: _Context) -> Never:
        del request, context
        await asyncio.sleep(1)
        raise AssertionError("deadline did not cancel the boundary handler")

    await _expect(
        handler,
        CapabilityAssessmentClient(timeout_seconds=0.01),
        SfpCapabilityAssessmentDeadlineError,
    )
    return {"deadline_enforced": True}


async def _malformed() -> dict[str, object]:
    async def handler(request: _Request, context: _Context) -> bytes:
        del request, context
        return b"\x80"

    await _expect(
        handler,
        CapabilityAssessmentClient(),
        SfpCapabilityAssessmentMalformedResponseError,
    )
    return {"malformed_response_rejected": True}


async def _mismatch() -> dict[str, object]:
    mismatches = (
        ("session_id", "different-session"),
        ("endpoint_generation", 2),
        ("manifest_revision", "sha256:" + ("1" * 64)),
    )
    rejected: list[str] = []
    for field, value in mismatches:
        await _expect(
            _mismatched_handler(field, value),
            CapabilityAssessmentClient(),
            SfpCapabilityAssessmentMalformedResponseError,
        )
        rejected.append(field)
    return {"mismatched_fences_rejected": rejected}


def _mismatched_handler(field: str, value: str | int) -> _Handler:
    """Return one peer handler that corrupts a response fence."""

    async def handler(request: _Request, context: _Context) -> _Response:
        del context
        response = _response(request)
        setattr(response, field, value)
        return response

    return handler


async def _limits() -> dict[str, object]:
    oversized_request = _request()
    oversized_request.execution_context["oversized"] = (
        "x" * contract.MAX_REQUEST_BYTES
    )
    disconnected = create_provider_channel("127.0.0.1:1")
    try:
        try:
            await CapabilityAssessmentClient().assess(
                disconnected,
                oversized_request,
                scope=_scope(),
            )
        except SfpCapabilityAssessmentLimitError:
            request_rejected = True
        else:
            raise AssertionError("Oversized request was accepted")
    finally:
        await disconnected.close()

    async def handler(request: _Request, context: _Context) -> bytes:
        del request, context
        return bytes(contract.MAX_RESPONSE_BYTES + 1)

    await _expect(
        handler,
        CapabilityAssessmentClient(),
        SfpCapabilityAssessmentLimitError,
    )
    return {
        "request_limit_enforced": request_rejected,
        "response_limit_enforced": True,
    }


async def _run(scenario: str) -> dict[str, object]:
    result = await {
        "capability-timeout": _timeout,
        "capability-malformed": _malformed,
        "capability-mismatch": _mismatch,
        "capability-limits": _limits,
    }[scenario]()
    return {"scenario": scenario, **result}


def main() -> None:
    """Run one selected boundary and emit retained JSON evidence."""
    arguments = _arguments()
    sys.stdout.write(
        json.dumps(
            asyncio.run(_run(arguments.scenario)),
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
