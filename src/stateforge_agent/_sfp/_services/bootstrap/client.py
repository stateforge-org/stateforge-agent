"""Perform and validate the SFP bootstrap handshake."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import grpc
from stateforge_protocol.sfp.bootstrap.v1 import (
    bootstrap_pb2,
    bootstrap_pb2_grpc,
    contract,
    validation,
)

from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import (
    SfpConnectionError,
    SfpIncompatibleError,
)
from stateforge_agent._sfp.version import SfpVersion


@dataclass(frozen=True, slots=True)
class HandshakeResult:
    """Validated Provider identity and negotiated SFP version."""

    selected_sfp_version: SfpVersion
    provider_runtime_id: UUID
    session_id: str


async def perform_handshake(
    channel: grpc.aio.Channel,
    descriptor: ProviderEndpointDescriptor,
    *,
    agent_runtime_id: UUID,
    agent_software_version: str,
    supported_sfp_versions: tuple[SfpVersion, ...],
    timeout_seconds: float,
) -> HandshakeResult:
    """Negotiate SFP and validate the Provider's response."""
    stub = bootstrap_pb2_grpc.BootstrapServiceStub(channel)
    request = bootstrap_pb2.HandshakeRequest(
        agent_runtime=bootstrap_pb2.AgentRuntime(
            runtime_id=str(agent_runtime_id),
            software_version=agent_software_version,
        ),
        expected_provider_instance_id=str(descriptor.provider_instance_id),
        expected_endpoint_generation=descriptor.endpoint_generation,
        supported_sfp_versions=[
            bootstrap_pb2.ProtocolVersion(
                major=version.major,
                minor=version.minor,
            )
            for version in supported_sfp_versions
        ],
    )
    if request.ByteSize() > contract.MAX_REQUEST_BYTES:
        raise SfpConnectionError(
            "Handshake request exceeds the canonical Bootstrap limit"
        )
    try:
        validation.validate_handshake_request(request)
    except validation.BootstrapValidationError as error:
        raise SfpConnectionError(
            f"Handshake request violates the Bootstrap contract: {error}"
        ) from error

    try:
        response = await stub.Handshake(request, timeout=timeout_seconds)
    except grpc.aio.AioRpcError as error:
        if error.code() is grpc.StatusCode.UNIMPLEMENTED:
            raise SfpIncompatibleError(
                "Provider and Agent share no supported SFP wire version"
            ) from error
        raise SfpConnectionError(
            "Provider handshake failed: "
            f"status={error.code().name} details={error.details()}"
        ) from error

    if response.ByteSize() > contract.MAX_RESPONSE_BYTES:
        raise SfpConnectionError(
            "Handshake response exceeds the canonical Bootstrap limit"
        )
    try:
        validated = validation.validate_handshake_response(response)
    except validation.BootstrapValidationError as error:
        raise SfpConnectionError(
            f"Handshake response violates the Bootstrap contract: {error}"
        ) from error

    if validated.provider_instance_id != descriptor.provider_instance_id:
        raise SfpConnectionError(
            "Handshake returned a different Provider instance identity"
        )
    if validated.endpoint_generation != descriptor.endpoint_generation:
        raise SfpConnectionError(
            "Handshake returned a different endpoint generation"
        )
    selected_version = SfpVersion(
        major=validated.selected_sfp_version.major,
        minor=validated.selected_sfp_version.minor,
    )
    if selected_version not in supported_sfp_versions:
        raise SfpConnectionError(
            "Handshake selected an SFP version the Agent did not offer"
        )

    return HandshakeResult(
        selected_sfp_version=selected_version,
        provider_runtime_id=validated.provider_runtime_id,
        session_id=validated.session_id,
    )
