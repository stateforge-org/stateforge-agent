"""Perform and validate the SFP bootstrap handshake."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import grpc
from stateforge_protocol.sfp.bootstrap.v1 import (
    bootstrap_pb2,
    bootstrap_pb2_grpc,
    contract,
)

from stateforge_agent._identity import parse_canonical_uuid4
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

    if response.provider_instance_id != str(descriptor.provider_instance_id):
        raise SfpConnectionError(
            "Handshake returned a different Provider instance identity"
        )
    if response.endpoint_generation != descriptor.endpoint_generation:
        raise SfpConnectionError(
            "Handshake returned a different endpoint generation"
        )
    if not response.HasField("selected_sfp_version"):
        raise SfpConnectionError("Handshake did not select an SFP version")
    try:
        selected_version = SfpVersion(
            major=response.selected_sfp_version.major,
            minor=response.selected_sfp_version.minor,
        )
    except ValueError as error:
        raise SfpConnectionError(
            "Handshake selected an invalid SFP version"
        ) from error
    if selected_version not in supported_sfp_versions:
        raise SfpConnectionError(
            "Handshake selected an SFP version the Agent did not offer"
        )

    try:
        provider_runtime_id = parse_canonical_uuid4(
            response.provider_runtime_id
        )
    except ValueError as error:
        raise SfpConnectionError(
            "Handshake provider_runtime_id must be a canonical UUIDv4"
        ) from error

    session_id = response.session_id
    if not session_id or len(session_id) > contract.MAX_SESSION_ID_CODE_POINTS:
        raise SfpConnectionError(
            "Handshake session_id must contain 1-"
            f"{contract.MAX_SESSION_ID_CODE_POINTS} Unicode code points"
        )

    return HandshakeResult(
        selected_sfp_version=selected_version,
        provider_runtime_id=provider_runtime_id,
        session_id=session_id,
    )
