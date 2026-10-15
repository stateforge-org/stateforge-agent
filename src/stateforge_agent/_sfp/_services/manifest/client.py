"""Retrieve and accept Provider manifests on the negotiated SFP channel."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Protocol

import grpc
from google.protobuf.message import DecodeError
from stateforge_protocol.sfp.manifest.v1 import contract as manifest_contract
from stateforge_protocol.sfp.manifest.v1 import manifest_pb2

from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import (
    SfpConnectionError,
    SfpIncompatibleError,
    SfpManifestError,
)
from stateforge_agent._sfp._services.manifest.cache import ManifestCache
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestDeclaration,
    cache_key,
)
from stateforge_agent._sfp._services.manifest.policy import (
    DEFAULT_READ_ATTEMPTS,
    DEFAULT_RETRY_INITIAL_DELAY_SECONDS,
    MAX_MANIFEST_SCHEMA_VERSIONS,
    SUPPORTED_MANIFEST_SCHEMA_VERSIONS,
)
from stateforge_agent._sfp.version import SfpVersion


class ManifestRetriever(Protocol):
    """Retrieve one validated declaration over an existing SFP channel."""

    async def retrieve(
        self,
        channel: grpc.aio.Channel,
        descriptor: ProviderEndpointDescriptor,
        *,
        sfp_version: SfpVersion,
        session_id: str,
    ) -> ManifestDeclaration:
        """Return a declaration bound to the supplied negotiated session."""
        ...


class _ManifestUnaryCall(Protocol):
    """Type the generated asynchronous unary call at its dynamic boundary."""

    def __call__(
        self,
        request: manifest_pb2.GetManifestRequest,
        *,
        timeout: float,
    ) -> Awaitable[_DecodedManifestResponse | None]:
        """Issue one Manifest Exchange request."""
        ...


@dataclass(frozen=True, slots=True)
class _DecodedManifestResponse:
    """Carry a decoded response or a safe wire-rejection reason."""

    response: manifest_pb2.GetManifestResponse | None = None
    rejection: str | None = None


_GET_MANIFEST_METHOD = (
    "/stateforge.sfp.manifest.v1.ManifestService/GetManifest"
)
_MAX_UINT32 = (1 << 32) - 1


class ManifestClient:
    """Perform bounded Manifest Exchange and cache only accepted snapshots."""

    def __init__(
        self,
        *,
        cache: ManifestCache | None = None,
        supported_schema_versions: tuple[int, ...] = (
            SUPPORTED_MANIFEST_SCHEMA_VERSIONS
        ),
        timeout_seconds: float = 5.0,
        read_attempts: int = DEFAULT_READ_ATTEMPTS,
        retry_initial_delay_seconds: float = (
            DEFAULT_RETRY_INITIAL_DELAY_SECONDS
        ),
    ) -> None:
        """Configure one reusable client for the shared Provider channels."""
        if not supported_schema_versions:
            raise ValueError("At least one manifest schema must be supported")
        if len(supported_schema_versions) > MAX_MANIFEST_SCHEMA_VERSIONS:
            raise ValueError(
                "Manifest schema versions may contain at most "
                f"{MAX_MANIFEST_SCHEMA_VERSIONS} values"
            )
        if any(
            not isinstance(version, int) or isinstance(version, bool)
            for version in supported_schema_versions
        ):
            raise ValueError("Manifest schema versions must be integers")
        if len(set(supported_schema_versions)) != len(
            supported_schema_versions
        ):
            raise ValueError("Manifest schema versions must be unique")
        if any(
            version <= 0 or version > _MAX_UINT32
            for version in supported_schema_versions
        ):
            raise ValueError(
                "Manifest schema versions must be positive uint32 values"
            )
        if timeout_seconds <= 0:
            raise ValueError("Manifest timeout must be positive")
        if read_attempts <= 0:
            raise ValueError("Manifest read attempts must be positive")
        if retry_initial_delay_seconds < 0:
            raise ValueError("Manifest retry delay must not be negative")
        self._cache = cache if cache is not None else ManifestCache()
        self._supported_schema_versions = supported_schema_versions
        self._timeout_seconds = timeout_seconds
        self._read_attempts = read_attempts
        self._retry_initial_delay_seconds = retry_initial_delay_seconds

    async def retrieve(
        self,
        channel: grpc.aio.Channel,
        descriptor: ProviderEndpointDescriptor,
        *,
        sfp_version: SfpVersion,
        session_id: str,
    ) -> ManifestDeclaration:
        """Retrieve, validate, and cache one complete manifest snapshot."""
        request = manifest_pb2.GetManifestRequest(
            expected_provider_instance_id=str(descriptor.provider_instance_id),
            expected_endpoint_generation=descriptor.endpoint_generation,
            supported_manifest_schema_versions=(
                self._supported_schema_versions
            ),
            expected_session_id=session_id,
        )
        if request.ByteSize() > manifest_contract.MAX_REQUEST_BYTES:
            raise SfpManifestError("Manifest request exceeds the P1 limit")

        # Bind the P1 response limit to this procedure rather than raising the
        # ceiling for every service multiplexed over the shared SFP channel.
        call: _ManifestUnaryCall = channel.unary_unary(
            _GET_MANIFEST_METHOD,
            request_serializer=(
                manifest_pb2.GetManifestRequest.SerializeToString
            ),
            response_deserializer=_decode_manifest_response,
        )
        decoded = await self._read(call, request)
        if decoded is None:
            raise SfpManifestError("Manifest response protobuf is malformed")
        if decoded.rejection is not None:
            raise SfpManifestError(decoded.rejection)
        response = decoded.response
        if response is None:
            raise SfpManifestError("Manifest response protobuf is malformed")
        if response.ByteSize() > manifest_contract.MAX_RESPONSE_BYTES:
            raise SfpManifestError("Manifest response exceeds the P1 limit")
        self._validate_header(
            response,
            descriptor=descriptor,
            session_id=session_id,
        )
        key = cache_key(response, sfp_version=sfp_version)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        declaration = ManifestDeclaration.validate(
            response,
            sfp_version=sfp_version,
        )
        self._cache.put(declaration)
        return declaration

    async def _read(
        self,
        call: _ManifestUnaryCall,
        request: manifest_pb2.GetManifestRequest,
    ) -> _DecodedManifestResponse | None:
        """Retry only transient reads, with a finite deadline per attempt."""
        for attempt in range(self._read_attempts):
            try:
                return await call(
                    request,
                    timeout=self._timeout_seconds,
                )
            except grpc.aio.AioRpcError as error:
                code = error.code()
                if code is grpc.StatusCode.UNIMPLEMENTED:
                    raise SfpIncompatibleError(
                        "Provider and Agent share no manifest schema version"
                    ) from error
                if code is grpc.StatusCode.FAILED_PRECONDITION:
                    raise SfpConnectionError(
                        "Provider rejected the current manifest session"
                    ) from error
                if (
                    code is grpc.StatusCode.UNAVAILABLE
                    and attempt + 1 < self._read_attempts
                ):
                    await asyncio.sleep(
                        self._retry_initial_delay_seconds * (2**attempt)
                    )
                    continue
                if code in {
                    grpc.StatusCode.INVALID_ARGUMENT,
                    grpc.StatusCode.RESOURCE_EXHAUSTED,
                    grpc.StatusCode.INTERNAL,
                }:
                    raise SfpManifestError(
                        f"Provider manifest exchange failed: {code.name}"
                    ) from error
                raise SfpConnectionError(
                    f"Provider manifest exchange failed: {code.name}"
                ) from error
        raise RuntimeError("Manifest read loop exhausted unexpectedly")

    def _validate_header(
        self,
        response: manifest_pb2.GetManifestResponse,
        *,
        descriptor: ProviderEndpointDescriptor,
        session_id: str,
    ) -> None:
        """Validate per-call identity before consulting the cache."""
        if response.provider_instance_id != str(
            descriptor.provider_instance_id
        ):
            raise SfpManifestError(
                "Manifest returned a different Provider identity"
            )
        if response.endpoint_generation != descriptor.endpoint_generation:
            raise SfpManifestError(
                "Manifest returned a different endpoint generation"
            )
        if response.session_id != session_id:
            raise SfpManifestError(
                "Manifest returned a different negotiated session"
            )
        if (
            response.manifest_schema_version
            not in self._supported_schema_versions
        ):
            raise SfpManifestError(
                "Manifest selected a schema version the Agent did not offer"
            )
        if not response.HasField("manifest"):
            raise SfpManifestError("Manifest response is incomplete")


def _decode_manifest_response(payload: bytes) -> _DecodedManifestResponse:
    """Bound and decode one serialized P1 response without raising in gRPC."""
    if len(payload) > manifest_contract.MAX_RESPONSE_BYTES:
        return _DecodedManifestResponse(
            rejection="Manifest response exceeds the P1 limit"
        )
    try:
        response = manifest_pb2.GetManifestResponse.FromString(payload)
    except DecodeError:
        return _DecodedManifestResponse(
            rejection="Manifest response protobuf is malformed"
        )
    return _DecodedManifestResponse(response=response)
