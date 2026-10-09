"""Retrieve and accept Provider manifests on the negotiated SFP channel."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Protocol

import grpc
from stateforge_protocol.sfp.manifest.v1 import manifest_pb2, manifest_pb2_grpc

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
from stateforge_agent._sfp._services.manifest.limits import (
    DEFAULT_READ_ATTEMPTS,
    DEFAULT_RETRY_INITIAL_DELAY_SECONDS,
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
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
    ) -> Awaitable[manifest_pb2.GetManifestResponse]:
        """Issue one Manifest Exchange request."""
        ...


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
        if len(set(supported_schema_versions)) != len(
            supported_schema_versions
        ):
            raise ValueError("Manifest schema versions must be unique")
        if any(version <= 0 for version in supported_schema_versions):
            raise ValueError("Manifest schema versions must be positive")
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
        if request.ByteSize() > MAX_REQUEST_BYTES:
            raise SfpManifestError("Manifest request exceeds the P1 limit")

        stub = manifest_pb2_grpc.ManifestServiceStub(channel)
        response = await self._read(stub, request)
        if response.ByteSize() > MAX_RESPONSE_BYTES:
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
        stub: manifest_pb2_grpc.ManifestServiceStub,
        request: manifest_pb2.GetManifestRequest,
    ) -> manifest_pb2.GetManifestResponse:
        """Retry only transient reads, with a finite deadline per attempt."""
        # types-grpcio currently models generated aio unary calls as returning
        # the response directly even though grpc.aio returns an awaitable call.
        call: _ManifestUnaryCall = stub.GetManifest  # type: ignore[assignment]
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
