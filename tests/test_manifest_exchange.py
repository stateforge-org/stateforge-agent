"""Manifest validation, caching, transport, and retry evidence for A1."""

import hashlib
import tempfile
from collections import deque
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import grpc
import pytest
import rfc8785
from google.protobuf.json_format import MessageToDict, ParseDict
from stateforge_protocol.sfp.manifest.v1 import manifest_pb2, manifest_pb2_grpc

from stateforge_agent._sfp import SfpManifestError, SfpVersion
from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._services.manifest.cache import ManifestCache
from stateforge_agent._sfp._services.manifest.client import ManifestClient
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestDeclaration,
    cache_key,
)
from stateforge_agent._sfp._services.manifest.schema import validate_schema

_PROVIDER_INSTANCE_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_OTHER_PROVIDER_INSTANCE_ID = UUID("68d818de-3671-471d-a0ba-044cf60e2c8e")
_SFP_VERSION = SfpVersion(major=1, minor=0)
_SESSION_ID = "manifest-session-A"
_GOLDEN_REVISION = (
    "sha256:6bfdf3aedc2d4e45d77d4787cc0e744a488208b2510d81788291f51d24a623e8"
)
_EXPECTED_RETRY_CALLS = 3

_MANIFEST = {
    "providerIdentity": "stateforge.git",
    "implementationVersion": "1.0.0",
    "displayName": "Git Provider",
    "operations": {
        "stateforge.git/reference.resolve@1": {
            "inputSchema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "resultSchema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "implements": [],
        }
    },
    "runtimeRequirements": {
        "executable.git": {
            "description": "Git executable available to the Provider",
            "versionConstraint": ">=2.40",
        }
    },
    "platforms": [{"operatingSystem": "linux", "architecture": "x86_64"}],
    "configurationSchema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "repository": {"type": "string"},
            "token": {"type": "string", "writeOnly": True},
        },
        "additionalProperties": False,
        "required": ["repository"],
    },
    "triggers": {},
}


def _response(
    *,
    provider_instance_id: UUID = _PROVIDER_INSTANCE_ID,
    endpoint_generation: int = 4,
    session_id: str = _SESSION_ID,
    manifest: dict | None = None,
) -> manifest_pb2.GetManifestResponse:
    document = deepcopy(_MANIFEST if manifest is None else manifest)
    message = ParseDict(document, manifest_pb2.ProviderManifest())
    projected = MessageToDict(
        message,
        always_print_fields_with_no_presence=True,
    )
    preimage = {
        "manifest": projected,
        "manifestSchemaVersion": 1,
        "providerInstanceId": str(provider_instance_id),
    }
    revision = "sha256:" + hashlib.sha256(rfc8785.dumps(preimage)).hexdigest()
    return manifest_pb2.GetManifestResponse(
        provider_instance_id=str(provider_instance_id),
        endpoint_generation=endpoint_generation,
        manifest_schema_version=1,
        manifest_revision=revision,
        manifest=message,
        session_id=session_id,
    )


def _descriptor() -> ProviderEndpointDescriptor:
    return ProviderEndpointDescriptor(
        schema_version="1",
        provider_instance_id=_PROVIDER_INSTANCE_ID,
        endpoint_generation=4,
        socket_path=Path(tempfile.gettempdir()) / "unused-provider.sock",
    )


class _ManifestPeer(manifest_pb2_grpc.ManifestServiceServicer):
    """Serve scripted responses through a real gRPC Manifest service."""

    def __init__(self, responses: list[object]) -> None:
        self.responses = deque(responses)
        self.requests: list[manifest_pb2.GetManifestRequest] = []

    async def GetManifest(self, request, context):  # noqa: N802
        self.requests.append(request)
        response = self.responses.popleft()
        if isinstance(response, grpc.StatusCode):
            await context.abort(response, "scripted failure")
        assert isinstance(response, manifest_pb2.GetManifestResponse)
        return response


async def _start_peer(
    peer: _ManifestPeer,
) -> tuple[grpc.aio.Server, grpc.aio.Channel]:
    server = grpc.aio.server()
    manifest_pb2_grpc.add_ManifestServiceServicer_to_server(peer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    await channel.channel_ready()
    return server, channel


def test_validates_the_protocol_conformance_revision() -> None:
    response = _response()

    declaration = ManifestDeclaration.validate(
        response,
        sfp_version=_SFP_VERSION,
    )

    assert response.manifest_revision == _GOLDEN_REVISION
    assert declaration.key.revision == _GOLDEN_REVISION
    assert declaration.as_dict() == _MANIFEST


def test_installation_identity_contributes_to_revision() -> None:
    first = _response()
    second = _response(provider_instance_id=_OTHER_PROVIDER_INSTANCE_ID)

    assert first.manifest_revision != second.manifest_revision


def test_rejects_revision_mismatch() -> None:
    response = _response()
    response.manifest_revision = "sha256:" + ("0" * 64)

    with pytest.raises(SfpManifestError, match="revision mismatch"):
        ManifestDeclaration.validate(response, sfp_version=_SFP_VERSION)


@pytest.mark.parametrize(
    "schema",
    [
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"value": {"$ref": "https://example.invalid/x"}},
        },
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "secret": {
                    "type": "string",
                    "writeOnly": True,
                    "default": "must-not-leak",
                }
            },
        },
    ],
)
def test_rejects_unsafe_embedded_schemas_without_echoing_values(
    schema: dict,
) -> None:
    with pytest.raises(SfpManifestError) as error:
        validate_schema(schema, "manifest.configurationSchema")

    assert "must-not-leak" not in str(error.value)
    assert "example.invalid" not in str(error.value)


def test_manifest_cache_is_bounded() -> None:
    cache = ManifestCache(capacity=1)
    first = ManifestDeclaration.validate(_response(), sfp_version=_SFP_VERSION)
    changed_manifest = deepcopy(_MANIFEST)
    changed_manifest["displayName"] = "Changed Provider"
    second = ManifestDeclaration.validate(
        _response(manifest=changed_manifest),
        sfp_version=_SFP_VERSION,
    )

    cache.put(first)
    cache.put(second)

    assert len(cache) == 1
    assert cache.get(first.key) is None
    assert cache.get(second.key) is second


@pytest.mark.asyncio
async def test_real_rpc_retries_and_reuses_only_validated_cache() -> None:
    original = _response()
    conflicting = _response()
    conflicting.manifest.display_name = "Unvalidated replacement"
    conflicting.manifest_revision = original.manifest_revision
    peer = _ManifestPeer([grpc.StatusCode.UNAVAILABLE, original, conflicting])
    server, channel = await _start_peer(peer)
    client = ManifestClient(read_attempts=2, retry_initial_delay_seconds=0)

    try:
        first = await client.retrieve(
            channel,
            _descriptor(),
            sfp_version=_SFP_VERSION,
            session_id=_SESSION_ID,
        )
        repeated = await client.retrieve(
            channel,
            _descriptor(),
            sfp_version=_SFP_VERSION,
            session_id=_SESSION_ID,
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert repeated is first
    assert first.as_dict()["displayName"] == "Git Provider"
    assert len(peer.requests) == _EXPECTED_RETRY_CALLS
    assert list(peer.requests[0].supported_manifest_schema_versions) == [1]
    assert peer.requests[0].expected_session_id == _SESSION_ID


@pytest.mark.asyncio
async def test_changed_revision_replaces_the_normalized_declaration() -> None:
    changed_manifest = deepcopy(_MANIFEST)
    changed_manifest["displayName"] = "Changed Provider"
    peer = _ManifestPeer([_response(), _response(manifest=changed_manifest)])
    server, channel = await _start_peer(peer)
    client = ManifestClient()

    try:
        first = await client.retrieve(
            channel,
            _descriptor(),
            sfp_version=_SFP_VERSION,
            session_id=_SESSION_ID,
        )
        changed = await client.retrieve(
            channel,
            _descriptor(),
            sfp_version=_SFP_VERSION,
            session_id=_SESSION_ID,
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert changed is not first
    assert changed.key.revision != first.key.revision
    assert changed.as_dict()["displayName"] == "Changed Provider"


@pytest.mark.parametrize(
    ("change", "expected_error"),
    [
        ("identity", "different Provider identity"),
        ("generation", "different endpoint generation"),
        ("session", "different negotiated session"),
        ("schema", "schema version the Agent did not offer"),
        ("missing_manifest", "response is incomplete"),
    ],
)
@pytest.mark.asyncio
async def test_rejects_invalid_response_headers(
    change: str,
    expected_error: str,
) -> None:
    response = _response()
    if change == "identity":
        response.provider_instance_id = str(_OTHER_PROVIDER_INSTANCE_ID)
    elif change == "generation":
        response.endpoint_generation = 5
    elif change == "session":
        response.session_id = "other-session"
    elif change == "schema":
        response.manifest_schema_version = 2
    else:
        response.ClearField("manifest")
    peer = _ManifestPeer([response])
    server, channel = await _start_peer(peer)

    try:
        with pytest.raises(SfpManifestError, match=expected_error):
            await ManifestClient().retrieve(
                channel,
                _descriptor(),
                sfp_version=_SFP_VERSION,
                session_id=_SESSION_ID,
            )
    finally:
        await channel.close()
        await server.stop(grace=None)


def test_cache_key_rejects_malformed_revision() -> None:
    response = _response()
    response.manifest_revision = "not-a-revision"

    with pytest.raises(SfpManifestError, match="revision is invalid"):
        cache_key(response, sfp_version=_SFP_VERSION)
