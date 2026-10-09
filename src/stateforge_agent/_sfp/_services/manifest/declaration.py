"""Normalize and validate immutable Provider manifest declarations."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from google.protobuf.json_format import MessageToDict
from stateforge_protocol.sfp.manifest.v1 import manifest_pb2

from stateforge_agent._identity import parse_canonical_uuid4
from stateforge_agent._sfp._runtime.errors import SfpManifestError
from stateforge_agent._sfp._services.manifest.schema import (
    bounded_json,
    canonical_bytes,
    validate_schema,
    validate_structure,
)
from stateforge_agent._sfp.version import SfpVersion

_REVISION_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class ManifestCacheKey:
    """Identify one validated declaration independently of its session."""

    provider_instance_id: UUID
    sfp_version: SfpVersion
    schema_version: int
    revision: str


@dataclass(frozen=True, slots=True)
class ManifestDeclaration:
    """Retain one validated normalized declaration as canonical JSON."""

    key: ManifestCacheKey
    document: bytes

    @classmethod
    def validate(
        cls,
        response: manifest_pb2.GetManifestResponse,
        *,
        sfp_version: SfpVersion,
    ) -> ManifestDeclaration:
        """Validate a complete response and freeze its declaration."""
        projected = MessageToDict(
            response,
            always_print_fields_with_no_presence=True,
        )
        bounded_json(projected, "response", depth_limit=68)
        validate_structure(projected, "getManifestResponse", "response")

        manifest = projected.get("manifest")
        if not isinstance(manifest, dict):
            raise SfpManifestError("response: manifest must be an object")
        _validate_embedded_schemas(manifest)

        preimage = {
            "manifest": manifest,
            "manifestSchemaVersion": response.manifest_schema_version,
            "providerInstanceId": response.provider_instance_id,
        }
        canonical_preimage = canonical_bytes(preimage)
        revision = "sha256:" + hashlib.sha256(canonical_preimage).hexdigest()
        if revision != response.manifest_revision:
            raise SfpManifestError("response: manifest revision mismatch")

        key = cache_key(response, sfp_version=sfp_version)
        return cls(key=key, document=canonical_bytes(manifest))

    def as_dict(self) -> dict[str, Any]:
        """Return a fresh JSON object for a trusted internal consumer."""
        document = json.loads(self.document)
        if not isinstance(document, dict):
            raise RuntimeError("Validated manifest cache entry is corrupt")
        return document


def cache_key(
    response: manifest_pb2.GetManifestResponse,
    *,
    sfp_version: SfpVersion,
) -> ManifestCacheKey:
    """Build a cache key after validating its scalar components."""
    try:
        provider_instance_id = parse_canonical_uuid4(
            response.provider_instance_id
        )
    except ValueError as error:
        raise SfpManifestError(
            "response: provider identity must be a canonical UUIDv4"
        ) from error
    if response.manifest_schema_version <= 0:
        raise SfpManifestError("response: manifest schema version is invalid")
    if _REVISION_PATTERN.fullmatch(response.manifest_revision) is None:
        raise SfpManifestError("response: manifest revision is invalid")
    return ManifestCacheKey(
        provider_instance_id=provider_instance_id,
        sfp_version=sfp_version,
        schema_version=response.manifest_schema_version,
        revision=response.manifest_revision,
    )


def _validate_embedded_schemas(manifest: dict[str, Any]) -> None:
    """Validate every local JSON Schema without retrieving remote content."""
    configuration = manifest.get("configurationSchema")
    if not isinstance(configuration, dict):
        raise SfpManifestError(
            "manifest.configurationSchema: must be an object"
        )
    validate_schema(configuration, "manifest.configurationSchema")

    operations = manifest.get("operations")
    if not isinstance(operations, dict):
        raise SfpManifestError("manifest.operations: must be an object")
    for index, declaration in enumerate(operations.values()):
        if not isinstance(declaration, dict):
            raise SfpManifestError(
                f"manifest.operations[{index}]: must be an object"
            )
        for field in ("inputSchema", "resultSchema"):
            schema = declaration.get(field)
            if not isinstance(schema, dict):
                raise SfpManifestError(
                    f"manifest.operations[{index}].{field}: must be an object"
                )
            validate_schema(
                schema,
                f"manifest.operations[{index}].{field}",
            )

    triggers = manifest.get("triggers")
    if not isinstance(triggers, dict):
        raise SfpManifestError("manifest.triggers: must be an object")
    for index, declaration in enumerate(triggers.values()):
        if not isinstance(declaration, dict):
            raise SfpManifestError(
                f"manifest.triggers[{index}]: must be an object"
            )
        schema = declaration.get("payloadSchema")
        if not isinstance(schema, dict):
            raise SfpManifestError(
                f"manifest.triggers[{index}].payloadSchema: must be an object"
            )
        validate_schema(schema, f"manifest.triggers[{index}].payloadSchema")
