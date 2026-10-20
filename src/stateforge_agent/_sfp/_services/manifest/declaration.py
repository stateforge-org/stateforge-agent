"""Normalize and validate immutable Provider manifest declarations."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from stateforge_protocol.sfp.manifest.v1 import manifest_pb2
from stateforge_protocol.sfp.manifest.v1 import (
    validation as manifest_validation,
)

from stateforge_agent._sfp._runtime.errors import SfpManifestError
from stateforge_agent._sfp.version import SfpVersion


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
        try:
            document = manifest_validation.validate_manifest_response(response)
        except manifest_validation.ManifestValidationError as error:
            raise SfpManifestError(str(error)) from error

        key = cache_key(response, sfp_version=sfp_version)
        return cls(key=key, document=document)

    def as_dict(self) -> dict[str, Any]:
        """Return a fresh JSON object for a trusted internal consumer."""
        document = json.loads(self.document)
        if not isinstance(document, dict):
            raise RuntimeError("Validated manifest cache entry is corrupt")
        return document

    @property
    def operation_identifiers(self) -> frozenset[str]:
        """Return the versioned operations declared by this snapshot."""
        operations = self.as_dict().get("operations")
        if not isinstance(operations, dict):
            raise RuntimeError("Validated manifest cache entry is corrupt")
        return frozenset(operations)


def cache_key(
    response: manifest_pb2.GetManifestResponse,
    *,
    sfp_version: SfpVersion,
) -> ManifestCacheKey:
    """Build a cache key after validating its scalar components."""
    try:
        key = manifest_validation.validate_manifest_key(
            response.provider_instance_id,
            response.manifest_schema_version,
            response.manifest_revision,
        )
    except manifest_validation.ManifestValidationError as error:
        raise SfpManifestError(str(error)) from error
    return ManifestCacheKey(
        provider_instance_id=key.provider_instance_id,
        sfp_version=sfp_version,
        schema_version=key.schema_version,
        revision=key.revision,
    )
