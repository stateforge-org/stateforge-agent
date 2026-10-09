"""Validate bounded Manifest JSON and embedded schemas without retrieval."""

from __future__ import annotations

import io
import json
from collections import deque
from functools import lru_cache
from importlib.resources import files
from typing import Any

import rfc8785
from jsonschema import Draft202012Validator
from referencing import Registry
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012

from stateforge_agent._sfp._runtime.errors import SfpManifestError
from stateforge_agent._sfp._services.manifest.limits import (
    MAX_CANONICAL_REVISION_PREIMAGE_BYTES,
    MAX_EMBEDDED_JSON_DEPTH,
)

JSON_SCHEMA_DRAFT_2020_12_URI = str(Draft202012Validator.META_SCHEMA["$id"])
_MAX_JSON_NODES = 100_000
_MAX_SCHEMA_NODES = 25_000
_SCHEMA_MAPS = (
    "$defs",
    "definitions",
    "properties",
    "patternProperties",
    "dependentSchemas",
)
_SCHEMA_LISTS = ("allOf", "anyOf", "oneOf", "prefixItems")
_SCHEMA_SINGLE = (
    "additionalProperties",
    "unevaluatedProperties",
    "propertyNames",
    "items",
    "unevaluatedItems",
    "contains",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
)
_SAME_INSTANCE = {
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
}
_VALUES = {"default", "examples", "const", "enum"}


def bounded_json(  # noqa: PLR0912
    value: object,
    path: str,
    *,
    depth_limit: int = MAX_EMBEDDED_JSON_DEPTH,
) -> None:
    """Reject excessive or non-JSON content before schema processing."""
    pending = [(value, 0)]
    nodes = 0
    size = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > _MAX_JSON_NODES:
            raise SfpManifestError(f"{path}: JSON resource limit exceeded")
        if isinstance(item, (dict, list)):
            depth += 1
            if depth > depth_limit:
                raise SfpManifestError(f"{path}: JSON nesting limit exceeded")
            size += len(item) + 2
            if isinstance(item, dict):
                for key, child in item.items():
                    if not isinstance(key, str):
                        raise SfpManifestError(
                            f"{path}: JSON keys must be strings"
                        )
                    pending.append((key, depth))
                    pending.append((child, depth))
            else:
                pending.extend((child, depth) for child in item)
        elif isinstance(item, str):
            try:
                size += len(item.encode("utf-8")) + 2
            except UnicodeError:
                raise SfpManifestError(f"{path}: invalid Unicode") from None
        elif item is None or isinstance(item, (bool, int, float)):
            size += 1
        else:
            raise SfpManifestError(f"{path}: non-JSON value")
        if size > MAX_CANONICAL_REVISION_PREIMAGE_BYTES:
            raise SfpManifestError(f"{path}: JSON resource limit exceeded")


class _BoundedBuffer(io.BytesIO):
    """Stop RFC 8785 output before it exceeds the Protocol limit."""

    def write(self, data: bytes, /) -> int:  # type: ignore[override]
        if self.tell() + len(data) > MAX_CANONICAL_REVISION_PREIMAGE_BYTES:
            raise SfpManifestError(
                "manifest: canonical preimage limit exceeded"
            )
        return super().write(data)


def canonical_bytes(value: dict[str, Any]) -> bytes:
    """Return bounded RFC 8785 bytes for one JSON-compatible value."""
    stream = _BoundedBuffer()
    try:
        rfc8785.dump(value, stream)
    except rfc8785.CanonicalizationError:
        raise SfpManifestError(
            "manifest: non-interoperable JSON value"
        ) from None
    return stream.getvalue()


@lru_cache(maxsize=16)
def contract_validator(definition: str) -> Draft202012Validator:
    """Load a validation projection from the installed Protocol package."""
    document = json.loads(
        files("stateforge_protocol.sfp.manifest.v1")
        .joinpath("manifest.schema.json")
        .read_text(encoding="utf-8")
    )
    return Draft202012Validator(
        {"$ref": f"#/$defs/{definition}", "$defs": document["$defs"]}
    )


def validate_structure(value: object, definition: str, path: str) -> None:
    """Validate a packaged P1 shape without exposing rejected values."""
    error = next(contract_validator(definition).iter_errors(value), None)
    if error is not None:
        raise SfpManifestError(
            f"{path}: violates P1 {error.validator} constraint"
        )


def validate_schema(  # noqa: C901, PLR0912, PLR0915
    document: dict[str, Any],
    path: str,
) -> None:
    """Resolve local references and reject indirect secret values."""
    bounded_json(document, path)
    canonical_bytes(document)
    validate_structure(document, "schemaDocument", path)
    nodes: dict[int, dict[str, Any]] = {}
    children: dict[int, set[int]] = {}
    same: dict[int, set[int]] = {}
    pending = [document]
    while pending:
        node = pending.pop()
        key = id(node)
        if key in nodes:
            continue
        if (
            node.get("$schema", JSON_SCHEMA_DRAFT_2020_12_URI)
            != JSON_SCHEMA_DRAFT_2020_12_URI
        ):
            raise SfpManifestError(
                f"{path}: unsupported nested schema dialect"
            )
        nodes[key] = node
        if len(nodes) > _MAX_SCHEMA_NODES:
            raise SfpManifestError(f"{path}: schema node limit exceeded")
        children.setdefault(key, set())
        same.setdefault(key, set())
        for keyword, value in node.items():
            candidates: list[object] = []
            if keyword in _SCHEMA_MAPS and isinstance(value, dict):
                candidates = list(value.values())
            elif keyword in _SCHEMA_LISTS and isinstance(value, list):
                candidates = value
            elif keyword in _SCHEMA_SINGLE:
                candidates = [value]
            for child in candidates:
                if isinstance(child, dict):
                    pending.append(child)
                    if keyword not in {"$defs", "definitions"}:
                        children[key].add(id(child))
                    if keyword in _SAME_INSTANCE:
                        same[key].add(id(child))
                        same.setdefault(id(child), set()).add(key)

    resource = DRAFT202012.create_resource(document)
    registry: Registry[Any] = Registry().with_resource("", resource)
    resolver = registry.resolver("")
    for key, node in nodes.items():
        for keyword in ("$ref", "$dynamicRef"):
            if keyword not in node:
                continue
            try:
                target = resolver.lookup(node[keyword]).contents
            except Unresolvable:
                raise SfpManifestError(
                    f"{path}: unresolved local reference"
                ) from None
            if isinstance(target, bool):
                continue
            if not isinstance(target, dict) or id(target) not in nodes:
                raise SfpManifestError(
                    f"{path}: reference does not name a schema"
                )
            children[key].add(id(target))
            same[key].add(id(target))
            same[id(target)].add(key)

    secrets = {
        key for key, node in nodes.items() if node.get("writeOnly") is True
    }
    effective = _reachable(secrets, same)
    below = _reachable(effective, children)
    parents: dict[int, set[int]] = {key: set() for key in nodes}
    for key, links in children.items():
        for child in links:
            parents[child].add(key)
    protected = below | _reachable(effective, parents)
    for key in protected:
        if _VALUES.intersection(nodes[key]):
            raise SfpManifestError(
                f"{path}: secret schema contains value annotations"
            )


def _reachable(roots: set[int], edges: dict[int, set[int]]) -> set[int]:
    """Return every schema node reachable from the supplied roots."""
    visited = set(roots)
    pending = deque(roots)
    while pending:
        for child in edges[pending.popleft()]:
            if child not in visited:
                visited.add(child)
                pending.append(child)
    return visited
