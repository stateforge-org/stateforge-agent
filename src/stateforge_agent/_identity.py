"""Parse and validate StateForge runtime identities."""

from __future__ import annotations

from uuid import UUID

_UUID_VERSION = 4


def parse_canonical_uuid4(value: str) -> UUID:
    """Return a canonical lowercase UUIDv4 value.

    Args:
        value: Text representation to validate.

    Returns:
        The parsed UUIDv4 identifier.

    Raises:
        ValueError: The value is not a canonical lowercase UUIDv4 string.
    """
    try:
        identifier = UUID(value)
    except ValueError as error:
        raise ValueError("value must be a canonical UUIDv4") from error

    if identifier.version != _UUID_VERSION or str(identifier) != value:
        raise ValueError("value must be a canonical UUIDv4")
    return identifier
