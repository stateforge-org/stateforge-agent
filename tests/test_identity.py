"""Tests for canonical StateForge runtime identities."""

from uuid import UUID

import pytest

from stateforge_agent._identity import parse_canonical_uuid4


def test_parses_canonical_uuid4() -> None:
    value = "7f7db7d8-68b4-4f71-9769-065b5c4f941c"

    assert parse_canonical_uuid4(value) == UUID(value)


@pytest.mark.parametrize(
    "value",
    [
        "not-a-uuid",
        "7F7DB7D8-68B4-4F71-9769-065B5C4F941C",
        "032d9b0e-fd9f-11f0-a2a2-0242ac120002",
    ],
)
def test_rejects_noncanonical_or_non_uuid4_values(value: str) -> None:
    with pytest.raises(ValueError, match="canonical UUIDv4"):
        parse_canonical_uuid4(value)
