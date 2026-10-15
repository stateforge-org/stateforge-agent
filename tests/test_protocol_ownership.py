"""Prevent normative SFP validation from drifting back into the Agent."""

from pathlib import Path
from uuid import uuid4

import pytest

from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.errors import SfpConnectionError
from stateforge_agent._sfp._services.bootstrap import client
from stateforge_agent._sfp.version import SfpVersion


class _UnusedChannel:
    """Satisfy generated stub construction before request validation fails."""

    def unary_unary(self, *_args, **_kwargs):
        return object()


@pytest.mark.asyncio
async def test_bootstrap_requests_cross_protocol_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def reject(_request) -> None:
        nonlocal called
        called = True
        raise client.validation.BootstrapValidationError("canonical rejection")

    monkeypatch.setattr(
        client.validation,
        "validate_handshake_request",
        reject,
    )
    descriptor = ProviderEndpointDescriptor(
        schema_version="1",
        provider_instance_id=uuid4(),
        endpoint_generation=1,
        socket_path=Path("/run/provider.sock"),
    )

    with pytest.raises(SfpConnectionError, match="canonical rejection"):
        await client.perform_handshake(
            _UnusedChannel(),  # type: ignore[arg-type]
            descriptor,
            agent_runtime_id=uuid4(),
            agent_software_version="0.1.0",
            supported_sfp_versions=(SfpVersion(major=1, minor=0),),
            timeout_seconds=1,
        )
    assert called


def test_agent_version_delegates_to_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject(_major: object, _minor: object) -> None:
        raise client.validation.BootstrapValidationError("canonical rejection")

    monkeypatch.setattr(
        "stateforge_agent._sfp.version.validation.validate_protocol_version",
        reject,
    )

    with pytest.raises(ValueError, match="canonical rejection"):
        SfpVersion(major=1, minor=0)
