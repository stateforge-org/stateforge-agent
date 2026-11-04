"""Build immutable snapshots for Provider session-state transitions."""

from __future__ import annotations

from uuid import UUID

from stateforge_agent._sfp._runtime.descriptor import (
    ProviderEndpointDescriptor,
)
from stateforge_agent._sfp._runtime.models import (
    ProviderAvailabilityState,
    ProviderAvailabilityStatus,
    ProviderConnectionState,
    ProviderConnectionStatus,
    ProviderSession,
)


def _matches(
    session: ProviderSession,
    descriptor: ProviderEndpointDescriptor,
) -> bool:
    """Return whether a session already represents one descriptor."""
    return (
        session.endpoint_generation == descriptor.endpoint_generation
        and session.socket_path == descriptor.socket_path
    )


def _connected_status(session: ProviderSession) -> ProviderConnectionStatus:
    """Build transport status for one manifest-accepted session."""
    return ProviderConnectionStatus(
        provider_instance_id=session.provider_instance_id,
        state=ProviderConnectionState.CONNECTED,
        active_endpoint_generation=session.endpoint_generation,
        candidate_endpoint_generation=None,
    )


def _availability_status(
    session: ProviderSession,
    state: ProviderAvailabilityState,
    reason: str,
) -> ProviderAvailabilityStatus:
    """Build an eligibility snapshot bound to one exact current session."""
    return ProviderAvailabilityStatus(
        provider_instance_id=session.provider_instance_id,
        state=state,
        endpoint_generation=session.endpoint_generation,
        session_id=session.session_id,
        manifest_revision=session.manifest.key.revision,
        reason=reason,
    )


def _unavailable_status(
    provider_instance_id: UUID,
    reason: str,
) -> ProviderAvailabilityStatus:
    """Build an eligibility snapshot without an accepted current session."""
    return ProviderAvailabilityStatus(
        provider_instance_id=provider_instance_id,
        state=ProviderAvailabilityState.UNAVAILABLE,
        endpoint_generation=None,
        session_id=None,
        manifest_revision=None,
        reason=reason,
    )


def _unavailable_after(
    previous: ProviderAvailabilityStatus | None,
    provider_instance_id: UUID,
    reason: str,
) -> ProviderAvailabilityStatus:
    """Fail closed while retaining the last accepted session identity."""
    if previous is None or previous.endpoint_generation is None:
        return _unavailable_status(provider_instance_id, reason)
    return ProviderAvailabilityStatus(
        provider_instance_id=provider_instance_id,
        state=ProviderAvailabilityState.UNAVAILABLE,
        endpoint_generation=previous.endpoint_generation,
        session_id=previous.session_id,
        manifest_revision=previous.manifest_revision,
        reason=reason,
    )
