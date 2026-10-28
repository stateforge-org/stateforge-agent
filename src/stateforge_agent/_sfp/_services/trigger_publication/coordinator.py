"""Validate and durably accept Provider trigger publications."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from uuid import UUID

import grpc
from google.protobuf.json_format import MessageToDict
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from stateforge_protocol.sfp import primitives
from stateforge_protocol.sfp.trigger_publication.v1 import (
    trigger_publication_pb2,
    validation,
)

from stateforge_agent._journal import SqliteJournalError
from stateforge_agent._sfp._services.trigger_publication.client import (
    SfpTriggerPublicationProtocolError,
    TriggerPublicationClient,
    TriggerPublicationConsumer,
    TriggerStreamBinding,
)
from stateforge_agent._sfp._services.trigger_publication.store import (
    TriggerAcceptanceContext,
    TriggerAcceptanceDecision,
    TriggerPublicationStore,
)


@dataclass(frozen=True, slots=True)
class TriggerPublicationLease:
    """Expose only current session facts required by Trigger Publication."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    manifest_revision: str
    availability_generation: int
    trigger_schemas: Mapping[str, Mapping[str, object]]
    channel: grpc.aio.Channel = field(repr=False, compare=False)
    session_token: object = field(repr=False, compare=False)


TriggerPublicationCurrentnessCheck = Callable[
    [TriggerPublicationLease], Awaitable[None]
]


class TriggerPublicationCoordinator:
    """Own stream setup, manifest validation, and durable acknowledgement."""

    def __init__(
        self,
        store: TriggerPublicationStore,
        *,
        maximum_in_flight_publications: int,
        client: TriggerPublicationConsumer | None = None,
    ) -> None:
        """Bind the procedure to its durable responsibility store."""
        if maximum_in_flight_publications <= 0:
            raise ValueError("maximum_in_flight_publications must be positive")
        self._store = store
        self._maximum_in_flight_publications = maximum_in_flight_publications
        self._client = client or TriggerPublicationClient()

    async def run(
        self,
        lease: TriggerPublicationLease,
        *,
        assert_current: TriggerPublicationCurrentnessCheck,
    ) -> None:
        """Consume one stream attempt under an exact current-session lease."""
        await assert_current(lease)

        async def handle(
            publication: trigger_publication_pb2.TriggerPublication,
        ) -> trigger_publication_pb2.TriggerAcknowledgement:
            return await self._accept(
                lease,
                publication,
                assert_current=assert_current,
            )

        await self._client.consume(
            lease.channel,
            TriggerStreamBinding(
                provider_instance_id=str(lease.provider_instance_id),
                endpoint_generation=lease.endpoint_generation,
                session_id=lease.session_id,
                manifest_revision=lease.manifest_revision,
                maximum_in_flight_publications=(
                    self._maximum_in_flight_publications
                ),
            ),
            handle,
        )

    async def _accept(  # noqa: PLR0911
        self,
        lease: TriggerPublicationLease,
        publication: trigger_publication_pb2.TriggerPublication,
        *,
        assert_current: TriggerPublicationCurrentnessCheck,
    ) -> trigger_publication_pb2.TriggerAcknowledgement:
        """Return a terminal decision only after all required boundaries."""
        await assert_current(lease)
        invalid = _canonical_publication_error(publication)
        if invalid is not None:
            return _rejection(
                publication,
                trigger_publication_pb2.TRIGGER_REJECTION_REASON_INVALID_PUBLICATION,
                retryable=False,
                explanation=invalid,
            )
        schema = lease.trigger_schemas.get(publication.trigger_identifier)
        if schema is None:
            return _rejection(
                publication,
                trigger_publication_pb2.TRIGGER_REJECTION_REASON_TRIGGER_NOT_DECLARED,
                retryable=False,
                explanation=(
                    "Trigger is not declared by the accepted manifest."
                ),
            )
        if not _payload_matches(schema, publication):
            return _rejection(
                publication,
                trigger_publication_pb2.TRIGGER_REJECTION_REASON_PAYLOAD_SCHEMA_VIOLATION,
                retryable=False,
                explanation="Trigger payload does not match its declaration.",
            )

        try:
            result = await self._store.accept(
                TriggerAcceptanceContext(
                    provider_instance_id=lease.provider_instance_id,
                    endpoint_generation=lease.endpoint_generation,
                    session_id=lease.session_id,
                    manifest_revision=lease.manifest_revision,
                ),
                publication,
            )
        except SqliteJournalError:
            return _rejection(
                publication,
                trigger_publication_pb2.TRIGGER_REJECTION_REASON_DELIVERY_RESPONSIBILITY_UNAVAILABLE,
                retryable=True,
                explanation="Durable delivery responsibility is unavailable.",
            )

        await assert_current(lease)
        if result.decision is TriggerAcceptanceDecision.ACCEPTED_NEW:
            return _accepted(
                publication,
                trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_NEW,
            )
        if result.decision is TriggerAcceptanceDecision.ACCEPTED_DUPLICATE:
            return _accepted(
                publication,
                trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_ACCEPTED_DUPLICATE,
            )
        if result.decision is TriggerAcceptanceDecision.IDEMPOTENCY_CONFLICT:
            return _rejection(
                publication,
                trigger_publication_pb2.TRIGGER_REJECTION_REASON_IDEMPOTENCY_CONFLICT,
                retryable=False,
                explanation=(
                    "Publication ID was already accepted with different "
                    "content."
                ),
            )
        return _rejection(
            publication,
            trigger_publication_pb2.TRIGGER_REJECTION_REASON_AGENT_CAPACITY_EXHAUSTED,
            retryable=True,
            explanation="Agent trigger acceptance capacity is exhausted.",
        )


def _canonical_publication_error(
    publication: trigger_publication_pb2.TriggerPublication,
) -> str | None:
    """Return a safe publication-level error or fail invalid correlation."""
    try:
        primitives.parse_canonical_uuid4(publication.publication_id)
    except primitives.ProtocolScalarError as error:
        raise SfpTriggerPublicationProtocolError(
            "Trigger publication identity is invalid"
        ) from error
    try:
        validation.validate_trigger_publication(publication)
    except validation.TriggerPublicationValidationError:
        return "Trigger publication failed canonical validation."
    return None


def _payload_matches(
    schema: Mapping[str, object],
    publication: trigger_publication_pb2.TriggerPublication,
) -> bool:
    """Validate payload against the exact accepted manifest declaration."""
    payload = MessageToDict(publication.payload)
    try:
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(payload)
    except (SchemaError, ValidationError):
        return False
    return True


def _accepted(
    publication: trigger_publication_pb2.TriggerPublication,
    disposition: (
        trigger_publication_pb2.TriggerAcknowledgementDisposition.ValueType
    ),
) -> trigger_publication_pb2.TriggerAcknowledgement:
    """Construct and validate an accepted terminal acknowledgement."""
    acknowledgement = trigger_publication_pb2.TriggerAcknowledgement(
        publication_id=publication.publication_id,
        disposition=disposition,
    )
    validation.validate_publication_acknowledgement(
        publication,
        acknowledgement,
    )
    return acknowledgement


def _rejection(
    publication: trigger_publication_pb2.TriggerPublication,
    reason: trigger_publication_pb2.TriggerRejectionReason.ValueType,
    *,
    retryable: bool,
    explanation: str,
) -> trigger_publication_pb2.TriggerAcknowledgement:
    """Construct and validate one sanitized publication rejection."""
    acknowledgement = trigger_publication_pb2.TriggerAcknowledgement(
        publication_id=publication.publication_id,
        disposition=(
            trigger_publication_pb2.TRIGGER_ACKNOWLEDGEMENT_DISPOSITION_REJECTED
        ),
        rejection=trigger_publication_pb2.TriggerRejection(
            reason=reason,
            retryable=retryable,
            explanation=explanation,
        ),
    )
    validation.validate_trigger_acknowledgement(acknowledgement)
    return acknowledgement
