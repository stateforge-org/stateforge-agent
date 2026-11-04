"""Coordinate Agent-side Operation Admission and uncertainty recovery."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import grpc
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import ParseDict, ParseError
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from stateforge_protocol.sfp.operation_admission.v1 import (
    operation_admission_pb2,
    validation,
)

from stateforge_agent._configuration_validation import (
    require_bounded_integer,
    require_finite_non_negative,
    require_finite_positive,
)
from stateforge_agent._journal.base import JournalError
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessment,
    CapabilityRequirementState,
    CapabilitySupportState,
)
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestOperationDeclaration,
)
from stateforge_agent._sfp._services.operation_admission.client import (
    OperationAdmissionClient,
    OperationAdmissionRequester,
    SfpOperationAdmissionDeadlineError,
    SfpOperationAdmissionError,
    SfpOperationAdmissionProtocolError,
    SfpOperationAdmissionRejectedError,
    SfpOperationAdmissionStaleError,
    SfpOperationAdmissionUnavailableError,
    validate_request,
)
from stateforge_agent._sfp._services.operation_admission.models import (
    OperationAdmissionRequest,
    OperationAdmissionResult,
    OperationRejection,
)
from stateforge_agent._sfp._services.operation_admission.store import (
    OperationAdmissionStore,
    OperationAttempt,
    OperationIntentDecision,
)

_MAX_ATTEMPTS = 5
_LOCK_STRIPES = 64
_UNRESOLVED_REASONS = {
    "ADMISSION_ERROR_REASON_OPERATION_RECORD_UNAVAILABLE",
    "ADMISSION_ERROR_REASON_OPERATION_RECORD_EXPIRED",
}


class OperationAdmissionLocalError(RuntimeError):
    """Report a sanitized Agent-owned admission decision."""


class OperationAdmissionPreconditionError(OperationAdmissionLocalError):
    """Report that local declaration or evidence preflight failed."""


class OperationAdmissionConflictError(OperationAdmissionLocalError):
    """Report changed immutable content under one operation identity."""


class OperationAdmissionCapacityError(OperationAdmissionLocalError):
    """Report that durable unresolved-attempt capacity is exhausted."""


class OperationAdmissionUnresolvedError(OperationAdmissionLocalError):
    """Report an identity whose remote admission outcome is not proven."""

    def __init__(self, operation_id: UUID, reason: str) -> None:
        super().__init__(
            f"Operation Admission outcome is unresolved: {operation_id}"
        )
        self.operation_id = operation_id
        self.reason = reason


@dataclass(frozen=True, slots=True)
class OperationAdmissionLease:
    """Expose only current session facts needed by Operation Admission."""

    provider_instance_id: UUID
    endpoint_generation: int
    session_id: str
    manifest_revision: str
    availability_generation: int
    operation_declarations: Mapping[str, ManifestOperationDeclaration]
    channel: grpc.aio.Channel = field(repr=False, compare=False)
    session_token: object = field(repr=False, compare=False)


OperationAdmissionCurrentnessCheck = Callable[
    [OperationAdmissionLease], Awaitable[None]
]


class OperationAdmissionCoordinator:
    """Own immutable intent, P4 transport, retry, and durable resolution."""

    def __init__(
        self,
        store: OperationAdmissionStore,
        *,
        timeout_seconds: float = 5.0,
        attempts: int = 2,
        retry_initial_delay_seconds: float = 0.25,
        client: OperationAdmissionRequester | None = None,
    ) -> None:
        """Configure bounded admission attempts over one durable store."""
        timeout_seconds = require_finite_positive(
            timeout_seconds,
            name="Operation Admission timeout",
        )
        self._attempts = require_bounded_integer(
            attempts,
            name="Operation Admission attempts",
            minimum=1,
            maximum=_MAX_ATTEMPTS,
        )
        self._retry_initial_delay_seconds = require_finite_non_negative(
            retry_initial_delay_seconds,
            name="Operation Admission retry delay",
        )
        self._store = store
        self._client = client or OperationAdmissionClient(
            timeout_seconds=timeout_seconds
        )
        self._locks = tuple(asyncio.Lock() for _ in range(_LOCK_STRIPES))
        self._closed = False

    async def admit(
        self,
        lease: OperationAdmissionLease,
        request: OperationAdmissionRequest,
        assessment: CapabilityAssessment,
        *,
        assert_current: OperationAdmissionCurrentnessCheck,
    ) -> OperationAdmissionResult:
        """Prepare and submit one newly authorized immutable attempt."""
        operation_id = await self.prepare(
            lease,
            request,
            assessment,
            assert_current=assert_current,
        )
        return await self.submit(
            lease,
            operation_id,
            assessment,
            assert_current=assert_current,
        )

    async def prepare(
        self,
        lease: OperationAdmissionLease,
        request: OperationAdmissionRequest,
        assessment: CapabilityAssessment,
        *,
        assert_current: OperationAdmissionCurrentnessCheck,
    ) -> UUID:
        """Validate and durably create one identity before transport."""
        if self._closed:
            raise OperationAdmissionPreconditionError(
                "Operation Admission coordinator is closed"
            )
        await assert_current(lease)
        invocation = _invocation(lease, request)
        _preflight(lease, invocation, request, assessment)
        validate_request(
            _request(
                lease,
                invocation,
                operation_admission_pb2.ADMISSION_MODE_ADMIT_OR_RESOLVE,
            )
        )
        fingerprint = validation.compute_request_fingerprint(invocation)
        declaration = lease.operation_declarations[
            request.operation_identifier
        ]
        result_schema_json = json.dumps(
            dict(declaration.result_schema),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        try:
            stored = await self._store.create_intent(
                lease.provider_instance_id,
                invocation,
                fingerprint,
                result_schema_json=result_schema_json,
            )
        except JournalError as error:
            raise OperationAdmissionCapacityError(
                "Operation Admission intent could not be persisted"
            ) from error
        if stored.decision is OperationIntentDecision.CONFLICT:
            raise OperationAdmissionConflictError(
                "Operation identity is bound to different immutable content"
            )
        if stored.decision is OperationIntentDecision.CAPACITY_EXHAUSTED:
            raise OperationAdmissionCapacityError(
                "Operation Admission unresolved-attempt capacity is exhausted"
            )
        if stored.attempt is None:
            raise RuntimeError("Operation Admission store omitted its attempt")
        await assert_current(lease)
        return stored.attempt.operation_id

    async def submit(
        self,
        lease: OperationAdmissionLease,
        operation_id: UUID,
        assessment: CapabilityAssessment,
        *,
        assert_current: OperationAdmissionCurrentnessCheck,
    ) -> OperationAdmissionResult:
        """Submit or resolve one already durable operation identity."""
        key = (lease.provider_instance_id, operation_id)
        lock = self._locks[hash(key) % len(self._locks)]
        async with lock:
            return await self._submit_locked(
                lease,
                operation_id,
                assessment,
                assert_current=assert_current,
            )

    async def attempt(
        self,
        provider_instance_id: UUID,
        operation_id: UUID,
    ) -> OperationAttempt | None:
        """Return durable state for recovery or operator inspection."""
        return await self._store.get(provider_instance_id, operation_id)

    def close(self) -> None:
        """Reject new preparations while preserving durable attempts."""
        self._closed = True

    async def _submit_locked(
        self,
        lease: OperationAdmissionLease,
        operation_id: UUID,
        assessment: CapabilityAssessment,
        *,
        assert_current: OperationAdmissionCurrentnessCheck,
    ) -> OperationAdmissionResult:
        attempt = await self._pending_attempt(lease, operation_id)
        if isinstance(attempt, OperationAdmissionResult):
            return attempt
        _preflight_existing(lease, attempt, assessment)
        mode = _mode(lease, attempt)
        for attempt_number in range(self._attempts):
            await assert_current(lease)
            await self._require_live_deadline(lease, attempt)
            await self._store.record_transport_attempt(
                lease.provider_instance_id,
                operation_id,
                endpoint_generation=lease.endpoint_generation,
                session_id=lease.session_id,
            )
            request = _request(lease, attempt.invocation, mode)
            try:
                result = await self._client.start(lease.channel, request)
            except SfpOperationAdmissionRejectedError as error:
                await self._handle_rejection(
                    lease,
                    attempt,
                    attempt_number,
                    error,
                )
                continue
            except (
                SfpOperationAdmissionUnavailableError,
                SfpOperationAdmissionDeadlineError,
            ) as error:
                await self._retry_transport_error(
                    attempt,
                    attempt_number,
                    error,
                )
                continue
            except (
                SfpOperationAdmissionProtocolError,
                SfpOperationAdmissionStaleError,
            ) as error:
                raise OperationAdmissionUnresolvedError(
                    operation_id,
                    type(error).__name__,
                ) from error
            except SfpOperationAdmissionError:
                raise
            await assert_current(lease)
            try:
                await self._store.record_accepted(result)
            except JournalError as error:
                raise OperationAdmissionUnresolvedError(
                    operation_id,
                    "accepted response could not be persisted",
                ) from error
            return result
        raise RuntimeError("Operation Admission retry loop exhausted")

    async def _pending_attempt(
        self,
        lease: OperationAdmissionLease,
        operation_id: UUID,
    ) -> OperationAttempt | OperationAdmissionResult:
        attempt = await self._store.get(
            lease.provider_instance_id, operation_id
        )
        if attempt is None:
            raise OperationAdmissionPreconditionError(
                "Operation Admission identity is not durable"
            )
        if attempt.result is not None:
            return attempt.result
        if attempt.rejection is not None and not attempt.rejection.retryable:
            raise SfpOperationAdmissionRejectedError(attempt.rejection)
        return attempt

    async def _require_live_deadline(
        self,
        lease: OperationAdmissionLease,
        attempt: OperationAttempt,
    ) -> None:
        if _deadline(attempt.invocation) > datetime.now(UTC):
            return
        rejection = OperationRejection(
            operation_id=attempt.operation_id,
            reason="ADMISSION_ERROR_REASON_OPERATION_DEADLINE_EXPIRED",
            retryable=False,
            retry_after=None,
            explanation="Operation execution deadline has elapsed.",
            grpc_status=grpc.StatusCode.DEADLINE_EXCEEDED.name,
        )
        await self._store.record_rejected(
            lease.provider_instance_id,
            attempt.operation_id,
            rejection,
        )
        raise SfpOperationAdmissionRejectedError(rejection)

    async def _handle_rejection(
        self,
        lease: OperationAdmissionLease,
        attempt: OperationAttempt,
        attempt_number: int,
        error: SfpOperationAdmissionRejectedError,
    ) -> None:
        rejection = error.rejection
        if rejection.reason in _UNRESOLVED_REASONS:
            raise OperationAdmissionUnresolvedError(
                attempt.operation_id,
                rejection.reason,
            ) from error
        await self._store.record_rejected(
            lease.provider_instance_id,
            attempt.operation_id,
            rejection,
        )
        if not rejection.retryable or attempt_number + 1 >= self._attempts:
            raise error
        if not await _retry_delay(
            rejection,
            fallback=self._retry_initial_delay_seconds * (2**attempt_number),
            deadline=_deadline(attempt.invocation),
        ):
            raise error

    async def _retry_transport_error(
        self,
        attempt: OperationAttempt,
        attempt_number: int,
        error: SfpOperationAdmissionError,
    ) -> None:
        if attempt_number + 1 >= self._attempts:
            raise OperationAdmissionUnresolvedError(
                attempt.operation_id,
                type(error).__name__,
            ) from error
        await asyncio.sleep(
            self._retry_initial_delay_seconds * (2**attempt_number)
        )


def _invocation(
    lease: OperationAdmissionLease,
    request: OperationAdmissionRequest,
) -> operation_admission_pb2.OperationInvocation:
    if request.provider_instance_id != lease.provider_instance_id:
        raise OperationAdmissionPreconditionError(
            "Operation Admission Provider identity is not current"
        )
    if request.execution_deadline.tzinfo is None:
        raise OperationAdmissionPreconditionError(
            "Operation execution deadline must be timezone-aware"
        )
    payload = _structure(request.payload, name="operation payload")
    execution_context = _structure(
        request.execution_context,
        name="operation execution context",
    )
    deadline = timestamp_pb2.Timestamp()
    deadline.FromDatetime(request.execution_deadline.astimezone(UTC))
    return operation_admission_pb2.OperationInvocation(
        operation_id=str(uuid4()),
        operation_identifier=request.operation_identifier,
        payload=payload,
        execution_deadline=deadline,
        contract=operation_admission_pb2.OperationContractContext(
            manifest_revision=lease.manifest_revision,
            required_implementation_roles=sorted(
                request.required_implementation_roles
            ),
        ),
        execution_context=execution_context,
    )


def _structure(
    value: Mapping[str, object],
    *,
    name: str,
) -> struct_pb2.Struct:
    message = struct_pb2.Struct()
    try:
        ParseDict(dict(value), message)
    except (ParseError, TypeError, ValueError) as error:
        raise OperationAdmissionPreconditionError(
            f"{name} is not valid JSON"
        ) from error
    return message


def _preflight(
    lease: OperationAdmissionLease,
    invocation: operation_admission_pb2.OperationInvocation,
    request: OperationAdmissionRequest,
    assessment: CapabilityAssessment,
) -> None:
    declaration = lease.operation_declarations.get(
        invocation.operation_identifier
    )
    if declaration is None:
        raise OperationAdmissionPreconditionError(
            "Operation is not declared by the accepted manifest"
        )
    try:
        Draft202012Validator(declaration.input_schema).validate(
            dict(request.payload)
        )
    except ValidationError as error:
        raise OperationAdmissionPreconditionError(
            "Operation payload does not match its declared schema"
        ) from error
    if not set(request.required_implementation_roles).issubset(
        declaration.implements
    ):
        raise OperationAdmissionPreconditionError(
            "Operation requires an undeclared implementation profile"
        )
    _validate_assessment(lease, invocation, assessment)


def _preflight_existing(
    lease: OperationAdmissionLease,
    attempt: OperationAttempt,
    assessment: CapabilityAssessment,
) -> None:
    invocation = attempt.invocation
    if invocation.contract.manifest_revision != lease.manifest_revision:
        raise OperationAdmissionPreconditionError(
            "Operation Admission manifest revision is no longer current"
        )
    declaration = lease.operation_declarations.get(
        invocation.operation_identifier
    )
    if declaration is None or not set(
        invocation.contract.required_implementation_roles
    ).issubset(declaration.implements):
        raise OperationAdmissionPreconditionError(
            "Operation Admission declaration is no longer compatible"
        )
    _validate_assessment(lease, invocation, assessment)


def _validate_assessment(
    lease: OperationAdmissionLease,
    invocation: operation_admission_pb2.OperationInvocation,
    assessment: CapabilityAssessment,
) -> None:
    scope = assessment.scope
    context_digest = hashlib.sha256(
        invocation.execution_context.SerializeToString(deterministic=True)
    ).digest()
    if (
        scope.provider_instance_id != lease.provider_instance_id
        or scope.endpoint_generation != lease.endpoint_generation
        or scope.session_id != lease.session_id
        or scope.manifest_revision != lease.manifest_revision
        or scope.availability_generation != lease.availability_generation
        or scope.operation_identifiers != (invocation.operation_identifier,)
        or scope.execution_context_sha256 != context_digest
    ):
        raise OperationAdmissionPreconditionError(
            "Capability Assessment evidence does not match admission scope"
        )
    if len(assessment.findings) != 1:
        raise OperationAdmissionPreconditionError(
            "Capability Assessment did not return exactly one finding"
        )
    finding = assessment.findings[0]
    if finding.operation_identifier != invocation.operation_identifier:
        raise OperationAdmissionPreconditionError(
            "Capability Assessment finding identity is inconsistent"
        )
    if finding.support_state not in {
        CapabilitySupportState.SUPPORTED,
        CapabilitySupportState.SUPPORTED_WITH_REQUIREMENTS,
    } or any(
        requirement.state is not CapabilityRequirementState.SATISFIED
        for requirement in finding.requirements
    ):
        raise OperationAdmissionPreconditionError(
            "Capability Assessment does not support this operation"
        )


def _request(
    lease: OperationAdmissionLease,
    invocation: operation_admission_pb2.OperationInvocation,
    mode: operation_admission_pb2.AdmissionMode.ValueType,
) -> operation_admission_pb2.StartOperationRequest:
    return operation_admission_pb2.StartOperationRequest(
        session_binding=operation_admission_pb2.OperationSessionBinding(
            expected_provider_instance_id=str(lease.provider_instance_id),
            expected_endpoint_generation=lease.endpoint_generation,
            expected_session_id=lease.session_id,
        ),
        invocation=invocation,
        admission_mode=mode,
    )


def _mode(
    lease: OperationAdmissionLease,
    attempt: OperationAttempt,
) -> operation_admission_pb2.AdmissionMode.ValueType:
    if (
        attempt.last_endpoint_generation is not None
        and attempt.last_endpoint_generation != lease.endpoint_generation
    ):
        return operation_admission_pb2.ADMISSION_MODE_RESOLVE_ONLY
    return operation_admission_pb2.ADMISSION_MODE_ADMIT_OR_RESOLVE


def _deadline(
    invocation: operation_admission_pb2.OperationInvocation,
) -> datetime:
    return invocation.execution_deadline.ToDatetime(tzinfo=UTC)


async def _retry_delay(
    rejection: OperationRejection,
    *,
    fallback: float,
    deadline: datetime,
) -> bool:
    """Wait for the prescribed delay when another timely attempt can fit."""
    delay = (
        rejection.retry_after.total_seconds()
        if rejection.retry_after is not None
        else fallback
    )
    if datetime.now(UTC).timestamp() + delay >= deadline.timestamp():
        return False
    await asyncio.sleep(delay)
    return True
