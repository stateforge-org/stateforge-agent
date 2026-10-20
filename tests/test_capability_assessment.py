"""Capability Assessment normalization, fencing, and failure evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import pytest
from google.protobuf import struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from stateforge_protocol.sfp.capability_assessment.v1 import (
    capability_assessment_pb2,
    contract,
)

from stateforge_agent._sfp import (
    CapabilityAssessment,
    CapabilityRequirementState,
    CapabilitySupportState,
    ProviderAvailabilityState,
    ProviderAvailabilityStatus,
    ProviderSession,
    ProviderSessionManager,
    SfpCapabilityAssessmentDeadlineError,
    SfpCapabilityAssessmentLimitError,
    SfpCapabilityAssessmentMalformedResponseError,
    SfpCapabilityAssessmentProviderError,
    SfpCapabilityAssessmentRequestError,
    SfpCapabilityAssessmentStaleError,
    SfpCapabilityAssessmentTransportError,
    SfpCapabilityAssessmentUnavailableError,
    SfpVersion,
)
from stateforge_agent._sfp._runtime import sessions as sessions_module
from stateforge_agent._sfp._runtime.channel import create_provider_channel
from stateforge_agent._sfp._services.capability_assessment.cache import (
    CapabilityAssessmentCache,
)
from stateforge_agent._sfp._services.capability_assessment.client import (
    _ASSESS_METHOD_PATH,
    CapabilityAssessmentClient,
    _decode_response,
)
from stateforge_agent._sfp._services.capability_assessment.models import (
    CapabilityAssessmentScope,
)
from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestCacheKey,
    ManifestDeclaration,
)

_PROVIDER_ID = UUID("7f7db7d8-68b4-4f71-9769-065b5c4f941c")
_REQUEST_ID = UUID("6c1ae0b8-8d70-46dc-86c0-994247e1a39b")
_ASSESSMENT_ID = UUID("1d40d0d5-ff2a-4780-bc3d-c4a7c55fc3a2")
_MANIFEST_REVISION = "sha256:" + ("0" * 64)
_GENERIC_OPERATION = "stateforge.test/configure@1"
_PROFILE_OPERATION = "stateforge.test/snapshot@1"
_UNSUPPORTED_OPERATION = "stateforge.test/unsupported@1"
_INDETERMINATE_OPERATION = "stateforge.test/inspect@1"
_EXPECTED_RETRY_CALLS = 2
_SENSITIVE_DETAILS = "credential-that-must-not-cross-the-boundary"
_OPERATIONS = (
    _GENERIC_OPERATION,
    _PROFILE_OPERATION,
    _UNSUPPORTED_OPERATION,
    _INDETERMINATE_OPERATION,
)


def _request(
    operation_identifiers: tuple[str, ...] = _OPERATIONS,
) -> capability_assessment_pb2.AssessCapabilitiesRequest:
    return capability_assessment_pb2.AssessCapabilitiesRequest(
        assessment_request_id=str(_REQUEST_ID),
        expected_provider_instance_id=str(_PROVIDER_ID),
        expected_endpoint_generation=4,
        expected_session_id="session-a",
        expected_manifest_revision=_MANIFEST_REVISION,
        execution_context=struct_pb2.Struct(),
        operation_identifiers=operation_identifiers,
    )


def _response(
    request: capability_assessment_pb2.AssessCapabilitiesRequest,
) -> capability_assessment_pb2.AssessCapabilitiesResponse:
    timestamp = Timestamp(seconds=1)
    findings = {
        _GENERIC_OPERATION: capability_assessment_pb2.CapabilityFinding(
            operation_identifier=_GENERIC_OPERATION,
            support_state=capability_assessment_pb2.SUPPORT_STATE_SUPPORTED,
            requirement_findings=[
                capability_assessment_pb2.RequirementFinding(
                    requirement_id="runtime.generic.ready",
                    state=(
                        capability_assessment_pb2.REQUIREMENT_STATE_SATISFIED
                    ),
                    description="Generic runtime is ready",
                )
            ],
        ),
        _PROFILE_OPERATION: capability_assessment_pb2.CapabilityFinding(
            operation_identifier=_PROFILE_OPERATION,
            support_state=(
                capability_assessment_pb2.SUPPORT_STATE_SUPPORTED_WITH_REQUIREMENTS
            ),
            requirement_findings=[
                capability_assessment_pb2.RequirementFinding(
                    requirement_id="runtime.profile.pending",
                    state=(
                        capability_assessment_pb2.REQUIREMENT_STATE_UNSATISFIED
                    ),
                    description="Profile prerequisite is pending",
                    version_constraint=">=1",
                )
            ],
            reasons=[
                capability_assessment_pb2.ReasonDetail(
                    reason_code=contract.REASON_CODE_REQUIREMENT_UNMET,
                    explanation="A declared prerequisite is pending",
                )
            ],
            constraints=[
                capability_assessment_pb2.ConstraintDetail(
                    constraint_id="stateforge.test.read_only",
                    value="required",
                    explanation="Inspection remains read-only",
                )
            ],
        ),
        _UNSUPPORTED_OPERATION: capability_assessment_pb2.CapabilityFinding(
            operation_identifier=_UNSUPPORTED_OPERATION,
            support_state=(
                capability_assessment_pb2.SUPPORT_STATE_UNSUPPORTED
            ),
            reasons=[
                capability_assessment_pb2.ReasonDetail(
                    reason_code=contract.REASON_CODE_REQUIREMENT_UNMET,
                    explanation="Runtime is incompatible",
                )
            ],
        ),
        _INDETERMINATE_OPERATION: (
            capability_assessment_pb2.CapabilityFinding(
                operation_identifier=_INDETERMINATE_OPERATION,
                support_state=(
                    capability_assessment_pb2.SUPPORT_STATE_INDETERMINATE
                ),
                requirement_findings=[
                    capability_assessment_pb2.RequirementFinding(
                        requirement_id="runtime.inspectable",
                        state=(
                            capability_assessment_pb2.REQUIREMENT_STATE_UNKNOWN
                        ),
                        description="Inspection evidence is unavailable",
                    )
                ],
                reasons=[
                    capability_assessment_pb2.ReasonDetail(
                        reason_code=(
                            contract.REASON_CODE_INSUFFICIENT_EVIDENCE
                        ),
                        explanation="Inspection could not decide",
                    )
                ],
            )
        ),
    }
    return capability_assessment_pb2.AssessCapabilitiesResponse(
        assessment_request_id=request.assessment_request_id,
        provider_instance_id=request.expected_provider_instance_id,
        endpoint_generation=request.expected_endpoint_generation,
        session_id=request.expected_session_id,
        manifest_revision=request.expected_manifest_revision,
        assessment_id=str(_ASSESSMENT_ID),
        assessment_timestamp=timestamp,
        findings=[
            findings[identifier]
            for identifier in request.operation_identifiers
        ],
    )


def _scope() -> CapabilityAssessmentScope:
    return CapabilityAssessmentScope(
        provider_instance_id=_PROVIDER_ID,
        endpoint_generation=4,
        session_id="session-a",
        manifest_revision=_MANIFEST_REVISION,
        availability_generation=2,
        operation_identifiers=_OPERATIONS,
        execution_context_sha256=hashlib.sha256(b"").digest(),
    )


async def _start_server(
    handler: Callable[
        [
            capability_assessment_pb2.AssessCapabilitiesRequest,
            grpc.aio.ServicerContext,
        ],
        Awaitable[
            capability_assessment_pb2.AssessCapabilitiesResponse | bytes
        ],
    ],
) -> tuple[grpc.aio.Server, grpc.aio.Channel]:
    method = grpc.unary_unary_rpc_method_handler(
        handler,
        request_deserializer=(
            capability_assessment_pb2.AssessCapabilitiesRequest.FromString
        ),
        response_serializer=lambda value: (
            value if isinstance(value, bytes) else value.SerializeToString()
        ),
    )
    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "stateforge.sfp.capability_assessment.v1."
                "CapabilityAssessmentService",
                {"AssessCapabilities": method},
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = create_provider_channel(f"127.0.0.1:{port}")
    await channel.channel_ready()
    return server, channel


@pytest.mark.asyncio
async def test_client_normalizes_every_support_and_requirement_state() -> None:
    async def handler(request, context):
        del context
        return _response(request)

    server, channel = await _start_server(handler)
    try:
        assessment = await CapabilityAssessmentClient().assess(
            channel,
            _request(),
            scope=_scope(),
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert assessment.request_id == _REQUEST_ID
    assert assessment.assessment_id == _ASSESSMENT_ID
    assert assessment.assessed_at == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
    assert tuple(item.support_state for item in assessment.findings) == (
        CapabilitySupportState.SUPPORTED,
        CapabilitySupportState.SUPPORTED_WITH_REQUIREMENTS,
        CapabilitySupportState.UNSUPPORTED,
        CapabilitySupportState.INDETERMINATE,
    )
    assert assessment.findings[0].requirements[0].state is (
        CapabilityRequirementState.SATISFIED
    )
    assert assessment.findings[1].requirements[0].state is (
        CapabilityRequirementState.UNSATISFIED
    )
    assert assessment.findings[3].requirements[0].state is (
        CapabilityRequirementState.UNKNOWN
    )
    assert assessment.findings[1].constraints[0].value == "required"
    assert assessment.findings[2].reasons[0].code == (
        contract.REASON_CODE_REQUIREMENT_UNMET
    )


@pytest.mark.asyncio
async def test_client_rejects_malformed_and_mismatched_replies() -> None:
    async def malformed(request, context):
        del request, context
        return b"\x80"

    server, channel = await _start_server(malformed)
    try:
        with pytest.raises(
            SfpCapabilityAssessmentMalformedResponseError,
            match="protobuf is malformed",
        ):
            await CapabilityAssessmentClient().assess(
                channel, _request(), scope=_scope()
            )
    finally:
        await channel.close()
        await server.stop(grace=None)

    async def mismatched(request, context):
        del context
        response = _response(request)
        response.session_id = "different-session"
        return response

    server, channel = await _start_server(mismatched)
    try:
        with pytest.raises(
            SfpCapabilityAssessmentMalformedResponseError,
            match="canonical validation",
        ):
            await CapabilityAssessmentClient().assess(
                channel, _request(), scope=_scope()
            )
    finally:
        await channel.close()
        await server.stop(grace=None)


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (grpc.StatusCode.UNAVAILABLE, SfpCapabilityAssessmentUnavailableError),
        (
            grpc.StatusCode.FAILED_PRECONDITION,
            SfpCapabilityAssessmentStaleError,
        ),
        (
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            SfpCapabilityAssessmentLimitError,
        ),
        (
            grpc.StatusCode.INVALID_ARGUMENT,
            SfpCapabilityAssessmentRequestError,
        ),
        (grpc.StatusCode.INTERNAL, SfpCapabilityAssessmentProviderError),
        (grpc.StatusCode.UNKNOWN, SfpCapabilityAssessmentTransportError),
    ],
)
@pytest.mark.asyncio
async def test_client_maps_rpc_failures_without_exposing_details(
    status: grpc.StatusCode,
    error_type: type[Exception],
) -> None:
    async def handler(request, context):
        del request
        await context.abort(status, _SENSITIVE_DETAILS)

    server, channel = await _start_server(handler)
    try:
        with pytest.raises(error_type) as error:
            await CapabilityAssessmentClient().assess(
                channel, _request(), scope=_scope()
            )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert _SENSITIVE_DETAILS not in str(error.value)
    assert error.value.__context__ is None
    assert _SENSITIVE_DETAILS not in "".join(
        traceback.format_exception(error.value)
    )


@pytest.mark.asyncio
async def test_client_enforces_a_finite_deadline() -> None:
    async def handler(request, context):
        del request, context
        await asyncio.sleep(1)
        raise AssertionError("deadline should cancel the handler")

    server, channel = await _start_server(handler)
    try:
        with pytest.raises(SfpCapabilityAssessmentDeadlineError):
            await CapabilityAssessmentClient(timeout_seconds=0.01).assess(
                channel, _request(), scope=_scope()
            )
    finally:
        await channel.close()
        await server.stop(grace=None)


def test_response_decoder_enforces_the_protocol_byte_limit() -> None:
    decoded = _decode_response(bytes(contract.MAX_RESPONSE_BYTES + 1))

    assert decoded.limit_exceeded
    assert decoded.response is None


@pytest.mark.asyncio
async def test_client_rejects_request_byte_limit_before_semantic_work() -> (
    None
):
    request = _request()
    request.execution_context["oversized"] = "x" * contract.MAX_REQUEST_BYTES
    channel = grpc.aio.insecure_channel("127.0.0.1:1")
    try:
        with pytest.raises(
            SfpCapabilityAssessmentLimitError,
            match="request limit exceeded",
        ):
            await CapabilityAssessmentClient().assess(
                channel,
                request,
                scope=_scope(),
            )
    finally:
        await channel.close()


class _ScriptedRequester:
    """Return normalized evidence while exposing Manager retry behavior."""

    def __init__(
        self,
        *,
        unavailable_attempts: int = 0,
        on_unavailable: Callable[[], None] | None = None,
    ) -> None:
        self.unavailable_attempts = unavailable_attempts
        self.on_unavailable = on_unavailable
        self.requests: list[
            capability_assessment_pb2.AssessCapabilitiesRequest
        ] = []

    async def assess(self, channel, request, *, scope):
        del channel
        self.requests.append(request)
        if len(self.requests) <= self.unavailable_attempts:
            if self.on_unavailable is not None:
                self.on_unavailable()
            raise SfpCapabilityAssessmentUnavailableError("temporary")
        return CapabilityAssessment(
            scope=scope,
            request_id=UUID(request.assessment_request_id),
            assessment_id=uuid4(),
            assessed_at=datetime.now(UTC),
            findings=(),
            wire_size_bytes=1,
        )


def test_cache_enforces_aggregate_evidence_byte_limit() -> None:
    cache = CapabilityAssessmentCache(capacity=128, max_bytes=2)
    first = CapabilityAssessment(
        scope=_scope(),
        request_id=_REQUEST_ID,
        assessment_id=_ASSESSMENT_ID,
        assessed_at=datetime.now(UTC),
        findings=(),
        wire_size_bytes=2,
    )
    second = replace(
        first,
        scope=replace(first.scope, availability_generation=3),
        assessment_id=uuid4(),
    )

    cache.put(first)
    cache.put(second)

    assert cache.get(first.scope) is None
    assert cache.get(second.scope) is second


def test_cache_does_not_retain_an_oversized_entry() -> None:
    cache = CapabilityAssessmentCache(capacity=128, max_bytes=1)
    assessment = CapabilityAssessment(
        scope=_scope(),
        request_id=_REQUEST_ID,
        assessment_id=_ASSESSMENT_ID,
        assessed_at=datetime.now(UTC),
        findings=(),
        wire_size_bytes=2,
    )

    cache.put(assessment)

    assert cache.get(assessment.scope) is None


def _manifest(
    *,
    revision: str = _MANIFEST_REVISION,
) -> ManifestDeclaration:
    document = {
        "operations": {
            _GENERIC_OPERATION: {"implements": []},
            _PROFILE_OPERATION: {
                "implements": ["stateforge.profile.snapshot@1"]
            },
        }
    }
    return ManifestDeclaration(
        key=ManifestCacheKey(
            provider_instance_id=_PROVIDER_ID,
            sfp_version=SfpVersion(major=1, minor=0),
            schema_version=1,
            revision=revision,
        ),
        document=json.dumps(document).encode(),
    )


def _session(
    channel: grpc.aio.Channel,
    *,
    generation: int = 4,
    session_id: str = "session-a",
    manifest: ManifestDeclaration | None = None,
) -> ProviderSession:
    return ProviderSession(
        provider_instance_id=_PROVIDER_ID,
        endpoint_generation=generation,
        socket_path=Path("/run/stateforge-agent/providers/provider.sock"),
        selected_sfp_version=SfpVersion(major=1, minor=0),
        provider_runtime_id=uuid4(),
        session_id=session_id,
        manifest=manifest or _manifest(),
        _channel=channel,
    )


def _manager_with_current_session(
    requester: _ScriptedRequester,
) -> tuple[
    ProviderSessionManager,
    sessions_module._ProviderRuntimeState,
]:
    manager = ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version="0.1.0",
        supported_sfp_versions=(SfpVersion(major=1, minor=0),),
        capability_assessment_attempts=3,
        capability_assessment_retry_initial_delay_seconds=0,
        capability_assessment_client=requester,
    )
    session = _session(grpc.aio.insecure_channel("127.0.0.1:1"))
    status = ProviderAvailabilityStatus(
        provider_instance_id=_PROVIDER_ID,
        state=ProviderAvailabilityState.AVAILABLE,
        endpoint_generation=session.endpoint_generation,
        session_id=session.session_id,
        manifest_revision=session.manifest.key.revision,
        reason="test Provider is serving",
    )
    state = sessions_module._ProviderRuntimeState(
        active_session=session,
        availability_status=status,
        availability_generation=1,
    )
    manager._providers[_PROVIDER_ID] = state
    return manager, state


@pytest.mark.asyncio
async def test_manager_retries_same_request_after_revalidation() -> None:
    requester = _ScriptedRequester(unavailable_attempts=1)
    manager, _ = _manager_with_current_session(requester)
    try:
        assessment = await manager.assess_capabilities(
            _PROVIDER_ID,
            (_GENERIC_OPERATION,),
            execution_context={"region": "test"},
        )
    finally:
        await manager.close()

    assert assessment.scope.operation_identifiers == (_GENERIC_OPERATION,)
    assert len(requester.requests) == _EXPECTED_RETRY_CALLS
    assert requester.requests[0].assessment_request_id == (
        requester.requests[1].assessment_request_id
    )


@pytest.mark.asyncio
async def test_manager_rejects_undeclared_and_invalid_operations_locally() -> (
    None
):
    requester = _ScriptedRequester()
    manager, _ = _manager_with_current_session(requester)
    try:
        with pytest.raises(
            SfpCapabilityAssessmentRequestError,
            match="not declared",
        ):
            await manager.assess_capabilities(
                _PROVIDER_ID,
                ("stateforge.test/missing@1",),
            )
        with pytest.raises(
            SfpCapabilityAssessmentRequestError,
            match="canonical validation",
        ):
            await manager.assess_capabilities(_PROVIDER_ID, ("invalid",))
    finally:
        await manager.close()

    assert requester.requests == []


@pytest.mark.asyncio
async def test_manager_requires_current_provider_availability() -> None:
    requester = _ScriptedRequester()
    manager, state = _manager_with_current_session(requester)
    assert state.availability_status is not None
    state.availability_status = ProviderAvailabilityStatus(
        provider_instance_id=_PROVIDER_ID,
        state=ProviderAvailabilityState.NOT_SERVING,
        endpoint_generation=4,
        session_id="session-a",
        manifest_revision=_MANIFEST_REVISION,
        reason="test Provider is not serving",
    )
    try:
        with pytest.raises(SfpCapabilityAssessmentUnavailableError):
            await manager.assess_capabilities(
                _PROVIDER_ID, (_GENERIC_OPERATION,)
            )
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_retry_stops_when_the_active_session_changes() -> None:
    state_holder: list[sessions_module._ProviderRuntimeState] = []

    def replace_session() -> None:
        state = state_holder[0]
        assert state.active_session is not None
        old_channel = state.active_session._channel
        state.active_session = _session(
            old_channel,
            generation=5,
            session_id="session-b",
        )

    requester = _ScriptedRequester(
        unavailable_attempts=1,
        on_unavailable=replace_session,
    )
    manager, state = _manager_with_current_session(requester)
    state_holder.append(state)
    try:
        with pytest.raises(SfpCapabilityAssessmentStaleError):
            await manager.assess_capabilities(
                _PROVIDER_ID, (_GENERIC_OPERATION,)
            )
    finally:
        await manager.close()

    assert len(requester.requests) == 1


@pytest.mark.asyncio
async def test_cached_evidence_is_scoped_and_state_invalidated() -> None:
    requester = _ScriptedRequester()
    manager, state = _manager_with_current_session(requester)
    try:
        assessment = await manager.assess_capabilities(
            _PROVIDER_ID,
            (_GENERIC_OPERATION,),
            execution_context={"region": "a"},
        )
        assert (
            await manager.cached_capability_assessment(
                _PROVIDER_ID,
                (_GENERIC_OPERATION,),
                execution_context={"region": "a"},
            )
            is assessment
        )
        assert (
            await manager.cached_capability_assessment(
                _PROVIDER_ID,
                (_GENERIC_OPERATION,),
                execution_context={"region": "b"},
            )
            is None
        )

        assert state.availability_status is not None
        manager._record_availability(
            state,
            ProviderAvailabilityStatus(
                provider_instance_id=_PROVIDER_ID,
                state=ProviderAvailabilityState.NOT_SERVING,
                endpoint_generation=4,
                session_id="session-a",
                manifest_revision=_MANIFEST_REVISION,
                reason="readiness changed",
            ),
        )
        manager._record_availability(
            state,
            ProviderAvailabilityStatus(
                provider_instance_id=_PROVIDER_ID,
                state=ProviderAvailabilityState.AVAILABLE,
                endpoint_generation=4,
                session_id="session-a",
                manifest_revision=_MANIFEST_REVISION,
                reason="readiness recovered",
            ),
        )
        assert (
            await manager.cached_capability_assessment(
                _PROVIDER_ID,
                (_GENERIC_OPERATION,),
                execution_context={"region": "a"},
            )
            is None
        )
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_cache_does_not_cross_generation_or_manifest() -> None:
    requester = _ScriptedRequester()
    manager, state = _manager_with_current_session(requester)
    try:
        await manager.assess_capabilities(_PROVIDER_ID, (_GENERIC_OPERATION,))
        assert state.active_session is not None
        channel = state.active_session._channel
        state.active_session = _session(
            channel,
            generation=5,
            session_id="session-b",
            manifest=_manifest(revision="sha256:" + ("1" * 64)),
        )
        state.availability_generation += 1
        state.availability_status = ProviderAvailabilityStatus(
            provider_instance_id=_PROVIDER_ID,
            state=ProviderAvailabilityState.AVAILABLE,
            endpoint_generation=5,
            session_id="session-b",
            manifest_revision="sha256:" + ("1" * 64),
            reason="replacement is serving",
        )
        assert (
            await manager.cached_capability_assessment(
                _PROVIDER_ID, (_GENERIC_OPERATION,)
            )
            is None
        )
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_assessment_never_reuses_cache_as_authorization() -> None:
    requester = _ScriptedRequester()
    manager, _ = _manager_with_current_session(requester)
    try:
        first = await manager.assess_capabilities(
            _PROVIDER_ID, (_GENERIC_OPERATION,)
        )
        second = await manager.assess_capabilities(
            _PROVIDER_ID, (_GENERIC_OPERATION,)
        )
    finally:
        await manager.close()

    assert first.assessment_id != second.assessment_id
    assert len(requester.requests) == _EXPECTED_RETRY_CALLS


@pytest.mark.asyncio
async def test_context_limits_are_rejected_before_provider_code_runs() -> None:
    requester = _ScriptedRequester()
    manager, _ = _manager_with_current_session(requester)
    oversized_context = {f"entry-{index}": index for index in range(33)}
    try:
        with pytest.raises(
            SfpCapabilityAssessmentRequestError,
            match="canonical validation",
        ):
            await manager.assess_capabilities(
                _PROVIDER_ID,
                (_GENERIC_OPERATION,),
                execution_context=oversized_context,
            )
    finally:
        await manager.close()

    assert requester.requests == []


def test_agent_capability_boundary_has_no_provider_or_vendor_imports() -> None:
    root = (
        Path(__file__).parents[1]
        / "src/stateforge_agent/_sfp/_services/capability_assessment"
    )
    source = "\n".join(path.read_text() for path in root.glob("*.py"))

    assert "stateforge_provider" not in source
    assert "boto" not in source
    assert "github" not in source.lower()
    assert "stateforge_protocol.sfp._validation" not in source
    assert _ASSESS_METHOD_PATH == (
        "/stateforge.sfp.capability_assessment.v1."
        "CapabilityAssessmentService/AssessCapabilities"
    )
