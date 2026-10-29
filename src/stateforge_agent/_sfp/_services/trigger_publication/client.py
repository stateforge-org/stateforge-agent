"""Consume one fenced SFP Trigger Publication stream."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Never, Protocol

import grpc
from google.protobuf.message import DecodeError
from stateforge_protocol.sfp.trigger_publication.v1 import (
    contract,
    trigger_publication_pb2,
    validation,
)


class SfpTriggerPublicationError(RuntimeError):
    """Report a sanitized Trigger Publication stream failure."""


class SfpTriggerPublicationUnavailableError(SfpTriggerPublicationError):
    """Report a stream that may be reopened while its lease remains current."""


class SfpTriggerPublicationStaleError(SfpTriggerPublicationError):
    """Report a stream rejected because its session fences are stale."""


class SfpTriggerPublicationProtocolError(SfpTriggerPublicationError):
    """Report invalid framing or Provider response data."""


class SfpTriggerPublicationLimitError(SfpTriggerPublicationError):
    """Report a Provider response exceeding the canonical byte limit."""


@dataclass(frozen=True, slots=True)
class TriggerStreamBinding:
    """Carry the exact Agent-owned fences used to open one stream."""

    provider_instance_id: str
    endpoint_generation: int
    session_id: str
    manifest_revision: str
    maximum_in_flight_publications: int


TriggerPublicationHandler = Callable[
    [trigger_publication_pb2.TriggerPublication],
    Awaitable[trigger_publication_pb2.TriggerAcknowledgement],
]


class TriggerPublicationConsumer(Protocol):
    """Describe one finite ownership boundary for a stream attempt."""

    async def consume(
        self,
        channel: grpc.aio.Channel,
        binding: TriggerStreamBinding,
        handler: TriggerPublicationHandler,
    ) -> None:
        """Consume until the stream closes, fails, or is cancelled."""
        ...


class _TriggerStreamCall(Protocol):
    """Type the dynamic asynchronous bidirectional call used at runtime."""

    async def write(
        self,
        request: trigger_publication_pb2.StreamTriggersRequest,
    ) -> None:
        """Write one request message."""
        ...

    async def read(self) -> object:
        """Read one decoded response or end-of-stream marker."""
        ...

    def cancel(self) -> bool:
        """Cancel the active call."""
        ...


@dataclass(frozen=True, slots=True)
class _DecodedTriggerResponse:
    """Carry a decoded response or a safe wire rejection."""

    response: trigger_publication_pb2.StreamTriggersResponse | None = None
    malformed: bool = False
    limit_exceeded: bool = False


_SERVICE_DESCRIPTOR = trigger_publication_pb2.DESCRIPTOR.services_by_name[
    "TriggerPublicationService"
]
_STREAM_DESCRIPTOR = _SERVICE_DESCRIPTOR.methods_by_name["StreamTriggers"]
_STREAM_METHOD_PATH = (
    f"/{_SERVICE_DESCRIPTOR.full_name}/{_STREAM_DESCRIPTOR.name}"
)


class TriggerPublicationClient:
    """Open one Trigger Publication stream and acknowledge serially."""

    async def consume(
        self,
        channel: grpc.aio.Channel,
        binding: TriggerStreamBinding,
        handler: TriggerPublicationHandler,
    ) -> None:
        """Pass validated publications to the durable handler."""
        open_message = trigger_publication_pb2.TriggerStreamOpen(
            session_binding=trigger_publication_pb2.TriggerSessionBinding(
                expected_provider_instance_id=binding.provider_instance_id,
                expected_endpoint_generation=binding.endpoint_generation,
                expected_session_id=binding.session_id,
                expected_manifest_revision=binding.manifest_revision,
            ),
            requested_max_in_flight_publications=(
                binding.maximum_in_flight_publications
            ),
        )
        opening_request = trigger_publication_pb2.StreamTriggersRequest(
            open=open_message
        )
        _validate_request(opening_request)

        callable_ = channel.stream_stream(
            _STREAM_METHOD_PATH,
            request_serializer=(
                trigger_publication_pb2.StreamTriggersRequest.SerializeToString
            ),
            response_deserializer=_decode_response,
        )
        call: _TriggerStreamCall = callable_()
        try:
            await call.write(opening_request)
            first = await _read_response(call)
            if first.WhichOneof("message") != "ready":
                raise SfpTriggerPublicationProtocolError(
                    "Trigger Publication stream did not begin with ready"
                )
            try:
                validation.validate_stream_open_exchange(
                    open_message,
                    first.ready,
                )
            except validation.TriggerPublicationValidationError as error:
                raise SfpTriggerPublicationProtocolError(
                    "Trigger Publication ready response failed validation"
                ) from error
            await _consume_publications(
                call,
                maximum_in_flight=first.ready.maximum_in_flight_publications,
                handler=handler,
            )
        except grpc.aio.AioRpcError as error:
            _raise_rpc_error(error.code())
        finally:
            call.cancel()


async def _consume_publications(
    call: _TriggerStreamCall,
    *,
    maximum_in_flight: int,
    handler: TriggerPublicationHandler,
) -> None:
    """Bound concurrent durable work by the Provider-selected wire window."""
    reader = asyncio.create_task(_read_response(call))
    handlers: set[
        asyncio.Future[trigger_publication_pb2.TriggerAcknowledgement]
    ] = set()
    try:
        while True:
            waiters: set[asyncio.Future[Any]] = {reader}
            waiters.update(handlers)
            done, _ = await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if reader in done:
                response = reader.result()
                if response.WhichOneof("message") != "publication":
                    raise SfpTriggerPublicationProtocolError(
                        "Trigger Publication stream returned invalid framing"
                    )
                if len(handlers) >= maximum_in_flight:
                    raise SfpTriggerPublicationLimitError(
                        "Trigger Publication Provider exceeded the selected "
                        "receive window"
                    )
                handlers.add(
                    asyncio.ensure_future(handler(response.publication))
                )
                reader = asyncio.create_task(_read_response(call))
                continue

            for task in tuple(handlers):
                if task not in done:
                    continue
                handlers.remove(task)
                acknowledgement = task.result()
                request = trigger_publication_pb2.StreamTriggersRequest(
                    acknowledgement=acknowledgement
                )
                _validate_request(request)
                await call.write(request)
    finally:
        reader.cancel()
        for task in handlers:
            task.cancel()
        await asyncio.gather(reader, *handlers, return_exceptions=True)


def _validate_request(
    request: trigger_publication_pb2.StreamTriggersRequest,
) -> None:
    """Apply canonical request limits and validation before transmission."""
    if request.ByteSize() > contract.MAX_REQUEST_BYTES:
        raise SfpTriggerPublicationLimitError(
            "Trigger Publication request limit exceeded"
        )
    try:
        validation.validate_stream_request(request)
    except validation.TriggerPublicationValidationError as error:
        raise SfpTriggerPublicationProtocolError(
            "Trigger Publication request failed canonical validation"
        ) from error


async def _read_response(
    call: _TriggerStreamCall,
) -> trigger_publication_pb2.StreamTriggersResponse:
    """Read and validate one bounded Provider response."""
    decoded = await call.read()
    if decoded is grpc.aio.EOF:
        raise SfpTriggerPublicationUnavailableError(
            "Trigger Publication stream ended"
        )
    if not isinstance(decoded, _DecodedTriggerResponse):
        raise SfpTriggerPublicationProtocolError(
            "Trigger Publication transport returned invalid data"
        )
    if decoded.limit_exceeded:
        raise SfpTriggerPublicationLimitError(
            "Trigger Publication response limit exceeded"
        )
    if decoded.malformed or decoded.response is None:
        raise SfpTriggerPublicationProtocolError(
            "Trigger Publication response protobuf is malformed"
        )
    response = decoded.response
    try:
        validation.validate_stream_response(response)
    except validation.TriggerPublicationValidationError as error:
        raise SfpTriggerPublicationProtocolError(
            "Trigger Publication response failed canonical validation"
        ) from error
    return response


def _decode_response(payload: bytes) -> _DecodedTriggerResponse:
    """Reject oversized or malformed bytes before semantic handling."""
    if len(payload) > contract.MAX_RESPONSE_BYTES:
        return _DecodedTriggerResponse(limit_exceeded=True)
    try:
        response = trigger_publication_pb2.StreamTriggersResponse.FromString(
            payload
        )
    except DecodeError:
        return _DecodedTriggerResponse(malformed=True)
    return _DecodedTriggerResponse(response=response)


def _raise_rpc_error(code: grpc.StatusCode) -> Never:
    """Translate stable transport statuses into sanitized Agent errors."""
    if code in {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }:
        raise SfpTriggerPublicationUnavailableError(
            "Trigger Publication stream is unavailable"
        )
    if code is grpc.StatusCode.FAILED_PRECONDITION:
        raise SfpTriggerPublicationStaleError(
            "Trigger Publication stream binding is stale"
        )
    if code is grpc.StatusCode.RESOURCE_EXHAUSTED:
        raise SfpTriggerPublicationLimitError(
            "Trigger Publication stream limit exceeded"
        )
    raise SfpTriggerPublicationProtocolError(
        f"Trigger Publication stream failed: {code.name}"
    )
