"""Create Provider channels with canonical SFP transport ceilings."""

from typing import Final

import grpc
from stateforge_protocol.sfp import shared_endpoint

PROVIDER_CHANNEL_OPTIONS: Final[tuple[tuple[str, int], ...]] = (
    (
        "grpc.max_send_message_length",
        shared_endpoint.MAX_PROVIDER_RECEIVE_BYTES,
    ),
    (
        "grpc.max_receive_message_length",
        shared_endpoint.MAX_PROVIDER_SEND_BYTES,
    ),
)


def create_provider_channel(target: str) -> grpc.aio.Channel:
    """Open one shared SFP channel with Protocol-derived message ceilings."""
    return grpc.aio.insecure_channel(
        target,
        options=PROVIDER_CHANNEL_OPTIONS,
    )
