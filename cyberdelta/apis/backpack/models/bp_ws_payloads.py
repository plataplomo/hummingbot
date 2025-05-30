"""
CyberDeltaEngine: Backpack WebSocket Subscription Request Payload Models
------------------------------------------------------------------------

This module defines Pydantic models for WebSocket subscription/unsubscription
request payloads for the Backpack exchange API.

These models strictly represent the JSON structure sent to the WebSocket API
for subscribing to or unsubscribing from streams, as defined in the
openapi_backpack.json specification.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .bp_common_raw_types import (
    RawBpNonEmptyStringMax64,
    RawBpNonEmptyStringMax128,
    RawBpNonEmptyStringMax255,
)


class BackpackWsSignatureComponents(BaseModel):
    """
    Pydantic model for Backpack WebSocket subscription signature components.

    This model represents the components needed for authenticating private
    WebSocket subscriptions to the Backpack exchange.
    """

    api_key: RawBpNonEmptyStringMax255  # Base64 encoded verifying key
    timestamp: RawBpNonEmptyStringMax64  # Timestamp as string
    window: RawBpNonEmptyStringMax64  # Window as string
    signature: RawBpNonEmptyStringMax255  # Base64 encoded signature

    model_config = ConfigDict(extra="forbid", frozen=True)


class BackpackRawWsSubscriptionRequest(BaseModel):
    """
    Raw model for Backpack WebSocket subscription/unsubscription requests.

    Based on the Backpack API documentation:
    - Public streams: {"method": "SUBSCRIBE", "params": ["stream_name"]}
    - Private streams: {"method": "SUBSCRIBE", "params": ["stream_name"],
                       "signature": ["<verifying key>", "<signature>", "<timestamp>", "<window>"]}
    """

    method: Literal["SUBSCRIBE", "UNSUBSCRIBE"]
    params: list[RawBpNonEmptyStringMax128]  # Stream names
    signature: (
        tuple[
            RawBpNonEmptyStringMax255,  # Base64 encoded verifying key
            RawBpNonEmptyStringMax255,  # Base64 encoded signature
            RawBpNonEmptyStringMax64,  # Timestamp as string
            RawBpNonEmptyStringMax64,  # Window as string
        ]
        | None
    ) = Field(default=None)

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
