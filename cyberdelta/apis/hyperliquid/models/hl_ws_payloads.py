"""
CyberDeltaEngine: Hyperliquid WebSocket Subscription Request Payload Models
---------------------------------------------------------------------------

This module defines Pydantic models for WebSocket subscription/unsubscription
request payloads for the Hyperliquid exchange API.

These models strictly represent the JSON structure sent to the WebSocket API
for subscribing to or unsubscribing from streams, based on the explicit
schemas provided in the workflow documentation.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict

from .common_raw_types import (
    RawAssetString64HL,
    RawLaxEthereumAddressStrHL,
    RawTimeframeString,
)


class HyperliquidRawWsL2BookSubscriptionPayload(BaseModel):
    """Raw model for L2 order book subscription payload."""

    type: Literal["l2Book"]
    coin: RawAssetString64HL
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawWsTradesSubscriptionPayload(BaseModel):
    """Raw model for trades subscription payload."""

    type: Literal["trades"]
    coin: RawAssetString64HL
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawWsUserEventsSubscriptionPayload(BaseModel):
    """Raw model for user events subscription payload."""

    type: Literal["userEvents"]
    user: RawLaxEthereumAddressStrHL
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawWsCandleSubscriptionPayload(BaseModel):
    """Raw model for candle/kline subscription payload."""

    type: Literal["candle"]
    coin: RawAssetString64HL
    interval: RawTimeframeString
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawWsAllMidsSubscriptionPayload(BaseModel):
    """Raw model for all mids subscription payload."""

    type: Literal["allMids"]
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawWsSubscribeRequest(BaseModel):
    """
    Raw model for Hyperliquid WebSocket subscription/unsubscription requests.

    Based on Hyperliquid API patterns:
    - {"method": "subscribe", "subscription": {...}}
    - {"method": "unsubscribe", "subscription": {...}}

    Note: Hyperliquid uses lowercase for methods unlike Backpack.
    """

    method: Literal["subscribe", "unsubscribe"]
    subscription: (
        HyperliquidRawWsL2BookSubscriptionPayload
        | HyperliquidRawWsTradesSubscriptionPayload
        | HyperliquidRawWsUserEventsSubscriptionPayload
        | HyperliquidRawWsCandleSubscriptionPayload
        | HyperliquidRawWsAllMidsSubscriptionPayload
    )
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
