"""
CyberDeltaEngine: Hyperliquid API Raw Models (WebSocket Events Group)
--------------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange WebSocket event payloads.

- All models are defined locally in this file to avoid cross-file imports between model files.
- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- All models use `extra=\"forbid\"` to ensure strict schema validation—any unexpected field
  will raise a validation error.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal models with type conversions
  and business logic.
- See the Hyperliquid OpenAPI spec, SDK, and docs for field details and allowed values.

**Authoritative Reference:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

Usage:
    raw = HyperliquidRawWsFillEvent.model_validate(ws_event_dict)
    # ...then transform to internal event model

Do not use these models for internal business logic—use your core models for that.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawWsFillEvent(BaseModel):
    """
    WebSocket fill event (user fill/execution).
    Fields:
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        side: Side ('B' or 'A')
        time: Timestamp (int)
        hash: Trade hash (str)
        oid: Order ID (int)
        cloid: Client order ID (str | None)
        is_maker: Is maker (bool)
    """

    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    side: str = Field(..., alias="side")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    oid: int = Field(..., alias="oid")
    cloid: str | None = Field(None, alias="cloid")
    is_maker: bool = Field(..., alias="isMaker")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawBookLevel(BaseModel):
    """
    A single price level in the order book.
    Fields:
        px: Price (str)
        sz: Size (str)
        n: Number of orders (int)
    """

    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    n: int = Field(..., alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsBookUpdate(BaseModel):
    """
    WebSocket order book update event (l2Book channel).
    Fields:
        coin: Asset symbol (str)
        levels: [bids, asks] (list[list[HyperliquidRawBookLevel]])
        time: Snapshot timestamp (int)
    """

    coin: str = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsTradeEvent(BaseModel):
    """
    WebSocket trade event (trades channel).
    Fields:
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        side: Side ('B' or 'A')
        time: Timestamp (int)
        hash: Trade hash (str)
    """

    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    side: str = Field(..., alias="side")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsOrderUpdate(BaseModel):
    """
    WebSocket order update event (user channel).
    Fields:
        event_type: Event type (str)
        data: Event data (dict)
    """

    event_type: str = Field(..., alias="eventType")
    data: dict[str, Any] = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawLeverage(BaseModel):
    """
    Leverage settings for a position.
    Fields:
        type: Leverage type ('cross' or 'isolated')
        value: Leverage value (int)
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawPositionInfo(BaseModel):
    """
    Detailed info about a user position.
    Fields:
        coin: Asset symbol (str)
        entry_px: Entry price (str | None)
        leverage: Leverage settings (HyperliquidRawLeverage)
        liquidation_px: Liquidation price (str | None)
        margin_used: Margin used (str)
        max_leverage: Max leverage (int)
        position_value: Position value (str)
        return_on_equity: ROE (str)
        szi: Size (str)
        unrealized_pnl: Unrealized PnL (str)
    """

    coin: str = Field(..., alias="coin")
    entry_px: str | None = Field(None, alias="entryPx")
    leverage: HyperliquidRawLeverage = Field(..., alias="leverage")
    liquidation_px: str | None = Field(None, alias="liquidationPx")
    margin_used: str = Field(..., alias="marginUsed")
    max_leverage: int = Field(..., alias="maxLeverage")
    position_value: str = Field(..., alias="positionValue")
    return_on_equity: str = Field(..., alias="returnOnEquity")
    szi: str = Field(..., alias="szi")
    unrealized_pnl: str = Field(..., alias="unrealizedPnl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsPositionUpdateEvent(BaseModel):
    """
    WebSocket position update event (user position change).
    Fields:
        asset: Asset symbol (str)
        position: Position info (HyperliquidRawPositionInfo)
        time: Timestamp (int)
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
