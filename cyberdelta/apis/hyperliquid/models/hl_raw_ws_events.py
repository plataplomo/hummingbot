"""
CyberDeltaEngine: Hyperliquid API Raw Models (WebSocket Events Group)
--------------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange WebSocket event payloads. It is a core part of CyberDeltaEngine's boundary
validation layer for real-time data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures received from
  Hyperliquid's WebSocket channels, including user fills, order book updates, trades, and position
  updates.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or
  malformed fields in upstream data are immediately rejected. This is critical for robust, secure,
  and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate
  external data at the boundary, then map to internal business models with type conversions and
  business logic.

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawWsFillEvent.model_validate(ws_event_dict)
    # ...then transform to internal event model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for
boundary validation only.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawWsFillEvent(BaseModel):
    """
    Represents a WebSocket fill event (user fill/execution) as received from the Hyperliquid user
    channel.

    This model is used to validate the structure of fill events, which indicate a user's order has
    been executed. It is a strict mirror of the upstream API schema and should not be used for
    internal business logic.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        px (str): Price at which the fill occurred.
        sz (str): Size of the fill.
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        time (int): Timestamp of the fill event (epoch ms).
        hash (str): Unique trade hash.
        oid (int): Order ID associated with the fill.
        cloid (Optional[str]): Client order ID, if present.
        is_maker (bool): True if the user was the maker in this trade.
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
    Represents a single price level in the order book as received via WebSocket updates.

    This model is used to validate the structure of each price level entry in order book update
    events.

    Fields:
        px (str): Price at this level.
        sz (str): Size available at this price level.
        n (int): Number of orders at this price level.
    """

    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    n: int = Field(..., alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsBookUpdate(BaseModel):
    """
    Represents a WebSocket order book update event (l2Book channel).

    This model is used to validate the structure of order book update events, which provide the
    latest bids and asks for an asset.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        levels (List[List[HyperliquidRawBookLevel]]): Nested list of price levels [bids, asks].
        time (int): Snapshot timestamp (epoch ms).
    """

    coin: str = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsTradeEvent(BaseModel):
    """
    Represents a WebSocket trade event (trades channel) as received from the Hyperliquid public
    stream.

    This model is used to validate the structure of public trade events, which provide real-time
    trade data for an asset.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        px (str): Price at which the trade occurred.
        sz (str): Size of the trade.
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        time (int): Timestamp of the trade event (epoch ms).
        hash (str): Unique trade hash.
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
    Represents a WebSocket order update event (user channel) as received from the Hyperliquid
    private stream.

    This model is used to validate the structure of order update events, which notify the user of
    changes to their orders (e.g., open, filled, canceled).

    Fields:
        event_type (str): Type of the event (e.g., 'orderUpdate').
        data (dict): Event data payload (structure may vary by event type).
    """

    event_type: str = Field(..., alias="eventType")
    data: dict[str, Any] = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawLeverage(BaseModel):
    """
    Represents leverage settings for a position as received in WebSocket position updates.

    This model is used as a submodel in position update events to describe the leverage type and
    value.

    Fields:
        type (str): Leverage type ('cross' or 'isolated').
        value (int): Leverage value.
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawPositionInfo(BaseModel):
    """
    Represents detailed information about a user position as received in WebSocket position
    updates.

    This model is used as a submodel in position update events to describe the user's position for
    a given asset.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        entry_px (Optional[str]): Entry price, if present.
        leverage (HyperliquidRawLeverage): Leverage settings for this position.
        liquidation_px (Optional[str]): Liquidation price, if present.
        margin_used (str): Margin used for this position.
        max_leverage (int): Maximum leverage allowed for this asset.
        position_value (str): Value of the position.
        return_on_equity (str): Return on equity (ROE) for this position.
        szi (str): Size of the position.
        unrealized_pnl (str): Unrealized profit and loss for this position.
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
    Represents a WebSocket position update event (user position change) as received from the
    Hyperliquid private stream.

    This model is used to validate the structure of position update events, which notify the user
    of changes to their open positions.

    Fields:
        asset (str): Asset symbol (e.g., 'ETH', 'BTC').
        position (HyperliquidRawPositionInfo): Detailed position information.
        time (int): Timestamp of the position update event (epoch ms).
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
