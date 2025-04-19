"""
CyberDeltaEngine: Hyperliquid API Raw Models
-------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses.

- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- Timestamp fields are typed as `int | str | float | None` to accept ISO8601 strings, epoch
  ms/µs/seconds, or null, per the spec.
- All models use `extra="forbid"` to ensure strict schema validation—any unexpected field
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
    raw = HyperliquidRawOrder.model_validate(api_response_dict)
    # ...then transform to internal Order model

Do not use these models for internal business logic—use your core models for that.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# --- Error Model ---


class HyperliquidRawApiError(BaseModel):
    """
    Raw error response from Hyperliquid API.
    Fields:
        error: Error message string
    Strictly validated (extra fields forbidden).
    """

    error: str = Field(..., alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Meta & Asset Context ---


class HyperliquidRawAssetDefinition(BaseModel):
    """
    Asset/market definition from 'meta' response.
    Fields:
        name: Asset symbol (str)
        sz_decimals: Size decimals (int)
        max_leverage: Max leverage (int)
        only_isolated: Only isolated margin allowed (bool)
    """

    name: str = Field(..., alias="name")
    sz_decimals: int = Field(..., alias="szDecimals")
    max_leverage: int = Field(..., alias="maxLeverage")
    only_isolated: bool = Field(..., alias="onlyIsolated")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawAssetCtx(BaseModel):
    """
    Contextual info for an asset from 'metaAndAssetCtxs'.
    Fields:
        name: Asset symbol (str)
        funding: Hourly funding rate string (str)
        mark_px: Mark price (str)
        prev_day_px: Previous day price (str)
        day_ntl_vlm: Daily notional volume (str)
        impact_px: Impact price (str | None)
    """

    name: str = Field(..., alias="name")
    funding: str = Field(..., alias="funding")
    mark_px: str = Field(..., alias="markPx")
    prev_day_px: str = Field(..., alias="prevDayPx")
    day_ntl_vlm: str = Field(..., alias="dayNtlVlm")
    impact_px: str | None = Field(None, alias="impactPx")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- User State ---


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


class HyperliquidRawAssetPosition(BaseModel):
    """
    User's position details for a specific asset.
    Fields:
        asset: Asset symbol (str)
        position: Position info (HyperliquidRawPositionInfo)
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMarginSummary(BaseModel):
    """
    Margin summary for user state.
    Fields:
        account_value: Account value (str)
        total_margin_used: Total margin used (str)
        total_ntl_pos: Total notional position (str)
        total_raw_usd: Total raw USD (str)
    """

    account_value: str = Field(..., alias="accountValue")
    total_margin_used: str = Field(..., alias="totalMarginUsed")
    total_ntl_pos: str = Field(..., alias="totalNtlPos")
    total_raw_usd: str = Field(..., alias="totalRawUsd")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawClearinghouseState(BaseModel):
    """
    User state including positions and margin.
    Fields:
        asset_positions: List of asset positions (list[HyperliquidRawAssetPosition])
        margin_summary: Margin summary (HyperliquidRawMarginSummary)
        cross_maintenance_margin_used: Cross maintenance margin used (str)
        cross_margin_summary: Cross margin summary (HyperliquidRawMarginSummary)
        isolated_maintenance_margin_used: Isolated maintenance margin used (str)
        isolated_margin_summary: Isolated margin summary (HyperliquidRawMarginSummary)
        withdrawable: Withdrawable amount (str)
    """

    asset_positions: list[HyperliquidRawAssetPosition] = Field(..., alias="assetPositions")
    margin_summary: HyperliquidRawMarginSummary = Field(..., alias="marginSummary")
    cross_maintenance_margin_used: str = Field(..., alias="crossMaintenanceMarginUsed")
    cross_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="crossMarginSummary")
    isolated_maintenance_margin_used: str = Field(..., alias="isolatedMaintenanceMarginUsed")
    isolated_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="isolatedMarginSummary")
    withdrawable: str = Field(..., alias="withdrawable")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Order Book, Trades, Orders, Fills ---


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


class HyperliquidRawL2Book(BaseModel):
    """
    L2 Order Book snapshot.
    Fields:
        coin: Asset symbol (str)
        levels: [bids, asks] (list[list[HyperliquidRawBookLevel]])
        time: Snapshot timestamp (int)
    """

    coin: str = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawPublicTrade(BaseModel):
    """
    Public trade object from recent trades.
    Fields:
        coin: Asset symbol (str)
        side: Side ('B' or 'A')
        px: Price (str)
        sz: Size (str)
        time: Timestamp (int)
        hash: Trade hash (str)
    """

    coin: str = Field(..., alias="coin")
    side: str = Field(..., alias="side")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Exchange Action Response ---


class HyperliquidRawExchangeStatusObject(BaseModel):
    """
    Status object for order/cancel/modify responses.
    Fields:
        resting: Resting order (dict or None)
        filled: Filled order (dict or None)
        error: Error message (str or None)
    """

    resting: dict[str, Any] | None = Field(None, alias="resting")
    filled: dict[str, Any] | None = Field(None, alias="filled")
    error: str | None = Field(None, alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawExchangeResponseData(BaseModel):
    """
    Structure within the 'data' field of a successful exchange action.
    Fields:
        type: Type of response (str)
        statuses: List of status objects or strings
    """

    type: str = Field(..., alias="type")
    statuses: list[str | HyperliquidRawExchangeStatusObject] = Field(..., alias="statuses")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawExchangeActionResponse(BaseModel):
    """
    Top-level response for exchange actions.
    Fields:
        status: Status string (should be 'ok')
        data: Exchange response data (HyperliquidRawExchangeResponseData)
    """

    status: str = Field(..., alias="status")
    data: HyperliquidRawExchangeResponseData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawTriggerInfo(BaseModel):
    """
    Trigger details if present (for conditional orders).
    Fields:
        trigger_px: Trigger price (str)
        is_market: Is market order (bool)
        tpsl: Trigger type ('tp' or 'sl')
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: str = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOrder(BaseModel):
    """
    Core order details from open orders or order status.
    Fields:
        oid: Order ID (int)
        cloid: Client order ID (str | None)
        asset: Asset symbol (str)
        side: Side ('B' or 'A')
        limit_px: Limit price (str)
        sz: Size (str)
        timestamp: Creation timestamp (int)
        order_type: Order type (dict[str, Any])
        reduce_only: Reduce-only flag (bool)
        remaining_sz: Remaining size (str)
        status: Status string (e.g., 'open')
        status_timestamp: Last update timestamp (int)
    """

    oid: int = Field(..., alias="oid")
    cloid: str | None = Field(None, alias="cloid")
    asset: str = Field(..., alias="asset")
    side: str = Field(..., alias="side")
    limit_px: str = Field(..., alias="limitPx")
    sz: str = Field(..., alias="sz")
    timestamp: int = Field(..., alias="timestamp")
    order_type: dict[str, Any] = Field(..., alias="orderType")
    reduce_only: bool = Field(..., alias="reduceOnly")
    remaining_sz: str = Field(..., alias="remainingSz")
    status: str = Field(..., alias="status")
    status_timestamp: int = Field(..., alias="statusTimestamp")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOpenOrder(BaseModel):
    """
    Structure for one open order (with optional trigger).
    Fields:
        order: Order details (HyperliquidRawOrder)
        trigger: Trigger info (HyperliquidRawTriggerInfo | None)
    """

    order: HyperliquidRawOrder = Field(..., alias="order")
    trigger: HyperliquidRawTriggerInfo | None = Field(None, alias="trigger")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUserFill(BaseModel):
    """
    User fill/trade details from userFills response.
    Fields:
        tid: Trade ID (int)
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        time: Timestamp (int)
        side: Side ('B' or 'A')
        oid: Order ID (int)
        start_position: Start position (str)
        dir: Direction (str)
        hash: Trade hash (str)
        fee: Fee (str)
        is_maker: Is maker (bool)
        liquidation_mark_px: Liquidation mark price (str | None)
        cloid: Client order ID (str | None)
    """

    tid: int = Field(..., alias="tid")
    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    side: str = Field(..., alias="side")
    oid: int = Field(..., alias="oid")
    start_position: str = Field(..., alias="startPosition")
    dir: str = Field(..., alias="dir")
    hash: str = Field(..., alias="hash")
    fee: str = Field(..., alias="fee")
    is_maker: bool = Field(..., alias="isMaker")
    liquidation_mark_px: str | None = Field(None, alias="liquidationMarkPx")
    cloid: str | None = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawCandleSnapshot(BaseModel):
    """
    Candle snapshot response from candleSnapshot.
    Fields:
        t: List of timestamps (list[int])
        o: List of open prices (list[str])
        h: List of high prices (list[str])
        low: List of low prices (list[str]), field alias 'l'
        c: List of close prices (list[str])
        v: List of volumes (list[str])
        s: Status string (str)
    """

    t: list[int] = Field(..., alias="t")
    o: list[str] = Field(..., alias="o")
    h: list[str] = Field(..., alias="h")
    low: list[str] = Field(..., alias="l")
    c: list[str] = Field(..., alias="c")
    v: list[str] = Field(..., alias="v")
    s: str = Field(..., alias="s")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawAllMids(BaseModel):
    """
    AllMids response: mapping of asset symbol to mid price (as string).
    Fields:
        __root__: Dict[str, str]
    """

    __root__: dict[str, str]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaAndAssetCtxs(BaseModel):
    """
    MetaAndAssetCtxs response: [MetaResponse, List[AssetCtx]].
    Fields:
        __root__: list[Any] (should be [HyperliquidRawMetaResponse, list[HyperliquidRawAssetCtx]])
    """

    __root__: list[Any]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- WebSocket Event Payloads (Drafts) ---


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


# --- Batch/Array and Tuple Response Models ---


class HyperliquidRawOpenOrdersResponse(BaseModel):
    """
    Array of open orders from openOrders response.
    Fields:
        __root__: List of HyperliquidRawOpenOrder
    """

    __root__: list[HyperliquidRawOpenOrder]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUserFillsResponse(BaseModel):
    """
    Array of user fills from userFills response.
    Fields:
        __root__: List of HyperliquidRawUserFill
    """

    __root__: list[HyperliquidRawUserFill]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawRecentTradesResponse(BaseModel):
    """
    Array of public trades from recentTrades response.
    Fields:
        __root__: List of HyperliquidRawPublicTrade
    """

    __root__: list[HyperliquidRawPublicTrade]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaResponse(BaseModel):
    """
    Meta response: universe/market metadata.
    Fields:
        universe: List of HyperliquidRawAssetDefinition
    """

    universe: list[HyperliquidRawAssetDefinition] = Field(..., alias="universe")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    """
    MetaAndAssetCtxs response: strict 2-tuple [meta, assetCtxs].
    Fields:
        meta: HyperliquidRawMetaResponse
        asset_ctxs: List of HyperliquidRawAssetCtx
    """

    meta: HyperliquidRawMetaResponse
    asset_ctxs: list[HyperliquidRawAssetCtx]

    @classmethod
    def model_validate(
        cls, obj: Any, *args: Any, **kwargs: Any
    ) -> "HyperliquidRawMetaAndAssetCtxsResponse":
        """
        Validate a MetaAndAssetCtxs response from a list [meta, assetCtxs].
        """
        if (
            isinstance(obj, list)
            and len(obj) == 2
            and isinstance(obj[0], dict)
            and isinstance(obj[1], list)
        ):
            meta = HyperliquidRawMetaResponse.model_validate(obj[0])
            asset_ctxs = [HyperliquidRawAssetCtx.model_validate(x) for x in obj[1]]
            return cls(meta=meta, asset_ctxs=asset_ctxs)
        raise ValueError("Invalid MetaAndAssetCtxs response structure")

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUserStateResponse(BaseModel):
    """
    User state response: clearinghouseState.
    Fields:
        clearinghouse_state: HyperliquidRawClearinghouseState
    """

    clearinghouse_state: HyperliquidRawClearinghouseState = Field(..., alias="clearinghouseState")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- WebSocket Event Models (Expanded) ---


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


# --- Aliases for clarity (for batch/array responses) ---
HyperliquidRawOpenOrders = HyperliquidRawOpenOrdersResponse
HyperliquidRawUserFills = HyperliquidRawUserFillsResponse
HyperliquidRawRecentTrades = HyperliquidRawRecentTradesResponse

# --- Request Payload Models (for outgoing validation) ---


class HyperliquidRawMetaRequestPayload(BaseModel):
    """
    Request payload for 'meta' info type.
    Fields:
        type: Must be 'meta'
    """

    type: str = Field("meta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaAndAssetCtxsRequestPayload(BaseModel):
    """
    Request payload for 'metaAndAssetCtxs' info type.
    Fields:
        type: Must be 'metaAndAssetCtxs'
    """

    type: str = Field("metaAndAssetCtxs", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawAllMidsRequestPayload(BaseModel):
    """
    Request payload for 'allMids' info type.
    Fields:
        type: Must be 'allMids'
    """

    type: str = Field("allMids", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUserStateRequestPayload(BaseModel):
    """
    Request payload for 'clearinghouseState' info type.
    Fields:
        type: Must be 'clearinghouseState'
        user: Wallet address (str)
    """

    type: str = Field("clearinghouseState", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOpenOrdersRequestPayload(BaseModel):
    """
    Request payload for 'openOrders' info type.
    Fields:
        type: Must be 'openOrders'
        user: Wallet address (str)
    """

    type: str = Field("openOrders", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUserFillsRequestPayload(BaseModel):
    """
    Request payload for 'userFills' info type.
    Fields:
        type: Must be 'userFills'
        user: Wallet address (str)
    """

    type: str = Field("userFills", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawL2BookRequestPayload(BaseModel):
    """
    Request payload for 'l2Book' info type.
    Fields:
        type: Must be 'l2Book'
        coin: Asset symbol (str)
    """

    type: str = Field("l2Book", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawRecentTradesRequestPayload(BaseModel):
    """
    Request payload for 'recentTrades' info type.
    Fields:
        type: Must be 'recentTrades'
        coin: Asset symbol (str)
    """

    type: str = Field("recentTrades", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """
    Request payload for 'candleSnapshot' info type.
    Fields:
        type: Must be 'candleSnapshot'
        coin: Asset symbol (str)
        interval: Interval string (e.g., '1m', '1h', '1d')
        start_time: Start timestamp (int)
        end_time: End timestamp (int)
    """

    type: str = Field("candleSnapshot", alias="type")
    coin: str = Field(..., alias="coin")
    interval: str = Field(..., alias="interval")
    start_time: int = Field(..., alias="startTime")
    end_time: int = Field(..., alias="endTime")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Strict submodels for orderType polymorphic fields ---


class HyperliquidRawTifLimit(BaseModel):
    """
    Time-in-force for limit orders.
    Fields:
        tif: Time in force (str: 'Gtc', 'Ioc', 'Alo')
    """

    tif: str = Field(..., alias="tif")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOrderTypeLimit(BaseModel):
    """
    Limit order type for orderType field.
    Fields:
        limit: HyperliquidRawTifLimit
    """

    limit: HyperliquidRawTifLimit = Field(..., alias="limit")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOrderTypeMarket(BaseModel):
    """
    Market order type for orderType field.
    Fields:
        market: dict (empty object)
    """

    market: dict[str, Any] = Field(..., alias="market")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Strict models for exchange action request payloads ---


class HyperliquidRawTriggerSpec(BaseModel):
    """
    Trigger spec for conditional orders.
    Fields:
        trigger_px: Trigger price (str)
        is_market: Is market order (bool)
        tpsl: Trigger type ('tp' or 'sl')
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: str = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOrderSpec(BaseModel):
    """
    Order spec for placing an order (exchange action request).
    Fields:
        asset: Asset index (int)
        is_buy: Is buy (bool)
        limit_px: Limit price (str)
        sz: Size (float)
        reduce_only: Reduce-only flag (bool)
        order_type: One of HyperliquidRawOrderTypeLimit or HyperliquidRawOrderTypeMarket
        trigger: Optional trigger spec
        cloid: Optional client order ID (str)
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    limit_px: str = Field(..., alias="limitPx")
    sz: float = Field(..., alias="sz")
    reduce_only: bool = Field(..., alias="reduceOnly")
    order_type: dict[str, Any] = Field(..., alias="orderType")  # Could use Union of strict models
    trigger: HyperliquidRawTriggerSpec | None = Field(None, alias="trigger")
    cloid: str | None = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawCancelRequest(BaseModel):
    """
    Cancel request payload (by exchange OID).
    Fields:
        asset: Asset index (int)
        oid: Order ID (int)
    """

    asset: int = Field(..., alias="asset")
    oid: int = Field(..., alias="oid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawCancelByCloidRequest(BaseModel):
    """
    Cancel request payload (by client OID).
    Fields:
        asset: Asset index (int)
        cloid: Client order ID (str)
    """

    asset: int = Field(..., alias="asset")
    cloid: str = Field(..., alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawModifyOrderRequest(BaseModel):
    """
    Modify order request payload.
    Fields:
        oid: Order ID (int)
        order: HyperliquidRawOrderSpec
    """

    oid: int = Field(..., alias="oid")
    order: HyperliquidRawOrderSpec = Field(..., alias="order")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUpdateLeverageRequest(BaseModel):
    """
    Update leverage request payload.
    Fields:
        asset: Asset index (int)
        is_cross: Is cross margin (bool)
        leverage: Leverage value (int)
    """

    asset: int = Field(..., alias="asset")
    is_cross: bool = Field(..., alias="isCross")
    leverage: int = Field(..., alias="leverage")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUpdateIsolatedMarginRequest(BaseModel):
    """
    Update isolated margin request payload.
    Fields:
        asset: Asset index (int)
        is_buy: Is buy (bool)
        ntli: Amount (int)
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    ntli: int = Field(..., alias="ntli")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# ... end of new/expanded request and strict submodels ...
