"""
CyberDeltaEngine: Backpack API Raw Models
-----------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Backpack Exchange API (REST and WebSocket) responses.

- Each `BackpackRaw*` model mirrors the official Backpack OpenAPI spec or WebSocket event
  payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Backpack's JSON
  (including short names for WS events).
- Timestamp fields are typed as `int | str | float | None` to accept ISO8601 strings, epoch
  ms/µs/seconds, or null, per the spec.
- All models use `extra="forbid"` to ensure strict schema validation—any unexpected field
  will raise a validation error.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal models with type conversions
  and business logic.
- See the Backpack OpenAPI spec and WS documentation for field details and allowed values.

**Authoritative Reference:**
- Official Backpack Exchange API documentation: https://docs.backpack.exchange/
- OpenAPI JSON specification: [see docs site for download link]

Usage:
    raw = BackpackRawOrder.model_validate(api_response_dict)
    # ...then transform to internal Order model

Do not use these models for internal business logic—use your core models for that.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# --- REST API Response Models ---


class BackpackRawAccount(BaseModel):
    """
    Raw account summary from `/api/v1/account`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /account)
    Fields:
        id: Unique account identifier (str)
        email: User's registered email address (str)
        status: Account status (e.g., 'active', 'suspended') (str)
    """

    id: str = Field(..., alias="id")
    email: str = Field(..., alias="email")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawBalance(BaseModel):
    """
    Raw asset balance from `/api/v1/capital`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /capital)
    Fields:
        asset: Asset/currency symbol (e.g., 'USDC', 'BTC') (str)
        available: Amount available for trading (as string)
        total: Total balance (as string)
    """

    asset: str = Field(..., alias="asset")
    available: str = Field(..., alias="available")
    total: str = Field(..., alias="total")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawPosition(BaseModel):
    """
    Raw open position from `/api/v1/positions`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /positions)
    Fields:
        symbol: Trading symbol (e.g., 'BTC_USDC_PERP') (str)
        size: Net position size (as string)
        entry_price: Average entry price (as string)
        mark_price: Current mark price (as string)
    """

    symbol: str = Field(..., alias="symbol")
    size: str = Field(..., alias="positionSize")
    entry_price: str = Field(..., alias="entryPrice")
    mark_price: str = Field(..., alias="markPrice")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawOrder(BaseModel):
    """
    Raw order object from `/api/v1/order` or `/api/v1/orders`.
    Field names and types mirror the Backpack API response exactly.
    Mapping to internal model names is handled in the transformation/mapping layer.
    """

    clientId: str | None = Field(None, description="Client-generated unique order ID (UUID).")
    id: str = Field(..., description="Exchange-provided order ID.")
    relatedOrderId: str | None = Field(
        None,
        description="ID of related order (e.g., parent, trigger target).",
    )
    exchange: str | None = Field(
        None,
        description="Name of the exchange (not present in Backpack, set in mapping).",
    )
    symbol: str = Field(..., description="Trading symbol.")
    side: str = Field(..., description="Order side ('buy', 'sell', 'Bid', 'Ask').")
    orderType: str = Field(..., description="Order type ('LIMIT', 'MARKET', etc.).")
    status: str = Field(..., description="Order status ('NEW', 'FILLED', etc.).")
    quantity: str = Field(..., description="Requested order quantity.")
    executedQuantity: str | None = Field(None, description="Total filled quantity.")
    executedQuoteQuantity: str | None = Field(None, description="Filled quote quantity.")
    price: str | None = Field(None, description="Limit price.")
    triggerPrice: str | None = Field(None, description="Stop trigger price.")
    avgFillPrice: str | None = Field(None, description="Weighted average fill price.")
    triggerBy: str | None = Field(None, description="Reference price type for triggers.")
    timeInForce: str | None = Field(None, description="Time in force.")
    reduceOnly: bool | None = Field(None, description="Reduce-only flag.")
    postOnly: bool | None = Field(
        None,
        description="Post-only flag (not present in Backpack REST, set in mapping if needed).",
    )
    selfTradePrevention: str | None = Field(None, description="Self-trade prevention behavior.")
    createdAt: int | str | float | None = Field(..., description="Order creation time (UTC).")
    updatedAt: int | str | float | None = Field(None, description="Last update time.")
    triggeredAt: int | str | float | None = Field(
        None, description="Time the conditional order was triggered."
    )
    expiryReason: str | None = Field(None, description="Reason for expiry/cancellation.")
    origin: str | None = Field(None, description="Origin of the last update.")
    strategyName: str | None = Field(
        None,
        description="Optional strategy identifier (not present in Backpack, set in mapping if needed).",
    )
    signalId: str | None = Field(
        None,
        description="Optional signal identifier (not present in Backpack, set in mapping if needed).",
    )
    trades: list[Any] | None = Field(
        None,
        description="List of associated trade fills (not present in Backpack order response).",
    )
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawTrade(BaseModel):
    """
    Raw trade/fill from `/api/v1/trades`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /trades)
    Fields:
        id: Trade ID (str)
        order_id: Associated order ID (str)
        symbol: Trading symbol (str)
        price: Execution price (as string)
        quantity: Executed quantity (as string)
        time: Execution timestamp (int | str | float | None)
    """

    id: str = Field(..., alias="id")
    order_id: str = Field(..., alias="orderId")
    symbol: str = Field(..., alias="symbol")
    price: str = Field(..., alias="price")
    quantity: str = Field(..., alias="qty")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawFundingRate(BaseModel):
    """
    Raw funding rate object from `/api/v1/funding`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /funding)
    Fields:
        symbol: Trading symbol (str)
        funding_rate: Current funding rate (as string)
        mark_price: Mark price (as string)
        index_price: Index price (as string)
        time: Data timestamp (int | str | float | None)
    """

    symbol: str = Field(..., alias="symbol")
    funding_rate: str = Field(..., alias="rate")
    mark_price: str = Field(..., alias="markPrice")
    index_price: str = Field(..., alias="indexPrice")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawOrderBook(BaseModel):
    """
    Raw order book snapshot from `/api/v1/depth`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /depth)
    Fields:
        symbol: Trading symbol (str)
        bids: List of [price, quantity] for bids (List[List[str]])
        asks: List of [price, quantity] for asks (List[List[str]])
        time: Snapshot timestamp (int | str | float | None)
    """

    symbol: str = Field(..., alias="symbol")
    bids: list[list[str]] = Field(..., alias="bids")
    asks: list[list[str]] = Field(..., alias="asks")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawTicker(BaseModel):
    """
    Raw ticker object from `/api/v1/ticker`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /ticker)
    Fields:
        symbol: Trading symbol (str)
        price: Last traded price (as string, optional)
        bid: Best bid price (as string, optional)
        ask: Best ask price (as string, optional)
        volume: 24h trading volume (as string, optional)
        time: Ticker timestamp (int | str | float | None)
    """

    symbol: str = Field(..., alias="symbol")
    price: str | None = Field(None, alias="price")
    bid: str | None = Field(None, alias="bid")
    ask: str | None = Field(None, alias="ask")
    volume: str | None = Field(None, alias="volume")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawMarket(BaseModel):
    """
    Raw market metadata object from `/api/v1/markets`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /markets)
    Fields:
        symbol: Trading symbol (str)
        base_asset: Base asset symbol (str)
        quote_asset: Quote asset symbol (str)
    """

    symbol: str = Field(..., alias="symbol")
    base_asset: str = Field(..., alias="baseAsset")
    quote_asset: str = Field(..., alias="quoteAsset")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawWithdrawal(BaseModel):
    """
    Raw withdrawal object from `/api/v1/withdrawals`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /withdrawals)
    Fields:
        id: Withdrawal ID (str)
        asset: Asset symbol (str)
        amount: Withdrawal amount (as string)
        status: Withdrawal status (e.g., 'pending', 'completed') (str)
    """

    id: str = Field(..., alias="id")
    asset: str = Field(..., alias="asset")
    amount: str = Field(..., alias="amount")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawDeposit(BaseModel):
    """
    Raw deposit object from `/api/v1/deposits`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /deposits)
    Fields:
        id: Deposit ID (str)
        asset: Asset symbol (str)
        amount: Deposit amount (as string)
        status: Deposit status (e.g., 'pending', 'completed') (str)
    """

    id: str = Field(..., alias="id")
    asset: str = Field(..., alias="asset")
    amount: str = Field(..., alias="amount")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawOpenInterest(BaseModel):
    """
    Raw open interest data from `/api/v1/openInterest`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /openInterest)
    Fields:
        symbol: Trading symbol (str)
        open_interest: Open interest (as string)
    """

    symbol: str = Field(..., alias="symbol")
    open_interest: str = Field(..., alias="openInterest")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawLiquidation(BaseModel):
    """
    Raw liquidation event from `/api/v1/liquidations`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /liquidations)
    Fields:
        symbol: Trading symbol (str)
        price: Liquidation price (as string)
        quantity: Liquidated quantity (as string)
        side: Side ('buy', 'sell', etc.) (str)
    """

    symbol: str = Field(..., alias="symbol")
    price: str = Field(..., alias="price")
    quantity: str = Field(..., alias="quantity")
    side: str = Field(..., alias="side")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawMarkPrice(BaseModel):
    """
    Raw mark price and funding info from `/api/v1/markPrice`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /markPrice)
    Fields:
        symbol: Trading symbol (str)
        mark_price: Mark price (as string)
        funding_rate: Funding rate (as string)
    """

    symbol: str = Field(..., alias="symbol")
    mark_price: str = Field(..., alias="markPrice")
    funding_rate: str = Field(..., alias="fundingRate")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- WebSocket Event Models ---


class BackpackRawOrderUpdate(BaseModel):
    """
    Raw order update event from the Backpack WebSocket stream (`orderUpdate`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for orderUpdate)
    Fields:
        event_type: Event type (e.g., 'orderAccepted', 'orderFill', ...) (str)
        event_time: Event time (int | str | float | None)
        symbol: Trading symbol (str)
        client_order_id: Client order ID (str | None)
        side: Order side ('Bid', 'Ask') (str)
        order_type: Order type ('LIMIT', 'MARKET', etc.) (str)
        time_in_force: Time in force (str | None)
        quantity: Quantity (str | None)
        price: Price (str | None)
        order_status: Order state/status (str)
        ...plus other fields per WS event spec
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    client_order_id: str | None = Field(None, alias="c")
    side: str = Field(..., alias="S")
    order_type: str = Field(..., alias="o")
    time_in_force: str | None = Field(None, alias="f")
    quantity: str | None = Field(None, alias="q")
    price: str | None = Field(None, alias="p")
    order_status: str = Field(..., alias="X")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawPositionUpdate(BaseModel):
    """
    Raw position update event from the Backpack WebSocket stream (`positionUpdate`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for positionUpdate)
    Fields:
        event_type: Event type (e.g., 'positionOpened', ...) (str)
        event_time: Event time (int | str | float | None)
        symbol: Trading symbol (str)
        break_event_price: Break event price (str | None)
        entry_price: Entry price (str | None)
        liquidation_price: Estimated liquidation price (str | None)
        initial_margin_fraction: Initial margin fraction (str | None)
        mark_price: Mark price (str | None)
        maintenance_margin_fraction: Maintenance margin fraction (str | None)
        net_quantity: Net quantity (str | None)
        net_exposure_quantity: Net exposure quantity (str | None)
        net_exposure_notional: Net exposure notional (str | None)
        ...plus other fields per WS event spec
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    break_event_price: str | None = Field(None, alias="b")
    entry_price: str | None = Field(None, alias="B")
    liquidation_price: str | None = Field(None, alias="l")
    initial_margin_fraction: str | None = Field(None, alias="f")
    mark_price: str | None = Field(None, alias="M")
    maintenance_margin_fraction: str | None = Field(None, alias="m")
    net_quantity: str | None = Field(None, alias="q")
    net_exposure_quantity: str | None = Field(None, alias="Q")
    net_exposure_notional: str | None = Field(None, alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawTradeEvent(BaseModel):
    """
    Raw trade event from the Backpack WebSocket stream (`trade`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for trade)
    Fields:
        event_type: Event type ('trade') (str)
        event_time: Event time (int | str | float | None)
        symbol: Trading symbol (str)
        price: Price (as string)
        quantity: Quantity (as string)
        buyer_order_id: Buyer order ID (str)
        seller_order_id: Seller order ID (str)
        trade_id: Trade ID (str)
        engine_timestamp: Engine timestamp (int | str | float | None)
        is_buyer_the_maker: Is buyer the maker? (bool)
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    price: str = Field(..., alias="p")
    quantity: str = Field(..., alias="q")
    buyer_order_id: str = Field(..., alias="b")
    seller_order_id: str = Field(..., alias="a")
    trade_id: str = Field(..., alias="t")
    engine_timestamp: int | str | float | None = Field(..., alias="T")
    is_buyer_the_maker: bool = Field(..., alias="m")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawBookTicker(BaseModel):
    """
    Raw book ticker event from the Backpack WebSocket stream (`bookTicker`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for bookTicker)
    Fields:
        event_type: Event type ('bookTicker') (str)
        event_time: Event time (int | str | float | None)
        symbol: Trading symbol (str)
        inside_ask_price: Inside ask price (str)
        inside_ask_quantity: Inside ask quantity (str)
        inside_bid_price: Inside bid price (str)
        inside_bid_quantity: Inside bid quantity (str)
        update_id: Update ID (str)
        engine_timestamp: Engine timestamp (int | str | float | None)
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    inside_ask_price: str = Field(..., alias="a")
    inside_ask_quantity: str = Field(..., alias="A")
    inside_bid_price: str = Field(..., alias="b")
    inside_bid_quantity: str = Field(..., alias="B")
    update_id: str = Field(..., alias="u")
    engine_timestamp: int | str | float | None = Field(..., alias="T")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawDepthEvent(BaseModel):
    """
    Raw depth event from the Backpack WebSocket stream (`depth`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for depth)
    Fields:
        event_type: Event type ('depth') (str)
        event_time: Event time (int | str | float | None)
        symbol: Trading symbol (str)
        asks: List of asks [[price, qty], ...] (List[List[str]])
        bids: List of bids [[price, qty], ...] (List[List[str]])
        first_update_id: First update ID (str)
        last_update_id: Last update ID (str)
        engine_timestamp: Engine timestamp (int | str | float | None)
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    asks: list[list[str]] = Field(..., alias="a")
    bids: list[list[str]] = Field(..., alias="b")
    first_update_id: str = Field(..., alias="U")
    last_update_id: str = Field(..., alias="u")
    engine_timestamp: int | str | float | None = Field(..., alias="T")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawKlineEvent(BaseModel):
    """
    Raw kline/candlestick event from the Backpack WebSocket stream (`kline`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for kline)
    Fields:
        event_type: Event type ('kline') (str)
        event_time: Event time (int | str | float | None)
        symbol: Trading symbol (str)
        kline_start_time: K-line start time (str)
        kline_close_time: K-line close time (str)
        open_price: Open price (as string)
        close_price: Close price (as string)
        high_price: High price (as string)
        liquidation_price: Low price (as string)
        base_asset_volume: Base asset volume (as string)
        number_of_trades: Number of trades (as string)
        is_kline_closed: Is this k-line closed? (bool)
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    kline_start_time: str = Field(..., alias="t")
    kline_close_time: str = Field(..., alias="T")
    open_price: str = Field(..., alias="o")
    close_price: str = Field(..., alias="c")
    high_price: str = Field(..., alias="h")
    liquidation_price: str = Field(..., alias="l")
    base_asset_volume: str = Field(..., alias="v")
    number_of_trades: str = Field(..., alias="n")
    is_kline_closed: bool = Field(..., alias="X")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawRFQEvent(BaseModel):
    """
    Raw RFQ (Request for Quote) event from the Backpack WebSocket stream (`rfqUpdate`).
    Mirrors the schema in the Backpack OpenAPI spec and WS event documentation.

    Reference: https://docs.backpack.exchange/ (see WebSocket docs for rfqUpdate)
    Fields:
        event_type: Event type (e.g., 'rfqActive', 'quoteAccepted', ...) (str)
        event_time: Event time (int | str | float | None)
        rfq_id: RFQ ID (str)
        quote_id: Quote ID (str)
        client_quote_id: Client Quote ID (str | None)
        symbol: Trading symbol (str)
        quantity: Quantity (as string)
        side: Side ('Bid', 'Ask', optional) (str | None)
        price: Price (as string, optional) (str | None)
        submission_time: Submission time (int | str | float | None)
        expiry_time: Expiry time (int | str | float | None)
        rfq_quote_status: RFQ/Quote status (str)
        engine_timestamp: Engine timestamp (int | str | float | None)
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    rfq_id: str = Field(..., alias="R")
    quote_id: str = Field(..., alias="Q")
    client_quote_id: str | None = Field(None, alias="C")
    symbol: str = Field(..., alias="s")
    quantity: str = Field(..., alias="q")
    side: str | None = Field(None, alias="S")
    price: str | None = Field(None, alias="p")
    submission_time: int | str | float | None = Field(..., alias="w")
    expiry_time: int | str | float | None = Field(..., alias="W")
    rfq_quote_status: str = Field(..., alias="X")
    engine_timestamp: int | str | float | None = Field(..., alias="T")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
