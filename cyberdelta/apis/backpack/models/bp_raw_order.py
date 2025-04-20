"""
Backpack API Order Models
------------------------

Strict Pydantic models for validating order, order book, and order update responses from the
Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BackpackRawOrder(BaseModel):
    """
    Pydantic model for a raw order object from `/api/v1/order` or `/api/v1/orders`
    (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order payloads received from the exchange.

    Attributes:
        clientId (str | None): Client-generated unique order ID (UUID).
        id (str): Exchange-provided order ID.
        relatedOrderId (str | None): ID of related order (e.g., parent, trigger target).
        exchange (str | None): Name of the exchange (not present in Backpack, set in mapping).
        symbol (str): Trading symbol.
        side (str): Order side ('buy', 'sell', 'Bid', 'Ask').
        orderType (str): Order type ('LIMIT', 'MARKET', etc.).
        status (str): Order status ('NEW', 'FILLED', etc.).
        quantity (str): Requested order quantity.
        executedQuantity (str | None): Total filled quantity.
        executedQuoteQuantity (str | None): Filled quote quantity.
        price (str | None): Limit price.
        triggerPrice (str | None): Stop trigger price.
        avgFillPrice (str | None): Weighted average fill price.
        triggerBy (str | None): Reference price type for triggers.
        timeInForce (str | None): Time in force.
        reduceOnly (bool | None): Reduce-only flag.
        postOnly (bool | None): Post-only flag (not present in Backpack REST, set in mapping
            if needed).
        selfTradePrevention (str | None): Self-trade prevention behavior.
        createdAt (int | str | float | None): Order creation time (UTC).
        updatedAt (int | str | float | None): Last update time.
        triggeredAt (int | str | float | None): Time the conditional order was triggered.
        expiryReason (str | None): Reason for expiry/cancellation.
        origin (str | None): Origin of the last update.
        strategyName (str | None): Optional strategy identifier (not present in Backpack,
            set in mapping if needed).
        signalId (str | None): Optional signal identifier (not present in Backpack, set in
            mapping if needed).
        trades (list[Any] | None): List of associated trade fills (not present in Backpack
            order response).
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
        description=(
            "Optional strategy identifier (not present in Backpack, set in mapping if needed)."
        ),
    )
    signalId: str | None = Field(
        None,
        description=(
            "Optional signal identifier (not present in Backpack, set in mapping if needed)."
        ),
    )
    trades: list[Any] | None = Field(
        None,
        description="List of associated trade fills (not present in Backpack order response).",
    )
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawOrderBook(BaseModel):
    """
    Pydantic model for a raw order book snapshot from `/api/v1/depth` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order book payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        bids (list[list[str]]): List of [price, quantity] for bids.
        asks (list[list[str]]): List of [price, quantity] for asks.
        time (int | str | float | None): Snapshot timestamp.
    """

    symbol: str = Field(..., alias="symbol")
    bids: list[list[str]] = Field(..., alias="bids")
    asks: list[list[str]] = Field(..., alias="asks")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawOrderUpdate(BaseModel):
    """
    Pydantic model for a raw order update event from the Backpack WebSocket stream (`orderUpdate`).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order update events received from the exchange.

    Attributes:
        event_type (str): Event type (e.g., 'orderAccepted', 'orderFill', ...).
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        client_order_id (str | None): Client order ID.
        side (str): Order side ('Bid', 'Ask').
        order_type (str): Order type ('LIMIT', 'MARKET', etc.).
        time_in_force (str | None): Time in force.
        quantity (str | None): Quantity.
        price (str | None): Price.
        order_status (str): Order state/status.
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
