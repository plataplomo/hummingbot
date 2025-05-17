"""
Backpack API Order, Order Book, and Order Update Models
------------------------------------------------------

This module defines strict Pydantic models for validating order, order book, and order update
responses from the Backpack Exchange API. These models are used for boundary validation and
transformation, not for internal business logic.

Models:
    - BackpackRawOrder: Validates REST/WebSocket order objects (all aliases, strict schema).
    - BackpackRawOrderBook: Validates order book snapshots (symbol, bids, asks, time).
    - BackpackRawOrderUpdate: Validates order update events from the WebSocket stream.

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.
    - Enum fields are strictly validated against allowed values.

These models act as a strict shield between external API data and internal business logic,
ensuring robustness and security at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Remove old utils imports if they become unused after refactoring
from .bp_common_raw_types import (
    RawBpExtendedOrderSideString,
    RawBpFlexibleTimestamp,
    RawBpNonEmptyStringMax32,
    RawBpNonEmptyStringMax64,
    RawBpOptionalFlexibleTimestamp,
    RawBpOptionalNonEmptyStringMax32,
    RawBpOptionalNonEmptyStringMax64,
    RawBpOptionalParsableFiniteDecimalString,
    RawBpOptionalStrictBool,
    RawBpOrderSideString,
    RawBpOrderStatusString,
    RawBpOrderTypeString,
    RawBpParsableFiniteDecimalString,
)


class BackpackRawOrder(BaseModel):
    """
    Pydantic model for a raw order object from `/api/v1/order`, `/api/v1/orders`,
    or WebSocket order update events.

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order payloads received from the exchange.
    Uses common raw types for field validation. The alias mapping logic is preserved.

    Attributes:
        clientId (str | None): Client-generated unique order ID (UUID).
        id (str): Exchange-provided order ID.
        relatedOrderId (str | None): ID of related order.
        symbol (str): Trading symbol.
        side (str): Order side ('buy', 'sell', 'Bid', 'Ask').
        orderType (str): Order type ('LIMIT', 'MARKET', etc.).
        status (str): Order status ('NEW', 'FILLED', etc.).
        quantity (str): Requested order quantity (parsable finite decimal string).
        executedQuantity (str | None): Total filled quantity (parsable finite decimal string).
        executedQuoteQuantity (str | None): Filled quote quantity (parsable finite decimal string).
        price (str | None): Limit price (parsable finite decimal string).
        triggerPrice (str | None): Stop/trigger price (parsable finite decimal string).
        avgFillPrice (str | None): Weighted average fill price (parsable finite decimal string).
        triggerBy (str | None): Reference price type for triggers.
        timeInForce (str | None): Time in force.
        reduceOnly (bool | None): Reduce-only flag.
        postOnly (bool | None): Post-only flag (REST only).
        selfTradePrevention (str | None): Self-trade prevention behavior.
        createdAt (int | float | str): Order creation time (UTC).
        updatedAt (int | float | str | None): Last update time.
        triggeredAt (int | float | str | None): Time the conditional order was triggered.
        expiryReason (str | None): Reason for expiry/cancellation.
        origin (str | None): Origin of the last update.
    """

    clientId: RawBpOptionalNonEmptyStringMax64 = Field(
        None, alias="clientId", description="Client-generated unique order ID (UUID). Alias: 'c'"
    )
    id: RawBpNonEmptyStringMax64 = Field(
        ..., alias="id", description="Exchange-provided order ID. Alias: 'i'"
    )
    relatedOrderId: RawBpOptionalNonEmptyStringMax64 = Field(
        None, alias="relatedOrderId", description="ID of related order. Alias: 'I'"
    )
    symbol: RawBpNonEmptyStringMax64 = Field(
        ..., alias="symbol", description="Trading symbol. Alias: 's'"
    )
    side: RawBpExtendedOrderSideString = Field(
        ..., alias="side", description="Order side ('buy', 'sell', 'Bid', 'Ask'). Alias: 'S'"
    )
    orderType: RawBpOrderTypeString = Field(
        ..., alias="orderType", description="Order type ('LIMIT', 'MARKET', etc.). Alias: 'o'"
    )
    status: RawBpOrderStatusString = Field(
        ..., alias="status", description="Order status ('NEW', 'FILLED', etc.). Alias: 'X'"
    )
    quantity: RawBpParsableFiniteDecimalString = Field(
        ..., alias="quantity", description="Requested order quantity. Alias: 'q'"
    )
    executedQuantity: RawBpOptionalParsableFiniteDecimalString = Field(
        None, alias="executedQuantity", description="Total filled quantity. Alias: 'z'"
    )
    executedQuoteQuantity: RawBpOptionalParsableFiniteDecimalString = Field(
        None, alias="executedQuoteQuantity", description="Filled quote quantity. Alias: 'Z'"
    )
    price: RawBpOptionalParsableFiniteDecimalString = Field(
        None, alias="price", description="Limit price. Alias: 'p'"
    )
    triggerPrice: RawBpOptionalParsableFiniteDecimalString = Field(
        None, alias="triggerPrice", description="Stop/trigger price. Alias: 'P'"
    )
    avgFillPrice: RawBpOptionalParsableFiniteDecimalString = Field(
        None, alias="avgFillPrice", description="Weighted average fill price. Alias: 'L'"
    )
    triggerBy: RawBpOptionalNonEmptyStringMax32 = Field(
        None, alias="triggerBy", description="Reference price type for triggers. Alias: 'B'"
    )
    timeInForce: RawBpOptionalNonEmptyStringMax32 = Field(
        None, alias="timeInForce", description="Time in force. Alias: 'f'"
    )
    reduceOnly: RawBpOptionalStrictBool = Field(
        None, alias="reduceOnly", description="Reduce-only flag. Alias: 'r'"
    )
    postOnly: RawBpOptionalStrictBool = Field(
        None, alias="postOnly", description="Post-only flag (REST only)"
    )
    selfTradePrevention: RawBpOptionalNonEmptyStringMax32 = Field(
        None, alias="selfTradePrevention", description="Self-trade prevention behavior. Alias: 'V'"
    )
    createdAt: RawBpFlexibleTimestamp = Field(
        ..., alias="createdAt", description="Order creation time (UTC). Aliases: 'E', 'T', 'time'"
    )
    updatedAt: RawBpOptionalFlexibleTimestamp = Field(
        None, alias="updatedAt", description="Last update time."
    )
    triggeredAt: RawBpOptionalFlexibleTimestamp = Field(
        None, alias="triggeredAt", description="Time the conditional order was triggered."
    )
    expiryReason: RawBpOptionalNonEmptyStringMax64 = Field(
        None, alias="expiryReason", description="Reason for expiry/cancellation. Alias: 'R'"
    )
    origin: RawBpOptionalNonEmptyStringMax64 = Field(
        None, alias="origin", description="Origin of the last update. Alias: 'O'"
    )

    model_config = ConfigDict(extra="forbid", validate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def support_all_aliases(cls, values: dict[str, object]) -> dict[str, object]:
        """
        Normalizes all supported field aliases to canonical field names before validation.
        Ensures compatibility with both REST and WebSocket payloads.
        """
        alias_map: dict[str, list[str]] = {
            "id": ["id", "i"],
            "clientId": ["clientId", "c"],
            "relatedOrderId": ["relatedOrderId", "I"],
            "symbol": ["symbol", "s"],
            "side": ["side", "S"],
            "orderType": ["orderType", "o"],
            "status": ["status", "X"],
            "quantity": ["quantity", "q"],
            "executedQuantity": ["executedQuantity", "z"],
            "executedQuoteQuantity": ["executedQuoteQuantity", "Z"],
            "price": ["price", "p"],
            "triggerPrice": ["triggerPrice", "P"],
            "avgFillPrice": ["avgFillPrice", "L"],
            "triggerBy": ["triggerBy", "B"],
            "timeInForce": ["timeInForce", "f"],
            "reduceOnly": ["reduceOnly", "r"],
            "selfTradePrevention": ["selfTradePrevention", "V"],
            "createdAt": ["createdAt", "E", "T", "time"],
            "expiryReason": ["expiryReason", "R"],
            "origin": ["origin", "O"],
            "postOnly": ["postOnly"],
        }
        current_values = dict(values)
        processed_values: dict[str, object] = {}

        for canonical_name, alias_list in alias_map.items():
            found_alias = False
            for alias in alias_list:
                if alias in current_values:
                    processed_values[canonical_name] = current_values.pop(alias)
                    found_alias = True
                    break
            if not found_alias and canonical_name in current_values:
                processed_values[canonical_name] = current_values.pop(canonical_name)

        for key, value in current_values.items():
            processed_values[key] = value

        return processed_values


class BackpackRawOrderBook(BaseModel):
    """
    Pydantic model for a raw order book snapshot from `/api/v1/depth` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order book payloads received from the exchange.
    Uses common raw types for field validation.

    Attributes:
        symbol (str): Trading symbol.
        bids (list[tuple[str, str]]): List of [price_str, quantity_str] for bids. Validated as
                                       parsable finite decimal strings.
        asks (list[tuple[str, str]]): List of [price_str, quantity_str] for asks. Validated as
                                       parsable finite decimal strings.
        time (int | str | float | None): Snapshot timestamp.
    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    bids: list[tuple[RawBpParsableFiniteDecimalString, RawBpParsableFiniteDecimalString]] = Field(
        ..., alias="bids"
    )
    asks: list[tuple[RawBpParsableFiniteDecimalString, RawBpParsableFiniteDecimalString]] = Field(
        ..., alias="asks"
    )
    time: RawBpOptionalFlexibleTimestamp = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawOrderUpdate(BaseModel):
    """
    Pydantic model for a raw order update event from the Backpack WebSocket stream (`orderUpdate`).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order update events received from the exchange.
    Uses common raw types for field validation.

    Attributes:
        event_type (str): Event type (e.g., 'orderAccepted', 'orderFill', ...).
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        client_order_id (str | None): Client order ID.
        side (str): Order side ('Bid', 'Ask').
        order_type (str): Order type ('LIMIT', 'MARKET', etc.).
        time_in_force (str | None): Time in force.
        quantity (str | None): Quantity (parsable finite decimal string).
        price (str | None): Price (parsable finite decimal string).
        order_status (str): Order state/status.
    """

    event_type: RawBpNonEmptyStringMax32 = Field(..., alias="e")
    event_time: RawBpOptionalFlexibleTimestamp = Field(..., alias="E")
    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="s")
    client_order_id: RawBpOptionalNonEmptyStringMax64 = Field(None, alias="c")
    side: RawBpOrderSideString = Field(..., alias="S")
    order_type: RawBpOrderTypeString = Field(..., alias="o")
    time_in_force: RawBpOptionalNonEmptyStringMax32 = Field(None, alias="f")
    quantity: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="q")
    price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="p")
    order_status: RawBpOrderStatusString = Field(..., alias="X")
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", frozen=True, validate_assignment=True
    )
