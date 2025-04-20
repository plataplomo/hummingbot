"""
Backpack API Order Models (Spec-Accurate, Full Alias & Validation)
------------------------

Strict Pydantic models for validating order, order book, and order update
responses from the Backpack Exchange API. These models are used for boundary
validation and transformation, not for internal business logic.

This version is fully aligned with the Backpack OpenAPI spec and supports all
REST and WebSocket field aliases, types, and validation requirements.
"""

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator


class BackpackRawOrder(BaseModel):
    """
    Pydantic model for a raw order object from `/api/v1/order`, `/api/v1/orders`,
    or WebSocket order update events.

    - All REST and WebSocket field names/aliases are supported.
    - Types and optionality are enforced per the Backpack OpenAPI spec.
    - No business logic or enum mapping is performed here.
    - All extra fields are forbidden.
    """

    clientId: str | None = Field(
        None, alias="clientId", description="Client-generated unique order ID (UUID). Alias: 'c'"
    )
    id: str = Field(..., alias="id", description="Exchange-provided order ID. Alias: 'i'")
    relatedOrderId: str | None = Field(
        None, alias="relatedOrderId", description="ID of related order. Alias: 'I'"
    )
    symbol: str = Field(..., alias="symbol", description="Trading symbol. Alias: 's'")
    side: str = Field(
        ..., alias="side", description="Order side ('buy', 'sell', 'Bid', 'Ask'). Alias: 'S'"
    )
    orderType: str = Field(
        ..., alias="orderType", description="Order type ('LIMIT', 'MARKET', etc.). Alias: 'o'"
    )
    status: str = Field(
        ..., alias="status", description="Order status ('NEW', 'FILLED', etc.). Alias: 'X'"
    )
    quantity: str = Field(..., alias="quantity", description="Requested order quantity. Alias: 'q'")
    executedQuantity: str | None = Field(
        None, alias="executedQuantity", description="Total filled quantity. Alias: 'z'"
    )
    executedQuoteQuantity: str | None = Field(
        None, alias="executedQuoteQuantity", description="Filled quote quantity. Alias: 'Z'"
    )
    price: str | None = Field(None, alias="price", description="Limit price. Alias: 'p'")
    triggerPrice: str | None = Field(
        None, alias="triggerPrice", description="Stop/trigger price. Alias: 'P'"
    )
    avgFillPrice: str | None = Field(
        None, alias="avgFillPrice", description="Weighted average fill price. Alias: 'L'"
    )
    triggerBy: str | None = Field(
        None, alias="triggerBy", description="Reference price type for triggers. Alias: 'B'"
    )
    timeInForce: str | None = Field(
        None, alias="timeInForce", description="Time in force. Alias: 'f'"
    )
    reduceOnly: bool | None = Field(
        None, alias="reduceOnly", description="Reduce-only flag. Alias: 'r'"
    )
    postOnly: bool | None = Field(None, alias="postOnly", description="Post-only flag (REST only)")
    selfTradePrevention: str | None = Field(
        None, alias="selfTradePrevention", description="Self-trade prevention behavior. Alias: 'V'"
    )
    createdAt: int | float | str = Field(
        ..., alias="createdAt", description="Order creation time (UTC). Aliases: 'E', 'T', 'time'"
    )
    updatedAt: int | float | str | None = Field(
        None, alias="updatedAt", description="Last update time."
    )
    triggeredAt: int | float | str | None = Field(
        None, alias="triggeredAt", description="Time the conditional order was triggered."
    )
    expiryReason: str | None = Field(
        None, alias="expiryReason", description="Reason for expiry/cancellation. Alias: 'R'"
    )
    origin: str | None = Field(
        None, alias="origin", description="Origin of the last update. Alias: 'O'"
    )

    class Config:
        extra = "forbid"
        allow_population_by_field_name = True

    @model_validator(mode="before")
    @classmethod
    def support_all_aliases(cls, values: dict[str, object]) -> dict[str, object]:
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
        }
        for field, aliases in alias_map.items():
            for a in aliases:
                if a in values:
                    values[field] = values[a]
                    break
        return values

    @field_validator(
        "quantity",
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        mode="before",
    )
    @classmethod
    def validate_decimal_str(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is None:
            return v
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        allowed = {"buy", "sell", "Bid", "Ask"}
        if v not in allowed:
            raise ValueError(f"Invalid side: {v}")
        return v

    @field_validator("orderType", mode="before")
    @classmethod
    def validate_order_type_enum(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        allowed = {"LIMIT", "MARKET", "STOP", "TRAILING_STOP", "TAKE_PROFIT"}
        if v not in allowed:
            raise ValueError(f"Invalid orderType: {v}")
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        allowed = {
            "NEW",
            "FILLED",
            "CANCELLED",
            "EXPIRED",
            "REJECTED",
            "PARTIALLY_FILLED",
        }
        if v not in allowed:
            raise ValueError(f"Invalid status: {v}")
        return v

    @field_validator("symbol", "id", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str, info: ValidationInfo) -> str:
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("createdAt", "updatedAt", "triggeredAt", mode="before")
    @classmethod
    def validate_timestamp(cls, v: int | float | str | None) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, (int, float)):
            return v
        if v.isdigit():
            return int(v)
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_string_and_enum(cls, v: str, info: ValidationInfo) -> str:
        field_name = info.field_name or "status"
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        allowed_values = {
            "NEW",
            "PARTIALLY_FILLED",
            "FILLED",
            "CANCELED",
            "REJECTED",
            "EXPIRED",
            "OPEN",
            "FAILED",
            "UNKNOWN",
        }
        if v not in allowed_values:
            raise ValueError(f"{field_name}: Invalid value '{v}'. Expected one of {allowed_values}")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string '{v}': {e}") from e
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_string_and_enum(cls, v: str, info: ValidationInfo) -> str:
        """
        Validates that side is a non-empty string and a valid enum value.
        Args:
            v: The value to validate (should be a string).
            info: Pydantic ValidationInfo for context.
        Returns:
            The validated string value.
        Raises:
            ValueError: If the value is not a valid enum value.
        """
        field_name = info.field_name or "side"
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        allowed_values = {"buy", "sell", "Bid", "Ask"}
        if v not in allowed_values:
            raise ValueError(f"{field_name}: Invalid value '{v}'. Expected one of {allowed_values}")
        try:
            v.encode("utf-8", "strict")
        except Exception as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string '{v}': {e}") from e
        return v

    @field_validator("orderType", mode="before")
    @classmethod
    def validate_order_type_string_and_enum(cls, v: str, info: ValidationInfo) -> str:
        """
        Validates that orderType is a non-empty string and a valid enum value.
        Args:
            v: The value to validate (should be a string).
            info: Pydantic ValidationInfo for context.
        Returns:
            The validated string value.
        Raises:
            ValueError: If the value is not a valid enum value.
        """
        field_name = info.field_name or "orderType"
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        allowed_values = {"LIMIT", "MARKET", "STOP", "TRAILING_STOP", "TAKE_PROFIT"}
        if v not in allowed_values:
            raise ValueError(f"{field_name}: Invalid value '{v}'. Expected one of {allowed_values}")
        try:
            v.encode("utf-8", "strict")
        except Exception as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string '{v}': {e}") from e
        return v

    @field_validator(
        "quantity",
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        mode="before",
    )
    @classmethod
    def validate_decimal_string_format(cls, v: str, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Args:
            v: The value to validate (should be a string).
            info: Pydantic ValidationInfo for context.
        Returns:
            The validated string value.
        Raises:
            ValueError: If the value is not a valid, finite decimal string.
        """
        field_name = info.field_name or "field"
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        dec_val = parse_decimal_value(v, allow_none=False, field_name=field_name)
        assert dec_val is not None  # For type checkers; guaranteed by allow_none=False
        if not dec_val.is_finite():
            raise ValueError(
                f"{field_name}: Input must be a finite number, got '{v}' (parsed as {dec_val})."
            )
        return v

    @field_validator("symbol", "id", mode="before")
    @classmethod
    def validate_required_string(cls, v: str, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string of max 64 chars and valid UTF-8.
        Args:
            v: The value to validate (should be a string).
            info: Pydantic ValidationInfo for context.
        Returns:
            The validated string value.
        Raises:
            ValueError: If the value is not a valid string or exceeds max length.
        """
        field_name = info.field_name or "field"
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        max_len = 64
        if len(v) > max_len:
            raise ValueError(f"{field_name}: String value too long (max {max_len} chars)")
        try:
            v.encode("utf-8", "strict")
        except Exception as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string '{v}': {e}") from e
        return v

    @field_validator("createdAt", "updatedAt", "triggeredAt", mode="before")
    @classmethod
    def validate_timestamp_format(
        cls, v: int | float | str | None, info: ValidationInfo
    ) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
        return v


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

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: Any) -> str:
        # This check is required for runtime safety with Pydantic 'before' validators.
        if not isinstance(v, str):
            raise ValueError("Must be a string")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        return v

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def validate_bids_asks(cls, v: list[list[str]]) -> list[list[str]]:
        for entry in v:
            if not (len(entry) == 2 and all(x for x in entry)):
                raise ValueError("Each bid/ask must be a [str, str] pair")
        return v

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: int | float | str | None) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, int | float):
            return v
        # v is str by type hint if not int or float
        if not v.strip():
            raise ValueError("Timestamp string cannot be empty")
        return v


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

    @field_validator("event_type", "symbol", "side", "order_type", "order_status", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: str | None) -> str | None:
        allowed = {"Bid", "Ask"}  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid side: {v}")
        return v

    @field_validator("order_type", mode="before")
    @classmethod
    def validate_order_type_enum(cls, v: str | None) -> str | None:
        allowed = {"LIMIT", "MARKET", "STOP", "TRAILING_STOP", "TAKE_PROFIT"}  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid order_type: {v}")
        return v

    @field_validator("order_status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: str | None) -> str | None:
        allowed = {
            "NEW",
            "FILLED",
            "CANCELLED",
            "EXPIRED",
            "REJECTED",
            "PARTIALLY_FILLED",
        }  # Update as per spec
        if v is None or v not in allowed:
            raise ValueError(f"Invalid order_status: {v}")
        return v

    @field_validator("quantity", "price", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if type(v) is not str:
            raise ValueError("Must be a string representing a decimal value")
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: int | float | str | None) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, int | float):
            return v
        # v is str by type hint if not int or float
        if not v.strip():
            raise ValueError("Timestamp string cannot be empty")
        return v


# Utility for decimal parsing


def parse_decimal_value(
    v: str | None, allow_none: bool = False, field_name: str = "field"
) -> Decimal | None:
    """
    Parses a string as a Decimal. If allow_none is True, returns None for None input.
    Otherwise, always returns Decimal or raises ValueError.
    Never returns None if allow_none is False.

    Args:
        v: The value to parse (string or None).
        allow_none: If True, allows None and returns None. If False, raises on None.
        field_name: Name of the field for error messages.
    Returns:
        Decimal or None (if allow_none and v is None).
    Raises:
        ValueError: If input is invalid or cannot be parsed as Decimal.
    """
    if v is None:
        if allow_none:
            return None
        raise ValueError(f"{field_name}: Value is required and cannot be None")
    if not v.strip():
        raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
    try:
        dec_val = Decimal(v)
    except Exception as e:
        raise ValueError(f"{field_name}: Could not parse decimal from '{v}': {e}") from e
    return dec_val


def parse_datetime_utc(v: int | float | str, field_name: str = "field") -> int | float | str:
    """
    Accepts int, float, or non-empty string as a timestamp. Raises ValueError otherwise.

    Args:
        v: The value to parse (int, float, or str).
        field_name: Name of the field for error messages.
    Returns:
        The parsed timestamp value.
    Raises:
        ValueError: If input is not a valid timestamp type.
    """
    if isinstance(v, int | float):
        return v
    # v is str by type hint if not int or float
    if not v.strip():
        raise ValueError(f"{field_name}: Timestamp string cannot be empty")
    return v
