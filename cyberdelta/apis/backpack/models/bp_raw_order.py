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

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


class BackpackRawOrder(BaseModel):
    """
    Pydantic model for a raw order object from `/api/v1/order`, `/api/v1/orders`,
    or WebSocket order update events.

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse order payloads received from the exchange.

    Attributes:
        clientId (str | None): Client-generated unique order ID (UUID).
        id (str): Exchange-provided order ID.
        relatedOrderId (str | None): ID of related order.
        symbol (str): Trading symbol.
        side (str): Order side ('buy', 'sell', 'Bid', 'Ask').
        orderType (str): Order type ('LIMIT', 'MARKET', etc.).
        status (str): Order status ('NEW', 'FILLED', etc.).
        quantity (str): Requested order quantity.
        executedQuantity (str | None): Total filled quantity.
        executedQuoteQuantity (str | None): Filled quote quantity.
        price (str | None): Limit price.
        triggerPrice (str | None): Stop/trigger price.
        avgFillPrice (str | None): Weighted average fill price.
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
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
        field_name = info.field_name or "field"
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
        s = validate_str_field(v, field_name=field_name)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that side is a non-empty string and a valid enum value.
        Raises ValueError if not a string or not in allowed set.
        """
        field_name = info.field_name or "side"
        if v is None or not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
        s = validate_str_field(v, field_name=field_name)
        return validate_enum_field(s, allowed={"buy", "sell", "Bid", "Ask"}, field_name=field_name)

    @field_validator("orderType", mode="before")
    @classmethod
    def validate_order_type_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that orderType is a non-empty string and a valid enum value.
        Raises ValueError if not a string or not in allowed set.
        """
        field_name = info.field_name or "orderType"
        if v is None or not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
        s = validate_str_field(v, field_name=field_name)
        return validate_enum_field(
            s,
            allowed={"LIMIT", "MARKET", "STOP", "TRAILING_STOP", "TAKE_PROFIT"},
            field_name=field_name,
        )

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that status is a non-empty string and a valid enum value.
        Raises ValueError if not a string or not in allowed set.
        """
        field_name = info.field_name or "status"
        if v is None or not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
        s = validate_str_field(v, field_name=field_name)
        return validate_enum_field(
            s,
            allowed={
                "NEW",
                "FILLED",
                "CANCELLED",
                "EXPIRED",
                "REJECTED",
                "PARTIALLY_FILLED",
            },
            field_name=field_name,
        )

    @field_validator("symbol", "id", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "field"
        if v is None or not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
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
    def validate_timestamp(cls, v: object) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        if v is None:
            return None
        if isinstance(v, int | float):
            return v
        if not isinstance(v, str):
            raise ValueError(f"timestamp: Expected string, int, or float, got {type(v).__name__}")
        s = v.strip()
        if not s:
            raise ValueError("timestamp: Input string cannot be empty or just whitespace.")
        # Accept only all-digit (int), or ISO8601-like (must contain T, and at least one digit)
        if s.isdigit():
            return int(s)
        if ("T" in s or "-" in s or ":" in s) and any(c.isdigit() for c in s):
            # Very basic ISO8601 check: must contain at least one digit and a separator
            return s
        raise ValueError(f"timestamp: Invalid timestamp string '{v}' (not numeric or ISO8601)")

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_string_and_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that status is a non-empty string and a valid enum value, and valid UTF-8.
        Raises ValueError if not a string, is empty, not in allowed set, or not valid UTF-8.
        """
        field_name = info.field_name or "status"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
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
    def validate_side_string_and_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that side is a non-empty string and a valid enum value, and valid UTF-8.
        Raises ValueError if not a string, is empty, not in allowed set, or not valid UTF-8.
        """
        field_name = info.field_name or "side"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
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
    def validate_order_type_string_and_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that orderType is a non-empty string and a valid enum value, and valid UTF-8.
        Raises ValueError if not a string, is empty, not in allowed set, or not valid UTF-8.
        """
        field_name = info.field_name or "orderType"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
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
        "executedQuantity",
        "executedQuoteQuantity",
        "price",
        "triggerPrice",
        "avgFillPrice",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def validate_optional_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates that the value is a non-empty string representing a finite decimal, or None.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
        field_name = info.field_name or "field"
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
        s = validate_str_field(v, field_name=field_name)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("symbol", "id", mode="before")
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string of max 64 chars and valid UTF-8.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Expected string, got {type(v).__name__}")
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
    def validate_timestamp_format(cls, v: object, info: ValidationInfo) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        if v is None:
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
        if isinstance(v, int | float):
            return v
        if not isinstance(v, str):
            raise ValueError(f"timestamp: Expected string, int, or float, got {type(v).__name__}")
        if not v.strip():
            raise ValueError("timestamp: Input string cannot be empty or just whitespace.")
        if "T" in v or "-" in v or ":" in v:
            return v
        raise ValueError(f"timestamp: Invalid timestamp string '{v}' (not numeric or ISO8601)")


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
    def validate_non_empty_str(cls, v: object) -> str:
        """
        Validates that the symbol is a non-empty string.
        Raises ValueError if not a string or is empty.
        """
        if v is None or not isinstance(v, str):
            raise ValueError("symbol: Must be a string")
        if not v.strip():
            raise ValueError("symbol: Must be a non-empty string")
        return v

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def validate_bids_asks(cls, v: object, info: ValidationInfo) -> list[list[str]]:
        """
        Validates that bids/asks are lists of [str, str] pairs representing price and quantity.
        Raises ValueError if not a list, not pairs, or not valid decimals.
        """
        field_name = info.field_name or "bids/asks"
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Expected list, got {type(v).__name__}")

        v_list_any: list[Any] = v  # Explicitly type v as list[Any] after the check

        validated_list: list[list[str]] = []
        for i, entry_raw_item in enumerate(v_list_any):  # entry_raw_item is now Any
            current_entry_field_name = f"{field_name}[{i}]"
            if not isinstance(entry_raw_item, list):  # entry_raw_item is now known to be list
                raise ValueError(
                    f"{current_entry_field_name}: Expected list for entry, "
                    f"got {type(entry_raw_item).__name__}"
                )

            current_price_quantity_list: list[Any] = entry_raw_item  # Explicitly list[Any]

            entry_pair_validated: list[
                str
            ] = []  # This will hold the validated [price_str, quantity_str]
            if len(current_price_quantity_list) != 2:
                raise ValueError(
                    f"{current_entry_field_name}: Expected list of 2 items (price, quantity), "
                    f"got {len(current_price_quantity_list)}"
                )

            price_obj: Any = current_price_quantity_list[0]
            quantity_obj: Any = current_price_quantity_list[1]

            # Validate price string
            if not isinstance(price_obj, str):
                raise ValueError(
                    f"{current_entry_field_name}[0]: Price must be a string, "
                    f"got {type(price_obj).__name__}"
                )
            price_str: str = price_obj  # Now known to be str
            parse_decimal_value(price_str, field_name=f"{current_entry_field_name}[0] Price")
            entry_pair_validated.append(price_str)

            # Validate quantity string
            if not isinstance(quantity_obj, str):
                raise ValueError(
                    f"{current_entry_field_name}[1]: Quantity must be a string, "
                    f"got {type(quantity_obj).__name__}"
                )
            quantity_str: str = quantity_obj  # Now known to be str
            parse_decimal_value(quantity_str, field_name=f"{current_entry_field_name}[1] Quantity")
            entry_pair_validated.append(quantity_str)

            validated_list.append(entry_pair_validated)
        return validated_list

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        if v is None:
            return None
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("timestamp: Input string cannot be empty or just whitespace.")
            if v.isdigit():
                return int(v)
            if "T" in v or "-" in v or ":" in v:
                return v
            raise ValueError(f"timestamp: Invalid timestamp string '{v}' (not numeric or ISO8601)")
        raise ValueError(f"timestamp: Invalid type {type(v)}, expected int, float, or ISO string")


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
    def validate_non_empty_str(cls, v: object) -> str:
        """
        Validates that the value is a non-empty string.
        Raises ValueError if not a string or is empty.
        """
        if v is None or not isinstance(v, str):
            raise ValueError("event_type/symbol/side/order_type/order_status: Must be a string")
        if not v.strip():
            raise ValueError(
                "event_type/symbol/side/order_type/order_status: Must be a non-empty string"
            )
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object) -> str:
        """
        Validates that side is a string and one of the allowed enum values.
        Raises ValueError if not a string or not in allowed set.
        """
        if v is None or not isinstance(v, str):
            raise ValueError("side: Must be a string")
        allowed = {"Bid", "Ask"}  # Update as per spec
        if v not in allowed:
            raise ValueError(f"Invalid side: {v}")
        return v

    @field_validator("order_type", mode="before")
    @classmethod
    def validate_order_type_enum(cls, v: object) -> str:
        """
        Validates that order_type is a string and one of the allowed enum values.
        Raises ValueError if not a string or not in allowed set.
        """
        if v is None or not isinstance(v, str):
            raise ValueError("order_type: Must be a string")
        allowed = {"LIMIT", "MARKET", "STOP", "TRAILING_STOP", "TAKE_PROFIT"}  # Update as per spec
        if v not in allowed:
            raise ValueError(f"Invalid order_type: {v}")
        return v

    @field_validator("order_status", mode="before")
    @classmethod
    def validate_status_enum(cls, v: object) -> str:
        """
        Validates that order_status is a string and one of the allowed enum values.
        Raises ValueError if not a string or not in allowed set.
        """
        if v is None or not isinstance(v, str):
            raise ValueError("order_status: Must be a string")
        allowed = {
            "NEW",
            "FILLED",
            "CANCELLED",
            "EXPIRED",
            "REJECTED",
            "PARTIALLY_FILLED",
        }  # Update as per spec
        if v not in allowed:
            raise ValueError(f"Invalid order_status: {v}")
        return v

    @field_validator("quantity", "price", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object) -> str | None:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError("quantity/price: Must be a string")
        if not v.strip():
            raise ValueError(
                "quantity/price: Must be a non-empty string representing a decimal value"
            )
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError(
                "quantity/price: Must be a string representing a decimal value"
            ) from err
        if not dec_val.is_finite():
            raise ValueError("quantity/price: Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        if v is None:
            return None
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("event_time: Input string cannot be empty or just whitespace.")
            if v.isdigit():
                return int(v)
            if "T" in v or "-" in v or ":" in v:
                return v
            raise ValueError(f"event_time: Invalid timestamp string '{v}' (not numeric or ISO8601)")
        raise ValueError(f"event_time: Invalid type {type(v)}, expected int, float, or ISO string")
