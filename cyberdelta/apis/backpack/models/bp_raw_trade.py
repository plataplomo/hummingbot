"""
Backpack API Trade Models
------------------------

This module defines strict Pydantic models for validating trade and trade event responses from the
Backpack Exchange API. These models are used for boundary validation and transformation, not for
internal business logic.

Models:
    - BackpackRawTrade: Validates REST trade/fill objects (id, order_id, symbol, price,
      quantity, time).
    - BackpackRawTradeEvent: Validates WebSocket trade event objects (event_type,
      event_time, symbol, price, quantity, buyer/seller order IDs, trade_id,
      engine_timestamp, is_buyer_the_maker).
    - BackpackRawFill: Validates fill records from the Backpack /wapi/v1/history/fills

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

import logging

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)

logger = logging.getLogger("cyberdelta.models.raw")


class BackpackRawTrade(BaseModel):
    """
    Pydantic model for a raw trade/fill from `/api/v1/trades` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse trade payloads received from the exchange.

    Attributes:
        id (str): Trade ID.
        order_id (str): Associated order ID.
        symbol (str): Trading symbol.
        price (str): Execution price (as string).
        quantity (str): Executed quantity (as string).
        time (int | str | float | None): Execution timestamp.
    """

    id: str = Field(..., alias="id", max_length=64)
    order_id: str = Field(..., alias="orderId", max_length=64)
    symbol: str = Field(..., alias="symbol", max_length=64)
    price: str = Field(..., alias="price", max_length=64)
    quantity: str = Field(..., alias="qty", max_length=64)
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("id", "order_id", "symbol", mode="before")
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 64:
            raise ValueError(f"{field_name}: String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string: {e}") from None
        return v

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal
        (max 64 chars).
        Raises ValueError if not a string, not parseable as decimal, not finite, or
        exceeds max length.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string: {e}") from None
        try:
            dec_val = parse_decimal_value(v, allow_none=False, field_name=field_name)
        except Exception as e:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value: {e}"
            ) from None
        if dec_val is None:
            raise ValueError(
                f"{field_name}: Parsing returned None unexpectedly for non-optional field."
            )
        if not dec_val.is_finite():
            raise ValueError(
                f"{field_name}: Input must be a finite number, got '{v}' (parsed as {dec_val})."
            )
        return v

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp_format(cls, v: object, info: ValidationInfo) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        field_name = info.field_name or "time"
        if v is None:
            raise ValueError(f"{field_name}: Value cannot be None.")
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
            try:
                # Try parsing as int or float
                if v.isdigit():
                    return int(v)
                return float(v)
            except Exception as err:
                # If not parseable as a number, treat as ISO8601 or raise
                try:
                    v.encode("utf-8", "strict")
                except UnicodeEncodeError as err2:
                    raise ValueError(f"{field_name}: String must be valid UTF-8: {err2}") from err2
                # Accept as string if it looks like ISO8601 (basic check)
                if ("T" in v or "-" in v or ":" in v) and any(c.isdigit() for c in v):
                    return v
                raise ValueError(f"{field_name}: Invalid timestamp format") from err
        raise ValueError(
            f"{field_name}: Invalid type {type(v)}, expected int, float, or ISO string"
        )


class BackpackRawTradeEvent(BaseModel):
    """
    Pydantic model for a raw trade event from the Backpack WebSocket stream (`trade`).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse trade event payloads received from the exchange.

    Attributes:
        event_type (str): Event type ('trade').
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        price (str): Price (as string).
        quantity (str): Quantity (as string).
        buyer_order_id (str): Buyer order ID.
        seller_order_id (str): Seller order ID.
        trade_id (str): Trade ID.
        engine_timestamp (int | str | float | None): Engine timestamp.
        is_buyer_the_maker (bool): Is buyer the maker?
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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_event_type_enum(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that event_type is a non-empty UTF-8 string and matches allowed values.
        Raises ValueError if not a string, is empty, not valid UTF-8, or not in allowed set.
        """
        field_name = info.field_name or "event_type"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        allowed_values = {"trade"}
        if v not in allowed_values:
            if v.lower() in allowed_values:
                logger.warning(
                    f"[{cls.__name__}] {field_name}: Received value '{v}' with incorrect case, "
                    f"expected one of {allowed_values}. Allowing but may indicate API change."
                )
            else:
                raise ValueError(
                    f"{field_name}: Invalid value '{v}'. Expected one of {allowed_values}"
                )
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string: {e}") from None
        return v

    @field_validator("symbol", "buyer_order_id", "seller_order_id", "trade_id", mode="before")
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
        if len(v) > 64:
            raise ValueError(f"{field_name}: String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string: {e}") from None
        return v

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal
        (max 64 chars).
        Raises ValueError if not a string, not parseable as decimal, not finite, or
        exceeds max length.
        """
        field_name = info.field_name or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as e:
            raise ValueError(f"{field_name}: Invalid UTF-8 sequence in string: {e}") from None
        try:
            dec_val = parse_decimal_value(v, allow_none=False, field_name=field_name)
        except Exception as e:
            raise ValueError(
                f"{field_name}: Must be a string representing a decimal value: {e}"
            ) from None
        if dec_val is None:
            raise ValueError(
                f"{field_name}: Parsing returned None unexpectedly for non-optional field."
            )
        if not dec_val.is_finite():
            raise ValueError(
                f"{field_name}: Input must be a finite number, got '{v}' (parsed as {dec_val})."
            )
        return v

    @field_validator("event_time", "engine_timestamp", mode="before")
    @classmethod
    def validate_timestamp_format(cls, v: object, info: ValidationInfo) -> int | float | str | None:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        field_name = info.field_name or "event_time"
        if v is None:
            raise ValueError(f"{field_name}: Value cannot be None.")
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError(f"{field_name}: Input string cannot be empty or just whitespace.")
            try:
                # Try parsing as int or float
                if v.isdigit():
                    return int(v)
                return float(v)
            except Exception as err:
                # If not parseable as a number, treat as ISO8601 or raise
                try:
                    v.encode("utf-8", "strict")
                except UnicodeEncodeError as err2:
                    raise ValueError(f"{field_name}: String must be valid UTF-8: {err2}") from err2
                # Accept as string if it looks like ISO8601 (basic check)
                if ("T" in v or "-" in v or ":" in v) and any(c.isdigit() for c in v):
                    return v
                raise ValueError(f"{field_name}: Invalid timestamp format") from err
        raise ValueError(
            f"{field_name}: Invalid type {type(v)}, expected int, float, or ISO string"
        )


# --- Raw Fill Model (from History) ---


class BackpackRawFill(BaseModel):
    """
    Raw Pydantic model for a single fill record from the Backpack /wapi/v1/history/fills endpoint.
    Corresponds to the OpenAPI schema OrderFill.
    Performs basic type validation and parsing for numeric/boolean fields.
    """

    fee: str = Field(..., description="The fee charged on the fill.")
    fee_symbol: str = Field(
        ..., alias="feeSymbol", description="The asset that is charged as a fee.", max_length=32
    )
    is_maker: bool = Field(
        ..., alias="isMaker", description="Whether the fill was made by the maker."
    )
    order_id: str = Field(
        ..., alias="orderId", description="The order ID of the fill.", max_length=128
    )
    price: str = Field(..., description="The price of the fill.")
    quantity: str = Field(..., description="The quantity of the fill.")
    side: str = Field(..., description="The side of the fill.")
    symbol: str = Field(..., description="The market symbol of the fill.", max_length=64)
    timestamp: str = Field(
        ..., description="The timestamp of the fill (UTC string, e.g., YYYY-MM-DDTHH:MM:SS.ffffffZ)"
    )
    trade_id: int = Field(..., alias="tradeId", description="The trade ID of the fill.", ge=0)
    client_id: str | None = Field(
        None, alias="clientId", description="Client id of the order.", max_length=128
    )
    # system_order_type: str | None = Field(None, alias="systemOrderType",
    #      description="Type of system order triggering fill") # Field exists but complex enum?

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",  # Changed from ignore/default
        frozen=True,
        # alias_generator=to_camel # Removed as aliases are explicit
    )

    # --- Field Validators --- #

    @field_validator("fee", "price", "quantity", mode="before")
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty string representing a finite decimal
        (max 64 chars).
        Raises ValueError if not a string, not parseable as decimal, not finite, or
        exceeds max length.
        """
        field_name = info.field_name or "decimal_field"
        # Validate as string first (reusing logic from bp_raw_trade validator)
        v_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        # Now validate parseability and finiteness
        try:
            dec_val = parse_decimal_value(v_str, allow_none=False, field_name=field_name)
        except ValueError as e:
            # Re-raise with context if parsing fails
            raise ValueError(f"{field_name}: String '{v_str}' not parseable as Decimal: {e}") from e
        if dec_val is None:
            # Should not happen with allow_none=False, but defensive check
            raise ValueError(f"{field_name}: Decimal parsing returned None unexpectedly.")
        if not dec_val.is_finite():
            raise ValueError(f"{field_name}: Must be a finite Decimal, got '{v_str}'.")
        return v_str  # Return the original valid string

    @field_validator("fee_symbol", "order_id", "symbol", mode="before")
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        """Validate required, non-empty string fields with specific max_lengths."""
        field_name = info.field_name or "unknown_field"  # Fallback for field name
        # Safely access max_length, provide default if metadata or max_length is missing
        max_len = getattr(
            getattr(cls.model_fields.get(field_name), "metadata", [None])[0], "max_length", 64
        )
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object, info: ValidationInfo) -> str:
        """Validate the 'side' field against allowed values ('Bid', 'Ask')."""
        # cyberdelta.utils.parsing: Use standard enum validator
        return validate_enum_field(v, allowed={"Bid", "Ask"}, field_name="side")

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp_format(cls, v: object, info: ValidationInfo) -> str:
        """Validate timestamp is a non-empty string and parseable as UTC datetime."""
        field_name = info.field_name or "timestamp"  # Fallback for field name
        # Validate it's a non-empty string first
        v_str = validate_str_field(v, field_name=field_name, allow_empty=False)
        # Attempt parsing to ensure format validity
        try:
            dt = parse_datetime_utc(v_str, field_name=field_name)
            if dt is None:
                raise ValueError("Timestamp parsing returned None unexpectedly.")
        except ValueError as e:
            raise ValueError(
                f"{field_name}: Invalid timestamp format or value '{v_str}': {e}"
            ) from e
        # Return original valid string
        return v_str

    @field_validator("trade_id", mode="before")
    @classmethod
    def validate_trade_id(cls, v: object, info: ValidationInfo) -> int:
        """Validate trade_id is a non-negative integer."""
        field_name = info.field_name or "trade_id"  # Fallback for field name
        if not isinstance(v, int):
            # Try converting if string
            if isinstance(v, str) and v.isdigit():
                v_int = int(v)
            else:
                raise TypeError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        else:
            v_int = v

        if v_int < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v_int}")
        return v_int

    @field_validator("is_maker", mode="before")
    @classmethod
    def validate_is_maker(cls, v: object, info: ValidationInfo) -> bool:
        """Validate is_maker is a boolean."""
        field_name = info.field_name or "is_maker"  # Fallback for field name
        if not isinstance(v, bool):
            # Allow common string representations? No, spec says boolean.
            raise TypeError(f"{field_name}: Must be a boolean, got {type(v).__name__}")
        return v

    @field_validator("client_id", mode="before")
    @classmethod
    def validate_optional_string(cls, v: object | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields: must be non-empty if present."""
        if v is None:
            return None
        field_name = info.field_name or "optional_field"  # Fallback for field name
        # Safely access max_length, provide default if metadata or max_length is missing
        max_len = getattr(
            getattr(cls.model_fields.get(field_name), "metadata", [None])[0], "max_length", 128
        )
        # If not None, validate as a non-empty string
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)
