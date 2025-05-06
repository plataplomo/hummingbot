"""
Backpack API Market, Ticker, and Open Interest Models
----------------------------------------------------

This module defines strict Pydantic models for validating market metadata, ticker, and open
interest responses from the Backpack Exchange API. These models are used for boundary validation
and transformation, not for internal business logic.

Models:
    - BackpackRawMarket: Validates market metadata (symbol, base/quote asset).
    - BackpackRawTicker: Validates ticker data (symbol, price, bid, ask, volume, time).
    - BackpackRawOpenInterest: Validates open interest data (symbol, open interest).
    - BackpackRawBookLevel: Validates a single price level in the raw order book response.
    - BackpackRawOrderBook: Validates the raw order book depth object.

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

import logging
import math
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_str_field,
)

# Get logger for the module
logger = logging.getLogger(__name__)


class BackpackRawMarket(BaseModel):
    """
    Pydantic model for a raw market metadata object from `/api/v1/markets` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse market metadata payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        base_asset (str): Base asset symbol.
        quote_asset (str): Quote asset symbol.
    """

    symbol: str = Field(..., alias="symbol", max_length=64)
    base_asset: str = Field(..., alias="baseAsset", max_length=64)
    quote_asset: str = Field(..., alias="quoteAsset", max_length=64)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", "base_asset", "quote_asset", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = getattr(info, "field_name", None) or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)


class BackpackRawTicker(BaseModel):
    """
    Pydantic model for a raw ticker object from `/api/v1/ticker` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse ticker payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        price (str | None): Last traded price (as string, optional).
        bid (str | None): Best bid price (as string, optional).
        ask (str | None): Best ask price (as string, optional).
        volume (str | None): 24h trading volume (as string, optional).
        time (int | str | float | None): Ticker timestamp.
    """

    symbol: str = Field(..., alias="symbol")
    price: str | None = Field(None, alias="price")
    bid: str | None = Field(None, alias="bid")
    ask: str | None = Field(None, alias="ask")
    volume: str | None = Field(None, alias="volume")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="symbol", max_length=64)

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        field_name = getattr(info, "field_name", None) or "field"
        if v is None:
            return v
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=True, field_name=field_name)
        if d is not None and not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object) -> int | float | str:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
        """
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("String must be non-empty")
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
                    raise ValueError(f"String must be valid UTF-8: {err2}") from err2
                # Accept as string if it looks like ISO8601 (basic check)
                if ("T" in v or "-" in v or ":" in v) and any(c.isdigit() for c in v):
                    return v
                raise ValueError("Invalid timestamp format") from err
        raise ValueError("Invalid timestamp format")


class BackpackRawOpenInterest(BaseModel):
    """
    Pydantic model for a raw open interest object from `/api/v1/openInterest` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse open interest payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        open_interest (str): Open interest (as string).
    """

    symbol: str = Field(..., alias="symbol")
    open_interest: str = Field(..., alias="openInterest")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", "open_interest", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = getattr(info, "field_name", None) or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("open_interest", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = getattr(info, "field_name", None) or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


class BackpackRawOrderBook(BaseModel):
    """
    Pydantic model for the raw order book depth object from `/api/v1/depth` (Backpack REST API).

    This model mirrors the Backpack OpenAPI Depth schema exactly, enforcing strict field validation.
    Use this model to validate and parse depth payloads received from the exchange.

    Attributes:
        asks (List[Tuple[str, str]]): List of asks [price, quantity].
        bids (List[Tuple[str, str]]): List of bids [price, quantity].
        last_update_id (str): ID of the last update that changed the book.
        timestamp (int): Matching engine timestamp in microseconds.
    """

    asks: list[tuple[str, str]] = Field(..., alias="asks")
    bids: list[tuple[str, str]] = Field(..., alias="bids")
    last_update_id: str = Field(..., alias="lastUpdateId")
    timestamp: int = Field(..., alias="timestamp")
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True
    )

    @field_validator("asks", "bids", mode="before")
    @classmethod
    def validate_levels(cls, v: object, info: ValidationInfo) -> list[tuple[str, str]]:
        """Validates bids/asks are lists of [price_str, quantity_str] pairs.

        Ensures:
            - Input `v` is a list.
            - Each item in `v` is a list/tuple of exactly 2 elements.
            - Both elements are non-empty strings (max_length=64).
            - Both elements parse to finite Decimals.
            - Quantity string parses to a non-negative Decimal.

        Returns:
            List[Tuple[str, str]]: The validated list of string pairs.

        Raises:
            ValueError: If validation fails at any step.
        """
        field_name = info.field_name
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Must be a list of [price_str, quantity_str] pairs.")

        raw_list: list[Any] = v
        validated_levels: list[tuple[str, str]] = []

        for i, item_raw in enumerate(raw_list):
            current_item_desc = f"{field_name}[{i}]"
            # Check item structure
            # Use Any type hint for item_raw and rely on runtime checks
            item: Any = item_raw
            if not isinstance(item, list | tuple):
                raise ValueError(f"{current_item_desc}: Item is not a list or tuple.")

            # DEFENSIVE CHECK: Runtime check ensures item is sized.
            # Pyright=[arg-type] reports unknown type due to item: Any.
            # Mypy=[unused-ignore] flags this ignore as unused.
            if len(item) != 2:  # type: ignore[arg-type]
                raise ValueError(
                    f"{current_item_desc}: Must be a list/tuple of length 2 "
                    f"[price_str, quantity_str]."
                )

            # Runtime checks above ensure item is indexable
            price_raw: Any = item[0]
            quantity_raw: Any = item[1]

            # Validate Price String
            try:
                price_str = validate_str_field(
                    price_raw,
                    field_name=f"{current_item_desc}[0](price)",
                    max_length=64,
                    allow_empty=False,
                )
                price_dec = parse_decimal_value(price_str, allow_none=False)
                # Add explicit check for None before is_finite
                if price_dec is None:
                    raise ValueError("Price parsing unexpectedly returned None.")
                if not price_dec.is_finite():
                    raise ValueError("Price must be finite.")
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"{current_item_desc}[0](price): Invalid finite decimal string "
                    f"'{price_raw}'. {e}"
                ) from e

            # Validate Quantity String
            try:
                quantity_str = validate_str_field(
                    quantity_raw,
                    field_name=f"{current_item_desc}[1](quantity)",
                    max_length=64,
                    allow_empty=False,
                )
                quantity_dec = parse_decimal_value(quantity_str, allow_none=False)
                # Add explicit check for None before is_finite and comparison
                if quantity_dec is None:
                    raise ValueError("Quantity parsing unexpectedly returned None.")
                if not quantity_dec.is_finite() or quantity_dec < 0:
                    raise ValueError("Quantity must be finite and non-negative.")
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"{current_item_desc}[1](quantity): Invalid non-negative finite "
                    f"decimal string '{quantity_raw}'. {e}"
                ) from e

            validated_levels.append((price_str, quantity_str))
        return validated_levels

    @field_validator("last_update_id", mode="before")
    @classmethod
    def validate_last_update_id(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="last_update_id", max_length=64)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object, info: ValidationInfo) -> int:
        """Validate timestamp is strictly an integer."""
        if not isinstance(v, int):
            raise ValueError("timestamp: Must be an integer (microseconds)")
        return v


# --- Raw WebSocket Event Models ---


class BackpackRawTickerEvent(BaseModel):
    """
    Raw Pydantic model for a WebSocket ticker update event (`ticker.<symbol>`).
    Includes comprehensive validation.
    """

    # Fields confirmed from limited observation/comparison to REST Ticker
    # Assume required unless seen otherwise in streams
    symbol: str = Field(..., alias="s", max_length=64)
    last_price: str = Field(..., alias="lastPrice", max_length=64)
    high: str = Field(..., alias="high", max_length=64)
    low: str = Field(..., alias="low", max_length=64)
    volume: str = Field(..., alias="volume", max_length=64)
    quote_volume: str = Field(..., alias="quoteVolume", max_length=64)
    price_change_percent: str = Field(..., alias="priceChangePercent", max_length=64)
    # Assuming event time (E) and event type (e) might be present based on other streams
    event_type: str | None = Field(None, alias="e", max_length=32)
    event_time: int | float | str | None = Field(None, alias="E")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        frozen=True,
        validate_assignment=True,
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string, max 64 chars."""
        field_name = info.field_name or "symbol"
        return validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)

    @field_validator(
        "last_price", "high", "low", "volume", "quote_volume", "price_change_percent", mode="before"
    )
    @classmethod
    def validate_required_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate required fields are non-empty, finite decimal strings."""
        field_name = info.field_name or "decimal_field"
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        # Defensive check for None before is_finite()
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("event_type", mode="before")
    @classmethod
    def validate_optional_event_type(cls, v: object | None, info: ValidationInfo) -> str | None:
        """Validate optional event_type string (non-empty if present). Maximum length 32."""
        if v is None:
            return None
        field_name = info.field_name or "event_type"
        # Raw model: Just validate non-empty string, max length. Don't enforce specific enum here.
        return validate_str_field(v, field_name=field_name, max_length=32, allow_empty=False)

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object | None, info: ValidationInfo) -> int | float | str | None:
        """Validate optional event_time (int, float, or parsable string)."""
        if v is None:
            return None
        field_name = info.field_name or "event_time"

        if isinstance(v, int | float):
            if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
                raise ValueError(f"{field_name}: Numeric timestamp must be finite")
            return v
        elif isinstance(v, str):
            s = validate_str_field(v, field_name=field_name, allow_empty=False)
            try:
                if s.isdigit():
                    return int(s)
                val_float = float(s)
                if math.isinf(val_float) or math.isnan(val_float):
                    raise ValueError(f"{field_name}: Numeric timestamp string must be finite")
                return val_float
            except ValueError:
                try:
                    _ = parse_datetime_utc(s, field_name=field_name)
                    return s
                except ValueError as e:
                    raise ValueError(
                        f"{field_name}: String timestamp '{s}' is not a valid number "
                        f"or ISO-like format: {e}"
                    ) from e
        else:
            raise ValueError(
                f"{field_name}: Invalid type {type(v)}, expected int, float, or string"
            )


class BackpackRawDepthUpdateEvent(BaseModel):
    """
    Raw Pydantic model for a WebSocket depth update event (`depth.<symbol>`).
    Includes comprehensive validation for levels.
    """

    last_update_id: str = Field(..., alias="lastUpdateId", max_length=64)
    bids: list[tuple[str, str]] = Field(..., alias="bids")
    asks: list[tuple[str, str]] = Field(..., alias="asks")
    event_type: str | None = Field(None, alias="e", max_length=32)
    event_time: int | float | str | None = Field(None, alias="E")

    model_config = ConfigDict(
        populate_by_name=True, extra="ignore", frozen=True, validate_by_name=True
    )

    @field_validator("asks", "bids", mode="before")
    @classmethod
    def validate_levels(cls, v: object, info: ValidationInfo) -> list[tuple[str, str]]:
        """Validates bids/asks are lists of [price_str, quantity_str] pairs.

        Reuses the robust validation logic from BackpackRawOrderBook.
        Ensures finite prices and non-negative finite quantities.
        """
        field_name = info.field_name or "levels"
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Must be a list of [price_str, quantity_str] pairs.")

        raw_list: list[Any] = v
        validated_levels: list[tuple[str, str]] = []

        for i, item_raw in enumerate(raw_list):
            current_item_desc = f"{field_name}[{i}]"
            # Check item structure
            # Use Any type hint for item_raw and rely on runtime checks
            item: Any = item_raw
            if not isinstance(item, list | tuple):
                raise ValueError(f"{current_item_desc}: Item is not a list or tuple.")

            # DEFENSIVE CHECK: Runtime check ensures item is sized.
            # Pyright=[arg-type] reports unknown type due to item: Any.
            # Mypy=[unused-ignore] flags this ignore as unused.
            if len(item) != 2:  # type: ignore[arg-type]
                raise ValueError(
                    f"{current_item_desc}: Must be a list/tuple of length 2 "
                    f"[price_str, quantity_str]."
                )

            # Runtime checks above ensure item is indexable
            price_raw: Any = item[0]
            quantity_raw: Any = item[1]

            # Validate Price String
            try:
                price_str = validate_str_field(
                    price_raw,
                    field_name=f"{current_item_desc}[0](price)",
                    max_length=64,
                    allow_empty=False,
                )
                price_dec = parse_decimal_value(price_str, allow_none=False)
                # Add explicit check for None before is_finite
                if price_dec is None:
                    raise ValueError("Price parsing unexpectedly returned None.")
                if not price_dec.is_finite():
                    raise ValueError("Price must be finite.")
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"{current_item_desc}[0](price): Invalid finite decimal string "
                    f"'{price_raw}'. {e}"
                ) from e

            # Validate Quantity String
            try:
                quantity_str = validate_str_field(
                    quantity_raw,
                    field_name=f"{current_item_desc}[1](quantity)",
                    max_length=64,
                    allow_empty=False,
                )
                quantity_dec = parse_decimal_value(quantity_str, allow_none=False)
                # Add explicit check for None before is_finite and comparison
                if quantity_dec is None:
                    raise ValueError("Quantity parsing unexpectedly returned None.")
                if not quantity_dec.is_finite() or quantity_dec < 0:
                    raise ValueError("Quantity must be finite and non-negative.")
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"{current_item_desc}[1](quantity): Invalid non-negative finite "
                    f"decimal string '{quantity_raw}'. {e}"
                ) from e

            validated_levels.append((price_str, quantity_str))
        return validated_levels

    @field_validator("last_update_id", mode="before")
    @classmethod
    def validate_update_id_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate required last_update_id string."""
        return validate_str_field(
            v, field_name=info.field_name or "last_update_id", max_length=64, allow_empty=False
        )

    # Optional validators if E and e are present
    @field_validator("event_type", mode="before")
    @classmethod
    def validate_optional_event_type(cls, v: object | None, info: ValidationInfo) -> str | None:
        """Validate optional event_type string (non-empty if present). Maximum length 32."""
        if v is None:
            return None
        field_name = info.field_name or "event_type"
        # Raw model: Just validate non-empty string, max length. Don't enforce specific enum here.
        return validate_str_field(v, field_name=field_name, max_length=32, allow_empty=False)

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object | None, info: ValidationInfo) -> int | float | str | None:
        """Validate optional event_time (int, float, or parsable string)."""
        if v is None:
            return None
        field_name = info.field_name or "event_time"

        if isinstance(v, int | float):
            if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
                raise ValueError(f"{field_name}: Numeric timestamp must be finite")
            return v
        elif isinstance(v, str):
            s = validate_str_field(v, field_name=field_name, allow_empty=False)
            try:
                if s.isdigit():
                    return int(s)
                val_float = float(s)
                if math.isinf(val_float) or math.isnan(val_float):
                    raise ValueError(f"{field_name}: Numeric timestamp string must be finite")
                return val_float
            except ValueError:
                try:
                    _ = parse_datetime_utc(s, field_name=field_name)
                    return s
                except ValueError as e:
                    raise ValueError(
                        f"{field_name}: String timestamp '{s}' is not a valid number "
                        f"or ISO-like format: {e}"
                    ) from e
        else:
            raise ValueError(
                f"{field_name}: Invalid type {type(v)}, expected int, float, or string"
            )
