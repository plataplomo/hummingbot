"""
CyberDeltaEngine: Hyperliquid API Raw Models (Candles Group)
-----------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related
to candlestick (candle) data.
It is a core part of CyberDeltaEngine's boundary validation layer for historical
and real-time price series.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's 'candleSnapshot' endpoint, which provides OHLCV
  (open, high, low, close, volume) data for assets.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with
  type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawCandleSnapshot.model_validate(api_response_dict)
    # ...then transform to internal candle model
"""

from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field
from cyberdelta.utils.typing import is_sequence_of_any


class HyperliquidRawCandleSnapshot(BaseModel):
    """
    Strict boundary model for a candle snapshot response from Hyperliquid's
    'candleSnapshot' info endpoint.

    This model validates the raw API response which consists of parallel lists for
    timestamp, open, high, low, close, volume data, and a status string.
    It adheres to the project's Raw Model policies, including strict type/format
    validation, 'extra="forbid"', and 'frozen=True'.

    The structure mirrors the 'CandleSnapshotResponse' definition found in
    the `openapi_hl.json` specification for Hyperliquid.

    Fields:
        t (list[int]): List of candle timestamps (Unix epoch in milliseconds).
            Alias "t".
        o (list[str]): List of candle open prices, as strings representing
            finite decimals. Alias "o".
        h (list[str]): List of candle high prices, as strings representing
            finite decimals. Alias "h".
        l (list[str]): List of candle low prices, as strings representing
            finite decimals. Alias "l".
        c (list[str]): List of candle close prices, as strings representing
            finite decimals. Alias "c".
        v (list[str]): List of candle volumes, as strings representing
            non-negative, finite decimals. Alias "v".
        s (str): Status string from the API (e.g., "ok"). Alias "s".
    """

    t: list[int] = Field(..., alias="t")
    o: list[str] = Field(..., alias="o")
    h: list[str] = Field(..., alias="h")
    l: list[str] = Field(..., alias="l")  # noqa: E741
    c: list[str] = Field(..., alias="c")
    v: list[str] = Field(..., alias="v")
    s: str = Field(..., alias="s")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("t", mode="before")
    @classmethod
    def validate_timestamp_list(cls, val: object, info: ValidationInfo) -> list[int]:
        """Validates 't' field: list of non-negative integer timestamps."""
        field_name = info.field_name or "t"
        if not is_sequence_of_any(val):
            raise TypeError(f"{field_name}: Must be a list, got {type(val).__name__}.")
        if not isinstance(val, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(val).__name__}.")

        validated_list: list[int] = []
        for i, item_raw in enumerate(val):
            item: Any = item_raw
            if not isinstance(item, int):
                raise TypeError(
                    f"{field_name}[{i}]: Must be an integer, got {type(item).__name__}."
                )
            if item < 0:
                raise ValueError(f"{field_name}[{i}]: Timestamp must be non-negative, got {item}.")
            validated_list.append(item)
        return validated_list

    @field_validator("o", "h", "l", "c", mode="before")
    @classmethod
    def validate_price_list(cls, val: object, info: ValidationInfo) -> list[str]:
        """Validates price fields ('o','h','l','c'): list of finite decimal strings
        (max 64 chars)."""
        field_name = info.field_name or "price_list"
        if not is_sequence_of_any(val):
            raise TypeError(f"{field_name}: Must be a list, got {type(val).__name__}.")
        if not isinstance(val, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(val).__name__}.")

        validated_list: list[str] = []
        for i, item_raw in enumerate(val):
            item: Any = item_raw
            current_item_desc = f"{field_name}[{i}]"
            str_item = validate_str_field(
                item,
                field_name=current_item_desc,
                max_length=64,
                allow_empty=False,
            )
            try:
                parsed_decimal = parse_decimal_value(
                    str_item, field_name=current_item_desc, allow_none=False
                )
                if parsed_decimal is None:
                    raise ValueError(f"{current_item_desc}: Parsed decimal is None unexpectedly.")
                if not parsed_decimal.is_finite():
                    raise ValueError(
                        f"{current_item_desc}: Value '{str_item}' must represent a finite decimal."
                    )
            except ValueError as e:
                error_message = (
                    f"{current_item_desc}: Invalid finite decimal string '{str_item}'. Reason: {e}"
                )
                raise ValueError(error_message) from e
            validated_list.append(str_item)
        return validated_list

    @field_validator("v", mode="before")
    @classmethod
    def validate_volume_list(cls, val: object, info: ValidationInfo) -> list[str]:
        """Validates 'v' field: list of non-negative finite decimal strings (max 64 chars)."""
        field_name = info.field_name or "v"
        if not is_sequence_of_any(val):
            raise TypeError(f"{field_name}: Must be a list, got {type(val).__name__}.")
        if not isinstance(val, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(val).__name__}.")

        validated_list: list[str] = []
        for i, item_raw in enumerate(val):
            item: Any = item_raw
            current_item_desc = f"{field_name}[{i}]"
            str_item = validate_str_field(
                item,
                field_name=current_item_desc,
                max_length=64,
                allow_empty=False,
            )
            try:
                parsed_decimal = parse_decimal_value(
                    str_item, field_name=current_item_desc, allow_none=False
                )
                if parsed_decimal is None:
                    raise ValueError(f"{current_item_desc}: Parsed decimal is None unexpectedly.")
                if not parsed_decimal.is_finite():
                    raise ValueError(
                        f"{current_item_desc}: Value '{str_item}' must represent a finite decimal."
                    )
                if parsed_decimal < Decimal(0):
                    raise ValueError(
                        f"{current_item_desc}: Value '{str_item}' must be non-negative."
                    )
            except ValueError as e:
                error_message = (
                    f"{current_item_desc}: Invalid non-negative finite decimal string "
                    f"'{str_item}'. Reason: {e}"
                )
                raise ValueError(error_message) from e
            validated_list.append(str_item)
        return validated_list

    @field_validator("s", mode="before")
    @classmethod
    def validate_status_string(cls, val: object, info: ValidationInfo) -> str:
        """Validates 's' field: non-empty string (max 32 chars)."""
        field_name = info.field_name or "s"
        return validate_str_field(val, field_name=field_name, max_length=32, allow_empty=False)

    @model_validator(mode="after")
    def check_list_lengths(self) -> Self:
        """Ensures all data lists (t, o, h, l, c, v) have the exact same length."""
        list_lengths = {
            len(self.t),
            len(self.o),
            len(self.h),
            len(self.l),
            len(self.c),
            len(self.v),
        }
        if len(list_lengths) > 1:
            # Shortened f-string for length
            msg = (
                "Data lists (t, o, h, l, c, v) must all have the same length. "
                f"Found: t={len(self.t)}, o={len(self.o)}, h={len(self.h)}, "
                f"l={len(self.l)}, c={len(self.c)}, v={len(self.v)}"
            )
            raise ValueError(msg)
        return self


class HyperliquidRawCandleRequestDetails(BaseModel):
    """
    Details for a candle snapshot request, nested under 'req' field.
    """

    coin: str = Field(..., min_length=1, max_length=24)
    interval: str = Field(..., min_length=1, max_length=8)  # e.g., "1m", "1h", "1d"
    start_time: int = Field(..., alias="startTime", ge=0)
    end_time: int = Field(..., alias="endTime", ge=0)

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("coin", "interval", mode="before")
    @classmethod
    def validate_string_fields(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        max_len = 24 if field_name == "coin" else 8
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def validate_timestamp_fields(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "timestamp"
        if not isinstance(v, int):
            raise TypeError(f"{field_name}: Must be an integer, got {type(v).__name__}.")
        if v < 0:
            raise ValueError(f"{field_name}: Timestamp must be non-negative, got {v}.")
        return v

    @model_validator(mode="after")
    def check_start_end_time(self) -> Self:
        if self.end_time < self.start_time:
            raise ValueError(
                f"endTime ({self.end_time}) cannot be before startTime ({self.start_time})."
            )
        return self


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'candleSnapshot' info type.
    Uses a nested 'req' object.
    """

    type: Literal["candleSnapshot"] = Field("candleSnapshot", alias="type")
    req: HyperliquidRawCandleRequestDetails

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_literal(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=32)
        if s != "candleSnapshot":
            raise ValueError(f"{field_name} must be 'candleSnapshot', got '{s}'")
        return s
