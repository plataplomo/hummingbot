"""
CyberDeltaEngine: Hyperliquid API Raw Models (Candles Group)
-----------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to candlestick (candle) data.
It is a core part of CyberDeltaEngine's boundary validation layer for historical and real-time
price series.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data structures returned by
  Hyperliquid's 'candleSnapshot' endpoint, which provides OHLCV (open, high, low, close, volume)
  data for assets.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking, and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately rejected. This is critical for robust, secure, and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate external data at the boundary, then map to internal business models with type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawCandleSnapshot.model_validate(api_response_dict)
    # ...then transform to internal candle model
"""

from typing import Any, TypeGuard

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


def is_list(obj: object) -> TypeGuard[list[Any]]:
    """TypeGuard to check if an object is a list"""
    return isinstance(obj, list)


def is_string(obj: object) -> TypeGuard[str]:
    """TypeGuard to check if an object is a string"""
    return isinstance(obj, str)


class HyperliquidRawCandleSnapshot(BaseModel):
    """
    Represents a candle snapshot response from the 'candleSnapshot' endpoint, containing OHLCV
    data for an asset.

    This model is used to validate the structure of the 'candleSnapshot' endpoint response, which
    provides lists of timestamps, open, high, low, close prices, volumes, and a status string.

    Fields:
        t (List[int]): List of timestamps (epoch ms).
        o (List[str]): List of open prices as strings.
        h (List[str]): List of high prices as strings.
        low (List[str]): List of low prices as strings (field alias 'l').
        c (List[str]): List of close prices as strings.
        v (List[str]): List of volumes as strings.
        s (str): Status string for the response.
    """

    t: list[int] = Field(..., alias="t")
    o: list[str] = Field(..., alias="o")
    h: list[str] = Field(..., alias="h")
    low: list[str] = Field(..., alias="l")
    c: list[str] = Field(..., alias="c")
    v: list[str] = Field(..., alias="v")
    s: str = Field(..., alias="s")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("o", "h", "low", "c", "v", mode="before")
    @classmethod
    def validate_price_volume_list(cls, v: object, info: ValidationInfo) -> list[str]:
        field_name = getattr(info, "field_name", None) or "field"

        # Check if input is a list using TypeGuard
        if not is_list(v):
            raise ValueError(f"{field_name}: Must be a list of strings.")

        # At this point, mypy and pyright know v is a List[Any]
        result: list[str] = []

        # Process each item in the list
        for i, item_obj in enumerate(v):
            # Verify the item is a string
            if not is_string(item_obj):
                raise ValueError(f"{field_name}[{i}]: Must be a string.")

            # Now item_obj is known to be a string
            s = validate_str_field(item_obj, field_name=f"{field_name}[{i}]", max_length=64)

            # Validate it's a valid decimal
            d = parse_decimal_value(s, allow_none=False, field_name=f"{field_name}[{i}]")
            if d is None or not d.is_finite():
                raise ValueError(
                    f"{field_name}[{i}]: Value must be a finite decimal (not NaN or inf)"
                )

            # Add the validated string to our result list
            result.append(s)

        return result

    @field_validator("s", mode="before")
    @classmethod
    def validate_status_str(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="s", max_length=32)


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """
    Represents the request payload for the 'candleSnapshot' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting candlestick data for a specific asset and interval.

    Fields:
        type (str): Must be 'candleSnapshot'.
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        interval (str): Interval string (e.g., '1m', '1h', '1d').
        start_time (int): Start timestamp (epoch ms).
        end_time (int): End timestamp (epoch ms).
    """

    type: str = Field("candleSnapshot", alias="type")
    coin: str = Field(..., alias="coin")
    interval: str = Field(..., alias="interval")
    start_time: int = Field(..., alias="startTime")
    end_time: int = Field(..., alias="endTime")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        # Enforce max_length=64 and valid UTF-8 for asset symbol
        return validate_str_field(v, field_name="coin", max_length=64)

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def validate_int_strict(cls, v: object, info: ValidationInfo) -> int:
        # Enforce strict int type (no float or str coercion)
        field_name = info.field_name or "field"
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Must be an integer (no coercion allowed)")
        return v

    @field_validator("interval", mode="before")
    @classmethod
    def validate_interval(cls, v: object, info: ValidationInfo) -> str:
        # Enforce max_length=16 and valid UTF-8 for interval string
        return validate_str_field(v, field_name="interval", max_length=16)
