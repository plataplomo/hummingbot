"""CyberDeltaEngine: Hyperliquid API Raw Models (Candles Group).

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

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawHLCandleStatusString,
    RawNonNegativeFiniteDecimalStr,
    RawTimestampMsInt,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawCandleSnapshot(BaseModel):
    """Strict boundary model for candle snapshot response from Hyperliquid candleSnapshot endpoint.

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

    t: list[RawTimestampMsInt] = Field(..., alias="t")
    o: list[RawFiniteDecimalStr] = Field(..., alias="o")
    h: list[RawFiniteDecimalStr] = Field(..., alias="h")
    l: list[RawFiniteDecimalStr] = Field(..., alias="l")  # noqa: E741
    c: list[RawFiniteDecimalStr] = Field(..., alias="c")
    v: list[RawNonNegativeFiniteDecimalStr] = Field(..., alias="v")
    s: RawHLCandleStatusString = Field(..., alias="s", max_length=32)

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def preprocess_candle_response(cls, values: object) -> dict[str, object]:
        """Preprocess candle snapshot response before validation.
        
        This handles the preprocessing logic that was previously in the 
        HyperliquidResponsePreprocessingMapper.preprocess_candle_snapshot_response method.
        """
        # Handle empty list response (no candle data available)
        if isinstance(values, list):
            if len(values) == 0:
                # Return a minimal empty candle snapshot with proper OHLCV structure
                return {
                    "t": [],  # timestamps
                    "o": [],  # open prices
                    "h": [],  # high prices
                    "l": [],  # low prices
                    "c": [],  # close prices
                    "v": [],  # volumes
                    "s": "ok",  # status
                }
            # If it's a non-empty list, something is wrong
            raise ValueError(
                "Candle snapshot response must be a dict, not a list"
            )
        
        if not isinstance(values, dict):
            raise ValueError(
                f"Candle snapshot response must be a dict, got {type(values).__name__}"
            )
        
        return values

    @model_validator(mode="after")
    def check_list_lengths(self) -> Self:
        """Check that all OHLCV lists have the same length as the timestamp list."""
        list_len = len(self.t)
        if not (
            len(self.o) == list_len
            and len(self.h) == list_len
            and len(self.l) == list_len
            and len(self.c) == list_len
            and len(self.v) == list_len
        ):
            raise ValueError(
                "Data lists (t, o, h, l, c, v) must all have the same length. "
                f"Got lengths: t({len(self.t)}), o({len(self.o)}), h({len(self.h)}), "
                f"l({len(self.l)}), c({len(self.c)}), v({len(self.v)})",
            )
        return self


class HyperliquidRawCandleRequestDetails(BaseModel):
    """Details for a candle snapshot request, nested under 'req' field."""

    coin: RawDefaultString = Field(..., min_length=1, max_length=24)
    interval: RawDefaultString = Field(..., min_length=1, max_length=8)  # e.g., "1m", "1h", "1d"
    start_time: RawTimestampMsInt = Field(..., alias="startTime")
    end_time: RawTimestampMsInt = Field(..., alias="endTime")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawCandleSnapshotRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'candleSnapshot' info type.

    Uses a nested 'req' object to encapsulate the specific parameters required for
    requesting candlestick data from Hyperliquid's info endpoint. This model ensures
    proper validation of the request structure before sending to the API.
    """

    type: Annotated[
        Literal["candleSnapshot"],
        BeforeValidator(
            lambda v: validate_str_field(v, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field("candleSnapshot", alias="type")
    req: HyperliquidRawCandleRequestDetails

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
