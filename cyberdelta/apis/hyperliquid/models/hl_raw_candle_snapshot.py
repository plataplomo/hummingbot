"""
CyberDeltaEngine: Hyperliquid API Raw Models (Candle Snapshot)
--------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of candlestick data
(kline/OHLCV) from the Hyperliquid Exchange API, specifically for responses from the
`"candlesSnapshot"` info type.

Models:
    - HyperliquidRawCandle: Validates a single raw candle object, expecting fields like
      timestamp (`t`), open (`o`), high (`h`), low (`l`), close (`c`), volume (`v`), and
      trade count (`n`). Performs basic type validation (integer for time/count, string for
      prices/volume) and format checks (non-negative int, parsable decimal string).
    - HyperliquidRawCandleSnapshotResponse: Validates the top-level response structure, expecting
      a `candles` field containing a list of `HyperliquidRawCandle` objects.

These models adhere to the Raw Model Policy:
- Validate the external contract (list of candle objects, specific fields within each).
- Validate raw data types and basic formats.
- Use `model_config(extra="forbid" / "ignore", frozen=True)` appropriately.
- Field validators operate on raw input and return validated raw types or raise errors.
- Contain NO business logic (e.g., OHLC consistency, which belongs in internal models).
"""

import logging

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawFiniteDecimalStr,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawTimestampMsInt,
)

logger = logging.getLogger(__name__)


class HyperliquidRawCandle(BaseModel):
    """
    Represents a single raw candle within the snapshot response.
    NOTE: Structure is assumed based on common API patterns, as it's not
    explicitly defined in the synthesized OpenAPI spec.
    Assumes fields like 't' (timestamp), 'o', 'h', 'l', 'c', 'v'.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    t: RawTimestampMsInt = Field(..., description="Timestamp (Unix milliseconds)")
    o: RawFiniteDecimalStr = Field(..., description="Open price (string)")
    h: RawFiniteDecimalStr = Field(..., description="High price (string)")
    low_price: RawFiniteDecimalStr = Field(..., alias="l", description="Low price (string)")
    c: RawFiniteDecimalStr = Field(..., description="Close price (string)")
    v: RawNonNegativeFiniteDecimalStr = Field(..., description="Volume (string, non-negative)")
    n: RawNonNegativeInt = Field(..., description="Number of trades")


class HyperliquidRawCandleSnapshotResponse(BaseModel):
    """
    Represents the assumed structure of the 'candlesSnapshot' info response from Hyperliquid.

    This model validates that the response contains a `candles` field, which is a list
    of `HyperliquidRawCandle` objects. It allows extra fields at the top level (`extra="ignore"`)
    as the exact full response structure might vary or include metadata not strictly needed.

    Attributes:
        candles (list[HyperliquidRawCandle]): A list of raw candle data objects, each validated
                                             by `HyperliquidRawCandle`.
    """

    model_config = {"extra": "ignore"}  # Allow extra fields

    candles: list[HyperliquidRawCandle] = Field(..., description="List of raw candle data objects")

    # Potentially other metadata fields?
    # interval: Optional[str] = Field(None)
    # coin: Optional[str] = Field(None)


# Removed stray </rewritten_file>
