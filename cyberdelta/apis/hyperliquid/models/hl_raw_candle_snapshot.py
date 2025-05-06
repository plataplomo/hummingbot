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
from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

logger = logging.getLogger(__name__)


class HyperliquidRawCandle(BaseModel):
    """
    Represents a single raw candle within the snapshot response.
    NOTE: Structure is assumed based on common API patterns, as it's not
    explicitly defined in the synthesized OpenAPI spec.
    Assumes fields like 't' (timestamp), 'o', 'h', 'l', 'c', 'v'.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    t: int = Field(..., description="Timestamp (Unix milliseconds)")
    o: str = Field(..., description="Open price (string)")
    h: str = Field(..., description="High price (string)")
    low_price: str = Field(..., alias="l", description="Low price (string)")
    c: str = Field(..., description="Close price (string)")
    v: str = Field(..., description="Volume (string)")
    n: int = Field(..., description="Number of trades")

    @field_validator("t", "n", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> object:
        """Validate that the raw value is a non-negative integer.

        Used for fields like timestamp (`t`) and number of trades (`n`).

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            object: The validated integer value.

        Raises:
            ValueError: If `v` is not an integer or is negative.
        """
        if not isinstance(v, int) or v < 0:
            raise ValueError(f"Expected non-negative integer for {info.field_name}, got {type(v)}")
        return v

    @field_validator("o", "h", "low_price", "c", "v", mode="before")
    @classmethod
    def validate_decimal_string(cls, v: object, info: ValidationInfo) -> object:
        """Validate that the raw value is a non-empty string parseable to a finite Decimal.

        Used for price fields (o, h, l, c) and volume (v).
        Volume specifically must be non-negative.

        Args:
            v (object): The raw input value.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            object: The validated string value.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is not a valid, finite decimal string (or non-negative for volume).
        """
        if not isinstance(v, str):
            raise TypeError(
                f"Expected string for decimal parsing for field '{info.field_name}', got {type(v)}"
            )
        if not v.strip():  # Ensure not empty or just whitespace
            raise ValueError(
                f"Expected non-empty decimal string for field '{info.field_name}', got '{v}'"
            )
        try:
            d = Decimal(v)
            if not d.is_finite():
                raise ValueError(
                    f"Expected finite decimal string for field '{info.field_name}', got '{v}'"
                )
            if info.field_name == "v" and d < Decimal(0):
                raise ValueError(f"Volume (v) must be non-negative, got '{v}'")
        except InvalidOperation as e:
            raise ValueError(
                f"Invalid decimal string format for field '{info.field_name}': '{v}'"
            ) from e
        return v


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
