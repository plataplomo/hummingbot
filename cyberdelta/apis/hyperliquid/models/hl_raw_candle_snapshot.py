"""
Hyperliquid Raw Candle Snapshot Model
"""

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

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
    def validate_non_negative_int(cls, v: Any, info: ValidationInfo) -> int:
        """Ensure timestamp and number of trades are non-negative integers."""
        if not isinstance(v, int) or v < 0:
            raise ValueError(f"Expected non-negative integer for {info.field_name}, got {type(v)}")
        return v

    @field_validator("o", "h", "low_price", "c", "v", mode="before")
    @classmethod
    def validate_decimal_strings(cls, v: Any) -> str:
        """Ensure price/volume fields are valid decimal strings."""
        if not isinstance(v, str):
            raise TypeError(f"Expected string for decimal parsing, got {type(v)}")
        try:
            # Test parse to ensure it's a valid decimal format
            _ = Decimal(v)
        except InvalidOperation as e:
            raise ValueError(f"Invalid decimal string format: '{v}'") from e
        return v


class HyperliquidRawCandleSnapshotResponse(BaseModel):
    """
    Represents the assumed structure of the 'candlesSnapshot' info response.
    NOTE: Structure is assumed. It likely contains a list of candle objects.
    """

    model_config = {"extra": "ignore"}  # Allow extra fields

    candles: list[HyperliquidRawCandle] = Field(..., description="List of raw candle data objects")

    # Potentially other metadata fields?
    # interval: Optional[str] = Field(None)
    # coin: Optional[str] = Field(None)


# Removed stray </rewritten_file>
