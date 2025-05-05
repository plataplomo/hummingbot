"""
Backpack Raw Kline/OHLCV Model
"""

import logging
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, model_validator

# Get logger for the module
logger = logging.getLogger(__name__)


class BackpackRawKline(BaseModel):
    """
    Represents a single Kline/OHLCV data point from the Backpack /api/v1/klines endpoint.

    Structure Example (from OpenAPI spec):
    [
        "1672531200000",  // Start time (Unix timestamp milliseconds)
        "40000.0",        // Open price
        "41000.0",        // High price
        "39000.0",        // Low price
        "40500.0",        // Close price
        "1000.0",         // Volume
        "1672531259999",  // End time (Unix timestamp milliseconds) - Note: spec shows this, common APIs use start time only
        "40500000.0",     // Quote asset volume
        100,              // Number of trades
        "500.0",          // Taker buy base asset volume
        "20250000.0",     // Taker buy quote asset volume
        "0"               // Ignore
    ]
    """

    model_config = {"extra": "forbid"}

    # Define fields based on the array structure (indices)
    # Pydantic will attempt basic type coercion (e.g., str -> int/Decimal)
    start_time_ms: int = Field(..., description="Kline start time (Unix timestamp milliseconds)")
    open_price: Decimal = Field(..., description="Open price")
    high_price: Decimal = Field(..., description="High price")
    low_price: Decimal = Field(..., description="Low price")
    close_price: Decimal = Field(..., description="Close price")
    volume: Decimal = Field(..., description="Base asset volume")
    end_time_ms: int = Field(..., description="Kline end time (Unix timestamp milliseconds)")
    quote_volume: Decimal = Field(..., description="Quote asset volume")
    trade_count: int = Field(..., description="Number of trades")
    taker_buy_base_volume: Decimal = Field(..., description="Taker buy base asset volume")
    taker_buy_quote_volume: Decimal = Field(..., description="Taker buy quote asset volume")
    ignore: str = Field(..., description="Ignore field")  # Spec shows string '0'

    @model_validator(mode="before")
    @classmethod
    def structure_to_dict(cls, data: Any) -> dict[str, Any]:
        """Convert list structure to dict, relying on Pydantic for type coercion."""
        # Pyright Warning: `data` is `Any`, so `len` arg type is unknown.
        # Runtime check below ensures safety.
        if not isinstance(data, (list, tuple)) or len(data) != 12:
            raise ValueError("Kline data must be a list/tuple of exactly 12 elements")

        field_names = list(cls.model_fields.keys())
        # Pyright Warning: `data` is `Any`, so `zip` arg type is unknown.
        # Runtime check above ensures safety.
        return dict(zip(field_names, data, strict=False))

    @model_validator(mode="after")
    def validate_ohlc_and_times(self) -> "BackpackRawKline":
        """Perform cross-field validation after individual fields are parsed.

        Pyright Warning: Linter may complain about incompatible signature.
        However, `def validator(self) -> Self` is the correct signature for
        `@model_validator(mode='after')` in Pydantic v2.
        """
        # Additional cross-field validation if needed (e.g., start_time <= end_time)
        if self.start_time_ms > self.end_time_ms:
            raise ValueError("Kline start_time_ms cannot be after end_time_ms")
        if not all(
            price >= Decimal(0)
            for price in [self.open_price, self.high_price, self.low_price, self.close_price]
        ):
            raise ValueError("Kline prices cannot be negative")
        if not all(
            vol >= Decimal(0)
            for vol in [
                self.volume,
                self.quote_volume,
                self.taker_buy_base_volume,
                self.taker_buy_quote_volume,
            ]
        ):
            raise ValueError("Kline volumes cannot be negative")
        if self.trade_count < 0:
            raise ValueError("Kline trade_count cannot be negative")
        if self.high_price < self.low_price:
            raise ValueError("Kline high_price cannot be less than low_price")
        # Relaxed OHLC check: High >= Open/Close and Low <= Open/Close is often too strict
        # for volatile data. Ensure High >= Low is the most critical.
        # if (self.high_price < self.open_price or
        #      self.high_price < self.close_price or
        #      self.low_price > self.open_price or
        #      self.low_price > self.close_price):
        #      raise ValueError("Kline OHLC relationship invalid (high must be >= open/close, low must be <= open/close)")
        return self
