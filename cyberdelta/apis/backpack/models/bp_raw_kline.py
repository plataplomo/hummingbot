"""
Backpack Raw Kline/OHLCV Model
"""

from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


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

    model_config = ConfigDict(extra="forbid")

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
    def structure_to_dict(cls, data: list[Any] | tuple[Any, ...]) -> dict[str, Any]:
        """Convert list structure to dict, relying on Pydantic for type coercion."""
        # DEFENSIVE CHECK: Ensure runtime type is list or tuple despite signature.
        # Pyright=[reportUnnecessaryIsInstance]
        if not isinstance(data, list | tuple) or len(data) != 12:
            raise ValueError("Kline data must be a list/tuple of exactly 12 elements")

        # Map list indices to field names (adjust field names if needed)
        field_names = [
            "start_time_ms",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "end_time_ms",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignored",
        ]

        # Create a dictionary from the list using field names
        # This assumes the order in the list matches field_names exactly
        return dict(zip(field_names, data, strict=False))

    @model_validator(mode="after")
    def validate_ohlc_relationship(self) -> Self:
        """Validate basic OHLC consistency after individual fields are parsed."""
        # DEFENSIVE CHECK: Ensure prices are finite Decimals post-parsing.
        # Relies on 'before' validators for type conversion.
        if not all(
            p.is_finite()
            for p in [self.open_price, self.high_price, self.low_price, self.close_price]
        ):
            raise ValueError("Internal error: Non-finite Decimal price found after parsing")

        # Basic OHLC checks
        if self.high_price < self.low_price:
            raise ValueError("Kline high price cannot be less than low price")

        if (
            self.high_price < self.open_price
            or self.high_price < self.close_price
            or self.low_price > self.open_price
            or self.low_price > self.close_price
        ):
            raise ValueError(
                "Kline OHLC relationship invalid (high >= open/close, low <= open/close)"
            )
        return self
