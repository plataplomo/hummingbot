"""Candlestick (OHLCV) data model for market data representation.

This module provides the Candle model for representing immutable OHLCV
(Open, High, Low, Close, Volume) candlestick data with strict validation
and financial precision using Decimal types.

The Candle model ensures data integrity through:
- Strict validation of all price and volume fields
- Logical consistency checks (high >= low, etc.)
- UTC timezone enforcement for timestamps
- Immutable design to prevent accidental modification
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import (
    Field,
    field_validator,
    model_validator,
)

from cyberdelta.exceptions.field_validation import (
    OHLCConsistencyError,
)
from cyberdelta.models.base_validators import (
    ImmutableModel,
    required_datetime_validator,
    required_decimal_validator,
)
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import validate_str_field


class Candle(ImmutableModel):
    """Represents an immutable, validated OHLCV candlestick bar for a specific symbol and interval.

    This model ensures data integrity for historical or interval-based market data through
    strict validation, Decimal usage for financial precision, and logical consistency checks.

    Attributes:
        symbol: Trading symbol (validated: required, non-empty, max 64 chars, UTF-8).
        interval: Time interval of the candle (e.g., "1m", "1h")
                    (validated: required, non-empty, max 16 chars).
        open_time: Start time of the candle interval (validated: required, UTC).
        open: Opening price for the interval (validated: required, > 0, finite).
        high: Highest price for the interval (validated: required, > 0, finite).
        low: Lowest price for the interval (validated: required, > 0, finite).
        close: Closing price for the interval (validated: required, > 0, finite).
        volume: Trading volume for the interval (validated: required, >= 0, finite).

    Configuration:
        - `frozen=True`: Guarantees immutability.
        - `extra='forbid'`: Prevents unexpected fields.
        - `validate_assignment=True`: Ensures validation on assignment
                (mostly redundant with frozen=True).

    """

    symbol: Symbol
    interval: str
    open_time: datetime
    # Prices must be strictly positive
    open: Decimal = Field(gt=Decimal(0))
    high: Decimal = Field(gt=Decimal(0))
    low: Decimal = Field(gt=Decimal(0))
    close: Decimal = Field(gt=Decimal(0))
    # Volume can be zero
    volume: Decimal = Field(ge=Decimal(0))

    # Config: Immutable (inherited from ImmutableModel)
    # Use centralized validators
    _validate_open_time = required_datetime_validator("open_time")
    _validate_decimals = required_decimal_validator("open", "high", "low", "close", "volume")

    # Symbol validation is handled by Pydantic's type system
    # No need for a custom validator since Symbol is always valid

    @field_validator("interval", mode="before")
    @classmethod
    def validate_interval(cls, v: object) -> str:
        """Validate the 'interval' field.

        Returns:
            str: The validated interval string.
        """
        # Basic validation for now, consider adding regex for common patterns if needed.
        return validate_str_field(v, field_name="interval", max_length=16, allow_empty=False)

    @model_validator(mode="after")
    def check_ohlc_consistency(self) -> Self:
        """Validate the logical consistency of OHLC prices (high >= low, etc.).

        Returns:
            Self: The validated Candle instance.

        Raises:
            OHLCConsistencyError: If OHLC prices are logically inconsistent.
        """
        if self.high < self.low:
            raise OHLCConsistencyError(
                constraint="high must be >= low",
                high=self.high,
                low=self.low,
            )
        if self.high < self.open:
            raise OHLCConsistencyError(
                constraint="high must be >= open",
                high=self.high,
                open_price=self.open,
            )
        if self.high < self.close:
            raise OHLCConsistencyError(
                constraint="high must be >= close",
                high=self.high,
                close=self.close,
            )
        if self.low > self.open:
            raise OHLCConsistencyError(
                constraint="low must be <= open",
                low=self.low,
                open_price=self.open,
            )
        if self.low > self.close:
            raise OHLCConsistencyError(
                constraint="low must be <= close",
                low=self.low,
                close=self.close,
            )
        return self
