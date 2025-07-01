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
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.exceptions.field_validation import (
    DateTimeFieldError,
    DecimalFieldError,
    DecimalFiniteError,
    OHLCConsistencyError,
    RequiredFieldNoneError,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


class Candle(BaseModel):
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

    symbol: str
    interval: str
    open_time: datetime
    # Prices must be strictly positive
    open: Decimal = Field(gt=Decimal(0))
    high: Decimal = Field(gt=Decimal(0))
    low: Decimal = Field(gt=Decimal(0))
    close: Decimal = Field(gt=Decimal(0))
    # Volume can be zero
    volume: Decimal = Field(ge=Decimal(0))

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object) -> str:
        """Validate the 'symbol' field."""
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("interval", mode="before")
    @classmethod
    def validate_interval(cls, v: object) -> str:
        """Validate the 'interval' field."""
        # Basic validation for now, consider adding regex for common patterns if needed.
        return validate_str_field(v, field_name="interval", max_length=16, allow_empty=False)

    @field_validator("open_time", mode="before")
    @classmethod
    def validate_open_time(cls, v: datetime | float | str | None) -> datetime:
        """Validate and parse the 'open_time' field to a required UTC datetime object."""
        dt = parse_datetime_utc(v, field_name="open_time")
        if dt is None:
            raise DateTimeFieldError(
                field_name="open_time",
                value=v,
                reason="must not be None and must be a valid format",
            )
        return dt

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def validate_and_parse_decimal_required(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal:
        """Validate, parse, and check finiteness for required Decimal fields (OHLCV).

        Uses `parse_decimal_value`. Ensures the result is non-None and finite.
        Positive/Non-negative constraints (`gt=0`/`ge=0`) are handled by `Field`.

        Args:
            v: The raw input value (can be various numeric types).
            info: Pydantic validation context. Used for field name in error messages.

        Returns:
            The parsed, finite Decimal value.

        Raises:
            ValueError: If input is None, cannot be parsed, or is not finite.

        """
        field_name = info.field_name if info.field_name is not None else "unknown_decimal_field"
        # Ensure value is not None
        if v is None:
            raise RequiredFieldNoneError(
                field_name=field_name,
                reason="Required OHLCV field cannot be None",
            )

        parsed_decimal = parse_decimal_value(v, allow_none=False, field_name=field_name)

        # Redundant check as parse_decimal_value(allow_none=False) should handle this,
        # but provides extra safety.
        if parsed_decimal is None:
            raise DecimalFieldError(
                field_name=field_name,
                value=v,
                reason="parsing returned None unexpectedly",
            )

        # Ensure non-None results are finite. NaN/Infinity are invalid for candle data.
        if not parsed_decimal.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed_decimal,
                context="(NaN/Infinity are invalid for candle data)",
            )

        return parsed_decimal

    @model_validator(mode="after")
    def check_ohlc_consistency(self) -> Self:
        """Validate the logical consistency of OHLC prices (high >= low, etc.)."""
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
