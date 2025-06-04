from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field

# Instantiate logger for this module
logger = logging.getLogger(__name__)


class Ticker(BaseModel):
    """Represents an immutable, validated snapshot of the latest ticker data for a symbol.

    Provides core price (last, bid, ask) and volume information, ensuring data integrity
    through strict validation and Decimal usage for financial precision.

    Attributes:
        symbol: Trading symbol (validated: required, non-empty, max 64 chars, UTF-8).
        timestamp: UTC timestamp of the ticker snapshot (validated: required).
        price: Last traded price. Must be non-negative if provided.
        bid: Best bid price. Must be non-negative if provided.
        ask: Best ask price. Must be non-negative if provided.
        volume: Trading volume (e.g., 24h). Must be non-negative if provided.

    Configuration:
        - `frozen=True`: Guarantees immutability.
        - `extra='forbid'`: Prevents unexpected fields.
        - `validate_assignment=True`: Ensures validation on assignment (redundant with frozen=True).

    """

    symbol: str
    timestamp: datetime
    # Using Field for default=None and validation (ge=0)
    price: Decimal | None = Field(default=None, ge=Decimal("0"))
    bid: Decimal | None = Field(default=None, ge=Decimal("0"))
    ask: Decimal | None = Field(default=None, ge=Decimal("0"))
    volume: Decimal | None = Field(default=None, ge=Decimal("0"))

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object) -> str:
        """Validate the 'symbol' field."""
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: datetime | int | float | str | None) -> datetime:
        """Validate and parse the 'timestamp' field to a required UTC datetime object."""
        dt = parse_datetime_utc(v, field_name="timestamp")
        if dt is None:
            raise ValueError("timestamp must not be None and must be a valid format")
        return dt

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def validate_and_parse_decimal_optional(
        cls,
        v: str | int | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Validate, parse, and check finiteness for optional Decimal fields (price, bid, ask, volume).

        Uses `parse_decimal_value` which handles None input gracefully (returns None).
        Adds an explicit check to ensure that any non-None parsed Decimal is finite.
        The non-negativity (`ge=0`) constraint is handled by `Field`.

        Args:
            v: The raw input value (can be various numeric types or None).
            info: Pydantic validation context. Used for field name in error messages if needed.

        Returns:
            The parsed Decimal value if input is valid and non-None, None if input is None,
            or raises ValueError for invalid/non-finite inputs.

        Raises:
            ValueError: If a non-None input cannot be parsed to a finite Decimal.

        """
        # Ensure field_name is a str for the parsing utility.
        field_name = info.field_name if info.field_name is not None else "unknown_field"

        # parse_decimal_value returns None if v is None, raises ValueError otherwise on failure.
        parsed_decimal = parse_decimal_value(v, allow_none=True, field_name=field_name)

        # Ensure non-None results are finite. NaN/Infinity are invalid for ticker data.
        if parsed_decimal is not None and not parsed_decimal.is_finite():
            raise ValueError(f"Field '{field_name}' must be a finite Decimal, got {parsed_decimal}")

        return parsed_decimal

    @property
    def mid_price(self) -> Decimal | None:
        """Calculate the mid-price (average of bid and ask).

        Returns:
            The mid-price as a Decimal if both bid and ask are valid and non-None,
            otherwise returns None.

        """
        # Type hints and earlier validation make isinstance checks redundant here.
        if (
            self.bid is not None
            and self.ask is not None
            and self.bid.is_finite()
            and self.ask.is_finite()
        ):
            try:
                mid = (self.bid + self.ask) / Decimal("2")
                # Check if calculation resulted in non-finite
                if mid.is_finite():
                    return mid
                else:
                    logger.warning(
                        f"Mid-price calculation for {self.symbol} resulted in non-finite value "
                        f"from bid={self.bid}, ask={self.ask}",
                    )
                    return None
            except InvalidOperation:  # Catch only InvalidOperation for calculation issues
                # Should not happen if inputs are finite Decimals, but defensive
                logger.error(
                    f"Error calculating mid-price for {self.symbol} "
                    f"from bid={self.bid}, ask={self.ask}",
                    exc_info=True,
                )
                return None
        return None  # Return None if bid or ask is None or non-finite
