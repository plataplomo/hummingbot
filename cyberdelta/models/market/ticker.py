"""Ticker data model for real-time market price information.

This module provides the Ticker model for representing immutable snapshots
of real-time market data including last price, bid/ask prices, and volume.

The Ticker model ensures data integrity through:
- Strict validation of all price and volume fields
- Decimal precision for financial calculations
- Immutable design to prevent accidental modification
- Optional fields with non-negative constraints
- Computed mid-price property for spread analysis
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import (
    DecimalFiniteError,
    InvalidExchangeNameError,
    RequiredFieldNoneError,
    TypeFieldError,
)
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value


# Instantiate logger for this module
logger = get_logger(__name__)


class Ticker(BaseModel):
    """Represents an immutable, validated snapshot of the latest ticker data for a symbol.

    Provides core price (last, bid, ask) and volume information, ensuring data integrity
    through strict validation and Decimal usage for financial precision. Supports
    exchange-specific extension slots for preserving additional ticker data.

    Attributes:
        symbol: Exchange-specific trading symbol domain object.
        exchange: Exchange name (validated: required, non-empty, max 64 chars).
        timestamp: UTC timestamp of the ticker snapshot (validated: required).
        price: Last traded price. Must be non-negative if provided.
        bid: Best bid price. Must be non-negative if provided.
        ask: Best ask price. Must be non-negative if provided.
        volume: Trading volume (e.g., 24h). Must be non-negative if provided.
        hl_details: Hyperliquid-specific ticker enrichment (optional).
        bp_details: Backpack-specific ticker enrichment (optional).

    Configuration:
        - `frozen=True`: Guarantees immutability.
        - `extra='forbid'`: Prevents unexpected fields.
        - `validate_assignment=True`: Ensures validation on assignment (redundant with frozen=True).

    """

    symbol: Symbol
    exchange: ExchangeName
    timestamp: datetime
    # Using Field for default=None and validation (ge=0)
    price: Decimal | None = Field(default=None, ge=Decimal(0))
    bid: Decimal | None = Field(default=None, ge=Decimal(0))
    ask: Decimal | None = Field(default=None, ge=Decimal(0))
    volume: Decimal | None = Field(default=None, ge=Decimal(0))
    hl_details: HyperliquidTickerDetails | None = Field(default=None)
    bp_details: BackpackTickerDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    # Symbol validation is handled by Pydantic's type system
    # No need for a custom validator since Symbol is always valid

    @field_validator("exchange", mode="before")
    @classmethod
    def validate_exchange(cls, v: object, info: ValidationInfo) -> ExchangeName:
        """Validate the 'exchange' field.

        Returns:
            ExchangeName: Validated exchange name.

        Raises:
            InvalidExchangeNameError: If exchange name is invalid.
            TypeFieldError: If value is not a string or ExchangeName.
        """
        if isinstance(v, ExchangeName):
            return v
        if isinstance(v, str):
            try:
                return ExchangeName(v.lower())
            except ValueError as e:
                raise InvalidExchangeNameError(
                    value=v,
                    valid_exchanges=[ex.value for ex in ExchangeName],
                ) from e
        raise TypeFieldError(
            field_name="exchange",
            expected_type="string or ExchangeName",
            actual_type=type(v).__name__,
            actual_value=v,
        )

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: datetime | float | str | None) -> datetime:
        """Validate and parse the 'timestamp' field to a required UTC datetime object.

        Args:
            v: Value to validate and parse.

        Returns:
            datetime: Parsed UTC datetime object.

        Raises:
            RequiredFieldNoneError: If timestamp is None.
        """
        dt = parse_datetime_utc(v, field_name="timestamp")
        if dt is None:
            raise RequiredFieldNoneError(
                field_name="timestamp",
                reason="Ticker timestamp is required and must be a valid format",
            )
        return dt

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def validate_and_parse_decimal_optional(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Validate, parse, and check finiteness for optional Decimal fields.

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
            DecimalFiniteError: If a non-None input is not finite.

        """
        # Ensure field_name is a str for the parsing utility.
        field_name = info.field_name if info.field_name is not None else "unknown_field"

        # parse_decimal_value returns None if v is None, raises ValueError otherwise on failure.
        parsed_decimal = parse_decimal_value(v, allow_none=True, field_name=field_name)

        # Ensure non-None results are finite. NaN/Infinity are invalid for ticker data.
        if parsed_decimal is not None and not parsed_decimal.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed_decimal,
                context="for ticker price data",
            )

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
                mid = (self.bid + self.ask) / Decimal(2)
                # Check if calculation resulted in non-finite
                if mid.is_finite():
                    mid_price_result = mid
                else:
                    logger.warning(
                        "mid_price_calculation_non_finite",
                        symbol=self.symbol.value,
                        exchange=self.symbol.exchange,
                        bid=self.bid,
                        ask=self.ask,
                        message="Mid-price calculation resulted in non-finite value",
                    )
                    mid_price_result = None
            except InvalidOperation:  # Catch only InvalidOperation for calculation issues
                # Should not happen if inputs are finite Decimals, but defensive
                logger.exception(
                    "mid_price_calculation_error",
                    symbol=self.symbol.value,
                    exchange=self.symbol.exchange,
                    bid=self.bid,
                    ask=self.ask,
                    message="Error calculating mid-price",
                )
                mid_price_result = None
            else:
                return mid_price_result
        return None  # Return None if bid or ask is None or non-finite


class HyperliquidTickerDetails(BaseModel):
    """Hyperliquid-specific ticker enrichment fields for extension slot on Ticker.

    Fields:
        mid_price_source (Optional[str]): Source of the mid-price calculation (e.g., 'allMids')
    """

    mid_price_source: str | None = Field(default=None)

    model_config = ConfigDict(extra="ignore", frozen=True)


class BackpackTickerDetails(BaseModel):
    """Backpack-specific ticker enrichment fields for extension slot on Ticker.

    Preserves the rich 24-hour ticker statistics provided by Backpack's REST API
    that are not part of the core ticker model.

    Fields:
        first_price (Optional[Decimal]): Opening price (24h ago)
        high (Optional[Decimal]): Highest price in 24h
        low (Optional[Decimal]): Lowest price in 24h
        price_change (Optional[Decimal]): Absolute price change (lastPrice - firstPrice)
        price_change_percent (Optional[Decimal]): Percentage change
        quote_volume (Optional[Decimal]): Quote asset volume (24h)
        trades (Optional[int]): Number of trades (24h)
    """

    first_price: Decimal | None = Field(default=None, ge=Decimal(0))
    high: Decimal | None = Field(default=None, ge=Decimal(0))
    low: Decimal | None = Field(default=None, ge=Decimal(0))
    price_change: Decimal | None = Field(default=None)  # Can be negative
    price_change_percent: Decimal | None = Field(default=None)  # Can be negative
    quote_volume: Decimal | None = Field(default=None, ge=Decimal(0))
    trades: int | None = Field(default=None, ge=0)

    model_config = ConfigDict(extra="ignore", frozen=True)
