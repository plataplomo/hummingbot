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

from pydantic import Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.base_validators import (
    ExchangeValidationMixin,
    ExtensionSlotModel,
    ImmutableModel,
    optional_decimal_validator,
    required_datetime_validator,
)
from cyberdelta.symbols.models import Symbol


# Instantiate logger for this module
logger = get_logger(__name__)


class Ticker(ExchangeValidationMixin, ImmutableModel):
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

    # Config: Immutable (inherited from ImmutableModel)
    # Exchange validation: ExchangeValidationMixin provides validate_exchange()

    # Symbol validation is handled by Pydantic's type system
    # No need for a custom validator since Symbol is always valid

    # Exchange validation provided by ExchangeValidationMixin

    _validate_timestamp = required_datetime_validator("timestamp")
    _validate_optional_decimals = optional_decimal_validator("price", "bid", "ask", "volume")

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


class HyperliquidTickerDetails(ExtensionSlotModel):
    """Hyperliquid-specific ticker enrichment fields for extension slot on Ticker.

    Fields:
        mid_price_source (Optional[str]): Source of the mid-price calculation (e.g., 'allMids')
    """

    mid_price_source: str | None = Field(default=None)

    # Config: Immutable (inherited from ExtensionSlotModel)


class BackpackTickerDetails(ExtensionSlotModel):
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

    # Config: Immutable (inherited from ExtensionSlotModel)
