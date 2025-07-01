"""Market metadata model for trading symbol information.

This module provides the Market model for representing immutable snapshots
of market configuration including tick sizes, trading limits, and status.

The Market model ensures data integrity through:
- Strict validation of all price and quantity fields
- Decimal precision for financial calculations
- Immutable design to prevent accidental modification
- Optional fields with non-negative constraints
- Exchange-specific extension slots for additional market data
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions.field_validation import DecimalFiniteError
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


# Instantiate logger for this module
logger = get_logger(__name__)


class Market(BaseModel):
    """Represents an immutable, validated snapshot of market metadata for a trading symbol.

    Provides core market configuration including tick sizes, trading limits, and status,
    ensuring data integrity through strict validation and Decimal usage for financial precision.
    Supports exchange-specific extension slots for preserving additional market data.

    Attributes:
        symbol: Trading symbol (validated: required, non-empty, max 64 chars, UTF-8).
        base_symbol: Base asset symbol (e.g., "SOL").
        quote_symbol: Quote asset symbol (e.g., "USDC").
        market_type: Market type (e.g., "Spot", "Perpetual").
        tick_size: Minimum price increment. Must be positive.
        step_size: Minimum quantity increment. Must be positive.
        min_price: Minimum allowed price. Must be non-negative if provided.
        max_price: Maximum allowed price. Must be non-negative if provided.
        min_quantity: Minimum allowed quantity. Must be non-negative if provided.
        max_quantity: Maximum allowed quantity. Must be non-negative if provided.
        status: Market status (e.g., "Trading", "Halted").
        created_at: Market creation timestamp (optional).
        bp_details: Backpack-specific market enrichment (optional).
        hl_details: Hyperliquid-specific market enrichment (optional).

    Configuration:
        - `frozen=True`: Guarantees immutability.
        - `extra='forbid'`: Prevents unexpected fields.
        - `validate_assignment=True`: Ensures validation on assignment.

    """

    symbol: str
    base_symbol: str
    quote_symbol: str
    market_type: str
    tick_size: Decimal = Field(gt=Decimal(0))
    step_size: Decimal = Field(gt=Decimal(0))
    min_price: Decimal | None = Field(default=None, ge=Decimal(0))
    max_price: Decimal | None = Field(default=None, ge=Decimal(0))
    min_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    max_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    status: str
    created_at: datetime | None = Field(default=None)
    bp_details: BackpackMarketDetails | None = Field(default=None)
    hl_details: HyperliquidMarketDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator(
        "symbol",
        "base_symbol",
        "quote_symbol",
        "market_type",
        "status",
        mode="before",
    )
    @classmethod
    def validate_string_fields(cls, v: object, info: ValidationInfo) -> str:
        """Validate string fields."""
        field_name = info.field_name if info.field_name is not None else "unknown_field"
        return validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)

    @field_validator("created_at", mode="before")
    @classmethod
    def validate_created_at(cls, v: datetime | float | str | None) -> datetime | None:
        """Validate and parse the 'created_at' field to an optional UTC datetime object."""
        if v is None:
            return None
        return parse_datetime_utc(v, field_name="created_at")

    @field_validator(
        "tick_size",
        "step_size",
        "min_price",
        "max_price",
        "min_quantity",
        "max_quantity",
        mode="before",
    )
    @classmethod
    def validate_and_parse_decimal_fields(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Validate, parse, and check finiteness for Decimal fields.

        Args:
            v: The raw input value (can be various numeric types or None).
            info: Pydantic validation context. Used for field name in error messages.

        Returns:
            The parsed Decimal value if input is valid, None if input is None for optional fields.

        Raises:
            ValueError: If input cannot be parsed to a finite Decimal.

        """
        field_name = info.field_name if info.field_name is not None else "unknown_field"

        # tick_size and step_size are required, others are optional
        allow_none = field_name not in {"tick_size", "step_size"}

        parsed_decimal = parse_decimal_value(v, allow_none=allow_none, field_name=field_name)

        # Ensure non-None results are finite
        if parsed_decimal is not None and not parsed_decimal.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed_decimal,
                context="for market configuration",
            )

        return parsed_decimal


class BackpackMarketDetails(BaseModel):
    """Backpack-specific market enrichment fields for extension slot on Market.

    Preserves additional market configuration provided by Backpack's API
    that are not part of the core market model.

    Fields:
        order_book_state: Current order book state (e.g., "Live", "Paused")
        created_at_raw: Raw creation timestamp from API
    """

    order_book_state: str | None = Field(default=None)
    created_at_raw: str | None = Field(default=None)

    model_config = ConfigDict(extra="ignore", frozen=True)


class HyperliquidMarketDetails(BaseModel):
    """Hyperliquid-specific market enrichment fields for extension slot on Market.

    Contains Hyperliquid-specific trading rules and current market state.

    Attributes:
        max_leverage: Maximum leverage allowed for this asset (1-1000).
        only_isolated: True if only isolated margin is allowed (optional).
        sz_decimals: Number of decimals for size/quantity precision (0-18).
        mark_price: Current mark price (optional, from asset context).
        funding_rate: Current funding rate (optional, from asset context).
    """

    max_leverage: int = Field(ge=1, le=1000)
    only_isolated: bool | None = Field(default=None)
    sz_decimals: int = Field(ge=0, le=18)
    mark_price: Decimal | None = Field(default=None, ge=Decimal(0))
    funding_rate: Decimal | None = Field(default=None)

    model_config = ConfigDict(extra="ignore", frozen=True)
