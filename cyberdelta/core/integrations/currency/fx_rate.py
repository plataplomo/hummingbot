"""FX rate data model."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.exceptions.service import CurrencyConverterError


@dataclass
class FXRate:
    """Foreign exchange rate."""

    from_currency: str = Field(description="Source currency code")
    to_currency: str = Field(description="Target currency code")
    rate: Decimal = Field(gt=0, description="Exchange rate")
    timestamp: float = Field(gt=0, description="Rate timestamp")
    source: str = Field(
        description="Rate source: market, fixed, or derived"
    )  # e.g., "market", "fixed", "derived"
    bid: Decimal | None = Field(default=None, gt=0, description="Bid price")
    ask: Decimal | None = Field(default=None, gt=0, description="Ask price")
    mid: Decimal | None = Field(default=None, gt=0, description="Mid price")

    @field_validator("from_currency", "to_currency", mode="before")
    @classmethod
    def validate_currency_code(cls, v: str) -> str:
        """Validate currency codes are uppercase and non-empty.

        Args:
            v: Currency code to validate

        Returns:
            Uppercase, trimmed currency code

        Raises:
            CurrencyConverterError: If currency code is empty or invalid
        """
        if not v or not v.strip():
            raise CurrencyConverterError(
                operation_type="currency_validation", requirement="currency code cannot be empty"
            )
        return v.upper().strip()

    @field_validator("source", mode="before")
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Validate source is one of allowed values.

        Args:
            v: Source value to validate

        Returns:
            Validated source value

        Raises:
            CurrencyConverterError: If source is not one of: market, fixed, derived
        """
        valid_sources = {"market", "fixed", "derived"}
        if v not in valid_sources:
            raise CurrencyConverterError(
                operation_type="source_validation",
                requirement=f"source must be one of: {', '.join(valid_sources)}",
            )
        return v

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def validate_bid_ask(cls, v: Decimal | None, info: ValidationInfo) -> Decimal | None:
        """Validate bid/ask relationship.

        Args:
            v: Bid or ask price to validate
            info: Validation context with field name and other data

        Returns:
            Validated price or None if input is None

        Raises:
            CurrencyConverterError: If ask price is not greater than bid price
        """
        if v is None:
            return v
        if (
            info.field_name == "ask"
            and "bid" in info.data
            and info.data["bid"] is not None
            and v <= info.data["bid"]
        ):
            raise CurrencyConverterError(
                operation_type="price_validation",
                requirement="ask price must be greater than bid price",
            )
        return v

    @property
    def age_seconds(self) -> float:
        """Get age of rate in seconds."""
        return time.time() - self.timestamp

    @property
    def spread(self) -> Decimal | None:
        """Get bid-ask spread if available."""
        if self.bid and self.ask:
            return self.ask - self.bid
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of the FX rate with all fields
        """
        return {
            "from_currency": self.from_currency,
            "to_currency": self.to_currency,
            "rate": str(self.rate),
            "timestamp": self.timestamp,
            "source": self.source,
            "bid": str(self.bid) if self.bid else None,
            "ask": str(self.ask) if self.ask else None,
            "mid": str(self.mid) if self.mid else None,
            "age_seconds": self.age_seconds,
        }