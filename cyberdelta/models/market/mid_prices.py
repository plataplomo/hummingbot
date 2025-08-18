"""Mid prices model for multiple symbols."""

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, field_serializer

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.base_validators import ExchangeValidationMixin, StandardModel
from cyberdelta.symbols.models import Symbol


class MidPrices(ExchangeValidationMixin, StandardModel):
    """Mid prices for multiple symbols from an exchange.

    Represents a collection of mid prices (best bid + best ask / 2)
    for multiple symbols at a point in time. This is typically used
    for efficient batch price fetching and market order pricing.

    NOTE: Due to Pydantic limitation (https://github.com/pydantic/pydantic/issues/5711),
    Symbol objects as dict keys require custom serialization for model_dump().
    """

    prices: dict[Symbol, Decimal] = Field(description="Symbol to mid price mapping")
    timestamp: datetime | None = Field(default=None, description="When prices were captured")
    exchange: ExchangeName = Field(description="Source exchange name")

    @field_serializer("prices")
    def serialize_prices(self, prices: dict[Symbol, Decimal]) -> list[dict[str, Any]]:
        """Serialize prices dict to list of dicts for JSON compatibility.

        This is required because JSON doesn't support complex objects as keys.
        See: https://github.com/pydantic/pydantic/issues/5711

        Returns:
            List of dicts with symbol and price data for JSON serialization.
        """
        return [
            {"symbol": symbol.model_dump(), "price": str(price)} for symbol, price in prices.items()
        ]

    def get(self, symbol: Symbol) -> Decimal | None:
        """Get mid price for symbol.

        Args:
            symbol: Trading symbol to look up

        Returns:
            Mid price as Decimal, or None if symbol not found
        """
        return self.prices.get(symbol)

    def symbols(self) -> list[Symbol]:
        """Get list of available symbols.

        Returns:
            List of symbols with prices
        """
        return list(self.prices.keys())

    def __len__(self) -> int:
        """Number of symbols with prices.

        Returns:
            Count of symbols in the snapshot
        """
        return len(self.prices)

    def has_symbol(self, symbol: Symbol) -> bool:
        """Check if symbol exists in snapshot.

        Args:
            symbol: Trading symbol to check

        Returns:
            True if symbol has a price, False otherwise
        """
        return symbol in self.prices
