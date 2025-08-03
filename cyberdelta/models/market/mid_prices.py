"""Mid prices model for multiple symbols."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName


class MidPrices(BaseModel):
    """Mid prices for multiple symbols from an exchange.

    Represents a collection of mid prices (best bid + best ask / 2)
    for multiple symbols at a point in time. This is typically used
    for efficient batch price fetching and market order pricing.
    """

    prices: dict[Symbol, Decimal] = Field(description="Symbol to mid price mapping")
    timestamp: datetime | None = Field(default=None, description="When prices were captured")
    exchange: ExchangeName = Field(description="Source exchange name")

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
