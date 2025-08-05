"""Market snapshot model for aggregated market data.

This module provides the MarketSnapshot model which represents a type-safe
aggregated view of market data across all exchanges at a point in time.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker


class MarketSnapshot(BaseModel):
    """Type-safe aggregated market data across all exchanges.

    This provides a consistent view of market state at a point in time,
    with helper methods for type-safe access to specific exchange/symbol data.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - Uses Symbol objects, not strings
    - Uses ExchangeName enum, not strings
    - Provides type-safe accessors
    """

    # Using Dict with string keys as we need "{exchange}:{symbol}" format
    tickers: dict[str, Ticker] = Field(
        description='Tickers keyed by "{exchange}:{symbol}" e.g. "hyperliquid:BTC"'
    )

    order_books: dict[str, OrderBook] = Field(
        description='Order books keyed by "{exchange}:{symbol}" e.g. "backpack:ETH"'
    )

    timestamp: datetime = Field(description="UTC timestamp of this market snapshot")

    def get_ticker(self, exchange: ExchangeName, symbol: Symbol) -> Ticker | None:
        """Get ticker for specific exchange and symbol.

        Args:
            exchange: Exchange to query (uses ExchangeName enum)
            symbol: Symbol to query (uses Symbol object)

        Returns:
            Ticker if found, None otherwise
        """
        key = f"{exchange.value}:{symbol.value}"
        return self.tickers.get(key)

    def get_order_book(self, exchange: ExchangeName, symbol: Symbol) -> OrderBook | None:
        """Get order book for specific exchange and symbol.

        Args:
            exchange: Exchange to query (uses ExchangeName enum)
            symbol: Symbol to query (uses Symbol object)

        Returns:
            OrderBook if found, None otherwise
        """
        key = f"{exchange.value}:{symbol.value}"
        return self.order_books.get(key)

    def get_all_tickers_for_symbol(self, symbol: Symbol) -> dict[ExchangeName, Ticker]:
        """Get all tickers for a symbol across all exchanges.

        Args:
            symbol: Symbol to query (uses Symbol object)

        Returns:
            Dictionary mapping exchange to ticker
        """
        result: dict[ExchangeName, Ticker] = {}
        symbol_suffix = f":{symbol.value}"

        for key, ticker in self.tickers.items():
            if key.endswith(symbol_suffix):
                exchange_str = key.split(":")[0]
                # Convert string back to ExchangeName enum
                try:
                    exchange = ExchangeName(exchange_str)
                    result[exchange] = ticker
                except ValueError:
                    # Skip invalid exchange names
                    pass

        return result

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True
