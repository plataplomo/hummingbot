"""Market data service protocol for accessing current market information.

This protocol defines the interface for market data services that provide
current market information needed for validation and order execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable


if TYPE_CHECKING:
    from cyberdelta.models.market.market_snapshot import MarketSnapshot
    from cyberdelta.models.market.order_book import OrderBook
    from cyberdelta.models.market.ticker import Ticker
    from cyberdelta.symbols.models import Symbol


@runtime_checkable
class MarketDataServiceProtocol(Protocol):
    """Protocol for market data services that provide current market information.

    This protocol ensures type safety for market data access needed for
    validation rules like price precision, market status, and liquidity checks.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO assumptions about data availability
    - Type-safe access to market data
    - Clear contract for market information
    """

    async def get_ticker(self, symbol: Symbol) -> Ticker:
        """Get current ticker for a symbol.

        Args:
            symbol: Symbol to get ticker for

        Returns:
            Current ticker with price and status information, or None if unavailable

        Note:
            - This method may return None if market data is temporarily unavailable
            - Validation rules should handle None gracefully by skipping validation
        """
        ...

    async def get_order_book(self, symbol: Symbol) -> OrderBook:
        """Get current order book for a symbol.

        Args:
            symbol: Symbol to get order book for

        Returns:
            Current order book with bid/ask data, or None if unavailable

        Note:
            - This method may return None if order book data is temporarily unavailable
            - Validation rules should handle None gracefully by skipping liquidity checks
        """
        ...

    async def create_market_snapshot(self, symbol: Symbol) -> MarketSnapshot:
        """Create a market snapshot for validation purposes.

        Args:
            symbol: Symbol to create snapshot for

        Returns:
            MarketSnapshot with current ticker and order book data, or None if unavailable

        Note:
            - This is a convenience method that combines ticker and order book data
            - May return None if underlying market data is unavailable
            - Validation rules will skip market-dependent checks if None
        """
        ...
