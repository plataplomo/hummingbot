"""Market data caching manager.

This module handles caching of market data with TTL-based validation
and stale data detection.
"""

from __future__ import annotations

from datetime import UTC, datetime

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker


logger = get_logger(__name__)


class CacheManager:
    """Market data cache manager with TTL-based validation.

    This class handles:
    - Ticker and order book caching with timestamps
    - TTL-based cache validation
    - Stale data detection
    - Cache clearing operations

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL TTL values from AppSettings configuration
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - NO hardcoded cache durations
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize cache manager with configuration.

        Args:
            config: Application settings containing cache configuration
        """
        self.config = config
        self._monitoring_config = config.monitoring

        # Cache settings from config
        self._cache_ttl = self._monitoring_config.market_data.cache.default_ttl
        self._stale_threshold = self._monitoring_config.market_data.cache.stale_while_revalidate

        # Market data cache with TTL
        self._ticker_cache: dict[str, tuple[Ticker, datetime]] = {}
        self._order_book_cache: dict[str, tuple[OrderBook, datetime]] = {}

        logger.debug(
            "cache_manager_initialized",
            cache_ttl_seconds=self._cache_ttl,
            stale_threshold_seconds=self._stale_threshold,
        )

    def get_ticker(self, symbol: Symbol, exchange: ExchangeName) -> Ticker | None:
        """Get ticker from cache if valid.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Cached ticker if valid, None if not cached or expired
        """
        cache_key = f"{exchange.value}:{symbol.value}"

        if cache_key not in self._ticker_cache:
            return None

        ticker, cached_at = self._ticker_cache[cache_key]
        age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

        if age_seconds < self._cache_ttl:
            logger.debug(
                "ticker_served_from_cache",
                symbol=symbol.value,
                exchange=exchange.value,
                age_seconds=age_seconds,
            )
            return ticker

        # Remove expired entry
        del self._ticker_cache[cache_key]
        logger.debug(
            "ticker_cache_expired",
            symbol=symbol.value,
            exchange=exchange.value,
            age_seconds=age_seconds,
            ttl_seconds=self._cache_ttl,
        )
        return None

    def cache_ticker(self, symbol: Symbol, exchange: ExchangeName, ticker: Ticker) -> None:
        """Cache ticker with current timestamp.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            ticker: Ticker to cache
        """
        cache_key = f"{exchange.value}:{symbol.value}"
        self._ticker_cache[cache_key] = (ticker, datetime.now(UTC))

        logger.debug(
            "ticker_cached",
            symbol=symbol.value,
            exchange=exchange.value,
            price=float(ticker.price) if ticker.price else None,
        )

    def get_order_book(self, symbol: Symbol, exchange: ExchangeName) -> OrderBook | None:
        """Get order book from cache if valid.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            Cached order book if valid, None if not cached or expired
        """
        cache_key = f"{exchange.value}:{symbol.value}"

        if cache_key not in self._order_book_cache:
            return None

        order_book, cached_at = self._order_book_cache[cache_key]
        age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

        if age_seconds < self._cache_ttl:
            return order_book

        # Remove expired entry
        del self._order_book_cache[cache_key]
        return None

    def cache_order_book(
        self, symbol: Symbol, exchange: ExchangeName, order_book: OrderBook
    ) -> None:
        """Cache order book with current timestamp.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            order_book: Order book to cache
        """
        cache_key = f"{exchange.value}:{symbol.value}"
        self._order_book_cache[cache_key] = (order_book, datetime.now(UTC))

    def is_data_stale(self, timestamp: datetime) -> bool:
        """Check if data is stale based on configured threshold.

        Args:
            timestamp: Timestamp of the data

        Returns:
            True if data is stale, False otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured stale threshold, NO hardcoded values
        """
        age_seconds = (datetime.now(UTC) - timestamp).total_seconds()
        return age_seconds > self._stale_threshold

    def get_ticker_count(self) -> int:
        """Get count of cached tickers.

        Returns:
            Number of tickers currently cached
        """
        return len(self._ticker_cache)

    def get_order_book_count(self) -> int:
        """Get count of cached order books.

        Returns:
            Number of order books currently cached
        """
        return len(self._order_book_cache)

    def clear_cache(self) -> None:
        """Clear all cached market data.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit cache clearing
        - Structured logging of operation
        """
        ticker_count = len(self._ticker_cache)
        order_book_count = len(self._order_book_cache)

        self._ticker_cache.clear()
        self._order_book_cache.clear()

        logger.info(
            "market_data_cache_cleared",
            ticker_count=ticker_count,
            order_book_count=order_book_count,
        )
