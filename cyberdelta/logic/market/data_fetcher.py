"""Market data fetching operations.

This module handles fetching market data from exchange APIs
including individual tickers, bulk operations, and order books.
"""

from __future__ import annotations

import asyncio

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.config.models import AppSettings, ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker


logger = get_logger(__name__)


class DataFetcher:
    """Market data fetcher for exchange APIs.

    This class handles:
    - Individual ticker fetching from APIs
    - Bulk ticker and order book operations
    - Per-exchange data fetching with timeouts
    - Error handling for failed API calls

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL timeouts from AppSettings configuration
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - NO assumptions about API availability
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize data fetcher with configuration.

        Args:
            config: Application settings containing fetch configuration
        """
        self.config = config
        self._monitoring_config = config.monitoring

        # Fetch settings from config
        self._max_order_books = (
            self._monitoring_config.market_data.cache.max_order_books_per_exchange
        )
        self._order_book_depth = self._monitoring_config.market_data.fetch.order_book_depth

        logger.debug(
            "data_fetcher_initialized",
            max_order_books=self._max_order_books,
            order_book_depth=self._order_book_depth,
        )

    async def fetch_ticker(
        self, api_client: ExchangeAPI, symbol: Symbol, exchange: ExchangeName
    ) -> Ticker | None:
        """Fetch ticker from exchange API.

        Args:
            api_client: Exchange API client
            symbol: Symbol to fetch
            exchange: Exchange name

        Returns:
            Ticker if successful, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol object, adapts to API client needs
        - NO assumptions about API client interface
        - Returns typed Ticker, NOT dict
        """
        try:
            # ExchangeAPI.get_ticker expects a Symbol object
            return await api_client.get_ticker(symbol)

        except Exception as e:
            logger.exception(
                "ticker_api_fetch_error", symbol=symbol.value, exchange=exchange.value, error=str(e)
            )
            return None

    async def fetch_all_tickers(
        self, api_client: ExchangeAPI, exchange_name: str
    ) -> dict[str, Ticker]:
        """Fetch all tickers for an exchange.

        Args:
            api_client: Exchange API client
            exchange_name: Name of the exchange

        Returns:
            Dictionary of tickers keyed by "{exchange}:{symbol}"

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about available symbols
        - Returns empty dict on failure, not None
        """
        try:
            tickers: dict[str, Ticker] = {}

            # Get all markets first to know which symbols are available
            markets = await api_client.get_markets(GetMarketsArgs())

            # Fetch ticker for each market
            # Note: This could be optimized if exchanges provide bulk ticker endpoints
            for market in markets:
                try:
                    ticker = await api_client.get_ticker(market.symbol)
                    if ticker:
                        key = f"{exchange_name}:{market.symbol.value}"
                        tickers[key] = ticker
                except (TimeoutError, ConnectionError, OSError) as e:
                    logger.debug(
                        "ticker_fetch_failed_for_symbol",
                        exchange=exchange_name,
                        symbol=market.symbol.value,
                        error=str(e),
                    )
                except Exception as e:
                    logger.exception(
                        "ticker_fetch_unexpected_error_for_symbol",
                        exchange=exchange_name,
                        symbol=market.symbol.value,
                        error=str(e),
                    )
                    # Continue with other symbols - don't let one symbol break all

            logger.debug("all_tickers_fetched", exchange=exchange_name, count=len(tickers))

        except Exception as e:
            logger.exception("all_tickers_fetch_error", exchange=exchange_name, error=str(e))
            return {}

        return tickers

    async def fetch_all_order_books(
        self, api_client: ExchangeAPI, exchange_name: str
    ) -> dict[str, OrderBook]:
        """Fetch all order books for an exchange.

        Args:
            api_client: Exchange API client
            exchange_name: Name of the exchange

        Returns:
            Dictionary of order books keyed by "{exchange}:{symbol}"

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about available symbols
        - Returns empty dict on failure, not None
        """
        order_books: dict[str, OrderBook] = {}
        try:
            # Get all markets to know which symbols are available
            markets = await api_client.get_markets(GetMarketsArgs())

            # Fetch order book for each market - limit from config
            # to avoid overwhelming the API
            for market in markets[: self._max_order_books]:
                try:
                    order_book = await api_client.get_order_book(
                        market.symbol, depth=self._order_book_depth
                    )
                    if order_book:
                        key = f"{exchange_name}:{market.symbol.value}"
                        order_books[key] = order_book
                except (TimeoutError, ConnectionError, OSError) as e:
                    logger.debug(
                        "order_book_fetch_failed_for_symbol",
                        exchange=exchange_name,
                        symbol=market.symbol.value,
                        error=str(e),
                    )
                except Exception as e:
                    logger.exception(
                        "order_book_fetch_unexpected_error_for_symbol",
                        exchange=exchange_name,
                        symbol=market.symbol.value,
                        error=str(e),
                    )
                    # Continue with other symbols - don't let one symbol break all

        except Exception as e:
            logger.exception("order_books_fetch_error", exchange=exchange_name, error=str(e))
            return order_books

        return order_books

    async def fetch_exchange_data(
        self, api_client: ExchangeAPI, exchange_name: str, exchange_config: ExchangeSpecificConfig
    ) -> tuple[dict[str, Ticker], dict[str, OrderBook]]:
        """Fetch all market data for a specific exchange.

        Args:
            api_client: Exchange API client
            exchange_name: Name of the exchange
            exchange_config: Exchange-specific configuration

        Returns:
            Tuple of (tickers dict, order_books dict)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout for this exchange
        - NO assumptions about available symbols
        - Returns empty dicts on failure, not None
        """
        timeout = exchange_config.request_timeout_seconds

        try:
            # Fetch tickers and order books with timeout
            tickers_task = asyncio.create_task(self.fetch_all_tickers(api_client, exchange_name))
            order_books_task = asyncio.create_task(
                self.fetch_all_order_books(api_client, exchange_name)
            )

            tickers, order_books = await asyncio.wait_for(
                asyncio.gather(tickers_task, order_books_task), timeout=timeout
            )

        except Exception as e:
            logger.exception(
                "exchange_data_fetch_error", exchange=exchange_name, timeout=timeout, error=str(e)
            )
            return {}, {}
        else:
            return tickers, order_books
