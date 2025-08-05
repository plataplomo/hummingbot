"""Market snapshot aggregation.

This module handles aggregating market data from multiple exchanges
into unified market snapshots with parallel data collection.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.logic.market.data_fetcher import DataFetcher
from cyberdelta.logic.market.exchange_connector import ExchangeConnector
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker


logger = get_logger(__name__)


class MarketAggregator:
    """Market data aggregator for multi-exchange snapshots.

    This class handles:
    - Multi-exchange data collection coordination
    - Parallel data fetching from enabled exchanges
    - Market snapshot creation and assembly
    - Error handling for failed exchange data

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL timeouts from exchange configurations
    - NO assumptions about data availability
    - Returns typed MarketSnapshot, NOT dict
    - Continues with partial data on exchange failures
    """

    def __init__(self, exchange_connector: ExchangeConnector, data_fetcher: DataFetcher) -> None:
        """Initialize market aggregator with dependencies.

        Args:
            exchange_connector: Exchange connection manager
            data_fetcher: Market data fetcher
        """
        self._exchange_connector = exchange_connector
        self._data_fetcher = data_fetcher

        logger.debug("market_aggregator_initialized")

    async def create_market_snapshot(self) -> MarketSnapshot:
        """Create aggregated market snapshot across all enabled exchanges.

        Returns:
            MarketSnapshot with current market data from all exchanges

        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL timeouts from config, NO hardcoded values
        - Uses Symbol/ExchangeName types consistently
        - Returns typed MarketSnapshot, NOT dict
        - NO assumptions about data availability
        """
        logger.debug("market_snapshot_request_starting")

        all_tickers: dict[str, Ticker] = {}
        all_order_books: dict[str, OrderBook] = {}

        # Get enabled exchanges
        enabled_exchanges = self._exchange_connector.get_enabled_exchanges()

        # Fetch from all enabled exchanges in parallel
        tasks: list[tuple[str, asyncio.Task[tuple[dict[str, Ticker], dict[str, OrderBook]]]]] = []
        for exchange_name, exchange_config in enabled_exchanges.items():
            api_client = self._exchange_connector.get_api_client(exchange_name)
            if api_client:
                task = asyncio.create_task(
                    self._data_fetcher.fetch_exchange_data(
                        api_client, exchange_name, exchange_config
                    )
                )
                tasks.append((exchange_name, task))

        # Wait for all exchanges with individual timeouts
        for exchange_name, task in tasks:
            try:
                exchange_tickers, exchange_order_books = await task

                # Merge into aggregated data
                all_tickers.update(exchange_tickers)
                all_order_books.update(exchange_order_books)

                logger.debug(
                    "exchange_data_fetched",
                    exchange=exchange_name,
                    ticker_count=len(exchange_tickers),
                    order_book_count=len(exchange_order_books),
                )

            except Exception as e:
                logger.exception("exchange_data_fetch_failed", exchange=exchange_name, error=str(e))
                # Continue with other exchanges - NO silent failures

        snapshot = MarketSnapshot(
            tickers=all_tickers, order_books=all_order_books, timestamp=datetime.now(UTC)
        )

        logger.info(
            "market_snapshot_created",
            total_tickers=len(all_tickers),
            total_order_books=len(all_order_books),
            exchanges_included=len([name for name, _ in tasks]),
        )

        return snapshot
