"""Backpack Market Data Request Builder.

This module handles the construction of request payloads for market data operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- Price ticker requests
- Order book queries
- Recent trades requests
- Market metadata requests
- Historical data requests (funding rates, klines)
"""

from __future__ import annotations

from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetFundingRateParams,
    BackpackRawGetHistoricalFundingRatesParams,
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetMarketParams,
    BackpackRawGetMarketsParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
)
from cyberdelta.apis.backpack.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.exceptions import MissingRequiredParameterError
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class BackpackMarketDataRequestBuilder(MarketDataRequestBuilderProtocol):
    """Focused request builder for Backpack market data operations.

    This class contains static methods for constructing validated request payloads
    for all market data related API endpoints.
    """

    # Constants
    MAX_CANDLE_LIMIT = 1500

    def __init__(self) -> None:
        """Initialize the market data request builder."""
        logger.debug("Initializing Backpack market data request builder")

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Generic request builder dispatch method.

        This method serves as the entry point for the registry system
        and dispatches to the appropriate specific builder method based on context.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments including 'operation' to specify the request type

        Returns:
            Request payload dictionary

        Raises:
            NotImplementedError: If operation is not supported or parameters are insufficient
        """
        operation = kwargs.get("operation")
        if not operation:
            raise NotImplementedError(
                "Market data request builder requires 'operation' parameter for "
                "generic build_request"
            )

        # Note: Market data request builders require specific parameters for each operation
        # which are not available in the generic build_request interface.
        # This dispatcher is implemented for protocol consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "get_ticker",
            "get_orderbook",
            "get_klines",
            "get_markets",
            "get_recent_trades",
            "get_funding_rate",
            "get_market_data",
        }:
            # Market data operations require specific parameters not available in generic interface
            raise NotImplementedError(
                f"Market data operation '{operation}' requires specific parameters not available "
                f"in generic build_request interface. Use specific builder methods directly."
            )
        raise NotImplementedError(
            f"Market data operation '{operation}' not supported by registry dispatch"
        )

    @staticmethod
    def format_symbol(symbol: str) -> str:
        """Format symbol according to Backpack API requirements.

        Args:
            symbol: Raw symbol string

        Returns:
            Formatted symbol string
        """
        # Backpack expects symbols in uppercase with underscores (e.g., SOL_USDC)
        return symbol.replace("-", "_").upper()

    @staticmethod
    def build_get_ticker_params(symbol: str) -> BackpackRawGetTickerParams:
        """Build query parameters for fetching ticker data.

        Args:
            symbol: Trading symbol

        Returns:
            BackpackRawGetTickerParams: Validated query parameters
        """
        logger.debug(
            "building_get_ticker_params",
            symbol=symbol,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        return BackpackRawGetTickerParams(symbol=formatted_symbol)

    @staticmethod
    def build_get_order_book_params(
        symbol: str,
        depth: int | None = None,
    ) -> BackpackRawGetOrderBookParams:
        """Build query parameters for fetching order book data.

        Args:
            symbol: Trading symbol
            depth: Optional depth limit for order book levels

        Returns:
            BackpackRawGetOrderBookParams: Validated query parameters
        """
        logger.debug(
            "building_get_order_book_params",
            symbol=symbol,
            depth=depth,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        params_dict: dict[str, Any] = {"symbol": formatted_symbol}

        if depth is not None:
            params_dict["limit"] = depth

        return BackpackRawGetOrderBookParams(**params_dict)

    @staticmethod
    def build_get_recent_trades_params(
        symbol: str,
        limit: int | None = None,
    ) -> BackpackRawGetRecentTradesParams:
        """Build query parameters for fetching recent trades.

        Args:
            symbol: Trading symbol
            limit: Optional limit on number of trades

        Returns:
            BackpackRawGetRecentTradesParams: Validated query parameters
        """
        logger.debug(
            "building_get_recent_trades_params",
            symbol=symbol,
            limit=limit,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        params_dict: dict[str, Any] = {"symbol": formatted_symbol}

        if limit is not None:
            params_dict["limit"] = limit

        return BackpackRawGetRecentTradesParams(**params_dict)

    @staticmethod
    def build_get_markets_params() -> BackpackRawGetMarketsParams:
        """Build query parameters for fetching all markets.

        Returns:
            BackpackRawGetMarketsParams: Empty params object (no parameters needed)
        """
        logger.debug("building_get_markets_params")
        return BackpackRawGetMarketsParams()

    @staticmethod
    def build_get_market_params(symbol: str) -> BackpackRawGetMarketParams:
        """Build query parameters for fetching a specific market.

        Args:
            symbol: Trading symbol

        Returns:
            BackpackRawGetMarketParams: Validated query parameters
        """
        logger.debug(
            "building_get_market_params",
            symbol=symbol,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        return BackpackRawGetMarketParams(symbol=formatted_symbol)

    @staticmethod
    def build_get_funding_rate_params(symbol: str) -> BackpackRawGetFundingRateParams:
        """Build query parameters for fetching current funding rate.

        Args:
            symbol: Trading symbol

        Returns:
            BackpackRawGetFundingRateParams: Validated query parameters
        """
        logger.debug(
            "building_get_funding_rate_params",
            symbol=symbol,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        return BackpackRawGetFundingRateParams(symbol=formatted_symbol)

    @staticmethod
    def build_get_historical_funding_rates_params(
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
    ) -> BackpackRawGetHistoricalFundingRatesParams:
        """Build query parameters for fetching historical funding rates.

        Args:
            symbol: Trading symbol
            start_time: Optional start timestamp (milliseconds)
            end_time: Optional end timestamp (milliseconds)
            limit: Maximum number of results (default 100)

        Returns:
            BackpackRawGetHistoricalFundingRatesParams: Validated query parameters
        """
        logger.debug(
            "building_get_historical_funding_rates_params",
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        params_dict: dict[str, Any] = {
            "symbol": formatted_symbol,
            "limit": limit,
        }

        if start_time is not None:
            params_dict["startTime"] = start_time
        if end_time is not None:
            params_dict["endTime"] = end_time

        return BackpackRawGetHistoricalFundingRatesParams(**params_dict)

    @staticmethod
    def build_get_market_data_params(
        symbol: str,
        interval: str,
        start_time: int,
        end_time: int | None = None,
        limit: int = 500,
    ) -> BackpackRawGetMarketDataParams:
        """Build query parameters for fetching historical market data (klines).

        Args:
            symbol: Trading symbol
            interval: Kline interval (e.g., "1m", "1h", "1d")
            start_time: Start timestamp (milliseconds)
            end_time: Optional end timestamp (milliseconds)
            limit: Maximum number of results (default 500, max 1500)

        Returns:
            BackpackRawGetMarketDataParams: Validated query parameters
        """
        logger.debug(
            "building_get_market_data_params",
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

        # Validate interval
        valid_intervals = [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        ]
        if interval not in valid_intervals:
            raise MissingRequiredParameterError(
                parameter_name="interval",
                operation=f"get market data (valid intervals: {', '.join(valid_intervals)})",
            )

        # Validate limit
        if limit > BackpackMarketDataRequestBuilder.MAX_CANDLE_LIMIT:
            logger.warning(
                "market_data_limit_exceeded",
                requested_limit=limit,
                max_limit=BackpackMarketDataRequestBuilder.MAX_CANDLE_LIMIT,
                message="Capping limit to maximum allowed value",
            )
            limit = BackpackMarketDataRequestBuilder.MAX_CANDLE_LIMIT

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        params_dict: dict[str, Any] = {
            "symbol": formatted_symbol,
            "interval": interval,
            "startTime": start_time // 1000,  # Convert milliseconds to seconds
            "limit": limit,
        }

        if end_time is not None:
            params_dict["endTime"] = end_time // 1000  # Convert milliseconds to seconds

        return BackpackRawGetMarketDataParams(**params_dict)

    @staticmethod
    def build_get_historical_trades_params(
        symbol: str,
        limit: int = 100,
        from_id: str | None = None,
    ) -> BackpackRawGetHistoricalTradesParams:
        """Build query parameters for fetching historical trades.

        Args:
            symbol: Trading symbol
            limit: Maximum number of results (default 100)
            from_id: Optional trade ID to start pagination from

        Returns:
            BackpackRawGetHistoricalTradesParams: Validated query parameters
        """
        logger.debug(
            "building_get_historical_trades_params",
            symbol=symbol,
            limit=limit,
            from_id=from_id,
        )

        formatted_symbol = BackpackMarketDataRequestBuilder.format_symbol(symbol)
        return BackpackRawGetHistoricalTradesParams(
            symbol=formatted_symbol,
            limit=limit,
            fromId=from_id,
        )
