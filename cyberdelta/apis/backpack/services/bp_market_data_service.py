"""Backpack Market Data Service - Composite service combining market data operations.

This service combines all market data-related operations from the decomposed services
to provide a unified interface for market data access, following Hyperliquid's
composite pattern.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from cyberdelta.apis.backpack.mappers import (
    BackpackCandleMapper,
    BackpackFundingRateMapper,
    BackpackMarketMapper,
    BackpackOrderBookMapper,
    BackpackTickerMapper,
    BackpackTradeMapper,
)
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.backpack.services.market_data.bp_historical_data_service import (
    BackpackHistoricalDataService,
)
from cyberdelta.apis.backpack.services.market_data.bp_market_metadata_service import (
    BackpackMarketMetadataService,
)
from cyberdelta.apis.backpack.services.market_data.bp_order_book_service import (
    BackpackOrderBookService,
)
from cyberdelta.apis.backpack.services.market_data.bp_price_ticker_service import (
    BackpackPriceTickerService,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    FundingRate,
    Market,
    Market as MarketInfo,
    OrderBook,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackMarketDataService:
    """Composite service for Backpack market data operations.

    Combines all market data-related decomposed services to provide a unified interface
    for market data operations including tickers, order books, trades, and candles.

    This follows Hyperliquid's composite pattern where:
    - The composite owns instances of decomposed services
    - All operations are delegated to the appropriate decomposed service
    - Optional dependency injection is supported for all mappers
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackMarketDataRequestBuilder,
        response_handler: BackpackMarketDataResponseHandler,
        exchange_name: str,
        # Optional mapper injection for testability
        ticker_mapper: BackpackTickerMapper | None = None,
        order_book_mapper: BackpackOrderBookMapper | None = None,
        trade_mapper: BackpackTradeMapper | None = None,
        candle_mapper: BackpackCandleMapper | None = None,
        market_mapper: BackpackMarketMapper | None = None,
        funding_rate_mapper: BackpackFundingRateMapper | None = None,
    ) -> None:
        """Initialize the Backpack market data service.

        Creates and configures all decomposed service components.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            exchange_name: Name identifier for this exchange instance
            ticker_mapper: Optional ticker mapper instance for dependency injection
            order_book_mapper: Optional order book mapper instance for dependency injection
            trade_mapper: Optional trade mapper instance for dependency injection
            candle_mapper: Optional candle mapper instance for dependency injection
            market_mapper: Optional market mapper instance for dependency injection
            funding_rate_mapper: Optional funding rate mapper instance for dependency injection
        """
        # Store core parameters
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._exchange_name = exchange_name

        # Create mappers if not provided (following Hyperliquid pattern)
        self._ticker_mapper = ticker_mapper or BackpackTickerMapper()
        self._order_book_mapper = order_book_mapper or BackpackOrderBookMapper()
        self._trade_mapper = trade_mapper or BackpackTradeMapper()
        self._candle_mapper = candle_mapper or BackpackCandleMapper()
        self._market_mapper = market_mapper or BackpackMarketMapper()
        self._funding_rate_mapper = funding_rate_mapper or BackpackFundingRateMapper()

        # Initialize decomposed service components
        self._price_ticker_service = BackpackPriceTickerService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            ticker_mapper=self._ticker_mapper,
            authenticator=None,  # Market data doesn't need auth
            exchange_name=exchange_name,
        )

        self._order_book_service = BackpackOrderBookService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            order_book_mapper=self._order_book_mapper,
            authenticator=None,  # Market data doesn't need auth
            exchange_name=exchange_name,
        )

        self._historical_data_service = BackpackHistoricalDataService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            trade_mapper=self._trade_mapper,
            candle_mapper=self._candle_mapper,
            funding_rate_mapper=self._funding_rate_mapper,
            authenticator=None,  # Market data doesn't need auth
            exchange_name=exchange_name,
        )

        self._market_metadata_service = BackpackMarketMetadataService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            market_mapper=self._market_mapper,
            funding_rate_mapper=self._funding_rate_mapper,
            authenticator=None,  # Market data doesn't need auth
            exchange_name=exchange_name,
        )

        logger.info(
            "market_data_service_initialized",
            exchange=exchange_name,
            mappers={
                "ticker": type(self._ticker_mapper).__name__,
                "order_book": type(self._order_book_mapper).__name__,
                "trade": type(self._trade_mapper).__name__,
                "candle": type(self._candle_mapper).__name__,
                "market": type(self._market_mapper).__name__,
                "funding_rate": type(self._funding_rate_mapper).__name__,
            },
            message="Backpack market data service initialized with decomposed services",
        )

    # Price Ticker Operations

    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker information for a symbol.

        Delegates to the price ticker service component.
        """
        return await self._price_ticker_service.get_ticker(symbol)

    async def get_all_tickers(self) -> dict[str, Ticker]:
        """Get ticker information for all symbols.

        Delegates to the price ticker service component.
        """
        return await self._price_ticker_service.get_all_tickers()

    # Order Book Operations

    async def get_order_book(self, symbol: str, limit: int | None = None) -> OrderBook:
        """Get order book for a symbol.

        Delegates to the order book service component.
        """
        return await self._order_book_service.get_order_book(symbol, limit)

    # Historical Data Operations

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Get recent trades for a symbol.

        Delegates to the historical data service component.
        """
        return await self._historical_data_service.get_recent_trades(symbol, limit)

    async def get_candles(self, get_candles_args: GetMarketDataArgs) -> list[Candle]:
        """Get historical candle data.

        Delegates to the historical data service component.
        """
        return await self._historical_data_service.get_market_data(get_candles_args)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get historical market data (candlesticks) for a specific symbol.

        This is an alias for get_candles to maintain API compatibility.
        """
        return await self.get_candles(args)

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Get current funding rate for a specific symbol.

        Delegates to the market metadata service component.
        """
        args = GetFundingRatesArgs(symbols=[symbol])
        rates = await self._market_metadata_service.get_funding_rates(args)
        if not rates:
            raise APIError(
                code=APIErrorCode.INVALID_SYMBOL.value,
                message=f"No funding rate found for symbol {symbol}",
            )
        return rates[0]

    async def get_funding_rates(
        self, get_funding_rates_args: GetFundingRatesArgs
    ) -> list[FundingRate]:
        """Get funding rate information.

        Delegates to the market metadata service component.
        """
        return await self._market_metadata_service.get_funding_rates(get_funding_rates_args)

    async def get_historical_funding_rates(
        self, args: GetHistoricalFundingRatesArgs
    ) -> list[FundingRate]:
        """Get historical funding rates for a specific symbol.

        Delegates to the market metadata service component.
        """
        # For now, delegate to the current funding rates method
        # This would need proper historical implementation
        funding_args = GetFundingRatesArgs(symbols=[args.symbol])
        return await self._market_metadata_service.get_funding_rates(funding_args)

    # Market Metadata Operations

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Get market metadata for a specific symbol.

        Delegates to the market metadata service component.
        """
        return await self._market_metadata_service.get_market(args)

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Get metadata for all available markets.

        Delegates to the market metadata service component.
        """
        return await self._market_metadata_service.get_markets(args)

    async def get_exchange_info(self, get_exchange_info_args: GetMarketsArgs) -> list[MarketInfo]:
        """Get exchange market information.

        Delegates to the market metadata service component.
        """
        return await self._market_metadata_service.get_markets(get_exchange_info_args)
