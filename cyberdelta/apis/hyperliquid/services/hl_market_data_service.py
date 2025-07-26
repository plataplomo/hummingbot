"""Hyperliquid Market Data Service - Composite service combining market data operations.

This service combines all market data-related operations from the decomposed services
to provide a unified interface for market data access.
"""

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from cyberdelta.apis.hyperliquid.mappers import (
    HyperliquidHistoricalDataMapper,
    HyperliquidMarketMetadataMapper,
    HyperliquidOrderBookMapper,
    HyperliquidPriceTickerMapper,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    MarketDataResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    HistoricalDataMapperProtocol,
    MarketMetadataMapperProtocol,
    OrderBookMapperProtocol,
    PriceTickerMapperProtocol,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_historical_data_service import (
    HyperliquidHistoricalDataService,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_market_metadata_service import (
    HyperliquidMarketMetadataService,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_order_book_service import (
    HyperliquidOrderBookService,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_price_ticker_service import (
    HyperliquidPriceTickerService,
)
from cyberdelta.apis.models.service_args import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market import Candle, Market
from cyberdelta.core.models.market.mid_prices import MidPrices
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    pass


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidMarketDataService:
    """Composite service for Hyperliquid market data operations.

    Combines all market data-related decomposed services to provide a unified interface
    for accessing market data including tickers, order books, trades, and historical data.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        exchange_name: str,
        price_ticker_mapper: PriceTickerMapperProtocol | None = None,
        order_book_mapper: OrderBookMapperProtocol | None = None,
        historical_data_mapper: HistoricalDataMapperProtocol | None = None,
        market_metadata_mapper: MarketMetadataMapperProtocol | None = None,
        order_book_service: HyperliquidOrderBookService | None = None,
    ) -> None:
        """Initialize the Hyperliquid market data service.

        Creates and configures all decomposed service components.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            exchange_name: Name identifier for this exchange instance
            price_ticker_mapper: Optional price ticker mapper instance
            order_book_mapper: Optional order book mapper instance
            historical_data_mapper: Optional historical data mapper instance
            market_metadata_mapper: Optional market metadata mapper instance
            order_book_service: Optional order book service instance to share with other services
        """
        # Create mappers if not provided
        self._price_ticker_mapper = price_ticker_mapper or HyperliquidPriceTickerMapper()
        self._order_book_mapper = order_book_mapper or HyperliquidOrderBookMapper()
        self._historical_data_mapper = historical_data_mapper or HyperliquidHistoricalDataMapper()
        self._market_metadata_mapper = market_metadata_mapper or HyperliquidMarketMetadataMapper()

        # Initialize decomposed services
        self._price_ticker_service = HyperliquidPriceTickerService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._price_ticker_mapper,
            historical_data_mapper=self._historical_data_mapper,
            exchange_name=exchange_name,
        )

        # Use provided order book service or create a new one
        self._order_book_service = order_book_service or HyperliquidOrderBookService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_book_mapper,
            exchange_name=exchange_name,
        )

        self._historical_data_service = HyperliquidHistoricalDataService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._historical_data_mapper,
            exchange_name=exchange_name,
        )

        self._market_metadata_service = HyperliquidMarketMetadataService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._market_metadata_mapper,
            exchange_name=exchange_name,
        )

        # Store shared components
        self._exchange_name = exchange_name

        logger.info(
            "market_data_service_initialized",
            exchange=exchange_name,
            message="Hyperliquid market data service initialized with decomposed services",
        )

    # Price Ticker Operations

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Retrieve the latest ticker information for a specific symbol."""
        return await self._price_ticker_service.get_ticker(symbol)

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Retrieve the current funding rate for a specific perpetual contract symbol."""
        return await self._price_ticker_service.get_funding_rate(symbol)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Retrieve current funding rates for specified symbols or all."""
        return await self._historical_data_service.get_funding_rates(args)

    async def get_all_mids(self) -> MidPrices:
        """Fetch all mid prices efficiently for market order pricing."""
        return await self._price_ticker_service.get_all_mids()

    # Order Book Operations

    async def get_order_book(self, symbol: str) -> OrderBook | None:
        """Retrieve the order book for a specific symbol."""
        return await self._order_book_service.get_order_book(symbol)

    async def get_recent_trades(self, symbol: str) -> list[Trade]:
        """Retrieve recent public trades for a specific symbol."""
        return await self._order_book_service.get_recent_trades(symbol)

    # Historical Data Operations

    async def get_historical_funding_rates(
        self, args: GetHistoricalFundingRatesArgs
    ) -> list[FundingRate]:
        """Retrieve historical funding rates for a specific symbol and time range."""
        return await self._historical_data_service.get_historical_funding_rates(args)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Retrieve historical kline/candlestick data for a symbol and timeframe."""
        return await self._historical_data_service.get_market_data(args)

    # Market Metadata Operations

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Retrieve market metadata for all available markets."""
        return await self._market_metadata_service.get_markets(args)

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Retrieve market metadata for a specific symbol."""
        return await self._market_metadata_service.get_market(args)
