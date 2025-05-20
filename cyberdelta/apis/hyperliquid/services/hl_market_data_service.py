"""
CyberDeltaEngine: Hyperliquid Market Data Service
-------------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Hyperliquid Exchange. It uses the HttpClient, HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

# Typing and Pydantic
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

# Project-specific imports for connectivity and base types
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidCandleMapper, HyperliquidMapper

# Mappers
# Hyperliquid-specific imports
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)

# Removed unused raw model imports as handlers return these directly now
# from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)

# from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
# from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Internal Domain Models
from cyberdelta.core.models import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle

# Utilities
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]

if TYPE_CHECKING:
    pass


class HyperliquidMarketDataService:
    """
    Service class for handling Hyperliquid market data API endpoints.

    This class centralizes the logic for fetching market data such as tickers,
    order books, trades, funding rates, and candlestick data.
    It leverages shared components like HttpClient, HyperliquidRequestBuilder,
    HyperliquidResponseHandler, and RateLimiterService to perform its tasks.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: HyperliquidRequestBuilder
    _response_handler: HyperliquidResponseHandler
    _exchange_name: str
    _info_url: str
    _mapper: HyperliquidMapper
    _candle_mapper: HyperliquidCandleMapper

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        mapper: HyperliquidMapper,
        exchange_name: str,
        info_url: str,
    ) -> None:
        """
        Initialize the HyperliquidMarketDataService.

        Args:
            http_client_requester: An instance of HttpClientRequesterSig for making HTTP requests.
            request_builder: An instance of HyperliquidRequestBuilder for preparing
                API requests.
            response_handler: An instance of HyperliquidResponseHandler for validating
                API responses.
            mapper: An instance of HyperliquidMapper for mapping raw data to internal models.
            exchange_name: The name of the exchange.
            info_url: The base URL for the exchange's API.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._candle_mapper = HyperliquidCandleMapper()
        self._exchange_name = exchange_name
        self._info_url = info_url

    async def get_all_asset_contexts_raw(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """
        Retrieves the metadata for all listed assets and their current context
        (mark price, funding rate, etc.) by calling the /info endpoint.
        Hyperliquid's /info endpoint often returns multiple data types; the handler
        is responsible for extracting and validating the metaAndAssetCtxs part.

        Returns:
            A HyperliquidRawMetaAndAssetCtxsResponse object containing validated raw data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"
        # HyperliquidRequestBuilder.build_info_request_payload() now returns a Pydantic model.
        request_payload_model = self._request_builder.build_info_request_payload()
        request_payload_data_dict = request_payload_model.model_dump(
            by_alias=True,
            exclude_none=True,  # Use by_alias if model uses aliases
        )

        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            # The HttpClient is assumed to be configured with INFO_URL as its base.
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data_dict,  # Pass the dumped dictionary
                is_info_endpoint=True,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw all_asset_contexts response: {raw_response_content!r}, Status: {status_code}, Headers: {headers}"
            )

            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    raw_response_content,
                    status_code=status_code,
                    headers=headers,
                )
            )
            return validated_response

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Asset contexts response validation failed: {e_val}. "
                f"Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Failed to validate asset contexts response: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching asset contexts: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing asset contexts: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_unhandled

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """
        Retrieves the latest ticker/context information for a specific symbol.
        This involves fetching all asset contexts and then finding the specific one.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A Ticker object if the symbol is found, otherwise None.
            The object contains ticker-like data (mark price, funding, etc.).

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
                      (Note: original hl_api.get_ticker raised SYMBOL_NOT_FOUND specifically)
        """
        try:
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if all_contexts_response and all_contexts_response.asset_ctxs:
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol:
                        return HyperliquidMapper.map_raw_ctx_to_ticker(asset_ctx)

            # Symbol not found in the contexts
            logger.warning(
                f"[{self._exchange_name}] Ticker data (asset context) not found "
                f"for symbol '{symbol}' after fetching all asset contexts."
            )
            return None  # Consistent with method signature if not found

        except APIError:  # Propagate APIErrors from get_all_asset_contexts
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_ticker "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            # To maintain consistency with original behavior of raising APIError for failures
            raise APIError(
                message=f"Unexpected error fetching ticker for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_book(self, symbol: str) -> OrderBook | None:
        """
        Retrieves the L2 order book for a specific symbol using a POST request to /info
        with a specific payload: {"type": "l2Book", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            An OrderBook object containing the validated raw order book data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"
        # Assuming HyperliquidRequestBuilder has or will have this method:
        request_payload_model = self._request_builder.build_l2_book_request_payload(symbol=symbol)
        request_payload_data: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data,
                is_info_endpoint=True,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw order book for {symbol}: {raw_response_content!r}, Status: {status_code}, Headers: {headers}"
            )

            validated_raw_book = self._response_handler.handle_info_l2_book_response(
                raw_response_content,
                symbol=symbol,
                status_code=status_code,
                headers=headers,
            )
            return HyperliquidMapper.map_raw_order_book(validated_raw_book)

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Order book response validation failed "
                f"for {symbol}: {e_val}. Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Failed to validate order book response for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching order book "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing order book for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_unhandled

    async def get_recent_trades(
        self,
        symbol: str,
        # limit: int = 100, # Limit is not part of HL /info request for recentTrades
    ) -> list[Trade]:
        """
        Retrieves recent public trades for a specific symbol using a POST request to /info
        with a payload: {"type": "recentTrades", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A list of Trade objects.
            The number of trades is determined by the Hyperliquid API.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"
        # Assuming HyperliquidRequestBuilder has or will have this method:
        request_payload_model = self._request_builder.build_recent_trades_request_payload(
            symbol=symbol
        )
        request_payload_data: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data,
                is_info_endpoint=True,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw recent_trades response for {symbol}: {raw_response_content!r}, Status: {status_code}, Headers: {headers}"
            )

            validated_raw_trades = self._response_handler.handle_info_recent_trades_response(
                raw_response_content,
                symbol=symbol,
                status_code=status_code,
                headers=headers,
            )
            # The response handler is expected to return the full list of trades from the API.
            # Limiting should be done by the caller or a mapping layer if needed.
            internal_trades: list[Trade] = []
            for raw_trade in validated_raw_trades:
                try:
                    # Use static call to mapper - Name was correct
                    trade = HyperliquidMapper.transform_raw_public_trade_to_internal(raw_trade)
                    if trade:
                        internal_trades.append(trade)
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping trade map error for {symbol}: {e_map_item}. Raw: {raw_trade.model_dump_json() if hasattr(raw_trade, 'model_dump_json') else raw_trade!r}"
                    )

            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_trades)} recent_trades for {symbol}"
            )
            return internal_trades

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Recent trades response validation failed "
                f"for {symbol}: {e_val}. Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Failed to validate recent trades response for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching recent trades "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing recent trades for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_unhandled

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """
        Retrieves the current funding rate information for a specific perpetual contract symbol.
        This is typically part of the broader asset context. It calls get_all_asset_contexts
        and extracts the relevant context.

        Args:
            symbol: The perpetual contract symbol (e.g., "ETH").

        Returns:
            A FundingRate object if the symbol is found, otherwise None.
            The 'funding' field of this object contains the funding rate.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
        """
        try:
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if all_contexts_response and all_contexts_response.asset_ctxs:
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol:
                        return HyperliquidMapper.map_raw_ctx_to_funding_rate(asset_ctx)

            logger.warning(
                f"[{self._exchange_name}] Funding rate data (from asset context) not found for symbol '{symbol}'."
            )
            return None
        except APIError:  # Propagate APIErrors from get_all_asset_contexts
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_funding_rate "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error fetching funding rate for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """
        Retrieves current funding rates for specified symbols, or all if None.

        Args:
            symbols: A list of symbols to get funding rates for. If None, fetches for all.

        Returns:
            A list of FundingRate objects.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
        """
        rates: list[FundingRate] = []
        try:
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if not all_contexts_response or not all_contexts_response.asset_ctxs:
                logger.warning(
                    f"[{self._exchange_name}] No asset contexts found to derive funding rates."
                )
                return []

            symbols_to_process: list[str]
            if symbols:
                symbols_to_process = symbols
            else:
                symbols_to_process = [
                    ctx.name for ctx in all_contexts_response.asset_ctxs if ctx.name
                ]

            for symbol_name in symbols_to_process:
                found_ctx = False
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol_name:
                        try:
                            rate = HyperliquidMapper.map_raw_ctx_to_funding_rate(asset_ctx)
                            if rate:
                                rates.append(rate)
                            found_ctx = True
                            break
                        except Exception as e_map:
                            logger.error(
                                f"[{self._exchange_name}] Error mapping funding rate for {symbol_name} from context: {e_map}. Context: {asset_ctx.model_dump_json(indent=2)}"
                            )
                if (
                    not found_ctx and symbols
                ):  # Only warn if specific symbols were requested and not found
                    logger.warning(
                        f"[{self._exchange_name}] Context for symbol '{symbol_name}' not found in fetched asset contexts."
                    )
            return rates
        except APIError:  # Propagate APIErrors from get_all_asset_contexts_raw
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_funding_rates: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error fetching funding rates: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_historical_funding_rates(
        self,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int | None = None,
    ) -> list[FundingRate]:
        """Retrieves historical funding rates for a specific symbol and time range."""
        logger.debug(
            f"[{self._exchange_name}] Getting historical funding rates for {symbol} "
            f"from {start_time_ms} to {end_time_ms if end_time_ms is not None else 'now'}."
        )

        payload = self._request_builder.build_historical_funding_rates_payload(
            symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )

        raw_response_content, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint_path="/info",
            data=payload,
            is_info_endpoint=True,
        )

        if raw_response_content is None:
            logger.warning(
                f"[{self._exchange_name}] No content for historical funding rates for {symbol}. "
                f"Status: {status_code}. Returning empty list."
            )
            return []

        raw_funding_history_items = self._response_handler.handle_historical_funding_rates_response(
            raw_response_content=raw_response_content
        )

        internal_funding_rates: list[FundingRate] = []
        for raw_item in raw_funding_history_items:
            try:
                internal_rate = self._mapper.transform_raw_funding_history_item_to_internal(
                    raw_item
                )
                internal_funding_rates.append(internal_rate)
            except APIError as e:
                logger.error(
                    f"[{self._exchange_name}] Failed to map raw funding history item for {symbol}: {e}. "
                    f"Raw item: {raw_item!r}. Skipping."
                )
                continue

        return internal_funding_rates

    async def get_market_data(
        self, symbol: str, interval: str, start_time_ms: int, end_time_ms: int
    ) -> list[Candle]:
        """
        Retrieves historical kline/candlestick data for a symbol and timeframe.
        Uses a POST request to /info with payload:
        {"type": "candleSnapshot",
         "req": {"coin": SYMBOL, "interval": INTERVAL,
                 "startTime": START_MS, "endTime": END_MS}}

        Args:
            symbol: The trading symbol (e.g., "ETH").
            interval: The kline interval (e.g., "1m", "1h", "1d"). Refer to Hyperliquid API docs.
            start_time_ms: Start timestamp in milliseconds.
            end_time_ms: End timestamp in milliseconds.

        Returns:
            A list of Candle objects containing lists of candle data points.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        logger.debug(
            f"[{self._exchange_name}] Getting market data (candles) for {symbol}, interval {interval}, "
            f"start {start_time_ms}, end {end_time_ms}"
        )
        payload = self._request_builder.build_candle_snapshot_payload(
            symbol=symbol, timeframe=interval, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )

        raw_response_content, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint_path="/info",  # Candle data is from /info
            data=payload,  # Payload itself is a dict[str, Any]
            is_info_endpoint=True,  # Crucial for routing to the correct HttpClient
        )

        if raw_response_content is None:
            logger.error(f"[{self._exchange_name}] No content received for candles {symbol}.")
            # Consider raising APIError or returning empty list based on desired strictness
            return []

        # Assuming raw_response_content is list[dict[str, Any]] for candles
        # The handler expects RawJsonResponse which can be list.
        if not isinstance(raw_response_content, list):
            logger.error(
                f"[{self._exchange_name}] Expected list for candle data, got {type(raw_response_content)}."
            )
            # Handle error appropriately, perhaps raise APIError
            raise APIError(
                f"Unexpected response type for candle data: {type(raw_response_content)}",
                APIErrorCode.INVALID_RESPONSE.value,
            )

        # The handler expects raw JSON, not already Pydantic validated models typically
        # For candles, it might be list of lists or list of dicts
        raw_candles = self._response_handler.handle_info_candle_snapshot_response(
            raw_response_content, symbol, interval, status_code
        )
        return self._candle_mapper.map(raw_candles, symbol, interval)
