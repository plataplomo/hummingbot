"""
CyberDeltaEngine: Backpack Market Data Service
---------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Backpack Exchange. It uses the HttpClient, BackpackRequestBuilder,
BackpackResponseHandler, and RateLimiterService to interact with the API
and returns validated Raw Pydantic Models.
"""

from pydantic import ValidationError  # Import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
    RawJsonResponse,  # Import this type alias
)
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.connectivity.http_client import HttpClient

# Import RateLimiterService
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError  # Corrected import for APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode  # Import APIErrorCode
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class BackpackMarketDataService:
    """
    Service class for Backpack market data operations.
    """

    def __init__(
        self,
        http_client: HttpClient,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        rate_limiter_service: RateLimiterService,  # Added RateLimiterService
        exchange_name: str,
    ) -> None:
        """
        Initialize the BackpackMarketDataService.

        Args:
            http_client: An instance of HttpClient for making API requests.
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            rate_limiter_service: An instance of RateLimiterService.
            exchange_name: The name of the exchange.
        """
        self._http_client = http_client
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._rate_limiter_service = rate_limiter_service  # Store RateLimiterService
        self._exchange_name = exchange_name

    async def get_ticker(self, symbol: str) -> BackpackRawTicker:
        """
        Retrieves the latest ticker information for a specific symbol.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").

        Returns:
            A BackpackRawTicker object containing the validated raw ticker data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint = "/api/v1/ticker"
        params = self._request_builder.build_get_ticker_params(symbol=symbol)
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw, _, _ = await self._http_client.request(
                method="GET",
                endpoint_path=endpoint,
                rate_limiter_service=self._rate_limiter_service,  # Pass the service
                params=params,
            )

            if not isinstance(response_data_raw, dict):
                logger.error(
                    f"[{self._exchange_name}] Unexpected ticker response format for "
                    f"{symbol}: {type(response_data_raw)}"
                )
                raise APIError(
                    message=f"Unexpected ticker response format: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            raw_ticker: BackpackRawTicker = self._response_handler.handle_get_ticker_response(
                response_data_raw, symbol
            )
            return raw_ticker

        except APIError:
            raise
        except ValueError as e_val:
            logger.error(
                f"[{self._exchange_name}] Ticker response validation/parsing failed for {symbol}: "
                f"{e_val}. Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Failed to validate/parse ticker data for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching ticker for {symbol}: "
                f"{e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing ticker for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_book(self, symbol: str, depth: int = 20) -> BackpackRawOrderBook:
        """
        Retrieves the order book for a specific symbol.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            depth: The number of depth levels to retrieve. Defaults to 20.
                   Note: Backpack API might have specific limits or ways to specify depth.
                   The Backpack API documentation for /api/v1/depth indicates an optional
                   `limit` parameter (default 100, max 1000). The old implementation passed
                   the `depth` param as this `limit`.

        Returns:
            A BackpackRawOrderBook object containing the validated raw order book data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint = "/api/v1/depth"
        params = self._request_builder.build_get_order_book_params(symbol=symbol, limit=depth)
        response_raw: RawJsonResponse | None = None
        try:
            response_raw, _, _ = await self._http_client.request(
                method="GET",
                endpoint_path=endpoint,
                rate_limiter_service=self._rate_limiter_service,
                params=params,
            )

            # Validate raw order book data using the response handler
            # The handler expects RawJsonResponse, which can be dict or list.
            # For order book, Backpack returns a dict.
            if not isinstance(response_raw, dict):
                logger.error(
                    f"[{self._exchange_name}] Unexpected order book response format for "
                    f"{symbol}: {type(response_raw)}"
                )
                raise APIError(
                    message=f"Unexpected order book response format: {type(response_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            validated_book: BackpackRawOrderBook = (
                self._response_handler.handle_get_order_book_response(response_raw, symbol)
            )
            return validated_book

        except APIError:
            raise
        except ValueError as e_val:  # Catches errors from response_handler or Pydantic validation
            logger.error(
                f"[{self._exchange_name}] Order book response validation/parsing failed for "
                f"{symbol}: {e_val}. Raw Data: {response_raw!r}"
            )
            raise APIError(
                f"Order book validation/parsing failed for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_order_book for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order book for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_recent_trades(
        self, symbol: str, limit: int | None = 50
    ) -> list[BackpackRawTrade]:
        """
        Retrieves recent public trades for a specific symbol.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of trades to retrieve. Defaults to 50.
                   Backpack API specifies default 100, max 1000 for its `limit` parameter.
                   The old implementation used the provided `limit`.

        Returns:
            A list of BackpackRawTrade objects containing validated raw trade data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint = "/api/v1/trades"
        params = self._request_builder.build_get_recent_trades_params(symbol=symbol, limit=limit)
        response_raw: RawJsonResponse | None = None
        try:
            response_raw, _, _ = await self._http_client.request(
                method="GET",
                endpoint_path=endpoint,
                rate_limiter_service=self._rate_limiter_service,
                params=params,
            )

            # Validate using the handler (handles list structure and item validation)
            # Backpack /api/v1/trades returns a list of trade objects.
            if not isinstance(response_raw, list):
                logger.error(
                    f"[{self._exchange_name}] Unexpected recent trades response format for "
                    f"{symbol}: {type(response_raw)}"
                )
                raise APIError(
                    message=f"Unexpected recent trades response format: {type(response_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            validated_trades: list[BackpackRawTrade] = (
                self._response_handler.handle_get_recent_trades_response(response_raw, symbol)
            )
            return validated_trades

        except APIError:
            raise
        except ValueError as e_val:  # Catches errors from response_handler or Pydantic validation
            logger.error(
                f"[{self._exchange_name}] Recent trades response validation/parsing failed for "
                f"{symbol}: {e_val}. Raw Data: {response_raw!r}"
            )
            raise APIError(
                f"Recent trades validation/parsing failed for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_recent_trades for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting recent trades for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_funding_rate(self, symbol: str) -> BackpackRawFundingRate:
        """
        Retrieves the current funding rate for a specific perpetual contract symbol.

        Args:
            symbol: The perpetual contract symbol (e.g., "SOL-PERP").

        Returns:
            A BackpackRawFundingRate object containing the validated raw funding rate data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        # Note: BackpackRequestBuilder.format_symbol might be needed if symbol format varies.
        # For funding, it seems to be part of the path directly.
        endpoint = f"/api/v1/markets/{self._request_builder.format_symbol(symbol)}/funding"
        # build_get_funding_rate_params currently returns {} as per old implementation context
        params = self._request_builder.build_get_funding_rate_params(symbol=symbol)
        response_raw: RawJsonResponse | None = None
        try:
            response_raw, _, _ = await self._http_client.request(
                method="GET",
                endpoint_path=endpoint,
                rate_limiter_service=self._rate_limiter_service,
                params=params,
            )

            if not isinstance(response_raw, dict):
                logger.error(
                    f"[{self._exchange_name}] Unexpected funding rate response format for "
                    f"{symbol}: {type(response_raw)}"
                )
                raise APIError(
                    message=f"Unexpected funding rate response format: {type(response_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            raw_funding_rate: BackpackRawFundingRate = (
                self._response_handler.handle_get_funding_rate_response(response_raw, symbol)
            )
            return raw_funding_rate

        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:  # Catches Pydantic and other ValueErrors
            logger.error(
                f"[{self._exchange_name}] Funding rate validation/parsing failed for {symbol}: "
                f"{e_val}. Raw: {response_raw!r}"
            )
            raise APIError(
                message=f"Invalid funding rate data from exchange for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching funding rate for "
                f"{symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing funding rate for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_market_data(
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> list[BackpackRawKline]:
        """
        Retrieves historical kline (candlestick) data for a specific symbol and timeframe.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            timeframe: The kline interval (e.g., "1m", "1h", "1D").
                       Refer to Backpack API documentation for supported intervals.
            limit: The maximum number of klines to retrieve. Defaults to 100.
                   Backpack API default 100, max 1000.

        Returns:
            A list of BackpackRawKline objects containing validated raw kline data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint = "/api/v1/klines"
        params = self._request_builder.build_get_market_data_params(
            symbol=symbol,
            timeframe_str=timeframe,
            start_time_ms=None,  # Not used by current service layer, but builder supports it
            end_time_ms=None,  # Not used by current service layer, but builder supports it
            limit=limit,
        )
        response_data_raw: RawJsonResponse | None = None  # Changed from Any for more specificity
        try:
            response_data_raw, _, _ = await self._http_client.request(
                method="GET",
                endpoint_path=endpoint,
                rate_limiter_service=self._rate_limiter_service,
                params=params,
            )

            # Validate using the handler
            # The handler expects RawJsonResponse, which can be a list for klines.
            if not isinstance(response_data_raw, list):
                logger.error(
                    f"[{self._exchange_name}] Unexpected klines response format for "
                    f"{(symbol)}@{(timeframe)}: {type(response_data_raw)}"
                )
                raise APIError(
                    message=f"Unexpected klines response format: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            # Assuming handle_get_market_data_response returns a list of raw kline dicts (RawJson items)
            # based on the linter error. We need to parse them into BackpackRawKline.
            raw_kline_data_list = self._response_handler.handle_get_market_data_response(
                response_data_raw, symbol, timeframe
            )

            validated_kline_list: list[BackpackRawKline] = []
            for kline_data_item in raw_kline_data_list:  # Iterate directly
                try:
                    if isinstance(kline_data_item, dict):
                        # Perform Pydantic validation for each item
                        validated_kline_list.append(
                            BackpackRawKline.model_validate(kline_data_item)
                        )
                    else:
                        logger.warning(
                            f"[{self._exchange_name}] Skipping non-dict kline item in list for "
                            f"{(symbol)}@{(timeframe)}: {type(kline_data_item)} - {kline_data_item!r}"
                        )
                except ValidationError as e_item_val:
                    logger.warning(
                        f"[{self._exchange_name}] Failed to validate individual kline item for "
                        f"{(symbol)}@{(timeframe)}: {e_item_val}. Item: {kline_data_item!r}"
                    )
                    # Optionally, decide whether to continue or raise an error for the whole batch

            return validated_kline_list

        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:  # Catches Pydantic and other ValueErrors
            logger.error(
                f"[{self._exchange_name}] Klines response validation/parsing failed for {symbol}@{timeframe}: "
                f"{e_val}. Raw Data: {response_data_raw!r}"
            )
            raise APIError(
                f"Klines validation/parsing failed for {symbol}@{timeframe}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_market_data for {symbol}@{timeframe}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting klines for {symbol}@{timeframe}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled
