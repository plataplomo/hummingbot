"""
CyberDeltaEngine: Backpack Market Data Service
---------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Backpack Exchange. It uses the HttpClient, BackpackRequestBuilder,
BackpackResponseHandler, and RateLimiterService to interact with the API
and returns validated Raw Pydantic Models.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder

# Import RawJsonResponse from bp_response_handler where it's defined as an alias
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
)
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)

# Assuming BackpackRawKline is for individual klines, used in lists
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline

# Singular Raw models specific to Backpack responses
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawOrderBook,
    # Removed non-existent BackpackRawAllTickers, BackpackRawRecentTrades
    BackpackRawTicker,
)

# Assuming BackpackRawTrade is for individual trades, used in lists
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse  # Import ParsedJsonResponse

# Base API error models
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.market import (
    FundingRate,
    OrderBook,
    Ticker,
    Trade,
)

# Internal domain models
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.utils.logging_config import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class BackpackMarketDataService:
    """
    Service class for Backpack market data operations.
    Returns Internal Domain Models.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: BackpackRequestBuilder
    _response_handler: BackpackResponseHandler
    _mapper: BackpackOrderMapper
    _exchange_name: str

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> None:
        """
        Initialize the BackpackMarketDataService.

        Args:
            http_client_requester: A callable for making API requests (e.g., BackpackAPI._request).
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            exchange_name: The name of the exchange.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._exchange_name = exchange_name
        self._mapper = BackpackOrderMapper()

    async def get_ticker(self, symbol: str) -> Ticker:
        """Retrieves the latest ticker information for a specific symbol."""
        endpoint_path, params = self._request_builder.build_get_ticker_params(symbol=symbol)
        logger.debug(
            f"[{self._exchange_name}] Requesting ticker for {symbol} from {endpoint_path} "
            f"with params: {params}"
        )
        raw_data: ParsedJsonResponse | None = None  # For logging in except blocks
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            # _http_client_requester now returns a tuple
            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,  # Corrected from endpoint_path
                params=params,
                is_public_info_endpoint=True,
            )
            raw_data, status_code, headers = response_tuple
            logger.debug(
                f"[{self._exchange_name}] Raw ticker response for {symbol}: {raw_data!r} (Status: {status_code}, Headers: {headers})"
            )
            if raw_data is None:  # Check for None before handler
                # This indicates an issue not caught by _request's error handling (e.g. 204 no content but expected content)
                # Or if _request allows None for ParsedJsonResponse, then this is a valid check.
                raise APIError(
                    f"No data for ticker {symbol}, status: {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            if not isinstance(raw_data, dict):
                raise APIError(
                    f"Ticker data for {symbol} is not a dict: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_ticker_model: BackpackRawTicker = self._response_handler.handle_get_ticker_response(
                raw_data, symbol, status_code, headers
            )
            internal_ticker = self._mapper.transform_raw_ticker_to_internal(
                raw_ticker_model, symbol_override=symbol
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped internal ticker for {symbol}: {internal_ticker}"
            )
            return internal_ticker
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for ticker {symbol}: {e_val}. Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing ticker data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for ticker {symbol}: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for ticker: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_all_tickers(self) -> dict[str, Ticker]:
        """Retrieves tickers for all available markets."""
        # TODO: Backpack API does not seem to have a single endpoint for all tickers.
        # This might require fetching all symbols first, then getting ticker for each.
        # Or, the OpenAPI spec might list all tickers directly under /markets.
        # For now, this is not implemented as the builder/handler methods are missing.
        logger.warning(
            f"[{self._exchange_name}] get_all_tickers is not implemented yet for Backpack."
        )
        raise NotImplementedError(
            "get_all_tickers is not implemented for BackpackMarketDataService"
        )
        # endpoint_path, params = self._request_builder.build_get_all_tickers_params()
        # logger.debug(f"[{self._exchange_name}] Requesting all tickers from {endpoint_path}")
        # raw_data_list: RawJsonResponse | None = None
        # status_code: int = 0
        # try:
        #     raw_data_list, status_code, _ = await self._http_client_requester(
        #         method="GET", endpoint=endpoint_path, params=params, is_public_info_endpoint=True
        #     )
        #     logger.debug(f"[{self._exchange_name}] Raw all_tickers response: {raw_data_list!r} (Status: {status_code})")
        #     if raw_data_list is None and not (200 <= status_code < 300):
        #         raise APIError(f"No data for all_tickers, status: {status_code}", APIErrorCode.INVALID_RESPONSE.value, http_status=status_code)
        #     if not isinstance(raw_data_list, list):
        #         raise APIError(f"All tickers data not list: {type(raw_data_list)}", APIErrorCode.INVALID_RESPONSE.value, http_status=status_code)

        #     raw_ticker_models: list[BackpackRawTicker] = self._response_handler.handle_get_all_tickers_response(
        #         raw_data_list
        #     )
        #     internal_tickers_dict: dict[str, Ticker] = {}
        #     for raw_model in raw_ticker_models:
        #         ticker = self._mapper.transform_raw_ticker_to_internal(raw_model)
        #         internal_tickers_dict[ticker.symbol] = ticker
        #     logger.debug(f"[{self._exchange_name}] Mapped internal tickers_dict: {internal_tickers_dict}")
        #     return internal_tickers_dict
        # except APIError:
        #     raise
        # except (ValidationError, ValueError) as e_val:
        #     logger.error(f"Validation/map error for all_tickers: {e_val}. Raw: {raw_data_list!r}, Status: {status_code}")
        #     raise APIError(message=f"Processing all_tickers failed: {e_val}", code=APIErrorCode.INVALID_RESPONSE.value, original_exception=e_val, http_status=status_code, exchange_message=str(raw_data_list)) from e_val
        # except Exception as e_unhandled:
        #     logger.error(f"Unhandled error for all_tickers: {e_unhandled}", exc_info=True)
        #     raise APIError(message=f"Unexpected error for all_tickers: {e_unhandled}", code=APIErrorCode.UNKNOWN.value, original_exception=e_unhandled, http_status=status_code, exchange_message=str(raw_data_list)) from e_unhandled

    async def get_order_book(self, symbol: str, limit: int | None = None) -> OrderBook:
        """Retrieves the order book for a specific symbol."""
        effective_limit = limit if limit is not None else 100  # Default Backpack limit
        endpoint_path, params = self._request_builder.build_get_order_book_params(
            symbol=symbol, limit=effective_limit
        )
        logger.debug(
            f"[{self._exchange_name}] Requesting order book for {symbol} (limit: {effective_limit})"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_tuple = await self._http_client_requester(
                method="GET", endpoint=endpoint_path, params=params, is_public_info_endpoint=True
            )
            raw_data, status_code, headers = response_tuple
            logger.debug(
                f"[{self._exchange_name}] Raw order_book for {symbol}: {raw_data!r} (Status: {status_code}, Headers: {headers})"
            )
            if raw_data is None:
                raise APIError(
                    f"No data for order_book {symbol}, status: {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data, dict):
                raise APIError(
                    f"Order_book data for {symbol} is not a dict: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_book_model: BackpackRawOrderBook = (
                self._response_handler.handle_get_order_book_response(
                    raw_data, symbol, status_code, headers
                )
            )
            internal_order_book = self._mapper.transform_raw_orderbook_to_internal(
                symbol, raw_order_book_model
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped order_book for {symbol}: {internal_order_book}"
            )
            return internal_order_book
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for order_book {symbol}: {e_val}. Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing order_book failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for order_book {symbol}: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for order_book: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Retrieves recent public trades for a specific symbol."""
        effective_limit = limit if limit is not None else 100  # Default Backpack limit
        endpoint_path, params = self._request_builder.build_get_recent_trades_params(
            symbol=symbol, limit=effective_limit
        )
        logger.debug(
            f"[{self._exchange_name}] Requesting recent_trades for {symbol} (limit: {effective_limit})"
        )
        raw_data_list: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_tuple = await self._http_client_requester(
                method="GET", endpoint=endpoint_path, params=params, is_public_info_endpoint=True
            )
            raw_data_list, status_code, headers = response_tuple
            logger.debug(
                f"[{self._exchange_name}] Raw recent_trades for {symbol}: {raw_data_list!r} (Status: {status_code}, Headers: {headers})"
            )
            if raw_data_list is None:
                raise APIError(
                    f"No data for recent_trades {symbol}, status: {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data_list, list):
                raise APIError(
                    f"Recent_trades data for {symbol} is not a list: {type(raw_data_list)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_trade_models: list[BackpackRawTrade] = (
                self._response_handler.handle_get_recent_trades_response(
                    raw_data_list, symbol, status_code, headers
                )
            )
            internal_trades: list[Trade] = []
            for raw_model in raw_trade_models:
                try:
                    trade = self._mapper.transform_raw_trade_to_internal(raw_model)
                    if trade is not None:
                        internal_trades.append(trade)
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"Skipping trade map error: {e_map_item}. Raw: {raw_model.model_dump_json() if hasattr(raw_model, 'model_dump_json') else raw_model!r}"
                    )
            logger.debug(
                f"[{self._exchange_name}] Mapped recent_trades for {symbol}: {internal_trades}"
            )
            return internal_trades
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for recent_trades {symbol}: {e_val}. Raw: {raw_data_list!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing recent_trades failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data_list),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data_list!r}" if raw_data_list is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for recent_trades {symbol}: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for recent_trades: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data_list),
            ) from e_unhandled

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Retrieves the current funding rate for a specific perpetual contract."""
        endpoint_path, params = self._request_builder.build_get_funding_rate_params(symbol=symbol)
        logger.debug(f"[{self._exchange_name}] Requesting funding_rate for {symbol}")
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_tuple = await self._http_client_requester(
                method="GET", endpoint=endpoint_path, params=params, is_public_info_endpoint=True
            )
            raw_data, status_code, headers = response_tuple
            logger.debug(
                f"[{self._exchange_name}] Raw funding_rate for {symbol}: {raw_data!r} (Status: {status_code}, Headers: {headers})"
            )
            if raw_data is None:
                raise APIError(
                    f"No data for funding_rate {symbol}, status: {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data, dict | list):  # Backpack is dict, HL can be list
                raise APIError(
                    f"Funding_rate data for {symbol} is not a dict or list: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_funding_rate_model: BackpackRawFundingRate = (
                self._response_handler.handle_get_funding_rate_response(
                    raw_data, symbol, status_code, headers
                )
            )
            internal_funding_rate = self._mapper.transform_raw_funding_rate_to_internal(
                raw_funding_rate_model  # Removed symbol_override=symbol as mapper does not take it
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped funding_rate for {symbol}: {internal_funding_rate}"
            )
            return internal_funding_rate
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for funding_rate {symbol}: {e_val}. Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing funding_rate failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for funding_rate {symbol}: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for funding_rate: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """
        Retrieves current funding rates for one or more symbols.
        If Backpack API doesn't support a bulk endpoint, this method iterates
        and calls the single-symbol funding rate endpoint.
        """
        if not symbols:
            # Consistent with how the API client was raising, Backpack requires a symbol.
            logger.error(
                f"[{self._exchange_name}] get_funding_rates requires at least one symbol for Backpack."
            )
            # Or return empty list if that's preferred for "no symbols given"
            raise APIError(
                code=APIErrorCode.INVALID_PARAMS.value,
                message="At least one symbol is required for get_funding_rates on Backpack.",
            )

        rates: list[FundingRate] = []
        for symbol_item in symbols:
            try:
                # Call the service's own get_funding_rate method for a single symbol
                current_rate: FundingRate = await self.get_funding_rate(symbol_item)
                rates.append(current_rate)
            except APIError as e:
                logger.error(
                    f"[{self._exchange_name}] Failed to fetch current funding rate for "
                    f"{symbol_item} within get_funding_rates service method: {e.message}"
                )
                # Option: collect errors and continue, or raise immediately.
                # For consistency with how API was, re-raising.
                raise APIError(
                    message=f"Failed to get funding rate for {symbol_item}: {e.message}",
                    code=e.code,  # Preserve original error code
                    http_status=e.http_status,
                    original_exception=e,
                    exchange_message=e.exchange_message,
                ) from e
        return rates

    async def get_historical_funding_rates(
        self,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int | None = None,
    ) -> list[FundingRate]:
        """
        Retrieves historical funding rates for a symbol within a given time range.

        Args:
            symbol: The trading symbol (e.g., 'SOL_USDC').
            start_time_ms: Optional start time in milliseconds since Unix epoch.
            end_time_ms: Optional end time in milliseconds since Unix epoch.
            limit: Optional limit on the number of funding rates to return.

        Returns:
            A list of FundingRate internal domain models.

        Raises:
            APIError: If the API request fails or the response is invalid.
            ValueError: If transformation of raw data to internal models fails.
        """
        endpoint_path = "/api/v1/fundingRates"
        # Parameters now align with the corrected request builder method
        params = self._request_builder.build_get_historical_funding_rates_params(
            symbol=symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=limit,
        )

        logger.debug(
            f"[{self._exchange_name}] Requesting historical funding rates for {symbol} with params: {params}"
        )

        try:
            # Correctly call the _http_client_requester as a callable
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,  # Ensure using 'endpoint' to match typical call signature
                params=params,
                # Add is_public_info_endpoint=True if appropriate for this endpoint
                # Most market data endpoints are public.
                is_public_info_endpoint=True,
            )

            raw_funding_interval_rates: list[BackpackRawFundingIntervalRate] = (
                self._response_handler.handle_get_historical_funding_rates_response(
                    raw_response_content,
                    symbol=symbol,  # Pass symbol for context in handler
                    status_code=status_code,
                    headers=headers,
                )
            )

            internal_funding_rates: list[FundingRate] = []
            for raw_rate in raw_funding_interval_rates:
                try:
                    internal_rate = self._mapper.transform_raw_funding_interval_rate_to_internal(
                        raw=raw_rate,
                        symbol=raw_rate.symbol,  # Use symbol from raw data for mapper
                    )
                    internal_funding_rates.append(internal_rate)
                except ValueError as e_map:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping mapping for historical funding rate item "
                        f"for {raw_rate.symbol if raw_rate else 'unknown'}: {e_map}. Item: {raw_rate!r}"
                    )
            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_funding_rates)} historical funding rates for {symbol}"
            )
            return internal_funding_rates

        except APIError as e_api:
            logger.error(
                f"[{self._exchange_name}] API error fetching historical funding rates for {symbol}: {e_api}"
            )
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Validation error processing historical funding rates for {symbol}: {e_val}"
            )
            raise APIError(
                message=f"Validation error processing historical funding rates for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from e_val
        except ValueError as e_value:  # Catch mapping errors propagated from mapper
            logger.error(
                f"[{self._exchange_name}] Value error processing historical funding rates for {symbol}: {e_value}"
            )
            raise APIError(
                message=f"Value error processing historical funding rates for {symbol}: {e_value}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            ) from e_value
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching historical funding rates for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unhandled error during historical funding rates fetch for {symbol}",
                code=APIErrorCode.SERVICE_UNAVAILABLE.value,
            ) from e_unhandled

    async def get_market_data(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,  # Unix timestamp in seconds
        end_time: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        """Retrieves historical klines (OHLCV) for a specific symbol and interval."""
        start_time_ms = start_time * 1000 if start_time is not None else None
        end_time_ms = end_time * 1000 if end_time is not None else None
        effective_limit = limit if limit is not None else 100  # Default Backpack limit

        endpoint_path, params = self._request_builder.build_get_market_data_params(
            symbol=symbol,
            timeframe_str=interval,
            limit=effective_limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        logger.debug(f"[{self._exchange_name}] Requesting klines for {symbol}@{interval}")
        raw_data_list: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_tuple = await self._http_client_requester(
                method="GET", endpoint=endpoint_path, params=params, is_public_info_endpoint=True
            )
            raw_data_list, status_code, headers = response_tuple
            logger.debug(
                f"[{self._exchange_name}] Raw klines for {symbol}@{interval}: {raw_data_list!r} (Status: {status_code}, Headers: {headers})"
            )
            if raw_data_list is None:
                raise APIError(
                    f"No data for klines {symbol}@{interval}, status: {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Defensive check: klines endpoint should return a list.
            # RawJsonResponse can be dict | list | str | None. We expect list for klines.
            if not isinstance(raw_data_list, list):
                # This ignore was removed as the linter no longer flagged it as unreachable without the ignore.
                # If it becomes an issue again, it implies the linter needs this ignore.
                raise APIError(
                    f"Klines data received from requester is not list: {type(raw_data_list)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Handler now returns list[BackpackRawKline]
            raw_kline_models: list[BackpackRawKline] = (
                self._response_handler.handle_get_market_data_response(
                    raw_data_list, symbol, interval, status_code, headers
                )
            )
            internal_candles: list[Candle] = []
            for (
                raw_kline_model
            ) in raw_kline_models:  # Iterate directly over BackpackRawKline models
                try:
                    # No need to validate to BackpackRawKline here anymore
                    candle = self._mapper.transform_raw_kline_to_internal(
                        symbol, interval, raw_kline_model
                    )
                    internal_candles.append(candle)
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"Skipping kline map error for {symbol}@{interval}: {e_map_item}. Item: {repr(raw_kline_model)}"
                    )
            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_candles)} candles for {symbol}@{interval}"
            )
            return internal_candles
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for klines {symbol}@{interval}: {e_val}. Raw: {raw_data_list!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing klines failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data_list),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data_list!r}" if raw_data_list is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for klines {symbol}@{interval}: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for klines: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data_list),
            ) from e_unhandled
