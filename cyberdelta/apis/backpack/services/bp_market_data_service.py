"""CyberDeltaEngine: Backpack Market Data Service.

---------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Backpack Exchange. It uses the HttpClient, BackpackRequestBuilder,
BackpackResponseHandler to interact with the API
and returns validated Raw Pydantic Models.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Literal, cast

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder

# Import RawJsonResponse from bp_response_handler where it's defined as an alias
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
)
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
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
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketDataArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.market import (
    FundingRate,
    OrderBook,
    Ticker,
    Trade,
)

# Internal domain models
from cyberdelta.core.models.market.candle import Candle

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackMarketDataService:
    """Service class for Backpack market data operations.

    Returns Internal Domain Models.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: BackpackRequestBuilder
    _response_handler: BackpackResponseHandler
    _mapper: BackpackMarketDataMapper
    _exchange_name: str

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
        mapper: BackpackMarketDataMapper | None = None,
    ) -> None:
        """Initialize the BackpackMarketDataService.

        Args:
            http_client_requester: A callable for making API requests (e.g., BackpackAPI._request).
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            exchange_name: The name of the exchange.
            mapper: Optional mapper instance for dependency injection.

        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._exchange_name = exchange_name
        self._mapper = mapper or BackpackMarketDataMapper()

    async def get_ticker(self, symbol: str) -> Ticker:
        """Retrieve the latest ticker information for a specific symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_ticker"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            params = self._request_builder.build_get_ticker_params(symbol=symbol)
            endpoint_path = "/api/v1/ticker"
            logger.debug(
                f"[{self._exchange_name}] Requesting ticker for {symbol} from {endpoint_path} "
                f"with params: {params}",
            )

            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            raw_data, status_code, headers = response_tuple

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw ticker response for {symbol}: {raw_data!r} "
                f"(Status: {status_code}, Headers: {headers})",
            )

            if raw_data is None or not isinstance(raw_data, dict):
                raise APIError(
                    f"Ticker for {symbol} returned invalid data (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_ticker_model: BackpackRawTicker = self._response_handler.handle_get_ticker_response(
                raw_data,
                symbol,
                status_code,
                headers,
            )
            internal_ticker = self._mapper.transform_raw_ticker_to_internal(
                raw_ticker_model,
                symbol_override=symbol,
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped internal ticker for {symbol}: {internal_ticker}",
            )
            return internal_ticker

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected error "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unhandled

    async def get_all_tickers(self) -> dict[str, Ticker]:
        """Retrieves tickers for all available markets."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_tickers"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            # TODO: Backpack API does not seem to have a single endpoint for all tickers.
            # This might require fetching all symbols first, then getting ticker for each.
            # Or, the OpenAPI spec might list all tickers directly under /markets.
            # For now, this is not implemented as the builder/handler methods are missing.
            logger.warning(
                f"[{self._exchange_name}] get_all_tickers is not implemented yet for Backpack.",
            )
            raise NotImplementedError(
                "get_all_tickers is not implemented for BackpackMarketDataService",
            )

        except APIError:
            # Re-raise APIErrors from any future implementation
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for get_all_tickers: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for get_all_tickers: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for get_all_tickers: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for get_all_tickers: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_order_book(self, symbol: str, limit: int | None = 20) -> OrderBook:
        """Retrieves the order book for a specific symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_book"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")
        if limit is not None and limit <= 0:
            raise ValueError(f"[{current_method}] 'limit' must be positive when provided.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            params = self._request_builder.build_get_order_book_params(symbol=symbol, limit=limit)
            endpoint_path = "/api/v1/depth"
            logger.debug(
                f"[{self._exchange_name}] Requesting order book for {symbol} (limit: {limit}) "
                f"from {endpoint_path} with params: {params}",
            )

            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            raw_data, status_code, headers = response_tuple

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw order_book for {symbol}: {raw_data!r} "
                f"(Status: {status_code}, Headers: {headers})",
            )

            if raw_data is None or not isinstance(raw_data, dict):
                raise APIError(
                    f"Order book for {symbol} returned invalid data (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_book_model: BackpackRawOrderBook = (
                self._response_handler.handle_get_order_book_response(
                    raw_data,
                    symbol,
                    status_code,
                    headers,
                )
            )
            internal_order_book = self._mapper.transform_raw_order_book_to_internal(
                symbol,
                raw_order_book_model,
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped order_book for {symbol}: {internal_order_book}",
            )
            return internal_order_book

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected error "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unhandled

    def _validate_recent_trades_params(
        self,
        symbol: str,
        limit: int | None,
        current_method: str,
    ) -> None:
        """Validate parameters for get_recent_trades."""
        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")
        if limit is not None and limit <= 0:
            raise ValueError(f"[{current_method}] 'limit' must be positive when provided.")

    async def _execute_recent_trades_request(
        self,
        symbol: str,
        limit: int | None = 100,
    ) -> tuple[ParsedJsonResponse, int, dict[str, str]]:
        """Execute recent trades API request."""
        params = self._request_builder.build_get_recent_trades_params(
            symbol=symbol,
            limit=limit,
        )
        endpoint_path = "/api/v1/trades"
        logger.debug(
            f"[{self._exchange_name}] Requesting recent trades for {symbol} (limit: {limit}) "
            f"from {endpoint_path} with params: {params}",
        )

        response_tuple = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        raw_data_list, status_code, headers = response_tuple

        logger.debug(
            f"[{self._exchange_name}] Raw recent_trades for {symbol}: {raw_data_list!r} "
            f"(Status: {status_code}, Headers: {headers})",
        )

        if raw_data_list is None or not isinstance(raw_data_list, list):
            raise APIError(
                f"Recent trades for {symbol} returned invalid data (status: {status_code})",
                APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        return raw_data_list, status_code, dict(headers)

    def _process_recent_trades_response(
        self,
        raw_data_list: ParsedJsonResponse,
        symbol: str,
        status_code: int,
        headers: dict[str, str],
    ) -> list[Trade]:
        """Process and transform recent trades response."""
        raw_trade_models: list[BackpackRawTrade] = (
            self._response_handler.handle_get_recent_trades_response(
                raw_data_list,
                symbol,
                status_code,
                headers,
            )
        )
        internal_trades: list[Trade] = []
        for raw_model in raw_trade_models:
            try:
                trade = self._mapper.transform_raw_trade_to_internal(raw_model)
                internal_trades.append(trade)
            except (ValidationError, ValueError) as e_map_item:
                raw_data_str = (
                    raw_model.model_dump_json()
                    if hasattr(raw_model, "model_dump_json")
                    else repr(raw_model)
                )
                logger.warning(f"Skipping trade map error: {e_map_item}. Raw: {raw_data_str}")
        logger.debug(
            f"[{self._exchange_name}] Mapped recent_trades for {symbol}: {internal_trades}",
        )
        return internal_trades

    def _handle_recent_trades_exceptions(
        self,
        e: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various recent trades-related exceptions."""
        if isinstance(e, APIError):
            raise
        elif isinstance(e, TransformationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        elif isinstance(e, ValidationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        elif isinstance(e, ValueError | TypeError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        else:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected error for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = 100) -> list[Trade]:
        """Retrieves recent trades for a specific symbol."""
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_recent_trades"

        self._validate_recent_trades_params(symbol, limit, current_method)

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            raw_data_list, status_code, headers = await self._execute_recent_trades_request(
                symbol,
                limit,
            )
            raw_response_content = str(raw_data_list)
            return self._process_recent_trades_response(raw_data_list, symbol, status_code, headers)
        except Exception as e:
            self._handle_recent_trades_exceptions(
                e,
                current_method,
                symbol,
                status_code,
                raw_response_content,
            )
            # DEFENSIVE CHECK: This should never be reached as
            # _handle_recent_trades_exceptions raises
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in get_recent_trades",
                original_exception=e,
                http_status=status_code,
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Retrieves the current funding rate for a specific symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rate"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            params = self._request_builder.build_get_funding_rate_params(symbol=symbol)
            endpoint_path = "/api/v1/funding"  # Define endpoint path in service
            logger.debug(
                f"[{self._exchange_name}] Requesting funding rate for {symbol} from "
                f"{endpoint_path} with params: {params}",
            )

            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            raw_data, status_code, headers = response_tuple

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw funding_rate for {symbol}: {raw_data!r} "
                f"(Status: {status_code}, Headers: {headers})",
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
                    raw_data,
                    symbol,
                    status_code,
                    headers,
                )
            )
            internal_funding_rate = self._mapper.transform_raw_funding_rate_to_internal(
                raw_funding_rate_model,  # Removed symbol_override=symbol as mapper does not take it
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped funding_rate for {symbol}: "
                f"{internal_funding_rate}",
            )
            return internal_funding_rate

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    def _validate_funding_rates_symbols(self, symbols: list[str], current_method: str) -> None:
        """Validate symbols for get_funding_rates."""
        if not symbols:
            raise ValueError(f"[{current_method}] At least one symbol is required for Backpack.")
        for symbol in symbols:
            if not symbol:
                raise ValueError(
                    f"[{current_method}] All symbols in list must be non-empty strings.",
                )

    async def _fetch_individual_funding_rates(self, symbols: list[str]) -> list[FundingRate]:
        """Fetch funding rates for individual symbols."""
        rates: list[FundingRate] = []
        for symbol_item in symbols:
            try:
                current_rate: FundingRate = await self.get_funding_rate(symbol_item)
                rates.append(current_rate)
            except APIError as e:
                logger.error(
                    f"[{self._exchange_name}] Failed to fetch current funding rate for "
                    f"{symbol_item} within get_funding_rates service method: {e.message}",
                )
                raise APIError(
                    message=f"Failed to get funding rate for {symbol_item}: {e.message}",
                    code=e.code,
                    http_status=e.http_status,
                    original_exception=e,
                    exchange_message=e.exchange_message,
                ) from e
        return rates

    def _handle_funding_rates_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle exceptions for get_funding_rates."""
        if isinstance(e, APIError):
            raise
        elif isinstance(e, TransformationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange data: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        elif isinstance(e, ValidationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation failed: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        elif isinstance(e, ValueError | TypeError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        else:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure: {e}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Retrieves current funding rates for one or more symbols.

        If Backpack API doesn't support a bulk endpoint, this method iterates
        and calls the single-symbol funding rate endpoint.
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rates"

        # DEFENSIVE CHECK: Ensure symbols is not None. Mypy=[arg-type]
        if args.symbols is None:
            raise ValueError(f"[{current_method}] symbols cannot be None")

        self._validate_funding_rates_symbols(args.symbols, current_method)

        status_code: int = 0
        raw_response_content: str | None = None

        try:
            return await self._fetch_individual_funding_rates(args.symbols)
        except Exception as e:
            self._handle_funding_rates_exceptions(
                e,
                current_method,
                status_code,
                raw_response_content,
            )
            # DEFENSIVE CHECK: This should never be reached as
            # _handle_funding_rates_exceptions raises
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in get_funding_rates",
                original_exception=e,
                http_status=status_code,
            ) from e

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Retrieves historical funding rates for a symbol within a given time range.

        Args:
            args: Parameters for historical funding rate request including
                 symbol (required), optional time range (start_time, end_time),
                 and optional limit.

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "get_historical_funding_rates"
        )

        # Convert and validate time parameters
        start_time_ms, end_time_ms = self._process_funding_rate_time_params(args, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            return await self._execute_funding_rates_request(
                args, start_time_ms, end_time_ms, current_method
            )
        except APIError:
            raise
        except TransformationError as e_transform:
            raise self._create_funding_rates_api_error(
                e_transform,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_funding_rates_api_error(
                e_val,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._create_funding_rates_api_error(
                e_service_logic,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Service internal logic error.",
                APIErrorCode.UNKNOWN,
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._create_funding_rates_api_error(
                e_unexpected,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

    def _process_funding_rate_time_params(
        self, args: GetHistoricalFundingRatesArgs, current_method: str
    ) -> tuple[int | None, int | None]:
        """Process and validate time parameters for funding rate requests."""
        start_time_ms: int | None = None
        if args.start_time is not None:
            if args.start_time.tzinfo is None:
                logger.warning(
                    f"[{self._exchange_name}] start_time for get_historical_funding_rates "
                    f"is naive. Assuming UTC.",
                )
            start_time_ms = int(args.start_time.timestamp())
            if start_time_ms <= 0:
                raise ValueError(f"[{current_method}] 'start_time' must be positive when provided.")

        end_time_ms: int | None = None
        if args.end_time is not None:
            if args.end_time.tzinfo is None:
                logger.warning(
                    f"[{self._exchange_name}] end_time for get_historical_funding_rates "
                    f"is naive. Assuming UTC.",
                )
            end_time_ms = int(args.end_time.timestamp())
            if end_time_ms <= 0:
                raise ValueError(f"[{current_method}] 'end_time' must be positive when provided.")

        return start_time_ms, end_time_ms

    async def _execute_funding_rates_request(
        self,
        args: GetHistoricalFundingRatesArgs,
        start_time_ms: int | None,
        end_time_ms: int | None,
        current_method: str,
    ) -> list[FundingRate]:
        """Execute the funding rates API request and process the response."""
        # Core operational logic
        params = self._request_builder.build_get_historical_funding_rates_params(
            symbol=args.symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=args.limit,
        )
        endpoint_path = "/api/v1/funding/history"
        logger.debug(
            f"[{self._exchange_name}] Requesting historical funding rates for {args.symbol} "
            f"from {endpoint_path} with params: {params}",
        )

        response_tuple = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        raw_data, status_code, headers = response_tuple

        return await self._process_funding_rates_response(
            raw_data, status_code, headers, args.symbol
        )

    async def _process_funding_rates_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        headers: Mapping[str, str],
        symbol: str,
    ) -> list[FundingRate]:
        """Process the funding rates API response and transform to internal models."""
        if raw_data is not None:
            str(raw_data)

        logger.debug(
            f"[{self._exchange_name}] Raw historical funding rates for {symbol}: "
            f"{raw_data!r} (Status: {status_code}, Headers: {headers})",
        )

        if raw_data is None:
            raise APIError(
                message=f"No data for historical funding rates {symbol}, status: {status_code}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        if not isinstance(raw_data, list):
            logger.error(
                f"[{self._exchange_name}] Historical funding rates data for {symbol} "
                f"is not a list: {type(raw_data)}. Raw: {raw_data!r}, Status: {status_code}",
            )
            raise APIError(
                f"Historical funding rates data for {symbol} is not a list: {type(raw_data)}",
                APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
                exchange_message=str(raw_data),
            )

        raw_funding_interval_rates: list[BackpackRawFundingIntervalRate] = (
            self._response_handler.handle_get_historical_funding_rates_response(
                raw_data,
                symbol,
                status_code,
                headers,
            )
        )

        return self._transform_funding_rates_to_internal(raw_funding_interval_rates, symbol)

    def _transform_funding_rates_to_internal(
        self,
        raw_funding_interval_rates: list[BackpackRawFundingIntervalRate],
        symbol: str,
    ) -> list[FundingRate]:
        """Transform raw funding rates to internal domain models."""
        internal_funding_rates: list[FundingRate] = []
        for raw_rate in raw_funding_interval_rates:
            try:
                transformed_rate = self._mapper.transform_raw_funding_interval_rate_to_internal(
                    raw_rate,
                    symbol=symbol,
                )
                internal_funding_rates.append(transformed_rate)
            except (ValidationError, ValueError) as e_map_item:
                logger.warning(
                    f"[{self._exchange_name}] Skipping mapping for historical funding rate "
                    f"item for {symbol}: {e_map_item}. Item: {raw_rate!r}",
                )

        logger.debug(
            f"[{self._exchange_name}] Mapped {len(internal_funding_rates)} internal "
            f"historical funding rates for {symbol}",
        )
        return internal_funding_rates

    def _create_funding_rates_api_error(
        self,
        original_exception: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for funding rates operations."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: {message} for {symbol}: "
            f"{original_exception}",
            exc_info=True,
        )
        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Retrieves market data (K-lines/candlesticks) for a specific symbol and timeframe."""
        # Service Input Parameter Validation is now handled by GetMarketDataArgs Pydantic model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market_data"

        # Validate timeframe and prepare request
        validated_timeframe = self._validate_and_prepare_timeframe(args.timeframe, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            return await self._execute_market_data_request(args, validated_timeframe)
        except APIError:
            raise
        except TransformationError as e_transform:
            raise self._create_market_data_api_error(
                e_transform,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_market_data_api_error(
                e_val,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._create_market_data_api_error(
                e_service_logic,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Service internal logic error.",
                APIErrorCode.UNKNOWN,
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._create_market_data_api_error(
                e_unexpected,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

    def _validate_and_prepare_timeframe(
        self, timeframe: str, current_method: str
    ) -> Literal[
        "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
    ]:
        """Validate timeframe is supported by Backpack and return typed literal."""
        supported_intervals = {
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
        }
        if timeframe not in supported_intervals:
            raise ValueError(
                f"[{current_method}] Unsupported interval '{timeframe}'. "
                f"Supported intervals: {sorted(supported_intervals)}",
            )

        # DEFENSIVE CHECK: Service validates timeframe is supported literal value.
        # Mypy=[arg-type]
        return cast(
            Literal[
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
            ],
            timeframe,
        )

    async def _execute_market_data_request(
        self,
        args: GetMarketDataArgs,
        validated_timeframe: Literal[
            "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
        ],
    ) -> list[Candle]:
        """Execute the market data API request and process the response."""
        params = self._request_builder.build_get_market_data_params(
            symbol=args.symbol,
            timeframe_str=validated_timeframe,
            start_time_ms=args.start_time_ms,
            end_time_ms=args.end_time_ms,
            limit=args.limit,
        )
        endpoint_path = "/api/v1/klines"
        logger.debug(
            f"[{self._exchange_name}] Requesting klines for {args.symbol}@{args.timeframe} "
            f"from {endpoint_path} with params: {params}",
        )

        response_tuple = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        raw_data_list, status_code, headers = response_tuple

        return await self._process_market_data_response(
            raw_data_list, status_code, headers, args.symbol, args.timeframe
        )

    async def _process_market_data_response(
        self,
        raw_data_list: ParsedJsonResponse | None,
        status_code: int,
        headers: Mapping[str, str],
        symbol: str,
        timeframe: str,
    ) -> list[Candle]:
        """Process the market data API response and transform to internal models."""
        if raw_data_list is not None:
            str(raw_data_list)

        logger.debug(
            f"[{self._exchange_name}] Raw klines for {symbol}@{timeframe}: "
            f"{raw_data_list!r} (Status: {status_code}, Headers: {headers})",
        )

        if raw_data_list is None:
            raise APIError(
                f"No data for klines {symbol}@{timeframe}, status: {status_code}",
                APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        if not isinstance(raw_data_list, list):
            raise APIError(
                f"Klines data received from requester is not list: {type(raw_data_list)}",
                APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        raw_kline_models: list[BackpackRawKline] = (
            self._response_handler.handle_get_market_data_response(
                raw_data_list,
                symbol,
                timeframe,
                status_code,
                headers,
            )
        )

        return self._transform_klines_to_internal(raw_kline_models, symbol, timeframe)

    def _transform_klines_to_internal(
        self,
        raw_kline_models: list[BackpackRawKline],
        symbol: str,
        timeframe: str,
    ) -> list[Candle]:
        """Transform raw klines to internal domain models."""
        internal_candles: list[Candle] = []
        for raw_kline_model in raw_kline_models:
            try:
                candle = self._mapper.transform_raw_kline_to_internal(
                    symbol,
                    timeframe,
                    raw_kline_model,
                )
                internal_candles.append(candle)
            except (ValidationError, ValueError) as e_map_item:
                logger.warning(
                    f"Skipping kline map error for {symbol}@{timeframe}: "
                    f"{e_map_item}. Item: {repr(raw_kline_model)}",
                )
        logger.debug(
            f"[{self._exchange_name}] Mapped {len(internal_candles)} candles for "
            f"{symbol}@{timeframe}",
        )
        return internal_candles

    def _create_market_data_api_error(
        self,
        original_exception: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for market data operations."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: {message} for {symbol}: "
            f"{original_exception}",
            exc_info=True,
        )
        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
