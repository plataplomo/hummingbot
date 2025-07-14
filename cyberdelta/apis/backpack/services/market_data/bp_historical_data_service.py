"""Backpack Historical Data Service.

This service handles all historical data operations for the Backpack exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Recent trades retrieval
- Market data (K-lines/candlesticks) retrieval
- Historical data transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Literal, NoReturn, TypeGuard

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import (
    BackpackCandleMapper,
    BackpackFundingRateMapper,
    BackpackTradeMapper,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawRecentPublicTrade
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.market_data_service import (
    EmptySymbolError,
    InvalidLimitError,
    UnsupportedIntervalError,
)
from cyberdelta.apis.exceptions.response_validation import UnreachableCodeError
from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
from cyberdelta.apis.utils.response_validation import ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import Trade
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

# Type alias for supported timeframes
BackpackTimeframe = Literal[
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


class BackpackHistoricalDataService:
    """Focused service for Backpack historical data operations.

    Handles validation, processing, and transformation of historical data requests
    including recent trades and market data (K-lines/candlesticks).
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackMarketDataRequestBuilder,
        response_handler: BackpackMarketDataResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        trade_mapper: BackpackTradeMapper | None = None,
        candle_mapper: BackpackCandleMapper | None = None,
        funding_rate_mapper: BackpackFundingRateMapper | None = None,
    ) -> None:
        """Initialize the historical data service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            trade_mapper: Optional trade mapper instance (defaults to new instance)
            candle_mapper: Optional candle mapper instance (defaults to new instance)
            funding_rate_mapper: Optional funding rate mapper instance (defaults to new instance)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._trade_mapper = trade_mapper or BackpackTradeMapper()
        self._candle_mapper = candle_mapper or BackpackCandleMapper()
        self._funding_rate_mapper = funding_rate_mapper or BackpackFundingRateMapper()

    async def get_recent_trades(self, symbol: str, limit: int | None = 100) -> list[Trade]:
        """Retrieves recent trades for a specific symbol.

        Args:
            symbol: The trading symbol to get recent trades for
            limit: Maximum number of trades to retrieve (default: 100)

        Returns:
            list[Trade]: List of recent trades

        Raises:
            APIError: If trade retrieval fails or processing fails
            EmptySymbolError: If symbol is empty or whitespace
            InvalidLimitError: If limit is <= 0
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_recent_trades"

        self._validate_recent_trades_params(symbol, limit, current_method)

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            logger.info(
                "retrieving_recent_trades",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                limit=limit,
                message="Retrieving recent trades from exchange",
            )

            raw_data_list, status_code, headers = await self._execute_recent_trades_request(
                symbol,
                limit,
            )
            raw_response_content = str(raw_data_list)

            self._validate_trades_response(raw_data_list, status_code)

            # Type narrowing: after validation, raw_data_list cannot be None
            if raw_data_list is None:
                self._raise_unreachable_none_error("raw_data_list")

            # Type narrowing after validation ensures raw_data_list is not None
            validated_data = raw_data_list
            trades = self._process_recent_trades_response(
                validated_data, symbol, status_code, headers
            )

            logger.info(
                "recent_trades_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                trade_count=len(trades),
                message="Successfully retrieved recent trades",
            )

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
            ) from e
        else:
            return trades

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Retrieves market data (K-lines/candlesticks) for a specific symbol and timeframe.

        Args:
            args: Market data request arguments containing symbol, timeframe, and time range

        Returns:
            list[Candle]: List of candlestick data

        Raises:
            APIError: If market data retrieval fails or processing fails
            UnsupportedIntervalError: If timeframe is not supported
        """
        # Service Input Parameter Validation is now handled by GetMarketDataArgs Pydantic model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market_data"

        # Validate timeframe and prepare request
        validated_timeframe = self._validate_and_prepare_timeframe(args.timeframe, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "retrieving_market_data",
                exchange=self._exchange_name,
                method=current_method,
                symbol=args.symbol,
                timeframe=args.timeframe,
                start_time=args.start_time_ms,
                end_time=args.end_time_ms,
                message="Retrieving market data from exchange",
            )

            candles = await self._execute_market_data_request(args, validated_timeframe)

            logger.info(
                "market_data_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=args.symbol,
                candle_count=len(candles),
                message="Successfully retrieved market data",
            )

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
        else:
            return candles

    async def _execute_recent_trades_request(
        self,
        symbol: str,
        limit: int | None,
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Execute the recent trades API request.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades

        Returns:
            tuple: Raw data, status code, and headers
        """
        params = self._request_builder.build_get_recent_trades_params(symbol=symbol, limit=limit)
        endpoint_path = "/api/v1/trades"

        logger.debug(
            "recent_trades_request",
            exchange=self._exchange_name,
            symbol=symbol,
            limit=limit,
            endpoint_path=endpoint_path,
            params=params,
            message="Requesting recent trades from endpoint",
        )

        raw_data, status_code, headers = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(by_alias=True, exclude_none=True),
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

        return raw_data, status_code, headers

    def _process_recent_trades_response(
        self,
        raw_data_list: ParsedJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[Trade]:
        """Process the recent trades response.

        Args:
            raw_data_list: Raw response data
            symbol: Trading symbol
            status_code: HTTP status code
            headers: Response headers

        Returns:
            list[Trade]: Processed trades
        """
        validated_list = ensure_list_response(
            raw_data_list, f"recent trades ({symbol})", status_code
        )

        raw_trades_list: list[BackpackRawRecentPublicTrade] = (
            self._response_handler.handle_get_recent_trades_response(
                validated_list,
                symbol,
                status_code,
                headers,
            )
        )

        return [
            self._trade_mapper.transform_raw_recent_trade_to_internal(raw_trade, symbol)
            for raw_trade in raw_trades_list
        ]

    async def _execute_market_data_request(
        self,
        args: GetMarketDataArgs,
        validated_timeframe: BackpackTimeframe,
    ) -> list[Candle]:
        """Execute the market data (K-lines) API request.

        Args:
            args: Market data request arguments
            validated_timeframe: Validated timeframe string

        Returns:
            list[Candle]: Processed candles
        """
        params = self._request_builder.build_get_market_data_params(
            symbol=args.symbol,
            interval=validated_timeframe,
            start_time=args.start_time_ms or 0,
            end_time=args.end_time_ms,
            limit=500,  # Default limit instead of None
        )
        endpoint_path = "/api/v1/klines"

        logger.debug(
            "market_data_request",
            exchange=self._exchange_name,
            symbol=args.symbol,
            interval=validated_timeframe,
            endpoint_path=endpoint_path,
            params=params,
            message="Requesting market data from endpoint",
        )

        raw_data, status_code, headers = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(by_alias=True, exclude_none=True),
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

        return self._process_market_data_response(
            raw_data,
            args.symbol,
            validated_timeframe,
            status_code,
            headers,
        )

    def _process_market_data_response(
        self,
        raw_data: ParsedJsonResponse | None,
        symbol: str,
        interval: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[Candle]:
        """Process the market data response.

        Args:
            raw_data: Raw response data
            symbol: Trading symbol
            interval: Timeframe/interval
            status_code: HTTP status code
            headers: Response headers

        Returns:
            list[Candle]: Processed candles
        """
        validated_data = ensure_list_response(
            raw_data,
            f"market data ({symbol}, {interval})",
            status_code,
        )

        raw_klines: list[BackpackRawKline] = self._response_handler.handle_get_market_data_response(
            validated_data,
            symbol,
            interval,
            status_code,
            headers,
        )

        return [
            self._candle_mapper.transform_raw_kline_to_internal(symbol, interval, raw_kline)
            for raw_kline in raw_klines
        ]

    def _validate_recent_trades_params(
        self, symbol: str, limit: int | None, current_method: str
    ) -> None:
        """Validate recent trades parameters.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades
            current_method: Name of calling method

        Raises:
            EmptySymbolError: If symbol is empty
            InvalidLimitError: If limit is invalid
        """
        if not symbol:
            raise EmptySymbolError(current_method)
        if limit is not None and limit <= 0:
            raise InvalidLimitError(current_method, limit)

    def _validate_and_prepare_timeframe(
        self, timeframe: str, current_method: str
    ) -> BackpackTimeframe:
        """Validate and prepare the timeframe parameter.

        Args:
            timeframe: Requested timeframe
            current_method: Name of calling method

        Returns:
            BackpackTimeframe: Validated timeframe

        Raises:
            UnsupportedIntervalError: If timeframe is not supported
        """
        if not self._is_valid_backpack_timeframe(timeframe):
            raise UnsupportedIntervalError(
                current_method, timeframe, ["1m", "5m", "15m", "1h", "4h", "1d"]
            )
        return timeframe

    @staticmethod
    def _is_valid_backpack_timeframe(value: str) -> TypeGuard[BackpackTimeframe]:
        """Check if a string is a valid Backpack timeframe.

        Args:
            value: Timeframe string to validate

        Returns:
            bool: True if valid timeframe
        """
        valid_timeframes: set[BackpackTimeframe] = {
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
        }
        return value in valid_timeframes

    def _handle_recent_trades_exceptions(
        self,
        exception: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle exceptions for recent trades operations.

        Args:
            exception: The exception to handle
            current_method: Name of calling method
            symbol: Trading symbol
            status_code: HTTP status code
            raw_response_content: Raw response content
        """
        if isinstance(exception, APIError):
            raise exception

        error_message = "Failed to retrieve recent trades"
        error_code = APIErrorCode.UNKNOWN

        if isinstance(exception, TransformationError):
            error_message = "Failed to process/transform exchange data"
            error_code = APIErrorCode.INVALID_RESPONSE
        elif isinstance(exception, ValidationError):
            error_message = "Internal data validation failed"
            error_code = APIErrorCode.INVALID_RESPONSE

        logger.error(
            "recent_trades_error",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            error=str(exception),
            message=error_message,
        )

        raise APIError(
            code=error_code.value,
            message=error_message,
            original_exception=exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from exception

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
        """Create a standardized APIError for market data operations.

        Args:
            original_exception: The original exception
            current_method: Name of calling method
            symbol: Trading symbol
            status_code: HTTP status code
            raw_response_content: Raw response content
            message: Error message
            error_code: API error code

        Returns:
            APIError: Standardized error
        """
        logger.error(
            "market_data_error",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            error=str(original_exception),
            message=f"Market data failed: {message}",
        )

        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _validate_trades_response(
        self, raw_data_list: ParsedJsonResponse | None, status_code: int
    ) -> None:
        """Validate trades response data.

        Args:
            raw_data_list: Raw response data
            status_code: HTTP status code

        Raises:
            APIError: If response is None
        """
        if raw_data_list is None:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Received None response from recent trades API",
                http_status=status_code,
            )

    def _raise_unreachable_none_error(self, variable_name: str) -> NoReturn:
        """Raise UnreachableCodeError for None after validation.

        Args:
            variable_name: Name of the variable that is None

        Raises:
            UnreachableCodeError: Always raises
        """
        raise UnreachableCodeError
