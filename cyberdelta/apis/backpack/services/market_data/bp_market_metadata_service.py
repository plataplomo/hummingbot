"""Backpack Market Metadata Service.

This service handles all market metadata operations for the Backpack exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Market information retrieval (symbols, tick sizes, trading rules)
- Funding rate retrieval
- Market metadata transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackFundingRateMapper, BackpackMarketMapper
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingIntervalRate
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawMarketResponse
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.market_data_service import (
    EmptySymbolError,
    EmptySymbolInListError,
    EmptySymbolListError,
    NoFundingDataError,
    NullSymbolsError,
)
from cyberdelta.apis.models.service_args.market_data import (
    GetFundingRatesArgs,
    GetMarketArgs,
    GetMarketsArgs,
)
from cyberdelta.apis.utils.response_validation import ensure_dict_response, ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import FundingRate, Market
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackMarketMetadataService:
    """Focused service for Backpack market metadata operations.

    Handles validation, processing, and transformation of market metadata requests
    including market information, trading rules, and funding rates.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackMarketDataRequestBuilder,
        response_handler: BackpackMarketDataResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        market_mapper: BackpackMarketMapper | None = None,
        funding_rate_mapper: BackpackFundingRateMapper | None = None,
    ) -> None:
        """Initialize the market metadata service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            market_mapper: Optional market mapper instance (defaults to new instance)
            funding_rate_mapper: Optional funding rate mapper instance (defaults to new instance)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._market_mapper = market_mapper or BackpackMarketMapper()
        self._funding_rate_mapper = funding_rate_mapper or BackpackFundingRateMapper()

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Retrieve market metadata for a specific symbol.

        Returns market metadata including tick size and trading rules for a single symbol.
        This method provides access to the /api/v1/market endpoint to get
        precision information needed for order placement.

        Args:
            args: Parameters for market metadata request including symbol.

        Returns:
            Market: Market metadata for the specified symbol

        Raises:
            APIError: If market retrieval fails or processing fails
            EmptySymbolError: If symbol is empty
        """
        symbol = args.symbol
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market"

        # Service Input Parameter Validation
        if not symbol:
            raise EmptySymbolError(current_method)

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic following the established pattern
            logger.info(
                "retrieving_market",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                message="Retrieving market metadata from exchange",
            )

            params = self._request_builder.build_get_market_params(symbol=symbol)
            endpoint_path = "/api/v1/market"

            logger.debug(
                "market_request",
                exchange=self._exchange_name,
                symbol=symbol,
                endpoint_path=endpoint_path,
                params=params,
                message="Requesting market metadata from endpoint",
            )

            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                    endpoint_group="public",
                    request_weight=1,
                ),
            )
            raw_data, status_code, headers = response_tuple

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                "market_response",
                exchange=self._exchange_name,
                symbol=symbol,
                raw_data=raw_data,
                status_code=status_code,
                headers=headers,
                message="Received raw market response",
            )

            validated_data = ensure_dict_response(raw_data, f"market ({symbol})", status_code)

            raw_market_model: BackpackRawMarketResponse = (
                self._response_handler.handle_get_market_response(
                    validated_data,
                    symbol,
                    status_code,
                    headers,
                )
            )
            internal_market = self._market_mapper.transform_raw_market_to_internal(raw_market_model)

            logger.info(
                "market_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                tick_size=str(internal_market.tick_size) if internal_market.tick_size else None,
                message="Successfully retrieved market metadata",
            )

            logger.debug(
                "market_mapped",
                exchange=self._exchange_name,
                symbol=symbol,
                internal_market=internal_market,
                message="Mapped market to internal model",
            )

        except APIError:
            raise
        except Exception as e:
            self._handle_market_exceptions(
                e,
                current_method,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # Re-raise after handling

        else:
            return internal_market

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Retrieve market metadata for all available markets.

        Returns market metadata including tick sizes and trading rules for all
        available trading pairs on the exchange.

        Args:
            args: Parameters for markets request

        Returns:
            list[Market]: List of all available markets

        Raises:
            APIError: If markets retrieval fails or processing fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_markets"

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "retrieving_markets",
                exchange=self._exchange_name,
                method=current_method,
                message="Retrieving all markets metadata from exchange",
            )

            endpoint_path = "/api/v1/markets"

            logger.debug(
                "markets_request",
                exchange=self._exchange_name,
                endpoint_path=endpoint_path,
                message="Requesting all markets from endpoint",
            )

            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                    endpoint_group="public",
                    request_weight=1,
                ),
            )
            raw_data, status_code, _headers = response_tuple

            if raw_data is not None:
                raw_response_content = str(raw_data)

            validated_data = ensure_list_response(raw_data, "markets", status_code)

            raw_markets_list: list[BackpackRawMarketResponse] = (
                self._response_handler.handle_get_markets_response(
                    validated_data,
                    status_code,
                )
            )

            internal_markets = [
                self._market_mapper.transform_raw_market_to_internal(raw_market)
                for raw_market in raw_markets_list
            ]

            logger.info(
                "markets_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                market_count=len(internal_markets),
                message="Successfully retrieved all markets metadata",
            )

        except APIError:
            raise
        except Exception as e:
            self._handle_markets_exceptions(
                e,
                current_method,
                status_code,
                raw_response_content,
            )
            raise  # Re-raise after handling
        else:
            return internal_markets

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Retrieves the current funding rate for a specific symbol.

        Args:
            symbol: The trading symbol to get funding rate for

        Returns:
            FundingRate: Current funding rate information

        Raises:
            APIError: If funding rate retrieval fails
        """
        self._validate_funding_rate_symbol(symbol)

        try:
            logger.info(
                "retrieving_funding_rate",
                exchange=self._exchange_name,
                symbol=symbol,
                message="Retrieving funding rate from exchange",
            )

            raw_funding_interval_rates = await self._fetch_funding_rate_data(symbol)
            funding_rate = self._process_funding_rate_response(raw_funding_interval_rates, symbol)

            logger.info(
                "funding_rate_retrieved",
                exchange=self._exchange_name,
                symbol=symbol,
                rate=str(funding_rate.funding_rate),
                message="Successfully retrieved funding rate",
            )

        except APIError:
            raise
        except Exception as e:
            self._handle_funding_rate_error(e, symbol)
            raise
        else:
            return funding_rate

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Retrieves current funding rates for one or more symbols.

        If Backpack API doesn't support a bulk endpoint, this method iterates
        and calls the single-symbol funding rate endpoint.

        Args:
            args: Parameters for funding rates request

        Returns:
            list[FundingRate]: List of funding rates

        Raises:
            APIError: If funding rates retrieval fails
            NullSymbolsError: If symbols list is None (from validation)
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rates"

        # DEFENSIVE CHECK: Ensure symbols is not None
        if args.symbols is None:
            raise NullSymbolsError(current_method)

        self._validate_funding_rates_symbols(args.symbols, current_method)

        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "retrieving_funding_rates",
                exchange=self._exchange_name,
                method=current_method,
                symbol_count=len(args.symbols),
                message="Retrieving funding rates for multiple symbols",
            )

            funding_rates = await self._fetch_individual_funding_rates(args.symbols)

            logger.info(
                "funding_rates_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                rate_count=len(funding_rates),
                message="Successfully retrieved funding rates",
            )

        except Exception as e:
            self._handle_funding_rates_exceptions(
                e,
                current_method,
                status_code,
                raw_response_content,
            )
            # DEFENSIVE CHECK: This should never be reached
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in get_funding_rates",
                original_exception=e,
            ) from e

        return funding_rates

    async def _fetch_funding_rate_data(self, symbol: str) -> list[BackpackRawFundingIntervalRate]:
        """Fetch funding rate data from the API.

        Args:
            symbol: Trading symbol

        Returns:
            list[BackpackRawFundingIntervalRate]: Raw funding rate data
        """
        params = self._request_builder.build_get_funding_rate_params(symbol=symbol)
        endpoint_path = "/api/v1/fundingRates"

        logger.debug(
            "funding_rate_request",
            exchange=self._exchange_name,
            symbol=symbol,
            endpoint_path=endpoint_path,
            params=params,
            message="Requesting funding rate from endpoint",
        )

        raw_data, status_code, headers = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(by_alias=True, exclude_none=True),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.UNSIGNED,
                endpoint_group="public",
                request_weight=1,
            ),
        )

        validated_data = ensure_list_response(
            raw_data,
            f"funding rate ({symbol})",
            status_code,
        )

        return self._response_handler.handle_get_historical_funding_rates_response(
            validated_data,
            symbol,
            status_code,
            headers,
        )

    def _process_funding_rate_response(
        self,
        raw_funding_interval_rates: list[BackpackRawFundingIntervalRate],
        symbol: str,
    ) -> FundingRate:
        """Process funding rate response.

        Args:
            raw_funding_interval_rates: Raw funding rate data
            symbol: Trading symbol

        Returns:
            FundingRate: Processed funding rate

        Raises:
            NoFundingDataError: If no funding data available
        """
        if not raw_funding_interval_rates:
            raise NoFundingDataError(symbol)

        # Use the first (most recent) funding rate
        most_recent_rate = raw_funding_interval_rates[0]
        return self._funding_rate_mapper.transform_raw_funding_interval_rate_to_internal(
            most_recent_rate, symbol
        )

    async def _fetch_individual_funding_rates(self, symbols: list[str]) -> list[FundingRate]:
        """Fetch funding rates individually for each symbol.

        Args:
            symbols: List of trading symbols

        Returns:
            list[FundingRate]: List of funding rates

        Raises:
            APIError: If funding rate retrieval fails
        """
        funding_rates: list[FundingRate] = []
        failed_symbols: list[str] = []

        for symbol in symbols:
            try:
                rate = await self.get_funding_rate(symbol)
                funding_rates.append(rate)
            except (APIError, ValidationError, TransformationError) as e:
                logger.warning(
                    "funding_rate_failed",
                    exchange=self._exchange_name,
                    symbol=symbol,
                    error=str(e),
                    message=f"Failed to get funding rate for {symbol}: {e}",
                )
                failed_symbols.append(symbol)

        if failed_symbols:
            logger.warning(
                "partial_funding_rates",
                exchange=self._exchange_name,
                total_symbols=len(symbols),
                successful=len(funding_rates),
                failed=len(failed_symbols),
                failed_symbols=failed_symbols,
                message="Some funding rates could not be retrieved",
            )

        # If all symbols failed, raise an error
        if len(failed_symbols) == len(symbols) and symbols:
            raise APIError(
                code=APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
                message=f"Funding rates not available for symbols: {', '.join(symbols)}",
            )

        return funding_rates

    def _validate_funding_rate_symbol(self, symbol: str) -> None:
        """Validate symbol for funding rate request.

        Args:
            symbol: Trading symbol

        Raises:
            EmptySymbolError: If symbol is empty
        """
        if not symbol:
            raise EmptySymbolError("get_funding_rate")

    def _validate_funding_rates_symbols(self, symbols: list[str], current_method: str) -> None:
        """Validate symbols list for funding rates request.

        Args:
            symbols: List of trading symbols
            current_method: Name of calling method

        Raises:
            EmptySymbolListError: If symbols list is empty
            EmptySymbolInListError: If any symbol in list is empty
        """
        if not symbols:
            raise EmptySymbolListError(current_method)

        for i, symbol in enumerate(symbols):
            if not symbol:
                raise EmptySymbolInListError(current_method, i)

    def _handle_market_exceptions(
        self,
        exception: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle exceptions for market operations.

        Args:
            exception: The exception to handle
            current_method: Name of calling method
            symbol: Trading symbol
            status_code: HTTP status code
            raw_response_content: Raw response content

        Raises:
            APIError: Always raises APIError (re-raises or converts exceptions)
        """
        if isinstance(exception, APIError):
            raise exception

        error_message = "Failed to retrieve market metadata"
        error_code = APIErrorCode.UNKNOWN

        if isinstance(exception, TransformationError):
            error_message = "Failed to process/transform exchange data"
            error_code = APIErrorCode.INVALID_RESPONSE
        elif isinstance(exception, ValidationError):
            error_message = "Internal data validation failed"
            error_code = APIErrorCode.INVALID_RESPONSE

        logger.error(
            "market_error",
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

    def _handle_markets_exceptions(
        self,
        exception: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle exceptions for markets operations.

        Args:
            exception: The exception to handle
            current_method: Name of calling method
            status_code: HTTP status code
            raw_response_content: Raw response content

        Raises:
            APIError: Always raises APIError (re-raises or converts exceptions)
        """
        if isinstance(exception, APIError):
            raise exception

        error_message = "Failed to retrieve markets metadata"
        error_code = APIErrorCode.UNKNOWN

        if isinstance(exception, TransformationError):
            error_message = "Failed to process/transform exchange data"
            error_code = APIErrorCode.INVALID_RESPONSE
        elif isinstance(exception, ValidationError):
            error_message = "Internal data validation failed"
            error_code = APIErrorCode.INVALID_RESPONSE

        logger.error(
            "markets_error",
            exchange=self._exchange_name,
            method=current_method,
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

    def _handle_funding_rate_error(self, exception: Exception, symbol: str) -> None:
        """Handle funding rate errors.

        Args:
            exception: The exception to handle
            symbol: Trading symbol

        Raises:
            APIError: Always raises APIError (re-raises or converts exceptions)
        """
        if isinstance(exception, APIError):
            raise exception

        logger.error(
            "funding_rate_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(exception),
            message="Failed to retrieve funding rate",
        )

        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Failed to retrieve funding rate",
            original_exception=exception,
        ) from exception

    def _handle_funding_rates_exceptions(
        self,
        exception: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle exceptions for funding rates operations.

        Args:
            exception: The exception to handle
            current_method: Name of calling method
            status_code: HTTP status code
            raw_response_content: Raw response content

        Raises:
            APIError: Always raises APIError (re-raises or converts exceptions)
        """
        if isinstance(exception, APIError):
            raise exception

        error_message = "Failed to retrieve funding rates"
        error_code = APIErrorCode.UNKNOWN

        if isinstance(exception, TransformationError):
            error_message = "Failed to process/transform exchange data"
            error_code = APIErrorCode.INVALID_RESPONSE
        elif isinstance(exception, ValidationError):
            error_message = "Internal data validation failed"
            error_code = APIErrorCode.INVALID_RESPONSE

        logger.error(
            "funding_rates_error",
            exchange=self._exchange_name,
            method=current_method,
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
