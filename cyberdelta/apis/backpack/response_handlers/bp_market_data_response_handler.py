"""Backpack Market Data Response Handler.

This module handles validation of market data responses from the Backpack API,
extracted from the monolithic response handler to improve maintainability and testability.

Focused on:
- Ticker responses
- Order book responses
- Recent trades responses
- Market information responses
- Historical data responses (funding rates, klines)
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, TypeGuard

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRateResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarketResponse,
    BackpackRawOrderBook,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.protocols.handler_protocols import MarketDataResponseHandlerProtocol
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


def _safe_json_repr(value: object) -> str:
    """Convert any value to a JSON string representation for logging.

    This helper function ensures type safety when logging dynamic data
    from API responses while avoiding pyright strict mode issues.

    Args:
        value: Any value to convert to JSON representation

    Returns:
        JSON string representation or string fallback
    """
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        # Fallback for non-JSON-serializable objects
        return repr(value)


def _is_list_of_any(value: object) -> TypeGuard[list[Any]]:
    """Type guard to check if value is a list.

    This helps pyright understand type narrowing in strict mode.

    Args:
        value: Value to check

    Returns:
        True if value is a list, False otherwise
    """
    return isinstance(value, list)


class BackpackMarketDataResponseHandler(MarketDataResponseHandlerProtocol):
    """Handles validation of market data responses from Backpack REST API endpoints.

    Uses Pydantic models defined in `cyberdelta.apis.backpack.models` to validate
    the structure and types of the raw data. Raises APIError if validation fails.
    """

    @staticmethod
    def _handle_validation_error(
        e: ValidationError,
        context: str,
        raw_data: RawJsonResponse,
    ) -> APIError:
        """Create a standardized APIError from a ValidationError.

        Returns:
            APIError with INVALID_RESPONSE code and validation details.
        """
        logger.error(
            "backpack_pydantic_validation_failed",
            context=context,
            validation_error=str(e),
            raw_data=raw_data,
            handler_class="BackpackMarketDataResponseHandler",
            message="Pydantic validation failed",
        )
        # Use INVALID_RESPONSE code as per architecture rules
        return APIError(
            message=f"Invalid {context} response from exchange: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=e,
        )

    def handle_response(
        self,
        response: ParsedJsonResponse,
        status_code: int,
        headers: dict[str, str],
        context: str,
    ) -> object:
        """Generic response handler dispatch method.

        This method serves as the entry point for the registry system
        and dispatches to the appropriate specific handler method based on context.

        Args:
            response: Parsed JSON response data
            status_code: HTTP status code
            headers: Response headers
            context: Context string indicating the operation (e.g., "market_data.get_ticker")

        Raises:
            NotImplementedError: If context is not supported
        """
        # Extract operation from context (format: "domain.operation")
        if "." in context:
            _, operation = context.split(".", 1)
        else:
            operation = context

        # Note: Market data handlers require additional parameters (symbol, etc.)
        # which are not available in the generic handle_response interface.
        # This dispatcher is implemented for consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "get_ticker",
            "get_order_book",
            "get_recent_trades",
            "get_markets",
            "get_market",
            "get_funding_rate",
            "get_current_funding_rate",
            "get_historical_funding_rates",
            "get_market_data",
            "get_historical_trades",
        }:
            # Market data operations require additional context not available in generic interface
            raise NotImplementedError(
                f"Market data operation '{operation}' requires specific parameters not available "
                f"in generic handle_response interface. Use specific handler methods directly.",
            )
        raise NotImplementedError(
            f"Market data operation '{operation}' not supported by registry dispatch",
        )

    @staticmethod
    def handle_get_ticker_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawTickerResponse:
        """Validate the raw response for the Get Ticker endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            Validated BackpackRawTickerResponse model.
        """
        context = f"ticker ({symbol.value}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawTickerResponse.model_validate(validated_data)
        except ValidationError as e:
            api_error = BackpackMarketDataResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            )
            raise api_error from e

    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawOrderBook:
        """Validate the raw response for the Get Order Book endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            Validated BackpackRawOrderBook model.
        """
        context = f"order book ({symbol.value}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrderBook.model_validate(validated_data)
        except ValidationError as e:
            api_error = BackpackMarketDataResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            )
            raise api_error from e

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawRecentPublicTrade]:
        """Validate the raw response for the Get Recent Trades endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            List of validated BackpackRawRecentPublicTrade models.
        """
        context = f"recent trades ({symbol.value}) - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_items: list[BackpackRawRecentPublicTrade] = []
        for i, item in enumerate(validated_list):
            # Ensure item is a dict before validating
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_items.append(BackpackRawRecentPublicTrade.model_validate(validated_item))
            except ValidationError as e:
                api_error = BackpackMarketDataResponseHandler._handle_validation_error(
                    e,
                    f"single trade item in {context}",
                    validated_item,
                )
                raise api_error from e
        return validated_items

    @staticmethod
    def handle_get_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRateResponse:
        """Validate the raw response for the Get Funding Rate endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            Validated BackpackRawFundingRateResponse model.

        Raises:
            APIError: If response format is unexpected (empty list or non-dict/list type).
            _handle_validation_error: Internal validation error handler.
        """
        context = f"funding rate ({symbol.value}) - Status: {status_code}"

        # Try to handle as dict first
        if isinstance(raw_response_content, dict):
            raw_data_to_validate = raw_response_content
        elif isinstance(raw_response_content, list):
            # Check if it is a list, as HL funding rate is a list
            if not raw_response_content:  # Empty list
                raise APIError(
                    message=(
                        f"Empty list for {context} response, expected dict or non-empty list."
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )
            # Assuming the first element is the target if it's a list
            # (adapting for HL-like structures)
            raw_data_to_validate = ensure_dict_response(
                raw_response_content[0],
                f"{context} (first item in list)",
                status_code,
            )
        else:
            raise APIError(
                message=f"Unexpected {context} response format: expected dict or list, "
                f"got {type(raw_response_content).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        try:
            return BackpackRawFundingRateResponse.model_validate(raw_data_to_validate)
        except ValidationError as e:
            raise BackpackMarketDataResponseHandler._handle_validation_error(
                e,
                context,
                raw_data_to_validate,
            ) from e

    @staticmethod
    def handle_get_markets_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> list[BackpackRawMarketResponse]:
        """Validate the raw response for the Get Markets endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            status_code: HTTP status code of the response

        Returns:
            List of validated BackpackRawMarketResponse models.

        Raises:
            _handle_validation_error: Internal validation error handler.
        """
        context = "markets"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        markets: list[BackpackRawMarketResponse] = []
        for i, market_data in enumerate(validated_list):
            validated_item = ensure_dict_response(
                market_data,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                market_model = BackpackRawMarketResponse.model_validate(validated_item)
                markets.append(market_model)
            except ValidationError as e:
                raise BackpackMarketDataResponseHandler._handle_validation_error(
                    e,
                    f"{context} item {i}",
                    validated_item,
                ) from e

        return markets

    @staticmethod
    def handle_get_market_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMarketResponse:
        """Validate the raw response for the Get Market endpoint.

        Args:
            raw_response_content: The raw response data from the API.
            symbol: The requested symbol for context in error messages.
            status_code: HTTP status code of the response.
            headers: HTTP response headers.

        Returns:
            BackpackRawMarketResponse: The validated market model.

        Raises:
            _handle_validation_error: Internal validation error handler.
        """
        context = f"market for {symbol.value}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )

        try:
            return BackpackRawMarketResponse.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackMarketDataResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        timeframe: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawKlineResponse]:  # Changed return type
        """Validate the raw response for the Get Market Data (Klines) endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            timeframe: Kline timeframe for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            List of validated BackpackRawKlineResponse models.

        Raises:
            APIError: If unexpected error occurs validating kline items.
            _handle_validation_error: Internal validation error handler.
        """
        context = f"market data (klines {timeframe}) for {symbol.value} - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_klines: list[BackpackRawKlineResponse] = []
        for item_raw in validated_list:
            if not _is_list_of_any(item_raw):  # Backpack klines are lists of values
                # Convert item_raw to string representation for logging
                item_str = str(item_raw) if item_raw is not None else "None"
                logger.warning(
                    "backpack_non_list_kline_item",
                    context=context,
                    item_raw=item_str,
                    module_name=__name__,
                    message="Skipping non-list kline item",
                )
                continue
            # Now item_raw is known to be list[Any] thanks to the type guard
            # Backpack klines are lists of values (typically 12 elements)
            # For logging, we'll convert the entire list to string at once
            # This avoids iterating over unknown types
            try:
                # BackpackRawKlineResponse is now imported at module level
                validated_klines.append(BackpackRawKlineResponse.model_validate(item_raw))
            except ValidationError as e:
                # Log the specific item that failed validation
                # Use JSON-style formatting for better readability of kline data
                logger.exception(
                    "backpack_kline_validation_failed",
                    context=context,
                    validation_error=str(e),
                    item_raw=_safe_json_repr(item_raw),
                    module_name=__name__,
                    message="Pydantic validation failed for single kline item",
                )
                # Re-raise to fail the entire response if one kline is bad, or collect valid ones
                # Pass the full validated_list since item_raw type is not fully known
                raise BackpackMarketDataResponseHandler._handle_validation_error(
                    e,
                    f"single kline item in {context}",
                    validated_list,
                ) from e
            except Exception as e_unk_item:
                # Format the list for logging
                logger.exception(
                    "backpack_kline_unexpected_error",
                    context=context,
                    error=str(e_unk_item),
                    item_raw=_safe_json_repr(item_raw),
                    module_name=__name__,
                    message="Unexpected error validating single kline item",
                )
                raise APIError(
                    message=f"Unexpected error validating kline item: {e_unk_item}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    original_exception=e_unk_item,
                ) from e_unk_item

        return validated_klines

    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawPublicTrade]:
        """Validate the raw response for the Get Historical Trades endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            List of validated BackpackRawPublicTrade models.

        Raises:
            _handle_validation_error: Internal validation error handler.
        """
        context = f"historical trades ({symbol.value}) - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_trades: list[BackpackRawPublicTrade] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_trades.append(BackpackRawPublicTrade.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackMarketDataResponseHandler._handle_validation_error(
                    e,
                    f"single historical trade item in {context}",
                    validated_item,
                ) from e
        return validated_trades

    @staticmethod
    def handle_get_current_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRateResponse:
        """Validate the raw response for the Get Current Funding Rate endpoint.

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            Validated BackpackRawFundingRateResponse model.
        """
        context = f"current funding rate ({symbol.value}) - Status: {status_code}"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            # Assuming BackpackRawFundingRateResponse is the correct model for a single rate
            return BackpackRawFundingRateResponse.model_validate(validated_data)
        except ValidationError as e:
            api_error = BackpackMarketDataResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            )
            raise api_error from e

    @staticmethod
    def handle_get_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawFundingIntervalRate]:
        """Validate the raw response for the Get Historical Funding Rates endpoint.

        Endpoint: /api/v1/fundingRates

        Args:
            raw_response_content: Raw JSON response from the API
            symbol: Trading symbol for context in error messages
            status_code: HTTP status code of the response
            headers: HTTP response headers

        Returns:
            List of validated BackpackRawFundingIntervalRate models.

        Raises:
            _handle_validation_error: Internal validation error handler.
        """
        context = f"historical funding rates ({symbol.value}) - Status: {status_code}"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_rates: list[BackpackRawFundingIntervalRate] = []
        for i, item_raw in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item_raw,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_rates.append(
                    BackpackRawFundingIntervalRate.model_validate(validated_item),
                )
            except ValidationError as e:
                raise BackpackMarketDataResponseHandler._handle_validation_error(
                    e,
                    f"single historical funding rate item in {context}",
                    validated_item,
                ) from e
        return validated_rates
