"""Hyperliquid Order Book Service.

This service handles order book and recent trades operations for the Hyperliquid exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- L2 order book data retrieval with bid/ask levels
- Recent public trades retrieval and processing
- Order book validation and transformation
- Trade data filtering and error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, NoReturn

from pydantic import ValidationError

from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.response_validation import EmptyResponseError
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    MarketDataResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderBookMapperProtocol
from cyberdelta.apis.models.service_args.market_data import GetL2BookArgs, GetRecentTradesArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models import OrderBook, Trade
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidOrderBookService:
    """Focused service for Hyperliquid order book and recent trades operations.

    Handles L2 order book data, recent public trades, and related data validation
    with comprehensive error handling and transformation.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        mapper: OrderBookMapperProtocol,
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the order book service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            mapper: Mapper for converting raw data to internal domain models
            exchange_name: Name identifier for this exchange instance
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_order_book(self, symbol: Symbol) -> OrderBook | None:
        """Retrieve the order book for a specific symbol using a POST request to /info.

        Uses payload: {"type": "l2Book", "coin": "SYMBOL"} to get L2 order book data
        with bid and ask levels.

        Args:
            symbol: The Symbol domain object

        Returns:
            OrderBook object or None if the symbol is not found

        Raises:
            APIError: If the API request fails or the response is invalid
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_book"

        # Validate symbol object directly
        self._validate_symbol(symbol.value, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "fetching_order_book",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                symbol_exchange=symbol.exchange.value,
                message="Fetching L2 order book data for symbol",
            )

            # Core operational logic - symbol is already domain object
            endpoint_path = "/info"
            try:
                request_payload_model = self._request_builder.build_l2_book_request_payload(
                    GetL2BookArgs(symbol=symbol),  # Use domain object directly
                )
            except Exception as e:
                # Wrap request builder exceptions in APIError
                logger.exception(
                    "request_builder_failed",
                    exchange=self._exchange_name,
                    method=current_method,
                    request_type="l2Book",
                    error=str(e),
                    message="Request builder failed for l2Book",
                )
                error_msg = f"Failed to build l2Book request for symbol {symbol}: {e}"
                raise APIError(
                    message=error_msg,
                    code=APIErrorCode.UNKNOWN.value,
                    original_exception=e,
                ) from e

            request_payload_data: dict[str, Any] = request_payload_model.model_dump(
                by_alias=True,
                exclude_none=True,
            )

            raw_response_content_parsed: ParsedJsonResponse | None = None
            headers: Mapping[str, str] = {}
            raw_response_content_parsed, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                    endpoint_group="public",
                    request_weight=1,
                ),
            )

            if raw_response_content_parsed is not None:
                raw_response_content = str(raw_response_content_parsed)

            logger.debug(
                "l2_orderbook_response",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                status_code=status_code,
                has_data=raw_response_content_parsed is not None,
                message="Raw l2 orderbook response received",
            )

            validated_response = self._validate_response_not_none(
                raw_response_content_parsed,
                "l2Book data",
                f"l2Book for {symbol}",
                status_code,
            )

            # Type guard for response handler
            if not isinstance(validated_response, dict):
                self._raise_invalid_l2book_response_type_error(validated_response, status_code)

            validated_raw_book = self._response_handler.handle_info_l2_book_response(
                validated_response,
                symbol=symbol,  # Pass Symbol object directly
                status_code=status_code,
                headers=headers,
            )

            order_book = self._mapper.transform_raw_order_book_to_internal(validated_raw_book)

            logger.info(
                "order_book_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                bid_levels=len(order_book.bids) if order_book else 0,
                ask_levels=len(order_book.asks) if order_book else 0,
                message="Successfully retrieved order book data",
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_transform),
                message="Failed to transform exchange data for order book",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_val),
                message="Internal data validation failed for order book",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_service_logic),
                message="Service internal logic error for order book",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_unexpected),
                message="Unexpected service failure for order book",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
        else:
            return order_book

    async def get_recent_trades(self, symbol: Symbol) -> list[Trade]:
        """Retrieve recent public trades for a specific symbol using a POST request to /info.

        Uses payload: {"type": "recentTrades", "coin": "SYMBOL"} to get recent public trades.

        Args:
            symbol: The Symbol domain object

        Returns:
            List of Trade objects. The number of trades is determined by the Hyperliquid API

        Raises:
            APIError: If the API request fails or the response is invalid
            TypeError: If service logic encounters type errors
            ValueError: If service logic encounters value errors
            TransformationError: If data transformation fails
            ValidationError: If data validation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_recent_trades"

        # Validate symbol object directly
        if not symbol.value:
            error_msg = "'symbol' must be a non-empty string."
            raise ValueError(error_msg)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "fetching_recent_trades",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                message="Fetching recent public trades for symbol",
            )

            (
                raw_response_content_parsed,
                status_code,
                headers,
            ) = await self._fetch_recent_trades_data(symbol.value)

            validated_response = self._validate_response_not_none(
                raw_response_content_parsed,
                "recent trades data",
                f"recent trades for {symbol}",
                status_code,
            )

            validated_raw_trades = self._process_recent_trades_response(
                validated_response,
                symbol.value,
                status_code,
                headers,
            )

            internal_trades = self._map_recent_trades_to_internal(
                validated_raw_trades,
                symbol.value,
            )

            logger.info(
                "recent_trades_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                trade_count=len(internal_trades),
                message="Successfully retrieved recent trades",
            )

        except APIError:
            # Re-raise APIErrors from lower layers
            raise
        except TransformationError as error:
            self._handle_recent_trades_transformation_error(
                error,
                symbol.value,
                status_code,
                raw_response_content,
            )
            raise  # Re-raise after handling
        except ValidationError as error:
            self._handle_recent_trades_validation_error(
                error,
                symbol.value,
                status_code,
                raw_response_content,
            )
            raise  # Re-raise after handling
        except (ValueError, TypeError) as error:
            self._handle_recent_trades_service_logic_error(error, symbol.value)
            raise  # Re-raise after handling
        except Exception as error:
            self._handle_recent_trades_unexpected_error(
                error,
                symbol.value,
                status_code,
                raw_response_content,
            )
            raise  # Re-raise after handling
        else:
            return internal_trades

    async def _fetch_recent_trades_data(
        self,
        symbol: str,
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Fetch raw recent trades data from API.

        Args:
            symbol: The trading symbol to fetch trades for

        Returns:
            Tuple of raw response data, status code, and headers

        Raises:
            APIError: If API request fails
        """
        endpoint_path = "/info"

        exchange_symbol = exchanges.hyperliquid(value=symbol)
        request_payload_model = self._request_builder.build_recent_trades_request_payload(
            GetRecentTradesArgs(symbol=exchange_symbol),
        )
        request_payload_data: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True,
            exclude_none=True,
        )

        raw_response_content_parsed, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=request_payload_data,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.UNSIGNED,
                endpoint_group="public",
                request_weight=1,
            ),
        )

        logger.debug(
            "recent_trades_raw_response",
            exchange=self._exchange_name,
            symbol=symbol,
            status_code=status_code,
            has_data=raw_response_content_parsed is not None,
            message="Raw recent trades response received",
        )

        if raw_response_content_parsed is None:
            error_msg = (
                f"No content received from HTTP client for recentTrades for {symbol}. "
                f"Status: {status_code}"
            )
            logger.error(
                "trade_history_empty_response",
                exchange=self._exchange_name,
                symbol=symbol,
                status_code=status_code,
                message="Empty response received for recent trades",
            )
            raise APIError(
                message=error_msg,
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        return raw_response_content_parsed, status_code, headers

    def _process_recent_trades_response(
        self,
        raw_response_content_parsed: ParsedJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[HyperliquidRawPublicTrade]:
        """Process the raw response and validate recent trades data.

        Args:
            raw_response_content_parsed: Raw response data from API
            symbol: Trading symbol for context
            status_code: HTTP status code
            headers: Response headers

        Returns:
            List of validated HyperliquidRawPublicTrade objects

        Raises:
            APIError: If response processing fails
        """
        # Type guard for response handler - recent trades returns a list
        if not isinstance(raw_response_content_parsed, list):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Expected list response for recent trades, "
                    f"got {type(raw_response_content_parsed)}"
                ),
                http_status=status_code,
            )

        return self._response_handler.handle_info_recent_trades_response(
            raw_response_content_parsed,
            symbol,  # coin parameter
            status_code,
            headers,
        )

    def _map_recent_trades_to_internal(
        self,
        validated_raw_trades: list[HyperliquidRawPublicTrade],
        symbol: str,
    ) -> list[Trade]:
        """Map validated raw trades to internal Trade objects.

        Args:
            validated_raw_trades: List of validated raw trade data
            symbol: Trading symbol for context

        Returns:
            List of internal Trade objects

        Raises:
            None - This method handles errors internally and returns partial results
        """
        internal_trades: list[Trade] = []
        for raw_trade in validated_raw_trades:
            try:
                trade = self._mapper.transform_raw_public_trade_to_internal(raw_trade)
                if trade:
                    internal_trades.append(trade)
            except (ValidationError, ValueError) as e_map_item:
                logger.warning(
                    "recent_trade_mapping_skipped",
                    exchange=self._exchange_name,
                    symbol=symbol,
                    error=str(e_map_item),
                    raw_trade_id=getattr(raw_trade, "id", None),
                    message="Skipping mapping for recent trade item",
                )
                continue

        logger.debug(
            "recent_trades_mapped",
            exchange=self._exchange_name,
            symbol=symbol,
            mapped_count=len(internal_trades),
            total_raw_trades=len(validated_raw_trades),
            message="Successfully mapped recent trades to internal format",
        )

        return internal_trades

    def _validate_response_not_none(
        self,
        response: ParsedJsonResponse | None,
        response_type: str,
        operation: str,
        status_code: int,
    ) -> ParsedJsonResponse:
        """Validate that response is not None.

        Args:
            response: The response to validate
            response_type: Type of response expected
            operation: Operation that returned the response
            status_code: HTTP status code

        Returns:
            The validated non-None response for type narrowing

        Raises:
            EmptyResponseError: If response is None
        """
        if response is None:
            raise EmptyResponseError(
                response_type=response_type,
                operation=operation,
                http_status=status_code,
                exchange=self._exchange_name,
            )
        return response

    def _validate_symbol(self, symbol: str, current_method: str) -> None:
        """Validate symbol parameter.

        Args:
            symbol: Trading symbol to validate
            current_method: Calling method name for error context

        Raises:
            ValueError: If symbol is invalid
        """
        if not symbol:
            error_msg = f"[{current_method}] 'symbol' must be a non-empty string."
            raise ValueError(error_msg)

        # Strip and check again
        if not symbol.strip():
            error_msg = f"[{current_method}] 'symbol' cannot be empty or whitespace only."
            raise ValueError(error_msg)

    def _handle_recent_trades_transformation_error(
        self,
        error: TransformationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors for recent trades.

        Args:
            error: The transformation error that occurred
            symbol: Trading symbol for context
            status_code: HTTP status code from the request
            raw_response_content: Raw response content for debugging

        Raises:
            APIError: Always raises an APIError wrapping the original TransformationError
        """
        logger.error(
            "recent_trades_transform_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Failed to transform exchange data for recent trades",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_recent_trades_validation_error(
        self,
        error: ValidationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle validation errors for recent trades.

        Args:
            error: The validation error that occurred
            symbol: Trading symbol for context
            status_code: HTTP status code from the request
            raw_response_content: Raw response content for debugging

        Raises:
            APIError: Always raises an APIError wrapping the original ValidationError
        """
        logger.error(
            "recent_trades_validation_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Internal data validation failed for recent trades",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_recent_trades_service_logic_error(
        self,
        error: ValueError | TypeError,
        symbol: str,
    ) -> None:
        """Handle service logic errors for recent trades.

        Args:
            error: The service logic error that occurred
            symbol: Trading symbol for context

        Raises:
            APIError: Always raises an APIError wrapping the original service logic error
        """
        logger.error(
            "recent_trades_service_logic_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Service internal logic error for recent trades",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_recent_trades_unexpected_error(
        self,
        error: Exception,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors for recent trades.

        Args:
            error: The unexpected error that occurred
            symbol: Trading symbol for context
            status_code: HTTP status code from the request
            raw_response_content: Raw response content for debugging

        Raises:
            APIError: Always raises an APIError wrapping the original unexpected error
        """
        logger.error(
            "recent_trades_unexpected_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Unexpected service failure for recent trades",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _raise_invalid_l2book_response_type_error(
        self,
        validated_response: object,
        status_code: int,
    ) -> NoReturn:
        """Raise an APIError for invalid l2Book response type.

        Args:
            validated_response: The invalid response data
            status_code: HTTP status code

        Raises:
            APIError: Always raises an APIError for invalid response type
        """
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message=f"Expected dict response for l2Book, got {type(validated_response)}",
            http_status=status_code,
        )
