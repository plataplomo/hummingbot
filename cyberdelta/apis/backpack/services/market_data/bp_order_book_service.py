"""Backpack Order Book Service.

This service handles all order book operations for the Backpack exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Order book retrieval with depth limits
- Bid/ask data transformation
- Order book validation and processing
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackOrderBookMapper
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook
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
from cyberdelta.apis.exceptions.market_data_service import EmptySymbolError, InvalidLimitError
from cyberdelta.apis.utils.response_validation import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import OrderBook
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackOrderBookService:
    """Focused service for Backpack order book operations.

    Handles validation, processing, and transformation of order book requests
    with comprehensive error handling and depth management.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackMarketDataRequestBuilder,
        response_handler: BackpackMarketDataResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        order_book_mapper: BackpackOrderBookMapper | None = None,
    ) -> None:
        """Initialize the order book service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            order_book_mapper: Optional order book mapper instance (defaults to new instance)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._order_book_mapper = order_book_mapper or BackpackOrderBookMapper()

    async def get_order_book(self, symbol: str, limit: int | None = 20) -> OrderBook:
        """Retrieves the order book for a specific symbol.

        Args:
            symbol: The trading symbol to get order book for
            limit: Maximum number of price levels per side (default: 20)

        Returns:
            OrderBook: Current order book with bids and asks

        Raises:
            APIError: If order book retrieval fails or processing fails
            EmptySymbolError: If symbol is empty or whitespace
            InvalidLimitError: If limit is <= 0
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_book"

        if not symbol:
            raise EmptySymbolError(current_method)
        if limit is not None and limit <= 0:
            raise InvalidLimitError(current_method, limit)

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                "retrieving_order_book",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                limit=limit,
                message="Retrieving order book from exchange",
            )

            params = self._request_builder.build_get_order_book_params(symbol=symbol, depth=limit)
            endpoint_path = "/api/v1/depth"

            logger.debug(
                "order_book_request",
                exchange=self._exchange_name,
                symbol=symbol,
                limit=limit,
                endpoint_path=endpoint_path,
                params=params,
                message="Requesting order book from endpoint",
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
            raw_response_content = str(raw_data) if raw_data is not None else None

            logger.debug(
                "order_book_response",
                exchange=self._exchange_name,
                symbol=symbol,
                raw_data=raw_data,
                status_code=status_code,
                headers=headers,
                message="Received raw order book response",
            )

            validated_data = ensure_dict_response(raw_data, f"order book ({symbol})", status_code)

            raw_order_book_model: BackpackRawOrderBook = (
                self._response_handler.handle_get_order_book_response(
                    validated_data,
                    symbol,
                    status_code,
                    headers,
                )
            )
            internal_order_book = self._order_book_mapper.transform_raw_order_book_to_internal(
                symbol,
                raw_order_book_model,
            )

            logger.info(
                "order_book_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                bid_levels=len(internal_order_book.bids),
                ask_levels=len(internal_order_book.asks),
                best_bid=str(internal_order_book.bids[0][0]) if internal_order_book.bids else None,
                best_ask=str(internal_order_book.asks[0][0]) if internal_order_book.asks else None,
                message="Successfully retrieved order book",
            )

            logger.debug(
                "order_book_mapped",
                exchange=self._exchange_name,
                symbol=symbol,
                internal_order_book=internal_order_book,
                message="Mapped order book to internal model",
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
                message="Failed to transform exchange data",
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
                message="Internal data validation failed",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            error_msg = str(e_service_logic)
            if current_method in error_msg and ("symbol" in error_msg or "limit" in error_msg):
                # This is likely from our input parameter validation - re-raise as is
                raise
            # This is from service internal logic - wrap as APIError
            logger.exception(
                "logic_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_service_logic),
                message="Service internal logic error",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                error=str(e_unexpected),
                message="Unexpected error occurred",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
        else:
            return internal_order_book
