"""Hyperliquid Order History Service.

This service handles all order history operations for the Hyperliquid exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Historical order retrieval
- Order filtering and time range processing
- Order transformation and mapping
- Comprehensive error handling
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import AccountRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderMapperProtocol
from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs
from cyberdelta.apis.utils.response_validation import ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.exceptions.base import RequiredParameterError
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidOrderHistoryService:
    """Focused service for Hyperliquid order history operations.

    Handles validation, processing, and transformation of order history requests
    with comprehensive error handling and filtering capabilities.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: AccountRequestBuilderProtocol,
        response_handler: AccountResponseHandlerProtocol,
        mapper: OrderMapperProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str = "hyperliquid",
        wallet_address: str | None = None,
    ) -> None:
        """Initialize the order history service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            mapper: Data mapper for transforming raw responses to internal models
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for order history requests
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieve historical order data using the 'historicalOrders' endpoint.

        Args:
            args: Order history request arguments

        Returns:
            list[Order]: List of historical orders

        Raises:
            APIError: If order history retrieval fails or processing fails
            RequiredParameterError: If required parameters are missing
        """
        self._validate_order_history_args(args)

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "retrieving_order_history",
                exchange=self._exchange_name,
                symbol=args.symbol,
                start_time=args.start_time,
                end_time=args.end_time,
                message="Retrieving order history from API",
            )

            raw_data, status_code, raw_response_content = await self._fetch_order_history_data(args)
            # Validation already done in _fetch_order_history_data
            if raw_data is None:
                logger.info(
                    "no_order_history",
                    exchange=self._exchange_name,
                    symbol=args.symbol,
                    message="No order history data found",
                )
                return []

            raw_historical_order_responses = self._process_order_history_response(
                raw_data, status_code
            )
            internal_orders = self._map_historical_orders_to_internal(
                raw_historical_order_responses
            )

            # Filter by time range since historicalOrders endpoint doesn't support time filtering
            if args.start_time and args.end_time:
                logger.debug(
                    "filtering_by_time_range",
                    exchange=self._exchange_name,
                    start_time=args.start_time,
                    end_time=args.end_time,
                    total_orders=len(internal_orders),
                    message="Filtering orders by time range",
                )
                internal_orders = [
                    order
                    for order in internal_orders
                    if order.created_at and args.start_time <= order.created_at <= args.end_time
                ]

            filtered_orders = self._filter_orders_by_symbol(internal_orders, args.symbol)

            logger.info(
                "order_history_retrieved",
                exchange=self._exchange_name,
                symbol=args.symbol,
                order_count=len(filtered_orders),
                message="Successfully retrieved order history",
            )

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_transformation_error(
                e_transform, "get_order_history", status_code, raw_response_content
            )
            raise
        except ValidationError as e_val:
            self._handle_validation_error(
                e_val, "get_order_history", status_code, raw_response_content
            )
            raise
        except (ValueError, TypeError) as e_service_logic:
            self._handle_service_logic_error(e_service_logic, "get_order_history")
            raise
        except Exception as e_unexpected:
            self._handle_unexpected_error(
                e_unexpected, "get_order_history", status_code, raw_response_content
            )
            raise
        else:
            return filtered_orders

    async def _fetch_order_history_data(
        self,
        args: GetOrderHistoryArgs,
    ) -> tuple[ParsedJsonResponse | None, int, str | None]:
        """Fetch order history data from the API.

        Args:
            args: Order history request arguments

        Returns:
            tuple: Raw data, status code, and response content

        Raises:
            APIError: If the request fails or response is invalid
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for retrieving order history",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        if not self._wallet_address:
            raise APIError(
                message="Wallet address required for order history requests",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint = "/info"

        payload = self._request_builder.build_historical_orders_payload(self._wallet_address)

        logger.debug(
            "fetching_order_history_data",
            exchange=self._exchange_name,
            wallet_address=self._wallet_address,
            message="Fetching order history data from API",
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint,
            data=payload,
            is_signed=False,  # Historical data requests don't require signing
            endpoint_group="info",
            request_weight=1,
        )

        raw_response_content = str(raw_data) if raw_data is not None else None
        return raw_data, status_code, raw_response_content

    def _validate_order_history_args(self, args: GetOrderHistoryArgs) -> None:
        """Validate order history arguments.

        Args:
            args: Order history arguments to validate

        Raises:
            RequiredParameterError: If validation fails
        """
        if not self._wallet_address:
            raise RequiredParameterError(
                parameter="wallet_address",
                context="order history requests",
                exchange=self._exchange_name,
            )

        # Additional validation can be added here as needed
        logger.debug(
            "order_history_args_validated",
            exchange=self._exchange_name,
            symbol=args.symbol,
            start_time=args.start_time,
            end_time=args.end_time,
            message="Order history arguments validated",
        )

    def _process_order_history_response(
        self, raw_data: ParsedJsonResponse, status_code: int
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Process the order history response.

        Args:
            raw_data: Raw response data
            status_code: HTTP status code for validation

        Returns:
            list[HyperliquidRawHistoricalOrderResponse]: Processed historical orders with status

        Raises:
            APIError: If processing fails
        """
        validated_data = ensure_list_response(
            raw_data,
            "order history",
            status_code,
        )

        raw_historical_order_responses: list[HyperliquidRawHistoricalOrderResponse] = (
            self._response_handler.handle_historical_orders_response(
                validated_data,
                self._wallet_address or "",
                status_code,
            )
        )
        return raw_historical_order_responses

    def _map_historical_orders_to_internal(
        self, raw_historical_order_responses: list[HyperliquidRawHistoricalOrderResponse]
    ) -> list[Order]:
        """Map raw historical order responses to internal Order models.

        Args:
            raw_historical_order_responses: Raw historical order response list with status

        Returns:
            list[Order]: Mapped internal orders

        Raises:
            TransformationError: If mapping fails
        """
        return [
            self._mapper.transform_raw_historical_order_to_internal(
                HyperliquidRawHistoricalOrder(
                    **response.order.model_dump(),
                    status=response.status,
                    statusTimestamp=response.status_timestamp,
                )
            )
            for response in raw_historical_order_responses
        ]

    def _filter_orders_by_symbol(self, orders: list[Order], symbol: str | None) -> list[Order]:
        """Filter orders by symbol if specified.

        Args:
            orders: List of orders to filter
            symbol: Symbol to filter by (optional)

        Returns:
            list[Order]: Filtered orders
        """
        if symbol:
            filtered = [order for order in orders if order.symbol == symbol]
            logger.debug(
                "orders_filtered_by_symbol",
                exchange=self._exchange_name,
                symbol=symbol,
                original_count=len(orders),
                filtered_count=len(filtered),
                message="Filtered orders by symbol",
            )
            return filtered
        return orders

    def _handle_transformation_error(
        self,
        e_transform: TransformationError,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors."""
        logger.error(
            "transformation_error",
            action=method_name,
            exchange=self._exchange_name,
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

    def _handle_validation_error(
        self,
        e_val: ValidationError,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle validation errors."""
        logger.error(
            "validation_error",
            action=method_name,
            exchange=self._exchange_name,
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

    def _handle_service_logic_error(
        self, e_service_logic: ValueError | TypeError, method_name: str
    ) -> None:
        """Handle service logic errors."""
        error_msg = str(e_service_logic)
        if method_name in error_msg:
            # Input validation error - re-raise
            raise e_service_logic
        # Internal service error
        logger.error(
            "service_logic_error",
            action=method_name,
            exchange=self._exchange_name,
            error=str(e_service_logic),
            message="Service internal logic error",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=e_service_logic,
        ) from e_service_logic

    def _handle_unexpected_error(
        self,
        e_unexpected: Exception,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors."""
        logger.error(
            "unexpected_service_failure",
            action=method_name,
            exchange=self._exchange_name,
            error=str(e_unexpected),
            message="Unexpected service failure",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e_unexpected,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e_unexpected
