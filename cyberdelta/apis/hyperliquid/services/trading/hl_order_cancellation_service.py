"""Hyperliquid Order Cancellation Service.

This service handles all order cancellation operations for the Hyperliquid exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Single order cancellation
- Batch order cancellation
- Cancel all orders functionality
- Cancellation result processing
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, NoReturn

from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
    SerializationMode,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import (
    EmptyResponseError,
    InvalidParameterTypeError,
    MissingRequiredParameterError,
    ServiceParameterError,
)
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.services.trading.hl_base_trading_service import (
    HyperliquidBaseTradingService,
)

# Import needed at runtime (not just for type checking)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_query_service import (
    HyperliquidOrderQueryService,
)
from cyberdelta.apis.hyperliquid.services.utils.response_formatting import (
    format_batch_cancel_results,
    format_cancel_order_result,
)
from cyberdelta.apis.hyperliquid.services.utils.status_processing import (
    check_error_response,
    process_exchange_status,
    validate_batch_response_counts,
)
from cyberdelta.apis.models.service_args.trading import CancelOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.models import Order
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse, is_dict_response


logger = get_logger(__name__)

# Constants
HYPERLIQUID_MAX_BATCH_SIZE = 50  # Maximum orders allowed in a single batch request

# HTTP client signature type
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidOrderCancellationService(HyperliquidBaseTradingService):
    """Focused service for Hyperliquid order cancellation operations.

    Handles validation, processing, and transformation of order cancellation requests
    with comprehensive error handling and result processing.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        error_mapper: HyperliquidErrorMapper,
        authenticator: IAuthenticator,
        get_asset_index_callable: Callable[[str], Awaitable[int | None]],
        order_query_service: HyperliquidOrderQueryService,
        action_endpoint: str = "/exchange",
        exchange_name: ExchangeName = ExchangeName.HYPERLIQUID,
    ) -> None:
        """Initialize the order cancellation service.

        Args:
            http_client_requester: HTTP client callable for making requests
            request_builder: Request builder for creating API payloads
            response_handler: Response handler for processing API responses
            error_mapper: Error mapper for handling Hyperliquid-specific errors
            authenticator: Authentication interface for request signing
            get_asset_index_callable: Function to retrieve asset index for symbols
            order_query_service: Service for querying order information
            action_endpoint: API endpoint for exchange actions
            exchange_name: Name of the exchange for logging
        """
        super().__init__(exchange_name)
        self._http_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._error_mapper = error_mapper
        self._authenticator = authenticator
        self._get_asset_index_callable = get_asset_index_callable
        self._action_endpoint = action_endpoint
        self._exchange_name = exchange_name
        self._order_query_service = order_query_service

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel a specific order and return the cancellation result.

        Args:
            args: Arguments containing order_id and symbol for order cancellation

        Returns:
            CancelOrderResult containing detailed cancellation information
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_order"

        # Use the unified core method with a single cancellation
        results = await self._cancel_orders_core([args], current_method)
        return results[0]

    async def cancel_all_orders(self, symbol: Symbol | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders, optionally filtered by symbol.

        Args:
            symbol: Optional symbol to filter orders (if None, cancels all symbols)

        Returns:
            List of CancelOrderResult objects for all cancelled orders
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_all_orders"

        logger.info(
            "cancel_all_orders_called",
            method=current_method,
            symbol=symbol,
            has_order_query_service=True,  # Always available in this service
            message=f"cancel_all_orders called with symbol={symbol}",
        )

        try:
            # Validate prerequisites (authentication, wallet address)
            self._validate_cancel_all_orders_prerequisites()

            # Get all open orders
            try:
                open_orders = await self._get_open_orders_for_cancellation(symbol)
            except Exception as e:
                logger.exception(
                    "cancel_all_orders_fetch_error",
                    method=current_method,
                    symbol=symbol,
                    error_type=type(e).__name__,
                    error=str(e),
                    message=f"Error fetching open orders: {e}",
                )
                raise

            logger.info(
                "cancel_all_orders_fetched_open_orders",
                method=current_method,
                symbol=symbol,
                order_count=len(open_orders),
                has_order_query_service=True,  # Always available in this service
                message=f"Fetched {len(open_orders)} open orders for cancellation",
            )

            if not open_orders:
                logger.info(
                    "no_orders_to_cancel",
                    method=current_method,
                    symbol=symbol,
                    message="No open orders found for cancellation",
                )
                return []

            return await self._process_all_order_cancellations(open_orders, current_method)

        except (APIError, TransformationError, ValidationError, ValueError, TypeError) as error:
            api_error = self._handle_service_error(error, current_method, "cancel all orders")
            raise api_error from error

    async def _cancel_orders_core(
        self,
        cancel_args: list[CancelOrderArgs],
        current_method: str,
    ) -> list[CancelOrderResult]:
        """Core unified method for canceling single or batch orders.

        Args:
            cancel_args: List of cancellation arguments
            current_method: Name of calling method for error context

        Returns:
            List of CancelOrderResult objects

        Raises:
            APIError: If API request fails.
            TransformationError: If data transformation fails.
            ValidationError: If validation fails.
            ValueError: If arguments are invalid.
            TypeError: If type validation fails.
        """
        self._validate_cancel_args_list(cancel_args, current_method)

        try:
            cancel_items = await self._prepare_cancel_data(cancel_args, current_method)
            cancel_request_payload = await self._build_cancel_payload(cancel_args, cancel_items)

            raw_exchange_response, http_status = await self._cancel_order_raw(
                cancel_request_payload,
            )

            return await self._process_cancel_response(
                raw_exchange_response,
                http_status,
                cancel_args,
                cancel_items,
            )

        except APIError:
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            order_context = f"order {cancel_args[0].order_id}" if cancel_args else "unknown"
            raise self._handle_service_error(e, current_method, order_context, 0, None) from e

    async def _cancel_order_raw(
        self,
        request_payload_model: HyperliquidApiCancelOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Execute the raw order cancellation request.

        Args:
            request_payload_model: Validated cancellation request payload

        Returns:
            Tuple of (raw exchange response, HTTP status code)

        Raises:
            APIError: If response is invalid.
        """
        raw_content, http_status, _ = await self._http_requester(
            method="POST",
            endpoint=self._action_endpoint,
            data=request_payload_model,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                serialization_mode=SerializationMode.EXPLICIT_NULL,
            ),
        )

        if not is_dict_response(raw_content):
            error_msg = f"Exchange action ({request_payload_model.type}) returned invalid content"
            raise APIError(error_msg, APIErrorCode.INVALID_RESPONSE.value)

        exchange_response = self._response_handler.handle_exchange_response(
            raw_content,
            http_status,
        )

        return exchange_response, http_status

    def _validate_cancel_args_list(
        self,
        cancel_args: list[CancelOrderArgs],
        current_method: str,
    ) -> None:
        """Validate the cancellation arguments list.

        Args:
            cancel_args: List of cancellation arguments to validate
            current_method: Name of calling method for error context

        Raises:
            ValueError: If validation fails
        """
        if not cancel_args:
            error_msg = f"[{current_method}] Cancel args list cannot be empty"
            logger.error("empty_cancel_args_list", method=current_method, message=error_msg)
            raise ValueError(error_msg)

        if len(cancel_args) > HYPERLIQUID_MAX_BATCH_SIZE:
            error_msg = (
                f"[{current_method}] Too many orders in batch: {len(cancel_args)}. "
                f"Maximum is {HYPERLIQUID_MAX_BATCH_SIZE}"
            )
            logger.error(
                "cancel_batch_size_exceeded",
                method=current_method,
                count=len(cancel_args),
            )
            raise ValueError(error_msg)

        # Validate each cancellation argument
        for i, args in enumerate(cancel_args):
            try:
                self._validate_batch_cancel_params(args)
            except ValueError as e:
                error_msg = f"[{current_method}] Cancel args {i} validation failed: {e}"
                logger.exception(
                    "cancel_args_validation_failed",
                    method=current_method,
                    index=i,
                    error=str(e),
                )
                raise ValueError(error_msg) from e

    def _validate_batch_cancel_params(self, args: CancelOrderArgs) -> tuple[str, int]:
        """Validate individual cancellation parameters.

        Args:
            args: Cancellation arguments to validate

        Returns:
            Tuple of (validated symbol, validated order ID)

        Raises:
            InvalidParameterTypeError: If symbol is None.
            MissingRequiredParameterError: If order_id is None.
        """
        if not args.symbol:
            raise InvalidParameterTypeError(
                parameter_name="symbol",
                expected_type="non-empty string",
                actual_type=type(args.symbol).__name__,
                value=args.symbol,
            )

        if not args.order_id:
            raise MissingRequiredParameterError(
                parameter_name="order_id",
                operation="order cancellation",
            )

        # Convert string order_id to int for Hyperliquid API
        try:
            order_id_int = int(args.order_id)
            if order_id_int <= 0:
                self._raise_positive_integer_error(args.order_id)
        except ValueError as e:
            self._raise_invalid_integer_error(args.order_id, e)

        # Convert Symbol to string for internal processing
        symbol_str = str(args.symbol)  # String conversion at boundary
        return symbol_str, order_id_int

    async def _prepare_cancel_data(
        self,
        cancel_args: list[CancelOrderArgs],
        current_method: str,
    ) -> list[tuple[str, int]]:
        """Prepare cancellation data for request building.

        Args:
            cancel_args: List of cancellation arguments
            current_method: Name of calling method for error context

        Returns:
            List of (symbol, order_id) tuples
        """
        cancel_items: list[tuple[str, int]] = []

        for args in cancel_args:
            symbol, order_id = self._validate_batch_cancel_params(args)
            cancel_items.append((symbol, order_id))

        return cancel_items

    async def _build_cancel_payload(
        self,
        cancel_args: list[CancelOrderArgs],
        cancel_items: list[tuple[str, int]],
    ) -> HyperliquidApiCancelOrderRequest:
        """Build the order cancellation request payload.

        Args:
            cancel_args: Original cancellation arguments
            cancel_items: Prepared cancellation data

        Returns:
            Validated cancellation request payload model

        Raises:
            APIError: If asset index cannot be found for symbol.
        """
        # Convert (symbol, order_id) tuples to (asset_index, order_id) tuples
        # Convert to format expected by request builder: [(order_id, asset_index, symbol)]
        formatted_items: list[tuple[str, int, str]] = []
        for symbol, order_id in cancel_items:
            # Get the correct asset index for the symbol (symbol is already string)
            asset_index = await self._get_asset_index_callable(symbol)
            if asset_index is None:
                raise APIError(
                    message=f"Symbol {symbol} not found in asset index mapping",
                    code=APIErrorCode.SYMBOL_NOT_FOUND.value,
                )
            formatted_items.append((str(order_id), asset_index, symbol))

        return self._request_builder.build_batch_cancel_order_payload(
            formatted_items,
        )

    async def _process_cancel_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        cancel_args: list[CancelOrderArgs],
        cancel_items: list[tuple[str, int]],
    ) -> list[CancelOrderResult]:
        """Process the order cancellation response.

        Args:
            raw_exchange_response: Raw response from exchange
            http_status: HTTP status code
            cancel_args: Original cancellation arguments
            cancel_items: Prepared cancellation data

        Returns:
            List of processed CancelOrderResult objects

        Raises:
            APIError: If response is empty or invalid.
        """
        # Check for exchange-level errors
        check_error_response(raw_exchange_response, http_status, self._error_mapper)

        # Process response data
        if raw_exchange_response.response is None:
            raise APIError(
                message="Empty response data from order cancellation",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=http_status,
            )

        # Handle batch vs single cancellation response
        if len(cancel_args) == 1:
            return await self._process_single_cancel_response(
                raw_exchange_response,
                cancel_args[0],
            )
        return await self._process_batch_cancel_response(
            raw_exchange_response,
            cancel_args,
            cancel_items,
        )

    async def _process_single_cancel_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        cancel_args: CancelOrderArgs,
    ) -> list[CancelOrderResult]:
        """Process response for a single order cancellation.

        Args:
            raw_exchange_response: Raw response from exchange
            cancel_args: Original cancellation arguments

        Returns:
            List containing single processed CancelOrderResult object

        Raises:
            EmptyResponseError: If response is empty.
            ServiceParameterError: If response contains errors.
        """
        symbol_value = cancel_args.symbol.value if cancel_args.symbol else "None"
        action_description = f"cancel order {cancel_args.order_id} ({symbol_value})"

        # Extract the actual status from the nested response structure
        response_data = raw_exchange_response.response_data
        if not response_data or not response_data.statuses:
            raise EmptyResponseError(
                response_type="status data",
                operation=action_description,
                exchange=ExchangeName.HYPERLIQUID,
            )

        # Process the status using utility functions
        processed_status = process_exchange_status(
            response_data.statuses[0],
            action_description,
        )

        # Format the result using utility function
        # Symbol is validated to be non-None earlier in the method
        if cancel_args.symbol is None:
            raise ServiceParameterError(
                parameter="symbol",
                issue="cannot be None at this point in processing",
                value=cancel_args.symbol,
                exchange=ExchangeName.HYPERLIQUID,
                operation="single order cancellation",
                expected_type="non-empty string",
                suggestion="This indicates an internal validation error",
            )
        result = format_cancel_order_result(
            processed_status,
            cancel_args.symbol,  # Pass Symbol directly
            str(cancel_args.order_id) if cancel_args.order_id else None,
        )

        return [result]

    async def _process_batch_cancel_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        cancel_args: list[CancelOrderArgs],
        cancel_items: list[tuple[str, int]],
    ) -> list[CancelOrderResult]:
        """Process response for batch order cancellation.

        Args:
            raw_exchange_response: Raw response from exchange
            cancel_args: Original cancellation arguments
            cancel_items: Prepared cancellation data

        Returns:
            List of processed CancelOrderResult objects

        Raises:
            APIError: If batch response validation fails.
        """
        # Extract the actual status list from the nested response structure
        response_data = raw_exchange_response.response_data
        if not response_data or not response_data.statuses:
            raise APIError(
                message="No status data in response for batch order cancellation",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        response_list = response_data.statuses

        # Validate response count
        validate_batch_response_counts(
            len(cancel_args),
            len(response_list),
            "batch order cancellation",
        )

        # Process each cancellation result
        batch_statuses: list[dict[str, Any]] = []
        symbols: list[Symbol] = []
        order_ids: list[str] = []

        for i, cancel_args_item in enumerate(cancel_args):
            symbol_value = cancel_args_item.symbol.value if cancel_args_item.symbol else "None"
            action_description = f"cancel batch order {i} ({symbol_value})"

            # Process the status for this cancellation
            processed_status = process_exchange_status(
                response_list[i],
                action_description,
            )

            batch_statuses.append(processed_status)
            # Collect Symbol objects for batch results formatting
            if cancel_args_item.symbol:
                symbols.append(cancel_args_item.symbol)
            order_ids.append(str(cancel_args_item.order_id))

        # Format batch results using utility function
        # No need to filter - all values are now guaranteed to be strings
        return format_batch_cancel_results(batch_statuses, symbols, order_ids)

    def _validate_cancel_all_orders_prerequisites(self) -> None:
        """Validate prerequisites for cancel all orders operation.

        Raises:
            ServiceParameterError: If authentication is not configured.
        """
        # Check authentication
        if not self._authenticator:
            raise ServiceParameterError(
                parameter="authenticator",
                issue="is required for cancel all orders operation",
                value=self._authenticator,
                exchange=ExchangeName.HYPERLIQUID,
                operation="cancel all orders",
                expected_type="IAuthenticator",
                suggestion="Ensure authentication is properly configured",
            )

        # Additional validation would go here (wallet address, etc.)
        logger.debug("cancel_all_orders_prerequisites_validated")

    async def _get_open_orders_for_cancellation(self, symbol: Symbol | None) -> list[Order]:
        """Get open orders for cancellation.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of open orders to cancel

        Note:
            Uses the order query service to fetch open orders.
        """
        if not self._order_query_service:
            logger.error(
                "order_query_service_not_set",
                symbol=symbol,
                message="Order query service not set - cannot fetch open orders",
            )
            return []

        # Use the order query service to get open orders
        return await self._order_query_service.get_open_orders(symbol)

    async def _process_all_order_cancellations(
        self,
        open_orders: list[Order],
        current_method: str,
    ) -> list[CancelOrderResult]:
        """Process cancellation of all open orders.

        Args:
            open_orders: List of open orders to cancel
            current_method: Name of calling method for error context

        Returns:
            List of CancelOrderResult objects
        """
        if not open_orders:
            return []

        # Convert orders to cancellation arguments
        cancel_args = [
            CancelOrderArgs(
                order_id=str(order.exchange_order_id),
                symbol=order.symbol,
            )
            for order in open_orders
            if order.exchange_order_id and order.symbol
        ]

        if not cancel_args:
            logger.warning(
                "no_cancellable_orders",
                method=current_method,
                total_orders=len(open_orders),
                message="No orders have valid cancellation parameters",
            )
            return []

        # Use batch cancellation for efficiency
        return await self._cancel_orders_core(cancel_args, current_method)

    def _raise_positive_integer_error(self, order_id: str | int) -> NoReturn:
        """Raise ServiceParameterError for non-positive order IDs.

        Args:
            order_id: The invalid order ID

        Raises:
            ServiceParameterError: Always raised with appropriate error message.
        """
        raise ServiceParameterError(
            parameter="order_id",
            issue="must be a positive integer",
            value=order_id,
            exchange=ExchangeName.HYPERLIQUID,
            operation="order cancellation",
            expected_type="positive integer",
            suggestion="Provide an order ID greater than 0",
        )

    def _raise_invalid_integer_error(
        self,
        order_id: str | int,
        original_error: ValueError,
    ) -> NoReturn:
        """Raise ServiceParameterError for invalid order ID format.

        Args:
            order_id: The invalid order ID
            original_error: The original ValueError from conversion

        Raises:
            ServiceParameterError: Always raised with appropriate error message.
        """
        raise ServiceParameterError(
            parameter="order_id",
            issue="must be a valid integer",
            value=order_id,
            exchange=ExchangeName.HYPERLIQUID,
            operation="order cancellation",
            expected_type="integer string",
            suggestion="Provide a valid numeric order ID",
        ) from original_error
