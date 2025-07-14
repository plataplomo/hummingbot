"""Hyperliquid Batch Order Service.

This service handles all batch order operations for the Hyperliquid exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Batch order placement (multiple orders in single request)
- Batch order cancellation (multiple cancellations in single request)
- Batch response processing and validation
- Performance optimization for high-frequency trading
"""

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import (
    InvalidParameterTypeError,
    OrderError,
    ServiceParameterError,
    SymbolNotFoundError,
)
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import RawStatusStringHL
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderResponseMapperProtocol
from cyberdelta.apis.hyperliquid.services.trading.hl_base_trading_service import (
    HyperliquidBaseTradingService,
)
from cyberdelta.apis.hyperliquid.services.utils.order_validation import validate_place_order_params
from cyberdelta.apis.hyperliquid.services.utils.response_formatting import (
    format_batch_cancel_results,
)
from cyberdelta.apis.hyperliquid.services.utils.status_processing import (
    check_error_response,
    process_exchange_status,
    validate_batch_response_counts,
)
from cyberdelta.apis.models.service_args_models import CancelOrderArgs, PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import OrderType
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.typing import ParsedJsonResponse, is_dict_response


logger = get_logger(__name__)

# Batch processing constraints
MAX_BATCH_SIZE = 50  # Maximum number of orders per batch
MIN_BATCH_SIZE = 1  # Minimum number of orders per batch

# HTTP client signature type
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidBatchOrderService(HyperliquidBaseTradingService):
    """Focused service for Hyperliquid batch order operations.

    Handles validation, processing, and transformation of batch order requests
    with optimized performance for high-frequency trading scenarios.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        mapper: OrderResponseMapperProtocol,
        error_mapper: HyperliquidErrorMapper,
        authenticator: IAuthenticator,
        action_endpoint: str = "/exchange",
        exchange_name: str = "hyperliquid",
        get_asset_index_callable: Callable[[str], Awaitable[int | None]] | None = None,
    ) -> None:
        """Initialize the batch order service.

        Args:
            http_client_requester: HTTP client callable for making requests
            request_builder: Request builder for creating API payloads
            response_handler: Response handler for processing API responses
            mapper: Order response mapper for transforming order placement responses
            error_mapper: Error mapper for handling Hyperliquid-specific errors
            authenticator: Authentication interface for request signing
            action_endpoint: API endpoint for exchange actions
            exchange_name: Name of the exchange for logging
            get_asset_index_callable: Callable to get asset index for a symbol
        """
        super().__init__(exchange_name)
        self._http_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._error_mapper = error_mapper
        self._authenticator = authenticator
        self._action_endpoint = action_endpoint
        self._exchange_name = exchange_name
        self._get_asset_index_callable = get_asset_index_callable

    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Place multiple orders in a single batch request for massive performance improvement.

        This method batches multiple order placements into a single API call, reducing:
        - N HTTP requests to 1 (6x performance improvement)
        - N EIP-712 signatures to 1
        - Network overhead and latency
        - API rate limit consumption

        Expected performance: 6 orders in <1 second vs ~9 seconds sequential

        Args:
            orders: List of validated PlaceOrderArgs for batch placement

        Returns:
            List of placed Order objects (one per successful order)

        Raises:
            APIError: If validation fails, batch size exceeded, or API request fails
            ValueError: If orders list is empty or contains invalid parameters
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_batch_orders"

        return await self._place_orders_core(orders, current_method)

    async def cancel_batch_orders(
        self, cancel_args: list[CancelOrderArgs]
    ) -> list[CancelOrderResult]:
        """Cancel multiple orders in a single batch request for improved performance.

        This method batches multiple order cancellations into a single API call,
        reducing network overhead and improving cancellation speed.

        Args:
            cancel_args: List of validated CancelOrderArgs for batch cancellation

        Returns:
            List of CancelOrderResult objects indicating success/failure for each order

        Raises:
            APIError: If validation fails, batch size exceeded, or API request fails
            ValueError: If cancel_args list is empty
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_batch_orders"

        return await self._cancel_orders_core(cancel_args, current_method)

    async def _place_orders_core(
        self,
        orders: list[PlaceOrderArgs],
        current_method: str,
    ) -> list[Order]:
        """Core unified method for placing batch orders.

        Args:
            orders: List of order placement arguments
            current_method: Name of calling method for error context

        Returns:
            List of successfully placed Order objects
        """
        self._validate_batch_orders(orders, current_method)

        try:
            # Prepare and validate order data
            place_order_items = await self._prepare_batch_order_data(orders, current_method)
            place_order_payload = self._build_batch_place_payload(orders, place_order_items)

            # Execute batch order placement
            raw_exchange_response, http_status = await self._place_orders_raw(place_order_payload)

            # Process response and return placed orders
            return await self._process_batch_place_order_response(
                raw_exchange_response,
                http_status,
                orders,
            )

        except APIError:
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            order_context = f"batch of {len(orders)} orders" if orders else "unknown batch"
            raise self._handle_service_error(e, current_method, order_context, 0, None) from e

    async def _cancel_orders_core(
        self,
        cancel_args: list[CancelOrderArgs],
        current_method: str,
    ) -> list[CancelOrderResult]:
        """Core unified method for canceling batch orders.

        Args:
            cancel_args: List of cancellation arguments
            current_method: Name of calling method for error context

        Returns:
            List of CancelOrderResult objects
        """
        self._validate_cancel_args_list(cancel_args, current_method)

        try:
            cancel_items = await self._prepare_cancel_data(cancel_args, current_method)
            cancel_request_payload = self._build_cancel_payload(cancel_args, cancel_items)

            raw_exchange_response, http_status = await self._cancel_orders_raw(
                cancel_request_payload,
            )

            return await self._process_batch_cancel_response(
                raw_exchange_response,
                http_status,
                cancel_args,
            )

        except APIError:
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            order_context = (
                f"batch of {len(cancel_args)} cancellations" if cancel_args else "unknown batch"
            )
            raise self._handle_service_error(e, current_method, order_context, 0, None) from e

    async def _place_orders_raw(
        self,
        request_payload_model: HyperliquidApiPlaceOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Execute the raw batch order placement request.

        Args:
            request_payload_model: Validated batch placement request payload

        Returns:
            Tuple of (raw exchange response, HTTP status code)
        """
        raw_content, http_status, _ = await self._http_requester(
            method="POST",
            endpoint=self._action_endpoint,
            data=request_payload_model,
            is_signed=True,
            serialize_none_as_null=True,
        )

        if not is_dict_response(raw_content):
            error_msg = f"Exchange action ({request_payload_model.type}) returned invalid content"
            raise APIError(error_msg, APIErrorCode.INVALID_RESPONSE.value)

        exchange_response = self._response_handler.handle_exchange_response(
            raw_content,
            http_status,
        )

        return exchange_response, http_status

    async def _cancel_orders_raw(
        self,
        request_payload_model: HyperliquidApiCancelOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Execute the raw batch order cancellation request.

        Args:
            request_payload_model: Validated batch cancellation request payload

        Returns:
            Tuple of (raw exchange response, HTTP status code)
        """
        raw_content, http_status, _ = await self._http_requester(
            method="POST",
            endpoint=self._action_endpoint,
            data=request_payload_model,
            is_signed=True,
            serialize_none_as_null=True,
        )

        if not is_dict_response(raw_content):
            error_msg = f"Exchange action ({request_payload_model.type}) returned invalid content"
            raise APIError(error_msg, APIErrorCode.INVALID_RESPONSE.value)

        exchange_response = self._response_handler.handle_exchange_response(
            raw_content,
            http_status,
        )

        return exchange_response, http_status

    def _is_error_status(
        self, status: str | dict[str, Any] | HyperliquidRawExchangeStatusObject
    ) -> bool:
        """Check if a status object represents an error.

        Args:
            status: Status object to check (can be dict, string, or status object)

        Returns:
            True if the status represents an error, False otherwise
        """
        # Handle dict-like objects (raw API responses)
        if isinstance(status, dict):
            return "error" in status

        # Handle string statuses (these are typically not errors)
        if isinstance(status, str):
            return False

        # Handle status objects with error attribute
        if hasattr(status, "error"):
            error_value = status.error
            return error_value is not None and bool(error_value)

        return False

    def _validate_batch_orders(self, orders: list[PlaceOrderArgs], current_method: str) -> None:
        """Validate the batch orders list.

        Args:
            orders: List of order arguments to validate
            current_method: Name of calling method for error context

        Raises:
            ValueError: If validation fails
        """
        if not orders:
            error_msg = "Cannot place empty batch of orders"
            logger.error("empty_orders_list", method=current_method, message=error_msg)
            raise ValueError(error_msg)

        if len(orders) > MAX_BATCH_SIZE:
            error_msg = (
                f"[{current_method}] Too many orders in batch: {len(orders)}. "
                f"Maximum is {MAX_BATCH_SIZE}"
            )
            logger.error("batch_size_exceeded", method=current_method, count=len(orders))
            raise ValueError(error_msg)

        # Validate each order using utility function
        for i, order_args in enumerate(orders):
            try:
                validate_place_order_params(order_args, f"{current_method}[{i}]")
                # Additional validation: no market orders in batch operations
                if order_args.order_type == OrderType.MARKET:
                    self._raise_market_order_error()
            except ValueError as e:
                error_msg = f"[{current_method}] Order {i} validation failed: {e}"
                logger.exception(
                    "order_validation_failed", method=current_method, index=i, error=str(e)
                )
                raise ValueError(error_msg) from e

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
            error_msg = "Cannot cancel empty batch of orders"
            logger.error("empty_cancel_args_list", method=current_method, message=error_msg)
            raise ValueError(error_msg)

        if len(cancel_args) > MAX_BATCH_SIZE:
            error_msg = (
                f"[{current_method}] Too many orders in batch: {len(cancel_args)}. "
                f"Maximum is {MAX_BATCH_SIZE}"
            )
            logger.error(
                "cancel_batch_size_exceeded", method=current_method, count=len(cancel_args)
            )
            raise ValueError(error_msg)

        # Validate each cancellation argument
        for i, args in enumerate(cancel_args):
            try:
                self._validate_batch_cancel_params(args)
            except ValueError as e:
                error_msg = f"[{current_method}] Cancel args {i} validation failed: {e}"
                logger.exception(
                    "cancel_args_validation_failed", method=current_method, index=i, error=str(e)
                )
                raise ValueError(error_msg) from e

    def _validate_batch_cancel_params(self, args: CancelOrderArgs) -> tuple[str, int]:
        """Validate individual cancellation parameters.

        Args:
            args: Cancellation arguments to validate

        Returns:
            Tuple of (validated symbol, validated order ID)

        Raises:
            ValueError: If parameters are invalid
        """
        if not args.symbol:
            raise InvalidParameterTypeError(
                parameter_name="symbol",
                expected_type="non-empty string",
                actual_type=type(args.symbol).__name__,
                value=args.symbol,
            )

        # order_id is a required field in CancelOrderArgs

        # Convert string order_id to int for Hyperliquid API
        try:
            order_id_int = int(args.order_id)
        except ValueError as e:
            self._raise_invalid_order_id_error(args.order_id, e)
            return args.symbol, -1  # Never reached but needed for type checking
        else:
            if order_id_int <= 0:
                self._raise_positive_order_id_error(args.order_id)
                return args.symbol, -1  # Never reached but needed for type checking
            return args.symbol, order_id_int

    async def _prepare_batch_order_data(
        self,
        orders: list[PlaceOrderArgs],
        current_method: str,
    ) -> list[tuple[PlaceOrderArgs, int]]:
        """Prepare batch order data for request building.

        Args:
            orders: List of order placement arguments
            current_method: Name of calling method for error context

        Returns:
            List of (PlaceOrderArgs, asset_index) tuples

        Raises:
            APIError: If symbol not found or asset index lookup fails
        """
        orders_with_indices: list[tuple[PlaceOrderArgs, int]] = []

        if not self._get_asset_index_callable:
            # Fallback for testing - use index 0 for all
            logger.warning(
                "no_asset_index_callable",
                method=current_method,
                message="No asset index callable provided, using index 0 for all orders",
            )
            return [(order, 0) for order in orders]

        for order in orders:
            asset_index = await self._get_asset_index_callable(order.symbol)
            if asset_index is None:
                raise SymbolNotFoundError(symbol=order.symbol, exchange=self._exchange_name)
            orders_with_indices.append((order, asset_index))

        return orders_with_indices

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

    def _build_batch_place_payload(
        self,
        orders: list[PlaceOrderArgs],
        place_order_items: list[tuple[PlaceOrderArgs, int]],
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build the batch order placement request payload.

        Args:
            orders: Original order placement arguments
            place_order_items: Prepared order placement data with asset indices

        Returns:
            Validated batch placement request payload model
        """
        return self._request_builder.build_batch_place_order_payload(
            orders,
            place_order_items,
            None,  # tif_mapping - could be added later if needed
        )

    def _build_cancel_payload(
        self,
        cancel_args: list[CancelOrderArgs],
        cancel_items: list[tuple[str, int]],
    ) -> HyperliquidApiCancelOrderRequest:
        """Build the batch order cancellation request payload.

        Args:
            cancel_args: Original cancellation arguments
            cancel_items: Prepared cancellation data

        Returns:
            Validated batch cancellation request payload model
        """
        # Convert to format expected by request builder: [(order_id, asset_index, symbol)]
        formatted_items = [(str(item[1]), 0, item[0]) for item in cancel_items]
        return self._request_builder.build_batch_cancel_order_payload(
            formatted_items,
        )

    async def _process_batch_place_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        original_orders: list[PlaceOrderArgs],
    ) -> list[Order]:
        """Process batch order placement response with individual order status handling.

        Args:
            raw_exchange_response: Raw response from exchange
            http_status: HTTP status code
            original_orders: Original order placement arguments

        Returns:
            List of successfully placed Order objects
        """
        # Check for exchange-level errors
        check_error_response(raw_exchange_response, http_status, self._error_mapper)

        # Extract the actual status list from the nested response structure
        response_data = raw_exchange_response.response_data
        if not response_data or not response_data.statuses:
            raise APIError(
                message="No status data in response for batch order placement",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=http_status,
            )

        # Handle special case: single error status for all orders
        # When all orders fail with the same validation error, Hyperliquid returns
        # a single error status rather than repeating it for each order
        statuses_to_process = response_data.statuses
        if len(response_data.statuses) == 1 and len(original_orders) > 1:
            # Check if the single status is an error
            single_status = response_data.statuses[0]
            is_error = self._is_error_status(single_status)

            if is_error:
                # Duplicate the error status for each order
                logger.warning(
                    "batch_single_error_for_all_orders",
                    exchange=self._exchange_name,
                    orders_count=len(original_orders),
                    message="Single error status applies to all orders in batch",
                )
                statuses_to_process = response_data.statuses * len(original_orders)
            else:
                # Not an error - validate normal response count
                validate_batch_response_counts(
                    len(original_orders),
                    len(response_data.statuses),
                    "batch order placement",
                )
        else:
            # Normal case - validate response count matches order count
            validate_batch_response_counts(
                len(original_orders),
                len(response_data.statuses),
                "batch order placement",
            )

        # Process each order status individually
        placed_orders: list[Order] = []
        failed_orders: list[tuple[int, str]] = []

        await self._process_batch_order_statuses(
            statuses_to_process,
            original_orders,
            placed_orders,
            failed_orders,
        )

        self._handle_batch_order_failures(failed_orders, placed_orders, original_orders)

        return placed_orders

    async def _process_batch_cancel_response(
        self,
        raw_response: HyperliquidRawExchangeResponse,
        http_status: int,
        original_cancel_args: list[CancelOrderArgs],
    ) -> list[CancelOrderResult]:
        """Process batch cancel response with individual cancellation status handling.

        Args:
            raw_response: Raw response from Hyperliquid API
            http_status: HTTP status code from the request
            original_cancel_args: Original cancel arguments for context

        Returns:
            List of CancelOrderResult objects
        """
        # Check for exchange-level errors
        check_error_response(raw_response, http_status, self._error_mapper)

        # Extract the actual status list from the nested response structure
        response_data = raw_response.response_data
        if not response_data or not response_data.statuses:
            raise APIError(
                message="No status data in response for batch order cancellation",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=http_status,
            )

        response_list = response_data.statuses

        # Handle special case: single error status for all cancellations
        # When all cancellations fail with the same error, Hyperliquid may return
        # a single error status rather than repeating it for each order
        if len(response_list) == 1 and len(original_cancel_args) > 1:
            # Check if the single status is an error
            single_status = response_list[0]
            is_error = self._is_error_status(single_status)

            if is_error:
                # Duplicate the error status for each cancellation
                logger.warning(
                    "batch_single_error_for_all_cancellations",
                    exchange=self._exchange_name,
                    cancellations_count=len(original_cancel_args),
                    message="Single error status applies to all cancellations in batch",
                )
                response_list = response_data.statuses * len(original_cancel_args)
            else:
                # Not an error - validate normal response count
                validate_batch_response_counts(
                    len(original_cancel_args),
                    len(response_list),
                    "batch order cancellation",
                )
        else:
            # Normal case - validate response count matches cancellation count
            validate_batch_response_counts(
                len(original_cancel_args),
                len(response_list),
                "batch order cancellation",
            )

        # Process each cancellation result
        batch_statuses: list[dict[str, Any]] = []
        symbols: list[str] = []
        order_ids: list[str] = []

        for i, cancel_args_item in enumerate(original_cancel_args):
            action_description = f"cancel batch order {i} ({cancel_args_item.symbol})"

            # Process the status for this cancellation
            processed_status = process_exchange_status(
                response_list[i],
                action_description,
            )

            batch_statuses.append(processed_status)
            # symbol is validated to be non-empty in validation step
            symbols.append(cancel_args_item.symbol or "")
            order_ids.append(str(cancel_args_item.order_id))

        # Format batch results using utility function
        # No need to filter - all values are now guaranteed to be strings
        return format_batch_cancel_results(batch_statuses, symbols, order_ids)

    async def _process_batch_order_statuses(
        self,
        statuses: list[RawStatusStringHL | HyperliquidRawExchangeStatusObject],
        original_orders: list[PlaceOrderArgs],
        placed_orders: list[Order],
        failed_orders: list[tuple[int, str]],
    ) -> None:
        """Process individual order statuses from batch response.

        Args:
            statuses: List of status objects from the response
            original_orders: Original order placement arguments
            placed_orders: List to append successfully placed orders
            failed_orders: List to append failed order information
        """
        # Process available statuses
        for i, status in enumerate(statuses):
            if i >= len(original_orders):
                logger.error(
                    "batch_unexpected_status",
                    action="process_batch_order_statuses",
                    message="Unexpected extra status in batch response",
                    exchange_name=self._exchange_name,
                    status_index=i,
                )
                break

            args = original_orders[i]
            try:
                await self._process_single_batch_order_status(
                    status,
                    args,
                    i,
                    placed_orders,
                    failed_orders,
                )
            except (TransformationError, ValidationError) as e:
                failed_orders.append((i, f"Processing error: {e!s}"))

        # Handle orders that didn't get a status response (filtered by exchange)
        failed_orders.extend(
            (i, "Order filtered or rejected by exchange (no status returned)")
            for i in range(len(statuses), len(original_orders))
        )

    async def _process_single_batch_order_status(
        self,
        status: RawStatusStringHL | HyperliquidRawExchangeStatusObject,
        args: PlaceOrderArgs,
        order_index: int,
        placed_orders: list[Order],
        failed_orders: list[tuple[int, str]],
    ) -> None:
        """Process a single order status within a batch response.

        Args:
            status: Individual status object from the batch response
            args: Original order placement arguments
            order_index: Index of this order in the batch
            placed_orders: List to append successfully placed orders
            failed_orders: List to append failed order information
        """
        processed_status = process_exchange_status(
            status,
            f"place_batch_orders[{order_index}]",
        )

        if "error" in processed_status:
            error_msg = processed_status["error"]
            failed_orders.append((order_index, error_msg))
            return

        # Handle successful statuses using the mapper
        if "resting" in processed_status:
            order = await self._handle_resting_order(processed_status["resting"], args)
            placed_orders.append(order)
        elif "filled" in processed_status:
            order = await self._handle_filled_order(processed_status["filled"], args)
            placed_orders.append(order)
        elif "canceled" in processed_status:
            failed_orders.append((
                order_index,
                f"Order was canceled unexpectedly: {processed_status}",
            ))
        else:
            failed_orders.append((order_index, f"Unknown status: {processed_status}"))

    async def _handle_resting_order(
        self,
        resting_data: HyperliquidRawExchangeStatusResting,
        args: PlaceOrderArgs,
    ) -> Order:
        """Handle a resting order from batch response.

        Args:
            resting_data: Resting order data from the exchange response
            args: Original order placement arguments

        Returns:
            Order object for the resting order
        """
        # This would be implemented using the mapper
        # For now, return a placeholder
        return self._mapper.transform_resting_order_to_internal(resting_data, args)

    async def _handle_filled_order(
        self,
        filled_data: HyperliquidRawExchangeStatusFilled,
        args: PlaceOrderArgs,
    ) -> Order:
        """Handle a filled order from batch response.

        Args:
            filled_data: Filled order data from the exchange response
            args: Original order placement arguments

        Returns:
            Order object for the filled order
        """
        # This would be implemented using the mapper
        # For now, return a placeholder
        return self._mapper.transform_filled_order_to_internal(filled_data, args)

    def _handle_batch_order_failures(
        self,
        failed_orders: list[tuple[int, str]],
        placed_orders: list[Order],
        original_orders: list[PlaceOrderArgs],
    ) -> None:
        """Handle batch order failures by raising appropriate error.

        Args:
            failed_orders: List of failed order indices and error messages
            placed_orders: List of successfully placed orders
            original_orders: Original order placement arguments

        Raises:
            OrderError: If any orders failed to place
        """
        if failed_orders:
            error_details = "; ".join([f"Order {i + 1}: {error}" for i, error in failed_orders])
            raise OrderError(
                message=f"Batch order placement partially failed. "
                f"Successful: {len(placed_orders)}/{len(original_orders)}. "
                f"Failures: {error_details}",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            )

    def _raise_market_order_error(self) -> None:
        """Raise ServiceParameterError for market orders in batch operations."""
        raise ServiceParameterError(
            parameter="order_type",
            issue="market orders are not supported in batch operations",
            value="MARKET",
            exchange="hyperliquid",
            operation="batch order placement",
            expected_type="non-market order type",
            suggestion="Use LIMIT orders for batch operations",
        )

    def _raise_positive_order_id_error(self, order_id: str | int) -> None:
        """Raise ServiceParameterError for non-positive order IDs.

        Args:
            order_id: The invalid order ID
        """
        raise ServiceParameterError(
            parameter="order_id",
            issue="must be a positive integer",
            value=order_id,
            exchange="hyperliquid",
            operation="batch order cancellation",
            expected_type="positive integer",
            suggestion="Provide an order ID greater than 0",
        )

    def _raise_invalid_order_id_error(
        self, order_id: str | int, original_error: ValueError
    ) -> None:
        """Raise ServiceParameterError for invalid order ID format.

        Args:
            order_id: The invalid order ID
            original_error: The original ValueError from conversion
        """
        raise ServiceParameterError(
            parameter="order_id",
            issue="must be a valid integer",
            value=order_id,
            exchange="hyperliquid",
            operation="batch order cancellation",
            expected_type="integer string",
            suggestion="Provide a valid numeric order ID",
        ) from original_error
