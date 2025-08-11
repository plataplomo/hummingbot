"""Hyperliquid Order Status Processor.

This service handles all order status processing operations for the Hyperliquid exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Status validation and normalization
- Order state transitions (resting, filled, canceled)
- Response processing and error handling
- Order object creation from status data
"""

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base.trading_execution_domain import LiquidityRequirement, PositionIntent
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import OrderError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderResponseMapperProtocol

# Import needed at runtime (not just for type checking)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_query_service import (
    HyperliquidOrderQueryService,
)
from cyberdelta.apis.models.service_args.trading import GetOrderArgs, PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import ExchangeName
from cyberdelta.models import Order
from cyberdelta.utils.secure_transformation import secure_transform
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# HTTP client signature type for order refetching
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidOrderStatusProcessor:
    """Focused service for Hyperliquid order status processing.

    Handles validation, normalization, and transformation of order status data
    with comprehensive error handling and state management.
    """

    def __init__(
        self,
        mapper: OrderResponseMapperProtocol,
        error_mapper: HyperliquidErrorMapper,
        order_query_service: HyperliquidOrderQueryService | None = None,  # Injected
        exchange_name: ExchangeName = ExchangeName.HYPERLIQUID,
    ) -> None:
        """Initialize the order status processor.

        Args:
            mapper: Data mapper for transforming raw responses to internal models
            error_mapper: Error mapper for handling Hyperliquid-specific errors
            order_query_service: Order query service for refetching order details
            exchange_name: Name of the exchange for logging
        """
        self._mapper = mapper
        self._error_mapper = error_mapper
        self._order_query_service = order_query_service
        self._exchange_name = exchange_name

    def set_order_query_service(self, order_query_service: HyperliquidOrderQueryService) -> None:
        """Set the order query service to avoid circular dependency during initialization.

        Args:
            order_query_service: Order query service instance
        """
        self._order_query_service = order_query_service

    def process_exchange_status(
        self,
        status_raw: object,
        action_description: str,
    ) -> dict[str, Any]:
        """Process raw exchange status into a standardized format.

        Args:
            status_raw: Raw status object from the exchange
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing standardized status information

        Raises:
            OrderError: If status structure is unknown or invalid
        """
        # Handle Pydantic model status
        if isinstance(status_raw, HyperliquidRawExchangeStatusObject):
            return self._process_pydantic_status(status_raw, action_description)

        # Handle dict status (for backwards compatibility)
        if isinstance(status_raw, dict):
            # Type assertion for pyright - we know it's a dict after isinstance check
            status_dict = cast("dict[str, Any]", status_raw)
            result = self._process_dict_status(status_dict, action_description)
            if result:  # If we found a recognized status
                return result

        # Handle string status
        elif isinstance(status_raw, str):
            return self._process_string_status(status_raw, action_description)

        # Unknown status type
        raise OrderError(
            message=f"Unknown status structure for {action_description}: {status_raw!r}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    def check_error_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
    ) -> None:
        """Check if the response is an error and raise appropriate exception.

        Args:
            raw_exchange_response: Raw response from the exchange
            http_status: HTTP status code

        Note:
            Raises the mapped error (typically APIError) if response contains an error.
        """
        if (raw_exchange_response.status == "err" and raw_exchange_response.response) and (
            isinstance(raw_exchange_response.response, str)
        ):
            # Use the error mapper to get the specific error code for this message
            mapped_error = self._error_mapper.map_string_error(
                raw_exchange_response.response,
                http_status=http_status,
            )
            raise mapped_error

    async def process_place_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        args: PlaceOrderArgs,
    ) -> Order:
        """Process the place order response and return the internal Order.

        Args:
            raw_exchange_response: Raw response from the exchange
            http_status: HTTP status code
            args: Original order placement arguments

        Returns:
            Order object created from the exchange response

        Raises:
            OrderError: If order processing fails or has invalid status
        """
        # Check if this is an error response
        self.check_error_response(raw_exchange_response, http_status)

        # Process successful response using the normalized property
        response_data = raw_exchange_response.response_data

        if response_data and response_data.statuses:
            first_status = response_data.statuses[0]

            # Process the status - moved business logic from ResponseHandler
            processed_status = self.process_exchange_status(first_status, "place_order")

            if "error" in processed_status:
                # Use the error mapper for error messages
                mapped_error = self._error_mapper.map_string_error(
                    processed_status["error"],
                    http_status=http_status,
                )
                raise mapped_error

            # Handle successful statuses
            if "resting" in processed_status:
                return await self.handle_resting_order(processed_status["resting"], args)
            if "filled" in processed_status:
                return await self.handle_filled_order(processed_status["filled"], args)
            if "canceled" in processed_status:
                raise OrderError(
                    message=f"Order was canceled unexpectedly: {processed_status}",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )
            raise OrderError(
                message=f"Unknown order status: {processed_status}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        raise OrderError(
            message="Invalid place order response: missing status data",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    async def handle_resting_order(
        self,
        resting_info: HyperliquidRawExchangeStatusResting,
        args: PlaceOrderArgs,
    ) -> Order:
        """Handle a resting (open) order response.

        Args:
            resting_info: Resting order information from the exchange
            args: Original order placement arguments

        Returns:
            Order object for the resting order
        """
        new_oid = resting_info.oid
        logger.info(
            "order_placed_refetching",
            message="Order placed with OID: %s. Re-fetching for full details.",
            message_args=(new_oid,),
        )

        try:
            # Try to get full order details if order query service is available
            if self._order_query_service:
                internal_order = await self._order_query_service.get_order(
                    GetOrderArgs(symbol=args.symbol, order_id=str(new_oid)),
                )
                if internal_order:
                    return internal_order
        except APIError as e:
            # If order status fetch fails, create a minimal Order object
            logger.warning(
                "order_details_fetch_failed",
                message=(
                    "Failed to fetch full order details for OID %s: %s. "
                    "Creating minimal order object."
                ),
                message_args=(new_oid, e),
            )

        # Create a minimal order object with the information we have
        return self._create_minimal_order(new_oid, args, OrderStatus.OPEN)

    async def handle_filled_order(
        self,
        filled_info: HyperliquidRawExchangeStatusFilled,
        args: PlaceOrderArgs,
    ) -> Order:
        """Handle a filled order response.

        Args:
            filled_info: Filled order information from the exchange
            args: Original order placement arguments

        Returns:
            Order object for the filled order
        """
        filled_oid = filled_info.oid
        logger.info(
            "order_filled_refetching",
            message="Order filled with OID: %s. Re-fetching for full details.",
            message_args=(filled_oid,),
        )

        try:
            # Try to get full order details if order query service is available
            if self._order_query_service:
                internal_order = await self._order_query_service.get_order(
                    GetOrderArgs(symbol=args.symbol, order_id=str(filled_oid)),
                )
                if internal_order:
                    return internal_order
        except APIError as e:
            # If order status fetch fails, create a filled Order object
            logger.warning(
                "filled_order_details_fetch_failed",
                message=(
                    "Failed to fetch full order details for filled OID %s: %s. "
                    "Creating minimal filled order object."
                ),
                message_args=(filled_oid, e),
            )

        # Create a filled order object with the available information
        return self._create_filled_order(filled_info, args)

    def _process_pydantic_status(
        self,
        status_raw: HyperliquidRawExchangeStatusObject,
        action_description: str,
    ) -> dict[str, Any]:
        """Process Pydantic model status.

        Args:
            status_raw: Pydantic status object
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing processed status information

        Raises:
            OrderError: If status structure is unknown
        """
        if status_raw.resting:
            return {"resting": status_raw.resting}
        if status_raw.filled:
            return {"filled": status_raw.filled}
        if status_raw.error:
            return {"error": status_raw.error}
        raise OrderError(
            message=f"Unknown status structure for {action_description}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    def _process_dict_status(
        self,
        status_raw: dict[str, Any],
        action_description: str,
    ) -> dict[str, Any]:
        """Process dict status for backwards compatibility.

        Args:
            status_raw: Dictionary status object
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing processed status or empty dict if unrecognized
        """
        # Check for resting status
        if "resting" in status_raw and isinstance(status_raw["resting"], dict):
            return self._process_dict_resting_status(status_raw, action_description)

        # Check for filled status
        if "filled" in status_raw and isinstance(status_raw["filled"], dict):
            return self._process_dict_filled_status(status_raw, action_description)

        # Check for canceled status
        if "canceled" in status_raw and isinstance(status_raw["canceled"], dict):
            return self._process_dict_canceled_status(status_raw, action_description)

        # Check for error status
        if "error" in status_raw and isinstance(status_raw["error"], str):
            return {"error": status_raw["error"]}

        # No recognized status found
        return {}

    def _process_dict_resting_status(
        self,
        status_raw: dict[str, Any],
        action_description: str,
    ) -> dict[str, Any]:
        """Process dict resting status.

        Args:
            status_raw: Dictionary containing resting status
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing processed resting status

        Raises:
            OrderError: If resting status data is invalid
        """
        oid = status_raw["resting"].get("oid")
        if not isinstance(oid, int):
            raise OrderError(
                message=f"Invalid or missing 'oid' in resting status for {action_description}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        return {"resting": HyperliquidRawExchangeStatusResting(oid=oid)}

    def _process_dict_filled_status(
        self,
        status_raw: dict[str, Any],
        action_description: str,
    ) -> dict[str, Any]:
        """Process dict filled status.

        Args:
            status_raw: Dictionary containing filled status
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing processed filled status

        Raises:
            OrderError: If filled status data is invalid
        """
        filled_details = status_raw["filled"]
        oid = filled_details.get("oid")
        total_sz = filled_details.get("totalSz")
        avg_px = filled_details.get("avgPx")

        if not isinstance(oid, int):
            raise OrderError(
                message=f"Invalid or missing 'oid' in filled status for {action_description}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        if not isinstance(total_sz, str) or not isinstance(avg_px, str):
            raise OrderError(
                message=f"Invalid filled status data for {action_description}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        return {
            "filled": HyperliquidRawExchangeStatusFilled(
                oid=oid,
                totalSz=total_sz,
                avgPx=avg_px,
            ),
        }

    def _process_dict_canceled_status(
        self,
        status_raw: dict[str, Any],
        action_description: str,
    ) -> dict[str, Any]:
        """Process dict canceled status.

        Args:
            status_raw: Dictionary containing canceled status
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing processed canceled status

        Raises:
            OrderError: If canceled status data is invalid
        """
        oid = status_raw["canceled"].get("oid")
        if not isinstance(oid, int):
            raise OrderError(
                message=f"Invalid or missing 'oid' in canceled status for {action_description}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        return {"canceled": {"oid": oid}}

    def _process_string_status(self, status_raw: str, action_description: str) -> dict[str, Any]:
        """Process string status.

        Args:
            status_raw: String status value
            action_description: Description of the action for error reporting

        Returns:
            Dictionary containing processed string status
        """
        status_lower = status_raw.lower()

        # Handle success statuses
        if status_lower in {"success", "ok", "accepted"}:
            return {"success": status_raw}

        # Handle canceled status
        if status_lower == "canceled":
            return {"canceled": {"type": "string"}}

        # Any other string is treated as an error
        logger.warning(
            "direct_string_status_error",
            message="Encountered direct string status for %s: '%s'. Treating as error.",
            message_args=(action_description, status_raw),
        )
        return {"error": status_raw}

    def _create_minimal_order(
        self,
        order_id: int,
        args: PlaceOrderArgs,
        status: OrderStatus,
    ) -> Order:
        """Create a minimal order object with available information.

        Args:
            order_id: Exchange order ID
            args: Original order placement arguments
            status: Order status

        Returns:
            Minimal Order object
        """
        # Generate a client order ID if none provided
        client_order_id = (
            args.client_order_id or f"HL_{order_id}_{int(datetime.now(UTC).timestamp())}"
        )

        # Use secure_transform to ensure validation
        order_data = {
            "exchange": ExchangeName.HYPERLIQUID.value,
            "exchange_order_id": str(order_id),
            "symbol": args.symbol,
            "side": args.side.value,
            "order_type": args.order_type.value,
            "quantity_requested": str(args.quantity),
            "price": str(args.price) if args.price else None,
            "time_in_force": args.time_in_force.value,
            "status": status.value,
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "reduce_only": args.execution.position_intent == PositionIntent.REDUCE_ONLY,
            "post_only": args.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY,
            "client_order_id": client_order_id,
        }

        return secure_transform(
            data=order_data,
            model_class=Order,
            context=f"place_order_{status.value}_{args.symbol}",
            source_exchange=ExchangeName.HYPERLIQUID.value,
        )

    def _create_filled_order(
        self,
        filled_info: HyperliquidRawExchangeStatusFilled,
        args: PlaceOrderArgs,
    ) -> Order:
        """Create a filled order object with available information.

        Args:
            filled_info: Filled order information from the exchange
            args: Original order placement arguments

        Returns:
            Filled Order object
        """
        filled_oid = filled_info.oid

        # Generate a client order ID if none provided
        client_order_id = (
            args.client_order_id or f"HL_{filled_oid}_{int(datetime.now(UTC).timestamp())}"
        )

        # Use secure_transform to ensure validation
        order_data = {
            "exchange": ExchangeName.HYPERLIQUID.value,
            "exchange_order_id": str(filled_oid),
            "symbol": args.symbol,
            "side": args.side.value,
            "order_type": args.order_type.value,
            "quantity_requested": str(args.quantity),
            "quantity_filled": filled_info.total_sz,
            "price": str(args.price) if args.price else None,
            "average_fill_price": filled_info.avg_px,
            "time_in_force": args.time_in_force.value,
            "status": OrderStatus.FILLED.value,
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "reduce_only": args.execution.position_intent == PositionIntent.REDUCE_ONLY,
            "post_only": args.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY,
            "client_order_id": client_order_id,
        }

        return secure_transform(
            data=order_data,
            model_class=Order,
            context=f"place_order_filled_{args.symbol}",
            source_exchange=ExchangeName.HYPERLIQUID.value,
        )

    def handle_service_error(
        self,
        error: Exception,
        current_method: str,
        context: str,
        status_code: int = 0,
        raw_response_content: str | None = None,
    ) -> APIError:
        """Handle service errors in a standardized way.

        Args:
            error: The original exception
            current_method: Name of the method where error occurred
            context: Context description
            status_code: HTTP status code if available
            raw_response_content: Raw response content if available

        Returns:
            APIError instance with appropriate error details
        """
        if isinstance(error, TransformationError):
            logger.error(
                "transformation_error",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error=str(error),
                message="Failed to transform exchange data for %s: %s",
            )
            return APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

        if isinstance(error, ValidationError):
            logger.error(
                "validation_error",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error=str(error),
                message="Internal data validation failed for %s: %s",
            )
            return APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

        if isinstance(error, (ValueError, TypeError)):
            # Distinguish input validation from internal errors
            error_msg = str(error)
            if current_method in error_msg:
                # Re-raise input validation errors
                raise error

            logger.error(
                "service_logic_error",
                action=current_method,
                exchange=self._exchange_name,
                context=context,
                error=str(error),
                message="Service internal logic error for %s: %s",
            )
            return APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

        logger.error(
            "unexpected_service_failure",
            action=current_method,
            exchange=self._exchange_name,
            context=context,
            error=str(error),
            message="Unexpected service failure for %s: %s",
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
