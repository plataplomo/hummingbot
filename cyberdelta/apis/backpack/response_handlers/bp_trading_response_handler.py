"""Backpack Trading Response Handler.

This module handles validation of trading responses from the Backpack API,
extracted from the monolithic response handler to improve maintainability and testability.

Focused on:
- Order placement responses
- Order cancellation responses
- Order query responses
- Trade history responses
- Fill history responses
"""

from __future__ import annotations

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.backpack.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import CancelOrderResultStatus
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for raw JSON response from HTTP client
# Aligned with ParsedJsonResponse from http_client.py
type RawJsonResponse = ParsedJsonResponse


class BackpackTradingResponseHandler(TradingResponseHandlerProtocol):
    """Handles validation of trading responses from Backpack REST API endpoints.

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
            handler_class="BackpackTradingResponseHandler",
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
            context: Context string indicating the operation (e.g., "trading.place_order")

        Returns:
            Processed response data

        Raises:
            APIError: If response processing fails
            NotImplementedError: If context is not supported
        """
        # Extract operation from context (format: "domain.operation")
        if "." in context:
            _, operation = context.split(".", 1)
        else:
            operation = context

        # Note: Trading handlers require additional parameters (symbol, order_id, etc.)
        # which are not available in the generic handle_response interface.
        # This dispatcher is implemented for protocol consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "place_order",
            "cancel_order",
            "get_open_orders",
            "get_order_history",
            "get_order_status",
            "get_fills",
            "get_trade_history",
            "cancel_all_orders",
        }:
            # Trading operations require additional context not available in generic interface
            raise NotImplementedError(
                f"Trading operation '{operation}' requires specific parameters not available "
                f"in generic handle_response interface. Use specific handler methods directly."
            )
        raise NotImplementedError(
            f"Trading operation '{operation}' not supported by registry dispatch"
        )

    @staticmethod
    def handle_place_order_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawOrder:
        """Validate the raw response for the Place Order endpoint.

        Returns:
            Validated BackpackRawOrder model.

        Raises:
            APIError: If validation of order data fails.
        """
        context = "place order response"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrder.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackTradingResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: RawJsonResponse,
        order_id: str,
        symbol: str,
    ) -> CancelOrderResult:
        """Validate the raw response for the Cancel Order endpoint.

        Expects no content on success.

        Returns:
            CancelOrderResult indicating successful cancellation.
        """
        if raw_response_content not in [None, {}]:
            # If we get content, it might be an error structure or unexpected success data.
            # For Backpack, successful cancel usually returns 200 OK with empty body or {}.
            logger.warning(
                "backpack_unexpected_cancel_content",
                order_id=order_id,
                symbol=symbol,
                raw_response_content=raw_response_content,
                message="Received unexpected content after cancelling order",
            )

        # Create CancelOrderResult for successful cancellation
        return CancelOrderResult(
            symbol=symbol,
            order_id=order_id,
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=raw_response_content if isinstance(raw_response_content, dict) else None,
        )

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawOrder]:
        """Validate the raw response for the Get Open Orders endpoint.

        Returns:
            List of validated BackpackRawOrder models for open orders.

        Raises:
            APIError: If validation of order data fails.
        """
        context = f"open orders ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_orders: list[BackpackRawOrder] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackTradingResponseHandler._handle_validation_error(
                    e,
                    f"single open order item in {context}",
                    validated_item,
                ) from e
        return validated_orders

    @staticmethod
    def handle_get_order_history_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawOrder]:
        """Validate the raw response for the Get Order History endpoint.

        Returns:
            List of validated BackpackRawOrder models from order history.

        Raises:
            APIError: If validation of order data fails.
        """
        context = f"order history ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_orders: list[BackpackRawOrder] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackTradingResponseHandler._handle_validation_error(
                    e,
                    f"single order history item in {context}",
                    validated_item,
                ) from e
        return validated_orders

    @staticmethod
    def handle_get_trade_history_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawPublicTrade]:
        """Validate the raw response for the Get Trade History endpoint.

        Now returns list[BackpackRawPublicTrade] as per user request.

        Returns:
            List of validated BackpackRawPublicTrade models.

        Raises:
            APIError: If validation of trade data fails.
        """
        context = f"trade history ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_items: list[BackpackRawPublicTrade] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_items.append(BackpackRawPublicTrade.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackTradingResponseHandler._handle_validation_error(
                    e,
                    f"single trade item in {context}",
                    validated_item,
                ) from e
        return validated_items

    @staticmethod
    def handle_get_fills_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawFill]:
        """Validate the raw response for the Get Fills (/wapi/v1/history/fills) endpoint.

        This endpoint returns BackpackRawFill format, different from BackpackRawPublicTrade.

        Returns:
            List of validated BackpackRawFill models.

        Raises:
            APIError: If validation of fill data fails.
        """
        context = f"fills history ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_fills: list[BackpackRawFill] = []
        for i, item in enumerate(validated_list):
            validated_item = ensure_dict_response(
                item,
                f"{context} item[{i}]",
                status_code,
            )
            try:
                validated_fills.append(BackpackRawFill.model_validate(validated_item))
            except ValidationError as e:
                raise BackpackTradingResponseHandler._handle_validation_error(
                    e,
                    f"single fill item in {context}",
                    validated_item,
                ) from e
        return validated_fills

    @staticmethod
    def handle_get_order_status_response(
        raw_response_content: RawJsonResponse,
        identifier: str,
        status_code: int,
    ) -> BackpackRawOrder:
        """Validate the raw response for the Get Order Status endpoint.

        Returns:
            Validated BackpackRawOrder model with current order status.
        """
        context = f"order status (id={identifier})"
        validated_data = ensure_dict_response(
            raw_response_content,
            context,
            status_code,
        )
        try:
            return BackpackRawOrder.model_validate(validated_data)
        except ValidationError as e:
            raise BackpackTradingResponseHandler._handle_validation_error(
                e,
                context,
                validated_data,
            ) from e

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int,
    ) -> list[BackpackRawOrder]:
        """Validate the raw response for the Cancel All Orders endpoint.

        (DELETE /api/v1/orders/cancelAll).
        Expects a list of successfully cancelled orders.

        Returns:
            List of validated BackpackRawOrder models for cancelled orders.
        """
        context = f"cancel all orders ({symbol or 'all'})"
        validated_list = ensure_list_response(
            raw_response_content,
            context,
            status_code,
        )

        validated_orders: list[BackpackRawOrder] = []
        for i, item in enumerate(validated_list):
            # Skip non-dict items with logging, but ensure dict before validation
            try:
                validated_item = ensure_dict_response(
                    item,
                    f"{context} item[{i}]",
                    status_code,
                )
            except APIError:
                logger.warning(
                    "backpack_non_dict_item_skip",
                    context=context,
                    item=item,
                    raw_response_content=raw_response_content,
                    module_name=__name__,
                    message="Skipping non-dict item in response list",
                )
                continue  # Skip non-dict items, but don't fail the whole batch

            try:
                validated_orders.append(BackpackRawOrder.model_validate(validated_item))
            except ValidationError as e:
                # Log the specific item that failed validation but continue processing others
                # to return successfully validated items if any.
                # Or, re-raise if strictness is required. For cancelAll, it might be better
                # to return what was successfully parsed as cancelled.
                logger.exception(
                    "backpack_order_validation_failed",
                    context=context,
                    validation_error=str(e),
                    validated_item=validated_item,
                    module_name=__name__,
                    message="Pydantic validation failed for single order item",
                )
                # Optionally, re-raise if any single item failing should invalidate
                # the whole response:
                # raise BackpackTradingResponseHandler._handle_validation_error(
                # ) from e
                # For now, we'll be lenient and collect valid ones.
                # Consider if this behavior is desired or if it should be stricter.
        return validated_orders
