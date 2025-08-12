"""Backpack Batch Order Service.

This service handles all batch order operations for the Backpack exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Cancel all orders operations
- Batch validation and processing
- Response handling and transformation
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.backpack.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.backpack.protocols.mapper_protocols import OrderMapperProtocol
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import MissingRequiredFieldError
from cyberdelta.apis.utils.response_validation import ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import CancelOrderResultStatus
from cyberdelta.enums import ExchangeName
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse, is_dict_response


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackBatchOrderService:
    """Focused service for Backpack batch order operations.

    Handles validation, processing, and transformation of batch order requests
    with comprehensive error handling and status processing.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: ExchangeName = ExchangeName.BACKPACK,
        mapper: OrderMapperProtocol | None = None,
    ) -> None:
        """Initialize the batch order service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            mapper: Order mapper instance for transformations (optional)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._mapper = mapper or BackpackOrderMapper()

    async def cancel_all_orders(self, symbol: Symbol | None = None) -> list[CancelOrderResult]:
        """Cancel all orders, optionally filtered by symbol.

        Cancels all open orders for the account. If a symbol is provided,
        only orders for that symbol will be cancelled.

        Args:
            symbol: Optional symbol to filter orders by

        Returns:
            list[CancelOrderResult]: List of cancellation results for each order

        Raises:
            APIError: If cancellation fails due to API errors
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_all_orders"

        self._validate_cancel_all_orders_params(symbol, current_method)

        try:
            return await self._execute_cancel_all_orders_request(symbol, current_method)
        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            raise self._create_cancel_all_orders_api_error(
                e_transform,
                current_method,
                symbol,
                0,
                None,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_cancel_all_orders_api_error(
                e_val,
                current_method,
                symbol,
                0,
                None,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._create_cancel_all_orders_api_error(
                e_service_logic,
                current_method,
                symbol,
                0,
                None,
                "Service internal logic error.",
                APIErrorCode.UNKNOWN,
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._create_cancel_all_orders_api_error(
                e_unexpected,
                current_method,
                symbol,
                0,
                None,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

    async def _execute_cancel_all_orders_request(
        self,
        symbol: Symbol | None,
        current_method: str,
    ) -> list[CancelOrderResult]:
        """Execute the cancel all orders API request and process the response.

        Args:
            symbol: Symbol to filter orders by (validated)
            current_method: Name of calling method for error context

        Returns:
            list[CancelOrderResult]: List of cancellation results

        Raises:
            APIError: If the request fails or response is invalid
            MissingRequiredFieldError: If authenticator is not available
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for cancelling orders",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # DEFENSIVE CHECK: Ensure symbol is not None before proceeding
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange=ExchangeName.BACKPACK,
                operation="cancel all orders",
            )

        logger.info(
            "cancelling_all_orders",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            message="Cancelling all orders on exchange",
        )

        endpoint = "/api/v1/orders"
        # Backpack's Cancel All Orders: DELETE /api/v1/orders with symbol query parameter
        payload = self._request_builder.build_cancel_all_orders_payload(symbol=symbol)

        raw_data, status_code, _ = await self._http_client_requester(
            method="DELETE",
            endpoint=endpoint,
            data=payload,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        result = self._process_cancel_all_orders_response(raw_data, status_code, symbol)

        logger.info(
            "all_orders_cancelled",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            cancelled_count=len(result),
            message="Successfully cancelled all orders",
        )

        return result

    def _validate_cancel_all_orders_params(
        self,
        symbol: Symbol | None,
        current_method: str,
    ) -> None:
        """Validate cancel all orders parameters for Backpack exchange.

        Args:
            symbol: Symbol to validate
            current_method: Name of calling method for error context

        Raises:
            MissingRequiredFieldError: If validation fails
        """
        # Business Logic Pre-Validation (moved from RequestBuilder)
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange=ExchangeName.BACKPACK,
                operation="cancel all orders",
            )

        # Symbol objects don't have .strip() method - check if value is empty
        if not symbol.value.strip():
            raise MissingRequiredFieldError(
                field="symbol",
                exchange=ExchangeName.BACKPACK,
                operation="cancel all orders",
                reason="cannot be empty or whitespace only",
            )

        logger.debug(
            "cancel_all_orders_params_validated",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            message="Cancel all orders parameters validated successfully",
        )

    def _process_cancel_all_orders_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        symbol: Symbol,
    ) -> list[CancelOrderResult]:
        """Process the cancel all orders API response.

        Args:
            raw_data: Raw response data from the API
            status_code: HTTP status code
            symbol: The trading symbol that was filtered

        Returns:
            list[CancelOrderResult]: List of cancellation results
        """
        try:
            validated_data = ensure_list_response(
                raw_data,
                f"cancel all orders for {symbol.value}",
                status_code,
            )
        except APIError:
            return self._handle_invalid_cancel_all_response(raw_data, status_code, symbol)

        # Use the response handler to get validated BackpackRawOrderResponse objects
        try:
            raw_orders: list[BackpackRawOrderResponse] = (
                self._response_handler.handle_cancel_all_orders_response(
                    validated_data,
                    symbol,
                    status_code,
                )
            )
        except APIError:
            # If response handler fails, return empty list or re-raise depending on requirements
            logger.warning(
                "parse_cancel_all_failed",
                exchange=self._exchange_name,
                symbol=symbol,
                message="Failed to parse cancel all orders response",
            )
            return []

        # Transform raw orders to CancelOrderResult objects
        # All orders returned by cancel all should have been cancelled
        results: list[CancelOrderResult] = [
            CancelOrderResult(
                order_id=raw_order.id,
                client_order_id=str(raw_order.clientId) if raw_order.clientId else None,
                symbol=exchanges.backpack(
                    value=raw_order.symbol,
                ),
                success=True,  # If returned by cancel all, it was successfully cancelled
                message="Successfully cancelled.",
                status=CancelOrderResultStatus.SUCCESS,
            )
            for raw_order in raw_orders
        ]

        logger.info(
            "orders_cancelled",
            exchange=self._exchange_name,
            count=len(results),
            symbol=symbol,
            message="Successfully cancelled orders",
        )
        return results

    def _handle_invalid_cancel_all_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        symbol: Symbol,
    ) -> list[CancelOrderResult]:
        """Handle invalid response from cancel all orders API.

        Args:
            raw_data: Raw response data
            status_code: HTTP status code
            symbol: Trading symbol

        Returns:
            list[CancelOrderResult]: Empty list or error handling

        Raises:
            APIError: If explicit error in response
        """
        error_message = (
            f"Cancel all orders for {symbol.value if symbol else 'all'} returned invalid data "
            f"or no content (status: {status_code})"
        )
        logger.error(
            "cancel_all_orders_invalid_response",
            action="cancel_all_orders",
            exchange=self._exchange_name,
            error_message=error_message,
            raw_data=raw_data,
            message=f"[{self._exchange_name}] {error_message}. Raw: {raw_data}",
        )

        # If response is not a list, it might be an error structure or unexpected.
        # We can't confirm any cancellations.
        if is_dict_response(raw_data) and raw_data.get("error"):  # Check for explicit error
            # raw_data is now typed as dict[str, Any], so .get() is type-safe
            error_info = raw_data.get("error", {})
            raise APIError(
                error_info.get("message", "Failed to cancel all orders due to API error response."),
                APIErrorCode.UNKNOWN.value,  # Using UNKNOWN as OPERATION_FAILED is not available
                http_status=status_code,
                exchange_message=str(raw_data),
            )

        logger.warning(
            "cancel_all_invalid_response",
            exchange=self._exchange_name,
            error_message=error_message,
            message="Invalid response, assuming no orders were cancelled",
        )
        # Create a generic failure result if no orders could be confirmed cancelled.
        # This assumes that if there were orders and they failed to cancel, an error
        # would be raised.
        # If there were no orders, an empty list response is typical and correct.
        # If raw_data is None, it's ambiguous.
        return []  # Or a list with a single generic failure if that's preferred

    def _create_cancel_all_orders_api_error(
        self,
        original_exception: Exception,
        current_method: str,
        symbol: Symbol | None,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for cancel all orders operations.

        Args:
            original_exception: The original exception
            current_method: Name of the method where error occurred
            symbol: Trading symbol for context
            status_code: HTTP status code if available
            raw_response_content: Raw response content if available
            message: Error message
            error_code: API error code

        Returns:
            APIError: Standardized error object
        """
        logger.error(
            "cancel_all_orders_error",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            error=str(original_exception),
            status_code=status_code,
            message=f"Cancel all orders failed: {message}",
        )

        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
