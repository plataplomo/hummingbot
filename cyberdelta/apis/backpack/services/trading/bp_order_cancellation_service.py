"""Backpack Order Cancellation Service.

This service handles all order cancellation operations for the Backpack exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Single order cancellation
- Order validation and processing
- Response handling and transformation
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackOrderMapper
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
from cyberdelta.apis.models.service_args.trading import CancelOrderArgs
from cyberdelta.apis.utils.response_validation import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackOrderCancellationService:
    """Focused service for Backpack order cancellation operations.

    Handles validation, processing, and transformation of order cancellation requests
    with comprehensive error handling and status processing.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        mapper: OrderMapperProtocol | None = None,
    ) -> None:
        """Initialize the order cancellation service.

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

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel an existing order on the Backpack exchange.

        Attempts to cancel the specified order by its ID. The order must be
        in a cancellable state (not already filled or cancelled).

        Args:
            args: CancelOrderArgs containing the order ID and optional symbol

        Returns:
            CancelOrderResult: Detailed cancellation result information

        Raises:
            APIError: If cancellation fails due to API errors or order not found
            ValueError: If the order ID is invalid
            TypeError: If there are type-related errors during processing
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_order"

        # Validate cancel order parameters
        self._validate_cancel_order_params(args, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            return await self._execute_cancel_order_request(args, current_method)
        except APIError:
            raise
        except TransformationError as e_transform:
            raise self._create_cancel_order_api_error(
                e_transform,
                current_method,
                args.order_id,
                args.symbol.value if args.symbol else None,  # Use domain object's value
                status_code,
                raw_response_content,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_cancel_order_api_error(
                e_val,
                current_method,
                args.order_id,
                args.symbol.value if args.symbol else None,  # Use domain object's value
                status_code,
                raw_response_content,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            error_msg = str(e_service_logic)
            if current_method in error_msg and any(
                param in error_msg for param in ["order_id", "symbol"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            # This is from service internal logic - wrap as APIError
            raise self._create_cancel_order_api_error(
                e_service_logic,
                current_method,
                args.order_id,
                args.symbol.value if args.symbol else None,  # Use domain object's value
                status_code,
                raw_response_content,
                "Service internal logic error.",
                APIErrorCode.UNKNOWN,
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._create_cancel_order_api_error(
                e_unexpected,
                current_method,
                args.order_id,
                args.symbol.value if args.symbol else None,  # Use domain object's value
                status_code,
                raw_response_content,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

    async def _execute_cancel_order_request(
        self,
        args: CancelOrderArgs,
        current_method: str,
    ) -> CancelOrderResult:
        """Execute the cancel order API request and process the response.

        Args:
            args: Validated cancellation arguments
            current_method: Name of calling method for error context

        Returns:
            CancelOrderResult: The cancellation result

        Raises:
            APIError: If the request fails or response is invalid
            MissingRequiredFieldError: If required fields are missing
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for cancelling orders",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # DEFENSIVE CHECK: Ensure symbol is not None before passing to request builder
        if args.symbol is None:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange="Backpack",
                operation="cancel order",
            )

        logger.info(
            "cancelling_order",
            exchange=self._exchange_name,
            method=current_method,
            order_id=args.order_id,
            symbol=str(args.symbol),  # String conversion at HTTP boundary
            message="Cancelling order on exchange",
        )

        # Build request parameters
        endpoint = "/api/v1/order"
        payload = self._request_builder.build_cancel_order_payload(
            symbol=args.symbol,  # Pass Symbol object directly
            order_id=args.order_id,
        )

        # Execute API request
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

        # Process the response
        result = self._process_cancel_order_response(
            raw_data,
            status_code,
            args.order_id,
            args.symbol,
        )

        logger.info(
            "order_cancelled",
            exchange=self._exchange_name,
            method=current_method,
            order_id=args.order_id,
            symbol=str(args.symbol),  # String conversion at HTTP boundary
            status=result.status.value,
            message="Successfully cancelled order",
        )

        return result

    def _validate_cancel_order_params(self, args: CancelOrderArgs, current_method: str) -> None:
        """Validate cancel order parameters for Backpack exchange.

        Args:
            args: Cancel order arguments to validate
            current_method: Name of calling method for error context

        Raises:
            MissingRequiredFieldError: If validation fails
        """
        # Extract validated fields from Pydantic model
        order_id = args.order_id
        # Convert Symbol to string for HTTP request
        symbol = str(args.symbol) if args.symbol else None  # String conversion at boundary

        # For Backpack, symbol is required
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange="Backpack",
                operation="cancel order",
            )

        # Business Logic Pre-Validation (moved from RequestBuilder)
        # For Backpack, either order_id or client_order_id must be provided, but not both
        # Since this method only accepts order_id, we validate it's provided and non-empty
        if not order_id.strip():
            raise MissingRequiredFieldError(
                field="order_id",
                exchange="Backpack",
                operation="cancel order",
                reason="cannot be empty or whitespace only",
            )

        logger.debug(
            "cancel_order_params_validated",
            exchange=self._exchange_name,
            method=current_method,
            order_id=order_id,
            symbol=symbol,
            message="Cancel order parameters validated successfully",
        )

    def _process_cancel_order_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        order_id: str,
        symbol: Symbol | None,
    ) -> CancelOrderResult:
        """Process the cancel order API response.

        Args:
            raw_data: Raw response data from the API
            status_code: HTTP status code
            order_id: The order ID that was cancelled
            symbol: The trading symbol

        Returns:
            CancelOrderResult: Processed cancellation result

        Raises:
            MissingRequiredFieldError: If required fields are missing during processing
        """
        # Backpack's cancel order returns the cancelled order details or an error.
        # The response handler needs to determine success.
        validated_data = ensure_dict_response(
            raw_data,
            f"cancel order {order_id} ({symbol})",
            status_code,
        )

        # DEFENSIVE CHECK: Ensure symbol is not None before passing to response handler
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange="Backpack",
                operation="cancel order response processing",
            )

        return self._response_handler.handle_cancel_order_response(
            raw_response_content=validated_data,
            order_id=order_id,
            symbol=symbol,  # Now guaranteed to be str, not str | None
        )

    def _create_cancel_order_api_error(
        self,
        original_exception: Exception,
        current_method: str,
        order_id: str,
        symbol: str | None,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for cancel order operations.

        Args:
            original_exception: The original exception
            current_method: Name of the method where error occurred
            order_id: Order ID for context
            symbol: Trading symbol for context
            status_code: HTTP status code if available
            raw_response_content: Raw response content if available
            message: Error message
            error_code: API error code

        Returns:
            APIError: Standardized error object
        """
        logger.error(
            "cancel_order_error",
            exchange=self._exchange_name,
            method=current_method,
            order_id=order_id,
            symbol=symbol,
            error=str(original_exception),
            status_code=status_code,
            message=f"Cancel order failed: {message}",
        )

        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
