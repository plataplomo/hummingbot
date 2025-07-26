"""Backpack Order Placement Service.

This service handles all order placement operations for the Backpack exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Single order placement
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
from cyberdelta.apis.exceptions import (
    InvalidEnumValueError,
    InvalidParameterTypeError,
    MissingRequiredParameterError,
)
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs
from cyberdelta.apis.utils.response_validation import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.enums import (
    OrderType,
    TimeInForce,
)
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackOrderPlacementService:
    """Focused service for Backpack order placement operations.

    Handles validation, processing, and transformation of order placement requests
    with comprehensive error handling and status processing.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        # Optional dependency injection for mapper
        mapper: OrderMapperProtocol | None = None,
    ) -> None:
        """Initialize the order placement service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            mapper: Optional order mapper instance for dependency injection
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._mapper = mapper or BackpackOrderMapper()

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order on the Backpack exchange.

        Validates the order parameters, submits the order via the API, and returns
        the created order with its assigned ID and current status.

        Args:
            args: PlaceOrderArgs containing order details (symbol, side, type, etc.)

        Returns:
            Order: The created order object with exchange-assigned ID and status

        Raises:
            APIError: If order placement fails due to API errors
            ValueError: If order parameters are invalid for Backpack exchange
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_order"

        # Validate order parameters
        self._validate_place_order_params(args, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            return await self._execute_place_order_request(args, current_method)
        except APIError:
            raise
        except TransformationError as e_transform:
            raise self._create_place_order_api_error(
                e_transform,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_place_order_api_error(
                e_val,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._create_place_order_api_error(
                e_service_logic,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Service internal logic error.",
                APIErrorCode.UNKNOWN,
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._create_place_order_api_error(
                e_unexpected,
                current_method,
                args.symbol,
                status_code,
                raw_response_content,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

    async def _execute_place_order_request(
        self,
        args: PlaceOrderArgs,
        current_method: str,
    ) -> Order:
        """Execute the order placement request and process the response.

        Args:
            args: Validated order placement arguments
            current_method: Name of calling method for error context

        Returns:
            Order: The created order object

        Raises:
            APIError: If the request fails or response is invalid
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for placing orders",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        logger.info(
            "placing_order",
            exchange=self._exchange_name,
            method=current_method,
            symbol=args.symbol,
            side=args.side.value,
            order_type=args.order_type.value,
            message="Placing order on exchange",
        )

        # Build request parameters using the OrderExecution domain object from args
        request_params = self._request_builder.build_place_order_payload(
            symbol=args.symbol,
            order_type=args.order_type,
            order_side=args.side,
            quantity=args.quantity,
            price=args.price,
            time_in_force=args.time_in_force,
            client_order_id=args.client_order_id,
            execution=args.execution,
            stop_price=args.stop_price,
        )

        # Execute API request
        raw_response, status_code, _headers = await self._http_client_requester(
            method="POST",
            endpoint="/api/v1/order",
            data=request_params,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        # Validate response format
        validated_response = ensure_dict_response(raw_response, "place order", status_code)

        # Handle response through response handler
        raw_order = self._response_handler.handle_place_order_response(
            validated_response,
            status_code,
        )

        # Transform to internal model
        order = self._mapper.transform_raw_order_to_internal(raw_order)

        logger.info(
            "order_placed",
            exchange=self._exchange_name,
            method=current_method,
            symbol=args.symbol,
            order_id=order.exchange_order_id,
            status=order.status.value,
            message="Successfully placed order",
        )

        return order

    def _validate_place_order_params(self, args: PlaceOrderArgs, current_method: str) -> None:
        """Validate order placement parameters.

        Args:
            args: Order placement arguments to validate
            current_method: Name of calling method for error context

        Raises:
            ValueError: If validation fails
        """
        # Validate symbol (Pydantic ensures it's a string, just check if empty)
        if not args.symbol:
            raise InvalidParameterTypeError(
                parameter_name="symbol",
                expected_type="non-empty string",
                actual_type="empty string",
                value=args.symbol,
            )

        # Note: side, order_type, and quantity are required fields in PlaceOrderArgs
        # and validated by Pydantic, so they cannot be None here
        if args.quantity <= 0:
            raise InvalidParameterTypeError(
                parameter_name="quantity",
                expected_type="positive decimal",
                actual_type="non-positive decimal",
                value=args.quantity,
            )

        # Validate price for limit orders
        if args.order_type == OrderType.LIMIT:
            if args.price is None:
                raise MissingRequiredParameterError(
                    parameter_name="price",
                    operation="limit order placement",
                )
            if args.price <= 0:
                raise InvalidParameterTypeError(
                    parameter_name="price",
                    expected_type="positive decimal",
                    actual_type="non-positive decimal",
                    value=args.price,
                )

        # Validate time in force (required field in PlaceOrderArgs, so never None)
        if args.time_in_force not in {TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK}:
            raise InvalidEnumValueError(
                parameter_name="time_in_force",
                value=args.time_in_force.value,
                valid_values=["GTC", "IOC", "FOK"],
                enum_type="TimeInForce",
            )

        logger.debug(
            "order_params_validated",
            exchange=self._exchange_name,
            method=current_method,
            symbol=args.symbol,
            message="Order parameters validated successfully",
        )

    def _create_place_order_api_error(
        self,
        error: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create standardized APIError for place order operations.

        Args:
            error: The original exception
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
            "place_order_error",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol,
            error=str(error),
            status_code=status_code,
            message=f"Place order failed: {message}",
        )

        return APIError(
            code=error_code.value,
            message=message,
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
