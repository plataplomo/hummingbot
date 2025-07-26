"""Backpack Order Query Service.

This service handles all order query operations for the Backpack exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Single order status queries
- Open orders retrieval
- Order validation and processing
- Response handling and transformation
- Comprehensive error handling
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from http import HTTPStatus

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
from cyberdelta.apis.exceptions.trading import OrderNotFoundError
from cyberdelta.apis.models.service_args.trading import GetOrderArgs
from cyberdelta.apis.utils.response_validation import ensure_dict_response, ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackOrderQueryService:
    """Focused service for Backpack order query operations.

    Handles validation, processing, and transformation of order query requests
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
        """Initialize the order query service.

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

    async def get_order(self, args: GetOrderArgs) -> Order:
        """Get a specific order by ID from the Backpack exchange.

        Retrieves detailed information about a specific order using either
        the exchange order ID or client order ID.

        Args:
            args: GetOrderArgs containing order identifiers and symbol

        Returns:
            Order: The order object with current status and details

        Raises:
            APIError: If the request fails or order processing fails
            OrderNotFoundError: If the order doesn't exist
            ValueError: If order parameters are invalid
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order"

        # Validate get order parameters
        self._validate_get_order_params(args, current_method)

        identifier = self._determine_order_identifier(args)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            order = await self._execute_get_order_request(args, current_method)
        except APIError as e:
            order = self._handle_get_order_api_error(e, identifier, args.symbol)
        except TransformationError as e_transform:
            raise self._create_get_order_api_error(
                e_transform,
                current_method,
                identifier,
                args.symbol,
                status_code,
                raw_response_content,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_get_order_api_error(
                e_val,
                current_method,
                identifier,
                args.symbol,
                status_code,
                raw_response_content,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            order = self._handle_get_order_service_logic_error(
                e_service_logic,
                current_method,
                identifier,
                args.symbol,
                status_code,
                raw_response_content,
            )
        except Exception as e_unexpected:
            raise self._create_get_order_api_error(
                e_unexpected,
                current_method,
                identifier,
                args.symbol,
                0,
                None,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

        # Validate order exists
        self._validate_order_exists(order, args)

        # After validation, order cannot be None, but we need explicit check for mypy
        if order is None:
            # This should never happen after _validate_order_exists
            raise OrderNotFoundError(
                order_id=identifier,
                symbol=args.symbol,
                exchange=self._exchange_name,
            )
        return order

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all open orders, optionally filtered by symbol.

        Retrieves all currently open orders for the account. Orders can be
        optionally filtered by trading symbol.

        Args:
            symbol: Optional symbol to filter orders by

        Returns:
            list[Order]: List of open orders

        Raises:
            APIError: If the request fails or order processing fails
            ValueError: If symbol parameter is invalid
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        if symbol is not None and not symbol:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange="Backpack",
                operation="get all open orders",
                reason="must be a non-empty string when provided",
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            return await self._execute_get_open_orders_request(symbol, current_method)
        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transformation_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_transform),
                message="Failed to transform exchange data for open orders",
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
                symbol=symbol or "all",
                error=str(e_val),
                message="Internal data validation failed for open orders",
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
            # Input parameter validation errors should propagate as ValueError
            # Service logic errors should be wrapped as APIError
            error_msg = str(e_service_logic)
            if current_method in error_msg and "symbol" in error_msg:
                # This is likely from our input parameter validation - re-raise as is
                raise
            # This is from service internal logic - wrap as APIError
            logger.exception(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_service_logic),
                message="Service internal logic error for open orders",
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
                symbol=symbol or "all",
                error=str(e_unexpected),
                message="Unexpected service failure for open orders",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def _execute_get_order_request(
        self,
        args: GetOrderArgs,
        current_method: str,
    ) -> Order | None:
        """Execute the get order API request and process the response.

        Args:
            args: Validated order query arguments
            current_method: Name of calling method for error context

        Returns:
            Order | None: The order object if found, None if not found

        Raises:
            APIError: If the request fails or response is invalid
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for querying orders",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        identifier = self._determine_order_identifier(args)

        # DEFENSIVE CHECK: Ensure symbol is not None before passing to request builder
        if args.symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="get order"
            )

        logger.info(
            "querying_order",
            exchange=self._exchange_name,
            method=current_method,
            order_id=identifier,
            symbol=args.symbol,
            message="Querying order on exchange",
        )

        endpoint = f"/api/v1/order/{identifier}"
        params = self._request_builder.build_get_order_params(
            symbol=args.symbol,  # Now guaranteed to be str, not str | None
        )  # Symbol is a query param

        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint,
            params=params.model_dump(by_alias=True, exclude_none=True),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        order = self._process_get_order_response(raw_data, status_code, identifier, args.symbol)

        if order:
            logger.info(
                "order_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                order_id=identifier,
                symbol=args.symbol,
                status=order.status.value,
                message="Successfully retrieved order",
            )

        return order

    async def _execute_get_open_orders_request(
        self,
        symbol: str | None,
        current_method: str,
    ) -> list[Order]:
        """Execute the get open orders API request and process the response.

        Args:
            symbol: Optional symbol to filter by
            current_method: Name of calling method for error context

        Returns:
            list[Order]: List of open orders

        Raises:
            APIError: If the request fails or response is invalid
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for querying orders",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        logger.info(
            "querying_open_orders",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol or "all",
            message="Querying open orders on exchange",
        )

        # Core operational logic
        endpoint = "/api/v1/orders"
        params = self._request_builder.build_get_open_orders_params(symbol=symbol)

        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint,
            params=params.model_dump(by_alias=True, exclude_none=True),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        if raw_data is not None:
            str(raw_data)

        validated_data = ensure_list_response(
            raw_data,
            f"get open orders for {symbol or 'all'}",
            status_code,
        )

        raw_orders_list: list[BackpackRawOrderResponse] = (
            self._response_handler.handle_get_open_orders_response(
                validated_data,
                symbol,
                status_code,
            )
        )

        orders = [self._mapper.transform_raw_order_to_internal(ro) for ro in raw_orders_list]

        logger.info(
            "open_orders_retrieved",
            exchange=self._exchange_name,
            method=current_method,
            symbol=symbol or "all",
            count=len(orders),
            message="Successfully retrieved open orders",
        )

        return orders

    def _validate_get_order_params(self, args: GetOrderArgs, current_method: str) -> None:
        """Validate get order parameters for Backpack exchange.

        Args:
            args: Order query arguments to validate
            current_method: Name of calling method for error context

        Raises:
            MissingRequiredFieldError: If validation fails
        """
        # Backpack requires symbol for its GET /order/{id} endpoint
        if args.symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="get order"
            )
        if not args.symbol:
            raise MissingRequiredFieldError(
                field="symbol",
                exchange="Backpack",
                operation="get order",
                reason="must be a non-empty string",
            )

        logger.debug(
            "get_order_params_validated",
            exchange=self._exchange_name,
            method=current_method,
            symbol=args.symbol,
            message="Get order parameters validated successfully",
        )

    def _validate_order_exists(self, order: Order | None, args: GetOrderArgs) -> None:
        """Validate that an order was found, raise OrderNotFoundError if not.

        Args:
            order: The order object (may be None)
            args: Original query arguments

        Raises:
            OrderNotFoundError: If order is None
        """
        if order is None:
            identifier = (
                args.client_order_id
                if not args.order_id and args.client_order_id
                else args.order_id
            )
            raise OrderNotFoundError(
                order_id=identifier, symbol=args.symbol, exchange=self._exchange_name
            )

    def _determine_order_identifier(self, args: GetOrderArgs) -> str:
        """Determine which identifier to use for the order lookup.

        Args:
            args: Order query arguments

        Returns:
            str: The identifier to use

        Raises:
            MissingRequiredFieldError: If no valid identifier provided
        """
        if not args.order_id and args.client_order_id:
            return args.client_order_id
        if not args.order_id and not args.client_order_id:
            raise MissingRequiredFieldError(
                field="order_id or client_order_id",
                exchange="Backpack",
                operation="get order",
                reason="at least one identifier must be provided",
            )
        return args.order_id

    def _process_get_order_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        identifier: str,
        symbol: str | None,
    ) -> Order | None:
        """Process the get order API response.

        Args:
            raw_data: Raw response data from the API
            status_code: HTTP status code
            identifier: The order identifier that was queried
            symbol: The trading symbol

        Returns:
            Order | None: The order object if found, None if not found

        Raises:
            APIError: If response processing fails
        """
        if status_code == HTTPStatus.NOT_FOUND.value:  # Order not found
            logger.info(
                "order_not_found",
                exchange=self._exchange_name,
                order_id=identifier,
                symbol=symbol,
                message="Order not found",
            )
            return None

        validated_data = ensure_dict_response(
            raw_data,
            f"get order {identifier} ({symbol})",
            status_code,
        )

        raw_order_model: BackpackRawOrderResponse = (
            self._response_handler.handle_get_order_status_response(
                validated_data,
                identifier,
                status_code,
            )
        )
        return self._mapper.transform_raw_order_to_internal(raw_order_model)

    def _handle_get_order_api_error(
        self,
        e: APIError,
        identifier: str,
        symbol: str | None,
    ) -> Order | None:
        """Handle APIError exceptions for get_order method.

        Args:
            e: The APIError exception
            identifier: Order identifier
            symbol: Trading symbol

        Returns:
            Order | None: None if order not found, otherwise re-raises

        Raises:
            APIError: If not an order not found error
        """
        # Allow ORDER_NOT_FOUND from handler to propagate if it maps it
        if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
            logger.info(
                "order_not_found_handler",
                exchange=self._exchange_name,
                order_id=identifier,
                symbol=symbol,
                message="Order not found via handler mapping",
            )
            return None
        raise e

    def _handle_get_order_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        current_method: str,
        identifier: str,
        symbol: str | None,
        status_code: int,
        raw_response_content: str | None,
    ) -> Order | None:
        """Handle service logic errors for get_order method.

        Args:
            e_service_logic: The service logic error
            current_method: Name of calling method
            identifier: Order identifier
            symbol: Trading symbol
            status_code: HTTP status code
            raw_response_content: Raw response content

        Returns:
            Order | None: None or raises APIError

        Raises:
            ValueError: If validation error
            APIError: If service logic error
        """
        # Check if this is from our own input parameter validation
        error_msg = str(e_service_logic)
        if current_method in error_msg and any(
            param in error_msg for param in ["order_id", "client_order_id", "symbol"]
        ):
            # This is likely from our input parameter validation - re-raise as is
            raise e_service_logic
        # This is from service internal logic - wrap as APIError
        raise self._create_get_order_api_error(
            e_service_logic,
            current_method,
            identifier,
            symbol,
            status_code,
            raw_response_content,
            "Service internal logic error.",
            APIErrorCode.UNKNOWN,
        ) from e_service_logic

    def _create_get_order_api_error(
        self,
        original_exception: Exception,
        current_method: str,
        identifier: str,
        symbol: str | None,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for get order operations.

        Args:
            original_exception: The original exception
            current_method: Name of the method where error occurred
            identifier: Order identifier for context
            symbol: Trading symbol for context
            status_code: HTTP status code if available
            raw_response_content: Raw response content if available
            message: Error message
            error_code: API error code

        Returns:
            APIError: Standardized error object
        """
        logger.error(
            "get_order_error",
            exchange=self._exchange_name,
            method=current_method,
            order_id=identifier,
            symbol=symbol,
            error=str(original_exception),
            status_code=status_code,
            message=f"Get order failed: {message}",
        )

        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )
