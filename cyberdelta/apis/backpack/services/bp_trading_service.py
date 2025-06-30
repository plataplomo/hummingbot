"""CyberDeltaEngine: Backpack Trading Service.

-------------------------------------------

This service encapsulates the logic for trading operations on the Backpack Exchange.
It uses the HttpClient (via a requester callable), BackpackRequestBuilder,
BackpackResponseHandler, and BackpackTradingDataMapper to interact with the API
and returns Internal Domain Models.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from http import HTTPStatus

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.exceptions import (
    MissingRequiredFieldError,
)
from cyberdelta.exceptions.service_validation import (
    IntegerConversionError,
    OrderParameterError,
)
from cyberdelta.exceptions.trading import OrderNotFoundError
from cyberdelta.utils.typing import ParsedJsonResponse, is_dict_response


logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackTradingService:
    """Service class for Backpack trading operations. Returns Internal Domain Models."""

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        authenticator: IAuthenticator | None,  # Added authenticator
        exchange_name: str,
        mapper: BackpackTradingDataMapper | None = None,
    ) -> None:
        """Initialize the BackpackTradingService.

        Args:
            http_client_requester: A callable for making API requests.
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            authenticator: An instance of IAuthenticator for signed requests.
            exchange_name: The name of the exchange.
            mapper: Optional mapper instance for dependency injection.

        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._trading_mapper = (
            mapper or BackpackTradingDataMapper()
        )  # Instantiate or use static methods

    @staticmethod
    def _ensure_order_not_none(order: object) -> None:
        """Ensure order is not None after validation.

        Args:
            order: Order object to validate

        Raises:
            RuntimeError: If order is None
        """
        if order is None:
            msg = "Order should not be None after validation"
            raise RuntimeError(msg)

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
            # Check if this is from our own input parameter validation
            error_msg = str(e_service_logic)
            if current_method in error_msg and any(
                param in error_msg for param in ["client_order_id", "reduce_only"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            # This is from service internal logic - wrap as APIError
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

    def _validate_place_order_params(self, args: PlaceOrderArgs, current_method: str) -> None:
        """Validate order parameters for Backpack exchange."""
        # Validate order type is supported by Backpack
        supported_order_types = [
            OrderType.LIMIT,
            OrderType.MARKET,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_MARKET,
            OrderType.TAKE_PROFIT_LIMIT,
        ]
        if args.order_type not in supported_order_types:
            raise OrderParameterError(
                parameter="order_type",
                value=args.order_type.value,
                valid_values=[ot.value for ot in supported_order_types],
                exchange="Backpack",
            )

        # Validate time in force for limit orders
        if args.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}:
            supported_tif = [TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK]
            if args.time_in_force not in supported_tif:
                supported_values = [tif.value for tif in supported_tif]
                raise OrderParameterError(
                    parameter="time_in_force",
                    value=args.time_in_force.value,
                    valid_values=supported_values,
                    exchange="Backpack",
                    context="limit orders",
                )

        # Validate client_order_id can be converted to int if provided
        if args.client_order_id:
            try:
                int(args.client_order_id)
            except ValueError as e:
                raise IntegerConversionError(
                    field="client_order_id", value=args.client_order_id, original_exception=e
                ) from e

        # Validate reduce_only is not supported (log warning)
        if args.reduce_only:
            logger.warning(
                "reduce_only_not_supported: 'reduce_only' parameter is not supported for "
                "place_order and will be ignored",
                exchange=self._exchange_name,
            )

    async def _execute_place_order_request(
        self,
        args: PlaceOrderArgs,
        current_method: str,
    ) -> Order:
        """Execute the place order API request and process the response."""
        endpoint = "/api/v1/order"

        # Determine the correct trigger price field based on order type
        # Backpack uses standard trigger mechanism (triggerPrice + triggerQuantity)
        # Pass stop_price as trigger_price if provided, regardless of order type
        # The exchange will validate whether it's appropriate for the order type
        trigger_price = args.stop_price

        payload = self._request_builder.build_place_order_payload(
            symbol=args.symbol,
            side=args.side,
            order_type=args.order_type,
            quantity=args.quantity,
            time_in_force=args.time_in_force,
            price=args.price,
            client_order_id=args.client_order_id,
            post_only=args.post_only,
            trigger_price=trigger_price,
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint,
            data=payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        return self._process_place_order_response(
            raw_data,
            status_code,
            args.symbol,
            args.order_type,
        )

    def _process_place_order_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        symbol: str,
        original_order_type: OrderType | None = None,
    ) -> Order:
        """Process the place order API response and transform to internal model."""
        if raw_data is not None:
            str(raw_data)

        validated_data = ensure_dict_response(raw_data, f"place order for {symbol}", status_code)

        raw_order_model: BackpackRawOrder = self._response_handler.handle_place_order_response(
            validated_data,
            status_code,
        )
        internal_order = self._trading_mapper.transform_raw_order_to_internal(raw_order_model)

        # Preserve original order type intent for take profit orders
        # Backpack represents take profit orders the same as stop orders in API responses
        if original_order_type in {
            OrderType.TAKE_PROFIT_MARKET,
            OrderType.TAKE_PROFIT_LIMIT,
        } and internal_order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}:
            internal_order.order_type = original_order_type

        return internal_order

    def _create_place_order_api_error(
        self,
        original_exception: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for place order operations."""
        logger.error(
            "place_order_error: Failed to place order",
            exchange=self._exchange_name,
            method=current_method,
            error_message=message,
            symbol=symbol,
            original_exception=str(original_exception),
        )
        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

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
                args.symbol,
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
                args.symbol,
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
                args.symbol,
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
                args.symbol,
                status_code,
                raw_response_content,
                "Unexpected service failure.",
                APIErrorCode.UNKNOWN,
            ) from e_unexpected

    def _validate_cancel_order_params(self, args: CancelOrderArgs, current_method: str) -> None:
        """Validate cancel order parameters for Backpack exchange."""
        # Extract validated fields from Pydantic model
        order_id = args.order_id
        symbol = args.symbol

        # For Backpack, symbol is required
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="cancel order"
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

    async def _execute_cancel_order_request(
        self,
        args: CancelOrderArgs,
        current_method: str,
    ) -> CancelOrderResult:
        """Execute the cancel order API request and process the response."""
        # DEFENSIVE CHECK: Ensure symbol is not None before passing to request builder
        if args.symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="cancel order"
            )

        endpoint = "/api/v1/order"
        payload = self._request_builder.build_cancel_order_payload(
            symbol=args.symbol,  # Now guaranteed to be str, not str | None
            order_id=args.order_id,
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="DELETE",
            endpoint=endpoint,
            data=payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        return self._process_cancel_order_response(
            raw_data,
            status_code,
            args.order_id,
            args.symbol,
        )

    def _process_cancel_order_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        order_id: str,
        symbol: str | None,
    ) -> CancelOrderResult:
        """Process the cancel order API response."""
        # Backpack's cancel order returns the cancelled order details or an error.
        # The response handler needs to determine success.
        # Assuming handle_cancel_order_response returns bool based on successful cancellation.
        validated_data = ensure_dict_response(
            raw_data,
            f"cancel order {order_id} ({symbol})",
            status_code,
        )

        # DEFENSIVE CHECK: Ensure symbol is not None before passing to response handler
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="cancel order response processing"
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
        """Create a standardized APIError for cancel order operations."""
        logger.error(
            "cancel_order_error: Failed to cancel order",
            exchange=self._exchange_name,
            method=current_method,
            error_message=message,
            order_id=order_id,
            symbol=symbol,
            original_exception=str(original_exception),
        )
        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all open orders, optionally filtered by symbol."""
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
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint = "/api/v1/orders"
            params = self._request_builder.build_get_open_orders_params(symbol=symbol)

            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint,
                params=params.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            validated_data = ensure_list_response(
                raw_data,
                f"get open orders for {symbol or 'all'}",
                status_code,
            )

            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_get_open_orders_response(
                    validated_data,
                    symbol,
                    status_code,
                )
            )
            return [
                self._trading_mapper.transform_raw_order_to_internal(ro) for ro in raw_orders_list
            ]

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transformation_error: Failed to transform exchange data for open orders",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_transform),
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
                "validation_error: Internal data validation failed for open orders",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_val),
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
                "service_logic_error: Service internal logic error for open orders",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_service_logic),
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
                "unexpected_error: Unexpected service failure for open orders",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Get a single order by its ID."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order"

        self._validate_get_order_params(args, current_method)

        # Initialize identifier for error handling
        identifier = args.order_id

        try:
            return await self._execute_get_order_request(args, current_method)
        except APIError as e:
            return self._handle_get_order_api_error(e, identifier, args.symbol)
        except TransformationError as e_transform:
            raise self._create_get_order_api_error(
                e_transform,
                current_method,
                identifier,
                args.symbol,
                0,
                None,
                "Failed to process/transform exchange data.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_transform
        except ValidationError as e_val:
            raise self._create_get_order_api_error(
                e_val,
                current_method,
                identifier,
                args.symbol,
                0,
                None,
                "Internal data validation failed.",
                APIErrorCode.INVALID_RESPONSE,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            return self._handle_get_order_service_logic_error(
                e_service_logic,
                current_method,
                identifier,
                args.symbol,
                0,
                None,
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

    def _validate_get_order_params(self, args: GetOrderArgs, current_method: str) -> None:
        """Validate get order parameters for Backpack exchange."""
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

    def _validate_order_exists(self, order: Order | None, args: GetOrderArgs) -> None:
        """Validate that an order was found, raise OrderNotFoundError if not."""
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
        """Determine which identifier to use for the order lookup."""
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

    async def _execute_get_order_request(
        self,
        args: GetOrderArgs,
        current_method: str,
    ) -> Order | None:
        """Execute the get order API request and process the response."""
        identifier = self._determine_order_identifier(args)

        # DEFENSIVE CHECK: Ensure symbol is not None before passing to request builder
        if args.symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="cancel order"
            )

        endpoint = f"/api/v1/order/{identifier}"
        params = self._request_builder.build_get_order_params(
            symbol=args.symbol,  # Now guaranteed to be str, not str | None
        )  # Symbol is a query param

        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint,
            params=params.model_dump(by_alias=True, exclude_none=True),
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        return self._process_get_order_response(raw_data, status_code, identifier, args.symbol)

    def _process_get_order_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        identifier: str,
        symbol: str | None,
    ) -> Order | None:
        """Process the get order API response."""
        if status_code == HTTPStatus.NOT_FOUND.value:  # Order not found
            logger.info(
                "order_not_found: Order not found",
                exchange=self._exchange_name,
                order_id=identifier,
                symbol=symbol,
            )
            return None

        validated_data = ensure_dict_response(
            raw_data,
            f"get order {identifier} ({symbol})",
            status_code,
        )

        raw_order_model: BackpackRawOrder = self._response_handler.handle_get_order_status_response(
            validated_data,
            identifier,
            status_code,
        )
        return self._trading_mapper.transform_raw_order_to_internal(raw_order_model)

    def _handle_get_order_api_error(
        self,
        e: APIError,
        identifier: str,
        symbol: str | None,
    ) -> Order | None:
        """Handle APIError exceptions for get_order method."""
        # Allow ORDER_NOT_FOUND from handler to propagate if it maps it
        if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
            logger.info(
                "order_not_found_handler: Order not found via handler mapping",
                exchange=self._exchange_name,
                order_id=identifier,
                symbol=symbol,
            )
            return None
        raise

    def _handle_get_order_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        current_method: str,
        identifier: str,
        symbol: str | None,
        status_code: int,
        raw_response_content: str | None,
    ) -> Order | None:
        """Handle service logic errors for get_order method."""
        # Check if this is from our own input parameter validation
        error_msg = str(e_service_logic)
        if current_method in error_msg and any(
            param in error_msg for param in ["order_id", "symbol"]
        ):
            # This is likely from our input parameter validation - re-raise as is
            raise
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
        """Create a standardized APIError for get order operations."""
        logger.error(
            "get_order_error: Failed to get order",
            exchange=self._exchange_name,
            method=current_method,
            error_message=message,
            order_id=identifier,
            symbol=symbol,
            original_exception=str(original_exception),
        )
        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    async def get_order_status(self, args: GetOrderArgs) -> Order:
        """Get the status of a specific order."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_status"

        if args.symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="get order"
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            order = await self.get_order(args=args)
            self._validate_order_exists(order, args)
            # After validation, order is guaranteed to be not None
            BackpackTradingService._ensure_order_not_none(order)
            if order is None:
                raise APIError(
                    message="Order is None after validation - this should not happen",
                    code=APIErrorCode.UNKNOWN.value,
                )

        except APIError:
            # Re-raise APIErrors from get_order method or self-raised
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transformation_error: Failed to transform exchange data for order status",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_transform),
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
                "validation_error: Internal data validation failed for order status",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_val),
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
                "service_logic_error: Service internal logic error for order status",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
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
                "unexpected_error: Unexpected service failure for order status",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
        else:
            return order

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all orders, optionally filtered by symbol."""
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

    def _validate_cancel_all_orders_params(self, symbol: str | None, current_method: str) -> None:
        """Validate cancel all orders parameters for Backpack exchange."""
        # Business Logic Pre-Validation (moved from RequestBuilder)
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="cancel all orders"
            )

        if not symbol.strip():
            raise MissingRequiredFieldError(
                field="symbol",
                exchange="Backpack",
                operation="cancel all orders",
                reason="cannot be empty or whitespace only",
            )

    async def _execute_cancel_all_orders_request(
        self,
        symbol: str | None,
        current_method: str,
    ) -> list[CancelOrderResult]:
        """Execute the cancel all orders API request and process the response."""
        # DEFENSIVE CHECK: Ensure symbol is not None before proceeding
        if symbol is None:
            raise MissingRequiredFieldError(
                field="symbol", exchange="Backpack", operation="cancel all orders"
            )

        endpoint = "/api/v1/orders"
        # Backpack's Cancel All Orders: DELETE /api/v1/orders with symbol query parameter
        payload = self._request_builder.build_cancel_all_orders_payload(symbol=symbol)

        raw_data, status_code, _ = await self._http_client_requester(
            method="DELETE",
            endpoint=endpoint,
            data=payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        return self._process_cancel_all_orders_response(raw_data, status_code, symbol)

    def _process_cancel_all_orders_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        symbol: str,
    ) -> list[CancelOrderResult]:
        """Process the cancel all orders API response."""
        try:
            validated_data = ensure_list_response(
                raw_data,
                f"cancel all orders for {symbol}",
                status_code,
            )
        except APIError:
            return self._handle_invalid_cancel_all_response(raw_data, status_code, symbol)

        # Use the response handler to get validated BackpackRawOrder objects
        try:
            raw_orders: list[BackpackRawOrder] = (
                self._response_handler.handle_cancel_all_orders_response(
                    validated_data,
                    symbol,
                    status_code,
                )
            )
        except APIError:
            # If response handler fails, return empty list or re-raise depending on requirements
            logger.warning(
                "parse_cancel_all_failed: Failed to parse cancel all orders response",
                exchange=self._exchange_name,
                symbol=symbol,
            )
            return []

        # Transform raw orders to CancelOrderResult objects
        # All orders returned by cancel all should have been cancelled
        results: list[CancelOrderResult] = [
            CancelOrderResult(
                order_id=raw_order.id,
                client_order_id=str(raw_order.clientId) if raw_order.clientId else None,
                symbol=raw_order.symbol,
                success=True,  # If returned by cancel all, it was successfully cancelled
                message="Successfully cancelled.",
                status=CancelOrderResultStatus.SUCCESS,
            )
            for raw_order in raw_orders
        ]

        logger.info(
            "orders_cancelled: Successfully cancelled orders",
            exchange=self._exchange_name,
            count=len(results),
            symbol=symbol,
        )
        return results

    def _handle_invalid_cancel_all_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
        symbol: str,
    ) -> list[CancelOrderResult]:
        """Handle invalid response from cancel all orders API."""
        error_message = (
            f"Cancel all orders for {symbol or 'all'} returned invalid data "
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
            "cancel_all_invalid_response: Invalid response, assuming no orders were cancelled",
            exchange=self._exchange_name,
            error_message=error_message,
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
        symbol: str | None,
        status_code: int,
        raw_response_content: str | None,
        message: str,
        error_code: APIErrorCode,
    ) -> APIError:
        """Create a standardized APIError for cancel all orders operations."""
        logger.error(
            "cancel_all_orders_error: Failed to cancel all orders",
            exchange=self._exchange_name,
            method=current_method,
            error_message=message,
            original_exception=str(original_exception),
        )
        return APIError(
            code=error_code.value,
            message=message,
            original_exception=original_exception,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol.

        Args:
            args: Parameters for filtering open orders including optional symbol.

        """
        # Service Input Parameter Validation is now handled by GetAllOpenOrdersArgs Pydantic model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_open_orders"

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic - delegate to get_open_orders with validated symbol
            return await self.get_open_orders(symbol=args.symbol)

        except APIError:
            # Re-raise APIErrors from get_open_orders method
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transformation_error: Failed to transform exchange data for all open orders",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_transform),
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
                "validation_error: Internal data validation failed for all open orders",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_val),
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
            if current_method in error_msg and "symbol" in error_msg:
                # This is likely from our input parameter validation - re-raise as is
                raise
            # This is from service internal logic - wrap as APIError
            logger.exception(
                "service_logic_error: Service internal logic error for all open orders",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
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
                "unexpected_error: Unexpected service failure for all open orders",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
