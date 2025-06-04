"""CyberDeltaEngine: Backpack Trading Service
-------------------------------------------

This service encapsulates the logic for trading operations on the Backpack Exchange.
It uses the HttpClient (via a requester callable), BackpackRequestBuilder,
BackpackResponseHandler, and BackpackTradingDataMapper to interact with the API
and returns Internal Domain Models.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult

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

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_order"

        # Validate order type is supported by Backpack
        supported_order_types = [
            OrderType.LIMIT,
            OrderType.MARKET,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
        ]
        if args.order_type not in supported_order_types:
            raise ValueError(
                f"[{current_method}] Unsupported order type for Backpack: {args.order_type.value}",
            )

        # Validate time in force for limit orders
        if args.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
            supported_tif = [TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK]
            if args.time_in_force not in supported_tif:
                raise ValueError(
                    f"[{current_method}] Unsupported time in force for limit orders: "
                    f"{args.time_in_force.value}. Supported: {[tif.value for tif in supported_tif]}",
                )

        # Validate client_order_id can be converted to int if provided
        if args.client_order_id:
            try:
                int(args.client_order_id)
            except ValueError as e:
                raise ValueError(
                    f"[{current_method}] client_order_id must be convertible to integer, "
                    f"got: {args.client_order_id}",
                ) from e

        # Validate reduce_only is not supported (log warning)
        if args.reduce_only:
            logger.warning(
                f"[{self._exchange_name}] 'reduce_only' parameter is not supported for "
                f"place_order and will be ignored.",
            )

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint = "/api/v1/order"
            payload = self._request_builder.build_place_order_payload(
                symbol=args.symbol,
                side=args.side,
                order_type=args.order_type,
                quantity=args.quantity,
                time_in_force=args.time_in_force,
                price=args.price,
                client_order_id=args.client_order_id,
                post_only=args.post_only,
                trigger_price=args.stop_price,
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=endpoint,
                data=payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            if raw_data is None or not isinstance(raw_data, dict):
                raise APIError(
                    f"Place order for {args.symbol} returned invalid data (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_model: BackpackRawOrder = self._response_handler.handle_place_order_response(
                raw_data,
            )
            internal_order = self._trading_mapper.transform_raw_order_to_internal(raw_order_model)
            return internal_order

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {args.symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {args.symbol}: {e_val}",
                exc_info=True,
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
            if current_method in error_msg and any(
                param in error_msg for param in ["client_order_id", "reduce_only"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for {args.symbol}: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {args.symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def cancel_order(self, args: CancelOrderArgs) -> bool:
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_order"

        # Extract validated fields from Pydantic model
        order_id = args.order_id
        symbol = args.symbol

        # For Backpack, symbol is required
        if symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' is required for Backpack.")

        # Business Logic Pre-Validation (moved from RequestBuilder)
        # For Backpack, either order_id or client_order_id must be provided, but not both
        # Since this method only accepts order_id, we validate it's provided and non-empty
        if not order_id.strip():
            raise ValueError(f"[{current_method}] 'order_id' cannot be empty or whitespace only.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint = "/api/v1/order"
            payload = self._request_builder.build_cancel_order_payload(
                symbol=symbol,
                order_id=order_id,
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="DELETE",
                endpoint=endpoint,
                data=payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            # Backpack's cancel order returns the cancelled order details or an error.
            # The response handler needs to determine success.
            # Assuming handle_cancel_order_response returns bool based on successful cancellation.
            if raw_data is None:
                logger.error(
                    f"[{self._exchange_name}] Cancel order for {order_id} ({symbol}) received "
                    f"no content. Status: {status_code}",
                )
                raise APIError(
                    message=f"No data received when cancelling order {order_id} ({symbol}), "
                    f"status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            return self._response_handler.handle_cancel_order_response(
                raw_response_content=raw_data,
                order_id=order_id,
                symbol=symbol,
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order {order_id} ({symbol}): {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for order {order_id} ({symbol}): {e_val}",
                exc_info=True,
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
            if current_method in error_msg and any(
                param in error_msg for param in ["order_id", "symbol"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for order {order_id} ({symbol}): {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for order {order_id} ({symbol}): {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all open orders, optionally filtered by symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided.",
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
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            if raw_data is None or not isinstance(raw_data, list):
                raise APIError(
                    f"Get open orders for {symbol or 'all'} returned invalid data "
                    f"(status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_get_open_orders_response(raw_data, symbol)
            )
            internal_orders = [
                self._trading_mapper.transform_raw_order_to_internal(ro) for ro in raw_orders_list
            ]
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol or 'all'}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol or 'all'}: {e_val}",
                exc_info=True,
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
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for {symbol or 'all'}: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol or 'all'}: {e_unexpected}",
                exc_info=True,
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

        # Backpack requires symbol for its GET /order/{id} endpoint
        if args.symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' parameter is required for Backpack.")
        if not args.symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None
        identifier = args.order_id  # Initialize identifier outside try block

        try:
            # Core operational logic
            # Backpack uses orderId or clientOrderId in path for GET /api/v1/order/{identifier}
            # The builder should handle which identifier to use, or service needs logic.
            # Assuming order_id is the exchange order ID if provided.
            # If only client_order_id, that should be used as identifier.

            if not args.order_id and args.client_order_id:
                identifier = args.client_order_id
            elif not args.order_id and not args.client_order_id:
                raise ValueError("Either order_id or client_order_id must be provided.")

            endpoint = f"/api/v1/order/{identifier}"
            params = self._request_builder.build_get_order_params(
                symbol=args.symbol,
            )  # Symbol is a query param

            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint,
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            if status_code == 404:  # Order not found
                logger.info(
                    f"[{self._exchange_name}] Order {identifier} ({args.symbol}) not found.",
                )
                return None

            if raw_data is None or not isinstance(raw_data, dict):
                raise APIError(
                    f"Get order {identifier} ({args.symbol}) returned invalid data "
                    f"(status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_model: BackpackRawOrder = (
                self._response_handler.handle_get_order_status_response(raw_data, identifier)
            )
            internal_order = self._trading_mapper.transform_raw_order_to_internal(raw_order_model)
            return internal_order

        except APIError as e:
            # Allow ORDER_NOT_FOUND from handler to propagate if it maps it
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.info(
                    f"[{self._exchange_name}] Order {identifier} ({args.symbol}) not found "
                    f"via handler mapping.",
                )
                return None
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order {args.order_id} ({args.symbol}): {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for order {identifier} ({args.symbol}): {e_val}",
                exc_info=True,
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
            if current_method in error_msg and any(
                param in error_msg for param in ["order_id", "symbol"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for order {identifier} ({args.symbol}): {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for order {identifier} ({args.symbol}): {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_order_status(self, args: GetOrderArgs) -> Order:
        """Get the status of a specific order."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_status"

        if args.symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' parameter is required for Backpack.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            order = await self.get_order(args=args)
            if order is None:
                identifier = (
                    args.client_order_id
                    if not args.order_id and args.client_order_id
                    else args.order_id
                )
                raise APIError(
                    f"Order {identifier} for symbol {args.symbol} not found on "
                    f"{self._exchange_name}.",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                )
            return order

        except APIError:
            # Re-raise APIErrors from get_order method or self-raised
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order status: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for order status: {e_val}",
                exc_info=True,
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
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for order status: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for order status: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all orders, optionally filtered by symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_all_orders"

        # Business Logic Pre-Validation (moved from RequestBuilder)
        if symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' is required for cancel all orders.")

        if not symbol.strip():
            raise ValueError(f"[{current_method}] 'symbol' cannot be empty or whitespace only.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint = "/api/v1/orders"
            # Backpack's Cancel All Orders: DELETE /api/v1/orders with symbol query parameter
            payload = self._request_builder.build_cancel_all_orders_payload(symbol=symbol)

            results: list[CancelOrderResult] = []

            raw_data, status_code, _ = await self._http_client_requester(
                method="DELETE",
                endpoint=endpoint,
                data=payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            # Backpack's response for cancel all is a list of strings
            # (order IDs that were cancelled)
            if raw_data is None or not isinstance(raw_data, list):
                error_message = (
                    f"Cancel all orders for {symbol or 'all'} returned invalid data "
                    f"or no content (status: {status_code})"
                )
                logger.error(f"[{self._exchange_name}] {error_message}. Raw: {raw_data}")
                # If response is not a list, it might be an error structure or unexpected.
                # We can't confirm any cancellations.
                # Depending on strictness, either raise or return empty/failed results.
                # For now, if it's not a list, assume general failure or no orders to cancel.
                # If some orders were open, this would be a partial failure.
                # This part needs careful handling based on actual API error
                # responses for cancel all.
                if isinstance(raw_data, dict) and raw_data.get("error"):  # Check for explicit error
                    raise APIError(
                        raw_data.get("error", {}).get(
                            "message",
                            "Failed to cancel all orders due to API error response.",
                        ),
                        APIErrorCode.UNKNOWN.value,  # Using UNKNOWN as OPERATION_FAILED
                        # is not available
                        http_status=status_code,
                        exchange_message=str(raw_data),
                    )

                logger.warning(
                    f"[{self._exchange_name}] {error_message}. "
                    f"Assuming no orders were cancelled or confirmable.",
                )
                # Create a generic failure result if no orders could be confirmed cancelled.
                # This assumes that if there were orders and they failed to cancel,
                # an error would be raised.
                # If there were no orders, an empty list response is typical and correct.
                # If raw_data is None, it's ambiguous.
                return []  # Or a list with a single generic failure if that's preferred

            # If raw_data is a list, it should be a list of successfully
            # cancelled order IDs (strings)
            for cancelled_order_id_any in raw_data:
                if isinstance(cancelled_order_id_any, str):
                    results.append(
                        CancelOrderResult(
                            order_id=cancelled_order_id_any,
                            client_order_id=None,  # Backpack doesn't return this in cancel all
                            symbol=symbol,  # We assume all were for this symbol if provided
                            success=True,
                            message="Successfully cancelled.",
                            status=CancelOrderResultStatus.SUCCESS,
                        ),
                    )
                else:  # Should not happen if API conforms
                    logger.warning(
                        f"[{self._exchange_name}] Unexpected item in cancel all orders "
                        f"response list: {cancelled_order_id_any}",
                    )

            logger.info(
                f"[{self._exchange_name}] Cancelled {len(results)} orders for {symbol or 'all'}.",
            )
            return results

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for cancel all orders: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for cancel all orders: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for cancel all orders: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for cancel all orders: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

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
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed: {e_val}",
                exc_info=True,
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
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error: "
                    f"{e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure: "
                f"{e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
