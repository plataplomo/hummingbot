"""CyberDeltaEngine: Hyperliquid Trading Service.

----------------------------------------------

This service encapsulates the logic for trading operations on the Hyperliquid Exchange.
It uses the HttpClient (via a requester callable), HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API.
This version of the service returns Internal Domain Models by using the HyperliquidOrderMapper.
"""

import inspect
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal

from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler

# Internal Domain Models & Mappers
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
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
    OrderStatus,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.parsing import parse_decimal_value

logger = get_logger(__name__)


HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidTradingService:
    """Service class for Hyperliquid trading operations. Returns Internal Domain Models."""

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Awaitable[int | None]],
        trading_mapper: HyperliquidTradingDataMapper,
        error_mapper: HyperliquidErrorMapper,
    ) -> None:
        """Initialize the Hyperliquid trading service with required dependencies.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for authenticated operations (optional)
            get_asset_index_callable: Function to retrieve asset index for symbols
            trading_mapper: Mapper for converting raw data to internal domain models
            error_mapper: Mapper for handling and transforming API errors

        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        self._get_asset_index_callable = get_asset_index_callable
        self._trading_mapper = trading_mapper
        self._error_mapper = error_mapper
        self._action_endpoint = "/exchange"
        self._info_endpoint = "/info"

    async def _place_order_raw(
        self,
        place_order_payload: HyperliquidApiPlaceOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Private method to place an order, using the API request payload model.

        The payload is already built by the request builder with the correct format.
        Returns the raw exchange response Pydantic model and HTTP status code.
        """
        # Early authentication checks - fail fast if auth requirements not met
        if not self._authenticator:
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        _error_msg_wallet_addr = "Wallet address is required for placing an order."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        # Use the payload directly - it's already in the correct format
        request_payload_model = place_order_payload

        try:
            raw_content, http_status, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=request_payload_model.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                serialize_none_as_null=False,
            )
            if raw_content is None:
                _error_msg_no_content = (
                    f"Exchange action ({request_payload_model.type}) returned no content."
                )
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

            exchange_response = self._response_handler.handle_exchange_response(
                raw_content,
                action_type=request_payload_model.type,
            )
            return exchange_response, http_status
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error placing order raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(f"[{self._exchange_name}] Unexpected error placing order raw: {e}")
            _error_msg_unexpected = f"Unexpected error placing order raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _cancel_order_raw(
        self,
        cancel_action: HyperliquidRawCancelOrderAction,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Private method to cancel an order, using the raw action model.

        The builder wraps this in HyperliquidApiCancelOrderRequest.
        Returns the raw exchange response Pydantic model and HTTP status code.
        """
        # Early authentication checks - fail fast if auth requirements not met
        if not self._authenticator:
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        _error_msg_wallet_addr = "Wallet address is required for cancelling an order."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model: HyperliquidApiCancelOrderRequest = HyperliquidApiCancelOrderRequest(
            type="cancel",
            action=cancel_action,
        )

        try:
            raw_content, http_status, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=request_payload_model.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                serialize_none_as_null=False,
            )
            if raw_content is None:
                _error_msg_no_content = (
                    f"Exchange action ({request_payload_model.type}) returned no content."
                )
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

            exchange_response = self._response_handler.handle_exchange_response(
                raw_content,
                action_type=request_payload_model.type,
            )
            return exchange_response, http_status
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error cancelling order raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(f"[{self._exchange_name}] Unexpected error cancelling order raw: {e}")
            _error_msg_unexpected = f"Unexpected error cancelling order raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _get_open_orders_raw(self) -> list[HyperliquidRawOpenOrder]:
        """Private method to fetch raw open orders.

        Returns a list of HyperliquidRawOpenOrder Pydantic models.
        """
        _error_msg_wallet_addr = "Wallet address is required to fetch open orders."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model = self._request_builder.build_open_orders_payload(
            wallet_address=self._wallet_address,
        )

        try:
            raw_response_content, _, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=True,
            )
            if raw_response_content is None:
                _error_msg_no_content = "Fetching open orders returned no content."
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

            validated_response: HyperliquidRawOpenOrdersResponse = (
                self._response_handler.handle_info_open_orders_response(
                    raw_response_content,
                    user_address=self._wallet_address,
                )
            )
            return validated_response.items
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error fetching open orders raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error fetching open orders raw: {e}",
            )
            _error_msg_unexpected = f"Unexpected error fetching open orders raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _get_order_status_raw(
        self,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrder | None:
        """Private method to fetch raw order status.

        Returns a HyperliquidRawHistoricalOrder Pydantic model or None if not found.
        The actual response from HL for orderStatus is a HyperliquidRawHistoricalOrderResponse,
        which contains the HyperliquidRawHistoricalOrder.
        """
        _error_msg_wallet_addr = "Wallet address is required to fetch order status."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model: HyperliquidRawOrderStatusRequestPayload = (
            self._request_builder.build_order_status_payload(
                wallet_address=self._wallet_address,
                order_id=order_id,
            )
        )
        try:
            raw_response_content, _, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=True,
            )
            if raw_response_content is None:
                logger.error(
                    f"[{self._exchange_name}] No content received for order status "
                    f"for OID {order_id}.",
                )
                raise APIError(
                    message=f"No data received for order status for OID {order_id}.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            historical_order_response: HyperliquidRawHistoricalOrderResponse = (
                self._response_handler.handle_info_order_status_response(
                    raw_response_content,
                    user_address=self._wallet_address,
                    order_id=order_id,
                )
            )
            if historical_order_response and historical_order_response.order:
                return historical_order_response.order
            return None

        except APIError as e:
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value or "Order not found" in e.message:
                logger.info(f"[{self._exchange_name}] Order OID {order_id} not found via API.")
                return None
            logger.error(
                f"[{self._exchange_name}] API error fetching order status raw for OID {order_id}: "
                f"{e.message}",
            )
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error fetching order status raw for OID "
                f"{order_id}: {e}",
            )
            _error_msg_unexpected = f"Unexpected error fetching order status raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Retrieve a specific order by ID for a given symbol.

        Args:
            args: Arguments containing order_id and symbol for order retrieval

        Returns:
            Order object if found, None otherwise

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If order_id is not a valid integer

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order"

        # args.order_id is guaranteed to be non-empty by the GetOrderArgs validator
        # Hyperliquid doesn't require symbol for orderStatus endpoint, so we don't validate it

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            # DEFENSIVE CHECK: Convert order_id to int with proper error handling.
            # args.order_id is guaranteed to be str by Pydantic validator.
            try:
                order_id_int = int(args.order_id)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"[{current_method}] 'order_id' must be a valid integer, got '{args.order_id}'",
                ) from e

            raw_historical_order = await self._get_order_status_raw(order_id_int)
            if raw_historical_order is None:
                return None

            # Use trading mapper to convert to internal order
            internal_order = self._trading_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order,
                trigger=None,
            )
            return internal_order

        except APIError:
            # Re-raise APIErrors from _get_order_status_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order {args.order_id}: {e_transform}",
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
                f"failed for order {args.order_id}: {e_val}",
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
                f"for order {args.order_id}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for order {args.order_id}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Places an order and maps the raw response to an internal Order model."""
        # Service Input Parameter Validation is now handled by PlaceOrderArgs model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_order"

        # Handle optional price - use Decimal("0") for market orders
        order_price = args.price if args.price is not None else Decimal("0")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            asset_index = await self._get_asset_index_callable(args.symbol)
            if asset_index is None:
                raise APIError(
                    f"Asset index for {args.symbol} not found.",
                    APIErrorCode.INVALID_SYMBOL.value,
                )

            # Use the request builder to create the proper payload format
            place_order_payload = self._request_builder.build_place_order_payload(
                asset_index=asset_index,
                side=args.side,
                order_type=args.order_type,
                quantity=args.quantity,
                time_in_force=args.time_in_force,
                price=order_price,
                stop_price=args.stop_price,
                client_order_id=args.client_order_id,
                reduce_only=args.reduce_only,
                post_only=args.post_only,
            )

            raw_exchange_response, http_status = await self._place_order_raw(place_order_payload)
            status_code = http_status

            # Process the response and return the order
            return await self._process_place_order_response(
                raw_exchange_response, http_status, args
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            raise self._handle_place_order_transformation_error(
                e_transform, current_method, args.symbol, status_code, raw_response_content
            ) from e_transform
        except ValidationError as e_val:
            raise self._handle_place_order_validation_error(
                e_val, current_method, args.symbol, status_code, raw_response_content
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._handle_place_order_service_logic_error(
                e_service_logic, current_method, args.symbol
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._handle_place_order_unexpected_error(
                e_unexpected, current_method, args.symbol, status_code, raw_response_content
            ) from e_unexpected

    async def _process_place_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        args: PlaceOrderArgs,
    ) -> Order:
        """Process the place order response and return the internal Order."""
        if raw_exchange_response.data and raw_exchange_response.data.statuses:
            first_status = raw_exchange_response.data.statuses[0]
            if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                if first_status.resting:
                    return await self._handle_resting_order(first_status.resting, args)
                elif first_status.filled:
                    return await self._handle_filled_order(first_status.filled, args)
                elif first_status.error:
                    # Use the error mapper to get the specific error code for this message
                    mapped_error = self._error_mapper.map_string_error(
                        first_status.error,
                        http_status=http_status,
                    )
                    raise mapped_error
            elif "error" in first_status.lower():
                raise APIError(
                    f"Failed to place order: {first_status}",
                    APIErrorCode.UNKNOWN.value,
                )

        raise APIError("Failed to place order or parse response.", APIErrorCode.UNKNOWN.value)

    async def _handle_resting_order(
        self, resting_info: HyperliquidRawExchangeStatusResting, args: PlaceOrderArgs
    ) -> Order:
        """Handle a resting (open) order response."""
        new_oid = resting_info.oid
        logger.info(
            f"Order placed with OID: {new_oid}. Re-fetching for full details.",
        )
        internal_order = await self.get_order(
            GetOrderArgs(symbol=args.symbol, order_id=str(new_oid)),
        )
        if internal_order:
            return internal_order
        else:
            raise APIError(
                f"Order placed (OID {new_oid}) but failed to re-fetch details.",
                APIErrorCode.UNKNOWN.value,
            )

    async def _handle_filled_order(
        self, filled_info: HyperliquidRawExchangeStatusFilled, args: PlaceOrderArgs
    ) -> Order:
        """Handle a filled order response."""
        filled_oid = filled_info.oid
        logger.info(
            f"Order OID {filled_oid} filled immediately. Re-fetching for full details.",
        )
        internal_order = await self.get_order(
            GetOrderArgs(symbol=args.symbol, order_id=str(filled_oid)),
        )
        if internal_order:
            internal_order.status = OrderStatus.FILLED
            internal_order.quantity_filled = (
                parse_decimal_value(
                    filled_info.total_sz,
                    allow_none=False,
                    field_name="totalSz",
                )
                or args.quantity
            )
            internal_order.average_fill_price = parse_decimal_value(
                filled_info.avg_px,
                allow_none=False,
                field_name="avgPx",
            )
            return internal_order
        else:
            raise APIError(
                f"Order filled (OID {filled_oid}) but failed to re-fetch details.",
                APIErrorCode.UNKNOWN.value,
            )

    def _handle_place_order_transformation_error(
        self,
        e_transform: TransformationError,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle transformation errors for place_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
            f"data for {symbol}: {e_transform}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=e_transform,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_place_order_validation_error(
        self,
        e_val: ValidationError,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle validation errors for place_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Internal data validation "
            f"failed for {symbol}: {e_val}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=e_val,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_place_order_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        current_method: str,
        symbol: str,
    ) -> APIError:
        """Handle service logic errors for place_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Service internal logic error "
            f"for {symbol}: {e_service_logic}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=e_service_logic,
        )

    def _handle_place_order_unexpected_error(
        self,
        e_unexpected: Exception,
        current_method: str,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle unexpected errors for place_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Unexpected service failure "
            f"for {symbol}: {e_unexpected}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e_unexpected,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Retrieve all open orders, optionally filtered by symbol.

        Maps the raw orders to a list of internal Order models.

        Args:
            symbol: Optional symbol filter for orders (case-insensitive)

        Returns:
            List of Order objects representing open orders

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If symbol is provided but empty

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided.",
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            raw_open_orders = await self._get_open_orders_raw()
            internal_orders: list[Order] = []

            for raw_open_order_item_wrapper in raw_open_orders:
                raw_order_details = raw_open_order_item_wrapper.order
                raw_trigger_details = raw_open_order_item_wrapper.trigger

                if symbol is None or raw_order_details.asset.upper() == symbol.upper():
                    try:
                        mapped_order = self._trading_mapper.transform_raw_order_to_internal(
                            raw_order=raw_order_details,
                            trigger=raw_trigger_details,
                        )
                        internal_orders.append(mapped_order)
                    except Exception as e:
                        logger.error(
                            f"[{self._exchange_name}] Error mapping raw open order to "
                            f"internal: {e}. Raw order: {raw_order_details.model_dump_json()}",
                        )
                        # Continue processing other orders

            logger.info(
                f"[{self._exchange_name}] Retrieved {len(internal_orders)} open orders "
                f"(symbol filter: {symbol})",
            )
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
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
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error: "
                f"{e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
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

    async def cancel_order(self, args: CancelOrderArgs) -> bool:
        """Cancel a specific order and return True if successful.

        Args:
            args: Arguments containing order_id and symbol for order cancellation

        Returns:
            True if order was successfully cancelled

        Raises:
            APIError: If API request fails or order cancellation fails
            ValueError: If order_id is not a valid positive integer or symbol is missing

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_order"

        # Validate and prepare cancellation parameters
        symbol, order_id_int = self._validate_and_prepare_cancel_order_params(args, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic - need asset index for cancellation
            asset_index = await self._get_asset_index_callable(symbol)
            if asset_index is None:
                raise APIError(
                    f"Asset index for {args.symbol} not found for cancellation.",
                    APIErrorCode.INVALID_SYMBOL.value,
                )

            cancel_action = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id_int)
            raw_exchange_response, http_status = await self._cancel_order_raw(cancel_action)
            status_code = http_status

            # Process the cancellation response
            return self._process_cancel_order_response(
                raw_exchange_response, http_status, order_id_int
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            raise self._handle_cancel_order_transformation_error(
                e_transform, current_method, order_id_int, status_code, raw_response_content
            ) from e_transform
        except ValidationError as e_val:
            raise self._handle_cancel_order_validation_error(
                e_val, current_method, order_id_int, status_code, raw_response_content
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._handle_cancel_order_service_logic_error(
                e_service_logic, current_method, order_id_int
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._handle_cancel_order_unexpected_error(
                e_unexpected, current_method, order_id_int, status_code, raw_response_content
            ) from e_unexpected

    def _validate_and_prepare_cancel_order_params(
        self, args: CancelOrderArgs, current_method: str
    ) -> tuple[str, int]:
        """Validate and prepare parameters for order cancellation."""
        # Extract validated fields from Pydantic model
        symbol = args.symbol
        order_id = args.order_id

        # For Hyperliquid, symbol is required
        if symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' parameter is required.")

        # Convert order_id to int (it's always a string from Pydantic model)
        try:
            order_id_int = int(order_id)
        except ValueError as e:
            raise ValueError(
                f"[{current_method}] 'order_id' must be a valid integer: {order_id}",
            ) from e

        if order_id_int <= 0:
            raise ValueError(f"[{current_method}] 'order_id' must be positive.")

        return symbol, order_id_int

    def _process_cancel_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        order_id_int: int,
    ) -> bool:
        """Process the cancel order response and return success status."""
        if raw_exchange_response.data and raw_exchange_response.data.statuses:
            first_status = raw_exchange_response.data.statuses[0]
            if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                if first_status.error:
                    # Use the error mapper to get the specific error code for this message
                    mapped_error = self._error_mapper.map_string_error(
                        first_status.error,
                        http_status=http_status,
                    )
                    raise mapped_error
                else:
                    logger.info(
                        f"[{self._exchange_name}] Successfully cancelled order OID {order_id_int}",
                    )
                    return True
            elif "error" in first_status.lower():
                raise APIError(
                    f"Failed to cancel order: {first_status}",
                    APIErrorCode.UNKNOWN.value,
                )
            else:
                logger.info(
                    f"[{self._exchange_name}] Successfully cancelled order OID {order_id_int}",
                )
                return True

        # If we reach here, something unexpected happened
        raise APIError("Failed to cancel order or parse response.", APIErrorCode.UNKNOWN.value)

    def _handle_cancel_order_transformation_error(
        self,
        e_transform: TransformationError,
        current_method: str,
        order_id_int: int,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle transformation errors for cancel_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
            f"data for order {order_id_int}: {e_transform}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=e_transform,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_cancel_order_validation_error(
        self,
        e_val: ValidationError,
        current_method: str,
        order_id_int: int,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle validation errors for cancel_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Internal data validation "
            f"failed for order {order_id_int}: {e_val}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=e_val,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_cancel_order_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        current_method: str,
        order_id_int: int,
    ) -> APIError:
        """Handle service logic errors for cancel_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Service internal logic error "
            f"for order {order_id_int}: {e_service_logic}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=e_service_logic,
        )

    def _handle_cancel_order_unexpected_error(
        self,
        e_unexpected: Exception,
        current_method: str,
        order_id_int: int,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle unexpected errors for cancel_order."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Unexpected service failure "
            f"for order {order_id_int}: {e_unexpected}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e_unexpected,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders, optionally filtered by symbol.

        Returns a list of CancelOrderResult for each attempted cancellation.

        Args:
            symbol: Optional symbol filter for orders to cancel (case-insensitive)

        Returns:
            List of CancelOrderResult objects with cancellation status for each order

        Raises:
            APIError: If API request fails or authentication is missing

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_all_orders"

        # No specific input validation needed for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            self._validate_cancel_all_orders_prerequisites()

            open_orders_internal = await self.get_open_orders(symbol=symbol)
            if not open_orders_internal:
                logger.info(
                    f"[{self._exchange_name}] No open orders found matching symbol "
                    f"'{symbol if symbol else 'any'}' to cancel.",
                )
                return []

            # Process cancellations for all orders
            return await self._process_all_order_cancellations(open_orders_internal)

        except APIError:
            # Re-raise APIErrors from get_open_orders, cancel_order, etc.
            raise
        except TransformationError as e_transform:
            raise self._handle_cancel_all_orders_transformation_error(
                e_transform, current_method, status_code, raw_response_content
            ) from e_transform
        except ValidationError as e_val:
            raise self._handle_cancel_all_orders_validation_error(
                e_val, current_method, status_code, raw_response_content
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            raise self._handle_cancel_all_orders_service_logic_error(
                e_service_logic, current_method, status_code, raw_response_content
            ) from e_service_logic
        except Exception as e_unexpected:
            raise self._handle_cancel_all_orders_unexpected_error(
                e_unexpected, current_method, status_code, raw_response_content
            ) from e_unexpected

    def _validate_cancel_all_orders_prerequisites(self) -> None:
        """Validate prerequisites for cancelling all orders."""
        _error_msg_wallet_addr = "Wallet address is required to cancel all orders."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

    async def _process_all_order_cancellations(
        self, open_orders_internal: list[Order]
    ) -> list[CancelOrderResult]:
        """Process cancellations for all open orders."""
        results: list[CancelOrderResult] = []
        active_cancels_count = 0

        for order_to_cancel in open_orders_internal:
            if order_to_cancel.exchange_order_id is None:
                results.append(self._create_failed_cancel_result_no_id(order_to_cancel))
                continue

            cancel_result = await self._attempt_single_order_cancellation(order_to_cancel)
            results.append(cancel_result)

            if cancel_result.success:
                active_cancels_count += 1

        if active_cancels_count == 0 and len(open_orders_internal) > 0:
            logger.info(
                f"[{self._exchange_name}] Found {len(open_orders_internal)} orders but none "
                f"had valid exchange_order_id for cancellation attempt.",
            )

        return results

    def _create_failed_cancel_result_no_id(self, order_to_cancel: Order) -> CancelOrderResult:
        """Create a failed cancel result for orders without exchange_order_id."""
        logger.warning(
            f"[{self._exchange_name}] Skipping cancellation for order without "
            f"exchange_order_id: ClientOID {order_to_cancel.client_order_id}, "
            f"Symbol {order_to_cancel.symbol}",
        )
        return CancelOrderResult(
            symbol=order_to_cancel.symbol,
            order_id=None,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message="Order has no exchange_order_id, cannot be canceled by ID.",
            status=CancelOrderResultStatus.FAILED,
        )

    async def _attempt_single_order_cancellation(self, order_to_cancel: Order) -> CancelOrderResult:
        """Attempt to cancel a single order and return the result."""
        order_id_to_cancel_str = order_to_cancel.exchange_order_id
        order_symbol_for_cancel = order_to_cancel.symbol

        # DEFENSIVE CHECK: exchange_order_id should not be None at this point
        if order_id_to_cancel_str is None:
            raise ValueError("exchange_order_id is None")

        try:
            order_id_int = int(order_id_to_cancel_str)
            logger.debug(
                f"Attempting to cancel order {order_id_int} for symbol {order_symbol_for_cancel}",
            )

            cancel_args = CancelOrderArgs(
                order_id=str(order_id_int),
                symbol=order_symbol_for_cancel,
            )
            success_flag = await self.cancel_order(args=cancel_args)

            return CancelOrderResult(
                symbol=order_symbol_for_cancel,
                order_id=str(order_id_int),
                client_order_id=order_to_cancel.client_order_id,
                success=success_flag,
                message="Successfully canceled." if success_flag else "Failed to cancel via API.",
                status=CancelOrderResultStatus.SUCCESS
                if success_flag
                else CancelOrderResultStatus.FAILED,
            )
        except ValueError:
            return self._create_invalid_order_id_result(
                order_to_cancel, order_id_to_cancel_str, order_symbol_for_cancel
            )
        except APIError as e_api:
            return self._create_api_error_result(
                order_to_cancel, order_id_to_cancel_str, order_symbol_for_cancel, e_api
            )
        except Exception as e_generic:
            return self._create_generic_error_result(
                order_to_cancel, order_id_to_cancel_str, order_symbol_for_cancel, e_generic
            )

    def _create_invalid_order_id_result(
        self, order_to_cancel: Order, order_id_to_cancel_str: str, order_symbol_for_cancel: str
    ) -> CancelOrderResult:
        """Create result for invalid order ID format."""
        logger.error(
            f"[{self._exchange_name}] Invalid order_id format "
            f"'{order_id_to_cancel_str}' for cancellation.",
        )
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=order_id_to_cancel_str,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message=f"Invalid order_id format: {order_id_to_cancel_str}.",
            status=CancelOrderResultStatus.FAILED,
        )

    def _create_api_error_result(
        self,
        order_to_cancel: Order,
        order_id_to_cancel_str: str,
        order_symbol_for_cancel: str,
        e_api: APIError,
    ) -> CancelOrderResult:
        """Create result for API errors during cancellation."""
        logger.error(
            f"[{self._exchange_name}] APIError cancelling order "
            f"{order_id_to_cancel_str} for {order_symbol_for_cancel}: {e_api.message}",
        )
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=order_id_to_cancel_str,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message=e_api.message,
            status=CancelOrderResultStatus.FAILED,
            raw_response={
                "error_code": e_api.code,
                "http_status": e_api.http_status,
            },
        )

    def _create_generic_error_result(
        self,
        order_to_cancel: Order,
        order_id_to_cancel_str: str,
        order_symbol_for_cancel: str,
        e_generic: Exception,
    ) -> CancelOrderResult:
        """Create result for generic errors during cancellation."""
        logger.exception(
            f"[{self._exchange_name}] Unexpected error cancelling order "
            f"{order_id_to_cancel_str} for {order_symbol_for_cancel}: {e_generic}",
        )
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=order_id_to_cancel_str,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message=str(e_generic),
            status=CancelOrderResultStatus.UNKNOWN,
        )

    def _handle_cancel_all_orders_transformation_error(
        self,
        e_transform: TransformationError,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle transformation errors for cancel_all_orders."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
            f"data for cancel all orders: {e_transform}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=e_transform,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_cancel_all_orders_validation_error(
        self,
        e_val: ValidationError,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle validation errors for cancel_all_orders."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Internal data validation "
            f"failed for cancel all orders: {e_val}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=e_val,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_cancel_all_orders_service_logic_error(
        self,
        e_service_logic: ValueError | TypeError,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle service logic errors for cancel_all_orders."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Service internal logic error "
            f"for cancel all orders: {e_service_logic}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=e_service_logic,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        )

    def _handle_cancel_all_orders_unexpected_error(
        self,
        e_unexpected: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> APIError:
        """Handle unexpected errors for cancel_all_orders."""
        logger.error(
            f"[{self._exchange_name}] {current_method}: Unexpected service failure "
            f"for cancel all orders: {e_unexpected}",
            exc_info=True,
        )
        return APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e_unexpected,
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
