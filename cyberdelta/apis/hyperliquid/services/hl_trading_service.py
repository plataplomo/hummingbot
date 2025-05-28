"""
CyberDeltaEngine: Hyperliquid Trading Service
----------------------------------------------

This service encapsulates the logic for trading operations on the Hyperliquid Exchange.
It uses the HttpClient (via a requester callable), HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API.
This version of the service returns Internal Domain Models by using the HyperliquidOrderMapper.
"""

import inspect
from collections.abc import Callable, Coroutine, Mapping
from decimal import Decimal
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)

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
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.parsing import parse_decimal_value

logger = get_logger(__name__)


HttpClientRequesterSig = Callable[
    ..., Coroutine[Any, Any, tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class HyperliquidTradingService:
    """
    Service class for Hyperliquid trading operations. Returns Internal Domain Models.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Coroutine[Any, Any, int | None]],
        trading_mapper: HyperliquidTradingDataMapper,
        error_mapper: HyperliquidErrorMapper,
    ) -> None:
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
        """
        Private method to place an order, using the API request payload model.
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
                data=request_payload_model.model_dump(by_alias=True, exclude_none=False),
                is_signed=True,
                serialize_none_as_null=True,
            )
            if raw_content is None:
                _error_msg_no_content = (
                    f"Exchange action ({request_payload_model.type}) returned no content."
                )
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

            exchange_response = self._response_handler.handle_exchange_response(
                cast(RawJsonResponse, raw_content),
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
        """
        Private method to cancel an order, using the raw action model.
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
            type="cancel", action=cancel_action
        )

        try:
            raw_content, http_status, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=request_payload_model.model_dump(by_alias=True, exclude_none=False),
                is_signed=True,
                serialize_none_as_null=True,
            )
            if raw_content is None:
                _error_msg_no_content = (
                    f"Exchange action ({request_payload_model.type}) returned no content."
                )
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

            exchange_response = self._response_handler.handle_exchange_response(
                cast(RawJsonResponse, raw_content),
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
        """
        Private method to fetch raw open orders.
        Returns a list of HyperliquidRawOpenOrder Pydantic models.
        """
        _error_msg_wallet_addr = "Wallet address is required to fetch open orders."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model = HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders", user=self._wallet_address
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
                    raw_response_content, user_address=self._wallet_address
                )
            )
            return validated_response.items
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error fetching open orders raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error fetching open orders raw: {e}"
            )
            _error_msg_unexpected = f"Unexpected error fetching open orders raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _get_order_status_raw(
        self,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrder | None:
        """
        Private method to fetch raw order status.
        Returns a HyperliquidRawHistoricalOrder Pydantic model or None if not found.
        The actual response from HL for orderStatus is a HyperliquidRawHistoricalOrderResponse,
        which contains the HyperliquidRawHistoricalOrder.
        """
        _error_msg_wallet_addr = "Wallet address is required to fetch order status."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model: HyperliquidRawOrderStatusRequestPayload = (
            self._request_builder.build_order_status_payload(
                wallet_address=self._wallet_address, order_id=order_id
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
                    f"for OID {order_id}."
                )
                raise APIError(
                    message=f"No data received for order status for OID {order_id}.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            historical_order_response: HyperliquidRawHistoricalOrderResponse = (
                self._response_handler.handle_info_order_status_response(
                    raw_response_content, user_address=self._wallet_address, order_id=order_id
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
                f"{e.message}"
            )
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error fetching order status raw for OID "
                f"{order_id}: {e}"
            )
            _error_msg_unexpected = f"Unexpected error fetching order status raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def get_order(self, symbol: str | None, order_id: str | int) -> Order | None:
        """Retrieves a specific order by ID for a given symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided."
            )
        if not order_id:
            raise ValueError(f"[{current_method}] 'order_id' must be a non-empty value.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            if isinstance(order_id, str):
                try:
                    order_id_int = int(order_id)
                except ValueError as e:
                    raise ValueError(
                        f"[{current_method}] 'order_id' must be a valid integer, got '{order_id}'"
                    ) from e
            else:
                order_id_int = order_id

            raw_historical_order = await self._get_order_status_raw(order_id_int)
            if raw_historical_order is None:
                return None

            # Use trading mapper to convert to internal order
            internal_order = self._trading_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order, trigger=None
            )
            return internal_order

        except APIError:
            # Re-raise APIErrors from _get_order_status_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order {order_id}: {e_transform}",
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
                f"failed for order {order_id}: {e_val}",
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
                f"for order {order_id}: {e_service_logic}",
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
                f"for order {order_id}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal | None,
        time_in_force: TimeInForce,
        stop_price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """
        Places an order and maps the raw response to an internal Order model.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_order"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")
        if not quantity.is_finite() or quantity <= 0:
            raise ValueError(f"[{current_method}] 'quantity' must be a positive finite Decimal.")

        # Handle optional price - use Decimal("0") for market orders
        order_price = price if price is not None else Decimal("0")

        if not order_price.is_finite() or order_price < 0:
            raise ValueError(f"[{current_method}] 'price' must be a non-negative finite Decimal.")
        if stop_price is not None and (not stop_price.is_finite() or stop_price <= 0):
            raise ValueError(
                f"[{current_method}] 'stop_price' must be a positive finite Decimal when provided."
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            asset_index = await self._get_asset_index_callable(symbol)
            if asset_index is None:
                raise APIError(
                    f"Asset index for {symbol} not found.", APIErrorCode.INVALID_SYMBOL.value
                )

            # Use the request builder to create the proper payload format
            place_order_payload = self._request_builder.build_place_order_payload(
                asset_index=asset_index,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=order_price,
                stop_price=stop_price,
                client_order_id=client_order_id,
                reduce_only=reduce_only,
                post_only=post_only,
            )

            raw_exchange_response, http_status = await self._place_order_raw(place_order_payload)
            status_code = http_status

            if raw_exchange_response.data and raw_exchange_response.data.statuses:
                first_status = raw_exchange_response.data.statuses[0]
                if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                    if first_status.resting:
                        new_oid = first_status.resting.oid
                        logger.info(
                            f"Order placed with OID: {new_oid}. Re-fetching for full details."
                        )
                        internal_order = await self.get_order(symbol=symbol, order_id=new_oid)
                        if internal_order:
                            return internal_order
                        else:
                            raise APIError(
                                f"Order placed (OID {new_oid}) but failed to re-fetch details.",
                                APIErrorCode.UNKNOWN.value,
                            )

                    elif first_status.filled:
                        filled_oid = first_status.filled.oid
                        logger.info(
                            f"Order OID {filled_oid} filled immediately. "
                            f"Re-fetching for full details."
                        )
                        internal_order = await self.get_order(symbol=symbol, order_id=filled_oid)
                        if internal_order:
                            internal_order.status = OrderStatus.FILLED
                            internal_order.quantity_filled = (
                                parse_decimal_value(
                                    first_status.filled.total_sz,
                                    allow_none=False,
                                    field_name="totalSz",
                                )
                                or quantity
                            )
                            internal_order.average_fill_price = parse_decimal_value(
                                first_status.filled.avg_px, allow_none=False, field_name="avgPx"
                            )
                            return internal_order
                        else:
                            raise APIError(
                                f"Order filled (OID {filled_oid}) but failed to re-fetch details.",
                                APIErrorCode.UNKNOWN.value,
                            )
                    elif first_status.error:
                        # Use the error mapper to get the specific error code for this message
                        mapped_error = self._error_mapper.map_string_error(
                            first_status.error, http_status=http_status
                        )
                        raise mapped_error
                elif "error" in first_status.lower():
                    raise APIError(
                        f"Failed to place order: {first_status}", APIErrorCode.UNKNOWN.value
                    )

            raise APIError("Failed to place order or parse response.", APIErrorCode.UNKNOWN.value)

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
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
                f"failed for {symbol}: {e_val}",
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
                f"for {symbol}: {e_service_logic}",
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
                f"for {symbol}: {e_unexpected}",
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
        """
        Retrieves all open orders, optionally filtered by symbol, and maps them
        to a list of internal Order models.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided."
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
                            raw_order=raw_order_details, trigger=raw_trigger_details
                        )
                        internal_orders.append(mapped_order)
                    except Exception as e:
                        logger.error(
                            f"[{self._exchange_name}] Error mapping raw open order to "
                            f"internal: {e}. Raw order: {raw_order_details.model_dump_json()}"
                        )
                        # Continue processing other orders

            logger.info(
                f"[{self._exchange_name}] Retrieved {len(internal_orders)} open orders "
                f"(symbol filter: {symbol})"
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

    async def cancel_order(self, symbol: str | None, order_id: str | int) -> bool:
        """
        Cancels a specific order and returns True if successful.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_order"

        if symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' parameter is required.")
        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Convert order_id to int if it's a string
        if isinstance(order_id, str):
            try:
                order_id_int = int(order_id)
            except ValueError as e:
                raise ValueError(
                    f"[{current_method}] 'order_id' must be a valid integer: {order_id}"
                ) from e
        else:
            order_id_int = order_id

        if order_id_int <= 0:
            raise ValueError(f"[{current_method}] 'order_id' must be positive.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic - need asset index for cancellation
            asset_index = await self._get_asset_index_callable(symbol)
            if asset_index is None:
                raise APIError(
                    f"Asset index for {symbol} not found for cancellation.",
                    APIErrorCode.INVALID_SYMBOL.value,
                )

            cancel_action = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id_int)
            raw_exchange_response, http_status = await self._cancel_order_raw(cancel_action)
            status_code = http_status

            if raw_exchange_response.data and raw_exchange_response.data.statuses:
                first_status = raw_exchange_response.data.statuses[0]
                if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                    if first_status.error:
                        # Use the error mapper to get the specific error code for this message
                        mapped_error = self._error_mapper.map_string_error(
                            first_status.error, http_status=http_status
                        )
                        raise mapped_error
                    else:
                        logger.info(
                            f"[{self._exchange_name}] Successfully cancelled order "
                            f"OID {order_id_int}"
                        )
                        return True
                elif "error" in first_status.lower():
                    raise APIError(
                        f"Failed to cancel order: {first_status}", APIErrorCode.UNKNOWN.value
                    )
                else:
                    logger.info(
                        f"[{self._exchange_name}] Successfully cancelled order OID {order_id_int}"
                    )
                    return True

            # If we reach here, something unexpected happened
            raise APIError("Failed to cancel order or parse response.", APIErrorCode.UNKNOWN.value)

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order {order_id_int}: {e_transform}",
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
                f"failed for order {order_id_int}: {e_val}",
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
                f"for order {order_id_int}: {e_service_logic}",
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
                f"for order {order_id_int}: {e_unexpected}",
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
        """
        Cancels all open orders, optionally filtered by symbol.
        Returns a list of CancelOrderResult for each attempted cancellation.
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
            _error_msg_wallet_addr = "Wallet address is required to cancel all orders."
            if not self._wallet_address:
                raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

            open_orders_internal = await self.get_open_orders(symbol=symbol)
            if not open_orders_internal:
                logger.info(
                    f"[{self._exchange_name}] No open orders found matching symbol "
                    f"'{symbol if symbol else 'any'}' to cancel."
                )
                return []

            results: list[CancelOrderResult] = []
            active_cancels_count = 0

            for order_to_cancel in open_orders_internal:
                if order_to_cancel.exchange_order_id is None:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping cancellation for order without "
                        f"exchange_order_id: ClientOID {order_to_cancel.client_order_id}, "
                        f"Symbol {order_to_cancel.symbol}"
                    )
                    results.append(
                        CancelOrderResult(
                            symbol=order_to_cancel.symbol,
                            order_id=None,
                            client_order_id=order_to_cancel.client_order_id,
                            success=False,
                            message="Order has no exchange_order_id, cannot be canceled by ID.",
                            status=CancelOrderResultStatus.FAILED,
                        )
                    )
                    continue

                order_id_to_cancel_str = order_to_cancel.exchange_order_id
                order_symbol_for_cancel = order_to_cancel.symbol

                try:
                    order_id_int = int(order_id_to_cancel_str)
                    active_cancels_count += 1
                    logger.debug(
                        f"Attempting to cancel order {order_id_int} for symbol "
                        f"{order_symbol_for_cancel}"
                    )

                    success_flag = await self.cancel_order(
                        symbol=order_symbol_for_cancel, order_id=order_id_int
                    )

                    results.append(
                        CancelOrderResult(
                            symbol=order_symbol_for_cancel,
                            order_id=str(order_id_int),
                            client_order_id=order_to_cancel.client_order_id,
                            success=success_flag,
                            message="Successfully canceled."
                            if success_flag
                            else "Failed to cancel via API.",
                            status=CancelOrderResultStatus.SUCCESS
                            if success_flag
                            else CancelOrderResultStatus.FAILED,
                        )
                    )
                except ValueError:
                    logger.error(
                        f"[{self._exchange_name}] Invalid order_id format '{order_id_to_cancel_str}' "
                        f"for cancellation."
                    )
                    results.append(
                        CancelOrderResult(
                            symbol=order_symbol_for_cancel,
                            order_id=order_id_to_cancel_str,
                            client_order_id=order_to_cancel.client_order_id,
                            success=False,
                            message=f"Invalid order_id format: {order_id_to_cancel_str}.",
                            status=CancelOrderResultStatus.FAILED,
                        )
                    )
                except APIError as e_api:
                    logger.error(
                        f"[{self._exchange_name}] APIError cancelling order {order_id_to_cancel_str} "
                        f"for {order_symbol_for_cancel}: {e_api.message}"
                    )
                    results.append(
                        CancelOrderResult(
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
                    )
                except Exception as e_generic:
                    logger.exception(
                        f"[{self._exchange_name}] Unexpected error cancelling order "
                        f"{order_id_to_cancel_str} for {order_symbol_for_cancel}: {e_generic}"
                    )
                    results.append(
                        CancelOrderResult(
                            symbol=order_symbol_for_cancel,
                            order_id=order_id_to_cancel_str,
                            client_order_id=order_to_cancel.client_order_id,
                            success=False,
                            message=str(e_generic),
                            status=CancelOrderResultStatus.UNKNOWN,
                        )
                    )

            if active_cancels_count == 0 and len(open_orders_internal) > 0:
                logger.info(
                    f"[{self._exchange_name}] Found {len(open_orders_internal)} orders but none had "
                    f"valid exchange_order_id for cancellation attempt."
                )

            return results

        except APIError:
            # Re-raise APIErrors from get_open_orders, cancel_order, etc.
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
