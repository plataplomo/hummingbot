"""
CyberDeltaEngine: Hyperliquid Trading Service
----------------------------------------------

This service encapsulates the logic for trading operations on the Hyperliquid Exchange.
It uses the HttpClient (via a requester callable), HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API.
This version of the service returns Internal Domain Models by using the HyperliquidOrderMapper.
"""

from collections.abc import Callable, Coroutine, Mapping
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
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
from cyberdelta.apis.hyperliquid.models.hl_raw_order import HyperliquidRawPlaceOrderAction
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Internal Domain Models & Mappers
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.logging_config import get_logger
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
        exchange_http_client_requester: HttpClientRequesterSig,
        info_http_client_requester: Callable[..., Coroutine[Any, Any, RawJsonResponse | None]],
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Coroutine[Any, Any, int | None]],
        order_mapper: HyperliquidOrderMapper,
    ) -> None:
        self._exchange_http_client_requester = exchange_http_client_requester
        self._info_http_client_requester = info_http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        self._get_asset_index_callable = get_asset_index_callable
        self._order_mapper = order_mapper
        self._action_endpoint = "/exchange"
        self._info_endpoint = "/info"

    async def _place_order_raw(
        self,
        place_order_action: HyperliquidRawPlaceOrderAction,
    ) -> HyperliquidRawExchangeResponse:
        """
        Private method to place an order, using the raw action model.
        The builder will wrap this action into the full HyperliquidApiPlaceOrderRequest.
        Returns the raw exchange response Pydantic model.
        """
        _error_msg_wallet_addr = "Wallet address is required for placing an order."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model: HyperliquidApiPlaceOrderRequest = HyperliquidApiPlaceOrderRequest(
            type="order", actions=[place_order_action]
        )

        try:
            raw_response_tuple = await self._exchange_http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=request_payload_model,
                is_signed=True,
            )
            raw_content = raw_response_tuple[0]
            if raw_content is None:
                _error_msg_no_content = (
                    f"Exchange action ({request_payload_model.type}) returned no content."
                )
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)
            return self._response_handler.handle_exchange_response(
                raw_content,
                action_type=request_payload_model.type,
            )
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
    ) -> HyperliquidRawExchangeResponse:
        """
        Private method to cancel an order, using the raw action model.
        The builder wraps this in HyperliquidApiCancelOrderRequest.
        Returns the raw exchange response Pydantic model.
        """
        _error_msg_wallet_addr = "Wallet address is required for cancelling an order."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model: HyperliquidApiCancelOrderRequest = HyperliquidApiCancelOrderRequest(
            type="cancel", action=cancel_action
        )

        try:
            raw_response_tuple = await self._exchange_http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=request_payload_model,
                is_signed=True,
            )
            raw_content = raw_response_tuple[0]
            if raw_content is None:
                _error_msg_no_content = (
                    f"Exchange action ({request_payload_model.type}) returned no content."
                )
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)
            return self._response_handler.handle_exchange_response(
                raw_content,
                action_type=request_payload_model.type,
            )
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
            raw_response_content = await self._info_http_client_requester(
                method="POST",
                endpoint_path=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                authenticator=self._authenticator,
                rate_limiter_service=None,
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
            raw_response_content = await self._info_http_client_requester(
                method="POST",
                endpoint_path=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                authenticator=self._authenticator,
                rate_limiter_service=None,
                is_signed=True,
            )
            if raw_response_content is None:
                logger.warning(
                    f"[{self._exchange_name}] Order status for OID {order_id} returned no content, "
                    f"likely not found."
                )
                return None

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

    async def get_order(self, symbol: str, order_id: int) -> Order | None:
        """
        Retrieves a specific order by its exchange ID and maps it to an internal Order model.
        Returns None if the order is not found.
        """
        raw_historical_order = await self._get_order_status_raw(order_id=order_id)
        if raw_historical_order is None:
            return None

        trigger_info = getattr(raw_historical_order, "trigger", None)

        return self._order_mapper.transform_raw_historical_order_to_internal(
            raw_historical_order=raw_historical_order, trigger=trigger_info
        )

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal,
        time_in_force: TimeInForce,
        stop_price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """
        Places an order and maps the raw response to an internal Order model.
        """
        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            _error_msg_asset_idx = f"Asset index for {symbol} not found."
            raise APIError(_error_msg_asset_idx, APIErrorCode.INVALID_SYMBOL.value)

        raw_limit_px_str = str(price)
        raw_sz_str = str(quantity)

        hl_order_type_obj: Any
        if order_type == OrderType.LIMIT or (post_only and order_type == OrderType.MARKET):
            tif_str_val = "Gtc"
            if time_in_force == TimeInForce.IOC:
                tif_str_val = "Ioc"
            if time_in_force == TimeInForce.ALO or post_only:
                tif_str_val = "Alo"
            hl_order_type_obj = {"limit": {"tif": tif_str_val}}
        elif order_type == OrderType.MARKET:
            hl_order_type_obj = {"market": {}}
        else:
            raise ValueError(
                f"Order type {order_type} basic mapping not fully implemented here, "
                f"use Limit/Market."
            )

        place_action = HyperliquidRawPlaceOrderAction(
            asset=asset_index,
            isBuy=(side == OrderSide.BUY),
            limitPx=raw_limit_px_str,
            sz=raw_sz_str,
            reduceOnly=reduce_only,
            orderType=hl_order_type_obj,
            cloid=client_order_id,
        )

        raw_exchange_response = await self._place_order_raw(place_action)

        if raw_exchange_response.data and raw_exchange_response.data.statuses:
            first_status = raw_exchange_response.data.statuses[0]
            if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                if first_status.resting:
                    new_oid = first_status.resting.oid
                    logger.info(f"Order placed with OID: {new_oid}. Re-fetching for full details.")
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
                        f"Order OID {filled_oid} filled immediately. Re-fetching for full details."
                    )
                    internal_order = await self.get_order(symbol=symbol, order_id=filled_oid)
                    if internal_order:
                        internal_order.status = OrderStatus.FILLED
                        internal_order.quantity_filled = (
                            parse_decimal_value(
                                first_status.filled.total_sz, allow_none=False, field_name="totalSz"
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
                    raise APIError(
                        f"Failed to place order: {first_status.error}", APIErrorCode.UNKNOWN.value
                    )
            elif "error" in first_status.lower():
                raise APIError(f"Failed to place order: {first_status}", APIErrorCode.UNKNOWN.value)

        raise APIError("Failed to place order or parse response.", APIErrorCode.UNKNOWN.value)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Retrieves all open orders, optionally filtered by symbol, and maps them
        to a list of internal Order models.
        """
        raw_open_orders = await self._get_open_orders_raw()
        internal_orders: list[Order] = []
        for raw_open_order_item_wrapper in raw_open_orders:
            raw_order_details = raw_open_order_item_wrapper.order
            raw_trigger_details = raw_open_order_item_wrapper.trigger

            if symbol is None or raw_order_details.asset.upper() == symbol.upper():
                try:
                    mapped_order = self._order_mapper.transform_raw_order_to_internal(
                        raw=raw_order_details, trigger=raw_trigger_details
                    )
                    internal_orders.append(mapped_order)
                except Exception as e:
                    logger.error(
                        f"[{self._exchange_name}] Error mapping raw open order to internal: {e}. "
                        f"Raw order: {raw_order_details.model_dump_json()}"
                    )
        return internal_orders

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        """
        Cancels a specific order and returns True if successful, False otherwise.
        """
        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            _error_msg_asset_idx = f"Asset index for {symbol} not found for cancellation."
            raise APIError(_error_msg_asset_idx, APIErrorCode.INVALID_SYMBOL.value)

        cancel_action = HyperliquidRawCancelOrderAction(asset=asset_index, oid=order_id)
        raw_exchange_response = await self._cancel_order_raw(cancel_action)

        if raw_exchange_response.data and raw_exchange_response.data.statuses:
            first_status = raw_exchange_response.data.statuses[0]
            if isinstance(first_status, str) and first_status.lower() == "success":
                return True
            if isinstance(first_status, HyperliquidRawExchangeStatusObject):
                if first_status.success:
                    return True
                if first_status.error:
                    logger.error(
                        f"[{self._exchange_name}] Failed to cancel order {order_id} for {symbol}: "
                        f"{first_status.error}"
                    )
                    return False
            logger.warning(
                f"[{self._exchange_name}] Ambiguous cancel response "
                f"for order {order_id} ({symbol}): {first_status}"
            )
            return False

        logger.error(
            f"[{self._exchange_name}] Failed to cancel order {order_id} for {symbol} "
            f"or parse response."
        )
        return False

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """
        Cancels all open orders, optionally filtered by symbol.
        Returns a list of CancelOrderResult for each attempted cancellation.
        """
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
                        raw_response={"error_code": e_api.code, "http_status": e_api.http_status},
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
