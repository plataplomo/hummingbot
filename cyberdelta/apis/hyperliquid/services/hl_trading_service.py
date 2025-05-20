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
from typing import Any, TypedDict

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Internal Domain Models & Mappers
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


HttpClientRequesterSig = Callable[
    ..., Coroutine[Any, Any, tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class HyperliquidCancelDetail(TypedDict):
    asset: int
    oid: int


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

    async def place_order_raw(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force_options: dict[str, Any] | None = None,
        client_order_id: str | None = None,
        post_only: bool = False,
    ) -> HyperliquidRawExchangeResponse:
        _error_msg_wallet_addr = "Wallet address is required."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            _error_msg_asset_idx = f"Asset index for {symbol} not found."
            raise APIError(_error_msg_asset_idx, APIErrorCode.INVALID_SYMBOL.value)

        tif_enum = TimeInForce.GTC
        stop_px_for_builder: Decimal | None = None

        if time_in_force_options:
            raw_tif_str = str(time_in_force_options.get("type", "Gtc")).upper()
            if raw_tif_str == "IOC":
                tif_enum = TimeInForce.IOC
            elif raw_tif_str == "FOK":
                tif_enum = TimeInForce.ALO
            elif raw_tif_str == "ALO":
                tif_enum = TimeInForce.ALO

            if order_type in [OrderType.TAKE_PROFIT_MARKET, OrderType.STOP_MARKET]:
                raw_trigger_px = time_in_force_options.get("triggerPx")
                if raw_trigger_px is not None:
                    try:
                        stop_px_for_builder = Decimal(str(raw_trigger_px))
                    except Exception as e:
                        _error_msg_trigger_px = (
                            f"Invalid triggerPx '{raw_trigger_px}' in time_in_force_options: {e}"
                        )
                        raise ValueError(_error_msg_trigger_px) from e

        limit_price_for_builder = price

        request_payload_model = self._request_builder.build_place_order_payload(
            asset_index=asset_index,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=tif_enum,
            price=limit_price_for_builder,
            stop_price=stop_px_for_builder,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            post_only=post_only,
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

    async def cancel_order_raw(self, symbol: str, order_id: int) -> HyperliquidRawExchangeResponse:
        _error_msg_wallet_addr = "Wallet address is required."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            _error_msg_asset_idx = f"Asset index for {symbol} not found."
            raise APIError(_error_msg_asset_idx, APIErrorCode.INVALID_SYMBOL.value)

        request_payload_model = self._request_builder.build_cancel_order_payload(
            asset_index=asset_index,
            order_id=order_id,
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

    async def get_open_orders_raw(self) -> list[HyperliquidRawOpenOrder]:
        _error_msg_wallet_addr = "Wallet address required for open orders."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        request_payload_model = HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders",
            user=self._wallet_address,
        )

        try:
            raw_response_content = await self._info_http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=False,
            )

            if raw_response_content is None:
                logger.warning(
                    f"[{self._exchange_name}] Get open orders raw returned None content."
                )
                _error_msg_no_content = "Open orders request returned no content."
                raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

            validated_response: HyperliquidRawOpenOrdersResponse = (
                self._response_handler.handle_info_open_orders_response(
                    raw_response_content,
                    user_address=self._wallet_address,
                )
            )
            return validated_response.root
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error getting open orders raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error getting open orders raw: {e}",
            )
            _error_msg_unexpected = f"Unexpected error getting open orders raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def get_order_status_raw(
        self,
        symbol: str,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrder | None:
        _error_msg_wallet_addr = "Wallet address required for order status."
        if not self._wallet_address:
            raise APIError(
                _error_msg_wallet_addr,
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        request_payload_model = self._request_builder.build_order_status_payload(
            wallet_address=self._wallet_address,
            order_id=order_id,
        )
        try:
            raw_response_content = await self._info_http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=False,
            )

            if raw_response_content is None:
                logger.info(
                    f"Order status for oid {order_id} returned None. Assuming not found or error."
                )
                # Pass # No specific error here, handler will check

            try:
                validated_response = self._response_handler.handle_info_order_status_response(
                    raw_response_content,
                    user_address=self._wallet_address,
                    order_id=order_id,
                )
                return validated_response.order if validated_response else None
            except APIError as e_handler:
                if e_handler.code == APIErrorCode.ORDER_NOT_FOUND.value:
                    logger.info(f"Order {order_id} not found by handler for symbol {symbol}.")
                    return None
                raise

        except APIError as e_req:
            logger.exception(
                f"APIError requesting order status for oid {order_id} ({symbol}): {e_req}",
            )
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Error getting order status for {symbol}, "
                f"oid {order_id}: {e}"
            )
            _error_msg_unexpected = (
                f"Unexpected error getting order status for {order_id} ({symbol}): {e}"
            )
            raise APIError(
                message=_error_msg_unexpected,
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_order(self, symbol: str, order_id: int) -> Order | None:
        raw_historical_order = await self.get_order_status_raw(symbol=symbol, order_id=order_id)
        if raw_historical_order:
            return self._order_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order,
            )
        return None

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
        tif_options: dict[str, Any] = {"type": time_in_force.value}
        if post_only and time_in_force == TimeInForce.GTC:
            tif_options["type"] = "Alo"

        if order_type in [OrderType.STOP_LIMIT, OrderType.STOP_MARKET] or stop_price is not None:
            if stop_price is None:
                _error_msg_stop_price = "stop_price is required for stop orders."
                raise ValueError(_error_msg_stop_price)
            tif_options["triggerPx"] = str(stop_price)
            tif_options["isMarket"] = order_type == OrderType.STOP_MARKET

        raw_exchange_response = await self.place_order_raw(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            reduce_only=reduce_only,
            time_in_force_options=tif_options,
            client_order_id=client_order_id,
            post_only=post_only,
        )

        if raw_exchange_response.status == "ok" and raw_exchange_response.data:
            order_statuses = raw_exchange_response.data.statuses
            if order_statuses and order_statuses[0]:
                status_dict = order_statuses[0]
                oid_to_fetch: int | None = None
                if isinstance(status_dict, HyperliquidRawExchangeStatusObject):
                    if status_dict.resting:
                        oid_to_fetch = status_dict.resting.oid
                    elif status_dict.filled:
                        oid_to_fetch = status_dict.filled.oid
                    elif status_dict.error:
                        logger.error(
                            f"[{self._exchange_name}] Order placement failed. "
                            f"Error in status: {status_dict.error}"
                        )

                elif isinstance(status_dict, str):  # pyright: ignore [reportUnnecessaryIsInstance]
                    # This is an unexpected format, should be a dict with 'resting' or 'error'
                    logger.debug(
                        f"[{self._exchange_name}] Order status is a string: {status_dict}, "
                        "cannot extract OID."
                    )

                if oid_to_fetch is not None:
                    final_order_details = await self.get_order(symbol=symbol, order_id=oid_to_fetch)
                    if final_order_details:
                        return final_order_details
                    _error_msg_fetch_failed = (
                        f"Failed to fetch order status for {oid_to_fetch} after placement."
                    )
                    raise APIError(
                        _error_msg_fetch_failed,
                        APIErrorCode.ORDER_NOT_FOUND.value,
                    )

        error_message_unclear = (
            f"Order placement status unclear. Raw response: "
            f"{raw_exchange_response.model_dump_json() if raw_exchange_response else 'None'}"
        )
        logger.error(f"[{self._exchange_name}] {error_message_unclear}")
        raise APIError(error_message_unclear, APIErrorCode.UNKNOWN.value)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        raw_open_orders_list = await self.get_open_orders_raw()
        internal_orders: list[Order] = []
        for raw_open_order_item in raw_open_orders_list:
            mapped_order = self._order_mapper.transform_raw_order_to_internal(
                raw=raw_open_order_item.order,
                trigger=raw_open_order_item.trigger,
            )
            if mapped_order:
                if symbol is None or mapped_order.symbol == symbol:
                    internal_orders.append(mapped_order)
        return internal_orders

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        raw_exchange_response = await self.cancel_order_raw(symbol=symbol, order_id=order_id)

        if raw_exchange_response.status == "ok" and raw_exchange_response.data:
            cancel_statuses = raw_exchange_response.data.statuses
            if cancel_statuses and cancel_statuses[0]:
                status_val = cancel_statuses[0]
                if isinstance(status_val, str) and status_val == "canceled":
                    return True
                if isinstance(status_val, HyperliquidRawExchangeStatusObject):
                    if status_val.error:
                        logger.warning(
                            f"[{self._exchange_name}] Cancel for order {order_id} "
                            f"reported error: {status_val.error}"
                        )
                        return False

                    if status_val.resting and status_val.resting.oid == order_id:
                        logger.info(
                            f"[{self._exchange_name}] Cancel for order {order_id} "
                            "has status 'resting' with matching OID."
                        )
                        return True  # Explicitly True if resting OID matches

                    return True  # Default to True if status object exists without error

        logger.warning(
            f"[{self._exchange_name}] Cancel order for {order_id} ({symbol}) failed or "
            f"status unclear. Raw response: "
            f"{raw_exchange_response.model_dump_json() if raw_exchange_response else 'None'}"
        )
        return False

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        results: list[CancelOrderResult] = []
        logger.info(
            f"[{self._exchange_name}] (Service) Attempting to cancel all open orders"
            f"{f' for symbol {symbol}' if symbol else ''}."
        )
        try:
            open_orders_to_cancel = await self.get_open_orders(symbol=symbol)
            if not open_orders_to_cancel:
                logger.info(f"[{self._exchange_name}] (Service) No open orders found to cancel.")
                return []

            for order_to_cancel in open_orders_to_cancel:
                try:
                    hl_order_id_str = order_to_cancel.exchange_order_id
                    if hl_order_id_str is None:
                        logger.warning(
                            f"[{self._exchange_name}] (Service) Order missing exchange_order_id: "
                            f"{order_to_cancel.client_order_id}",
                        )
                        results.append(
                            CancelOrderResult(
                                order_id=None,
                                client_order_id=order_to_cancel.client_order_id,
                                symbol=order_to_cancel.symbol,
                                success=False,
                                message="Missing exchange_order_id",
                                status=CancelOrderResultStatus.FAILED,
                            ),
                        )
                        continue

                    hl_order_id_int = int(hl_order_id_str)
                    order_sym = order_to_cancel.symbol

                    if not order_sym:
                        logger.error(
                            f"[{self._exchange_name}] (Service) Order (ID: {hl_order_id_str}) "
                            "missing symbol. Skipping.",
                        )
                        results.append(
                            CancelOrderResult(
                                order_id=hl_order_id_str,
                                client_order_id=order_to_cancel.client_order_id,
                                symbol=None,
                                success=False,
                                message="Order missing symbol",
                                status=CancelOrderResultStatus.FAILED,
                            ),
                        )
                        continue

                    cancelled = await self.cancel_order(symbol=order_sym, order_id=hl_order_id_int)
                    results.append(
                        CancelOrderResult(
                            order_id=hl_order_id_str,
                            client_order_id=order_to_cancel.client_order_id,
                            symbol=order_sym,
                            success=cancelled,
                            message="Cancelled by service."
                            if cancelled
                            else "Failed to cancel via service.",
                            status=CancelOrderResultStatus.SUCCESS
                            if cancelled
                            else CancelOrderResultStatus.FAILED,
                        ),
                    )
                except ValueError:
                    err_msg_invalid_oid = (
                        f"Invalid exchange_order_id format for HL: "
                        f"{order_to_cancel.exchange_order_id}"
                    )
                    logger.error(f"[{self._exchange_name}] (Service) {err_msg_invalid_oid}")
                    results.append(
                        CancelOrderResult(
                            order_id=order_to_cancel.exchange_order_id,
                            client_order_id=order_to_cancel.client_order_id,
                            symbol=order_to_cancel.symbol,
                            success=False,
                            message=err_msg_invalid_oid,
                            status=CancelOrderResultStatus.FAILED,
                        ),
                    )
                except APIError as e_cancel:
                    results.append(
                        CancelOrderResult(
                            order_id=order_to_cancel.exchange_order_id,
                            client_order_id=order_to_cancel.client_order_id,
                            symbol=order_to_cancel.symbol,
                            success=False,
                            message=e_cancel.message,
                            status=CancelOrderResultStatus.FAILED,
                            raw_response=getattr(e_cancel.original_exception, "response_body", None)
                            if isinstance(e_cancel.original_exception, APIError)
                            else None,
                        ),
                    )
                except Exception as e_unexp:
                    log_message = (
                        f"[{self._exchange_name}] (Service) Unexpected error cancelling order "
                        f"{order_to_cancel.exchange_order_id}: {e_unexp}"
                    )
                    logger.exception(log_message)
                    results.append(
                        CancelOrderResult(
                            order_id=order_to_cancel.exchange_order_id,
                            client_order_id=order_to_cancel.client_order_id,
                            symbol=order_to_cancel.symbol,
                            success=False,
                            message=str(e_unexp),
                            status=CancelOrderResultStatus.FAILED,
                        ),
                    )

        except APIError as e_fetch:
            log_message_fetch_err = (
                f"[{self._exchange_name}] (Service) APIError fetching open orders "
                f"for cancel_all: {e_fetch.message}"
            )
            logger.exception(log_message_fetch_err)  # Use .exception for APIError as well
            results.append(
                CancelOrderResult(
                    order_id=None,
                    symbol=symbol,
                    success=False,
                    message=f"Failed to fetch open orders: {e_fetch.message}",
                    status=CancelOrderResultStatus.FAILED,
                    raw_response=getattr(e_fetch.original_exception, "response_body", None)
                    if isinstance(e_fetch.original_exception, APIError)
                    else None,
                ),
            )
        except Exception as e_outer:
            log_message_outer_err = (
                f"[{self._exchange_name}] (Service) Unexpected error in cancel_all_orders: "
                f"{e_outer}"
            )
            logger.exception(log_message_outer_err)
            results.append(
                CancelOrderResult(
                    order_id=None,
                    symbol=symbol,
                    success=False,
                    message=str(e_outer),
                    status=CancelOrderResultStatus.FAILED,
                ),
            )
        return results

    async def cancel_orders_raw(
        self,
        cancels: list[HyperliquidCancelDetail],
    ) -> HyperliquidRawExchangeResponse:
        """Cancels a batch of orders using their asset index and order ID."""
        _error_msg_wallet_addr = "Wallet address is required."
        if not self._wallet_address:
            raise APIError(_error_msg_wallet_addr, APIErrorCode.AUTHENTICATION_FAILED.value)

        # 1. Construct the specific action payload for batch cancellation
        batch_cancel_action_item = {"type": "batchCancel", "cancels": cancels}

        # 2. Use the RequestBuilder to create the full "execute" envelope.
        #    This service expects the builder to have a method (e.g., build_execute_actions_envelope)
        #    that takes a list of action items and returns the complete Pydantic model
        #    for the request body (e.g., HyperliquidActionEnvelope).
        #    If this method is missing from HyperliquidRequestBuilder, linter errors on the next line
        #    will correctly indicate that the builder needs to be updated.
        request_payload_model = self._request_builder.build_execute_actions_envelope(
            actions=[batch_cancel_action_item]
        )

        try:
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=request_payload_model,  # Pass the Pydantic model from the builder
                is_signed=True,
            )
            raw_response_data = raw_response_data_tuple[0]
            if not isinstance(raw_response_data, dict):
                _err_msg_batch_cancel_format = "Unexpected batch cancel response format"
                raise APIError(
                    _err_msg_batch_cancel_format,
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data,
                action_type="batchCancel",
            )
        except Exception as e:
            logger.exception(f"[{self._exchange_name}] Error in cancel_orders_raw: {e}")
            _err_msg_unexpected_batch_cancel = f"Unexpected error in cancel_orders_raw: {e}"
            raise APIError(
                _err_msg_unexpected_batch_cancel,
                APIErrorCode.UNKNOWN.value,
            ) from e
