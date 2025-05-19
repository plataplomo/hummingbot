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
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrder,
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
    ) -> HyperliquidRawExchangeResponse:
        if not self._wallet_address:
            raise APIError("Wallet address is required.", APIErrorCode.AUTHENTICATION_FAILED.value)

        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            raise APIError(
                f"Asset index for {symbol} not found.", APIErrorCode.INVALID_SYMBOL.value
            )

        effective_tif: TimeInForce = TimeInForce.GTC
        stop_px_for_builder: Decimal | None = None
        post_only_for_builder: bool = False

        if time_in_force_options:
            tif_type_str = str(time_in_force_options.get("type", "Gtc")).upper()
            if tif_type_str == "GTC":
                effective_tif = TimeInForce.GTC
            elif tif_type_str == "IOC":
                effective_tif = TimeInForce.IOC
            elif tif_type_str == "ALO":
                effective_tif = TimeInForce.ALO
                post_only_for_builder = True
            elif tif_type_str in ("TP", "SL"):
                effective_tif = TimeInForce.GTC
                trigger_px_val = time_in_force_options.get("triggerPx")
                if trigger_px_val is None:
                    raise ValueError(f"triggerPx is required for {tif_type_str} TIF.")
                stop_px_for_builder = Decimal(str(trigger_px_val))

        order_request_payload = self._request_builder.build_place_order_payload(
            asset_index=asset_index,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=effective_tif,
            stop_price=stop_px_for_builder,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            post_only=post_only_for_builder,
        )

        actions_payload = order_request_payload.model_dump(by_alias=True)

        try:
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_payload, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]

            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for place_order_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="order"
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error placing order raw: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error placing order raw: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error placing order raw: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_order_raw(self, symbol: str, order_id: int) -> HyperliquidRawExchangeResponse:
        if not self._wallet_address:
            raise APIError("Wallet address is required.", APIErrorCode.AUTHENTICATION_FAILED.value)
        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            raise APIError(
                f"Asset index for {symbol} not found.", APIErrorCode.INVALID_SYMBOL.value
            )

        action_item = self._request_builder.build_cancel_order_payload(
            asset_index=asset_index,
            order_id=order_id,
        )
        actions_list = [action_item]
        try:
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]
            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for cancel_order_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="cancel"
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error cancelling order raw: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error cancelling order raw: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error cancelling order raw: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def get_open_orders_raw(self) -> list[HyperliquidRawOrder]:
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for open orders.", APIErrorCode.AUTHENTICATION_FAILED.value
            )

        request_payload_model = HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders", user=self._wallet_address
        )

        try:
            raw_response = await self._info_http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=False,
            )
            validated_response: HyperliquidRawOpenOrdersResponse = (
                self._response_handler.handle_info_open_orders_response(
                    raw_response, user_address=self._wallet_address
                )
            )
            return [open_order_item.order for open_order_item in validated_response.items]
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error getting open orders raw: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error getting open orders raw: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting open orders raw: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def get_order_status_raw(
        self, symbol: str, order_id: int
    ) -> HyperliquidRawHistoricalOrder | None:
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for order status.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        payload = self._request_builder.build_order_status_payload(
            wallet_address=self._wallet_address, order_id=order_id
        )
        try:
            raw_response = await self._info_http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=payload.model_dump(by_alias=True),
                is_signed=False,
            )
            validated_response: HyperliquidRawHistoricalOrderResponse | None
            try:
                validated_response = self._response_handler.handle_info_order_status_response(
                    raw_response, user_address=self._wallet_address, order_id=order_id
                )
            except APIError as e_handler:
                if e_handler.code == APIErrorCode.ORDER_NOT_FOUND.value:
                    logger.info(
                        f"[{self._exchange_name}] Order {order_id} for {symbol} not found "
                        f"by handler. Error: {e_handler.message}"
                    )
                    return None
                raise

            if validated_response:
                return validated_response.order
            return None

        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error getting order status raw: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error getting order status raw for {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting order status raw for {order_id}: {e}",
                APIErrorCode.UNKNOWN.value,
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
                raise ValueError("stop_price is required for stop orders.")
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
                            f"[{self._exchange_name}] Order placement failed. Error in status: {status_dict.error}"
                        )
                    # No OID to fetch if there's an error string in the status object.
                    # The error will be raised at the end of place_order if oid_to_fetch remains None.

                elif isinstance(status_dict, str):  # pyright: ignore [reportUnnecessaryIsInstance]
                    logger.debug(
                        f"[{self._exchange_name}] Order status is a string: {status_dict}, cannot extract OID."
                    )
                # Removed final 'else' here, as status_dict must be either HyperliquidRawExchangeStatusObject or str
                # due to the Union type of items in statuses list. If it's neither, Pydantic validation
                # of HyperliquidRawExchangeResponseData.statuses would have failed earlier.

                if oid_to_fetch is not None:
                    final_order_details = await self.get_order(symbol=symbol, order_id=oid_to_fetch)
                    if final_order_details:
                        return final_order_details
                    raise APIError(
                        f"Failed to fetch order status for {oid_to_fetch} after placement.",
                        APIErrorCode.ORDER_NOT_FOUND.value,
                    )

        error_message = f"Order placement status unclear. Raw response: {raw_exchange_response.model_dump_json() if raw_exchange_response else 'None'}"
        logger.error(f"[{self._exchange_name}] {error_message}")
        raise APIError(error_message, APIErrorCode.UNKNOWN.value)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        raw_open_orders_list = await self.get_open_orders_raw()
        internal_orders: list[Order] = []
        for raw_order in raw_open_orders_list:
            mapped_order = self._order_mapper.transform_raw_order_to_internal(raw=raw_order)
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
                            f"[{self._exchange_name}] Cancel for order {order_id} reported error: {status_val.error}"
                        )
                        return False

                    if status_val.resting and status_val.resting.oid == order_id:
                        logger.info(
                            f"[{self._exchange_name}] Cancel for order {order_id} has status 'resting' with matching OID."
                        )
                        return True

                    return True

        logger.warning(
            f"[{self._exchange_name}] Cancel order for {order_id} ({symbol}) failed or status unclear. "
            f"Raw response: {raw_exchange_response.model_dump_json() if raw_exchange_response else 'None'}"
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
                            f"[{self._exchange_name}] (Service) Order missing exchange_order_id: {order_to_cancel.client_order_id}"
                        )
                        results.append(
                            CancelOrderResult(
                                order_id=None,
                                client_order_id=order_to_cancel.client_order_id,
                                symbol=order_to_cancel.symbol,
                                success=False,
                                message="Missing exchange_order_id",
                                status=CancelOrderResultStatus.FAILED,
                            )
                        )
                        continue

                    hl_order_id_int = int(hl_order_id_str)
                    order_sym = order_to_cancel.symbol

                    if not order_sym:
                        logger.error(
                            f"[{self._exchange_name}] (Service) Order (ID: {hl_order_id_str}) missing symbol. Skipping."
                        )
                        results.append(
                            CancelOrderResult(
                                order_id=hl_order_id_str,
                                client_order_id=order_to_cancel.client_order_id,
                                symbol=None,
                                success=False,
                                message="Order missing symbol",
                                status=CancelOrderResultStatus.FAILED,
                            )
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
                        )
                    )
                except ValueError:
                    logger.error(
                        f"[{self._exchange_name}] (Service) Invalid exchange_order_id format for HL: {order_to_cancel.exchange_order_id}"
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=order_to_cancel.exchange_order_id,
                            client_order_id=order_to_cancel.client_order_id,
                            symbol=order_to_cancel.symbol,
                            success=False,
                            message="Invalid order ID format for cancellation.",
                            status=CancelOrderResultStatus.FAILED,
                        )
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
                        )
                    )
                except Exception as e_unexp:
                    logger.error(
                        f"[{self._exchange_name}] (Service) Unexpected error cancelling order {order_to_cancel.exchange_order_id}: {e_unexp}",
                        exc_info=True,
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=order_to_cancel.exchange_order_id,
                            client_order_id=order_to_cancel.client_order_id,
                            symbol=order_to_cancel.symbol,
                            success=False,
                            message=str(e_unexp),
                            status=CancelOrderResultStatus.FAILED,
                        )
                    )

        except APIError as e_fetch:
            logger.error(
                f"[{self._exchange_name}] (Service) APIError fetching open orders for cancel_all: {e_fetch.message}"
            )
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
                )
            )
        except Exception as e_outer:
            logger.error(
                f"[{self._exchange_name}] (Service) Unexpected error in cancel_all_orders: {e_outer}",
                exc_info=True,
            )
            results.append(
                CancelOrderResult(
                    order_id=None,
                    symbol=symbol,
                    success=False,
                    message=str(e_outer),
                    status=CancelOrderResultStatus.FAILED,
                )
            )
        return results

    async def cancel_orders_raw(
        self, cancels: list[HyperliquidCancelDetail]
    ) -> HyperliquidRawExchangeResponse:
        if not self._wallet_address:
            raise APIError("Wallet address is required.", APIErrorCode.AUTHENTICATION_FAILED.value)
        action_item = {"type": "batchCancel", "cancels": cancels}
        actions_list = [action_item]
        try:
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]
            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected batch cancel response format", APIErrorCode.INVALID_RESPONSE.value
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="batchCancel"
            )
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error in cancel_orders_raw: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error in cancel_orders_raw: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    # TODO: HyperliquidRequestBuilder is missing build_update_leverage_payload.
    # This method cannot be fully implemented without it or changing the approach.
    # Commenting out for now as it's outside the scope of place/cancel/get order methods.
    # async def update_leverage_raw(
    #     self, symbol: str, leverage: Decimal, is_cross_margin: bool
    # ) -> HyperliquidRawExchangeResponse:
    #     if not self._wallet_address:
    #         raise APIError("Wallet address is required.", APIErrorCode.AUTHENTICATION_FAILED.value)
    #     asset_index = await self._get_asset_index_callable(symbol)
    #     if asset_index is None:
    #         raise APIError(
    #             f"Asset index for {symbol} not found.", APIErrorCode.INVALID_SYMBOL.value
    #         )
    #     # This builder method does not exist currently
    #     action = self._request_builder.build_update_leverage_payload(
    #         asset_index=asset_index, is_cross_margin=is_cross_margin, leverage=int(leverage)
    #     )
    #     actions_list = [action]
    #     try:
    #         raw_response_data_tuple = await self._exchange_http_client_requester(
    #             method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
    #         )
    #         raw_response_data = raw_response_data_tuple[0]
    #         if not isinstance(raw_response_data, dict):
    #             raise APIError(
    #                 "Unexpected update leverage response format",
    #                 APIErrorCode.INVALID_RESPONSE.value,
    #             )
    #         return self._response_handler.handle_exchange_response(
    #             raw_response_data, action_type="updateLeverage"
    #         )
    #     except Exception as e:
    #         logger.error(
    #             f"[{self._exchange_name}] Error in update_leverage_raw: {e}", exc_info=True
    #         )
    #         raise APIError(
    #             f"Unexpected error in update_leverage_raw: {e}", APIErrorCode.UNKNOWN.value
    #         ) from e
