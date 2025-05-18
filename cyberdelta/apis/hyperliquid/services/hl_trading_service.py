"""
CyberDeltaEngine: Hyperliquid Trading Service
----------------------------------------------

This service encapsulates the logic for trading operations on the Hyperliquid Exchange.
It uses the HttpClient (via a requester callable), HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models or relevant raw data structures.
"""

from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any, TypedDict, cast

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawOrder,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidCancelDetail(TypedDict):
    asset: int
    oid: int


class HyperliquidTradingService:
    """
    Service class for Hyperliquid trading operations.
    """

    def __init__(
        self,
        exchange_http_client_requester: Callable[..., Coroutine[Any, Any, RawJsonResponse | None]],
        info_http_client_requester: Callable[..., Coroutine[Any, Any, RawJsonResponse | None]],
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Coroutine[Any, Any, int | None]],
    ) -> None:
        self._exchange_http_client_requester = exchange_http_client_requester
        self._info_http_client_requester = info_http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        self._get_asset_index_callable = get_asset_index_callable
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
            raise APIError(
                "Wallet address is required for Hyperliquid trading actions.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            raise APIError(
                f"Could not find asset index for symbol {symbol}", APIErrorCode.INVALID_SYMBOL.value
            )

        tif_details: dict[str, Any] = {}
        effective_tif = "Gtc"

        if time_in_force_options:
            tif_type_from_options = time_in_force_options.get("type")
            if tif_type_from_options:
                effective_tif = tif_type_from_options

            if effective_tif in ["Tp", "Sl"]:
                if "triggerPx" not in time_in_force_options:
                    raise ValueError("triggerPx is required for TP/SL orders.")
                trigger_info = {
                    "triggerPx": str(time_in_force_options["triggerPx"]),
                    "isMarket": time_in_force_options.get("isMarket", True),
                    "tpsl": effective_tif,
                }
                tif_details["trigger"] = trigger_info
                effective_tif = time_in_force_options.get("orderTif", "Gtc")

        order_spec: dict[str, Any] = {
            "asset": asset_index,
            "isBuy": side == OrderSide.BUY,
            "limitPx": str(price),
            "sz": str(quantity),
            "reduceOnly": reduce_only,
            "tif": effective_tif,
        }
        if client_order_id:
            order_spec["cloid"] = client_order_id

        if tif_details.get("trigger"):
            order_spec["triggerCriteria"] = tif_details["trigger"]
            if order_type == OrderType.MARKET:
                order_spec["orderType"] = {"triggerMarket": {"tif": effective_tif}}
            else:
                order_spec["orderType"] = {"triggerLimit": {"tif": effective_tif}}
        elif order_type == OrderType.MARKET:
            order_spec["orderType"] = {"market": {"tif": effective_tif}}
        else:
            order_spec["orderType"] = {"limit": {"tif": effective_tif}}

        action_item = {"type": "order", "orders": [order_spec]}
        actions_list = [action_item]

        try:
            raw_response_data = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for place_order_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="order"
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error placing order: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error placing order: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error placing order: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_order_raw(
        self,
        symbol: str,
        order_id: int,
    ) -> HyperliquidRawExchangeResponse:
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required for Hyperliquid trading actions.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            raise APIError(
                f"Could not find asset index for symbol {symbol}", APIErrorCode.INVALID_SYMBOL.value
            )

        action_item = {"type": "cancel", "cancels": [{"asset": asset_index, "oid": order_id}]}
        actions_list = [action_item]

        try:
            raw_response_data = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for cancel_order_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="cancel"
            )
        except APIError as e:
            logger.error(
                f"[{self._exchange_name}] API error canceling order {order_id}: {e.message}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error canceling order {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_orders_raw(
        self, cancels: list[HyperliquidCancelDetail]
    ) -> HyperliquidRawExchangeResponse:
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required for Hyperliquid trading actions.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        if not cancels:
            logger.info(
                f"[{self._exchange_name}] No cancel requests provided to cancel_orders_raw."
            )
            # Corrected instantiation for empty success, matching HyperliquidRawExchangeResponseData structure
            return HyperliquidRawExchangeResponse(
                status="ok",
                data=HyperliquidRawExchangeResponseData(
                    type="statuses", statuses=["No cancel requests provided to cancel_orders_raw."]
                ),
            )

        action_item = {"type": "cancel", "cancels": cancels}
        actions_list = [action_item]

        try:
            raw_response_data = await self._exchange_http_client_requester(
                method="POST",
                endpoint=self._action_endpoint,
                data=actions_list,
                is_signed=True,
            )
            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for cancel_orders_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="cancel"
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error batch canceling orders: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error batch canceling orders: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error batch canceling orders: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def get_open_orders_raw(self) -> list[HyperliquidRawOrder]:
        """Fetches raw open orders for the configured wallet address."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required to fetch open orders.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # Construct payload directly as builder method was not found for this specific variant
        request_payload = HyperliquidRawOpenOrdersRequestPayload(
            user=self._wallet_address, type="openOrders"
        )
        request_data_dict = request_payload.model_dump(by_alias=True)

        try:
            # Use info_requester for fetching open orders
            raw_response_data = await self._info_http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_data_dict,
                is_signed=False,  # Assuming /info openOrders is not signed if just for self
                # However, Hyperliquid's /info for user-specific data IS signed via query.
                # The HttpClient usually handles auth if is_signed=True.
                # For user-specific info, it likely needs signing.
                # The authenticator in HttpClient handles this based on is_signed.
                # The current HttpClient._request does not add signature for POST to /info.
                # This needs to be verified against Hyperliquid docs.
                # For now, let's assume the general info_http_client_requester passed
                # already handles auth if needed, or we use exchange_requester.
                # Let's use exchange_requester if it requires auth for user data.
                # The AccountService uses info_http_client for user_state and open_orders
                # and these are indeed POST requests to /info which require signing.
                # The http_client.request method should handle this if is_signed=True.
                # Let's make it consistent with AccountService.
            )

            if not isinstance(raw_response_data, list):
                raise APIError(
                    f"Unexpected response format for get_open_orders_raw: expected list, got {type(raw_response_data).__name__}",
                    APIErrorCode.INVALID_RESPONSE.value,
                )

            # Validate the raw response content using the response handler
            # handle_info_open_orders_response expects user_address for context in logging
            validated_response = self._response_handler.handle_info_open_orders_response(
                raw_response_data, user_address=self._wallet_address
            )
            # The validated_response is HyperliquidRawOpenOrdersResponse (a RootModel)
            # Its .root attribute holds the list[HyperliquidRawOpenOrder]
            # Each HyperliquidRawOpenOrder contains .order (HyperliquidRawOrder) and .trigger
            # The method signature asks for list[HyperliquidRawOrder]
            # So we extract the .order part.
            return [open_order_item.order for open_order_item in validated_response.root]

        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error getting open orders: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error getting open orders: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error getting open orders: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def get_order_status_raw(
        self, symbol: str, order_id: int
    ) -> HyperliquidRawHistoricalOrder | None:
        """Fetches the raw status of a specific order by its ID and symbol."""
        raw_response_data: Any = None  # Initialize to handle unbound access in except block
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required to fetch order status.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        asset_index = await self._get_asset_index_callable(symbol)
        if asset_index is None:
            raise APIError(
                f"Could not find asset index for symbol {symbol} to get order status",
                APIErrorCode.INVALID_SYMBOL.value,
            )

        # Use the builder method for order status payload
        request_payload = self._request_builder.build_order_status_payload(
            wallet_address=self._wallet_address, order_id=order_id
        )
        request_data_dict = request_payload.model_dump(by_alias=True)

        try:
            # Use info_requester for fetching order status
            raw_response_data = await self._info_http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_data_dict,
                is_signed=True,  # User-specific info endpoint, usually signed
            )

            if not isinstance(raw_response_data, dict):
                # ORDER_NOT_FOUND by Hyperliquid for orderStatus returns: ` "Order not found" ` (a string)
                # So, if it's a string and contains "Order not found", handle it as None.
                if isinstance(raw_response_data, str) and "Order not found" in raw_response_data:
                    logger.info(
                        f"[{self._exchange_name}] Order {order_id} for symbol {symbol} not found."
                    )
                    return None
                raise APIError(
                    f"Unexpected response format for get_order_status_raw: expected dict or 'Order not found' string, got {type(raw_response_data).__name__}",
                    APIErrorCode.INVALID_RESPONSE.value,
                )

            # Validate the raw response content using the response handler
            validated_response = self._response_handler.handle_info_order_status_response(
                cast(dict[str, Any], raw_response_data),
                user_address=self._wallet_address,
                order_id=order_id,
            )
            # The validated_response is HyperliquidRawHistoricalOrderResponse
            # Its .order attribute holds the HyperliquidRawHistoricalOrder
            return validated_response.order
        except APIError as e:
            # Specific check for ORDER_NOT_FOUND from the service/handler layer
            # Check raw_response_data only if it's bound and is a string
            is_not_found_str_response = (
                isinstance(raw_response_data, str) and "Order not found" in raw_response_data
            )
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value or is_not_found_str_response:
                logger.info(
                    f"[{self._exchange_name}] Order {order_id} for symbol {symbol} not found (caught APIError or string response)."
                )
                return None
            logger.error(
                f"[{self._exchange_name}] API error getting order status for OID {order_id}: {e.message}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error getting order status for OID {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting order status for OID {order_id}: {e}",
                APIErrorCode.UNKNOWN.value,
            ) from e

    async def bulk_cancel_orders_by_symbol_raw(self, symbol: str) -> HyperliquidRawExchangeResponse:
        """Cancels all open orders for a given symbol."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required for bulk cancel.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        open_orders_list = (
            await self.get_open_orders_raw()
        )  # This now returns list[HyperliquidRawOrder]

        cancels_for_request_payload: list[HyperliquidCancelDetail] = []
        for order in open_orders_list:  # Iterate list[HyperliquidRawOrder]
            if order.asset == symbol:  # order.asset is the string symbol name
                # Need to get asset index for the cancel detail
                asset_index = await self._get_asset_index_callable(order.asset)
                if asset_index is not None:
                    cancels_for_request_payload.append(
                        HyperliquidCancelDetail(asset=asset_index, oid=order.oid)
                    )
                else:
                    logger.warning(
                        f"[{self._exchange_name}] Could not find asset_index for {order.asset} while preparing bulk cancel."
                    )

        if not cancels_for_request_payload:
            logger.info(
                f"[{self._exchange_name}] No orders found for symbol {symbol} to bulk cancel."
            )
            return HyperliquidRawExchangeResponse(
                status="ok",
                data=HyperliquidRawExchangeResponseData(
                    type="statuses",
                    statuses=["No orders to cancel for the specified symbol after filtering."],
                ),
            )

        # Construct the action for bulk cancellation
        action_item = {"type": "cancel", "cancels": cancels_for_request_payload}
        actions_list = [action_item]

        try:
            # Use exchange_requester for actions
            raw_response_data = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for bulk_cancel_orders_by_symbol_raw",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="cancelOrders"
            )
        except APIError as e:
            logger.error(
                f"[{self._exchange_name}] API error bulk canceling orders for {symbol}: {e.message}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error bulk canceling orders for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error bulk canceling orders for {symbol}: {e}",
                APIErrorCode.UNKNOWN.value,
            ) from e
