"""
CyberDeltaEngine: Hyperliquid Trading Service
----------------------------------------------

This service encapsulates the logic for trading operations on the Hyperliquid Exchange.
It uses the HttpClient (via a requester callable), HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models or relevant raw data structures.
"""

from collections.abc import Callable, Coroutine, Mapping
from decimal import Decimal
from typing import Any, TypedDict

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
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


# Define HttpClientRequesterSig for the 3-tuple response
HttpClientRequesterSig = Callable[
    ..., Coroutine[Any, Any, tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class HyperliquidCancelDetail(TypedDict):
    asset: int
    oid: int


class HyperliquidTradingService:
    """
    Service class for Hyperliquid trading operations.
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
            # Unpack the 3-tuple from the requester
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]  # Extract the actual data

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

        action_item = {
            "type": "cancel",
            "cancels": [HyperliquidCancelDetail(asset=asset_index, oid=order_id)],
        }
        actions_list = [action_item]

        try:
            # Unpack the 3-tuple from the requester
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]  # Extract the actual data

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
                f"Unexpected error canceling order {order_id}: {e}",
                APIErrorCode.UNKNOWN.value,
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
            # Unpack the 3-tuple from the requester
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]  # Extract the actual data

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
        """
        Retrieves raw open orders for the connected wallet.
        Uses the /info endpoint with type "openOrders".
        """
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required for Hyperliquid get_open_orders_raw.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        request_payload_model = HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders", user=self._wallet_address
        )
        request_data_dict: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        try:
            # For /info endpoint calls
            # The `info_http_client_requester` in `HyperliquidTradingService` is `_info_request_wrapper`
            # from `hl_api.py`. This wrapper ALREADY extracts the first element of the tuple
            # and returns `RawJsonResponse | None`. So, no tuple unpacking is needed here.
            raw_response_data = await self._info_http_client_requester(
                method="POST",
                endpoint_path=self._info_endpoint,  # Corrected path
                data=request_data_dict,
                is_signed=False,  # /info endpoints are typically not signed
            )
            # The response_handler expects the direct list of orders from the /info response
            if not isinstance(raw_response_data, list):
                # This could happen if _info_request_wrapper returns None or str
                logger.error(
                    f"[{self._exchange_name}] Unexpected response format for open orders: {type(raw_response_data)}. Raw: {raw_response_data!r}"
                )
                raise APIError(
                    "Open orders response is not a list.", APIErrorCode.INVALID_RESPONSE.value
                )

            validated_response = self._response_handler.handle_info_open_orders_response(
                raw_response_data, user_address=self._wallet_address
            )
            # HyperliquidRawOpenOrdersResponse is a RootModel, its .root attribute holds list[HyperliquidRawOpenOrderItem]
            # Each item in .root has a .order attribute which is HyperliquidRawOrder
            return [item.order for item in validated_response.root]
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error fetching open orders: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error fetching open orders: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching open orders: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def get_order_status_raw(
        self, symbol: str, order_id: int
    ) -> HyperliquidRawHistoricalOrder | None:
        """
        Retrieves the raw status of a specific historical order by its OID.
        Uses the /info endpoint with type "orderStatus".
        """
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for Hyperliquid get_order_status_raw.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        request_payload_data = {
            "type": "orderStatus",
            "user": self._wallet_address,
            "oid": order_id,
        }
        try:
            # Call to /info endpoint, no tuple unpacking needed due to _info_request_wrapper
            raw_response_data = await self._info_http_client_requester(
                method="POST",
                endpoint_path=self._info_endpoint,  # Corrected path
                data=request_payload_data,
                is_signed=False,  # /info endpoints are typically not signed
            )
            # The handler expects the direct order status dict from the /info response
            # Ensure raw_response_data is a dict before passing to handler
            if not isinstance(raw_response_data, dict):
                # This could happen if _info_request_wrapper returns None or list/str
                # For orderStatus, we expect a dict if found, or specific error if not.
                # Hyperliquid's actual response for non-existent order needs to be checked.
                # Assuming if not a dict, it implies not found or error.
                logger.warning(
                    f"[{self._exchange_name}] Order status for OID {order_id} not found or unexpected format: {type(raw_response_data)}. Raw: {raw_response_data!r}"
                )
                return None  # Consistent with method returning None if not found

            validated_response = self._response_handler.handle_info_order_status_response(
                raw_response_data, user_address=self._wallet_address, order_id=order_id
            )
            # HyperliquidRawHistoricalOrderResponse has an .order attribute which is HyperliquidRawHistoricalOrder
            return validated_response.order
        except APIError as e:
            logger.error(
                f"[{self._exchange_name}] API error fetching order status for OID {order_id}: {e.message}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error fetching order status for OID {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order status for OID {order_id}: {e}",
                APIErrorCode.UNKNOWN.value,
            ) from e

    async def bulk_cancel_orders_by_symbol_raw(self, symbol: str) -> HyperliquidRawExchangeResponse:
        """Raw method to bulk cancel all open orders for a specific symbol."""
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

        # This action type is specific to Hyperliquid for canceling all orders of an asset
        action_item = {"type": "bulkCancelOpenOrders", "assetIndex": asset_index}
        actions_list = [action_item]
        try:
            # Unpack the 3-tuple from the requester
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]  # Extract the actual data

            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for bulk_cancel_orders_by_symbol_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            # Assuming the response handler can process the status for "bulkCancelOpenOrders"
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="bulkCancelOpenOrders"
            )
        except APIError as e:
            logger.error(
                f"[{self._exchange_name}] API error bulk canceling orders by symbol {symbol}: {e.message}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error bulk canceling orders by symbol {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error bulk canceling orders by symbol {symbol}: {e}",
                APIErrorCode.UNKNOWN.value,
            ) from e

    async def update_leverage_raw(
        self, symbol: str, leverage: Decimal, is_cross_margin: bool
    ) -> HyperliquidRawExchangeResponse:
        """Raw method to update leverage."""
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

        # Hyperliquid requires leverage as integer if it's whole, or string if decimal.
        # For safety, always pass as string as Decimal can be non-integer.
        # Ensure leverage is correctly formatted for the API (e.g., 10, 10.5)
        # leverage_str = str(leverage) # This variable was unused

        action_item = {
            "type": "updateLeverage",
            "asset": asset_index,
            "isCross": is_cross_margin,
            "leverage": int(leverage)
            if leverage.is_zero() or leverage % 1 == 0
            else float(leverage),
        }  # Use int if whole, else float
        actions_list = [action_item]

        try:
            # Unpack the 3-tuple from the requester
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]  # Extract the actual data

            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for update_leverage_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="updateLeverage"
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error updating leverage: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error updating leverage: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error updating leverage: {e}", APIErrorCode.UNKNOWN.value
            ) from e

    async def bulk_cancel_orders_raw(
        self, cancels: list[HyperliquidCancelDetail]
    ) -> HyperliquidRawExchangeResponse:
        """Raw method to bulk cancel orders."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address is required for Hyperliquid trading actions.",
                APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # The 'cancels' argument is already a list of HyperliquidCancelDetail
        # which is list[dict[str, int]] where keys are "asset" and "oid".
        # This matches the expected structure for "cancels" in the action item.
        action_item = {"type": "cancel", "cancels": cancels}
        actions_list = [action_item]

        try:
            # Unpack the 3-tuple from the requester
            raw_response_data_tuple = await self._exchange_http_client_requester(
                method="POST", endpoint=self._action_endpoint, data=actions_list, is_signed=True
            )
            raw_response_data = raw_response_data_tuple[0]  # Extract the actual data

            if not isinstance(raw_response_data, dict):
                raise APIError(
                    "Unexpected response format for bulk_cancel_orders_raw on Hyperliquid",
                    APIErrorCode.INVALID_RESPONSE.value,
                )
            # Assuming action_type for bulk cancel is 'cancel'. If HL has a specific one like 'cancelOrders', adjust.
            return self._response_handler.handle_exchange_response(
                raw_response_data, action_type="cancel"
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error bulk canceling orders: {e.message}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error bulk canceling orders: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error bulk canceling orders: {e}", APIErrorCode.UNKNOWN.value
            ) from e
