"""
CyberDeltaEngine: Backpack Trading Service
-------------------------------------------

This service encapsulates the logic for trading operations on the Backpack Exchange.
It uses the HttpClient (via a requester callable), BackpackRequestBuilder,
and BackpackResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class BackpackTradingService:
    """
    Service class for Backpack trading operations.
    """

    def __init__(
        self,
        http_client_requester: Callable[
            ..., Coroutine[Any, Any, RawJsonResponse | list[Any] | dict[str, Any] | None]
        ],  # More specific type for http_client_requester if possible
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
    ) -> None:
        """
        Initialize the BackpackTradingService.

        Args:
            http_client_requester: A callable (typically a method from an HttpClient instance)
                                   for making API requests.
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            authenticator: An instance of IAuthenticator for signing requests.
            exchange_name: The name of the exchange.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator  # Storing authenticator
        self._exchange_name = exchange_name

    async def place_order_raw(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        client_order_id: str | None = None,
        post_only: bool = False,
    ) -> BackpackRawOrder:
        """
        Place an order on Backpack Exchange and return the raw validated response.

        Args:
            symbol: Trading symbol (e.g., 'BTC_USDC')
            side: Order side (BUY or SELL)
            order_type: Order type (LIMIT, MARKET, etc.)
            quantity: Order quantity (as Decimal)
            time_in_force: Time in force (GTC, IOC, FOK).
            price: Order price (required for limit orders, as Decimal)
            stop_price: Stop price for stop orders.
            client_order_id: Custom client order ID
            post_only: Whether this is a post-only order (bool)

        Returns:
            BackpackRawOrder object.

        Raises:
            ValueError: If price is missing for a LIMIT order.
            APIError: On API errors or if the order placement fails.
        """
        endpoint = "/api/v1/order"
        payload = self._request_builder.build_place_order_payload(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            client_order_id=client_order_id,
            post_only=post_only,
            trigger_price=stop_price,
        )

        try:
            # The http_client_requester is self._request from BackpackAPI
            response_data = await self._http_client_requester(
                method="POST", endpoint=endpoint, data=payload, is_signed=True
            )
            # Response handler expects RawJsonResponse which can be dict or list
            if not isinstance(response_data, (dict, list)):
                logger.error(
                    f"[{self._exchange_name}] Unexpected response type from _http_client_requester "
                    f"for place_order: {type(response_data)}. Expected dict or list."
                )
                raise APIError(
                    f"Unexpected response type for place_order: {type(response_data)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            raw_order = self._response_handler.handle_place_order_response(response_data)
            return raw_order
        except ValueError as ve:
            logger.error(f"[{self._exchange_name}] ValueError placing order: {ve}")
            raise
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API Error placing order: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error placing order: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error placing order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_order_raw(self, order_id: str, symbol: str) -> RawJsonResponse:
        """
        Cancel an existing order and return the raw response.
        Backpack API requires symbol for cancellation.

        Args:
            order_id: The ID of the order to cancel.
            symbol: The symbol of the order to cancel.

        Returns:
            RawJsonResponse (typically a dict representing the cancelled order or success status).

        Raises:
            APIError: On API errors or if the cancellation fails.
        """
        endpoint = "/api/v1/order"
        params = self._request_builder.build_cancel_order_params(symbol=symbol, order_id=order_id)
        try:
            response_raw = await self._http_client_requester(
                method="DELETE", endpoint=endpoint, params=params, is_signed=True
            )
            if response_raw is None:  # Ensure response_raw is not None
                raise APIError(
                    "Received null response from cancel_order_raw",
                    APIErrorCode.INVALID_RESPONSE.value,
                )

            # The handler validates and returns the raw response (e.g. BackpackRawOrder of cancelled order)
            # or raises an APIError if cancellation failed.
            # The _response_handler.handle_cancel_order_response might return BackpackRawOrder or similar.
            # Let's assume it returns RawJsonResponse which is a dict or list.
            # For cancel, the handler should return the raw response if successful.
            return self._response_handler.handle_cancel_order_response(
                raw_response_content=response_raw, order_id=order_id, symbol=symbol
            )
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API Error canceling order {order_id}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Error canceling order {order_id}: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_open_orders_raw(self, symbol: str | None = None) -> list[BackpackRawOrder]:
        """
        Get all open orders, optionally filtered by symbol, and return raw validated orders.
        """
        endpoint = "/api/v1/orders"
        params = self._request_builder.build_get_open_orders_params(symbol=symbol)
        try:
            response_data_raw = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            if response_data_raw is None:  # Ensure response_raw is not None
                raise APIError(
                    "Received null response from get_open_orders_raw",
                    APIErrorCode.INVALID_RESPONSE.value,
                )

            raw_orders = self._response_handler.handle_get_open_orders_response(
                response_data_raw, symbol
            )
            return raw_orders
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API Error getting open orders: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error getting open orders: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error getting open orders: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_order_status_raw(
        self, order_id: str, symbol: str, client_order_id: str | None = None
    ) -> BackpackRawOrder | None:
        """
        Fetches the status of a single order by its orderId or clientId.
        Returns the raw validated order or None if not found.
        Symbol is required by Backpack.
        """
        identifier = order_id if order_id else client_order_id
        if not identifier:  # Should be caught by API client but good to have defense
            raise ValueError("Either order_id or client_order_id must be provided.")

        endpoint = f"/api/v1/orders/{identifier}"
        params = {"symbol": self._request_builder.format_symbol(symbol)}  # Use builder's formatter
        try:
            response_data_raw = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            # The response_handler.handle_get_order_status_response raises ORDER_NOT_FOUND
            # if the response indicates the order doesn't exist (e.g. 404 or specific error code)
            # or if response_data_raw is None.
            # If it returns, it's a valid BackpackRawOrder.
            return self._response_handler.handle_get_order_status_response(
                response_data_raw, identifier
            )
        except APIError as e:
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(f"[{self._exchange_name}] Order {identifier} not found (service).")
                return None
            logger.error(
                f"[{self._exchange_name}] API error fetching order status for {identifier}: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error fetching order status for {identifier}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order status {identifier}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    async def cancel_all_orders_raw(self, symbol: str | None = None) -> list[RawJsonResponse]:
        """
        Cancels all open orders, optionally filtered by symbol.
        Backpack requires canceling orders one by one.
        Returns a list of raw responses for each cancellation attempt.
        """
        logger.info(
            f"[{self._exchange_name}] Attempting to cancel all orders for symbol: {symbol or 'all'} (service)"
        )
        open_orders_raw = await self.get_open_orders_raw(symbol=symbol)
        if not open_orders_raw:
            logger.info(
                f"[{self._exchange_name}] No open orders to cancel for {symbol or 'all'} (service)"
            )
            return []

        results: list[RawJsonResponse] = []
        for raw_order in open_orders_raw:
            order_id_to_cancel = raw_order.id
            order_symbol = raw_order.symbol  # Use symbol from the raw_order

            if not order_id_to_cancel:  # Should not happen with BackpackRawOrder.id being mandatory
                logger.error(
                    f"[{self._exchange_name}] Cannot cancel order, missing ID for order: "
                    f"{raw_order.model_dump_json(exclude_none=True)}"
                )
                results.append({"error": "Missing order ID", "orderData": raw_order.model_dump()})
                continue
            try:
                # cancel_order_raw returns RawJsonResponse (dict typically)
                cancel_response = await self.cancel_order_raw(order_id_to_cancel, order_symbol)
                results.append(cancel_response)
            except APIError as e:
                logger.error(
                    f"[{self._exchange_name}] Error cancelling order {order_id_to_cancel} "
                    f"for symbol {order_symbol} during cancel_all_orders_raw: {e}"
                )
                # Append error information to results
                results.append(
                    {"error": str(e), "orderId": order_id_to_cancel, "symbol": order_symbol}
                )
            except Exception as e_unhandled:
                logger.error(
                    f"[{self._exchange_name}] Unhandled error cancelling order {order_id_to_cancel} "
                    f"for symbol {order_symbol} during cancel_all_orders_raw: {e_unhandled}"
                )
                results.append(
                    {
                        "error": f"Unhandled: {e_unhandled}",
                        "orderId": order_id_to_cancel,
                        "symbol": order_symbol,
                    }
                )
        return results
