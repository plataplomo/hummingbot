"""
CyberDeltaEngine: Backpack Trading Service
-------------------------------------------

This service encapsulates the logic for trading operations on the Backpack Exchange.
It uses the HttpClient (via a requester callable), BackpackRequestBuilder,
and BackpackResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

from collections.abc import Callable, Mapping, Awaitable
from decimal import Decimal

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
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
            ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
        ],
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
            response_data, status_code, headers = await self._http_client_requester(
                method="POST", endpoint=endpoint, data=payload, is_signed=True
            )
            logger.debug(f"[{self._exchange_name}] Place order raw response: {response_data}, Status: {status_code}, Headers: {headers}")

            # Response handler expects RawJsonResponse which can be dict or list
            # Basic check based on status and type before passing to handler
            if not (200 <= status_code < 300):
                 raise APIError(f"Place order failed with status {status_code}", APIErrorCode.EXCHANGE_SPECIFIC.value, http_status=status_code, exchange_message=str(response_data))

            if not isinstance(response_data, dict): # Backpack order response is typically a dict
                logger.error(
                    f"[{self._exchange_name}] Unexpected response type from _http_client_requester "
                    f"for place_order: {type(response_data)}. Expected dict. Status: {status_code}"
                )
                raise APIError(
                    f"Unexpected response type for place_order: {type(response_data)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                )

            # Current handle_place_order_response does not take status_code, headers
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

    async def cancel_order_raw(self, order_id: str, symbol: str) -> bool:
        """
        Cancel an existing order and return the raw response.
        Backpack API requires symbol for cancellation.

        Args:
            order_id: The ID of the order to cancel.
            symbol: The symbol of the order to cancel.

        Returns:
            bool: True if cancellation was processed successfully by the handler, False otherwise.

        Raises:
            APIError: On API errors or if the cancellation fails significantly.
        """
        endpoint = "/api/v1/order"
        params = self._request_builder.build_cancel_order_params(symbol=symbol, order_id=order_id)
        response_data: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_data, status_code, headers = await self._http_client_requester(
                method="DELETE", endpoint=endpoint, params=params, is_signed=True
            )
            logger.debug(f"[{self._exchange_name}] Cancel order response: {response_data}, Status: {status_code}, Headers: {headers}")

            # Check for definite HTTP errors first
            if not (200 <= status_code < 300) and status_code != 404: # Allow 404 for already cancelled/not found
                 raise APIError(f"Cancel order failed with status {status_code}", APIErrorCode.EXCHANGE_SPECIFIC.value, http_status=status_code, exchange_message=str(response_data)) # Changed to EXCHANGE_SPECIFIC
            
            # response_data would be None if http_client got empty body (e.g. 204 No Content, or 200 OK with empty)
            # or it could be a dict (e.g. details of cancelled order)
            # The handler `handle_cancel_order_response` expects `RawJsonResponse` (which can be None, dict, list etc.)
            # and returns bool.

            success = self._response_handler.handle_cancel_order_response(
                raw_response_content=response_data, # Pass the data part
                order_id=order_id,
                symbol=symbol
            )

            if not success and (200 <= status_code < 300):
                # HTTP status was OK, but handler logic (e.g. based on content) deemed it not a success.
                logger.warning(
                    f"[{self._exchange_name}] Cancel order for {order_id} ({symbol}) received HTTP success (status {status_code}) "
                    f"but handler indicated failure. Response data: {response_data!r}"
                )
                # We trust the handler's boolean for logical success in this case.
                # If the handler's criteria for success aren't met despite a 2xx status, it's a failure.
                # No need to raise APIError here if handler simply returns False for specific content cases
                # unless an APIError is desired for any non-true return from handler.
                # For now, just return the handler's boolean.

            return success # Return the boolean from the handler

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
        response_data: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_data, status_code, headers = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            logger.debug(f"[{self._exchange_name}] Get open orders response: {response_data}, Status: {status_code}, Headers: {headers}")

            if not (200 <= status_code < 300):
                 raise APIError(f"Get open orders failed with status {status_code}", APIErrorCode.EXCHANGE_SPECIFIC.value, http_status=status_code, exchange_message=str(response_data)) # Changed to EXCHANGE_SPECIFIC

            if response_data is None : # Check the data part of the response (was response_data_raw)
                logger.warning(f"[{self._exchange_name}] Get open orders for {symbol} returned None with status {status_code}, assuming empty list.")
                return [] 
            
            if not isinstance(response_data, list):
                raise APIError(
                    f"Unexpected response type for get_open_orders: {type(response_data)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                )

            raw_orders = self._response_handler.handle_get_open_orders_response(
                response_data, symbol # Pass the data part
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
        response_data: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_data, status_code, headers = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            logger.debug(f"[{self._exchange_name}] Get order status response for {identifier}: {response_data}, Status: {status_code}, Headers: {headers}")

            if not (200 <= status_code < 300) and status_code != 404: 
                 raise APIError(f"Get order status for {identifier} failed with status {status_code}", APIErrorCode.EXCHANGE_SPECIFIC.value, http_status=status_code, exchange_message=str(response_data)) # Changed to EXCHANGE_SPECIFIC
            
            if response_data is None and (200 <= status_code < 300):
                raise APIError(
                    f"Unexpected None response for get_order_status of {identifier} with status {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                )
            
            return self._response_handler.handle_get_order_status_response(
                response_data, identifier # Pass the data part
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

    async def cancel_all_orders_raw(self, symbol: str | None = None) -> list[BackpackRawOrder]:
        """Cancels all open orders, optionally filtered by symbol."""
        delete_endpoint = "/api/v1/orders" 
        # Use build_cancel_all_orders_payload as it correctly returns params for DELETE query
        params = self._request_builder.build_cancel_all_orders_payload(symbol=symbol) 
        response_data: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            response_data, status_code, headers = await self._http_client_requester(
                method="DELETE", endpoint=delete_endpoint, params=params, is_signed=True
            )
            logger.debug(f"[{self._exchange_name}] Cancel all orders response (symbol: {symbol}): {response_data}, Status: {status_code}, Headers: {headers}")

            if not (200 <= status_code < 300):
                 raise APIError(f"Cancel all orders (symbol: {symbol}) failed with status {status_code}", APIErrorCode.EXCHANGE_SPECIFIC.value, http_status=status_code, exchange_message=str(response_data)) # Changed to EXCHANGE_SPECIFIC

            if not isinstance(response_data, list): 
                 logger.warning(f"Cancel all orders for {symbol} returned non-list: {type(response_data)}. Status: {status_code}. Assuming empty list for successful status.")
                 if (200 <= status_code < 300): return [] 
                 raise APIError(f"Cancel all orders for {symbol} response not list: {type(response_data)}", APIErrorCode.INVALID_RESPONSE.value, http_status=status_code)

            return self._response_handler.handle_cancel_all_orders_response(response_data, symbol) # Pass the data part
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error cancelling all orders (symbol: {symbol}): {e}")
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Service: Unhandled exception during cancel_all_orders_raw for "
                f"symbol '{symbol or 'all'}': {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error in cancel_all_orders_raw for symbol '{symbol or 'all'}': {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            )
