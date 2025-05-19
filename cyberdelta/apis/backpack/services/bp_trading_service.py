"""
CyberDeltaEngine: Backpack Trading Service
-------------------------------------------

This service encapsulates the logic for trading operations on the Backpack Exchange.
It uses the HttpClient (via a requester callable), BackpackRequestBuilder,
and BackpackResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

from __future__ import annotations  # Added for type hints like BackpackRawOrder | None

from collections.abc import Awaitable, Callable, Mapping  # Added Mapping for HttpClientRequesterSig
from decimal import Decimal
from typing import Any

# Mappers
from cyberdelta.apis.backpack.bp_order_mapper import (
    BackpackOrderMapper,
)  # Added for transformations
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
    # RawJsonResponse, # Not directly used here if ParsedJsonResponse covers it
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Internal Domain Models
from cyberdelta.core.models import Order  # Import internal Order model
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class BackpackTradingService:
    """
    Service class for Backpack trading operations.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,  # Corrected signature
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

    async def place_order(
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
    ) -> Order:  # Returns internal Order model
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
            trigger_price=stop_price,  # Ensure builder handles this if applicable for Backpack
        )

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {} # Headers not used by this specific handler
        try:
            response_tuple = await self._http_client_requester(
                method="POST", endpoint=endpoint, data=payload, is_signed=True
            )
            raw_data, status_code, _ = response_tuple  # Headers ignored for this handler
            logger.debug(
                f"[{self._exchange_name}] Place order raw response: {raw_data}, status: {status_code}"
            )

            if raw_data is None:
                raise APIError(
                    f"Place order for {symbol} returned no content (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data, dict):
                raise APIError(
                    f"Unexpected response type for place_order: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_model: BackpackRawOrder = self._response_handler.handle_place_order_response(
                raw_data
            )
            internal_order = BackpackOrderMapper.transform_raw_order_to_internal(raw_order_model)
            return internal_order
        except ValueError as ve:  # From builder or mapper
            logger.error(f"[{self._exchange_name}] ValueError placing order: {ve}", exc_info=True)
            raise APIError(
                f"Data validation error placing order: {ve}",
                APIErrorCode.INVALID_PARAMS.value,
                original_exception=ve,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from ve
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error placing order: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error placing order: {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        endpoint = "/api/v1/order"
        params = self._request_builder.build_cancel_order_params(symbol=symbol, order_id=order_id)

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {} # Headers not used by this specific handler
        try:
            response_tuple = await self._http_client_requester(
                method="DELETE", endpoint=endpoint, params=params, is_signed=True
            )
            raw_data, status_code, _ = response_tuple  # Headers ignored for this handler
            logger.debug(
                f"[{self._exchange_name}] Cancel order response: {raw_data}, status: {status_code}"
            )

            success = self._response_handler.handle_cancel_order_response(
                raw_response_content=raw_data,
                order_id=order_id,
                symbol=symbol,
                # No status_code, headers passed here
            )
            if not success:
                logger.warning(
                    f"[{self._exchange_name}] Cancel order for {order_id} ({symbol}) handler indicated failure. "
                    f"Raw: {raw_data!r}, Status: {status_code}"
                )
            return success
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Error cancelling order {order_id}: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error cancelling order {order_id}: {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e

    async def get_open_orders(
        self, symbol: str | None = None
    ) -> list[Order]:  # Returns list of internal Order
        endpoint = "/api/v1/orders"
        params = self._request_builder.build_get_open_orders_params(symbol=symbol)
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {} # Headers not used by this specific handler
        try:
            response_tuple = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            raw_data, status_code, _ = response_tuple  # Headers ignored for this handler
            logger.debug(
                f"[{self._exchange_name}] Get open orders response: {raw_data}, status: {status_code}"
            )

            if raw_data is None:
                raise APIError(
                    f"Get open orders for {symbol or 'all'} returned no content (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data, list):
                raise APIError(
                    f"Unexpected response type for get_open_orders: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_get_open_orders_response(raw_data, symbol)
            )
            internal_orders = [
                BackpackOrderMapper.transform_raw_order_to_internal(ro) for ro in raw_orders_list
            ]
            return internal_orders
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error getting open orders: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error getting open orders: {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e

    async def get_order(
        self, order_id: str, symbol: str, client_order_id: str | None = None
    ) -> Order | None:  # Returns internal Order or None
        identifier = order_id if order_id else client_order_id
        if not identifier:
            raise ValueError("Either order_id or client_order_id must be provided.")
        if not symbol:  # Backpack requires symbol for getting a specific order by ID
            raise ValueError("Symbol must be provided to get a specific order for Backpack.")

        endpoint = f"/api/v1/orders/{identifier}"
        # Backpack requires symbol as a query param for this endpoint, even if order_id is in path.
        # The builder `build_cancel_order_params` can be reused if it only includes symbol when needed.
        # Or, create specific params dict here.
        query_params = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        if client_order_id and not order_id:
            query_params["clientId"] = client_order_id  # If API differentiates by this query param
        elif order_id:
            query_params["orderId"] = order_id  # If API differentiates by this query param

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {} # Headers not used by this specific handler
        try:
            response_tuple = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=query_params, is_signed=True
            )
            raw_data, status_code, _ = response_tuple  # Headers ignored for this handler
            logger.debug(
                f"[{self._exchange_name}] Get order status for {identifier} ({symbol}): {raw_data}, status: {status_code}"
            )

            # The _http_client_requester or its underlying _request should raise APIError for 404s.
            # If it maps to ORDER_NOT_FOUND, the except block below will handle it.
            # If it doesn't, then the error mapper in ExchangeAPI should convert it.

            if raw_data is None:
                # This case implies a successful HTTP status (e.g., 200 OK) but empty body,
                # which is unexpected for a get_order endpoint that should return data or 404.
                raise APIError(
                    f"Get order status for {identifier} ({symbol}) returned no content despite successful status {status_code}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data, dict):
                raise APIError(
                    f"Unexpected response type for get_order_status: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Expect handle_get_order_status_response to return BackpackRawOrder or raise APIError (e.g. for validation)
            raw_order_model: BackpackRawOrder = (
                self._response_handler.handle_get_order_status_response(raw_data, identifier)
            )
            # If raw_order_model was typed as BackpackRawOrder | None, the linter error was due to that.
            # Now that it's BackpackRawOrder, the None check previously was indeed problematic.
            # The APIError.ORDER_NOT_FOUND should be the mechanism for "not found".
            return BackpackOrderMapper.transform_raw_order_to_internal(raw_order_model)
        except APIError as e:
            # Specifically catch ORDER_NOT_FOUND (or any 404 mapped to it) and return None
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value or e.http_status == 404:
                logger.info(
                    f"[{self._exchange_name}] Order {identifier} ({symbol}) not found: {e.message}"
                )
                return None
            raise  # Re-raise other APIErrors
        except Exception as e:  # Catch any other unexpected errors
            logger.error(
                f"[{self._exchange_name}] Error getting order status for {identifier} ({symbol}): {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting order status for {identifier} ({symbol}): {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:  # Added method
        """Fetch all open orders for a given symbol or all symbols."""
        # This is an alias for get_open_orders as per ExchangeAPI interface
        return await self.get_open_orders(symbol=symbol)

    async def cancel_all_orders(
        self, symbol: str | None = None
    ) -> list[Order]:  # Returns list of internal Order objects
        endpoint = "/api/v1_1/orders/cancelAll"
        payload_or_params = self._request_builder.build_cancel_all_orders_payload(symbol=symbol)

        request_args: dict[str, Any] = {"method": "DELETE", "endpoint": endpoint, "is_signed": True}
        if isinstance(payload_or_params, dict):
            request_args["data"] = payload_or_params

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {} # Headers not used by this specific handler
        try:
            response_tuple = await self._http_client_requester(**request_args)
            raw_data, status_code, _ = response_tuple  # Headers ignored for this handler
            logger.debug(
                f"[{self._exchange_name}] Cancel all orders response: {raw_data}, status: {status_code}"
            )

            if raw_data is None:
                raise APIError(
                    f"Cancel all orders for {symbol or 'all'} returned no content (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            if not isinstance(raw_data, list):
                raise APIError(
                    f"Unexpected response type for cancel_all_orders: {type(raw_data)}",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_cancel_all_orders_response(raw_data, symbol)
            )
            internal_orders = [
                BackpackOrderMapper.transform_raw_order_to_internal(ro) for ro in raw_orders_list
            ]
            return internal_orders
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error cancelling all orders: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error cancelling all orders: {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e
