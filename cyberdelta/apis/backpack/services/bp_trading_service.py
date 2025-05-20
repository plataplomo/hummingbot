"""
CyberDeltaEngine: Backpack Trading Service
-------------------------------------------

This service encapsulates the logic for trading operations on the Backpack Exchange.
It uses the HttpClient (via a requester callable), BackpackRequestBuilder,
BackpackResponseHandler, and BackpackOrderMapper to interact with the API
and returns Internal Domain Models.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal

from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
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

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class BackpackTradingService:
    """
    Service class for Backpack trading operations. Returns Internal Domain Models.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        authenticator: IAuthenticator | None,  # Added authenticator
        exchange_name: str,
    ) -> None:
        """
        Initialize the BackpackTradingService.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._order_mapper = BackpackOrderMapper()  # Instantiate or use static methods

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,  # Renamed from trigger_price for consistency
        client_order_id: str | None = None,
        post_only: bool = False,
        # reduce_only is not directly supported by Backpack's place_order from openapi_backpack.json
    ) -> Order:
        endpoint = "/api/v1/order"
        payload = self._request_builder.build_place_order_payload(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,  # Corrected: Pass TimeInForce enum
            price=price,
            client_order_id=client_order_id,
            post_only=post_only,
            trigger_price=stop_price,
        )

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="POST", endpoint=endpoint, data=payload, is_signed=True
            )
            if raw_data is None or not isinstance(raw_data, dict):
                raise APIError(
                    f"Place order for {symbol} returned invalid data (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_model: BackpackRawOrder = self._response_handler.handle_place_order_response(
                raw_data
            )
            internal_order = self._order_mapper.transform_raw_order_to_internal(raw_order_model)
            return internal_order
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error placing order: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error placing order for {symbol}: {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=str(raw_data) if raw_data else None,
            ) from e

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        endpoint = "/api/v1/order"
        payload = self._request_builder.build_cancel_order_payload(symbol=symbol, order_id=order_id)
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="DELETE", endpoint=endpoint, data=payload, is_signed=True
            )
            # Backpack's cancel order returns the cancelled order details or an error.
            # The response handler needs to determine success.
            # Assuming handle_cancel_order_response returns bool based on successful cancellation.
            if (
                raw_data is None
            ):  # Explicitly check for None if that's a possible "success but no content"
                logger.warning(
                    f"[{self._exchange_name}] Cancel order for {order_id} ({symbol}) received no content, assuming failure or unconfirmed success."
                )
                return False  # Or specific handling if None means success for Backpack

            return self._response_handler.handle_cancel_order_response(
                raw_response_content=raw_data, order_id=order_id, symbol=symbol
            )
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Error cancelling order {order_id} ({symbol}): {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error cancelling order {order_id} ({symbol}): {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=str(raw_data) if raw_data else None,
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        endpoint = "/api/v1/orders"
        params = self._request_builder.build_get_open_orders_params(symbol=symbol)

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            if raw_data is None or not isinstance(raw_data, list):
                raise APIError(
                    f"Get open orders for {symbol or 'all'} returned invalid data (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_get_open_orders_response(raw_data, symbol)
            )
            internal_orders = [
                self._order_mapper.transform_raw_order_to_internal(ro) for ro in raw_orders_list
            ]
            return internal_orders
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self._exchange_name}] Error getting open orders: {e}", exc_info=True)
            raise APIError(
                f"Unexpected error getting open orders for {symbol or 'all'}: {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=str(raw_data) if raw_data else None,
            ) from e

    async def get_order(
        self, order_id: str, symbol: str, client_order_id: str | None = None
    ) -> Order | None:
        # Backpack uses orderId or clientOrderId in path for GET /api/v1/order/{identifier}
        # The builder should handle which identifier to use, or service needs logic.
        # Assuming order_id is the exchange order ID if provided.
        # If only client_order_id, that should be used as identifier.

        identifier = order_id
        if not order_id and client_order_id:
            identifier = client_order_id
        elif not order_id and not client_order_id:
            raise ValueError("Either order_id or client_order_id must be provided.")

        endpoint = f"/api/v1/order/{identifier}"
        params = self._request_builder.build_get_order_params(
            symbol=symbol
        )  # Symbol is a query param

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            if status_code == 404:  # Order not found
                logger.info(f"[{self._exchange_name}] Order {identifier} ({symbol}) not found.")
                return None
            if raw_data is None or not isinstance(raw_data, dict):
                raise APIError(
                    f"Get order {identifier} ({symbol}) returned invalid data (status: {status_code})",
                    APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_order_model: BackpackRawOrder = (
                self._response_handler.handle_get_order_status_response(raw_data, identifier)
            )
            internal_order = self._order_mapper.transform_raw_order_to_internal(raw_order_model)
            return internal_order
        except APIError as e:
            # Allow ORDER_NOT_FOUND from handler to propagate if it maps it
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.info(
                    f"[{self._exchange_name}] Order {identifier} ({symbol}) not found via handler mapping."
                )
                return None
            raise
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Error getting order {identifier} ({symbol}): {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting order {identifier} ({symbol}): {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=str(raw_data) if raw_data else None,
            ) from e

    async def get_order_status(
        self, order_id: str, symbol: str, client_order_id: str | None = None
    ) -> Order:  # As per prompt, this implies it raises if not found.
        order = await self.get_order(
            order_id=order_id, symbol=symbol, client_order_id=client_order_id
        )
        if order is None:
            identifier = client_order_id if not order_id and client_order_id else order_id
            raise APIError(
                f"Order {identifier} for symbol {symbol} not found on {self._exchange_name}.",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )
        return order

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        endpoint = "/api/v1/orders"
        # Backpack's Cancel All Orders: DELETE /api/v1/orders with symbol query parameter
        payload = self._request_builder.build_cancel_all_orders_payload(symbol=symbol)

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        results: list[CancelOrderResult] = []
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="DELETE",
                endpoint=endpoint,
                data=payload,
                is_signed=True,  # data, not params for DELETE body
            )
            # Backpack's response for cancel all is a list of strings (order IDs that were cancelled)
            if raw_data is None or not isinstance(raw_data, list):
                error_message = f"Cancel all orders for {symbol or 'all'} returned invalid data or no content (status: {status_code})"
                logger.error(f"[{self._exchange_name}] {error_message}. Raw: {raw_data}")
                # If response is not a list, it might be an error structure or unexpected.
                # We can't confirm any cancellations.
                # Depending on strictness, either raise or return empty/failed results.
                # For now, if it's not a list, assume general failure or no orders to cancel.
                # If some orders were open, this would be a partial failure.
                # This part needs careful handling based on actual API error responses for cancel all.
                # If an error occurs, it usually returns a JSON error object, not a list.
                if isinstance(raw_data, dict) and raw_data.get("error"):  # Check for explicit error
                    raise APIError(
                        raw_data.get("error", {}).get(
                            "message", "Failed to cancel all orders due to API error response."
                        ),
                        APIErrorCode.UNKNOWN.value,  # Using UNKNOWN as OPERATION_FAILED is not available
                        http_status=status_code,
                        exchange_message=str(raw_data),
                    )

                logger.warning(
                    f"[{self._exchange_name}] {error_message}. Assuming no orders were cancelled or confirmable."
                )
                # Create a generic failure result if no orders could be confirmed cancelled.
                # This assumes that if there were orders and they failed to cancel, an error would be raised.
                # If there were no orders, an empty list response is typical and correct.
                # If raw_data is None, it's ambiguous.
                return []  # Or a list with a single generic failure if that's preferred

            # If raw_data is a list, it should be a list of successfully cancelled order IDs (strings)
            for cancelled_order_id_any in raw_data:
                if isinstance(cancelled_order_id_any, str):
                    results.append(
                        CancelOrderResult(
                            order_id=cancelled_order_id_any,
                            client_order_id=None,  # Backpack doesn't return this in cancel all
                            symbol=symbol,  # We assume all were for this symbol if provided
                            success=True,
                            message="Successfully cancelled.",
                            status=CancelOrderResultStatus.SUCCESS,
                        )
                    )
                else:  # Should not happen if API conforms
                    logger.warning(
                        f"[{self._exchange_name}] Unexpected item in cancel all orders response list: {cancelled_order_id_any}"
                    )

            logger.info(
                f"[{self._exchange_name}] Cancelled {len(results)} orders for {symbol or 'all'}."
            )
            return results

        except APIError as e:  # Catch APIErrors raised from _http_client_requester or earlier
            logger.error(
                f"[{self._exchange_name}] APIError cancelling all orders for {symbol or 'all'}: {e.message}",
                exc_info=True,
            )
            # Construct a generic failure result for the batch
            results.append(
                CancelOrderResult(
                    order_id=None,
                    client_order_id=None,
                    symbol=symbol,
                    success=False,
                    message=f"APIError: {e.message}",
                    status=CancelOrderResultStatus.FAILED,
                    raw_response={"message": e.exchange_message} if e.exchange_message else None,
                )
            )
            return results  # Return list with the failure entry
        except Exception as e:
            logger.error(
                f"[{self._exchange_name}] Unexpected error cancelling all orders for {symbol or 'all'}: {e}",
                exc_info=True,
            )
            raw_error_data_str = str(raw_data) if raw_data is not None else None
            results.append(
                CancelOrderResult(
                    order_id=None,
                    client_order_id=None,
                    symbol=symbol,
                    success=False,
                    message=f"Unexpected error: {str(e)}",
                    status=CancelOrderResultStatus.FAILED,
                    raw_response={"message": raw_error_data_str} if raw_error_data_str else None,
                )
            )
            return results  # Return list with the failure entry

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Fetch all open orders, optionally filtering by symbol.
        This is an alias for get_open_orders as per Backpack API structure.
        """
        return await self.get_open_orders(symbol=symbol)
