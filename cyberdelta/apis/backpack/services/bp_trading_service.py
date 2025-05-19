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
from cyberdelta.core.models.enums import (  # Added CancelOrderResultStatus
    CancelOrderResultStatus,
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult  # Added CancelOrderResult
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
        endpoint = f"/api/v1/orders/{order_id}"
        # According to BackpackRequestBuilder.build_get_order_params and its test,
        # this specific endpoint (GET /api/v1/orders/{orderIdOrClientId})
        # does not take query parameters. The order_id is in the path.
        # The symbol parameter is for context for the mapper/handler, not for the request query itself.
        built_params = self._request_builder.build_get_order_params()  # This returns None

        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {} # Headers not used by this specific handler
        try:
            response_tuple = await self._http_client_requester(
                method="GET",
                endpoint=endpoint,
                params=built_params,
                is_signed=True,  # built_params will be None
            )
            raw_data, status_code, _ = response_tuple  # Headers ignored for this handler
            logger.debug(
                f"[{self._exchange_name}] Get order status for {order_id} ({symbol}): {raw_data}, status: {status_code}"
            )

            # The _http_client_requester or its underlying _request should raise APIError for 404s.
            # If it maps to ORDER_NOT_FOUND, the except block below will handle it.
            # If it doesn't, then the error mapper in ExchangeAPI should convert it.

            if raw_data is None:
                # This case implies a successful HTTP status (e.g., 200 OK) but empty body,
                # which is unexpected for a get_order endpoint that should return data or 404.
                raise APIError(
                    f"Get order status for {order_id} ({symbol}) returned no content despite successful status {status_code}",
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
                self._response_handler.handle_get_order_status_response(raw_data, order_id)
            )
            # If raw_order_model was typed as BackpackRawOrder | None, the linter error was due to that.
            # Now that it's BackpackRawOrder, the None check previously was indeed problematic.
            # The APIError.ORDER_NOT_FOUND should be the mechanism for "not found".
            return BackpackOrderMapper.transform_raw_order_to_internal(raw_order_model)
        except APIError as e:
            # Specifically catch ORDER_NOT_FOUND (or any 404 mapped to it) and return None
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value or e.http_status == 404:
                logger.info(
                    f"[{self._exchange_name}] Order {order_id} ({symbol}) not found: {e.message}"
                )
                return None
            raise  # Re-raise other APIErrors
        except Exception as e:  # Catch any other unexpected errors
            logger.error(
                f"[{self._exchange_name}] Error getting order status for {order_id} ({symbol}): {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting order status for {order_id} ({symbol}): {e}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        results: list[CancelOrderResult] = []
        logger.info(
            f"[{self._exchange_name}] Attempting to cancel all open orders in service"
            f"{f' for symbol {symbol}' if symbol else ''}."
        )
        try:
            open_orders_to_cancel = await self.get_open_orders(symbol=symbol)
            if not open_orders_to_cancel:
                logger.info(
                    f"[{self._exchange_name}] No open orders found in service"
                    f"{f' for symbol {symbol}' if symbol else ''} to cancel."
                )
                return []

            for order_to_cancel in open_orders_to_cancel:
                exch_order_id = order_to_cancel.exchange_order_id
                client_id = order_to_cancel.client_order_id
                order_symbol = order_to_cancel.symbol

                if not exch_order_id:
                    logger.warning(
                        f"[{self._exchange_name}] Open order from service has no exchange_order_id. "
                        f"Order details: client_id={client_id}, symbol={order_symbol}. Skipping."
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=None,
                            client_order_id=client_id,
                            symbol=order_symbol,
                            success=False,
                            message="Order has no exchange_order_id for cancellation.",
                            status=CancelOrderResultStatus.FAILED,
                        )
                    )
                    continue

                if not order_symbol:
                    logger.error(
                        f"[{self._exchange_name}] Open order (ID: {exch_order_id}) missing symbol. Skipping."
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=exch_order_id,
                            client_order_id=client_id,
                            symbol=None,
                            success=False,
                            message="Order is missing symbol information for cancellation.",
                            status=CancelOrderResultStatus.FAILED,
                        )
                    )
                    continue

                try:
                    cancelled = await self.cancel_order(
                        order_id=exch_order_id,
                        symbol=order_symbol,
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=exch_order_id,
                            client_order_id=client_id,
                            symbol=order_symbol,
                            success=cancelled,
                            message="Successfully cancelled by service."
                            if cancelled
                            else "Failed to cancel via service.",
                            status=CancelOrderResultStatus.SUCCESS
                            if cancelled
                            else CancelOrderResultStatus.FAILED,
                        )
                    )
                except APIError as e_cancel:
                    raw_body = (
                        getattr(e_cancel.original_exception, "response_body", None)
                        if isinstance(e_cancel.original_exception, APIError)
                        else None
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=exch_order_id,
                            client_order_id=client_id,
                            symbol=order_symbol,
                            success=False,
                            message=e_cancel.message,
                            status=CancelOrderResultStatus.FAILED,
                            raw_response=raw_body,
                        )
                    )
                except Exception as e_unexp_cancel:
                    logger.error(
                        f"[{self._exchange_name}] Unexpected error in service cancelling order {exch_order_id} "
                        f"for {order_symbol}: {e_unexp_cancel}",
                        exc_info=True,
                    )
                    results.append(
                        CancelOrderResult(
                            order_id=exch_order_id,
                            client_order_id=client_id,
                            symbol=order_symbol,
                            success=False,
                            message=str(e_unexp_cancel),
                            status=CancelOrderResultStatus.FAILED,
                        )
                    )

            num_successful = sum(1 for r in results if r.success)
            if not open_orders_to_cancel:
                pass
            elif num_successful == len(open_orders_to_cancel):
                logger.info(
                    f"[{self._exchange_name}] Successfully cancelled all ({num_successful}) "
                    f"open orders in service{f' for symbol {symbol}' if symbol else ''}."
                )
            else:
                logger.warning(
                    f"[{self._exchange_name}] Attempted to cancel {len(open_orders_to_cancel)} orders in service, "
                    f"but only {num_successful} were confirmed cancelled"
                    f"{f' for symbol {symbol}' if symbol else ''}."
                )

        except APIError as e_fetch_orders:
            logger.error(
                f"[{self._exchange_name}] APIError fetching open orders for service cancel_all_orders"
                f"{f' (symbol: {symbol})' if symbol else ''}: {e_fetch_orders.message}"
            )
            results.append(
                CancelOrderResult(
                    order_id=None,
                    symbol=symbol,
                    success=False,
                    message=f"Service failed to fetch open orders: {e_fetch_orders.message}",
                    status=CancelOrderResultStatus.FAILED,
                    raw_response=getattr(e_fetch_orders.original_exception, "response_body", None)
                    if isinstance(e_fetch_orders.original_exception, APIError)
                    else None,
                )
            )
        except Exception as e_unexp_outer:
            logger.error(
                f"[{self._exchange_name}] Unexpected error during service cancel_all_orders"
                f"{f' (symbol: {symbol})' if symbol else ''}: {e_unexp_outer}",
                exc_info=True,
            )
            results.append(
                CancelOrderResult(
                    order_id=None,
                    symbol=symbol,
                    success=False,
                    message=f"Service unexpected outer error: {str(e_unexp_outer)}",
                    status=CancelOrderResultStatus.FAILED,
                )
            )
        return results
