"""
CyberDeltaEngine: Backpack Exchange Integration
----------------------------------------------

This module implements the Backpack exchange adapter for CyberDeltaEngine, including:
- REST and WebSocket API client (`BackpackAPI`)
- Centralized error mapping and normalization (`BackpackErrorMapper`)
- Order and event transformation utilities (`BackpackOrderMapper`)

**Key architectural patterns:**
- All external (exchange) errors are mapped to canonical APIErrorCode values, validated and
  normalized via APIErrorResponse, and propagated as APIError exceptions.
- All API methods are type-safe, defensive, and log/handle edge cases robustly.
- All transformation logic is modular and testable.

**Onboarding Note:**
- When extending this module for new endpoints or error types, always use strict Pydantic
  validation, map all error codes, and document any non-obvious logic or edge cases.
"""

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import (
    BackpackResponseHandler,
    RawJson,
    RawJsonResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import (
    BackpackRawWithdrawalResponse,
)
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.base.exchange_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class BackpackAPI(ExchangeAPI):
    """
    Backpack Exchange API Client.

    Implements connectivity and data handling for the Backpack exchange,
    adhering to the ExchangeAPI interface.

    Handles REST API requests and WebSocket connections for market data and account updates.
    Rate limit handling is currently a placeholder as Backpack's OpenAPI specification
    does not clearly define standard rate limit response headers. Further investigation
    or documentation from Backpack would be needed to implement robust rate limiting.
    """

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        """
        Initialize the BackpackAPI client with configuration and secrets.

        Args:
            api_config: Dictionary of API configuration parameters.
            secrets: Dictionary of secret values (API key/secret).
        """
        self._api_key = secrets.get("BACKPACK_API_KEY")
        self._api_secret = secrets.get("BACKPACK_API_SECRET")

        if self._api_key and self._api_secret:
            self._bp_authenticator: BackpackHmacAuthenticator | None = BackpackHmacAuthenticator(
                api_key=self._api_key, api_secret=self._api_secret
            )
        else:
            logger.warning(
                "Backpack API key/secret not provided. Signed operations will fail. "
                "Authenticator not initialized."
            )
            self._bp_authenticator = None

        self._backpack_error_mapper = BackpackErrorMapper()
        self._bp_mapper = BackpackOrderMapper()

        super().__init__(
            exchange_name="backpack",
            config=api_config,
            secrets=secrets,
            authenticator=self._bp_authenticator,
            error_mapper=self._backpack_error_mapper,
        )

        self.default_headers: dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

    # --- WebSocket Implementation --- #

    def _construct_subscription_payload(self, topic: str) -> dict[str, Any] | None:
        """Constructs the subscription payload for a given topic for Backpack."""
        # Based on existing BackpackAPI.subscribe method
        return {
            "op": "subscribe",
            "channel": topic,
            "args": {},  # Backpack doesn't seem to use args for common subscriptions
        }

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message from WebSocketManager, then route it.
        This is called by the WebSocketManager.
        """
        # For Backpack, current logic seems to be direct routing.
        # Add any pre-processing if needed before routing.
        # For example, parsing a common outer envelope if Backpack had one.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """
        Route incoming WebSocket messages to the appropriate handler based on topic.
        Backpack messages typically have a 'stream' or 'topic' field in the outer message
        or the data payload itself indicates the type.
        Example Backpack WS message structure: {"topic": "depth.SOL_USDC", "data": {...}}
        """
        # Backpack messages typically have a 'topic' field identifying the stream
        # The actual data payload is often in message["data"]
        topic = message.get("topic")  # Or derive from message structure if different
        data_payload = message.get("data")

        if not topic:
            # Sometimes the topic might be part of the data key, e.g. for user streams
            # For Backpack, public streams have a top-level topic key.
            # Private streams like 'fills' or 'orders' might be structured differently,
            # e.g. message = {"type": "fills", "data": ...}
            # Adapt this logic based on actual Backpack private stream formats.
            event_type = message.get("type")  # Example for a private stream format
            if event_type in ["fills", "orders"] and isinstance(
                event_type, str
            ):  # Ensure event_type is str
                topic = event_type  # Use type as topic for private streams
                data_payload = message  # Handler might expect the whole message
            else:
                # Ensure topic is not None and is a string before proceeding
                if topic is None or not isinstance(topic, str):
                    logger.debug(
                        f"[{self.exchange_name}] Unroutable message "
                        f"(no clear string topic/type): {message}"
                    )
                    return

        if data_payload is None:  # Ensure data_payload is present
            logger.debug(
                f"[{self.exchange_name}] Received message with topic '{topic}'"
                f" but no data: {message}"
            )
            return

        handler = self._ws_handlers.get(topic)
        if handler:
            try:
                # Pass the data_payload to the handler. For Backpack,
                # this is typically message["data"]
                # For private streams, if data_payload was set to message, it works out.
                await handler(data_payload, message)
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error in handler for topic {topic}: {e}",
                    exc_info=True,
                )
        else:
            logger.debug(f"[{self.exchange_name}] No handler registered for topic: {topic}")

    # --- Authentication --- #

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Authenticate and sign an API request for Backpack using BackpackHmacAuthenticator.

        Returns:
            Dictionary with signed headers, params, and data as expected by base _request.
        """
        if not self._bp_authenticator:
            logger.error(
                f"[{self.exchange_name}] Backpack authenticator not initialized. "
                f"Cannot make signed request."
            )
            raise APIError(
                "Backpack authenticator not initialized. Cannot make signed request.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        current_headers = self.default_headers.copy()
        # Add other necessary headers like X-BP-Timestamp, X-BP-Window
        # These are typically added by the authenticator, but let's ensure the call is right.

        # --- DEBUG PRINT --- #
        print(
            f"[DEBUG BP_API _authenticate] About to await prepare_request. "
            f"Authenticator: {self._bp_authenticator}",
            flush=True,
        )
        # --- END DEBUG --- #

        auth_components: AuthenticatedRequestComponents = (
            await self._bp_authenticator.prepare_request(
                method=method, path=path, params=params, data=data, headers=current_headers
            )
        )

        # --- DEBUG PRINT --- #
        print("[DEBUG BP_API _authenticate] Finished awaiting prepare_request.", flush=True)
        # --- END DEBUG --- #

        # _authenticate should return a dict matching the structure expected by _request
        return {
            "headers": auth_components["headers"],
            "params": auth_components["params"],
            "data": auth_components["data"],
        }

    # async def _handle_ws_message(self, message: Mapping[str, Any], ws_url: str) -> None:
    #     # This method is not provided in the original file or the code block
    #     # It's assumed to exist as it's called in the _route_ws_message method
    #     pass # REMOVING THIS UNUSED AND CONFUSING METHOD

    # --- Core API Implementation --- #

    async def get_ticker(self, symbol: str) -> Ticker:
        """Fetches the latest ticker information for a specific symbol."""
        endpoint = "/api/v1/ticker"
        params = BackpackRequestBuilder.build_get_ticker_params(symbol=symbol)
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw = await self._request("GET", endpoint, params=params)

            if not isinstance(response_data_raw, dict):
                logger.error(
                    f"[{self.exchange_name}] Unexpected ticker response format for "
                    f"{symbol}: {type(response_data_raw)}"
                )
                # Use the new APIErrorCode.INVALID_RESPONSE
                raise APIError(
                    message=f"Unexpected ticker response format: {type(response_data_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            # Validate using the handler
            raw_ticker: BackpackRawTicker = BackpackResponseHandler.handle_get_ticker_response(
                response_data_raw, symbol
            )
            # Transform using the instance mapper method
            return self._bp_mapper.transform_raw_ticker_to_internal(
                raw_ticker, symbol_override=symbol
            )

        except ValueError as e_transform:  # Catch ValueErrors from transform_raw_ticker_to_internal
            logger.error(
                f"[{self.exchange_name}] Ticker transformation failed for {symbol}: "
                f"{e_transform}. Raw: {response_data_raw!r}"
            )
            raise APIError(
                message=f"Failed to transform ticker data for {symbol}: {e_transform}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_transform,
            ) from e_transform
        except APIError:  # Re-raise APIErrors directly (e.g., from _request)
            raise
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unhandled error fetching ticker for {symbol}: "
                f"{e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing ticker for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Fetch the order book for a symbol, validated via Raw model and transformed via Mapper.

        Args:
            symbol: The trading symbol (e.g., 'BTC_USDC').
            depth: Ignored for Backpack (uses default depth). Included for interface consistency.

        Returns:
            OrderBook object.

        Raises:
            APIError: If the order book cannot be fetched or validated.
        """
        endpoint = "/api/v1/depth"
        params = BackpackRequestBuilder.build_get_order_book_params(symbol=symbol, limit=depth)
        response_raw: RawJsonResponse | None = None
        try:
            response_raw = await self._request("GET", endpoint, params=params)
            # Validate raw order book data using Pydantic model
            validated_book: BackpackRawOrderBook = (
                BackpackResponseHandler.handle_get_order_book_response(response_raw, symbol)
            )
            # Transform validated raw data to internal model using instance mapper
            internal_book = self._bp_mapper.transform_raw_orderbook_to_internal(
                symbol=symbol, raw=validated_book
            )
            # TODO: Apply depth limit if needed (Backpack provides full depth)
            # For now, return the full book as mapped
            return internal_book
        except ValueError as e:  # Catches errors from transform method
            logger.error(
                f"[{self.exchange_name}] Order book transformation failed for {symbol}: {e}. "
                f"Raw Data: {response_raw}"
            )
            raise APIError(
                f"Order book transformation failed for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting order book for {symbol}: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_order_book: {e}", exc_info=True
            )
            # Re-raise as a generic APIError
            raise APIError(
                f"Unexpected error fetching order book for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """
        Get recent trades for a symbol, mapping all required fields for the Trade model.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade objects
        """
        endpoint = "/api/v1/trades"
        params = BackpackRequestBuilder.build_get_recent_trades_params(symbol=symbol, limit=limit)
        try:
            response_raw: RawJsonResponse = await self._request("GET", endpoint, params=params)

            # DEFENSIVE CHECK: Runtime check before processing
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected trades response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            # Validate using the handler (handles list structure and item validation)
            validated_trades: list[BackpackRawTrade] = (
                BackpackResponseHandler.handle_get_recent_trades_response(response_raw, symbol)
            )

            # Transformation logic remains here
            trades: list[Trade] = []
            for raw_trade in validated_trades:
                try:
                    # Transform validated raw trade to internal Trade using instance mapper
                    internal_trade = self._bp_mapper.transform_raw_trade_to_internal(raw=raw_trade)

                    # Append only if transformation succeeded (returned Trade, not None)
                    if internal_trade:
                        trades.append(internal_trade)

                except ValidationError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade due to validation error: {e}. "
                        f"Data: {raw_trade.model_dump_json()}"
                    )
                    continue
                except ValueError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade due to transformation error: {e}. "
                        f"Raw Data: {raw_trade.model_dump_json()}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing trade: {e}. "
                        f"Data: {raw_trade.model_dump_json()}",
                        exc_info=True,
                    )
                    continue
            return trades
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API Error getting recent trades for {symbol}: {e}"
            )
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_recent_trades: {e}", exc_info=True
            )
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error getting recent trades: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances, validated via Raw models and transformed via Mapper."""
        endpoint = "/api/v1/capital"
        params = BackpackRequestBuilder.build_get_balances_params()
        response_data_raw: RawJsonResponse = await self._request("GET", endpoint, params=params)

        # DEFENSIVE CHECK: Runtime check before processing
        if not isinstance(response_data_raw, dict):
            logger.warning(
                f"[{self.exchange_name}] Unexpected response type for balances "
                f"({type(response_data_raw)}). Expected dict. Returning empty balances."
            )
            return {}

        # Validate using the handler
        validated_balances: dict[str, BackpackRawBalance] = (
            BackpackResponseHandler.handle_get_balances_response(response_data_raw)
        )

        # Transformation logic remains here
        processed_balances: dict[str, SpotBalance] = {}
        for asset_symbol, raw_balance in validated_balances.items():
            try:
                # Transform validated raw balance to internal SpotBalance using instance mapper
                internal_balance = self._bp_mapper.transform_raw_balance_to_internal(
                    asset_symbol=asset_symbol, raw=raw_balance
                )
                processed_balances[internal_balance.asset] = internal_balance

            except ValidationError as e:
                logger.error(
                    f"[{self.exchange_name}] Failed Pydantic validation for balance "
                    f"{asset_symbol}: {e}. Data: {raw_balance}"
                )
                continue  # Skip this asset if validation fails
            except ValueError as e:  # Catches errors from transform_raw_balance_to_internal
                logger.error(
                    f"[{self.exchange_name}] Failed transformation for balance "
                    f"{asset_symbol}: {e}. Raw Data: {raw_balance}"
                )
                continue  # Skip this asset if transformation fails
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Unexpected error processing balance for "
                    f"{asset_symbol}: {e}",
                    exc_info=True,
                )
                continue
        return processed_balances

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Fetches current open positions, optionally filtered by symbol."""
        endpoint: str
        if symbol:
            endpoint = f"/api/v1/positions/{BackpackRequestBuilder.format_symbol(symbol)}"
        else:
            endpoint = "/api/v1/positions"

        # build_get_positions_params returns None, which is correct as symbol is in path
        # or not used.
        # So, explicitly pass None if that was the intent, or ensure builder returns {}.
        # The current builder returns None. self._request handles params=None.
        request_params = BackpackRequestBuilder.build_get_positions_params(symbol=symbol)
        response_raw: RawJsonResponse | None = None
        positions: list[DerivativePosition] = []  # Initialize outside try for return path
        try:
            response_raw = await self._request(
                method="GET", endpoint=endpoint, params=request_params, is_signed=True
            )
            # DEFENSIVE CHECK: Ensure response is list
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected response type for positions: "
                    f"{type(response_raw)}. Returning empty list."
                )
                return []

            # Validate using the handler
            validated_positions: list[BackpackRawPosition] = (
                BackpackResponseHandler.handle_get_positions_response(response_raw, symbol)
            )

            # Transformation logic remains here
            for raw_position in validated_positions:
                try:
                    # Transform validated raw position to internal DerivativePosition
                    # using the instance mapper
                    internal_position = self._bp_mapper.transform_raw_position_to_internal(
                        raw=raw_position
                    )
                    positions.append(internal_position)

                except ValidationError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping position due to validation error: "
                        f"{e}. Data: {raw_position.model_dump_json()}"
                    )
                    continue
                except ValueError as e:  # Catches errors from transform_raw_position_to_internal
                    logger.warning(
                        f"[{self.exchange_name}] Skipping position due to transformation "
                        f"error: {e}. Raw Data: {raw_position.model_dump_json()}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing position: "
                        f"{e}. Data: {raw_position.model_dump_json()}",
                        exc_info=True,
                    )
                    continue
            return positions
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting positions: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_positions: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error getting positions: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,  # Added to match ExchangeAPI base method
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """
        Place an order on Backpack Exchange. Conforms to ExchangeAPI interface.

        Args:
            symbol: Trading symbol (e.g., 'BTC_USDC')
            side: Order side (BUY or SELL)
            order_type: Order type (LIMIT, MARKET, etc.)
            quantity: Order quantity (as Decimal)
            time_in_force: Time in force (GTC, IOC, FOK).
            price: Order price (required for limit orders, as Decimal)
            stop_price: Stop price for stop orders (currently NOT supported by Backpack
                        for basic order placement via this method).
            client_order_id: Custom client order ID
            reduce_only: Whether this is a reduce-only order (bool)
            post_only: Whether this is a post-only order (bool)

        Returns:
            Order object if successful.

        Raises:
            ValueError: If price is missing for a LIMIT order.
            APIError: On API errors or if the order placement fails.
        """
        endpoint = "/api/v1/order"

        # Old payload construction removed
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            client_order_id=client_order_id,
            post_only=post_only,
            trigger_price=stop_price,  # Pass stop_price as trigger_price to builder
        )

        try:
            # Make a single request. The type 'Any' is used initially as _request
            # from base might return various structures before validation.
            response_data: Any = await self._request(
                method="POST", endpoint=endpoint, data=payload, is_signed=True
            )

            # Attempt to use response_data directly if it's a dict, assuming it might
            # be a direct success response that the handler can process.
            if isinstance(response_data, dict):
                try:
                    # We've confirmed response_data is a dict, cast for the handler
                    raw_order_from_direct: BackpackRawOrder = (
                        BackpackResponseHandler.handle_place_order_response(
                            cast(dict[str, Any], response_data)
                        )
                    )
                    return self._bp_mapper.transform_raw_order_to_internal(raw_order_from_direct)
                except (APIError, ValidationError, ValueError) as e_direct_map:
                    logger.warning(
                        f"[{self.exchange_name}] Failed to directly map initial place_order "
                        f"response (was dict): {e_direct_map}. Response: {response_data!r}. "
                        f"Falling back to generic RawJsonResponse validation."
                    )
                    # Fall through if direct dict mapping fails, try validating response_data
                    # as RawJsonResponse

            # Fallback or default path: Validate response_data as RawJsonResponse
            # This assumes response_data, despite being Any, holds the raw content.
            # If _request guarantees RawJsonResponse on success, this is safer.
            # If not, further checks on response_data structure might be needed here.
            raw_order: BackpackRawOrder = BackpackResponseHandler.handle_place_order_response(
                cast(RawJsonResponse, response_data)  # Cast to expected handler input
            )
            return self._bp_mapper.transform_raw_order_to_internal(raw_order)

        except ValueError as ve:  # e.g., missing price for limit order from builder
            raise ve
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error placing order: {e}")
            raise e
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}", exc_info=True)
            # Ensure final exception path raises APIError, satisfying return type check
            raise APIError(
                f"Unexpected error placing order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order. Returns True if successful."""
        if symbol is None:
            # Attempt to lookup symbol from order_id if possible, or raise error
            # For now, raise error as Backpack API likely requires symbol
            raise ValueError("Symbol is required to cancel order on Backpack")

        endpoint = "/api/v1/order"
        params = BackpackRequestBuilder.build_cancel_order_params(symbol=symbol, order_id=order_id)
        try:
            # Assign the response to response_raw
            response_raw: RawJsonResponse = await self._request(
                "DELETE", endpoint, params=params, is_signed=True
            )
            # Use handler, passing the fetched response_raw
            _ = BackpackResponseHandler.handle_cancel_order_response(
                raw_response_content=response_raw, order_id=order_id, symbol=symbol
            )
            logger.info(f"[{self.exchange_name}] Canceled order {order_id} for {symbol}")
            # Return True on success (APIError wasn't raised)
            return True
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error canceling order {order_id}: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error canceling order {order_id}: {e}", exc_info=True
            )
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        request_path = "/api/v1/orders"
        params = BackpackRequestBuilder.build_get_open_orders_params(symbol=symbol)
        try:
            response = await self._request("GET", request_path, params=params, is_signed=True)
            response_raw: RawJsonResponse = await self._request(
                "GET", request_path, params=params, is_signed=True
            )
            orders: list[Order] = []
            if not isinstance(response, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected open orders response type: {type(response)}"
                )
                return []
            # Validate using the handler
            validated_orders: list[BackpackRawOrder] = (
                BackpackResponseHandler.handle_get_open_orders_response(response_raw, symbol)
            )
            for raw_order in validated_orders:
                try:
                    internal_order: Order = self._bp_mapper.transform_raw_order_to_internal(
                        raw_order
                    )
                    if internal_order.status in [
                        OrderStatus.NEW,
                        OrderStatus.OPEN,
                        OrderStatus.PARTIALLY_FILLED,
                    ]:
                        orders.append(internal_order)
                except ValidationError as e:
                    msg = (
                        f"[{self.exchange_name}] Skipping order due to "
                        f"Pydantic validation error: {e}. "
                        f"Data: {raw_order.model_dump_json()}"
                    )
                    logger.warning(msg)
                except APIError as e:
                    msg = (
                        f"[{self.exchange_name}] Skipping order due to "
                        f"transformation error: {e}. "
                        f"Data: {raw_order.model_dump_json()}"
                    )
                    logger.warning(msg)
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing single order: {e}",
                        exc_info=True,
                    )
            return orders
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting open orders: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_open_orders: {e}", exc_info=True
            )
            raise APIError(
                f"Failed to get open orders: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Fetches the current funding rate for a given perpetual market."""
        endpoint = f"/api/v1/markets/{BackpackRequestBuilder.format_symbol(symbol)}/funding"
        params = BackpackRequestBuilder.build_get_funding_rate_params(symbol=symbol)  # Returns {}
        response_raw: RawJsonResponse | None = None
        try:
            response_raw = await self._request(method="GET", endpoint=endpoint, params=params)
            if not isinstance(response_raw, dict):
                logger.error(
                    f"[{self.exchange_name}] Unexpected funding rate response format for "
                    f"{symbol}: {type(response_raw)}"
                )
                # Use the new APIErrorCode.INVALID_RESPONSE
                raise APIError(
                    message=f"Unexpected funding rate response format: {type(response_raw)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            # Validate with BackpackRawFundingRate
            raw_funding_rate: BackpackRawFundingRate = (
                BackpackResponseHandler.handle_get_funding_rate_response(response_raw, symbol)
            )
            # Transform using the instance mapper method
            return self._bp_mapper.transform_raw_funding_rate_to_internal(raw_funding_rate)

        except ValidationError as e_val:
            logger.error(
                f"[{self.exchange_name}] Funding rate validation failed for {symbol}: "
                f"{e_val}. Raw: {response_raw!r}"
            )
            raise APIError(
                message=f"Invalid funding rate data from exchange for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except (
            ValueError
        ) as e_transform:  # Catch ValueErrors from transform_raw_funding_rate_to_internal
            logger.error(
                f"[{self.exchange_name}] Funding rate transformation failed for {symbol}: "
                f"{e_transform}. Raw: {response_raw!r}"
            )
            raise APIError(
                message=f"Failed to transform funding rate data for {symbol}: {e_transform}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_transform,
            ) from e_transform
        except APIError:  # Re-raise APIErrors directly (e.g., from _request)
            raise
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unhandled error fetching funding rate for "
                f"{symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing funding rate for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    # --- Placeholder for required abstract method --- #
    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """(Not Implemented) Get funding rates for one/all symbols."""
        logger.warning(
            f"[{self.exchange_name}] get_funding_rates not fully implemented. "
            f"Fetching current rate only."
        )
        rates: list[FundingRate] = []
        if symbols:
            for symbol in symbols:
                try:
                    current_rate: FundingRate = await self.get_funding_rate(symbol)
                    rates.append(current_rate)
                except APIError as e:
                    logger.error(
                        f"[{self.exchange_name}] Failed to fetch current funding rate for "
                        f"{symbol} within get_funding_rates: {e}"
                    )
                    raise
        else:
            logger.error(
                f"[{self.exchange_name}] get_funding_rates without a specific symbol "
                f"is not supported by Backpack API."
            )
            raise APIError(
                code=APIErrorCode.INVALID_PARAMS.value,
                message="Symbol is required for get_funding_rates on Backpack",
            )
        return rates

    # --- Account Management --- #
    async def get_account_info(self) -> BackpackRawAccountSummary | None:
        """Fetches raw account settings and fee structures from Backpack.
        Returns the validated raw Pydantic model or None if an error occurs.
        """
        # Construct the full endpoint URL for the account info
        # Backpack's account endpoint is typically /api/v1/account relative to base URL
        if not self.rest_endpoint:
            logger.error(f"[{self.exchange_name}] REST endpoint not configured.")
            return None

        # endpoint_path = "account" # Unused variable

        api_path = "/api/v1/account"  # The specific path for this endpoint

        try:
            response_raw = await self._request(  # Call the _request method from ExchangeAPI
                method="GET",
                endpoint=api_path,  # Pass the relative path
                is_signed=True,  # This is a private, signed endpoint
            )

            validated_account_summary = BackpackResponseHandler.handle_get_account_info_response(
                response_raw
            )
            return validated_account_summary

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error fetching account info: {e}")
            # if e.code == APIErrorCode.RESOURCE_NOT_FOUND.value: # Temporarily commented out
            #     logger.warning(f"[{self.exchange_name}] Account info not found on Backpack.")
            #     return None
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching account info: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error getting account info: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """
        Fetches and transforms account summary information, balances, and positions
        into the internal MarginAccountSummary model.
        """
        logger.debug(f"[{self.exchange_name}] Fetching full account summary...")
        raw_account_settings: BackpackRawAccountSummary | None = None
        raw_spot_balances: dict[str, BackpackRawBalance] = {}
        raw_derivative_positions: list[BackpackRawPosition] = []

        try:
            # 1. Fetch Raw Account Settings
            raw_account_settings = await self.get_account_info()
            if not raw_account_settings:
                logger.warning(
                    f"[{self.exchange_name}] Failed to fetch raw account settings for summary."
                )
                return None

            # 2. Fetch and Validate Raw Balances
            balances_endpoint = "/api/v1/capital"
            balances_params = BackpackRequestBuilder.build_get_balances_params()
            balances_response_raw: RawJsonResponse | None = None
            try:
                balances_response_raw = await self._request(
                    "GET", balances_endpoint, params=balances_params, is_signed=True
                )
                if not isinstance(balances_response_raw, dict):
                    logger.error(
                        f"[{self.exchange_name}] Unexpected balances response type in "
                        f"get_account_summary: {type(balances_response_raw)}. Expected dict. "
                        f"Raw: {balances_response_raw!r}"
                    )
                    # Depending on strictness, might raise or return None here
                    # For now, assign empty dict to raw_spot_balances if response is bad
                    raw_spot_balances = {}
                else:
                    raw_spot_balances = BackpackResponseHandler.handle_get_balances_response(
                        balances_response_raw
                    )
            except APIError as e_balance_fetch:
                logger.error(
                    f"[{self.exchange_name}] API Error fetching balances for summary: "
                    f"{e_balance_fetch}"
                )
                # Decide if partial summary is acceptable or if we should return None/re-raise
                # For now, allow proceeding with empty raw_spot_balances
                raw_spot_balances = {}

            # 3. Fetch and Validate Raw Positions
            positions_endpoint = "/api/v1/positions"
            positions_params = BackpackRequestBuilder.build_get_positions_params(symbol=None)
            positions_response_raw: RawJsonResponse | None = None
            try:
                positions_response_raw = await self._request(
                    "GET",  # Use positional for method
                    positions_endpoint,  # Use positional for endpoint
                    params=positions_params,
                    is_signed=True,
                )
                if not isinstance(positions_response_raw, list):
                    logger.error(
                        f"[{self.exchange_name}] Unexpected positions response type in "
                        f"get_account_summary: {type(positions_response_raw)}. Expected list. "
                        f"Raw: {positions_response_raw!r}"
                    )
                    raw_derivative_positions = []  # Assign empty list if response is bad
                else:
                    raw_derivative_positions = (
                        BackpackResponseHandler.handle_get_positions_response(
                            positions_response_raw,
                            symbol=None,  # symbol=None for all positions
                        )
                    )
            except APIError as e_positions_fetch:
                logger.error(
                    f"[{self.exchange_name}] API Error fetching positions for summary: "
                    f"{e_positions_fetch}"
                )
                # Allow proceeding with empty raw_derivative_positions
                raw_derivative_positions = []

            # 4. Transform all raw components using the instance mapper
            try:
                internal_summary = self._bp_mapper.transform_raw_account_summary_to_internal(
                    raw_settings=raw_account_settings,
                    spot_balances_raw=raw_spot_balances,
                    derivative_positions_raw=raw_derivative_positions,
                    # timestamp=datetime.now(UTC) # Example if timestamp was needed by mapper
                )
                logger.info(
                    f"[{self.exchange_name}] Successfully generated internal account summary."
                )
                return internal_summary
            except ValueError as e_map:
                logger.error(
                    f"[{self.exchange_name}] Error mapping raw account data to internal "
                    f"summary: {e_map}",
                    exc_info=True,
                )
                raise APIError(
                    message=f"Failed to map account summary due to invalid data "
                    f"or mapper error: {e_map}",
                    code=APIErrorCode.INVALID_RESPONSE.value,  # Use INVALID_RESPONSE
                    # for mapping issues
                    original_exception=e_map,
                ) from e_map
            # except Exception as e_map_other: # Catch other potential mapper errors if necessary
            #     logger.error(
            #         f"[{self.exchange_name}] Unexpected error during account data "
            #         f"mapping: {e_map_other}",
            #         exc_info=True,
            #     )
            #     raise APIError(
            #         message=f"Unexpected error mapping account summary: {e_map_other}",
            #         code=APIErrorCode.UNKNOWN.value,
            #         original_exception=e_map_other,
            #     ) from e_map_other

        except APIError as e:
            # This will catch APIErrors from get_account_info, or from _request for
            # balances/positions if they raise APIError
            # Also catches the re-raised APIError from the new mapper exception handling above.
            logger.error(
                f"[{self.exchange_name}] API Error in get_account_summary orchestration: {e}"
            )
            # If the caught error is already the one from mapping, just let it propagate
            if e.code == APIErrorCode.INVALID_RESPONSE.value and isinstance(
                e.original_exception, ValueError
            ):
                raise  # Re-raise the specific mapping error
            return None  # For other APIErrors during fetching, return None
        except Exception as e_unhandled:  # Catch-all for truly unexpected issues
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_account_summary "
                f"orchestration: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error during get_account_summary: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,  # e.g., "spot", "futures" - specific values TBD by exchange
        to_account_type: str,  # e.g., "spot", "futures" - specific values TBD by exchange
        client_transfer_id: str | None = None,
    ) -> dict[str, Any]:  # Return type TBD by actual API response structure
        """
        Initiate an internal transfer of assets between user accounts (e.g., spot to futures).

        Backpack's OpenAPI specification (as of review) does not explicitly detail a separate
        endpoint for internal account transfers distinct from general withdrawals/deposits.
        If such functionality exists, it may be part of a subaccount system or implicitly handled
        via specific parameters in deposit/withdrawal flows to other owned accounts/addresses.
        This method remains as a placeholder for potential future implementation if a dedicated
        API for internal transfers is identified or becomes available.

        Args:
            asset: The asset to transfer (e.g., "USDC").
            amount: The amount of the asset to transfer.
            from_account_type: The type of account to transfer from.
            to_account_type: The type of account to transfer to.
            client_transfer_id: Optional client-provided ID for the transfer.

        Returns:
            A dictionary containing the API response upon successful transfer.

        Raises:
            NotImplementedError: As this specific functionality is not clearly defined in
                                 Backpack's API.
            APIError: For API-level errors encountered during the request.
        """
        logger.warning(
            f"[{self.exchange_name}] The 'transfer' method is not implemented. Backpack API "
            f"does not clearly define a separate internal transfer endpoint. Consider using "
            f"withdraw/deposit mechanisms if applicable."
        )
        # Per OpenAPI, there is no dedicated internal transfer endpoint distinct from
        # deposit/withdrawals.
        # If transfers between subaccounts or to other owned accounts are needed, they likely use
        # the withdrawal mechanism with specific parameters or target addresses.
        raise NotImplementedError(
            "Backpack API does not provide a dedicated internal transfer endpoint. "
            "Use withdrawal/deposit with appropriate parameters if applicable."
        )

    async def withdraw(
        self,
        asset: str,
        amount: Decimal,
        address: str,
        network: str | None = None,  # Blockchain network
        tag: str | None = None,  # Destination tag / memo, if required
        client_withdrawal_id: str | None = None,  # Optional client-provided ID
        two_factor_token: str | None = None,  # Optional 2FA token
        **kwargs: dict[str, Any],  # For additional exchange-specific parameters
    ) -> dict[str, Any]:  # Return validated raw response
        """Initiate a withdrawal of assets from the exchange.

        Args:
            asset: The asset symbol to withdraw (e.g., "USDC").
            amount: The amount of the asset to withdraw.
            address: The destination address for the withdrawal.
            network: The blockchain network to use (e.g., "Solana", "Ethereum").
                     This maps to `blockchain` in the Backpack API.
            tag: Optional destination tag or memo, if required by the address/network.
                 (Note: Backpack API does not explicitly show a 'tag' field in
                  AccountWithdrawalPayload, this might need to be part of the
                  'address' or handled differently).
            client_withdrawal_id: Optional client-provided ID for the withdrawal.
                                  Maps to `clientId`.
            two_factor_token: Optional 2FA token if required by user settings.
            **kwargs: Additional keyword arguments for exchange-specific options,
                      e.g., `auto_borrow: bool`, `auto_lend_redeem: bool`.

        Returns:
            A BackpackRawWithdrawalResponse object containing the API response upon
            successful withdrawal.

        Raises:
            APIError: For API-level errors encountered during the request.
            ValueError: If required parameters like `network` are missing.
        """
        endpoint = "/api/v1/capital/withdrawals"

        # Build payload
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
            tag=tag,
            client_withdrawal_id=client_withdrawal_id,
            two_factor_token=two_factor_token,
        )
        # Initialize response_data to None
        response_data: Any = None
        # response_data_raw removed as it was unused

        try:
            # Make a single request.
            # Rename this variable to avoid redefinition error with the outer scope 'response_data'
            response_data_attempt: Any = await self._request(
                "POST",
                endpoint,
                data=payload,
                is_signed=True,
            )
            # Assign the result back to the outer scope variable for use in except blocks
            response_data = response_data_attempt

            # Attempt to use response_data directly if it's a dict.
            if isinstance(response_data, dict):
                try:
                    validated_response_direct: BackpackRawWithdrawalResponse = (
                        BackpackResponseHandler.handle_withdraw_response(
                            cast(dict[str, Any], response_data)  # Cast to dict
                        )
                    )
                    return self._bp_mapper.transform_raw_withdrawal_response_to_internal(
                        validated_response_direct
                    )
                except (APIError, ValidationError, ValueError) as e_direct_map:
                    logger.warning(
                        f"[{self.exchange_name}] Failed to directly map initial withdraw "
                        f"response (was dict): {e_direct_map}. Response: {response_data!r}. "
                        f"Falling back to generic RawJsonResponse validation."
                    )
                    # Fall through if direct dict mapping fails, try validating response_data
                    # as RawJsonResponse

            # Fallback or default path: Validate response_data as RawJsonResponse
            validated_response: BackpackRawWithdrawalResponse = (
                BackpackResponseHandler.handle_withdraw_response(
                    cast(RawJsonResponse, response_data)  # Cast to expected handler input
                )
            )
            return self._bp_mapper.transform_raw_withdrawal_response_to_internal(validated_response)

        except APIError:  # Handles validation errors from handler or _request
            # Assuming APIError is already logged by handler or _request if it originates there
            raise
        except (
            ValidationError,
            ValueError,
        ) as ve:  # Catch Pydantic/parsing errors if not wrapped by APIError
            logger.error(
                f"[{self.exchange_name}] Validation/Value error during withdrawal: {ve}. "
                f"Response: {response_data!r}",
                exc_info=True,
            )
            raise APIError(
                f"Validation/Value error during withdrawal: {ve}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=ve,
            ) from ve
        except Exception as e:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error during withdrawal: {e}. "
                f"Response: {response_data!r}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error during withdrawal: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
                http_status=None,  # Correct parameter name
                # response_body removed
            ) from e

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Subscribe to order book updates for a symbol."""
        topic = f"depth.{symbol}"
        # This method should likely just prepare the topic and potentially
        # trigger the subscription via a shared mechanism if needed,
        # but handler registration happens via self.subscribe called elsewhere.
        # For now, log intent. Actual subscription initiated by caller via self.subscribe.
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")
        # await self.subscribe(topic, handler) # Incorrect: Handler not passed here

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Subscribe to ticker updates for a symbol."""
        topic = f"ticker.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")
        # await self.subscribe(topic, handler) # Incorrect: Handler not passed here

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Subscribe to public trade updates for a symbol."""
        topic = f"trades.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")
        # await self.subscribe(topic, handler) # Incorrect: Handler not passed here

    async def subscribe_to_account_updates(self) -> None:
        """Subscribe to private account updates (balances, positions, orders)."""
        # This method signals intent or triggers setup. Actual subscriptions
        # with handlers are done via self.subscribe elsewhere.
        fill_topic = "fills"
        order_topic = "orders"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for account topics: "
            f"{fill_topic}, {order_topic}"
        )
        # await self.subscribe(fill_topic, handler) # Incorrect
        # await self.subscribe(order_topic, handler) # Incorrect

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = 100,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Fetches historical orders from Backpack.

        Args:
            symbol: Optional symbol filter.
            start_time: Optional start time filter (datetime UTC).
            end_time: Optional end time filter (datetime UTC).
            limit: Maximum number of orders to return (default 100).
            order_id: Optional filter by exchange order ID.
            client_order_id: Optional filter by client order ID.

        Returns:
            List of Order objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        endpoint = "/api/v1/ordersHistory"
        params: dict[str, str | int | None] = {}
        # Initialize limit first if not None
        if limit is not None:
            # Mypy struggles with conditional assignment to Union type dict value
            params["limit"] = limit
        # Add other params
        if symbol:
            params["symbol"] = symbol
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["clientId"] = client_order_id
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        response_raw: RawJsonResponse | None = None
        orders: list[Order] = []
        try:
            response_raw = await self._request("GET", endpoint, params=params, is_signed=True)

            # Validate using the handler
            validated_orders: list[BackpackRawOrder] = (
                BackpackResponseHandler.handle_get_order_history_response(response_raw, symbol)
            )

            # Transformation remains here
            for raw_order in validated_orders:
                try:
                    internal_order = self._bp_mapper.transform_raw_order_to_internal(raw_order)
                    orders.append(internal_order)
                except ValueError as e_transform:  # Catch transformation errors per item
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order in history due to "
                        f"transformation error: {e_transform}. Data: {raw_order.model_dump_json()}"
                    )
                    continue
            return orders

        except APIError as e:  # Catches validation errors from handler too
            logger.error(f"[{self.exchange_name}] API Error getting order history: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting order history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Error getting order history for {symbol or 'all'}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetch historical trades (fills), validated via Raw models and transformed via Mapper."""
        endpoint = "/api/v1/fillsHistory"
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = BackpackRequestBuilder.format_symbol(symbol)

        response_raw: RawJsonResponse | None = None
        try:
            response_raw = await self._request("GET", endpoint, params=params, is_signed=True)

            # Validate using the handler
            validated_trades: list[BackpackRawTrade] = (
                BackpackResponseHandler.handle_get_trade_history_response(response_raw, symbol)
            )

            # Transformation remains here
            trades: list[Trade] = []
            for raw_trade_model in validated_trades:
                try:
                    # Then transform to internal model using instance mapper
                    internal_trade = self._bp_mapper.transform_raw_trade_to_internal(
                        raw_trade_model
                    )
                    if internal_trade:  # Mapper returns Trade | None
                        trades.append(internal_trade)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping trade in history due to "
                            f"transformation failure (mapper returned None). "
                            f"Data: {raw_trade_model.model_dump_json()}"
                        )
                except (
                    ValidationError,
                    ValueError,
                ) as e:  # Catch Pydantic and other validation errors
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade in history due to validation/"
                        f"transformation error: {e}. Data: {raw_trade_model.model_dump_json()}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing historical "
                        f"trade: {e}. Data: {raw_trade_model.model_dump_json()}",
                        exc_info=True,
                    )
                    continue  # Continue with the next trade item
            return trades
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting trade history: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_trade_history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Error getting trade history for {symbol or 'all'}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """Fetch a single order by its ID.

        Args:
            order_id: The exchange-assigned order ID.
            symbol: The market symbol (required by Backpack history endpoint).

        Returns:
            The Order object if found, otherwise None.
        """
        try:
            # Reuse get_order_status which handles fetching and transformation
            return await self.get_order_status(order_id=order_id, symbol=symbol)
        except APIError as e:
            # If get_order_status raises ORDER_NOT_FOUND, return None as per this method's contract
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(
                    f"[{self.exchange_name}] Order {order_id} not found for symbol "
                    f"{symbol} (get_order)."
                )
                return None
            # Re-raise other API errors
            logger.error(f"[{self.exchange_name}] API error fetching order {order_id}: {e}")
            raise
        except Exception as e:
            # Re-raise unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching order {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order {order_id}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    # All abstract methods should now be implemented.

    # --- Abstract Method Implementations ---
    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """
        Update rate limit information based on response headers.
        Backpack does not typically provide rate limit info in standard headers.
        This is a placeholder implementation.
        """
        # Backpack does not seem to provide standard rate limit headers.
        # If specific headers are discovered, they could be parsed here.
        logger.debug(
            f"[{self.exchange_name}] _update_rate_limit_from_headers called (no-op for Backpack). "
            f"Headers: {headers}, Method: {method}, Path: {path}"
        )
        pass  # No specific Backpack headers known for this

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        # Backpack's get_open_orders handles symbol=None to fetch all.
        return await self.get_open_orders(symbol=symbol)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription."""
        # This should call super().subscribe to use WebSocketManager
        # Ensure ExchangeAPI.subscribe is not abstract or implement fully here
        logger.info(
            f"[{self.exchange_name}] Subscribe called for topic: {topic}. Delegating to base."
        )
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Callback for when WebSocket connects, typically to resubscribe."""
        logger.info(
            f"[{self.exchange_name}] WebSocket connected. Triggering resubscription via base."
        )
        await (
            super()._on_ws_connected()
        )  # This will call self._resubscribe if base implements it that way

    async def _resubscribe(self) -> None:
        """Resubscribe to topics upon WebSocket (re)connection."""
        logger.info(f"[{self.exchange_name}] Resubscribe called. Delegating to base.")
        await super()._resubscribe()

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        """Fetch historical market data (OHLCV/Kline) for a specific symbol and timeframe."""
        endpoint = "/api/v1/klines"
        params = BackpackRequestBuilder.build_get_market_data_params(
            symbol=symbol,
            timeframe_str=timeframe,
            start_time_ms=None,
            end_time_ms=None,
            limit=limit,
        )
        try:
            response_data: Any = await self._request(method="GET", endpoint=endpoint, params=params)
            # Validate using the handler (returns raw list if structure valid)
            validated_kline_list: list[RawJson] = (
                BackpackResponseHandler.handle_get_market_data_response(
                    response_data, symbol, timeframe
                )
            )

            # Transformation logic (placeholder) remains here
            if validated_kline_list:
                # Placeholder: Need actual mapping logic here
                # candles = [Candle(...) for item in response_data]
                # return candles
                logger.warning(
                    f"[{self.exchange_name}] Market data mapping not fully implemented "
                    f"for Backpack."
                )
                # Returning [] for now to satisfy list[Candle] return type until implemented
                # return response_data # This caused the Mypy error [no-any-return]
                return []  # Return empty list matching the required type
            else:
                logger.error(
                    f"[{self.exchange_name}] Unexpected market data response type: "
                    f"{type(response_data)}"
                )
                return []  # Return empty list on unexpected type

        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting market data: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_market_data: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Error getting market data for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_historical_trades(
        self, symbol: str, limit: int = 100, from_id: str | None = None
    ) -> list[Trade]:
        """Fetch historical trades for a symbol, mapped to internal Trade objects.

        Corresponds to Backpack's /api/v1/trades/history endpoint.
        The `from_id` parameter for Backpack corresponds to `offset`.
        """
        endpoint = "/api/v1/trades/history"
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id:
            params["fromId"] = from_id

        response_raw: RawJsonResponse | None = None
        try:
            response_raw = await self._request("GET", endpoint, params=params)

            validated_trades: list[BackpackRawTrade] = (
                BackpackResponseHandler.handle_get_historical_trades_response(response_raw, symbol)
            )

            internal_trades: list[Trade] = []
            for raw_trade_model in validated_trades:
                try:
                    internal_trade = self._bp_mapper.transform_raw_trade_to_internal(
                        raw_trade_model
                    )
                    if internal_trade:
                        internal_trades.append(internal_trade)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping trade in history due to "
                            f"transformation failure (mapper returned None). "
                            f"Data: {raw_trade_model.model_dump_json()}"
                        )
                except (
                    ValidationError,
                    ValueError,
                ) as e:  # Catch Pydantic and other validation errors during transformation
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade in history due to validation/"
                        f"transformation error: {e}. Data: {raw_trade_model.model_dump_json()}"
                    )
                    continue
                except Exception as e:  # Catch any other unexpected errors during item processing
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing historical "
                        f"trade: {e}. Data: {raw_trade_model.model_dump_json()}",
                        exc_info=True,
                    )
                    continue  # Continue with the next trade item
            return internal_trades
        except APIError as e:  # This will catch APIError from _request or from the handler
            logger.error(f"[{self.exchange_name}] API Error getting trades history: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting trades history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting trades history for {symbol or 'all'}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """Fetches the status of a single order by its orderId or clientId."""
        if not symbol:
            raise ValueError(
                "Symbol must be provided when fetching order status by ID/ClientID "
                "from Backpack, as it's a required query parameter."
            )

        identifier = order_id if order_id else client_order_id
        if not identifier:
            raise ValueError("Either order_id or client_order_id must be provided.")

        endpoint = f"/api/v1/orders/{identifier}"
        params: dict[str, str] = {"symbol": BackpackRequestBuilder.format_symbol(symbol)}
        response_data_raw: RawJsonResponse | None = None
        try:
            response_data_raw = await self._request(
                method="GET", endpoint=endpoint, params=params, is_signed=True
            )
            if response_data_raw is None:
                return None  # Or raise, depending on desired behavior for not found
            # Validate using the handler (handles None case by raising ORDER_NOT_FOUND)
            raw_order: BackpackRawOrder = BackpackResponseHandler.handle_get_order_status_response(
                response_data_raw, identifier
            )

            # Transformation remains here using instance mapper
            return self._bp_mapper.transform_raw_order_to_internal(raw_order)

        except APIError as e:  # Catches validation errors and ORDER_NOT_FOUND
            # Handle ORDER_NOT_FOUND specifically as per method contract
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(
                    f"[{self.exchange_name}] Order {identifier} not found on Backpack "
                    f"(get_order_status)."
                )
                return None  # Return None for not found

            # Log and re-raise other API errors
            logger.error(
                f"[{self.exchange_name}] API error fetching order {identifier} for {symbol}: {e}"
            )
            raise  # Re-raise other APIErrors (like INVALID_RESPONSE)

        except Exception:
            return None  # Or raise, depending on desired behavior for not found

    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        """
        Cancel all orders for a given symbol.
        """
        logger.info(
            f"[{self.exchange_name}] Attempting to cancel all orders for symbol: {symbol or 'all'}"
        )
        # Backpack does not have a single endpoint to cancel *all* orders across all symbols
        # or for a specific symbol in one go using DELETE /api/v1/orders without an ID.
        # DELETE /api/v1/orders with `symbol` in body/query might be for specific symbol's orders.
        # The current implementation in bp_api.py fetches open orders and cancels them one by one.
        # We will keep this logic but use the builder for individual cancel_order calls.

        # If Backpack had a bulk cancel: DELETE /api/v1/orders with params = {"symbol": ...}
        # params = BackpackRequestBuilder.build_cancel_all_orders_payload(symbol=symbol)
        # await self._request(
        # method="DELETE", endpoint="/api/v1/orders", params=params, is_signed=True
        # )
        # return

        # Current one-by-one cancellation logic:
        open_orders = await self.get_open_orders(symbol=symbol)
        if not open_orders:
            logger.info(f"[{self.exchange_name}] No open orders to cancel for {symbol or 'all'}")
            return

        for order_item in open_orders:
            try:
                order_to_cancel_id: str | None = order_item.exchange_order_id
                # Backpack cancel_order requires an ID. Prioritize exchange_order_id.
                if order_to_cancel_id:
                    await self.cancel_order(order_to_cancel_id, order_item.symbol)
                elif order_item.client_order_id:
                    # If Backpack primarily uses orderId (exchange) for cancellation,
                    # using clientId might need different handling or might not be supported
                    # by the DELETE /api/v1/order?symbol=X&orderId=Y endpoint structure.
                    # The builder build_cancel_order_params prefers orderId.
                    # If client_order_id is the *only* thing available, and cancel_order
                    # needs to use it, cancel_order method signature or builder logic
                    # might need adjustment.
                    # For now, attempting with client_order_id if exchange_order_id is missing.
                    logger.warning(
                        f"[{self.exchange_name}] Attempting to cancel order using "
                        f"client_order_id '{order_item.client_order_id}' as "
                        f"exchange_order_id is missing."
                    )
                    await self.cancel_order(order_item.client_order_id, order_item.symbol)
                else:
                    logger.error(
                        f"[{self.exchange_name}] Cannot cancel order, missing any usable ID "
                        f"for order: {order_item.model_dump_json(exclude_none=True)}"
                    )

            except APIError as e:
                # Log with whichever ID was available or attempted
                attempted_id = (
                    order_item.exchange_order_id or order_item.client_order_id or "[MISSING_ID]"
                )
                logger.error(f"[{self.exchange_name}] Error cancelling order {attempted_id}: {e}")
                continue
