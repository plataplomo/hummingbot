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
    RawJsonResponse,
)
from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill, BackpackRawTrade
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
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
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import Order
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

    account_service: BackpackAccountService

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
        self._bp_response_handler = BackpackResponseHandler()
        self._bp_request_builder = BackpackRequestBuilder()

        super().__init__(
            exchange_name="backpack",
            config=api_config,
            secrets=secrets,
            authenticator=self._bp_authenticator,
            error_mapper=self._backpack_error_mapper,
        )

        self.market_data = BackpackMarketDataService(
            http_client=self._http_client,
            request_builder=self._bp_request_builder,
            response_handler=self._bp_response_handler,
            rate_limiter_service=self._rate_limiter_service,
            exchange_name=self.exchange_name,
        )
        self.account_service = BackpackAccountService(
            http_client_requester=self._request,
            request_builder=self._bp_request_builder,
            response_handler=self._bp_response_handler,
            authenticator=self._bp_authenticator,
            rate_limiter_service=self._rate_limiter_service,
            exchange_name=self.exchange_name,
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

        This method now first validates the raw payload using BackpackWsRawMessageHandler
        before passing the validated Pydantic model to the registered application handler.
        """
        topic_str: str | None = message.get("topic")
        data_payload: dict[str, Any] | None = message.get("data")
        event_type_str: str | None = None

        if not topic_str:
            raw_event_type = message.get("type")
            if isinstance(raw_event_type, str) and raw_event_type in [
                "fills",
                "orders",
                "positionUpdate",
            ]:
                event_type_str = raw_event_type
                topic_str = event_type_str
            else:
                logger.debug(
                    f"[{self.exchange_name}] Unroutable message - no clear string topic "
                    f"and not a known event type: {message}"
                )
                return

        if data_payload is None:
            logger.debug(
                f"[{self.exchange_name}] Received message with topic/type '{topic_str}' "
                f"but no data_payload: {message}"
            )
            return

        # topic_str is now guaranteed to be a string.
        app_handler = self._ws_handlers.get(
            str(topic_str)
        )  # Ensure topic_str is treated as str for key

        base_topic = str(topic_str)  # Use str() to satisfy linters if topic_str could be None path
        if base_topic.startswith("depth."):
            base_topic = "depth"
        elif base_topic.startswith("ticker."):
            base_topic = "ticker"

        if not app_handler:
            logger.debug(
                f"[{self.exchange_name}] No application handler registered for topic: "
                f"{topic_str} (or base topic: {base_topic})"
            )
            return

        try:
            validated_payload: Any = None
            if base_topic == "depth":
                validated_payload = BackpackWsRawMessageHandler.handle_depth_payload(data_payload)
            elif base_topic == "ticker":
                validated_payload = BackpackWsRawMessageHandler.handle_ticker_payload(data_payload)
            elif base_topic == "fills":
                validated_payload = BackpackWsRawMessageHandler.handle_trade_event_payload(
                    data_payload
                )
            elif base_topic == "orders":
                validated_payload = BackpackWsRawMessageHandler.handle_order_update_payload(
                    data_payload
                )
            elif base_topic == "positionUpdate":
                validated_payload = BackpackWsRawMessageHandler.handle_position_update_payload(
                    data_payload
                )
            else:
                logger.warning(
                    f"[{self.exchange_name}] No specific raw WS validator for topic "
                    f"'{topic_str}' (base: '{base_topic}'). "
                    f"Application handler will receive raw payload."
                )
                await app_handler(data_payload, message)
                return

            await app_handler(validated_payload, message)

        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] APIError validating WS payload for topic "
                f"{topic_str} (base: {base_topic}): {e.message}",
                exc_info=True,
            )
        except ValidationError as e_val:
            logger.error(
                f"[{self.exchange_name}] Unexpected ValidationError in _route_ws_message "
                f"for topic {topic_str} (base: {base_topic}): {e_val}",
                exc_info=True,
            )
        except Exception as e_app:
            logger.error(
                f"[{self.exchange_name}] Error in application handler for topic {topic_str} "
                f"(base: {base_topic}): {e_app}",
                exc_info=True,
            )

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

        auth_components: AuthenticatedRequestComponents = (
            await self._bp_authenticator.prepare_request(
                method=method, path=path, params=params, data=data, headers=current_headers
            )
        )

        # _authenticate should return a dict matching the structure expected by _request
        return {
            "headers": auth_components["headers"],
            "params": auth_components["params"],
            "data": auth_components["data"],
        }

    # --- Core API Implementation --- #

    async def get_ticker(self, symbol: str) -> Ticker:
        """Fetches the latest ticker information for a specific symbol."""
        raw_ticker = await self.market_data.get_ticker(symbol)
        # Assuming symbol in raw_ticker is correct; pass explicitly if needed
        return self._bp_mapper.transform_raw_ticker_to_internal(raw_ticker, symbol_override=symbol)

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
        # Depth parameter is unused by Backpack service, but kept for interface compatibility
        raw_order_book = await self.market_data.get_order_book(symbol, depth)
        return self._bp_mapper.transform_raw_orderbook_to_internal(symbol, raw_order_book)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """
        Get recent trades for a symbol, mapping all required fields for the Trade model.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade objects
        """
        raw_trades = await self.market_data.get_recent_trades(symbol, limit)
        internal_trades: list[Trade] = []
        for raw_trade in raw_trades:
            # transform_raw_trade_to_internal might return None if critical info is missing
            # from raw REST API trade data.
            trade = self._bp_mapper.transform_raw_trade_to_internal(raw_trade)
            if trade:
                internal_trades.append(trade)
            else:
                logger.warning(
                    f"[{self.exchange_name}] Failed to transform raw trade to internal model "
                    f"for symbol {symbol}. Raw trade: {raw_trade.model_dump_json()}"
                )
        return internal_trades

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances, validated via Raw models and transformed via Mapper."""
        # Step 1-3 (Build, Request, Raw Validate) delegated to service
        raw_balances_dict: dict[
            str, BackpackRawBalance
        ] = await self.account_service.get_balances_raw()

        # Step 4 (Transform - still in API client for now)
        processed_balances: dict[str, SpotBalance] = {}
        for asset_symbol, raw_balance in raw_balances_dict.items():
            try:
                # Transform validated raw balance to internal SpotBalance using instance mapper
                internal_balance = self._bp_mapper.transform_raw_balance_to_internal(
                    asset_symbol=asset_symbol, raw=raw_balance
                )
                processed_balances[internal_balance.asset] = internal_balance

            except ValidationError as e:
                logger.error(
                    f"[{self.exchange_name}] Failed Pydantic validation for balance "
                    f"{asset_symbol}: {e}. Data: {raw_balance.model_dump_json()}"
                )
                continue  # Skip this asset if validation fails
            except ValueError as e:  # Catches errors from transform_raw_balance_to_internal
                logger.error(
                    f"[{self.exchange_name}] Failed transformation for balance "
                    f"{asset_symbol}: {e}. Raw Data: {raw_balance.model_dump_json()}"
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
        # Step 1-3 (Build, Request, Raw Validate) delegated to service
        raw_positions_list: list[
            BackpackRawPosition
        ] = await self.account_service.get_positions_raw(symbol=symbol)

        # Step 4 (Transform - still in API client for now)
        positions: list[DerivativePosition] = []
        for raw_position in raw_positions_list:
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
        """Get open orders, mapping all required fields."""
        endpoint = "/api/v1/orders"
        params = BackpackRequestBuilder.build_get_open_orders_params(symbol=symbol)
        response_data_raw: RawJsonResponse = await self._request(
            method="GET", endpoint=endpoint, params=params, is_signed=True
        )

        # Validate using the handler
        raw_orders: list[BackpackRawOrder] = (
            BackpackResponseHandler.handle_get_open_orders_response(response_data_raw, symbol)
        )

        # Transformation logic remains here
        orders: list[Order] = []
        for raw_order in raw_orders:
            try:
                # Transform validated raw order to internal Order using instance mapper
                internal_order = self._bp_mapper.transform_raw_order_to_internal(raw=raw_order)
                if internal_order:
                    orders.append(internal_order)
            except Exception as e:
                logger.warning(
                    f"[{self.exchange_name}] Skipping order due to transformation error: {e}. "
                    f"Data: {raw_order.model_dump_json()}"
                )
                continue
        return orders

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Fetches the current funding rate for a given perpetual market."""
        raw_funding_rate = await self.market_data.get_funding_rate(symbol)
        return self._bp_mapper.transform_raw_funding_rate_to_internal(raw_funding_rate)

    # --- Placeholder for required abstract method --- #
    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """(Not Implemented) Get funding rates for one/all symbols."""
        logger.warning(
            f"[{self.exchange_name}] get_funding_rates not fully implemented. "
            f"Fetching current rate only."
        )
        rates: list[FundingRate] = []
        if symbols:
            for symbol_item in symbols:
                try:
                    # get_funding_rate now returns internal FundingRate
                    current_rate: FundingRate = await self.get_funding_rate(symbol_item)
                    rates.append(current_rate)
                except APIError as e:
                    logger.error(
                        f"[{self.exchange_name}] Failed to fetch current funding rate for "
                        f"{symbol_item} within get_funding_rates: {e}"
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
        Delegates to BackpackAccountService.
        """
        logger.info(f"[{self.exchange_name}] Fetching raw account info via service.")
        try:
            # Delegate to service, which returns the Raw Model or None
            return await self.account_service.get_account_info_raw()
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error fetching account info via service: {e}")
            # The service method get_account_info_raw already handles 404 to return None.
            # Re-raise other APIErrors that might come from the service.
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching account info via service: {e_unhandled}",
                exc_info=True,
            )
            # Ensure APIError is raised for unhandled exceptions
            raise APIError(
                message=f"Unexpected error getting account info via service: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Fetches the raw account summary components and maps them to an internal model."""
        logger.info(f"[{self.exchange_name}] Fetching account summary.")
        try:
            (
                raw_account_settings,
                raw_balances,
                raw_positions,
            ) = await self.account_service.get_account_summary_components_raw()

            if raw_account_settings is None:
                logger.warning(
                    f"[{self.exchange_name}] Failed to retrieve core account settings for summary."
                )
                return None

            return self._bp_mapper.transform_raw_account_summary_to_internal(
                raw_settings=raw_account_settings,
                spot_balances_raw=raw_balances,
                derivative_positions_raw=raw_positions,
            )
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API error fetching account summary components: {e}",
                exc_info=True,
            )
            raise  # Re-raise the APIError
        except ValidationError as e_map_val:
            logger.error(
                f"[{self.exchange_name}] Validation error mapping account summary: {e_map_val}",
                exc_info=True,
            )
            return None
        except Exception as e:
            # Catch any other unexpected error during the summary process
            logger.exception("Unexpected error mapping account summary: %s", str(e))
            # This was the missing part: ensure an APIError is raised for mapping issues.
            raise APIError(
                message=f"Failed to map raw account summary data: {str(e)}",
                code=APIErrorCode.INVALID_RESPONSE.value,  # Use .value for the enum
                original_exception=e,
            )

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,  # e.g., "spot", "futures" - specific values TBD by exchange
        to_account_type: str,  # e.g., "spot", "futures" - specific values TBD by exchange
        client_transfer_id: str | None = None,
    ) -> dict[str, Any]:  # Return type TBD by actual API response structure
        """Initiates an asset transfer between accounts.

        Delegates to BackpackAccountService for the raw API call and returns the raw response.
        Note: Transformation to an internal model is not currently implemented for transfers.
        """
        logger.info(
            f"[{self.exchange_name}] Initiating transfer of {amount} {asset} from "
            f"{from_account_type} to {to_account_type}."
        )
        try:
            # Backpack's transfer endpoint might not be fully defined/stable yet.
            # The service method is prepared to call it.
            # Current service method returns RawJsonResponse, which is often a dict.
            raw_response = await self.account_service.transfer_raw(
                asset=asset,
                amount=amount,
                from_account_type=from_account_type,
                to_account_type=to_account_type,
                client_transfer_id=client_transfer_id,
            )
            # Assuming the raw response for a transfer is a dictionary.
            # If it can be other types from RawJsonResponse, this might need adjustment
            # or a specific internal model + mapper.
            if not isinstance(raw_response, dict):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected transfer response type: {type(raw_response)}. "
                    f"Expected dict. Response: {raw_response!r}"
                )
                # Fallback or raise, depending on how strictly we expect a dict.
                # For now, let's assume it should be a dict if successful.
                raise APIError(
                    message=f"Unexpected transfer response format: {type(raw_response)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )
            return raw_response  # Return the raw JSON dictionary
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API error during transfer of {amount} {asset}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error during transfer: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error during transfer: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

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
        """Initiates a withdrawal of assets from the exchange.

        Delegates to BackpackAccountService for the raw API call, then maps the raw
        withdrawal response to a dictionary using BackpackOrderMapper.
        """
        logger.info(
            f"[{self.exchange_name}] Initiating withdrawal of {amount} {asset} to {address}."
        )
        try:
            raw_withdrawal_response = await self.account_service.withdraw_raw(
                asset=asset,
                amount=amount,
                address=address,
                network=network,
                tag=tag,
                client_withdrawal_id=client_withdrawal_id,
                two_factor_token=two_factor_token,
                **kwargs,
            )

            # Map the raw response to an internal representation (currently dict)
            return self._bp_mapper.transform_raw_withdrawal_response_to_internal(
                raw_withdrawal_response
            )
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API error during withdrawal of {amount} {asset} to {address}: {e}",
                exc_info=True,
            )
            raise
        except ValidationError as e_val:  # Catch Pydantic errors from mapper
            logger.error(
                f"[{self.exchange_name}] Validation error mapping withdrawal response: {e_val}. "
                f"Raw: {getattr(e_val, 'input_data', 'N/A')!r}",  # Attempt to log raw data
                exc_info=True,
            )
            raise APIError(
                message=f"Withdrawal response mapping validation failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error during withdrawal: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error during withdrawal: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

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
        # Step 1-3 (Build, Request, Raw Validate) delegated to service
        raw_orders_list: list[BackpackRawOrder] = await self.account_service.get_order_history_raw(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            order_id=order_id,
            client_order_id=client_order_id,
        )

        # Step 4 (Transform - still in API client for now)
        orders: list[Order] = []
        for raw_order in raw_orders_list:
            try:
                internal_order = self._bp_mapper.transform_raw_order_to_internal(raw_order)
                orders.append(internal_order)
            except ValueError as e_transform:  # Catch transformation errors per item
                logger.warning(
                    f"[{self.exchange_name}] Skipping order in history due to "
                    f"transformation error: {e_transform}. Data: {raw_order.model_dump_json()}"
                )
                continue
            except Exception as e:  # Catch any other unexpected error during mapping
                logger.error(
                    f"[{self.exchange_name}] Unexpected error mapping order history item: {e}. "
                    f"Raw: {raw_order.model_dump_json()}",
                    exc_info=True,
                )
                continue
        return orders

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetches recent trade history for a symbol or all symbols.

        Delegates to BackpackAccountService to get raw trade fills, then maps them to
        internal Trade models using BackpackOrderMapper.
        """
        logger.info(
            f"[{self.exchange_name}] Fetching trade history for symbol: "
            f"{symbol if symbol else 'ALL'} with limit: {limit}."
        )
        try:
            # Service returns list[BackpackRawFill]
            raw_fills: list[BackpackRawFill] = await self.account_service.get_trade_history_raw(
                symbol=symbol, limit=limit
            )

            internal_trades: list[Trade] = []
            for raw_fill in raw_fills:
                try:
                    # Mapper transforms individual BackpackRawFill to Trade
                    trade = self._bp_mapper.transform_raw_fill_to_internal(raw_fill)
                    internal_trades.append(trade)
                except ValidationError as e_map_item:  # Catch Pydantic errors during item mapping
                    logger.error(
                        f"[{self.exchange_name}] Validation error mapping individual trade fill: "
                        f"{e_map_item}. Raw fill: {raw_fill!r}",
                        exc_info=True,
                    )
                    # Optionally skip this trade or raise a more general error
                    continue  # Skip this problematic fill
                except (
                    Exception
                ) as e_map_item_gen:  # Catch other general errors during item mapping
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error mapping individual trade fill: "
                        f"{e_map_item_gen}. Raw fill: {raw_fill!r}",
                        exc_info=True,
                    )
                    continue  # Skip this problematic fill
            return internal_trades
        except APIError as e_api:  # Catch critical fetch errors from service
            logger.error(
                f"[{self.exchange_name}] API error fetching trade history: {e_api}", exc_info=True
            )
            raise
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_trade_history: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing trade history: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

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
        raw_klines = await self.market_data.get_market_data(symbol, timeframe, limit)
        internal_candles: list[Candle] = []
        for raw_kline in raw_klines:
            try:
                candle = self._bp_mapper.transform_raw_kline_to_internal(
                    symbol, timeframe, raw_kline
                )
                internal_candles.append(candle)
            except ValueError as e:
                logger.warning(
                    f"[{self.exchange_name}] Failed to transform raw kline for symbol {symbol}, "
                    f"timeframe {timeframe}. Error: {e}. Raw kline: {raw_kline.model_dump_json()}"
                )
        return internal_candles

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
