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
from typing import Any

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
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
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
    CancelOrderResultStatus,
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import CancelOrderResult, Order
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
    trading_service: BackpackTradingService

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

        # Instantiate BackpackTradingService
        self.trading_service = BackpackTradingService(
            http_client_requester=self._request,  # Pass the _request method
            request_builder=self._bp_request_builder,  # Pass the instance
            response_handler=self._bp_response_handler,  # Pass the instance
            authenticator=self._bp_authenticator,  # Pass the authenticator instance
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
        Retrieves recent public trades for a specific symbol and maps them to internal Trade models.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of trades to retrieve. Defaults to 50.
                   The underlying service handles API limits.

        Returns:
            A list of internal Trade objects.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        logger.debug(f"[{self.exchange_name}] Getting recent trades for {symbol} with limit {limit}")
        raw_trades_data: list[BackpackRawTrade] = await self.market_data.get_recent_trades(symbol=symbol, limit=limit)

        internal_trades: list[Trade] = []
        for raw_trade in raw_trades_data:
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
        # Delegate to trading_service to get raw order
        raw_order: BackpackRawOrder = await self.trading_service.place_order_raw(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            stop_price=stop_price,
            client_order_id=client_order_id,
            post_only=post_only,
            # reduce_only is not directly used by Backpack's place_order payload
            # but is part of the generic ExchangeAPI interface.
            # The BackpackRequestBuilder.build_place_order_payload does not take reduce_only.
            # If Backpack supports it via a different field, builder should be updated.
        )

        # Transform raw order to internal Order object
        # The _bp_mapper is an instance of BackpackOrderMapper
        internal_order = self._bp_mapper.transform_raw_order_to_internal(raw_order)
        return internal_order

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order. Returns True if successful."""
        if symbol is None:
            # Attempt to lookup symbol from order_id if possible, or raise error
            # For now, raise error as Backpack API likely requires symbol
            raise ValueError("Symbol is required to cancel order on Backpack")

        # Delegate to trading_service. cancel_order_raw returns RawJsonResponse
        # We expect a dict from handle_cancel_order_response upon success.
        # The original method returned True on success, False/APIError on failure.
        # We need to inspect the raw_response or trust the handler to raise APIError on failure.
        try:
            _raw_response: RawJsonResponse = await self.trading_service.cancel_order_raw(
                order_id=order_id, symbol=symbol
            )
            # Assuming handle_cancel_order_response in the service would raise an APIError
            # if the cancellation was not successful (e.g., order not found, invalid params).
            # If it returns without error, cancellation is considered successful.
            logger.info(
                f"[{self.exchange_name}] Successfully canceled order {order_id} for "
                f"{symbol} via service."
            )
            return True
        except APIError as e:
            # APIError is already logged by the service or the _request method.
            # Re-raise it to be handled by the caller.
            raise e
        except Exception as e_unhandled:  # Should ideally be caught by service, but as a fallback
            logger.error(
                f"[{self.exchange_name}] Unexpected error in API cancel_order for OID {order_id}: "
                f"{e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error canceling order {order_id} via API: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders, mapping all required fields."""
        # Delegate to trading_service to get raw orders
        raw_orders: list[BackpackRawOrder] = await self.trading_service.get_open_orders_raw(
            symbol=symbol
        )

        # Transformation logic
        orders: list[Order] = []
        for raw_order in raw_orders:
            try:
                # Transform validated raw order to internal Order using instance mapper
                internal_order = self._bp_mapper.transform_raw_order_to_internal(raw=raw_order)
                # Original code did not check if internal_order is None,
                # assuming transform always works or raises.
                # Adding a check for robustness if mapper can return None.
                if internal_order:
                    orders.append(internal_order)
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order: transform returned None. "
                        f"Data: {raw_order.model_dump_json()}"
                    )
            except Exception as e:  # Catch transformation errors
                logger.warning(
                    f"[{self.exchange_name}] Skipping order due to transformation error: {e}. "
                    f"Data: {raw_order.model_dump_json()}"
                )
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
                f"[{self.exchange_name}] Unexpected error fetching account info via service: "
                f"{e_unhandled}",
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
            ) from e

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
                    f"[{self.exchange_name}] Unexpected transfer response type: "
                    f"{type(raw_response)}. Expected dict. Response: {raw_response!r}"
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
                f"[{self.exchange_name}] API error during withdrawal of {amount} {asset} "
                f"to {address}: {e}",
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
            # Service now returns list[BackpackRawTrade]
            # RENAMED raw_fills to raw_trades_list for clarity with new type
            raw_trades_list: list[BackpackRawTrade] = await self.account_service.get_trade_history_raw(
                symbol=symbol, limit=limit
            )

            internal_trades: list[Trade] = []
            # MODIFIED loop variable and mapper call
            for raw_trade_item in raw_trades_list:
                try:
                    # Mapper transforms individual BackpackRawTrade to Trade
                    # This uses transform_raw_trade_to_internal which expects BackpackRawTrade
                    trade = self._bp_mapper.transform_raw_trade_to_internal(raw_trade_item)
                    # transform_raw_trade_to_internal can return None if essential data missing
                    if trade:
                        internal_trades.append(trade)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping trade: transform_raw_trade_to_internal returned None. "
                            f"Raw data: {raw_trade_item.model_dump_json(exclude_none=True)}"
                        )
                except ValidationError as e_map_item:  # Catch Pydantic errors during item mapping
                    logger.error(
                        f"[{self.exchange_name}] Validation error mapping individual trade: "
                        f"{e_map_item}. Raw trade: {raw_trade_item.model_dump_json(exclude_none=True)!r}",
                        exc_info=True,
                    )
                    # Optionally skip this trade or raise a more general error
                    continue  # Skip this problematic trade
                except (
                    Exception
                ) as e_map_item_gen:  # Catch other general errors during item mapping
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error mapping individual trade: "
                        f"{e_map_item_gen}. Raw trade: {raw_trade_item.model_dump_json(exclude_none=True)!r}",
                        exc_info=True,
                    )
                    continue  # Skip this problematic trade
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
            symbol: The symbol for the order (required by Backpack).

        Returns:
            The Order object if found, None otherwise.
        """
        if not symbol:  # Symbol is required for get_order_status_raw
            _error_msg = "Symbol is required for get_order on Backpack."
            raise ValueError(_error_msg)

        # Delegate to trading_service.get_order_status_raw as it fetches a single order
        raw_order: BackpackRawOrder | None = await self.trading_service.get_order_status_raw(
            order_id=order_id,
            symbol=symbol,
        )
        if raw_order is None:
            return None

        try:
            # RET504: Direct return; TRY300: else block not strictly needed due to return
            return self._bp_mapper.transform_raw_order_to_internal(raw_order)
        except Exception as e:  # Catch transformation errors
            logger.exception(
                "[%s] Failed to transform raw order %s to internal model: %s. Raw data: %s",
                self.exchange_name,
                order_id,
                e,
                raw_order.model_dump_json(),
            )
            # Re-raise as APIError or allow specific exception to propagate if preferred
            # For now, returning None as per original logic for failed transformation
            return None

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
            "[%s] _update_rate_limit_from_headers called (no-op for Backpack). "
            "Headers: %s, Method: %s, Path: %s",
            self.exchange_name,
            headers,
            method,
            path,
        )

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders for a given symbol or all symbols."""
        # This is a facade over the trading_service.get_open_orders_raw method,
        # which already handles fetching for the specific user. Backpack's API
        # for listing open orders doesn't require a user identifier in the path/query
        # if the request is authenticated.
        logger.info(
            "[%s] Fetching all open orders for %s.",
            self.exchange_name,
            symbol if symbol else "all symbols",
        )
        try:
            raw_open_orders: list[
                BackpackRawOrder
            ] = await self.trading_service.get_open_orders_raw(symbol=symbol)
            internal_orders: list[Order] = []
            for raw_order in raw_open_orders:
                try:
                    order = self._bp_mapper.transform_raw_order_to_internal(raw_order)
                    if order:  # Mapper might return None if transformation is impossible
                        internal_orders.append(order)
                except Exception as e_map:
                    logger.warning(
                        "[%s] Failed to map raw open order: %s. Raw: %s. Skipping.",
                        self.exchange_name,
                        e_map,
                        raw_order.model_dump_json(exclude_none=True),
                    )
            return internal_orders
        except APIError as e:
            logger.error(
                "[%s] API Error fetching all open orders for %s: %s",
                self.exchange_name,
                symbol if symbol else "all symbols",
                e,
            )
            raise
        except Exception as e:
            logger.exception(
                "[%s] Unexpected error fetching all open orders for %s: %s",
                self.exchange_name,
                symbol if symbol else "all symbols",
                e,
            )
            raise APIError(
                message=f"Unexpected error getting all open orders: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription via WebSocketManager."""
        # Ensure ExchangeAPI.subscribe is not abstract or implement fully here
        logger.info(
            "[%s] Subscribe called for topic: %s. Delegating to base.",
            self.exchange_name,
            topic,  # COM812 fixed by adding comma here
        )
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Handle actions upon WebSocket connection, typically resubscribing to topics."""
        logger.info(
            "[%s] WebSocket connected. Triggering resubscription via base.",
            self.exchange_name,  # COM812 fixed by adding comma here
        )
        await super()._on_ws_connected()  # Assuming base class handles resubscription logic

    async def _resubscribe(self) -> None:
        """Resubscribe to topics upon WebSocket (re)connection."""
        logger.info(
            "[%s] Resubscribe called. Delegating to base.",
            self.exchange_name,
        )
        await super()._resubscribe()

    async def get_market_data(
        self,
        symbol: str,
        timeframe: str,  # e.g., "1m", "1h", "1d"
        limit: int | None = 100,  # Default limit, ensure service can handle None or default it
    ) -> list[Candle]:
        """Fetch historical klines/candles for a symbol and timeframe."""
        # Ensure limit passed to service is compatible; service uses default 100 if limit is None.
        service_limit = limit if limit is not None else 100  # Service expects int
        raw_klines: list[BackpackRawKline] = await self.market_data.get_market_data(
            symbol=symbol,
            timeframe=timeframe,
            limit=service_limit,
        )
        internal_candles: list[Candle] = []
        for raw_kline in raw_klines:
            try:
                candle = self._bp_mapper.transform_raw_kline_to_internal(
                    symbol,
                    timeframe,
                    raw_kline,
                )
                internal_candles.append(candle)
            except ValueError as e:
                logger.warning(
                    "[%s] Failed to transform raw kline for symbol %s, timeframe %s. "
                    "Error: %s. Raw: %s",
                    self.exchange_name,
                    symbol,
                    timeframe,
                    e,
                    raw_kline.model_dump_json(),
                )
        return internal_candles

    async def get_historical_trades(
        self,
        symbol: str,
        limit: int | None = 50,
        from_id: str | None = None,
    ) -> list[Trade]:
        """
        Retrieves historical public trades for a specific symbol.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC").
            limit: The maximum number of trades to retrieve. Defaults to 50.
                   Backpack API default 100, max 1000. This limit is applied post-fetch
                   if the service doesn't directly support it.
            from_id: Optional. If provided, get trades from this trade ID.
                     Backpack API supports a `from` parameter (trade ID).

        Returns:
            A list of internal Trade objects.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        logger.debug(
            f"[{self.exchange_name}] Getting historical trades for {symbol}, limit {limit}, from_id {from_id}"
        )

        raw_historical_trades_data: list[BackpackRawTrade]
        try:
            if from_id:
                logger.info(f"[{self.exchange_name}] from_id='{from_id}' requested for historical trades. Ensure service supports this.")
                # Current BackpackMarketDataService.get_recent_trades does not accept from_id.
                # This would require either updating that service method or adding a new one.
                # For now, from_id is logged but not passed if get_recent_trades is the only option.

            raw_historical_trades_data = await self.market_data.get_recent_trades(
                symbol=symbol, limit=limit # from_id would be passed here if service method supported it
            )

        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API error fetching historical trades for "
                f"{symbol}: {e.message}"
            )
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching historical trades for "
                f"{symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error fetching historical trades for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

        internal_trades: list[Trade] = []
        for raw_trade in raw_historical_trades_data:
            trade = self._bp_mapper.transform_raw_trade_to_internal(raw_trade)
            if trade:
                internal_trades.append(trade)
            else:
                logger.warning(
                    f"[{self.exchange_name}] Failed to transform raw historical trade "
                    f"to internal model for {symbol}. Raw: {raw_trade.model_dump_json()}"
                )
        return internal_trades

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:  # Ensure return type matches ExchangeAPI.get_order_status
        """Fetches the status of a single order by its orderId or clientId."""
        if not symbol:
            raise ValueError(
                "Symbol must be provided when fetching order status by ID/ClientID "
                "from Backpack, as it's a required query parameter."
            )

        # Delegate to trading_service
        # The service method get_order_status_raw handles identifier logic
        raw_order: BackpackRawOrder | None = await self.trading_service.get_order_status_raw(
            order_id=order_id, symbol=symbol, client_order_id=client_order_id
        )

        if raw_order is None:
            # Service method returns None if order not found (and logs it).
            return None

        # Transform raw order to internal Order object
        try:
            internal_order = self._bp_mapper.transform_raw_order_to_internal(raw_order)
            return internal_order
        except Exception as e:  # Catch transformation errors
            logger.error(
                f"[{self.exchange_name}] Failed to transform raw order status for "
                f"{order_id or client_order_id} to internal model: {e}. "
                f"Raw data: {raw_order.model_dump_json()}",
                exc_info=True,
            )
            # Consider raising APIError or returning None as per contract
            return None  # Consistent with returning None if mapping fails or order not found

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders for a given symbol or all symbols.

        Args:
            symbol: The trading symbol (e.g., "SOL_USDC"). If None, cancels orders for all symbols.

        Returns:
            A list of CancelOrderResult objects, one for each order affected or a summary result.
        """
        results: list[CancelOrderResult] = []
        target_symbol_log = symbol if symbol else "all symbols"
        logger.info(
            "[%s] Attempting to cancel all orders for %s.",
            self.exchange_name,
            target_symbol_log,
        )

        try:
            # Returns list[BackpackRawOrder] for succeeded, empty list for no_op,
            # or raises APIError for failures.
            cancelled_raw_orders: list[
                BackpackRawOrder
            ] = await self.trading_service.cancel_all_orders_raw(symbol=symbol)

            if not cancelled_raw_orders:
                # This case means the operation was successful but no orders were found/cancelled.
                logger.info(
                    "[%s] No open orders found or cancelled for %s via bulk operation.",
                    self.exchange_name,
                    target_symbol_log,
                )
                results.append(
                    CancelOrderResult(
                        symbol=symbol,  # Or None if global
                        order_id="ALL",  # Indicates a bulk operation for the symbol
                        success=True,  # Op successful (no orders to cancel is success state)
                        message=f"No open orders found to cancel for {target_symbol_log}.",
                        status=CancelOrderResultStatus.NOT_FOUND,
                    )
                )
            else:
                # Orders were successfully cancelled
                # PERF401: Use list comprehension and extend
                results.extend(
                    [
                        CancelOrderResult(
                            symbol=raw_order.symbol,
                            order_id=raw_order.id,
                            client_order_id=raw_order.clientId,
                            success=True,
                            message="Order cancelled successfully via bulk operation.",
                            status=CancelOrderResultStatus.SUCCESS,
                            raw_response=raw_order.model_dump(exclude_none=True),
                        )  # COM812 fixed
                        for raw_order in cancelled_raw_orders
                    ]
                )
                logger.info(
                    ("[%s] Successfully cancelled %d orders for %s."),  # G004 fixed, E501 addressed
                    self.exchange_name,
                    len(cancelled_raw_orders),
                    target_symbol_log,  # COM812 fixed
                )
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API error during bulk cancel for {target_symbol_log}: {e}"
            )
            results.append(
                CancelOrderResult(
                    symbol=symbol,  # Or None if global
                    order_id="ALL",
                    success=False,
                    message=f"Failed to cancel all orders for {target_symbol_log}: {e.message}",
                    status=CancelOrderResultStatus.FAILED,
                    raw_response=e.model.metadata.get("raw_response") if e.model.metadata else None,
                )
            )
        except Exception as e_unhandled:
            logger.exception(
                f"[{self.exchange_name}] Unexpected error in bulk cancel for {target_symbol_log}: "
                f"{e_unhandled}"
            )
            results.append(
                CancelOrderResult(
                    symbol=symbol,  # Or None if global
                    order_id="ALL",
                    success=False,
                    message=f"Unexpected error cancelling orders for {target_symbol_log}: "
                            f"{str(e_unhandled)}",
                    status=CancelOrderResultStatus.FAILED,
                )
            )

        return results

    async def request_historical_funding_rates(
        self,
        # TODO: Define parameters based on actual exchange capabilities if this endpoint exists
        # symbol: str,
        # start_time: datetime | None = None,
        # end_time: datetime | None = None,
        # limit: int | None = None,
    ) -> list[FundingRate]:
        """(Placeholder) Request historical funding rates."""
        logger.warning(
            f"[{self.exchange_name}] get_historical_funding_rates placeholder - not implemented."
        )
        # Example: Fetch current rates if historical not available
        # return await self.get_funding_rates(symbols=[symbol] if symbol else None)
        return []  # Return empty list as placeholder
