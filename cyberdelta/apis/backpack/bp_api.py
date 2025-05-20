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

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
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
    Order,
    OrderSide,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.models.operations import Transfer, Withdrawal
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
    market_data_service: BackpackMarketDataService

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
        self._bp_response_handler = BackpackResponseHandler()
        self._bp_request_builder = BackpackRequestBuilder(api_config)
        self._bp_order_mapper = BackpackOrderMapper()

        super().__init__(
            exchange_name="backpack",
            config=api_config,
            secrets=secrets,
            authenticator=self._bp_authenticator,
            error_mapper=self._backpack_error_mapper,
        )

        # Use self._request directly, services will handle the tuple response
        service_requester = self._request

        self.market_data_service = BackpackMarketDataService(
            http_client_requester=service_requester,
            request_builder=self._bp_request_builder,
            response_handler=self._bp_response_handler,
            exchange_name=self.exchange_name,
        )
        self.account_service = BackpackAccountService(
            http_client_requester=service_requester,
            request_builder=self._bp_request_builder,
            response_handler=self._bp_response_handler,
            authenticator=self._bp_authenticator,
            exchange_name=self.exchange_name,
        )

        self.trading_service = BackpackTradingService(
            http_client_requester=service_requester,
            request_builder=self._bp_request_builder,
            response_handler=self._bp_response_handler,
            authenticator=self._bp_authenticator,
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
        """Retrieves the latest ticker information for a specific symbol."""
        return await self.market_data_service.get_ticker(symbol=symbol)

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Retrieves the order book for a specific symbol."""
        return await self.market_data_service.get_order_book(symbol=symbol, limit=depth)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Retrieves recent public trades for a specific symbol."""
        return await self.market_data_service.get_recent_trades(symbol=symbol, limit=limit)

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Retrieves the current funding rate for a specific symbol."""
        return await self.market_data_service.get_funding_rate(symbol=symbol)

    async def get_market_data(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[Candle]:
        """Retrieves historical kline/candlestick data for a symbol and timeframe.

        Args:
            symbol: The trading symbol (e.g., 'SOL_USDC').
            timeframe: The kline interval (e.g., '1m', '1h', '1d').
            limit: The maximum number of klines to retrieve (default: 100).
            start_time_ms: Optional start time in milliseconds (Unix epoch).
            end_time_ms: Optional end time in milliseconds (Unix epoch).

        Returns:
            A list of Candle objects.
        """
        # Convert milliseconds to seconds for the service layer if provided
        start_time_sec = int(start_time_ms / 1000) if start_time_ms is not None else None
        end_time_sec = int(end_time_ms / 1000) if end_time_ms is not None else None

        return await self.market_data_service.get_market_data(
            symbol=symbol,
            interval=timeframe,  # Service uses 'interval'
            start_time=start_time_sec,
            end_time=end_time_sec,
            limit=limit,
        )

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances. Delegates to BackpackAccountService."""
        # The service method get_balances returns list[SpotBalance]
        # This needs to be changed to dict[str, SpotBalance] to match ExchangeAPI
        # The actual change will be in the account_service and its mapper usage.
        # For now, the direct delegation might cause a type error until service is fixed.
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Fetches current open positions, optionally filtered by symbol.
        Delegates to BackpackAccountService.
        """
        return await self.account_service.get_positions(symbol=symbol)

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
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """
        Place an order on Backpack Exchange. Delegates to BackpackTradingService.
        """
        return await self.trading_service.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            stop_price=stop_price,  # Pass stop_price to service
            client_order_id=client_order_id,
            post_only=post_only,
        )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order. Delegates to BackpackTradingService."""
        if not symbol:
            raise ValueError("Symbol is required to cancel an order on Backpack.")
        return await self.trading_service.cancel_order(order_id=order_id, symbol=symbol)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders. Delegates to BackpackTradingService."""
        return await self.trading_service.get_open_orders(symbol=symbol)

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
    async def get_account_summary(self) -> MarginAccountSummary:
        """Fetches overall account information (settings, balances, positions).
        Delegates to BackpackAccountService.
        """
        # The service's get_account_info method returns MarginAccountSummary
        return await self.account_service.get_account_info()

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,
        to_account_type: str,
        client_transfer_id: str | None = None,
    ) -> Transfer:
        """Initiates an asset transfer between accounts.
        Delegates to BackpackAccountService.
        """
        return await self.account_service.transfer(
            asset=asset,
            amount=amount,
            from_account_type=from_account_type,
            to_account_type=to_account_type,
            client_transfer_id=client_transfer_id,
        )

    async def withdraw(
        self,
        asset: str,
        amount: Decimal,
        address: str,
        network: str | None = None,
        tag: str | None = None,
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
        **kwargs: dict[str, Any],
    ) -> Withdrawal:
        """Initiates a withdrawal of assets from the exchange.
        Delegates to BackpackAccountService.
        """
        return await self.account_service.withdraw(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
            tag=tag,
            client_withdrawal_id=client_withdrawal_id,
            two_factor_token=two_factor_token,
            # **kwargs are not explicitly passed if service doesn't accept them.
            # Check service signature. For now, not passing **kwargs.
        )

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
        Delegates to BackpackAccountService.
        """
        return await self.account_service.get_order_history(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            order_id=order_id,
            client_order_id=client_order_id,
        )

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetches recent trade history for a symbol or all symbols.
        Delegates to BackpackAccountService.
        """
        return await self.account_service.get_trade_history(symbol=symbol, limit=limit)

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """Fetch a single order by its ID. Delegates to BackpackTradingService."""
        if not symbol:
            _error_msg = "Symbol is required for get_order on Backpack."
            logger.error(_error_msg)
            raise ValueError(_error_msg)
        return await self.trading_service.get_order(
            order_id=order_id, symbol=symbol, client_order_id=None
        )

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:
        """Fetch the status of a specific order. Delegates to BackpackTradingService's get_order."""
        if not symbol:
            # Symbol is required by the service's get_order method.
            _error_msg = "Symbol is required for get_order_status on Backpack."
            logger.error(_error_msg)
            raise ValueError(_error_msg)

        # Now symbol is guaranteed to be a str
        order = await self.trading_service.get_order(
            order_id=order_id, symbol=symbol, client_order_id=client_order_id
        )
        if order is None:
            raise APIError(
                f"Order {order_id} not found for symbol {symbol}.",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )
        return order

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
        """Fetch all open orders for a given symbol or all symbols.
        Delegates to BackpackTradingService's get_open_orders method.
        """
        return await self.trading_service.get_open_orders(symbol=symbol)

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

    async def get_historical_funding_rates(
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

    async def close(self) -> None:
        """Close the WebSocket connection and clean up resources."""
        await super().close()

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancels all open orders, optionally filtered by symbol.
        Delegates to BackpackTradingService.
        """
        return await self.trading_service.cancel_all_orders(symbol=symbol)
