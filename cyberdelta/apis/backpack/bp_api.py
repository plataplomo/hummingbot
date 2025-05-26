"""
CyberDeltaEngine: Backpack Exchange Integration
----------------------------------------------

This module implements the Backpack exchange adapter for CyberDeltaEngine, including:
- REST and WebSocket API client (`BackpackAPI`)
- Centralized error mapping and normalization (`BackpackErrorMapper`)

- Domain-specific data transformation mappers (`BackpackAccountDataMapper`,
  `BackpackMarketDataMapper`, `BackpackTradingDataMapper`)

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

from cyberdelta.apis.backpack.bp_api_components_factory import BackpackAPIComponentsFactory
from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.bp_ws_message_router import BackpackWsMessageRouter
from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
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

    def __init__(
        self,
        api_config: dict[str, Any],
        secrets: dict[str, str | None],
        # Optional dependency injection parameters for testing
        authenticator: BackpackHmacAuthenticator | None = None,
        error_mapper: BackpackErrorMapper | None = None,
        response_handler: BackpackResponseHandler | None = None,
        request_builder: BackpackRequestBuilder | None = None,
        # New domain-specific mappers
        account_data_mapper: BackpackAccountDataMapper | None = None,
        market_data_mapper: BackpackMarketDataMapper | None = None,
        trading_data_mapper: BackpackTradingDataMapper | None = None,
        # Services
        account_service: BackpackAccountService | None = None,
        trading_service: BackpackTradingService | None = None,
        market_data_service: BackpackMarketDataService | None = None,
    ) -> None:
        """
        Initialize the BackpackAPI client with configuration and secrets.

        Args:
            api_config: Dictionary of API configuration parameters.
            secrets: Dictionary of secret values (API key/secret).
            authenticator: Optional authenticator instance for dependency injection
            error_mapper: Optional error mapper instance for dependency injection
            response_handler: Optional response handler instance for dependency injection
            request_builder: Optional request builder instance for dependency injection

            account_data_mapper: Optional account data mapper instance for dependency injection
            market_data_mapper: Optional market data mapper instance for dependency injection
            trading_data_mapper: Optional trading data mapper instance for dependency injection
            account_service: Optional account service instance for dependency injection
            trading_service: Optional trading service instance for dependency injection
            market_data_service: Optional market data service instance for dependency injection
        """
        # Create the factory to handle component instantiation
        factory = BackpackAPIComponentsFactory(api_config, secrets)

        # Use injected components or create them via factory
        self._bp_authenticator = authenticator or factory.create_authenticator()
        self._backpack_error_mapper = error_mapper or factory.create_error_mapper()
        self._bp_request_builder = request_builder or factory.create_request_builder()
        self._bp_response_handler = response_handler or factory.create_response_handler()

        # Create instances of the new domain-specific mappers
        self._bp_account_data_mapper = account_data_mapper or factory.create_account_data_mapper()
        self._bp_market_data_mapper = market_data_mapper or factory.create_market_data_mapper()
        self._bp_trading_data_mapper = trading_data_mapper or factory.create_trading_data_mapper()

        super().__init__(
            exchange_name="backpack",
            config=api_config,
            secrets=secrets,
            authenticator=self._bp_authenticator,
            error_mapper=self._backpack_error_mapper,
        )

        # Initialize WebSocket message router
        self._bp_ws_router = BackpackWsMessageRouter(
            market_data_mapper=self._bp_market_data_mapper,
            account_data_mapper=self._bp_account_data_mapper,
            trading_data_mapper=self._bp_trading_data_mapper,
            raw_ws_handler=BackpackWsRawMessageHandler(),
            exchange_name=self.exchange_name,
        )

        # Use self._request directly, services will handle the tuple response
        service_requester = self._request

        # Use injected services or create them via factory
        if market_data_service is not None:
            self.market_data_service = market_data_service
        else:
            self.market_data_service = factory.create_market_data_service(
                http_client_requester=service_requester,
                rate_limiter_service=self._rate_limiter_service,
                market_data_mapper=self._bp_market_data_mapper,
                request_builder=self._bp_request_builder,
                response_handler=self._bp_response_handler,
                exchange_name=self.exchange_name,
            )

        if account_service is not None:
            self.account_service = account_service
        else:
            self.account_service = factory.create_account_service(
                http_client_requester=service_requester,
                rate_limiter_service=self._rate_limiter_service,
                authenticator=self._bp_authenticator,
                account_data_mapper=self._bp_account_data_mapper,
                request_builder=self._bp_request_builder,
                response_handler=self._bp_response_handler,
                exchange_name=self.exchange_name,
            )

        if trading_service is not None:
            self.trading_service = trading_service
        else:
            self.trading_service = factory.create_trading_service(
                http_client_requester=service_requester,
                rate_limiter_service=self._rate_limiter_service,
                authenticator=self._bp_authenticator,
                trading_data_mapper=self._bp_trading_data_mapper,
                request_builder=self._bp_request_builder,
                response_handler=self._bp_response_handler,
                exchange_name=self.exchange_name,
            )

        self.default_headers: dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

    # --- WebSocket Implementation --- #

    def _construct_subscription_payload(self, topic: str) -> dict[str, Any] | None:
        """Delegate subscription payload construction to the WebSocket router."""
        return self._bp_ws_router.construct_subscription_payload(topic)

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message from WebSocketManager, then route it.
        This is called by the WebSocketManager.
        """
        # For Backpack, current logic seems to be direct routing.
        # Add any pre-processing if needed before routing.
        # For example, parsing a common outer envelope if Backpack had one.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Delegate WebSocket message routing to the WebSocket router."""
        await self._bp_ws_router.route_message(message, self._ws_handlers)

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
        return await self.market_data_service.get_market_data(
            symbol=symbol,
            timeframe=timeframe,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=limit,
        )

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances. Delegates to BackpackAccountService."""
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
        """Place an order on Backpack Exchange. Delegates to BackpackTradingService."""
        if reduce_only:
            logger.warning(
                f"[{self.exchange_name}] 'reduce_only' parameter is not supported for place_order "
                f"and will be ignored."
            )
        return await self.trading_service.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            stop_price=stop_price,
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
        """Retrieves current funding rates for specified symbols.
        Delegates to BackpackMarketDataService.
        """
        return await self.market_data_service.get_funding_rates(symbols=symbols)

    async def get_account_summary(self) -> MarginAccountSummary:
        """Get account summary. Delegates to BackpackAccountService."""
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
        )

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Subscribe to order book updates for a symbol."""
        topic = f"depth.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Subscribe to ticker updates for a symbol."""
        topic = f"ticker.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Subscribe to public trade updates for a symbol."""
        topic = f"trades.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")

    async def subscribe_to_account_updates(self) -> None:
        """Subscribe to private account updates (balances, positions, orders)."""
        fill_topic = "fills"
        order_topic = "orders"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for account topics: "
            f"{fill_topic}, {order_topic}"
        )

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

    async def get_order(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """Fetch a single order by its ID. Delegates to BackpackTradingService."""
        if not symbol:
            raise ValueError("Symbol is required for get_order on Backpack.")
        return await self.trading_service.get_order(
            order_id=order_id, symbol=symbol, client_order_id=client_order_id
        )

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:
        """Fetch the status of a specific order. Delegates to BackpackTradingService's
        get_order_status."""
        if not symbol:
            raise ValueError("Symbol is required for get_order_status on Backpack.")
        return await self.trading_service.get_order_status(
            order_id=order_id, symbol=symbol, client_order_id=client_order_id
        )

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
        Delegates to BackpackTradingService's get_all_open_orders method.
        """
        return await self.trading_service.get_all_open_orders(symbol=symbol)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription via WebSocketManager."""
        logger.info(
            "[%s] Subscribe called for topic: %s. Delegating to base.",
            self.exchange_name,
            topic,
        )
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Handle actions upon WebSocket connection, typically resubscribing to topics."""
        logger.info(
            "[%s] WebSocket connected. Triggering resubscription via base.",
            self.exchange_name,
        )
        await super()._on_ws_connected()

    async def _resubscribe(self) -> None:
        """Resubscribe to topics upon WebSocket (re)connection."""
        logger.info(
            "[%s] Resubscribe called. Delegating to base.",
            self.exchange_name,
        )
        await super()._resubscribe()

    async def get_historical_funding_rates(
        self,
        symbol: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
    ) -> list[FundingRate]:
        """Request historical funding rates for a specific symbol and time range.
        Delegates to BackpackMarketDataService.

        Args:
            symbol: The trading symbol (e.g., 'SOL-PERP').
            start_time: Optional start time for the data range (UTC-aware).
            end_time: Optional end time for the data range (UTC-aware).
            limit: Optional limit on the number of funding rates to return.

        Returns:
            A list of FundingRate objects.
        """
        start_time_sec: int | None = None
        if start_time:
            if start_time.tzinfo is None:
                logger.warning(
                    f"[{self.exchange_name}] start_time for get_historical_funding_rates is naive. "
                    f"Assuming UTC."
                )
            start_time_sec = int(start_time.timestamp())

        end_time_sec: int | None = None
        if end_time:
            if end_time.tzinfo is None:
                logger.warning(
                    f"[{self.exchange_name}] end_time for get_historical_funding_rates is naive. "
                    f"Assuming UTC."
                )
            end_time_sec = int(end_time.timestamp())
            if start_time_sec is not None and end_time_sec < start_time_sec:
                raise ValueError("end_time cannot be before start_time.")

        return await self.market_data_service.get_historical_funding_rates(
            symbol=symbol,
            start_time_ms=start_time_sec,
            end_time_ms=end_time_sec,
            limit=limit,
        )

    async def close(self) -> None:
        """Closes the API client connections."""
        await super().close()

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancels all open orders, optionally filtered by symbol.
        Delegates to BackpackTradingService.
        """
        return await self.trading_service.cancel_all_orders(symbol=symbol)
