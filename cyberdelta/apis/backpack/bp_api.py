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
from typing import Any

from cyberdelta.apis.backpack.bp_api_components_factory import BackpackAPIComponentsFactory
from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.bp_ws_message_router import BackpackWsMessageRouter
from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.models import BackpackRawWsSubscriptionRequest
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.base.exchange_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketDataArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
    TransferArgs,
    WithdrawArgs,
)
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.config.secrets_models import AnyExchangeSecrets as ExchangeSecretsConfig
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    Order,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.models.operations import Transfer, Withdrawal

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
        exchange_config: ExchangeSpecificConfig,
        exchange_secrets: ExchangeSecretsConfig,
        # Optional dependency injection parameters for testing
        authenticator: BackpackEd25519Authenticator | None = None,
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
            exchange_config: Exchange-specific configuration model.
            exchange_secrets: Exchange secrets configuration model.
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
        factory = BackpackAPIComponentsFactory(exchange_config, exchange_secrets)

        # Use injected components or create them via factory
        self._bp_authenticator = authenticator or factory.create_authenticator()
        self._backpack_error_mapper = error_mapper or factory.create_error_mapper()
        self._bp_request_builder = request_builder or factory.create_request_builder()
        self._bp_response_handler = response_handler or factory.create_response_handler()

        # Create instances of the new domain-specific mappers
        self._bp_account_data_mapper = account_data_mapper or factory.create_account_data_mapper()
        self._bp_market_data_mapper = market_data_mapper or factory.create_market_data_mapper()
        self._bp_trading_data_mapper = trading_data_mapper or factory.create_trading_data_mapper()

        # Construct config dict for super().__init__
        rest_endpoint_str = str(exchange_config.api_base_url)
        ws_endpoint_str = str(exchange_config.ws_url) if exchange_config.ws_url else None

        # Create Backpack's simple rate limit strategy
        if exchange_config.rate_limit_per_minute is None:
            raise ValueError("rate_limit_per_minute is required for Backpack")

        rate_per_second = exchange_config.rate_limit_per_minute / 60.0
        bucket_size = max(1, int(rate_per_second * 2))
        bp_limiter_primitive = TokenBucketRateLimiterRuntime(
            rate=rate_per_second, bucket_size=bucket_size
        )
        bp_strategy = SimpleTokenBucketStrategy(
            limiter=bp_limiter_primitive, default_request_weight=1
        )

        config_dict_for_super = {
            "exchange_name": exchange_config.exchange_name.value,
            "rest_endpoint": rest_endpoint_str,
            "ws_url": ws_endpoint_str,
            # Include optional HTTP/WS settings with correct field names
            "default_request_timeout": exchange_config.request_timeout_seconds,
            "max_retries": exchange_config.max_retries,
            "retry_delay_seconds": exchange_config.retry_delay_seconds,
            "ping_interval": exchange_config.ws_ping_interval_seconds,
            "reconnect_delay": exchange_config.ws_reconnect_delay_seconds,
            "max_reconnect_attempts": exchange_config.ws_max_reconnect_attempts,
            "connection_timeout": exchange_config.ws_connection_timeout_seconds,
        }

        # Remove None values from config_dict_for_super before passing to super()
        config_dict_for_super_cleaned = {
            k: v for k, v in config_dict_for_super.items() if v is not None
        }

        # Construct secrets dict for super().__init__
        # Check if we have the correct auth type for Backpack
        from cyberdelta.config.secrets_models import ApiKeyAuthSecrets

        secrets_dict_for_super: dict[str, str | None]
        if isinstance(exchange_secrets, ApiKeyAuthSecrets):
            secrets_dict_for_super = {
                "BACKPACK_API_KEY": exchange_secrets.api_key.get_secret_value()
                if exchange_secrets.api_key
                else None,
                "BACKPACK_API_SECRET": exchange_secrets.api_secret.get_secret_value()
                if exchange_secrets.api_secret
                else None,
            }
        else:
            # This should not happen if secrets validation is working correctly
            logger.error(
                f"Backpack API received wrong auth type: {exchange_secrets.auth_type}. "
                f"Expected 'api_key'. Authentication will fail."
            )
            secrets_dict_for_super = {
                "BACKPACK_API_KEY": None,
                "BACKPACK_API_SECRET": None,
            }

        super().__init__(
            exchange_name=exchange_config.exchange_name.value,
            config=config_dict_for_super_cleaned,
            secrets=secrets_dict_for_super,
            authenticator=self._bp_authenticator,
            error_mapper=self._backpack_error_mapper,
            rate_limit_strategy=bp_strategy,
            exchange_config=exchange_config,
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

    def _construct_subscription_payload(self, topic: str) -> BackpackRawWsSubscriptionRequest:
        """Construct subscription payload for the given topic.

        Args:
            topic: The WebSocket topic to subscribe to

        Returns:
            BackpackRawWsSubscriptionRequest model

        Raises:
            ValueError: If topic format is invalid or required info is missing
            APIError: If topic is not supported by the exchange
        """
        # Determine if this is a private topic that requires authentication
        # According to Backpack API docs: "Private streams are prefixed with `account.`"
        signature_components = None

        # Check if topic requires authentication (private streams start with "account.")
        if topic.startswith("account."):
            if not self._bp_authenticator or not hasattr(
                self._bp_authenticator, "get_ws_subscription_signature_components"
            ):
                raise APIError(
                    "ED25519 authenticator required for private WebSocket subscriptions",
                    code=APIErrorCode.AUTHENTICATION_FAILED.value,
                )

            # Extract subscription type and symbol from topic
            if "." in topic:
                subscription_type, symbol = topic.split(".", 1)
            else:
                subscription_type = topic
                symbol = None

            signature_components = self._bp_authenticator.get_ws_subscription_signature_components(
                subscription_type=subscription_type, symbol=symbol
            )

        # Delegate to the WebSocket router with signature components
        return self._bp_ws_router.construct_subscription_payload(topic, signature_components)

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """
        Handle raw WebSocket message from WebSocketManager, then route it.
        This method is called by the WebSocketManager.
        """
        # Following the pattern from HyperliquidAPI, directly route to _route_ws_message.
        # Add any pre-processing here if Backpack requires it for common message envelopes.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Delegate WebSocket message routing to the WebSocket router."""
        await self._bp_ws_router.route_message(message, self._ws_handlers)

    # --- Market Data Methods --- #

    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker information for a specific symbol."""
        return await self.market_data_service.get_ticker(symbol=symbol)

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Get order book for a specific symbol."""
        return await self.market_data_service.get_order_book(symbol=symbol, limit=depth)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Get recent trades for a specific symbol."""
        return await self.market_data_service.get_recent_trades(symbol=symbol, limit=limit)

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Get current funding rate for a specific symbol."""
        return await self.market_data_service.get_funding_rate(symbol=symbol)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get historical market data (candlesticks) for a specific symbol."""
        return await self.market_data_service.get_market_data(args=args)

    # --- Account Methods --- #

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Get derivative positions."""
        return await self.account_service.get_positions(symbol=symbol)

    # --- Trading Methods --- #

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order."""
        return await self.trading_service.place_order(args=args)

    async def cancel_order(self, args: CancelOrderArgs) -> bool:
        """Cancel an existing order."""
        return await self.trading_service.cancel_order(args=args)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all open orders."""
        return await self.trading_service.get_open_orders(symbol=symbol)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Get funding rates for specified symbols or all symbols."""
        return await self.market_data_service.get_funding_rates(args=args)

    async def get_account_summary(self) -> MarginAccountSummary:
        """Get account summary information."""
        return await self.account_service.get_account_info()

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Transfer funds between account types."""
        return await self.account_service.transfer(args=args)

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Withdraw funds to an external address."""
        return await self.account_service.withdraw(args=args)

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

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Get historical orders."""
        return await self.account_service.get_order_history(args=args)

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Get recent trade history.

        Args:
            args: Parameters for filtering trade history including symbol and limit.
        """
        return await self.account_service.get_trade_history(args=args)

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Fetch a single order by its ID."""
        if args.symbol is None:
            raise ValueError("'symbol' parameter is required for Backpack.get_order()")
        return await self.trading_service.get_order(args=args)

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Fetch the status of a specific order."""
        if args.symbol is None:
            raise ValueError("'symbol' parameter is required for Backpack.get_order_status()")
        # Return type changed to Order | None to align with abstract method
        order = await self.trading_service.get_order_status(args=args)
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

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Fetch all open orders.

        Args:
            args: Parameters for filtering open orders including optional symbol.
        """
        return await self.trading_service.get_all_open_orders(args=args)

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
        self, args: GetHistoricalFundingRatesArgs
    ) -> list[FundingRate]:
        """Get historical funding rates for a specific symbol."""
        return await self.market_data_service.get_historical_funding_rates(args=args)

    async def close(self) -> None:
        """Close the API client and clean up resources."""
        await super().close()

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders."""
        return await self.trading_service.cancel_all_orders(symbol=symbol)
