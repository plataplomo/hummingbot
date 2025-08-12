"""CyberDeltaEngine: Backpack Exchange Integration.

This module implements the Backpack exchange adapter for CyberDeltaEngine, including:
- REST and WebSocket API client (`BackpackAPI`)
- Centralized error mapping and normalization (`BackpackErrorMapper`)

- Domain-specific data transformation mappers (decomposed account mappers,
  decomposed market data mappers, `BackpackOrderMapper`)

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

from typing import TYPE_CHECKING, Any

from cyberdelta.apis.backpack.bp_api_components_factory import BackpackAPIComponentsFactory
from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy
from cyberdelta.apis.backpack.bp_registry_builder import BackpackRegistryBuilder
from cyberdelta.apis.backpack.bp_ws_router import BackpackWebSocketRouter
from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models import BackpackRawWsSubscriptionRequest
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.backpack.services.bp_market_data_service import (
    BackpackMarketDataService,
)
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.exceptions import AuthenticatorNotConfiguredError
from cyberdelta.apis.exceptions.configuration import (
    RateLimitConfigurationError,
    TestnetConfigurationError,
)
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.base import RequiredParameterError


if TYPE_CHECKING:
    from collections.abc import Mapping

    from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
    from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
    from cyberdelta.apis.common import MessageHandler
    from cyberdelta.apis.models.service_args.account import (
        TransferArgs,
        UpdateAccountSettingsArgs,
        WithdrawArgs,
    )
from cyberdelta.apis.models.service_args.market_data import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
)
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import AnyExchangeSecrets as ExchangeSecretsConfig
from cyberdelta.models import (
    AccountSettings,
    DerivativePosition,
    Fill,
    FundingRate,
    MarginAccountSummary,
    MidPrices,
    Order,
    SpotBalance,
    Ticker,
)
from cyberdelta.models.market import Candle, Market, OrderBook
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.models.operations import Transfer, Withdrawal
from cyberdelta.symbols.models import Symbol


logger = get_logger(__name__)


class BackpackAPI(ExchangeAPI):
    """Backpack Exchange API Client.

    Implements connectivity and data handling for the Backpack exchange,
    adhering to the ExchangeAPI interface.

    Handles REST API requests and WebSocket connections for market data and account updates.

    **Rate Limiting:**
    - Proactive rate limiting uses `BackpackRateLimitStrategy` (extends `SimpleTokenBucketStrategy`)
      based on `rate_limit_per_minute` from `ExchangeSpecificConfig`.
    - Dynamic adjustment of this strategy from response headers is not performed (Backpack does not
      provide the necessary headers).
    - When rate limit errors occur, `BackpackErrorMapper` parses `retry_after` durations from the
      error message body, populating `APIError.retry_after`. This value is informational and can be
      used by higher-level application logic (e.g., to inform the `RateLimitStrategy` instance to
      pause or for monitoring). `HttpClient`'s internal retry mechanism and the
      `BackpackRateLimitStrategy` do not directly consume this `APIError.retry_after` to modify
      their behavior without higher-level intervention.
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
        # Note: request_builder and response_handler are now created by factory
        # New domain-specific mappers
        trading_data_mapper: BackpackOrderMapper | None = None,
        # Services
        account_service: BackpackAccountService | None = None,
        trading_service: BackpackTradingService | None = None,
        market_data_service: BackpackMarketDataService | None = None,
    ) -> None:
        """Initialize the BackpackAPI client with configuration and secrets.

        Args:
            exchange_config: Exchange-specific configuration model.
            exchange_secrets: Exchange secrets configuration model.
            authenticator: Optional authenticator instance for dependency injection
            error_mapper: Optional error mapper instance for dependency injection
            trading_data_mapper: Optional trading data mapper instance for dependency injection
            account_service: Optional account service instance for dependency injection
            trading_service: Optional trading service instance for dependency injection
            market_data_service: Optional market data service instance for dependency injection

        Raises:
            TestnetConfigurationError: If testnet environment is requested but testnet URLs are not
                configured.
            RateLimitConfigurationError: If rate_limit_per_minute is not provided in the exchange
                configuration.

        """
        # Create the factory to handle component instantiation
        factory = BackpackAPIComponentsFactory(exchange_config, exchange_secrets)

        # Use injected components or create them via factory
        self._bp_authenticator = authenticator or factory.create_authenticator()
        self._backpack_error_mapper = error_mapper or factory.create_error_mapper()
        # Note: request builders are now created by individual services
        # Note: response handlers are now created by individual services

        # Create instances of the new domain-specific mappers
        self._bp_trading_data_mapper = trading_data_mapper or factory.create_trading_data_mapper()

        # Validate URLs based on environment
        if not exchange_config.environment_type.is_production:
            if exchange_config.api_base_url_testnet is None:
                raise TestnetConfigurationError("API")
            if exchange_config.ws_url_testnet is None:
                raise TestnetConfigurationError("WebSocket")

        # Create Backpack's simple rate limit strategy
        # The rate limit parameters (rate, bucket_size) are derived from the static
        # rate_limit_per_minute configured in exchange_config
        if exchange_config.rate_limit_per_minute is None:
            raise RateLimitConfigurationError(ExchangeName.BACKPACK)

        rate_per_second = exchange_config.rate_limit_per_minute / 60.0
        bucket_size = max(1, int(rate_per_second * 2))
        bp_limiter_primitive = TokenBucketRateLimiterRuntime(
            rate=rate_per_second,
            bucket_size=bucket_size,
        )
        bp_strategy = BackpackRateLimitStrategy(
            limiter=bp_limiter_primitive,
            default_request_weight=1,
        )

        super().__init__(
            exchange_name=exchange_config.exchange_name,
            config=exchange_config,
            secrets=exchange_secrets,
            authenticator=self._bp_authenticator,
            error_mapper=self._backpack_error_mapper,
            rate_limit_strategy=bp_strategy,
        )

        # Initialize enhanced WebSocket router with new architecture
        error_handler = BaseErrorHandler(exchange_name=exchange_config.exchange_name)

        # Create registry using Backpack-specific builder
        builder = BackpackRegistryBuilder()
        registry = builder.build_registry()
        typed_processor = TypeSafeWebSocketProcessor(registry)

        self._bp_ws_router = BackpackWebSocketRouter(
            error_handler=error_handler,
            typed_processor=typed_processor,
            order_book_mapper=factory.create_order_book_mapper(),
            ticker_mapper=factory.create_ticker_mapper(),
            trade_mapper=factory.create_trade_mapper(),
            balance_mapper=factory.create_balance_mapper(),
            position_mapper=factory.create_position_mapper(),
            order_mapper=factory.create_trading_data_mapper(),
            transaction_mapper=factory.create_transaction_mapper(),
        )

        # Use self._request directly, services will handle the tuple response
        service_requester = self._request

        # Use injected services or create them via factory
        if market_data_service is not None:
            self.market_data_service = market_data_service
        else:
            self.market_data_service = factory.create_market_data_service(
                http_client_requester=service_requester,
                exchange_name=self.exchange_name,
            )

        if account_service is not None:
            self.account_service = account_service
        else:
            self.account_service = factory.create_account_service(
                http_client_requester=service_requester,
                authenticator=self._bp_authenticator,
                exchange_name=self.exchange_name,
            )

        if trading_service is not None:
            self.trading_service = trading_service
        else:
            self.trading_service = factory.create_trading_service(
                http_client_requester=service_requester,
                authenticator=self._bp_authenticator,
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
            AuthenticatorNotConfiguredError: If authenticator is missing for private subscriptions

        """
        # Determine if this is a private topic that requires authentication
        # According to Backpack API docs: "Private streams are prefixed with `account.`"
        signature_components = None

        # Check if topic requires authentication (private streams start with "account.")
        if topic.startswith("account."):
            if not self._bp_authenticator:
                raise AuthenticatorNotConfiguredError(
                    auth_type="ED25519",
                    operation="private WebSocket subscriptions",
                )

            # Extract subscription type and symbol from topic
            if "." in topic:
                subscription_type, symbol = topic.split(".", 1)
            else:
                subscription_type = topic
                symbol = None

            signature_components = self._bp_authenticator.get_ws_subscription_signature_components(
                subscription_type=subscription_type,
                symbol=symbol,
            )

        # Delegate to the WebSocket router with signature components
        return self._bp_ws_router.construct_subscription_payload(topic, signature_components)

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message from WebSocketManager, then route it.

        This method is called by the WebSocketManager.
        """
        # Following the pattern from HyperliquidAPI, directly route to _route_ws_message.
        # Add any pre-processing here if Backpack requires it for common message envelopes.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Delegate WebSocket message routing to the WebSocket router."""
        await self._bp_ws_router.route_message(message, self._ws_handlers)

    # --- Market Data Methods --- #

    async def get_ticker(self, symbol: Symbol) -> Ticker:
        """Get ticker information for a specific symbol.

        Args:
            symbol: The trading symbol to get ticker information for

        Returns:
            Ticker information for the specified symbol

        """
        return await self.market_data_service.get_ticker(symbol)

    async def get_order_book(self, symbol: Symbol, depth: int = 20) -> OrderBook:
        """Get order book for a specific symbol.

        Args:
            symbol: The trading symbol to get order book for
            depth: Maximum number of price levels to return (default: 20)

        Returns:
            Order book containing bids and asks for the specified symbol

        """
        return await self.market_data_service.get_order_book(symbol, depth)

    async def get_recent_trades(self, symbol: Symbol, limit: int | None = 50) -> list[Fill]:
        """Get recent trades for a specific symbol.

        Args:
            symbol: The trading symbol to get recent trades for
            limit: Maximum number of trades to return (default: 50)

        Returns:
            List of recent trades for the specified symbol

        """
        return await self.market_data_service.get_recent_trades(symbol, limit)

    async def get_funding_rate(self, symbol: Symbol) -> FundingRate:
        """Get current funding rate for a specific symbol.

        Args:
            symbol: The trading symbol to get funding rate for

        Returns:
            Current funding rate information for the specified symbol

        """
        return await self.market_data_service.get_funding_rate(symbol=symbol)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get historical market data (candlesticks) for a specific symbol.

        Args:
            args: Arguments specifying symbol, interval, and time range

        Returns:
            List of candlestick data for the specified parameters

        """
        return await self.market_data_service.get_market_data(args)

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Get market metadata for a specific symbol.

        Args:
            args: Arguments specifying the symbol to get market data for

        Returns:
            Market metadata for the specified symbol

        """
        return await self.market_data_service.get_market(args)

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Get market metadata for all available markets.

        Args:
            args: Arguments specifying market filters

        Returns:
            List of market metadata for all matching markets

        """
        return await self.market_data_service.get_markets(args)

    # --- Account Methods --- #

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances.

        Returns:
            Dictionary mapping asset symbols to their spot balances

        """
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: Symbol | None = None) -> list[DerivativePosition]:
        """Get derivative positions.

        Args:
            symbol: Optional symbol to filter positions (default: all positions)

        Returns:
            List of derivative positions

        """
        return await self.account_service.get_positions(symbol=symbol)

    # --- Trading Methods --- #

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order.

        Args:
            args: Order placement arguments including symbol, size, price, etc.

        Returns:
            Order object representing the placed order

        """
        return await self.trading_service.place_order(args)

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel an existing order.

        Args:
            args: Arguments specifying the order to cancel

        Returns:
            Result of the cancellation operation

        """
        return await self.trading_service.cancel_order(args)

    async def get_open_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Get all open orders.

        Args:
            symbol: Optional symbol to filter orders (default: all symbols)

        Returns:
            List of currently open orders

        """
        return await self.trading_service.get_open_orders(symbol)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Get funding rates for specified symbols or all symbols.

        Args:
            args: Arguments specifying which symbols to get funding rates for

        Returns:
            List of funding rates for the requested symbols

        """
        return await self.market_data_service.get_funding_rates(args)

    async def get_account_summary(self) -> MarginAccountSummary:
        """Get comprehensive account margin information.

        HYPERLIQUID CONSISTENCY:
        - Same method name as HyperliquidAPI.get_account_summary()
        - Same return type: MarginAccountSummary
        - Enhanced with Backpack collateral data in bp_details extension slot
        - Automatic fallback maintains compatibility

        Returns:
            MarginAccountSummary with bp_details populated when available
        """
        return await self.account_service.get_account_summary()

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits and auto-trading preferences.

        Args:
            args: Account settings to update

        Note:
            This allows updating leverage limits which directly impacts maximum position sizes
            for large balance testing. Changes take effect immediately.

        Returns:
            Updated account settings

        """
        return await self.account_service.update_account_settings(args)

    async def invalidate_account_cache(self, subaccount_id: int | None = None) -> None:
        """Invalidate cached account state data.

        This should be called after operations that modify account state
        (like placing orders) to ensure fresh data on subsequent queries.

        Args:
            subaccount_id: Optional subaccount ID (None for main account)
        """
        await self.account_service.invalidate_account_cache(subaccount_id)

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Transfer funds between account types.

        Args:
            args: Transfer arguments including amount, from/to account types

        Returns:
            Transfer object with transaction details

        """
        return await self.account_service.transfer(args)

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Withdraw funds to an external address.

        Args:
            args: Withdrawal arguments including amount, address, etc.

        Returns:
            Withdrawal object with transaction details

        """
        return await self.account_service.withdraw(args)

    async def subscribe_to_order_book(self, symbol: Symbol) -> None:
        """Subscribe to order book updates for a symbol."""
        topic = f"depth.{symbol.value}"
        logger.debug(
            "backpack_orderbook_subscription_prepared",
            exchange=self.exchange_name,
            symbol=symbol,
            topic=topic,
            subscription_type="depth",
            message=f"[{self.exchange_name.value}] Preparing subscription for topic: {topic}",
        )

    async def subscribe_to_ticker(self, symbol: Symbol) -> None:
        """Subscribe to ticker updates for a symbol."""
        topic = f"ticker.{symbol.value}"
        logger.debug(
            "backpack_ticker_subscription_prepared",
            exchange=self.exchange_name,
            symbol=symbol,
            topic=topic,
            subscription_type="ticker",
            message=f"[{self.exchange_name.value}] Preparing subscription for topic: {topic}",
        )

    async def subscribe_to_trades(self, symbol: Symbol) -> None:
        """Subscribe to public trade updates for a symbol."""
        # NOTE: Backpack uses "trade" (singular) not "trades" for the stream name
        topic = f"trade.{symbol.value}"
        logger.debug(
            "backpack_trades_subscription_prepared",
            exchange=self.exchange_name,
            symbol=symbol,
            topic=topic,
            subscription_type="trade",
            message=f"[{self.exchange_name.value}] Preparing subscription for topic: {topic}",
        )

    async def subscribe_to_account_updates(self) -> None:
        """Subscribe to private account updates (balances, positions, orders)."""
        fill_topic = "fills"
        order_topic = "orders"
        logger.debug(
            "backpack_account_updates_subscription_prepared",
            exchange=self.exchange_name,
            topics=[fill_topic, order_topic],
            subscription_type="account_updates",
            message=(
                f"[{self.exchange_name.value}] Preparing subscription for account topics: "
                f"{fill_topic}, {order_topic}"
            ),
        )

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Get historical orders.

        Args:
            args: Arguments for filtering order history

        Returns:
            List of historical orders

        """
        return await self.account_service.get_order_history(args)

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Fill]:
        """Get recent trade history.

        Args:
            args: Parameters for filtering trade history including symbol and limit.

        Returns:
            List of recent trades

        """
        return await self.account_service.get_trade_history(args)

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Fetch a single order by its ID.

        Args:
            args: Arguments containing order ID and symbol

        Returns:
            Order object if found, None otherwise

        Raises:
            RequiredParameterError: If symbol parameter is not provided

        """
        if args.symbol is None:
            raise RequiredParameterError(
                parameter="symbol",
                context="get_order",
                exchange=ExchangeName.BACKPACK,
            )
        return await self.trading_service.get_order(args)

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Fetch the status of a specific order.

        Args:
            args: Arguments containing order ID and symbol

        Returns:
            Order object with current status if found, None otherwise

        Raises:
            RequiredParameterError: If symbol parameter is not provided

        """
        if args.symbol is None:
            raise RequiredParameterError(
                parameter="symbol",
                context="get_order_status",
                exchange=ExchangeName.BACKPACK,
            )
        # Return type changed to Order | None to align with abstract method
        return await self.trading_service.get_order(args)

    # All abstract methods should now be implemented.

    # --- Abstract Method Implementations ---
    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """Update rate limit information based on response headers.

        This method is a no-op for dynamic limiter adjustments due to lack of Backpack headers.
        Backpack Exchange does not provide standard or known non-standard HTTP response headers
        that detail remaining rate limits or explicit `Retry-After` durations.

        Note that `retry-after` hints are parsed from error *message bodies* by the mapper for
        informational purposes.
        """
        # Backpack does not seem to provide standard rate limit headers.
        # If specific headers are discovered, they could be parsed here.
        logger.debug(
            "backpack_rate_limit_headers_noop",
            exchange=self.exchange_name,
            method=method,
            path=path,
            headers_count=len(headers) if headers else 0,
            action="no_dynamic_adjustment",
            message=(
                f"[{self.exchange_name.value}] No actionable rate limit headers found "
                f"for dynamic adjustment. Headers: {headers}, Method: {method}, Path: {path}"
            ),
        )

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Fetch all open orders.

        Args:
            args: Parameters for filtering open orders including optional symbol.

        Returns:
            List of all open orders

        """
        # Extract Symbol object from args
        symbol = args.symbol if args else None
        return await self.trading_service.get_open_orders(symbol)

    async def get_all_mids(self) -> MidPrices:
        """Get all mid prices - not supported by Backpack.

        Backpack does not provide a bulk mid prices endpoint.
        Use individual ticker calls or order book data instead.

        Raises:
            NotImplementedError: Backpack does not support bulk mid prices.
        """
        raise NotImplementedError(
            "Backpack exchange does not support get_all_mids operation. "
            "Use get_ticker() for individual symbols or get_order_book() for pricing data."
        )

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription via WebSocketManager."""
        # Removed redundant subscription log - base class already logs
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Handle actions upon WebSocket connection, typically resubscribing to topics."""
        logger.info(
            "backpack_websocket_connected",
            exchange=self.exchange_name,
            action="triggering_resubscription",
            message=(
                f"[{self.exchange_name.value}] WebSocket connected. "
                f"Triggering resubscription via base."
            ),
        )
        await super()._on_ws_connected()

    async def _resubscribe(self) -> None:
        """Resubscribe to topics upon WebSocket (re)connection."""
        logger.info(
            "backpack_websocket_resubscribing",
            exchange=self.exchange_name,
            action="delegating_to_base",
            message=f"[{self.exchange_name.value}] Resubscribe called. Delegating to base.",
        )
        await super()._resubscribe()

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Get historical funding rates for a specific symbol.

        Returns:
            List of funding rates for the specified symbol and time range.
        """
        return await self.market_data_service.get_historical_funding_rates(args)

    async def close(self) -> None:
        """Close the API client and clean up resources."""
        await super().close()

    async def cancel_all_orders(self, symbol: Symbol | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders.

        Returns:
            List of cancel order results for each cancelled order.
        """
        return await self.trading_service.cancel_all_orders(symbol)

    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Place multiple orders in a single batch request.

        Note: Backpack exchange does not currently support native batch operations.
        This implementation falls back to sequential order placement for compatibility.

        Args:
            orders: List of validated PlaceOrderArgs for batch placement

        Returns:
            List of successfully placed Order objects

        Raises:
            NotImplementedError: Backpack does not support batch operations yet
        """
        raise NotImplementedError(
            "Batch order placement is not yet implemented for Backpack exchange. "
            "Backpack does not support native batch operations. "
            "Use individual place_order() calls instead.",
        )

    async def cancel_batch_orders(
        self,
        cancel_args: list[CancelOrderArgs],
    ) -> list[CancelOrderResult]:
        """Cancel multiple orders in a single batch request.

        Note: Backpack exchange does not currently support native batch operations.
        This implementation falls back to sequential order cancellation for compatibility.

        Args:
            cancel_args: List of validated CancelOrderArgs for batch cancellation

        Returns:
            List of CancelOrderResult objects indicating success/failure for each order

        Raises:
            NotImplementedError: Backpack does not support batch operations yet
        """
        raise NotImplementedError(
            "Batch order cancellation is not yet implemented for Backpack exchange. "
            "Backpack does not support native batch operations. "
            "Use individual cancel_order() calls instead.",
        )
