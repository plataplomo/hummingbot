"""Hyperliquid DEX API client implementation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import aiohttp

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError, APIErrorCode, MessageHandler

# Removed get_symbol_integration_service import - no longer needed with new Symbol system
from cyberdelta.apis.connectivity.connectivity_models import HttpClientConfig
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
)
from cyberdelta.apis.hyperliquid.hl_api_components_factory import HyperliquidAPIComponentsFactory
from cyberdelta.apis.hyperliquid.hl_asset_indexer import HyperliquidAssetIndexResolver
from cyberdelta.apis.hyperliquid.hl_rate_limit_strategy import HyperliquidRateLimitStrategy
from cyberdelta.apis.hyperliquid.hl_registry_builder import HyperliquidRegistryBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.hl_ws_router import HyperliquidWebSocketRouter

# Import decomposed mappers that are used directly
from cyberdelta.apis.hyperliquid.mappers import (
    HyperliquidOrderMapper,
    HyperliquidOrderResponseMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import HyperliquidRawWsSubscribeRequest
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import (
    HyperliquidMarketDataService,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
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

# BaseErrorHandler import removed - deprecated and not used
from cyberdelta.apis.websocket.error_handling.error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.memory import get_memory_config_for_router
from cyberdelta.apis.websocket.ws_context_factory import WebSocketContextFactory
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.config.models.websocket_processor_config import WebSocketProcessorConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.base import RequiredParameterError
from cyberdelta.models import (
    AccountSettings,
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    MidPrices,
    SpotBalance,
    Ticker,
)
from cyberdelta.models.market import Candle, Market, OrderBook
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.market.order import (
    CancelOrderResult,
    Order,
)
from cyberdelta.models.operations import Transfer, Withdrawal

# Removed SymbolError import - no longer needed with new Symbol system
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import HyperliquidMetadata, Symbol


# Create instances of the new domain-specific mappers


if TYPE_CHECKING:
    from collections.abc import Mapping

    from cyberdelta.apis.base.authenticator_interface import (
        AuthenticatedRequestComponents,
    )
    from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
    from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
    # These are already imported above the TYPE_CHECKING block
from cyberdelta.config.secrets_models import AnyExchangeSecrets as ExchangeSecretsConfig


logger = get_logger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    account_service: HyperliquidAccountService
    trading_service: HyperliquidTradingService
    market_data_service: HyperliquidMarketDataService

    def __init__(
        self,
        exchange_config: ExchangeSpecificConfig,
        exchange_secrets: ExchangeSecretsConfig,
        # Optional dependency injection parameters for testing
        authenticator: HyperliquidEip712Authenticator | None = None,
        error_mapper: HyperliquidErrorMapper | None = None,
        request_builder: HyperliquidMarketDataRequestBuilder | None = None,
        response_handler: HyperliquidResponseHandler | None = None,
        # Decomposed mappers for direct usage
        order_mapper: HyperliquidOrderMapper | None = None,
        order_response_mapper: HyperliquidOrderResponseMapper | None = None,
        # HTTP client
        http_client: HttpClient | None = None,
        # Services
        account_service: HyperliquidAccountService | None = None,
        trading_service: HyperliquidTradingService | None = None,
        market_data_service: HyperliquidMarketDataService | None = None,
    ) -> None:
        """Initialize the HyperliquidAPI client.

        Args:
            exchange_config: Exchange-specific configuration model.
            exchange_secrets: Exchange secrets configuration model.
            authenticator: Optional authenticator instance for dependency injection
            error_mapper: Optional error mapper instance for dependency injection
            request_builder: Optional request builder instance for dependency injection
            response_handler: Optional response handler instance for dependency injection
            order_mapper: Optional order mapper instance for dependency injection
            order_response_mapper: Optional order response mapper instance for dependency injection
            http_client: Optional HTTP client instance for dependency injection
            account_service: Optional account service instance for dependency injection
            trading_service: Optional trading service instance for dependency injection
            market_data_service: Optional market data service instance for dependency injection

        Raises:
            RequiredParameterError: If chain_id is required but missing from exchange_config.
        """
        # URL Selection Logic based on environment
        if exchange_config.environment_type.is_production:
            self.active_api_base_url = str(exchange_config.api_base_url_mainnet)
            self.active_ws_url = (
                str(exchange_config.ws_url_mainnet) if exchange_config.ws_url_mainnet else None
            )
            logger.info(
                "hyperliquid_api_initializing_mainnet",
                exchange=exchange_config.exchange_name.value,
                environment="mainnet",
                api_base_url=self.active_api_base_url,
                ws_url=self.active_ws_url,
                message=(
                    f"[{exchange_config.exchange_name.value}] Initializing for MAINNET environment."
                ),
            )
        elif exchange_config.api_base_url_testnet:  # Check if testnet URL is actually configured
            self.active_api_base_url = str(exchange_config.api_base_url_testnet)
            self.active_ws_url = (
                str(exchange_config.ws_url_testnet) if exchange_config.ws_url_testnet else None
            )
            logger.info(
                "hyperliquid_api_initializing_testnet",
                exchange=exchange_config.exchange_name.value,
                environment="testnet",
                api_base_url=self.active_api_base_url,
                ws_url=self.active_ws_url,
                message=(
                    f"[{exchange_config.exchange_name.value}] Initializing for TESTNET environment."
                ),
            )
        else:
            # Fallback or error if environment_type is testnet but no testnet URLs
            logger.error(
                "hyperliquid_api_config_error_fallback_mainnet",
                exchange=exchange_config.exchange_name.value,
                environment_type=exchange_config.environment_type.value,
                has_testnet_url=bool(exchange_config.api_base_url_testnet),
                action="falling_back_to_mainnet",
                message=(
                    f"[{exchange_config.exchange_name.value}] Configuration error: "
                    f"environment_type is testnet, but no testnet URLs "
                    f"(api_base_url_testnet) are provided. Falling back to mainnet URLs."
                ),
            )
            self.active_api_base_url = str(exchange_config.api_base_url_mainnet)
            self.active_ws_url = (
                str(exchange_config.ws_url_mainnet) if exchange_config.ws_url_mainnet else None
            )

        # Set endpoints to the active URLs
        self.rest_endpoint = self.active_api_base_url
        self.ws_endpoint = self.active_ws_url

        # Check chain_id is present for Hyperliquid
        if exchange_config.chain_id is None:
            raise RequiredParameterError(
                parameter="chain_id",
                context="Hyperliquid initialization",
                exchange=ExchangeName.HYPERLIQUID,
            )

        # Create the factory to handle component instantiation
        factory = HyperliquidAPIComponentsFactory(
            exchange_config,
            exchange_secrets,
            exchange_config.chain_id,
        )

        # Use injected components or create them via factory
        self._hl_authenticator = authenticator or factory.create_authenticator()
        self._hyperliquid_error_mapper = error_mapper or factory.create_error_mapper()
        # Note: request_builder parameter is deprecated - services create their own builders
        self._hl_response_handler = response_handler or factory.create_response_handler()

        # Use injected mappers or create them via factory
        self._hl_order_mapper = order_mapper or factory.create_order_mapper()
        self._hl_order_response_mapper = (
            order_response_mapper or factory.create_order_response_mapper()
        )

        # Create serialization strategy via factory
        self._hl_serialization_strategy = factory.create_serialization_strategy()

        # Get wallet address from authenticator if created
        self._wallet_address = (
            self._hl_authenticator.wallet_address if self._hl_authenticator else None
        )

        # Create Hyperliquid-specific rate limit strategy
        hl_strategy = HyperliquidRateLimitStrategy(exchange_config)

        super().__init__(
            exchange_name=exchange_config.exchange_name,
            config=exchange_config,
            secrets=exchange_secrets,
            authenticator=self._hl_authenticator,
            error_mapper=self._hyperliquid_error_mapper,
            rate_limit_strategy=hl_strategy,
            serialization_strategy=self._hl_serialization_strategy,
        )

        # Use injected HTTP client or create one
        if http_client is not None:
            self._http_client = http_client
        else:
            # Build HttpClientConfig from exchange_config
            http_client_config_data = {
                "rest_endpoint": self.active_api_base_url,  # Use active URL
                "default_request_timeout": exchange_config.request_timeout_seconds,
                "max_retries": exchange_config.max_retries,
                "retry_delay_seconds": exchange_config.retry_delay_seconds,
            }
            # Filter out None values so Pydantic defaults apply
            http_client_config_data_cleaned = {
                k: v for k, v in http_client_config_data.items() if v is not None
            }
            # Ensure rest_endpoint is always passed
            if "rest_endpoint" not in http_client_config_data_cleaned:
                http_client_config_data_cleaned["rest_endpoint"] = self.active_api_base_url

            http_client_config_obj = HttpClientConfig.model_validate(
                http_client_config_data_cleaned,
            )
            self._http_client = HttpClient(
                exchange_config.exchange_name.value, http_client_config_obj
            )

        # Initialize asset index resolver with concrete instances
        # The asset indexer needs concrete implementations
        market_data_response_handler = factory.create_market_data_response_handler()
        market_data_request_builder = factory.create_market_data_request_builder()

        self._asset_indexer = HyperliquidAssetIndexResolver(
            requester=self._request,
            response_handler=market_data_response_handler,
            request_builder=market_data_request_builder,
            exchange_name_for_log=exchange_config.exchange_name.value,
            environment_type=exchange_config.environment_type,
        )

        # Create order book service to share between market data and trading services
        # Only create if both services are being created by factory
        shared_order_book_service = None
        if market_data_service is None and trading_service is None:
            shared_order_book_service = factory.create_order_book_service(
                http_client_requester=self._request,
                mapper=factory.create_order_book_mapper(),
                request_builder=factory.create_market_data_request_builder(),
                response_handler=factory.create_market_data_response_handler(),
                exchange_name=self.exchange_name,
            )

        # Use injected market data service or create one via factory
        if market_data_service is not None:
            self.market_data_service = market_data_service
        else:
            self.market_data_service = factory.create_market_data_service(
                http_client_requester=self._request,
                request_builder=factory.create_market_data_request_builder(),
                response_handler=factory.create_market_data_response_handler(),
                exchange_name=self.exchange_name,
                order_book_service=shared_order_book_service,
            )

        # Use injected account service or create one via factory
        if account_service is not None:
            self.account_service = account_service
        else:
            self.account_service = factory.create_account_service(
                http_client_requester=self._request,
                authenticator=self._hl_authenticator,
                request_builder=factory.create_account_request_builder(),
                response_handler=factory.create_account_response_handler(),
                exchange_name=self.exchange_name,
                wallet_address=self._wallet_address,
                get_asset_index_callable=self._get_asset_index,
            )

        # Use injected trading service or create one via factory
        if trading_service is not None:
            self.trading_service = trading_service
        else:
            self.trading_service = factory.create_trading_service(
                http_client_requester=self._request,
                authenticator=self._hl_authenticator,
                order_mapper=self._hl_order_mapper,
                order_response_mapper=self._hl_order_response_mapper,
                error_mapper=self._hyperliquid_error_mapper,
                exchange_name=self.exchange_name,
                wallet_address=self._wallet_address,
                get_asset_index_callable=self._get_asset_index,
                order_book_service=shared_order_book_service,
            )

        self.default_headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.trade_callback: MessageHandler | None = None
        self.order_update_callback: MessageHandler | None = None
        self.fill_callback: MessageHandler | None = None
        self.orderbook_callback: MessageHandler | None = None

        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_lock = asyncio.Lock()
        self._symbol_map: dict[str, str] = {}
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._ws_subscriptions: dict[str, MessageHandler] = {}
        self._is_connected = False

        # Initialize enhanced WebSocket router with new architecture
        # Create stream error handler for new architecture
        default_error_config = WebSocketErrorConfig()
        stream_error_handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=exchange_config.exchange_name,
            config=default_error_config.get_exchange_config(exchange_config.exchange_name),
        )

        # Get WebSocket processor config with memory settings
        processor_config = WebSocketProcessorConfig()
        memory_config = processor_config.performance.memory

        # Create registry using Hyperliquid-specific builder
        builder = HyperliquidRegistryBuilder()
        registry = builder.build_registry()
        typed_processor = WebSocketContextFactory(registry)

        # Get memory configuration from processor config (CODING_STANDARDS.md compliant)
        memory_optimization_mode, memory_pool_size = get_memory_config_for_router(memory_config)

        self._hl_ws_router = HyperliquidWebSocketRouter(
            stream_error_handler=stream_error_handler,
            context_factory=typed_processor,
            order_book_mapper=factory.create_order_book_mapper(),
            price_ticker_mapper=factory.create_price_ticker_mapper(),
            balance_mapper=factory.create_balance_mapper(),
            position_mapper=factory.create_position_mapper(),
            order_mapper=self._hl_order_mapper,
            transaction_mapper=factory.create_transaction_mapper(),
            historical_data_mapper=factory.create_historical_data_mapper(),
            memory_optimization_mode=memory_optimization_mode,
            memory_pool_size=memory_pool_size,
        )

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Use the HyperliquidEip712Authenticator to prepare request components.

        Args:
            method: HTTP method for the request
            path: API path for the request
            params: Optional query parameters
            data: Optional request body data

        Returns:
            Dictionary containing authentication headers for the request.

        Raises:
            APIError: If authentication fails or authenticator is not initialized
        """
        if not self._hl_authenticator:
            logger.error(
                "hyperliquid_authentication_missing",
                exchange=self.exchange_name,
                method=method,
                path=path,
                action="authentication_failed",
                message=(
                    f"[{self.exchange_name.value}] Attempt to call signed endpoint "
                    f"({method} {path}) without configured HL authenticator."
                ),
            )
            auth_not_initialized_msg = (
                "HL authenticator not initialized (e.g., missing/invalid private key)."
            )
            raise APIError(
                auth_not_initialized_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        current_headers = self.default_headers.copy()

        try:
            auth_components: AuthenticatedRequestComponents = (
                await self._hl_authenticator.prepare_request(
                    method,
                    path,
                    params,
                    data,
                    current_headers,
                )
            )
        except APIError:
            # Re-raise APIErrors from authenticator directly
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            # Wrap other exceptions as authentication failures
            logger.exception(
                "hyperliquid_authentication_preparation_error",
                exchange=self.exchange_name,
                method=method,
                path=path,
                error_type=type(e).__name__,
                error=str(e),
                action="authentication_failed",
                message="[%s] Unexpected error during authentication preparation for %s %s: %s",
                message_args=(self.exchange_name.value, method, path, str(e)),
            )
            auth_prep_failed_msg = f"Authentication preparation failed: {e}"
            raise APIError(
                auth_prep_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        return {
            "headers": auth_components.headers,
            "params": auth_components.params,
            "data": auth_components.data,
        }

    async def _get_asset_index(self, symbol: str) -> int | None:
        """Fetch or retrieve from cache the asset_index for a given symbol.

        Uses the new unified symbol system with fallback to legacy asset indexer.

        Returns:
            Integer asset index for the given symbol, or None if symbol not found.
        """
        # Try new symbol system first
        try:
            # Create symbol object - if asset_index was provided during creation,
            # it will be available
            symbol_obj = exchanges.hyperliquid(symbol)

            # Check if this is a Hyperliquid symbol with asset_index
            if (
                isinstance(symbol_obj.metadata, HyperliquidMetadata)
                and symbol_obj.metadata.asset_index is not None
            ):
                return symbol_obj.metadata.asset_index

            # If asset_index is None, we need to fetch it from the market metadata
            # Use the same logic as the legacy indexer but integrated with the symbol system
            return await self._fetch_asset_index_from_api(symbol)

        except (ValueError, KeyError, AttributeError) as e:
            # Symbol creation or metadata access error, fall back to legacy asset indexer
            logger.debug(
                "symbol_system_fallback_to_legacy",
                symbol=symbol,
                error=str(e),
                reason="symbol_system_error",
            )

        # Fallback to legacy asset indexer
        return await self._asset_indexer.get_asset_index_or_none(symbol)

    async def _fetch_asset_index_from_api(self, symbol: str) -> int | None:
        """Fetch asset index from API by delegating to the legacy asset indexer.

        This method will eventually be replaced when the Symbol system fully integrates
        asset index resolution, but for now it provides a bridge.

        Returns:
            Asset index for the symbol, or None if not found.
        """
        try:
            # Delegate to the asset indexer for now - this avoids circular dependencies
            # and reuses the existing logic for fetching from /info endpoint
            return await self._asset_indexer.get_asset_index_or_none(symbol)

        except (APIError, ValueError) as e:
            logger.debug(
                "fetch_asset_index_from_api_error",
                symbol=symbol,
                error=str(e),
                reason="asset_indexer_error",
            )
            return None

    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """Update rate limit information based on response headers.

        Args:
            headers: Response headers from the API
            method: HTTP method used for the request
            path: API path used for the request

        Note:
            Hyperliquid does not typically provide rate limit info in standard headers.
            This is a placeholder implementation.
        """
        logger.debug(
            "hyperliquid_rate_limit_update_noop",
            exchange=self.exchange_name,
            method=method,
            path=path,
            headers_count=len(headers) if headers else 0,
            action="rate_limit_noop",
            message=(
                f"[{self.exchange_name.value}] _update_rate_limit_from_headers called "
                f"(no-op for Hyperliquid). Headers: {headers}, Method: {method}, Path: {path}"
            ),
        )

    def _construct_subscription_payload(self, topic: str) -> HyperliquidRawWsSubscribeRequest:
        """Construct subscription payload for the given topic.

        Args:
            topic: The WebSocket topic to subscribe to

        Returns:
            HyperliquidRawWsSubscribeRequest model
        """
        # Delegate to the WebSocket router for payload construction
        return self._hl_ws_router.construct_subscription_payload(topic, self._wallet_address)

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message from WebSocketManager, then route it.

        Args:
            message: Raw WebSocket message dictionary

        Note:
            This method is called by the WebSocketManager.
        """
        # Following the pattern from BackpackAPI, directly route to _route_ws_message.
        # Add any pre-processing here if Hyperliquid requires it for common message envelopes.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Delegate WebSocket message routing to the WebSocket router.

        Args:
            message: WebSocket message dictionary to route
        """
        await self._hl_ws_router.route_message(message, self._ws_handlers)

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic.

        Note:
            This method delegates to the parent class's WebSocket connection logic.
        """
        await super().connect_websocket()

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances.

        Returns:
            Dictionary mapping asset names to SpotBalance objects
        """
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: Symbol | None = None) -> list[DerivativePosition]:
        """Get derivative positions.

        Args:
            symbol: Optional symbol to filter positions for

        Returns:
            List of DerivativePosition objects
        """
        return await self.account_service.get_positions(symbol=symbol)

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits and auto-trading preferences.

        Args:
            args: Account settings to update

        Returns:
            Updated AccountSettings object

        Note:
            Hyperliquid uses a fundamentally different approach than Backpack:
            - Leverage is set per-asset, not globally
            - No equivalent for auto_lend, auto_borrow_settlements, auto_realize_pnl,
              auto_repay_borrows
            - Uses EIP-712 signed actions via /exchange endpoint

        """
        return await self.account_service.update_account_settings(args=args)

    async def get_open_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Get all open orders.

        Args:
            symbol: Optional symbol to filter orders for

        Returns:
            List of open Order objects
        """
        return await self.trading_service.get_open_orders(symbol=symbol)

    async def get_ticker(self, symbol: Symbol) -> Ticker | None:
        """Get ticker information for a specific symbol.

        Args:
            symbol: The trading symbol to get ticker for

        Returns:
            Ticker object if found, None otherwise
        """
        return await self.market_data_service.get_ticker(symbol=symbol)

    async def get_order_book(self, symbol: Symbol, depth: int | None = None) -> OrderBook | None:
        """Get order book for a specific symbol.

        Args:
            symbol: The trading symbol to get order book for
            depth: Optional depth limit for order book levels

        Returns:
            OrderBook object if found, None otherwise
        """
        return await self.market_data_service.get_order_book(symbol=symbol)

    async def get_recent_trades(self, symbol: Symbol, limit: int | None = 50) -> list[Fill]:
        """Get recent trades for a specific symbol.

        Args:
            symbol: The trading symbol to get trades for
            limit: Maximum number of trades to return (default 50)

        Returns:
            List of recent Fill objects
        """
        return await self.market_data_service.get_recent_fills(symbol=symbol)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Get funding rates for specified symbols or all symbols.

        Args:
            args: Parameters for filtering funding rates

        Returns:
            List of FundingRate objects
        """
        return await self.market_data_service.get_funding_rates(args=args)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get historical market data (candlesticks) for a specific symbol.

        Args:
            args: Parameters for historical market data including symbol and timeframe

        Returns:
            List of Candle objects
        """
        return await self.market_data_service.get_market_data(args=args)

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Get market metadata for a specific symbol.

        Args:
            args: Parameters including the symbol to get market data for

        Returns:
            Market object with metadata
        """
        return await self.market_data_service.get_market(args=args)

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Get market metadata for all available markets.

        Args:
            args: Parameters for filtering markets

        Returns:
            List of Market objects
        """
        return await self.market_data_service.get_markets(args=args)

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order.

        Args:
            args: Order placement parameters

        Returns:
            Placed Order object
        """
        return await self.trading_service.place_order(args)

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel an existing order.

        Args:
            args: Order cancellation parameters

        Returns:
            CancelOrderResult indicating success/failure
        """
        return await self.trading_service.cancel_order(args=args)

    async def cancel_all_orders(self, symbol: Symbol | None = None) -> list[CancelOrderResult]:
        """Cancel all orders for a given symbol, or all if symbol is None.

        Args:
            symbol: Optional symbol to cancel orders for, or None for all orders

        Returns:
            List of CancelOrderResult objects
        """
        return await self.trading_service.cancel_all_orders(symbol=symbol)

    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Place multiple orders in a single batch request for massive performance improvement.

        This method provides significant performance benefits by batching multiple order
        placements into a single API call, reducing:
        - N HTTP requests to 1 (6x performance improvement for 6 orders)
        - N EIP-712 signatures to 1
        - Network overhead and latency
        - API rate limit consumption

        Expected performance: 6 orders placed in <1 second vs ~9 seconds sequential

        Args:
            orders: List of validated PlaceOrderArgs for batch placement (max 50 orders)

        Returns:
            List of successfully placed Order objects

        Example:
            ```python
            orders = [
                PlaceOrderArgs(symbol="ETH-USD", side=OrderSide.BUY, ...),
                PlaceOrderArgs(symbol="BTC-USD", side=OrderSide.BUY, ...),
                # ... up to 50 orders
            ]
            placed_orders = await api.place_batch_orders(orders)
            ```

        Note:
            - Market orders are not supported in batch operations for safety
            - All orders must pass individual validation
            - Partial failures are reported with detailed error context
        """
        return await self.trading_service.place_batch_orders(orders)

    async def cancel_batch_orders(
        self,
        cancel_args: list[CancelOrderArgs],
    ) -> list[CancelOrderResult]:
        """Cancel multiple orders in a single batch request for improved performance.

        This method batches multiple order cancellations into a single API call,
        reducing network overhead and improving cancellation speed.

        Args:
            cancel_args: List of validated CancelOrderArgs for batch cancellation (max 50)

        Returns:
            List of CancelOrderResult objects indicating success/failure for each order

        Example:
            ```python
            cancellations = [
                CancelOrderArgs(order_id="123", symbol="ETH-USD"),
                CancelOrderArgs(order_id="456", symbol="BTC-USD"),
                # ... up to 50 cancellations
            ]
            results = await api.cancel_batch_orders(cancellations)
            ```

        Note:
            - Symbol must be provided for each cancellation
            - Results include individual success/failure status for each order
        """
        return await self.trading_service.cancel_batch_orders(cancel_args)

    async def get_account_summary(self) -> MarginAccountSummary:
        """Get account summary information.

        Returns:
            MarginAccountSummary with account details
        """
        return await self.account_service.get_account_summary()

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Fetch the status of a specific order.

        Args:
            args: Parameters including order ID to fetch

        Returns:
            Order object if found, None otherwise
        """
        return await self.trading_service.get_order(args=args)

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Fetch a single order by its ID.

        Args:
            args: Parameters including order ID to fetch

        Returns:
            Order object if found, None otherwise
        """
        return await self.trading_service.get_order(args=args)

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Get historical orders.

        Args:
            args: Parameters for filtering order history

        Returns:
            List of historical Order objects
        """
        return await self.account_service.get_order_history(args=args)

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Fill]:
        """Get recent trade history.

        Args:
            args: Parameters for filtering trade history including symbol and limit.

        Returns:
            List of Fill objects from history
        """
        return await self.account_service.get_fill_history(args=args)

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Get historical funding rates for a specific symbol.

        Args:
            args: Parameters including symbol and start_time for funding rates

        Returns:
            List of historical FundingRate objects

        Raises:
            RequiredParameterError: If start_time is not provided
        """
        # Hyperliquid requires start_time
        if args.start_time is None:
            raise RequiredParameterError(
                parameter="start_time",
                context="get_historical_funding_rates",
                exchange=ExchangeName.HYPERLIQUID,
            )
        return await self.market_data_service.get_historical_funding_rates(args=args)

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Transfer funds between account types.

        Args:
            args: Transfer parameters including amount and account types

        Returns:
            Transfer object with transaction details
        """
        return await self.account_service.transfer(args)

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Withdraw funds to an external address.

        Args:
            args: Withdrawal parameters including amount and destination

        Returns:
            Withdrawal object with transaction details
        """
        return await self.account_service.withdraw(args)

    async def subscribe_to_order_book(self, symbol: Symbol) -> None:
        """Prepare subscription to order book updates for a symbol.

        Args:
            symbol: The trading symbol to subscribe to

        Note:
            Actual subscription with a handler is done via self.subscribe().
        """
        # Hyperliquid topic format: "l2Book:SYMBOL"
        topic = f"l2Book:{symbol.value}"
        logger.debug(
            "hyperliquid_orderbook_subscription_prepared",
            exchange=self.exchange_name,
            symbol=symbol,
            topic=topic,
            subscription_type="l2Book",
            message=(
                f"[{self.exchange_name.value}] Preparing subscription for order book "
                f"(l2Book) topic: {topic}"
            ),
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe_to_ticker(self, symbol: Symbol) -> None:
        """Prepare subscription to ticker updates for a symbol.

        Args:
            symbol: The trading symbol to subscribe to

        Note:
            Hyperliquid does not have a direct per-symbol ticker stream like 'ticker.SYMBOL'.
            It uses 'allMids' for all symbols or relies on order book/trades for ticker-like data.
            This method will log a warning. Consider subscribing to 'allMids' or 'l2Book' instead.
        """
        # Hyperliquid uses "allMids" for a combined stream.
        # Individual ticker streams like "ticker:SYMBOL" are not standard for HL.
        logger.warning(
            "hyperliquid_ticker_stream_not_available",
            exchange=self.exchange_name,
            symbol=symbol,
            alternative_streams=["allMids", f"l2Book:{symbol.value}"],
            recommendation="use_allmids_or_orderbook",
            message=(
                f"[{self.exchange_name.value}] Hyperliquid does not have a direct "
                f"'ticker:{symbol.value}' "
                f"stream. Consider subscribing to 'allMids' for all mid prices, or "
                f"'l2Book:{symbol.value}' and derive ticker data."
            ),
        )
        # No direct topic construction for a non-existent stream type.

    async def subscribe_to_trades(self, symbol: Symbol) -> None:
        """Prepare subscription to public trade updates for a symbol.

        Args:
            symbol: The trading symbol to subscribe to

        Note:
            Actual subscription with a handler is done via self.subscribe().
        """
        # Hyperliquid topic format: "trades:SYMBOL"
        topic = f"trades:{symbol.value}"
        logger.debug(
            "hyperliquid_trades_subscription_prepared",
            exchange=self.exchange_name,
            symbol=symbol,
            topic=topic,
            subscription_type="trades",
            message=(
                f"[{self.exchange_name.value}] Preparing subscription for public trades "
                f"topic: {topic}"
            ),
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe_to_account_updates(self) -> None:
        """Prepare subscription to private account updates (fills, orders, positions).

        Note:
            Actual subscription with a handler is done via self.subscribe().
            Hyperliquid uses a single 'userEvents' stream for this.
        """
        # Hyperliquid topic format for all user data: "userEvents"
        # This requires wallet_address to be known by _construct_subscription_payload
        topic = "userEvents"
        logger.debug(
            "hyperliquid_account_updates_subscription_prepared",
            exchange=self.exchange_name,
            topic=topic,
            subscription_type="userEvents",
            message=(
                f"[{self.exchange_name.value}] Preparing subscription for user account updates "
                f"(userEvents) topic: {topic}"
            ),
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription via WebSocketManager.

        Args:
            topic: The WebSocket topic to subscribe to
            handler: Message handler function for processing messages
        """
        # Removed redundant subscription log - base class already logs
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Handle WebSocket connection, typically to resubscribe to topics.

        Note:
            This method is called automatically when WebSocket connection is established.
        """
        logger.info(
            "hyperliquid_websocket_connected",
            exchange=self.exchange_name,
            action="triggering_resubscription",
            message=(
                f"[{self.exchange_name.value}] WebSocket connected. "
                f"Triggering resubscription via base ExchangeAPI."
            ),
        )
        await super()._on_ws_connected()

    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics upon WebSocket (re)connection.

        Note:
            This method delegates to the base ExchangeAPI implementation.
        """
        logger.info(
            "hyperliquid_websocket_resubscribing",
            exchange=self.exchange_name,
            action="delegating_to_base_api",
            message=(
                f"[{self.exchange_name.value}] Resubscribing to topics. "
                f"Delegating to base ExchangeAPI."
            ),
        )
        await super()._resubscribe()

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Retrieve all open orders, optionally filtered by symbol.

        Args:
            args: Parameters for filtering open orders including optional symbol.

        Returns:
            List of open Order objects
        """
        return await self.trading_service.get_all_open_orders(args=args)

    async def get_all_mids(self) -> MidPrices:
        """Get all mid prices for efficient market order pricing.

        Returns:
            MidPrices object containing mid price data for all available symbols.
            This is used for reference pricing in market order calculations.
        """
        return await self.market_data_service.get_all_mids()
