from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import aiohttp

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
)
from cyberdelta.apis.base.exchange_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.connectivity.connectivity_models import HttpClientConfig
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
)
from cyberdelta.apis.hyperliquid.hl_api_components_factory import HyperliquidAPIComponentsFactory
from cyberdelta.apis.hyperliquid.hl_asset_indexer import HyperliquidAssetIndexResolver
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.hl_ws_message_router import HyperliquidWsMessageRouter
from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler

# Create instances of the new domain-specific mappers
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import (
    HyperliquidAccountDataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import (
    HyperliquidMarketDataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import (
    HyperliquidTradingDataMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import HyperliquidRawWsSubscribeRequest
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
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
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.config.secrets_models import AnyExchangeSecrets as ExchangeSecretsConfig
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import (
    CancelOrderResult,
    Order,
)
from cyberdelta.core.models.operations import Transfer, Withdrawal

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
        request_builder: HyperliquidRequestBuilder | None = None,
        response_handler: HyperliquidResponseHandler | None = None,
        # Domain-specific mappers
        account_data_mapper: HyperliquidAccountDataMapper | None = None,
        market_data_mapper: HyperliquidMarketDataMapper | None = None,
        trading_data_mapper: HyperliquidTradingDataMapper | None = None,
        # HTTP client
        http_client: HttpClient | None = None,
        # Services
        account_service: HyperliquidAccountService | None = None,
        trading_service: HyperliquidTradingService | None = None,
        market_data_service: HyperliquidMarketDataService | None = None,
    ) -> None:
        """
        Initialize the HyperliquidAPI client.

        Args:
            exchange_config: Exchange-specific configuration model.
            exchange_secrets: Exchange secrets configuration model.
            authenticator: Optional authenticator instance for dependency injection
            error_mapper: Optional error mapper instance for dependency injection
            request_builder: Optional request builder instance for dependency injection
            response_handler: Optional response handler instance for dependency injection
            account_data_mapper: Optional account data mapper instance for dependency injection
            market_data_mapper: Optional market data mapper instance for dependency injection
            trading_data_mapper: Optional trading data mapper instance for dependency injection
            http_client: Optional HTTP client instance for dependency injection
            account_service: Optional account service instance for dependency injection
            trading_service: Optional trading service instance for dependency injection
            market_data_service: Optional market data service instance for dependency injection
        """
        # Extract endpoints from config
        self.rest_endpoint = str(exchange_config.api_base_url)
        self.ws_endpoint = str(exchange_config.ws_url) if exchange_config.ws_url else None

        # Check chain_id is present for Hyperliquid
        if exchange_config.chain_id is None:
            raise ValueError(
                "chain_id is required for Hyperliquid but was None in exchange_config. "
                "Check AppSettings validator."
            )

        # Create the factory to handle component instantiation
        factory = HyperliquidAPIComponentsFactory(
            exchange_config, exchange_secrets, exchange_config.chain_id
        )

        # Use injected components or create them via factory
        self._hl_authenticator = authenticator or factory.create_authenticator()
        self._hyperliquid_error_mapper = error_mapper or factory.create_error_mapper()
        self._hl_request_builder = request_builder or factory.create_request_builder()
        self._hl_response_handler = response_handler or factory.create_response_handler()

        # Use injected mappers or create them via factory
        self._hl_account_data_mapper = account_data_mapper or factory.create_account_data_mapper()
        self._hl_trading_data_mapper = trading_data_mapper or factory.create_trading_data_mapper()
        self._hl_market_data_mapper = market_data_mapper or factory.create_market_data_mapper()

        # Get wallet address from authenticator if created
        self._wallet_address = (
            self._hl_authenticator.wallet_address if self._hl_authenticator else None
        )

        self.exchange_name = "hyperliquid"

        # Construct config dict for super().__init__
        rate_per_second = exchange_config.rate_limit_per_minute / 60.0
        bucket_size = max(1, int(rate_per_second * 2))

        config_dict_for_super = {
            "exchange_name": exchange_config.exchange_name.value,
            "rest_endpoint": self.rest_endpoint,
            "ws_url": self.ws_endpoint,
            "rate_limits": {
                "default_rate": rate_per_second,
                "default_bucket_size": bucket_size,
            },
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
        # But keep rate_limits since it's always required and doesn't contain None
        config_dict_for_super_cleaned = {
            k: v for k, v in config_dict_for_super.items() if v is not None or k == "rate_limits"
        }

        # Construct secrets dict for super().__init__
        # Check if we have the correct auth type for Hyperliquid
        from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
        
        secrets_dict_for_super: dict[str, str | None]
        if isinstance(exchange_secrets, PrivateKeyAuthSecrets):
            secrets_dict_for_super = {
                "private_key": exchange_secrets.private_key.get_secret_value()
                if exchange_secrets.private_key
                else None,
                "passphrase": exchange_secrets.passphrase.get_secret_value()
                if exchange_secrets.passphrase
                else None,
                "wallet_address": self._wallet_address,
            }
        else:
            # This should not happen if secrets validation is working correctly
            logger.error(
                f"Hyperliquid API received wrong auth type: {exchange_secrets.auth_type}. "
                f"Expected 'private_key'. Authentication will fail."
            )
            secrets_dict_for_super = {
                "private_key": None,
                "passphrase": None,
                "wallet_address": self._wallet_address,
            }

        super().__init__(
            exchange_name=exchange_config.exchange_name.value,
            config=config_dict_for_super_cleaned,
            secrets=secrets_dict_for_super,
            authenticator=self._hl_authenticator,
            error_mapper=self._hyperliquid_error_mapper,
        )

        # Use injected HTTP client or create one
        if http_client is not None:
            self._http_client = http_client
        else:
            # Build HttpClientConfig from exchange_config
            http_client_config_data = {
                "rest_endpoint": exchange_config.api_base_url,
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
                http_client_config_data_cleaned["rest_endpoint"] = exchange_config.api_base_url

            http_client_config_obj = HttpClientConfig.model_validate(
                http_client_config_data_cleaned
            )
            self._http_client = HttpClient(self.exchange_name, http_client_config_obj)

        # Initialize asset index resolver
        self._asset_indexer = HyperliquidAssetIndexResolver(
            requester=self._request,
            response_handler=self._hl_response_handler,
            request_builder=self._hl_request_builder,
            exchange_name_for_log=self.exchange_name,
        )

        # Use injected market data service or create one via factory
        if market_data_service is not None:
            self.market_data_service = market_data_service
        else:
            self.market_data_service = factory.create_market_data_service(
                http_client_requester=self._request,
                market_data_mapper=self._hl_market_data_mapper,
                request_builder=self._hl_request_builder,
                response_handler=self._hl_response_handler,
                exchange_name=self.exchange_name,
            )

        # Use injected account service or create one via factory
        if account_service is not None:
            self.account_service = account_service
        else:
            self.account_service = factory.create_account_service(
                http_client_requester=self._request,
                authenticator=self._hl_authenticator,
                account_data_mapper=self._hl_account_data_mapper,
                trading_data_mapper=self._hl_trading_data_mapper,
                request_builder=self._hl_request_builder,
                response_handler=self._hl_response_handler,
                exchange_name=self.exchange_name,
                wallet_address=self._wallet_address,
            )

        # Use injected trading service or create one via factory
        if trading_service is not None:
            self.trading_service = trading_service
        else:
            self.trading_service = factory.create_trading_service(
                http_client_requester=self._request,
                authenticator=self._hl_authenticator,
                trading_data_mapper=self._hl_trading_data_mapper,
                error_mapper=self._hyperliquid_error_mapper,
                request_builder=self._hl_request_builder,
                response_handler=self._hl_response_handler,
                exchange_name=self.exchange_name,
                wallet_address=self._wallet_address,
                get_asset_index_callable=self._get_asset_index,
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

        # Initialize WebSocket message router
        self._hl_ws_router = HyperliquidWsMessageRouter(
            market_data_mapper=self._hl_market_data_mapper,
            account_data_mapper=self._hl_account_data_mapper,
            trading_data_mapper=self._hl_trading_data_mapper,
            raw_ws_handler=HyperliquidWsRawMessageHandler(),
            exchange_name=self.exchange_name,
        )

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Uses the HyperliquidEip712Authenticator to prepare request components."""
        if not self._hl_authenticator:
            logger.error(
                f"[{self.exchange_name}] Attempt to call signed endpoint ({method} {path}) "
                "without configured HL authenticator."
            )
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        current_headers = self.default_headers.copy()

        try:
            auth_components: AuthenticatedRequestComponents = (
                await self._hl_authenticator.prepare_request(
                    method, path, params, data, current_headers
                )
            )
        except APIError:
            # Re-raise APIErrors from authenticator directly
            raise
        except Exception as e:
            # Wrap other exceptions as authentication failures
            logger.error(
                f"[{self.exchange_name}] Unexpected error during authentication preparation "
                f"for {method} {path}: {e}"
            )
            raise APIError(
                f"Authentication preparation failed: {e}",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        return {
            "headers": auth_components.headers,
            "params": auth_components.params,
            "data": auth_components.data,
        }

    async def _get_asset_index(self, symbol: str) -> int:
        """Fetch or retrieve from cache the asset_index for a given symbol."""
        return await self._asset_indexer.get_asset_index(symbol)

    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """
        Update rate limit information based on response headers.
        Hyperliquid does not typically provide rate limit info in standard headers.
        This is a placeholder implementation.
        """
        logger.debug(
            f"[{self.exchange_name}] _update_rate_limit_from_headers called "
            f"(no-op for Hyperliquid). Headers: {headers}, Method: {method}, Path: {path}"
        )
        pass

    def _construct_subscription_payload(self, topic: str) -> HyperliquidRawWsSubscribeRequest:
        """Construct subscription payload for the given topic.

        Args:
            topic: The WebSocket topic to subscribe to

        Returns:
            HyperliquidRawWsSubscribeRequest model

        Raises:
            ValueError: If topic format is invalid or required info is missing
            APIError: If topic is not supported by the exchange
        """
        # Delegate to the WebSocket router for payload construction
        return self._hl_ws_router.construct_subscription_payload(topic, self._wallet_address)

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """
        Handle raw WebSocket message from WebSocketManager, then route it.
        This method is called by the WebSocketManager.
        """
        # Following the pattern from BackpackAPI, directly route to _route_ws_message.
        # Add any pre-processing here if Hyperliquid requires it for common message envelopes.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Delegate WebSocket message routing to the WebSocket router."""
        await self._hl_ws_router.route_message(message, self._ws_handlers)

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Get derivative positions."""
        return await self.account_service.get_positions(symbol=symbol)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all open orders."""
        return await self.trading_service.get_open_orders(symbol=symbol)

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Get ticker information for a specific symbol."""
        return await self.market_data_service.get_ticker(symbol=symbol)

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None:
        """Get order book for a specific symbol."""
        return await self.market_data_service.get_order_book(symbol=symbol)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Get recent trades for a specific symbol."""
        return await self.market_data_service.get_recent_trades(symbol=symbol)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Get funding rates for specified symbols or all symbols."""
        return await self.market_data_service.get_funding_rates(args=args)

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get historical market data (candlesticks) for a specific symbol."""
        return await self.market_data_service.get_market_data(args=args)

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order."""
        return await self.trading_service.place_order(args)

    async def cancel_order(self, args: CancelOrderArgs) -> bool:
        """Cancel an existing order."""
        return await self.trading_service.cancel_order(args=args)

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all orders for a given symbol, or all if symbol is None."""
        return await self.trading_service.cancel_all_orders(symbol=symbol)

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Get account summary information."""
        return await self.account_service.get_account_summary()

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Fetch the status of a specific order."""
        return await self.trading_service.get_order(args=args)

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Fetch a single order by its ID."""
        return await self.trading_service.get_order(args=args)

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Get historical orders."""
        return await self.account_service.get_order_history(args=args)

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Get recent trade history.

        Args:
            args: Parameters for filtering trade history including symbol and limit.
        """
        return await self.account_service.get_trade_history(args=args)

    async def get_historical_funding_rates(
        self, args: GetHistoricalFundingRatesArgs
    ) -> list[FundingRate]:
        """Get historical funding rates for a specific symbol."""
        # Hyperliquid requires start_time
        if args.start_time is None:
            raise ValueError(
                "start_time is required for Hyperliquid.get_historical_funding_rates()"
            )
        return await self.market_data_service.get_historical_funding_rates(args=args)

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Transfer funds between account types."""
        return await self.account_service.transfer(args)

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Withdraw funds to an external address."""
        return await self.account_service.withdraw(args)

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Prepare subscription to order book updates for a symbol.
        Actual subscription with a handler is done via self.subscribe().
        """
        # Hyperliquid topic format: "l2Book:SYMBOL"
        topic = f"l2Book:{symbol}"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for order book (l2Book) topic: {topic}"
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Prepare subscription to ticker updates for a symbol.
        Hyperliquid does not have a direct per-symbol ticker stream like 'ticker.SYMBOL'.
        It uses 'allMids' for all symbols or relies on order book/trades for ticker-like data.
        This method will log a warning. Consider subscribing to 'allMids' or 'l2Book' instead.
        """
        # Hyperliquid uses "allMids" for a combined stream.
        # Individual ticker streams like "ticker:SYMBOL" are not standard for HL.
        logger.warning(
            f"[{self.exchange_name}] Hyperliquid does not have a direct 'ticker:{symbol}' stream. "
            f"Consider subscribing to 'allMids' for all mid prices, or 'l2Book:{symbol}' "
            f"and derive ticker data."
        )
        # No direct topic construction for a non-existent stream type.

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Prepare subscription to public trade updates for a symbol.
        Actual subscription with a handler is done via self.subscribe().
        """
        # Hyperliquid topic format: "trades:SYMBOL"
        topic = f"trades:{symbol}"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for public trades topic: {topic}"
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe_to_account_updates(self) -> None:
        """Prepare subscription to private account updates (fills, orders, positions).
        Actual subscription with a handler is done via self.subscribe().
        Hyperliquid uses a single 'userEvents' stream for this.
        """
        # Hyperliquid topic format for all user data: "userEvents"
        # This requires wallet_address to be known by _construct_subscription_payload
        topic = "userEvents"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for user account updates "
            f"(userEvents) topic: {topic}"
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription via WebSocketManager."""
        logger.info(
            f"[{self.exchange_name}] Subscribing to topic: {topic}. Delegating to base ExchangeAPI."
        )
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Callback for when WebSocket connects, typically to resubscribe to topics."""
        logger.info(
            f"[{self.exchange_name}] WebSocket connected. "
            f"Triggering resubscription via base ExchangeAPI."
        )
        await super()._on_ws_connected()

    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics upon WebSocket (re)connection."""
        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics. Delegating to base ExchangeAPI."
        )
        await super()._resubscribe()

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Retrieves all open orders, optionally filtered by symbol.

        Args:
            args: Parameters for filtering open orders including optional symbol.
        """
        return await self.trading_service.get_all_open_orders(args=args)
