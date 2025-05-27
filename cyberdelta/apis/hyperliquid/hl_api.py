from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

import aiohttp
from pydantic import HttpUrl

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
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
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
from cyberdelta.core.models.market.order import (
    CancelOrderResult,
    Order,
)
from cyberdelta.core.models.operations import Transfer, Withdrawal
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    BASE_URL = "https://api.hyperliquid.xyz"
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    CHAIN_ID = 1337

    account_service: HyperliquidAccountService
    trading_service: HyperliquidTradingService
    market_data_service: HyperliquidMarketDataService

    def __init__(
        self,
        api_config: dict[str, Any],
        secrets: dict[str, str | None],
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
            api_config: Configuration dictionary with connection parameters
            secrets: Dictionary containing private_key and wallet_address
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
        self.rest_endpoint = api_config.get("rest_endpoint", self.BASE_URL)
        self.ws_endpoint = api_config.get("ws_endpoint", self.WS_URL)
        self._wallet_address = secrets.get("wallet_address")

        # Create the factory to handle component instantiation
        factory = HyperliquidAPIComponentsFactory(api_config, secrets, self.CHAIN_ID)

        # Use injected components or create them via factory
        self._hl_authenticator = authenticator or factory.create_authenticator()
        self._hyperliquid_error_mapper = error_mapper or factory.create_error_mapper()
        self._hl_request_builder = request_builder or factory.create_request_builder()
        self._hl_response_handler = response_handler or factory.create_response_handler()

        # Use injected mappers or create them via factory
        self._hl_account_data_mapper = account_data_mapper or factory.create_account_data_mapper()
        self._hl_trading_data_mapper = trading_data_mapper or factory.create_trading_data_mapper()
        self._hl_market_data_mapper = market_data_mapper or factory.create_market_data_mapper()

        self.exchange_name = "hyperliquid"

        super().__init__(
            exchange_name="hyperliquid",
            config={
                "rest_endpoint": self.rest_endpoint,
                "ws_endpoint": self.ws_endpoint,
                "rate_limits": api_config.get("rate_limits", {}),
                "request_timeout": api_config.get("request_timeout", 30.0),
                "ws_ping_interval": api_config.get("ws_ping_interval"),
                "ws_reconnect_delay": api_config.get("ws_reconnect_delay"),
                "ws_max_reconnect_attempts": api_config.get("ws_max_reconnect_attempts"),
                "ws_connection_timeout": api_config.get("ws_connection_timeout"),
            },
            secrets=secrets,
            authenticator=self._hl_authenticator,
            error_mapper=self._hyperliquid_error_mapper,
        )

        # Use injected HTTP client or create one
        if http_client is not None:
            self._http_client = http_client
        else:
            http_client_raw_config = api_config.get("http_client", {})
            http_client_config = HttpClientConfig(
                rest_endpoint=HttpUrl(self.BASE_URL),
                default_request_timeout=http_client_raw_config.get("default_request_timeout", 10.0),
                max_retries=http_client_raw_config.get("max_retries", 3),
                retry_delay_seconds=http_client_raw_config.get("retry_delay_seconds", 5.0),
            )
            self._http_client = HttpClient(self.exchange_name, http_client_config)

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
            "headers": auth_components["headers"],
            "params": auth_components["params"],
            "data": auth_components["data"],
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

    def _construct_subscription_payload(self, topic: str) -> dict[str, Any] | None:
        """Delegate subscription payload construction to the WebSocket router."""
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
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_balances.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Get current positions."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_positions.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        return await self.account_service.get_positions(symbol=symbol)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Retrieves all open orders for the current user, optionally filtered by symbol."""
        return await self.trading_service.get_open_orders(symbol=symbol)

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Retrieves the latest ticker information for a specific symbol."""
        return await self.market_data_service.get_ticker(symbol)

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None:
        """Retrieves the order book for a specific symbol."""
        return await self.market_data_service.get_order_book(symbol)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Retrieves recent public trades for a specific symbol."""
        return await self.market_data_service.get_recent_trades(symbol)

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """Retrieves current funding rates for specified symbols, or all if None.
        Delegates to HyperliquidMarketDataService.
        """
        return await self.market_data_service.get_funding_rates(symbols=symbols)

    async def get_market_data(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[Candle]:
        """Retrieves historical kline/candlestick data for a symbol and timeframe."""
        # Calculate time range if not provided
        if start_time_ms is None or end_time_ms is None:
            # Import timeframe_to_ms here to avoid circular import
            import time

            from cyberdelta.utils.parsing import timeframe_to_ms

            interval_ms = timeframe_to_ms(timeframe)
            if interval_ms == 0:
                raise ValueError(f"Invalid or unsupported timeframe: {timeframe}")

            current_time_ms = int(time.time() * 1000)
            end_time_ms = end_time_ms or current_time_ms
            start_time_ms = start_time_ms or (end_time_ms - (limit * interval_ms))

        return await self.market_data_service.get_market_data(
            symbol=symbol,
            interval=timeframe,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

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
        """Places an order on the exchange."""
        if price is None and order_type != OrderType.MARKET:
            raise ValueError("Price must be specified for non-market order types.")
        if price is None and order_type == OrderType.MARKET:
            raise ValueError(
                "Hyperliquid requires a price (as limit_px for slippage) even for MARKET orders."
            )

        if price is None:
            raise APIError(
                "Price cannot be None for Hyperliquid place_order service call.",
                APIErrorCode.INVALID_REQUEST.value,
            )

        return await self.trading_service.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            stop_price=stop_price,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            post_only=post_only,
        )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancels a specific order by its ID."""
        if symbol is None:
            raise ValueError("Symbol is required to cancel an order on Hyperliquid.")
        try:
            order_id_int = int(order_id)
        except ValueError:
            logger.error(
                f"[{self.exchange_name}] Invalid order_id format for cancellation: {order_id}"
            )
            return False

        return await self.trading_service.cancel_order(symbol=symbol, order_id=order_id_int)

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancels all open orders, optionally filtered by symbol."""
        return await self.trading_service.cancel_all_orders(symbol=symbol)

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Fetches and combines account balance and positions for Hyperliquid."""
        if not self._wallet_address:
            raise APIError(
                message=(
                    f"HLAPI: Wallet address required for get_account_summary. "
                    f"Exchange: {self.exchange_name}"
                ),
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        return await self.account_service.get_account_summary()

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """Retrieves the status of a specific order by its ID."""
        if symbol is None:
            raise ValueError("Symbol is required for get_order_status on Hyperliquid.")
        try:
            order_id_int = int(order_id)
        except ValueError:
            logger.error(
                f"[{self.exchange_name}] Invalid order_id format for get_order_status: {order_id}"
            )
            return None

        order = await self.trading_service.get_order(symbol=symbol, order_id=order_id_int)
        return order

    async def get_order(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """Retrieves a specific order by its ID, returning None if not found."""
        if symbol is None:
            raise ValueError("Symbol is required for get_order on Hyperliquid.")
        try:
            order_id_int = int(order_id)
        except ValueError:
            logger.warning(
                f"[{self.exchange_name}] Invalid order_id format for get_order: {order_id}. "
                f"Returning None."
            )
            return None

        return await self.trading_service.get_order(symbol=symbol, order_id=order_id_int)

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Retrieves historical orders."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_order_history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        return await self.account_service.get_order_history(
            symbol=symbol, start_time=start_time, end_time=end_time
        )

    async def get_trade_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[Trade]:
        """Retrieves historical trades (fills)."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_trade_history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        return await self.account_service.get_trade_history(symbol=symbol)

    async def get_historical_funding_rates(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime | None = None,
    ) -> list[FundingRate]:
        """Request historical funding rates for a specific symbol and time range."""
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms: int | None = None
        if end_time is not None:
            end_time_ms = int(end_time.timestamp() * 1000)
            if end_time_ms < start_time_ms:
                raise ValueError("end_time cannot be before start_time.")

        return await self.market_data_service.get_historical_funding_rates(
            symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,  # e.g., "spot", "margin" - less relevant for HL L1<->L2
        to_account_type: str,  # e.g., "spot", "margin"
        client_transfer_id: str | None = None,
    ) -> Transfer:  # cyberdelta.core.models.operations.Transfer
        """(Not Applicable) Initiates an asset transfer between accounts.
        Hyperliquid is a DEX; transfers are typically L1 wallet deposits/withdrawals,
        not internal account-to-account transfers like on a CEX.
        """
        logger.error(
            f"[{self.exchange_name}] The 'transfer' operation as defined for CEXs "
            f"(e.g., spot to margin) is not directly applicable to Hyperliquid (DEX). "
            f"L1 deposits/withdrawals are handled differently."
        )
        raise NotImplementedError(
            f"The 'transfer' operation is not applicable to {self.exchange_name}."
        )

    async def withdraw(
        self,
        asset: str,  # For HL, this is usually implied by the L1 token
        amount: Decimal,
        address: str,  # L1 destination address
        network: str | None = None,  # L1 network, e.g., "Arbitrum"
        tag: str | None = None,  # Destination tag/memo, if applicable
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
        **kwargs: dict[str, Any],  # Changed from Any
    ) -> Withdrawal:  # cyberdelta.core.models.operations.Withdrawal
        """(Not Applicable) Initiates a withdrawal of assets from the exchange.
        Hyperliquid is a DEX; withdrawals are L1 transactions signed by the user's wallet,
        not initiated via an API call in this manner.
        """
        logger.error(
            f"[{self.exchange_name}] The 'withdraw' operation via API is not applicable "
            f"to Hyperliquid (DEX). L1 withdrawals are user-signed transactions."
        )
        raise NotImplementedError(
            f"The 'withdraw' operation is not applicable to {self.exchange_name}."
        )

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

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Retrieves all open orders, optionally filtered by symbol. Alias for get_open_orders."""
        return await self.trading_service.get_open_orders(symbol=symbol)
