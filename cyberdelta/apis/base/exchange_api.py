from __future__ import annotations  # Enable postponed evaluation

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine, Mapping
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import aiohttp

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
    HttpRequestFailedError,
    ParsedJsonResponse,
)
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.connectivity.ws_manager import WebSocketManager
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderSide,  # Moved back
    OrderType,  # Moved back
    TimeInForce,  # Moved back
)
from cyberdelta.core.models.market.candle import Candle

if TYPE_CHECKING:
    # Import models only needed for type hints here
    from cyberdelta.core.models.market import Candle

# Define what is explicitly exported by this module
__all__ = [
    "ExchangeAPI",
    "APIError",  # Export APIError
    "APIErrorCode",  # Export APIErrorCode (already likely used but good practice)
    "MessageHandler",
    "IErrorMapper",  # Export IErrorMapper
]

# Get logger instance for this module
logger = logging.getLogger(__name__)

# Type alias for WebSocket message handlers
# Handler receives data_payload (dict) and the full_message (dict)
MessageHandler = Callable[[dict[str, Any], dict[str, Any]], Coroutine[Any, Any, None]]


class ExchangeAPI(ABC):
    """
    Abstract base class defining the interface for all exchange API implementations.
    Provides common functionality for API request handling, rate limiting, and error management.
    """

    def __init__(
        self,
        exchange_name: str,
        config: dict[str, Any],
        secrets: dict[str, str | None],
        error_mapper: IErrorMapper,
        loop: asyncio.AbstractEventLoop | None = None,
        authenticator: IAuthenticator | None = None,
    ) -> None:
        """
        Initialize the exchange API client.

        Args:
            exchange_name: Name of the exchange (e.g., 'hyperliquid', 'backpack')
            config: Dictionary of configuration parameters including endpoints, rate limits
            secrets: Dictionary of API keys and secrets for authentication
            error_mapper: Instance of an IErrorMapper implementation.
            loop: Optional event loop for rate limiters
            authenticator: Optional authenticator instance for signed requests.
        """
        self.exchange_name = exchange_name
        self._config = config
        self._secrets = secrets
        self.error_mapper = error_mapper
        self.loop = loop if loop else asyncio.get_event_loop()
        self.authenticator = authenticator
        self._ws_handlers: dict[str, MessageHandler] = {}

        self._rate_limiter_service = RateLimiterService(
            exchange_name=self.exchange_name,
            config=self._config,
            loop=self.loop,
        )

        self.rest_endpoint = self._config.get("rest_endpoint", self._config.get("base_url"))
        if not self.rest_endpoint or not isinstance(self.rest_endpoint, str):
            raise ValueError(
                f"[{exchange_name}] Missing or invalid 'rest_endpoint' or 'base_url' in config"
            )

        self.ws_endpoint = self._config.get("ws_endpoint", self._config.get("ws_url"))
        if not self.ws_endpoint or not isinstance(self.ws_endpoint, str):
            logger.warning(
                f"[{exchange_name}] Missing or invalid 'ws_endpoint'/'ws_url' in config. "
                f"WebSocket functionality will be disabled."
            )
            self.ws_endpoint = None

        self._http_client = HttpClient(
            exchange_name=self.exchange_name,
            rest_endpoint=self.rest_endpoint,
            default_request_timeout=self._config.get("request_timeout", 30.0),
            max_retries=self._config.get("max_retries"),
            retry_delay_seconds=self._config.get("retry_delay_seconds"),
        )

        self._ws_manager: WebSocketManager | None = None
        if self.ws_endpoint:
            self._ws_manager = WebSocketManager(
                exchange_name=self.exchange_name,
                ws_url=self.ws_endpoint,
                message_handler=self._handle_websocket_message,
                on_connected_callback=self._on_ws_connected,
                ping_interval=self._config.get("ws_ping_interval"),
                reconnect_delay=self._config.get("ws_reconnect_delay"),
                max_reconnect_attempts=self._config.get("ws_max_reconnect_attempts"),
                connection_timeout=self._config.get("ws_connection_timeout"),
            )

        logger.info(
            f"[{self.exchange_name}] API initialized. REST: {self.rest_endpoint}, "
            f"WS: {self.ws_endpoint}"
        )

    @property
    def is_connected(self) -> bool:
        """Returns whether the WebSocket connection is active via WebSocketManager."""
        return self._ws_manager.is_connected if self._ws_manager else False

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        endpoint_group: str | None = None,
        is_public_info_endpoint: bool = False,
    ) -> ParsedJsonResponse | None:
        """
        Execute an API request, delegating to HttpClient and handling exchange-specific
        error mapping.

        Args:
            method: HTTP method ('GET', 'POST', etc.)
            endpoint: API endpoint path (relative to base) or full URL if handled by concrete API.
            params: URL parameters for the request.
            data: Request body data.
            headers: HTTP headers.
            is_signed: Whether the request requires authentication.
            endpoint_group: Optional logical group for the endpoint.
            is_public_info_endpoint: Flag for specific endpoints (e.g. Hyperliquid INFO).

        Returns:
            Parsed API response content (JSON dict/list, raw text) or None
            for empty responses (204).

        Raises:
            APIError: For mapped exchange-specific errors or unrecoverable issues.
        """
        path_for_http_client: str
        request_path_for_mapper: str  # For error mapping context

        # Hyperliquid specific: if endpoint is full URL for /info, it bypasses _http_client
        # This logic should ideally be within HyperliquidAPI or HttpClient should handle full URLs.
        # For now, assume if it's a full URL, the concrete API handles it or this isn't
        # the method called.
        # Let's assume `endpoint` is typically a path.
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            # If endpoint is a full URL, this method might be called by a concrete API that
            # wants to use the common error handling but not the standard _http_client
            # path construction.
            # In this case, path_for_http_client might not be used directly if the call
            # is different.
            path_for_http_client = (
                endpoint  # This implies HttpClient can take full URLs, or this path won't be used
            )
            request_path_for_mapper = endpoint
            # This scenario (full URL to _request) suggests a need for more flexible HttpClient
            # or direct calling.
            # For now, we assume _http_client.request is called with a *relative* path.
            # If Hyperliquid calls _request with a full INFO_URL, it must handle it differently
            # (e.g., not using self._http_client, or _http_client needs to support it)
            # Given the current HttpClient design, it expects endpoint_path.
            # Let's clarify that `endpoint` to this method is typically a relative path.
            # Hyperliquid API needs to ensure its calls to /info are handled appropriately.
            # For now, we'll assume if it's a full URL, it is passed to the mapper correctly.
            # And path_for_http_client will likely be the relative part if extracted.
            # This part is tricky. For now, if full URL, we set path_for_http_client to it, but
            # it may not be what HttpClient expects if it strictly prepends its own base_url.
            # Let's assume `endpoint` is always a relative path for calls going through
            # `_http_client`.
            # If a concrete API calls this `_request` with a full URL, it implies it has its own
            # HTTP execution logic
            # but wants to use this method for error mapping and rate limit header updates.
            # This case needs to be very carefully handled by the concrete API.
            # A safer assumption: `endpoint` here is always a relative path when `_http_client`
            # is used.
            logger.warning(
                f"[{self.exchange_name}] _request called with full URL endpoint '{endpoint}'. "
                f"Ensure HTTP execution logic is appropriate. HttpClient expects a relative path."
            )
            # Attempt to derive a relative path if possible, otherwise use full.
            if self.rest_endpoint and endpoint.startswith(self.rest_endpoint):
                path_for_http_client = endpoint.replace(self.rest_endpoint, "", 1).lstrip("/")
            else:
                path_for_http_client = endpoint  # Fallback, but HttpClient might misbehave.
        else:
            path_for_http_client = endpoint.lstrip("/")
            request_path_for_mapper = f"/{path_for_http_client}"

        effective_authenticator = self.authenticator if is_signed else None

        response_content: ParsedJsonResponse | str | None = None
        response_headers_dict: Mapping[str, str] = {}

        try:
            # HttpClient.request returns a tuple: (content, processed_headers, raw_headers)
            content, _processed_headers, raw_headers_multidict = await self._http_client.request(
                method=method,
                endpoint_path=path_for_http_client,  # This must be a relative path
                rate_limiter_service=self._rate_limiter_service,
                authenticator=effective_authenticator,
                params=params,
                data=data,
                headers=headers,
                is_signed=is_signed,  # Pass is_signed to HttpClient
            )
            response_content = content
            # Assign the raw headers for rate limit processing
            response_headers_dict = raw_headers_multidict
            self._update_rate_limit_from_headers(
                response_headers_dict, method, path_for_http_client
            )
            return response_content

        except HttpRequestFailedError as e_http_failed:
            logger.warning(
                f"[{self.exchange_name}] HTTP request failed for {method} "
                f"{request_path_for_mapper}: Status={e_http_failed.http_status}, "
                f"Body='{e_http_failed.exchange_message}'"
            )
            # Error is already HttpRequestFailedError (subclass of APIError)
            # We need to map its *contents* using the exchange-specific mapper
            parsed_error_data: dict[str, Any] | None = None
            if e_http_failed.exchange_message:
                try:
                    parsed_error_data = json.loads(e_http_failed.exchange_message)
                    if not isinstance(parsed_error_data, dict):
                        parsed_error_data = None  # Only use if it's a dict
                except json.JSONDecodeError:
                    pass  # Keep as None

            # Delegate to the new error_mapper instance
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=e_http_failed.http_status or 500,  # Ensure status_code is int
                error_body=e_http_failed.exchange_message or "",
                error_data=parsed_error_data,
                request_path=request_path_for_mapper,
            )
            # Preserve original exception if map_exchange_error doesn't already do it
            # (current IErrorMapper signature doesn't take original_exception)
            # APIError constructor should take it.
            # We can raise the mapped_error from e_http_failed to keep context.
            raise mapped_error from e_http_failed

        except (TimeoutError, aiohttp.ClientError) as e_client:
            # These are already raised by HttpClient after its retries
            logger.error(
                f"[{self.exchange_name}] Unrecoverable client error for {method} "
                f"[{self.exchange_name}] Unrecoverable client error for {method} "
                f"{request_path_for_mapper}: {e_client}"
            )
            # Map to a generic APIError
            # Here, we don't have a specific exchange error body, so pass what we have.
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=503,  # Service Unavailable or similar for network issues
                error_body=str(e_client),
                error_data=None,
                request_path=request_path_for_mapper,
            )
            raise mapped_error from e_client

        except APIError:  # Re-raise APIErrors (e.g. from authenticator)
            raise
        except Exception as e_unhandled:
            logger.exception(
                f"[{self.exchange_name}] Unhandled exception during request {method} "
                f"{request_path_for_mapper}: {e_unhandled}"
            )
            # Map to a generic unknown APIError
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=500,  # Internal Server Error equivalent
                error_body=str(e_unhandled),
                error_data=None,
                request_path=request_path_for_mapper,
            )
            raise mapped_error from e_unhandled

    @abstractmethod
    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """
        Update rate limit information based on response headers.
        This allows dynamic adaptation to exchange-reported limits.

        Args:
            headers: Response headers
            method: HTTP method used
            path: API endpoint path
        """
        # This is a base implementation - exchange-specific classes should override
        # to handle their specific rate limit header formats
        pass

    def _map_error_response(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None,
        request_path: str | None = None,
    ) -> APIError:
        """
        Map exchange-specific error responses to a standardized APIError object.
        This method now delegates to the configured `self.error_mapper`.

        Args:
            status_code: HTTP status code from the response.
            error_body: Raw error response body as a string.
            error_data: Parsed error data dictionary from the response, if available.
            request_path: The API endpoint path that was called (for diagnostic metadata).

        Returns:
            APIError: A fully populated `APIError` exception object.
        """
        if not self.error_mapper:
            # This should not happen if __init__ forces error_mapper
            logger.error(
                f"[{self.exchange_name}] Error mapper not configured. "
                f"Falling back to generic error."
            )
            return APIError(
                message=f"Exchange error (mapper not configured): {error_body}",
                code=APIErrorCode.UNKNOWN.value,
                http_status=status_code,
            )
        return self.error_mapper.map_exchange_error(
            status_code=status_code,
            error_body=error_body,
            error_data=error_data,
            request_path=request_path,
        )

    async def close(self) -> None:
        """Close all connections, including HTTP client and WebSocket manager."""
        logger.info(f"[{self.exchange_name}] Initiating shutdown sequence...")

        if self._http_client:
            await self._http_client.close_session()
            logger.info(f"[{self.exchange_name}] HTTP client session closed.")

        if self._ws_manager:
            await self._ws_manager.close()
            logger.info(f"[{self.exchange_name}] WebSocket manager closed.")

    # --- Abstract Methods for Exchange API Implementation --- #

    @abstractmethod
    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Internal method to generate authentication headers/parameters for signed requests.
        Concrete ExchangeAPI implementations will use their specific IAuthenticator here.
        """
        raise NotImplementedError("ExchangeAPI._authenticate must be implemented by subclasses.")

    @abstractmethod
    async def _route_ws_message(self, message: dict[str, Any]) -> None:  # Added return type
        """Internal method to route incoming WebSocket messages to appropriate handlers."""
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self, topic: str, handler: MessageHandler) -> None:  # Added return type
        """Register a handler for a specific WebSocket topic/channel and send subscription."""
        self._ws_handlers[topic] = handler
        if self._ws_manager and self.is_connected:
            subscription_payload = self._construct_subscription_payload(topic)
            if subscription_payload:
                await self._ws_manager.send_json(subscription_payload)
                logger.info(f"[{self.exchange_name}] Sent subscription request for topic: {topic}")
            else:
                logger.warning(
                    f"[{self.exchange_name}] Could not construct subscription payload "
                    f"for {topic}. Not subscribing."
                )
        elif self._ws_manager:
            logger.warning(
                f"[{self.exchange_name}] WebSocket not connected. Subscription to {topic} "
                f"will be attempted upon connection."
            )
        else:
            logger.error(
                f"[{self.exchange_name}] WebSocket manager not initialized. "
                f"Cannot subscribe to {topic}."
            )

    @abstractmethod
    def _construct_subscription_payload(self, topic: str) -> dict[str, Any] | None:
        """Helper method to construct exchange-specific subscription payload."""
        raise NotImplementedError

    @abstractmethod
    async def _on_ws_connected(self) -> None:
        """Callback executed by WebSocketManager after a successful connection."""
        logger.info(
            f"[{self.exchange_name}] WebSocket connected, attempting to resubscribe to topics."
        )
        await self._resubscribe()

    @abstractmethod
    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics after (re)connection."""
        if not self._ws_handlers:
            logger.info(f"[{self.exchange_name}] No topics to resubscribe to.")
            return

        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics: {list(self._ws_handlers.keys())}"
        )
        if self._ws_manager and self.is_connected:
            for topic, _handler in self._ws_handlers.copy().items():
                subscription_payload = self._construct_subscription_payload(topic)
                if subscription_payload:
                    success = await self._ws_manager.send_json(subscription_payload)
                    if success:
                        logger.info(
                            f"[{self.exchange_name}] Successfully re-sent subscription for {topic}."
                        )
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Failed to re-send subscription for {topic}."
                        )
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Could not construct resubscription "
                        f"payload for {topic}."
                    )
                await asyncio.sleep(0.1)
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot resubscribe, WebSocket not connected "
                f"or manager not available."
            )

    @abstractmethod
    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Internal handler to process raw WebSocket messages.

        Subclasses should implement this to perform initial parsing,
        authentication checks (if applicable to WS), and routing before
        potentially calling _route_ws_message or directly invoking handlers.
        """
        raise NotImplementedError

    # --- Core Data Fetching --- #

    @abstractmethod
    async def get_ticker(self, symbol: str) -> Ticker:
        """Fetch the latest ticker information for a symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Fetch the order book for a symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """Fetch historical funding rates for specific symbols or all symbols if None."""
        raise NotImplementedError

    @abstractmethod
    async def get_market_data(
        self, symbol: str, timeframe: str, limit: int = 100
    ) -> list[Candle]:  # Type hint should now work
        """Fetch historical market data (OHLCV/Kline) for a specific symbol and timeframe."""
        raise NotImplementedError

    # --- Account Information --- #

    @abstractmethod
    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Fetch current open positions, optionally filtered by symbol."""
        raise NotImplementedError

    # --- Order Management --- #

    @abstractmethod
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
        """Place a new order on the exchange."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order by its ID. Returns True if successful."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        """Cancel all orders for a given symbol, or all if symbol is None.

        Args:
            symbol: The trading symbol (optional, if None cancels all orders).

        Raises:
            APIError: If the API returns an error.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all currently open orders, optionally filtered by symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Fetch historical orders.

        Optionally filter by symbol, time range, limit, order ID, or client order ID.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetch historical trade data (account fills)."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """
        Fetch the current status of a specific order by its ID or client_order_id.
        At least one of order_id or client_order_id should be provided by implementations.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """
        Fetch a single order by its ID, potentially specific to a symbol.
        Returns None if the order is not found.
        """
        raise NotImplementedError

    # --- WebSocket Management & Subscriptions --- #

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection via WebSocketManager."""
        if self._ws_manager:
            await self._ws_manager.connect()
        else:
            logger.warning(
                f"[{self.exchange_name}] WebSocket endpoint not configured. "
                f"Cannot connect WebSocket."
            )

    async def ping_websocket(self) -> None:
        """Send a WebSocket ping.
        Default WebSocketManager handles standard pings automatically if ping_interval > 0.
        This method can be used for custom application-level pings if required by the exchange.
        """
        if self._ws_manager and self.is_connected:  # Check is_connected for active session
            # Custom ping logic would go here if needed, e.g., sending a specific JSON message
            # For now, log that standard ping is handled by WebSocketManager
            logger.debug(
                f"[{self.exchange_name}] Standard WebSocket ping is handled by WebSocketManager "
                f"if configured. Call this for custom pings."
            )
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot send custom ping, WebSocket not connected "
                f"or manager not available."
            )

    # --- Helper Methods --- #

    @abstractmethod
    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        raise NotImplementedError
