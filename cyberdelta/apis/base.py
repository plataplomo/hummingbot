import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Coroutine

import aiohttp

# Assuming models are in src.core.models
# Adjust import path if structure changes
from ..core.models import (
    Order, OrderBook, Ticker, Trade, Position, Balance, FundingRate, OrderType, OrderSide
)

logger = logging.getLogger(__name__)

# Type alias for WebSocket message handlers
# The handler should accept the message (dict) and return a coroutine
MessageHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]

class APIError(Exception):
    """Custom exception for API related errors."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code

class ExchangeAPI(ABC):
    """Abstract Base Class for exchange API clients."""

    def __init__(self, exchange_name: str, api_config: Dict[str, Any], secrets: Dict[str, Optional[str]]):
        """Initialize the API client.

        Args:
            exchange_name: Name of the exchange (e.g., 'hyperliquid').
            api_config: Dictionary containing API configuration (endpoints, etc.).
            secrets: Dictionary containing API keys/secrets.
        """
        self.exchange_name = exchange_name
        self.rest_endpoint = api_config.get('rest_endpoint')
        self.ws_endpoint = api_config.get('ws_endpoint')
        self._secrets = secrets
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_handlers: Dict[str, MessageHandler] = {} # Map subscription keys/topics to handlers
        self._is_connected = False
        self._ws_listener_task: Optional[asyncio.Task] = None

        if not self.rest_endpoint:
            logger.warning(f"REST endpoint not configured for {self.exchange_name}")
        if not self.ws_endpoint:
            logger.warning(f"WebSocket endpoint not configured for {self.exchange_name}")

    @property
    def is_connected(self) -> bool:
        """Returns True if the WebSocket connection is active."""
        return self._is_connected

    async def connect(self):
        """Establish connections (e.g., create aiohttp session, connect WebSocket)."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            logger.info(f"[{self.exchange_name}] aiohttp session created.")

        if self.ws_endpoint and (self._ws_connection is None or self._ws_connection.closed):
            await self._connect_ws()
        else:
            logger.info(f"[{self.exchange_name}] WebSocket connection already established or not configured.")

    async def _connect_ws(self):
        """Connects to the WebSocket endpoint."""
        if not self.ws_endpoint or not self._session:
            logger.error(f"[{self.exchange_name}] Cannot connect WebSocket without endpoint or session.")
            return

        try:
            logger.info(f"[{self.exchange_name}] Connecting to WebSocket: {self.ws_endpoint}")
            self._ws_connection = await self._session.ws_connect(self.ws_endpoint)
            self._is_connected = True
            self._ws_listener_task = asyncio.create_task(self._ws_listener())
            logger.info(f"[{self.exchange_name}] WebSocket connected successfully.")
            # Resubscribe to topics upon reconnection if needed
            await self._resubscribe()
        except aiohttp.ClientError as e:
            logger.error(f"[{self.exchange_name}] WebSocket connection failed: {e}")
            self._is_connected = False
            self._ws_connection = None
            # Consider implementing reconnection logic here or in the caller

    async def _ws_listener(self):
        """Listens for messages on the WebSocket connection."""
        if not self._ws_connection:
            return

        logger.info(f"[{self.exchange_name}] Starting WebSocket listener.")
        try:
            async for msg in self._ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = msg.json()
                    # Find appropriate handler based on message content (e.g., topic, channel)
                    await self._route_ws_message(data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    logger.debug(f"[{self.exchange_name}] Received binary message (unhandled).")
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning(f"[{self.exchange_name}] WebSocket connection closed by peer.")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"[{self.exchange_name}] WebSocket connection error: {self._ws_connection.exception()}")
                    break
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error in WebSocket listener: {e}", exc_info=True)
        finally:
            logger.warning(f"[{self.exchange_name}] WebSocket listener stopped.")
            self._is_connected = False
            # Consider cleanup or reconnection trigger

    @abstractmethod
    async def _route_ws_message(self, message: Dict[str, Any]):
        """Route incoming WebSocket messages to appropriate handlers.
           Needs to be implemented by subclasses based on exchange-specific message formats.
        """
        pass

    @abstractmethod
    async def subscribe(self, topic: str, handler: MessageHandler):
        """Subscribe to a WebSocket topic/channel.

        Args:
            topic: The topic/channel to subscribe to (exchange-specific).
            handler: The async function to call when a message for this topic is received.
        """
        pass

    @abstractmethod
    async def _resubscribe(self):
        """Resubscribe to all previously registered topics (e.g., after reconnection)."""
        pass

    async def close(self):
        """Close connections."""
        if self._ws_listener_task and not self._ws_listener_task.done():
            self._ws_listener_task.cancel()
            try:
                await self._ws_listener_task
            except asyncio.CancelledError:
                logger.info(f"[{self.exchange_name}] WebSocket listener task cancelled.")

        if self._ws_connection and not self._ws_connection.closed:
            await self._ws_connection.close()
            logger.info(f"[{self.exchange_name}] WebSocket connection closed.")
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info(f"[{self.exchange_name}] aiohttp session closed.")
        self._is_connected = False

    # --- Abstract methods for core functionality --- #
    # These need to be implemented by each specific exchange client

    @abstractmethod
    async def get_balances(self) -> Dict[str, Balance]:
        pass

    @abstractmethod
    async def get_positions(self) -> Dict[str, Position]:
        pass

    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        pass

    @abstractmethod
    async def place_order(self, symbol: str, side: OrderSide, order_type: OrderType,
                          quantity: float, price: Optional[float] = None,
                          client_order_id: Optional[str] = None, time_in_force: Optional[Any] = None) -> Order:
        # Note: time_in_force type might vary by exchange
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Ticker:
        pass

    @abstractmethod
    async def fetch_order_book(self, symbol: str, depth: Optional[int] = None) -> OrderBook:
        pass

    @abstractmethod
    async def fetch_trades(self, symbol: str, limit: Optional[int] = None) -> List[Trade]:
        pass

    @abstractmethod
    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        pass

    @abstractmethod
    async def transfer(self, asset: str, amount: float, from_account: str, to_account: str) -> Dict[str, Any]:
        """Handle internal transfers if supported (e.g., spot <-> futures)."""
        pass

    @abstractmethod
    async def withdraw(self, asset: str, amount: float, address: str, network: Optional[str] = None) -> Dict[str, Any]:
        """Initiate a withdrawal (requires secure key handling)."""
        pass

    # Helper methods (optional, can be implemented here or in subclasses)
    async def _request(self, method: str, path: str, params: Optional[Dict] = None, data: Optional[Dict] = None, signed: bool = False) -> Dict[str, Any]:
        """Generic method to make REST requests."""
        if not self._session or not self.rest_endpoint:
            raise APIError("Session or REST endpoint not initialized.")

        url = f"{self.rest_endpoint.rstrip('/')}/{path.lstrip('/')}"
        headers = self._get_default_headers()

        if signed:
            auth_details = self._sign_request(method=method, path=path, params=params, data=data)
            headers.update(auth_details.get('headers', {}))
            params = auth_details.get('params', params)
            data = auth_details.get('data', data)

        try:
            async with self._session.request(method, url, params=params, json=data, headers=headers) as response:
                logger.debug(f"[{self.exchange_name}] Request: {method} {url} Params: {params} Data: {data}")
                response_text = await response.text()
                logger.debug(f"[{self.exchange_name}] Response Status: {response.status} Body: {response_text[:500]}") # Log truncated response
                if response.status >= 400:
                    # Attempt to parse error details if JSON, otherwise use text
                    try:
                        error_details = await response.json()
                        error_message = f"{self.exchange_name} API Error: {error_details}"
                    except aiohttp.ContentTypeError:
                        error_message = f"{self.exchange_name} API Error (Status {response.status}): {response_text}"
                    raise APIError(error_message, status_code=response.status)

                # Handle potential non-JSON success responses if necessary
                try:
                    return await response.json()
                except aiohttp.ContentTypeError:
                    logger.warning(f"[{self.exchange_name}] Non-JSON response for presumed success: {response_text[:200]}")
                    return {"raw_response": response_text} # Or handle appropriately

        except aiohttp.ClientError as e:
            logger.error(f"[{self.exchange_name}] Network or Client error for {method} {url}: {e}")
            raise APIError(f"Network error communicating with {self.exchange_name}: {e}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error during request {method} {url}: {e}", exc_info=True)
            raise APIError(f"Unexpected error: {e}")

    def _get_default_headers(self) -> Dict[str, str]:
        """Return default headers for REST requests."""
        return {'Content-Type': 'application/json'}

    @abstractmethod
    def _sign_request(self, method: str, path: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Sign the request parameters/data according to exchange requirements.
           Should return a dictionary containing potentially modified headers, params, data.
        """
        pass

    # --- WebSocket Subscription/Message Handling --- #
    # The base class provides the listener loop and connection management.
    # Subclasses need to implement the actual subscribe logic and message routing. 