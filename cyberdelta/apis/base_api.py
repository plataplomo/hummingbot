from __future__ import annotations  # Enable postponed evaluation

import asyncio
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine, Mapping
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import aiohttp
from aiohttp import ClientTimeout, ClientWSTimeout

from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
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
]

# Get logger instance for this module
logger = logging.getLogger(__name__)

# Use `type` keyword for type aliases (PEP 695)
type MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
type ParsedJsonResponse = dict[str, Any] | list[Any] | str


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
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """
        Initialize the exchange API client.

        Args:
            exchange_name: Name of the exchange (e.g., 'hyperliquid', 'backpack')
            config: Dictionary of configuration parameters including endpoints, rate limits
            secrets: Dictionary of API keys and secrets for authentication
            loop: Optional event loop for rate limiters
        """
        self.exchange_name = exchange_name
        self.config = config
        self._secrets = secrets
        self._loop = loop or asyncio.get_event_loop()
        self._session: aiohttp.ClientSession | None = None
        self._ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._is_connected = False
        self._reconnect_task: asyncio.Task[None] | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self.max_retries: int = config.get(
            "max_retries", 2
        )  # Default to 2 retries (3 total attempts)

        # Initialize RateLimiterService
        self._rate_limiter_service = RateLimiterService(
            exchange_name=self.exchange_name,
            config=self.config,  # Pass the main config dict
            loop=self._loop,
        )
        # Placeholder for connection state and WebSocket management attributes
        self._ws_connection = None
        self._ws_listener_task: asyncio.Task[None] | None = None
        self._ws_reconnect_task: asyncio.Task[None] | None = None
        self._ws_ping_task: asyncio.Task[None] | None = None
        self._is_ws_connected = False
        self._is_connecting = False
        self._should_reconnect = True

        # Extract key configurations
        # Check for both rest_endpoint (standard) and base_url (alternative naming)
        self.rest_endpoint = config.get("rest_endpoint", config.get("base_url"))
        if not self.rest_endpoint or not isinstance(self.rest_endpoint, str):
            raise ValueError(
                f"[{exchange_name}] Missing or invalid 'rest_endpoint' or 'base_url' in config"
            )
        # Check for both ws_endpoint (standard) and ws_url (alternative naming)
        self.ws_endpoint = config.get("ws_endpoint")
        if not self.ws_endpoint or not isinstance(self.ws_endpoint, str):
            logger.warning(
                f"[{exchange_name}] Missing or invalid 'ws_endpoint' in config. "
                f"WebSocket functionality disabled."
            )
            self.ws_endpoint = None  # Explicitly set to None if invalid

        # Validation
        if not self.rest_endpoint:
            logger.warning(f"REST endpoint not configured for {self.exchange_name}")
        if not self.ws_endpoint:
            logger.warning(f"WebSocket endpoint not configured for {self.exchange_name}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get the existing aiohttp ClientSession or create a new one."""
        if self._session is None or self._session.closed:
            # Consider adding timeout configurations from self.config if available
            timeout = ClientTimeout(total=self.config.get("request_timeout", 30))  # Default 30s
            self._session = aiohttp.ClientSession(timeout=timeout)
            logger.debug(f"[{self.exchange_name}] Created new aiohttp ClientSession.")
        # Ensure session is returned even if it existed but was closed
        if self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=ClientTimeout(total=self.config.get("request_timeout", 30))
            )
            logger.debug(f"[{self.exchange_name}] Recreated closed aiohttp ClientSession.")

        return self._session

    async def _get_rate_limiter(self, method: str, path: str) -> TokenBucketRateLimiterRuntime:
        """
        Get the appropriate rate limiter for the given HTTP method and path.
        Delegates to RateLimiterService.
        """
        return self._rate_limiter_service.get_limiter(method, path)

    @property
    def is_connected(self) -> bool:
        """Returns whether the WebSocket connection is active."""
        return (
            self._is_connected
            and self._ws_connection is not None
            and not self._ws_connection.closed
        )

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        endpoint_group: str | None = None,
    ) -> ParsedJsonResponse | None:
        """
        Execute an API request with comprehensive error handling and retry logic.

        Args:
            method: HTTP method ('GET', 'POST', etc.)
            endpoint: API endpoint path
            params: URL parameters for the request
            data: Request body data
            headers: HTTP headers
            is_signed: Whether the request requires authentication
            endpoint_group: Optional group for rate limiting

        Returns:
            Parsed API response (dict, list, str) or None for empty responses.

        Raises:
            APIError: If the request fails after retries
        """
        session = await self._get_session()

        if not self.rest_endpoint:
            raise APIError(
                f"REST endpoint not configured for {self.exchange_name}. Check configuration.",
                code=APIErrorCode.CONNECTION_ERROR.value,
            )

        url = f"{self.rest_endpoint.rstrip('/')}/{endpoint.lstrip('/')}"

        if is_signed:
            auth_data = await self._authenticate(method, endpoint, params, data)
            # Remove cast, accept potential Pyright warning on .get
            auth_headers = auth_data.get("headers", {})
            auth_params = auth_data.get("params", {})
            auth_data_body = auth_data.get("data", {})
            headers = {**headers, **auth_headers} if headers else auth_headers
            params = {**params, **auth_params} if params else auth_params
            data = {**data, **auth_data_body} if data else auth_data_body

        rate_limiter = await self._get_rate_limiter(method, endpoint)
        await rate_limiter.acquire()

        last_error: Exception | None = None
        attempt = -1

        for attempt in range(self.max_retries + 1):
            try:
                start_time = time.monotonic()
                async with session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    headers=headers,
                    timeout=ClientTimeout(total=30.0),
                ) as response:
                    duration = time.monotonic() - start_time
                    logger.debug(
                        f"[{self.exchange_name}] {method} {endpoint} completed in {duration:.3f}s "
                        f"with status {response.status}"
                    )

                    self._update_rate_limit_from_headers(response.headers, method, endpoint)

                    if response.status >= 400:
                        error_body_text = await response.text()
                        parsed_error_data: dict[str, Any] | None = None  # Initialize to None
                        try:
                            # Attempt to parse the error body as JSON
                            json_content: Any = json.loads(error_body_text)
                            # If parsing is successful and the result is a dictionary, use it
                            if isinstance(json_content, dict):
                                parsed_error_data = json_content
                            else:
                                # Log if valid JSON but not a dict (e.g., a list or string error)
                                logger.debug(
                                    f"[{self.exchange_name}] Error response body is valid JSON "
                                    f"but not a dict: {error_body_text}"
                                )
                        except json.JSONDecodeError:
                            # Log if the error body is not valid JSON
                            logger.debug(
                                f"[{self.exchange_name}] Error response body is not "
                                f"valid JSON: {error_body_text}"
                            )
                        # parsed_error_data is now either a dict or None

                        # error_data is now dict[str, Any] | None for the _map_error_response call
                        # DEFENSIVE CHECK: error_data could be None or dict with Any values.
                        # The _map_error_response method is typed to handle this.
                        # Pyright=[reportUnknownArgumentType] (for error_data when passed)
                        error = self._map_error_response(
                            status_code=response.status,
                            error_body=error_body_text,  # Pass the original text
                            error_data=parsed_error_data,  # Pass the dict or None
                        )

                        if error.is_retryable and attempt < self.max_retries:
                            retry_delay = error.retry_after if error.retry_after else (2**attempt)
                            logger.warning(
                                f"[{self.exchange_name}] Request failed with retryable error: "
                                f"{error}. Retrying in {retry_delay:.1f}s "
                                f"({attempt + 1}/{self.max_retries + 1})"
                            )
                            await asyncio.sleep(retry_delay)
                            continue
                        raise error

                    # Success (status < 400)
                    content_type = response.headers.get("Content-Type", "").lower()
                    if response.status == 204:  # Handle No Content specifically
                        return None
                    if "application/json" in content_type:
                        try:
                            # Use a variable with explicit ParsedJsonResponse hint
                            raw_json_data: ParsedJsonResponse = await response.json()
                            # Now check the type and return with the specific type
                            if isinstance(raw_json_data, dict):
                                return raw_json_data
                            elif isinstance(raw_json_data, list):
                                return raw_json_data
                            # Type system guarantees str here due to ParsedJsonResponse type hint
                            # and prior checks, so direct return is safe.
                            return raw_json_data
                        except aiohttp.ContentTypeError:
                            # Return raw text if parsing fails despite content-type header
                            raw_text = await response.text()
                            return raw_text
                    else:
                        # Handle non-JSON success responses (e.g., plain text, HTML)
                        raw_text = await response.text()
                        return raw_text

            except (TimeoutError, aiohttp.ClientError) as e:
                last_error = e
                error = APIError(
                    message=f"Connection error: {str(e)}",
                    code=APIErrorCode.TIMEOUT.value
                    if isinstance(e, asyncio.TimeoutError)
                    else APIErrorCode.CONNECTION_ERROR.value,
                    original_exception=e,
                )
                if attempt < self.max_retries:
                    retry_delay = 2**attempt
                    logger.warning(
                        f"[{self.exchange_name}] Request failed with connection error: {error}. "
                        f"Retrying in {retry_delay:.1f}s ({attempt + 1}/{self.max_retries + 1})"
                    )
                    await asyncio.sleep(retry_delay)
                    continue
                raise error from last_error

        # Should be unreachable if loop always raises or returns
        logger.error(
            f"[{self.exchange_name}] _request loop completed unexpectedly for {method} {endpoint}"
        )
        raise APIError(
            message=f"Request failed unexpectedly after {attempt + 1} attempts.",
            code=APIErrorCode.UNKNOWN.value,
            original_exception=last_error,
        )

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
    ) -> APIError:
        """
        Map exchange-specific error responses to standardized APIError.

        Args:
            status_code: HTTP status code
            error_body: Raw error response body
            error_data: Parsed error data (if JSON)

        Returns:
            Standardized APIError
        """
        # Attempt to parse error_body if error_data is None
        parsed_error_data: dict[str, Any] | None = error_data
        if parsed_error_data is None:
            try:
                parsed_error_data = json.loads(error_body)
                if not isinstance(parsed_error_data, dict):
                    # If parsing doesn't yield a dict, revert to None
                    # and log the unexpected structure.
                    logger.warning(
                        f"[{self.exchange_name}] Error body parsed but was not a dict: "
                        f"{type(parsed_error_data)}. Original body: {error_body[:500]}"
                    )
                    parsed_error_data = None  # Ensure it's None if not a dict
            except json.JSONDecodeError:
                logger.debug(
                    f"[{self.exchange_name}] Failed to parse error body as JSON: {error_body[:500]}"
                )
                # Keep parsed_error_data as None if JSON parsing fails

        # Default mappings based on HTTP status
        if status_code == 401:
            code = APIErrorCode.AUTHENTICATION_FAILED
        elif status_code == 403:
            code = APIErrorCode.AUTHENTICATION_FAILED
        elif status_code == 404:
            code = APIErrorCode.SYMBOL_NOT_FOUND
        elif status_code == 429:
            code = APIErrorCode.RATE_LIMITED
        elif status_code == 400:
            code = APIErrorCode.INVALID_PARAMS
        elif status_code == 500:
            code = APIErrorCode.SERVER_ERROR
        elif status_code == 503:
            code = APIErrorCode.MAINTENANCE
        elif 400 <= status_code < 500:
            code = APIErrorCode.INVALID_PARAMS
        elif 500 <= status_code < 600:
            code = APIErrorCode.SERVER_ERROR
        else:
            code = APIErrorCode.UNKNOWN

        # Extract message and exchange-specific code from error data if available
        message = "Unknown error"
        exchange_code = None
        retry_after = None

        # Check if error_data is not None before accessing
        if error_data:
            # Handle various message field names in different exchange responses
            message = (
                error_data.get("message")
                or error_data.get("msg")
                or error_data.get("error")
                or error_data.get("error_message")
                or error_data.get("description")
                or message
            )

            # Extract exchange-specific error code if present
            exchange_code = (
                error_data.get("code")
                or error_data.get("error_code")
                or error_data.get("err")
                or None
            )

            # Extract retry-after if present (rate limiting)
            retry_after = (
                error_data.get("retry_after")
                or error_data.get("retryAfter")
                or error_data.get("Retry-After")
                or None
            )

            # Try to map known error patterns to our standard codes
            if any(
                keyword in message.lower() for keyword in ["insufficient", "not enough", "balance"]
            ):
                code = APIErrorCode.INSUFFICIENT_FUNDS
            elif any(
                keyword in message.lower() for keyword in ["order", "not found", "unknown order"]
            ):
                code = APIErrorCode.ORDER_NOT_FOUND
            elif any(
                keyword in message.lower()
                for keyword in ["symbol", "instrument", "market", "not found"]
            ):
                code = APIErrorCode.SYMBOL_NOT_FOUND
            elif any(keyword in message.lower() for keyword in ["precision", "decimal"]):
                code = APIErrorCode.PRECISION_ERROR
            elif any(
                keyword in message.lower() for keyword in ["min notional", "minimum notional"]
            ):
                code = APIErrorCode.MIN_NOTIONAL_NOT_MET
            elif any(
                keyword in message.lower()
                for keyword in ["quantity", "size", "amount", "out of range"]
            ):
                code = APIErrorCode.QUANTITY_OUT_OF_RANGE
            elif any(keyword in message.lower() for keyword in ["max position", "position limit"]):
                code = APIErrorCode.MAX_POSITION_EXCEEDED
            elif any(keyword in message.lower() for keyword in ["liquidation", "liquidating"]):
                code = APIErrorCode.LIQUIDATION_IN_PROGRESS
            elif any(keyword in message.lower() for keyword in ["price", "out of range"]):
                code = APIErrorCode.PRICE_OUT_OF_RANGE
            elif any(keyword in message.lower() for keyword in ["market closed", "not open"]):
                code = APIErrorCode.MARKET_CLOSED
            elif any(keyword in message.lower() for keyword in ["duplicate", "already exists"]):
                code = APIErrorCode.DUPLICATE_ORDER
            elif any(
                keyword in message.lower()
                for keyword in ["rate limit", "ratelimit", "too many requests"]
            ):
                code = APIErrorCode.RATE_LIMITED
            elif any(keyword in message.lower() for keyword in ["maintenance", "unavailable"]):
                code = APIErrorCode.MAINTENANCE
            elif any(keyword in message.lower() for keyword in ["rejected", "cancel reject"]):
                code = APIErrorCode.ORDER_REJECTED
            elif any(keyword in message.lower() for keyword in ["funding", "rate", "unavailable"]):
                code = APIErrorCode.FUNDING_RATE_UNAVAILABLE
            elif any(
                keyword in message.lower() for keyword in ["network", "connection", "timeout"]
            ):
                code = APIErrorCode.NETWORK_ISSUE

        return APIError(
            message=f"API error: {message}",
            code=code.value,
            http_status=status_code,
            exchange_code=exchange_code,
            exchange_message=message,
            retry_after=retry_after,
        )

    async def connect(self) -> None:
        """
        Establish connections to the exchange API (REST and WebSocket).
        Must be called before making any API requests.
        """
        # Import here to avoid circular imports
        import aiohttp

        # Initialize HTTP session if needed
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            logger.info(f"[{self.exchange_name}] HTTP session created")

        # Connect WebSocket if configured
        if self.ws_endpoint and (self._ws_connection is None or self._ws_connection.closed):
            await self._connect_ws()

    async def _connect_ws(self) -> None:
        """
        Establish WebSocket connection with the exchange.
        Implements automatic reconnection and subscription recovery.
        """
        import aiohttp

        if not self.ws_endpoint or not self._session:
            logger.error(
                f"[{self.exchange_name}] Cannot connect WebSocket without endpoint or session"
            )
            return

        try:
            logger.info(f"[{self.exchange_name}] Connecting to WebSocket: {self.ws_endpoint}")

            # NOTE: The following ws_connect usage matches aiohttp's official documentation.
            # Some type checkers (e.g., Pylance, Pyright) may incorrectly flag this as an error
            # due to outdated or incomplete type stubs. This is a false positive; see:
            # https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientSession.ws_connect
            # Suppressing with pyright: ignore as this is correct and safe.
            # Mypy doesnt show this error here
            self._ws_connection = await self._session.ws_connect(
                self.ws_endpoint,
                heartbeat=30.0,
                timeout=ClientWSTimeout(30.0),  # pyright: ignore[reportCallIssue]
            )

            self._is_connected = True
            logger.info(f"[{self.exchange_name}] WebSocket connected successfully")

            # Start listener task
            if self._ws_listener_task is None or self._ws_listener_task.done():
                self._ws_listener_task = asyncio.create_task(
                    self._ws_listener(), name=f"{self.exchange_name}_ws_listener"
                )

            # Resubscribe to topics after connection
            await self._resubscribe()

        except (TimeoutError, aiohttp.ClientError) as e:
            self._is_connected = False
            self._ws_connection = None
            logger.error(f"[{self.exchange_name}] WebSocket connection failed: {e}")

            # Schedule reconnection attempt
            asyncio.create_task(self._reconnect_ws())

    async def _reconnect_ws(self, delay: float = 5.0, max_attempts: int = 10) -> None:
        """
        Attempt to reconnect WebSocket with exponential backoff.

        Args:
            delay: Initial delay before first reconnection attempt
            max_attempts: Maximum number of reconnection attempts
        """
        for attempt in range(max_attempts):
            # Calculate backoff delay with jitter
            backoff = delay * (2**attempt)
            jitter = backoff * 0.1 * (2 * (0.5 - random.random()))
            sleep_time = max(1.0, backoff + jitter)

            logger.info(
                f"[{self.exchange_name}] WebSocket reconnection attempt {attempt + 1}/"
                f"{max_attempts} scheduled in {sleep_time:.1f}s"
            )

            await asyncio.sleep(sleep_time)

            try:
                await self._connect_ws()
                # Return on successful connection
                if self.is_connected:
                    logger.info(f"[{self.exchange_name}] WebSocket reconnected successfully")
                    return
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] WebSocket reconnection attempt {attempt + 1} "
                    f"failed: {e}"
                )

        logger.critical(
            f"[{self.exchange_name}] WebSocket reconnection failed after {max_attempts} attempts"
        )

    async def _ws_listener(self) -> None:
        # Adjust type hint for e to accommodate CancelledError
        e: BaseException | None = None  # Define e outside the except block for finally
        if not self._ws_connection or self._ws_connection.closed:
            logger.error(
                f"[{self.exchange_name}] WebSocket listener started without a valid connection"
            )
            self._is_connected = False
            if self._should_reconnect and (
                self._reconnect_task is None or self._reconnect_task.done()
            ):
                self._reconnect_task = asyncio.create_task(self._reconnect_ws())
            return
        logger.info(f"[{self.exchange_name}] WebSocket listener started")
        try:
            async for msg in self._ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._handle_websocket_message(data)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"[{self.exchange_name}] Received non-JSON WebSocket message: "
                            f"{msg.data[:100]}..."
                        )
                    except Exception as e_inner:
                        logger.exception(
                            f"[{self.exchange_name}] Error processing WebSocket message: {e_inner}"
                        )
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    # Use ws_connection.exception() which returns Exception | None
                    ws_exception = self._ws_connection.exception()
                    if ws_exception:
                        e = ws_exception  # Assign if not None
                        logger.error(f"[{self.exchange_name}] WebSocket connection error: {e}")
                    else:
                        logger.error(
                            f"[{self.exchange_name}] WebSocket connection error "
                            f"reported, but exception is None."
                        )
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    logger.warning(
                        f"[{self.exchange_name}] WebSocket connection closed by server or closing."
                    )
                    break
        except asyncio.CancelledError as e_cancel:
            e = e_cancel
            logger.info(f"[{self.exchange_name}] WebSocket listener task cancelled")
        except Exception as e_outer:
            e = e_outer
            logger.exception(f"[{self.exchange_name}] Unexpected error in WebSocket listener: {e}")
        finally:
            logger.warning(f"[{self.exchange_name}] WebSocket listener stopped.")
            self._is_connected = False
            # Check type of e before deciding to reconnect
            if self._should_reconnect and not isinstance(e, asyncio.CancelledError):
                if self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect_ws())

    async def close(self) -> None:
        """
        Close all connections to the exchange.
        Should be called during application shutdown.
        """
        # Cancel WebSocket listener task
        if self._ws_listener_task and not self._ws_listener_task.done():
            self._ws_listener_task.cancel()
            try:
                await self._ws_listener_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket connection
        if self._ws_connection and not self._ws_connection.closed:
            await self._ws_connection.close()
            logger.info(f"[{self.exchange_name}] WebSocket connection closed")

        # Close HTTP session
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info(f"[{self.exchange_name}] HTTP session closed")

        self._is_connected = False

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
        """Register a handler for a specific WebSocket topic/channel."""
        raise NotImplementedError

    @abstractmethod
    async def _resubscribe(self) -> None:  # Added return type
        """Internal method to resubscribe to topics upon WebSocket reconnection."""
        raise NotImplementedError

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

    @abstractmethod
    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection."""
        raise NotImplementedError

    @abstractmethod
    async def ping_websocket(self) -> None:
        """Send a ping frame over the WebSocket connection."""
        # Provide a default implementation, allow override if needed
        if self._ws_connection and not self._ws_connection.closed:
            try:
                await self._ws_connection.ping()
                logger.debug(f"[{self.exchange_name}] Sent WebSocket ping")
            except Exception as e:
                logger.warning(f"[{self.exchange_name}] Failed to send WebSocket ping: {e}")
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot ping, WebSocket not connected or already closed."
            )

    # --- Helper Methods --- #

    @abstractmethod
    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        raise NotImplementedError
