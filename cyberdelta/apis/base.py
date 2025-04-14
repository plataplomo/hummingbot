from __future__ import annotations  # Enable postponed evaluation

import asyncio
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine, Mapping
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

import aiohttp
from aiohttp import ClientTimeout, ClientWSTimeout

# Assuming models are in src.core.models
# Adjust import path if structure changes
# Moved under TYPE_CHECKING to break circular import
# from ..core.models import (
#     Balance,
#     FundingRate,
#     MarketData,
#     Order,
#     OrderBook,
#     OrderSide,
#     OrderType,
#     Position,
#     Ticker,
#     TimeInForce,
#     Trade,
# )

# Import Enums needed at runtime outside TYPE_CHECKING
# from ..core.models import OrderSide, OrderType, TimeInForce # REMOVING THIS RUNTIME IMPORT

if TYPE_CHECKING:
    # Import other models only needed for type hints here
    from ..core.models import (
        Balance,
        FundingRate,
        MarketData,
        Order,
        OrderBook,
        OrderSide,  # Moved back
        OrderType,  # Moved back
        Position,
        Ticker,
        TimeInForce,  # Moved back
        Trade,
    )

logger = logging.getLogger(__name__)

# Type hint for WebSocket message handlers
MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class APIErrorCode(Enum):
    """
    Standard error codes for API failures to allow consistent handling across exchanges.
    These codes abstract the exchange-specific error codes into a common format.
    """

    UNKNOWN = 0
    AUTHENTICATION_FAILED = 1
    INSUFFICIENT_FUNDS = 2
    RATE_LIMITED = 3
    ORDER_NOT_FOUND = 4
    SYMBOL_NOT_FOUND = 5
    INVALID_PARAMS = 6
    SERVER_ERROR = 7
    CONNECTION_ERROR = 8
    TIMEOUT = 9
    MAINTENANCE = 10
    # New error codes for exchange-specific failures
    ORDER_REJECTED = 11
    PRICE_OUT_OF_RANGE = 12
    MARKET_CLOSED = 13
    DUPLICATE_ORDER = 14
    MIN_NOTIONAL_NOT_MET = 15
    MAX_POSITION_EXCEEDED = 16
    LIQUIDATION_IN_PROGRESS = 17
    FUNDING_RATE_UNAVAILABLE = 18
    EXCHANGE_SPECIFIC = 19  # For truly exchange-specific errors that don't fit other categories
    NETWORK_ISSUE = 20  # Specific network connectivity issues (DNS, routing, etc.)
    QUANTITY_OUT_OF_RANGE = 21  # For min/max quantity violations
    PRECISION_ERROR = 22  # When price/quantity doesn't match required precision
    INVALID_REQUEST = 23  # Added for non-retryable client errors (like bad params)


class APIError(Exception):
    """
    Custom exception for API-related errors with enhanced context information
    to enable better error handling and recovery mechanisms.
    """

    def __init__(
        self,
        message: str,
        code: APIErrorCode = APIErrorCode.UNKNOWN,
        http_status: int | None = None,
        exchange_code: str | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        # Comprehensive error information for debugging and recovery
        self.message = message
        self.code = code
        self.http_status = http_status
        self.exchange_code = exchange_code
        self.exchange_message = exchange_message
        self.retry_after = retry_after
        self.original_exception = original_exception

        # Construct the full message with all available context
        full_message = f"{message}"
        if http_status:
            full_message += f" (HTTP {http_status})"
        if exchange_code:
            full_message += f" [Exchange code: {exchange_code}]"
        if exchange_message:
            full_message += f": {exchange_message}"
        if retry_after:
            full_message += f" - Retry after {retry_after}s"

        super().__init__(full_message)

    @property
    def is_retryable(self) -> bool:
        """
        Determines if this error can be retried based on its nature.
        Rate limits, timeouts and some server errors can be retried.
        """
        return (
            self.code
            in (
                APIErrorCode.RATE_LIMITED,
                APIErrorCode.TIMEOUT,
                APIErrorCode.CONNECTION_ERROR,
            )
            or (
                self.code == APIErrorCode.SERVER_ERROR
                and self.http_status
                and 500 <= self.http_status < 600
            )
            or self.code == APIErrorCode.NETWORK_ISSUE
        )


class RateLimiter:
    """
    Token bucket rate limiter for API request management.
    Ensures compliance with exchange rate limits while maximizing throughput.
    """

    def __init__(self, rate: float, bucket_size: int) -> None:
        """
        Initialize a rate limiter with given constraints.

        Args:
            rate: Maximum requests per second
            bucket_size: Maximum burst capacity
        """
        self.rate = rate  # tokens per second
        self.bucket_size = bucket_size  # maximum bucket capacity
        self.tokens: float = float(bucket_size)  # current token count, start with full bucket
        self.last_refill: float = time.monotonic()  # timestamp of last token refill
        self.lock = asyncio.Lock()  # thread safety for token management

    async def acquire(self) -> float:
        """
        Acquire a token for making a request, waiting if necessary.

        Returns:
            wait_time: Time waited in seconds (0 if no wait)
        """
        wait_time = 0.0

        async with self.lock:
            # Refill tokens based on time elapsed
            now = time.monotonic()
            elapsed = now - self.last_refill
            new_tokens = elapsed * self.rate

            if new_tokens > 0:
                self.tokens = min(self.bucket_size, self.tokens + new_tokens)
                self.last_refill = now

            # If no tokens available, calculate wait time
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate

                # Release lock during wait
                self.lock.release()
                try:
                    await asyncio.sleep(wait_time)
                finally:
                    # Reacquire lock after wait
                    await self.lock.acquire()

                # Refresh token count after waiting
                now = time.monotonic()
                elapsed = now - self.last_refill
                new_tokens = elapsed * self.rate
                self.tokens = min(self.bucket_size, self.tokens + new_tokens)
                self.last_refill = now

            # Consume one token
            self.tokens -= 1

            return wait_time


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
    ) -> None:
        """
        Initialize the exchange API client.

        Args:
            exchange_name: Name of the exchange (e.g., 'hyperliquid', 'backpack')
            config: Dictionary of configuration parameters including endpoints, rate limits
            secrets: Dictionary of API keys and secrets for authentication
        """
        self.exchange_name = exchange_name
        self.config = config
        self._secrets = secrets

        # Extract key configurations
        # Check for both rest_endpoint (standard) and base_url (alternative naming)
        self.rest_endpoint = config.get("rest_endpoint", config.get("base_url"))
        # Check for both ws_endpoint (standard) and ws_url (alternative naming)
        self.ws_endpoint = config.get("ws_endpoint", config.get("ws_url"))

        # Initialize HTTP session
        self._session: aiohttp.ClientSession | None = None

        # Initialize WebSocket
        self._ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._ws_listener_task: asyncio.Task | None = None
        self._is_connected = False  # WebSocket connection state

        # Set up rate limiters
        self._setup_rate_limiters(config.get("rate_limits", {}))

        # Validation
        if not self.rest_endpoint:
            logger.warning(f"REST endpoint not configured for {self.exchange_name}")
        if not self.ws_endpoint:
            logger.warning(f"WebSocket endpoint not configured for {self.exchange_name}")

    def _setup_rate_limiters(self, rate_limit_config: dict[str, Any]) -> None:
        """
        Set up rate limiters based on configuration.

        Args:
            rate_limit_config: Dictionary containing rate limit configuration
        """
        # Simplified: Using a single default limiter for now.
        # Can be expanded later to support per-endpoint or per-resource limits
        # based on the structure of rate_limit_config.

        # Default rate limiter
        default_rate = rate_limit_config.get("default_rate", 1.0)
        default_bucket = rate_limit_config.get("default_bucket", 5)

        self._default_limiter = RateLimiter(default_rate, default_bucket)

        # Endpoint-specific rate limiters
        self._endpoint_limiters = {}
        endpoints = rate_limit_config.get("endpoints", {})

        for endpoint, config in endpoints.items():
            rate = config.get("rate", default_rate)
            bucket = config.get("bucket", default_bucket)
            self._endpoint_limiters[endpoint] = RateLimiter(rate, bucket)

    async def _get_rate_limiter(self, method: str, path: str) -> RateLimiter:
        """
        Get the appropriate rate limiter for a specific API endpoint.

        Args:
            method: HTTP method (e.g., 'GET', 'POST')
            path: API endpoint path

        Returns:
            The rate limiter to use for this request
        """
        endpoint_key = f"{method}:{path}"

        # Try exact match first
        if endpoint_key in self._endpoint_limiters:
            return self._endpoint_limiters[endpoint_key]

        # Try method-only match
        method_key = f"{method}:*"
        if method_key in self._endpoint_limiters:
            return self._endpoint_limiters[method_key]

        # Try path-only match
        for pattern, limiter in self._endpoint_limiters.items():
            if pattern.endswith("*") and path.startswith(pattern[:-1]):
                return limiter

        # Default limiter as fallback
        return self._default_limiter

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
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        signed: bool = False,
        retry_count: int = 3,
        timeout: float = 30.0,
    ) -> dict[str, Any] | list[Any] | str | None:
        """
        Execute an API request with comprehensive error handling and retry logic.

        Args:
            method: HTTP method ('GET', 'POST', etc.)
            path: API endpoint path
            params: URL parameters for the request
            data: Request body data
            headers: HTTP headers
            signed: Whether the request requires authentication
            retry_count: Maximum number of retry attempts
            timeout: Request timeout in seconds

        Returns:
            Parsed API response

        Raises:
            APIError: If the request fails after retries
        """
        if not self._session:
            raise APIError(
                "HTTP session not initialized. Call connect() first.",
                code=APIErrorCode.CONNECTION_ERROR,
            )

        # Check for missing endpoint configuration
        if not self.rest_endpoint:
            raise APIError(
                f"REST endpoint not configured for {self.exchange_name}. Check configuration.",
                code=APIErrorCode.CONNECTION_ERROR,
            )

        # Construct full URL
        url = f"{self.rest_endpoint.rstrip('/')}/{path.lstrip('/')}"

        # Handle authentication if required
        if signed:
            auth_data = await self._authenticate(method, path, params, data)
            # Merge auth data into request parameters
            headers = (
                {**headers, **auth_data.get("headers", {})}
                if headers
                else auth_data.get("headers", {})
            )
            params = (
                {**params, **auth_data.get("params", {})} if params else auth_data.get("params", {})
            )
            data = {**data, **auth_data.get("data", {})} if data else auth_data.get("data", {})

        # Apply rate limiting
        rate_limiter = await self._get_rate_limiter(method, path)
        await rate_limiter.acquire()

        last_error = None

        # Retry loop
        for attempt in range(retry_count):
            try:
                # Record request start time for logging/monitoring
                start_time = time.monotonic()

                # Execute the request
                async with self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    headers=headers,
                    timeout=ClientTimeout(total=timeout),
                ) as response:
                    # Calculate request duration
                    duration = time.monotonic() - start_time

                    # Log request details at DEBUG level
                    logger.debug(
                        f"[{self.exchange_name}] {method} {path} completed in {duration:.3f}s "
                        f"with status {response.status}"
                    )

                    # Store rate limit information for future adjustments
                    self._update_rate_limit_from_headers(response.headers, method, path)

                    # Handle HTTP errors
                    if response.status >= 400:
                        error_body = await response.text()
                        error_data = {}

                        # Try to parse error response as JSON
                        try:
                            error_data = json.loads(error_body)
                        except json.JSONDecodeError:
                            # Handle non-JSON error responses
                            pass

                        # Map exchange-specific error to our standard format
                        error = self._map_error_response(
                            status_code=response.status,
                            error_body=error_body,
                            error_data=error_data,
                        )

                        # Handle retryable errors
                        if error.is_retryable and attempt < retry_count - 1:
                            # Determine retry delay with exponential backoff
                            retry_delay = error.retry_after if error.retry_after else (2**attempt)

                            logger.warning(
                                f"[{self.exchange_name}] Request failed with retryable error: {error}. "
                                f"Retrying in {retry_delay:.1f}s ({attempt + 1}/{retry_count})"
                            )

                            await asyncio.sleep(retry_delay)
                            continue

                        # Non-retryable error or max retries exceeded
                        raise error

                    # Handle successful response
                    resp_text = await response.text()

                    # Try to parse as JSON
                    if resp_text:
                        try:
                            # Cast the result to inform Mypy it matches the expected types
                            parsed_json = json.loads(resp_text)
                            return cast(dict[str, Any] | list[Any], parsed_json)
                        except json.JSONDecodeError:
                            # Return raw text if not JSON
                            return resp_text

                    # Empty response
                    return {}

            except (TimeoutError, aiohttp.ClientError) as e:
                # Handle network/timeout errors
                last_error = e

                error = APIError(
                    message=f"Connection error: {str(e)}",
                    code=APIErrorCode.TIMEOUT
                    if isinstance(e, asyncio.TimeoutError)
                    else APIErrorCode.CONNECTION_ERROR,
                    original_exception=e,
                )

                # Only retry on network errors if attempts remain
                if attempt < retry_count - 1:
                    retry_delay = 2**attempt  # Exponential backoff

                    logger.warning(
                        f"[{self.exchange_name}] Request failed with connection error: {error}. "
                        f"Retrying in {retry_delay:.1f}s ({attempt + 1}/{retry_count})"
                    )

                    await asyncio.sleep(retry_delay)
                    continue

                # Max retries exceeded
                raise error from last_error  # Chain the original connection/timeout error

        # This should typically not be reached due to the raise in the loop,
        # but as a fallback in case of unexpected flow:
        if last_error:
            raise APIError(
                message=f"Request failed after {retry_count} attempts: {str(last_error)}",
                code=APIErrorCode.UNKNOWN,
                original_exception=last_error,
            )

        raise APIError(
            message=f"Request failed after {retry_count} attempts due to unknown error",
            code=APIErrorCode.UNKNOWN,
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
        self, status_code: int, error_body: str, error_data: dict[str, Any]
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
        # This is a base implementation - exchange-specific classes should override
        # to handle their specific error response formats

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

        if isinstance(error_data, dict):
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
            code=code,
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

            self._ws_connection = await self._session.ws_connect(
                self.ws_endpoint,
                timeout=ClientWSTimeout(30.0),  # Use ClientWSTimeout object
                heartbeat=30.0,  # Enable heartbeat to detect disconnects
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
                f"[{self.exchange_name}] WebSocket reconnection attempt {attempt + 1}/{max_attempts} "
                f"scheduled in {sleep_time:.1f}s"
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
                    f"[{self.exchange_name}] WebSocket reconnection attempt {attempt + 1} failed: {e}"
                )

        logger.critical(
            f"[{self.exchange_name}] WebSocket reconnection failed after {max_attempts} attempts"
        )

    async def _ws_listener(self) -> None:
        """
        Main WebSocket message handling loop.
        Processes incoming messages and routes them to appropriate handlers.
        """
        import aiohttp

        if not self._ws_connection:
            logger.error(f"[{self.exchange_name}] WebSocket listener started without connection")
            return

        logger.info(f"[{self.exchange_name}] WebSocket listener started")

        try:
            async for msg in self._ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    # Parse JSON message
                    try:
                        data = json.loads(msg.data)
                        # Process and route message to appropriate handler
                        await self._route_ws_message(data)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"[{self.exchange_name}] Received non-JSON WebSocket message: {msg.data[:100]}..."
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.exchange_name}] Error processing WebSocket message: {e}",
                            exc_info=True,
                        )

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    logger.debug(f"[{self.exchange_name}] Received binary WebSocket message")
                    # Some exchanges use binary messages - subclasses can override _route_ws_message
                    # to handle these appropriately

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(
                        f"[{self.exchange_name}] WebSocket connection error: {self._ws_connection.exception()}"
                    )
                    break

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning(f"[{self.exchange_name}] WebSocket connection closed by server")
                    break

                elif msg.type == aiohttp.WSMsgType.CLOSING:
                    logger.info(f"[{self.exchange_name}] WebSocket connection closing")
                    break

        except asyncio.CancelledError:
            # Task cancellation is expected during shutdown
            logger.info(f"[{self.exchange_name}] WebSocket listener task cancelled")
            raise

        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in WebSocket listener: {e}",
                exc_info=True,
            )

        finally:
            # Mark connection as closed
            self._is_connected = False
            logger.warning(f"[{self.exchange_name}] WebSocket listener stopped")

            # Schedule reconnection if not during shutdown
            try:
                if self._session is not None and not self._session.closed:
                    asyncio.create_task(self._reconnect_ws())
            except Exception:
                pass

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
        """Internal method to generate authentication headers/parameters for signed requests."""
        raise NotImplementedError

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
    ) -> list[MarketData]:
        """Fetch historical market data (OHLCV/Kline) for a specific symbol and timeframe."""
        raise NotImplementedError

    # --- Account Information --- #

    @abstractmethod
    async def get_balances(self) -> dict[str, Balance] | list[Balance | dict[str, Any]] | None:
        """Fetch account balances for all assets."""
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self, symbol: str | None = None) -> list[Position]:
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
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """Place a new order on the exchange."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        """Cancel an existing order by its ID."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        """Cancel all open orders, optionally filtered by symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all currently open orders, optionally filtered by symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_history(self, symbol: str | None = None, limit: int = 100) -> list[Order]:
        """Fetch historical order data."""
        raise NotImplementedError

    @abstractmethod
    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetch historical trade data (account fills)."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_status(self, order_id: str, symbol: str | None = None) -> Order:
        """Fetch the current status of a specific order by its ID."""
        raise NotImplementedError

    # --- WebSocket Management & Subscriptions --- #

    @abstractmethod
    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection."""
        raise NotImplementedError

    @abstractmethod
    async def _handle_websocket_message(self, message: Any) -> None:
        """Internal handler to process raw WebSocket messages."""
        raise NotImplementedError

    @abstractmethod
    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Subscribe to ticker updates for a symbol."""
        raise NotImplementedError

    @abstractmethod
    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Subscribe to order book updates for a symbol."""
        raise NotImplementedError

    @abstractmethod
    async def subscribe_to_trades(self, symbol: str) -> None:
        """Subscribe to public trade updates for a symbol."""
        raise NotImplementedError

    @abstractmethod
    async def subscribe_to_account_updates(self) -> None:
        """Subscribe to private account updates (balances, positions)."""
        raise NotImplementedError

    @abstractmethod
    async def ping_websocket(self) -> None:
        """Send a ping frame over the WebSocket connection."""
        raise NotImplementedError

    # --- WebSocket Message Parsing Helpers --- #

    @abstractmethod
    def get_message_type(self, message: dict[str, Any]) -> str:
        """Determine the type of a received WebSocket message."""
        raise NotImplementedError

    @abstractmethod
    def parse_ticker(self, data: dict[str, Any], symbol: str) -> Ticker:
        """Parse raw ticker data into a Ticker object."""
        raise NotImplementedError

    @abstractmethod
    def parse_order_book(self, data: dict[str, Any], symbol: str) -> OrderBook:
        """Parse raw order book data into an OrderBook object."""
        raise NotImplementedError

    @abstractmethod
    def parse_trade(self, data: dict[str, Any], symbol: str) -> Trade:
        """Parse raw trade data into a Trade object."""
        raise NotImplementedError

    @abstractmethod
    def parse_balance(self, data: dict[str, Any]) -> Balance:
        """Parse raw balance data into a Balance object."""
        raise NotImplementedError

    @abstractmethod
    def parse_position(self, data: dict[str, Any]) -> Position:
        """Parse raw position data into a Position object."""
        raise NotImplementedError

    @abstractmethod
    def parse_order(self, data: dict[str, Any]) -> Order:
        """Parse raw order data into an Order object."""
        raise NotImplementedError

    @abstractmethod
    def parse_funding_rate(self, data: dict[str, Any]) -> FundingRate:
        """Parse raw funding rate data into a FundingRate object."""
        raise NotImplementedError

    @abstractmethod
    def parse_ticker_message(self, message: dict[str, Any]) -> Ticker | tuple[str, Ticker] | None:
        """Parse a WebSocket message containing ticker information."""
        raise NotImplementedError

    @abstractmethod
    def parse_orderbook_message(self, message: dict[str, Any]) -> OrderBook | None:
        """Parse a WebSocket message containing order book information."""
        raise NotImplementedError

    @abstractmethod
    def parse_trade_message(self, message: dict[str, Any]) -> Trade | None:
        """Parse a WebSocket message containing trade information."""
        raise NotImplementedError

    @abstractmethod
    def parse_account_update_message(
        self, message: dict[str, Any]
    ) -> tuple[dict[str, Balance] | None, dict[str, Position] | None]:
        """Parse a WebSocket message containing account (balance/position) updates."""
        raise NotImplementedError

    @abstractmethod
    def parse_order_update_message(self, message: dict[str, Any]) -> Order | None:
        """Parse a WebSocket message containing order updates."""
        raise NotImplementedError

    @abstractmethod
    def parse_funding_rate_message(self, message: dict[str, Any]) -> FundingRate | None:
        """Parse a WebSocket message containing funding rate updates."""
        raise NotImplementedError

    # --- Helper Methods --- #

    def _generate_client_order_id(self) -> str:
        # Implementation of _generate_client_order_id method
        # Example implementation:
        return f"cde-{self.exchange_name}-{int(time.time() * 1e6)}-{random.randint(1000, 9999)}"
