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

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.rate_limiter_config import RateLimiterConfig
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

# Type hint for WebSocket message handlers
MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


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

        # Initialize Rate Limiters safely
        rate_limit_config_raw = config.get("rate_limits")
        rate_limit_config: dict[str, Any] = {}  # Default to empty dict
        if isinstance(rate_limit_config_raw, dict):
            rate_limit_config = rate_limit_config_raw
        elif rate_limit_config_raw is not None:
            logger.warning(
                f"[{exchange_name}] Invalid 'rate_limits' config type: {type(rate_limit_config_raw)}. Using defaults."
            )

        # Safely get default rate and bucket size
        default_rate_raw = rate_limit_config.get("default_rate", 10.0)
        default_rate: float = 10.0
        if isinstance(default_rate_raw, int | float | str):
            try:
                default_rate = float(default_rate_raw)
            except (ValueError, TypeError):
                logger.warning(
                    f"[{exchange_name}] Invalid 'default_rate' value: {default_rate_raw}. Using default {default_rate}."
                )
        elif default_rate_raw is not None:
            logger.warning(
                f"[{exchange_name}] Invalid type for 'default_rate': {type(default_rate_raw)}. Using default {default_rate}."
            )

        default_bucket_raw = rate_limit_config.get("default_bucket_size", 10)
        default_bucket: int = 10
        # Corrected UP038
        if isinstance(default_bucket_raw, int | str):
            try:
                default_bucket = int(default_bucket_raw)
            except (ValueError, TypeError):
                logger.warning(
                    f"[{exchange_name}] Invalid 'default_bucket_size' value: {default_bucket_raw}. Using default {default_bucket}."
                )
        elif default_bucket_raw is not None:
            logger.warning(
                f"[{exchange_name}] Invalid type for 'default_bucket_size': {type(default_bucket_raw)}. Using default {default_bucket}."
            )

        self._default_limiter_config = RateLimiterConfig(
            rate=default_rate, bucket_size=default_bucket, tokens=None, last_refill=None
        )
        self._default_limiter = TokenBucketRateLimiterRuntime(default_rate, default_bucket)
        self._endpoint_limiter_configs: dict[str, RateLimiterConfig] = {}
        self._endpoint_limiters: dict[str, TokenBucketRateLimiterRuntime] = {}

        endpoints_raw = rate_limit_config.get("endpoints", {})
        endpoints: dict[str, Any] = {}
        if isinstance(endpoints_raw, dict):
            endpoints = endpoints_raw
        else:
            logger.warning(
                f"[{exchange_name}] Invalid 'endpoints' rate limit config type: {type(endpoints_raw)}. Ignoring endpoint-specific limits."
            )

        for endpoint, config_dict_raw in endpoints.items():
            # Endpoint key is guaranteed to be str if endpoints is dict[str, Any]
            # No need for isinstance check if endpoints is correctly typed or asserted
            endpoint_str = str(endpoint)  # Ensure it's treated as string

            if isinstance(config_dict_raw, dict):
                # Explicitly handle potential None from .get before float/int conversion
                rate_raw = config_dict_raw.get("rate", default_rate)
                rate: float = default_rate
                # Corrected UP038
                if isinstance(rate_raw, int | float | str):
                    try:
                        rate = float(rate_raw)
                    except (ValueError, TypeError):
                        logger.warning(
                            f"[{exchange_name}] Could not convert 'rate' for endpoint '{endpoint_str}': {rate_raw}. Using default {rate}."
                        )
                elif rate_raw is not None:
                    logger.warning(
                        f"[{exchange_name}] Invalid type for 'rate' ({type(rate_raw)}) for endpoint '{endpoint_str}'. Using default {rate}."
                    )
                # else: rate_raw is None, use default_rate

                bucket_raw = config_dict_raw.get("bucket_size", default_bucket)
                bucket: int = default_bucket
                # Corrected UP038
                if isinstance(bucket_raw, int | str):
                    try:
                        bucket = int(bucket_raw)
                        if bucket <= 0:
                            raise ValueError("Bucket size must be positive")
                    except (ValueError, TypeError):
                        logger.warning(
                            f"[{exchange_name}] Could not convert 'bucket_size' for endpoint '{endpoint_str}': {bucket_raw}. Using default {bucket}."
                        )
                elif bucket_raw is not None:  # Log if not convertible type and not None
                    logger.warning(
                        f"[{exchange_name}] Invalid type for 'bucket_size' ({type(bucket_raw)}) for endpoint '{endpoint_str}'. Using default {bucket}."
                    )
                # else: bucket_raw is None, use default_bucket

                self._endpoint_limiter_configs[endpoint_str] = RateLimiterConfig(
                    rate=rate, bucket_size=bucket, tokens=None, last_refill=None
                )
                self._endpoint_limiters[endpoint_str] = TokenBucketRateLimiterRuntime(rate, bucket)
            else:
                logger.warning(
                    f"[{exchange_name}] Invalid rate limit config type for endpoint '{endpoint_str}': {type(config_dict_raw)}"
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
                f"[{exchange_name}] Missing or invalid 'ws_endpoint' in config. WebSocket functionality disabled."
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
        Get the appropriate runtime rate limiter for a specific API endpoint.
        """
        endpoint_key = f"{method}:{path}"
        if endpoint_key in self._endpoint_limiters:
            return self._endpoint_limiters[endpoint_key]
        method_key = f"{method}:*"
        if method_key in self._endpoint_limiters:
            return self._endpoint_limiters[method_key]
        for pattern, limiter in self._endpoint_limiters.items():
            if pattern.endswith("*") and path.startswith(pattern[:-1]):
                return limiter
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
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        endpoint_group: str | None = None,
    ) -> dict[str, Any] | list[Any] | str | None:
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
            Parsed API response

        Raises:
            APIError: If the request fails after retries
        """
        if not self._session:
            raise APIError(
                "HTTP session not initialized. Call connect() first.",
                code=APIErrorCode.CONNECTION_ERROR.value,
            )

        # Check for missing endpoint configuration
        if not self.rest_endpoint:
            raise APIError(
                f"REST endpoint not configured for {self.exchange_name}. Check configuration.",
                code=APIErrorCode.CONNECTION_ERROR.value,
            )

        # Construct full URL
        url = f"{self.rest_endpoint.rstrip('/')}/{endpoint.lstrip('/')}"

        # Handle authentication if required
        if is_signed:
            auth_data = await self._authenticate(method, endpoint, params, data)
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
        rate_limiter = await self._get_rate_limiter(method, endpoint)
        await rate_limiter.acquire()

        session = await self._get_session()
        last_error: Exception | None = None
        attempt = -1  # Initialize attempt before the loop

        for attempt in range(self.max_retries + 1):
            try:
                # Record request start time for logging/monitoring
                start_time = time.monotonic()

                # Execute the request
                async with session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    headers=headers,
                    timeout=ClientTimeout(total=30.0),
                ) as response:
                    # Calculate request duration
                    duration = time.monotonic() - start_time

                    # Log request details at DEBUG level
                    logger.debug(
                        f"[{self.exchange_name}] {method} {endpoint} completed in {duration:.3f}s "
                        f"with status {response.status}"
                    )

                    # Store rate limit information for future adjustments
                    self._update_rate_limit_from_headers(response.headers, method, endpoint)

                    # Handle HTTP errors
                    if response.status >= 400:
                        error_body = await response.text()
                        error_data = {}

                        # Try to parse error response as JSON
                        try:
                            # Ensure body is str before json.loads
                            if isinstance(error_body, str):
                                error_data = json.loads(error_body)
                            else:
                                # Handle cases where error_body might not be str (e.g., bytes)
                                # This depends on how aiohttp handles different content types
                                logger.warning(
                                    f"Received non-string error body type: {type(error_body)}"
                                )
                                error_data = {}  # Default to empty if cannot parse

                        except json.JSONDecodeError:
                            # Handle non-JSON error responses
                            logger.debug(f"Non-JSON error body: {error_body[:200]}")  # Log snippet
                            error_data = {}  # Ensure error_data is a dict

                        # Map exchange-specific error to our standard format
                        # error_data is now guaranteed to be a dict
                        error = self._map_error_response(
                            status_code=response.status,
                            error_body=str(error_body),  # Ensure body passed is string
                            error_data=error_data,
                        )

                        # Handle retryable errors
                        if error.is_retryable and attempt < 2:
                            # Determine retry delay with exponential backoff
                            retry_delay = error.retry_after if error.retry_after else (2**attempt)

                            logger.warning(
                                f"[{self.exchange_name}] Request failed with retryable error: "
                                f"{error}. "
                                f"Retrying in {retry_delay:.1f}s "
                                f"({attempt + 1}/{self.max_retries + 1})"
                            )

                            await asyncio.sleep(retry_delay)
                            continue

                        # Non-retryable error or max retries exceeded
                        raise error

                    # Process based on expected content type
                    content_type = response.headers.get("Content-Type", "").lower()
                    if "application/json" in content_type:
                        try:
                            # aiohttp response.json() returns Any
                            json_data: Any = await response.json()
                            # Add runtime checks for common structures before returning
                            if isinstance(json_data, dict | list):
                                return json_data
                            elif isinstance(json_data, str):
                                # Allow raw string if JSON parser returns a string
                                return json_data
                            else:
                                # Raise if it's an unexpected JSON type (e.g., null, number, bool)
                                raise APIError(
                                    f"Expected JSON dictionary or list, got {type(json_data).__name__}",
                                    # Use INVALID_REQUEST for unexpected JSON structure
                                    code=APIErrorCode.INVALID_REQUEST.value,
                                    http_status=response.status,
                                )
                        except aiohttp.ContentTypeError:
                            # Handle cases where content type says JSON but body is not valid JSON
                            raw_text = await response.text()
                            # Return raw text if JSON parser fails
                            return raw_text

                    # Empty response
                    return {}

            except (TimeoutError, aiohttp.ClientError) as e:
                # Handle network/timeout errors
                last_error = e

                error = APIError(
                    message=f"Connection error: {str(e)}",
                    code=APIErrorCode.TIMEOUT.value
                    if isinstance(e, asyncio.TimeoutError)
                    else APIErrorCode.CONNECTION_ERROR.value,
                    original_exception=e,
                )

                # Only retry on network errors if attempts remain
                if attempt < 2:
                    retry_delay = 2**attempt  # Exponential backoff

                    logger.warning(
                        f"[{self.exchange_name}] Request failed with connection error: {error}. "
                        f"Retrying in {retry_delay:.1f}s ({attempt + 1}/{self.max_retries + 1})"
                    )

                    await asyncio.sleep(retry_delay)
                    continue

                # Max retries exceeded
                raise error from last_error  # Chain the original connection/timeout error

        # This path should ideally not be reached if the loop always raises
        # Raise a generic error if loop finishes without success or expected exception
        # DEFENSIVE CHECK: Unreachable in theory if loop always raises, safety net.
        logger.error(
            f"[{self.exchange_name}] _request loop completed unexpectedly for {method} {endpoint}"
        )
        raise APIError(
            message=f"Request failed unexpectedly after {attempt + 1} attempts.",
            code=APIErrorCode.UNKNOWN.value,
            original_exception=last_error,  # Keep last generic error
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

        # error_data is always dict[str, Any] by type contract
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
                        # Process and route message via the handler method
                        await self._handle_websocket_message(data)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"[{self.exchange_name}] Received non-JSON WebSocket message: "
                            f"{msg.data[:100]}..."
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
                        f"[{self.exchange_name}] WebSocket connection error: "
                        f"{self._ws_connection.exception()}"
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
        """Fetch historical orders, optionally filtering by symbol, time, limit, or ID."""
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

    def _generate_client_order_id(self) -> str:
        # Implementation of _generate_client_order_id method
        # Example implementation:
        return f"cde-{self.exchange_name}-{int(time.time() * 1e6)}-{random.randint(1000, 9999)}"

    @abstractmethod
    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        raise NotImplementedError
