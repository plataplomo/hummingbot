"""Base Exchange API Implementation.

This module provides the abstract base class and common functionality
for exchange-specific API implementations in the CyberDeltaEngine.
"""

from __future__ import annotations  # Enable postponed evaluation

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import aiohttp
from pydantic import BaseModel

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.base.payload_serialization_strategy import (
    DefaultSerializationStrategy,
    PayloadSerializationStrategy,
)
from cyberdelta.apis.base.rate_limit_models import RateLimitRequestContext
from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
from cyberdelta.apis.connectivity.connectivity_models import (
    HttpClientConfig,
    WebSocketManagerConfig,
)
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
    HttpRequestFailedError,
)
from cyberdelta.apis.connectivity.ws_manager import WebSocketManager
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import AnyExchangeSecrets
from cyberdelta.core.models import (
    AccountSettings,
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    Order,
    OrderBook,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market import Market
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.models.operations import Transfer, Withdrawal
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    # Import models only needed for type hints here
    from cyberdelta.apis.models.service_args_models import (
        CancelOrderArgs,
        GetAllOpenOrdersArgs,
        GetFundingRatesArgs,
        GetHistoricalFundingRatesArgs,
        GetMarketArgs,
        GetMarketDataArgs,
        GetMarketsArgs,
        GetOrderArgs,
        GetOrderHistoryArgs,
        GetTradeHistoryArgs,
        PlaceOrderArgs,
        TransferArgs,
        UpdateAccountSettingsArgs,
        WithdrawArgs,
    )

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
    """Abstract base class defining the interface for all exchange API implementations.

    Provides common functionality for API request handling, rate limiting, and error management.
    """

    def __init__(
        self,
        exchange_name: str,
        config: ExchangeSpecificConfig,
        secrets: AnyExchangeSecrets,
        error_mapper: IErrorMapper,
        loop: asyncio.AbstractEventLoop | None = None,
        authenticator: IAuthenticator | None = None,
        http_client: HttpClient | None = None,
        ws_manager: WebSocketManager | None = None,
        rate_limit_strategy: RateLimitStrategy | None = None,
        exchange_config: ExchangeSpecificConfig | None = None,
        serialization_strategy: PayloadSerializationStrategy | None = None,
    ) -> None:
        """Initialize the exchange API client.

        Args:
            exchange_name: Name of the exchange (e.g., 'hyperliquid', 'backpack')
            config: Exchange-specific configuration model
            secrets: Exchange secrets configuration model
            error_mapper: Instance of an IErrorMapper implementation.
            loop: Optional event loop for rate limiters
            authenticator: Optional authenticator instance for signed requests.
            http_client: Optional HttpClient instance for dependency injection (testing)
            ws_manager: Optional WebSocketManager instance for dependency injection (testing)
            rate_limit_strategy: Optional rate limiting strategy instance
            exchange_config: Optional ExchangeSpecificConfig for backward compatibility
            serialization_strategy: Optional payload serialization strategy for model conversion

        """
        self.exchange_name = exchange_name
        self._config = config
        self._secrets = secrets
        self.error_mapper = error_mapper
        self._authenticator = authenticator
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._serialization_strategy = serialization_strategy or DefaultSerializationStrategy()

        # Initialize event loop
        self.loop = self._setup_event_loop(loop)

        # Setup rate limiting
        self.rate_limit_strategy = self._setup_rate_limiting(
            rate_limit_strategy, exchange_config or config
        )
        self.exchange_config = exchange_config or config

        # Setup HTTP client
        self.rest_endpoint, self._http_client = self._setup_http_client(http_client)

        # Setup WebSocket manager
        self.ws_endpoint, self._ws_manager = self._setup_websocket_manager(
            ws_manager, exchange_config
        )

        logger.info(
            f"[{self.exchange_name}] API initialized. REST: {self.rest_endpoint}, "
            f"WS: {self.ws_endpoint}",
        )

    def _setup_event_loop(
        self, loop: asyncio.AbstractEventLoop | None
    ) -> asyncio.AbstractEventLoop:
        """Setup the event loop for the exchange API."""
        if loop:
            return loop

        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            logger.warning(
                f"[{self.exchange_name}] ExchangeAPI initialized without a running "
                f"event loop and no loop provided. "
                f"Creating a new event loop. This might not be intended.",
            )
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            return new_loop

    def _setup_rate_limiting(
        self,
        rate_limit_strategy: RateLimitStrategy | None,
        exchange_config: ExchangeSpecificConfig | None,
    ) -> RateLimitStrategy | None:
        """Setup rate limiting strategy."""
        if rate_limit_strategy is not None:
            return rate_limit_strategy

        if exchange_config and exchange_config.rate_limit_per_minute is not None:
            return self._create_default_rate_limiter(exchange_config)

        logger.warning(
            f"[{self.exchange_name}] No rate limit strategy provided and no "
            f"rate_limit_per_minute in exchange config. Rate limiting may not work.",
        )
        return None

    def _create_default_rate_limiter(
        self, exchange_config: ExchangeSpecificConfig
    ) -> RateLimitStrategy:
        """Create a default rate limiting strategy."""
        # DEFENSIVE CHECK: exchange_config.rate_limit_per_minute is confirmed not None by caller.
        # Mypy=[operator, arg-type]
        if exchange_config.rate_limit_per_minute is None:
            raise ValueError("rate_limit_per_minute cannot be None")

        rate_per_second: float = exchange_config.rate_limit_per_minute / 60.0
        bucket_size: int = max(1, int(rate_per_second * 2))
        default_limiter_primitive = TokenBucketRateLimiterRuntime(
            rate=rate_per_second,
            bucket_size=bucket_size,
        )
        strategy = SimpleTokenBucketStrategy(
            limiter=default_limiter_primitive,
            default_request_weight=1,
        )
        logger.info(f"[{self.exchange_name}] Created default simple rate limit strategy")
        return strategy

    def _setup_http_client(self, http_client: HttpClient | None) -> tuple[str, HttpClient]:
        """Setup HTTP client and return endpoint and client."""
        if http_client is not None:
            # Extract endpoint from config model
            rest_endpoint = str(self._config.api_base_url_mainnet)
            if not rest_endpoint:
                raise ValueError(
                    f"[{self.exchange_name}] Missing 'api_base_url_mainnet' in config",
                )
            return rest_endpoint, http_client

        # Create HTTP client config
        http_config_data = self._build_http_config_data()
        http_client_config = HttpClientConfig.model_validate(http_config_data)

        rest_endpoint = str(http_client_config.rest_endpoint)
        if not rest_endpoint:
            raise ValueError(
                f"[{self.exchange_name}] Missing or invalid 'rest_endpoint' in config",
            )

        new_http_client = HttpClient(
            exchange_name=self.exchange_name,
            config=http_client_config,
        )
        return rest_endpoint, new_http_client

    def _build_http_config_data(self) -> dict[str, Any]:
        """Build HTTP client configuration data from config model."""
        # Determine the active endpoint based on environment
        if self._config.is_mainnet_environment:
            rest_endpoint = str(self._config.api_base_url_mainnet)
        elif self._config.api_base_url_testnet:
            rest_endpoint = str(self._config.api_base_url_testnet)
        else:
            # Fallback to mainnet if testnet not configured
            rest_endpoint = str(self._config.api_base_url_mainnet)

        http_config_data: dict[str, Any] = {
            "rest_endpoint": rest_endpoint,
        }

        # Map optional fields from config model
        if self._config.request_timeout_seconds is not None:
            http_config_data["default_request_timeout"] = self._config.request_timeout_seconds
        if self._config.max_retries is not None:
            http_config_data["max_retries"] = self._config.max_retries
        if self._config.retry_delay_seconds is not None:
            http_config_data["retry_delay_seconds"] = self._config.retry_delay_seconds

        return http_config_data

    def _setup_websocket_manager(
        self,
        ws_manager: WebSocketManager | None,
        exchange_config: ExchangeSpecificConfig | None,
    ) -> tuple[str | None, WebSocketManager | None]:
        """Setup WebSocket manager and return endpoint and manager."""
        if ws_manager is not None:
            ws_endpoint = self._validate_ws_endpoint()
            return ws_endpoint, ws_manager

        # Validate WebSocket endpoint
        ws_endpoint = self._validate_ws_endpoint()
        if not ws_endpoint:
            return None, None

        # Create WebSocket manager
        ws_manager_instance = self._create_websocket_manager(ws_endpoint, exchange_config)
        return ws_endpoint, ws_manager_instance

    def _validate_ws_endpoint(self) -> str | None:
        """Validate and return WebSocket endpoint."""
        # Determine WebSocket URL based on environment
        if self._config.is_mainnet_environment:
            ws_endpoint = str(self._config.ws_url_mainnet) if self._config.ws_url_mainnet else None
        elif self._config.ws_url_testnet:
            ws_endpoint = str(self._config.ws_url_testnet)
        else:
            # Fallback to mainnet if testnet not configured
            ws_endpoint = str(self._config.ws_url_mainnet) if self._config.ws_url_mainnet else None

        if not ws_endpoint:
            logger.warning(
                f"[{self.exchange_name}] Missing WebSocket URL in config. "
                f"WebSocket functionality will be disabled.",
            )
            return None

        return ws_endpoint

    def _create_websocket_manager(
        self,
        ws_endpoint: str,
        exchange_config: ExchangeSpecificConfig | None,
    ) -> WebSocketManager:
        """Create WebSocket manager instance."""
        ws_config_data = self._build_ws_config_data(ws_endpoint)
        websocket_manager_config = WebSocketManagerConfig.model_validate(ws_config_data)

        # Create outgoing message limiter if needed
        outgoing_message_limiter = self._create_ws_rate_limiter(exchange_config)

        return WebSocketManager(
            exchange_name=self.exchange_name,
            config=websocket_manager_config,
            message_handler=self._handle_websocket_message,
            on_connected_callback=self._on_ws_connected,
            outgoing_message_limiter=outgoing_message_limiter,
        )

    def _build_ws_config_data(self, ws_endpoint: str) -> dict[str, Any]:
        """Build WebSocket configuration data from config model."""
        ws_config_data: dict[str, Any] = {"ws_url": ws_endpoint}

        # Map optional fields from config model
        if self._config.ws_ping_interval_seconds is not None:
            ws_config_data["ping_interval"] = self._config.ws_ping_interval_seconds
        if self._config.ws_reconnect_delay_seconds is not None:
            ws_config_data["reconnect_delay"] = self._config.ws_reconnect_delay_seconds
        if self._config.ws_max_reconnect_attempts is not None:
            ws_config_data["max_reconnect_attempts"] = self._config.ws_max_reconnect_attempts
        if self._config.ws_connection_timeout_seconds is not None:
            ws_config_data["connection_timeout"] = self._config.ws_connection_timeout_seconds

        return ws_config_data

    def _create_ws_rate_limiter(
        self, exchange_config: ExchangeSpecificConfig | None
    ) -> TokenBucketRateLimiterRuntime | None:
        """Create WebSocket rate limiter if needed."""
        if (
            self.exchange_name == "hyperliquid"
            and exchange_config
            and exchange_config.websocket_send_rate_per_minute is not None
        ):
            ws_rate_per_minute = exchange_config.websocket_send_rate_per_minute
            ws_rate_per_second = ws_rate_per_minute / 60.0
            ws_bucket_size = max(1, int(ws_rate_per_second * 2))
            limiter = TokenBucketRateLimiterRuntime(
                rate=ws_rate_per_second,
                bucket_size=ws_bucket_size,
            )
            logger.info(
                f"[{self.exchange_name}] Created WebSocket outgoing message limiter: "
                f"rate={ws_rate_per_second:.2f} msg/sec",
            )
            return limiter
        return None

    @property
    def is_connected(self) -> bool:
        """Returns whether the WebSocket connection is active via WebSocketManager."""
        return self._ws_manager.is_connected if self._ws_manager else False

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: BaseModel | dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        endpoint_group: str | None = None,
        request_weight: int = 1,
        serialize_none_as_null: bool = False,
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Execute an API request with exchange-specific error mapping.

        Delegates to HttpClient and handles exchange-specific error responses.

        Args:
            method: HTTP method ('GET', 'POST', etc.')
            endpoint: API endpoint path (relative to base) or full URL if handled by concrete API.
            params: URL parameters for the request.
            data: Request body data (can be Pydantic BaseModel or dict).
            headers: HTTP headers.
            is_signed: Whether the request requires authentication.
            endpoint_group: Optional logical group for the endpoint, used for rate limiting.
            request_weight: Optional request weight for rate limiting.
            serialize_none_as_null: If True, serialize Pydantic models with None values
                                  as null instead of excluding them.

        Returns:
            A tuple containing:
                - Parsed API response content (JSON dict/list, raw text) or None for empty
                  responses (204).
                - HTTP status code of the response.
                - Raw response headers.

        Raises:
            APIError: For mapped exchange-specific errors or unrecoverable issues.

        """
        request_url = urljoin(self.rest_endpoint, endpoint.lstrip("/"))
        data_dict_for_http_client = self._prepare_request_data(data, serialize_none_as_null)

        try:
            # Apply rate limiting
            await self._apply_rate_limiting(
                method, endpoint, data_dict_for_http_client, request_weight, endpoint_group
            )

            # Execute HTTP request
            response_content, status_code, response_headers_dict = await self._execute_http_request(
                method,
                request_url,
                params,
                data_dict_for_http_client,
                headers,
                is_signed,
                serialize_none_as_null,
            )

            self._update_rate_limit_from_headers(response_headers_dict, method, endpoint)
            return response_content, status_code, response_headers_dict

        except HttpRequestFailedError as e_http_failed:
            raise self._handle_http_request_error(e_http_failed, request_url) from e_http_failed
        except (TimeoutError, aiohttp.ClientError) as e_client:
            raise self._handle_client_error(e_client, method, request_url) from e_client
        except APIError:
            raise
        except Exception as e_unhandled:
            raise self._handle_unhandled_error(e_unhandled, method, request_url) from e_unhandled

    def _prepare_request_data(
        self, data: BaseModel | dict[str, Any] | None, serialize_none_as_null: bool
    ) -> dict[str, Any] | None:
        """Prepare data for HttpClient - handle Pydantic model serialization."""
        if isinstance(data, BaseModel):
            return self._serialization_strategy.serialize_model(data, serialize_none_as_null)
        elif isinstance(data, dict) or data is None:
            return data
        else:
            raise TypeError(
                f"ExchangeAPI._request 'data' param must be BaseModel, dict, or None. "
                f"Got {type(data)}",
            )

    async def _apply_rate_limiting(
        self,
        method: str,
        endpoint: str,
        data_dict: dict[str, Any] | None,
        request_weight: int,
        endpoint_group: str | None,
    ) -> None:
        """Apply rate limiting if strategy is configured."""
        if self.rate_limit_strategy:
            request_context = RateLimitRequestContext(
                exchange_name=self.exchange_name,
                method=method,
                endpoint=endpoint,
                action_payload=data_dict,
                request_weight=request_weight,
                endpoint_group=endpoint_group,
            )
            await self.rate_limit_strategy.prepare_and_acquire(request_context)

    async def _execute_http_request(
        self,
        method: str,
        request_url: str,
        params: dict[str, Any] | None,
        data_dict: dict[str, Any] | None,
        headers: dict[str, Any] | None,
        is_signed: bool,
        serialize_none_as_null: bool,
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Execute the HTTP request and return response data."""
        (
            response_content,
            status_code,
            _processed_headers,
            response_headers_dict,
        ) = await self._http_client.request(
            method=method,
            endpoint_path=request_url,
            params=params,
            data=data_dict,
            headers=headers,
            authenticator=self._authenticator,
            is_signed=is_signed,
            serialize_none_as_null=serialize_none_as_null,
        )
        return response_content, status_code, response_headers_dict

    def _handle_http_request_error(
        self, e_http_failed: HttpRequestFailedError, request_url: str
    ) -> APIError:
        """Handle HTTP request failed errors."""
        logger.warning(
            f"[{self.exchange_name}] HTTP request failed for "
            f"{request_url}: Status={e_http_failed.http_status}, "
            f"Body='{e_http_failed.exchange_message}'",
        )

        # Parse error data if available
        parsed_error_data: dict[str, Any] | None = None
        if e_http_failed.exchange_message:
            try:
                parsed_error_data = json.loads(e_http_failed.exchange_message)
                if not isinstance(parsed_error_data, dict):
                    parsed_error_data = None
            except json.JSONDecodeError:
                pass

        # Map error using exchange-specific mapper
        return self.error_mapper.map_exchange_error(
            status_code=e_http_failed.http_status or 500,
            error_body=e_http_failed.exchange_message or "",
            error_data=parsed_error_data,
            request_path=request_url,
            original_exception=e_http_failed,
        )

    def _handle_client_error(self, e_client: Exception, method: str, request_url: str) -> APIError:
        """Handle client errors (timeout, connection issues)."""
        logger.error(
            f"[{self.exchange_name}] Unrecoverable client error for {method} "
            f"{request_url}: {e_client}",
        )
        return self.error_mapper.map_exchange_error(
            status_code=503,  # Service Unavailable
            error_body=str(e_client),
            error_data=None,
            request_path=request_url,
            original_exception=e_client,
        )

    def _handle_unhandled_error(
        self, e_unhandled: Exception, method: str, request_url: str
    ) -> APIError:
        """Handle unexpected errors."""
        logger.exception(
            f"[{self.exchange_name}] Unhandled exception during request {method} "
            f"{request_url}: {e_unhandled}",
        )
        return self.error_mapper.map_exchange_error(
            status_code=500,  # Internal Server Error
            error_body=str(e_unhandled),
            error_data=None,
            request_path=request_url,
            original_exception=e_unhandled,
        )

    @abstractmethod
    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """Update rate limit information based on response headers.

        This allows dynamic adaptation to exchange-reported limits.

        Args:
            headers: Response headers
            method: HTTP method used
            path: API endpoint path

        """
        # This is a base implementation - exchange-specific classes should override
        # to handle their specific rate limit header formats
        raise NotImplementedError

    def _map_error_response(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None,
        request_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """Maps an HTTP error response to an APIError using the configured error_mapper."""
        # Ensure error_mapper is available
        if not self.error_mapper:
            # This should not happen if __init__ forces error_mapper
            logger.error(
                f"[{self.exchange_name}] Error mapper not configured. "
                f"Falling back to generic error.",
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
            original_exception=original_exception,
        )

    async def close(self) -> None:
        """Close the exchange API and clean up resources.

        Properly closes the HTTP client session and logs the shutdown process.
        """
        logger.info(f"Closing ExchangeAPI for {self.exchange_name}")
        if self._http_client:
            await self._http_client.close_session()
            logger.info(f"HTTP client for {self.exchange_name} closed.")
        else:
            logger.info(
                f"HTTP client for {self.exchange_name} was not initialized or already closed.",
            )

        if self._ws_manager:
            await self._ws_manager.close()
            logger.info(f"WebSocket manager for {self.exchange_name} closed.")
        else:
            logger.info(
                f"WebSocket manager for {self.exchange_name} was not initialized "
                f"or already closed.",
            )

        logger.info(f"ExchangeAPI for {self.exchange_name} closed successfully.")

    # --- Abstract Methods for Exchange API Implementation --- #

    @abstractmethod
    async def _route_ws_message(self, message: dict[str, Any]) -> None:  # Added return type
        """Internal method to route incoming WebSocket messages to appropriate handlers."""
        raise NotImplementedError

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a specific WebSocket topic/channel and send subscription."""
        self._ws_handlers[topic] = handler
        if self._ws_manager and self.is_connected:
            try:
                subscription_payload = self._construct_subscription_payload(topic)
                await self._ws_manager.send_json(subscription_payload)
                logger.info(f"[{self.exchange_name}] Sent subscription request for topic: {topic}")
            except (ValueError, APIError) as e:
                logger.warning(
                    f"[{self.exchange_name}] Could not construct/send subscription payload "
                    f"for topic '{topic}': {e}. Not subscribing to this topic.",
                )
        elif self._ws_manager:
            logger.warning(
                f"[{self.exchange_name}] WebSocket not connected. Subscription to {topic} "
                f"will be attempted upon connection.",
            )
        else:
            logger.error(
                f"[{self.exchange_name}] WebSocket manager not initialized. "
                f"Cannot subscribe to {topic}.",
            )

    @abstractmethod
    def _construct_subscription_payload(self, topic: str) -> BaseModel:
        """Helper method to construct exchange-specific subscription payload.

        Returns a Pydantic BaseModel that will be serialized by WebSocketManager.send_json().

        Should raise ValueError or APIError if a valid payload cannot be constructed
        for the given topic (e.g., invalid topic format, missing required info for topic type,
        unsupported topic by the exchange).
        """
        raise NotImplementedError

    async def _on_ws_connected(self) -> None:
        """Callback executed by WebSocketManager after a successful connection."""
        logger.info(
            f"[{self.exchange_name}] WebSocket connected, attempting to resubscribe to topics.",
        )
        await self._resubscribe()

    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics after (re)connection."""
        if not self._ws_handlers:
            logger.info(f"[{self.exchange_name}] No topics to resubscribe to.")
            return

        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics: {list(self._ws_handlers.keys())}",
        )
        if self._ws_manager and self.is_connected:
            for topic, _handler in self._ws_handlers.copy().items():
                try:
                    subscription_payload = self._construct_subscription_payload(topic)
                    success = await self._ws_manager.send_json(subscription_payload)
                    if success:
                        logger.info(
                            f"[{self.exchange_name}] Successfully re-sent subscription "
                            f"for {topic}.",
                        )
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Failed to re-send subscription for {topic}.",
                        )
                except (ValueError, APIError) as e:
                    logger.warning(
                        f"[{self.exchange_name}] Could not construct/send resubscription "
                        f"payload for topic '{topic}': {e}. Skipping this topic.",
                    )
                await asyncio.sleep(0.1)
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot resubscribe, WebSocket not connected "
                f"or manager not available.",
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
    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Retrieve the latest ticker information for a specific symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook | None:
        """Retrieves the order book for a specific symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Fetch current funding rates for specific symbols or all symbols if None."""
        raise NotImplementedError

    @abstractmethod
    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Fetch historical funding rates for a specific symbol.

        Args:
            args: Parameters for historical funding rate request including
                 symbol (required), optional time range (start_time, end_time),
                 and optional limit.

        Returns:
            List of FundingRate objects with historical data.

        Raises:
            APIError: If the API request fails.

        """
        raise NotImplementedError

    @abstractmethod
    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Fetch historical market data (OHLCV/Kline) for a specific symbol and timeframe.

        Args:
            args: Parameters for market data request including symbol, timeframe,
                 limit, and optional time range constraints.

        """
        raise NotImplementedError

    @abstractmethod
    async def get_market(self, args: GetMarketArgs) -> Market:
        """Retrieve market metadata for a specific symbol.

        Returns market configuration including tick size, step size, trading limits,
        and other market-specific rules required for order placement and validation.

        Args:
            args: Parameters for market metadata request including symbol.

        Returns:
            Market object containing validated market metadata.

        Raises:
            APIError: If the API request fails or symbol is not found.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Retrieve market metadata for all available markets.

        Returns market configuration for all tradable symbols including tick sizes,
        step sizes, trading limits, and other market-specific rules.

        Args:
            args: Parameters for markets metadata request (currently no parameters).

        Returns:
            List of Market objects containing validated market metadata.

        Raises:
            APIError: If the API request fails.
        """
        raise NotImplementedError

    # --- Account Information --- #

    @abstractmethod
    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        raise NotImplementedError

    @abstractmethod
    async def get_account_summary(self) -> MarginAccountSummary:
        """Fetch the account summary for the exchange."""
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Fetch current open positions, optionally filtered by symbol."""
        raise NotImplementedError

    @abstractmethod
    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits and auto-trading preferences.

        Args:
            args: Account settings to update including optional leverage_limit,
                 auto_borrow_settlements, auto_lend, auto_realize_pnl, and auto_repay_borrows.

        Note:
            This allows updating leverage limits which directly impacts maximum position sizes
            for large balance testing. Changes take effect immediately.

        Raises:
            APIError: If the account settings update fails.

        """
        raise NotImplementedError

    # --- Order Management --- #

    @abstractmethod
    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order on the exchange.

        Args:
            args: PlaceOrderArgs model containing all order parameters including
                 symbol, side, order_type, quantity, time_in_force, and optional
                 parameters like price, stop_price, client_order_id, reduce_only,
                 and post_only.

        Returns:
            Order: The placed order details.

        Raises:
            APIError: If the order placement fails.

        """
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel an existing order by its ID. Returns detailed cancellation result."""
        raise NotImplementedError

    @abstractmethod
    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all orders for a given symbol, or all if symbol is None.

        Args:
            symbol: The trading symbol (optional, if None cancels all orders).

        Returns:
            A list of CancelOrderResult objects detailing the outcome for each affected order
            or a summary result.

        Raises:
            APIError: If the API returns an error during the operation.

        """
        raise NotImplementedError

    @abstractmethod
    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Place multiple orders in a single batch request for improved performance.

        This method provides significant performance benefits by batching multiple order
        placements into a single API call when supported by the exchange. Implementations
        should fall back to sequential placement if batch operations are not supported.

        Args:
            orders: List of validated PlaceOrderArgs for batch placement

        Returns:
            List of successfully placed Order objects

        Raises:
            APIError: If validation fails or API request fails
            ValueError: If orders list is empty or contains invalid parameters
            NotImplementedError: If the exchange does not support batch operations

        Note:
            - Not all exchanges support batch operations
            - Implementations may have batch size limits
            - Market orders may not be supported in batch operations
            - Partial failures should be handled gracefully with detailed error reporting
        """
        raise NotImplementedError

    @abstractmethod
    async def cancel_batch_orders(
        self, cancel_args: list[CancelOrderArgs]
    ) -> list[CancelOrderResult]:
        """Cancel multiple orders in a single batch request for improved performance.

        This method batches multiple order cancellations into a single API call when
        supported by the exchange. Implementations should fall back to sequential
        cancellation if batch operations are not supported.

        Args:
            cancel_args: List of validated CancelOrderArgs for batch cancellation

        Returns:
            List of CancelOrderResult objects indicating success/failure for each order

        Raises:
            APIError: If validation fails or API request fails
            ValueError: If cancel_args list is empty
            NotImplementedError: If the exchange does not support batch operations

        Note:
            - Not all exchanges support batch cancellation
            - Implementations may have batch size limits
            - Results include individual success/failure status for each order
        """
        raise NotImplementedError

    @abstractmethod
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all currently open orders, optionally filtered by symbol."""
        raise NotImplementedError

    @abstractmethod
    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Fetch historical orders.

        Args:
            args: Parameters for filtering order history including symbol, time range,
                 limit, order ID, and client order ID.

        """
        raise NotImplementedError

    @abstractmethod
    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Fetch historical trade data (account fills).

        Args:
            args: Parameters for filtering trade history including symbol and limit.

        """
        raise NotImplementedError

    @abstractmethod
    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Fetch the current status of a specific order.

        Args:
            args: GetOrderArgs model containing order_id (primary identifier)
                 and optional symbol and client_order_id parameters.

        Returns:
            Order if found, None otherwise.

        Raises:
            APIError: If the API request fails.

        """
        raise NotImplementedError

    @abstractmethod
    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Get a specific order by its ID.

        Args:
            args: GetOrderArgs model containing order_id (primary identifier)
                 and optional symbol and client_order_id parameters.

        Returns:
            Order if found, None otherwise.

        Raises:
            APIError: If the API request fails.

        """
        raise NotImplementedError

    # --- WebSocket Connection Management Methods ---
    async def connect_websocket(self) -> None:
        """Establishes a WebSocket connection with the exchange."""
        if not self._ws_manager:
            logger.error(
                f"[{self.exchange_name}] WebSocket manager not initialized. "
                f"Cannot connect WebSocket.",
            )
            raise APIError(
                message=f"[{self.exchange_name}] WebSocket not configured or enabled.",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,  # Corrected: Use .value
            )
        try:
            connect_task = self._ws_manager.connect()
            if connect_task:  # ADDED: Check if task is not None
                await connect_task
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error connecting WebSocket: {e}")
            # Corrected: Use map_string_error or map_exchange_error based on available info
            # Assuming 'e' is primarily a string representation of the error here.
            # If 'e' were an HTTP-like error with status_code, map_exchange_error might be better.
            raise self.error_mapper.map_string_error(str(e)) from e

    async def close_websocket(self) -> None:
        """Closes the WebSocket connection."""
        if self._ws_manager and self._ws_manager.is_connected:  # Corrected: Use is_connected
            await self._ws_manager.close()
        else:
            logger.info(
                f"[{self.exchange_name}] WebSocket manager not active or not initialized. "
                f"No WebSocket to close.",
            )

    async def ping_websocket(self) -> None:
        """Sends a ping frame over the WebSocket to keep the connection alive."""
        if self._ws_manager and self.is_connected:  # Check is_connected for active session
            # Custom ping logic would go here if needed, e.g., sending a specific JSON message
            # For now, log that standard ping is handled by WebSocketManager
            logger.debug(
                f"[{self.exchange_name}] Standard WebSocket ping is handled by WebSocketManager "
                f"if configured. Call this for custom pings.",
            )
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot send custom ping, WebSocket not connected "
                f"or manager not available.",
            )

    # --- Helper Methods --- #

    @abstractmethod
    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol.

        Args:
            args: Parameters for filtering open orders including optional symbol.

        """
        raise NotImplementedError

    # --- Transfer and Withdrawal Operations --- #

    @abstractmethod
    async def transfer(self, args: TransferArgs) -> Transfer:
        """Execute an internal funds transfer between account types within the exchange.

        Args:
            args: TransferArgs model containing all transfer parameters including
                 asset, amount, from_account_type, to_account_type, and optional
                 client_transfer_id.

        Returns:
            Transfer: The transfer operation details and status.

        Raises:
            APIError: If the transfer operation fails.

        """
        raise NotImplementedError

    @abstractmethod
    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Execute a fund withdrawal to an external address.

        Args:
            args: WithdrawArgs model containing all withdrawal parameters including
                 asset, amount, address, and optional parameters like network, tag,
                 client_withdrawal_id, and two_factor_token.

        Returns:
            Withdrawal: The withdrawal operation details and status.

        Raises:
            APIError: If the withdrawal operation fails.

        """
        raise NotImplementedError
