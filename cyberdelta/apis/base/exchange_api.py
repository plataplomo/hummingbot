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
from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
from cyberdelta.apis.connectivity.connectivity_models import (
    HttpClientConfig,
    WebSocketManagerConfig,
)
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
    HttpRequestFailedError,
    ParsedJsonResponse,
)
from cyberdelta.apis.connectivity.ws_manager import WebSocketManager
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    Order,
    OrderBook,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.models.operations import Transfer, Withdrawal

if TYPE_CHECKING:
    # Import models only needed for type hints here
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
        config: dict[str, Any],
        secrets: dict[str, str | None],
        error_mapper: IErrorMapper,
        loop: asyncio.AbstractEventLoop | None = None,
        authenticator: IAuthenticator | None = None,
        http_client: HttpClient | None = None,
        ws_manager: WebSocketManager | None = None,
        rate_limit_strategy: RateLimitStrategy | None = None,
        exchange_config: ExchangeSpecificConfig | None = None,
    ) -> None:
        """Initialize the exchange API client.

        Args:
            exchange_name: Name of the exchange (e.g., 'hyperliquid', 'backpack')
            config: Dictionary of configuration parameters including endpoints, rate limits
            secrets: Dictionary of API keys and secrets for authentication
            error_mapper: Instance of an IErrorMapper implementation.
            loop: Optional event loop for rate limiters
            authenticator: Optional authenticator instance for signed requests.
            http_client: Optional HttpClient instance for dependency injection (testing)
            ws_manager: Optional WebSocketManager instance for dependency injection (testing)
            rate_limit_strategy: Optional rate limiting strategy instance
            exchange_config: Optional ExchangeSpecificConfig for creating default strategy

        """
        self.exchange_name = exchange_name
        self._config = config
        self._secrets = secrets
        self.error_mapper = error_mapper
        if loop:
            self.loop = loop
        else:
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.warning(  # Assuming 'logger' is defined in this class or module scope
                    f"[{self.exchange_name}] ExchangeAPI initialized without a running "
                    f"event loop and no loop provided. "
                    f"Creating a new event loop. This might not be intended.",
                )
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

        self._authenticator = authenticator
        self._ws_handlers: dict[str, MessageHandler] = {}

        # Construct HttpClientConfig parameters using direct field mapping
        # The concrete API classes are responsible for providing correctly named keys
        http_config_data = {}

        # Required field: rest_endpoint
        if "rest_endpoint" in self._config and self._config["rest_endpoint"] is not None:
            http_config_data["rest_endpoint"] = self._config["rest_endpoint"]
        # If rest_endpoint is not provided, HttpClientConfig.model_validate will fail,
        # which is the desired behavior for a required field

        # Optional fields - only include if explicitly provided to let Pydantic defaults apply
        if (
            "default_request_timeout" in self._config
            and self._config["default_request_timeout"] is not None
        ):
            http_config_data["default_request_timeout"] = self._config["default_request_timeout"]
        if "max_retries" in self._config and self._config["max_retries"] is not None:
            http_config_data["max_retries"] = self._config["max_retries"]
        if (
            "retry_delay_seconds" in self._config
            and self._config["retry_delay_seconds"] is not None
        ):
            http_config_data["retry_delay_seconds"] = self._config["retry_delay_seconds"]

        # Use model_validate for robust parsing and type coercion from the dict.
        # Pydantic will raise ValidationError here if required fields (like rest_endpoint)
        # are missing or if types are incorrect, which is the desired behavior.
        http_client_config = HttpClientConfig.model_validate(http_config_data)

        # Set up rate limiting strategy
        self.rate_limit_strategy = rate_limit_strategy
        self.exchange_config = exchange_config

        # Create default strategy if none provided
        if self.rate_limit_strategy is None:
            if exchange_config and exchange_config.rate_limit_per_minute is not None:
                # Create a simple token bucket strategy with default settings
                rate_per_second = exchange_config.rate_limit_per_minute / 60.0
                bucket_size = max(1, int(rate_per_second * 2))  # 2-second bucket
                default_limiter_primitive = TokenBucketRateLimiterRuntime(
                    rate=rate_per_second,
                    bucket_size=bucket_size,
                )
                self.rate_limit_strategy = SimpleTokenBucketStrategy(
                    limiter=default_limiter_primitive,
                    default_request_weight=1,
                )
                logger.info(f"[{self.exchange_name}] Created default simple rate limit strategy")
            else:
                logger.warning(
                    f"[{self.exchange_name}] No rate limit strategy provided and no "
                    f"rate_limit_per_minute in exchange config. Rate limiting may not work.",
                )

        self.rest_endpoint = str(http_client_config.rest_endpoint)  # Get validated endpoint
        # Ensure rest_endpoint is still validated as before, though Pydantic does it now
        if not self.rest_endpoint:
            raise ValueError(
                f"[{exchange_name}] Missing or invalid 'rest_endpoint' or 'base_url' in config",
            )

        # Construct WebSocketManagerConfig using direct field mapping
        # The concrete API classes are responsible for providing correctly named keys
        self.ws_endpoint = self._config.get("ws_url")
        if self.ws_endpoint and not isinstance(self.ws_endpoint, str):
            logger.warning(
                f"[{exchange_name}] Invalid 'ws_url' in config (must be str). "
                f"WebSocket functionality will be disabled.",
            )
            self.ws_endpoint = None
        elif not self.ws_endpoint:
            logger.warning(
                f"[{exchange_name}] Missing 'ws_url' in config. "
                f"WebSocket functionality will be disabled.",
            )

        # Use injected HttpClient if provided, otherwise create one
        if http_client is not None:
            self._http_client = http_client
        else:
            self._http_client = HttpClient(
                exchange_name=self.exchange_name,
                config=http_client_config,
            )

        # Use injected WebSocketManager if provided, otherwise create one if ws_endpoint exists
        # Type annotation: _ws_manager can be None when no WebSocket endpoint is configured
        self._ws_manager: WebSocketManager | None
        if ws_manager is not None:
            self._ws_manager = ws_manager
        else:
            self._ws_manager = None
            if self.ws_endpoint:  # At this point, ws_endpoint is either a valid string or None
                ws_config_data = {"ws_url": self.ws_endpoint}  # ws_url is required

                # Optional fields - only include if explicitly provided to let defaults apply
                if "ping_interval" in self._config and self._config["ping_interval"] is not None:
                    ws_config_data["ping_interval"] = self._config["ping_interval"]
                if (
                    "reconnect_delay" in self._config
                    and self._config["reconnect_delay"] is not None
                ):
                    ws_config_data["reconnect_delay"] = self._config["reconnect_delay"]
                if (
                    "max_reconnect_attempts" in self._config
                    and self._config["max_reconnect_attempts"] is not None
                ):
                    ws_config_data["max_reconnect_attempts"] = self._config[
                        "max_reconnect_attempts"
                    ]
                if (
                    "connection_timeout" in self._config
                    and self._config["connection_timeout"] is not None
                ):
                    ws_config_data["connection_timeout"] = self._config["connection_timeout"]

                # Use model_validate for robust parsing and type coercion.
                websocket_manager_config = WebSocketManagerConfig.model_validate(ws_config_data)

                # Check if we need to create a WebSocket rate limiter for Hyperliquid
                outgoing_message_limiter = None
                if (
                    self.exchange_name == "hyperliquid"
                    and exchange_config
                    and exchange_config.websocket_send_rate_per_minute is not None
                ):
                    ws_rate_per_minute = exchange_config.websocket_send_rate_per_minute
                    ws_rate_per_second = ws_rate_per_minute / 60.0
                    ws_bucket_size = max(1, int(ws_rate_per_second * 2))
                    outgoing_message_limiter = TokenBucketRateLimiterRuntime(
                        rate=ws_rate_per_second,
                        bucket_size=ws_bucket_size,
                    )
                    logger.info(
                        f"[{self.exchange_name}] Created WebSocket outgoing message limiter: "
                        f"rate={ws_rate_per_second:.2f} msg/sec",
                    )

                self._ws_manager = WebSocketManager(
                    exchange_name=self.exchange_name,
                    config=websocket_manager_config,  # Pass the WebSocketManagerConfig object
                    message_handler=self._handle_websocket_message,
                    on_connected_callback=self._on_ws_connected,
                    outgoing_message_limiter=outgoing_message_limiter,
                )

        logger.info(
            f"[{self.exchange_name}] API initialized. REST: {self.rest_endpoint}, "
            f"WS: {self.ws_endpoint}",
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

        # Prepare data for HttpClient - handle Pydantic model serialization here
        data_dict_for_http_client: dict[str, Any] | None
        if isinstance(data, BaseModel):
            data_dict_for_http_client = data.model_dump(
                by_alias=True,
                exclude_none=not serialize_none_as_null,
            )
        elif isinstance(data, dict) or data is None:
            data_dict_for_http_client = data
        else:
            raise TypeError(
                f"ExchangeAPI._request 'data' param must be BaseModel, dict, or None. "
                f"Got {type(data)}",
            )

        response_content: ParsedJsonResponse | str | None = None
        status_code: int = 0  # Default, will be overwritten
        response_headers_dict: Mapping[str, str] = {}

        try:
            # Rate limiting step using the new strategy pattern
            if self.rate_limit_strategy:
                request_context = {
                    "exchange_name": self.exchange_name,
                    "method": method,
                    "endpoint": endpoint,
                    "action_payload": data_dict_for_http_client,
                    "request_weight": request_weight,
                    "endpoint_group": endpoint_group,
                }
                await self.rate_limit_strategy.prepare_and_acquire(request_context)

            # HttpClient.request now returns: (content, status_code, processed_headers, raw_headers)
            (
                response_content,
                status_code,
                _processed_headers,
                response_headers_dict,
            ) = await self._http_client.request(
                method=method,
                endpoint_path=request_url,
                params=params,
                data=data_dict_for_http_client,
                headers=headers,
                authenticator=self._authenticator,
                is_signed=is_signed,
                serialize_none_as_null=serialize_none_as_null,
            )
            self._update_rate_limit_from_headers(response_headers_dict, method, endpoint)
            return response_content, status_code, response_headers_dict

        except HttpRequestFailedError as e_http_failed:
            logger.warning(
                f"[{self.exchange_name}] HTTP request failed for {method} "
                f"{request_url}: Status={e_http_failed.http_status}, "
                f"Body='{e_http_failed.exchange_message}'",
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
                request_path=request_url,
                original_exception=e_http_failed,
            )

            # Simply raise the mapped error. No further inspection or calls based on its
            # content here.
            raise mapped_error from e_http_failed

        except (TimeoutError, aiohttp.ClientError) as e_client:
            # These are already raised by HttpClient after its retries
            logger.error(
                f"[{self.exchange_name}] Unrecoverable client error for {method} "
                f"{request_url}: {e_client}",
            )
            # Map to a generic APIError
            # Here, we don't have a specific exchange error body, so pass what we have.
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=503,  # Service Unavailable or similar for network issues
                error_body=str(e_client),
                error_data=None,
                request_path=request_url,
                original_exception=e_client,
            )
            raise mapped_error from e_client

        except APIError:  # Re-raise APIErrors (e.g. from authenticator)
            raise
        except Exception as e_unhandled:
            logger.exception(
                f"[{self.exchange_name}] Unhandled exception during request {method} "
                f"{request_url}: {e_unhandled}",
            )
            # Map to a generic unknown APIError
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=500,  # Internal Server Error equivalent
                error_body=str(e_unhandled),
                error_data=None,
                request_path=request_url,
                original_exception=e_unhandled,
            )
            raise mapped_error from e_unhandled

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

    # --- Account Information --- #

    @abstractmethod
    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        raise NotImplementedError

    @abstractmethod
    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Fetch the account summary for the exchange."""
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Fetch current open positions, optionally filtered by symbol."""
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
    async def cancel_order(self, args: CancelOrderArgs) -> bool:
        """Cancel an existing order by its ID. Returns True if successful."""
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
