from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.models.enums import APIErrorCode
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce

# --- API Boundary Models ---


class PlaceOrderRequest(BaseModel):
    """
    Request model for placing a new order via the API.
    """

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float  # Use Decimal if required by your API
    price: float | None = None
    time_in_force: TimeInForce
    client_order_id: str | None = None
    reduce_only: bool = False
    post_only: bool = False


class CancelOrderRequest(BaseModel):
    """
    Request model for canceling an order via the API.
    """

    order_id: str
    symbol: str | None = None


class OrdersResponse(BaseModel):
    """
    Response model for a batch of orders (e.g., open orders, order history).
    """

    orders: list[Any]  # Should be list[Order], but import from models.py if needed
    next_page_token: str | None = None


class WebSocketMessage(BaseModel):
    """
    Envelope model for WebSocket messages.
    """

    topic: str
    data: dict[str, Any] | list[Any] | None = None
    event: str | None = None
    ts: int | None = None  # Optional timestamp


class APIErrorResponse(BaseModel):
    """
    Standardized error response model for API errors.
    """

    message: str
    code: int | str
    http_status: int | None = None
    exchange_code: str | None = None
    exchange_message: str | None = None
    retry_after: float | None = None


T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Generic paginated response envelope for API endpoints returning lists of items.
    """

    items: list[T]
    next_page_token: str | None = None


# --- Exchange-Specific Request/Response Models ---


class BackpackPlaceOrderRequest(PlaceOrderRequest):
    """
    Backpack-specific order placement request model.
    Example: includes a 'margin_type' field required by Backpack.
    """

    margin_type: str | None = None  # e.g., 'cross' or 'isolated'
    # Add any other Backpack-specific fields here


class HyperliquidPlaceOrderRequest(PlaceOrderRequest):
    """
    Hyperliquid-specific order placement request model.
    Example: includes a 'chain_id' field required by Hyperliquid.
    """

    chain_id: int | None = None
    # Add any other Hyperliquid-specific fields here


# --- Event-Specific WebSocket Models ---


class AccountUpdateEvent(BaseModel):
    """
    WebSocket event model for account updates (balances, positions, etc.).
    """

    event: str
    account_id: str
    balances: list[Any] | None = None  # Should be list[Balance], import if needed
    positions: list[Any] | None = None  # Should be list[Position], import if needed
    ts: int | None = None


class OrderUpdateEvent(BaseModel):
    """
    WebSocket event model for order updates (new, filled, canceled, etc.).
    """

    event: str
    order: Any  # Should be Order, import if needed
    ts: int | None = None


class TradeFillEvent(BaseModel):
    """
    WebSocket event model for trade/fill updates.
    """

    event: str
    trade: Any  # Should be Trade, import if needed
    ts: int | None = None


class APIErrorModel(BaseModel):
    """
    Pydantic model for API error details, used for validation and serialization.
    """

    message: str = Field(..., description="Human-readable error message.")
    code: APIErrorCode = Field(..., description="Standardized error code enum.")
    http_status: int | None = Field(None, description="HTTP status code, if available.")
    exchange_code: str | None = Field(None, description="Exchange-specific error code, if any.")
    exchange_message: str | None = Field(
        None, description="Exchange-specific error message, if any."
    )
    retry_after: float | None = Field(
        None, description="Retry-after value in seconds, if rate limited."
    )
    original_exception: Exception | None = Field(
        None, description="Original exception, if chained."
    )

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class APIError(Exception):
    """
    Custom exception for API-related errors with enhanced context information
    to enable better error handling and recovery mechanisms. Now Pydantic-compatible.
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
        self.model = APIErrorModel(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            original_exception=original_exception,
        )
        super().__init__(self.model.message)

    @property
    def message(self) -> str:
        return self.model.message

    @property
    def code(self) -> APIErrorCode:
        return self.model.code

    @property
    def http_status(self) -> int | None:
        return self.model.http_status

    @property
    def exchange_code(self) -> str | None:
        return self.model.exchange_code

    @property
    def exchange_message(self) -> str | None:
        return self.model.exchange_message

    @property
    def retry_after(self) -> float | None:
        return self.model.retry_after

    @property
    def original_exception(self) -> Exception | None:
        return self.model.original_exception

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


class RateLimiterConfig(BaseModel):
    """
    Pydantic model for configuration and (optionally) serializable state of a token bucket rate limiter.
    This model is for config, validation, and checkpointing only. It does NOT include any runtime logic or async methods.

    Attributes:
        rate (float): Maximum number of requests per second.
        bucket_size (int): Maximum burst capacity (number of tokens).
        tokens (float): Current token count (optional, for checkpointing).
        last_refill (float): Timestamp of last token refill (optional, for checkpointing).
    """

    rate: float = Field(..., description="Maximum requests per second.")
    bucket_size: int = Field(..., description="Maximum burst capacity (tokens).")
    tokens: float | None = Field(
        None, description="Current token count (for checkpointing, optional)."
    )
    last_refill: float | None = Field(
        None, description="Timestamp of last refill (for checkpointing, optional)."
    )


class ExchangeAPIConfig(BaseModel):
    """
    Pydantic model for configuration of an ExchangeAPI instance.
    This model is for config/validation only, not for runtime logic or state.

    Attributes:
        rest_endpoint (str): Base URL for REST API.
        ws_endpoint (str): Base URL for WebSocket API.
        api_key (str): API key for authentication.
        api_secret (str): API secret for authentication.
        rate_limits (dict | None): Optional rate limit configuration (raw dict or validated model).
    """

    rest_endpoint: str = Field(..., description="Base URL for REST API.")
    ws_endpoint: str = Field(..., description="Base URL for WebSocket API.")
    api_key: str = Field(..., description="API key for authentication.")
    api_secret: str = Field(..., description="API secret for authentication.")
    rate_limits: dict[str, Any] | None = Field(
        None, description="Optional rate limit configuration (raw dict or validated model)."
    )
