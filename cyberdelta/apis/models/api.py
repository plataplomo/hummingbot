from __future__ import annotations

from decimal import Decimal
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.core.models import (
    DerivativePosition,
    Order,
    OrderSide,
    OrderType,
    SpotBalance,
    TimeInForce,
    Trade,
)

# --- API Boundary Models ---


class PlaceOrderRequest(BaseModel):
    """
    Request model for placing a new order via the API.
    All financial values must use Decimal for precision and compliance.
    """

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal  # Use Decimal for all financial values
    price: Decimal | None = None
    time_in_force: TimeInForce
    client_order_id: str | None = None
    reduce_only: bool = False
    post_only: bool = False

    @field_validator("quantity", "price", mode="before")
    @classmethod
    def parse_decimal(cls, v: Decimal | str | int | float | None) -> Decimal | None:
        if v is None:
            return None
        return Decimal(str(v))


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

    orders: list[Order]
    next_page_token: str | None = None


class WebSocketMessage(BaseModel):
    """
    Envelope model for WebSocket messages.
    """

    topic: str
    data: dict[str, Any] | list[Any] | None = None  # This may remain generic for now
    event: str | None = None
    ts: int | None = None  # Optional timestamp


class APIErrorResponse(BaseModel):
    """
    Standardized error response model for API errors in CyberDeltaEngine.

    This model is used to validate, normalize, and transport error information from any exchange
    (e.g., Backpack, Hyperliquid) into a consistent internal format for business logic, logging,
    and user-facing error handling.

    Fields:
        message: Human-readable error message (from exchange or mapped internally).
        code: Canonical error code (int, typically from APIErrorCode enum; may be str for raw
            exchange codes).
        http_status: HTTP status code if available (e.g., 400, 404, 500).
        exchange_code: Raw error code from the exchange, if present (str or int).
        exchange_message: Raw error message from the exchange, if present.
        retry_after: If present, indicates how many seconds to wait before retrying
            (for rate limits, etc.).
        metadata: Optional dict for additional diagnostic or context info
            (extensible for future use).
        original_exception: The original exception, if chained (optional).

    Usage:
        - Use this model as the single source of truth for error handling in business logic.
        - Construct via the classmethod 'from_exchange_error' for robust validation and mapping.
        - All error mapping logic should produce or consume this model.
    """

    message: str = Field(..., description="Human-readable error message.")
    code: int | str = Field(
        ..., description="Canonical error code (int, or raw exchange code as str)."
    )
    http_status: int | None = Field(None, description="HTTP status code, if available.")
    exchange_code: str | int | None = Field(
        None, description="Raw error code from the exchange, if present."
    )
    exchange_message: str | None = Field(
        None, description="Raw error message from the exchange, if present."
    )
    retry_after: float | None = Field(
        None, description="Seconds to wait before retrying (for rate limits, etc.)."
    )
    metadata: dict[str, Any] | None = Field(None, description="Additional context or diagnostics.")
    original_exception: Exception | None = Field(
        None, description="Original exception, if chained."
    )

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    @field_validator("code", mode="before")
    @classmethod
    def validate_code(cls, raw_code: str | int | float | None) -> int | str:
        """
        Ensure 'code' is an int if possible, otherwise leave as str.
        Args:
            raw_code: The raw code value from the exchange or mapping logic. Accepts str, int,
                float, or None.
        Returns:
            int or str: The normalized code value. If input is None, returns 'UNKNOWN'.
        """
        if raw_code is None:
            return "UNKNOWN"
        if isinstance(raw_code, int):
            return raw_code
        if isinstance(raw_code, float):
            # Accept floats but convert to int if possible
            if raw_code.is_integer():
                return int(raw_code)
            return str(raw_code)
        try:
            return int(raw_code)
        except (ValueError, TypeError):
            return str(raw_code)

    @classmethod
    def from_exchange_error(
        cls,
        *,
        message: str,
        code: int | str,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> APIErrorResponse:
        """
        Construct an APIErrorResponse from raw exchange error data, performing validation and
        normalization.
        This is the preferred way to create error responses from mapping logic.
        """
        return cls(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


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
    Inherits Decimal fields from PlaceOrderRequest.
    """

    margin_type: str | None = None  # e.g., 'cross' or 'isolated'
    # Add any other Backpack-specific fields here


class HyperliquidPlaceOrderRequest(PlaceOrderRequest):
    """
    Hyperliquid-specific order placement request model.
    Example: includes a 'chain_id' field required by Hyperliquid.
    Inherits Decimal fields from PlaceOrderRequest.
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
    balances: list[SpotBalance] | None = None
    positions: list[DerivativePosition] | None = None
    ts: int | None = None


class OrderUpdateEvent(BaseModel):
    """
    WebSocket event model for order updates (new, filled, canceled, etc.).
    """

    event: str
    order: Order
    ts: int | None = None


class TradeFillEvent(BaseModel):
    """
    WebSocket event model for trade/fill updates.
    """

    event: str
    trade: Trade
    ts: int | None = None


class RateLimiterConfig(BaseModel):
    """
    Pydantic model for configuration and (optionally) serializable state of a token bucket
    rate limiter. This model is for config, validation, and checkpointing only. It does NOT
    include any runtime logic or async methods.

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


class APIError(Exception):
    """
    Custom exception for API-related errors with enhanced context information
    to enable better error handling and recovery mechanisms. Now Pydantic-compatible.
    Stores an APIErrorResponse as its model.
    """

    def __init__(
        self,
        message: str,
        code: int | str,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        self.model = APIErrorResponse(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )
        super().__init__(self.model.message)

    @property
    def message(self) -> str:
        return self.model.message

    @property
    def code(self) -> int | str:
        return self.model.code

    @property
    def http_status(self) -> int | None:
        return self.model.http_status

    @property
    def exchange_code(self) -> str | int | None:
        return self.model.exchange_code

    @property
    def exchange_message(self) -> str | None:
        return self.model.exchange_message

    @property
    def retry_after(self) -> float | None:
        return self.model.retry_after

    @property
    def metadata(self) -> dict[str, Any] | None:
        return self.model.metadata

    @property
    def original_exception(self) -> Exception | None:
        return self.model.original_exception

    @property
    def is_retryable(self) -> bool:
        """
        Determines if this error can be retried based on its nature.
        Rate limits, timeouts and some server errors can be retried.
        """
        # Defensive: handle both int and str code
        code_val = self.code
        if isinstance(code_val, int):
            return (
                code_val
                in (
                    109,  # RATE_LIMITED
                    1,  # TIMEOUT
                    0,  # CONNECTION_ERROR
                )
                or (code_val == 4 and self.http_status and 500 <= self.http_status < 600)
                or code_val == 2  # NETWORK_ISSUE
            )
        return False
