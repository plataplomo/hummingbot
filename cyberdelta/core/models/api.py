from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel

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
