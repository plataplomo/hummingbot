from __future__ import annotations

from decimal import Decimal
from typing import Generic, TypeVar

from pydantic import BaseModel, field_validator

from cyberdelta.core.models import (
    OrderSide,
    OrderType,
    TimeInForce,
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


T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Generic paginated response envelope for API endpoints returning lists of items.
    """

    items: list[T]
    next_page_token: str | None = None
