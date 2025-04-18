from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

from ..enums import OrderSide, OrderStatus, OrderType


class Order(BaseModel):
    """
    Order represents a unified, internal model for all order states and parameters, supporting both
    Backpack and Hyperliquid perpetuals. It is immutable (frozen=True) to ensure auditability and
    correctness.

    Fields:
        id (str): Unique internal order identifier.
        symbol (str): Trading symbol.
        side (OrderSide): Buy or sell.
        type (OrderType): Order type (limit, market, etc.).
        status (OrderStatus): Current order status.
        price (Decimal | None): Limit price (if applicable).
        quantity (Decimal): Order quantity.
        filled (Decimal): Quantity filled so far.
        remaining (Decimal): Quantity remaining to fill.
        cost (Decimal | None): Total cost (price * quantity, if applicable).
        avg_fill_price (Decimal | None): Average fill price (if filled).
        created_at (datetime): UTC timestamp of order creation.
        updated_at (datetime | None): UTC timestamp of last update.
        exchange (str): Exchange name.
        exchange_order_id (str): Exchange order ID.
        client_order_id (str): Client-generated order ID.
        is_post_only (bool | None): True if post-only, else None.
        reduce_only (bool | None): True if reduce-only, else None.
        stop_price (Decimal | None): Stop price (if stop order).
        timestamp (int | None): Optional integer timestamp (for legacy/exchange compatibility).

    Notes:
        - All financial fields use Decimal for accuracy.
        - This model is not intended for mutation after creation.
    """

    id: str
    symbol: str
    side: OrderSide
    type: OrderType
    status: OrderStatus
    price: Decimal | None = None
    quantity: Decimal
    filled: Decimal
    remaining: Decimal
    cost: Decimal | None = None
    avg_fill_price: Decimal | None = None
    created_at: datetime
    updated_at: datetime | None = None
    exchange: str
    exchange_order_id: str
    client_order_id: str
    is_post_only: bool | None = None
    reduce_only: bool | None = None
    stop_price: Decimal | None = None
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def parse_datetimes(
        cls, raw_value: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        if raw_value is None:
            return None
        return parse_datetime_utc(raw_value)

    @field_validator(
        "price",
        "quantity",
        "filled",
        "remaining",
        "cost",
        "avg_fill_price",
        "stop_price",
        mode="before",
    )
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        field_name = getattr(info, "field_name", "")
        if raw_value is None:
            return None
        decimal_value = parse_decimal_value(raw_value)
        if decimal_value is None:
            raise ValueError(
                f"Field '{field_name}' could not be parsed to Decimal and is required."
            )
        return decimal_value

    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        if self.quantity < 0:
            raise ValueError("Order quantity must be non-negative.")
        if self.filled < 0:
            raise ValueError("Order filled must be non-negative.")
        if self.remaining < 0:
            raise ValueError("Order remaining must be non-negative.")
        if self.price is not None and self.price < 0:
            raise ValueError("Order price must be non-negative if specified.")
        if self.cost is not None and self.cost < 0:
            raise ValueError("Order cost must be non-negative if specified.")
        if self.avg_fill_price is not None and self.avg_fill_price < 0:
            raise ValueError("Order avg_fill_price must be non-negative if specified.")
        if self.stop_price is not None and self.stop_price < 0:
            raise ValueError("Order stop_price must be non-negative if specified.")
        if not self.side:
            raise ValueError("Order side is required.")
        if not self.type:
            raise ValueError("Order type is required.")
        if not self.status:
            raise ValueError("Order status is required.")
        if not self.exchange:
            raise ValueError("Order exchange is required.")
        if not self.exchange_order_id:
            raise ValueError("Order exchange_order_id is required.")
        if not self.client_order_id:
            raise ValueError("Order client_order_id is required.")
        return self

    def to_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, OrderSide | OrderType | OrderStatus):
                data[key] = value.value
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
