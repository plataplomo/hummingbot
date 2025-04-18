from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

from ..enums import OrderSide


class Trade(BaseModel):
    """
    Trade models a single execution event (fill) against an order, capturing all relevant details
    for audit, reconciliation, and analytics. This model is immutable (frozen=True) to ensure that
    execution records are never altered after creation, supporting robust audit trails and
    compliance.

    Fields:
        id (str): Unique identifier for the trade (may be exchange or internal).
        symbol (str): Trading symbol.
        executed_at (datetime): UTC timestamp of execution.
        side (OrderSide): Buy or sell.
        order_id (str): Exchange order ID.
        exchange (str): Exchange name.
        client_order_id (str): Client-generated order ID.
        price (Decimal): Execution price (must be positive).
        quantity (Decimal): Executed quantity (must be positive).
        cost (Decimal): Total cost (price * quantity, must be positive).
        fee (Decimal): Fee paid for this trade (can be negative for rebates/promotions).
        fee_asset (str): Asset in which the fee was paid (required if fee != 0).
        is_maker (bool | None): True if maker fill, False if taker, None if unknown.
        timestamp (int | None): Optional integer timestamp (for legacy/exchange compatibility).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Negative fee values are allowed for rebates or promotions.
        - This model is not intended for mutation after creation.
    """

    id: str
    symbol: str
    executed_at: datetime
    side: OrderSide
    order_id: str
    exchange: str
    client_order_id: str
    price: Decimal = Field(gt=0, description="Execution price must be positive.")
    quantity: Decimal = Field(gt=0, description="Executed quantity must be positive.")
    cost: Decimal = Field(gt=0, description="Total cost (price * quantity) must be positive.")
    fee: Decimal = Field(
        default=Decimal("0"),
        description=(
            "Fee paid for this trade. Can be negative if exchange pays a rebate or promotion."
        ),
    )
    fee_asset: str = Field(
        default="", description="Asset in which the fee was paid. Required if fee != 0."
    )
    is_maker: bool | None = Field(
        default=None, description="True if maker fill, False if taker, None if unknown."
    )
    timestamp: int | None = Field(default=None, exclude=True)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("executed_at", mode="before")
    @classmethod
    def parse_executed_at(
        cls, raw_value: str | int | float | datetime | None, info: object
    ) -> datetime:
        datetime_value = parse_datetime_utc(raw_value)
        if datetime_value is None:
            raise ValueError("executed_at cannot be None")
        return datetime_value

    @field_validator("price", "quantity", "cost", "fee", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal:
        field_name = getattr(info, "field_name", "")
        if raw_value is None:
            if field_name == "fee":
                return Decimal("0")
            raise ValueError(f"Field '{field_name}' is required and cannot be None.")
        decimal_value = parse_decimal_value(raw_value)
        if decimal_value is None:
            raise ValueError(
                f"Field '{field_name}' could not be parsed to Decimal and is required."
            )
        return decimal_value

    @model_validator(mode="after")
    def check_trade_logic(self) -> Self:
        if self.price <= 0:
            raise ValueError("Trade price must be positive.")
        if self.quantity <= 0:
            raise ValueError("Trade quantity must be positive.")
        if self.cost <= 0:
            raise ValueError("Trade cost must be positive.")
        if self.fee != 0 and not self.fee_asset:
            raise ValueError("fee_asset must be provided if fee is nonzero.")
        if not self.side:
            raise ValueError("Trade side is required.")
        if not self.order_id:
            raise ValueError("Trade order_id is required.")
        if not self.exchange:
            raise ValueError("Trade exchange is required.")
        if not self.client_order_id:
            raise ValueError("Trade client_order_id is required.")
        return self

    def to_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, OrderSide):
                data[key] = value.value
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
