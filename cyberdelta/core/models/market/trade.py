from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field

from ..enums import OrderSide


class Trade(BaseModel):
    """
    Superset internal model for a single execution event (fill) across Backpack and Hyperliquid.
    Immutable, robust, and validated. Captures all relevant details for audit, reconciliation,
    and analytics.

    Fields:
        id (str): Trade ID (Backpack: str, HL: int/tid)
        symbol (str): Trading symbol (Backpack: symbol, HL: coin)
        executed_at (datetime): UTC timestamp of execution (Backpack: time, HL: time)
        side (OrderSide): Buy or sell (Backpack: inferred, HL: side)
        order_id (str): Exchange order ID (Backpack: order_id, HL: oid)
        exchange (str): Exchange name (internal, required)
        client_order_id (Optional[str]): Client-generated order ID (Backpack: client_order_id,
            HL: cloid)
        price (Decimal): Execution price (must be positive)
        quantity (Decimal): Executed quantity (must be positive)
        fee (Decimal): Fee paid for this trade (can be negative for rebates/promotions)
        fee_asset (Optional[str]): Asset in which the fee was paid (required if fee != 0)
        is_maker (Optional[bool]): True if maker fill, False if taker, None if unknown
        trade_hash (Optional[str]): Unique trade hash (HL only)
        liquidation_mark_px (Optional[Decimal]): Liquidation mark price (HL only)
        start_position (Optional[Decimal]): Start position before fill (HL only)
        dir (Optional[str]): Direction of fill (HL only)
        timestamp (Optional[int]): Optional integer timestamp (for legacy/exchange compatibility)

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
    client_order_id: str | None = None
    price: Decimal = Field(gt=0, description="Execution price must be positive.")
    quantity: Decimal = Field(gt=0, description="Executed quantity must be positive.")
    fee: Decimal = Field(
        default=Decimal("0"),
        description="Fee paid for this trade. Can be negative for rebates or promotion.",
    )
    fee_asset: str | None = Field(
        default=None, description="Asset in which the fee was paid. Required if fee != 0."
    )
    is_maker: bool | None = Field(
        default=None, description="True if maker fill, False if taker, None if unknown."
    )
    trade_hash: str | None = Field(default=None, description="Unique trade hash (HL only).")
    liquidation_mark_px: Decimal | None = Field(
        default=None, description="Liquidation mark price (HL only)."
    )
    start_position: Decimal | None = Field(
        default=None, description="Start position before fill (HL only)."
    )
    dir: str | None = Field(default=None, description="Direction of fill (HL only).")
    timestamp: int | None = Field(default=None, exclude=True)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, v: str, info: object) -> str:
        field_name = getattr(info, "field_name", "id")
        return validate_str_field(v, field_name=str(field_name), max_length=64)

    @field_validator("order_id", mode="before")
    @classmethod
    def validate_order_id(cls, v: str, info: object) -> str:
        field_name = getattr(info, "field_name", "order_id")
        return validate_str_field(v, field_name=str(field_name), max_length=64)

    @field_validator("executed_at", mode="before")
    @classmethod
    def parse_executed_at(
        cls, raw_value: str | int | float | datetime | None, info: object
    ) -> datetime:
        dt = parse_datetime_utc(raw_value, field_name="executed_at")
        if dt is None:
            raise ValueError("executed_at cannot be None")
        return dt

    @field_validator(
        "price", "quantity", "fee", "liquidation_mark_px", "start_position", mode="before"
    )
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        field_name = getattr(info, "field_name", None)
        field_name_str = str(field_name) if field_name is not None else "unknown"
        # Optional fields: allow None
        if field_name in {"liquidation_mark_px", "start_position"}:
            if raw_value is None:
                return None
        # Required fields: price, quantity, fee (fee has default, but should not be None)
        if raw_value is None:
            raise ValueError(f"{field_name_str}: Value cannot be None.")
        d = parse_decimal_value(raw_value, allow_none=False, field_name=field_name_str)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name_str}: Value must be a finite decimal.")
        return d

    @field_validator("client_order_id", "fee_asset", "trade_hash", "dir", mode="before")
    @classmethod
    def validate_optional_str(cls, v: str | None, info: object) -> str | None:
        field_name = getattr(info, "field_name", None)
        field_name_str = str(field_name) if field_name is not None else "unknown"
        if v is None:
            return None
        return validate_str_field(v, field_name=field_name_str, max_length=64)

    @model_validator(mode="after")
    def check_fee_logic(self) -> Self:
        if self.fee != Decimal("0") and not self.fee_asset:
            raise ValueError("fee_asset must be provided if fee is nonzero.")
        return self

    @computed_field
    def cost(self) -> Decimal:
        """Total cost (price * quantity) for this trade."""
        return self.price * self.quantity

    def to_dict(self) -> dict[str, Any]:
        """Subject to deprecation: Prefer model_dump(mode='json') for future serialization."""
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, OrderSide):
                data[key] = value.value
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
