from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field

from ..enums import OrderSide


class Trade(BaseModel):
    """Lean core internal model for a single execution event (fill) across all supported exchanges.
    Contains only essential, universal fields. Immutable, robust, and validated.

    Fields:
        id (str): Trade ID (string, unique per exchange fill)
        symbol (str): Trading symbol (e.g., 'BTC-PERP')
        executed_at (datetime): UTC timestamp of execution
        side (OrderSide): Buy or sell
        order_id (str): Exchange order ID
        exchange (str): Exchange name (internal, required)
        client_order_id (Optional[str]): Client-generated order ID
        price (Decimal): Execution price (must be positive)
        quantity (Decimal): Executed quantity (must be positive)
        fee (Decimal): Fee paid for this trade (can be negative for rebates/promotions)
        fee_asset (Optional[str]): Asset in which the fee was paid (required if fee != 0)
        is_maker (Optional[bool]): True if maker fill, False if taker, None if unknown
        hl_details (Optional[HyperliquidTradeDetails]): Hyperliquid-specific enrichment slot
        bp_details (Optional[BackpackTradeDetails]): Backpack-specific enrichment slot
    """

    id: str
    symbol: str
    executed_at: datetime
    side: OrderSide
    order_id: str
    exchange: str
    price: Decimal = Field(gt=Decimal("0"))
    quantity: Decimal = Field(gt=Decimal("0"))
    client_order_id: str | None = Field(default=None)
    fee: Decimal = Field(default=Decimal("0"))
    fee_asset: str | None = Field(default=None)
    is_maker: bool | None = Field(default=None)
    hl_details: HyperliquidTradeDetails | None = Field(default=None)
    bp_details: BackpackTradeDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("id", "order_id", mode="before")
    @classmethod
    def validate_id_fields(cls, v: str, info: object) -> str:
        field_name = getattr(info, "field_name", "id")
        # max_length=128 is a generous default; revisit if stricter limits are found in
        # exchange specs
        return validate_str_field(v, field_name=str(field_name), max_length=128)

    @field_validator("symbol", "exchange", mode="before")
    @classmethod
    def validate_symbol_exchange(cls, v: str, info: object) -> str:
        field_name = getattr(info, "field_name", None)
        return validate_str_field(v, field_name=str(field_name), max_length=64)

    @field_validator("executed_at", mode="before")
    @classmethod
    def parse_executed_at(
        cls, raw_value: str | int | float | datetime | None, info: object,
    ) -> datetime:
        dt = parse_datetime_utc(raw_value, field_name="executed_at")
        if dt is None:
            raise ValueError("executed_at cannot be None")
        return dt

    @field_validator("price", "quantity", "fee", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object,
    ) -> Decimal:
        field_name = getattr(info, "field_name", None)
        d = parse_decimal_value(raw_value, allow_none=False, field_name=str(field_name))
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal.")
        return d

    @field_validator("client_order_id", "fee_asset", mode="before")
    @classmethod
    def validate_optional_str(cls, v: str | None, info: object) -> str | None:
        field_name = getattr(info, "field_name", None)
        if v is None:
            return None
        return validate_str_field(v, field_name=str(field_name), max_length=64)

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


class HyperliquidTradeDetails(BaseModel):
    """Hyperliquid-specific trade enrichment fields for extension slot on Trade.

    Fields:
        trade_hash (str): Unique trade hash (ApiUserFill.hash)
        liquidation_mark_px (Optional[Decimal]): Mark price at liquidation
            (ApiUserFill.liquidationMarkPx)
        start_position (Optional[Decimal]): Position size before fill
            (ApiUserFill.startPosition)
        dir (Optional[str]): Direction of fill (ApiUserFill.dir).
            Enum validation to be added if values are known.
    """

    trade_hash: str
    liquidation_mark_px: Decimal | None = None
    start_position: Decimal | None = None
    dir: str | None = None

    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("trade_hash", mode="before")
    @classmethod
    def validate_trade_hash(cls, v: str, info: object) -> str:
        return validate_str_field(v, field_name="trade_hash", max_length=128)

    @field_validator("dir", mode="before")
    @classmethod
    def validate_dir(cls, v: str | None, info: object) -> str | None:
        if v is None:
            return None
        # TODO: Replace with enum validation if/when values are known
        return validate_str_field(v, field_name="dir", max_length=32)

    @field_validator("liquidation_mark_px", "start_position", mode="before")
    @classmethod
    def validate_decimals(
        cls, v: str | int | float | Decimal | None, info: object,
    ) -> Decimal | None:
        if v is None:
            return None
        field_name = getattr(info, "field_name", "unknown")
        d = parse_decimal_value(v, allow_none=False, field_name=field_name)
        if d is not None and not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal.")
        return d


class BackpackTradeDetails(BaseModel):
    """Backpack-specific trade enrichment fields for extension slot on Trade.

    Fields:
        system_order_type (Optional[str]): Type of system order that triggered the fill
            (OrderFill.systemOrderType). Enum validation to be added if values are known.
    """

    system_order_type: str | None = None

    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("system_order_type", mode="before")
    @classmethod
    def validate_system_order_type(cls, v: str | None, info: object) -> str | None:
        if v is None:
            return None
        # TODO: Replace with enum validation if/when values are known
        return validate_str_field(v, field_name="system_order_type", max_length=32)
