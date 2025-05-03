from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.core.models.enums import (
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TimeInForce,
    TriggerType,
)
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field

logger = logging.getLogger(__name__)


# --- Enrichment Details Models ---
class HyperliquidOrderDetails(BaseModel):
    """
    Hyperliquid-specific order enrichment fields for extension slot on Order.

    Fields:
        remaining_sz (Optional[Decimal]): Remaining unfilled size (from HL API, non-negative)
        # Add other HL-specific fields as needed
    """

    remaining_sz: Decimal | None = Field(
        default=None, ge=0, description="Remaining unfilled size (non-negative, from HL API)."
    )
    # TODO: Add more HL-specific fields as discovered
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("remaining_sz", mode="before")
    @classmethod
    def parse_remaining_sz(cls, v: object, info: ValidationInfo) -> Decimal | None:
        if v is None:
            return None
        d = parse_decimal_value(
            v if isinstance(v, Decimal | str | int | float | None) else None,
            allow_none=False,
            field_name="remaining_sz",
        )
        if d is None or not d.is_finite() or d < 0:
            raise ValueError("remaining_sz must be a non-negative, finite decimal.")
        return d


class BackpackOrderDetails(BaseModel):
    """
    Backpack-specific order enrichment fields for extension slot on Order.

    Fields:
        executed_quote_quantity (Optional[Decimal]): Filled quote quantity (executedQuoteQuantity,
            must be non-negative if set)
        self_trade_prevention (Optional[SelfTradePrevention]): Self-trade prevention behavior (enum)
        expiry_reason (Optional[OrderExpiryReason]): Reason for expiry/cancellation (enum)
        origin (Optional[OrderUpdateOrigin]): Origin of the last update (enum)
    """

    executed_quote_quantity: Decimal | None = None
    self_trade_prevention: SelfTradePrevention | None = None
    expiry_reason: OrderExpiryReason | None = None
    origin: OrderUpdateOrigin | None = None
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("executed_quote_quantity", mode="before")
    @classmethod
    def parse_executed_quote_quantity(cls, v: object, info: ValidationInfo) -> Decimal | None:
        if v is None:
            return None
        d = parse_decimal_value(
            v if isinstance(v, Decimal | str | int | float | None) else None,
            allow_none=False,
            field_name="executed_quote_quantity",
        )
        if d is not None and (not d.is_finite() or d < 0):
            raise ValueError("executed_quote_quantity must be a non-negative, finite decimal.")
        return d


class Order(BaseModel):
    """
    Core internal model for a single order across all supported exchanges.
    Contains only essential, universal fields. Mutable, robust, and validated.

    Fields:
        client_order_id (str): Client-generated unique order ID (UUID).
        exchange_order_id (Optional[str]): Exchange-provided order ID.
        related_order_id (Optional[str]): Related order ID (e.g., parent/trigger).
        exchange (str): Name of the exchange ('hyperliquid', 'backpack', etc.).
        symbol (str): Trading symbol.
        side (OrderSide): Buy or sell.
        order_type (OrderType): Order type (MARKET, LIMIT, STOP, etc.).
        status (OrderStatus): Current order status.
        quantity_requested (Decimal): Requested order quantity.
        quantity_filled (Decimal): Total filled quantity.
        price (Optional[Decimal]): Limit price.
        stop_price (Optional[Decimal]): Stop trigger price.
        average_fill_price (Optional[Decimal]): Weighted average fill price.
        trigger_by (Optional[TriggerType]): Reference price for triggers (e.g., Mark, Index, Last).
        time_in_force (TimeInForce): Time in force.
        reduce_only (bool): Reduce-only flag.
        post_only (bool): Post-only flag.
        created_at (datetime): Order creation time (UTC).
        updated_at (Optional[datetime]): Last update time.
        triggered_at (Optional[datetime]): Time the conditional order was triggered.
        strategy_name (Optional[str]): Optional strategy identifier.
        signal_id (Optional[str]): Optional signal identifier.
        trades (List[Trade]): List of associated trade fills.
        hl_details (Optional[HyperliquidOrderDetails]): Hyperliquid-specific enrichment slot
        bp_details (Optional[BackpackOrderDetails]): Backpack-specific enrichment slot
    """

    client_order_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Client-generated unique order ID (UUID).",
    )
    exchange_order_id: str | None = Field(
        None, description="Exchange-provided order ID (string for BP, int as str for HL)."
    )
    related_order_id: str | None = Field(
        None, description="ID of related order (e.g., parent, trigger target)."
    )
    exchange: str = Field(
        ...,
        description="Name of the exchange this order belongs to (e.g., 'hyperliquid', 'backpack').",
    )
    symbol: str = Field(..., description="Trading symbol (e.g., 'BTC-PERP', 'SOL_USDC').")
    side: OrderSide
    order_type: OrderType
    status: OrderStatus = Field(default=OrderStatus.NEW, description="Current status of the order.")
    quantity_requested: Decimal = Field(
        ..., gt=0, description="Requested order quantity (must be positive)."
    )
    quantity_filled: Decimal = Field(
        default=Decimal("0.0"), ge=0, description="Total filled quantity (non-negative)."
    )
    price: Decimal | None = Field(
        default=None, description="Limit price (positive if set). Used for LIMIT types."
    )
    stop_price: Decimal | None = Field(
        default=None, description="Stop trigger price (positive if set). Required for STOP types."
    )
    average_fill_price: Decimal | None = Field(
        default=None, description="Weighted average fill price (positive if filled > 0)."
    )
    trigger_by: TriggerType | None = Field(
        default=None, description="Reference price for triggers (e.g., Mark, Index, Last)."
    )
    time_in_force: TimeInForce = Field(
        default=TimeInForce.GTC, description="Time in force for the order."
    )
    reduce_only: bool = Field(
        default=False, description="True if order can only reduce position size."
    )
    post_only: bool = Field(
        default=False, description="True if order should only provide liquidity (LIMIT types)."
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Order creation/submission time (UTC).",
    )
    updated_at: datetime | None = Field(None, description="Last status/fill update time (UTC).")
    triggered_at: datetime | None = Field(
        None, description="Time the conditional order was triggered (UTC)."
    )
    strategy_name: str | None = Field(None, description="Optional strategy identifier.")
    signal_id: str | None = Field(None, description="Optional signal identifier.")
    trades: list[Trade] = Field(default_factory=list, description="List of associated trade fills.")
    hl_details: HyperliquidOrderDetails | None = Field(default=None)
    bp_details: BackpackOrderDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("exchange_order_id", "related_order_id", mode="before")
    @classmethod
    def validate_optional_str_id(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is None:
            return None
        return validate_str_field(v, field_name=info.field_name or "id", max_length=128)

    @field_validator("client_order_id", mode="before")
    @classmethod
    def validate_client_order_id(cls, v: str, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="client_order_id", max_length=128)

    @field_validator("symbol", "exchange", mode="before")
    @classmethod
    def validate_symbol_exchange(cls, v: str, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name=info.field_name or "field", max_length=64)

    @field_validator("price", "stop_price", "average_fill_price", mode="before")
    @classmethod
    def parse_optional_decimal(
        cls, v: Decimal | str | int | float | None, info: ValidationInfo
    ) -> Decimal | None:
        if v is None:
            return None
        d = parse_decimal_value(v, allow_none=False, field_name=info.field_name or "field")
        if d is None or not d.is_finite():
            raise ValueError(f"{info.field_name}: Value must be a finite decimal.")
        return d

    @field_validator("quantity_requested", "quantity_filled", mode="before")
    @classmethod
    def parse_required_decimal(
        cls, v: Decimal | str | int | float, info: ValidationInfo
    ) -> Decimal:
        d = parse_decimal_value(v, allow_none=False, field_name=info.field_name or "field")
        if d is None or not d.is_finite():
            raise ValueError(f"{info.field_name}: Value must be a finite decimal.")
        return d

    @field_validator("created_at", "updated_at", "triggered_at", mode="before")
    @classmethod
    def parse_optional_datetime(
        cls, v: datetime | int | float | str | None, info: ValidationInfo
    ) -> datetime | None:
        if v is None:
            return None
        dt = parse_datetime_utc(v, field_name=info.field_name or "field")
        if dt is None:
            raise ValueError(f"{info.field_name}: Value must be a valid datetime.")
        return dt

    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        limit_types = {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}
        stop_types = {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}
        if self.order_type in limit_types and self.price is None:
            raise ValueError(f"Order type {self.order_type} requires a price.")
        if self.order_type in stop_types and self.stop_price is None:
            raise ValueError(f"Order type {self.order_type} requires a stop_price.")
        return self

    def to_dict(self) -> dict[str, Any]:
        """Subject to deprecation: Prefer model_dump(mode='json') for future serialization."""
        data = self.model_dump(exclude={"trades"})
        data["trades"] = [trade.model_dump(mode="json") for trade in self.trades]
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, Enum):
                data[key] = value.value
        return data
