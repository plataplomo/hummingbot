"""Order management models for trading operations.

This module provides comprehensive models for representing and managing trading orders
across different exchanges. The models follow the "Core + Typed Extension Slots" pattern,
allowing for common order fields while providing exchange-specific enrichment.

Key models:
- Order: Core mutable order model with lifecycle management
- HyperliquidOrderDetails: Hyperliquid-specific order enrichment
- BackpackOrderDetails: Backpack-specific order enrichment
- CancelOrderResult: Result model for order cancellation operations

The Order model is mutable by design to support order lifecycle updates (fills, status changes)
while maintaining strict validation and type safety throughout the trading process.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
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


# --- Core Order Model (Mutable) ---
class Order(BaseModel):
    """Core internal model for a single order across all supported exchanges.

    Contains essential, universal fields. Mutable, robust, and validated.
    Follows the "Core + Typed Extension Slots" pattern (Idea 5).
    """

    # --- Core Fields (as per Task) ---
    client_order_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Client-generated unique order ID (UUID).",
    )
    exchange_order_id: str | None = Field(default=None, description="Exchange-provided order ID.")
    related_order_id: str | None = Field(
        default=None,
        description="ID of related order (e.g., parent, trigger target).",
    )
    exchange: str = Field(..., description="Name of the exchange.")
    symbol: str = Field(..., description="Trading symbol.")
    side: OrderSide
    order_type: OrderType
    status: OrderStatus = Field(default=OrderStatus.NEW, description="Current status of the order.")
    quantity_requested: Decimal = Field(
        ...,
        gt=Decimal("0"),
        description="Requested base quantity (must be positive).",
    )
    quote_quantity_requested: Decimal | None = Field(
        default=None,
        gt=Decimal("0"),
        description="Optional requested quote quantity (must be positive if set).",
    )
    quantity_filled: Decimal = Field(
        default=Decimal("0"),
        ge=Decimal("0"),
        description="Total filled base quantity (non-negative).",
    )
    price: Decimal | None = Field(
        default=None,
        gt=Decimal("0"),
        description="Limit price (positive if set).",
    )
    stop_price: Decimal | None = Field(
        default=None,
        gt=Decimal("0"),
        description="Stop trigger price (positive if set).",
    )
    average_fill_price: Decimal | None = Field(
        default=None,
        gt=Decimal("0"),
        description="Weighted average fill price (positive if filled > 0).",
    )
    trigger_by: TriggerType | None = Field(
        default=None,
        description="Reference price for triggers (e.g., Mark, Index, Last).",
    )
    time_in_force: TimeInForce = Field(
        ...,
        description="Time in force for the order.",
    )  # Removed default=GTC, should be required
    reduce_only: bool = Field(
        default=False,
        description="True if order can only reduce position size.",
    )
    post_only: bool = Field(
        default=False,
        description="True if order should only provide liquidity (LIMIT types).",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Order creation/submission time (UTC).",
    )
    updated_at: datetime | None = Field(None, description="Last status/fill update time (UTC).")
    triggered_at: datetime | None = Field(
        None,
        description="Time the conditional order was triggered (UTC).",
    )
    strategy_name: str | None = Field(None, description="Optional strategy identifier.")
    signal_id: str | None = Field(None, description="Optional signal identifier.")
    trades: list[Trade] = Field(
        default_factory=lambda: [], description="List of associated trade fills."
    )

    # --- Extension Slots ---
    hl_details: HyperliquidOrderDetails | None = Field(default=None)
    bp_details: BackpackOrderDetails | None = Field(default=None)

    # --- Config (Mutable) ---
    model_config = ConfigDict(extra="forbid", validate_assignment=True)  # NO frozen=True

    # --- Field Validators ---
    @field_validator(
        "client_order_id",
        "exchange_order_id",
        "related_order_id",
        "strategy_name",
        "signal_id",
        mode="before",
    )
    @classmethod
    def validate_optional_str_id(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string ID fields with appropriate length limits."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        # client_order_id is required by default factory, others are optional
        if v is None and field_name != "client_order_id":
            return None
        if v is None and field_name == "client_order_id":
            # Should not happen with default_factory, but defensive check
            raise ValueError(f"{field_name}: Required string value cannot be None")

        return validate_str_field(v, field_name=field_name, max_length=128)

    @field_validator("symbol", "exchange", mode="before")
    @classmethod
    def validate_required_str_short(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields with shorter length limits."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator(
        "price",
        "stop_price",
        "average_fill_price",
        "quote_quantity_requested",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal_finite_positive(
        cls,
        v: str | int | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite and positive if present (via Field)."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness. gt=0 handled by Field.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed

    @field_validator("quantity_requested", mode="before")
    @classmethod
    def parse_required_decimal_finite_positive(
        cls,
        v: str | int | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and positive (via Field)."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if parsed is None:
            raise ValueError(f"{field_name}: Required value parsed as None or was invalid.")
        # Check finiteness. gt=0 handled by Field.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite")
        return parsed

    @field_validator("quantity_filled", mode="before")
    @classmethod
    def parse_required_decimal_finite_non_negative(
        cls,
        v: str | int | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and non-negative (via Field)."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if parsed is None:
            raise ValueError(f"{field_name}: Required value parsed as None or was invalid.")
        # Check finiteness. ge=0 handled by Field.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite")
        return parsed

    @field_validator("created_at", mode="before")
    @classmethod
    def parse_required_datetime_utc(
        cls,
        v: str | int | float | datetime,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        # created_at has default factory, should not receive None, but check anyway
        dt = parse_datetime_utc(v, field_name=field_name)
        if dt is None:
            raise ValueError(f"{field_name}: Required datetime parsed as None or invalid.")
        return dt

    @field_validator("updated_at", "triggered_at", mode="before")
    @classmethod
    def parse_optional_datetime_utc(
        cls,
        v: str | int | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime | None:
        """Parse optional datetime, ensuring UTC if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if v is None:
            return None
        dt = parse_datetime_utc(v, field_name=field_name)
        # Return None if parsing failed for optional field
        return dt

    # Enum fields (side, order_type, status, trigger_by, time_in_force) rely on Pydantic's
    # default enum validation. Ensure type hints are correct.

    # --- Model Validator ---
    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        """Validate essential cross-field order logic."""
        limit_types = {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}
        stop_types = {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}

        # Price required for limit types
        if self.order_type in limit_types and (self.price is None or self.price <= 0):
            raise ValueError(f"Order type {self.order_type.value} requires a positive price.")

        # Stop price required for stop types
        if self.order_type in stop_types and (self.stop_price is None or self.stop_price <= 0):
            raise ValueError(f"Order type {self.order_type.value} requires a positive stop_price.")

        # Average fill price must be positive if quantity filled is positive
        avg_price: Decimal | None = self.average_fill_price
        if self.quantity_filled > 0 and (avg_price is None or avg_price <= 0):
            raise ValueError("average_fill_price must be positive if quantity_filled > 0.")

        # Quantity filled cannot exceed quantity requested
        if self.quantity_filled > self.quantity_requested:
            raise ValueError(
                f"quantity_filled ({self.quantity_filled}) cannot exceed "
                f"quantity_requested ({self.quantity_requested})",
            )

        # Check Extension Slot Consistency (Placeholder - can be more specific if needed)
        if self.exchange == "hyperliquid" and self.bp_details is not None:
            raise ValueError("Backpack details (bp_details) must be None for a Hyperliquid order")
        if self.exchange == "backpack" and self.hl_details is not None:
            raise ValueError("Hyperliquid details (hl_details) must be None for a Backpack order")

        return self


# --- Enrichment Details Models (Immutable) ---
class HyperliquidOrderDetails(BaseModel):
    """Hyperliquid-specific order enrichment fields. Immutable."""

    remaining_sz: Decimal | None = Field(
        default=None,
        ge=Decimal("0"),
        description="Remaining unfilled size (non-negative).",
    )
    # Add other HL-specific fields as needed
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("remaining_sz", mode="before")
    @classmethod
    def parse_optional_decimal_finite(
        cls,
        v: str | int | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness if a valid Decimal was parsed. ge=0 handled by Field.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed


class BackpackOrderDetails(BaseModel):
    """Backpack-specific order enrichment fields. Immutable."""

    # Fields based on Task Instructions
    executed_quote_quantity: Decimal | None = Field(default=None, ge=Decimal("0"))
    self_trade_prevention: SelfTradePrevention | None = None
    expiry_reason: OrderExpiryReason | None = None
    origin: OrderUpdateOrigin | None = None
    sl_trigger_price: Decimal | None = Field(default=None, gt=Decimal("0"))
    sl_limit_price: Decimal | None = Field(default=None, gt=Decimal("0"))
    sl_trigger_by: TriggerType | None = None
    tp_trigger_price: Decimal | None = Field(default=None, gt=Decimal("0"))
    tp_limit_price: Decimal | None = Field(default=None, gt=Decimal("0"))
    tp_trigger_by: TriggerType | None = None
    trigger_quantity: Decimal | None = Field(default=None, gt=Decimal("0"))

    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator(
        "executed_quote_quantity",
        "sl_trigger_price",
        "sl_limit_price",
        "tp_trigger_price",
        "tp_limit_price",
        "trigger_quantity",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal_finite(
        cls,
        v: str | int | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness if a valid Decimal was parsed. gt/ge=0 handled by Field.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed

    # Enum fields rely on Pydantic's default validation for Optional[EnumType]


# --- Cancel Order Result Model ---
class CancelOrderResult(BaseModel):
    """Represents the result of a cancel order operation."""

    symbol: str | None = Field(
        default=None,
        description="Symbol of the order(s) targeted for cancellation.",
    )
    order_id: str | None = Field(
        default=None,
        description="Specific order ID targeted, if applicable. 'ALL' for bulk symbol cancels.",
    )
    client_order_id: str | None = Field(
        default=None,
        description="Client order ID, if provided in the cancel request.",
    )
    success: bool = Field(
        ...,
        description="True if the cancellation was broadly successful for the target.",
    )
    message: str | None = Field(
        default=None,
        description="Additional information or error message.",
    )
    status: CancelOrderResultStatus = Field(
        ...,
        description="Detailed status of the cancellation operation.",
    )
    raw_response: dict[str, Any] | None = Field(
        default=None,
        description="Optional raw response from the exchange for this specific cancellation.",
    )

    model_config = ConfigDict(extra="forbid", frozen=True)


__all__ = [
    "Order",
    "HyperliquidOrderDetails",
    "BackpackOrderDetails",
    "CancelOrderResult",
]
