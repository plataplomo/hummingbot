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

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import (
    CancelOrderResultStatus,
    OrderExpiryReason,
    OrderStatus,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TriggerType,
)
from cyberdelta.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    DecimalFiniteError,
    FieldNameMissingError,
    OrderLogicError,
    RequiredFieldNoneError,
)
from cyberdelta.exceptions.field_validation import InvalidExchangeNameError, TypeFieldError
from cyberdelta.models.market.fill import Fill
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


logger = get_logger(__name__)


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
    exchange: ExchangeName = Field(..., description="Name of the exchange.")
    symbol: Symbol = Field(..., description="Exchange-specific trading symbol")
    side: OrderSide
    order_type: OrderType
    status: OrderStatus = Field(default=OrderStatus.NEW, description="Current status of the order.")
    quantity_requested: Decimal = Field(
        ...,
        gt=Decimal(0),
        description="Requested base quantity (must be positive).",
    )
    quote_quantity_requested: Decimal | None = Field(
        default=None,
        gt=Decimal(0),
        description="Optional requested quote quantity (must be positive if set).",
    )
    quantity_filled: Decimal = Field(
        default=Decimal(0),
        ge=Decimal(0),
        description="Total filled base quantity (non-negative).",
    )
    price: Decimal | None = Field(
        default=None,
        gt=Decimal(0),
        description="Limit price (positive if set).",
    )
    stop_price: Decimal | None = Field(
        default=None,
        gt=Decimal(0),
        description="Stop trigger price (positive if set).",
    )
    average_fill_price: Decimal | None = Field(
        default=None,
        gt=Decimal(0),
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
    # PYRIGHT BUG: Known regression in pyright 1.1.399+ where Field(default_factory=list)
    # with generic types is incorrectly reported as "partially unknown". This is a pyright
    # static analysis bug, not a code issue. The type is fully known at runtime.
    # See: https://github.com/microsoft/pyright/issues/10442
    # TODO: Remove this ignore when pyright fixes the regression
    trades: list[Fill] = Field(  # pyright: ignore[reportUnknownVariableType]
        default_factory=list,
        description="List of associated trade fills.",
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
        """Validate optional string ID fields with appropriate length limits.

        Args:
            v: The value to validate (optional string)
            info: Validation context containing field name

        Returns:
            Validated string value or None if optional field is None

        Raises:
            FieldNameMissingError: If field name is None
            RequiredFieldNoneError: If client_order_id is None (required)
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        # client_order_id is required by default factory, others are optional
        if v is None and field_name != "client_order_id":
            return None
        if v is None and field_name == "client_order_id":
            # Should not happen with default_factory, but defensive check
            raise RequiredFieldNoneError(field_name, "Required string value cannot be None")

        return validate_str_field(v, field_name=field_name, max_length=128)

    @field_validator("exchange", mode="before")
    @classmethod
    def validate_exchange(cls, v: object, info: ValidationInfo) -> ExchangeName:
        """Validate the 'exchange' field is a valid ExchangeName.

        Args:
            v: The value to validate
            info: Validation context containing field name

        Returns:
            Validated ExchangeName value

        Raises:
            InvalidExchangeNameError: If not a valid exchange name
            TypeFieldError: If value is not a string or ExchangeName enum
        """
        if isinstance(v, ExchangeName):
            return v
        if isinstance(v, str):
            try:
                return ExchangeName(v.lower())
            except ValueError as e:
                raise InvalidExchangeNameError(
                    value=v,
                    valid_exchanges=[ex.value for ex in ExchangeName],
                ) from e
        raise TypeFieldError(
            field_name="exchange",
            expected_type="string or ExchangeName",
            actual_type=type(v).__name__,
            actual_value=v,
        )

    # Symbol validation is handled by Pydantic's type system
    # No need for a custom validator since Symbol is always valid

    @field_validator("average_fill_price", mode="before")
    @classmethod
    def validate_average_fill_price(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Validate average fill price specifically.

        Args:
            v: The value to validate (string, float, Decimal, or None)
            info: Validation context containing field name

        Returns:
            Validated Decimal value or None if not provided

        Raises:
            DecimalFiniteError: If the value is not finite
        """
        if v is None:
            return None
        if isinstance(v, Decimal):
            # Already a Decimal, just validate it
            if not v.is_finite():
                raise DecimalFiniteError("average_fill_price", v)
            # Don't validate positive here, do it in model validator
            return v
        # Parse from string/int/float
        parsed = parse_decimal_value(v, field_name="average_fill_price", allow_none=True)
        if parsed is None:
            return None
        if not parsed.is_finite():
            raise DecimalFiniteError("average_fill_price", parsed)
        # Don't validate positive here, do it in model validator
        return parsed

    @field_validator(
        "price",
        "stop_price",
        "quote_quantity_requested",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal_finite_positive(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite and positive if present.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field name

        Returns:
            Parsed Decimal value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness and positive value
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed, context="if provided")
        return parsed

    @field_validator("quantity_requested", mode="before")
    @classmethod
    def parse_required_decimal_finite_positive(
        cls,
        v: str | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and positive (via Field).

        Args:
            v: The value to parse (string, float, or Decimal)
            info: Validation context containing field name

        Returns:
            Parsed finite Decimal value

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        # allow_none=False ensures parsed is never None
        # Check finiteness. gt=0 handled by Field.
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed)
        return parsed

    @field_validator("quantity_filled", mode="before")
    @classmethod
    def parse_required_decimal_finite_non_negative(
        cls,
        v: str | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and non-negative (via Field).

        Args:
            v: The value to parse (string, float, or Decimal)
            info: Validation context containing field name

        Returns:
            Parsed finite Decimal value

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        # allow_none=False ensures parsed is never None
        # Check finiteness. ge=0 handled by Field.
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed)
        return parsed

    @field_validator("created_at", mode="before")
    @classmethod
    def parse_required_datetime_utc(
        cls,
        v: str | float | datetime,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC.

        Args:
            v: The value to parse (string, float, or datetime)
            info: Validation context containing field name

        Returns:
            Parsed UTC datetime

        Raises:
            FieldNameMissingError: If field name is None
            RequiredFieldNoneError: If parsed datetime is None
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        # created_at has default factory, should not receive None, but check anyway
        dt = parse_datetime_utc(v, field_name=field_name)
        if dt is None:
            raise RequiredFieldNoneError(field_name, "Required datetime parsed as None or invalid")
        return dt

    @field_validator("updated_at", "triggered_at", mode="before")
    @classmethod
    def parse_optional_datetime_utc(
        cls,
        v: str | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime | None:
        """Parse optional datetime, ensuring UTC if present.

        Args:
            v: The value to parse (string, float, datetime, or None)
            info: Validation context containing field name

        Returns:
            Parsed UTC datetime or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        if v is None:
            return None
        return parse_datetime_utc(v, field_name=field_name)
        # Return None if parsing failed for optional field

    # Enum fields (side, order_type, status, trigger_by, time_in_force) rely on Pydantic's
    # default enum validation. Ensure type hints are correct.

    # --- Model Validator ---
    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        """Validate essential cross-field order logic.

        Returns:
            Self instance after validation

        Raises:
            OrderLogicError: If cross-field validation fails
        """
        limit_types = {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}
        stop_types = {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}

        # Price required for limit types
        if self.order_type in limit_types and (self.price is None or self.price <= 0):
            raise OrderLogicError(
                "price_required",
                f"Order type {self.order_type.value} requires a positive price",
                order_type=self.order_type.value,
                fields={"price": self.price, "order_type": self.order_type.value},
            )

        # Stop price required for stop types
        if self.order_type in stop_types and (self.stop_price is None or self.stop_price <= 0):
            raise OrderLogicError(
                "stop_price_required",
                f"Order type {self.order_type.value} requires a positive stop_price",
                order_type=self.order_type.value,
                fields={"stop_price": self.stop_price, "order_type": self.order_type.value},
            )

        # Average fill price must be positive if quantity filled is positive
        avg_price: Decimal | None = self.average_fill_price
        if self.quantity_filled > 0 and (avg_price is None or avg_price <= 0):
            raise OrderLogicError(
                "average_fill_price_validation",
                "average_fill_price must be positive if quantity_filled > 0",
                fields={"quantity_filled": self.quantity_filled, "average_fill_price": avg_price},
            )

        # Quantity filled cannot exceed quantity requested
        if self.quantity_filled > self.quantity_requested:
            raise OrderLogicError(
                "quantity_validation",
                f"quantity_filled ({self.quantity_filled}) cannot exceed "
                f"quantity_requested ({self.quantity_requested})",
                fields={
                    "quantity_filled": self.quantity_filled,
                    "quantity_requested": self.quantity_requested,
                },
            )

        # Check Extension Slot Consistency (Placeholder - can be more specific if needed)
        if self.exchange == ExchangeName.HYPERLIQUID and self.bp_details is not None:
            raise OrderLogicError(
                "exchange_details_consistency",
                "Backpack details (bp_details) must be None for a Hyperliquid order",
                fields={"exchange": self.exchange, "bp_details": self.bp_details},
            )
        if self.exchange == ExchangeName.BACKPACK and self.hl_details is not None:
            raise OrderLogicError(
                "exchange_details_consistency",
                "Hyperliquid details (hl_details) must be None for a Backpack order",
                fields={"exchange": self.exchange, "hl_details": self.hl_details},
            )

        return self


# --- Enrichment Details Models (Immutable) ---
class HyperliquidOrderDetails(BaseModel):
    """Hyperliquid-specific order enrichment fields. Immutable."""

    remaining_sz: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        description="Remaining unfilled size (non-negative).",
    )
    # Add other HL-specific fields as needed
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("remaining_sz", mode="before")
    @classmethod
    def parse_optional_decimal_finite(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field name

        Returns:
            Parsed finite Decimal value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness if a valid Decimal was parsed. ge=0 handled by Field.
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed, context="if provided")
        return parsed


class BackpackOrderDetails(BaseModel):
    """Backpack-specific order enrichment fields. Immutable."""

    # Fields based on Task Instructions
    executed_quote_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    self_trade_prevention: SelfTradePrevention | None = None
    expiry_reason: OrderExpiryReason | None = None
    origin: OrderUpdateOrigin | None = None
    sl_trigger_price: Decimal | None = Field(default=None, gt=Decimal(0))
    sl_limit_price: Decimal | None = Field(default=None, gt=Decimal(0))
    sl_trigger_by: TriggerType | None = None
    tp_trigger_price: Decimal | None = Field(default=None, gt=Decimal(0))
    tp_limit_price: Decimal | None = Field(default=None, gt=Decimal(0))
    tp_trigger_by: TriggerType | None = None
    trigger_quantity: Decimal | None = Field(default=None, gt=Decimal(0))

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
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field name

        Returns:
            Parsed finite Decimal value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness if a valid Decimal was parsed. gt/ge=0 handled by Field.
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed, context="if provided")
        return parsed

    # Enum fields rely on Pydantic's default validation for Optional[EnumType]


# --- Cancel Order Result Model ---
class CancelOrderResult(BaseModel):
    """Represents the result of a cancel order operation."""

    symbol: Symbol | None = Field(
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
    "BackpackOrderDetails",
    "CancelOrderResult",
    "HyperliquidOrderDetails",
    "Order",
]
