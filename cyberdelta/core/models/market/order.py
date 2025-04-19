import logging
import typing
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
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class Order(BaseModel):
    """
    Unified Order model for CyberDeltaEngine, supporting both Backpack and Hyperliquid APIs.

    This model captures all relevant order parameters, state, and metadata required to represent
    the superset of order types, modifiers, and lifecycle events from both exchanges.

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
        executed_quote_quantity (Optional[Decimal]): Filled quote quantity (Backpack).
        price (Optional[Decimal]): Limit price.
        stop_price (Optional[Decimal]): Stop trigger price.
        average_fill_price (Optional[Decimal]): Weighted average fill price.
        trigger_by (Optional[TriggerType]): Reference price for triggers.
        time_in_force (TimeInForce): Time in force.
        reduce_only (bool): Reduce-only flag.
        post_only (bool): Post-only flag.
        self_trade_prevention (Optional[SelfTradePrevention]): Self-trade prevention behavior.
        created_at (datetime): Order creation time (UTC).
        updated_at (Optional[datetime]): Last update time.
        triggered_at (Optional[datetime]): Time the conditional order was triggered.
        expiry_reason (Optional[OrderExpiryReason]): Reason for expiry/cancellation.
        origin (Optional[OrderUpdateOrigin]): Origin of the last update.
        strategy_name (Optional[str]): Optional strategy identifier.
        signal_id (Optional[str]): Optional signal identifier.
        trades (List[Trade]): List of associated trade fills.
    """

    # --- Core Identifiers ---
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

    # --- Basic Order Details ---
    exchange: str = Field(
        ...,
        description="Name of the exchange this order belongs to (e.g., 'hyperliquid', 'backpack').",
    )
    symbol: str = Field(..., description="Trading symbol (e.g., 'BTC-PERP', 'SOL_USDC').")
    side: OrderSide
    order_type: OrderType
    status: OrderStatus = Field(default=OrderStatus.NEW, description="Current status of the order.")

    # --- Quantities ---
    quantity_requested: Decimal = Field(
        ..., gt=0, description="Requested order quantity (must be positive)."
    )
    quantity_filled: Decimal = Field(
        default=Decimal("0.0"), ge=0, description="Total filled quantity (non-negative)."
    )
    executed_quote_quantity: Decimal | None = Field(
        None, ge=0, description="Filled quote quantity (from Backpack history/WS)."
    )

    # --- Pricing ---
    price: Decimal | None = Field(
        default=None, description="Limit price (positive if set). Used for LIMIT types."
    )
    stop_price: Decimal | None = Field(
        default=None, description="Stop trigger price (positive if set). Required for STOP types."
    )
    average_fill_price: Decimal | None = Field(
        default=None, description="Weighted average fill price (positive if filled > 0)."
    )

    # --- Conditional & Modifier Parameters ---
    trigger_by: TriggerType | None = Field(
        None, description="Reference price type for triggering stops/TPs."
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
    self_trade_prevention: SelfTradePrevention | None = Field(
        None, description="Self-trade prevention behavior (Backpack)."
    )

    # --- Timestamps & Metadata ---
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Order creation/submission time (UTC).",
    )
    updated_at: datetime | None = Field(None, description="Last status/fill update time (UTC).")
    triggered_at: datetime | None = Field(
        None, description="Time the conditional order was triggered (UTC)."
    )

    # --- Status/Lifecycle Details ---
    expiry_reason: OrderExpiryReason | None = Field(
        None, description="Reason for expiry/cancellation/rejection."
    )
    origin: OrderUpdateOrigin | None = Field(
        None, description="Origin of the last update (from Backpack WS)."
    )

    # --- Tracking & Association ---
    strategy_name: str | None = Field(None, description="Optional strategy identifier.")
    signal_id: str | None = Field(None, description="Optional signal identifier.")

    # --- Associated Executions ---
    trades: list[Trade] = Field(default_factory=list, description="List of associated trade fills.")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "quantity_requested",
        "quantity_filled",
        "price",
        "stop_price",
        "average_fill_price",
        "executed_quote_quantity",
        mode="before",
    )
    @classmethod
    def parse_decimal_fields(cls, raw_value: object, info: ValidationInfo) -> Decimal | None:
        """
        Pydantic field validator for all Decimal fields in Order.
        Uses parse_decimal_value with field_name for robust error context.
        All parsing errors will include the field name for traceability.
        Applies field-specific business rules (positivity, non-negativity, required/optional).
        Args:
            raw_value: The input value to convert to Decimal.
            info: Pydantic validation info (used for field name context).
        Returns:
            Decimal or None: The parsed decimal value or None if input is None and allowed.
        Raises:
            ValueError: If conversion fails or value is invalid for the field.
        """
        field_name = info.field_name if info.field_name is not None else ""
        field_info = cls.model_fields.get(field_name) if field_name else None
        allow_none = False
        if field_info is not None:
            annotation = getattr(field_info, "annotation", None)
            origin = typing.get_origin(annotation)
            if origin is typing.Union:
                args = typing.get_args(annotation)
                allow_none = type(None) in args
            allow_none = allow_none or (not field_info.is_required())
        # Type guard for static and runtime safety
        if not isinstance(raw_value, Decimal | str | int | float) and raw_value is not None:
            raise TypeError(
                f"{field_name}: Invalid type {type(raw_value)} for decimal field; "
                "expected Decimal, str, int, float, or None."
            )
        try:
            dec_val = parse_decimal_value(raw_value, allow_none=allow_none, field_name=field_name)
            if dec_val is not None:
                if field_name == "quantity_requested" and dec_val <= 0:
                    raise ValueError(f"Field '{field_name}' must be positive.")
                if field_name in ["quantity_filled", "executed_quote_quantity"] and dec_val < 0:
                    raise ValueError(f"Field '{field_name}' cannot be negative.")
                if field_name in ["price", "stop_price", "average_fill_price"] and dec_val <= 0:
                    raise ValueError(f"Field '{field_name}' must be positive if set.")
            elif not allow_none:
                raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
            return dec_val
        except ValueError:
            raise

    @field_validator("created_at", "updated_at", "triggered_at", mode="before")
    @classmethod
    def parse_datetime_fields(cls, raw_value: object, info: ValidationInfo) -> datetime | None:
        """
        Pydantic field validator for all datetime fields in Order.
        Uses parse_datetime_utc with field_name for robust error context.
        All parsing errors will include the field name for traceability.
        Applies field-specific business rules (required/optional).
        Args:
            raw_value: The input value to convert to datetime.
            info: Pydantic validation info (used for field name context).
        Returns:
            datetime or None: The parsed datetime value or None if input is None and allowed.
        Raises:
            ValueError: If conversion fails or value is invalid for the field.
        """
        field_name = info.field_name if info.field_name is not None else ""
        field_info = cls.model_fields.get(field_name) if field_name else None
        allow_none = False
        if field_info is not None:
            annotation = getattr(field_info, "annotation", None)
            origin = typing.get_origin(annotation)
            if origin is typing.Union:
                args = typing.get_args(annotation)
                allow_none = type(None) in args
            allow_none = allow_none or (not field_info.is_required())
        # Type guard for static and runtime safety
        if not isinstance(raw_value, datetime | int | float | str) and raw_value is not None:
            raise TypeError(
                f"{field_name}: Invalid type {type(raw_value)} for datetime field; "
                "expected datetime, int, float, str, or None."
            )
        try:
            parsed = parse_datetime_utc(raw_value, field_name=field_name)
            if parsed is None and not allow_none:
                raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
            return parsed
        except ValueError:
            raise

    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        limit_types = {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}
        market_types = {OrderType.MARKET, OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET}

        if self.order_type in limit_types and self.price is None:
            raise ValueError(
                f"Order type {self.order_type} requires a positive price. Field 'price' is None."
            )
        if self.order_type in market_types and self.price is not None:
            logger.warning(
                f"Order {self.client_order_id} ({self.order_type}) has price {self.price}; "
                "price should be None for market order types. Check order construction logic."
            )

        stop_types = {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}
        if self.order_type in stop_types and self.stop_price is None:
            raise ValueError(
                f"Order type {self.order_type} requires a positive stop_price. "
                "Field 'stop_price' is None."
            )
        if self.order_type not in stop_types and self.stop_price is not None:
            logger.warning(
                f"Order {self.client_order_id} ({self.order_type}) has stop_price "
                f"{self.stop_price}; stop_price should be None for non-stop order types."
            )

        if self.post_only and self.order_type not in limit_types:
            logger.warning(
                f"Order {self.client_order_id}: post_only=True used with non-limit order type {self.order_type}. This may be ignored by the exchange."
            )

        if self.quantity_filled > self.quantity_requested:
            tolerance = Decimal("1e-9")
            if (self.quantity_filled - self.quantity_requested) > tolerance:
                raise ValueError(
                    f"Internal inconsistency: quantity_filled ({self.quantity_filled}) > "
                    f"quantity_requested ({self.quantity_requested})"
                )
            else:
                logger.warning(
                    f"Order {self.client_order_id}: Snapping slightly overfilled qty "
                    f"{self.quantity_filled} to requested {self.quantity_requested}. "
                    "This may indicate a rounding issue."
                )
                object.__setattr__(self, "quantity_filled", self.quantity_requested)

        if self.quantity_filled > 0 and self.average_fill_price is None and self.trades:
            logger.warning(
                f"Order {self.client_order_id}: filled > 0 but average_fill_price is None. "
                "Check trade fill logic."
            )
        if self.average_fill_price is not None and self.average_fill_price <= 0:
            raise ValueError("Average fill price must be positive.")

        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)

        return self

    def add_trade(self, trade: Trade) -> None:
        if trade.order_id and self.exchange_order_id and trade.order_id != self.exchange_order_id:
            raise ValueError("Trade order_id does not match this order's exchange_order_id")
        if trade.client_order_id and trade.client_order_id != self.client_order_id:
            raise ValueError("Trade client_order_id does not match this order's client_order_id")
        if trade.symbol != self.symbol or (trade.side and trade.side != self.side):
            raise ValueError("Trade details mismatch order details")

        current_total_value = (self.average_fill_price or Decimal(0)) * self.quantity_filled
        new_total_value = current_total_value + (trade.price * trade.quantity)
        new_quantity_filled = self.quantity_filled + trade.quantity

        if new_quantity_filled > 0:
            self.average_fill_price = new_total_value / new_quantity_filled
        else:
            self.average_fill_price = None

        self.quantity_filled = new_quantity_filled
        self.trades.append(trade)
        self.updated_at = datetime.now(UTC)

        tolerance = Decimal("1e-9")
        if self.status not in [
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        ]:
            if abs(self.quantity_filled - self.quantity_requested) < tolerance:
                self.status = OrderStatus.FILLED
            elif self.quantity_filled > 0:
                self.status = OrderStatus.PARTIALLY_FILLED

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Order to a dictionary, serializing Decimals, Enums, datetimes, and
        nested Trades.
        Returns:
            dict[str, Any]: Dictionary representation of the order.
        """
        data = self.model_dump(exclude={"trades"})
        data["trades"] = [trade.model_dump(mode="json") for trade in self.trades]
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, Enum):
                data[key] = value.value
        return data
