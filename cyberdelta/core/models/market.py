"""
Exchange Models for CyberDeltaEngine

This module contains all data models that directly represent exchange state, market data,
order book snapshots, funding rates, and trading actions (orders and trades) as seen by the
exchange APIs. These models are used for parsing, validation, and downstream processing of
exchange data.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

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
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class MarketData(BaseModel):
    """
    MarketData represents a snapshot of market information for a specific trading symbol,
    including OHLCV (Open, High, Low, Close, Volume) and optional ticker data. This model is
    immutable (frozen=True) to ensure that once market data is captured from an exchange or data
    provider, it cannot be altered, preserving auditability and data integrity.

    Fields:
        symbol (str): The trading symbol (e.g., 'BTC-PERP').
        timestamp (datetime): The UTC timestamp of the data snapshot.
        open (Decimal): Opening price for the period.
        high (Decimal): Highest price for the period.
        low (Decimal): Lowest price for the period.
        close (Decimal): Closing price for the period.
        volume (Decimal): Trading volume for the period (default 0.0).
        ticker_data (dict[str, dict[str, Ticker]] | None): Optional nested ticker data for advanced
            analytics or multi-venue aggregation.

    Notes:
        - All price and volume fields use Decimal for precision (see Decimal usage rule).
        - This model is not intended for mutation after creation; use a new instance for new data.
    """

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0.0")
    ticker_data: dict[str, dict[str, Ticker]] | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def parse_decimal(cls, raw_value: str | int | float | Decimal | None, info: object) -> Decimal:
        """
        Validator for decimal fields in MarketData.
        Converts the input to Decimal using parse_decimal_value.
        Raises ValueError if conversion fails or value is None.
        Args:
            raw_value: The input value to convert to Decimal.
            info: Pydantic validation info (used for field name context).
        Returns:
            Decimal: The parsed decimal value.
        """
        decimal_value = parse_decimal_value(raw_value)
        if decimal_value is None:
            raise ValueError(
                f"Field '{getattr(info, 'field_name', '<unknown>')}' cannot be None or invalid."
            )
        return decimal_value

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_datetime(
        cls, raw_value: str | int | float | datetime | None, info: object
    ) -> datetime:
        """
        Validator for timestamp field in MarketData.
        Converts the input to a UTC datetime using parse_datetime_utc.
        Raises ValueError if conversion fails or value is None.
        Args:
            raw_value: The input value to convert to datetime.
            info: Pydantic validation info (used for field name context).
        Returns:
            datetime: The parsed UTC datetime value.
        """
        datetime_value = parse_datetime_utc(raw_value)
        if datetime_value is None:
            raise ValueError(
                f"Field '{getattr(info, 'field_name', '<unknown>')}' cannot be None or invalid."
            )
        return datetime_value

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the MarketData instance to a dictionary, serializing Decimals and datetimes.
        Returns:
            dict[str, Any]: Dictionary representation of the market data.
        """
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


class Ticker(BaseModel):
    """
    Ticker provides a lightweight snapshot of the best bid/ask, last price, and volume for a symbol.
    It is immutable (frozen=True) to ensure that ticker data reflects the exact state at the time of
    retrieval.

    Fields:
        symbol (str): Trading symbol.
        price (Decimal | None): Last traded price.
        bid (Decimal | None): Best bid price.
        ask (Decimal | None): Best ask price.
        volume (Decimal | None): Trading volume.
        timestamp (int | None): Optional timestamp.

    Notes:
        - All price/volume fields use Decimal for precision.
        - This model is not intended for mutation after creation.
    """

    symbol: str
    price: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    volume: Decimal | None = None
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def parse_decimal(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        """
        Validator for decimal fields in Ticker.
        Converts the input to Decimal using parse_decimal_value.
        Args:
            raw_value: The input value to convert to Decimal.
            info: Pydantic validation info.
        Returns:
            Decimal or None: The parsed decimal value or None if input is None.
        """
        return parse_decimal_value(raw_value)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Ticker instance to a dictionary, serializing Decimals.
        Returns:
            dict[str, Any]: Dictionary representation of the ticker.
        """
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
        return data


class OrderBook(BaseModel):
    """
    OrderBook represents the current state of the order book for a symbol, including all bid and ask
    levels. It is immutable (frozen=True) to ensure that order book snapshots are reliable and
    auditable.

    Fields:
        symbol (str): Trading symbol.
        bids (list[tuple[Decimal, Decimal]]): List of (price, quantity) tuples for bids.
        asks (list[tuple[Decimal, Decimal]]): List of (price, quantity) tuples for asks.
        timestamp (int | None): Optional timestamp.

    Notes:
        - All price/quantity fields use Decimal for accuracy.
        - This model is not intended for mutation after creation.
    """

    symbol: str
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def parse_levels(
        cls,
        raw_levels: list[tuple[str | int | float | Decimal, str | int | float | Decimal]],
        info: object,
    ) -> list[tuple[Decimal, Decimal]]:
        """
        Validator for order book levels.
        Converts each price/quantity pair to Decimals, validates structure.
        Args:
            raw_levels: List of (price, quantity) pairs to convert.
            info: Pydantic validation info (used for field name context).
        Returns:
            list[tuple[Decimal, Decimal]]: List of validated (price, quantity) pairs.
        Raises:
            ValueError: If any entry is not a valid pair or cannot be converted.
        """
        result: list[tuple[Decimal, Decimal]] = []
        for field_index, level in enumerate(raw_levels):
            if len(level) != 2:
                raise ValueError(
                    f"Invalid item in '{getattr(info, 'field_name', '<unknown>')}' "
                    f"at index {field_index}: {level}"
                )
            price = parse_decimal_value(level[0])
            quantity = parse_decimal_value(level[1])
            if price is None or quantity is None:
                raise ValueError(
                    f"Invalid price/quantity in '{getattr(info, 'field_name', '<unknown>')}' "
                    f"at index {field_index}: {level}"
                )
            result.append((price, quantity))
        return result


class FundingRate(BaseModel):
    """
    FundingRate models the funding rate and related data for a perpetual contract. It is immutable
    (frozen=True) to ensure that funding data is not altered after retrieval.

    Fields:
        symbol (str): Trading symbol.
        funding_rate (Decimal | None): Current funding rate.
        predicted_rate (Decimal | None): Predicted next funding rate.
        mark_price (Decimal | None): Current mark price.
        index_price (Decimal | None): Current index price.
        next_funding_time (int | None): Timestamp of next funding event.
        timestamp (int | None): Data snapshot timestamp.
        historical_rates (list[dict[str, Any]] | None): Optional historical funding data.

    Notes:
        - All rate/price fields use Decimal for precision.
        - This model is not intended for mutation after creation.
    """

    symbol: str
    funding_rate: Decimal | None = None
    predicted_rate: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    next_funding_time: int | None = None
    timestamp: int | None = None
    historical_rates: list[dict[str, Any]] | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("funding_rate", "predicted_rate", "mark_price", "index_price", mode="before")
    @classmethod
    def parse_decimal(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        """
        Validator for decimal fields in FundingRate.
        Converts the input to Decimal using parse_decimal_value.
        Args:
            raw_value: The input value to convert to Decimal.
            info: Pydantic validation info.
        Returns:
            Decimal or None: The parsed decimal value or None if input is None.
        """
        return parse_decimal_value(raw_value)


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
        """
        Validator for executed_at field in Trade.
        Converts the input to a UTC datetime using parse_datetime_utc.
        Args:
            raw_value: The input value to convert to datetime.
            info: Pydantic validation info.
        Returns:
            datetime: The parsed UTC datetime value.
        Raises:
            ValueError: If conversion fails or value is None.
        """
        datetime_value = parse_datetime_utc(raw_value)
        if datetime_value is None:
            raise ValueError("executed_at cannot be None")
        return datetime_value

    @field_validator("price", "quantity", "cost", "fee", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal:
        """
        Validator for price, quantity, cost, and fee fields in Trade.
        Converts the input to Decimal using parse_decimal_value.
        Args:
            raw_value: The input value to convert to Decimal.
            info: Pydantic validation info.
        Returns:
            Decimal: The parsed decimal value.
        Raises:
            ValueError: If conversion fails or value is None (except fee, which defaults to 0).
        """
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
        """
        Ensure all required fields are present and logically valid.
        - fee_asset must be present if fee != 0.
        - fee can be negative in rare cases (rebates/promotions).
        """
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
        """
        Convert the Trade to a dictionary, serializing Decimals, Enums, and datetimes appropriately.
        Returns:
            dict[str, Any]: Dictionary representation of the trade.
        """
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, OrderSide):
                data[key] = value.value
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


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
            is_optional = annotation is not None and ("| None" in str(annotation))
            allow_none = is_optional or not field_info.is_required()
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
            allow_none = annotation is not None and ("| None" in str(annotation))
        try:
            parsed = parse_datetime_utc(raw_value, field_name=field_name)
            if parsed is None and not allow_none:
                raise ValueError(f"Field '{field_name}' cannot be None or invalid.")
            return parsed
        except ValueError:
            raise

    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        limit_types = {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}
        market_types = {OrderType.MARKET, OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET}

        if self.order_type in limit_types and self.price is None:
            raise ValueError(f"Order type {self.order_type} requires a positive price.")
        if self.order_type in market_types and self.price is not None:
            logger.warning(
                f"Order {self.client_order_id} ({self.order_type}) has price {self.price}; should be None for market types."
            )

        stop_types = {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}
        if self.order_type in stop_types and self.stop_price is None:
            raise ValueError(f"Order type {self.order_type} requires a positive stop_price.")
        if self.order_type not in stop_types and self.stop_price is not None:
            logger.warning(
                f"Order {self.client_order_id} ({self.order_type}) has stop_price {self.stop_price}; should be None."
            )

        if self.post_only and self.order_type not in limit_types:
            logger.warning(f"post_only=True used with non-limit order type {self.order_type}.")

        if self.quantity_filled > self.quantity_requested:
            tolerance = Decimal("1e-9")
            if (self.quantity_filled - self.quantity_requested) > tolerance:
                raise ValueError(
                    f"Internal inconsistency: quantity_filled ({self.quantity_filled}) > "
                    f"quantity_requested ({self.quantity_requested})"
                )
            else:
                logger.warning(
                    f"Snapping slightly overfilled qty {self.quantity_filled} to requested "
                    f"{self.quantity_requested} for {self.client_order_id}"
                )
                object.__setattr__(self, "quantity_filled", self.quantity_requested)

        if self.quantity_filled > 0 and self.average_fill_price is None and self.trades:
            logger.warning(
                f"Order {self.client_order_id} filled > 0 but average_fill_price is None."
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
        Convert the Order to a dictionary, serializing Decimals, Enums, datetimes, and nested Trades.
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
