from __future__ import annotations  # Enable postponed evaluation

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, SignalType
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)

# Set Decimal precision globally if desired, or manage context locally
# getcontext().prec = 28


class MarketData(BaseModel):
    """
    Represents market data for a symbol, including OHLCV information.
    """

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0.0")
    ticker_data: dict[str, dict[str, Ticker]] | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal:
        field_name = getattr(info, "field_name", "<unknown>")
        if v is None:
            raise ValueError(f"Field '{field_name}' cannot be None")
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{field_name}': cannot convert value '{v}' to Decimal: {e}"
            ) from e

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_datetime(cls, v: str | int | float | datetime | None) -> datetime:
        if v is None:
            raise ValueError("timestamp cannot be None")
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=UTC)
        if isinstance(v, int | float):
            return datetime.fromtimestamp(v, tz=UTC)
        try:
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError as e:
            raise ValueError(f"timestamp: cannot parse datetime string '{v}': {e}") from e
        raise ValueError(f"timestamp: unsupported type {type(v)}")

    def to_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
        return d


class Balance(BaseModel):
    """
    Represents an account balance for a single asset.
    """

    asset: str
    total: Decimal
    available: Decimal | None = None
    free: Decimal | None = None
    locked: Decimal | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("total", "available", "free", "locked", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        field_name = getattr(info, "field_name", "<unknown>")
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{field_name}': cannot convert value '{v}' to Decimal: {e}"
            ) from e

    @model_validator(mode="after")
    def set_defaults(self) -> Self:
        if self.available is None:
            object.__setattr__(self, "available", self.total)
        if self.free is None:
            object.__setattr__(self, "free", Decimal("0.0"))
        if self.locked is None:
            object.__setattr__(self, "locked", Decimal("0.0"))
        return self

    def to_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
        return d

    @property
    def quantity(self) -> Decimal:
        return self.total

    def is_active(self) -> bool:
        return self.total > Decimal("0.0")

    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        if self.asset in ["USD", "USDC", "USDT"]:
            return Decimal("0.0")
        return Decimal("0.0")


class Position(BaseModel):
    """
    Represents the current net holding or exposure (position)
        in a specific asset/contract on a specific exchange.

    This model aggregates the result of all trades for a symbol and side,
        and is updated as new trades occur.

    Attributes:
        symbol (str): The trading symbol (e.g., 'BTC-PERP').
        side (OrderSide): The current net side (long/buy or short/sell).
        size (Decimal): The current net quantity held (positive for long, negative for short).
        entry_price (Decimal): The average entry price for the current position.
        leverage (Decimal | None): Leverage used for the position, if applicable.
        id (str | None): Optional unique identifier for the position.
        status (str | None): Optional status string.
        mark_price (Decimal | None): Current mark price for the symbol.
        liquidation_price (Decimal | None): Liquidation price for the position.
        unrealized_pnl (Decimal | None): Current unrealized profit and loss.
        realized_pnl (Decimal | None): Realized profit and loss from closed portions.
        margin_type (str | None): Margin type (e.g., 'cross', 'isolated').
        margin_used (Decimal | None): Margin used for the position.
        timestamp (int | None): Optional timestamp for the position.
        strategy_name (str | None): Optional strategy identifier.
        close_price (Decimal | None): Price at which the position was closed.
        close_time (datetime | None): Time at which the position was closed.
        pnl (Decimal | None): Total profit and loss for the position.
    """

    symbol: str
    side: OrderSide
    size: Decimal
    entry_price: Decimal = Field(
        gt=0, description="Entry price must be positive if position is open."
    )
    leverage: Decimal | None = None
    id: str | None = None
    status: str | None = None
    mark_price: Decimal | None = None
    liquidation_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    margin_type: str | None = None
    margin_used: Decimal | None = None
    timestamp: int | None = None
    strategy_name: str | None = None
    close_price: Decimal | None = None
    close_time: datetime | None = None
    pnl: Decimal | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "size",
        "entry_price",
        "leverage",
        "mark_price",
        "liquidation_price",
        "unrealized_pnl",
        "realized_pnl",
        "margin_used",
        "close_price",
        "pnl",
        mode="before",
    )
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("close_time", mode="before")
    @classmethod
    def parse_datetime(
        cls, v: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        return parse_datetime_utc(v)

    def is_active(self) -> bool:
        """
        Returns True if the position is currently open (size != 0), False otherwise.
        """
        return self.size != Decimal("0")

    def calculate_unrealized_pnl(self, current_mark_price: Decimal | None = None) -> Decimal | None:
        """
        Calculate the unrealized PnL for the position based on the current mark price.
        If no mark price is provided, returns the stored unrealized_pnl.

        Args:
            current_mark_price (Decimal | None): The current mark price for the symbol.
        Returns:
            Decimal | None: The calculated or stored unrealized PnL.
        """
        if current_mark_price is None:
            return self.unrealized_pnl
        if self.size == Decimal("0"):
            return Decimal("0.0")
        if self.side == OrderSide.BUY:
            pnl = (current_mark_price - self.entry_price) * self.size
        else:
            pnl = (self.entry_price - current_mark_price) * self.size
        object.__setattr__(self, "unrealized_pnl", pnl)
        return pnl

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Position to a dictionary, serializing Decimals, Enums, and
        datetimes appropriately.
        Returns:
            dict[str, Any]: Dictionary representation of the position.
        """
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
            elif isinstance(v, OrderSide):
                d[k] = v.value
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    @model_validator(mode="after")
    def check_position_logic(self) -> Self:
        """
        Ensure entry_price is positive if size is not zero. Optionally, check side/size consistency.
        """
        if self.size != Decimal("0") and self.entry_price <= 0:
            raise ValueError("Entry price must be positive if position is open.")
        # Example: if self.side == OrderSide.BUY and self.size < 0: ...
        return self


class Trade(BaseModel):
    """
    Represents a single execution event (fill) that occurs against an order.
    All core fields are required for auditability and downstream processing.
    - 'side', 'order_id', 'exchange', 'cost', 'client_order_id', and 'fee' are always required.
    - 'fee_asset' is required if 'fee' is nonzero, else may be an empty string.
    - 'is_maker' is Optional due to upstream API variability.
    - 'timestamp' is retained as an optional raw value and excluded from serialization by default.
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
        default=Decimal("0"), ge=0, description="Fee paid for this trade. Defaults to 0."
    )
    fee_asset: str = Field(
        default="", description="Asset in which the fee was paid. Required if fee != 0."
    )
    is_maker: bool | None = Field(
        default=None, description="True if maker fill, False if taker, None if unknown."
    )
    timestamp: int | None = Field(default=None, exclude=True)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("executed_at", mode="before")
    @classmethod
    def parse_executed_at(cls, v: str | int | float | datetime | None, info: object) -> datetime:
        dt = parse_datetime_utc(v)
        if dt is None:
            raise ValueError("executed_at cannot be None")
        return dt

    @field_validator("price", "quantity", "cost", "fee", mode="before")
    @classmethod
    def parse_decimal_fields(cls, v: str | int | float | Decimal | None, info: object) -> Decimal:
        field_name = getattr(info, "field_name", "")
        if v is None:
            if field_name == "fee":
                return Decimal("0")
            raise ValueError(f"Field '{field_name}' is required and cannot be None.")
        dec = parse_decimal_value(v)
        if dec is None:
            raise ValueError(
                f"Field '{field_name}' could not be parsed to Decimal and is required."
            )
        return dec

    @model_validator(mode="after")
    def check_trade_logic(self) -> Self:
        """
        Ensure all required fields are present and logically valid.
        - fee_asset must be present if fee != 0.
        """
        if self.price <= 0:
            raise ValueError("Trade price must be positive.")
        if self.quantity <= 0:
            raise ValueError("Trade quantity must be positive.")
        if self.cost <= 0:
            raise ValueError("Trade cost must be positive.")
        if self.fee < 0:
            raise ValueError("Trade fee cannot be negative.")
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
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
            elif isinstance(v, OrderSide):
                d[k] = v.value
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
        return d


class Ticker(BaseModel):
    """
    Represents ticker information for a symbol.
    """

    symbol: str
    price: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    volume: Decimal | None = None
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        field_name = getattr(info, "field_name", "<unknown>")
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{field_name}': cannot convert value '{v}' to Decimal: {e}"
            ) from e

    def to_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
        return d


class OrderBook(BaseModel):
    """
    Represents an order book for a symbol.
    """

    symbol: str
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def parse_levels(
        cls, v: list[tuple[str | int | float | Decimal, str | int | float | Decimal]], info: object
    ) -> list[tuple[Decimal, Decimal]]:
        field_name = getattr(info, "field_name", "<unknown>")
        result: list[tuple[Decimal, Decimal]] = []
        for i, level in enumerate(v):
            if len(level) != 2:
                raise ValueError(f"Invalid item in '{field_name}' at index {i}: {level}")
            try:
                price: Decimal = Decimal(str(level[0]))
                quantity: Decimal = Decimal(str(level[1]))
                result.append((price, quantity))
            except (InvalidOperation, TypeError, IndexError) as err:
                raise ValueError(
                    f"Invalid price/quantity in '{field_name}' at index {i}: {level}"
                ) from err
        return result


class FundingRate(BaseModel):
    """
    Represents funding rate information for a perpetual contract.
    """

    symbol: str
    funding_rate: Decimal | None = None
    predicted_rate: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    next_funding_time: int | None = None
    timestamp: int | None = None
    historical_rates: list[dict[str, Any]] | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("funding_rate", "predicted_rate", "mark_price", "index_price", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        field_name = getattr(info, "field_name", "<unknown>")
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{field_name}': cannot convert value '{v}' to Decimal: {e}"
            ) from e


class ArbitrageOpportunity(BaseModel):
    """
    Represents a funding rate arbitrage opportunity between two exchanges for a given symbol.
    All financial fields are validated and parsed as Decimals for precision.
    """

    symbol: str
    long_exchange: str
    short_exchange: str
    long_price: Decimal = Field(gt=0, description="Long price must be positive.")
    short_price: Decimal = Field(gt=0, description="Short price must be positive.")
    long_funding_rate: Decimal
    short_funding_rate: Decimal
    net_funding_differential: Decimal
    timestamp: datetime
    optimal_size: Decimal | None = Field(
        default=None, gt=0, description="Optimal size must be positive if present."
    )
    expected_profit: Decimal | None = Field(
        default=None, description="Expected profit can be negative (loss)."
    )
    confidence: float | None = None
    basis_volatility: float | None = None
    utility_score: float | None = None
    expiration_timestamp: float
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "long_price",
        "short_price",
        "long_funding_rate",
        "short_funding_rate",
        "net_funding_differential",
        "optimal_size",
        "expected_profit",
        mode="before",
    )
    @classmethod
    def parse_decimal_fields(
        cls, v: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str | int | float | datetime | None, info: object) -> datetime:
        dt = parse_datetime_utc(v)
        if dt is None:
            raise ValueError("timestamp cannot be None")
        return dt

    @model_validator(mode="after")
    def set_expiration(self) -> Self:
        """
        Set expiration_timestamp to 1 hour after timestamp (UTC).
        """
        if self.timestamp.tzinfo:
            object.__setattr__(self, "expiration_timestamp", self.timestamp.timestamp() + 3600)
        else:
            object.__setattr__(
                self, "expiration_timestamp", self.timestamp.replace(tzinfo=UTC).timestamp() + 3600
            )
        return self

    @model_validator(mode="after")
    def validate_required_fields(self) -> Self:
        required_fields = [
            "long_price",
            "short_price",
            "long_funding_rate",
            "short_funding_rate",
            "net_funding_differential",
        ]
        missing = [f for f in required_fields if getattr(self, f) is None]
        if missing:
            raise ValueError(f"Required fields {', '.join(missing)} are None after conversion")
        return self

    @model_validator(mode="after")
    def check_arbitrage_logic(self) -> Self:
        """
        Ensure all required financial fields are positive where appropriate.
        """
        if self.long_price <= 0:
            raise ValueError("Long price must be positive.")
        if self.short_price <= 0:
            raise ValueError("Short price must be positive.")
        if self.optimal_size is not None and self.optimal_size <= 0:
            raise ValueError("Optimal size must be positive if present.")
        return self


class TradeSignal(BaseModel):
    """
    Represents a decision signal generated by a strategy.
    All financial fields are validated and parsed as Decimals for precision.
    """

    symbol: str
    signal_type: SignalType
    side: OrderSide
    price: Decimal | None = Field(
        default=None, gt=0, description="Signal price must be positive if present."
    )
    quantity: Decimal | None = Field(
        default=None, gt=0, description="Signal quantity must be positive if present."
    )
    timestamp: datetime | None = None
    confidence: float | None = None
    source_strategy: str | None = None
    stop_loss: Decimal | None = Field(
        default=None, gt=0, description="Stop loss must be positive if present."
    )
    take_profit: Decimal | None = Field(
        default=None, gt=0, description="Take profit must be positive if present."
    )
    expiration: datetime | None = None
    metadata: dict[str, Any] | None = None
    signal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("price", "quantity", "stop_loss", "take_profit", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, v: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("timestamp", "expiration", mode="before")
    @classmethod
    def parse_datetime_fields(
        cls, v: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        return parse_datetime_utc(v)

    @model_validator(mode="after")
    def ensure_signal_id(self) -> Self:
        if not self.signal_id:
            object.__setattr__(self, "signal_id", str(uuid.uuid4()))
        return self

    @model_validator(mode="after")
    def check_signal_logic(self) -> Self:
        """
        Ensure price, quantity, stop_loss, and take_profit are positive if present.
        """
        if self.price is not None and self.price <= 0:
            raise ValueError("Signal price must be positive if present.")
        if self.quantity is not None and self.quantity <= 0:
            raise ValueError("Signal quantity must be positive if present.")
        if self.stop_loss is not None and self.stop_loss <= 0:
            raise ValueError("Stop loss must be positive if present.")
        if self.take_profit is not None and self.take_profit <= 0:
            raise ValueError("Take profit must be positive if present.")
        return self

    def is_valid(self) -> bool:
        """
        Check if the signal is still valid (e.g., not expired).
        Returns:
            bool: True if not expired, False otherwise.
        """
        if self.expiration is None:
            return True
        return datetime.now(UTC) < self.expiration


class Order(BaseModel):
    """
    Represents a trading order instruction and its lifecycle state (intent)
    within the CyberDeltaEngine.

    This model tracks the order from creation through all possible states,
    and aggregates all associated trades (fills).
    """

    client_order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    exchange_order_id: str | None = None
    symbol: str
    side: OrderSide
    order_type: OrderType
    status: OrderStatus = OrderStatus.NEW
    quantity_requested: Decimal = Field(gt=0, description="Requested quantity must be positive.")
    quantity_filled: Decimal = Field(
        default=Decimal("0.0"), ge=0, description="Filled quantity cannot be negative."
    )
    price: Decimal | None = Field(
        default=None, gt=0, description="Limit price must be positive if present."
    )
    average_fill_price: Decimal | None = Field(
        default=None, gt=0, description="Average fill price must be positive if present."
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime | None = None
    trades: list[Trade] = Field(default_factory=list)
    strategy_name: str | None = None
    signal_id: str | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "quantity_requested",
        "quantity_filled",
        "price",
        "average_fill_price",
        mode="before",
    )
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        """
        Parse and validate Decimal fields for Order.
        Ensures all financial values are stored as Decimals for precision and
        consistency. Field-specific checks (e.g., non-negativity) are enforced
        after conversion.
        """
        return parse_decimal_value(v)

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def parse_datetime(
        cls, v: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        """
        Parse and validate datetime fields for Order, ensuring UTC awareness.
        Accepts datetime, int/float (epoch seconds or ms), or ISO string.
        """
        return parse_datetime_utc(v)

    @model_validator(mode="after")
    def check_order_logic(self) -> Self:
        """
        Ensure logical consistency between fields (e.g., filled <= requested,
        price for limit orders).
        """
        if self.quantity_requested <= 0:
            raise ValueError("quantity_requested must be positive")
        if self.quantity_filled < 0:
            raise ValueError("quantity_filled cannot be negative")
        if self.quantity_filled > self.quantity_requested:
            raise ValueError("quantity_filled cannot exceed quantity_requested")
        if self.price is not None and self.price <= 0:
            raise ValueError("Order price must be positive if present.")
        if self.average_fill_price is not None and self.average_fill_price <= 0:
            raise ValueError("Average fill price must be positive if present.")
        return self

    def add_trade(self, trade: Trade) -> None:
        """
        Add a trade (fill) to this order and update aggregate state.
        Updates filled quantity, recalculates average fill price, and updates order status.
        Args:
            trade (Trade): The trade execution to add.
        Raises:
            ValueError: If the trade does not match this order's IDs or symbol/side.
        """
        if trade.order_id and self.exchange_order_id and trade.order_id != self.exchange_order_id:
            raise ValueError("Trade order_id does not match this order's exchange_order_id")
        if trade.client_order_id and trade.client_order_id != self.client_order_id:
            raise ValueError("Trade client_order_id does not match this order's client_order_id")
        if trade.symbol != self.symbol or (trade.side and trade.side != self.side):
            raise ValueError("Trade details mismatch order details")

        # Update filled quantity and recalculate average fill price
        new_total_value = (
            self.average_fill_price or Decimal(0)
        ) * self.quantity_filled + trade.price * trade.quantity
        new_quantity_filled = self.quantity_filled + trade.quantity

        if new_quantity_filled > 0:
            self.average_fill_price = new_total_value / new_quantity_filled
        else:
            self.average_fill_price = None

        self.quantity_filled = new_quantity_filled
        self.trades.append(trade)
        self.updated_at = datetime.now(UTC)

        # Update status based on fills
        if self.quantity_filled >= self.quantity_requested:
            self.status = OrderStatus.FILLED
        elif self.quantity_filled > 0:
            self.status = OrderStatus.PARTIALLY_FILLED
        # Note: Actual status updates may also come from exchange events

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Order to a dictionary, serializing Decimals, Enums, and datetimes
        appropriately.
        Returns:
            dict[str, Any]: Dictionary representation of the order.
        """
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
            elif k == "trades" and isinstance(v, list):
                trades_list = cast(list[Trade], v)
                d[k] = [t.to_dict() for t in trades_list]
        return d
