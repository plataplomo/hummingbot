from __future__ import annotations  # Enable postponed evaluation

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
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
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal:
        dec = parse_decimal_value(v)
        if dec is None:
            raise ValueError(
                f"Field '{getattr(info, 'field_name', '<unknown>')}' cannot be None or invalid."
            )
        return dec

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_datetime(cls, v: str | int | float | datetime | None, info: object) -> datetime:
        dt = parse_datetime_utc(v)
        if dt is None:
            raise ValueError(
                f"Field '{getattr(info, 'field_name', '<unknown>')}' cannot be None or invalid."
            )
        return dt

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
    Balance models the account balance for a single asset (e.g., USDC, BTC) on an exchange. It is
    immutable (frozen=True) to ensure that balance snapshots reflect the exact state at the time of
    retrieval and are not accidentally mutated.

    Fields:
        asset (str): The asset/currency symbol.
        total (Decimal): Total balance for the asset.
        available (Decimal | None): Amount available for trading (may be None,
            defaults to total).
        free (Decimal | None): Unlocked/free amount (may be None, defaults to 0.0).
        locked (Decimal | None): Amount locked in orders or for margin (may be None,
            defaults to 0.0).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use is_active() to check if the balance is nonzero.
        - This model is not intended for mutation after creation.
    """

    asset: str
    total: Decimal
    available: Decimal | None = None
    free: Decimal | None = None
    locked: Decimal | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("total", "available", "free", "locked", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        return parse_decimal_value(v)

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
    Position represents the current net holding or exposure in a specific asset or contract on an
    exchange. Unlike most models here, Position is mutable because it aggregates and updates state
    as new trades occur, reflecting the live position for risk and PnL tracking.

    Fields:
        symbol (str): Trading symbol (e.g., 'BTC-PERP').
        side (OrderSide): Net side (long/buy or short/sell).
        size (Decimal): Net quantity held (positive for long, negative for short).
        entry_price (Decimal): Average entry price for the current position
            (must be positive if open).
        leverage (Decimal | None): Leverage used, if applicable.
        id (str | None): Optional unique identifier.
        status (str | None): Optional status string.
        mark_price (Decimal | None): Current mark price.
        liquidation_price (Decimal | None): Liquidation price.
        unrealized_pnl (Decimal | None): Current unrealized PnL.
        realized_pnl (Decimal | None): Realized PnL from closed portions.
        margin_type (str | None): Margin type (e.g., 'cross', 'isolated').
        margin_used (Decimal | None): Margin used for the position.
        timestamp (int | None): Optional timestamp.
        strategy_name (str | None): Optional strategy identifier.
        close_price (Decimal | None): Price at which the position was closed.
        close_time (datetime | None): Time at which the position was closed.
        pnl (Decimal | None): Total PnL for the position.

    Notes:
        - All financial fields use Decimal for precision.
        - Use is_active() to check if the position is open.
        - Use calculate_unrealized_pnl() for live PnL updates.
        - This model is mutable by design for real-time state tracking.
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
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        return parse_decimal_value(v)

    def to_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
        return d


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
        cls, v: list[tuple[str | int | float | Decimal, str | int | float | Decimal]], info: object
    ) -> list[tuple[Decimal, Decimal]]:
        result: list[tuple[Decimal, Decimal]] = []
        for i, level in enumerate(v):
            if len(level) != 2:
                raise ValueError(
                    f"Invalid item in '{getattr(info, 'field_name', '<unknown>')}' "
                    f"at index {i}: {level}"
                )
            price = parse_decimal_value(level[0])
            quantity = parse_decimal_value(level[1])
            if price is None or quantity is None:
                raise ValueError(
                    f"Invalid price/quantity in '{getattr(info, 'field_name', '<unknown>')}' "
                    f"at index {i}: {level}"
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
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        return parse_decimal_value(v)


class ArbitrageOpportunity(BaseModel):
    """
    ArbitrageOpportunity represents a funding rate arbitrage opportunity between two exchanges for a
    given symbol. This model is mutable because it may be updated with analytics, sizing, or
    confidence scores after initial creation.

    Fields:
        symbol (str): Trading symbol.
        long_exchange (str): Exchange to go long.
        short_exchange (str): Exchange to go short.
        long_price (Decimal): Long entry price (must be positive).
        short_price (Decimal): Short entry price (must be positive).
        long_funding_rate (Decimal): Funding rate on long exchange.
        short_funding_rate (Decimal): Funding rate on short exchange.
        net_funding_differential (Decimal): Net funding advantage (long - short).
        timestamp (datetime): UTC timestamp of opportunity detection.
        expected_profit (Decimal | None): Optional expected profit.
        basis_volatility (float | None): Optional basis volatility metric.
        utility_score (float | None): Optional utility score for ranking.
        optimal_size (Decimal | None): Optional optimal trade size.
        confidence (float | None): Optional confidence score.
        expiration_timestamp (float | None): Optional expiry (epoch seconds).
        id (str): Unique identifier (UUID).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use this model for opportunity tracking, analytics, and strategy input.
        - This model is mutable to allow enrichment after creation.
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
    # Optional fields: may not be available at opportunity creation
    expected_profit: Decimal | None = None
    basis_volatility: float | None = None
    utility_score: float | None = None
    optimal_size: Decimal | None = Field(
        default=None, gt=0, description="Optimal size, if calculated by risk/position sizing."
    )
    confidence: float | None = None  # Optional: model-derived or subjective score
    expiration_timestamp: float | None = None  # Optional: may be set by strategy or downstream
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
        # All required fields are enforced by Pydantic; no need to check for None.
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
        # No need to check for None or always-true conditions on required fields
        return self


class TradeSignal(BaseModel):
    """
    TradeSignal represents a decision or actionable signal generated by a trading strategy. It is
    mutable to allow enrichment with metadata, tracking, or downstream annotations.

    Fields:
        symbol (str): Trading symbol.
        signal_type (SignalType): Type of signal (e.g., ENTRY, EXIT).
        side (OrderSide): Buy or sell.
        price (Decimal): Signal price (must be positive).
        quantity (Decimal): Signal quantity (must be positive).
        timestamp (datetime | None): Optional signal creation time.
        confidence (float | None): Optional confidence score.
        source_strategy (str | None): Optional strategy identifier.
        stop_loss (Decimal | None): Optional stop loss price.
        take_profit (Decimal | None): Optional take profit price.
        expiration (datetime | None): Optional expiry time.
        metadata (dict[str, Any] | None): Optional extra metadata.
        signal_id (str): Unique identifier (UUID).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use is_valid() to check if the signal is still actionable.
        - This model is mutable to support enrichment and tracking.
    """

    symbol: str
    signal_type: SignalType
    side: OrderSide
    price: Decimal  # Required if always provided for actionable signals
    quantity: Decimal  # Required if always provided for actionable signals
    # Optional fields
    timestamp: datetime | None = None  # Optional: can default to now if not set
    confidence: float | None = None  # Optional: model-derived or subjective score
    source_strategy: str | None = None  # Optional: for tracking
    stop_loss: Decimal | None = Field(
        default=None, gt=0, description="Stop loss, if set by strategy."
    )
    take_profit: Decimal | None = Field(
        default=None, gt=0, description="Take profit, if set by strategy."
    )
    expiration: datetime | None = None  # Optional: signal expiry
    metadata: dict[str, Any] | None = None  # Optional: extra info
    signal_id: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )  # Always present after creation

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
        Enforce that actionable signals require a price.
        """
        if self.price <= 0:
            raise ValueError("Signal price must be positive.")
        if self.quantity <= 0:
            raise ValueError("Signal quantity must be positive.")
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
    Order models a trading order instruction and its lifecycle state within the CyberDeltaEngine. It
    is mutable because it tracks the order from creation through all possible states and aggregates
    all associated trades (fills), supporting real-time state management and reconciliation.

    Fields:
        client_order_id (str): Client-generated unique order ID (UUID).
        exchange_order_id (str | None): Exchange-provided order ID.
        symbol (str): Trading symbol.
        side (OrderSide): Buy or sell.
        order_type (OrderType): Order type (e.g., MARKET, LIMIT).
        status (OrderStatus): Current order status (default NEW).
        quantity_requested (Decimal): Requested order quantity (must be positive).
        quantity_filled (Decimal): Total filled quantity (cannot be negative).
        price (Decimal | None): Limit price (if applicable).
        average_fill_price (Decimal | None): Average fill price (if applicable).
        created_at (datetime): Order creation time (UTC).
        updated_at (datetime | None): Last update time.
        trades (list[Trade]): List of associated trade fills.
        strategy_name (str | None): Optional strategy identifier.
        signal_id (str | None): Optional signal identifier.

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use add_trade() to aggregate fills and update order state.
        - This model is mutable to support real-time order management.
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
        Also enforces business rules for order_type and price.
        """
        if self.quantity_requested <= 0:
            raise ValueError("quantity_requested must be positive")
        if self.quantity_filled < 0:
            raise ValueError("quantity_filled cannot be negative")
        if self.quantity_filled > self.quantity_requested:
            raise ValueError("quantity_filled cannot exceed quantity_requested")
        if self.order_type == OrderType.MARKET and self.price is not None:
            raise ValueError("Market orders should not have a price.")
        if (
            self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT)
            and self.price is None
        ):
            raise ValueError(f"Order type {self.order_type} requires a price.")
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
