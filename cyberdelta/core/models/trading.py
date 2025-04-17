from __future__ import annotations  # Enable postponed evaluation

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)

# Set Decimal precision globally if desired, or manage context locally
# getcontext().prec = 28


class OrderSide(Enum):
    """Enum representing the side of an order (BUY or SELL)."""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Enum representing the type of an order."""

    LIMIT = "limit"
    MARKET = "market"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"


class OrderStatus(Enum):
    """Enum representing the status of an order."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    OPEN = "OPEN"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class SignalType(Enum):
    """Enum representing the type of a trading signal."""

    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"
    REBALANCE = "REBALANCE"


class TimeInForce(Enum):
    """Enum representing the time in force for an order."""

    GTC = "GTC"  # Good 'Til Canceled
    IOC = "IOC"  # Immediate Or Cancel
    FOK = "FOK"  # Fill Or Kill


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
    entry_price: Decimal
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


class Trade(BaseModel):
    """
    Represents a single execution event (fill) that occurs against an order.

    This model records the details of a specific trade/fill, including price,
    quantity, fee, and execution time.
    """

    id: str
    symbol: str
    timestamp: int
    price: Decimal
    quantity: Decimal
    side: OrderSide | None = None
    order_id: str | None = None
    exchange: str | None = None
    executed_at: datetime | None = None
    fee: Decimal | None = None
    fee_asset: str | None = None
    is_maker: bool | None = None
    client_order_id: str | None = None
    cost: Decimal | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("price", "quantity", "fee", "cost", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        """
        Parse and validate Decimal fields for Trade.
        Ensures all financial values are stored as Decimals for precision and
        consistency.
        """
        return parse_decimal_value(v)

    @field_validator("executed_at", mode="before")
    @classmethod
    def parse_datetime(
        cls, v: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        """
        Parse and validate datetime fields for Trade, ensuring UTC awareness.
        Accepts datetime, int/float (epoch seconds or ms), or ISO string.
        """
        return parse_datetime_utc(v)

    @model_validator(mode="after")
    def set_cost_and_datetime(self) -> Self:
        """
        Post-model validation to set the cost (if not provided) and parse the
        datetime from timestamp.
        Ensures cost is always available and datetime is UTC-aware.
        """
        if self.cost is None:
            object.__setattr__(self, "cost", self.price * self.quantity)
        if self.timestamp and self.executed_at is None:
            try:
                object.__setattr__(
                    self, "executed_at", datetime.fromtimestamp(self.timestamp / 1000, tz=UTC)
                )
            except (TypeError, ValueError, OSError):
                object.__setattr__(self, "executed_at", None)
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


# ArbitrageOpportunity is not a dataclass, handle conversion in __init__
class ArbitrageOpportunity:
    """Represents a funding rate arbitrage opportunity."""

    def __init__(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        long_price: str | int | float | Decimal | None,
        short_price: str | int | float | Decimal | None,
        long_funding_rate: str | int | float | Decimal | None,
        short_funding_rate: str | int | float | Decimal | None,
        net_funding_differential: str | int | float | Decimal | None,
        timestamp: datetime,
        optimal_size: str | int | float | Decimal | None = None,
        expected_profit: str | int | float | Decimal | None = None,
        confidence: str | int | float | Decimal | None = None,
        basis_volatility: str | int | float | Decimal | None = None,
        utility_score: str | int | float | Decimal | None = None,
    ) -> None:
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        self.timestamp = timestamp

        # Perform safe Decimal conversions for financial values
        self.long_price = self._safe_decimal_convert(
            long_price, "long_price", symbol, allow_none=False
        )
        self.short_price = self._safe_decimal_convert(
            short_price, "short_price", symbol, allow_none=False
        )
        self.long_funding_rate = self._safe_decimal_convert(
            long_funding_rate, "long_funding_rate", symbol, allow_none=False
        )
        self.short_funding_rate = self._safe_decimal_convert(
            short_funding_rate, "short_funding_rate", symbol, allow_none=False
        )
        self.net_funding_differential = self._safe_decimal_convert(
            net_funding_differential, "net_funding_differential", symbol, allow_none=False
        )
        self.optimal_size = self._safe_decimal_convert(
            optimal_size, "optimal_size", symbol, allow_none=True
        )
        self.expected_profit = self._safe_decimal_convert(
            expected_profit, "expected_profit", symbol, allow_none=True
        )

        # Use float for statistical/ranking metrics that don't require financial precision
        self.confidence = self._safe_float_convert(confidence, "confidence", symbol)
        self.basis_volatility = self._safe_float_convert(
            basis_volatility, "basis_volatility", symbol
        )
        self.utility_score = self._safe_float_convert(utility_score, "utility_score", symbol)

        # Validate required fields are not None after conversion
        self.validate_required_fields()

        # Expiration calculation
        # Use timezone-aware comparison if timestamp has timezone
        if timestamp.tzinfo:
            self.expiration_timestamp = timestamp.timestamp() + 3600  # 1 hour
        else:
            # Assume UTC if no timezone provided for timestamp()
            self.expiration_timestamp = timestamp.replace(tzinfo=UTC).timestamp() + 3600

    @staticmethod
    def _safe_decimal_convert(
        value: str | int | float | Decimal | None,
        field_name: str,
        symbol: str,
        allow_none: bool = False,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None values."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none:
                return None
            else:
                raise ValueError(
                    f"ArbitrageOpportunity field '{field_name}' for symbol '{symbol}'"
                    " cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for ArbitrageOpportunity field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err

    @staticmethod
    def _safe_float_convert(
        value: str | int | float | Decimal | None,
        field_name: str,
        symbol: str,
    ) -> float | None:
        """Safely convert a value to float for non-financial metrics, handling None values."""
        if value is None:
            return None
        if isinstance(value, float):
            return value
        try:
            if isinstance(value, Decimal):
                # Convert via string to avoid float precision issues with direct float(Decimal)
                return float(str(value))
            return float(value)
        except (ValueError, TypeError):
            logger.warning(
                f"Invalid value '{value}' for ArbitrageOpportunity field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to float. Using None."
            )
            return None

    def validate_required_fields(self) -> None:
        """Check if all required fields are set and not None."""
        # Define required fields (critical for an arbitrage opportunity)
        required_fields = [
            "long_price",
            "short_price",
            "long_funding_rate",
            "short_funding_rate",
            "net_funding_differential",
        ]

        # Check which fields are None
        missing_fields = [field for field in required_fields if getattr(self, field) is None]

        if missing_fields:
            raise ValueError(
                f"Required fields {', '.join(missing_fields)} are None after conversion"
                f" for ArbitrageOpportunity (symbol: {self.symbol})"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert ArbitrageOpportunity to dictionary."""
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "long_price": str(self.long_price) if self.long_price is not None else None,
            "short_price": str(self.short_price) if self.short_price is not None else None,
            "long_funding_rate": (
                str(self.long_funding_rate) if self.long_funding_rate is not None else None
            ),
            "short_funding_rate": (
                str(self.short_funding_rate) if self.short_funding_rate is not None else None
            ),
            "net_funding_differential": (
                str(self.net_funding_differential)
                if self.net_funding_differential is not None
                else None
            ),
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "optimal_size": str(self.optimal_size) if self.optimal_size is not None else None,
            "expected_profit": (
                str(self.expected_profit) if self.expected_profit is not None else None
            ),
            # Float fields don't need str() conversion
            "confidence": self.confidence,
            "basis_volatility": self.basis_volatility,
            "utility_score": self.utility_score,
            "expiration_timestamp": self.expiration_timestamp,
            "id": str(uuid.uuid4()),
        }

    @property
    def is_expired(self) -> bool:
        """Check if the opportunity has expired."""
        # Compare current UTC time with expiration timestamp
        return datetime.now(UTC).timestamp() > self.expiration_timestamp


@dataclass(order=True)  # order=True needed for sorting? Depends on usage.
class TradeSignal:
    """Represents a decision signal generated by a strategy."""

    symbol: str
    signal_type: SignalType
    side: OrderSide
    price: Decimal | None = None
    quantity: Decimal | None = None
    timestamp: datetime | None = None
    confidence: float | None = None  # Changed from Decimal to float for statistical measure
    source_strategy: str | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    expiration: datetime | None = None
    metadata: dict[str, Any] | None = None
    signal_id: str | None = None  # Unique identifier, should be added if not in dataclass already

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal and handle None values."""
        # Convert Decimal fields safely - all are optional in TradeSignal
        self.price = self._safe_decimal_convert_optional(self.price, "price", self.symbol)
        self.quantity = self._safe_decimal_convert_optional(self.quantity, "quantity", self.symbol)
        self.stop_loss = self._safe_decimal_convert_optional(
            self.stop_loss, "stop_loss", self.symbol
        )
        self.take_profit = self._safe_decimal_convert_optional(
            self.take_profit, "take_profit", self.symbol
        )

        # Convert confidence to float | None (statistical measure, not financial)
        confidence_val = self.confidence
        final_confidence: float | None = None  # Explicitly type the target variable

        if isinstance(confidence_val, float):
            final_confidence = confidence_val
        # The following elif blocks were removed as they were deemed unreachable by Mypy
        # due to the type hint `confidence: float | None`.
        # The float case is handled above, and the None case is handled by the default value.

        # Assign the final processed value back
        self.confidence = final_confidence

        # Ensure timestamp and expiration are timezone-aware (UTC) if provided
        if self.timestamp and self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=UTC)
        if self.expiration and self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=UTC)

        # Generate unique signal_id if not provided
        if self.signal_id is None:
            self.signal_id = str(uuid.uuid4())

    @staticmethod
    def _safe_decimal_convert_optional(
        value: str | int | float | Decimal | None,  # ANN401 Fix
        field_name: str,
        symbol: str,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None values."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for TradeSignal field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err

    def is_valid(self) -> bool:
        """Check if the signal is still valid (e.g., not expired)."""
        if self.expiration is None:
            return True
        # Ensure comparison uses timezone-aware datetime
        return datetime.now(UTC) < self.expiration

    def to_dict(self) -> dict[str, Any]:
        """Convert TradeSignal to dictionary."""
        result = {
            "symbol": self.symbol,
            "signal_type": self.signal_type.value,
            "side": self.side.value,
            "price": str(self.price) if self.price is not None else None,
            "quantity": str(self.quantity) if self.quantity is not None else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp is not None else None,
            # Float values don't need str() conversion like Decimal
            "confidence": self.confidence,  # Now a float
            "source_strategy": self.source_strategy,
            "stop_loss": str(self.stop_loss) if self.stop_loss is not None else None,
            "take_profit": str(self.take_profit) if self.take_profit is not None else None,
            "expiration": self.expiration.isoformat() if self.expiration is not None else None,
            "metadata": self.metadata if self.metadata is not None else {},
            "signal_id": self.signal_id,
        }
        return result


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
    quantity_requested: Decimal
    quantity_filled: Decimal = Decimal("0.0")
    price: Decimal | None = None
    average_fill_price: Decimal | None = None
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
    def check_order_state(self) -> Self:
        """
        Post-model validation for order state and price logic.
        Ensures logical consistency between fields (e.g., filled <= requested,
        price for limit orders).
        """
        if self.quantity_requested <= 0:
            raise ValueError("quantity_requested must be positive")
        if self.quantity_filled < 0:
            raise ValueError("quantity_filled cannot be negative")
        if self.quantity_filled > self.quantity_requested:
            raise ValueError("quantity_filled cannot exceed quantity_requested")
        if self.order_type in [
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_LIMIT,
        ]:
            if self.price is None or self.price <= 0:
                raise ValueError(f"Limit price must be positive for order type {self.order_type}")
        if self.quantity_filled > 0 and self.average_fill_price is None and self.trades:
            # This may be temporarily valid if fills arrive before order update
            logger.warning(
                f"Order {self.client_order_id} partially/fully filled but "
                "average_fill_price is None"
            )
        if self.updated_at is None:
            object.__setattr__(self, "updated_at", self.created_at)
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
