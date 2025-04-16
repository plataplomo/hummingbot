from __future__ import annotations  # Enable postponed evaluation

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

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
    def parse_decimal(cls, v: Any, info: Any) -> Decimal:
        if v is None:
            raise ValueError(f"Field '{info.field_name}' cannot be None")
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{info.field_name}': cannot convert value '{v}' to Decimal: {e}"
            )

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> datetime:
        if v is None:
            raise ValueError("timestamp cannot be None")
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=UTC)
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=UTC)
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
                return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            except ValueError as e:
                raise ValueError(f"timestamp: cannot parse datetime string '{v}': {e}")
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
    def parse_decimal(cls, v: Any, info: Any) -> Decimal | None:
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{info.field_name}': cannot convert value '{v}' to Decimal: {e}"
            )

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
    Represents an open position.
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
    def parse_decimal(cls, v: Any, info: Any) -> Decimal | None:
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{info.field_name}': cannot convert value '{v}' to Decimal: {e}"
            )

    @field_validator("close_time", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=UTC)
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=UTC)
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
                return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            except ValueError as e:
                raise ValueError(f"close_time: cannot parse datetime string '{v}': {e}")
        raise ValueError(f"close_time: unsupported type {type(v)}")

    def is_active(self) -> bool:
        return self.size != Decimal("0")

    def calculate_unrealized_pnl(self, current_mark_price: Decimal | None = None) -> Decimal | None:
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
    Represents a trade execution.
    """

    id: str
    symbol: str
    timestamp: int
    price: Decimal
    quantity: Decimal
    side: OrderSide | None = None
    order_id: str | None = None
    exchange: str | None = None
    datetime: datetime | None = None
    fee: Decimal | None = None
    fee_asset: str | None = None
    is_maker: bool | None = None
    client_order_id: str | None = None
    cost: Decimal | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("price", "quantity", "fee", "cost", mode="before")
    @classmethod
    def parse_decimal(cls, v: Any, info: Any) -> Decimal | None:
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{info.field_name}': cannot convert value '{v}' to Decimal: {e}"
            )

    @model_validator(mode="after")
    def set_cost_and_datetime(self) -> Self:
        if self.cost is None:
            object.__setattr__(self, "cost", self.price * self.quantity)
        if self.timestamp and self.datetime is None:
            try:
                object.__setattr__(
                    self, "datetime", datetime.fromtimestamp(self.timestamp / 1000, tz=UTC)
                )
            except (TypeError, ValueError, OSError):
                object.__setattr__(self, "datetime", None)
        return self

    def to_dict(self) -> dict[str, Any]:
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
    def parse_decimal(cls, v: Any, info: Any) -> Decimal | None:
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{info.field_name}': cannot convert value '{v}' to Decimal: {e}"
            )

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
    def parse_levels(cls, v: Any, info: Any) -> list[tuple[Decimal, Decimal]]:
        if not isinstance(v, list):
            raise ValueError(f"Field '{info.field_name}' must be a list.")
        result: list[tuple[Decimal, Decimal]] = []
        for i, level in enumerate(v):
            if not isinstance(level, (list, tuple)) or len(level) != 2:
                raise ValueError(f"Invalid item in '{info.field_name}' at index {i}: {level}")
            try:
                price: Decimal = Decimal(str(level[0]))
                quantity: Decimal = Decimal(str(level[1]))
                result.append((price, quantity))
            except (InvalidOperation, TypeError, IndexError) as err:
                raise ValueError(
                    f"Invalid price/quantity in '{info.field_name}' at index {i}: {level}"
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
    def parse_decimal(cls, v: Any, info: Any) -> Decimal | None:
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(
                f"Field '{info.field_name}': cannot convert value '{v}' to Decimal: {e}"
            )


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
            "long_funding_rate": str(self.long_funding_rate)
            if self.long_funding_rate is not None
            else None,
            "short_funding_rate": str(self.short_funding_rate)
            if self.short_funding_rate is not None
            else None,
            "net_funding_differential": str(self.net_funding_differential)
            if self.net_funding_differential is not None
            else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "optimal_size": str(self.optimal_size) if self.optimal_size is not None else None,
            "expected_profit": str(self.expected_profit)
            if self.expected_profit is not None
            else None,
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
            "signal_type": self.signal_type.name
            if isinstance(self.signal_type, SignalType)
            else self.signal_type,
            "side": self.side.name if isinstance(self.side, OrderSide) else self.side,
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
