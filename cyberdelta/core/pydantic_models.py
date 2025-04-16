from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.core.models import OrderSide, OrderStatus, OrderType, TimeInForce


class OrderModel(BaseModel):
    """
    Pydantic model for representing an order on an exchange.
    Provides strict type validation and conversion for all fields.
    Mirrors the dataclass Order structure for incremental migration.
    """

    symbol: str
    id: str
    side: OrderSide
    type: OrderType
    quantity: Decimal
    status: OrderStatus
    client_order_id: str | None = None
    price: Decimal | None = None
    avg_fill_price: Decimal | None = None
    filled_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    time: datetime | None = None
    leverage: Decimal | None = None
    time_in_force: TimeInForce | None = None
    post_only: bool = False
    reduce_only: bool = False
    associated_signal_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "quantity",
        "price",
        "avg_fill_price",
        "filled_quantity",
        "remaining_quantity",
        "leverage",
        mode="before",
    )
    @classmethod
    def parse_decimal(cls, v: Any, info: Any) -> Decimal | None:
        """Convert input to Decimal, allowing None for optional fields."""
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

    @field_validator("time", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> datetime | None:
        """Parse various datetime formats, ensure UTC timezone awareness."""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=UTC)
        if isinstance(v, (int, float)):
            # Assume POSIX timestamp (seconds since epoch)
            return datetime.fromtimestamp(v, tz=UTC)
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
                return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            except ValueError as e:
                raise ValueError(f"Field 'time': cannot parse datetime string '{v}': {e}")
        raise ValueError(f"Field 'time': unsupported type {type(v)}")

    @field_validator("side", mode="before")
    @classmethod
    def parse_side(cls, v: Any) -> OrderSide:
        if isinstance(v, OrderSide):
            return v
        try:
            return OrderSide(v)
        except Exception as e:
            raise ValueError(f"Field 'side': invalid value '{v}': {e}")

    @field_validator("type", mode="before")
    @classmethod
    def parse_type(cls, v: Any) -> OrderType:
        if isinstance(v, OrderType):
            return v
        try:
            return OrderType(v)
        except Exception as e:
            raise ValueError(f"Field 'type': invalid value '{v}': {e}")

    @field_validator("status", mode="before")
    @classmethod
    def parse_status(cls, v: Any) -> OrderStatus:
        if isinstance(v, OrderStatus):
            return v
        try:
            return OrderStatus(v)
        except Exception as e:
            raise ValueError(f"Field 'status': invalid value '{v}': {e}")

    @field_validator("time_in_force", mode="before")
    @classmethod
    def parse_time_in_force(cls, v: Any) -> TimeInForce | None:
        if v is None:
            return None
        if isinstance(v, TimeInForce):
            return v
        try:
            return TimeInForce(v)
        except Exception as e:
            raise ValueError(f"Field 'time_in_force': invalid value '{v}': {e}")

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the OrderModel to a dictionary, serializing enums and decimals appropriately.
        """
        d = self.model_dump()
        # Serialize enums and decimals
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
            elif isinstance(v, (OrderSide, OrderType, OrderStatus, TimeInForce)):
                d[k] = v.value
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
        return d


# Note: This model is a draft for incremental migration. It should be thoroughly tested
# with real and edge-case data, and integrated stepwise into the codebase. See project
# documentation and workflow/pydantic.md for migration strategy and test requirements.


class MarketDataModel(BaseModel):
    """
    Pydantic model for market data (OHLCV and optional ticker data).
    """

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0.0")
    ticker_data: dict[str, dict[str, Any]] | None = None

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


class BalanceModel(BaseModel):
    """
    Pydantic model for an account balance for a single asset.
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


class PositionModel(BaseModel):
    """
    Pydantic model for an open position.
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


class TradeModel(BaseModel):
    """
    Pydantic model for a trade execution.
    """

    id: str
    symbol: str
    timestamp: int
    price: Decimal
    quantity: Decimal
    side: OrderSide | None = None
    order_id: str | None = None
    exchange: str | None = None
    trade_datetime: datetime | None = None
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

    @field_validator("trade_datetime", mode="before")
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
                raise ValueError(f"trade_datetime: cannot parse datetime string '{v}': {e}")
        raise ValueError(f"trade_datetime: unsupported type {type(v)}")


class TickerModel(BaseModel):
    """
    Pydantic model for ticker information for a symbol.
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


class OrderBookModel(BaseModel):
    """
    Pydantic model for an order book for a symbol.
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
            level: Any = level
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


class FundingRateModel(BaseModel):
    """
    Pydantic model for funding rate information for a perpetual contract.
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


# --- API Boundary Models ---


class PlaceOrderRequest(BaseModel):
    """
    Request model for placing a new order via the API.
    """

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Decimal | None = None
    time_in_force: TimeInForce
    client_order_id: str | None = None
    reduce_only: bool = False
    post_only: bool = False


class CancelOrderRequest(BaseModel):
    """
    Request model for canceling an order via the API.
    """

    order_id: str
    symbol: str | None = None


class OrdersResponse(BaseModel):
    """
    Response model for a batch of orders (e.g., open orders, order history).
    """

    orders: list[OrderModel]
    next_page_token: str | None = None


class WebSocketMessage(BaseModel):
    """
    Envelope model for WebSocket messages.
    """

    topic: str
    data: dict[str, Any] | list[Any] | None = None
    event: str | None = None
    ts: int | None = None  # Optional timestamp


class APIErrorResponse(BaseModel):
    """
    Standardized error response model for API errors.
    """

    message: str
    code: int | str
    http_status: int | None = None
    exchange_code: str | None = None
    exchange_message: str | None = None
    retry_after: float | None = None


T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Generic paginated response envelope for API endpoints returning lists of items.
    """

    items: list[T]
    next_page_token: str | None = None


# --- Exchange-Specific Request/Response Models ---


class BackpackPlaceOrderRequest(PlaceOrderRequest):
    """
    Backpack-specific order placement request model.
    Example: includes a 'margin_type' field required by Backpack.
    """

    margin_type: str | None = None  # e.g., 'cross' or 'isolated'
    # Add any other Backpack-specific fields here


class HyperliquidPlaceOrderRequest(PlaceOrderRequest):
    """
    Hyperliquid-specific order placement request model.
    Example: includes a 'chain_id' field required by Hyperliquid.
    """

    chain_id: int | None = None
    # Add any other Hyperliquid-specific fields here


# --- Event-Specific WebSocket Models ---


class AccountUpdateEvent(BaseModel):
    """
    WebSocket event model for account updates (balances, positions, etc.).
    """

    event: str
    account_id: str
    balances: list[BalanceModel] | None = None
    positions: list[PositionModel] | None = None
    ts: int | None = None


class OrderUpdateEvent(BaseModel):
    """
    WebSocket event model for order updates (new, filled, canceled, etc.).
    """

    event: str
    order: OrderModel
    ts: int | None = None


class TradeFillEvent(BaseModel):
    """
    WebSocket event model for trade/fill updates.
    """

    event: str
    trade: TradeModel
    ts: int | None = None
