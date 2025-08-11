# Event Structure Reference

## Complete Event Definitions

This document contains all event structure definitions for easy reference during implementation.

**Note on Nautilus Integration**: These event structures remain unchanged but are now processed by handlers with lifecycle management (on_start, on_stop, on_degrade), priority routing, and caching capabilities. The structures themselves are pure data containers optimized for performance.

## Important: Symbol and Enum Types in Events

### Symbol Handling
- **Decision**: Use `str` for all symbol fields in events
- **Rationale**: Zero conversion overhead, clean separation
- **Pattern**: Handlers convert to Symbol objects only when needed

### Enum Handling
- **Decision**: Use enums directly (they work perfectly in msgspec)
- **Supported**: ExchangeName, OrderSide, OrderType, ComponentState, etc.
- **No conversion needed**: msgspec serializes/deserializes enums automatically

## msgspec Events (7 Total)

### 1. MarketData

```python
class MarketData(msgspec.Struct, tag="market", array_like=True, gc=False):
    """Handles ALL market data types"""
    symbol: str  # String representation of Symbol (zero overhead)
    exchange: ExchangeName  # Enum works directly in msgspec!
    data_type: Literal["tick", "orderbook", "trade", "quote"]
    price: Decimal | None = None
    volume: int | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    bids: list[tuple[Decimal, Decimal]] | None = None
    asks: list[tuple[Decimal, Decimal]] | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

**Note**: 
- `symbol: str` - Always a string in events (zero conversion overhead)
- `exchange: ExchangeName` - Enum works directly, no conversion needed
- Handler creates Symbol object only when business logic requires it

**Usage Patterns:**
- `data_type="tick"`: Uses price, volume
- `data_type="orderbook"`: Uses bids, asks lists
- `data_type="trade"`: Uses price, volume
- `data_type="quote"`: Uses bid, ask

### 2. OrderEvent

```python
class OrderEvent(msgspec.Struct, tag="order"):
    """Handles ALL order lifecycle events"""
    order_id: str
    exchange: ExchangeName  # Enum works directly
    symbol: str  # String for zero overhead
    event_type: Literal[
        "placed", "filled", "partially_filled",
        "cancelled", "rejected", "expired", "amended"
    ]
    price: Decimal | None = None
    quantity: Decimal | None = None
    fill_price: Decimal | None = None
    fill_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    commission: Decimal | None = None
    reason: str | None = None
    error_code: str | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

**Field Usage by Event Type:**
- `"placed"`: price, quantity
- `"filled"`: fill_price, fill_quantity, commission
- `"partially_filled"`: fill_price, fill_quantity, remaining_quantity
- `"cancelled"`: reason
- `"rejected"`: reason, error_code
- `"amended"`: price, quantity (new values)

### 3. PositionEvent

```python
class PositionEvent(msgspec.Struct, tag="position"):
    """Handles ALL position changes"""
    position_id: str
    symbol: str  # String representation
    exchange: ExchangeName  # Enum works directly
    event_type: Literal["opened", "updated", "closed", "liquidated"]
    size: Decimal
    average_price: Decimal
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    close_price: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

**Field Usage by Event Type:**
- `"opened"`: size, average_price
- `"updated"`: size, average_price, unrealized_pnl
- `"closed"`: size (0), realized_pnl, close_price
- `"liquidated"`: size (0), realized_pnl, close_price

### 4. SignalEvent

```python
class SignalEvent(msgspec.Struct, tag="signal"):
    """Trading signals from strategies"""
    signal_id: str
    strategy_name: str
    symbol: str
    action: Literal["buy", "sell", "hold", "close"]
    confidence: float  # 0.0 to 1.0
    target_price: Decimal | None = None
    target_quantity: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

### 5. RiskEvent

```python
class RiskEvent(msgspec.Struct, tag="risk"):
    """Risk management events"""
    risk_type: Literal["limit_breach", "drawdown", "exposure", "margin_call"]
    severity: Literal["info", "warning", "critical", "emergency"]
    current_value: Decimal
    limit_value: Decimal
    symbol: str | None = None  # String representation when present
    exchange: ExchangeName | None = None  # Enum works directly
    message: str
    timestamp: float = msgspec.field(default_factory=time.time)
```

### 6. BalanceEvent

```python
class BalanceEvent(msgspec.Struct, tag="balance"):
    """Account balance updates"""
    account_id: str
    exchange: ExchangeName  # Enum works directly
    currency: str
    event_type: Literal["updated", "locked", "unlocked", "settled"]
    old_balance: Decimal
    new_balance: Decimal
    locked_amount: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

**Field Usage by Event Type:**
- `"updated"`: old_balance, new_balance
- `"locked"`: old_balance, new_balance, locked_amount
- `"unlocked"`: old_balance, new_balance, locked_amount
- `"settled"`: old_balance, new_balance

### 7. SystemEvent

```python
class SystemEvent(msgspec.Struct, tag="system"):
    """System-level events"""
    component: str
    event_type: Literal["started", "stopped", "error", "warning", "health_check"]
    status: Literal["healthy", "degraded", "failed"]
    message: str
    metadata: dict[str, str] | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

## Bubus Workflows (3-5 Total)

### 1. PlaceOrderWorkflow

```python
class PlaceOrderWorkflow(BaseEvent[str]):
    """Multi-step order placement with risk checks"""
    order_id: str
    symbol: str
    quantity: Decimal
    price: Decimal | None
    strategy_id: str
    risk_checks: list[str] = ["position_limit", "drawdown", "exposure"]
    max_retries: int = 3
```

### 2. RebalanceWorkflow

```python
class RebalanceWorkflow(BaseEvent[bool]):
    """Portfolio rebalancing across exchanges"""
    target_allocations: dict[str, Decimal]  # symbol -> target percentage
    max_slippage: Decimal = Decimal("0.01")
    exchanges: list[str]
    dry_run: bool = False
```

### 3. EmergencyLiquidation

```python
class EmergencyLiquidation(BaseEvent):
    """Urgent position closing"""
    reason: str
    positions: list[str]  # position_ids to close
    max_loss: Decimal
    force_market_orders: bool = True
```

### 4. GracefulShutdown

```python
class GracefulShutdown(BaseEvent):
    """System shutdown coordination"""
    save_state: bool = True
    cancel_open_orders: bool = True
    close_positions: bool = False
    timeout_seconds: int = 60
```

## EventType Enum Mapping

For migration from existing EventType enum:

```python
EVENT_TYPE_MAPPING = {
    # Order events
    EventType.ORDER_PLACED: ("order", "placed"),
    EventType.ORDER_FILLED: ("order", "filled"),
    EventType.ORDER_PARTIALLY_FILLED: ("order", "partially_filled"),
    EventType.ORDER_CANCELLED: ("order", "cancelled"),
    EventType.ORDER_REJECTED: ("order", "rejected"),
    EventType.ORDER_EXPIRED: ("order", "expired"),
    EventType.ORDER_AMENDED: ("order", "amended"),

    # Position events
    EventType.POSITION_OPENED: ("position", "opened"),
    EventType.POSITION_UPDATED: ("position", "updated"),
    EventType.POSITION_CLOSED: ("position", "closed"),
    EventType.POSITION_LIQUIDATED: ("position", "liquidated"),

    # Balance events
    EventType.BALANCE_UPDATED: ("balance", "updated"),
    EventType.BALANCE_LOCKED: ("balance", "locked"),
    EventType.BALANCE_UNLOCKED: ("balance", "unlocked"),

    # Risk events
    EventType.RISK_LIMIT_BREACHED: ("risk", "limit_breach"),
    EventType.DRAWDOWN_ALERT: ("risk", "drawdown"),
    EventType.EXPOSURE_WARNING: ("risk", "exposure"),

    # Market data
    EventType.MARKET_DATA: ("market", "tick"),
    EventType.ORDERBOOK_UPDATE: ("market", "orderbook"),
    EventType.TRADE_TICK: ("market", "trade"),

    # System events
    EventType.SYSTEM_STARTED: ("system", "started"),
    EventType.SYSTEM_STOPPED: ("system", "stopped"),
    EventType.SYSTEM_ERROR: ("system", "error"),
    EventType.HEALTH_CHECK: ("system", "health_check"),

    # Signal events
    EventType.SIGNAL_GENERATED: ("signal", None),  # Needs action field
    EventType.SIGNAL_EXECUTED: ("signal", None),
}

def map_event_type(old_type: EventType) -> tuple[str, str]:
    """Map old EventType to (struct_name, event_type)"""
    return EVENT_TYPE_MAPPING.get(old_type, ("system", "unknown"))
```

## Pattern Matching Examples

### Order Event Handler

```python
async def handle_order_event(event: OrderEvent):
    match event.event_type:
        case "placed":
            await record_order_placement(event.order_id, event.price, event.quantity)
        case "filled":
            await process_fill(event.order_id, event.fill_price, event.fill_quantity)
        case "partially_filled":
            await process_partial_fill(event.order_id, event.fill_quantity, event.remaining_quantity)
        case "cancelled":
            await process_cancellation(event.order_id, event.reason)
        case "rejected":
            await handle_rejection(event.order_id, event.reason, event.error_code)
```

### Market Data Handler

```python
async def handle_market_data(event: MarketData):
    match event.data_type:
        case "tick":
            await update_last_price(event.symbol, event.price, event.volume)
        case "orderbook":
            await update_order_book(event.symbol, event.bids, event.asks)
        case "trade":
            await record_trade(event.symbol, event.price, event.volume)
        case "quote":
            await update_quote(event.symbol, event.bid, event.ask)
```

## Performance Characteristics

| Event Type | Frequency | Processing Path | Latency Target |
|------------|-----------|-----------------|----------------|
| MarketData | 1000+/sec | Direct (bypass bus) | < 1ms |
| OrderEvent | 10-100/sec | Event bus | < 10ms |
| PositionEvent | 1-10/sec | Event bus | < 100ms |
| SignalEvent | 1-10/sec | Event bus | < 100ms |
| RiskEvent | 0.1-1/sec | Event bus + alerts | < 1s |
| BalanceEvent | 0.1-1/sec | Event bus | < 1s |
| SystemEvent | 0.01-0.1/sec | Event bus + logging | < 5s |

## Memory Layout Optimization

The `array_like=True, gc=False` flags on MarketData optimize for:
- Dense memory layout (array-like)
- No garbage collection overhead
- Cache-friendly access patterns
- Ideal for high-frequency data

Other events use default settings for flexibility.
