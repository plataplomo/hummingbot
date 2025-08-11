# Symbol and Enum Handling at Event Boundaries

## Key Decision: Consistent Symbol Conversion

**ALL handlers create Symbol objects at event boundaries for consistency and type safety**

### The Architecture

- **Events (msgspec)**: Use `str` for Symbol (zero overhead), enums work directly
- **Handlers**: ALWAYS create Symbol objects at boundaries
- **Domain Models (Pydantic)**: Use proper Symbol models everywhere
- **Pattern**: Convert at boundaries, not conditionally

## Boundary Conversion Pattern

### Event → Handler (ALWAYS Convert)

```python
from cyberdelta.symbols.models import Symbol
from cyberdelta.symbols.service import SymbolService
from cyberdelta.enums import ExchangeName

class OrderEventHandler:
    def __init__(self, symbol_service: SymbolService, ...):
        self.symbol_service = symbol_service
        self._symbol_cache = {}  # Cache for performance

    async def handle_order_event(self, event: OrderEvent):
        # ALWAYS create Symbol at the boundary
        cache_key = f"{event.symbol}:{event.exchange.value}"
        symbol = self._symbol_cache.get(cache_key)
        if not symbol:
            symbol = self.symbol_service.create_symbol(
                event.symbol,  # String from event
                event.exchange  # Enum works directly!
            )
            self._symbol_cache[cache_key] = symbol
        
        # All handler logic uses proper Symbol object
        await self.process_with_symbol(symbol)
        
        # Type-safe access to Symbol properties
        base = symbol.base_asset
        quote = symbol.quote_asset
        market = symbol.market_type
```

### Handler → Event (Convert Symbol to String)

```python
class OrderCommandHandler:
    """Creates events from domain operations"""

    async def place_order(self, order: Order) -> None:
        """Order has Symbol model and enums"""

        # Convert Symbol to string at boundary
        event = OrderEvent(
            order_id=order.order_id,
            symbol=str(order.symbol),  # Symbol → string
            exchange=order.exchange,  # Enums pass through!
            event_type="placed",
            side=order.side,  # Enums work directly
            order_type=order.order_type,  # Enums work directly
            price=order.price,
            quantity=order.quantity
        )

        await self.event_bus.publish(event)
```

### Why This Pattern?

1. **Consistency**: Every handler uses Symbol objects
2. **Type Safety**: Business logic always has proper types
3. **Performance**: Events have zero conversion overhead
4. **Caching**: Symbol creation can be cached at handler level
5. **Simplicity**: No conditional logic about conversion

## Complete Enum Mappings

### From CyberDeltaEngine Enums

```python
from cyberdelta.enums import (
    ExchangeName,
    OrderSide,
    OrderType,
    TimeInForce,
    MakerTaker,
    SignalType,
    EventType,
    EntityType,
)

# Event string literals to enums
EXCHANGE_MAP = {
    "hyperliquid": ExchangeName.HYPERLIQUID,
    "backpack": ExchangeName.BACKPACK,
}

SIDE_MAP = {
    "buy": OrderSide.BUY,
    "sell": OrderSide.SELL,
}

ORDER_TYPE_MAP = {
    "market": OrderType.MARKET,
    "limit": OrderType.LIMIT,
    # Add others as needed
}

# Enum to event strings
def enum_to_event_string(enum_value):
    """Convert enum to string for events"""
    return enum_value.value.lower()
```

## Handler Layer Pattern

**Nautilus Enhancement**: The handlers shown below now inherit from EventHandlerActor, providing lifecycle management. Symbol conversions can be cached during on_start() for performance, and the cache is properly cleaned up during on_stop().

### Complete Handler with Conversions

```python
from cyberdelta.events.core import OrderEvent, PositionEvent
from cyberdelta.models.orders import Order
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.symbols.service import SymbolService
from cyberdelta.domain.base_event_handler import EventHandlerActor

class OrderEventHandler(EventHandlerActor):
    """Handles conversions between events and domain models with lifecycle support"""

    def __init__(
        self,
        order_repository,
        symbol_service: SymbolService,
        event_bus: MsgspecEventBus
    ):
        super().__init__("order_handler", event_bus)
        self.order_repo = order_repository
        self.symbol_service = symbol_service
        self.event_bus = event_bus
        # Symbol cache for performance (Nautilus pattern)
        self._symbol_cache = {}

    async def on_start(self):
        """Warm symbol cache on startup (Nautilus lifecycle)"""
        # Pre-cache frequently used symbols for fast conversion
        common_symbols = ["BTC-USDC", "ETH-USDC", "SOL-USDC"]
        for symbol_str in common_symbols:
            for exchange in [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]:
                symbol = await self.symbol_service.get_symbol(symbol_str, exchange)
                cache_key = f"{symbol_str}:{exchange.value}"
                self._symbol_cache[cache_key] = symbol

    async def handle_order_event(self, event: OrderEvent):
        """Convert event data to domain types at the boundary"""

        # Step 1: ALWAYS create Symbol at the boundary for consistency
        # Check cache first (Nautilus performance optimization)
        cache_key = f"{event.symbol}:{event.exchange.value}"
        symbol = self._symbol_cache.get(cache_key)
        if not symbol:
            symbol = self.symbol_service.create_symbol(
                event.symbol,  # String from event
                event.exchange  # Enum already works!
            )
            self._symbol_cache[cache_key] = symbol
        
        # Now we have proper Symbol object for ALL handler logic

        # Step 2: Get or create domain model with proper Symbol
        order = await self.order_repo.get(event.order_id)
        if not order and event.event_type == "placed":
            # Create new order with proper Symbol object
            order = Order(
                order_id=event.order_id,
                symbol=symbol,  # Always a Symbol object now
                exchange=event.exchange,  # Enum from event
                quantity=event.quantity,
                price=event.price
            )

        # Step 3: Update based on event
        match event.event_type:
            case "filled":
                order.fill(
                    fill_price=event.fill_price,
                    fill_quantity=event.fill_quantity
                )
            case "cancelled":
                order.cancel(event.reason)

        # Step 4: Save with proper types
        await self.order_repo.save(order)
```

### WebSocket Handler with Conversions

```python
class WebSocketProcessor:
    """Process raw WebSocket to events"""

    def __init__(self, event_bus: MsgspecEventBus):
        self.event_bus = event_bus

    async def handle_exchange_message(self, exchange: ExchangeName, message: dict):
        """Convert exchange-specific format to events"""

        # Exchange sends different formats
        if exchange == ExchangeName.HYPERLIQUID:
            symbol_str = message["coin"]  # HL uses "coin"
        elif exchange == ExchangeName.BACKPACK:
            symbol_str = message["symbol"]  # BP uses "symbol"

        # Create event with strings
        event = OrderEvent(
            order_id=message["oid"],
            symbol=symbol_str,
            exchange=exchange.value,  # Convert enum to string
            event_type=self.map_status(message["status"]),
            fill_price=Decimal(str(message["px"])) if "px" in message else None,
            fill_quantity=Decimal(str(message["sz"])) if "sz" in message else None
        )

        await self.event_bus.publish(event)

    def map_status(self, exchange_status: str) -> str:
        """Map exchange status to our event types"""
        status_map = {
            "filled": "filled",
            "partial_fill": "partially_filled",
            "cancelled": "cancelled",
            "rejected": "rejected",
            # Exchange-specific mappings
        }
        return status_map.get(exchange_status, "unknown")
```

## Symbol Service Integration

```python
from cyberdelta.symbols.service import SymbolService
from cyberdelta.symbols.registry import SymbolRegistry

# During bootstrap
symbol_registry = SymbolRegistry()
symbol_service = SymbolService(symbol_registry)

# Register symbols
await symbol_service.register_symbol(
    "BTC-USDC",
    ExchangeName.HYPERLIQUID,
    metadata=HyperliquidMetadata(asset_index=0)
)

# In handlers
symbol = await symbol_service.get_symbol("BTC-USDC", ExchangeName.HYPERLIQUID)
```

## Key Principles

1. **Events use strings** for serialization compatibility
2. **Domain models use proper types** (Symbol, enums)
3. **Handler layer converts** between representations
4. **Symbol service manages** symbol lifecycle
5. **No domain model changes** required

## Common Conversions

```python
# Event → Domain
exchange = ExchangeName(event.exchange)
symbol = await symbol_service.get_symbol(event.symbol, exchange)
side = OrderSide(event.side) if event.side else None

# Domain → Event
event = OrderEvent(
    symbol=order.symbol.value,
    exchange=order.exchange.value,
    side=order.side.value if order.side else None,
    # ...
)
```

## Testing Conversions

```python
async def test_symbol_conversion():
    """Test event to domain conversion"""

    # Create event with strings
    event = OrderEvent(
        order_id="123",
        symbol="BTC-USDC",
        exchange="hyperliquid",
        event_type="placed"
    )

    # Handler converts to domain types
    await handler.handle_order_event(event)

    # Verify domain model has proper types
    order = await order_repo.get("123")
    assert isinstance(order.symbol, BaseSymbol)
    assert order.exchange == ExchangeName.HYPERLIQUID
```

## Summary

The handler layer acts as an anti-corruption layer that:
- Converts event strings to domain types
- Maintains type safety in both layers
- Keeps events serializable
- Keeps domain models unchanged
- Uses CyberDeltaEngine's Symbol system properly
