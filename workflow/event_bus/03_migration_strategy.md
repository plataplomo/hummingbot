# Migration Strategy - From DomainEvent to msgspec

## Critical Understanding: Event Publishing Architecture

### Who Publishes Events in CyberDeltaEngine

**APIs do NOT publish events.** This is a critical architectural understanding:

```python
# APIs (bp_api.py, hl_api.py) - Pure data providers
result = await bp_api.place_order(...)  # Returns data only, NO events

# Domain Services - Event publishers
await trading_service.execute_order(...)  # Publishes DomainEvent
```

**Impact on Migration:**
- ✅ APIs remain completely unchanged
- ✅ No API modifications at any phase
- ✅ Only domain services need updates
- ✅ Simpler migration path

## Overview

A **three-phase migration** that replaces DomainEvent with msgspec events enhanced with Nautilus-inspired patterns (lifecycle management, state tracking, hierarchical routing) while keeping domain models unchanged. The handler layer enables gradual migration without breaking existing functionality.

## Current State Assessment

### What We Have
- `DomainEvent` with `dict[str, Any]` payload (violates standards)
- `EventBus` using Pydantic validation
- Domain models that consume `DomainEvent` directly
- 33 EventType enum values

### What We're Building
- 7 msgspec event structures
- Enhanced handler layer with lifecycle management (Nautilus-inspired)
- MsgspecEventBus with priority queues and request/response
- Component state tracking (RUNNING, DEGRADED, STOPPED)
- 3-5 bubus workflows for orchestration

## Phase 1: Foundation (Week 1)

### Step 1.1: Create Event Structures

```bash
# Create new event structures in existing events folder
touch cyberdelta/models/events/core.py
```

Add the 7 msgspec structures from implementation guide to the existing `/models/events/` directory.

### Step 1.2: Create Base Handler with Lifecycle (Nautilus-Inspired)

```bash
# Create base handler
touch cyberdelta/domain/base_event_handler.py
```

```python
# cyberdelta/domain/base_event_handler.py
from enum import Enum

class ComponentState(Enum):
    PRE_INITIALIZED = "PRE_INITIALIZED"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"

class EventHandlerActor:
    """Base handler with lifecycle management"""

    async def on_start(self): pass
    async def on_stop(self): pass
    async def on_degrade(self): pass
```

### Step 1.3: Create First Domain Handler

```python
# cyberdelta/domain/trading/trading_event_handlers.py
class TradingEventHandler(EventHandlerActor):
    """Trading handler with lifecycle and caching"""

    def __init__(self, trading_service, symbol_service, event_bus):
        super().__init__("trading_handler", event_bus)
        self._order_cache = {}  # Performance optimization
        self._state = ComponentState.PRE_INITIALIZED

    async def on_start(self):
        """Initialize and warm caches"""
        open_orders = await self.trading_service.get_open_orders()
        for order in open_orders:
            self._order_cache[order.order_id] = order
        self._state = ComponentState.RUNNING
```

### Step 1.4: Create Enhanced MsgspecEventBus

```python
# cyberdelta/infrastructure/event_bus/msgspec_event_bus.py
from enum import Enum

class HandlerPriority(Enum):
    CRITICAL = 0  # Risk checks
    HIGH = 1      # Order validation
    NORMAL = 2    # Regular processing
    LOW = 3       # Logging

class MsgspecEventBus:
    """Enhanced event bus with priorities and request/response"""

    def subscribe(self, event_type, handler, priority=HandlerPriority.NORMAL):
        """Subscribe with priority support"""
        pass

    async def request(self, request, timeout=5.0):
        """Request/response pattern for queries"""
        pass
```

### Step 1.5: Adapter for Compatibility

```python
# cyberdelta/infrastructure/migration/event_adapter.py
from cyberdelta.models.events import DomainEvent
from cyberdelta.models.events.core import OrderEvent, PositionEvent
from cyberdelta.enums.events import EventType

class EventHandler:
    """Temporary adapter during migration"""

    @staticmethod
    def domain_to_msgspec(event: DomainEvent) -> msgspec.Struct:
        """Convert DomainEvent to msgspec"""

        # Map by EventType
        if event.event_type in [
            EventType.ORDER_PLACED,
            EventType.ORDER_FILLED,
            EventType.ORDER_CANCELLED,
            EventType.ORDER_REJECTED,
        ]:
            return OrderEvent(
                order_id=event.entity_id,
                exchange=str(event.exchange) if event.exchange else "",
                symbol=str(event.symbol) if event.symbol else "",
                event_type=event.event_type.value.lower().replace("order_", ""),
                fill_price=event.get_decimal("fill_price"),
                fill_quantity=event.get_decimal("fill_quantity"),
                reason=event.get_str("reason")
            )

        # Add other mappings

    @staticmethod
    def msgspec_to_domain(event: msgspec.Struct) -> DomainEvent:
        """Convert msgspec to DomainEvent for backward compatibility"""
        # Only if needed during migration
        pass
```

## Phase 2: Parallel Operation with Lifecycle Management (Week 2)

### Step 2.1: Dual Publishing

During migration, publish to both event buses:

```python
# cyberdelta/services/order_service.py
class OrderService:
    def __init__(self, old_bus: EventBus, new_bus: MsgspecEventBus):
        self.old_bus = old_bus
        self.new_bus = new_bus

    async def place_order(self, request: OrderRequest):
        # Create order (domain model unchanged)
        order = Order(...)

        # OLD: Publish DomainEvent
        old_event = DomainEvent(
            event_type=EventType.ORDER_PLACED,
            entity_id=order.order_id,
            payload={"price": str(order.price)}
        )
        await self.old_bus.publish(old_event)

        # NEW: Also publish msgspec event
        new_event = OrderEvent(
            order_id=order.order_id,
            event_type="placed",
            price=order.price
        )
        await self.new_bus.publish(new_event)
```

### Step 2.2: Migrate WebSocket Handlers

```python
# Before
async def handle_websocket(data: dict):
    event = DomainEvent(
        event_type=EventType.ORDER_FILLED,
        payload=data
    )
    await event_bus.publish(event)

# After
async def handle_websocket(raw_bytes: bytes):
    # Direct msgspec decode - 25x faster
    event = msgspec.json.decode(raw_bytes, type=OrderEvent)
    await msgspec_bus.publish(event)

    # Temporarily also publish old event for compatibility
    old_event = EventHandler.msgspec_to_domain(event)
    await old_bus.publish(old_event)
```

### Step 2.3: Add Handlers with Lifecycle Support

Add handlers for each domain with proper lifecycle:

```python
# Create and start handlers
handlers = []

# Week 2, Day 1: Trading handler
trading_handler = TradingEventHandler(trading_service, symbol_service, msgspec_bus)
await trading_handler.start()  # Initializes resources, subscribes to events
handlers.append(trading_handler)

# Week 2, Day 2: Portfolio handler
portfolio_handler = PortfolioEventHandler(portfolio_service, msgspec_bus)
await portfolio_handler.start()
handlers.append(portfolio_handler)

# Week 2, Day 3: Risk handler with priority
risk_handler = RiskEventHandler(risk_service, msgspec_bus)
await risk_handler.start()  # Subscribes with CRITICAL priority
handlers.append(risk_handler)

# Graceful shutdown
for handler in reversed(handlers):
    await handler.stop()
```

### Step 2.4: Monitor Handler Health

```python
# Monitor component states
async def check_health():
    health = {}
    for handler in handlers:
        health[handler.handler_id] = {
            "state": handler._state.value,
            "error_count": handler._error_count,
            "cache_size": len(handler._cache)
        }
    return health

# Auto-degradation on errors
if handler._error_count > 10:
    await handler.degrade()  # Reduced functionality mode
```

## Phase 3: Cutover (Week 3)

### Step 3.1: Stop Dual Publishing

```python
# Remove old event publishing
class OrderService:
    def __init__(self, event_bus: MsgspecEventBus):  # Only new bus
        self.event_bus = event_bus

    async def place_order(self, request: OrderRequest):
        order = Order(...)

        # Only msgspec event now
        event = OrderEvent(
            order_id=order.order_id,
            event_type="placed",
            price=order.price
        )
        await self.event_bus.publish(event)
```

### Step 3.2: Remove DomainEvent Dependencies

```python
# Before: Domain model knows about events
from cyberdelta.models.events import DomainEvent

class Order(BaseModel):
    def update_from_event(self, event: DomainEvent):
        # REMOVE THIS METHOD
        pass

# After: Domain model has pure business methods
class Order(BaseModel):
    def fill(self, price: Decimal, quantity: Decimal):
        # Pure business logic
        pass
```

### Step 3.3: Add Bubus Orchestration

```python
# Add complex workflows
from cyberdelta.orchestration.workflows import PlaceOrderWorkflow

orchestrator = BubusOrchestrator()

# Use for complex flows
workflow = PlaceOrderWorkflow(
    order_id=order_id,
    symbol=symbol,
    quantity=quantity
)
result = await orchestrator.execute(workflow)
```

### Step 3.4: Cleanup

```bash
# Remove old event system
rm cyberdelta/models/events/domain_event.py
rm cyberdelta/application/event_bus.py

# Remove adapter (no longer needed)
rm cyberdelta/infrastructure/migration/event_adapter.py
```

## Migration Checklist

### Week 1 - Foundation with Nautilus Patterns
- [ ] Create msgspec event structures (7 total)
- [ ] Create base EventHandlerActor with lifecycle
- [ ] Create enhanced MsgspecEventBus with priorities
- [ ] Create first domain handler (TradingEventHandler)
- [ ] Implement handler state management (RUNNING, DEGRADED, STOPPED)
- [ ] Add handler-level caching for performance
- [ ] Create EventAdapter for compatibility
- [ ] Test lifecycle methods (on_start, on_stop, on_degrade)

### Week 2 - Parallel Operation with Lifecycle
- [ ] Add dual publishing to services
- [ ] Start all handlers with proper initialization
- [ ] Implement hierarchical event routing
- [ ] Add priority subscriptions for risk handlers
- [ ] Monitor handler health and states
- [ ] Test auto-degradation on errors
- [ ] Verify handler cache performance
- [ ] Migrate WebSocket handlers
- [ ] Monitor both event buses
- [ ] Verify no data loss

### Week 3 - Cutover with Production Features
- [ ] Stop publishing DomainEvents
- [ ] Remove event methods from domain models
- [ ] Add bubus workflows with lifecycle
- [ ] Implement request/response patterns
- [ ] Add health check endpoints
- [ ] Verify graceful shutdown works
- [ ] Remove old event system
- [ ] Performance testing with metrics
- [ ] Test degraded mode handling

## Risk Mitigation

### Rollback Plan

If issues arise, we can quickly rollback because:

1. **Domain models unchanged** - No business logic affected
2. **Dual publishing** - Both systems run in parallel
3. **Adapter pattern** - Can convert between formats

### Testing Strategy with Tenacity

```python
from tenacity import retry, stop_after_attempt, wait_exponential

# Test both event types during migration with retry logic
async def test_order_fill_migration():
    # Create both events
    old_event = DomainEvent(
        event_type=EventType.ORDER_FILLED,
        payload={"fill_price": "100"}
    )

    new_event = OrderEvent(
        event_type="filled",
        fill_price=Decimal("100")
    )

    # Verify adapter works
    converted = EventHandler.domain_to_msgspec(old_event)
    assert converted.fill_price == new_event.fill_price

    # Verify handler works with retry for flaky tests
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=1, max=5)
    )
    async def test_handler():
        await handler.handle_order_event(new_event)
        # Assert domain model updated correctly

    await test_handler()
```

## Performance Monitoring

Track metrics during migration:

```python
from prometheus_client import Histogram

event_processing_time = Histogram(
    'event_processing_seconds',
    'Time to process events',
    ['event_type', 'system']  # old vs new
)

# Compare performance
with event_processing_time.labels(event_type='order_filled', system='old').time():
    await old_bus.publish(old_event)

with event_processing_time.labels(event_type='order_filled', system='new').time():
    await new_bus.publish(new_event)
```

## Success Criteria

### Week 1
- Handler pattern with lifecycle proven
- Enhanced MsgspecEventBus operational
- Component state management working
- Handler caching improving performance
- No domain model changes required

### Week 2
- Both event buses running
- All handlers started with proper initialization
- Handler health monitoring active
- Auto-degradation on errors working
- Priority routing for critical events
- WebSocket using msgspec

### Week 3
- DomainEvent eliminated
- 25x performance improvement verified
- Zero `dict[str, Any]` in events
- Graceful shutdown verified
- Degraded mode tested
- Production-ready reliability

## Common Pitfalls to Avoid

1. **Don't modify domain models** - Use handlers for translation
2. **Don't skip dual publishing** - Ensures smooth transition
3. **Don't migrate all at once** - Gradual migration reduces risk
4. **Don't forget monitoring** - Track both systems during migration
5. **Don't remove old system too early** - Ensure stability first

## FAQ

### Q: What if we find a case that doesn't fit the 7 events?

A: Add optional fields to existing structures rather than creating new ones. The 7 structures are designed to be flexible.

### Q: How do we handle event replay?

A: Bubus provides WAL (Write-Ahead Log) for event replay. Enable it for critical workflows.

### Q: What about existing event subscribers?

A: They continue working during dual publishing. Migrate them to handlers in Phase 2.

### Q: Can we skip the adapter?

A: Not recommended. The adapter ensures compatibility during migration and provides a rollback path.

---

**Migration Start Date**: [To be determined]
**Estimated Duration**: 3 weeks
**Risk Level**: LOW - No domain changes, parallel operation
**Rollback Time**: < 1 hour (switch back to old bus)
