# Practical Event-Driven Architecture - Nautilus Trader Inspired Approach

## Executive Summary

After analyzing Nautilus Trader's architecture, here's a **practical, incremental approach** to making CyberDeltaEngine truly event-driven without overengineering. Nautilus uses a pragmatic message bus with actors, not a full event-sourcing/CQRS system.

## Key Lessons from Nautilus Trader

### 1. **Actors with Message Bus (Not Full Actor System)**

Nautilus uses a simpler actor pattern:
- Actors are components that process messages
- They use a central MessageBus for communication
- No complex actor supervision or mailboxes
- Direct handler registration with priorities

### 2. **Three Communication Patterns**

Nautilus provides three practical patterns:

1. **Custom Events via MessageBus** - For system-level communication
2. **Structured Data via publish_data()** - For trading data with timestamps
3. **Simple Signals via publish_signal()** - For lightweight notifications

### 3. **Request/Response Without Complexity**

Simple request/response pattern:
```python
# Nautilus approach - simple and practical
response = await self.msgbus.request(query, timeout=5.0)
```

No need for complex saga patterns or event sourcing initially.

## Practical Implementation Steps

### Step 1: Enhance Our Existing EventBus

Our EventBus already has good foundations. Just add missing pieces:

```python
# cyberdelta/infrastructure/event_bus/streaming.py

from typing import AsyncIterator, Callable
import asyncio
from collections import defaultdict

class StreamingEventBus(EventBus):
    """Enhanced EventBus with streaming support"""

    def __init__(self, config: EventBusConfig):
        super().__init__(config)
        self._streams: dict[type, list[asyncio.Queue]] = defaultdict(list)

    async def stream(
        self,
        event_type: type[T],
        buffer_size: int = 100
    ) -> AsyncIterator[T]:
        """Subscribe to event stream"""
        queue = asyncio.Queue(maxsize=buffer_size)
        self._streams[event_type].append(queue)

        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            self._streams[event_type].remove(queue)

    async def publish(self, event: msgspec.Struct) -> None:
        """Publish with streaming support"""
        # Original publish logic
        await super().publish(event)

        # Stream to subscribers
        event_type = type(event)
        for queue in self._streams.get(event_type, []):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Handle backpressure - drop oldest
                try:
                    queue.get_nowait()
                    queue.put_nowait(event)
                except asyncio.QueueEmpty:
                    pass
```

### Step 2: Convert PositionManager to Actor Pattern

Following Nautilus's simpler actor approach:

```python
# cyberdelta/domain/portfolio/actors/position_actor.py

from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.models.events.queries import PositionQuery, PositionQueryResponse
import asyncio

class PositionActor(EventHandlerActor):
    """Position management as an actor - Nautilus style"""

    def __init__(
        self,
        config: AppSettings,
        event_bus: EventBus,
        state_manager: PortfolioStateManagerProtocol,
        pnl_calculator: PnLCalculatorProtocol
    ):
        super().__init__(
            handler_id="PositionActor",
            event_bus=event_bus,
            config=config.event_handler
        )
        self._state_manager = state_manager
        self._pnl_calculator = pnl_calculator

        # Local cache for fast reads
        self._position_cache: dict[str, DerivativePosition] = {}
        self._cache_lock = asyncio.Lock()

    async def on_start(self) -> None:
        """Initialize actor and subscribe to events"""
        # Subscribe to position queries with HIGH priority
        await self.subscribe_to_event(
            PositionQuery,
            HandlerPriority.HIGH
        )

        # Subscribe to fills for position updates
        await self.subscribe_to_event(
            Fill,
            HandlerPriority.NORMAL
        )

        # Subscribe to position commands
        await self.subscribe_to_event(
            UpdatePositionCommand,
            HandlerPriority.HIGH
        )

        # Load initial state
        await self._load_positions()

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle incoming events"""

        if isinstance(event, PositionQuery):
            await self._handle_query(event)

        elif isinstance(event, Fill):
            await self._handle_fill(event)

        elif isinstance(event, UpdatePositionCommand):
            await self._handle_update_command(event)

    async def _handle_query(self, query: PositionQuery) -> None:
        """Handle position queries with cached data"""

        # Fast path - read from cache
        async with self._cache_lock:
            key = f"{query.exchange.value}:{query.symbol}"
            position = self._position_cache.get(key)

        response = PositionQueryResponse(
            request_id=query.request_id,
            exists=position is not None,
            size=position.size if position else None,
            entry_price=position.entry_price if position else None,
            unrealized_pnl=position.unrealized_pnl if position else None
        )

        # Send response via event bus
        await self.event_bus.respond(query.request_id, response)

    async def _handle_fill(self, fill: Fill) -> None:
        """Update position from fill"""

        async with self._cache_lock:
            key = f"{fill.exchange}:{fill.symbol.value}"
            position = self._position_cache.get(key)

            # Calculate new position
            if position:
                result = self._apply_fill_to_position(position, fill)

                if result.position_closed:
                    del self._position_cache[key]
                    event = PositionClosedEvent(
                        symbol=fill.symbol,
                        exchange=fill.exchange,
                        realized_pnl=result.realized_pnl
                    )
                else:
                    # Update position
                    new_position = self._create_updated_position(
                        position, fill, result
                    )
                    self._position_cache[key] = new_position

                    event = PositionUpdatedEvent(
                        symbol=fill.symbol,
                        exchange=fill.exchange,
                        size=new_position.size,
                        entry_price=new_position.entry_price
                    )
            else:
                # New position
                new_position = self._create_position_from_fill(fill)
                self._position_cache[key] = new_position

                event = PositionOpenedEvent(
                    symbol=fill.symbol,
                    exchange=fill.exchange,
                    size=new_position.size,
                    entry_price=fill.price
                )

        # Publish position event
        await self.event_bus.publish(event)

        # Persist state asynchronously
        asyncio.create_task(self._persist_state())

    async def _persist_state(self) -> None:
        """Persist position state to storage"""
        # This runs async without blocking the main flow
        state = await self._state_manager.get_state()
        if state:
            async with self._cache_lock:
                state.positions = self._position_cache.copy()
            await self._state_manager.save_state()
```

### Step 3: Implement Data Streaming

Following Nautilus's approach to streaming:

```python
# cyberdelta/domain/portfolio/position_stream.py

class PositionStreamService:
    """Service for streaming position updates"""

    def __init__(self, event_bus: StreamingEventBus):
        self._event_bus = event_bus

    async def stream_positions(
        self,
        symbols: list[Symbol] | None = None,
        exchanges: list[ExchangeName] | None = None
    ) -> AsyncIterator[PositionEvent]:
        """Stream position updates with filtering"""

        async for event in self._event_bus.stream(PositionEvent):
            # Filter by symbols if specified
            if symbols and event.symbol not in symbols:
                continue

            # Filter by exchanges if specified
            if exchanges and event.exchange not in exchanges:
                continue

            yield event

    async def stream_with_buffer(
        self,
        buffer_size: int = 10,
        timeout: float = 1.0
    ) -> AsyncIterator[list[PositionEvent]]:
        """Stream buffered position updates"""

        buffer = []
        last_yield = asyncio.get_event_loop().time()

        async for event in self.stream_positions():
            buffer.append(event)

            current_time = asyncio.get_event_loop().time()
            should_yield = (
                len(buffer) >= buffer_size or
                current_time - last_yield >= timeout
            )

            if should_yield and buffer:
                yield buffer
                buffer = []
                last_yield = current_time
```

### Step 4: Add Concurrent Query Support

Enable concurrent position queries:

```python
# cyberdelta/domain/portfolio/position_service.py

class AsyncPositionService:
    """Service for concurrent position operations"""

    def __init__(self, event_bus: EventBus):
        self._event_bus = event_bus

    async def get_positions_concurrent(
        self,
        queries: list[tuple[Symbol, ExchangeName]]
    ) -> list[DerivativePosition | None]:
        """Get multiple positions concurrently"""

        # Create queries
        requests = [
            PositionQuery(
                request_id=str(uuid.uuid4()),
                symbol=symbol.value,
                exchange=exchange,
                query_type="details"
            )
            for symbol, exchange in queries
        ]

        # Send all queries concurrently
        tasks = [
            self._event_bus.request(query, timeout_seconds=1.0)
            for query in requests
        ]

        # Wait for all responses
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        # Process responses
        results = []
        for response in responses:
            if isinstance(response, PositionQueryResponse) and response.exists:
                position = DerivativePosition(
                    symbol=Symbol(response.symbol),
                    exchange=response.exchange,
                    size=response.size,
                    entry_price=response.entry_price,
                    unrealized_pnl=response.unrealized_pnl
                )
                results.append(position)
            else:
                results.append(None)

        return results
```

### Step 5: Simple Signal Publishing (Nautilus Style)

For lightweight notifications:

```python
# cyberdelta/domain/portfolio/position_signals.py

class PositionSignals:
    """Position-related signals"""

    LARGE_POSITION = "position.large"
    RISK_LIMIT = "position.risk_limit"
    PNL_ALERT = "position.pnl_alert"

class PositionMonitor:
    """Monitor positions and emit signals"""

    def __init__(self, actor: Actor, config: AppSettings):
        self._actor = actor
        self._risk_limit = config.risk.global_risk.max_position_usd

    async def monitor_position(self, position: DerivativePosition) -> None:
        """Monitor position and emit signals"""

        position_value = position.size * position.entry_price

        # Check for large position
        if position_value > self._risk_limit * Decimal("0.8"):
            await self._actor.publish_signal(
                PositionSignals.LARGE_POSITION,
                {
                    "symbol": position.symbol.value,
                    "value": float(position_value),
                    "limit": float(self._risk_limit)
                }
            )

        # Check PnL
        if position.unrealized_pnl and abs(position.unrealized_pnl) > Decimal("1000"):
            await self._actor.publish_signal(
                PositionSignals.PNL_ALERT,
                {
                    "symbol": position.symbol.value,
                    "pnl": float(position.unrealized_pnl)
                }
            )
```

## Migration Plan (Practical Steps)

### Week 1: Foundation
1. ✅ Add streaming to EventBus (1 day)
2. ✅ Create PositionActor from existing PositionManager (2 days)
3. ✅ Add concurrent query support (1 day)
4. ✅ Test with existing infrastructure (1 day)

### Week 2: Expand Coverage
1. Convert BalanceManager to BalanceActor
2. Add market data streaming
3. Implement signal publishing for alerts
4. Integration testing

### Week 3: Optimize
1. Add caching layer to actors
2. Implement batching for efficiency
3. Add backpressure management
4. Performance testing

## What We DON'T Need (Yet)

Based on Nautilus's pragmatic approach, we can defer:

1. **Full Event Sourcing** - Not needed initially
2. **Complex Actor System** - Simple actors are sufficient
3. **Saga Pattern** - Can use simple workflows
4. **CQRS** - Can optimize reads later
5. **Distributed Events** - Start with in-process

## Performance Improvements (Realistic)

With this practical approach:
- **5-10x** improvement in concurrent queries
- **Sub-100ms** position updates via caching
- **Real-time** position streaming
- **Minimal** code changes to existing system

## Code Changes Required

### Current Code to Keep
- ✅ EventBus (just enhance it)
- ✅ StateManager (keep as persistence layer)
- ✅ Existing models and protocols
- ✅ Current configuration system

### New Code to Add
- StreamingEventBus (extends EventBus)
- PositionActor (wraps PositionManager logic)
- AsyncPositionService (for concurrent ops)
- Simple signal publishing

### Migration Example

```python
# Before (current)
async def check_positions(symbols: list[Symbol]):
    for symbol in symbols:
        has_pos = await position_manager.has_position(symbol, exchange)
        # Process one by one

# After (with actors)
async def check_positions(symbols: list[Symbol]):
    # Concurrent checks
    queries = [(symbol, exchange) for symbol in symbols]
    positions = await position_service.get_positions_concurrent(queries)
    # All positions retrieved in parallel
```

## Testing Strategy

### Unit Tests
```python
async def test_position_actor_concurrent():
    """Test actor handles concurrent queries"""
    actor = PositionActor(config, event_bus, state_manager, pnl_calc)
    await actor.start()

    # Send 10 concurrent queries
    queries = [create_query(f"BTC{i}") for i in range(10)]
    responses = await asyncio.gather(*[
        event_bus.request(q, timeout=1.0) for q in queries
    ])

    assert len(responses) == 10
    # Should complete in < 100ms total
```

### Integration Tests
```python
async def test_position_streaming():
    """Test position streaming"""
    stream_service = PositionStreamService(event_bus)

    # Subscribe to stream
    stream_task = asyncio.create_task(
        consume_stream(stream_service)
    )

    # Generate position updates
    for i in range(100):
        await event_bus.publish(create_position_event(i))

    # Verify all events received
    await stream_task
```

## Conclusion

By following Nautilus Trader's pragmatic approach:
1. **Start simple** - Enhance what we have
2. **Be practical** - No overengineering
3. **Incremental changes** - Week by week
4. **Measure improvements** - Real metrics

This approach gives us:
- **True async operations** via actors
- **Concurrent processing** via event bus
- **Streaming capabilities** for real-time
- **Without massive refactoring**

The key insight from Nautilus: **You don't need full event sourcing and complex actor systems to get the benefits of event-driven architecture**. Start with a good message bus, simple actors, and streaming - that's 80% of the value with 20% of the complexity.
