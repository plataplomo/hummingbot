# Revised Implementation Plan - Practical Event-Driven Architecture

## Overview

Based on Nautilus Trader's architecture, here's a **simplified, practical plan** that delivers real async benefits without overengineering.

## Core Principle: Enhance, Don't Replace

Instead of building complex event sourcing and actor systems, we'll:
1. **Enhance** our existing EventBus with streaming
2. **Wrap** existing services as simple actors
3. **Add** concurrent query capabilities
4. **Keep** all existing infrastructure

## Phase 1: EventBus Enhancement (3 days)

### Day 1: Add Streaming Support

```python
# cyberdelta/infrastructure/event_bus/extensions.py

from typing import AsyncIterator
import asyncio

def add_streaming_to_event_bus(bus: EventBus) -> None:
    """Monkey-patch streaming support onto existing EventBus"""

    bus._streams = defaultdict(list)

    async def stream(event_type: type, buffer_size: int = 100) -> AsyncIterator:
        queue = asyncio.Queue(maxsize=buffer_size)
        bus._streams[event_type].append(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            bus._streams[event_type].remove(queue)

    # Patch the stream method
    bus.stream = stream

    # Enhance publish to support streaming
    original_publish = bus.publish

    async def enhanced_publish(event):
        await original_publish(event)
        # Stream to subscribers
        for queue in bus._streams.get(type(event), []):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop if full (backpressure)

    bus.publish = enhanced_publish
```

### Day 2: Add Batch Operations

```python
# cyberdelta/infrastructure/event_bus/batch.py

async def batch_request(
    bus: EventBus,
    requests: list[msgspec.Struct],
    timeout: float = 1.0
) -> list[Any]:
    """Send multiple requests concurrently"""

    tasks = [
        bus.request(req, timeout_seconds=timeout)
        for req in requests
    ]

    return await asyncio.gather(*tasks, return_exceptions=True)
```

### Day 3: Test and Integrate

- Test streaming with existing events
- Verify backward compatibility
- No breaking changes to existing code

## Phase 2: Convert Services to Actors (1 week)

### Simple Actor Wrapper Pattern

```python
# cyberdelta/domain/portfolio/position_manager_actor.py

class PositionManagerActor:
    """Thin actor wrapper around existing PositionManager"""

    def __init__(self, position_manager: PositionManager, event_bus: EventBus):
        self._pm = position_manager  # Reuse existing logic!
        self._bus = event_bus
        self._cache = {}  # Simple cache for performance

        # Subscribe to events
        event_bus.subscribe(PositionQuery, self._handle_query)
        event_bus.subscribe(Fill, self._handle_fill)

    async def _handle_query(self, query: PositionQuery) -> None:
        """Handle query using cached data when possible"""

        # Try cache first
        key = f"{query.exchange}:{query.symbol}"
        if key in self._cache:
            position = self._cache[key]
        else:
            # Fall back to original logic
            try:
                position = await self._pm.get_position(
                    Symbol(query.symbol),
                    query.exchange
                )
                self._cache[key] = position
            except PositionNotFoundError:
                position = None

        # Send response
        response = PositionQueryResponse(
            request_id=query.request_id,
            exists=position is not None,
            size=position.size if position else None
        )

        await self._bus.respond(query.request_id, response)

    async def _handle_fill(self, fill: Fill) -> None:
        """Update position and invalidate cache"""

        # Use existing logic
        realized_pnl = await self._pm.update_position_from_fill(fill)

        # Invalidate cache
        key = f"{fill.exchange}:{fill.symbol.value}"
        self._cache.pop(key, None)

        # Publish event
        await self._bus.publish(PositionUpdatedEvent(
            symbol=fill.symbol,
            exchange=fill.exchange,
            realized_pnl=realized_pnl
        ))
```

### Benefits of This Approach

1. **Reuses all existing logic** - No rewriting
2. **Adds caching layer** - Performance boost
3. **Enables concurrent queries** - Via event bus
4. **Backward compatible** - Old code still works

## Phase 3: Add Concurrent Operations (3 days)

### Concurrent Position Service

```python
# cyberdelta/domain/portfolio/concurrent_service.py

class ConcurrentPositionService:
    """Service for parallel position operations"""

    def __init__(self, event_bus: EventBus):
        self._bus = event_bus

    async def get_many_positions(
        self,
        queries: list[tuple[Symbol, ExchangeName]]
    ) -> list[DerivativePosition | None]:
        """Get multiple positions in parallel"""

        # Create queries
        requests = [
            PositionQuery(
                request_id=str(uuid.uuid4()),
                symbol=sym.value,
                exchange=ex,
                query_type="details"
            )
            for sym, ex in queries
        ]

        # Batch request
        responses = await batch_request(self._bus, requests)

        # Convert responses to positions
        return [
            self._response_to_position(r) if isinstance(r, PositionQueryResponse) else None
            for r in responses
        ]

    async def check_positions_exist(
        self,
        queries: list[tuple[Symbol, ExchangeName]]
    ) -> dict[str, bool]:
        """Check multiple positions exist in parallel"""

        positions = await self.get_many_positions(queries)

        return {
            f"{q[1].value}:{q[0].value}": p is not None
            for q, p in zip(queries, positions)
        }
```

### Usage Example

```python
# Before - Sequential (slow)
positions = []
for symbol in symbols:
    pos = await position_manager.get_position(symbol, exchange)
    positions.append(pos)
# Takes: N * 50ms = 500ms for 10 positions

# After - Concurrent (fast)
service = ConcurrentPositionService(event_bus)
positions = await service.get_many_positions([
    (symbol, exchange) for symbol in symbols
])
# Takes: ~50ms for 10 positions (10x faster!)
```

## Phase 4: Add Streaming (2 days)

### Position Update Streaming

```python
# cyberdelta/domain/portfolio/streaming.py

class PositionStreamer:
    """Stream position updates"""

    def __init__(self, event_bus: EventBus):
        self._bus = event_bus

    async def stream_updates(
        self,
        symbols: list[Symbol] | None = None
    ) -> AsyncIterator[PositionEvent]:
        """Stream position updates with filtering"""

        async for event in self._bus.stream(PositionEvent):
            if symbols and event.symbol not in symbols:
                continue
            yield event

    async def stream_pnl(self) -> AsyncIterator[Decimal]:
        """Stream total PnL updates"""

        total_pnl = Decimal(0)

        async for event in self._bus.stream(PositionUpdatedEvent):
            if event.unrealized_pnl:
                total_pnl += event.unrealized_pnl
                yield total_pnl
```

### Usage in Strategy

```python
class MyStrategy(Strategy):
    async def on_start(self):
        # Start streaming position updates
        self._stream_task = asyncio.create_task(
            self._consume_position_stream()
        )

    async def _consume_position_stream(self):
        """Consume position updates in background"""

        streamer = PositionStreamer(self.msgbus)

        async for event in streamer.stream_updates(self.symbols):
            if event.symbol in self.watched_symbols:
                await self._handle_position_update(event)
```

## Implementation Timeline

### Week 1: Foundation
- **Monday**: Enhance EventBus with streaming
- **Tuesday**: Add batch operations
- **Wednesday**: Create PositionManagerActor wrapper
- **Thursday**: Add concurrent service
- **Friday**: Integration testing

### Week 2: Expand
- **Monday-Tuesday**: Convert BalanceManager to actor
- **Wednesday**: Add market data streaming
- **Thursday**: Implement signal publishing
- **Friday**: System testing

### Week 3: Optimize
- **Monday**: Add caching strategies
- **Tuesday**: Implement batching
- **Wednesday**: Add metrics/monitoring
- **Thursday-Friday**: Performance testing

## Measuring Success

### Performance Metrics

```python
# Simple benchmark
async def benchmark_position_queries():
    # Sequential (current)
    start = time.time()
    for symbol in symbols[:100]:
        await position_manager.has_position(symbol, exchange)
    sequential_time = time.time() - start

    # Concurrent (new)
    start = time.time()
    await concurrent_service.check_positions_exist([
        (symbol, exchange) for symbol in symbols[:100]
    ])
    concurrent_time = time.time() - start

    print(f"Sequential: {sequential_time:.2f}s")
    print(f"Concurrent: {concurrent_time:.2f}s")
    print(f"Speedup: {sequential_time/concurrent_time:.1f}x")
```

Expected results:
- Sequential: 5.00s (50ms per query)
- Concurrent: 0.50s (parallel execution)
- Speedup: 10x

### Code Complexity Metrics

- **Lines changed**: < 1000 (mostly additions)
- **Breaking changes**: 0
- **New dependencies**: 0
- **Test coverage**: Maintained at current level

## Risk Mitigation

### Backward Compatibility

All existing code continues to work:
```python
# Old code still works
position = await position_manager.get_position(symbol, exchange)

# New code works alongside
positions = await concurrent_service.get_many_positions(queries)
```

### Gradual Migration

1. Start with read operations (queries)
2. Add write operations (updates) later
3. Migrate one service at a time
4. Keep old interfaces working

### Testing Strategy

```python
# Test both old and new interfaces
async def test_compatibility():
    # Old way
    pos1 = await position_manager.get_position(symbol, exchange)

    # New way
    pos2 = await concurrent_service.get_many_positions([(symbol, exchange)])[0]

    assert pos1 == pos2  # Must be identical
```

## Conclusion

This practical approach delivers:
- **Real concurrency** without complexity
- **10x performance** for bulk operations
- **Streaming support** for real-time updates
- **Zero breaking changes** to existing code
- **3-week implementation** instead of 6+

By following Nautilus Trader's pragmatic patterns, we get most of the benefits of event-driven architecture without the complexity of full event sourcing, CQRS, or distributed actors.
