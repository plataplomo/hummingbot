# Event-Driven Architecture Refactoring for PositionManager

## Current Issues

The current implementation is **pseudo-async** - synchronous logic wrapped in async functions without leveraging the event-driven architecture:

### 1. Synchronous Check Pattern
```python
async def has_position(self, symbol, exchange) -> bool:
    # This is synchronous logic in async wrapper
    state = await self._state_manager.get_state()
    key = f"{exchange}:{symbol}"
    return key in state.positions  # Simple dict lookup
```

### 2. Direct State Access
- Direct dictionary lookups
- No event emissions for queries
- No reactive streams
- No subscription patterns

### 3. Missed Async Opportunities
- No concurrent position lookups
- No event-driven queries
- No streaming updates
- No reactive position tracking

## Proposed Event-Driven Architecture

### 1. Query/Response Pattern

Create position query events using the EventBus request/response pattern:

```python
# models/events/portfolio/queries.py
import msgspec
from decimal import Decimal
from cyberdelta.enums import ExchangeName

class PositionQuery(msgspec.Struct, tag="position_query"):
    """Query for position information."""
    request_id: str
    symbol: str
    exchange: ExchangeName
    query_type: str  # "exists", "details", "pnl"

class PositionQueryResponse(msgspec.Struct, tag="position_response"):
    """Response to position query."""
    request_id: str
    symbol: str
    exchange: ExchangeName
    exists: bool
    position: DerivativePosition | None = None
    pnl: Decimal | None = None
    error: str | None = None

class PositionStreamRequest(msgspec.Struct, tag="position_stream"):
    """Request for position update stream."""
    request_id: str
    symbol: str | None = None  # None for all positions
    exchange: ExchangeName | None = None  # None for all exchanges

class PositionSnapshot(msgspec.Struct, tag="position_snapshot"):
    """Snapshot of all positions."""
    request_id: str | None = None
    positions: dict[str, dict]  # Serialized positions
    total_exposure: Decimal
    timestamp: float
```

### 2. Event-Driven Position Manager

```python
class EventDrivenPositionManager(PositionManagerProtocol):
    """Truly async, event-driven position manager."""

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManagerProtocol,
        pnl_calculator: PnLCalculatorProtocol,
        event_bus: EventBus,
    ):
        self.config = config
        self._state_manager = state_manager
        self._pnl_calculator = pnl_calculator
        self._event_bus = event_bus

        # Subscribe to queries
        self._event_bus.subscribe(PositionQuery, self._handle_position_query)
        self._event_bus.subscribe(PositionStreamRequest, self._handle_stream_request)

        # Active streams
        self._position_streams: dict[str, asyncio.Task] = {}

    async def _handle_position_query(self, query: PositionQuery) -> None:
        """Handle position queries via events."""
        try:
            state = await self._state_manager.get_state()
            key = f"{query.exchange.value}:{query.symbol}"

            if query.query_type == "exists":
                exists = key in state.positions
                response = PositionQueryResponse(
                    request_id=query.request_id,
                    symbol=query.symbol,
                    exchange=query.exchange,
                    exists=exists,
                )
            elif query.query_type == "details":
                position = state.positions.get(key)
                response = PositionQueryResponse(
                    request_id=query.request_id,
                    symbol=query.symbol,
                    exchange=query.exchange,
                    exists=position is not None,
                    position=position,
                )
            elif query.query_type == "pnl":
                position = state.positions.get(key)
                if position and position.entry_price:
                    # Would need current price from market data
                    pnl = self._calculate_pnl(position)
                    response = PositionQueryResponse(
                        request_id=query.request_id,
                        symbol=query.symbol,
                        exchange=query.exchange,
                        exists=True,
                        pnl=pnl,
                    )
                else:
                    response = PositionQueryResponse(
                        request_id=query.request_id,
                        symbol=query.symbol,
                        exchange=query.exchange,
                        exists=False,
                    )

            # Respond via event bus
            await self._event_bus.respond(query.request_id, response)

        except Exception as e:
            error_response = PositionQueryResponse(
                request_id=query.request_id,
                symbol=query.symbol,
                exchange=query.exchange,
                exists=False,
                error=str(e),
            )
            await self._event_bus.respond(query.request_id, error_response)

    async def _handle_stream_request(self, request: PositionStreamRequest) -> None:
        """Start streaming position updates."""
        stream_id = request.request_id

        # Cancel existing stream if any
        if stream_id in self._position_streams:
            self._position_streams[stream_id].cancel()

        # Start new stream
        task = asyncio.create_task(
            self._stream_positions(stream_id, request.symbol, request.exchange)
        )
        self._position_streams[stream_id] = task

    async def _stream_positions(
        self,
        stream_id: str,
        symbol: str | None,
        exchange: ExchangeName | None
    ) -> None:
        """Stream position updates at intervals."""
        try:
            while True:
                state = await self._state_manager.get_state()

                # Filter positions based on criteria
                positions = {}
                for key, pos in state.positions.items():
                    if symbol and not key.endswith(f":{symbol}"):
                        continue
                    if exchange and not key.startswith(f"{exchange.value}:"):
                        continue
                    positions[key] = pos.model_dump()

                # Calculate total exposure
                total_exposure = sum(
                    pos.size * (pos.entry_price or Decimal(0))
                    for pos in state.positions.values()
                    if self._matches_filter(pos, symbol, exchange)
                )

                # Emit snapshot
                snapshot = PositionSnapshot(
                    request_id=stream_id,
                    positions=positions,
                    total_exposure=total_exposure,
                    timestamp=time.time(),
                )
                await self._event_bus.publish(snapshot)

                # Stream interval from config
                await asyncio.sleep(self.config.portfolio.position_stream_interval_sec)

        except asyncio.CancelledError:
            logger.info(f"Position stream {stream_id} cancelled")
        except Exception as e:
            logger.error(f"Position stream {stream_id} error: {e}")
```

### 3. Reactive Position Updates

```python
class ReactivePositionTracker:
    """Reactive position tracking with observables."""

    def __init__(self, event_bus: EventBus):
        self._event_bus = event_bus
        self._position_observers: dict[str, list[Callable]] = defaultdict(list)

        # Subscribe to all position events
        event_bus.subscribe(PositionEvent, self._on_position_event)

    async def _on_position_event(self, event: PositionEvent) -> None:
        """React to position changes."""
        key = f"{event.exchange.value}:{event.symbol}"

        # Notify all observers for this position
        for observer in self._position_observers[key]:
            try:
                await observer(event)
            except Exception as e:
                logger.error(f"Observer error: {e}")

    def observe_position(
        self,
        symbol: str,
        exchange: ExchangeName,
        callback: Callable[[PositionEvent], Awaitable[None]]
    ) -> None:
        """Subscribe to position changes."""
        key = f"{exchange.value}:{symbol}"
        self._position_observers[key].append(callback)

    def stop_observing(
        self,
        symbol: str,
        exchange: ExchangeName,
        callback: Callable
    ) -> None:
        """Unsubscribe from position changes."""
        key = f"{exchange.value}:{symbol}"
        if callback in self._position_observers[key]:
            self._position_observers[key].remove(callback)
```

### 4. Concurrent Position Operations

```python
class ConcurrentPositionManager:
    """Handle multiple position operations concurrently."""

    async def get_multiple_positions(
        self,
        queries: list[tuple[Symbol, ExchangeName]]
    ) -> dict[str, DerivativePosition | None]:
        """Get multiple positions concurrently."""

        # Create queries with unique IDs
        requests = []
        request_map = {}

        for symbol, exchange in queries:
            request_id = str(uuid.uuid4())
            query = PositionQuery(
                request_id=request_id,
                symbol=symbol.value,
                exchange=exchange,
                query_type="details",
            )
            requests.append(query)
            request_map[request_id] = f"{exchange.value}:{symbol.value}"

        # Send all queries concurrently
        responses = await asyncio.gather(*[
            self._event_bus.request(query, timeout_seconds=1.0)
            for query in requests
        ])

        # Map responses back
        result = {}
        for response in responses:
            if response and hasattr(response, 'request_id'):
                key = request_map[response.request_id]
                result[key] = response.position

        return result

    async def update_positions_batch(
        self,
        updates: list[tuple[Symbol, ExchangeName, DerivativePosition]]
    ) -> list[bool]:
        """Update multiple positions concurrently."""

        tasks = [
            self._update_single_position(symbol, exchange, position)
            for symbol, exchange, position in updates
        ]

        return await asyncio.gather(*tasks, return_exceptions=False)
```

### 5. Event-Sourced Position State

```python
class EventSourcedPositionState:
    """Position state built from event stream."""

    def __init__(self):
        self._event_log: list[PositionEvent] = []
        self._snapshots: dict[float, dict] = {}
        self._current_state: dict[str, DerivativePosition] = {}

    async def apply_event(self, event: PositionEvent) -> None:
        """Apply event to state."""
        self._event_log.append(event)

        key = f"{event.exchange.value}:{event.symbol}"

        if event.event_type == PositionEventType.OPENED:
            self._current_state[key] = self._create_position(event)
        elif event.event_type == PositionEventType.UPDATED:
            if key in self._current_state:
                self._current_state[key] = self._update_position(
                    self._current_state[key], event
                )
        elif event.event_type in (PositionEventType.CLOSED, PositionEventType.LIQUIDATED):
            self._current_state.pop(key, None)

    async def rebuild_from_events(
        self,
        events: list[PositionEvent],
        after_timestamp: float | None = None
    ) -> None:
        """Rebuild state from event history."""
        self._current_state.clear()

        for event in events:
            if after_timestamp and event.timestamp <= after_timestamp:
                continue
            await self.apply_event(event)

    def create_snapshot(self) -> dict:
        """Create point-in-time snapshot."""
        snapshot = {
            "timestamp": time.time(),
            "positions": {k: v.model_dump() for k, v in self._current_state.items()},
            "event_count": len(self._event_log),
        }
        self._snapshots[snapshot["timestamp"]] = snapshot
        return snapshot
```

## Implementation Plan

### Phase 1: Add Query/Response Events
1. Create event models for position queries
2. Implement request/response handlers
3. Add timeout handling

### Phase 2: Implement Streaming
1. Add position streaming support
2. Create reactive observers
3. Implement subscription management

### Phase 3: Concurrent Operations
1. Batch position queries
2. Parallel position updates
3. Concurrent PnL calculations

### Phase 4: Event Sourcing (Optional)
1. Event log persistence
2. State reconstruction
3. Snapshot optimization

## Benefits

1. **True Async Operations**: Leverages event loop for concurrent operations
2. **Reactive Updates**: Subscribers get real-time position changes
3. **Scalability**: Can handle thousands of concurrent position queries
4. **Decoupling**: Position logic separated from state access
5. **Testing**: Easy to test with event mocking
6. **Audit Trail**: Event sourcing provides complete history

## Migration Strategy

1. Keep existing PositionManager for compatibility
2. Implement EventDrivenPositionManager alongside
3. Gradually migrate consumers to event-based queries
4. Remove old implementation once migration complete

## Configuration

```yaml
portfolio:
  position_stream_interval_sec: 1.0
  query_timeout_sec: 0.5
  max_concurrent_queries: 100
  enable_event_sourcing: false
  snapshot_interval_sec: 60.0
```

## Example Usage

```python
# Query single position
query = PositionQuery(
    request_id=str(uuid.uuid4()),
    symbol="BTC",
    exchange=ExchangeName.HYPERLIQUID,
    query_type="exists"
)
response = await event_bus.request(query, timeout_seconds=0.5)
if response and response.exists:
    print(f"Position exists: {response.position}")

# Stream all positions
stream_request = PositionStreamRequest(
    request_id="main_stream",
    symbol=None,  # All symbols
    exchange=None,  # All exchanges
)
await event_bus.publish(stream_request)

# Subscribe to snapshots
event_bus.subscribe(PositionSnapshot, handle_position_snapshot)

# Observe specific position
tracker.observe_position(
    symbol="ETH",
    exchange=ExchangeName.BACKPACK,
    callback=on_eth_position_change
)
```

## Conclusion

The current implementation is not truly async or event-driven. This refactoring would:
- Eliminate synchronous patterns wrapped in async
- Leverage the EventBus for all operations
- Enable reactive position tracking
- Support concurrent operations
- Provide streaming capabilities
- Create a scalable, testable architecture

The key insight is that **checking if a position exists should be an event query, not a synchronous dictionary lookup**.
