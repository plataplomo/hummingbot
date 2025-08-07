# Phase 1: API Layer Cache Enhancement - Event-Driven Cache Management

## Executive Summary

Phase 1 focuses on enhancing the current well-functioning API-level caching (60-70% hit rate) by connecting it to the existing EventBus infrastructure. This phase leverages existing components (EventBus, WebSocket routers, cache services) and implements selective cache invalidation for Hyperliquid and Backpack APIs.

**Timeline:** 3-5 days (much shorter due to existing infrastructure)
**Risk Level:** Very Low (connecting existing components)
**Expected Performance Improvement:** 70-80% cache hit rate (modest improvement from current 60-70%)

## Current State Analysis (ACTUAL)

### Hyperliquid API Cache - Working Well
```mermaid
graph TD
    A[Trading Operation] --> B[place_order/cancel_order]
    B --> C[Execute API Call]
    C --> D[No Cache Interaction]

    E[ClearinghouseCacheService] --> F[TTL-based Expiry<br/>5s default]
    F --> G[60-70% Hit Rate]

    H[WebSocket Events] --> I[Processed Separately]
    I --> J[Opportunity for Integration]

    style E fill:#51cf66,color:#333
    style G fill:#51cf66,color:#333
    style J fill:#74c0fc,color:#333
```

### Backpack API Cache - Similar Pattern
```mermaid
graph TD
    A[Account State Service] --> B[Internal Cache<br/>asyncio.Lock]
    B --> C[TTL-based Expiry]

    D[Trading Operations] --> E[No Cache Interaction]

    F[WebSocket Events] --> G[Type-safe Processing]
    G --> H[Opportunity for Integration]

    style B fill:#51cf66,color:#333
    style C fill:#51cf66,color:#333
    style H fill:#74c0fc,color:#333
```

## Phase 1 Architecture Design

### Event-Driven Cache System Overview
```mermaid
graph TD
    subgraph "Phase 1: API Cache Layer"
        A[WebSocket Events] --> B[Event Bus]
        B --> C[Cache Coordinator]
        C --> D[Selective Invalidation Engine]

        D --> E[Hyperliquid Cache]
        D --> F[Backpack Cache]

        G[Trading Operations] --> B
        H[API Responses] --> B
    end

    subgraph "Event Types"
        I[Position Update]
        J[Order Fill]
        K[Balance Change]
        L[Price Update]
    end

    I --> B
    J --> B
    K --> B
    L --> B

    style B fill:#74c0fc,color:#333
    style C fill:#51cf66,color:#333
    style D fill:#ffd93d,color:#333
```

### Event Flow Sequence
```mermaid
sequenceDiagram
    participant WS as WebSocket
    participant EB as Event Bus
        participant CC as Cache Coordinator
    participant SI as Selective Invalidation
    participant HC as Hyperliquid Cache
    participant BC as Backpack Cache

    WS->>EB: Position Update Event
    EB->>CC: Route Event
    CC->>SI: Determine Cache Impact

    alt Position-related update
        SI->>HC: Invalidate Position Cache
        SI->>BC: Keep Account Cache
    else Balance-related update
        SI->>HC: Invalidate Balance Cache
        SI->>BC: Invalidate Account State
    end

    Note over CC: Smart routing based on<br/>event type and scope
```

### Cache Invalidation Strategy
```mermaid
graph TD
    A[Event Received] --> B{Event Analysis}

    B --> C[Position Update]
    B --> D[Order Fill]
    B --> E[Balance Change]
    B --> F[Price Update]

    C --> G[Symbol-Scoped Invalidation]
    C --> H[User-Scoped Invalidation]

    D --> I[Order Cache Invalidation]
    D --> J[Balance Update Cascade]

    E --> K[Account-Wide Invalidation]
    E --> L[Cross-Exchange Sync]

    F --> M[Price Cache Update]
    F --> N[Dependent Cache Cascade]

    style G fill:#51cf66,color:#333
    style H fill:#51cf66,color:#333
    style I fill:#51cf66,color:#333
    style J fill:#74c0fc,color:#333
```

## Module Structure

### Code Tree Structure
```
cyberdelta/
├── core/
│   └── events/
│       ├── __init__.py
│       ├── event_bus.py                    # Core event bus implementation
│       ├── event_types.py                  # Event type definitions
│       └── protocols.py                    # Event handling protocols
│
├── apis/
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── cache_coordinator.py            # Central cache coordination
│   │   ├── invalidation_engine.py          # Selective invalidation logic
│   │   ├── invalidation_rules.py           # Event-to-cache mapping rules
│   │   └── protocols.py                    # Cache management protocols
│   │
│   ├── hyperliquid/
│   │   ├── services/
│   │   │   └── account/
│   │   │       ├── hl_clearinghouse_cache_service.py  # Enhanced with events
│   │   │       └── hl_cache_event_handler.py          # WebSocket integration
│   │   └── hl_api.py                       # Updated API without manual invalidation
│   │
│   ├── backpack/
│   │   ├── services/
│   │   │   ├── bp_account_cache_service.py # Enhanced with events
│   │   │   └── bp_cache_event_handler.py   # WebSocket integration
│   │   └── bp_api.py                       # Updated API with event integration
│   │
│   └── websocket/
│       ├── event_adapters/
│       │   ├── __init__.py
│       │   ├── hyperliquid_event_adapter.py # WebSocket to event conversion
│       │   └── backpack_event_adapter.py    # WebSocket to event conversion
│       └── ws_processor.py                   # Enhanced with event publishing
```

## Implementation Details

### 1. Leverage Existing Event Infrastructure

#### Existing Event Bus (NO CHANGES NEEDED)
```python
# cyberdelta/application/event_bus.py - ALREADY EXISTS
from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Callable, Dict, List, Protocol
from dataclasses import dataclass
from enum import Enum

from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

class EventType(Enum):
    """Core event types for cache management."""
    POSITION_UPDATED = "position_updated"
    ORDER_FILLED = "order_filled"
    BALANCE_CHANGED = "balance_changed"
    PRICE_UPDATED = "price_updated"
    TRADE_EXECUTED = "trade_executed"

@dataclass
class CacheEvent:
    """Base cache event with metadata."""
    event_type: EventType
    exchange: str
    symbol: str | None
    user_id: str | None
    data: Dict[str, Any]
    timestamp: float
    correlation_id: str | None = None

class EventHandler(Protocol):
    """Protocol for event handlers."""
    async def handle(self, event: CacheEvent) -> None: ...

class EventBus:
    """Core event bus for cache coordination."""

    def __init__(self) -> None:
        self._handlers: Dict[EventType, List[EventHandler]] = defaultdict(list)
        self._running = False
        self._event_queue: asyncio.Queue[CacheEvent] = asyncio.Queue()
        self._processor_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start event processing."""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        logger.info("event_bus_started")

    async def stop(self) -> None:
        """Stop event processing."""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        logger.info("event_bus_stopped")

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Subscribe handler to event type."""
        self._handlers[event_type].append(handler)
        logger.debug("event_handler_subscribed",
                    event_type=event_type.value,
                    handler=handler.__class__.__name__)

    async def publish(self, event: CacheEvent) -> None:
        """Publish event to bus."""
        await self._event_queue.put(event)
        logger.debug("event_published",
                    event_type=event.event_type.value,
                    exchange=event.exchange,
                    symbol=event.symbol)

    async def _process_events(self) -> None:
        """Process events from queue."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                handlers = self._handlers.get(event.event_type, [])

                # Process handlers concurrently
                if handlers:
                    await asyncio.gather(
                        *[handler.handle(event) for handler in handlers],
                        return_exceptions=True
                    )

            except asyncio.TimeoutError:
                continue  # Check if still running
            except Exception as e:
                logger.exception("event_processing_error", error=str(e))
```

#### NEW: Cache Coordinator (Minimal Implementation Needed)
```python
# cyberdelta/apis/cache/cache_coordinator.py - NEW FILE
from __future__ import annotations

from typing import Dict, Any
from cyberdelta.application.event_bus import EventBus  # Use existing
from cyberdelta.models.events.base_event import DomainEvent  # Use existing
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

class CacheCoordinator:
    """Central coordinator for all cache operations."""

    def __init__(self, invalidation_engine: InvalidationEngine) -> None:
        self.invalidation_engine = invalidation_engine
        self._cache_services: Dict[str, CacheServiceProtocol] = {}

    def register_cache_service(self, name: str, service: CacheServiceProtocol) -> None:
        """Register a cache service for coordination."""
        self._cache_services[name] = service
        logger.info("cache_service_registered", name=name)

    async def handle(self, event: CacheEvent) -> None:
        """Handle cache events with selective invalidation."""
        try:
            # Determine invalidation strategy
            invalidation_plan = self.invalidation_engine.create_invalidation_plan(event)

            # Execute invalidation plan
            for service_name, actions in invalidation_plan.items():
                service = self._cache_services.get(service_name)
                if service:
                    await self._execute_cache_actions(service, actions, event)

            logger.debug("cache_invalidation_completed",
                        event_type=event.event_type.value,
                        affected_services=list(invalidation_plan.keys()))

        except Exception as e:
            logger.exception("cache_coordination_error",
                           event_type=event.event_type.value,
                           error=str(e))

    async def _execute_cache_actions(
        self,
        service: CacheServiceProtocol,
        actions: Dict[str, Any],
        event: CacheEvent
    ) -> None:
        """Execute cache actions on a specific service."""
        for action_type, action_data in actions.items():
            if action_type == "invalidate_keys":
                for key in action_data:
                    await service.invalidate_key(key)
            elif action_type == "invalidate_pattern":
                await service.invalidate_pattern(action_data)
            elif action_type == "update_cache":
                await service.update_from_event(event)
```

### 2. Selective Invalidation Engine

```python
# cyberdelta/apis/cache/invalidation_engine.py
from __future__ import annotations

from typing import Dict, List, Any
from cyberdelta.core.events.event_bus import CacheEvent, EventType
from cyberdelta.apis.cache.invalidation_rules import INVALIDATION_RULES
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

class InvalidationEngine:
    """Engine for determining selective cache invalidation strategies."""

    def __init__(self) -> None:
        self.rules = INVALIDATION_RULES

    def create_invalidation_plan(self, event: CacheEvent) -> Dict[str, Dict[str, Any]]:
        """Create invalidation plan based on event type and scope."""
        rule = self.rules.get(event.event_type)
        if not rule:
            logger.warning("no_invalidation_rule", event_type=event.event_type.value)
            return {}

        plan: Dict[str, Dict[str, Any]] = {}

        # Apply rules based on event type
        for target_service in rule.target_services:
            plan[target_service] = self._create_service_actions(event, rule, target_service)

        # Apply cascade rules
        for cascade_service in rule.cascade_services:
            if cascade_service not in plan:
                plan[cascade_service] = {}
            plan[cascade_service].update(
                self._create_cascade_actions(event, rule, cascade_service)
            )

        return plan

    def _create_service_actions(
        self,
        event: CacheEvent,
        rule: InvalidationRule,
        service: str
    ) -> Dict[str, Any]:
        """Create actions for a specific service."""
        actions: Dict[str, Any] = {}

        if rule.scope == "symbol_specific" and event.symbol:
            # Invalidate symbol-specific cache keys
            actions["invalidate_pattern"] = f"*{event.symbol}*"
        elif rule.scope == "user_specific" and event.user_id:
            # Invalidate user-specific cache keys
            actions["invalidate_pattern"] = f"*{event.user_id}*"
        elif rule.scope == "exchange_specific":
            # Invalidate exchange-specific cache keys
            actions["invalidate_pattern"] = f"*{event.exchange}*"
        else:
            # Full service invalidation (use sparingly)
            actions["invalidate_pattern"] = "*"

        return actions

    def _create_cascade_actions(
        self,
        event: CacheEvent,
        rule: InvalidationRule,
        service: str
    ) -> Dict[str, Any]:
        """Create cascade actions for dependent services."""
        # Cascade actions are typically lighter - just mark for refresh
        return {"update_cache": event.data}
```

### 3. Enhanced Hyperliquid Cache Integration

```python
# cyberdelta/apis/hyperliquid/services/account/hl_cache_event_handler.py
from __future__ import annotations

from cyberdelta.core.events.event_bus import CacheEvent, EventHandler
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_cache_service import (
    HyperliquidClearinghouseCacheService
)
from cyberdelta.apis.cache.protocols import CacheServiceProtocol
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

class HyperliquidCacheEventHandler(EventHandler):
    """Event handler for Hyperliquid cache operations."""

    def __init__(self, cache_service: HyperliquidClearinghouseCacheService) -> None:
        self.cache_service = cache_service

    async def handle(self, event: CacheEvent) -> None:
        """Handle cache events for Hyperliquid."""
        if event.exchange != "hyperliquid":
            return

        try:
            if event.event_type == EventType.POSITION_UPDATED:
                await self._handle_position_update(event)
            elif event.event_type == EventType.ORDER_FILLED:
                await self._handle_order_fill(event)
            elif event.event_type == EventType.BALANCE_CHANGED:
                await self._handle_balance_change(event)

        except Exception as e:
            logger.exception("hyperliquid_cache_event_error",
                           event_type=event.event_type.value,
                           error=str(e))

    async def _handle_position_update(self, event: CacheEvent) -> None:
        """Handle position update events."""
        if event.symbol:
            # Invalidate position-specific cache entries
            await self.cache_service.invalidate_symbol_cache(event.symbol)
            logger.debug("position_cache_invalidated", symbol=event.symbol)

    async def _handle_order_fill(self, event: CacheEvent) -> None:
        """Handle order fill events."""
        # Invalidate both order and balance caches
        await self.cache_service.invalidate_order_cache()
        if event.symbol:
            await self.cache_service.invalidate_symbol_cache(event.symbol)
        logger.debug("order_cache_invalidated", symbol=event.symbol)

    async def _handle_balance_change(self, event: CacheEvent) -> None:
        """Handle balance change events."""
        # Invalidate account-wide cache
        await self.cache_service.invalidate_account_cache()
        logger.debug("account_cache_invalidated")
```

### 4. WebSocket Event Adapters

```python
# cyberdelta/apis/websocket/event_adapters/hyperliquid_event_adapter.py
from __future__ import annotations

import time
from typing import Any, Dict, Optional
from cyberdelta.core.events.event_bus import CacheEvent, EventType, EventBus
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

class HyperliquidEventAdapter:
    """Adapter to convert Hyperliquid WebSocket messages to cache events."""

    def __init__(self, event_bus: EventBus) -> None:
        self.event_bus = event_bus

    async def process_websocket_message(
        self,
        message_type: str,
        data: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> None:
        """Process WebSocket message and emit cache events."""
        try:
            if message_type == "allMids":
                await self._handle_price_update(data, user_id)
            elif message_type == "user":
                await self._handle_user_update(data, user_id)
            elif message_type == "orderUpdate":
                await self._handle_order_update(data, user_id)

        except Exception as e:
            logger.exception("websocket_event_adaptation_error",
                           message_type=message_type,
                           error=str(e))

    async def _handle_price_update(self, data: Dict[str, Any], user_id: Optional[str]) -> None:
        """Handle price update messages."""
        event = CacheEvent(
            event_type=EventType.PRICE_UPDATED,
            exchange="hyperliquid",
            symbol=None,  # Price updates are typically multi-symbol
            user_id=user_id,
            data=data,
            timestamp=time.time()
        )
        await self.event_bus.publish(event)

    async def _handle_user_update(self, data: Dict[str, Any], user_id: Optional[str]) -> None:
        """Handle user-specific updates (positions, balances)."""
        # Extract position updates
        if "assetPositions" in data:
            for position in data["assetPositions"]:
                event = CacheEvent(
                    event_type=EventType.POSITION_UPDATED,
                    exchange="hyperliquid",
                    symbol=position.get("position", {}).get("coin"),
                    user_id=user_id,
                    data=position,
                    timestamp=time.time()
                )
                await self.event_bus.publish(event)

        # Extract balance updates
        if "crossMaintenanceMarginUsed" in data:
            event = CacheEvent(
                event_type=EventType.BALANCE_CHANGED,
                exchange="hyperliquid",
                symbol=None,
                user_id=user_id,
                data=data,
                timestamp=time.time()
            )
            await self.event_bus.publish(event)

    async def _handle_order_update(self, data: Dict[str, Any], user_id: Optional[str]) -> None:
        """Handle order update messages."""
        for order_update in data.get("orders", []):
            if order_update.get("order", {}).get("orderStatus") == "filled":
                event = CacheEvent(
                    event_type=EventType.ORDER_FILLED,
                    exchange="hyperliquid",
                    symbol=order_update.get("order", {}).get("coin"),
                    user_id=user_id,
                    data=order_update,
                    timestamp=time.time()
                )
                await self.event_bus.publish(event)
```

## Integration Plan (Leveraging Existing Infrastructure)

### Step 1: Connect WebSocket to EventBus (Day 1)
1. **Create Event Adapters**
   - Simple adapter classes to convert WebSocket messages to DomainEvents
   - Wire into existing WebSocket router handlers
   - Publish to existing EventBus

2. **Define Cache Events**
   - Extend DomainEvent for cache-specific events
   - PositionUpdateEvent, BalanceChangeEvent, PriceUpdateEvent
   - Minimal new code required

### Step 2: Subscribe Cache Services (Day 2)
1. **Hyperliquid Cache Integration**
   - Add event handler method to HyperliquidClearinghouseCacheService
   - Subscribe to relevant events via EventBus
   - Use existing `invalidate_cache()` method

2. **Backpack Cache Integration**
   - Add event handler to BackpackAccountStateService
   - Subscribe to account-related events
   - Use existing `invalidate_cache()` and `clear_all_cache()` methods

### Step 3: Implement Selective Invalidation (Day 3-4)
1. **Smart Invalidation Logic**
   - Symbol-specific cache keys
   - User-specific invalidation
   - Simple if/else logic initially

2. **Basic Testing**
   - Unit tests for event flow
   - Integration test with mock WebSocket data
   - Monitor cache statistics

### Step 4: Performance Tuning (Day 5)
1. **Metrics and Monitoring**
   - Use existing cache statistics
   - Log invalidation patterns
   - Adjust TTL values if needed

2. **Documentation**
   - Update CLAUDE.md with new event flow
   - Document invalidation patterns
   - Add usage examples

## Success Metrics

### Realistic Performance Targets
- **Cache Hit Rate**: 75-80% during active trading (vs current 60-70%)
- **API Call Reduction**: 70-75% sustained (vs current 60-70%)
- **Data Freshness**: <2s for critical data (vs current 5s TTL)
- **Memory Efficiency**: Stable memory usage with proper cleanup

### Monitoring Points
- Event processing latency and throughput
- Cache invalidation accuracy and timeliness
- WebSocket integration reliability
- Error rates and recovery times

## Implementation Notes for v0.0.1

### Direct Implementation
- No feature flags or environment variables needed
- Replace existing manual invalidation with event-driven system
- Test thoroughly in development before deploying
- If issues arise, revert the commit

## Risk Mitigation

### Data Consistency
- Event ordering guarantees within symbol/user scope
- Atomic cache operations with proper locking
- Fallback to API calls if cache consistency is questioned

### Performance Impact
- Async event processing to avoid blocking trading operations
- Circuit breakers for event processing failures
- Memory limits and cleanup for event queues

## Phase 1 Summary

**Key Insights from Codebase Research:**
- The existing codebase has **all necessary components** (EventBus, WebSocket routers, cache services)
- Current cache performance is **already good** (60-70% hit rate)
- The enhancement is about **connecting existing pieces**, not building from scratch
- Implementation time reduced from **2-3 weeks to 3-5 days**

**Minimal Changes Required:**
1. Add event adapters (new files, ~200 lines total)
2. Add event handlers to existing cache services (~50 lines per service)
3. Wire WebSocket routers to EventBus (~20 lines per router)
4. No changes to core cache logic or data structures

**Reality vs Original Assessment:**
- **Original**: "Manual invalidation anti-pattern causing ~20% hit rate"
- **Reality**: No manual invalidation, stable 60-70% hit rate
- **Original**: "Need to build Event Bus and coordination"
- **Reality**: EventBus exists and works, just needs connection
- **Original**: "2-3 weeks implementation"
- **Reality**: 3-5 days by leveraging existing infrastructure

This Phase 1 enhancement creates a more responsive cache system by connecting existing components, providing better data freshness without major architectural changes.
