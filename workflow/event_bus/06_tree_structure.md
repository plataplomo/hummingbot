# Actual Directory Tree Structure with Event System

## Current CyberDeltaEngine Structure + Proposed Additions

```
cyberdelta/
├── apis/                                # ✅ EXISTS - Exchange integrations
│   ├── backpack/
│   │   ├── mappers/                   # Data mapping
│   │   ├── models/                    # Backpack-specific models
│   │   ├── request_builders/
│   │   ├── response_handlers/
│   │   └── bp_api.py                  # Main API
│   ├── hyperliquid/
│   │   ├── mappers/                   # Data mapping
│   │   ├── models/                    # HL-specific models
│   │   ├── services/                  # Service layer
│   │   └── hl_api.py                  # Main API
│   ├── websocket/                     # WebSocket handlers
│   └── integration/                   # Integration layer
│
├── application/                         # ✅ EXISTS - Application layer
│   ├── __init__.py
│   ├── event_bus.py                   # 🔧 TO REPLACE with new implementation
│   ├── service_registry.py
│   └── trading_engine.py              # 🔧 MODIFY to use new events
│
├── config/                             # ✅ EXISTS - Configuration
│   ├── __init__.py
│   ├── app_config.py
│   └── structlog_config.py
│
├── core/                               # ✅ EXISTS - Core business logic
│   ├── enums/                         # Core enumerations
│   └── ...
│
├── domain/                             # ✅ EXISTS - Domain logic + 🎯 NEW event handlers
│   ├── base_event_handler.py          # 🎯 NEW - Base handler with lifecycle (Nautilus-inspired)
│   ├── market/
│   │   ├── market_service.py
│   │   └── market_event_handlers.py         # 🎯 NEW - Market event handlers
│   ├── portfolio/
│   │   ├── portfolio_service.py
│   │   └── portfolio_event_handlers.py     # 🎯 NEW - Portfolio event handlers
│   ├── risk/
│   │   ├── risk_service.py
│   │   └── risk_event_handlers.py           # 🎯 NEW - Risk event handlers
│   ├── safety/
│   ├── signal/
│   ├── strategy/
│   └── trading/
│       ├── trading_service.py
│       ├── execution.py
│       └── trading_event_handlers.py         # 🎯 NEW - Trading event handlers
│
├── enums/                              # ✅ EXISTS - Enumerations
│   ├── __init__.py
│   ├── environment.py
│   ├── events.py                      # EventType, EntityType
│   ├── exchange_names.py              # ExchangeName
│   ├── signals.py                     # SignalType
│   └── trading.py                     # OrderSide, OrderType, etc.
│
│
├── exceptions/                         # ✅ EXISTS - Custom exceptions
│   └── ...
│
# Note: No separate handlers/ folder needed - event handlers
# are co-located with their domain logic (see domain/**/*_event_handlers.py)
│
├── infrastructure/                     # ✅ EXISTS + 🎯 NEW additions
│   ├── events/                        # ✅ EXISTS (but different purpose)
│   ├── persistence/                   # ✅ EXISTS
│   ├── event_bus/                     # 🎯 NEW - Enhanced MsgspecEventBus
│   │   ├── __init__.py
│   │   ├── msgspec_bus.py            # With priorities and request/response
│   │   ├── subscriptions.py
│   │   └── handler_manager.py        # 🎯 NEW - Manages handler lifecycle
│   ├── orchestrator/                  # 🎯 NEW - Bubus orchestrator
│   │   ├── __init__.py
│   │   └── bubus_engine.py
│   └── migration/                     # 🎯 NEW TEMPORARY - Migration helpers
│       ├── __init__.py
│       └── event_adapter.py
│
├── models/                             # ✅ EXISTS - Domain models
│   ├── __init__.py
│   ├── base_validators.py             # StandardModel base
│   ├── derivative_position.py
│   ├── events/                        # Event models
│   │   ├── __init__.py
│   │   ├── domain_event.py           # 🔧 TO DELETE after migration
│   │   └── core.py                   # 🎯 NEW - All 7 msgspec event structures
│   ├── market/
│   │   ├── order.py
│   │   └── fill.py
│   ├── operations.py
│   └── trade_signal.py
│
├── monitoring/                         # ✅ EXISTS - Monitoring
│   └── ...
│
├── orchestration/                     # 🎯 NEW - Bubus workflows
│   ├── __init__.py
│   └── workflows.py                   # PlaceOrderWorkflow, etc.
│
├── protocols/                          # ✅ EXISTS - Protocol definitions
│   └── ...
│
├── strategies/                         # ✅ EXISTS - Trading strategies
│   └── ...
│
├── symbols/                           # ✅ EXISTS - Symbol system
│   ├── __init__.py
│   ├── models.py                     # Symbol, BaseSymbol
│   ├── registry.py                   # SymbolRegistry
│   └── service.py                    # SymbolService
│
├── utils/                             # ✅ EXISTS - Utilities
│   └── ...
│
├── validation/                        # ✅ EXISTS - Validation logic
│   └── ...
│
└── visualization/                     # ✅ EXISTS - Visualization tools
    └── ...
```

## New Directories and Files to Add

### 1. `/cyberdelta/domain/base_event_handler.py` - Base Handler with Lifecycle
```python
# cyberdelta/domain/base_event_handler.py
from enum import Enum
from abc import ABC, abstractmethod

class ComponentState(Enum):
    """Nautilus-inspired component states"""
    PRE_INITIALIZED = "PRE_INITIALIZED"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"

class EventHandlerActor(ABC):
    """Base handler with lifecycle management (Nautilus-inspired)"""
    def __init__(self, handler_id: str, event_bus):
        self.handler_id = handler_id
        self.event_bus = event_bus
        self._state = ComponentState.PRE_INITIALIZED
        self._error_count = 0
        self._cache = {}  # Handler-level caching for performance

    # Lifecycle methods (Nautilus pattern)
    async def start(self):
        """Start handler and transition to RUNNING state"""
        await self.on_start()
        self._state = ComponentState.RUNNING

    async def stop(self):
        """Stop handler and cleanup resources"""
        await self.on_stop()
        self._state = ComponentState.STOPPED

    async def degrade(self):
        """Enter degraded mode (reduced functionality)"""
        self._state = ComponentState.DEGRADED
        await self.on_degrade()

    # Hooks to implement in subclasses
    @abstractmethod
    async def on_start(self): pass
    @abstractmethod
    async def on_stop(self): pass
    async def on_degrade(self): pass
```

### 2. `/cyberdelta/models/events/` - Event Definitions
```python
# cyberdelta/models/events/core.py
import msgspec
from decimal import Decimal
from typing import Literal

class MarketData(msgspec.Struct, tag="market"):
    symbol: str  # String for serialization
    exchange: str  # String for serialization
    data_type: Literal["tick", "orderbook", "trade", "quote"]
    # ... (7 total structures)
```

### 3. Domain Event Handlers - Co-located with Domain Logic
```python
# domain/trading/trading_event_handlers.py
from cyberdelta.domain.base_event_handler import EventHandlerActor, ComponentState

class TradingEventHandler(EventHandlerActor):
    """Trading handler with lifecycle and caching (Nautilus pattern)"""

    async def on_start(self):
        """Initialize resources, warm caches (Nautilus-inspired startup)"""
        # Warm cache with open orders for performance
        open_orders = await self.trading_service.get_open_orders()
        for order in open_orders:
            self._cache[order.order_id] = order
        # Subscribe with appropriate priority
        self.event_bus.subscribe(OrderEvent, self.handle_order, HandlerPriority.HIGH)

    async def on_degrade(self):
        """Enter degraded mode - only cancellations (Nautilus resilience)"""
        self._state = ComponentState.DEGRADED
        # In degraded mode, only process critical operations

    async def on_stop(self):
        """Cleanup and persist state (Nautilus graceful shutdown)"""
        # Save cached data before shutdown
        for order_id, order in self._cache.items():
            await self.trading_service.save_order(order)

# domain/portfolio/portfolio_event_handlers.py
class PortfolioEventHandler(EventHandlerActor):
    """Portfolio handler with state management (Nautilus pattern)"""
    # Implements lifecycle hooks for portfolio tracking

# domain/risk/risk_event_handlers.py
class RiskEventHandler(EventHandlerActor):
    """Risk handler with CRITICAL priority (Nautilus priority routing)"""
    async def on_start(self):
        # Subscribe with CRITICAL priority for immediate processing
        self.event_bus.subscribe(RiskEvent, self.handle_risk, HandlerPriority.CRITICAL)
```

### 4. `/cyberdelta/orchestration/` - Workflows
```python
# cyberdelta/orchestration/workflows.py
from bubus import BaseEvent

class PlaceOrderWorkflow(BaseEvent[str]):
    # ... workflow definitions
```

### 5. `/cyberdelta/infrastructure/event_bus/` - Enhanced Event Bus
```python
# cyberdelta/infrastructure/event_bus/msgspec_bus.py
from enum import Enum

class HandlerPriority(Enum):
    CRITICAL = 0  # Risk checks
    HIGH = 1      # Order validation
    NORMAL = 2    # Regular processing
    LOW = 3       # Logging

class MsgspecEventBus:
    """Enhanced with priorities and request/response (Nautilus-inspired)"""
    def subscribe(self, event_type, handler, priority=HandlerPriority.NORMAL):
        """Subscribe with priority routing (Nautilus pattern)"""
        # CRITICAL handlers execute first, then HIGH, NORMAL, LOW
        pass

    async def request(self, request, timeout=5.0):
        """Request/response pattern (Nautilus synchronous queries)"""
        # Enables synchronous-style queries in async system
        pass

# cyberdelta/infrastructure/event_bus/handler_manager.py
class HandlerManager:
    """Manages lifecycle of all handlers (Nautilus actor management)"""
    async def start_all(self):
        """Start handlers with proper initialization sequence"""
        for handler in self.handlers:
            await handler.start()
            # Handler transitions: PRE_INITIALIZED → RUNNING

    async def stop_all(self):
        """Graceful shutdown in reverse order (Nautilus pattern)"""
        for handler in reversed(self.handlers):
            await handler.stop()
            # Handler transitions: RUNNING → STOPPED

    async def monitor_health(self):
        """Auto-degradation on errors (Nautilus resilience)"""
        for handler in self.handlers:
            if handler._error_count > 10:
                await handler.degrade()
                # Handler transitions: RUNNING → DEGRADED
```

## Files to Modify

### 1. Application Layer
- `application/trading_engine.py` - Use new event bus
- `application/event_bus.py` - Replace with new implementation

### 2. Domain Services (Event Publishers)
- `domain/trading/trading_service.py` - Publish msgspec events
- `domain/portfolio/portfolio_service.py` - Publish msgspec events
- Various domain services to publish new events

### 3. API Layer - NO CHANGES NEEDED
**Important:** APIs do NOT publish events, they only return data:
- `apis/hyperliquid/hl_api.py` - NO CHANGES (doesn't publish events)
- `apis/backpack/bp_api.py` - NO CHANGES (doesn't publish events)
- `apis/websocket/` - May emit msgspec events in future phases

## Files to Delete (After Migration)

- `models/events/domain_event.py` - Old DomainEvent
- `application/event_bus.py` - Old EventBus (after replacing)
- `infrastructure/migration/` - Temporary migration helpers

## Integration Points

### Bootstrap Configuration with Lifecycle Management

```python
# application/bootstrap.py or service_registry.py modification
from cyberdelta.infrastructure.event_bus import MsgspecEventBus, HandlerPriority
from cyberdelta.infrastructure.event_bus.handler_manager import HandlerManager
from cyberdelta.domain.trading.trading_event_handlers import TradingEventHandler
from cyberdelta.domain.portfolio.portfolio_event_handlers import PortfolioEventHandler
from cyberdelta.domain.risk.risk_event_handlers import RiskEventHandler

class EventSystemManager:
    """Manages lifecycle of entire event system"""

    async def initialize(self):
        # Create enhanced event bus
        self.event_bus = MsgspecEventBus()

        # Create handlers with lifecycle
        self.handlers = [
            TradingEventHandler(trading_service, symbol_service, self.event_bus),
            PortfolioEventHandler(portfolio_service, self.event_bus),
            RiskEventHandler(risk_service, self.event_bus),  # Subscribes with CRITICAL priority
        ]

        # Start all handlers (initializes resources, subscribes to events)
        for handler in self.handlers:
            await handler.start()
            print(f"Started {handler.handler_id}: {handler._state}")

    async def shutdown(self):
        """Graceful shutdown in reverse order"""
        for handler in reversed(self.handlers):
            await handler.stop()
            print(f"Stopped {handler.handler_id}: {handler._state}")

    async def check_health(self):
        """Health check for all components"""
        return {
            handler.handler_id: {
                "state": handler._state.value,
                "errors": handler._error_count
            }
            for handler in self.handlers
        }
```

### WebSocket Integration

```python
# apis/websocket/base_handler.py (or similar)
from cyberdelta.models.events.core import MarketData, OrderEvent

class WebSocketHandler:
    def __init__(self, event_bus: MsgspecEventBus):
        self.event_bus = event_bus

    async def handle_message(self, raw: bytes):
        # Decode to msgspec
        event = msgspec.json.decode(raw, type=OrderEvent)
        await self.event_bus.publish(event)
```

## Testing Structure

```
tests/
├── unit/
│   ├── events/
│   │   └── test_event_structures.py
│   ├── domain/
│   │   ├── test_base_event_handler.py          # Test lifecycle management
│   │   ├── trading/
│   │   │   └── test_trading_event_handlers.py
│   │   └── portfolio/
│   │       └── test_portfolio_event_handlers.py
│   └── infrastructure/
│       ├── test_msgspec_bus.py
│       ├── test_handler_priorities.py          # Test priority routing
│       └── test_request_response.py            # Test request/response pattern
└── integration/
    ├── test_event_flow.py
    ├── test_handler_lifecycle.py               # Test start/stop/degrade
    ├── test_degraded_mode.py                   # Test degradation handling
    └── test_websocket_integration.py
```

## Key Differences from Assumed Structure

1. **`domain/` instead of `services/`** - Domain logic is organized differently
2. **`apis/` structure** - More complex with mappers, models per exchange
3. **`infrastructure/events/`** - Already exists (needs investigation)
4. **`core/` directory** - Contains core business logic
5. **No `repositories/` directory** - Persistence is in `infrastructure/persistence/`

## Migration Path Considering Actual Structure

### Phase 1: Add alongside existing
- Add `/models/events/core.py` for msgspec structures
- Add `/domain/*/<domain>_event_handlers.py` for event handlers
- Add `/orchestration/` for workflows
- Add `/infrastructure/event_bus/` for new bus

### Phase 2: Wire in parallel
- Modify `application/trading_engine.py` to support both buses
- Update WebSocket handlers in `apis/websocket/`
- Run both systems in parallel

### Phase 3: Cut over
- Remove `models/events/domain_event.py`
- Replace `application/event_bus.py`
- Clean up migration code

This reflects the ACTUAL structure of CyberDeltaEngine!
