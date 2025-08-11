# Handler Placement Analysis

## Current Handler Pattern in CyberDeltaEngine

After analyzing the actual codebase, handlers are already organized by domain:

```
cyberdelta/
├── domain/
│   └── trading/
│       └── fills/
│           └── fill_handler.py     # Domain-specific handler
├── apis/
│   ├── backpack/
│   │   └── response_handlers/      # API-specific handlers
│   └── hyperliquid/
│       └── response_handlers/      # API-specific handlers
└── symbols/
    └── handlers/                   # Symbol conversion handlers
```

## Better Approach: Domain-Aligned Event Handlers

Instead of a separate `/handlers/` folder, the event handlers should be co-located with their domain logic:

### Option 1: Event Handlers in Each Domain (RECOMMENDED)

```
cyberdelta/
├── domain/
│   ├── trading/
│   │   ├── execution.py
│   │   ├── trading_service.py
│   │   └── trading_event_handlers.py      # 🎯 NEW: Trading event handlers
│   ├── portfolio/
│   │   ├── portfolio_service.py
│   │   └── portfolio_event_handlers.py    # 🎯 NEW: Portfolio event handlers
│   ├── risk/
│   │   ├── risk_service.py
│   │   └── risk_event_handlers.py          # 🎯 NEW: Risk event handlers
│   └── market/
│       ├── market_service.py
│       └── market_event_handlers.py        # 🎯 NEW: Market event handlers
```

**Advantages:**
- Follows existing domain structure
- Co-locates event handling with business logic
- Maintains domain boundaries
- No artificial separation between handler and service
- Easier to find related code

### Option 2: Service Methods Handle Events Directly

```python
# domain/trading/trading_service.py
class TradingService:
    """Existing trading service with event handling methods"""

    async def handle_order_event(self, event: OrderEvent) -> None:
        """Convert event to domain operation"""
        # Event → Domain conversion here
        exchange = ExchangeName(event.exchange)
        symbol = await self.symbol_service.get_symbol(event.symbol, exchange)

        # Call existing domain methods
        await self.process_order_update(
            order_id=event.order_id,
            symbol=symbol,
            exchange=exchange,
            status=self._map_event_status(event.event_type)
        )
```

**Advantages:**
- No new files needed
- Services directly subscribe to events
- Simpler architecture
- Natural extension of existing services

### Option 3: Event Processing Layer in Infrastructure

```
cyberdelta/
├── infrastructure/
│   ├── event_bus/
│   │   ├── msgspec_bus.py
│   │   └── processors/           # Event processors by domain
│   │       ├── trading_processor.py
│   │       ├── portfolio_processor.py
│   │       └── market_processor.py
```

**Advantages:**
- Keeps infrastructure concerns separate
- Clear boundary between events and domain
- All event processing in one place

## Recommended Approach: Domain Event Handlers

Based on CyberDeltaEngine's patterns, I recommend **Option 1** with event handlers in each domain:

### Example: Trading Domain Event Handler

```python
# domain/trading/trading_event_handlers.py
from cyberdelta.events.core import OrderEvent, PositionEvent
from cyberdelta.domain.trading.trading_service import TradingService
from cyberdelta.enums import ExchangeName
from cyberdelta.symbols.service import SymbolService

class TradingEventHandler:
    """Handles events for trading domain - converts events to domain operations."""

    def __init__(
        self,
        trading_service: TradingService,
        symbol_service: SymbolService,
        event_bus: MsgspecEventBus
    ):
        self.trading_service = trading_service
        self.symbol_service = symbol_service

        # Subscribe to relevant events
        event_bus.subscribe(OrderEvent, self.handle_order_event)
        event_bus.subscribe(PositionEvent, self.handle_position_event)

    async def handle_order_event(self, event: OrderEvent) -> None:
        """Convert order event to domain operations."""
        # Convert strings to domain types
        exchange = ExchangeName(event.exchange)
        symbol = await self.symbol_service.get_symbol(event.symbol, exchange)

        # Delegate to existing service methods
        match event.event_type:
            case "filled":
                await self.trading_service.process_fill(
                    order_id=event.order_id,
                    symbol=symbol,
                    exchange=exchange,
                    fill_price=event.fill_price,
                    fill_quantity=event.fill_quantity
                )
            case "cancelled":
                await self.trading_service.cancel_order(
                    order_id=event.order_id,
                    reason=event.reason
                )
```

### Bootstrap Configuration

```python
# application/bootstrap.py
from cyberdelta.domain.trading.event_handlers import TradingEventHandler
from cyberdelta.domain.portfolio.event_handlers import PortfolioEventHandler
from cyberdelta.domain.risk.event_handlers import RiskEventHandler

async def bootstrap_event_system(config: AppSettings):
    # Create event bus
    event_bus = MsgspecEventBus()

    # Create domain services (existing)
    trading_service = TradingService(config, ...)
    portfolio_service = PortfolioService(config, ...)
    risk_service = RiskService(config, ...)

    # Create event handlers (new)
    trading_handler = TradingEventHandler(
        trading_service,
        symbol_service,
        event_bus
    )
    portfolio_handler = PortfolioEventHandler(
        portfolio_service,
        event_bus
    )
    risk_handler = RiskEventHandler(
        risk_service,
        event_bus
    )

    return event_bus
```

## Why Not a Separate Handlers Folder?

1. **Artificial separation** - Handlers are tightly coupled to domain logic
2. **Navigation overhead** - Developers have to jump between folders
3. **Breaks domain cohesion** - Domain logic split across multiple locations
4. **Against existing patterns** - CyberDeltaEngine already co-locates handlers with their domains

## Updated Directory Structure

```
cyberdelta/
├── events/                        # 🎯 NEW - Event definitions only
│   └── core.py                   # 7 msgspec structures
├── domain/                        # Domain logic + event handlers
│   ├── trading/
│   │   ├── trading_service.py
│   │   └── trading_event_handlers.py    # 🎯 NEW
│   ├── portfolio/
│   │   ├── portfolio_service.py
│   │   └── portfolio_event_handlers.py  # 🎯 NEW
│   ├── risk/
│   │   ├── risk_service.py
│   │   └── risk_event_handlers.py        # 🎯 NEW
│   └── market/
│       ├── market_service.py
│       └── market_event_handlers.py      # 🎯 NEW
├── orchestration/                 # 🎯 NEW - Bubus workflows
│   └── workflows.py
└── infrastructure/
    └── event_bus/                # 🎯 NEW - Event infrastructure
        ├── msgspec_bus.py
        └── subscriptions.py
```

## Benefits of This Approach

1. **Domain cohesion** - All trading logic in one place
2. **Clear boundaries** - Events are just data, adapters handle conversion
3. **Testability** - Can test adapters separately from services
4. **Gradual migration** - Can add adapters without changing services
5. **Follows existing patterns** - Similar to existing response_handlers in APIs

## Summary

No separate `/handlers/` folder is needed. Instead:
- Place event handlers within each domain folder (as `<domain>_event_handlers.py`)
- Keep event definitions in `/events/`
- Keep infrastructure in `/infrastructure/event_bus/`
- Keep workflows in `/orchestration/`

This maintains domain boundaries while adding event capabilities.
