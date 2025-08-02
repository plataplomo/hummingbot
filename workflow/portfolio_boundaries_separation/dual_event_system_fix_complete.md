# Dual Event System Fix - Complete

## Problem
After the refactoring, we had two competing event systems:
1. **BaseEvent[T]** - Rich, type-safe infrastructure in `infrastructure/events/`
2. **PortfolioEvent** - Simple Pydantic model in `portfolio/portfolio_types/infrastructure.py`

This caused type mismatches where components expected one but received the other.

## Solution Implemented

### 1. Replaced PortfolioEvent Class with Type Alias
```python
# Before: portfolio/portfolio_types/infrastructure.py
class PortfolioEvent(BaseModel):
    event_type: EventType
    exchange_id: str
    timestamp: float
    data: dict[str, Any]
    metadata: EventMetadata | None = None

# After: portfolio/portfolio_types/infrastructure.py
from cyberdelta.core.infrastructure.events import BaseEvent

# Type alias for compatibility
PortfolioEvent = BaseEvent[dict[str, Any]]
```

### 2. Created GenericPortfolioEvent for Untyped Events
Created `portfolio/events/generic_events.py`:
```python
@dataclass
class GenericPortfolioEvent(BaseEvent[dict[str, Any]]):
    """Generic portfolio event for untyped data."""
    
    @classmethod
    def create(cls, event_type: EventType, data: dict[str, Any], ...) -> GenericPortfolioEvent:
        # Factory method for creating generic events
    
    def _serialize_data(self) -> dict[str, Any]:
        # Required abstract method implementation
        return self.data
```

## Benefits

1. **Type Safety**: Components now use the rich BaseEvent infrastructure
2. **Backward Compatibility**: Existing imports of PortfolioEvent still work
3. **Unified System**: Only one event system to maintain
4. **Better Architecture**: Events are now properly in the infrastructure layer

## Migration Path for Existing Code

### Code that imports PortfolioEvent:
```python
# No change needed - type alias handles it
from cyberdelta.core.portfolio.portfolio_types.infrastructure import PortfolioEvent
```

### Code that creates PortfolioEvent instances:
```python
# Before
event = PortfolioEvent(
    event_type=EventType.TRADE_PROCESSED,
    exchange_id="binance",
    timestamp=time.time(),
    data={"trade_id": "123"}
)

# After - use concrete event classes or GenericPortfolioEvent
from cyberdelta.core.portfolio.events.generic_events import GenericPortfolioEvent

event = GenericPortfolioEvent.create(
    event_type=EventType.TRADE_PROCESSED,
    data={"trade_id": "123"},
    exchange_id="binance"
)
```

## Results

- Fixed type mismatches between BaseEvent and PortfolioEvent
- Eliminated duplicate event infrastructure
- Maintained backward compatibility through type alias
- Improved type safety with BaseEvent[T] generics

## Next Steps

1. Gradually migrate code to use specific event classes (TradeProcessedEvent, etc.) instead of GenericPortfolioEvent
2. Update components to leverage BaseEvent's rich features (is_expired(), age, etc.)
3. Consider removing the PortfolioEvent type alias once all code is migrated