# Fix Dual Event System Plan

## Current State
- **BaseEvent[T]**: Rich, type-safe infrastructure in `infrastructure/events/`
- **PortfolioEvent**: Simple model in `portfolio/portfolio_types/infrastructure.py`
- Components are split between using both

## Solution: Migrate Everything to BaseEvent

### Step 1: Create Migration Type Alias
First, we'll create a compatibility type to ease migration:

```python
# In portfolio/portfolio_types/infrastructure.py
from cyberdelta.core.infrastructure.events import BaseEvent
from typing import Any

# Compatibility alias during migration
PortfolioEvent = BaseEvent[dict[str, Any]]
```

### Step 2: Update Components Using PortfolioEvent
Components that import PortfolioEvent will automatically use BaseEvent through the alias.

### Step 3: Create Adapter for Legacy Code
For code that creates PortfolioEvent instances:

```python
def create_portfolio_event(
    event_type: EventType,
    exchange_id: str,
    timestamp: float,
    data: dict[str, Any],
    metadata: EventMetadata | None = None
) -> BaseEvent[dict[str, Any]]:
    """Create a portfolio event using BaseEvent infrastructure."""
    # Create a generic data event
    from cyberdelta.core.portfolio.events.base import GenericPortfolioEvent
    return GenericPortfolioEvent.create(
        data=data,
        event_type=event_type,
        exchange_id=exchange_id,
        timestamp=timestamp,
        metadata=metadata
    )
```

### Step 4: Update Analytics Components
Update MetricsAggregator and similar components to properly handle BaseEvent.

## Implementation Steps

1. Replace PortfolioEvent class with type alias
2. Create GenericPortfolioEvent for untyped events
3. Update imports and usages
4. Fix type checking issues
5. Remove old PortfolioEvent model