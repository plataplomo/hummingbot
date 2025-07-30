# Pydantic Patterns Guide for Portfolio Tracker Refactor

## Overview

CyberDeltaEngine uses both Pydantic BaseModel and Pydantic dataclasses strategically. This guide explains when to use each pattern during the portfolio tracker cleanup refactor.

## Pattern Selection Guide

### Use Pydantic BaseModel When:

1. **Complex Domain Models** with rich behavior
   - Example: `PortfolioState`, `CleanTradingEngine`, `PortfolioStrategyOrchestrator`
   - These models have methods, properties, and complex validation logic
   - Need mutable state that changes over runtime
   - Require `model_post_init` for complex initialization

2. **Service Classes** that need dependency injection
   - Example: `PerformanceAnalyticsService`, `ConfigLoaderService`
   - Services that maintain state and provide methods
   - Need lifecycle management (initialize/shutdown)

3. **Configuration Objects** that need serialization
   - When you need `model_dump()` or `model_dump_json()`
   - Complex nested configuration structures

### Use Pydantic Dataclasses When:

1. **Events** - Lightweight, immutable data carriers
   - Example: `BalanceChangeEvent`, `PositionUpdateEvent`, `TradeExecutedEvent`
   - Simple data transfer with validation
   - Typically frozen/immutable after creation

2. **Data Transfer Objects (DTOs)**
   - Example: `TradingSignal`, `StrategySignal`, `TradeRequest`
   - Pure data structures passed between components
   - Minimal behavior (maybe simple properties)

3. **Result Types** and **Value Objects**
   - Example: `StrategyPerformance`, `ValidationResult`, `ExecutionResult`
   - Immutable results from calculations or operations
   - Simple validation without complex logic

4. **Configuration Sections** (simple ones)
   - Example: `CacheConfiguration`, `PricingConfiguration`
   - Flat configuration objects without complex nesting

## Code Examples

### BaseModel Pattern
```python
from pydantic import BaseModel, ConfigDict, Field

class CleanTradingEngine(BaseModel):
    """Complex domain model with behavior."""

    unified_factory: UnifiedServiceFactory = Field(..., description="Service factory")

    # State management
    _state: EngineState = Field(default=EngineState.STOPPED)
    _active_orders: dict[str, dict] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    def model_post_init(self, __context: Any) -> None:
        """Complex initialization after validation."""
        self.coordinator = self.unified_factory.get_risk_coordinator()
        self._tasks = []

    async def start(self) -> None:
        """Behavioral method."""
        # Complex logic here
```

### Dataclass Pattern
```python
from pydantic.dataclasses import dataclass
from pydantic import Field, field_validator

@dataclass
class TradingSignal:
    """Simple DTO with validation."""

    symbol: str
    direction: str  # "long" | "short" | "close"
    strength: float  # 0.0 to 1.0
    strategy_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("strength", mode="before")
    @classmethod
    def validate_strength(cls, v: float) -> float:
        """Simple validation only."""
        if not 0.0 <= v <= 1.0:
            raise ValueError("Strength must be between 0 and 1")
        return v
```

## Migration Patterns

### Week 1-2: Type Consolidation & Service Cleanup
- Services: Use BaseModel for service classes
- Value types: Use dataclasses for simple DTOs

### Week 3-4: Legacy Removal & Integration
- Complex orchestrators: BaseModel
- Integration events: Dataclasses
- Coordination results: Dataclasses

### Week 5-6: Engine & Strategy Components
- Engine/Strategy classes: BaseModel (complex behavior)
- Signals: Dataclasses (simple DTOs)
- Performance metrics: Dataclasses (result types)

### Week 7-10: API Integration & Testing
- API response models: Dataclasses
- Test fixtures: Dataclasses
- Integration coordinators: BaseModel

## Key Differences

| Aspect | BaseModel | Pydantic Dataclass |
|--------|-----------|-------------------|
| **Use Case** | Complex domain models | Simple DTOs/Events |
| **Mutability** | Mutable by default | Can be frozen easily |
| **Performance** | Slightly heavier | Lighter weight |
| **Methods** | Rich behavior expected | Minimal behavior |
| **Initialization** | `model_post_init` available | Standard `__post_init__` |
| **Serialization** | Full Pydantic features | Basic serialization |
| **Inheritance** | Complex inheritance trees | Simple structures |

## Best Practices

1. **Start with dataclass** for simple data structures
2. **Upgrade to BaseModel** when you need:
   - Complex validation logic
   - Multiple methods
   - State management
   - Advanced Pydantic features

3. **Keep dataclasses immutable** where possible:
   ```python
   @dataclass(frozen=True)  # For truly immutable
   @dataclass  # For events that might need updates
   ```

4. **Use Field() for defaults** in both patterns:
   ```python
   metadata: dict[str, Any] = Field(default_factory=dict)
   ```

5. **Consistent validation patterns**:
   - Use `field_validator` for both
   - Keep validation simple in dataclasses
   - Complex validation belongs in BaseModel

This approach aligns with CyberDeltaEngine's established patterns while providing clear guidance for the portfolio tracker refactor.
