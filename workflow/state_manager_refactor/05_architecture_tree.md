# State Management Architecture Tree (Pydantic-First)

**Date:** 2025-01-18
**Decision:** Keep ALL Pydantic models, incremental improvements only

---

## 🌳 Current Architecture (What Exists)

```
cyberdelta/
├── domain/
│   ├── portfolio/
│   │   └── state_manager.py (786 lines)
│   │       - PortfolioStateManager
│   │       - Uses Pydantic PortfolioState
│   │       - Has persistence
│   │       - Missing: checksums, events
│   │
│   └── safety/
│       └── state_manager.py (220 lines)
│           - StateManager (generic name!)
│           - Circuit breaker FSM
│           - No persistence
│           - Missing: storage, events
│
├── utils/
│   ├── state_manager.py (481 lines)
│   │   - StateManager (another generic!)
│   │   - Has checksums, backups
│   │   - Uses orjson
│   │   - Missing: domain integration
│   │
│   └── serialization.py
│       - Hybrid approach (KEEP!)
│       - Pydantic → dict → msgspec/orjson
│
├── models/ (ALL STAY PYDANTIC)
│   ├── portfolio/
│   │   ├── state.py
│   │   │   - PortfolioState (Pydantic)
│   │   │   - SpotBalance (Pydantic)
│   │   │   - DerivativePosition (Pydantic)
│   │   └── ...
│   │
│   ├── events/ (msgspec ONLY for events)
│   │   ├── core.py
│   │   │   - OrderEvent (msgspec.Struct)
│   │   │   - PositionEvent (msgspec.Struct)
│   │   └── ...
│   │
│   └── ... (ALL other models stay Pydantic)
│
└── infrastructure/
    └── event_bus/
        └── bus.py
            - EventBus (already uses msgspec!)
            - T = TypeVar("T", bound=msgspec.Struct)
```

---

## 🎯 Improved Architecture (Incremental Changes)

```
cyberdelta/
├── protocols/ (NEW - minimal addition)
│   └── state_management.py
│       - StateManagerProtocol
│       - Common interface for all managers
│       - Uses Pydantic type hints
│
├── domain/
│   ├── portfolio/
│   │   ├── state_manager.py (ENHANCED)
│   │   │   - PortfolioStateManager
│   │   │   - KEEP: Pydantic PortfolioState
│   │   │   - ADD: get_checksum() method
│   │   │   - ADD: emit_change_event() method
│   │   │   - ADD: backup() method
│   │   │
│   │   └── event_handlers.py (NEW - simple)
│   │       - PortfolioEventHandler
│   │       - Adapts events to portfolio service
│   │
│   ├── safety/
│   │   ├── circuit_breaker_manager.py (RENAMED)
│   │   │   - CircuitBreakerStateManager (better name!)
│   │   │   - ADD: persistence via storage
│   │   │   - ADD: CircuitBreakerState (Pydantic)
│   │   │   - ADD: emit_change_event() method
│   │   │
│   │   └── event_handlers.py (NEW - simple)
│   │       - SafetyEventHandler
│   │       - Reacts to risk events
│   │
│   └── state_coordinator.py (NEW - coordination layer)
│       - StateCoordinator
│       - Manages all state managers
│       - System-wide snapshots
│       - Uses Pydantic SystemState
│
├── utils/
│   └── state_manager.py (UNCHANGED)
│       - Keep as-is (already good!)
│       - Share features with domain managers
│
├── models/ (NO CHANGES - ALL PYDANTIC)
│   ├── portfolio/
│   │   └── state.py
│   │       - PortfolioState (Pydantic) ✅
│   │       - SpotBalance (Pydantic) ✅
│   │       - DerivativePosition (Pydantic) ✅
│   │
│   ├── safety/
│   │   └── circuit_breaker.py (NEW)
│   │       - CircuitBreakerState (Pydantic) ✅
│   │
│   ├── events/ (msgspec for events ONLY)
│   │   ├── state_events.py (NEW - minimal)
│   │   │   - StateChanged (msgspec.Struct)
│   │   │   - SystemSnapshot (msgspec.Struct)
│   │   └── core.py (EXISTING)
│   │       - OrderEvent (msgspec.Struct)
│   │       - PositionEvent (msgspec.Struct)
│   │
│   └── system/ (NEW)
│       └── state.py
│           - SystemState (Pydantic) ✅
│           - Aggregates all manager states
│
└── infrastructure/
    └── event_bus/ (NO CHANGES NEEDED)
        └── bus.py
            - EventBus (already perfect!)
            - Already uses msgspec for events
```

---

## 📦 Model Type Summary

### Domain Models (100% Pydantic - NO CHANGES)
```python
# ALL of these stay exactly as they are:
PortfolioState(BaseModel)       # Rich validation
SpotBalance(BaseModel)          # Complex calculations
DerivativePosition(BaseModel)   # Business logic
Order(BaseModel)                 # Validation rules
Position(BaseModel)              # Computed fields
Fill(BaseModel)                  # Domain logic
Symbol(BaseModel)                # Complex validation
AppSettings(BaseModel)           # Configuration
ExchangeConfig(BaseModel)        # Settings
RiskConfig(BaseModel)           # Parameters
```

### Event Models (msgspec.Struct - ONLY for events)
```python
# Simple data transfer objects:
StateChanged(msgspec.Struct)    # State notifications
OrderEvent(msgspec.Struct)      # Order updates
PositionEvent(msgspec.Struct)   # Position changes
MarketEvent(msgspec.Struct)     # Market data
SystemEvent(msgspec.Struct)     # System status
```

### Serialization Flow (Hybrid - KEEP AS-IS)
```python
# Current approach works perfectly:
1. Pydantic model → model_dump() → dict
2. dict → msgspec.encode() → bytes (fast!)
3. bytes → storage/network

# No changes needed to this flow!
```

---

## 🔄 Migration Path (Incremental)

### Week 1: Interfaces & Protocols
```
protocols/
└── state_management.py (NEW)
    - Define StateManagerProtocol
    - All methods use Pydantic types
    - No msgspec in interfaces
```

### Week 2: Add Missing Features
```
domain/portfolio/state_manager.py:
    + get_checksum() using model_dump_json()
    + emit_change_event() with Pydantic serialization
    + backup() method

domain/safety/circuit_breaker_manager.py:
    + Rename from generic StateManager
    + Add persistence using Pydantic
    + Add event emission
```

### Week 3: Coordination Layer
```
domain/state_coordinator.py (NEW):
    - Coordinates all managers
    - System snapshots
    - Batch operations
    - All using Pydantic models
```

### Week 4: Event Integration
```
models/events/state_events.py (NEW):
    - Simple StateChanged event (msgspec)
    - SystemSnapshot event (msgspec)

domain/*/event_handlers.py (NEW):
    - Simple adapters
    - Convert events to service calls
    - No complex logic
```

---

## ❌ What We're NOT Doing

### No Model Conversions
```
❌ PortfolioState → msgspec.Struct (NO!)
❌ SpotBalance → msgspec.Struct (NO!)
❌ DerivativePosition → msgspec.Struct (NO!)
❌ Any Pydantic model → msgspec (NO!)
```

### No Architecture Rewrites
```
❌ New state module from scratch
❌ Replace all serialization
❌ Add Redis/database backends
❌ Complex caching layers
❌ Multi-process state sharing
```

### No Over-Engineering
```
❌ Event sourcing
❌ CQRS patterns
❌ Complex workflows
❌ State machines everywhere
❌ Distributed transactions
```

---

## ✅ What We ARE Doing

### Incremental Improvements
```
✅ Share checksums feature from utils
✅ Add persistence to safety manager
✅ Add events for observability
✅ Create simple coordinator
✅ Keep ALL Pydantic models
```

### Practical Enhancements
```
✅ Better naming (CircuitBreakerStateManager)
✅ Common interface (StateManagerProtocol)
✅ System snapshots for recovery
✅ Event notifications for monitoring
✅ No breaking changes
```

---

## 📊 Decision Matrix

| Component | Current Type | Future Type | Reason |
|-----------|-------------|-------------|---------|
| **PortfolioState** | Pydantic | Pydantic ✅ | Rich validation needed |
| **Order** | Pydantic | Pydantic ✅ | Complex business rules |
| **Position** | Pydantic | Pydantic ✅ | Computed fields |
| **AppSettings** | Pydantic | Pydantic ✅ | One-time parse |
| **Events** | msgspec | msgspec ✅ | Already optimal |
| **State Storage** | Hybrid | Hybrid ✅ | Works perfectly |

---

## 🎯 Final Architecture Principles

1. **Pydantic for Domain** - All business models stay Pydantic
2. **msgspec for Events** - Only simple event structs use msgspec
3. **Hybrid Serialization** - Keep current approach (Pydantic → dict → msgspec/orjson)
4. **Incremental Changes** - No rewrites, only enhancements
5. **Practical Focus** - Solve real problems, not imaginary ones

---

## Summary

This architecture tree shows:
- **ALL domain models remain Pydantic** (no conversions)
- **Events continue using msgspec** (already implemented)
- **Hybrid serialization stays** (already optimal)
- **Incremental improvements only** (low risk)
- **No over-engineering** (practical approach)

The cascade problem makes msgspec conversion impractical. Our current hybrid approach (Pydantic models + msgspec serialization) is the optimal solution.

**Timeline:** 3-4 weeks incremental work
**Risk:** Very low - keeping all existing models
**Benefit:** Better coordination and observability
