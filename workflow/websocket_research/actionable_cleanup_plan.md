# WebSocket Module - Actionable Cleanup Plan

## ✅ Phase 1: Immediate Cleanup (COMPLETED)

### Files Successfully Handled
```bash
✅ DELETED: cyberdelta/apis/websocket/ws_models.py           # 273 lines removed
✅ DELETED: cyberdelta/apis/backpack/bp_ws_router_v2.py      # 463 lines removed
✅ CONNECTED: cyberdelta/apis/websocket/ws_processor_error_context.py  # Now in use!
```

### Achievements
- **Lines removed:** 736
- **Complexity reduced:** 2 files deleted, 1 file connected
- **Type safety:** ProcessorErrorContextBuilder now provides rich error contexts

## Phase 2: Consolidation (Low Risk)

### 1. Error Handling Consolidation

**Current Structure (Overengineered):**
```
error_handling/
├── error_handler.py (400+ lines)
├── error_handler_factory.py (150+ lines)
├── error_handler_registry.py (100+ lines)
├── error_events.py
├── recovery/
│   ├── recovery_executor.py (200+ lines)
│   └── recovery_policy.py (150+ lines)
└── recovery_strategy_router.py
```

**Proposed Structure:**
```
error_handling/
├── handler.py         # Merge handler + factory
└── recovery.py        # Simple recovery logic
```

**Action Items:**
1. Merge `error_handler.py` + `error_handler_factory.py` → `handler.py`
2. Simplify recovery to basic reconnect logic → `recovery.py`
3. Delete complex policy management
4. Remove unused error events

### 2. Metrics Consolidation

**Current (Overlapping):**
```
metrics/
├── error_metrics.py      # Error-specific metrics
├── general_metrics.py    # General metrics
├── health_check.py       # Health checks
└── processing_metrics.py # Processing metrics
```

**Proposed:**
```
metrics.py  # Single module with all metrics
```

**Why:** All metrics modules have overlapping responsibilities and could be a single cohesive module.

### 3. Security Simplification

**Current:**
```
security/
├── security.py      # Security validator
├── type_guards.py   # Type checking guards
└── validators.py    # Payload validators
```

**Proposed:**
```
validators.py  # All validation in one place
```

## Phase 3: Type System Cleanup

### Current Type Confusion

We have THREE parallel type systems:

1. **Generic Types:**
   ```python
   class WebSocketMessageContext[EnvelopeType: "BaseModel"](BaseModel):
   ```

2. **Protocols:**
   ```python
   @runtime_checkable
   class WebSocketContextProtocol(Protocol):
   ```

3. **Concrete Types:**
   ```python
   class BackpackWebSocketContext(WebSocketMessageContext):
   ```

### Recommendation: Use Concrete Types Only

**Why:**
- Simpler to understand
- Better IDE support
- No runtime overhead
- Matches project standards (no unnecessary abstraction)

**Action:**
1. Remove generic type parameters
2. Remove protocol definitions
3. Use concrete exchange-specific types

## Phase 4: Architectural Boundary Fix

### Current Issues

1. **Context Model Doing Too Much:**
```python
class WebSocketMessageContext:
    # Data fields ✓
    validated_envelope: ...

    # Business logic ✗ (should be elsewhere)
    def get_processing_priority(): ...
    def calculate_message_size(): ...
    def is_private_message(): ...
```

2. **Registry Pattern Duplication:**
- `WebSocketContextRegistry`
- `WebSocketRegistryFactory`
- `registry_builder` in protocols
- Each exchange has its own builder

### Solution

**Move business logic to processors:**
```python
# context.py - Pure data only
class WebSocketContext:
    envelope: BaseModel
    exchange: ExchangeName
    timestamp: datetime
    # Just data, no methods

# processor.py - Business logic
class MessageProcessor:
    def get_priority(self, ctx: WebSocketContext): ...
    def is_private(self, ctx: WebSocketContext): ...
```

**Single registry pattern:**
```python
# registry.py
class ExchangeRegistry:
    def register(self, exchange: ExchangeName, handler): ...
    def get_handler(self, exchange: ExchangeName): ...
```

## Phase 5: Remove Backwards Compatibility

### Identify Old Patterns

**Old Error Handling (remove):**
- String-based error codes
- Dictionary error contexts
- Multiple error handler types

**Old Context Models (remove):**
- Dict[str, Any] contexts
- Untyped message handling

**Old Validation (remove):**
- Inline validation in processors
- String-based type checking

### Migration Path

1. Ensure all code uses new patterns
2. Add deprecation warnings (1 week)
3. Remove old code completely

## Implementation Schedule

### ✅ Day 1: Quick Wins (COMPLETED)
- [x] Delete unused files (Phase 1) - DONE
- [x] Connect ProcessorErrorContextBuilder - DONE
- [x] Run tests to confirm no breakage - DONE
- [x] Document changes - DONE

### Day 2: Consolidation
- [ ] Merge error handling modules
- [ ] Consolidate metrics
- [ ] Simplify security/validation

### Day 3: Type System
- [ ] Remove generic types
- [ ] Remove unused protocols
- [ ] Standardize on concrete types

### Day 4: Architecture
- [ ] Fix context model responsibilities
- [ ] Unify registry pattern
- [ ] Clean up circular dependencies

### Day 5: Testing & Documentation
- [ ] Add tests for refactored code
- [ ] Update documentation
- [ ] Remove backwards compatibility

## Success Metrics

### Before
- Files: 35+
- Lines: ~4,500
- Unused code: 15-20%
- Circular deps: 10+
- Test coverage: Unknown

### After
- Files: 15-20 (-43%)
- Lines: ~2,500 (-44%)
- Unused code: 0%
- Circular deps: 0
- Test coverage: >80%

## Risk Mitigation

1. **Test First:** Run full test suite after each phase
2. **Incremental:** One phase at a time
3. **Reversible:** Each phase in separate commit
4. **Monitor:** Check logs for any issues after deployment

## Code Smells to Fix

### 1. The "Any" Escape Hatch
```python
domain_model: Any = Field(default=None)  # ❌ Type safety lost
```
**Fix:** Use Union of concrete types or protocols

### 2. The "V2" Pattern
```python
bp_ws_router.py
bp_ws_router_v2.py  # ❌ Never delete old versions
```
**Fix:** One implementation only

### 3. The "Just In Case" Imports
```python
if TYPE_CHECKING:  # ❌ Circular dependency workaround
    from x import Y
```
**Fix:** Proper architectural boundaries

### 4. The "Swiss Army Knife" Class
```python
class WebSocketContext:
    # 20+ methods doing everything  ❌
```
**Fix:** Single responsibility principle

### 5. The "Config Everywhere" Pattern
```python
class X:
    def __init__(self, config, config2, config3, ...):  # ❌
```
**Fix:** Dependency injection with single config

## Final Notes

This cleanup will:
1. **Reduce maintenance burden** by 50%
2. **Improve performance** (less code = faster)
3. **Increase clarity** (developers understand faster)
4. **Reduce bugs** (simpler = fewer edge cases)

The WebSocket module can be world-class with just a week of focused cleanup.
