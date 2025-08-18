# WebSocket Module Cleanup Action Plan

## Priority 1: Remove Dead Code (Immediate)

### Files to Delete Completely
```bash
# These files are never used and can be safely deleted
cyberdelta/apis/websocket/error_handling/error_handler_registry.py
cyberdelta/apis/websocket/error_handling/error_handler_factory.py
```

### Modules to Investigate for Removal
```bash
# Check if these are actually needed
cyberdelta/apis/websocket/memory/stream_log_data.py  # No StreamLogDataManager usage
cyberdelta/apis/websocket/validation/error_validator.py  # Redundant validation
```

## Priority 2: Fix Type Safety Violations (Critical)

### Replace `Any` Types
Files needing immediate type fixes:
1. `ws_message_router.py` - 12 instances of `Any`
2. `ws_message_processor.py` - 4 instances of `Any`  
3. `ws_router_error_context.py` - 6 instances of `Any`
4. `ws_stream_context.py` - 3 instances of `Any`

### Specific Replacements Needed
```python
# BEFORE (ws_message_router.py line 62)
payload: dict[str, Any] | list[Any]

# AFTER - Create proper types
from cyberdelta.models.websocket import WebSocketPayload
payload: WebSocketPayload

# BEFORE (ws_message_router.py line 137)
self.processors: dict[str, Any] = {}

# AFTER - Use proper protocol
from cyberdelta.apis.protocols.websocket import MessageProcessorProtocol
self.processors: dict[str, MessageProcessorProtocol] = {}
```

### Fix `object` Type Workarounds
```python
# BEFORE (ws_protocols.py line 71)
domain_model: object

# AFTER - Use generic or union
from typing import Generic, TypeVar
DomainModel = TypeVar('DomainModel')
domain_model: DomainModel
```

## Priority 3: Consolidate Duplicate Systems

### Error Context - Merge into ONE
Current duplicate contexts:
1. `WebSocketRouterErrorContext`
2. `WebSocketProcessorErrorContext`
3. `StreamErrorContext`

**Decision**: Create single `WebSocketErrorContext` with all needed fields.

### Context Types - Unify
Current contexts:
1. `WebSocketMessageContext`
2. `MemoryOptimizedMessageContext`
3. Exchange-specific contexts

**Decision**: Single `WebSocketContext` with optional exchange-specific data.

## Priority 4: Simplify Overengineered Patterns

### Remove Unnecessary Factories
```python
# BEFORE - Factory creating registry
factory = WebSocketRegistryFactory()
registry = factory.create_registry()
context = registry.get_context()

# AFTER - Direct instantiation
context = WebSocketContext.create(exchange, data)
```

### Eliminate Redundant Registries
```python
# BEFORE - Registry for simple mapping
registry = WebSocketContextRegistry()
registry.register("backpack", BackpackContext)
context_class = registry.get("backpack")

# AFTER - Simple dict or match statement
CONTEXT_MAP = {
    ExchangeName.BACKPACK: BackpackContext,
    ExchangeName.HYPERLIQUID: HyperliquidContext,
}
context_class = CONTEXT_MAP[exchange]
```

### Simplify Protocol Usage
```python
# BEFORE - Runtime checkable protocol
@runtime_checkable
class WebSocketEnvelopeProtocol(Protocol):
    data: dict[str, object] | list[object]
    def model_dump(self, *, mode: str = "python") -> dict[str, object]: ...

# AFTER - Simple Pydantic model
class WebSocketEnvelope(BaseModel):
    data: dict[str, object] | list[object]
```

## Priority 5: Evaluate and Document Performance Patterns

### Memory Pool Assessment
```python
# BEFORE deciding to keep or remove:
# 1. Profile current memory pool effectiveness
# 2. Measure actual reuse rates in production
# 3. Compare performance with/without pooling
# 4. Document findings

# IF no measurable benefit found:
if memory_optimization_mode.is_enabled:
    self.memory_pool = MemoryPool(pool_size=2000)  # Remove if not justified

# IF benefit is proven, document:
# - Performance improvement metrics
# - Optimal pool size based on data
# - When to enable/disable pooling
```

### Reduce Validation Layers
Current validation stack (5 layers):
1. Envelope validation
2. Payload validation
3. Security validation  
4. Error validation
5. Context validation

**Reduce to 2 layers**:
1. Input validation (envelope + security)
2. Business validation (domain-specific)

## Priority 6: Fix Module Boundaries

### Resolve Circular Dependencies Properly
Instead of using `TYPE_CHECKING` workarounds:

1. **Move shared types to common module**
   ```
   cyberdelta/apis/websocket/types.py  # All shared types here
   ```

2. **Use dependency injection**
   ```python
   # Instead of importing concrete classes
   class Router:
       def __init__(self, processor: ProcessorProtocol):
           self.processor = processor
   ```

## Implementation Order

### Week 1: Clean House
1. Delete unused files (Priority 1)
2. Run tests to ensure nothing breaks
3. Commit: "chore: Remove unused WebSocket components"

### Week 2: Type Safety
1. Fix all `Any` types (Priority 2)
2. Remove `object` workarounds
3. Run mypy, pyright, ruff - must be clean
4. Commit: "fix: Restore type safety in WebSocket module"

### Week 3: Consolidation
1. Merge duplicate systems (Priority 3)
2. Update all references
3. Test thoroughly
4. Commit: "refactor: Consolidate WebSocket authentication and error handling"

### Week 4: Simplification
1. Remove overengineered patterns (Priority 4)
2. Profile and evaluate performance patterns (Priority 5)
3. Fix module boundaries (Priority 6)
4. Commit: "refactor: Simplify WebSocket architecture"

## Success Metrics

### Before Cleanup
- 10+ architectural layers
- 30+ `Any` type violations
- 4+ context types for same purpose
- 5 validation layers
- Multiple unused modules
- Complex factory-registry patterns

### After Cleanup Target
- 3-4 clear architectural layers
- 0 `Any` types
- 1 unified context type
- 2 validation layers
- 0 unused modules
- Direct instantiation patterns

## Testing Strategy

### Required Tests After Each Change
1. Unit tests for modified components
2. Integration tests for WebSocket data flow
3. End-to-end tests with real exchange connections
4. Performance benchmarks (ensure no regression)

### Specific Test Cases
```python
# Test authentication tracking consolidation
def test_unified_authentication_tracking():
    state = WebSocketConnectionState(...)
    state.mark_channel_authenticated("orders")
    assert state.is_channel_authenticated("orders")
    
# Test simplified error handling
def test_unified_error_context():
    error = WebSocketError(...)
    context = error.create_context()
    assert context.has_all_needed_fields()
```

## Risk Mitigation

1. **Create feature branch** for all changes
2. **Small, incremental commits** - easy to revert
3. **Run full test suite** after each change
4. **Keep old code commented** for 1 sprint, then delete
5. **Document breaking changes** for API users

## Expected Outcome

A clean, simple, type-safe WebSocket module that:
- Follows YAGNI principle
- Has clear architectural boundaries
- Uses proper types throughout
- Is easy to understand and maintain
- Actually connects to exchanges and handles messages

No more "enhancement" PRs that add complexity. Focus on simplicity and correctness.