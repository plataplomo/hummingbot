# Context Models Business Logic Refactoring Plan

## Problem Analysis

The `WebSocketMessageContext` class in `ws_context.py` contains business logic that violates the principle of data models being pure data structures.

### Business Logic Methods Found:

1. **`processing_priority`** (lines 130-147)
   - Computes priority based on routing key patterns
   - Pure business logic - should be in processor
   - NOT USED anywhere except in the context itself!

2. **`is_private_message`** (lines 89-96)
   - Determines if message is private based on routing key
   - Business logic - should be in security/processor
   - NOT USED anywhere except in the context itself!

3. **`message_size_bytes`** (lines 99-126)
   - Calculates message size via JSON serialization
   - Computational logic with caching
   - Used only within the same file for create_error_context

4. **`processing_duration_ms`** (lines 151-157)
   - Calculates elapsed time since processing started
   - Monitoring/metrics logic
   - Used in error context creation

5. **`get_transformer_params()`** (lines 164-171)
   - Returns empty dict - should be abstract or removed
   - Business logic placeholder

6. **`get_symbol_param()`** (lines 173-180)
   - Returns None - should be abstract or removed
   - Business logic placeholder

7. **`get_coin_param()`** (lines 182-189)
   - Returns None - should be abstract or removed
   - Business logic placeholder

## Key Finding: These Methods Are UNUSED!

**Shocking discovery**: Most of these business logic methods are not actually used anywhere in the codebase:
- `processing_priority` - NOT USED
- `is_private_message` - NOT USED
- `message_size_bytes` - Only used internally for error context
- `processing_duration_ms` - Only used for error context

## Refactoring Plan

### Step 1: Delete Unused Methods ✅ Immediate Win

**Delete these completely unused methods:**
```python
# DELETE - Not used anywhere
@computed_field
@property
def processing_priority(self) -> int:
    # 18 lines of unused code

# DELETE - Not used anywhere
@computed_field
@property
def is_private_message(self) -> bool:
    # 8 lines of unused code
```

**Impact**: Remove 26 lines of dead code immediately

### Step 2: Move Metrics Logic to Metrics Module

**Current (in context model):**
```python
@cached_property
def message_size_bytes(self) -> int:
    # Complex calculation logic

@computed_field
@property
def processing_duration_ms(self) -> float:
    # Time calculation
```

**Proposed (in metrics module):**
```python
# In cyberdelta/apis/websocket/metrics/message_metrics.py
class MessageMetrics:
    @staticmethod
    def calculate_message_size(context: WebSocketMessageContext) -> int:
        """Calculate message size in bytes."""
        # Move logic here

    @staticmethod
    def calculate_processing_duration(context: WebSocketMessageContext) -> float:
        """Calculate processing duration in ms."""
        return (time.perf_counter() - context.processing_start_time) * 1000
```

### Step 3: Remove Empty Placeholder Methods

**Delete these do-nothing methods:**
```python
# DELETE - Returns empty dict
def get_transformer_params(self) -> dict[str, str]:
    return {}

# DELETE - Returns None
def get_symbol_param(self) -> dict[str, str] | None:
    return None

# DELETE - Returns None
def get_coin_param(self) -> dict[str, str] | None:
    return None
```

**Impact**: Remove 27 lines of placeholder code

### Step 4: Simplify Context to Pure Data

**Final WebSocketMessageContext:**
```python
class WebSocketMessageContext[EnvelopeType: "BaseModel"](BaseModel):
    """Pure data context for WebSocket message processing."""

    # Core data fields only
    validated_envelope: EnvelopeType
    exchange_type: ExchangeName
    routing_key: str
    timestamp: datetime
    message_id: str
    connection_id: str
    symbol: str | None = None
    user_id: str | None = None
    processing_start_time: float
    domain_model: Any = None

    # Simple computed properties are OK (just data access)
    @computed_field
    @property
    def exchange_name(self) -> str:
        return self.exchange_type.value

    @computed_field
    @property
    def topic(self) -> str | None:
        # Simple data extraction, not business logic
        if self.exchange_type == ExchangeName.BACKPACK:
            return getattr(self.validated_envelope, "stream", None)
        return getattr(self.validated_envelope, "channel", None)

    # Keep only the error context creation (needed for error handling)
    def create_error_context(self, ...) -> StreamErrorContext:
        # This is infrastructure, not business logic
```

## Benefits of This Refactoring

1. **Immediate Impact**: Delete ~53 lines of unused/placeholder code
2. **Clear Separation**: Context becomes pure data model
3. **Better Testing**: Business logic in processors is easier to test
4. **No Breaking Changes**: Unused methods can be safely deleted
5. **Performance**: Remove unnecessary computed fields

## Implementation Order

1. **Phase 1** (5 minutes): Delete unused methods
   - Remove `processing_priority`
   - Remove `is_private_message`
   - Remove placeholder methods

2. **Phase 2** (15 minutes): Move metrics logic
   - Create `MessageMetrics` class
   - Move `message_size_bytes` logic
   - Move `processing_duration_ms` logic
   - Update error context creation to use metrics

3. **Phase 3** (5 minutes): Clean up
   - Remove unused imports
   - Update documentation
   - Run type checkers

## Risk Assessment

**Very Low Risk:**
- Most methods being deleted are completely unused
- Metrics logic is only used internally
- No external dependencies on these methods
- Type checkers will catch any issues

## Code Smell Fixed

This refactoring fixes the **"Smart Data Object"** anti-pattern where data models contain business logic they shouldn't know about. After refactoring:
- Context = Pure data
- Processors = Business logic
- Metrics = Monitoring logic

## Conclusion

This is a perfect example of **YAGNI** (You Aren't Gonna Need It) violation. These methods were added "just in case" but never used. Removing them makes the code:
- **53 lines shorter**
- **Clearer in purpose**
- **Easier to maintain**
- **Better architected**
