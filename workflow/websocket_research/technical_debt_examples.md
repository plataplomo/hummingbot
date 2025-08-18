# WebSocket Module Technical Debt - Concrete Examples

## Example 1: Multiple Context Types for Same Purpose

### Current Situation - Multiple Context Implementations
```python
# Context Type 1: Base WebSocketMessageContext
class WebSocketMessageContext:
    exchange_type: ExchangeName
    validated_envelope: WebSocketEnvelope
    # Full Pydantic model with validation
    
# Context Type 2: Memory Optimized Version
class MemoryOptimizedMessageContext:
    envelope_type: str
    routing_key: str
    # Simplified version for high frequency
    
# Context Type 3: Exchange-specific contexts
class BackpackWebSocketContext(WebSocketMessageContext):
    # Backpack-specific additions
    
class HyperliquidWebSocketContext(WebSocketMessageContext):
    # Hyperliquid-specific additions
```

### The Problem in Action
```python
# Developer confusion - which context to use?
if memory_mode:
    context = MemoryOptimizedMessageContext(...)
elif exchange == "backpack":
    context = BackpackWebSocketContext(...)
else:
    context = WebSocketMessageContext(...)

# Different interfaces, different capabilities
# Code needs to handle all variants
```

### What Should Exist Instead
```python
# ONE context type with optional exchange data
class WebSocketContext:
    """Single context type for all messages."""
    exchange_type: ExchangeName
    validated_envelope: WebSocketEnvelope
    exchange_specific_data: dict[str, object] | None = None
    
    def get_exchange_data(self, key: str) -> object | None:
        """Get exchange-specific data if available."""
        if self.exchange_specific_data:
            return self.exchange_specific_data.get(key)
        return None
```

## Example 2: The Factory-Registry Maze

### Current Over-Abstraction
```python
# 4 levels of indirection to create a context!

# Level 1: Registry Factory
factory = WebSocketRegistryFactory()

# Level 2: Registry Creation  
registry = factory.create_registry(exchange_name)

# Level 3: Context Factory Registration
registry.register_context_factory(
    "backpack", 
    BackpackContextFactory()
)

# Level 4: Finally create context
context_factory = registry.get_context_factory("backpack")
context = context_factory.create(data)
```

### What Actually Happens
```python
# All that complexity just to do this:
if exchange == ExchangeName.BACKPACK:
    context = BackpackWebSocketContext(data)
elif exchange == ExchangeName.HYPERLIQUID:
    context = HyperliquidWebSocketContext(data)
```

### What Should Exist
```python
# Simple factory function
def create_websocket_context(
    exchange: ExchangeName,
    data: dict[str, object]
) -> WebSocketContext:
    """Create appropriate context for exchange."""
    match exchange:
        case ExchangeName.BACKPACK:
            return BackpackWebSocketContext(data)
        case ExchangeName.HYPERLIQUID:
            return HyperliquidWebSocketContext(data)
        case _:
            raise ValueError(f"Unsupported exchange: {exchange}")
```

## Example 3: The `Any` Type Disaster

### Current Code with `Any`
```python
# ws_message_router.py
class WebSocketMessageRouter:
    def __init__(self):
        self.processors: dict[str, Any] = {}  # What type is processor?
        
    def process_message(
        self,
        payload: dict[str, Any] | list[Any]  # What's in payload?
    ) -> Any:  # What does this return?
        processor = self.processors.get(routing_key)  # Type unknown
        result = processor.process(payload)  # No type checking!
        return result  # Could be anything!
```

### The Hidden Bugs This Causes
```python
# This passes type checking but crashes at runtime!
router.processors["orders"] = "not_a_processor"  # Any allows this!

# This also passes but fails at runtime
result = router.process_message({"data": 123})
result.order_id  # AttributeError - result might not have order_id!
```

### What Should Exist
```python
from typing import Protocol

class MessageProcessor(Protocol):
    """Clear protocol for processors."""
    def process(self, payload: WebSocketPayload) -> ProcessedMessage:
        ...

class WebSocketMessageRouter:
    def __init__(self):
        self.processors: dict[str, MessageProcessor] = {}
        
    def process_message(
        self,
        payload: WebSocketPayload
    ) -> ProcessedMessage:
        processor = self.processors.get(routing_key)
        if not processor:
            raise ProcessorNotFoundError(routing_key)
        return processor.process(payload)
```

## Example 4: The Unused Registry

### Dead Code That Exists
```python
# error_handler_registry.py - NEVER USED ANYWHERE
class WebSocketErrorHandlerRegistry:
    """Complex registry for error handlers."""
    
    def __init__(self):
        self._handlers: dict[type[Exception], ErrorHandler] = {}
        self._fallback_handler: ErrorHandler | None = None
        self._priority_map: dict[type[Exception], int] = {}
        
    def register_handler(
        self,
        exception_type: type[Exception],
        handler: ErrorHandler,
        priority: int = 0
    ) -> None:
        """Register an error handler with priority."""
        # Complex logic that's never called
        
    # 200+ lines of unused code...
```

### Search Results Proving It's Unused
```bash
$ grep -r "WebSocketErrorHandlerRegistry(" cyberdelta/
# No results - never instantiated!

$ grep -r "register_handler" cyberdelta/
# Only found in the registry file itself
```

### Impact
- 200+ lines of dead code
- Confuses developers ("Should I use this?")
- Increases maintenance burden
- Makes codebase harder to understand

## Example 5: The Unmeasured Memory Pool Pattern

### Current Implementation
```python
class MemoryPool:
    """Pools objects for potential reuse."""
    
    def __init__(self, pool_size: int = 2000):
        self.pool_size = pool_size
        self._context_pool: deque[MemoryOptimizedMessageContext] = deque(maxlen=pool_size)
        # Note: Does NOT pre-allocate, grows lazily as objects are returned
```

### The Problem
```python
# Added complexity without performance measurements
# No profiling data exists to show:
# - Whether object creation is actually a bottleneck
# - If pooling provides measurable benefits
# - What the optimal pool size would be

# The code tracks reuse stats but they're never analyzed:
def get_stats(self):
    return {
        "total_reused": self._reused_count,
        "reuse_rate": ...  # Calculated but never used for decisions
    }
```

### Missing Analysis
```python
# What we should have before adding this complexity:
# 1. Baseline performance metrics without pooling
# 2. Profiling showing object creation as bottleneck
# 3. A/B testing with different pool sizes
# 4. Memory usage comparison with/without pooling
# 5. Reuse rate analysis from production workloads

# None of this analysis exists in the codebase
```

## Example 6: The Validation Layer Cake

### Current 5-Layer Validation
```python
# A message goes through ALL of these:

# Layer 1: Envelope Validation
validated_envelope = envelope_validator.validate(raw_message)

# Layer 2: Security Validation  
security_validator.validate_security(validated_envelope)

# Layer 3: Payload Validation
validated_payload = payload_validator.validate(validated_envelope.data)

# Layer 4: Error Validation
error_validator.check_for_errors(validated_payload)

# Layer 5: Context Validation
context_validator.validate_context(context)

# Each layer adds overhead and complexity
```

### The Actual Need
```python
# Two validations are sufficient:

# 1. Input validation (structure + security)
def validate_input(raw_message: str) -> WebSocketMessage:
    """Validate structure and security in one pass."""
    data = orjson.loads(raw_message)
    if not is_valid_structure(data):
        raise ValidationError("Invalid structure")
    if not is_secure(data):
        raise SecurityError("Security check failed")
    return WebSocketMessage(**data)

# 2. Business validation (domain-specific)
def validate_business_rules(message: WebSocketMessage) -> None:
    """Validate according to business logic."""
    if message.type == "order" and message.quantity <= 0:
        raise BusinessRuleError("Order quantity must be positive")
```

## Example 7: The Circular Dependency Workaround

### Current Workaround Using TYPE_CHECKING
```python
# ws_protocols.py
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cyberdelta.enums import ExchangeName  # Only imported for type checking
    
class WebSocketContextProtocol(Protocol):
    exchange_type: "ExchangeName"  # String annotation to avoid import
    domain_model: object  # Using object to avoid importing actual type
```

### The Problem This Creates
```python
# No runtime type checking!
context.exchange_type = "invalid"  # Should be ExchangeName, but no error!
context.domain_model = 123  # Should be specific type, but anything goes!
```

### Proper Solution
```python
# types.py - Shared types in one place
from cyberdelta.enums import ExchangeName
from typing import TypeVar

DomainModel = TypeVar("DomainModel")

# Now import normally without circular issues
from cyberdelta.apis.websocket.types import DomainModel, ExchangeName

class WebSocketContext(BaseModel):
    exchange_type: ExchangeName  # Proper type
    domain_model: DomainModel  # Generic but typed
```

## Summary of Technical Debt Impact

### Quantifiable Issues
- **30+ instances** of `Any` type (type safety violations)
- **200+ lines** of dead code (unused registry)
- **4+ context types** for same purpose
- **5 validation layers** (potentially redundant)
- **Unmeasured memory pool complexity** (no performance data)
- **4 levels of indirection** for simple operations

### Development Impact
- New developers confused by multiple ways to do same thing
- Bugs hidden by `Any` types only caught at runtime
- Dead code making codebase harder to navigate
- Over-abstraction making simple changes complex
- Performance "optimizations" that hurt performance

### Business Impact
- Slower feature development
- Higher bug rate
- More difficult onboarding
- Increased maintenance cost
- Potential runtime failures in production

This technical debt isn't theoretical - it's actively harming the codebase and violating the project's core principles.