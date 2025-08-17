# WebSocket Module - Technical Debt Examples

## ✅ UPDATE: Major Progress Made!

Several issues documented here have been resolved:
- ✅ `ws_models.py` - DELETED (273 lines removed)
- ✅ `bp_ws_router_v2.py` - DELETED (463 lines removed)
- ✅ `ws_processor_error_context.py` - NOW CONNECTED (providing rich error contexts)

---

## 1. ~~Dead Code Example: Entire Unused Model Hierarchy~~ **RESOLVED ✅**

### Location: `ws_models.py` - **DELETED**

~~This entire file (273 lines) defines a complete model hierarchy that is NEVER used:~~

```python
class BaseWebSocketMessage(BaseModel, ABC):
    """Base model for all WebSocket messages."""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_raw(cls, data: dict[str, Any]) -> BaseWebSocketMessage:
        return cls.model_validate(data)

    def to_wire_format(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"timestamp"})

class BaseSubscriptionRequest(BaseWebSocketMessage):
    """Base model for subscription requests."""
    method: Literal["subscribe", "unsubscribe"]
    id: str | None = None

    @abstractmethod
    def to_wire_format(self) -> dict[str, Any]: ...

class BaseSubscriptionResponse(BaseWebSocketMessage):
    """Base model for subscription responses."""
    id: str | None = None
    success: bool
    error: str | None = None
    subscribed_topics: list[str] = Field(default_factory=list)

    @field_validator("error")
    @classmethod
    def validate_error_consistency(cls, v: str | None, info: ValidationInfo):
        # Complex validation logic that's never executed
        if info.data.get("success") and v is not None:
            raise SuccessErrorMismatchError(success=True, has_error=True)
        return v

# ... 200+ more lines of unused models
```

**Problem:**
- Entire inheritance hierarchy built but never imported
- Complex validation logic that never runs
- Abstract methods that have no implementations

**Impact:**
- Confuses developers ("Should I use this?")
- Increases codebase size by ~6%
- Makes refactoring harder (might break "something")

## 2. ~~Duplicate Router Implementation~~ **RESOLVED ✅**

### Active Router: `bp_ws_router.py` (538 lines) - **KEPT**
```python
class BackpackWebSocketRouter(
    WebSocketMessageRouter[BackpackRawWebSocketEnvelope]
):
    """Production router - actually used"""
    def __init__(self, config: AppSettings, ...):
        # Complex initialization
        super().__init__(config, logger, metrics_collector)
        # ... 50+ lines of setup
```

### ~~Unused V2 Router: `bp_ws_router_v2.py` (463 lines)~~ - **DELETED ✅**

**Resolution:**
- ✅ V2 router deleted
- ✅ 463 lines of duplicate code removed
- ✅ Single implementation maintained

## 3. ~~Type Safety Lost Through "Any"~~ **IMPROVED ✅**

### Location: `ws_context.py` & Error Contexts

**Previous Issues:**
```python
# Before: Minimal error contexts with no metadata
error_context = StreamErrorContext(
    connection_id=context.connection_id,
    exchange=context.exchange_type.value,
)  # Only 2 fields!
```

**Now Fixed:**
```python
# After: Rich, typed error contexts
error_context = ProcessorErrorContextBuilder.from_validation_error(
    processor=self,
    payload=payload,
    context=context,
    validation_error=e,
)  # Full metadata with type safety!
```

**Improvements:**
- ✅ ProcessorErrorContextBuilder now provides typed metadata
- ✅ RouterErrorContextBuilder already provided typed metadata
- ✅ No more dict[str, Any] in error handling

**Better Approach:**
```python
from typing import Union

DomainModelType = Union[
    Order, Trade, Balance, Ticker, OrderBook
]

class WebSocketMessageContext:
    domain_model: DomainModelType | None = None
```

## 4. Circular Dependency Workarounds

### Throughout the codebase:

```python
# File: ws_context.py
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # ← Circular dependency workaround
    from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
    from cyberdelta.apis.websocket.metrics.general_metrics import WebSocketMetricsCollector

# File: ws_stream_context.py
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # ← More circular workarounds
    from cyberdelta.apis.websocket.ws_context import WebSocketMessageContext
```

**Problem:**
- Indicates poor architectural boundaries
- Makes imports fragile
- Hard to understand actual dependencies

**Root Cause:**
- Modules depending on each other bidirectionally
- No clear layering or hierarchy

## 5. Overengineered Error Recovery

### Current: 5+ files for simple reconnection

```python
# recovery_policy.py (excerpt)
class RecoveryPolicy:
    max_retries: int
    base_delay: float
    max_delay: float
    backoff_factor: float
    jitter: bool
    retry_on_errors: list[Type[Exception]]

    def calculate_delay(self, attempt: int) -> float:
        delay = min(
            self.base_delay * (self.backoff_factor ** attempt),
            self.max_delay
        )
        if self.jitter:
            delay *= (0.5 + random.random())
        return delay

# recovery_executor.py (excerpt)
class RecoveryExecutor:
    def __init__(
        self,
        policy_manager: RecoveryPolicyManager,
        connection_manager: ConnectionManagerProtocol,
        subscription_manager: SubscriptionManagerProtocol,
        state_manager: StateManagerProtocol,
        message_buffer: MessageBufferProtocol,
    ):
        # 5 managers for basic reconnection!
```

**What's Actually Needed:**
```python
async def reconnect_websocket(ws_url: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            return await connect(ws_url)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
```

**Problem:**
- 500+ lines for what could be 20
- 5 abstraction layers for simple exponential backoff
- Never actually uses the complex features

## 6. Swiss Army Knife Context

### Location: `ws_context.py`

```python
class WebSocketMessageContext(BaseModel):
    # Data fields - Good ✓
    validated_envelope: EnvelopeType
    exchange_type: ExchangeName

    # Business logic - Should be elsewhere ✗
    @computed_field
    @property
    def topic(self) -> str | None:
        """Extract topic based on exchange."""
        if self.exchange_type == ExchangeName.BACKPACK:
            return getattr(self.validated_envelope, "stream", None)
        return getattr(self.validated_envelope, "channel", None)

    @computed_field
    @property
    def is_private_message(self) -> bool:
        """Determine if message is private."""
        private_patterns = {"account", "user", "balance", "orders", "fills"}
        return any(pattern in self.routing_key.lower() for pattern in private_patterns)

    @cached_property
    def message_size_bytes(self) -> int:
        """Calculate message size."""
        # 20+ lines of serialization logic

    @computed_field
    @property
    def processing_priority(self) -> int:
        """Compute processing priority."""
        # Business logic for prioritization
```

**Problem:**
- Context model has 10+ methods
- Mixes data storage with business logic
- Should be split into:
  - Pure data model (context)
  - Business logic service (processor)
  - Metrics calculator (separate)

## 7. Registry Pattern Confusion

### Three Different Registry Implementations:

```python
# 1. WebSocketContextRegistry
class WebSocketContextRegistry:
    def register_context_factory(self, exchange: ExchangeName, factory):
        self._factories[exchange] = factory

# 2. WebSocketRegistryFactory
class WebSocketRegistryFactory:
    @staticmethod
    def create_registry() -> WebSocketContextRegistry:
        return WebSocketContextRegistry()

# 3. In protocols/websocket/registry_builder.py
class WebSocketRegistryBuilder:
    def build(self) -> WebSocketRegistry:
        # Yet another registry pattern
```

**Problem:**
- Three ways to do the same thing
- No clear winner
- Confusing for developers

## 8. Metrics Sprawl

### Four separate metrics modules doing similar things:

```python
# error_metrics.py
class WebSocketErrorMetrics:
    error_count: int
    last_error_time: datetime
    error_by_type: dict[str, int]

# general_metrics.py
class WebSocketMetricsCollector:
    message_count: int
    error_count: int  # Duplicate!
    last_activity: datetime

# processing_metrics.py
class ProcessingMetrics:
    messages_processed: int
    errors_encountered: int  # Duplicate again!
    processing_time: float

# health_check.py
class HealthCheck:
    is_healthy: bool
    error_count: int  # Duplicate yet again!
```

**Problem:**
- Same metrics tracked in 4 places
- No single source of truth
- Inconsistent metric names

## 9. Validation Overkill

### Three layers of validation for same data:

```python
# Layer 1: Pydantic validation
class BackpackRawWebSocketEnvelope(BaseModel):
    stream: str = Field(..., min_length=1)

    @field_validator("stream")
    def validate_stream(cls, v):
        if not v:
            raise ValueError("Stream cannot be empty")
        return v

# Layer 2: Security validator
class SecurityValidator:
    def validate_envelope(self, envelope):
        if not envelope.stream:
            raise SecurityError("Invalid stream")

# Layer 3: Business validator
class WebSocketPayloadValidators:
    @staticmethod
    def validate_stream(stream: str):
        if not stream or len(stream) == 0:
            raise ValidationError("Stream required")
```

**Problem:**
- Same validation done 3 times
- Different error types for same issue
- Performance overhead

## 10. Exception Hierarchy Explosion

### Too many custom exceptions:

```python
# exceptions/base.py
class WebSocketError(Exception): ...

# exceptions/envelope_validation.py
class EnvelopeValidationError(WebSocketError): ...
class EmptyEnvelopeDataError(EnvelopeValidationError): ...
class InvalidEnvelopeStructureError(EnvelopeValidationError): ...
class MissingRequiredFieldError(EnvelopeValidationError): ...

# exceptions/payload_validation.py
class PayloadValidationError(WebSocketError): ...
class PayloadSizeError(PayloadValidationError): ...
class PayloadFormatError(PayloadValidationError): ...

# exceptions/security.py
class SecurityError(WebSocketError): ...
class AuthenticationError(SecurityError): ...
class AuthorizationError(SecurityError): ...

# exceptions/stream.py
class StreamError(WebSocketError): ...
class StreamInterruptedError(StreamError): ...
# ... 20+ more custom exceptions
```

**Problem:**
- 30+ custom exceptions for simple WebSocket handling
- Most are never caught specifically
- Could use 5-6 well-designed exceptions instead

## Summary

These examples show:
1. **Dead code** that confuses and bloats
2. **Duplicate implementations** with no clear winner
3. **Type safety defeats** through Any types
4. **Circular dependencies** indicating poor architecture
5. **Overengineering** of simple features
6. **Responsibility confusion** in classes
7. **Pattern duplication** for same functionality
8. **Metric sprawl** with no single source of truth
9. **Validation overkill** with redundant checks
10. **Exception explosion** with too many types

Each of these can be fixed with focused refactoring, resulting in a cleaner, more maintainable codebase.
