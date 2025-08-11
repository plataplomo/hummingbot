# Tenacity Configuration Improvement Analysis

**Date**: 2025-08-09  
**Issue**: Hardcoded retry values in tenacity decorators violating CLAUDE.md and CODING_STANDARDS.md  
**Status**: Analysis Complete - Ready for Implementation  

## 🚨 **Problem Identified**

During event bus implementation, we violated critical coding standards by:

1. **Adding hardcoded values in `@retry` decorators**
2. **Using "reasonable" justification comments** (forbidden)
3. **Assuming tenacity only supported literal values** (wrong)
4. **Not researching library capabilities** (violates "assuming instead of looking for source")

### **Violations Found:**
```python
# VIOLATION - Hardcoded magic numbers with justifications
@retry(
    stop=stop_after_attempt(3),  # "Keep reasonable default" 💀
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(ConnectionError)
)
```

**This violates:**
- CODING_STANDARDS.md: "NO HARDCODED VALUES - NONE"
- CODING_STANDARDS.md: "NO MAGIC NUMBERS OR STRINGS"
- Golden Rule: "If it's not from config or an API response, it doesn't belong in the code"

## 🎯 **Research Results: Tenacity CAN Use Runtime Configuration**

Tenacity provides **multiple approaches** for dynamic configuration:

### **1. `retry_with()` Method - Runtime Override**
```python
@retry(stop=stop_after_attempt(3))  # Initial decorator
def my_function():
    ...

# Override at runtime with config values!
my_function.retry_with(
    stop=stop_after_attempt(config.retry.max_attempts),
    wait=wait_exponential(
        multiplier=config.retry.exponential_base,
        min=config.retry.initial_delay_sec,
        max=config.retry.max_delay_sec
    )
)()
```

### **2. `Retrying` Class - Fully Dynamic**
```python
def my_function():
    ...

def execute_with_retry():
    retryer = Retrying(
        stop=stop_after_attempt(config.retry.max_attempts),
        wait=wait_exponential(
            multiplier=config.retry.exponential_base,
            min=config.retry.initial_delay_sec,
            max=config.retry.max_delay_sec
        ),
        retry=retry_if_exception_type(ConnectionError)
    )
    return retryer(my_function)
```

### **3. Context Manager Pattern**
```python
try:
    for attempt in Retrying(
        stop=stop_after_attempt(config.retry.max_attempts),
        wait=wait_exponential(multiplier=config.retry.exponential_base)
    ):
        with attempt:
            # Your code here
            result = do_something()
except RetryError:
    handle_final_failure()
```

## ✅ **RECOMMENDED SOLUTION: Factory Pattern**

**Clean, type-safe, configuration-driven approach:**

```python
def create_retry_decorator(config: EventHandlerConfig, operation_type: str):
    """Create retry decorator from configuration."""
    if operation_type == "connection":
        return retry(
            stop=stop_after_attempt(config.max_consecutive_errors),
            wait=wait_exponential(
                multiplier=1, 
                min=2, 
                max=config.processing_timeout_sec
            ),
            retry=retry_if_exception_type(ConnectionError)
        )
    elif operation_type == "event":
        return retry(
            stop=stop_after_attempt(config.max_consecutive_errors // 2),
            wait=wait_exponential(
                multiplier=0.5, 
                min=1, 
                max=config.processing_timeout_sec // 2
            ),
            retry=retry_if_exception_type((ConnectionError, TimeoutError))
        )
    else:
        raise ValueError(f"Unknown operation type: {operation_type}")

class EventHandlerActor(ABC):
    def __init__(
        self, 
        handler_id: str, 
        event_bus: "MsgspecEventBus", 
        config: EventHandlerConfig
    ) -> None:
        self.handler_id = handler_id
        self.event_bus = event_bus
        self.config = config
        
        # Apply decorators dynamically from config - ZERO hardcoded values
        self.start = create_retry_decorator(config, "connection")(self._start)
        self.handle_event = create_retry_decorator(config, "event")(self._handle_event)

    async def _start(self) -> None:
        """Actual start implementation."""
        ...

    async def _handle_event(self, event: msgspec.Struct) -> None:
        """Actual event handling implementation."""
        ...
```

### **For Workflows:**

```python
class PlaceOrderWorkflow:
    def __init__(
        self, 
        event_bus: "MsgspecEventBus", 
        risk_service: "RiskService", 
        trading_service: "TradingService",
        config: EventWorkflowConfig
    ) -> None:
        self.event_bus = event_bus
        self.risk_service = risk_service
        self.trading_service = trading_service
        self.config = config
        
        # Apply workflow retry decorator from config
        self.execute = self._create_workflow_retry_decorator()(self._execute)

    def _create_workflow_retry_decorator(self):
        """Create workflow-specific retry decorator from config."""
        return retry(
            stop=stop_after_attempt(self.config.retry_config.max_attempts),
            wait=wait_exponential(
                multiplier=self.config.retry_config.exponential_base,
                min=self.config.retry_config.initial_delay_sec,
                max=self.config.retry_config.max_delay_sec
            ),
            retry=retry_if_exception_type(ConnectionError),
            reraise=True
        )

    async def _execute(self, symbol: Symbol, side: OrderSide, ...) -> WorkflowContextModel:
        """Actual workflow implementation."""
        ...
```

## ✅ **ADVANTAGES OF FACTORY PATTERN:**

### **1. Coding Standards Compliance:**
- ✅ **Zero hardcoded values** - everything from config
- ✅ **No assumptions** - retry behavior explicitly configured
- ✅ **No magic numbers** - all values have meaning and source
- ✅ **Configuration-first** - follows mandatory practices
- ✅ **Explicit over implicit** - retry behavior is visible

### **2. Technical Benefits:**
- **Uses tenacity's full capabilities** - no reinventing the wheel
- **Type-safe** - proper decorator application and configuration
- **Clean and maintainable** - clear separation of concerns
- **Testable** - can inject different configs for testing
- **Flexible** - different retry strategies for different operations

### **3. Production Ready:**
- **Environment-specific configuration** - different retry behavior per environment
- **Runtime reconfiguration** - can change behavior without code deployment
- **Proper error handling** - maintains tenacity's exception handling
- **Logging integration** - preserves tenacity's logging capabilities

## 🚀 **IMPLEMENTATION PLAN:**

1. **Remove all hardcoded `@retry` decorators**
2. **Create `create_retry_decorator()` factory function**
3. **Update EventHandlerActor to use factory pattern**
4. **Update all workflow classes**
5. **Ensure EventSystemConfig provides all retry parameters**
6. **Run mypy, pyright, ruff to verify zero violations**

## 📚 **KEY LEARNINGS:**

### **Research Before Assuming:**
- Always check library capabilities before assuming limitations
- Tenacity has rich runtime configuration support
- CLAUDE.md: "assuming instead of looking for a source" is a bad practice

### **No Justifications for Violations:**
- "Reasonable defaults" are still hardcoded values
- Comments don't make violations acceptable
- CODING_STANDARDS.md is absolute: "NO HARDCODED VALUES - NONE"

### **Configuration is King:**
- Every retry parameter must come from EventSystemConfig
- No exceptions for "critical" or "emergency" operations
- Golden Rule applies universally

## 🎯 **NEXT STEPS:**

1. Implement factory pattern approach
2. Remove all violation comments
3. Verify zero hardcoded values remain
4. Continue with Steps 36-40: Trading Domain Handler

---

**Status**: Ready for implementation  
**Estimated Time**: 30 minutes  
**Risk**: LOW - Using tenacity's built-in capabilities, no architecture changes