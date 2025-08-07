# JSON Handling Best Practices for CyberDeltaEngine

## Overview

This guide establishes best practices for JSON serialization and deserialization within CyberDeltaEngine, a cryptocurrency trading system where performance and precision are critical.

## Core Principles

### 1. **Use Centralized Serialization Module**

✅ **DO:**
```python
from cyberdelta.utils.serialization import dumps_json, loads_json

# Type-safe serialization
json_str = dumps_json(portfolio_state)
data = loads_json(response_text)
```

❌ **DON'T:**
```python
import json
import orjson

# Direct imports bypass type safety and monitoring
json_str = json.dumps(data)  # Missing type safety
json_str = orjson.dumps(data).decode()  # Inconsistent usage
```

### 2. **Financial Data Precision**

✅ **DO:**
```python
from decimal import Decimal

# Preserve precision with Decimal
price = Decimal("123.456789")
trade_data = {"price": price, "quantity": Decimal("10.5")}
json_str = dumps_json(trade_data)  # orjson handles Decimal natively
```

❌ **DON'T:**
```python
# DANGEROUS - Precision loss in financial operations
price = float(price_decimal)  # Converts to float, loses precision
trade_data = {"price": price}  # Risk of rounding errors
```

### 3. **Pydantic Model Serialization**

✅ **DO:**
```python
# Type-safe with proper field handling
model_data = portfolio.model_dump(mode="json", by_alias=True, exclude_none=True)
json_str = dumps_json(model_data)

# Or use model's built-in method for simple cases
json_str = portfolio.model_dump_json()
```

❌ **DON'T:**
```python
# Type-unsafe with potential data corruption
json_str = json.dumps(portfolio.model_dump(), default=str)  # Converts everything to string
```

## Performance Guidelines

### 4. **Use Performance Monitoring**

✅ **DO:**
```python
from cyberdelta.utils.json_performance_monitor import monitored_dumps, monitored_loads

# Monitored serialization with context
json_str = monitored_dumps(
    large_dataset,
    operation_context="websocket_message"
)

# Monitored deserialization
data = monitored_loads(
    response_json,
    operation_context="api_response"
)
```

### 5. **Context-Aware Operations**

✅ **DO:**
```python
# Different contexts for monitoring and debugging
websocket_data = monitored_dumps(msg, operation_context="websocket")
state_data = monitored_dumps(portfolio, operation_context="state_persistence")
api_request = monitored_dumps(params, operation_context="http_request")
```

### 6. **Async File Operations**

✅ **DO:**
```python
import aiofiles
from pathlib import Path

async def save_portfolio_state(state: PortfolioState, file_path: Path) -> None:
    # Serialize to bytes for efficiency
    state_bytes = dumps_json(state.model_dump(mode="json")).encode("utf-8")

    # Atomic write with temp file
    temp_file = file_path.with_suffix(".tmp")
    async with aiofiles.open(temp_file, "wb") as f:
        await f.write(state_bytes)

    # Atomic replacement
    temp_file.replace(file_path)
```

❌ **DON'T:**
```python
# Blocking synchronous I/O
with open(file_path, "w") as f:
    json.dump(state.model_dump(), f)  # Blocks trading operations
```

## Security Guidelines

### 7. **Untrusted Data Handling**

✅ **DO:**
```python
from cyberdelta.apis.connectivity.json_security import secure_json_loads

# Security validation first for external data
try:
    validated_data = secure_json_loads(external_json)
    # Re-parse with orjson for performance
    processed_data = loads_json(dumps_json(validated_data))
except ValueError as e:
    logger.error("Invalid external JSON", error=str(e))
    raise
```

❌ **DON'T:**
```python
# Direct parsing of untrusted data
data = loads_json(external_json)  # No size/depth limits
```

### 8. **Input Validation**

✅ **DO:**
```python
def process_api_response(response_json: str) -> MarketData:
    # Parse JSON first
    raw_data = loads_json(response_json)

    # Validate with Pydantic
    try:
        market_data = MarketDataResponse.model_validate(raw_data)
        return market_data.data
    except ValidationError as e:
        logger.error("Invalid market data format", validation_errors=e.errors())
        raise ValueError("Invalid market data") from e
```

## WebSocket Optimization

### 9. **Message Processing**

✅ **DO:**
```python
async def process_websocket_message(message: str) -> None:
    # Fast parsing with monitoring
    data = monitored_loads(message, operation_context="websocket_inbound")

    # Type validation
    validated_msg = WebSocketMessage.model_validate(data)

    # Process typed message
    await handle_market_update(validated_msg)
```

### 10. **Message Sending**

✅ **DO:**
```python
async def send_websocket_message(websocket, payload: BaseModel) -> None:
    # Serialize with monitoring
    json_str = monitored_dumps(
        payload.model_dump(mode="json"),
        operation_context="websocket_outbound"
    )

    # Send as string (not JSON object)
    await websocket.send_text(json_str)
```

## Error Handling

### 11. **JSON Parsing Errors**

✅ **DO:**
```python
import orjson

try:
    data = loads_json(json_string)
except orjson.JSONDecodeError as e:
    logger.error(
        "json_parse_error",
        error_msg=str(e),
        json_snippet=json_string[:100] if len(json_string) > 100 else json_string,
        source_context="api_response"
    )
    raise ValueError("Invalid JSON format") from e
```

### 12. **Serialization Errors**

✅ **DO:**
```python
try:
    json_str = dumps_json(complex_object)
except (TypeError, ValueError) as e:
    logger.error(
        "json_serialize_error",
        error_msg=str(e),
        object_type=type(complex_object).__name__,
    )
    # Fallback or raise appropriate business exception
    raise DataSerializationError("Cannot serialize portfolio data") from e
```

## Testing Guidelines

### 13. **Performance Testing**

✅ **DO:**
```python
import pytest
from decimal import Decimal

def test_json_performance_large_portfolio():
    """Test JSON performance with realistic portfolio size."""
    portfolio = create_large_portfolio(positions=1000)

    start = time.perf_counter()
    json_str = dumps_json(portfolio.model_dump(mode="json"))
    duration = time.perf_counter() - start

    # Assert performance targets
    assert duration < 0.010  # <10ms for 1000 positions
    assert len(json_str) > 100_000  # Realistic size
```

### 14. **Precision Testing**

✅ **DO:**
```python
def test_decimal_precision_preservation():
    """Ensure Decimal precision is preserved through JSON round-trip."""
    original_price = Decimal("123.456789012345")

    # Serialize and deserialize
    json_str = dumps_json({"price": original_price})
    data = loads_json(json_str)
    recovered_price = Decimal(data["price"])

    # Verify exact precision
    assert recovered_price == original_price
```

## Monitoring and Alerting

### 15. **Performance Monitoring Setup**

✅ **DO:**
```python
from cyberdelta.utils.json_performance_monitor import initialize_global_monitor

# Initialize at application startup
monitor = initialize_global_monitor(
    slow_serialize_threshold_ms=Decimal("1.0"),    # 1ms threshold
    slow_deserialize_threshold_ms=Decimal("2.0"),  # 2ms threshold
    very_slow_threshold_ms=Decimal("10.0"),        # 10ms error threshold
    enable_metrics=True
)

# Periodic performance reporting
async def performance_reporter():
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        monitor.log_performance_summary()
```

### 16. **CI/CD Integration**

✅ **DO:**
```python
# In CI/CD pipeline tests
def test_json_performance_regression():
    """Ensure JSON performance doesn't regress."""
    large_data = generate_test_data(size="large")

    # Benchmark current performance
    times = []
    for _ in range(100):
        start = time.perf_counter()
        json_str = dumps_json(large_data)
        times.append(time.perf_counter() - start)

    avg_time = sum(times) / len(times)

    # Fail if average time exceeds baseline + 10%
    assert avg_time < PERFORMANCE_BASELINE * 1.1
```

## Anti-Patterns to Avoid

### 17. **Common Mistakes**

❌ **DON'T:**
```python
# 1. Using default=str (converts everything to string)
json.dumps(data, default=str)

# 2. Mixing JSON libraries
import json, orjson
result = json.loads(orjson.dumps(data))

# 3. Ignoring performance context
json_str = dumps_json(huge_data)  # No context for monitoring

# 4. Float conversion in financial data
price_log = float(decimal_price)  # Precision loss

# 5. Synchronous file I/O in async context
with open("state.json", "w") as f:
    json.dump(state, f)  # Blocks event loop
```

## Configuration

### 18. **Application Setup**

```python
# In main application initialization
from cyberdelta.utils.json_performance_monitor import initialize_global_monitor
from cyberdelta.config.models.app_config import AppSettings

def setup_json_monitoring(config: AppSettings) -> None:
    """Setup JSON performance monitoring from app config."""
    initialize_global_monitor(
        slow_serialize_threshold_ms=config.monitoring.json_slow_threshold_ms,
        slow_deserialize_threshold_ms=config.monitoring.json_slow_deserialize_ms,
        very_slow_threshold_ms=config.monitoring.json_very_slow_ms,
        enable_metrics=config.monitoring.json_metrics_enabled,
    )
```

## Summary

**Key Takeaways:**

1. **Always use centralized serialization module** for consistency and type safety
2. **Never convert Decimal to float** for financial data - precision is critical
3. **Use performance monitoring** to catch regressions early
4. **Handle untrusted data securely** with size and depth limits
5. **Prefer async file operations** to avoid blocking trading operations
6. **Test for both performance and precision** in CI/CD pipeline
7. **Monitor in production** with appropriate alerting thresholds

**Remember**: In cryptocurrency trading, performance and precision aren't just optimizations - they're requirements for competitive advantage and financial safety.

---

*Last Updated: December 2024*
*Version: 1.0*
