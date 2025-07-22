# Portfolio Configuration Validation Reference

## Overview

This document provides a comprehensive reference of all validation rules, constraints, and error messages for portfolio configuration fields. Every constraint is enforced automatically by Pydantic at object creation time.

## CacheConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `max_size` | `int` | `gt=0`, `le=1000000` | `10000` | "ensure this value is greater than 0"<br>"ensure this value is less than or equal to 1000000" |
| `default_ttl` | `float` | `gt=0`, `le=86400` | `3600.0` | "ensure this value is greater than 0"<br>"TTL cannot exceed 24 hours (86400 seconds)" |
| `cleanup_interval` | `float` | `gt=0`, `le=3600` | `300.0` | "ensure this value is greater than 0"<br>"Cleanup interval too long" |
| `enabled` | `bool` | None | `True` | "value must be a valid boolean" |

### Validation Examples

```python
# ✅ Valid configurations
cache = CacheConfiguration(max_size=50000, default_ttl=1800.0)
cache = CacheConfiguration()  # Uses defaults

# ❌ Invalid configurations
cache = CacheConfiguration(max_size=0)
# ValidationError: ensure this value is greater than 0

cache = CacheConfiguration(max_size=2000000)
# ValidationError: ensure this value is less than or equal to 1000000

cache = CacheConfiguration(default_ttl=-100)
# ValidationError: ensure this value is greater than 0

cache = CacheConfiguration(default_ttl=100000)
# ValidationError: TTL cannot exceed 24 hours (86400 seconds)
```

## PricingConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `default_cache_ttl` | `float` | `gt=0`, `le=3600` | `60.0` | "ensure this value is greater than 0"<br>"Price cache TTL too long" |
| `batch_size_limit` | `int` | `gt=0`, `le=1000` | `100` | "ensure this value is greater than 0"<br>"Batch size too large" |
| `price_staleness_threshold` | `float` | `gt=0`, `le=3600` | `300.0` | "ensure this value is greater than 0"<br>"Staleness threshold too long" |
| `fallback_enabled` | `bool` | None | `True` | "value must be a valid boolean" |

### Validation Examples

```python
# ✅ Valid configurations
pricing = PricingConfiguration(default_cache_ttl=30.0, batch_size_limit=50)

# ❌ Invalid configurations
pricing = PricingConfiguration(default_cache_ttl=0)
# ValidationError: ensure this value is greater than 0

pricing = PricingConfiguration(batch_size_limit=2000)
# ValidationError: Batch size too large
```

## ScreeningConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `min_price_value` | `str` | Custom validator | `"0.000001"` | "Invalid decimal format"<br>"Price must be positive" |
| `max_price_value` | `str` | Custom validator | `"1000000"` | "Invalid decimal format"<br>"Price too high" |
| `min_quantity_value` | `str` | Custom validator | `"0.000001"` | "Invalid decimal format"<br>"Quantity must be positive" |
| `max_quantity_value` | `str` | Custom validator | `"1000000"` | "Invalid decimal format"<br>"Quantity too high" |
| `strict_mode` | `bool` | None | `True` | "value must be a valid boolean" |

### Cross-Field Validation

- `min_price_value` < `max_price_value`
- `min_quantity_value` < `max_quantity_value`

### Validation Examples

```python
# ✅ Valid configurations
screening = ScreeningConfiguration(
    min_price_value="0.01",
    max_price_value="10000",
    strict_mode=True
)

# ❌ Invalid configurations
screening = ScreeningConfiguration(min_price_value="invalid")
# ValidationError: Invalid decimal format

screening = ScreeningConfiguration(min_price_value="100", max_price_value="50")
# ValidationError: Minimum price must be less than maximum price
```

## BalanceConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `precision` | `int` | `ge=0`, `le=18` | `8` | "ensure this value is greater than or equal to 0"<br>"ensure this value is less than or equal to 18" |
| `auto_cleanup_enabled` | `bool` | None | `True` | "value must be a valid boolean" |
| `cleanup_interval` | `int` | `gt=0`, `le=86400` | `3600` | "ensure this value is greater than 0"<br>"Cleanup interval too long" |

### Validation Examples

```python
# ✅ Valid configurations
balance = BalanceConfiguration(precision=6, cleanup_interval=1800)

# ❌ Invalid configurations
balance = BalanceConfiguration(precision=-1)
# ValidationError: ensure this value is greater than or equal to 0

balance = BalanceConfiguration(precision=25)
# ValidationError: ensure this value is less than or equal to 18
```

## PositionConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `precision` | `int` | `ge=0`, `le=18` | `8` | "ensure this value is greater than or equal to 0"<br>"ensure this value is less than or equal to 18" |
| `auto_cleanup_enabled` | `bool` | None | `True` | "value must be a valid boolean" |
| `cleanup_interval` | `int` | `gt=0`, `le=86400` | `3600` | "ensure this value is greater than 0"<br>"Cleanup interval too long" |

### Validation Examples

```python
# ✅ Valid configurations
position = PositionConfiguration(precision=8, auto_cleanup_enabled=False)

# ❌ Invalid configurations
position = PositionConfiguration(cleanup_interval=0)
# ValidationError: ensure this value is greater than 0
```

## OrderConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `max_orders_per_exchange` | `int` | `gt=0`, `le=10000` | `1000` | "ensure this value is greater than 0"<br>"Too many orders per exchange" |
| `cleanup_completed_orders` | `bool` | None | `True` | "value must be a valid boolean" |
| `cleanup_interval` | `int` | `gt=0`, `le=86400` | `3600` | "ensure this value is greater than 0"<br>"Cleanup interval too long" |

### Validation Examples

```python
# ✅ Valid configurations
order = OrderConfiguration(max_orders_per_exchange=500)

# ❌ Invalid configurations
order = OrderConfiguration(max_orders_per_exchange=15000)
# ValidationError: Too many orders per exchange
```

## PnLConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `calculation_method` | `str` | Custom validator | `"FIFO"` | "Invalid calculation method. Must be FIFO, LIFO, or WEIGHTED_AVERAGE" |
| `precision` | `int` | `ge=0`, `le=18` | `8` | "ensure this value is greater than or equal to 0"<br>"ensure this value is less than or equal to 18" |
| `cache_results` | `bool` | None | `False` | "value must be a valid boolean" |
| `real_time_updates` | `bool` | None | `True` | "value must be a valid boolean" |

### Validation Examples

```python
# ✅ Valid configurations
pnl = PnLConfiguration(calculation_method="LIFO", precision=6)

# ❌ Invalid configurations
pnl = PnLConfiguration(calculation_method="INVALID")
# ValidationError: Invalid calculation method. Must be FIFO, LIFO, or WEIGHTED_AVERAGE
```

## ConcurrencyConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `max_concurrent_operations` | `int` | `gt=0`, `le=100` | `10` | "ensure this value is greater than 0"<br>"Too many concurrent operations" |
| `lock_timeout` | `float` | `gt=0`, `le=300` | `30.0` | "ensure this value is greater than 0"<br>"Lock timeout too long" |
| `deadlock_detection` | `bool` | None | `True` | "value must be a valid boolean" |

### Validation Examples

```python
# ✅ Valid configurations
concurrency = ConcurrencyConfiguration(max_concurrent_operations=20, lock_timeout=60.0)

# ❌ Invalid configurations
concurrency = ConcurrencyConfiguration(max_concurrent_operations=200)
# ValidationError: Too many concurrent operations

concurrency = ConcurrencyConfiguration(lock_timeout=500.0)
# ValidationError: Lock timeout too long
```

## StateManagerConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `cleanup_interval` | `int` | `gt=0`, `le=86400` | `3600` | "ensure this value is greater than 0"<br>"Cleanup interval too long" |
| `strict_validation` | `bool` | None | `True` | "value must be a valid boolean" |
| `enable_snapshots` | `bool` | None | `False` | "value must be a valid boolean" |
| `snapshot_interval` | `int` | `gt=0`, `le=86400` | `3600` | "ensure this value is greater than 0"<br>"Snapshot interval too long" |

### Validation Examples

```python
# ✅ Valid configurations
state_mgr = StateManagerConfiguration(cleanup_interval=1800, enable_snapshots=True)

# ❌ Invalid configurations
state_mgr = StateManagerConfiguration(snapshot_interval=100000)
# ValidationError: Snapshot interval too long
```

## MonitoringConfiguration

### Field Constraints

| Field | Type | Constraints | Default | Error Messages |
|-------|------|-------------|---------|----------------|
| `enabled` | `bool` | None | `True` | "value must be a valid boolean" |
| `metrics_interval` | `int` | `gt=0`, `le=3600` | `60` | "ensure this value is greater than 0"<br>"Metrics interval too long" |
| `health_check_interval` | `int` | `gt=0`, `le=300` | `30` | "ensure this value is greater than 0"<br>"Health check interval too long" |
| `performance_tracking` | `bool` | None | `False` | "value must be a valid boolean" |

### Cross-Field Validation

- `health_check_interval` ≤ `metrics_interval`

### Validation Examples

```python
# ✅ Valid configurations
monitoring = MonitoringConfiguration(metrics_interval=120, health_check_interval=30)

# ❌ Invalid configurations
monitoring = MonitoringConfiguration(metrics_interval=30, health_check_interval=60)
# ValidationError: Health check interval should not exceed metrics interval
```

## Error Message Patterns

### Common Error Types

1. **Range Validation**: `"ensure this value is greater than X"`
2. **Type Validation**: `"value must be a valid boolean/integer/float"`
3. **Custom Business Logic**: Descriptive messages explaining constraint violations
4. **Cross-Field Validation**: Messages comparing field relationships

### Catching Validation Errors

```python
from pydantic import ValidationError

try:
    config = CacheConfiguration(max_size=-1)
except ValidationError as e:
    print(f"Validation failed: {e}")
    # Prints detailed error information including:
    # - Field name that failed
    # - Value that was rejected
    # - Constraint that was violated
    # - Location in object hierarchy
```

### Startup Validation

```python
from cyberdelta.core.portfolio.config import validate_startup_configuration

try:
    config = create_dev_config()
    validate_startup_configuration(config)
except ValueError as e:
    print(f"Critical configuration error: {e}")
    # System should not start with invalid configuration
```

## Best Practices

1. **Validate Early**: Always validate configuration at application startup
2. **Handle Errors Gracefully**: Catch validation errors and provide helpful feedback
3. **Use Defaults**: Rely on well-tested default values when possible
4. **Test Edge Cases**: Test both valid and invalid configuration values
5. **Monitor Validation**: Log validation errors for debugging and monitoring

## Configuration Testing

```python
import pytest
from pydantic import ValidationError

def test_cache_configuration_validation():
    # Test valid configurations
    config = CacheConfiguration(max_size=50000)
    assert config.max_size == 50000
    
    # Test invalid configurations
    with pytest.raises(ValidationError) as exc_info:
        CacheConfiguration(max_size=0)
    assert "greater than 0" in str(exc_info.value)
    
    with pytest.raises(ValidationError) as exc_info:
        CacheConfiguration(max_size=2000000)
    assert "less than or equal to 1000000" in str(exc_info.value)
```

This validation reference ensures all configuration constraints are documented, testable, and provide clear error messages when violated.