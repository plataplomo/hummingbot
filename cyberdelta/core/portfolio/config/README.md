# Portfolio Configuration System

## Overview

The Portfolio Configuration System provides a comprehensive, type-safe configuration management solution for the CyberDeltaEngine portfolio module. All configurations use Pydantic dataclasses for automatic validation and serialization.

## Key Features

- **Type-Safe Configuration**: All configuration classes use Pydantic dataclasses with comprehensive field validation
- **Financial Safety**: Prevents invalid financial configurations (negative prices, invalid leverage, etc.)
- **Startup Validation**: Critical errors fail fast at startup to prevent runtime issues
- **Configuration Presets**: Development, production, test, and high-performance presets available
- **Environment Variable Support**: Load configuration from environment variables
- **Comprehensive Validation**: Both field-level and cross-field business logic validation

## Configuration Components

### Core Configuration Classes

1. **PortfolioConfiguration** - Main configuration container
2. **CacheConfiguration** - Cache settings (size, TTL, cleanup)
3. **PricingConfiguration** - Price service settings
4. **SymbolConfiguration** - Symbol normalization and validation
5. **ScreeningConfiguration** - Data validation settings
6. **BalanceConfiguration** - Balance precision and cleanup
7. **PositionConfiguration** - Position management settings
8. **OrderConfiguration** - Order limits and cleanup
9. **PnLConfiguration** - P&L calculation methods
10. **ConcurrencyConfiguration** - Thread safety settings
11. **StateManagerConfiguration** - State persistence settings
12. **MonitoringConfiguration** - Metrics and health checks
13. **CalculationConfiguration** - Calculator default values

### Validation Rules

#### Financial Constraints
- Prices must be positive (gt=0)
- Balances must be non-negative (ge=0)
- Leverage limits enforced (max 100x by default)
- Precision must be 0-18 for Decimal compatibility

#### Time Intervals
- All time values must be positive
- Maximum TTL: 24 hours (86400 seconds)
- Cleanup intervals must be reasonable ratios

#### Resource Limits
- Cache size: 1-1,000,000 entries
- Concurrent operations: 1-100
- Order limits per exchange enforced

## Usage Examples

### Basic Usage

```python
from cyberdelta.core.portfolio.config import (
    PortfolioConfigFactory,
    create_dev_config,
    create_prod_config,
)

# Use preset configurations
dev_config = create_dev_config()
prod_config = create_prod_config()

# Create custom configuration
custom_config = PortfolioConfigFactory.create_custom(
    cache=CacheConfiguration(max_size=50000, default_ttl=1800.0),
    pricing=PricingConfiguration(default_cache_ttl=30.0),
    debug_mode=False,
)
```

### Environment Variable Configuration

```python
# Set environment variables
# PORTFOLIO_CACHE_SIZE=50000
# PORTFOLIO_CACHE_TTL=1800
# PORTFOLIO_DEBUG_MODE=false

config = PortfolioConfigFactory.create_from_environment()
```

### Validation Example

```python
from cyberdelta.core.portfolio.config import (
    PortfolioConfiguration,
    CacheConfiguration,
    validate_startup_configuration,
)

# Create configuration with validation
try:
    config = PortfolioConfiguration(
        cache=CacheConfiguration(max_size=-1)  # Invalid!
    )
except ValueError as e:
    print(f"Validation error: {e}")

# Validate existing configuration
config = create_prod_config()
validate_startup_configuration(config)  # Raises if critical errors
```

### Getting Validation Report

```python
config = create_dev_config()
report = PortfolioConfigFactory.get_validation_report(config)

print(f"Valid: {report['is_valid']}")
print(f"Errors: {report['error_count']}")
print(f"Critical: {report['critical_errors']}")
print(f"Warnings: {report['warnings']}")
```

## Migration Guide

### From Dictionary Configuration

Before (dictionary-based):
```python
config = {
    "cache": {
        "max_size": 10000,
        "default_ttl": 3600
    },
    "pricing": {
        "default_cache_ttl": 60
    }
}
```

After (Type-safe Pydantic):
```python
config = PortfolioConfigFactory.create_custom(
    cache=CacheConfiguration(max_size=10000, default_ttl=3600.0),
    pricing=PricingConfiguration(default_cache_ttl=60.0)
)
```

### From Manual Validation

Before:
```python
if config.cache_size <= 0:
    raise ValueError("Cache size must be positive")
if config.cache_size > 1000000:
    raise ValueError("Cache size too large")
```

After (automatic validation):
```python
# Validation happens automatically
config = CacheConfiguration(max_size=10000)  # Validated!
```

## Type Safety

The configuration system uses Pydantic dataclasses exclusively, providing:
- **Compile-time type safety**: No dictionaries, full IDE support
- **Automatic validation**: Validation happens at object creation
- **Type conversion**: Automatic string to int/float conversion in validators
- **Descriptive error messages**: Clear validation errors from Pydantic
- **Immutable patterns**: Configuration objects are validated once

No dictionaries are used in the public API - everything is type-safe Pydantic dataclasses:

```python
# All type-safe, no dictionaries
cache_config = CacheConfiguration(max_size=10000, default_ttl=3600.0)
config = PortfolioConfiguration(cache=cache_config, ...)

# Validation happens automatically at creation
validated_config = PortfolioConfigFactory.validate_and_create(config)
```

## Best Practices

1. **Use Factory Methods**: Always use factory methods for creating configurations
2. **Validate at Startup**: Call `validate_startup_configuration()` before starting services
3. **Use Presets**: Start with presets and customize as needed
4. **Use Dataclasses**: Create configuration components as Pydantic dataclasses
5. **Handle Validation Errors**: Catch and log validation errors appropriately

## Configuration Presets

### Development
- Debug mode enabled
- Verbose logging
- Smaller caches
- Shorter intervals for testing

### Production
- Optimized for performance
- Larger caches
- Longer TTLs
- Strict validation

### Test
- Minimal resources
- Fast cleanup
- Disabled monitoring
- Predictable behavior

### High Performance
- Maximum cache sizes
- Minimal logging
- Aggressive caching
- High concurrency limits

## Error Handling

Configuration validation can raise:
- `ValueError`: For invalid field values
- `TypeError`: For type mismatches
- Custom validation errors with descriptive messages

Always handle these errors at startup to prevent runtime issues.

## Documentation Reference

For complete validation details, see:
- **[VALIDATION_REFERENCE.md](./VALIDATION_REFERENCE.md)** - Complete field-by-field validation rules and error messages
- **[MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)** - Migration from stdlib dataclasses to Pydantic

### Quick Validation Examples

```python
from pydantic import ValidationError
from cyberdelta.core.portfolio.config import CacheConfiguration, ScreeningConfiguration

# Field validation with specific error messages
try:
    cache = CacheConfiguration(max_size=0)
except ValidationError as e:
    print(e)  # "ensure this value is greater than 0"

try:
    cache = CacheConfiguration(max_size=2000000)
except ValidationError as e:
    print(e)  # "ensure this value is less than or equal to 1000000"

# Cross-field validation
try:
    screening = ScreeningConfiguration(
        min_price_value="100.0",
        max_price_value="50.0"  # Invalid: min > max
    )
except ValidationError as e:
    print(e)  # "Minimum price must be less than maximum price"

# String coercion with validation
config = CacheConfiguration(max_size="50000")  # String converted to int
assert config.max_size == 50000
```

## Future Enhancements

- Configuration hot-reloading
- Configuration versioning
- Remote configuration support
- Configuration change tracking
- Performance profiling integration