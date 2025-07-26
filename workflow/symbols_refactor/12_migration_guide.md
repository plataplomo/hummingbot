# Symbol System Refactoring - Migration Guide

**Document**: 12_migration_guide.md  
**Date**: 2025-01-24  
**For**: Developers using the CyberDeltaEngine symbol system

---

## Overview

This guide helps you migrate from the old symbol system to the simplified version. Most code will work without changes, but some imports and usage patterns need updates.

---

## Quick Migration Checklist

- [ ] Update imports for removed classes
- [ ] Replace transformation result unwrapping
- [ ] Update exception handling for removed types
- [ ] Remove any metrics/statistics collection code
- [ ] Test your symbol operations

---

## Import Changes

### Transformers

```python
# ❌ OLD - These classes no longer exist
from cyberdelta.core.symbols import (
    SymbolTransformerProtocol,
    TransformationResult,
    BatchTransformationResult,
    SymbolTransformer,
    BackpackSymbolTransformer,
    HyperliquidSymbolTransformer,
)

# ✅ NEW - Use these instead
from cyberdelta.core.symbols import (
    UnifiedSymbolTransformer,
    BackpackTransformer,
    HyperliquidTransformer,
    get_unified_transformer,
)
```

### Cache

```python
# ❌ OLD - These are removed
from cyberdelta.core.symbols import SymbolCache, MultiLevelCache

# ✅ NEW - Cache functions are available but simplified
from cyberdelta.core.symbols import (
    cache_internal_to_exchange,
    cache_exchange_to_internal,
    clear_all_caches,
)
```

### Exceptions

```python
# ❌ OLD - These exceptions are removed
from cyberdelta.core.symbols import (
    SymbolMigrationError,
    SymbolFormatError,
    SymbolTransformationError,
)

# ✅ NEW - Use these general exceptions
from cyberdelta.core.symbols import (
    SymbolError,           # Base exception
    SymbolValidationError, # For validation errors
    SymbolNotFoundError,   # For missing symbols
)
```

---

## Code Pattern Changes

### 1. Transformation Results

The transformation methods now return objects directly instead of wrapped results.

```python
# ❌ OLD - Result wrapper pattern
transformer = UnifiedSymbolTransformer()
result = transformer.transform_exchange_to_internal("BTC-PERP", ExchangeName.HYPERLIQUID)
if result.success:
    internal_symbol = result.data
    print(f"Transformed: {internal_symbol.value}")
else:
    print(f"Error: {result.error}")

# ✅ NEW - Direct returns with exceptions
transformer = get_unified_transformer()
try:
    internal_symbol = transformer.transform_exchange_to_internal("BTC-PERP", ExchangeName.HYPERLIQUID)
    print(f"Transformed: {internal_symbol.value}")
except SymbolError as e:
    print(f"Error: {e}")
```

### 2. Batch Transformations

Batch operations now return a simple dictionary.

```python
# ❌ OLD - BatchTransformationResult class
batch_result = transformer.batch_transform(symbols, exchange)
for success in batch_result.successful:
    print(f"Success: {success.input} -> {success.output}")
for failure in batch_result.failed:
    print(f"Failed: {failure.input} - {failure.error}")

# ✅ NEW - Simple dictionary
result = transformer.batch_transform_exchange_to_internal(symbols, exchange)
for symbol, internal in result["successful"]:
    print(f"Success: {symbol} -> {internal.value}")
for symbol, error in result["failed"]:
    print(f"Failed: {symbol} - {error}")
```

### 3. Validation

Validation methods are simplified but work the same way.

```python
# ❌ OLD - Complex validation with suggestions
validator = SymbolValidator()
result = validator.validate_with_suggestions(symbol, symbol_type)
if not result.valid:
    print(f"Invalid: {result.error}")
    print(f"Suggestions: {result.suggestions}")

# ✅ NEW - Direct validation
validator = SymbolValidator()
try:
    validated = validator.validate_symbol(symbol, symbol_type)
    print(f"Valid: {validated}")
except SymbolValidationError as e:
    print(f"Invalid: {e}")
    # Note: suggestions feature was removed as it was unused
```

### 4. Registry Usage

The registry API remains the same - no changes needed!

```python
# ✅ These all work exactly the same
registry = get_symbol_registry()
internal = registry.get_internal_symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
exchange = registry.get_exchange_symbol("BTC_USD", ExchangeName.BACKPACK)
unified = registry.get_unified_symbol("BTC_USD")
```

### 5. Caching

The caching is now automatic - you don't need to manage it.

```python
# ❌ OLD - Manual cache management
cache = get_global_cache()
cache.put("key", value)
cached = cache.get("key")
cache.invalidate_pattern("BTC*")

# ✅ NEW - Automatic caching
# Just use the registry normally - caching happens automatically
registry = get_symbol_registry()
symbol = registry.get_internal_symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
# Subsequent calls are automatically cached
```

---

## Removed Features

These features were removed as they were unused:

1. **Symbol correction suggestions** - The `suggest_corrections()` method
2. **Coverage reports** - The `get_symbol_coverage_report()` method
3. **Metrics collection** - The `SymbolMetrics` class
4. **Migration support** - The `SymbolMigrationError` exception
5. **Complex error codes** - The `SymbolErrorCodes` enum

If you were using any of these, you'll need to remove that code.

---

## Testing Your Migration

After updating your code:

1. **Run your tests**
   ```bash
   pytest tests/
   ```

2. **Check symbol operations**
   ```python
   # Test basic transformation
   transformer = get_unified_transformer()
   internal = transformer.transform_exchange_to_internal("BTC-PERP", ExchangeName.HYPERLIQUID)
   assert internal.value == "BTC_USD"
   ```

3. **Verify performance**
   ```python
   import time
   start = time.perf_counter()
   for _ in range(1000):
       registry.get_internal_symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
   end = time.perf_counter()
   print(f"Average: {(end-start)/1000*1000:.2f}ms per lookup")
   # Should be < 1ms
   ```

---

## Common Issues and Solutions

### Issue 1: ImportError for removed classes

**Error**: `ImportError: cannot import name 'SymbolTransformerProtocol'`

**Solution**: Update imports as shown above.

### Issue 2: AttributeError on result.success

**Error**: `AttributeError: 'InternalSymbol' object has no attribute 'success'`

**Solution**: Transformations now return objects directly, not wrapped results.

### Issue 3: Missing suggest_corrections

**Error**: `AttributeError: 'SymbolValidator' object has no attribute 'suggest_corrections'`

**Solution**: This feature was removed. Remove the code using it.

---

## Need Help?

If you encounter issues not covered here:

1. Check the updated symbol system tests for examples
2. Review the docstrings in the simplified modules
3. Contact the trading infrastructure team

---

## Summary

The migration is straightforward:
- Most code works without changes
- Only a few imports and patterns need updates
- The simplified system is faster and easier to use
- All business functionality is preserved

The refactoring makes the symbol system more maintainable while preserving all the features you rely on for trading.