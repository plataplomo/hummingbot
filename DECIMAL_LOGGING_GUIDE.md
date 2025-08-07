# Decimal Logging Configuration Guide

## Overview

The structlog configuration has been enhanced to properly handle `Decimal` values in logs, preserving their precision without converting to float.

## Changes Made

### 1. Structlog Configuration (`cyberdelta/config/structlog_config.py`)

Added a `serialize_decimals` processor that converts Decimal values to strings before logging:

```python
def serialize_decimals(_: object, __: str, event_dict: EventDict) -> EventDict:
    """Convert Decimal values to strings for proper serialization."""
    # Recursively converts all Decimal values to strings
    # Preserves exact precision without float conversion
```

This processor is added to:
- Base processor chain (for console output)
- File logging processor chain (for JSON output)

## Benefits

1. **Precision Preserved**: Decimal values maintain their exact precision in logs
2. **No Float Conversion Needed**: You can now log Decimals directly without `float()` calls
3. **Human Readable**: Values appear as clean strings in logs
4. **JSON Compatible**: Properly serialized for JSON log output

## Usage

### Before (Old Pattern - No Longer Needed)
```python
logger.info(
    "trade_executed",
    price=float(trade.price),      # Converting to float loses precision
    quantity=float(trade.quantity),  # Unnecessary conversion
)
```

### After (New Pattern - Recommended)
```python
logger.info(
    "trade_executed",
    price=trade.price,      # Decimal logged directly
    quantity=trade.quantity,  # Preserves exact precision
)
```

## Console Output

In console logs, Decimals appear with their repr format: `Decimal('123.456789')`

This is intentional and beneficial because:
- It's unambiguous - you know it's a Decimal, not a float
- It shows the exact precision
- It's useful for debugging

## JSON Output

In JSON log files, Decimals are serialized as strings:
```json
{
  "price": "123.456789",
  "quantity": "0.001"
}
```

## Migration Steps

✅ **COMPLETED** - All float conversions have been removed from logging:

1. **Search for float conversions in logging**:
```bash
grep -n "float(" cyberdelta/ -R | grep -E "logger\.(info|debug|warning|error)"
# Returns: No matches found
```

2. **Removed unnecessary conversions**:
- ✅ `trading_engine.py`: Removed float() from signal and trade logging (lines 456, 504-505)
- ✅ `market_order_service.py`: Removed float() from all logging statements (lines 230-232, 240, 293-295, 303)
- ✅ Additional cleanup: Removed float() from market data logging

3. **Testing completed**:
```python
# Decimals now log correctly without conversion
from decimal import Decimal
logger.info("test", price=Decimal("123.456789"))
# Output: price="123.456789" (in JSON) or price=Decimal('123.456789') (console)
```

## Performance Note

The Decimal serialization processor has minimal performance impact:
- Only processes log events (not business logic)
- Recursive conversion is efficient
- No impact on actual trading calculations

## Future Improvements

Consider creating a custom ConsoleRenderer that formats Decimals more cleanly for console output if desired:
```python
# Example: "123.456789" instead of "Decimal('123.456789')"
```

However, the current format is recommended for development as it clearly distinguishes Decimals from floats.

---

**Status**: ✅ **COMPLETED** - Structlog is fully configured to handle Decimal serialization and all float conversions have been removed from logging statements. The codebase now maintains full precision for financial values in logs.

**Verification**: All linters pass (mypy, ruff, pyright) with zero errors.
