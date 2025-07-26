# Symbol System - Transformation Method Return Types

**Document**: 14_return_types_detail.md  
**Date**: 2025-01-24  
**Purpose**: Clarify what objects the transformation methods return after refactoring

---

## Before Refactoring (Old API)

The transformation methods used to return wrapper objects:

```python
# OLD - Methods returned TransformationResult wrapper
result = transformer.transform_exchange_to_internal("BTC-PERP", ExchangeName.HYPERLIQUID)
# result was a TransformationResult object with:
# - result.success: bool
# - result.data: InternalSymbol (if success=True)
# - result.error: str (if success=False)
```

---

## After Refactoring (New API)

The transformation methods now return the actual symbol objects directly:

### 1. `transform_internal_to_exchange()`

**Returns**: `ExchangeSymbol` object directly

```python
# NEW - Returns ExchangeSymbol directly
exchange_symbol = transformer.transform_internal_to_exchange(
    internal_symbol,  # InternalSymbol object
    ExchangeName.HYPERLIQUID
)
# exchange_symbol is an ExchangeSymbol instance
# Raises SymbolValidationError on failure
```

**ExchangeSymbol attributes**:
- `value: str` - The exchange-specific symbol (e.g., "BTC-PERP")
- `exchange_id: ExchangeName` - The exchange this symbol is for
- `asset_index: int | None` - Hyperliquid asset index if applicable
- `symbol_id: str | None` - Backpack symbol ID if applicable
- Other inherited from BaseSymbol

### 2. `transform_exchange_to_internal()`

**Returns**: `InternalSymbol` object directly

```python
# NEW - Returns InternalSymbol directly
internal_symbol = transformer.transform_exchange_to_internal(
    "BTC-PERP",  # Exchange symbol as string
    ExchangeName.HYPERLIQUID,
    asset_index=None,  # Optional
    symbol_id=None     # Optional
)
# internal_symbol is an InternalSymbol instance
# Raises SymbolValidationError on failure
```

**InternalSymbol attributes**:
- `value: str` - The internal symbol (e.g., "BTC_USD")
- `base_asset: str` - Base asset (e.g., "BTC")
- `quote_asset: str | None` - Quote asset (e.g., "USD")
- `market_type: MarketType` - PERP or SPOT
- `is_pair: bool` - Computed property
- `canonical_name: str` - Computed property
- Other inherited from BaseSymbol

### 3. `batch_transform_exchange_to_internal()`

**Returns**: Plain `dict` with specific structure

```python
# NEW - Returns dict with "successful" and "failed" keys
result = transformer.batch_transform_exchange_to_internal(
    ["BTC-PERP", "ETH-PERP", "INVALID"],
    ExchangeName.HYPERLIQUID
)

# result structure:
{
    "successful": [
        ("BTC-PERP", InternalSymbol(...)),  # Tuple of (input_str, InternalSymbol)
        ("ETH-PERP", InternalSymbol(...))
    ],
    "failed": [
        ("INVALID", "Symbol validation failed for 'INVALID': ...")  # Tuple of (input_str, error_str)
    ]
}
```

### 4. `validate_arbitrage_compatibility()`

**Returns**: Plain `dict` with compatibility information

```python
# Returns dict with arbitrage compatibility info
result = transformer.validate_arbitrage_compatibility(
    internal_symbol,  # InternalSymbol
    [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]
)

# result structure:
{
    "compatible": True,  # or False
    "exchanges": {
        "hyperliquid": {
            "available": True,
            "symbol": "BTC-PERP"
        },
        "backpack": {
            "available": True,
            "symbol": "BTC_PERP"
        }
    }
}
```

---

## Error Handling

Instead of returning error information in a wrapper, methods now raise exceptions:

```python
# OLD - Check result.success
result = transformer.transform_exchange_to_internal("INVALID", exchange)
if not result.success:
    print(f"Error: {result.error}")

# NEW - Use try/except
try:
    internal = transformer.transform_exchange_to_internal("INVALID", exchange)
except SymbolValidationError as e:
    print(f"Error: {e}")
```

---

## Summary

The key change is that transformation methods now:
1. Return the actual symbol objects (`InternalSymbol`, `ExchangeSymbol`) directly
2. Raise exceptions on failure instead of returning error in a wrapper
3. For batch operations, return simple dicts instead of specialized result classes

This makes the API simpler and more Pythonic, following the principle of "return values or raise exceptions, not both".