# Symbol Type Errors Analysis

**Date:** 2025-07-30  
**Total mypy errors:** 998  
**Status:** Portfolio symbol services deleted (-48 errors)  

## Summary of Symbol-Related Issues

### 1. Core Services Using String Symbols
- **DataHandler**: 10 errors - stores symbols as strings in dicts
- **DataframeProcessor**: Creates Candle with string symbol
- **SignalQueue**: Creates TradeSignal with string symbol

### 2. Portfolio Services Type Mismatches
- **TradeValidationService**: Trying to call len() on Symbol object
- **TradeReconciliationService**: TradeDiscrepancy expects string but gets Symbol
- **OrderReconciliationService**: OrderDiscrepancy expects string but gets Symbol

### 3. Common Error Patterns

#### Pattern 1: Storage with String Keys
```python
# Current
dict[str, Ticker]  # symbol is string key

# Should be
dict[Symbol, Ticker]  # symbol is Symbol object key
```

#### Pattern 2: Model Creation with String
```python
# Current
Candle(symbol="BTC-PERP", ...)  # Error: expects Symbol

# Should be
Candle(symbol=create_symbol("BTC-PERP", exchange), ...)
```

#### Pattern 3: Discrepancy Models Expect Strings
```python
# Current
TradeDiscrepancy(symbol=trade.symbol, ...)  # Error: trade.symbol is Symbol

# Should be
TradeDiscrepancy(symbol=trade.symbol, ...)  # After updating TradeDiscrepancy to use Symbol
```

## Action Items by Priority

### HIGH Priority (Core Infrastructure)
1. **DataHandler** - Full refactor to use Symbol keys
2. **SignalQueue** - Update to create Symbols at entry
3. **DataframeProcessor** - Convert string symbols to Symbol objects

### MEDIUM Priority (Portfolio Services)  
1. **Reconciliation Services** - Update discrepancy models to use Symbol
2. **Validation Services** - Fix Symbol type handling

### LOW Priority (Cleanup)
1. Remove any remaining string symbol comparisons
2. Update tests to use Symbol objects

## Key Files to Update

1. `/cyberdelta/core/data_handler.py` - 10 errors
2. `/cyberdelta/core/dataframe_processor.py` - 1 error  
3. `/cyberdelta/core/signal_queue.py` - 1 error
4. `/cyberdelta/core/portfolio/services/reconciliation/trade_reconciliation_service.py` - 12 errors
5. `/cyberdelta/core/portfolio/services/reconciliation/order_reconciliation_service.py` - 7 errors
6. `/cyberdelta/core/portfolio/services/validation/trade_validation_service.py` - 1 error

## Clean Break Principles

1. **NO Union types** - Never use `Union[str, Symbol]`
2. **NO backwards compatibility** - Remove all string symbol support
3. **Symbol at boundaries** - Create Symbol objects only at system entry points
4. **Consistent types** - All internal APIs use Symbol, not string

## Next Steps

1. Start with high-impact files (DataHandler)
2. Update one service at a time with full Symbol integration
3. Run mypy after each change to track progress
4. Aim for 0 Symbol-related type errors