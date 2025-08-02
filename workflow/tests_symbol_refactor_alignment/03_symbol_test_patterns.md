# Symbol Test Patterns Guide

## Overview

This guide documents the standardized patterns for using symbols in the CyberDeltaEngine test suite after the comprehensive Symbol architecture migration.

## Import Patterns

### Standard Imports
```python
from tests.common_symbols import (
    # Hyperliquid symbols
    BTC_HL, ETH_HL, SOL_HL,
    # Backpack spot symbols
    BTC_USDC_BP, ETH_USDC_BP, SOL_USDC_BP,
    # Backpack perpetual symbols
    BTC_USDC_PERP_BP, ETH_USDC_PERP_BP, SOL_USDC_PERP_BP,
    # Additional symbols
    USDT_USDC_BP, MATIC_HL, ADA_BTC_BP,
    # Parametrized test lists
    COMMON_SPOT_SYMBOLS_BP, COMMON_PERP_SYMBOLS_BP, COMMON_SYMBOLS_HL,
    # Invalid symbols for error testing
    INVALID_SYMBOL_BP, INVALID_PERP_BP, INVALID_SPOT_BP, INVALID_SYMBOL_HL,
)
```

## Common Usage Patterns

### 1. Basic Symbol Usage
```python
# Direct symbol object usage
symbol = BTC_HL
await api.get_ticker(symbol)

# When string value needed (e.g., API calls expecting strings)
symbol_str = BTC_HL.value  # "BTC"
```

### 2. Parametrized Testing
```python
# Spot symbols
@pytest.mark.parametrize("symbol", COMMON_SPOT_SYMBOLS_BP)
async def test_spot_ticker(symbol: Symbol):
    ticker = await api.get_ticker(symbol)
    assert ticker.symbol == symbol.value

# Perpetual symbols (when string values needed)
@pytest.mark.parametrize("symbol", [s.value for s in COMMON_PERP_SYMBOLS_BP])
async def test_perp_order_book(symbol: str):
    order_book = await api.get_order_book(symbol)
    assert order_book.symbol == symbol
```

### 3. Error Testing
```python
async def test_invalid_symbol_error():
    with pytest.raises(APIError) as exc_info:
        await api.get_ticker(INVALID_PERP_BP)
    
    error = exc_info.value
    assert INVALID_PERP_BP.value in str(error) or "symbol" in str(error).lower()
```

### 4. Arbitrage Pairs
```python
from tests.common_symbols import ARBITRAGE_PAIRS, FUNDING_ARB_PAIRS

# Test arbitrage between exchanges
for hl_symbol, bp_symbol in ARBITRAGE_PAIRS:
    hl_price = await hl_api.get_ticker(hl_symbol)
    bp_price = await bp_api.get_ticker(bp_symbol)
    assert abs(hl_price.price - bp_price.price) < threshold
```

### 5. Symbol Mappings
```python
from tests.common_symbols import SYMBOL_MAPPINGS

# Map perpetual to spot symbols
perp_symbol = BTC_HL.value  # "BTC"
spot_symbol = SYMBOL_MAPPINGS[perp_symbol]  # "BTC_USDC"
```

## Test Infrastructure Patterns

### 1. Fixture Configuration
```python
# In fixtures/config_fixtures.py
from tests.common_symbols import BTC_HL, ETH_HL, BTC_USDC_BP, ETH_USDC_BP

@pytest.fixture
def exchange_config():
    return {
        "hyperliquid": {
            "symbols": {"BTC": BTC_HL.value, "ETH": ETH_HL.value},
        },
        "backpack": {
            "symbols": {"BTC": BTC_USDC_BP.value, "ETH": ETH_USDC_BP.value},
        },
    }
```

### 2. Mock Data
```python
# In fixtures/exchange_mocks.py
from tests.common_symbols import BTC_HL, ETH_HL

mock_positions = {
    BTC_HL.value: DerivativePosition(
        symbol=BTC_HL.value,
        size=Decimal("0.5"),
        # ...
    ),
}
```

### 3. Dynamic Symbol Extraction
```python
# Extract base asset from compound symbol
sol_asset = SOL_USDC_BP.value.split("_")[0]  # "SOL"
balance = balances.get(sol_asset)
```

## Advanced Patterns

### 1. Symbol Test Factory
```python
from tests.factories.symbol_test_factory import SymbolTestFactory

# Create test symbols with custom metadata
test_symbol = SymbolTestFactory.create_with_metadata(
    base_asset="TEST",
    quote_asset="USD",
    exchange_name="backpack",
    custom_data={"test": True}
)
```

### 2. Symbol Builders
```python
from tests.builders.symbol_builders import ArbitrageSymbolBuilder

# Build arbitrage symbol set
arb_symbols = (
    ArbitrageSymbolBuilder()
    .with_base_asset("BTC")
    .with_exchanges(["hyperliquid", "backpack"])
    .with_spread_threshold(Decimal("0.001"))
    .build()
)
```

### 3. Symbol Validation
```python
from tests.helpers.symbol_validators import SymbolTestValidator

# Validate symbol properties
validator = SymbolTestValidator()
assert validator.is_valid_format(symbol)
assert validator.has_required_metadata(symbol)
```

## Best Practices

### DO ✅
- Import symbols from `tests.common_symbols`
- Use Symbol objects directly when possible
- Use `.value` accessor only when strings are required
- Leverage parametrized test lists for comprehensive coverage
- Use invalid symbol constants for error testing

### DON'T ❌
- Hardcode symbol strings like `"BTC_USDC"`
- Create Symbol objects with `Symbol(value="...")`
- Use legacy factory patterns like `ExchangeSymbolFactory`
- Define symbol constants locally in test files
- Mix string and Symbol object usage unnecessarily

## Migration Checklist

When migrating tests to use the Symbol architecture:

1. **Replace hardcoded strings**
   ```python
   # Before
   symbol = "SOL_USDC"
   
   # After
   from tests.common_symbols import SOL_USDC_BP
   symbol = SOL_USDC_BP.value
   ```

2. **Update factory usage**
   ```python
   # Before
   from tests.factories.symbol_factories import ExchangeSymbolFactory
   symbol = ExchangeSymbolFactory.create_hyperliquid_btc_perp()
   
   # After
   from tests.common_symbols import BTC_HL
   symbol = BTC_HL
   ```

3. **Consolidate parametrized tests**
   ```python
   # Before
   @pytest.mark.parametrize("symbol", ["SOL_USDC", "BTC_USDC", "ETH_USDC"])
   
   # After
   @pytest.mark.parametrize("symbol", [s.value for s in COMMON_SPOT_SYMBOLS_BP])
   ```

4. **Standardize error testing**
   ```python
   # Before
   invalid_symbol = exchanges.backpack("INVALID_PERP")
   
   # After
   from tests.common_symbols import INVALID_PERP_BP
   invalid_symbol = INVALID_PERP_BP
   ```

## Summary

The Symbol test architecture provides:
- **Type safety**: Symbol objects prevent string typos
- **Centralization**: All symbols defined in one place
- **Consistency**: Standardized patterns across all tests
- **Efficiency**: Parametrized test lists for comprehensive coverage
- **Maintainability**: Easy to add/modify symbols globally

Following these patterns ensures consistent, maintainable, and type-safe symbol usage throughout the test suite.