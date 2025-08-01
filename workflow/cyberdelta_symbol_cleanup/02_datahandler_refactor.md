# DataHandler Symbol Refactoring - Clean Break

**Date:** 2025-07-30  
**Status:** Analysis Complete  
**Current State:** DataHandler uses string symbols in all storage  
**Target State:** DataHandler uses Symbol objects throughout  

## Current Issues

1. **String-based storage**:
   ```python
   self.tickers: dict[str, dict[str, Ticker]] = {}
   # Should be: dict[str, dict[Symbol, Ticker]]
   ```

2. **Type mismatches**: Models expect Symbol but DataHandler provides strings
3. **10 mypy errors** in DataHandler related to Symbol types

## Clean Break Implementation

### Step 1: Update Storage Declarations

```python
from cyberdelta.core.symbols import Symbol

class DataHandler:
    def __init__(self, ...):
        # OLD: dict[str, dict[str, Ticker]]
        # NEW: dict[str, dict[Symbol, Ticker]]
        self.tickers: dict[str, dict[Symbol, Ticker]] = {}
        self.order_books: dict[str, dict[Symbol, OrderBook]] = {}
        self.funding_rates: dict[str, dict[Symbol, FundingRate]] = {}
        self.user_fills: dict[str, dict[Symbol, list[Trade]]] = {}
        self.open_orders: dict[str, dict[Symbol, list[Order]]] = {}
        self.last_update_time: dict[str, dict[Symbol, dt_real]] = {}
```

### Step 2: Update Initialization

```python
def _setup_data_structures(self):
    for exchange_id, exchange_config in exchanges_dict.items():
        if not exchange_config.enabled:
            continue
        
        # Create Symbol objects at initialization
        symbol_objects = {
            symbol_str: create_symbol(symbol_str, ExchangeName(exchange_id))
            for symbol_str in exchange_config.symbols.keys()
        }
        
        self.tickers[exchange_id] = {
            sym: self._get_default_ticker(sym, exchange_id) 
            for sym in symbol_objects.values()
        }
```

### Step 3: Update ALL Method Signatures

```python
# OLD
def _update_ticker(self, exchange_id: str, symbol: str, data: Ticker, timestamp: dt_real):
    
# NEW  
def _update_ticker(self, exchange_id: str, symbol: Symbol, data: Ticker, timestamp: dt_real):
```

### Step 4: Create Symbol at Entry Points

WebSocket and API entry points must create Symbol objects:

```python
async def handle_ticker_update(self, exchange_id: str, symbol_str: str, data: dict):
    # Create Symbol at entry point
    symbol = create_symbol(symbol_str, ExchangeName(exchange_id))
    
    # Pass Symbol through system
    ticker = Ticker(symbol=symbol, ...)
    self._update_ticker(exchange_id, symbol, ticker, ...)
```

### Step 5: Update Lookup Methods

```python
def get_ticker(self, exchange_id: str, symbol: Symbol) -> Ticker | None:
    """Get ticker by exchange and Symbol object."""
    return self.tickers.get(exchange_id, {}).get(symbol)

def get_ticker_by_string(self, exchange_id: str, symbol_str: str) -> Ticker | None:
    """Convenience method for string lookup during transition."""
    symbol = create_symbol(symbol_str, ExchangeName(exchange_id))
    return self.get_ticker(exchange_id, symbol)
```

## Migration Challenges

1. **WebSocket Integration**: Need to update all WS message handlers
2. **Strategy Integration**: Strategies may be passing string symbols
3. **Config Loading**: Config has string symbols that need conversion

## NO COMPROMISES

- NO `Union[str, Symbol]` in storage
- NO backward compatibility methods after migration
- NO string symbols in method signatures
- ALL storage uses Symbol as keys
- ALL methods accept Symbol parameters

## Success Criteria

1. Zero mypy errors in DataHandler
2. All storage uses Symbol keys
3. Symbol creation only at system boundaries
4. No string symbol comparisons