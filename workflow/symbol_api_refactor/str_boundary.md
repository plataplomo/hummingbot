# String Boundary Rule for Symbol API Refactor

## The Fundamental Rule

**EVERYTHING uses domain models (ExchangeSymbol) EXCEPT RAW models.**

RAW models are the ONLY boundary between exchange APIs and our business logic. They are the ONLY place where strings exist for symbols.

## What Uses Domain Models (ExchangeSymbol)

1. **API Wrappers** (e.g., `hl_api.py`, `bp_api.py`)
   - Accept ExchangeSymbol from callers
   - Pass ExchangeSymbol to services

2. **All Services** (composite and decomposed)
   - Accept ExchangeSymbol in method signatures
   - Work with ExchangeSymbol internally
   - Pass ExchangeSymbol to other components

3. **Request Builders**
   - Accept ExchangeSymbol in method signatures
   - Convert to string ONLY when creating RAW request models
   ```python
   def build_get_order_book_params(self, symbol: ExchangeSymbol, depth: int | None = None):
       return BackpackRawGetOrderBookParams(
           symbol=str(symbol),  # String conversion ONLY at RAW boundary
           depth=depth
       )
   ```

4. **Response Handlers**
   - Accept ExchangeSymbol in method signatures
   - Work with ExchangeSymbol internally
   - RAW models they validate contain strings

5. **Mappers**
   - Create ExchangeSymbol from RAW model strings
   - Return domain models containing ExchangeSymbol
   ```python
   def transform_raw_ticker_to_internal(self, raw_ticker: HyperliquidRawTicker) -> Ticker:
       exchange_symbol = create_exchange_symbol(
           value=raw_ticker.coin,  # String from RAW model
           exchange_id=ExchangeName.HYPERLIQUID
       )
       return Ticker(symbol=exchange_symbol, ...)  # Domain model with ExchangeSymbol
   ```

6. **All Domain Models** (Order, Ticker, Position, etc.)
   - Contain ExchangeSymbol fields
   - Never contain string symbols

## What Uses Strings

**ONLY RAW Models:**
- `BackpackRawOrderBook`, `HyperliquidRawAssetCtx`, etc.
- These validate the exact structure from exchange APIs
- They are Pydantic models that match the exchange's JSON responses

## The Conversion Points

1. **Outgoing (Domain → RAW):**
   - Request builders convert ExchangeSymbol → string when creating RAW request models
   - This happens AT the RAW model creation, not before

2. **Incoming (RAW → Domain):**
   - Mappers create ExchangeSymbol from strings in RAW response models
   - This happens when transforming RAW → Domain models

## Why This Matters

- **Type Safety**: Domain objects flow through the entire system
- **Consistency**: One rule, no exceptions (except RAW models)
- **Clear Boundary**: RAW models are the explicit API contract boundary
- **No String Proliferation**: Strings don't leak into business logic

## Examples of INCORRECT Patterns

❌ **Wrong - Service accepting string:**
```python
async def get_ticker(self, symbol: str) -> Ticker:  # WRONG!
```

❌ **Wrong - Request builder accepting string:**
```python
def build_ticker_request(self, symbol: str):  # WRONG!
```

❌ **Wrong - Response handler accepting string:**
```python
def handle_ticker_response(self, response: dict, symbol: str):  # WRONG!
```

## Examples of CORRECT Patterns

✅ **Right - Service accepting ExchangeSymbol:**
```python
async def get_ticker(self, symbol: ExchangeSymbol) -> Ticker:  # CORRECT!
```

✅ **Right - Request builder converting at boundary:**
```python
def build_ticker_request(self, symbol: ExchangeSymbol):
    return HyperliquidRawTickerRequest(
        coin=str(symbol),  # Convert ONLY here
        type="ticker"
    )
```

✅ **Right - Mapper creating domain object:**
```python
def transform_raw_ticker(self, raw: RawTicker) -> Ticker:
    exchange_symbol = create_exchange_symbol(
        value=raw.symbol,  # String from RAW
        exchange_id=ExchangeName.BACKPACK
    )
    return Ticker(symbol=exchange_symbol, ...)
```

## Summary

**If it's not a RAW model, it uses ExchangeSymbol. Period.**

The only place strings exist for symbols is in RAW models, which represent the exact structure of exchange API requests and responses. Everything else is our domain, and our domain uses domain objects.