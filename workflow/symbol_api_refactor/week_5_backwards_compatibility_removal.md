# Week 5: Backwards Compatibility Removal Plan - Symbol Object Native Architecture

## Overview

This document outlines the complete removal of backwards compatibility layers by **fully adopting the Symbol object system** that is already correctly implemented. The Symbol object **IS the formatter** - no additional formatting methods needed.

**🔗 See Also**: [Actionable Requirements Document](./week_5_backwards_compatibility_removal_actionable_requirements.md) for detailed implementation specifications.

## Key Insight: Symbol Objects Are Already Formatters

The Symbol system creates exchange-specific objects where `.value` **IS** the correctly formatted string for that exchange:

```python
# Hyperliquid Symbol - value is already formatted for Hyperliquid API
hl_symbol = exchanges.hyperliquid("BTC-PERP")
print(hl_symbol.value)  # "BTC-PERP" - ready for Hyperliquid API

# Backpack Symbol - value is already formatted for Backpack API  
bp_symbol = exchanges.backpack("BTC_USD_PERP")
print(bp_symbol.value)  # "BTC_USD_PERP" - ready for Backpack API
```

**No additional formatting needed!** The Symbol object `.value` property contains the exchange-ready format.

## Architecture Principles - CRITICAL

### ✅ **Symbol System (Pure Domain) - ALREADY CORRECT**
- Symbol parsing and validation (`BTC-PERP` → components)
- Exchange-specific formatting built into Symbol.value
- Type-safe domain models with rich metadata
- Clean API via `exchanges.hyperliquid()` / `exchanges.backpack()`
- **NO HTTP, NO ASYNC, NO API DEPENDENCIES**

### ✅ **API Layer (Exchange Integration)**  
- Asset index resolution via HTTP calls (HyperliquidAssetIndexResolver)
- Exchange-specific error handling and authentication
- Environment-specific mappings (testnet/mainnet)  
- Business logic requiring external data
- **Uses Symbol objects' .value property directly**

## Current State Analysis

### ✅ **What's Working (Keep)**

1. **Symbol Object Creation**:
   ```python
   # Creates properly formatted symbols
   hl_symbol = exchanges.hyperliquid("BTC-PERP")  # .value = "BTC-PERP"
   bp_symbol = exchanges.backpack("BTC_USD_PERP")  # .value = "BTC_USD_PERP"
   ```

2. **Domain Formatters** (cyberdelta/apis/common/domain_formatters.py):
   ```python
   # ALREADY CORRECT - uses symbol.value directly
   def format_symbol_for_api(exchange_symbol: Symbol) -> dict[str, Any]:
       api_format = {"symbol": exchange_symbol.value}  # ✅ Uses .value
   ```

3. **HyperliquidAssetIndexResolver** (stays in API layer):
   ```python
   # CORRECT LOCATION - contains HTTP logic
   async def get_asset_index(self, symbol: str) -> int:
       # Makes HTTP requests - belongs in API layer
   ```

### ❌ **What's Broken (Remove)**

1. **SymbolIntegrationService** (937 lines) - **ARCHITECTURAL VIOLATION**:
   ```python
   # WRONG - creates backwards dependency API → Symbol system
   symbol_service = get_symbol_integration_service()
   normalized = symbol_service.normalize_symbol(symbol, "hyperliquid")
   ```

2. **Mapper normalize/denormalize methods** - **DUPLICATES SYMBOL SYSTEM**:
   ```python
   # WRONG - Symbol.value IS the normalized format
   def normalize_symbol(self, symbol: str) -> str:
       return symbol_service.normalize_symbol(symbol, "hyperliquid")
   ```

## Migration Strategy - Symbol Object Native

### Phase 1: Direct Symbol Object Usage

**Replace all SymbolIntegrationService calls**:

```python
# BEFORE (backwards compatibility violation)
symbol_service = get_symbol_integration_service()
normalized = symbol_service.normalize_symbol(symbol, "hyperliquid")

# AFTER (direct Symbol object usage)
symbol_obj = exchanges.hyperliquid(symbol)
formatted = symbol_obj.value  # Already formatted for Hyperliquid API
```

### Phase 2: Eliminate Mapper Compatibility Methods

**Remove all normalize/denormalize methods**:

```python
# BEFORE (duplicates Symbol system)
class SomeMapper:
    def normalize_symbol(self, symbol: str) -> str:
        # Unnecessary - Symbol.value IS normalized
        return symbol_service.normalize_symbol(symbol, "hyperliquid")

# AFTER (use Symbol objects directly)
class SomeMapper:
    def format_symbol_for_api(self, symbol: str) -> str:
        """Get exchange-formatted symbol using Symbol system."""
        symbol_obj = exchanges.hyperliquid(symbol)
        return symbol_obj.value  # Already formatted
```

### Phase 3: API Integration Pattern

**Correct integration preserving asset index resolution**:

```python
class HyperliquidOrderMapper:
    def __init__(self, asset_indexer: HyperliquidAssetIndexResolver):
        self._asset_indexer = asset_indexer  # Stays in API layer
    
    async def transform_order_request(self, args: PlaceOrderArgs):
        # 1. Create Symbol object (pure domain)
        symbol_obj = exchanges.hyperliquid(args.symbol)
        
        # 2. Get asset index (API layer with HTTP)
        asset_index = await self._asset_indexer.get_asset_index(symbol_obj.value)
        
        # 3. Use symbol.value directly (already formatted)
        return HyperliquidRawOrderRequest(
            symbol=symbol_obj.value,  # No additional formatting needed
            asset_index=asset_index,
            size=args.size
        )
```

### Phase 4: WebSocket Migration

**Convert WebSocket processing to use Symbol objects**:

```python
# BEFORE (string-based processing)
def process_websocket_message(raw_message: dict):
    symbol_str = raw_message.get("symbol")
    # String processing throughout...

# AFTER (Symbol object at boundary)
def process_websocket_message(raw_message: dict):
    symbol_str = raw_message.get("symbol")
    symbol_obj = exchanges.hyperliquid(symbol_str)  # Convert at boundary
    # Use symbol_obj.value for API calls, symbol_obj.base_asset for logic
```

### Phase 5: Complete Cleanup

**Files to Delete**:
1. `cyberdelta/apis/common/symbol_integration.py` (937 lines)

**Methods to Remove**:
- All `normalize_symbol()` / `denormalize_symbol()` methods
- All `get_symbol_integration_service()` imports
- All backwards compatibility wrapper methods

## Implementation Steps

### Step 1: Replace SymbolIntegrationService Calls

**Pattern**: Replace all instances throughout codebase:

```python
# REMOVE this pattern everywhere:
symbol_service = get_symbol_integration_service()
normalized = symbol_service.normalize_symbol(symbol, "hyperliquid")

# REPLACE with this pattern:
symbol_obj = exchanges.hyperliquid(symbol)
formatted = symbol_obj.value  # Already exchange-ready
```

### Step 2: Update Mapper Classes

**Target Files**: All mapper files with normalize/denormalize methods

```python
# REMOVE these methods:
def normalize_symbol(self, symbol: str) -> str:
def denormalize_symbol(self, symbol: str) -> str:

# REPLACE with:
def format_symbol_for_api(self, symbol: str) -> str:
    """Format symbol for API using Symbol system."""
    symbol_obj = exchanges.{exchange}(symbol)
    return symbol_obj.value
```

### Step 3: Preserve Critical API Logic

**Keep HyperliquidAssetIndexResolver unchanged** - it's infrastructure logic that belongs in API layer.

**Keep domain_formatters.py** - it already uses Symbol objects correctly.

### Step 4: Update All API Operations

**Pattern for all API operations**:

```python
# 1. Create Symbol object from string input
symbol_obj = exchanges.hyperliquid(symbol_string)

# 2. Use symbol_obj.value for API calls (already formatted)
api_request = SomeAPIRequest(symbol=symbol_obj.value)

# 3. Use symbol_obj properties for business logic
if symbol_obj.market_type == MarketType.PERP:
    # Handle perpetual logic
    
# 4. Use API layer for HTTP operations (asset index, etc.)
asset_index = await self._asset_indexer.get_asset_index(symbol_obj.value)
```

## Success Criteria

1. ✅ **Symbol Object Native**: All APIs use `exchanges.hyperliquid()` / `exchanges.backpack()` directly
2. ✅ **No Duplicate Formatting**: Remove all normalize/denormalize methods (Symbol.value IS formatted)
3. ✅ **Asset Index Preserved**: HyperliquidAssetIndexResolver stays in API layer unchanged
4. ✅ **Type Safety**: Full Symbol object usage with rich metadata (base_asset, quote_asset, etc)
5. ✅ **Backwards Compatibility Elimination**: Remove 937-line SymbolIntegrationService
6. ✅ **Business Logic Preservation**: All critical functionality preserved, just using Symbol objects

## Post-Migration Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Layer                                │
│  - HyperliquidAssetIndexResolver (HTTP asset index resolution) │
│  - Uses exchanges.hyperliquid() to create Symbol objects       │  
│  - Uses symbol_obj.value for API calls (already formatted)     │
│  - Uses symbol_obj.base_asset, .quote_asset for business logic │
│                                                                 │
│  Uses ↓ (Symbol Objects)                                       │
└─────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────┐
│                Symbol System (Pure Domain) - ALREADY WORKING   │
│  - exchanges.hyperliquid("BTC-PERP") → Symbol{.value="BTC-PERP"}│
│  - exchanges.backpack("BTC_PERP") → Symbol{.value="BTC_PERP"}  │
│  - Rich metadata: .base_asset, .quote_asset, .market_type      │
│  - NO HTTP, NO ASYNC, NO API DEPENDENCIES                      │
└─────────────────────────────────────────────────────────────────┘
```

## Key Insights

1. **Symbol Objects ARE Formatters**: No need for additional `format_for_exchange()` methods
2. **Symbol.value IS Exchange-Ready**: Contains the correctly formatted string for that exchange
3. **Domain Formatters Already Work**: They use `symbol.value` correctly
4. **Asset Index Resolution Belongs in API**: It's infrastructure, not domain logic
5. **Migration = Elimination**: Remove backwards compatibility, don't add new formatting

## Final Migration Strategy

1. **Replace** `get_symbol_integration_service()` calls with `exchanges.{exchange}()` 
2. **Remove** all `normalize_symbol()` / `denormalize_symbol()` methods
3. **Use** `symbol_obj.value` directly (already formatted)
4. **Keep** HyperliquidAssetIndexResolver in API layer (HTTP infrastructure)
5. **Delete** SymbolIntegrationService entirely (backwards compatibility violation)

This approach fully adopts the Symbol object system that's already working correctly, eliminating the backwards compatibility layer that's creating confusion and architectural violations.