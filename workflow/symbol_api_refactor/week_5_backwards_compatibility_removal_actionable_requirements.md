# Week 5: Backwards Compatibility Removal - Actionable Requirements (Symbol Object Native)

## Executive Summary

This document provides comprehensive, actionable requirements for **fully adopting the Symbol object system** that's already correctly implemented. We eliminate **1,300+ lines of backwards compatibility code** by using Symbol objects natively throughout the API layer.

## Key Insight: Symbol Objects ARE the Formatters

The Symbol system already works correctly:
- `exchanges.hyperliquid("BTC-PERP")` creates Symbol with `.value = "BTC-PERP"` (ready for Hyperliquid API)
- `exchanges.backpack("BTC_USD_PERP")` creates Symbol with `.value = "BTC_USD_PERP"` (ready for Backpack API)
- **No additional formatting methods needed** - Symbol.value IS the formatted string

## Architecture Principles - MANDATORY

### ✅ **CORRECT: Symbol Objects Native Usage**
- **Use exchanges.hyperliquid() / exchanges.backpack() directly**
- **Use symbol_obj.value for API calls (already formatted)**
- **Use symbol_obj.base_asset, .quote_asset for business logic**
- **No normalize/denormalize methods needed**

### 🚫 **FORBIDDEN: Backwards Compatibility Violations**
- **NO SymbolIntegrationService usage**
- **NO normalize_symbol() / denormalize_symbol() methods**
- **NO get_symbol_integration_service() calls**
- **NO duplicate formatting logic**

## Backwards Compatibility Analysis

### **Critical Systems to Remove**

#### **1. SymbolIntegrationService** (Priority 1 - Backwards Compatibility Violation)
**File**: `cyberdelta/apis/common/symbol_integration.py` (937 lines)
**Impact**: Used in 50+ files across all APIs
**Problem**: Duplicates Symbol system functionality, creates backwards dependency

**Current Anti-Pattern**:
```python
from cyberdelta.apis.common.symbol_integration import get_symbol_integration_service
symbol_service = get_symbol_integration_service()
normalized = symbol_service.normalize_symbol(symbol, "hyperliquid")
```

**Native Symbol Object Replacement**:
```python
from cyberdelta.core.symbols import exchanges

symbol_obj = exchanges.hyperliquid(symbol)
formatted = symbol_obj.value  # Already formatted for Hyperliquid API
```

#### **2. HyperliquidAssetIndexResolver** (Priority 1 - Keep in API Layer)
**File**: `cyberdelta/apis/hyperliquid/hl_asset_indexer.py` (379 lines)
**Status**: **KEEP IN API LAYER** - Contains critical business logic requiring HTTP

**Critical Logic to Preserve in API Layer**:
```python
async def get_asset_index(self, symbol: str) -> int:
    # Direct @N mapping
    if symbol.startswith("@") and symbol[1:].isdigit():
        return int(symbol[1:])

    # Environment mappings  
    spot_mappings = TESTNET_SPOT_SYMBOL_MAPPINGS if testnet else MAINNET_SPOT_SYMBOL_MAPPINGS
    if symbol in spot_mappings:
        return spot_mappings[symbol]

    # API fetch from metaAndAssetCtxs - REQUIRES HTTP
    response = await self._requester(method="POST", endpoint="/info", data=payload)
    validated_response = self._response_handler.handle_info_meta_and_asset_ctxs_response(response)
    
    # Cache population
    for index, asset_def in enumerate(validated_response.meta.universe):
        self._asset_to_index_cache[asset_def.name] = index
    
    return self._asset_to_index_cache.get(symbol) or raise_error()
```

**Why This Stays in API Layer**:
- Makes HTTP requests to `/info` endpoint
- Uses `HyperliquidRawMetaAndAssetCtxsResponse` models
- Requires authentication and rate limiting
- Environment-specific configurations
- API-layer caching strategy

## Migration Requirements by Phase

### **Phase 1: Direct Symbol Object Adoption**

#### **Requirement 1.1: Replace SymbolIntegrationService Calls**

**NO new methods needed** - Symbol objects already contain formatted values.

**Global Pattern Replacement**:

```python
# REMOVE this pattern everywhere:
from cyberdelta.apis.common.symbol_integration import get_symbol_integration_service
symbol_service = get_symbol_integration_service()
normalized = symbol_service.normalize_symbol(symbol, "hyperliquid")

# REPLACE with this pattern:
from cyberdelta.core.symbols import exchanges
symbol_obj = exchanges.hyperliquid(symbol)
formatted = symbol_obj.value  # Already formatted for Hyperliquid API
```

#### **Requirement 1.2: Update All Mapper normalize/denormalize Methods**

**Files Affected**: All mapper files with backwards compatibility methods

```python
# REMOVE these methods entirely:
def normalize_symbol(self, symbol: str) -> str:
    # Unnecessary - Symbol.value IS normalized
    
def denormalize_symbol(self, symbol: str) -> str:
    # Unnecessary - not needed with Symbol objects

# REPLACE with simple helper if needed:
def format_symbol_for_api(self, symbol: str) -> str:
    """Get exchange-formatted symbol using Symbol system."""
    symbol_obj = exchanges.hyperliquid(symbol)
    return symbol_obj.value  # Already formatted
```

### **Phase 2: Comprehensive API Migration**

#### **Requirement 2.1: Update All Mappers to Use Symbol Objects**

**Target Files**: All mapper files throughout the API layer

**Native Symbol Pattern**:

```python
# REMOVE this entirely:
from cyberdelta.apis.common.symbol_integration import get_symbol_integration_service

class SomeMapper:
    def normalize_symbol(self, symbol: str) -> str:
        symbol_service = get_symbol_integration_service()
        return symbol_service.normalize_symbol(symbol, "hyperliquid")

# REPLACE with Native Symbol usage:
from cyberdelta.core.symbols import exchanges

class SomeMapper:
    def transform_request(self, args):
        # Create Symbol object (rich domain object)
        symbol_obj = exchanges.hyperliquid(args.symbol)
        
        # Use symbol_obj.value for API calls (already formatted)
        # Use symbol_obj.base_asset, .quote_asset for business logic
        return SomeApiRequest(
            symbol=symbol_obj.value,  # Already formatted for exchange
            base_asset=symbol_obj.base_asset,
            market_type=symbol_obj.market_type
        )
```

#### **Requirement 2.2: Specific Mapper File Updates**

**Priority Files**:
- `cyberdelta/apis/backpack/mappers/utils/common_mappers.py`
- `cyberdelta/apis/hyperliquid/mappers/utils/hyperliquid_common_mappers.py`

**Changes**:
- Remove all `normalize_symbol()` / `denormalize_symbol()` methods
- Remove all `get_symbol_integration_service()` imports  
- Add simple `format_symbol_for_api()` method if needed:
  ```python
  def format_symbol_for_api(self, symbol: str) -> str:
      symbol_obj = exchanges.{exchange}(symbol)
      return symbol_obj.value
  ```

### **Phase 3: Native Symbol Integration Pattern**

#### **Requirement 3.1: Trading Operations with Native Symbol Objects**

**Example: Order Mapper with Symbol Objects**:
```python
class HyperliquidOrderMapper:
    def __init__(self, asset_indexer: HyperliquidAssetIndexResolver):
        # Asset indexer stays in API layer (HTTP infrastructure)
        self._asset_indexer = asset_indexer
    
    async def transform_order_request(self, args: PlaceOrderArgs) -> HyperliquidRawOrderRequest:
        # 1. Create Symbol object (rich domain object)
        symbol_obj = exchanges.hyperliquid(args.symbol)
        
        # 2. Use API layer for HTTP operations
        asset_index = await self._asset_indexer.get_asset_index(symbol_obj.value)
        
        # 3. Use symbol_obj.value directly (already formatted for Hyperliquid)
        return HyperliquidRawOrderRequest(
            symbol=symbol_obj.value,  # No additional formatting needed
            asset_index=asset_index,
            size=args.size,
            base_asset=symbol_obj.base_asset,  # Rich metadata available
            market_type=symbol_obj.market_type
        )
```

#### **Requirement 3.2: Universal API Pattern**

**Pattern for ALL API operations**:
```python
# Step 1: Create Symbol object from string input
symbol_obj = exchanges.{exchange}(symbol_string)

# Step 2: Use symbol_obj.value for API calls (already exchange-formatted)
api_request = SomeAPIRequest(symbol=symbol_obj.value)

# Step 3: Use symbol_obj metadata for business logic
if symbol_obj.market_type == MarketType.PERP:
    # Handle perpetual-specific logic
    
if symbol_obj.metadata.asset_index is not None:
    asset_index = symbol_obj.metadata.asset_index
else:
    # Use API layer for HTTP resolution when needed
    asset_index = await self._asset_indexer.get_asset_index(symbol_obj.value)
```

### **Phase 4: WebSocket Symbol Migration**

#### **Requirement 4.1: Update WebSocket Validators**

**File**: `cyberdelta/apis/websocket/ws_validators.py`

**Replace SymbolIntegrationService Usage**:
```python
# BEFORE
from cyberdelta.apis.common.symbol_integration import get_symbol_integration_service

def validate_symbol(symbol: str, exchange_id: str) -> str:
    symbol_service = get_symbol_integration_service()
    return symbol_service.normalize_symbol(symbol, exchange_id)

# AFTER  
from cyberdelta.core.symbols import exchanges

def validate_websocket_symbol(symbol: str, exchange_id: str) -> Symbol:
    """Convert string to Symbol object at ingestion boundary."""
    return exchanges.__getattr__(exchange_id.lower())(symbol)
```

#### **Requirement 4.2: Update WebSocket Context Models**

**Pattern**: Convert strings to Symbol objects at boundary, process internally as Symbol objects

```python
# ws_context.py
from cyberdelta.core.symbols.models import Symbol

class WebSocketMessageContext[EnvelopeType: "BaseModel"](BaseModel):
    symbol: Symbol | None = Field(default=None)  # Changed from str

# At message ingestion boundary
def process_websocket_message(raw_message: dict) -> WebSocketMessageContext:
    symbol_str = raw_message.get("symbol")
    symbol_obj = validate_websocket_symbol(symbol_str, exchange_id) if symbol_str else None
    return WebSocketMessageContext(symbol=symbol_obj, ...)
```

### **Phase 5: Cleanup and File Removal**

#### **Requirement 5.1: File Deletions**

**Files to Delete**:
1. `cyberdelta/apis/common/symbol_integration.py` (937 lines)

**Files to Keep** (In API Layer):
- `cyberdelta/apis/hyperliquid/hl_asset_indexer.py` - Contains critical HTTP business logic

#### **Requirement 5.2: Method Removals**

**Remove from ALL mapper files**:
- `normalize_symbol()` methods using SymbolIntegrationService
- `denormalize_symbol()` methods  
- All `get_symbol_integration_service()` imports

**Replace with**:
- `format_symbol_for_api()` methods using pure handlers
- Direct `exchanges.{exchange}()` usage
- Handler-based formatting

#### **Requirement 5.3: Import Updates**

**Global Find/Replace**:
```python
# Remove all occurrences
from cyberdelta.apis.common.symbol_integration import get_symbol_integration_service

# Replace with
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.registry import get_registry
```

## Testing Requirements

### **Critical Test Cases**

#### **Symbol System Purity Tests**
```python
def test_symbol_handlers_have_no_async():
    """Ensure handlers contain no async methods."""
    import inspect
    from cyberdelta.core.symbols.handlers import HyperliquidHandler, BackpackHandler
    
    for handler_class in [HyperliquidHandler, BackpackHandler]:
        for name, method in inspect.getmembers(handler_class, predicate=inspect.isfunction):
            assert not inspect.iscoroutinefunction(method), f"{handler_class.__name__}.{name} is async"

def test_symbol_system_has_no_http_imports():
    """Ensure symbol system has no HTTP dependencies."""
    import ast
    import os
    
    symbols_dir = "cyberdelta/core/symbols"
    forbidden_imports = ["aiohttp", "requests", "httpx", "urllib"]
    
    for root, dirs, files in os.walk(symbols_dir):
        for file in files:
            if file.endswith(".py"):
                with open(os.path.join(root, file)) as f:
                    tree = ast.parse(f.read())
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            assert alias.name not in forbidden_imports
```

#### **API Integration Tests**
```python
async def test_hyperliquid_order_mapping_with_asset_index():
    """Verify asset index resolution works in API layer."""
    mapper = HyperliquidOrderMapper(asset_indexer=mock_asset_indexer)
    
    # Test with @N format (should resolve directly)
    args = PlaceOrderArgs(symbol="@1", size=Decimal("1.0"))
    request = await mapper.transform_order_request(args)
    assert request.asset_index == 1
    
    # Test with API fetch (should use HTTP)
    args = PlaceOrderArgs(symbol="BTC", size=Decimal("1.0"))
    request = await mapper.transform_order_request(args)
    assert isinstance(request.asset_index, int)
```

#### **Symbol Formatting Accuracy**
```python
def test_symbol_formatting_accuracy():
    """Verify exact API formatting matches current behavior."""
    # Hyperliquid
    btc_perp = exchanges.hyperliquid("BTC-PERP")
    handler = get_registry().get_handlers()[ExchangeName.HYPERLIQUID]
    assert handler.format_for_exchange(btc_perp) == "BTC-PERP"
    
    # Backpack
    btc_perp = exchanges.backpack("BTC_USD_PERP")
    handler = get_registry().get_handlers()[ExchangeName.BACKPACK]
    assert handler.format_for_exchange(btc_perp) == "BTC_USD_PERP"
    
    # Special cases
    at_symbol = exchanges.hyperliquid("@1")
    handler = get_registry().get_handlers()[ExchangeName.HYPERLIQUID]
    assert handler.format_for_exchange(at_symbol) == "@1"
```

## Success Metrics

### **Quantitative Goals**
- ✅ **Code Reduction**: Remove 937 lines from SymbolIntegrationService
- ✅ **Import Cleanup**: Update 50+ files to use pure symbol system
- ✅ **Method Replacement**: Replace normalize/denormalize in 28 mapper files
- ✅ **Architecture Purity**: Zero HTTP imports in cyberdelta/core/symbols/
- ✅ **Performance**: No degradation in symbol processing latency

### **Qualitative Goals**
- ✅ **Clean Architecture**: Complete separation of domain and infrastructure concerns
- ✅ **Type Safety**: Full Symbol object usage throughout APIs
- ✅ **Maintainability**: Clear handler-based extension pattern
- ✅ **Testability**: Pure functions in symbol system, mockable API layer
- ✅ **Business Logic Preservation**: Hyperliquid signing continues to work exactly

## Risk Mitigation

### **🔴 High Risk: Architecture Violations**
- **Risk**: Accidentally adding API logic to symbol system
- **Mitigation**: Automated tests checking for HTTP imports and async methods
- **Verification**: Code review focusing on import statements and method signatures

### **🟡 Medium Risk: Asset Index Resolution**
- **Risk**: Breaking Hyperliquid order signing during integration
- **Mitigation**: Keep HyperliquidAssetIndexResolver in API layer unchanged
- **Verification**: Integration tests with real testnet orders

### **🟢 Low Risk: Symbol Formatting**
- **Risk**: API format mismatch during handler migration
- **Mitigation**: Comprehensive unit tests comparing old vs new formatting
- **Verification**: API response validation in integration tests

## Implementation Timeline

### **Day 1: Handler Extensions (Pure Domain)**
- [ ] Add `format_for_exchange()` to HyperliquidHandler
- [ ] Add `format_for_exchange()` to BackpackHandler  
- [ ] Write purity tests (no async, no HTTP imports)

### **Day 2: Mapper Replacements**
- [ ] Update common_mappers.py files
- [ ] Replace normalize/denormalize methods
- [ ] Test symbol formatting accuracy

### **Day 3: API Integration Pattern**
- [ ] Update order mappers to use clean integration
- [ ] Ensure asset indexer stays in API layer
- [ ] Test end-to-end order placement

### **Day 4: WebSocket Migration**
- [ ] Update WebSocket validators
- [ ] Convert to Symbol objects at boundaries
- [ ] Test WebSocket message processing

### **Day 5: Cleanup and Testing**
- [ ] Delete SymbolIntegrationService file
- [ ] Remove all backwards compatibility imports
- [ ] Run full integration test suite

This migration will eliminate backwards compatibility while maintaining perfect architectural separation between pure domain logic and API infrastructure concerns.