# Complete File Migration Guide - Simple DDD Refactor

## Overview: What Happens to Every File

This document provides a comprehensive breakdown of what happens to **every single file** in the current symbol system during the simple DDD breaking refactor.

**TL;DR:** Most files stay unchanged. You're deleting 3 complex files (~800+ lines) and adding 3 simple files (~300 lines) for a net reduction in code complexity.

---

## Current Symbol System File Structure

```
cyberdelta/core/symbols/
├── __init__.py                 # Module exports
├── models.py                   # Domain models (InternalSymbol, ExchangeSymbol, etc.)
├── registry.py                 # ❌ GOD CLASS - 544 lines of everything
├── transformers.py             # ❌ Complex transformation system
├── cache.py                    # ❌ Over-engineered caching
├── exceptions.py               # Error definitions
├── validators.py               # Symbol validation logic
└── operation_results.py        # Result models (SymbolBatchTransformResult, etc.)
```

---

## New Architecture File Structure

```
cyberdelta/core/symbols/
├── __init__.py                 # MODIFIED - Updated exports
├── models.py                   # MODIFIED - Remove 1 method only
├── store.py                    # 🆕 NEW - Pure storage (80 lines)
├── adapters.py                 # 🆕 NEW - Exchange plugins (100 lines)
├── service.py                  # 🆕 NEW - Application service (120 lines)
├── exceptions.py               # ✅ UNCHANGED - Keep as-is
├── validators.py               # ✅ UNCHANGED - Keep as-is
└── operation_results.py        # ✅ UNCHANGED - Keep as-is
```

---

## File-by-File Migration Strategy

### 🆕 **NEW FILES (Add These)**

#### 1. `store.py` - Pure Storage Layer
```python
# NEW FILE: cyberdelta/core/symbols/store.py
# PURPOSE: Replace storage logic from registry.py
# SIZE: ~80 lines
# RESPONSIBILITY: Thread-safe symbol storage with fast lookups

from threading import RLock
from typing import Dict, Optional, List
from cyberdelta.core.symbols.models import UnifiedSymbol

class SymbolStore:
    """Pure storage layer - single responsibility principle."""
    
    def __init__(self):
        self._symbols: Dict[str, UnifiedSymbol] = {}
        self._internal_to_exchange: Dict[str, Dict[str, str]] = {}
        self._exchange_to_internal: Dict[str, Dict[str, str]] = {}
        self._lock = RLock()
    
    # ... implementation from refactor document
```

#### 2. `adapters.py` - Exchange Plugin System  
```python
# NEW FILE: cyberdelta/core/symbols/adapters.py
# PURPOSE: Replace transformers.py with simpler plugin pattern
# SIZE: ~100 lines
# RESPONSIBILITY: Exchange-specific transformations only

from abc import ABC, abstractmethod
from typing import Dict
from cyberdelta.core.symbols.models import InternalSymbol, create_internal_symbol

class ExchangeAdapter(ABC):
    """Plugin interface for exchange-specific transformations."""
    
    @abstractmethod
    def internal_to_exchange(self, internal: InternalSymbol) -> str: ...
    
    @abstractmethod  
    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol: ...

# Concrete adapters for Hyperliquid, Backpack, future exchanges
EXCHANGE_ADAPTERS: Dict[str, ExchangeAdapter] = {
    "hyperliquid": HyperliquidAdapter(),
    "backpack": BackpackAdapter(),
}
```

#### 3. `service.py` - Clean Application API
```python
# NEW FILE: cyberdelta/core/symbols/service.py  
# PURPOSE: Replace registry.py API with clean service interface
# SIZE: ~120 lines
# RESPONSIBILITY: Orchestrate storage + adapters, provide clean API

from typing import Optional, List
from cyberdelta.core.symbols.store import SymbolStore
from cyberdelta.core.symbols.adapters import EXCHANGE_ADAPTERS

class SymbolService:
    """Application service - orchestrates domain operations."""
    
    def __init__(self, store: Optional[SymbolStore] = None):
        self.store = store or SymbolStore()
    
    def get_internal_symbol(self, exchange_symbol: str, exchange_name: str) -> InternalSymbol: ...
    def get_exchange_symbol(self, internal_symbol: str, exchange_name: str) -> ExchangeSymbol: ...
    # ... clean API methods
```

---

### ❌ **DELETE THESE FILES**

#### 1. `registry.py` - Delete Entire File
```python
# DELETE: cyberdelta/core/symbols/registry.py
# REASON: 544-line god class violates single responsibility
# REPLACED BY: Distributed logic across store.py + service.py

# What gets deleted:
class SymbolRegistry:  # ← 544 lines of mixed responsibilities
    def __init__(self):
        self._cache = MultiLevelCache()      # → Simplified in service.py
        self._asset_resolver = Resolver()    # → Moved to adapters
        self._symbols = {}                   # → Moved to store.py  
        self._lock = RLock()                # → Moved to store.py
        # + validation, transformation, statistics, etc.

class _SymbolRegistrySingleton: ...     # ← DELETE
def get_symbol_registry(): ...          # ← DELETE (replaced by SymbolService)
```

#### 2. `transformers.py` - Delete Entire File
```python
# DELETE: cyberdelta/core/symbols/transformers.py
# REASON: Over-engineered transformation system  
# REPLACED BY: Simple adapter pattern in adapters.py

# What gets deleted:
class BaseTransformer: ...              # ← DELETE (replaced by ExchangeAdapter)
class HyperliquidTransformer: ...       # ← DELETE (replaced by HyperliquidAdapter)
class BackpackTransformer: ...          # ← DELETE (replaced by BackpackAdapter)
class UnifiedSymbolTransformer: ...     # ← DELETE (logic moved to service.py)

# The new adapters are much simpler - just 2 methods per exchange
```

#### 3. `cache.py` - Delete Entire File (Optional)
```python
# DELETE: cyberdelta/core/symbols/cache.py (OPTIONAL)
# REASON: Over-engineered multi-level caching system
# REPLACED BY: Simple caching in service.py or keep if you want

# What gets deleted:
class MultiLevelCache: ...              # ← DELETE (too complex for needs)
class ThreadSafeAssetIndexResolver: ... # ← DELETE (move to adapters if needed)

# NOTE: You can keep this file if you want the advanced caching
# The new service.py doesn't require it but can use it
```

---

### ✅ **KEEP UNCHANGED (These Stay Exactly As-Is)**

#### 1. `exceptions.py` - No Changes Required
```python
# KEEP: cyberdelta/core/symbols/exceptions.py
# REASON: All exceptions still needed in new architecture
# CHANGES: None

class SymbolError(Exception): ...           # ← Still used
class SymbolNotFoundError(SymbolError): ... # ← Still used  
class SymbolValidationError(SymbolError): ...# ← Still used
class SymbolRegistryError(SymbolError): ... # ← Still used (service errors)

# All exception classes remain exactly the same
```

#### 2. `validators.py` - No Changes Required
```python
# KEEP: cyberdelta/core/symbols/validators.py
# REASON: Validation logic is still needed
# CHANGES: None

class SymbolValidator:
    @classmethod
    def validate_symbol(cls, ...): ...      # ← Still used by adapters
    
    @classmethod
    def validate_websocket_symbol(cls, ...): ...  # ← Still used
    
    # All validation methods stay exactly the same
```

#### 3. `operation_results.py` - No Changes Required
```python
# KEEP: cyberdelta/core/symbols/operation_results.py  
# REASON: Result models are still useful for complex operations
# CHANGES: None

class SymbolBatchTransformResult(BaseModel): ... # ← Can still use in service
class SymbolArbitrageCompatibility(BaseModel): ...# ← Can still use in service

# These Pydantic models remain exactly the same
```

---

### 🔧 **MINOR MODIFICATIONS (Small Changes)**

#### 1. `models.py` - Remove One Method Only
```python
# MODIFY: cyberdelta/core/symbols/models.py
# CHANGES: Delete one method that violates domain purity

class ExchangeSymbol(BaseModel):
    value: str
    asset_index: int | None = None
    symbol_id: str | None = None
    
    # DELETE THIS METHOD - violates domain layer purity
    # def to_api_format(self, exchange_id: ExchangeName) -> dict:
    #     if exchange_id == ExchangeName.HYPERLIQUID:
    #         return {"symbol": self.value, "assetIndex": self.asset_index}
    #     elif exchange_id == ExchangeName.BACKPACK:
    #         return {"symbol": self.value, "symbolId": self.symbol_id}

# EVERYTHING ELSE STAYS THE SAME:
class InternalSymbol(BaseModel): ...    # ← Unchanged
class UnifiedSymbol(BaseModel): ...     # ← Unchanged  
def create_internal_symbol(...): ...    # ← Unchanged
def create_exchange_symbol(...): ...    # ← Unchanged
```

**Migration for `to_api_format()` calls:**
```python
# OLD (breaks after deletion)
exchange_symbol = get_exchange_symbol(...)
api_data = exchange_symbol.to_api_format(ExchangeName.HYPERLIQUID)

# NEW (move to API layer where it belongs)
exchange_symbol = service.get_exchange_symbol(...)

def format_for_hyperliquid_api(symbol: ExchangeSymbol) -> dict:
    return {"symbol": symbol.value, "assetIndex": symbol.asset_index}

def format_for_backpack_api(symbol: ExchangeSymbol) -> dict:
    return {"symbol": symbol.value, "symbolId": symbol.symbol_id}

api_data = format_for_hyperliquid_api(exchange_symbol)
```

#### 2. `__init__.py` - Update Exports
```python
# MODIFY: cyberdelta/core/symbols/__init__.py
# CHANGES: Update exports to reflect new architecture

# DELETE these exports
# from .registry import get_symbol_registry, SymbolRegistry
# from .transformers import get_unified_transformer, UnifiedSymbolTransformer

# ADD these exports  
from .service import SymbolService
from .store import SymbolStore
from .adapters import EXCHANGE_ADAPTERS, ExchangeAdapter

# KEEP these exports (unchanged)
from .models import (
    InternalSymbol,
    ExchangeSymbol, 
    UnifiedSymbol,
    create_internal_symbol,
    create_exchange_symbol,
)
from .exceptions import (
    SymbolError,
    SymbolNotFoundError,
    SymbolValidationError,
    SymbolRegistryError,
)
from .validators import SymbolValidator, CrossExchangeValidator
from .operation_results import (
    SymbolBatchTransformResult,
    SymbolArbitrageCompatibility,
)

# Public API - what consumers should use
__all__ = [
    "SymbolService",           # ← NEW: Main entry point
    "SymbolStore",             # ← NEW: For advanced users
    "EXCHANGE_ADAPTERS",       # ← NEW: For extensibility
    # ... keep all existing exports
]
```

---

### 🔗 **INTEGRATION FILES (Update Imports)**

#### `symbol_integration.py` - Update Service Import
```python
# MODIFY: cyberdelta/apis/common/symbol_integration.py
# CHANGES: Update imports and initialization

# OLD imports
from cyberdelta.core.symbols.registry import get_symbol_registry
from cyberdelta.core.symbols.transformers import get_unified_transformer

# NEW imports
from cyberdelta.core.symbols.service import SymbolService

class SymbolIntegrationService:
    def __init__(self, ...):
        # OLD initialization
        # self.registry = get_symbol_registry()
        # self.transformer = get_unified_transformer()
        
        # NEW initialization  
        self.service = SymbolService()
        
    # Update method implementations to use self.service instead of self.registry
```

---

## Migration Timeline & Effort

### **Phase 1: Add New Files (No Breaking Changes)**
**Time: 1-2 hours**
1. Create `store.py` with SymbolStore class
2. Create `adapters.py` with exchange adapters  
3. Create `service.py` with SymbolService class
4. Test new classes work independently

### **Phase 2: Update Consumers (Breaking Changes Start)**  
**Time: 2-3 hours**
1. Update `__init__.py` exports
2. Find all `get_symbol_registry()` calls → replace with `SymbolService()`
3. Find all `to_api_format()` calls → move to API layer
4. Update integration files to use new service

### **Phase 3: Delete Old Files**
**Time: 30 minutes**  
1. Delete `registry.py` 
2. Delete `transformers.py`
3. Delete `cache.py` (optional)
4. Remove one method from `models.py`

### **Phase 4: Test & Validate**
**Time: 1 hour**
1. Run existing tests
2. Fix any remaining import issues
3. Validate all symbol operations work

---

## Code Impact Summary

### **Files Deleted: 3**
- `registry.py` (544 lines - god class)
- `transformers.py` (~200 lines - complex system)
- `cache.py` (~150+ lines - over-engineered)

### **Files Added: 3**
- `store.py` (80 lines - focused storage)
- `adapters.py` (100 lines - simple plugins)
- `service.py` (120 lines - clean API)

### **Files Modified: 2** 
- `models.py` (delete 1 method - 10 lines removed)
- `__init__.py` (update exports - 5 lines changed)

### **Files Unchanged: 3**
- `exceptions.py` (100% same)
- `validators.py` (100% same)
- `operation_results.py` (100% same)

### **Net Code Change**
- **Deleted**: ~900+ lines of complex, tightly-coupled code
- **Added**: ~300 lines of simple, focused code  
- **Modified**: ~15 lines of import/export changes

**Result: ~600 lines less code with infinitely better architecture!**

---

## Benefits After Migration

### ✅ **For Solo Developer**
- **New Exchange**: 5 minutes (implement 2 methods in adapter)
- **Debugging**: 3 focused classes instead of 544-line monster
- **Testing**: Each component mockable and isolated
- **Understanding**: Clear separation of concerns

### ✅ **Architecture Quality**
- **Single Responsibility**: Each class does one thing
- **Open/Closed**: New exchanges via adapters (no modification)
- **Dependency Inversion**: Service depends on abstractions
- **DDD Compliance**: Clean domain/infrastructure separation

### ✅ **Code Quality**
- **Less Code**: ~600 fewer lines
- **Better Structure**: Focused classes vs god class
- **Type Safety**: Full Pydantic validation maintained
- **Error Handling**: All existing exceptions preserved

This migration transforms your symbol system from an architectural liability into an architectural asset, setting you up for years of productive development as you add new exchanges.