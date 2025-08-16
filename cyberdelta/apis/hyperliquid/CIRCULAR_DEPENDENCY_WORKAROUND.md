# Hyperliquid Package - Circular Dependency Workaround

## Status: TEMPORARY WORKAROUND IN PLACE

### Problem
The Hyperliquid package `__init__.py` cannot import `HyperliquidAPI` due to a circular dependency that prevents the entire application from starting.

### Circular Dependency Chain
```
1. main.py → exchange_api.py
2. exchange_api.py → connectivity/__init__.py
3. connectivity → websocket/__init__.py
4. websocket → ws_type_adapters.py
5. ws_type_adapters.py → hyperliquid/models/hl_raw_ws_events.py
6. This triggers hyperliquid/__init__.py
7. __init__.py → hl_api.py (REMOVED TO BREAK CYCLE)
8. hl_api.py → exchange_api.py (WOULD BE CIRCULAR!)
```

### Current Workaround
- **Removed**: `from .hl_api import HyperliquidAPI` from `__init__.py`
- **Impact**: Package cannot be imported as `from cyberdelta.apis.hyperliquid import HyperliquidAPI`
- **Alternative**: Use direct import `from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI`

### Where This Affects Code
1. **ExchangeAPIFactory** - Already uses direct imports ✅
2. **Tests** - May need updates if using package imports
3. **User code** - Must use direct imports

### Example Usage
```python
# ❌ OLD WAY (No longer works)
from cyberdelta.apis.hyperliquid import HyperliquidAPI

# ✅ NEW WAY (Required workaround)
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
```

### Permanent Solution Required
See `/workflow/websocket_api_circular/` for the complete analysis and proposed plugin-based architecture that will properly fix this issue.

### Timeline
- **Current**: Workaround allows application to run
- **Next Sprint**: Implement proper solution with plugin architecture
- **Technical Debt**: Track as P1 issue
