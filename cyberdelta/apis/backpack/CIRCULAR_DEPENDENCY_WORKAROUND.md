# Backpack Package - Circular Dependency Workaround

## Status: TEMPORARY WORKAROUND IN PLACE

### Problem
The Backpack package `__init__.py` cannot import its API classes due to a circular dependency that prevents the entire application from starting.

### Circular Dependency Chain
```
1. main.py → exchange_api.py
2. exchange_api.py → connectivity/__init__.py
3. connectivity → websocket/__init__.py
4. websocket → ws_type_adapters.py
5. ws_type_adapters.py → backpack/models/bp_raw_ws_models.py
6. This triggers backpack/__init__.py
7. __init__.py → bp_api.py (REMOVED TO BREAK CYCLE)
8. bp_api.py → exchange_api.py (WOULD BE CIRCULAR!)
```

### Current Workaround
- **Removed**: All imports from `__init__.py`:
  - `from .bp_api import BackpackAPI`
  - `from .bp_auth import BackpackEd25519Authenticator`
  - `from .bp_error_mapper import BackpackErrorMapper`
  - `from .bp_rate_limit_strategy import BackpackRateLimitStrategy`
- **Impact**: Package classes cannot be imported from package root
- **Alternative**: Use direct imports from submodules

### Where This Affects Code
1. **ExchangeAPIFactory** - Already uses direct imports ✅
2. **Tests** - May need updates if using package imports
3. **User code** - Must use direct imports

### Example Usage
```python
# ❌ OLD WAY (No longer works)
from cyberdelta.apis.backpack import (
    BackpackAPI,
    BackpackEd25519Authenticator,
    BackpackErrorMapper,
    BackpackRateLimitStrategy
)

# ✅ NEW WAY (Required workaround)
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy
```

### Permanent Solution Required
See `/workflow/websocket_api_circular/` for the complete analysis and proposed plugin-based architecture that will properly fix this issue.

### Timeline
- **Current**: Workaround allows application to run
- **Next Sprint**: Implement proper solution with plugin architecture
- **Technical Debt**: Track as P1 issue
