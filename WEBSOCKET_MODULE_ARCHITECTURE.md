# WebSocket Module Architecture

## Overview

We've successfully refactored the WebSocket infrastructure into its own dedicated module, creating a cleaner and more maintainable architecture.

## Directory Structure

```
cyberdelta/apis/
├── base/          # General API infrastructure (16 files)
│   ├── authenticator_interface.py
│   ├── exchange_api.py
│   ├── rate_limit_*.py
│   ├── validation_*.py
│   └── ... (other general API files)
│
├── common/        # Shared types and errors (6 files)
│   ├── api_error.py
│   ├── api_error_codes.py
│   ├── api_error_response.py
│   ├── error_mapper_interface.py
│   └── types.py
│
├── websocket/     # WebSocket infrastructure (30+ files)
│   ├── ws_context.py
│   ├── ws_protocols.py
│   ├── ws_context_registry.py
│   ├── ws_router.py
│   ├── ws_processor.py
│   └── ... (all ws_*.py files)
│
├── backpack/      # Backpack exchange implementation
│   ├── bp_api.py
│   ├── bp_ws_context.py
│   ├── bp_ws_router.py
│   └── ...
│
└── hyperliquid/   # Hyperliquid exchange implementation
    ├── hl_api.py
    ├── hl_ws_context.py
    ├── hl_ws_router.py
    └── ...
```

## Benefits

### 1. **Clear Separation of Concerns**
- `base/`: General API infrastructure (authentication, rate limiting, validation)
- `websocket/`: All WebSocket-specific infrastructure
- `common/`: Shared types used across modules
- Exchange folders: Implementation-specific code

### 2. **No Circular Dependencies**
- WebSocket module is parallel to base, not nested within it
- Exchange implementations depend on websocket module
- Clear dependency flow: exchanges → websocket → base

### 3. **Better Discoverability**
- All WebSocket code in one place
- Easy to find related functionality
- Clear module boundaries

### 4. **Scalability**
- Can add WebSocket sub-modules if needed
- Easy to add new exchanges
- Protocol-based design supports extensibility

## Import Examples

### Before (mixed in base):
```python
from cyberdelta.apis.base.ws_context import WebSocketMessageContext
from cyberdelta.apis.base.ws_router import BaseWebSocketRouter
from cyberdelta.apis.base.exchange_api import ExchangeAPI
```

### After (clear separation):
```python
from cyberdelta.apis.websocket import WebSocketMessageContext, BaseWebSocketRouter
from cyberdelta.apis.base.exchange_api import ExchangeAPI
```

## Key Components

### WebSocket Module (`apis/websocket/`)
- **Core**: `ws_context.py`, `ws_router.py`, `ws_processor.py`
- **Protocols**: `ws_protocols.py` - defines interfaces
- **Registry**: `ws_context_registry.py` - manages exchange contexts
- **Type Safety**: `ws_typed_processor.py`, `ws_type_guards.py`
- **Performance**: `ws_performance.py`, `ws_memory_optimized.py`
- **Error Handling**: `ws_error_handler.py`, `ws_error_recovery.py`

### Base Module (`apis/base/`)
Now contains only general API infrastructure:
- Authentication interfaces
- Rate limiting strategies
- Validation policies
- General exchange API base class

### Common Module (`apis/common/`)
Minimal shared types:
- Error types and codes
- Type aliases (`MessageHandler`, `WebSocketContextUnion`)

## Migration Impact

All imports have been updated automatically. The refactoring is transparent to exchange implementations - they just import from `cyberdelta.apis.websocket` instead of `cyberdelta.apis.base`.

## Conclusion

This refactoring provides a much cleaner architecture that:
1. Eliminates the risk of circular dependencies
2. Makes the codebase more intuitive to navigate
3. Scales better as we add more exchanges
4. Maintains clear module boundaries

The WebSocket infrastructure is now properly isolated and can evolve independently of the general API infrastructure.