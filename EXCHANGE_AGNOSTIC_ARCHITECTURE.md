# Exchange-Agnostic WebSocket Architecture

## Overview

This document describes the exchange-agnostic WebSocket architecture implemented using Protocol-based typing and method dispatch, eliminating the need for `hasattr`, `isinstance`, or other runtime type checks.

## Core Components

### 1. Protocol Definition (`common/ws_protocols.py`)

The `WebSocketContextProtocol` defines the interface that all WebSocket contexts must implement:

```python
@runtime_checkable
class WebSocketContextProtocol(Protocol):
    # Required attributes
    validated_envelope: BaseModel
    exchange_type: ExchangeType
    symbol: str | None
    coin: str | None
    
    # Exchange-agnostic methods
    def get_transformer_params(self) -> dict[str, str]:
        """Get parameters needed by transformers for this exchange."""
        ...
    
    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter if applicable."""
        ...
    
    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter if applicable."""
        ...
```

### 2. Exchange-Specific Implementations

Each exchange implements the protocol methods according to its needs:

**Backpack** (`backpack/bp_ws_context.py`):
- Uses symbol-based routing
- `get_transformer_params()` returns `{"symbol": "SOL_USDC"}`
- `get_coin_param()` returns `None`

**Hyperliquid** (`hyperliquid/hl_ws_context.py`):
- Uses coin-based routing
- `get_transformer_params()` returns `{"coin": "BTC"}`
- `get_symbol_param()` maps coin to symbol for compatibility

### 3. Type-Safe Transformer Functions

The transformer functions use Protocol methods instead of runtime checks:

```python
def extract_symbol_from_context(context: WebSocketContextProtocol) -> dict[str, str]:
    """Extract symbol parameter using Protocol method."""
    symbol_param = context.get_symbol_param()
    if symbol_param:
        return symbol_param
    raise SymbolNotFoundError
```

## Benefits

### 1. **No Runtime Type Checks**
- No `hasattr()` or `isinstance()` calls
- Type safety enforced at compile time
- Clean, readable code

### 2. **True Exchange Agnosticism**
- Add new exchanges without modifying core code
- Each exchange self-contains its parameter extraction logic
- Core modules depend only on the Protocol interface

### 3. **Pattern Matching Support** (Python 3.10+)
When needed, you can still use pattern matching with exchange types:

```python
match context.exchange_type:
    case ExchangeType.BACKPACK:
        # Backpack-specific logic
    case ExchangeType.HYPERLIQUID:
        # Hyperliquid-specific logic
```

### 4. **Flexible Parameter Extraction**
Three levels of parameter extraction:
- `get_transformer_params()`: All parameters for the exchange
- `get_symbol_param()`: Symbol-specific parameter
- `get_coin_param()`: Coin-specific parameter

## Adding a New Exchange

To add a new exchange (e.g., Binance):

1. **Create Context Class**:
```python
class BinanceMessageContext(WebSocketMessageContext[BinanceRawWebSocketEnvelope]):
    def get_transformer_params(self) -> dict[str, str]:
        # Binance-specific implementation
        return {"pair": self.trading_pair} if self.trading_pair else {}
    
    def get_symbol_param(self) -> dict[str, str] | None:
        # Map to common interface
        return {"symbol": self.trading_pair} if self.trading_pair else None
```

2. **Register in Registry**:
```python
ws_context_registry.register_context_type(
    ExchangeType.BINANCE,
    BinanceMessageContext,
    validate_binance_envelope
)
```

3. **Done!** No core code modifications needed.

## Type Safety Guarantees

The Protocol ensures:
- All contexts implement required methods
- Return types are consistent
- IDEs provide full autocomplete
- Type checkers catch errors at development time

## Conclusion

This architecture provides a clean, type-safe, and truly exchange-agnostic solution that scales well as new exchanges are added. It follows Python best practices and eliminates code smells like runtime type checking in favor of Protocol-based polymorphism.