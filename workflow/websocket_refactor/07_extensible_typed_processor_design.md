# Extensible TypeSafeWebSocketProcessor Design

**Date:** 2025-07-08  
**Status:** Design Proposal  
**Objective:** Make TypeSafeWebSocketProcessor extensible for future exchanges

## 1. Current Design Issues

### 1.1 Tight Coupling
```python
# Current: Everything hardcoded in ws_typed_processor.py
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import HyperliquidRawWebSocketEnvelope

if self.type_guards.is_backpack_message(raw_data):
    return self._create_backpack_context(...)
if self.type_guards.is_hyperliquid_message(raw_data):
    return self._create_hyperliquid_context(...)
```

### 1.2 Scalability Issues
- Adding new exchange requires modifying core `ws_typed_processor.py`
- Violates Open/Closed Principle
- Creates circular dependencies
- All exchange-specific code loaded even if not used

## 2. Proposed Solution: Context Factory Registry

### 2.1 Core Components

#### Base Context Factory Protocol
```python
# cyberdelta/apis/base/ws_context_factory.py
from typing import Protocol, Any
from cyberdelta.apis.base.ws_context import WebSocketContextUnion

class ContextFactory(Protocol):
    """Protocol for exchange-specific context factories."""
    
    def can_handle(self, raw_data: dict[str, Any]) -> bool:
        """Check if this factory can handle the message format."""
        ...
    
    def create_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str,
    ) -> WebSocketContextUnion:
        """Create typed context for this exchange."""
        ...
    
    @property
    def exchange_name(self) -> str:
        """Return the exchange name for logging."""
        ...
```

#### Registry-Based Processor
```python
# cyberdelta/apis/base/ws_typed_processor.py
class TypeSafeWebSocketProcessor:
    """Extensible processor using factory registry."""
    
    def __init__(self) -> None:
        self._factories: list[ContextFactory] = []
        self.type_guards = WebSocketTypeGuards()
    
    def register_factory(self, factory: ContextFactory) -> None:
        """Register a context factory for an exchange."""
        self._factories.append(factory)
        logger.info(f"Registered context factory for {factory.exchange_name}")
    
    def create_typed_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> WebSocketContextUnion:
        """Create context using registered factories."""
        if message_id is None:
            message_id = str(uuid.uuid4())
        
        # Find the first factory that can handle this message
        for factory in self._factories:
            if factory.can_handle(raw_data):
                return factory.create_context(raw_data, connection_id, message_id)
        
        # No factory found
        msg = f"No context factory registered for message format: {list(raw_data.keys())}"
        raise ValueError(msg)

# Global instance
typed_processor = TypeSafeWebSocketProcessor()
```

### 2.2 Exchange-Specific Implementations

#### Backpack Context Factory
```python
# cyberdelta/apis/backpack/bp_context_factory.py
from cyberdelta.apis.base.ws_context_factory import ContextFactory
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.base.ws_context import BackpackMessageContext, ExchangeType

class BackpackContextFactory:
    """Context factory for Backpack WebSocket messages."""
    
    def can_handle(self, raw_data: dict[str, Any]) -> bool:
        """Check if message is from Backpack."""
        return "stream" in raw_data and "data" in raw_data
    
    def create_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str,
    ) -> BackpackMessageContext:
        """Create Backpack-specific context."""
        envelope = BackpackRawWebSocketEnvelope.model_validate(raw_data)
        
        # Extract routing key and symbol
        routing_key = self._extract_routing_key(envelope)
        symbol = self._extract_symbol(envelope)
        
        return BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.BACKPACK,
            routing_key=routing_key,
            timestamp=datetime.now(UTC),
            message_id=message_id,
            connection_id=connection_id,
            symbol=symbol,
        )
    
    @property
    def exchange_name(self) -> str:
        return "Backpack"
```

#### Hyperliquid Context Factory
```python
# cyberdelta/apis/hyperliquid/hl_context_factory.py
from cyberdelta.apis.base.ws_context_factory import ContextFactory
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import HyperliquidRawWebSocketEnvelope
from cyberdelta.apis.base.ws_context import HyperliquidMessageContext, ExchangeType

class HyperliquidContextFactory:
    """Context factory for Hyperliquid WebSocket messages."""
    
    def can_handle(self, raw_data: dict[str, Any]) -> bool:
        """Check if message is from Hyperliquid."""
        return "channel" in raw_data or "method" in raw_data
    
    def create_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str,
    ) -> HyperliquidMessageContext:
        """Create Hyperliquid-specific context."""
        # Handle user events vs regular messages
        if "method" in raw_data and raw_data.get("method") == "user":
            envelope = HyperliquidUserEventEnvelope.model_validate(raw_data)
        else:
            envelope = HyperliquidRawWebSocketEnvelope.model_validate(raw_data)
        
        # Extract routing key and coin
        routing_key = self._extract_routing_key(envelope)
        coin = self._extract_coin(envelope)
        
        return HyperliquidMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.HYPERLIQUID,
            routing_key=routing_key,
            timestamp=datetime.now(UTC),
            message_id=message_id,
            connection_id=connection_id,
            symbol=coin,
        )
    
    @property
    def exchange_name(self) -> str:
        return "Hyperliquid"
```

### 2.3 Registration at Import Time

#### Option 1: Explicit Registration
```python
# cyberdelta/apis/backpack/__init__.py
from cyberdelta.apis.base.ws_typed_processor import typed_processor
from cyberdelta.apis.backpack.bp_context_factory import BackpackContextFactory

# Register factory when module is imported
typed_processor.register_factory(BackpackContextFactory())
```

#### Option 2: Plugin Discovery
```python
# cyberdelta/apis/base/ws_factory_loader.py
import importlib
import pkgutil
from cyberdelta.apis import backpack, hyperliquid

def load_context_factories():
    """Automatically discover and load context factories."""
    for module_info in pkgutil.iter_modules([backpack.__path__[0], hyperliquid.__path__[0]]):
        if module_info.name.endswith('_context_factory'):
            module = importlib.import_module(f'cyberdelta.apis.{module_info.name}')
            # Look for factory class
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if hasattr(attr, 'can_handle') and hasattr(attr, 'create_context'):
                    typed_processor.register_factory(attr())
```

#### Option 3: Decorator Registration
```python
# cyberdelta/apis/base/ws_context_factory.py
_factories: list[type[ContextFactory]] = []

def register_context_factory(factory_class: type[ContextFactory]):
    """Decorator to register a context factory."""
    _factories.append(factory_class)
    return factory_class

# In exchange modules:
@register_context_factory
class BackpackContextFactory:
    ...
```

## 3. Adding a New Exchange (e.g., Binance)

### 3.1 Create Context Model
```python
# cyberdelta/apis/binance/models/bn_context.py
class BinanceMessageContext(WebSocketMessageContext[BinanceRawWebSocketEnvelope]):
    """Binance-specific context with computed fields."""
    
    @computed_field
    @property
    def symbol(self) -> str | None:
        # Extract from Binance format
        return self.validated_envelope.s  # Binance uses 's' for symbol
```

### 3.2 Create Context Factory
```python
# cyberdelta/apis/binance/bn_context_factory.py
@register_context_factory
class BinanceContextFactory:
    def can_handle(self, raw_data: dict[str, Any]) -> bool:
        return "e" in raw_data  # Binance uses 'e' for event type
    
    def create_context(self, raw_data: dict[str, Any], ...) -> BinanceMessageContext:
        # Implementation
```

### 3.3 That's It!
No modifications to core `ws_typed_processor.py` needed. The new exchange is automatically supported.

## 4. Benefits of Registry Pattern

### 4.1 True Extensibility
- Add new exchanges without modifying core code
- Each exchange owns its context creation logic
- No circular dependencies

### 4.2 Separation of Concerns
- Core processor only knows about the protocol
- Exchange-specific logic stays in exchange modules
- Clear boundaries between modules

### 4.3 Performance
- Lazy loading - only load exchange modules when needed
- Could add priority ordering for common exchanges
- Could cache factory lookups

### 4.4 Testing
- Mock individual factories easily
- Test exchange logic in isolation
- Core processor tests don't need exchange knowledge

## 5. Migration Path

### Phase 1: Add Registry Support
1. Keep existing hardcoded logic
2. Add registry pattern alongside
3. Gradually migrate exchanges

### Phase 2: Migrate Exchanges
1. Create BackpackContextFactory
2. Create HyperliquidContextFactory
3. Register both factories
4. Remove hardcoded logic

### Phase 3: Documentation
1. Document factory protocol
2. Create template for new exchanges
3. Add examples

## 6. Advanced Features

### 6.1 Factory Priority
```python
class ContextFactory(Protocol):
    @property
    def priority(self) -> int:
        """Higher priority factories are checked first."""
        return 0

# Sort factories by priority on registration
self._factories.sort(key=lambda f: f.priority, reverse=True)
```

### 6.2 Caching
```python
class TypeSafeWebSocketProcessor:
    def __init__(self):
        self._factory_cache: dict[frozenset, ContextFactory] = {}
    
    def create_typed_context(self, raw_data: dict[str, Any], ...):
        # Cache based on message keys
        cache_key = frozenset(raw_data.keys())
        if cache_key in self._factory_cache:
            factory = self._factory_cache[cache_key]
            return factory.create_context(raw_data, ...)
```

### 6.3 Metrics per Exchange
```python
class ContextFactory(Protocol):
    def create_context(self, ...) -> WebSocketContextUnion:
        start_time = time.perf_counter()
        context = self._create_context_internal(...)
        
        metrics.record_context_creation(
            exchange=self.exchange_name,
            duration_ms=(time.perf_counter() - start_time) * 1000
        )
        return context
```

## 7. Conclusion

The registry pattern provides true extensibility by:
1. **Decoupling** exchange logic from core processor
2. **Enabling** plugin-style architecture
3. **Maintaining** type safety throughout
4. **Simplifying** addition of new exchanges

This design follows SOLID principles and makes the WebSocket infrastructure truly extensible for future growth.