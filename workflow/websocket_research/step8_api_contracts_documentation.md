# Step 8: WebSocket API Contracts Documentation

**Date**: January 13, 2025
**Status**: COMPLETED
**Phase**: 1 - Assessment and Preparation

## Overview

This document catalogs all public API contracts in the WebSocket module to ensure safe refactoring with minimal breaking changes. The analysis identifies stable interfaces, deprecation candidates, and breaking change risks for the refactoring process.

---

## Public API Surface Analysis

### Module Exports (`cyberdelta/apis/websocket/__init__.py`)

The WebSocket module exposes the following public API:

```python
__all__ = [
    "ExchangeName",                    # Enum from cyberdelta.enums
    "TypeSafeWebSocketProcessor",      # Core processor
    "WebSocketContextProtocol",        # Protocol interface
    "WebSocketContextRegistry",        # Context registry
    "WebSocketMessageContext",         # Message context
    "WebSocketRegistryFactory",        # Registry factory
]
```

---

## Core Public Classes

### 1. **TypeSafeWebSocketProcessor**

**File**: `ws_typed_processor.py`
**Stability**: ✅ Stable - Core processing interface
**Usage**: Primary message processing entry point

#### Public Interface
```python
class TypeSafeWebSocketProcessor:
    def __init__(self, registry: WebSocketContextRegistry) -> None

    def create_typed_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> WebSocketContextProtocol

    def process_message(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> WebSocketContextProtocol
```

**Breaking Change Risk**: 🟡 LOW - Interface is stable, implementation may change
**Refactoring Impact**: Internal optimizations only, public interface preserved

### 2. **WebSocketContextRegistry**

**File**: `ws_context_registry.py`
**Stability**: ⚠️ Moderate - Registry pattern under review
**Usage**: Context type resolution

#### Public Interface
```python
class WebSocketContextRegistry:
    def register_context_creator(
        self,
        exchange: ExchangeName,
        creator: Callable[..., WebSocketContextProtocol],
    ) -> None

    def create_context(
        self,
        exchange: ExchangeName,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> WebSocketContextProtocol

    def get_registered_exchanges(self) -> set[ExchangeName]
```

**Breaking Change Risk**: 🟡 MEDIUM - Registry pattern may be simplified
**Refactoring Impact**: Phase 5 may replace with dependency injection

### 3. **WebSocketRegistryFactory**

**File**: `ws_registry_factory.py`
**Stability**: ⚠️ Under Review - Factory pattern evaluation
**Usage**: Registry creation and configuration

#### Public Interface
```python
class WebSocketRegistryFactory:
    @staticmethod
    def create_registry() -> WebSocketContextRegistry

    @staticmethod
    def create_configured_registry() -> WebSocketContextRegistry

    @staticmethod
    def create_empty_registry() -> WebSocketContextRegistry
```

**Breaking Change Risk**: 🟠 HIGH - Multiple factory methods may be consolidated
**Refactoring Impact**: Phase 2 will remove redundant factory methods

---

## Protocol Interfaces

### 1. **WebSocketContextProtocol**

**File**: `ws_protocols.py`
**Stability**: ✅ Stable - Core protocol definition
**Usage**: Type constraint for contexts

#### Protocol Definition
```python
@runtime_checkable
class WebSocketContextProtocol(Protocol):
    exchange: ExchangeName
    connection_id: str
    message_id: str
    raw_data: dict[str, Any]
    timestamp_ms: int

    def get_routing_key(self) -> str | None
    def get_exchange_name(self) -> ExchangeName
    def validate_message_format(self) -> bool
```

**Breaking Change Risk**: 🟢 VERY LOW - Protocol is stable and well-established
**Refactoring Impact**: No changes planned

### 2. **WebSocketMessageContext**

**File**: `ws_context.py`
**Stability**: ✅ Stable - Core data structure
**Usage**: Concrete context implementation

#### Public Interface
```python
@dataclass
class WebSocketMessageContext:
    exchange: ExchangeName
    connection_id: str
    message_id: str
    raw_data: dict[str, Any]
    timestamp_ms: int

    def get_routing_key(self) -> str | None
    def get_exchange_name(self) -> ExchangeName
    def validate_message_format(self) -> bool
```

**Breaking Change Risk**: 🟢 VERY LOW - Stable data structure
**Refactoring Impact**: Possible internal optimizations only

---

## Extended API Analysis

### Error Handling Interfaces

#### WebSocketStreamErrorHandler
**Stability**: ✅ Stable - Primary error handler
**Public Methods**:
```python
class WebSocketStreamErrorHandler:
    def __init__(
        self,
        config: WebSocketErrorConfig,
        logger: TypedLogger[WebSocketStreamLogData] | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
        recovery_handler: RecoveryHandlerProtocol | None = None,
    ) -> None

    async def handle_stream_error(self, error: WebSocketStreamError) -> None
    async def handle_validation_error(self, error: ValidationError, context: dict[str, Any]) -> None
    def get_metrics(self) -> AggregatedMetrics | None
```

**Breaking Change Risk**: 🟢 VERY LOW - Established interface
**Refactoring Impact**: Internal improvements only

### Router Interfaces

#### BaseWebSocketRouter
**Stability**: ✅ Stable - Base router implementation
**Public Methods**:
```python
class BaseWebSocketRouter[T]:
    def __init__(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        typed_processor: TypeSafeWebSocketProcessor,
        envelope_validator: Callable[[dict[str, Any]], T] | None = None,
    ) -> None

    async def route_message(self, raw_message: dict[str, Any], connection_id: str) -> None
    def register_handler(self, routing_key: str, handler: MessageHandler[T]) -> None
    def unregister_handler(self, routing_key: str) -> None
```

**Breaking Change Risk**: 🟢 VERY LOW - Core routing interface
**Refactoring Impact**: Performance optimizations only

---

## Configuration Interfaces

### WebSocketErrorConfig
**Stability**: ⚠️ Under Review - Configuration consolidation planned
**Breaking Change Risk**: 🟡 MEDIUM - Configuration structure may change
**Refactoring Impact**: Phase 8 configuration consolidation

### MemoryOptimizationConfig
**Stability**: ⚠️ Under Review - Memory config consolidation
**Breaking Change Risk**: 🟡 MEDIUM - May be merged with other configs
**Refactoring Impact**: Phase 8 configuration unification

---

## API Stability Classification

### ✅ **Stable APIs (No Breaking Changes Expected)**
1. **WebSocketContextProtocol** - Core protocol interface
2. **TypeSafeWebSocketProcessor** - Main processing interface
3. **WebSocketMessageContext** - Core data structure
4. **WebSocketStreamErrorHandler** - Primary error handler
5. **BaseWebSocketRouter** - Core routing interface

### ⚠️ **APIs Under Review (Potential Changes)**
1. **WebSocketContextRegistry** - May be simplified in Phase 5
2. **WebSocketRegistryFactory** - Factory methods consolidation in Phase 2
3. **Configuration Classes** - Consolidation in Phase 8

### 🟠 **Deprecated/Removal Candidates**
1. **WebSocketRegistryFactory.create_configured_registry()** - Redundant method
2. **WebSocketRegistryFactory.create_empty_registry()** - Redundant method
3. **BaseErrorHandler** - ✅ Already removed
4. **Unused Exception Classes** - ✅ Already removed (12 classes)

---

## Breaking Change Risk Assessment

### Phase 2: Remove Dead Code (Steps 11-20)
**Risk Level**: 🟡 LOW-MEDIUM
**Potential Breaking Changes**:
- Factory method removal (high probability)
- Unused configuration class removal (low impact)

**Mitigation**: Deprecation warnings before removal

### Phase 3: Consolidate Exception Hierarchy (Steps 21-30)
**Risk Level**: 🟢 LOW
**Potential Breaking Changes**:
- Exception hierarchy changes (internal only)
- Exception import paths (low probability)

**Mitigation**: Backward compatibility imports

### Phase 4: Unify Error Handling (Steps 31-40)
**Risk Level**: 🟡 LOW-MEDIUM
**Potential Breaking Changes**:
- Error handler interface changes (low probability)
- Recovery strategy interfaces (medium probability)

**Mitigation**: Interface adapters for compatibility

### Phase 5: Simplify Registry Pattern (Steps 41-50)
**Risk Level**: 🟠 MEDIUM
**Potential Breaking Changes**:
- Registry factory removal (high probability)
- Registry interface changes (medium probability)

**Mitigation**: Backward compatibility layer, migration tools

### Phase 6-10: Type Safety, Metrics, Configuration, Performance
**Risk Level**: 🟢 LOW
**Potential Breaking Changes**:
- Configuration structure changes (Phase 8)
- Type improvements (additive changes only)

**Mitigation**: Configuration migration tools

---

## API Contract Guarantees

### Compatibility Commitments
1. **Core Interfaces Stable**: WebSocketContextProtocol, TypeSafeWebSocketProcessor
2. **Data Structures Stable**: WebSocketMessageContext, core message formats
3. **Error Handling Stable**: WebSocketStreamErrorHandler interface
4. **Router Interface Stable**: BaseWebSocketRouter public methods

### Deprecation Strategy
1. **Phase Approach**: Gradual deprecation across phases
2. **Warning Period**: Minimum 1 phase before removal
3. **Migration Tools**: Provided for complex changes
4. **Documentation**: Clear migration guides

### Version Compatibility
- **Minor Changes**: Internal optimizations, additive features
- **Major Changes**: Interface changes with compatibility layers
- **Breaking Changes**: Only with migration paths and documentation

---

## Public API Usage Patterns

### Current Usage Analysis
```python
# Most common usage patterns in codebase:

# 1. Processor Creation
processor = TypeSafeWebSocketProcessor(registry)

# 2. Context Creation
context = processor.create_typed_context(raw_data, connection_id)

# 3. Router Setup
router = WebSocketRouter(
    stream_error_handler=error_handler,
    typed_processor=processor,
    envelope_validator=validator,
)

# 4. Registry Configuration
registry = WebSocketRegistryFactory.create_registry()
registry.register_context_creator(ExchangeName.BACKPACK, creator)
```

### Refactoring-Safe Patterns
1. **Dependency Injection**: Preferred over factories
2. **Protocol-Based**: Use protocols instead of concrete classes
3. **Configuration-Driven**: External configuration over hardcoded values
4. **Type-Safe**: Strong typing throughout

---

## Migration Planning

### Phase-by-Phase API Evolution

#### Phase 2: Factory Simplification
```python
# Before (to be deprecated)
registry = WebSocketRegistryFactory.create_configured_registry()

# After (recommended)
registry = WebSocketRegistryFactory.create_registry()
```

#### Phase 5: Registry Simplification
```python
# Before (current)
registry = WebSocketRegistryFactory.create_registry()
processor = TypeSafeWebSocketProcessor(registry)

# After (planned)
processor = TypeSafeWebSocketProcessor(context_creators)
```

#### Phase 8: Configuration Unification
```python
# Before (current)
error_config = WebSocketErrorConfig(...)
memory_config = MemoryOptimizationConfig(...)

# After (planned)
ws_config = UnifiedWebSocketConfig(
    error=error_settings,
    memory=memory_settings,
)
```

---

## API Documentation Requirements

### Required Documentation Updates
1. **Migration Guides**: For each breaking change
2. **Deprecation Notices**: Clear timelines and alternatives
3. **Type Annotations**: Complete type coverage
4. **Usage Examples**: Updated examples for new patterns

### Documentation Maintenance
- Update docstrings for all public methods
- Maintain backward compatibility examples
- Provide performance impact documentation
- Include migration automation tools

---

## Conclusion

The WebSocket module has a well-defined public API with clear stability guarantees. The refactoring plan minimizes breaking changes by:

1. **Preserving Core Interfaces**: Main protocols and processors remain stable
2. **Gradual Deprecation**: Phased removal of redundant components
3. **Migration Support**: Tools and documentation for changes
4. **Compatibility Layers**: Backward compatibility where feasible

**Risk Summary**:
- **Low Risk**: Core processing and error handling interfaces
- **Medium Risk**: Registry and factory patterns (Phase 5)
- **High Risk**: None - all breaking changes are planned and mitigated

The API contract analysis shows that the refactoring can proceed safely with minimal impact on consuming code, while achieving the consolidation and performance goals of the improvement plan.
