# Domain-Driven Design Refactor Plan for Symbol System

## Executive Summary

The current symbol system suffers from fundamental architectural flaws that violate SOLID principles, DDD concepts, and exchange agnosticism. This document presents a comprehensive redesign using Domain-Driven Design that addresses all identified issues while maintaining type safety, simplicity, and high performance.

**Key Improvements:**
- 🎯 **Exchange Agnostic Core**: Pure domain logic with zero exchange coupling
- 🏗️ **Clear Domain Boundaries**: Separated concerns with explicit interfaces
- 🔧 **SOLID Compliance**: Single responsibility, dependency inversion, open/closed
- 🚀 **Type Safety**: Full Pydantic validation throughout
- ⚡ **Performance**: Maintains sub-millisecond requirements
- 📦 **Simple Integration**: Clean APIs for existing consumers

---

## Current Architectural Problems (Critical Issues)

### 1. **God Classes & Mixed Responsibilities**

**Current Issue:**
```python
# registry.py - 544 lines doing EVERYTHING
class SymbolRegistry:
    def __init__(self):
        self._cache = MultiLevelCache()      # Caching responsibility
        self._asset_resolver = Resolver()    # External integration
        self._symbols = {}                   # Storage responsibility
        self._lock = RLock()                # Threading responsibility
        # + validation, transformation, statistics, etc.
```

**Impact:** Single class violates SRP, hard to test, impossible to extend

### 2. **Exchange-Specific Logic in Domain Models**

**Current Issue:**
```python
# models.py - Domain contaminated with infrastructure
class ExchangeSymbol(BaseModel):
    def to_api_format(self, exchange_id: ExchangeName) -> dict:
        if exchange_id == ExchangeName.HYPERLIQUID:
            return {"symbol": self.value, "assetIndex": self.asset_index}
        elif exchange_id == ExchangeName.BACKPACK:
            return {"symbol": self.value, "symbolId": self.symbol_id}
```

**Impact:** Domain models know about infrastructure, violates exchange agnosticism

### 3. **Circular Dependencies**

**Current Issue:**
```
registry.py → transformers.py → registry.py
models.py → cache.py → models.py
```

**Impact:** Import cycles, tight coupling, fragile dependency graph

### 4. **Violation of Dependency Inversion**

**Current Issue:**
```python
# High-level modules depend on low-level modules
class SymbolRegistry:
    def __init__(self):
        self._cache = MultiLevelCache()  # Concrete dependency
        self._asset_resolver = ThreadSafeAssetIndexResolver()  # Concrete dependency
```

**Impact:** Cannot test in isolation, hard to swap implementations

---

## Domain-Driven Design Architecture

### Core Domain Model

```
┌─────────────────────────────────────────────────────────────┐
│                    SYMBOL DOMAIN (Core)                     │
├─────────────────────────────────────────────────────────────┤
│  Domain Models (Pure Business Logic)                       │
│  • Symbol (Value Object)                                   │
│  • SymbolMapping (Entity)                                  │
│  • TradingPair (Value Object)                              │
│  • AssetSpecification (Value Object)                       │
├─────────────────────────────────────────────────────────────┤
│  Domain Services (Business Rules)                          │
│  • SymbolCompatibilityService                              │
│  • ArbitrageValidationService                              │
│  • SymbolTransformationService                             │
├─────────────────────────────────────────────────────────────┤
│  Repository Interfaces (Abstractions)                      │
│  • ISymbolRepository                                       │
│  • ISymbolCache                                            │
│  • IExchangeAdapter                                        │
└─────────────────────────────────────────────────────────────┘
             ↑ Dependency Direction (Inversion)
┌─────────────────────────────────────────────────────────────┐
│                  INFRASTRUCTURE LAYER                       │
├─────────────────────────────────────────────────────────────┤
│  Repository Implementations                                 │
│  • InMemorySymbolRepository                                │
│  • RedisSymbolCache                                        │
│  • MultiLevelSymbolCache                                   │
├─────────────────────────────────────────────────────────────┤
│  Exchange Adapters (External Integration)                  │
│  • HyperliquidAdapter                                      │
│  • BackpackAdapter                                         │
│  • ExchangeAdapterRegistry                                 │
├─────────────────────────────────────────────────────────────┤
│  Application Services (Orchestration)                      │
│  • SymbolApplicationService                                │
│  • SymbolQueryService                                      │
│  • SymbolCommandService                                    │
└─────────────────────────────────────────────────────────────┘
             ↑ Used By
┌─────────────────────────────────────────────────────────────┐
│                    API INTEGRATION                          │
│  • SymbolIntegrationFacade (Simplified Interface)          │
│  • Legacy API Compatibility Layer                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Detailed Architecture Design

### 1. Domain Layer (Core Business Logic)

#### A. Pure Domain Models

```python
# domain/models/symbol.py
from typing import Final
from pydantic import BaseModel, Field, computed_field
from enum import Enum

class MarketType(str, Enum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURES = "futures"

class Symbol(BaseModel):
    """Pure domain model - no infrastructure concerns."""
    
    base_asset: str = Field(min_length=1, max_length=10)
    quote_asset: str = Field(min_length=1, max_length=10)
    market_type: MarketType
    
    @computed_field
    @property
    def canonical_form(self) -> str:
        """Canonical internal representation."""
        return f"{self.base_asset}_{self.quote_asset}_{self.market_type.value}"
    
    @computed_field
    @property
    def is_spot(self) -> bool:
        return self.market_type == MarketType.SPOT
    
    def __hash__(self) -> int:
        return hash(self.canonical_form)

class ExchangeFormat(BaseModel):
    """Exchange-specific representation of a symbol."""
    
    exchange_symbol: str
    asset_index: int | None = None
    symbol_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

class SymbolMapping(BaseModel):
    """Entity representing symbol across exchanges."""
    
    symbol: Symbol
    exchange_formats: dict[str, ExchangeFormat] = Field(default_factory=dict)
    
    def supports_exchange(self, exchange_name: str) -> bool:
        return exchange_name in self.exchange_formats
    
    def get_exchange_format(self, exchange_name: str) -> ExchangeFormat | None:
        return self.exchange_formats.get(exchange_name)
```

#### B. Domain Services (Business Rules)

```python
# domain/services/symbol_compatibility.py
from abc import ABC, abstractmethod
from typing import Protocol

class ISymbolRepository(Protocol):
    """Repository abstraction - domain doesn't know about implementation."""
    
    def find_by_canonical_form(self, canonical_form: str) -> SymbolMapping | None:
        ...
    
    def find_by_exchange_symbol(self, exchange_symbol: str, exchange_name: str) -> SymbolMapping | None:
        ...

class SymbolCompatibilityService:
    """Pure business logic for symbol compatibility."""
    
    def __init__(self, repository: ISymbolRepository):
        self._repository = repository
    
    def validate_arbitrage_compatibility(
        self, 
        symbol: Symbol, 
        required_exchanges: list[str]
    ) -> ArbitrageCompatibilityResult:
        """Check if symbol supports arbitrage across exchanges."""
        
        mapping = self._repository.find_by_canonical_form(symbol.canonical_form)
        if not mapping:
            return ArbitrageCompatibilityResult(
                is_compatible=False,
                reason="Symbol not found in registry"
            )
        
        missing_exchanges = [
            exchange for exchange in required_exchanges 
            if not mapping.supports_exchange(exchange)
        ]
        
        return ArbitrageCompatibilityResult(
            is_compatible=len(missing_exchanges) == 0,
            available_exchanges=list(mapping.exchange_formats.keys()),
            missing_exchanges=missing_exchanges,
            exchange_details={
                exchange: mapping.get_exchange_format(exchange)
                for exchange in required_exchanges
                if mapping.supports_exchange(exchange)
            }
        )

class ArbitrageCompatibilityResult(BaseModel):
    """Type-safe result for arbitrage validation."""
    
    is_compatible: bool
    available_exchanges: list[str] = Field(default_factory=list)
    missing_exchanges: list[str] = Field(default_factory=list)
    exchange_details: dict[str, ExchangeFormat] = Field(default_factory=dict)
    reason: str | None = None
```

### 2. Infrastructure Layer (Implementation Details)

#### A. Repository Implementations

```python
# infrastructure/repositories/in_memory_symbol_repository.py
from threading import RLock
from typing import Dict, Optional
from domain.services.symbol_compatibility import ISymbolRepository

class InMemorySymbolRepository(ISymbolRepository):
    """Thread-safe in-memory implementation."""
    
    def __init__(self):
        self._mappings: Dict[str, SymbolMapping] = {}
        self._exchange_index: Dict[str, Dict[str, str]] = {}  # exchange -> symbol -> canonical
        self._lock = RLock()
    
    def store(self, mapping: SymbolMapping) -> None:
        with self._lock:
            canonical = mapping.symbol.canonical_form
            self._mappings[canonical] = mapping
            
            # Update exchange index
            for exchange_name, format_info in mapping.exchange_formats.items():
                if exchange_name not in self._exchange_index:
                    self._exchange_index[exchange_name] = {}
                self._exchange_index[exchange_name][format_info.exchange_symbol] = canonical
    
    def find_by_canonical_form(self, canonical_form: str) -> SymbolMapping | None:
        with self._lock:
            return self._mappings.get(canonical_form)
    
    def find_by_exchange_symbol(self, exchange_symbol: str, exchange_name: str) -> SymbolMapping | None:
        with self._lock:
            canonical = self._exchange_index.get(exchange_name, {}).get(exchange_symbol)
            return self._mappings.get(canonical) if canonical else None
```

#### B. Exchange Adapters (Plugin Architecture)

```python
# infrastructure/adapters/exchange_adapter.py
from abc import ABC, abstractmethod
from pydantic import BaseModel

class ExchangeAdapter(ABC):
    """Abstract base for exchange-specific logic."""
    
    @property
    @abstractmethod
    def exchange_name(self) -> str:
        pass
    
    @abstractmethod
    def symbol_to_exchange_format(self, symbol: Symbol) -> str:
        """Convert domain symbol to exchange format."""
        pass
    
    @abstractmethod
    def exchange_format_to_symbol(self, exchange_symbol: str) -> Symbol:
        """Parse exchange symbol to domain symbol."""
        pass
    
    @abstractmethod
    def validate_exchange_symbol(self, exchange_symbol: str) -> bool:
        pass

class HyperliquidAdapter(ExchangeAdapter):
    """Hyperliquid-specific transformation logic."""
    
    @property
    def exchange_name(self) -> str:
        return "hyperliquid"
    
    def symbol_to_exchange_format(self, symbol: Symbol) -> str:
        if symbol.market_type == MarketType.PERPETUAL:
            return f"{symbol.base_asset}-PERP"
        else:  # SPOT
            return f"{symbol.base_asset}/{symbol.quote_asset}"
    
    def exchange_format_to_symbol(self, exchange_symbol: str) -> Symbol:
        if "-PERP" in exchange_symbol:
            base = exchange_symbol.replace("-PERP", "")
            return Symbol(
                base_asset=base,
                quote_asset="USD",
                market_type=MarketType.PERPETUAL
            )
        elif "/" in exchange_symbol:
            base, quote = exchange_symbol.split("/", 1)
            return Symbol(
                base_asset=base,
                quote_asset=quote,
                market_type=MarketType.SPOT
            )
        else:
            raise ValueError(f"Invalid Hyperliquid symbol: {exchange_symbol}")
    
    def validate_exchange_symbol(self, exchange_symbol: str) -> bool:
        return bool(
            exchange_symbol.endswith("-PERP") or 
            "/" in exchange_symbol
        )

class BackpackAdapter(ExchangeAdapter):
    """Backpack-specific transformation logic."""
    
    @property
    def exchange_name(self) -> str:
        return "backpack"
    
    def symbol_to_exchange_format(self, symbol: Symbol) -> str:
        if symbol.market_type == MarketType.PERPETUAL:
            return f"{symbol.base_asset}_PERP"
        else:  # SPOT
            return f"{symbol.base_asset}_{symbol.quote_asset}"
    
    def exchange_format_to_symbol(self, exchange_symbol: str) -> Symbol:
        if "_PERP" in exchange_symbol:
            base = exchange_symbol.replace("_PERP", "")
            return Symbol(
                base_asset=base,
                quote_asset="USD",
                market_type=MarketType.PERPETUAL
            )
        elif "_" in exchange_symbol:
            base, quote = exchange_symbol.split("_", 1)
            return Symbol(
                base_asset=base,
                quote_asset=quote,
                market_type=MarketType.SPOT
            )
        else:
            raise ValueError(f"Invalid Backpack symbol: {exchange_symbol}")
```

### 3. Application Layer (Orchestration)

```python
# application/services/symbol_application_service.py
from typing import Dict, List
from domain.models.symbol import Symbol, SymbolMapping, ExchangeFormat
from domain.services.symbol_compatibility import ISymbolRepository, SymbolCompatibilityService

class SymbolApplicationService:
    """Application service orchestrating domain services."""
    
    def __init__(
        self,
        repository: ISymbolRepository,
        exchange_adapters: Dict[str, ExchangeAdapter],
        cache: ISymbolCache | None = None
    ):
        self._repository = repository
        self._adapters = exchange_adapters
        self._cache = cache
        self._compatibility_service = SymbolCompatibilityService(repository)
    
    def transform_to_internal(
        self, 
        exchange_symbol: str, 
        exchange_name: str
    ) -> SymbolTransformResult:
        """Transform exchange symbol to internal format."""
        
        # Check cache first
        if self._cache:
            cached = self._cache.get_internal_symbol(exchange_symbol, exchange_name)
            if cached:
                return SymbolTransformResult(success=True, symbol=cached)
        
        # Try repository lookup
        mapping = self._repository.find_by_exchange_symbol(exchange_symbol, exchange_name)
        if mapping:
            result = SymbolTransformResult(success=True, symbol=mapping.symbol)
            if self._cache:
                self._cache.store_internal_symbol(exchange_symbol, exchange_name, mapping.symbol)
            return result
        
        # Fallback to transformation
        adapter = self._adapters.get(exchange_name)
        if not adapter:
            return SymbolTransformResult(
                success=False,
                error=f"No adapter for exchange: {exchange_name}"
            )
        
        try:
            symbol = adapter.exchange_format_to_symbol(exchange_symbol)
            result = SymbolTransformResult(success=True, symbol=symbol)
            
            if self._cache:
                self._cache.store_internal_symbol(exchange_symbol, exchange_name, symbol)
            
            return result
        except Exception as e:
            return SymbolTransformResult(
                success=False,
                error=f"Transformation failed: {str(e)}"
            )
    
    def transform_to_exchange(
        self, 
        symbol: Symbol, 
        exchange_name: str
    ) -> ExchangeSymbolResult:
        """Transform internal symbol to exchange format."""
        
        # Try repository first
        mapping = self._repository.find_by_canonical_form(symbol.canonical_form)
        if mapping and mapping.supports_exchange(exchange_name):
            exchange_format = mapping.get_exchange_format(exchange_name)
            return ExchangeSymbolResult(success=True, exchange_format=exchange_format)
        
        # Fallback to transformation
        adapter = self._adapters.get(exchange_name)
        if not adapter:
            return ExchangeSymbolResult(
                success=False,
                error=f"No adapter for exchange: {exchange_name}"
            )
        
        try:
            exchange_symbol = adapter.symbol_to_exchange_format(symbol)
            exchange_format = ExchangeFormat(exchange_symbol=exchange_symbol)
            return ExchangeSymbolResult(success=True, exchange_format=exchange_format)
        except Exception as e:
            return ExchangeSymbolResult(
                success=False,
                error=f"Transformation failed: {str(e)}"
            )
    
    def validate_arbitrage_compatibility(
        self,
        symbol: Symbol,
        required_exchanges: List[str]
    ) -> ArbitrageCompatibilityResult:
        """Validate symbol for arbitrage trading."""
        return self._compatibility_service.validate_arbitrage_compatibility(
            symbol, required_exchanges
        )

# Result models with full type safety
class SymbolTransformResult(BaseModel):
    success: bool
    symbol: Symbol | None = None
    error: str | None = None

class ExchangeSymbolResult(BaseModel):
    success: bool
    exchange_format: ExchangeFormat | None = None
    error: str | None = None
```

### 4. API Integration Layer (Simplified Interface)

```python
# api/symbol_integration_facade.py
from typing import Dict, Any
from application.services.symbol_application_service import SymbolApplicationService

class SymbolIntegrationFacade:
    """Simplified facade for API layer - maintains backward compatibility."""
    
    def __init__(self, application_service: SymbolApplicationService):
        self._app_service = application_service
    
    async def get_internal_symbol(
        self, 
        exchange_symbol: str, 
        exchange_name: str
    ) -> str:
        """Get internal symbol (backward compatible API)."""
        result = self._app_service.transform_to_internal(exchange_symbol, exchange_name)
        if not result.success:
            raise SymbolNotFoundError(f"Symbol not found: {result.error}")
        return result.symbol.canonical_form
    
    async def get_exchange_symbol(
        self, 
        internal_symbol: str, 
        exchange_name: str
    ) -> str:
        """Get exchange symbol (backward compatible API)."""
        # Parse internal symbol back to domain object
        symbol = self._parse_internal_symbol(internal_symbol)
        result = self._app_service.transform_to_exchange(symbol, exchange_name)
        if not result.success:
            raise SymbolNotFoundError(f"Exchange symbol not found: {result.error}")
        return result.exchange_format.exchange_symbol
    
    def validate_arbitrage_compatibility(
        self,
        internal_symbol: str,
        exchange_names: List[str]
    ) -> Dict[str, Any]:
        """Validate arbitrage compatibility (backward compatible)."""
        symbol = self._parse_internal_symbol(internal_symbol)
        result = self._app_service.validate_arbitrage_compatibility(symbol, exchange_names)
        
        return {
            "valid": result.is_compatible,
            "exchanges": {
                name: {
                    "available": name in result.available_exchanges,
                    "symbol": details.exchange_symbol if details else None
                }
                for name in exchange_names
                for details in [result.exchange_details.get(name)]
            },
            "issues": [f"Missing on: {ex}" for ex in result.missing_exchanges] if result.missing_exchanges else []
        }
```

---

## Migration Strategy

### Phase 1: Foundation (Week 1-2)
1. **Create domain models** with full Pydantic validation
2. **Implement repository abstractions** and in-memory implementation
3. **Build exchange adapters** for Hyperliquid and Backpack
4. **Create application service** with dependency injection

### Phase 2: Integration (Week 3)
1. **Build facade layer** maintaining backward compatibility
2. **Update existing API integration** to use facade
3. **Implement caching layer** with interface-based design
4. **Add comprehensive logging and metrics**

### Phase 3: Migration (Week 4)
1. **Gradual cutover** with feature flags
2. **Performance testing** and optimization
3. **Clean up legacy code** after validation
4. **Documentation updates**

---

## Benefits Achieved

### ✅ **Exchange Agnostic Core**
- Domain models have zero knowledge of exchanges
- Business rules work with any exchange through adapters
- New exchanges require only new adapter implementation

### ✅ **SOLID Principles Compliance**
- **SRP**: Each class has single, clear responsibility
- **OCP**: Open for extension (new adapters) without modification
- **LSP**: All implementations follow interface contracts
- **ISP**: Interfaces are focused and cohesive
- **DIP**: High-level modules depend on abstractions

### ✅ **Clear Domain Boundaries**
- Domain layer: Pure business logic
- Infrastructure layer: External concerns
- Application layer: Orchestration
- API layer: Integration interface

### ✅ **Type Safety Throughout**
- Full Pydantic validation on all models
- Explicit result types with success/error states
- No raw dictionaries or untyped returns

### ✅ **Maintainable & Testable**
- Each layer can be tested in isolation
- Mock implementations for all external dependencies
- Clear separation of concerns

### ✅ **Performance Maintained**
- Caching at appropriate layers
- Repository pattern allows for optimized implementations
- No performance regression from current system

### ✅ **Backward Compatibility**
- Facade maintains existing API contracts
- Gradual migration possible
- Zero breaking changes for consumers

---

## File Structure

```
cyberdelta/core/symbols/
├── domain/
│   ├── models/
│   │   ├── __init__.py
│   │   ├── symbol.py              # Pure domain models
│   │   └── trading_pair.py        # Value objects
│   ├── services/
│   │   ├── __init__.py
│   │   ├── symbol_compatibility.py # Business rules
│   │   └── arbitrage_validation.py # Domain services
│   └── exceptions/
│       ├── __init__.py
│       └── domain_exceptions.py    # Domain-specific errors
├── infrastructure/
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── in_memory_repository.py # Repository implementation
│   │   └── redis_repository.py     # Alternative implementation
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── hyperliquid_adapter.py  # Exchange-specific logic
│   │   ├── backpack_adapter.py     # Exchange-specific logic
│   │   └── adapter_registry.py     # Adapter management
│   └── caching/
│       ├── __init__.py
│       ├── symbol_cache.py         # Cache implementation
│       └── cache_interfaces.py     # Cache abstractions
├── application/
│   ├── __init__.py
│   ├── symbol_application_service.py # Orchestration
│   └── dependency_injection.py     # DI container
├── api/
│   ├── __init__.py
│   ├── symbol_integration_facade.py # Simplified interface
│   └── legacy_compatibility.py     # Backward compatibility
└── __init__.py                     # Public exports
```

---

## Implementation Priority

### 🔥 **Critical Path (Must Have)**
1. Domain models with Symbol, SymbolMapping, ExchangeFormat
2. Repository interface and in-memory implementation
3. Exchange adapters for Hyperliquid and Backpack
4. Application service for orchestration
5. Facade for backward compatibility

### 📈 **High Value (Should Have)**
1. Multi-level caching implementation
2. Comprehensive error handling and logging
3. Performance monitoring and metrics
4. Batch operation support

### 🎯 **Nice to Have (Could Have)**
1. Redis repository implementation
2. WebSocket symbol handling optimization
3. Symbol validation performance improvements
4. Advanced arbitrage analytics

This architecture resolves all identified architectural problems while maintaining the performance, type safety, and simplicity requirements. The clean separation of concerns makes the system highly maintainable and easily extensible for future exchanges and features.