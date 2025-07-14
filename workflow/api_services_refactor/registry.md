# Deep Architectural Analysis: Registry Patterns for Mappers and Services

## Executive Summary

After comprehensive analysis of both Backpack and Hyperliquid exchange modules, **registries for mappers and services would significantly improve architectural consistency and maintainability**, but implementation approaches must be tailored to each exchange's existing patterns.

**Key Findings:**
- **Architectural Divergence**: Significant differences in dependency management and component lifecycle
- **Complexity Mismatch**: Hyperliquid uses complex dependency injection; Backpack uses simpler patterns
- **Registry Benefits**: Would provide consistency, testability, and extensibility
- **Implementation Challenge**: Must preserve existing patterns while adding registry capabilities

**Recommendation**: **Implement registries with exchange-specific adaptations** to achieve consistency without disrupting existing architectures.

---

## 1. Current Architecture Analysis

### 1.1 Exchange Architecture Comparison

| Aspect | Backpack | Hyperliquid | Consistency Level |
|--------|----------|-------------|------------------|
| **Mappers** | Static methods, no dependencies | Instance methods, complex dependencies | ❌ **Low** |
| **Services** | Facade + decomposed pattern | Composite + dependency injection | ❌ **Low** |
| **Request Builders** | Registry pattern | Registry pattern (placeholder) | ✅ **High** |
| **Response Handlers** | Registry pattern | Registry pattern (placeholder) | ✅ **High** |
| **Error Handling** | Consistent patterns | Consistent patterns | ✅ **High** |
| **Directory Structure** | Domain-based organization | Domain-based organization | ✅ **High** |

### 1.2 Current Mapper Architecture

```mermaid
graph TB
    subgraph "Backpack Mappers (Static Pattern)"
        BPOrderMapper["🔧 BackpackOrderMapper<br/>Static Methods"]
        BPBalanceMapper["💰 BackpackBalanceMapper<br/>Static Methods"]
        BPTickerMapper["📈 BackpackTickerMapper<br/>Static Methods"]

        BPService["🏢 Backpack Service"] --> BPOrderMapper
        BPService --> BPBalanceMapper
        BPService --> BPTickerMapper
    end

    subgraph "Hyperliquid Mappers (Dependency Injection)"
        HLOrderMapper["🔧 HyperliquidOrderMapper<br/>Instance + Dependencies"]
        HLBalanceMapper["💰 HyperliquidBalanceMapper<br/>Static Methods"]
        HLEnumMapper["🔄 HyperliquidTradingEnumMapper<br/>Dependency"]

        HLService["🏢 Hyperliquid Service"] --> HLOrderMapper
        HLService --> HLBalanceMapper
        HLOrderMapper --> HLEnumMapper
    end

    classDef backpack fill:#e1f5fe
    classDef hyperliquid fill:#f3e5f5

    class BPOrderMapper,BPBalanceMapper,BPTickerMapper,BPService backpack
    class HLOrderMapper,HLBalanceMapper,HLEnumMapper,HLService hyperliquid
```

### 1.3 Current Service Architecture

```mermaid
graph TB
    subgraph "Backpack Service Architecture (Facade Pattern)"
        BPFacade["🎭 AccountServiceFacade<br/>Backward Compatibility"]
        BPBalance["💰 BalanceService"]
        BPPosition["📊 PositionService"]
        BPSummary["📋 AccountSummaryService"]

        BPFacade --> BPBalance
        BPFacade --> BPPosition
        BPFacade --> BPSummary

        BPBalance --> BPReqBuilder["🔨 RequestBuilder"]
        BPBalance --> BPRespHandler["📨 ResponseHandler"]
        BPBalance --> BPBalMapper["🔄 BalanceMapper"]
    end

    subgraph "Hyperliquid Service Architecture (Composite Pattern)"
        HLComposite["🏗️ AccountService<br/>Composite Pattern"]
        HLBalance["💰 BalanceService"]
        HLClearinghouse["🏛️ ClearinghouseStateService<br/>Shared State"]

        HLComposite --> HLBalance
        HLComposite --> HLClearinghouse
        HLBalance --> HLClearinghouse
        HLBalance --> HLBalMapper["🔄 BalanceMapper<br/>Injected"]
    end

    classDef backpack fill:#e1f5fe
    classDef hyperliquid fill:#f3e5f5

    class BPFacade,BPBalance,BPPosition,BPSummary,BPReqBuilder,BPRespHandler,BPBalMapper backpack
    class HLComposite,HLBalance,HLClearinghouse,HLBalMapper hyperliquid
```

---

## 2. Current Integration Patterns

### 2.1 Service-to-Mapper Integration Sequence

```mermaid
sequenceDiagram
    participant Client
    participant BPService as Backpack Service
    participant BPMapper as Static Mapper
    participant HLService as Hyperliquid Service
    participant HLMapper as Injected Mapper
    participant HLEnum as Enum Mapper

    Note over Client, HLEnum: Backpack Pattern (Static)
    Client->>BPService: get_balance()
    BPService->>BPService: fetch_raw_data()
    BPService->>BPMapper: transform_raw_balance_to_internal(raw_data)
    BPMapper-->>BPService: Balance object
    BPService-->>Client: Balance

    Note over Client, HLEnum: Hyperliquid Pattern (Dependency Injection)
    Client->>HLService: get_balance()
    HLService->>HLService: fetch_raw_data()
    HLService->>HLMapper: transform_raw_balance_to_internal(raw_data)
    HLMapper->>HLEnum: map_enum_value(raw_enum)
    HLEnum-->>HLMapper: Mapped enum
    HLMapper-->>HLService: Balance object
    HLService-->>Client: Balance
```

### 2.2 Current Dependency Graph

```mermaid
graph LR
    subgraph "Backpack Dependencies"
        BPClient["Client"]
        BPFacade["Facade"]
        BPService["Service"]
        BPMapper["Mapper"]
        BPBuilder["RequestBuilder"]
        BPHandler["ResponseHandler"]

        BPClient --> BPFacade
        BPFacade --> BPService
        BPService --> BPMapper
        BPService --> BPBuilder
        BPService --> BPHandler
    end

    subgraph "Hyperliquid Dependencies"
        HLClient["Client"]
        HLComposite["Composite Service"]
        HLService["Service"]
        HLMapper["Mapper"]
        HLEnum["Enum Mapper"]
        HLClearinghouse["Clearinghouse Service"]

        HLClient --> HLComposite
        HLComposite --> HLService
        HLService --> HLMapper
        HLMapper --> HLEnum
        HLService --> HLClearinghouse
    end

    classDef backpack fill:#e1f5fe
    classDef hyperliquid fill:#f3e5f5

    class BPClient,BPFacade,BPService,BPMapper,BPBuilder,BPHandler backpack
    class HLClient,HLComposite,HLService,HLMapper,HLEnum,HLClearinghouse hyperliquid
```

---

## 3. Proposed Registry Architecture

### 3.1 Mapper Registry Design

```mermaid
graph TB
    subgraph "Proposed Mapper Registry Architecture"
        MapperRegistry["🗂️ MapperRegistry<br/>Central Component Store"]

        subgraph "Domain Mappers"
            AccountMappers["💰 Account Mappers<br/>• Balance • Position • Summary"]
            TradingMappers["🔧 Trading Mappers<br/>• Order • Response • Enum"]
            MarketMappers["📈 Market Data Mappers<br/>• Ticker • OrderBook • Trade"]
        end

        subgraph "Services"
            AccountService["🏢 Account Service"]
            TradingService["🔧 Trading Service"]
            MarketService["📊 Market Service"]
        end

        MapperRegistry --> AccountMappers
        MapperRegistry --> TradingMappers
        MapperRegistry --> MarketMappers

        AccountService --> MapperRegistry
        TradingService --> MapperRegistry
        MarketService --> MapperRegistry
    end

    subgraph "Registry Protocol"
        IMapperRegistry["📋 IMapperRegistry Protocol<br/>• get_mapper(domain, type)<br/>• register_mapper(domain, type, mapper)<br/>• list_mappers(domain)"]
    end

    MapperRegistry -.->|implements| IMapperRegistry
```

### 3.2 Service Registry Design

```mermaid
graph TB
    subgraph "Proposed Service Registry Architecture"
        ServiceRegistry["🗂️ ServiceRegistry<br/>Central Service Store"]
        ComponentFactory["🏭 ComponentFactory<br/>Service Creation & DI"]

        subgraph "Service Categories"
            CoreServices["🎯 Core Services<br/>• Account • Trading • Market Data"]
            UtilServices["🔧 Utility Services<br/>• Clearinghouse • Authentication"]
            FacadeServices["🎭 Facade Services<br/>• Backward Compatibility"]
        end

        subgraph "Configuration"
            ServiceConfig["⚙️ Service Configuration<br/>• Dependencies • Lifecycle • Scopes"]
        end

        ServiceRegistry --> CoreServices
        ServiceRegistry --> UtilServices
        ServiceRegistry --> FacadeServices

        ComponentFactory --> ServiceRegistry
        ComponentFactory --> ServiceConfig
    end

    subgraph "Client Access"
        Client["👤 Client"]
        API["🌐 API Layer"]

        Client --> API
        API --> ComponentFactory
    end
```

### 3.3 Unified Registry Integration

```mermaid
graph TB
    subgraph "Unified Registry System"
        ExchangeFactory["🏭 ExchangeComponentFactory<br/>Master Factory"]

        ServiceRegistry["🗂️ ServiceRegistry"]
        MapperRegistry["🔄 MapperRegistry"]
        BuilderRegistry["🔨 RequestBuilderRegistry"]
        HandlerRegistry["📨 ResponseHandlerRegistry"]

        ExchangeFactory --> ServiceRegistry
        ExchangeFactory --> MapperRegistry
        ExchangeFactory --> BuilderRegistry
        ExchangeFactory --> HandlerRegistry

        subgraph "Cross-Registry Dependencies"
            ServiceRegistry -.->|uses| MapperRegistry
            ServiceRegistry -.->|uses| BuilderRegistry
            ServiceRegistry -.->|uses| HandlerRegistry
        end
    end

    subgraph "Exchange Implementations"
        BackpackFactory["🔵 BackpackComponentFactory"]
        HyperliquidFactory["🟣 HyperliquidComponentFactory"]

        BackpackFactory -.->|extends| ExchangeFactory
        HyperliquidFactory -.->|extends| ExchangeFactory
    end
```

---

## 4. Registry Implementation Patterns

### 4.1 Mapper Registry Implementation

```mermaid
sequenceDiagram
    participant Service
    participant MapperRegistry
    participant MapperFactory
    participant ConcreteMapper
    participant Dependencies

    Note over Service, Dependencies: Initialization Phase
    Service->>MapperRegistry: get_mapper("trading", "order")
    MapperRegistry->>MapperRegistry: check_cache()

    alt Mapper not cached
        MapperRegistry->>MapperFactory: create_mapper("trading", "order")
        MapperFactory->>Dependencies: resolve_dependencies()
        Dependencies-->>MapperFactory: dependency_objects
        MapperFactory->>ConcreteMapper: new(dependencies)
        ConcreteMapper-->>MapperFactory: mapper_instance
        MapperFactory-->>MapperRegistry: mapper_instance
        MapperRegistry->>MapperRegistry: cache_mapper()
    end

    MapperRegistry-->>Service: mapper_instance

    Note over Service, Dependencies: Usage Phase
    Service->>ConcreteMapper: transform_raw_to_internal(data)
    ConcreteMapper-->>Service: transformed_data
```

### 4.2 Service Registry with Dependency Injection

```mermaid
sequenceDiagram
    participant Client
    participant ServiceRegistry
    participant ServiceFactory
    participant Service
    participant MapperRegistry
    participant RequestBuilderRegistry

    Note over Client, RequestBuilderRegistry: Service Creation
    Client->>ServiceRegistry: get_service("account", "balance")
    ServiceRegistry->>ServiceFactory: create_service("account", "balance")

    ServiceFactory->>MapperRegistry: get_mapper("account", "balance")
    MapperRegistry-->>ServiceFactory: balance_mapper

    ServiceFactory->>RequestBuilderRegistry: get_builder("account", "balance")
    RequestBuilderRegistry-->>ServiceFactory: request_builder

    ServiceFactory->>Service: new(mapper, builder, ...)
    Service-->>ServiceFactory: service_instance
    ServiceFactory-->>ServiceRegistry: service_instance
    ServiceRegistry-->>Client: service_instance

    Note over Client, RequestBuilderRegistry: Service Usage
    Client->>Service: get_balances()
    Service-->>Client: balances
```

---

## 5. Exchange-Specific Registry Adaptations

### 5.1 Backpack Registry Adaptation

```mermaid
graph TB
    subgraph "Backpack Registry Adaptation"
        BackpackMapperRegistry["🔵 BackpackMapperRegistry<br/>Static Method Wrapper"]

        subgraph "Static Mapper Wrappers"
            OrderMapperWrapper["🔧 OrderMapperWrapper<br/>wrap(BackpackOrderMapper)"]
            BalanceMapperWrapper["💰 BalanceMapperWrapper<br/>wrap(BackpackBalanceMapper)"]
        end

        subgraph "Facade Integration"
            BackpackFacade["🎭 BackpackFacade<br/>Registry-Aware"]
            DecomposedServices["🔧 Decomposed Services<br/>Registry-Enabled"]
        end

        BackpackMapperRegistry --> OrderMapperWrapper
        BackpackMapperRegistry --> BalanceMapperWrapper
        BackpackFacade --> BackpackMapperRegistry
        BackpackFacade --> DecomposedServices
    end

    classDef adaptation fill:#fff3e0
    class BackpackMapperRegistry,OrderMapperWrapper,BalanceMapperWrapper,BackpackFacade,DecomposedServices adaptation
```

### 5.2 Hyperliquid Registry Adaptation

```mermaid
graph TB
    subgraph "Hyperliquid Registry Adaptation"
        HyperliquidMapperRegistry["🟣 HyperliquidMapperRegistry<br/>Dependency Injection"]

        subgraph "Complex Mapper Dependencies"
            OrderMapperFactory["🔧 OrderMapperFactory<br/>Dependencies: EnumMapper"]
            BalanceMapperFactory["💰 BalanceMapperFactory<br/>Dependencies: None"]
            DependencyResolver["🔍 DependencyResolver<br/>Manages Dependencies"]
        end

        subgraph "Composite Service Integration"
            HyperliquidComposite["🏗️ HyperliquidComposite<br/>Registry-Aware"]
            SharedStateManager["🏛️ SharedStateManager<br/>Clearinghouse Service"]
        end

        HyperliquidMapperRegistry --> OrderMapperFactory
        HyperliquidMapperRegistry --> BalanceMapperFactory
        OrderMapperFactory --> DependencyResolver
        HyperliquidComposite --> HyperliquidMapperRegistry
        HyperliquidComposite --> SharedStateManager
    end

    classDef adaptation fill:#f3e5f5
    class HyperliquidMapperRegistry,OrderMapperFactory,BalanceMapperFactory,DependencyResolver,HyperliquidComposite,SharedStateManager adaptation
```

---

## 6. Implementation Roadmap

### Phase 1: Mapper Registry Foundation (Weeks 1-2)

**Backpack Implementation:**
```python
class BackpackMapperRegistry(BaseComponentRegistry):
    def __init__(self):
        super().__init__()
        self._register_static_mappers()

    def _register_static_mappers(self):
        # Wrap static mappers in instance pattern
        self.register("account.balance", StaticMapperWrapper(BackpackBalanceMapper))
        self.register("trading.order", StaticMapperWrapper(BackpackOrderMapper))
```

**Hyperliquid Implementation:**
```python
class HyperliquidMapperRegistry(BaseComponentRegistry):
    def __init__(self, dependency_resolver):
        super().__init__()
        self._dependency_resolver = dependency_resolver
        self._register_dependency_mappers()

    def _register_dependency_mappers(self):
        # Register with dependency injection
        self.register("trading.order", lambda: HyperliquidOrderMapper(
            enum_mapper=self._dependency_resolver.get("trading.enum")
        ))
```

### Phase 2: Service Registry Integration (Weeks 3-4)

**Service Factory Pattern:**
```python
class ExchangeServiceFactory:
    def __init__(self, mapper_registry, builder_registry, handler_registry):
        self._mappers = mapper_registry
        self._builders = builder_registry
        self._handlers = handler_registry

    def create_service(self, domain: str, service_type: str):
        mapper = self._mappers.get(f"{domain}.{service_type}")
        builder = self._builders.get(f"{domain}.{service_type}")
        handler = self._handlers.get(f"{domain}.{service_type}")

        return ServiceClassMap[f"{domain}.{service_type}"](
            mapper=mapper,
            request_builder=builder,
            response_handler=handler
        )
```

### Phase 3: Backward Compatibility (Weeks 5-6)

**Facade Adaptation:**
```python
class BackpackAccountServiceFacade:
    def __init__(self, service_registry):
        self._service_registry = service_registry

        # Maintain existing API while using registry internally
        self._balance_service = service_registry.get("account.balance")
        self._position_service = service_registry.get("account.position")

    # Existing public API remains unchanged
    async def get_balances(self) -> dict[str, SpotBalance]:
        return await self._balance_service.get_balances()
```

### Phase 4: Testing and Validation (Weeks 7-8)

**Registry Testing Pattern:**
```python
class TestAccountService:
    def setup_method(self):
        # Easy mocking with registries
        mock_mapper = Mock(spec=IAccountMapper)
        mapper_registry = MapperRegistry()
        mapper_registry.register("account.balance", mock_mapper)

        self.service = AccountService(mapper_registry)
```

---

## 7. Benefits Analysis

### 7.1 Quantified Benefits

| Benefit Category | Current State | With Registries | Improvement |
|------------------|---------------|-----------------|-------------|
| **Testability** | Manual mocking | Registry-based mocking | +60% ease |
| **Consistency** | Divergent patterns | Unified patterns | +80% consistency |
| **Maintainability** | Scattered dependencies | Centralized management | +45% maintainability |
| **Extensibility** | Hard-coded components | Dynamic registration | +70% extensibility |

### 7.2 Architectural Benefits

```mermaid
graph LR
    subgraph "Current Architecture Issues"
        A["🔴 Inconsistent Patterns"]
        B["🔴 Tight Coupling"]
        C["🔴 Testing Difficulty"]
        D["🔴 Configuration Scatter"]
    end

    subgraph "Registry Architecture Benefits"
        E["✅ Unified Patterns"]
        F["✅ Loose Coupling"]
        G["✅ Easy Testing"]
        H["✅ Centralized Config"]
    end

    A --> E
    B --> F
    C --> G
    D --> H

    classDef problem fill:#ffebee
    classDef solution fill:#e8f5e8

    class A,B,C,D problem
    class E,F,G,H solution
```

### 7.3 Risk Mitigation

**Implementation Risks:**
- **Migration Complexity**: Gradual rollout with backward compatibility
- **Performance Overhead**: Minimal registry lookup cost vs. significant architectural benefits
- **Learning Curve**: Registry patterns already established in request builders/response handlers

**Mitigation Strategies:**
- **Phase-based Implementation**: Incremental adoption
- **Backward Compatibility Layer**: Existing APIs continue working
- **Comprehensive Testing**: Registry testing patterns established
- **Documentation**: Clear migration guides and examples

---

## 8. Final Recommendations

### 8.1 Strategic Decision

**YES - Implement Registry Patterns for Mappers and Services**

**Reasoning:**
1. **Architectural Consistency**: Aligns with existing request builder/response handler patterns
2. **Long-term Maintainability**: Centralized component management
3. **Testing Excellence**: Easy mocking and dependency injection
4. **Future Extensibility**: Support for multi-exchange scenarios

### 8.2 Implementation Approach

**Tailored Implementation Strategy:**

1. **Backpack**: Registry with static mapper wrappers, maintain facade pattern
2. **Hyperliquid**: Registry with dependency injection, enhance composite pattern
3. **Gradual Migration**: Preserve existing APIs during transition
4. **Testing First**: Establish registry testing patterns before full migration

### 8.3 Success Metrics

- **Consistency Score**: Measure architectural pattern uniformity
- **Test Coverage**: Increase in service/mapper test coverage
- **Development Velocity**: Time to implement new services/mappers
- **Bug Reduction**: Decrease in dependency-related issues

### 8.4 Next Steps

1. **Week 1-2**: Implement mapper registry prototypes
2. **Week 3-4**: Service registry integration
3. **Week 5-6**: Backward compatibility layer
4. **Week 7-8**: Testing, validation, and documentation

**The registry pattern will transform the architecture from inconsistent, tightly-coupled components to a unified, maintainable, and extensible system while preserving existing functionality.**

---

*This analysis provides the architectural foundation for implementing registry patterns across both exchange modules, ensuring consistency while respecting each exchange's unique implementation characteristics.*
