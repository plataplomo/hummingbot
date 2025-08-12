# APIs Base Module - Architecture Documentation

## Current Architecture Overview

The `@cyberdelta/apis/base/` module serves as the foundation for all exchange API implementations in CyberDeltaEngine. It provides abstract interfaces, base implementations, and common functionality shared across different exchange integrations.

## Component Architecture Diagram

```mermaid
graph TB
    subgraph "External Layers"
        BP[Backpack API]
        HL[Hyperliquid API]
        WS[WebSocket Layer]
    end

    subgraph "Base Module Components"
        subgraph "Core"
            EA[ExchangeAPI<br/>1430 lines]
            AUTH[IAuthenticator<br/>51 lines]
            REG[IComponentRegistry<br/>178 lines]
        end

        subgraph "Rate Limiting"
            RLS[RateLimitStrategy<br/>70 lines]
            RLM[RateLimitModels<br/>29 lines]
            SRLS[SimpleTokenBucketStrategy<br/>94 lines]
            RLB[RateLimitBehavior<br/>36 lines]
        end

        subgraph "Configuration"
            ICD[InfrastructureConfigDomain<br/>859 lines]
            NSD[NetworkSecurityDomain<br/>258 lines]
            TED[TradingExecutionDomain<br/>203 lines]
        end

        subgraph "Validation"
            VC[ValidationContexts<br/>251 lines]
            VP[ValidationPolicies<br/>184 lines]
            VF[ValidationFactories<br/>163 lines]
        end

        subgraph "Protocols"
            BPROT[BaseProtocols<br/>100 lines]
            MPROT[MapperProtocols<br/>665 lines]
        end

        subgraph "Utilities"
            PSS[PayloadSerializationStrategy<br/>78 lines]
            SE[SchemaExport<br/>55 lines]
        end
    end

    subgraph "Dependencies"
        HTTP[HttpClient]
        WSM[WebSocketManager]
        RL[RateLimiter]
        ERR[ErrorMapper]
    end

    BP --> EA
    HL --> EA
    WS --> ICD

    EA --> AUTH
    EA --> RLS
    EA --> ICD
    EA --> PSS
    EA --> HTTP
    EA --> WSM

    RLS --> RLM
    SRLS --> RLS

    VC --> VP
    VF --> VC
    VF --> VP

    style EA fill:#ff9999
    style ICD fill:#ffcc99
    style MPROT fill:#ffcc99
```

## Data Flow Architecture

```mermaid
sequenceDiagram
    participant Client
    participant ExchangeAPI
    participant RateLimiter
    participant Authenticator
    participant HttpClient
    participant ErrorMapper
    participant Exchange

    Client->>ExchangeAPI: API Request
    ExchangeAPI->>RateLimiter: Check Rate Limits
    RateLimiter-->>ExchangeAPI: Acquire Token

    ExchangeAPI->>Authenticator: Sign Request
    Authenticator-->>ExchangeAPI: Signed Components

    ExchangeAPI->>HttpClient: Execute Request
    HttpClient->>Exchange: HTTP Request
    Exchange-->>HttpClient: Response

    alt Error Response
        HttpClient->>ErrorMapper: Map Error
        ErrorMapper-->>ExchangeAPI: APIError
        ExchangeAPI-->>Client: Throw APIError
    else Success Response
        HttpClient-->>ExchangeAPI: Parsed Response
        ExchangeAPI-->>Client: Domain Model
    end
```

## Class Hierarchy

```mermaid
classDiagram
    class ABC {
        <<abstract>>
    }

    class Protocol {
        <<interface>>
    }

    class ExchangeAPI {
        <<abstract>>
        +get_ticker()
        +place_order()
        +cancel_order()
        #_request()
        #_update_rate_limit()
    }

    class IAuthenticator {
        <<abstract>>
        +prepare_request()
    }

    class RateLimitStrategy {
        <<abstract>>
        +prepare_and_acquire()
        +handle_exchange_retry_after()
    }

    class IComponentRegistry {
        <<abstract>>
        +register()
        +get()
        +unregister()
    }

    class BaseComponentRegistry {
        -_components: dict
        +register()
        +get()
        +clear()
    }

    class SimpleTokenBucketStrategy {
        -limiter: TokenBucketRateLimiter
        +prepare_and_acquire()
    }

    class MapperProtocol {
        +parse_decimal_safely()
        +timestamp_ms_to_datetime()
    }

    class RequestBuilderProtocol {
        +build_request()
    }

    class ResponseHandlerProtocol {
        +handle_response()
    }

    ABC <|-- ExchangeAPI
    ABC <|-- IAuthenticator
    ABC <|-- RateLimitStrategy
    ABC <|-- IComponentRegistry

    IComponentRegistry <|-- BaseComponentRegistry
    RateLimitStrategy <|-- SimpleTokenBucketStrategy

    Protocol <|-- MapperProtocol
    Protocol <|-- RequestBuilderProtocol
    Protocol <|-- ResponseHandlerProtocol

    ExchangeAPI o-- IAuthenticator
    ExchangeAPI o-- RateLimitStrategy
    ExchangeAPI o-- HttpClient
    ExchangeAPI o-- WebSocketManager
```

## Module Dependencies

```mermaid
graph LR
    subgraph "External Dependencies"
        AIOHTTP[aiohttp]
        PYDANTIC[pydantic]
        DECIMAL[decimal]
        ASYNCIO[asyncio]
    end

    subgraph "Internal Dependencies"
        COMMON[apis.common]
        CONNECT[apis.connectivity]
        MODELS[models]
        CONFIG[config]
        ENUMS[enums]
        SYMBOLS[symbols]
    end

    subgraph "Base Module"
        BASE[apis.base]
    end

    BASE --> AIOHTTP
    BASE --> PYDANTIC
    BASE --> DECIMAL
    BASE --> ASYNCIO

    BASE --> COMMON
    BASE --> CONNECT
    BASE --> MODELS
    BASE --> CONFIG
    BASE --> ENUMS
    BASE --> SYMBOLS
```

## Configuration Domain Model

```mermaid
graph TD
    subgraph "Performance Configuration"
        PP[PerformanceProfile]
        VM[ValidationMode]
        MS[MemoryStrategy]
        OL[ObservabilityLevel]

        PP --> ULTRA_FAST
        PP --> BALANCED
        PP --> SECURE
        PP --> DEBUG

        VM --> MINIMAL
        VM --> STANDARD
        VM --> STRICT
        VM --> PARANOID
    end

    subgraph "Request Configuration"
        RC[RequestConfiguration]
        RAM[RequestAuthMode]
        SM[SerializationMode]

        RC --> RAM
        RC --> SM

        RAM --> SIGNED
        RAM --> UNSIGNED

        SM --> STANDARD_S[STANDARD]
        SM --> EXPLICIT_NULL
    end

    subgraph "Security Configuration"
        SP[SecurityPolicy]
        STM[SecurityThreatModel]
        ML[MonitoringLevel]
        AL[AuditLevel]

        SP --> STM
        SP --> ML
        SP --> AL

        STM --> DEVELOPMENT
        STM --> STANDARD_T[STANDARD]
        STM --> PARANOID_T[PARANOID]
        STM --> AUDITED
    end
```

## Problems in Current Architecture

### 1. Monolithic ExchangeAPI Class
- 1430 lines in single file
- Mixes multiple responsibilities
- Hard to test individual components
- Violates Single Responsibility Principle

### 2. Configuration Sprawl
- `infrastructure_config_domain.py`: 859 lines
- Contains 20+ enums and 10+ models
- Should be split into focused modules

### 3. Mixed Abstraction Patterns
- Uses both ABC and Protocol
- Inconsistent with project standards
- Creates confusion about which to use

### 4. Weak Type Safety
- Extensive use of `dict[str, Any]`
- Loss of type information at boundaries
- Potential runtime errors

### 5. Circular Dependency Risks
```
validation_contexts -> validation_policies
validation_factories -> validation_contexts
validation_factories -> validation_policies
```

## Proposed Target Architecture

```mermaid
graph TB
    subgraph "Exchange Implementations"
        BP2[Backpack API]
        HL2[Hyperliquid API]
    end

    subgraph "Core Layer"
        subgraph "Contracts"
            PROTO[Exchange Protocol]
            AUTH_P[Auth Protocol]
            RL_P[RateLimit Protocol]
        end

        subgraph "Base Implementation"
            BASE_IMPL[BaseExchange]
            BASE_AUTH[BaseAuthenticator]
            BASE_RL[BaseRateLimiter]
        end
    end

    subgraph "Domain Services"
        subgraph "Trading"
            ORDER[OrderService]
            POSITION[PositionService]
            EXEC[ExecutionService]
        end

        subgraph "Market Data"
            TICKER[TickerService]
            BOOK[OrderBookService]
            HIST[HistoricalService]
        end

        subgraph "Account"
            BAL[BalanceService]
            SUMMARY[SummaryService]
        end
    end

    subgraph "Infrastructure"
        subgraph "Network"
            HTTP2[HttpClient]
            WS2[WebSocketClient]
        end

        subgraph "Security"
            AUTH2[Authenticator]
            RATE2[RateLimiter]
        end

        subgraph "Observability"
            LOG[Logger]
            METRIC[Metrics]
            TRACE[Tracer]
        end
    end

    BP2 --> PROTO
    HL2 --> PROTO

    PROTO --> BASE_IMPL
    AUTH_P --> BASE_AUTH
    RL_P --> BASE_RL

    BASE_IMPL --> ORDER
    BASE_IMPL --> TICKER
    BASE_IMPL --> BAL

    ORDER --> HTTP2
    TICKER --> WS2

    HTTP2 --> AUTH2
    HTTP2 --> RATE2

    style PROTO fill:#99ff99
    style BASE_IMPL fill:#99ccff
```

## Migration Strategy

### Phase 1: Type Safety (Week 1)
1. Replace all `dict[str, Any]` with typed models
2. Convert ABC classes to Protocols
3. Add strict type checking

### Phase 2: Decomposition (Week 2)
1. Split ExchangeAPI into service modules
2. Break down configuration domain
3. Separate validation into own package

### Phase 3: Clean Architecture (Week 3)
1. Implement hexagonal architecture
2. Create clear domain boundaries
3. Add dependency injection

## Performance Considerations

### Current Bottlenecks
1. **Large file parsing**: 1430-line ExchangeAPI slows imports
2. **Deep inheritance**: ABC classes add overhead
3. **Runtime validation**: Excessive Pydantic validation

### Optimization Opportunities
1. **Lazy loading**: Import services on-demand
2. **Protocol efficiency**: Protocols have less overhead than ABC
3. **Caching**: Add strategic caching for market data

## Security Architecture

```mermaid
graph LR
    subgraph "Request Flow"
        REQ[Request] --> VAL[Validation]
        VAL --> AUTH[Authentication]
        AUTH --> RATE[Rate Limit]
        RATE --> SEND[Send]
    end

    subgraph "Response Flow"
        RECV[Receive] --> VERIFY[Verify]
        VERIFY --> PARSE[Parse]
        PARSE --> MAP[Map]
        MAP --> RESP[Response]
    end

    SEND --> RECV

    subgraph "Security Layers"
        SEC1[Input Validation]
        SEC2[HMAC Signing]
        SEC3[Rate Limiting]
        SEC4[Response Validation]
        SEC5[Error Sanitization]
    end

    VAL --> SEC1
    AUTH --> SEC2
    RATE --> SEC3
    VERIFY --> SEC4
    MAP --> SEC5
```

## Testing Architecture

### Current Coverage
- Unit tests: Limited due to monolithic design
- Integration tests: Good coverage
- Performance tests: Missing

### Target Coverage
```
apis/base/
├── tests/
│   ├── unit/
│   │   ├── test_protocols.py
│   │   ├── test_authenticator.py
│   │   ├── test_rate_limiter.py
│   │   └── test_validators.py
│   ├── integration/
│   │   ├── test_exchange_flow.py
│   │   └── test_websocket_flow.py
│   └── performance/
│       ├── test_throughput.py
│       └── test_latency.py
```

## Metrics and Monitoring

### Key Metrics to Track
1. **API Latency**: p50, p95, p99
2. **Rate Limit Usage**: tokens/second
3. **Error Rates**: by error type
4. **WebSocket Health**: connection uptime

### Proposed Monitoring Architecture
```mermaid
graph TD
    API[API Calls] --> COLLECTOR[Metrics Collector]
    WS[WebSocket Events] --> COLLECTOR
    ERRORS[Errors] --> COLLECTOR

    COLLECTOR --> PROMETHEUS[Prometheus]
    COLLECTOR --> LOGS[Structured Logs]

    PROMETHEUS --> GRAFANA[Grafana Dashboards]
    LOGS --> ELK[ELK Stack]

    GRAFANA --> ALERTS[Alerting]
    ELK --> ALERTS
```

## Conclusion

The current architecture of `apis/base` has evolved organically through multiple refactors, resulting in:
- Mixed patterns and responsibilities
- Type safety issues
- Maintenance challenges
- Performance bottlenecks

The proposed architecture addresses these issues by:
- Clear separation of concerns
- Strong type safety
- Protocol-based design
- Domain-driven structure
- Better testability

Implementation should proceed incrementally with careful testing at each phase to minimize risk to the production trading system.
