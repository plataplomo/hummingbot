# Architecture Diagrams: CyberDelta-Hummingbot Integration

## 1. Current CyberDelta Architecture

```mermaid
graph TB
    subgraph "CyberDelta API Layer"
        API[BackpackAPI]
        API --> AS[AccountService]
        API --> TS[TradingService]
        API --> MS[MarketDataService]

        AS --> ASS[AccountSummaryService]
        AS --> BS[BalanceService]
        AS --> PS[PositionService]

        TS --> OPS[OrderPlacementService]
        TS --> OCS[OrderCancellationService]
        TS --> OQS[OrderQueryService]

        MS --> OBS[OrderBookService]
        MS --> PTS[PriceTickerService]
        MS --> HDS[HistoricalDataService]
    end

    subgraph "Infrastructure Layer"
        HTTP[HttpClient]
        WS[WebSocketManager]
        AUTH[BackpackEd25519Authenticator]
        RL[RateLimitStrategy]
        EM[ErrorMapper]
    end

    subgraph "WebSocket Architecture"
        WSM[WebSocketManager]
        WSM --> WSC[WebSocketContext]
        WSC --> WSR[WebSocketRouter]
        WSR --> WSP[MessageProcessor]
        WSP --> MT[MessageTransformer]

        WSM --> ER[ErrorRecovery]
        ER --> EH[ErrorHandler]
        ER --> RP[RecoveryPolicy]

        WSM --> MM[MemoryManager]
        MM --> MC[MemoryConfig]
    end

    API --> HTTP
    API --> WS
    API --> AUTH
    API --> RL
    API --> EM

    WS --> WSM
```

## 2. Current Hummingbot Architecture

```mermaid
graph TB
    subgraph "Hummingbot Connector"
        EX[ExchangePyBase]
        EX --> OBT[OrderBookTracker]
        EX --> UST[UserStreamTracker]
        EX --> COT[ClientOrderTracker]

        OBT --> OBDS[OrderBookDataSource]
        UST --> USDS[UserStreamDataSource]

        EX --> WAF[WebAssistantsFactory]
        WAF --> AUTH[AuthBase]
        WAF --> THR[AsyncThrottler]
    end

    subgraph "Core Components"
        IFO[InFlightOrder]
        TR[TradingRule]
        OB[OrderBook]
        TS[TimeSynchronizer]
    end

    subgraph "Event System"
        EE[EventEmitter]
        EE --> MOC[MarketOrderCreated]
        EE --> MOF[MarketOrderFilled]
        EE --> MOX[MarketOrderCancelled]
        EE --> BU[BalanceUpdate]
    end

    EX --> IFO
    EX --> TR
    OBT --> OB
    EX --> TS
    EX --> EE
```

## 3. Proposed Integration Architecture

```mermaid
graph TB
    subgraph "Hummingbot Layer"
        HB[BackpackExchange<br/>HummingbotConnector]
        HB --> HBOB[BackpackOrderBookDataSource]
        HB --> HBUS[BackpackUserStreamDataSource]
        HB --> HBAUTH[BackpackAuth]
    end

    subgraph "Adapter Layer"
        WA[WrapperAdapter]
        TA[TypeAdapter<br/>Python 3.13→3.10]
        WSA[WebSocketAdapter]
        SMA[StateManager]
        EMA[EventMapper]
    end

    subgraph "CyberDelta Layer"
        CD[BackpackAPI]
        CD --> CDS[Services]
        CD --> CDWS[WebSocketManager]
        CD --> CDAUTH[Ed25519Auth]
    end

    HB --> WA
    WA --> TA
    WA --> WSA
    WA --> SMA
    WA --> EMA

    TA --> CD
    WSA --> CDWS
    SMA --> CDS
    EMA --> CD

    HBAUTH --> CDAUTH
    HBOB --> WSA
    HBUS --> WSA

    style WA fill:#f9f,stroke:#333,stroke-width:4px
    style TA fill:#f9f,stroke:#333,stroke-width:4px
    style WSA fill:#f9f,stroke:#333,stroke-width:4px
    style SMA fill:#f9f,stroke:#333,stroke-width:4px
    style EMA fill:#f9f,stroke:#333,stroke-width:4px
```

## 4. Order Lifecycle Flow

```mermaid
sequenceDiagram
    participant HS as Hummingbot Strategy
    participant HB as BackpackExchange
    participant WA as WrapperAdapter
    participant CD as CyberDeltaAPI
    participant BP as Backpack Exchange

    HS->>HB: place_order()
    HB->>HB: Generate client_order_id
    HB->>WA: _place_order()

    WA->>WA: Convert types (HB→CD)
    WA->>CD: place_order(PlaceOrderArgs)
    CD->>BP: POST /api/v1/order
    BP-->>CD: {order_id: "12345"}
    CD-->>WA: Order model

    WA->>WA: Map order_id
    WA->>HB: Return exchange_order_id

    HB->>HB: Track InFlightOrder
    HB->>HB: Emit BuyOrderCreated

    Note over BP,CD: WebSocket Updates
    BP-->>CD: Order update (WS)
    CD-->>WA: Process update
    WA->>HB: Update order state
    HB->>HB: Emit OrderFilled
    HB-->>HS: OrderFilledEvent
```

## 5. WebSocket Message Flow

```mermaid
graph LR
    subgraph "Backpack Exchange"
        BWS[WebSocket Server]
    end

    subgraph "CyberDelta WebSocket"
        CDWS[WebSocketManager]
        CDWS --> CTX[WebSocketContext]
        CTX --> RTR[Router]
        RTR --> PROC[Processor]
        PROC --> TRANS[Transformer]
    end

    subgraph "Adapter"
        WSA[WebSocketAdapter]
        MQ[MessageQueue]
        MC[MessageConverter]
    end

    subgraph "Hummingbot"
        USDS[UserStreamDataSource]
        Q[asyncio.Queue]
        EM[EventManager]
    end

    BWS --> CDWS
    TRANS --> WSA
    WSA --> MC
    MC --> MQ
    MQ --> USDS
    USDS --> Q
    Q --> EM
```

## 6. State Synchronization

```mermaid
stateDiagram-v2
    [*] --> Pending: place_order()

    state "Hummingbot States" as HB {
        Pending --> Open: Exchange confirms
        Open --> PartiallyFilled: Partial fill
        Open --> Filled: Complete fill
        Open --> Cancelled: Cancel success
        PartiallyFilled --> Filled: Remaining filled
        PartiallyFilled --> Cancelled: Cancel partial
    }

    state "CyberDelta States" as CD {
        New --> Active: Acknowledged
        Active --> PartialFilled: Fill event
        Active --> Filled: Complete
        Active --> Canceled: Cancel
        PartialFilled --> Filled: Complete
        PartialFilled --> Canceled: Cancel
    }

    state "State Mapper" as SM {
        New --> Pending
        Active --> Open
        PartialFilled --> PartiallyFilled
        Filled --> Filled
        Canceled --> Cancelled
    }
```

## 7. Component Dependencies

```mermaid
graph TD
    subgraph "Direct Dependencies"
        HB[BackpackExchange]
        HB --> CDAPI[CyberDelta.BackpackAPI]
        HB --> HBBASE[Hummingbot.ExchangePyBase]
    end

    subgraph "Indirect Dependencies"
        CDAPI --> CDHTTP[CyberDelta.HttpClient]
        CDAPI --> CDWS[CyberDelta.WebSocketManager]
        CDAPI --> CDAUTH[CyberDelta.Authenticator]

        HBBASE --> HBOT[Hummingbot.OrderTracker]
        HBBASE --> HBUST[Hummingbot.UserStreamTracker]
        HBBASE --> HBTHROT[Hummingbot.Throttler]
    end

    subgraph "Potential Circular Dependencies"
        CDWS -.-> HB
        HBOT -.-> CDAPI
    end

    style CDWS stroke:#f00,stroke-width:2px,stroke-dasharray: 5 5
    style HBOT stroke:#f00,stroke-width:2px,stroke-dasharray: 5 5
```

## 8. Error Handling Flow

```mermaid
flowchart TD
    Start([API Call]) --> CD[CyberDelta API]
    CD --> CDERR{Error?}
    CDERR -->|No| Success([Return Result])
    CDERR -->|Yes| CDEM[CyberDelta ErrorMapper]

    CDEM --> APIERR[APIError with retry_after]
    APIERR --> ADAPTER[Error Adapter]

    ADAPTER --> HBTYPE{Hummingbot Error Type?}

    HBTYPE -->|Order Not Found| ONF[OrderNotFound Exception]
    HBTYPE -->|Rate Limit| RL[RateLimitError]
    HBTYPE -->|Network| NET[NetworkError]
    HBTYPE -->|Other| GEN[Generic Exception]

    ONF --> HBHANDLE[Hummingbot Handler]
    RL --> HBHANDLE
    NET --> HBHANDLE
    GEN --> HBHANDLE

    HBHANDLE --> EVENT{Emit Event?}
    EVENT -->|Yes| EMIT[Emit Failure Event]
    EVENT -->|No| THROW[Re-throw Exception]
```

## 9. Module Import Tree

```mermaid
graph TD
    subgraph "Import Hierarchy"
        M[main.py]
        M --> HB[hummingbot.connector.backpack]
        HB --> BE[BackpackExchange]
        BE --> CDAPI[cyberdelta.apis.backpack.bp_api]

        CDAPI --> EXAPI[cyberdelta.apis.base.exchange_api]
        EXAPI --> CONN[cyberdelta.apis.connectivity]
        CONN --> WS[cyberdelta.apis.websocket]

        WS --> MODELS[cyberdelta.apis.backpack.models]
        MODELS -.-> INIT[backpack.__init__]
        INIT -.-> CDAPI
    end

    style MODELS stroke:#f00,stroke-width:2px
    style INIT stroke:#f00,stroke-width:2px
    style CDAPI stroke:#f00,stroke-width:2px

    Note1[Circular Dependency!] --> MODELS
```

## 10. Performance Impact Analysis

```mermaid
graph LR
    subgraph "Direct Hummingbot Connector"
        A1[Strategy] --> B1[Connector]
        B1 --> C1[Exchange API]

        T1[~50ms latency]
    end

    subgraph "With CyberDelta Wrapper"
        A2[Strategy] --> B2[HB Connector]
        B2 --> C2[Wrapper]
        C2 --> D2[Type Adapter]
        D2 --> E2[CD API]
        E2 --> F2[CD Services]
        F2 --> G2[Exchange API]

        T2[~150-200ms latency]
    end

    Note1[3-4x latency increase] --> T2
```

## Key Architecture Insights

### 1. Layer Complexity
The integration requires a multi-layer adapter architecture to bridge the two systems, adding complexity but maintaining separation of concerns.

### 2. State Management
Dual state machines (Hummingbot's InFlightOrder and CyberDelta's Order model) require careful synchronization.

### 3. WebSocket Adaptation
CyberDelta's sophisticated WebSocket architecture needs simplification for Hummingbot's queue-based approach.

### 4. Circular Dependency Risk
Multiple points where circular dependencies could occur, especially around WebSocket handlers and order tracking.

### 5. Performance Overhead
The wrapper approach adds 3-4 layers of indirection, potentially impacting latency-sensitive operations.

## Recommendations

1. **Minimize Layers**: Bypass CyberDelta services where possible for critical operations
2. **Lazy Loading**: Use lazy imports to prevent circular dependencies
3. **Async Optimization**: Ensure all adapter operations are truly async
4. **Caching**: Cache trading rules, market info to reduce API calls
5. **Direct Paths**: Create direct paths for time-critical operations (order placement/cancellation)
