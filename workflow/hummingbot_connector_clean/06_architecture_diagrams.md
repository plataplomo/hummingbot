# Architecture Diagrams - Backpack Hummingbot Connector

## Overview
This document provides visual representations of the Backpack connector architecture, showing data flow, component relationships, and integration patterns.

## 1. High-Level Architecture

```mermaid
graph TB
    subgraph "Hummingbot Core"
        HB[Hummingbot Strategy]
        OBT[OrderBookTracker]
        UST[UserStreamTracker]
        COT[ClientOrderTracker]
    end

    subgraph "Backpack Connector"
        BE[BackpackExchange]
        BA[BackpackAuth]
        OBDS[OrderBookDataSource]
        USDS[UserStreamDataSource]
        BC[BackpackConstants]
        BU[BackpackUtils]
        BWU[BackpackWebUtils]
    end

    subgraph "External"
        REST[Backpack REST API]
        WSP[Public WebSocket]
        WSV[Private WebSocket]
    end

    HB --> BE
    BE --> COT
    BE --> BA
    BE --> BC
    BE --> BU
    BE --> BWU

    OBT --> OBDS
    UST --> USDS

    BA --> REST
    BA --> WSV

    OBDS --> WSP
    OBDS --> REST

    USDS --> WSV
    USDS --> BA

    BE --> REST
```

## 2. Component Interaction Flow

```mermaid
sequenceDiagram
    participant S as Strategy
    participant E as BackpackExchange
    participant A as BackpackAuth
    participant W as WebAssistant
    participant API as Backpack API

    S->>E: place_order()
    E->>E: Create InFlightOrder
    E->>A: Generate signature
    A-->>E: Auth headers
    E->>W: Execute request
    W->>API: POST /api/v1/order
    API-->>W: {orderId: "123"}
    W-->>E: Response
    E->>E: Track order
    E-->>S: Order ID

    Note over E: Async updates via WebSocket

    API->>E: Order Update (WS)
    E->>E: Update InFlightOrder
    E->>S: Emit OrderFilledEvent
```

## 3. Data Flow Architecture

```mermaid
graph LR
    subgraph "Market Data Flow"
        REST1[REST API] --> OB[Order Book Snapshot]
        WS1[WebSocket] --> OBU[Order Book Updates]
        WS1 --> T[Trades]

        OB --> OBQ[OrderBook Queue]
        OBU --> OBQ
        T --> TQ[Trade Queue]

        OBQ --> OBT[OrderBookTracker]
        TQ --> OBT
    end

    subgraph "Private Data Flow"
        REST2[REST API] --> B[Balances]
        REST2 --> O[Orders]
        WS2[Private WS] --> BU2[Balance Updates]
        WS2 --> OU[Order Updates]
        WS2 --> F[Fills]

        B --> BE[BackpackExchange]
        BU2 --> BE
        OU --> BE
        F --> BE
        O --> BE
    end

    subgraph "Order Lifecycle"
        BE --> NEW[New Order]
        NEW --> PENDING[Pending]
        PENDING --> OPEN[Open]
        OPEN --> PARTIAL[Partially Filled]
        PARTIAL --> FILLED[Filled]
        OPEN --> CANCELLED[Cancelled]
    end
```

## 4. WebSocket Connection Architecture

```mermaid
stateDiagram-v2
    [*] --> Disconnected
    Disconnected --> Connecting: connect()
    Connecting --> Authenticating: Connected
    Authenticating --> Subscribing: Auth Success
    Authenticating --> Disconnected: Auth Failed
    Subscribing --> Listening: Subscribed
    Listening --> Processing: Message Received
    Processing --> Listening: Message Processed
    Listening --> Reconnecting: Connection Lost
    Reconnecting --> Connecting: Retry
    Reconnecting --> Disconnected: Max Retries
    Listening --> Disconnected: stop()
```

## 5. Authentication Flow

```mermaid
graph TD
    subgraph "REST Authentication"
        R1[API Request] --> R2[Add Timestamp]
        R2 --> R3[Build Signature Payload]
        R3 --> R4[Sign with Ed25519]
        R4 --> R5[Add Headers]
        R5 --> R6[Send Request]
    end

    subgraph "WebSocket Authentication"
        W1[Connect WebSocket] --> W2[Build Auth Message]
        W2 --> W3[Sign Payload]
        W3 --> W4[Send Auth Message]
        W4 --> W5{Auth Response}
        W5 -->|Success| W6[Subscribe Channels]
        W5 -->|Failure| W7[Disconnect]
    end

    subgraph "Signature Components"
        SC1[Timestamp]
        SC2[HTTP Method]
        SC3[Path]
        SC4[Body]
        SC1 & SC2 & SC3 & SC4 --> SIG[Ed25519 Signature]
    end
```

## 6. Order Management State Machine

```mermaid
stateDiagram-v2
    [*] --> Created: place_order()
    Created --> Pending: API Request
    Pending --> Open: Exchange Confirmed
    Pending --> Failed: API Error

    Open --> PartiallyFilled: Partial Fill Event
    Open --> Filled: Complete Fill Event
    Open --> Cancelling: cancel_order()
    Open --> Expired: Time Expired

    PartiallyFilled --> Filled: Remaining Filled
    PartiallyFilled --> Cancelling: cancel_order()
    PartiallyFilled --> Expired: Time Expired

    Cancelling --> Cancelled: Cancel Confirmed
    Cancelling --> CancelFailed: Cancel Rejected

    CancelFailed --> Open: Revert State

    Filled --> [*]: Complete
    Cancelled --> [*]: Complete
    Failed --> [*]: Complete
    Expired --> [*]: Complete
```

## 7. Class Hierarchy

```mermaid
classDiagram
    ExchangePyBase <|-- BackpackExchange
    OrderBookTrackerDataSource <|-- BackpackAPIOrderBookDataSource
    UserStreamTrackerDataSource <|-- BackpackAPIUserStreamDataSource
    AuthBase <|-- BackpackAuth

    class ExchangePyBase {
        <<abstract>>
        +name: str
        +authenticator: AuthBase
        +rate_limits_rules: List[RateLimit]
        +place_order()
        +cancel_order()
        +get_balance()
    }

    class BackpackExchange {
        -backpack_api_key: str
        -backpack_api_secret: str
        -_trading_pairs: List[str]
        +_place_order()
        +_place_cancel()
        +_update_balances()
        +_update_trading_rules()
    }

    class BackpackAuth {
        -api_key: str
        -api_secret: str
        +rest_authenticate()
        +ws_authenticate()
        -_sign_payload()
    }

    class BackpackAPIOrderBookDataSource {
        -_trading_pairs: List[str]
        +listen_for_order_book_diffs()
        +listen_for_order_book_snapshots()
        +listen_for_trades()
    }

    class BackpackAPIUserStreamDataSource {
        -_auth: BackpackAuth
        +listen_for_user_stream()
        -_authenticate_websocket()
    }
```

## 8. Rate Limiting Architecture

```mermaid
graph TD
    subgraph "AsyncThrottler"
        RL1[Rate Limit Rules]
        RL2[Token Buckets]
        RL3[Request Queue]
    end

    subgraph "API Requests"
        R1[place_order]
        R2[cancel_order]
        R3[get_balance]
        R4[get_orders]
    end

    R1 --> CHECK1{Check Limit}
    R2 --> CHECK2{Check Limit}
    R3 --> CHECK3{Check Limit}
    R4 --> CHECK4{Check Limit}

    CHECK1 -->|Available| EXEC1[Execute]
    CHECK1 -->|Exhausted| WAIT1[Wait]

    CHECK2 -->|Available| EXEC2[Execute]
    CHECK2 -->|Exhausted| WAIT2[Wait]

    CHECK3 -->|Available| EXEC3[Execute]
    CHECK3 -->|Exhausted| WAIT3[Wait]

    CHECK4 -->|Available| EXEC4[Execute]
    CHECK4 -->|Exhausted| WAIT4[Wait]

    WAIT1 --> EXEC1
    WAIT2 --> EXEC2
    WAIT3 --> EXEC3
    WAIT4 --> EXEC4

    RL1 --> RL2
    RL2 --> CHECK1
    RL2 --> CHECK2
    RL2 --> CHECK3
    RL2 --> CHECK4
```

## 9. Error Handling Flow

```mermaid
flowchart TD
    API[API Call] --> TRY{Try}
    TRY -->|Success| PARSE[Parse Response]
    TRY -->|Exception| CATCH[Catch Exception]

    PARSE --> VALIDATE{Validate}
    VALIDATE -->|Valid| SUCCESS[Return Data]
    VALIDATE -->|Invalid| ERROR1[Raise ValueError]

    CATCH --> TYPE{Exception Type}
    TYPE -->|Timeout| RETRY1[Retry Logic]
    TYPE -->|Auth Error| AUTH[Re-authenticate]
    TYPE -->|Rate Limit| WAIT[Wait & Retry]
    TYPE -->|Network| RETRY2[Exponential Backoff]
    TYPE -->|Other| LOG[Log & Raise]

    RETRY1 -->|Max Retries| FAIL1[Raise TimeoutError]
    RETRY1 -->|Success| SUCCESS

    AUTH -->|Success| API
    AUTH -->|Failure| FAIL2[Raise AuthError]

    WAIT --> API

    RETRY2 -->|Max Retries| FAIL3[Raise NetworkError]
    RETRY2 -->|Success| SUCCESS
```

## 10. Message Processing Pipeline

```mermaid
graph LR
    subgraph "WebSocket Message Pipeline"
        WS[WebSocket] --> RECV[Receive Message]
        RECV --> PARSE[Parse JSON]
        PARSE --> ROUTE{Route by Type}

        ROUTE -->|orderUpdate| OU[Process Order]
        ROUTE -->|balanceUpdate| BU[Process Balance]
        ROUTE -->|tradeUpdate| TU[Process Trade]
        ROUTE -->|depth| DU[Process Depth]
        ROUTE -->|unknown| LOG[Log Unknown]

        OU --> UPDATE1[Update InFlightOrder]
        BU --> UPDATE2[Update Balances]
        TU --> UPDATE3[Update Fills]
        DU --> UPDATE4[Update OrderBook]

        UPDATE1 --> EMIT1[Emit Events]
        UPDATE2 --> EMIT2[Emit Events]
        UPDATE3 --> EMIT3[Emit Events]
        UPDATE4 --> EMIT4[Emit Events]
    end
```

## 11. Minimal Implementation Path

```mermaid
graph TD
    START[Start] --> PHASE1[Phase 1: Foundation]

    subgraph "Phase 1"
        P1A[Create Constants]
        P1B[Implement Auth]
        P1C[Basic Exchange Class]
        P1A --> P1B --> P1C
    end

    PHASE1 --> PHASE2[Phase 2: Trading]

    subgraph "Phase 2"
        P2A[Place Order]
        P2B[Cancel Order]
        P2C[Update Balances]
        P2D[Trading Rules]
        P2A --> P2B --> P2C --> P2D
    end

    PHASE2 --> PHASE3[Phase 3: Streaming]

    subgraph "Phase 3"
        P3A[OrderBook DataSource]
        P3B[UserStream DataSource]
        P3C[Event Processing]
        P3A --> P3B --> P3C
    end

    PHASE3 --> PHASE4[Phase 4: Polish]

    subgraph "Phase 4"
        P4A[Error Handling]
        P4B[Testing]
        P4C[Documentation]
        P4A --> P4B --> P4C
    end

    PHASE4 --> END[Complete]
```

## 12. Testing Architecture

```mermaid
graph TD
    subgraph "Test Structure"
        UT[Unit Tests]
        IT[Integration Tests]
        MT[Mock Tests]
    end

    subgraph "Unit Test Coverage"
        UT1[BackpackAuth]
        UT2[BackpackUtils]
        UT3[Order Processing]
        UT4[Balance Updates]
    end

    subgraph "Integration Test Coverage"
        IT1[REST API Calls]
        IT2[WebSocket Streams]
        IT3[Order Lifecycle]
        IT4[Error Scenarios]
    end

    subgraph "Mock Infrastructure"
        MA[MockAssistant]
        MWS[Mock WebSocket]
        MAPI[Mock API Responses]
    end

    UT --> UT1
    UT --> UT2
    UT --> UT3
    UT --> UT4

    IT --> IT1
    IT --> IT2
    IT --> IT3
    IT --> IT4

    MT --> MA
    MA --> MWS
    MA --> MAPI

    UT1 --> MA
    IT1 --> MAPI
    IT2 --> MWS
```

## Summary

These diagrams illustrate:

1. **Component Architecture**: How Backpack connector integrates with Hummingbot core
2. **Data Flow**: How market and private data flows through the system
3. **State Management**: Order lifecycle and WebSocket connection states
4. **Authentication**: Ed25519 signing process for REST and WebSocket
5. **Error Handling**: Robust error recovery patterns
6. **Message Processing**: WebSocket message routing and handling
7. **Implementation Path**: Phased approach for building the connector

The architecture follows Hummingbot's established patterns while incorporating Backpack-specific requirements like Ed25519 authentication and the exchange's WebSocket message formats.
