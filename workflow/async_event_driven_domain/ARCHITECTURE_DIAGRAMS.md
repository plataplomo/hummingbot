# Architecture Diagrams - Current vs Practical Target State

## Current Architecture (Pseudo-Async)

### Current Flow - Synchronous Wrapped in Async

```mermaid
sequenceDiagram
    participant Client
    participant PositionManager
    participant StateManager
    participant StateDict
    participant EventBus

    Client->>+PositionManager: await has_position(symbol)
    PositionManager->>+EventBus: request(PositionQuery)
    EventBus->>+PositionManager: _handle_position_query()
    PositionManager->>+StateManager: await get_state()
    StateManager->>+StateDict: state.positions.get(key)
    Note over StateDict: SYNCHRONOUS dict lookup!
    StateDict-->>-StateManager: position or None
    StateManager-->>-PositionManager: state
    PositionManager->>+StateDict: key in positions
    StateDict-->>-PositionManager: True/False
    PositionManager-->>-EventBus: PositionQueryResponse
    EventBus-->>-PositionManager: response
    PositionManager-->>-Client: bool

    Note over Client,StateDict: Circular: Event→Handler→Same Dict!
```

### Current State Management

```mermaid
graph TB
    subgraph "Current State Management"
        SM[StateManager] --> |locks| AL[AsyncLock]
        SM --> |stores| SD[State Dictionary]
        SD --> |contains| POS[positions: dict]
        SD --> |contains| BAL[balances: dict]

        PM[PositionManager] --> |reads| SM
        PM --> |writes| SM
        BM[BalanceManager] --> |reads| SM
        BM --> |writes| SM

        Note1[All access through single lock]
        Note2[Direct dictionary mutations]
        Note3[Sequential processing only]
    end

    style Note1 fill:#ffcccc
    style Note2 fill:#ffcccc
    style Note3 fill:#ffcccc
```

## Practical Target Architecture (Incremental Improvements)

### Target Flow - Actor Pattern with Caching

```mermaid
sequenceDiagram
    participant Client
    participant PositionActor
    participant Cache
    participant StateManager
    participant EventBus

    Client->>+EventBus: request(PositionQuery)
    EventBus->>+PositionActor: handle_query()

    alt Cache Hit
        PositionActor->>+Cache: get(key)
        Cache-->>-PositionActor: position
    else Cache Miss
        PositionActor->>+StateManager: get_state()
        StateManager-->>-PositionActor: state
        PositionActor->>Cache: set(key, position)
    end

    PositionActor-->>-EventBus: PositionQueryResponse
    EventBus-->>-Client: response

    Note over Client,Cache: Fast path via cache, async all the way!
```

### Practical Service Communication

```mermaid
graph LR
    subgraph "Target - Event-Based with Actors"
        EB[EventBus] --> |publishes| Events

        PA[PositionActor] --> |subscribes| EB
        BA[BalanceActor] --> |subscribes| EB
        RA[RiskActor] --> |subscribes| EB

        PA --> |caches| PC[Position Cache]
        BA --> |caches| BC[Balance Cache]

        PA --> |persists| SM[StateManager]
        BA --> |persists| SM

        Note[Decoupled via events, cached for speed]
    end

    style Note fill:#ccffcc
```

## Concurrency Comparison

### Current - Sequential Processing

```mermaid
gantt
    title Current Sequential Processing
    dateFormat HH:mm:ss
    axisFormat %S

    section Position Queries
    Get Position 1    :01, 1s
    Get Position 2    :02, 1s
    Get Position 3    :03, 1s
    Get Position 4    :04, 1s
    Get Position 5    :05, 1s

    section Total Time
    Total 5 seconds   :01, 5s
```

### Target - Concurrent Processing with Batch

```mermaid
gantt
    title Target Concurrent Processing
    dateFormat HH:mm:ss
    axisFormat %S

    section Batch Query
    Prepare Batch     :01, 0.1s
    Concurrent Fetch  :01.1, 0.5s
    Process Results   :01.6, 0.1s

    section Total Time
    Total 0.7 seconds :01, 0.7s
```

## Event Flow Patterns

### Current - Blocking Request/Response

```mermaid
graph TB
    subgraph "Current Pattern"
        C1[Client] --> |await| S1[Service]
        S1 --> |blocks| Lock[AsyncLock]
        Lock --> |waits| State[State Dict]
        State --> |returns| S1
        S1 --> |response| C1

        Note[Each request blocks others]
    end

    style Note fill:#ffcccc
```

### Target - Non-Blocking with Streaming

```mermaid
graph TB
    subgraph "Practical Target Pattern"
        subgraph "Queries"
            C1[Client] --> |request| EB[EventBus]
            EB --> |route| Actor[Actor + Cache]
            Actor --> |response| EB
            EB --> |return| C1
        end

        subgraph "Updates"
            Fill[Fill Event] --> EB2[EventBus]
            EB2 --> |stream| Subscribers
            Subscribers --> C2[Client]
            Subscribers --> C3[Client]
        end

        Note[Non-blocking queries + Real-time streams]
    end

    style Note fill:#ccffcc
```

## Practical Actor Model

```mermaid
graph TB
    subgraph "Simple Actor Pattern (Nautilus-style)"
        Actor[PositionActor] --> |has| Handler[Event Handler]
        Actor --> |has| Cache[Local Cache]
        Actor --> |uses| PM[PositionManager Logic]

        Handler --> |receives| Query[Position Queries]
        Handler --> |receives| Fill[Fill Events]

        Cache --> |speeds up| Reads[Read Operations]
        PM --> |handles| Logic[Business Logic]

        Actor --> |publishes| Events[Position Events]

        Note[Thin wrapper around existing logic]
    end

    style Note fill:#ccffcc
    style Cache fill:#ffffcc
```

## Streaming Architecture

```mermaid
graph LR
    subgraph "Position Update Streaming"
        Source[Fill Events] --> EB[EventBus]

        EB --> |stream| Buffer[Buffer Queue]
        Buffer --> |backpressure| Control[Flow Control]

        Control --> |emit| Stream[Position Stream]

        Stream --> Observer1[Strategy 1]
        Stream --> Observer2[Strategy 2]
        Stream --> Observer3[Risk Monitor]

        Note[Simple streaming with backpressure]
    end

    style Buffer fill:#ffffcc
    style Note fill:#ccffcc
```

## State Consistency Model

### Current - Pessimistic Locking

```mermaid
sequenceDiagram
    participant T1 as Thread 1
    participant Lock
    participant State
    participant T2 as Thread 2

    T1->>+Lock: acquire()
    Lock-->>T1: locked
    T1->>State: read positions
    T1->>State: update position
    T1->>State: save state

    Note over T2: Thread 2 blocked...
    T2->>Lock: acquire() - WAITING

    T1->>Lock: release()
    Lock-->>T2: locked
    T2->>State: read positions

    Note over T1,T2: Sequential access, no concurrency
```

### Target - Cache-Based Optimistic Reads

```mermaid
sequenceDiagram
    participant R1 as Reader 1
    participant R2 as Reader 2
    participant Cache
    participant Writer
    participant State

    par Concurrent Reads
        R1->>Cache: get(position1)
        R2->>Cache: get(position2)
    end

    Cache-->>R1: position1 (cached)
    Cache-->>R2: position2 (cached)

    Writer->>State: update position
    Writer->>Cache: invalidate(key)
    Writer->>EventBus: publish update

    Note over R1,State: Reads don't block, writes invalidate cache
```

## Performance Characteristics

### Practical Improvements

```mermaid
graph TB
    subgraph "Current Performance"
        CP1[Sequential: O(n)]
        CP2[Lock Contention: High]
        CP3[Latency: 50-100ms]
        CP4[Throughput: 10-100 ops/sec]
    end

    subgraph "Achievable Target"
        TP1[Batch Concurrent: O(1) for batch]
        TP2[Cache Hits: No lock needed]
        TP3[Latency: 5-10ms cached]
        TP4[Throughput: 1K-5K ops/sec]
    end

    CP1 -.->|5-10x faster| TP1
    CP2 -.->|cache reduces| TP2
    CP3 -.->|10x faster| TP3
    CP4 -.->|10-50x more| TP4

    style CP1 fill:#ffcccc
    style CP2 fill:#ffcccc
    style CP3 fill:#ffcccc
    style CP4 fill:#ffcccc

    style TP1 fill:#ccffcc
    style TP2 fill:#ccffcc
    style TP3 fill:#ccffcc
    style TP4 fill:#ccffcc
```

## Implementation Phases

```mermaid
graph LR
    subgraph "Week 1: Foundation"
        W1A[Enhance EventBus] --> W1B[Add Streaming]
        W1B --> W1C[Create PositionActor]
        W1C --> W1D[Add Caching]
    end

    subgraph "Week 2: Expand"
        W2A[BalanceActor] --> W2B[Concurrent Queries]
        W2B --> W2C[Signal Publishing]
    end

    subgraph "Week 3: Optimize"
        W3A[Batch Operations] --> W3B[Performance Tuning]
        W3B --> W3C[Monitoring]
    end

    W1D --> W2A
    W2C --> W3A

    style W1A fill:#ccffcc
    style W1B fill:#ccffcc
    style W1C fill:#ccffcc
```

## Summary

The diagrams show a **practical migration path** from:

1. **Current State**: Pseudo-async with synchronous operations and tight coupling
2. **Target State**: Simple actors with caching, streaming, and concurrent operations

Key improvements:
- From **sequential processing** to **concurrent batches**
- From **blocking locks** to **cached reads**
- From **tight coupling** to **event-based communication**
- From **no streaming** to **real-time updates**

This represents an **incremental improvement** rather than a complete rewrite, achieving **5-10x performance gains** with **minimal complexity**.
