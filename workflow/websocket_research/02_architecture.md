# WebSocket Module Architecture Documentation

## Current Architecture Overview

The WebSocket module is a complex system designed to handle real-time message processing from multiple cryptocurrency exchanges. However, multiple refactoring iterations have created a labyrinthine architecture with significant redundancy.

## Architecture Diagrams

### 1. High-Level Component Architecture

```mermaid
graph TB
    subgraph "External"
        BP[Backpack Exchange]
        HL[Hyperliquid Exchange]
    end

    subgraph "WebSocket Module"
        subgraph "Core Layer"
            CTX[ws_context.py<br/>Message Context]
            PROTO[ws_protocols.py<br/>Protocol Definitions]
            ENV[ws_envelope.py<br/>Message Envelopes]
        end

        subgraph "Registry Layer"
            REG[ws_context_registry.py<br/>Context Registry]
            REGF[ws_registry_factory.py<br/>Registry Factory]
            REGB[ws_registry_builder.py<br/>Registry Builder]
        end

        subgraph "Processing Layer"
            PROC[ws_processor.py<br/>Message Processor]
            TPROC[ws_typed_processor.py<br/>Typed Processor]
            TRANS[ws_transformer.py<br/>Data Transformer]
            ROUT[ws_router.py<br/>Message Router]
        end

        subgraph "Error Handling"
            EH1[ws_error_handler.py<br/>DEPRECATED]
            EH2[ws_stream_error_handler.py<br/>Stream Handler]
            EREC[ws_error_recovery.py<br/>Recovery Logic]
            EMET[ws_error_metrics.py<br/>Error Metrics]
        end

        subgraph "Validation"
            VAL[ws_validators.py<br/>Payload Validators]
            SEC[ws_security.py<br/>Security Validation]
            TGUARD[ws_type_guards.py<br/>Type Guards]
        end
    end

    BP --> ROUT
    HL --> ROUT
    ROUT --> TPROC
    TPROC --> REG
    REG --> CTX
    CTX --> PROC
    PROC --> TRANS
    PROC --> EH2

    style EH1 fill:#ff6666
    style REGF fill:#ffcc66
```

### 2. Message Processing Flow

```mermaid
sequenceDiagram
    participant WS as WebSocket
    participant R as Router
    participant TP as TypedProcessor
    participant REG as Registry
    participant CTX as Context
    participant P as Processor
    participant T as Transformer
    participant H as Handler
    participant E as ErrorHandler

    WS->>R: Raw Message
    R->>TP: Route Message
    TP->>REG: Get Context Type
    REG->>CTX: Create Context
    CTX-->>TP: Typed Context
    TP->>P: Process with Context
    P->>P: Validate Payload
    P->>T: Transform to Domain Model
    T-->>P: Domain Model
    P->>H: Invoke Handler
    alt Error Occurs
        P->>E: Handle Error
        E->>E: Log & Recover
    end
    H-->>P: Complete
```

### 3. Error Handling Hierarchy (Current Chaos)

```mermaid
graph TB
    subgraph "Error Systems (Redundant)"
        BASE[BaseErrorHandler<br/>DEPRECATED]
        STREAM[WebSocketStreamErrorHandler<br/>PRIMARY]
        SEC[SecureErrorHandler]
        REC[WebSocketErrorRecovery]
        MET1[WebSocketErrorMetrics]
        MET2[WebSocketErrorMetricsCollector]
    end

    subgraph "Exception Classes"
        EX1[ws_exceptions.py<br/>20+ Exception Types]
        EX2[ws_envelope.py<br/>5 Exception Types]
        EX3[ws_validators.py<br/>10 Exception Types]
        EX4[ws_security.py<br/>8 Exception Types]
    end

    subgraph "Error Contexts"
        EC1[StreamErrorContext]
        EC2[ProcessorErrorContext]
        EC3[RouterErrorContext]
    end

    STREAM --> EC1
    BASE --> EC2
    REC --> EC3

    style BASE fill:#ff6666
```

### 4. Registry and Factory Pattern (Over-engineered)

```mermaid
graph LR
    subgraph "Current Implementation"
        F1[WebSocketRegistryFactory]
        F2[create_registry<br/>returns empty]
        F3[create_configured_registry<br/>returns empty]
        F4[create_empty_registry<br/>returns empty]

        F1 --> F2
        F1 --> F3
        F1 --> F4

        F2 --> REG[WebSocketContextRegistry]
        F3 --> REG
        F4 --> REG
    end

    subgraph "What It Should Be"
        SIMPLE[WebSocketContextRegistry<br/>Direct Instantiation]
    end

    style F1 fill:#ffcc66
    style F2 fill:#ffcc66
    style F3 fill:#ffcc66
    style F4 fill:#ffcc66
```

### 5. Type Safety Issues

```mermaid
graph TB
    subgraph "Type Unsafe Areas"
        CTX1[ws_context.py<br/>domain_model: Any]
        REG1[Registry<br/>dict str, Any]
        MEM1[Memory Optimized<br/>data: Any]
        PROC1[Processor<br/>Loose Generics]
    end

    subgraph "Type Safe Areas"
        PROTO[Protocols<br/>Well Typed]
        GUARD[Type Guards<br/>Runtime Checks]
        MODEL[Base Models<br/>Pydantic]
    end

    CTX1 -.->|Should Use| PROTO
    REG1 -.->|Should Use| MODEL
    MEM1 -.->|Should Use| PROTO

    style CTX1 fill:#ff9999
    style REG1 fill:#ff9999
    style MEM1 fill:#ff9999
    style PROC1 fill:#ff9999
```

## Proposed Simplified Architecture

### 1. Consolidated Module Structure

```mermaid
graph TB
    subgraph "Proposed Structure"
        subgraph "core/"
            C1[context.py]
            C2[protocols.py]
            C3[models.py]
        end

        subgraph "processing/"
            P1[processor.py]
            P2[transformer.py]
            P3[router.py]
        end

        subgraph "error/"
            E1[exceptions.py]
            E2[handler.py]
            E3[recovery.py]
        end

        subgraph "validation/"
            V1[validators.py]
            V2[security.py]
        end

        subgraph "metrics/"
            M1[collector.py]
            M2[types.py]
        end

        subgraph "config/"
            CF1[settings.py]
        end
    end

    style core/ fill:#90EE90
    style processing/ fill:#90EE90
    style error/ fill:#90EE90
    style validation/ fill:#90EE90
    style metrics/ fill:#90EE90
    style config/ fill:#90EE90
```

### 2. Simplified Processing Flow

```mermaid
sequenceDiagram
    participant WS as WebSocket
    participant R as Router
    participant C as Context
    participant P as Processor
    participant H as Handler
    participant E as ErrorHandler

    WS->>R: Raw Message
    R->>C: Create Context
    C->>P: Process
    P->>P: Validate & Transform
    P->>H: Handle
    alt Error
        P->>E: Handle Error
    end
    H-->>WS: Complete
```

## Dependency Graph Analysis

### Current Dependencies (Circular Issues)

```mermaid
graph TB
    subgraph "Circular Dependency Issues"
        A[ws_context] --> B[ws_stream_context]
        B --> C[ws_protocols]
        C --> A

        D[ws_processor] --> E[ws_error_handler]
        E --> F[ws_stream_error]
        F --> D
    end

    style A fill:#ff9999
    style B fill:#ff9999
    style C fill:#ff9999
    style D fill:#ff9999
    style E fill:#ff9999
    style F fill:#ff9999
```

### Proposed Clean Dependencies

```mermaid
graph TB
    subgraph "Clean Hierarchy"
        PROTO[protocols]
        MODEL[models]
        CTX[context]
        PROC[processor]
        HAND[handler]

        PROTO --> MODEL
        CTX --> PROTO
        PROC --> CTX
        HAND --> PROC
    end

    style PROTO fill:#90EE90
    style MODEL fill:#90EE90
    style CTX fill:#90EE90
    style PROC fill:#90EE90
    style HAND fill:#90EE90
```

## Performance Bottlenecks

### Current Hotspots

```mermaid
graph LR
    subgraph "Performance Issues"
        HOT1[message_size_bytes<br/>JSON serialization<br/>on every access]
        HOT2[Multiple validation<br/>layers]
        HOT3[Redundant metrics<br/>collection]
        HOT4[Deep object<br/>nesting]
    end

    HOT1 --> IMPACT1[High CPU]
    HOT2 --> IMPACT2[Latency]
    HOT3 --> IMPACT3[Memory]
    HOT4 --> IMPACT4[GC Pressure]

    style HOT1 fill:#ff9999
    style HOT2 fill:#ff9999
    style HOT3 fill:#ff9999
    style HOT4 fill:#ff9999
```

## Module Interconnections

### Exchange-Specific Integration

```mermaid
graph TB
    subgraph "Exchange Integration Points"
        subgraph "Backpack"
            BP_ROUTER[bp_ws_router.py]
            BP_CONTEXT[bp_ws_context.py]
            BP_ENV[bp_ws_envelope.py]
        end

        subgraph "Hyperliquid"
            HL_ROUTER[hl_ws_router.py]
            HL_CONTEXT[hl_ws_context.py]
            HL_ENV[hl_ws_envelope.py]
        end

        subgraph "Shared WebSocket"
            REGISTRY[Registry]
            PROCESSOR[Processor]
            ERROR[Error Handler]
        end

        BP_ROUTER --> REGISTRY
        HL_ROUTER --> REGISTRY
        REGISTRY --> PROCESSOR
        PROCESSOR --> ERROR
    end
```

## Refactoring Roadmap

### Phase 1: Clean Up (1-2 days)
1. Remove deprecated ws_error_handler.py
2. Consolidate duplicate exception classes
3. Remove unused factory methods
4. Delete unused discriminated union code

### Phase 2: Consolidate (3-4 days)
1. Merge error handling modules
2. Unify metrics collection
3. Consolidate configuration
4. Simplify registry pattern

### Phase 3: Type Safety (2-3 days)
1. Replace all `Any` types
2. Create proper domain model types
3. Add runtime type validation
4. Improve generic constraints

### Phase 4: Restructure (1 week)
1. Create new module structure
2. Migrate code to new structure
3. Update imports across codebase
4. Comprehensive testing

## Metrics for Success

- **Code Reduction**: 30-40% fewer files
- **Type Coverage**: 100% typed (no `Any`)
- **Performance**: 20% faster message processing
- **Maintainability**: Clear module boundaries
- **Testing**: 90%+ coverage

## Conclusion

The current WebSocket architecture suffers from:
1. **Over-engineering**: Too many abstraction layers
2. **Redundancy**: Multiple implementations of same functionality
3. **Type Safety Loss**: Excessive use of `Any`
4. **Circular Dependencies**: Poor module boundaries
5. **Performance Issues**: Unnecessary computations

The proposed architecture would:
1. **Simplify**: Direct, clear data flow
2. **Consolidate**: Single implementation per concern
3. **Type Safe**: Full type coverage
4. **Clean Dependencies**: Hierarchical structure
5. **Performant**: Optimized hot paths

This refactoring would make the WebSocket module more maintainable, performant, and easier to extend for new exchanges.
