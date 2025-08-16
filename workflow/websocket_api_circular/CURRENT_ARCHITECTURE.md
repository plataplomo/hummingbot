# Current Architecture - Detailed Analysis

## Overview

This document provides a detailed view of the current architecture showing how the circular dependency manifests across different layers and modules.

## Layer Structure

```mermaid
graph TB
    subgraph "Entry Points"
        MAIN[main.py]
        TEST[tests/*]
        FACT[ExchangeAPIFactory]
    end

    subgraph "Exchange Base Layer"
        BASE[exchange_api.py<br/>Abstract Base]
        PROTO[Exchange Protocols]
    end

    subgraph "Infrastructure Layer"
        subgraph "Connectivity"
            CMOD[connectivity_models.py]
            CMAN[ws_manager.py]
            VMAN[validated_ws_manager.py]
        end

        subgraph "WebSocket Core"
            WSTATE[websocket_states.py]
            WHAND[ws_message_handler.py]
            WADAP[ws_type_adapters.py ⚠️]
            WDISC[ws_discriminated_unions.py ⚠️]
        end
    end

    subgraph "Exchange Implementations"
        subgraph "Hyperliquid"
            HLAPI[hl_api.py]
            HLWS[hl_ws_router.py ⚠️]
            HLMOD[hl_raw_ws_events.py]
        end

        subgraph "Backpack"
            BPAPI[bp_api.py]
            BPWS[bp_ws_router.py ⚠️]
            BPMOD[bp_raw_ws_models.py]
        end
    end

    MAIN --> BASE
    TEST --> BASE
    FACT --> HLAPI
    FACT --> BPAPI

    BASE --> CMOD
    CMOD --> CMAN
    CMAN --> WSTATE
    WSTATE --> WADAP

    WADAP -->|❌ VIOLATION| HLMOD
    WADAP -->|❌ VIOLATION| BPMOD
    WDISC -->|❌ VIOLATION| HLMOD
    WDISC -->|❌ VIOLATION| BPMOD

    HLAPI --> BASE
    BPAPI --> BASE

    HLWS -->|❌ VIOLATION| WHAND
    BPWS -->|❌ VIOLATION| WHAND

    style WADAP fill:#ffcccc,stroke:#ff0000,stroke-width:2px,color:#000
    style WDISC fill:#ffcccc,stroke:#ff0000,stroke-width:2px,color:#000
    style HLWS fill:#ffe6cc,stroke:#ff9900,stroke-width:2px,color:#000
    style BPWS fill:#ffe6cc,stroke:#ff9900,stroke-width:2px,color:#000
```

## Module Dependencies

### Exchange API Base
```python
# exchange_api.py dependencies
cyberdelta.apis.connectivity.connectivity_models
cyberdelta.apis.connectivity.validated_ws_manager
cyberdelta.config.models.exchange_config
cyberdelta.models.market.*
cyberdelta.models.trading.*
```

### Connectivity Layer
```python
# ws_manager.py dependencies
cyberdelta.apis.websocket.websocket_states  # ← Starts upward dependency
cyberdelta.apis.connectivity.connection_config
cyberdelta.apis.connectivity.rate_limiter
```

### WebSocket Layer (Violation Point)
```python
# ws_type_adapters.py - THE MAIN VIOLATOR
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsLedgerUpdate,
    HyperliquidRawWsNonFundingLedgerUpdate,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsPong,
    HyperliquidRawWsUserEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_ws_models import (
    BackpackRawWsAccountData,
    BackpackRawWsBalanceData,
    BackpackRawWsEventData,
    BackpackRawWsOrderUpdate,
    BackpackRawWsPriceData,
    BackpackRawWsTransaction,
)
```

## Class Relationships

```mermaid
classDiagram
    class ExchangeAPI {
        <<abstract>>
        +get_ticker()
        +place_order()
        +cancel_order()
        +get_ws_manager()
    }

    class HyperliquidAPI {
        -ws_manager: WebSocketManager
        -ws_router: HyperliquidWebSocketRouter
    }

    class BackpackAPI {
        -ws_manager: WebSocketManager
        -ws_router: BackpackWebSocketRouter
    }

    class WebSocketManager {
        -handlers: dict
        -connection: WebSocketConnection
        +subscribe()
        +unsubscribe()
    }

    class WebSocketTypeAdapters {
        <<static>>
        +create_hyperliquid_fill_adapter()
        +create_backpack_transaction_adapter()
        +create_hyperliquid_order_adapter()
    }

    class HyperliquidWebSocketRouter {
        -message_handler: WebSocketMessageHandler
        +route_message()
    }

    ExchangeAPI <|-- HyperliquidAPI
    ExchangeAPI <|-- BackpackAPI
    HyperliquidAPI --> WebSocketManager
    BackpackAPI --> WebSocketManager
    WebSocketManager --> WebSocketTypeAdapters
    WebSocketTypeAdapters ..> HyperliquidRawWsFillEvent : knows about
    WebSocketTypeAdapters ..> BackpackRawWsTransaction : knows about
    HyperliquidWebSocketRouter --> WebSocketMessageHandler

    style WebSocketTypeAdapters fill:#ffcccc,stroke:#ff0000,stroke-width:2px,color:#000
```

## Import Flow Timeline

```mermaid
sequenceDiagram
    participant M as main.py
    participant EA as exchange_api
    participant C as connectivity
    participant WS as websocket
    participant WTA as ws_type_adapters
    participant HM as hyperliquid.models
    participant HI as hyperliquid.__init__
    participant HA as hl_api

    M->>EA: import ExchangeAPI
    EA->>C: import connectivity_models
    C->>WS: import websocket_states
    WS->>WTA: import ws_type_adapters
    WTA->>HM: import hl_raw_ws_events ❌
    HM->>HI: triggers __init__.py
    HI->>HA: from .hl_api import HyperliquidAPI
    HA->>EA: from exchange_api import ExchangeAPI ❌
    Note over EA: Module not fully initialized!
    EA-->>M: ImportError: circular import
```

## WebSocket Infrastructure Problems

### 1. Type Adapter Hardcoding
```python
class WebSocketTypeAdapters:
    """Central adapter factory with hardcoded exchange knowledge."""

    @staticmethod
    def create_hyperliquid_fill_adapter() -> TypeAdapter[HyperliquidRawWsFillEvent]:
        return TypeAdapter(HyperliquidRawWsFillEvent)

    @staticmethod
    def create_backpack_transaction_adapter() -> TypeAdapter[BackpackRawWsTransaction]:
        return TypeAdapter(BackpackRawWsTransaction)

    # One method per exchange type - not scalable!
```

### 2. Discriminated Union Hardcoding
```python
# ws_discriminated_unions.py
DiscriminatedHyperliquidData = Annotated[
    Union[
        HyperliquidRawWsChannel,
        HyperliquidRawWsSubscription,
        HyperliquidRawWsUserEvent,
        # ... all Hyperliquid types hardcoded
    ],
    Field(discriminator="channel"),
]

DiscriminatedBackpackData = Annotated[
    Union[
        BackpackRawWsEventData,
        BackpackRawWsOrderUpdate,
        # ... all Backpack types hardcoded
    ],
    Field(discriminator="type"),
]
```

### 3. Router Implementation Issues
```python
# hl_ws_router.py
from cyberdelta.apis.websocket.ws_message_handler import (
    WebSocketMessageHandler,  # Infrastructure detail
    WebSocketRouter,          # Should be protocol
)

class HyperliquidWebSocketRouter(WebSocketRouter):
    """Tightly coupled to WebSocket implementation."""

    def __init__(self, message_handler: WebSocketMessageHandler):
        # Direct dependency on implementation
        self._message_handler = message_handler
```

## Configuration Flow

```mermaid
graph LR
    subgraph "Configuration"
        CFG[AppSettings]
        SEC[SecretsConfig]
    end

    subgraph "Factory"
        FAC[ExchangeAPIFactory]
    end

    subgraph "Runtime Creation"
        CR[Create API Instance]
    end

    CFG --> FAC
    SEC --> FAC
    FAC --> CR
    CR -->|Dynamic Import| HyperliquidAPI
    CR -->|Dynamic Import| BackpackAPI

    HyperliquidAPI -->|Circular!| ExchangeAPI
    BackpackAPI -->|Circular!| ExchangeAPI

    style HyperliquidAPI fill:#ffcccc,stroke:#ff0000,stroke-width:2px,color:#000
    style BackpackAPI fill:#ffe6cc,stroke:#ff9900,stroke-width:2px,color:#000
```

## Key Architectural Smells

1. **Infrastructure knows about implementations**
   - `ws_type_adapters.py` imports specific exchange models
   - `ws_discriminated_unions.py` defines exchange-specific unions

2. **Implementations depend on infrastructure internals**
   - Exchange routers import `WebSocketMessageHandler` directly
   - Should depend on protocols/interfaces

3. **No abstraction layer between WebSocket and exchanges**
   - Direct coupling via imports
   - No registration or plugin mechanism

4. **Package initialization triggers imports**
   - `__init__.py` files import implementation classes
   - Causes immediate circular dependency

## Module Size and Complexity

| Module | Lines | Imports | Violations |
|--------|-------|---------|------------|
| ws_type_adapters.py | 300+ | 15+ | 10+ exchange-specific |
| ws_discriminated_unions.py | 250+ | 12+ | All unions hardcoded |
| hl_ws_router.py | 400+ | 8+ | Infrastructure imports |
| bp_ws_router.py | 350+ | 7+ | Infrastructure imports |
| exchange_api.py | 500+ | 20+ | Clean (base) |

## Coupling Metrics

- **Afferent Coupling (Ca):** Number of classes that depend on this class
  - `exchange_api.py`: 15+ (all exchanges + tests)
  - `ws_type_adapters.py`: 5+ (WebSocket infrastructure)

- **Efferent Coupling (Ce):** Number of classes this class depends on
  - `ws_type_adapters.py`: 20+ (all exchange models!)
  - `hl_ws_router.py`: 10+ (infrastructure + models)

- **Instability (I = Ce / (Ca + Ce)):**
  - `ws_type_adapters.py`: 0.8 (highly unstable, changes frequently)
  - `exchange_api.py`: 0.25 (stable abstraction)

The high instability of infrastructure components that should be stable indicates the architectural violation.
