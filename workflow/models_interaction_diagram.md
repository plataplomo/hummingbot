# CyberDeltaEngine Model Interactions & Boundaries

This diagram illustrates the relationships and boundaries between the core model modules in `cyberdelta/core/models` and their connections to external systems (API, strategy engine, portfolio manager, exchange APIs, etc.).

```mermaid
flowchart TD
    %% Core Model Modules
    subgraph Models
        MKT["market.py<br>MarketData, Ticker, OrderBook,<br>FundingRate, Trade, Order"]
        PORT["portfolio.py<br>Balance, Position"]
        STRAT["strategy.py<br>TradeSignal, ArbitrageOpportunity"]
        API["api.py<br>API Request/Response Models"]
        ENUMS["enums.py<br>OrderSide, OrderType, etc."]
    end

    %% Internal Relationships
    MKT -- uses --> ENUMS
    PORT -- uses --> ENUMS
    STRAT -- uses --> ENUMS
    API -- uses --> ENUMS

    %% Cross-Model Interactions
    STRAT -- generates signals for --> MKT
    STRAT -- tracks positions in --> PORT
    API -- serializes/deserializes --> MKT
    API -- serializes/deserializes --> PORT

    %% External Boundaries
    subgraph External
        EXCH["Exchange APIs<br>(REST, WebSocket)"]
        STRATEGY[Strategy Engine]
        PORTFOLIO[Portfolio Manager]
        CLIENT[External Client/API Consumer]
    end

    %% Crossing Boundaries
    API -- communicates with --> EXCH
    API -- serves --> CLIENT
    STRAT -- receives data from --> STRATEGY
    PORT -- managed by --> PORTFOLIO

    %% Data Flow
    EXCH -- market/trade data --> MKT
    STRATEGY -- trade signals --> STRAT
    PORTFOLIO -- position/balance updates --> PORT
    CLIENT -- API requests/responses --> API

    %% Inter-module dependencies
    MKT -- order/trade events --> PORT
    MKT -- order/trade events --> STRAT
    PORT -- position state --> STRAT
```

# Per-Model Sequence Flow Diagrams (Detailed)

---

## MarketData Lifecycle & Interactions

```mermaid
sequenceDiagram
    participant EXCH as Exchange API
    participant API as ExchangeAPI (Base/Hyperliquid/Backpack)
    participant DH as DataHandler
    participant STRAT as Strategy/SignalGenerator
    participant TEST as Tests/Mocks
    
    EXCH->>API: REST/WebSocket market data (ticker, OHLCV)
    API-->>DH: MarketData | Ticker
    DH->>DH: Convert Ticker to MarketData (if needed)
    DH-->>STRAT: MarketData (via observer/handler)
    STRAT-->>DH: Requests MarketData (get_ticker)
    TEST-->>DH: Injects MarketData mocks
    Note over DH,STRAT: MarketData is immutable, used for analytics, signals, and monitoring
```

*Figure: MarketData is the canonical snapshot for symbol state, produced by API clients, managed by DataHandler, and consumed by strategies and tests.*

---

## Ticker Lifecycle & Interactions

```mermaid
sequenceDiagram
    participant EXCH as Exchange API
    participant API as ExchangeAPI (Base/Hyperliquid/Backpack)
    participant DH as DataHandler
    participant STRAT as Strategy/SignalGenerator
    participant TEST as Tests/Mocks

    EXCH->>API: REST/WebSocket ticker data
    API-->>DH: Ticker
    DH->>DH: Convert Ticker to MarketData (for storage/notification)
    DH-->>STRAT: MarketData (with embedded Ticker)
    STRAT-->>DH: Requests Ticker/MarketData (get_ticker)
    TEST-->>DH: Injects Ticker mocks
    Note over DH,STRAT: Ticker is a lightweight, immutable price/volume snapshot, often wrapped in MarketData
```

*Figure: Ticker is the atomic market price/volume update, typically wrapped in MarketData for downstream use.*

---

## OrderBook Lifecycle & Interactions

```mermaid
sequenceDiagram
    participant EXCH as Exchange API
    participant API as ExchangeAPI (Base/Hyperliquid/Backpack)
    participant DH as DataHandler
    participant STRAT as Strategy/SignalGenerator
    participant TEST as Tests/Mocks

    EXCH->>API: REST/WebSocket order book data
    API-->>DH: OrderBook
    DH-->>STRAT: OrderBook (on request or via observer)
    STRAT-->>DH: Requests OrderBook (get_orderbook)
    TEST-->>DH: Injects OrderBook mocks
    Note over DH,STRAT: OrderBook is immutable, used for price discovery, liquidity, and strategy logic
```

*Figure: OrderBook represents the current market depth, produced by API clients, managed by DataHandler, and used by strategies and tests.*

---

## FundingRate Lifecycle & Interactions

```mermaid
sequenceDiagram
    participant EXCH as Exchange API
    participant API as ExchangeAPI (Base/Hyperliquid/Backpack)
    participant DH as DataHandler
    participant STRAT as Strategy/SignalGenerator
    participant TEST as Tests/Mocks

    EXCH->>API: REST/WebSocket funding rate data
    API-->>DH: FundingRate
    DH-->>STRAT: FundingRate (on request or via observer)
    STRAT-->>DH: Requests FundingRate (get_funding_rate)
    TEST-->>DH: Injects FundingRate mocks
    Note over DH,STRAT: FundingRate is immutable, used for arbitrage, risk, and analytics
```

*Figure: FundingRate is the canonical funding data, produced by API clients, managed by DataHandler, and consumed by strategies and tests.*

---

## Trade Lifecycle & Interactions

```mermaid
sequenceDiagram
    participant EXCH as Exchange API
    participant API as ExchangeAPI (Base/Hyperliquid/Backpack)
    participant DH as DataHandler
    participant EH as ExecutionHandler/Order Management
    participant PT as PortfolioTracker
    participant TEST as Tests/Mocks

    EXCH->>API: REST/WebSocket trade/fill data
    API-->>DH: Trade (parsed from API)
    EH-->>PT: Trade (from order execution/fill)
    PT-->>PT: Updates positions, balances, PnL with Trade
    TEST-->>PT: Injects Trade mocks
    Note over PT,EH: Trade is immutable, used for audit, reconciliation, and analytics
```

*Figure: Trade represents an immutable execution/fill, produced by API clients, processed by ExecutionHandler, and used by PortfolioTracker and tests.*

---

# Per-Model Flowchart Diagrams (Connectivity Overview)

---

## MarketData Connectivity

```mermaid
flowchart TD
    EXCH[Exchange API] -->|REST/WS market data| API[ExchangeAPI]
    API -->|MarketData| DH[DataHandler]
    DH -->|MarketData| STRAT[Strategy/SignalGenerator]
    STRAT -->|Requests MarketData| DH
    TEST[Tests/Mocks] -->|Injects MarketData| DH
    DH -->|MarketData| MON[Monitoring/Analytics]
```
*Figure: MarketData flows from Exchange APIs through API clients and DataHandler to strategies, monitoring, and tests.*

---

## Ticker Connectivity
```mermaid
flowchart TD
    EXCH[Exchange API] -->|REST/WS ticker| API[ExchangeAPI]
    API -->|Ticker| DH[DataHandler]
    DH -->|Ticker wrapped in MarketData| STRAT[Strategy/SignalGenerator]
    STRAT -->|Requests Ticker| DH
    TEST[Tests/Mocks] -->|Injects Ticker| DH
```
*Figure: Ticker is a direct price/volume update, typically wrapped in MarketData for downstream use.*

---

## OrderBook Connectivity

```mermaid
flowchart TD
    EXCH[Exchange API] -->|REST/WS order book| API[ExchangeAPI]
    API -->|OrderBook| DH[DataHandler]
    DH -->|OrderBook| STRAT[Strategy/SignalGenerator]
    STRAT -->|Requests OrderBook| DH
    TEST[Tests/Mocks] -->|Injects OrderBook| DH
```
*Figure: OrderBook flows from Exchange APIs through API clients and DataHandler to strategies and tests.*

---

## FundingRate Connectivity

```mermaid
flowchart TD
    EXCH[Exchange API] -->|REST/WS funding rate| API[ExchangeAPI]
    API -->|FundingRate| DH[DataHandler]
    DH -->|FundingRate| STRAT[Strategy/SignalGenerator]
    STRAT -->|Requests FundingRate| DH
    TEST[Tests/Mocks] -->|Injects FundingRate| DH
    DH -->|FundingRate| MON[Monitoring/Analytics]
```
*Figure: FundingRate is distributed from Exchange APIs to strategies, monitoring, and tests via DataHandler.*

---

## Trade Connectivity

```mermaid
flowchart TD
    EXCH[Exchange API] -->|REST/WS trade/fill| API[ExchangeAPI]
    API -->|Trade| DH[DataHandler]
    DH -->|Trade| EH[ExecutionHandler]
    EH -->|Trade| PT[PortfolioTracker]
    PT -->|Trade| MON[Monitoring/Analytics]
    TEST[Tests/Mocks] -->|Injects Trade| PT
```
*Figure: Trade objects flow from Exchange APIs through API clients, DataHandler, ExecutionHandler, and PortfolioTracker, supporting audit, reconciliation, and analytics.*

---

# Per-Model Class Diagrams (Structure & Relationships)

---

## MarketData Class Diagram

```mermaid
classDiagram
    class MarketData {
        +str symbol
        +datetime timestamp
        +Decimal open
        +Decimal high
        +Decimal low
        +Decimal close
        +Decimal volume
        +dict~str, dict~str, Ticker~~ ticker_data
        +to_dict()
    }
    class Ticker
    MarketData o-- Ticker : ticker_data
```
*Figure: MarketData aggregates Ticker objects for advanced analytics and provides immutable market snapshots.*

---

## Ticker Class Diagram

```mermaid
classDiagram
    class Ticker {
        +str symbol
        +Decimal~optional~ price
        +Decimal~optional~ bid
        +Decimal~optional~ ask
        +Decimal~optional~ volume
        +int~optional~ timestamp
        +to_dict()
    }
```
*Figure: Ticker is a lightweight, immutable snapshot of best bid/ask, last price, and volume for a symbol.*

---

## OrderBook Class Diagram

```mermaid
classDiagram
    class OrderBook {
        +str symbol
        +list~tuple~Decimal, Decimal~~ bids
        +list~tuple~Decimal, Decimal~~ asks
        +int~optional~ timestamp
    }
```
*Figure: OrderBook contains lists of price/quantity tuples for bids and asks, representing market depth.*

---

## FundingRate Class Diagram

```mermaid
classDiagram
    class FundingRate {
        +str symbol
        +Decimal~optional~ funding_rate
        +Decimal~optional~ predicted_rate
        +Decimal~optional~ mark_price
        +Decimal~optional~ index_price
        +int~optional~ next_funding_time
        +int~optional~ timestamp
        +list~dict~str, Any~~ historical_rates
    }
```
*Figure: FundingRate models the funding and related data for a perpetual contract, with optional historical data.*

---

## Trade Class Diagram

```mermaid
classDiagram
    class Trade {
        +str id
        +str symbol
        +datetime executed_at
        +OrderSide side
        +str order_id
        +str exchange
        +str client_order_id
        +Decimal price
        +Decimal quantity
        +Decimal cost
        +Decimal fee
        +str fee_asset
        +bool~optional~ is_maker
        +int~optional~ timestamp
        +to_dict()
    }
    class OrderSide
    Trade --> OrderSide : side
```
*Figure: Trade is an immutable record of a single execution event, referencing enums for side and containing all financial details.*

---

# Per-Module Component Diagrams

---

## market.py Component Diagram

```mermaid
flowchart TD
    subgraph market.py
        MarketData
        Ticker
        OrderBook
        FundingRate
        Trade
        Order
    end
    market.py -->|uses| enums[enums.py]
    market.py -->|uses| parsing[utils/parsing.py]
    market.py -->|used by| data_handler[data_handler.py]
    market.py -->|used by| execution_handler[execution_handler.py]
    market.py -->|used by| portfolio_tracker[portfolio_tracker.py]
    market.py -->|used by| signal_generator[signal_generator.py]
```
*Figure: market.py provides core data models used throughout the engine, depending on enums and parsing utilities.*

---

## data_handler.py Component Diagram

```mermaid
flowchart TD
    subgraph data_handler.py
        DataHandler
    end
    data_handler.py -->|uses| market[market.py]
    data_handler.py -->|uses| apis[apis/]
    data_handler.py -->|uses| config[utils/config.py]
    data_handler.py -->|used by| signal_generator[signal_generator.py]
    data_handler.py -->|used by| execution_handler[execution_handler.py]
    data_handler.py -->|used by| portfolio_tracker[portfolio_tracker.py]
```
*Figure: data_handler.py manages market data ingestion, normalization, and distribution, interfacing with APIs and core models.*

---

## execution_handler.py Component Diagram

```mermaid
flowchart TD
    subgraph execution_handler.py
        ExecutionHandler
        TradeExecution
    end
    execution_handler.py -->|uses| market[market.py]
    execution_handler.py -->|uses| data_handler[data_handler.py]
    execution_handler.py -->|uses| apis[apis/]
    execution_handler.py -->|uses| portfolio_tracker[portfolio_tracker.py]
```
*Figure: execution_handler.py coordinates order execution, status tracking, and trade reconciliation.*

---

## portfolio_tracker.py Component Diagram

```mermaid
flowchart TD
    subgraph portfolio_tracker.py
        PortfolioTracker
    end
    portfolio_tracker.py -->|uses| market[market.py]
    portfolio_tracker.py -->|uses| data_handler[data_handler.py]
    portfolio_tracker.py -->|uses| apis[apis/]
```
*Figure: portfolio_tracker.py tracks balances, positions, and order status, integrating with core models and APIs.*

---

## signal_generator.py Component Diagram

```mermaid
flowchart TD
    subgraph signal_generator.py
        SignalGenerator
    end
    signal_generator.py -->|uses| market[market.py]
    signal_generator.py -->|uses| data_handler[data_handler.py]
    signal_generator.py -->|uses| apis[apis/]
```
*Figure: signal_generator.py generates trading signals based on market data and funding rates.*

---

# Per-Module State Diagrams

---

## Order State Diagram (market.py)

```mermaid
stateDiagram-v2
    [*] --> NEW
    NEW --> PARTIALLY_FILLED : partial fill
    NEW --> FILLED : full fill
    NEW --> CANCELED : cancel
    PARTIALLY_FILLED --> FILLED : full fill
    PARTIALLY_FILLED --> CANCELED : cancel
    FILLED --> [*]
    CANCELED --> [*]
```
*Figure: Order lifecycle from creation to fill or cancellation, as managed in market.py.*

---

## Trade State Diagram (market.py)

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> RECORDED : processed by PortfolioTracker
    RECORDED --> [*]
```
*Figure: Trade is immutable and typically transitions from creation to being recorded in audit or portfolio systems.*

---

## DataHandler Connection State Diagram (data_handler.py)

```mermaid
stateDiagram-v2
    [*] --> DISCONNECTED
    DISCONNECTED --> CONNECTING : start
    CONNECTING --> CONNECTED : success
    CONNECTING --> DISCONNECTED : fail
    CONNECTED --> DISCONNECTED : error/close
```
*Figure: DataHandler manages WebSocket/REST connections with clear connection state transitions.*

---

## PortfolioTracker Position State Diagram (portfolio_tracker.py)

```mermaid
stateDiagram-v2
    [*] --> NO_POSITION
    NO_POSITION --> OPEN : open trade
    OPEN --> INCREASED : add to position
    OPEN --> REDUCED : partial close
    OPEN --> CLOSED : full close
    INCREASED --> REDUCED : partial close
    INCREASED --> CLOSED : full close
    REDUCED --> INCREASED : add to position
    REDUCED --> CLOSED : full close
    CLOSED --> NO_POSITION
```
*Figure: PortfolioTracker manages position states as trades are opened, increased, reduced, or closed.*

---