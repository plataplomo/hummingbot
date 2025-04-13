# Core Architecture - CyberDeltaEngine Prototype 0.0.1

**Status: Design Overview - Refinement Needed (Revised Aug 6, 2025)**

**Note:** While this document outlines the planned core architecture, critic feedback (Aug 6) mandates an immediate focus (Aug 6-10) on stabilizing and **testing** the interactions between core components and safety systems. The priority is verifying:
*   **Core Workflow:** API Clients ↔ Data Handler ↔ Portfolio Tracker ↔ Simplified Risk Manager (Hard Limits) ↔ Execution Handler.
*   **Safety Systems Integration:** Validation, Reconciliation, and Circuit Breakers correctly interacting with the core workflow (e.g., CBs halting EH, Reconciliation using PT/APIs).

Implementation and testing of more complex architectural patterns or deferred components are secondary to achieving this foundational stability. Additionally, the critic noted potential inconsistencies in component naming (e.g., `Trading Engine` vs `Main Orchestrator`) which **must be resolved** to ensure a single, clear source of truth reflected in both documentation and code.

## 1. Overview

This document describes the core architecture of the CyberDeltaEngine (`CyberDeltaEngine`) Prototype 0.0.1. The architecture is designed to be modular, testable, and extensible, focusing initially on funding rate arbitrage strategies.

## 2. Architectural Principles

- **Modularity:** Components are designed with clear responsibilities and interfaces.
- **Asynchronous:** Leverages `asyncio` for high concurrency and non-blocking I/O.
- **Event-Driven:** Core interactions are based on events (market data, orders, signals).
- **Testability:** Components are designed for unit and integration testing.
- **Reliability:** Incorporates safety mechanisms like circuit breakers and validation.
- **Extensibility:** Allows for adding new exchanges, strategies, and components.

## 3. Core Components

```mermaid
graph TD
    subgraph CyberDeltaEngine
        direction LR

        M(Main / Engine):::core --> Cfg(Config Manager):::util
        M --> Log(Logging):::util
        M --> SM(State Manager):::util
        M --> API(API Clients):::io
        M --> DH(Data Handler):::core
        M --> PT(Portfolio Tracker):::core
        M --> SG(Signal Generator / Strategies):::core
        M --> RM(Risk Manager):::core
        M --> EH(Execution Handler):::core
        M --> BM(Balance Monitor):::core
        M --> V(Validation System):::safety
        M --> CB(Circuit Breakers):::safety

        %% Interactions
        API -->|Market Data / Account Info| DH
        API -->|Account Info / Order Status| PT
        API -->|Place/Cancel Orders| EH
        API -->|Balance Info| BM

        DH -- Market Data --> SG
        DH -- Market Data --> RM
        PT -- Positions / Balances --> RM
        PT -- Order Fills --> SG # Optional, for analysis
        SG -- Trading Signals --> RM
        RM -- Sized Opportunities --> EH
        EH -- Order Fills --> PT
        BM -- Balance Status --> RM
        V -- Validation Status --> M
        V -- Validation Status --> SG # Strategy may use validation confidence
        V ---> API # Position Reconciliation needs API access
        V ---> PT  # Position Reconciliation needs PT access
        CB -- Breaker Status --> EH
        CB -- Breaker Status --> API # API client methods check breakers
        SM -- Load/Save State --> PT
        SM -- Load/Save State --> M # Engine state if any

    end

    %% Styling
    classDef core fill:#c9d7f0,stroke:#333,stroke-width:1px;
    classDef io fill:#d5f0c9,stroke:#333,stroke-width:1px;
    classDef safety fill:#f0d9c9,stroke:#333,stroke-width:1px;
    classDef util fill:#e0e0e0,stroke:#333,stroke-width:1px;

    class M,DH,PT,SG,RM,EH,BM core
    class API io
    class V,CB safety
    class Cfg,Log,SM util
```

**Component Descriptions:**

1.  **Main / Engine (`main.py` / `engine.py`):**
    -   Initializes all components.
    -   Loads configuration and secrets securely.
    -   Starts and manages the main application loop.
    -   Coordinates high-level system state (e.g., startup, shutdown, safe mode).
    -   **Note:** Resolve naming inconsistency (Engine vs. Orchestrator).

2.  **API Clients (`apis/`):**
    -   Handles communication with exchange APIs (Hyperliquid, Backpack).
    -   Abstracts exchange-specific details behind a common `ExchangeAPI` interface.
    -   Manages authentication, rate limiting, error handling, WebSocket connections.
    -   Integrates with Circuit Breakers.

3.  **Data Handler (`core/data_handler.py`):**
    -   Subscribes to market data streams (prices, funding rates) via API Clients.
    -   Processes, normalizes, and distributes market data events.
    -   Maintains local cache/history of relevant market data.

4.  **Portfolio Tracker (`core/portfolio_tracker.py`):**
    -   Tracks current positions, balances, and open orders across exchanges.
    -   Calculates P&L (realized and unrealized).
    -   Updates state based on order fill events received from Execution Handler.
    -   Provides consistent view of portfolio state to other components.
    -   Integrates with State Manager for persistence.
    -   Interacts with Position Reconciliation system.

5.  **Signal Generator / Strategies (`strategies/`, `core/signal_generator.py`):**
    -   Contains specific trading strategy logic (e.g., `FundingRateArbitrageStrategy`).
    -   Analyzes market data received from Data Handler.
    -   Identifies potential trading opportunities (signals).
    -   Passes signals to the Risk Manager for evaluation.
    -   May use metrics from Validation System.

6.  **Risk Manager (`core/risk_manager.py`):**
    -   Receives trading signals from Strategies.
    -   Evaluates signals against risk rules and portfolio constraints.
    -   Determines appropriate position size based on **simplified hard limits** (Max Size USD, Max Exposure %, Max Leverage) for v0.0.1.
    -   Performs pre-trade checks (e.g., available balance, margin).
    -   Outputs sized opportunities to the Execution Handler.

7.  **Execution Handler (`core/execution_handler.py`):**
    -   Receives sized opportunities from Risk Manager.
    -   Places, monitors, and cancels orders via API Clients.
    -   Handles order execution logic (e.g., sequential legs for arbitrage).
    -   Manages partial fills and execution errors.
    -   Reports fill events back to the Portfolio Tracker.
    -   Checks Circuit Breaker status before placing orders.

8.  **Balance Monitor (`core/balance_monitor.py`):**
    -   Periodically checks collateral/margin levels on exchanges via API Clients.
    -   Provides balance status to Risk Manager.
    -   Can trigger alerts or actions based on low balance thresholds.

9.  **Validation System (`validation/`):**
    -   Contains subsystems like `FundingRateValidator` and `PositionReconciliation`.
    -   Performs background checks to ensure data integrity and state consistency.
    -   Reports validation status and can trigger alerts or safe mode.

10. **Circuit Breakers (`circuit_breakers/`):**
    -   Manages Circuit Breaker instances for different operations/exchanges.
    -   Tracks failures and successes to determine state (CLOSED, OPEN, HALF_OPEN).
    -   Provides status checks to Execution Handler and API Clients.

11. **State Manager (`utils/state_manager.py`):**
    -   Handles persistent saving and loading of critical system state (e.g., Portfolio Tracker data).
    -   Ensures atomic saves and provides backup/recovery mechanisms.

12. **Config Manager (`utils/config.py`, `config/`):**
    -   Loads and validates configuration from `config.yaml`.
    -   Securely loads secrets (API keys) via `SecretsManager` from outside the source tree.
    -   Provides configuration access to all components.

13. **Logging (`utils/logging_config.py`):**
    -   Configures structured logging for the application.
    -   Provides consistent logging format and output (console, file).

## 4. Data Flow Example (Funding Rate Arbitrage Signal)

1.  **Data Handler** receives funding rate updates from **API Clients** (Hyperliquid, Backpack).
2.  **Data Handler** distributes `MarketData` events.
3.  **Strategy** receives `MarketData`, identifies an arbitrage opportunity (e.g., NFD > threshold).
4.  **Strategy** generates a `TradeSignal` event.
5.  **Risk Manager** receives `TradeSignal`.
6.  **Risk Manager** checks **Portfolio Tracker** for current positions/exposure and **Balance Monitor** for collateral.
7.  **Risk Manager** applies risk rules (hard limits) and calculates position size.
8.  **Risk Manager** generates a `SizedOpportunity` event.
9.  **Execution Handler** receives `SizedOpportunity`.
10. **Execution Handler** checks **Circuit Breakers** for relevant exchanges/operations.
11. **Execution Handler** places orders via **API Clients**.
12. **API Clients** confirm order placement/fills.
13. **Execution Handler** receives fill confirmations.
14. **Execution Handler** generates `OrderFill` events.
15. **Portfolio Tracker** receives `OrderFill` events and updates positions/balances.
16. **Portfolio Tracker** saves state periodically via **State Manager**.

## 5. Key Design Considerations

- **State Management:** Ensuring consistent and recoverable state is paramount. The `PortfolioTracker` is the primary owner of trading state, persisted by the `StateManager`.
- **Error Handling:** Robust error handling at API, execution, and component levels is critical. Circuit breakers provide a system-level safety net.
- **Concurrency:** Careful management of `asyncio` tasks and shared state is required to prevent race conditions.
- **Testing:** The modular design facilitates unit testing. **Integration and failure scenario testing are now the top priority** to validate component interactions and resilience.

## 6. Implementation Strategy

1. **Minimal Viable Product**: Focus on core funding rate arbitrage between Hyperliquid and Backpack
2. **Sequential Execution**: Implement sequential order execution with comprehensive failure handling
3. **Robust State Persistence**: Use atomic file operations and validation for state snapshots
4. **Comprehensive Error Handling**: Error handling at all levels with specific recovery strategies
5. **Detailed Logging**: Extensive logging for debugging, monitoring, and recovery
6. **Balance Monitoring**: Implement monitoring with alerts rather than automated transfers for 0.0.1

## 7. Scope Boundaries for Prototype 0.0.1

### 7.1 In Scope

- Hyperliquid exchange integration (complete)
- Backpack exchange integration for orderbook/execution
- Hyperliquid funding rate strategy (primary)
- Backup strategy: Hyperliquid vs Backpack spot if funding rate access is limited
- Concrete risk management with fixed limits and checks
- Sequential execution with comprehensive error handling
- Robust JSON file-based state persistence with validation
- Command-line monitoring interface
- Comprehensive logging

### 7.2 Out of Scope

- Automated cross-exchange transfers (manual transfers with alerts instead)
- Complex database integration (Redis, TSDB)
- Advanced risk models beyond simple Kelly and fixed caps
- Parallel execution
- Web dashboard
- Multiple strategy types
- Machine learning components
- Complex statistical models 