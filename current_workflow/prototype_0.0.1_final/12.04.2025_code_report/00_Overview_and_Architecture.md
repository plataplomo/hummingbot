# CyberDeltaEngine: Code Review Report (v0.0.1) - Overview and Architecture

## 1. Project Overview

**Project Name:** CyberDeltaEngine
**Version Goal:** v0.0.1 - Stable Funding Rate Arbitrage Bot (Hyperliquid-Perp vs Backpack-Spot/Perp - *Backpack Perp availability TBC*)
**Core Domain:** Automated, asynchronous (`asyncio`-based) Python trading engine.
**Primary Strategy (v0.0.1):** Delta-neutral funding rate arbitrage, capitalizing on discrepancies between Hyperliquid and Backpack funding rates.
**Key Priorities:** Robustness, Correctness, Security, Testability, Maintainability. The system handles financial assets, demanding high reliability and fail-safe design.

## 2. Current Architecture (as implemented)

The system architecture centers around an asynchronous event loop processing market data and generating trading signals. Components are designed for specific responsibilities, promoting modularity.

**High-Level Flow:**

```mermaid
graph TD
    subgraph Input & Configuration
        CLI[CLI Arguments] --parsed by--> Main
        ConfigFile[config.yaml] --loaded by--> ConfigMgr
        SecretsFile[secrets.yaml] --loaded by--> ConfigMgr
    end

    subgraph Core Processing Pipeline
        Main[main.py] --Initializes & Starts--> Engine(Engine Core)
        DataH(DataHandler) == Fetches/Subscribes ==> APIClients{API Clients}
        DataH -- Raw Market Data --> Engine
        Engine -- Routes Data --> Strategies{Strategies}
        Strategies -- Identified Opportunities --> OpportunityAnalysis[Opportunity Analysis]
        OpportunityAnalysis -- Generates --> ArbitrageOpportunity((Arbitrage Opportunity))
        OpportunityAnalysis -- Converts & Sends --> SignalQ(PrioritySignalQueue)
        SignalQ -- Prioritized Signal --> RiskM(RiskManager)
        RiskM -- Checks & Sizes --> SizedOpportunity((Sized Opportunity))
        RiskM -- Approved Signal --> ExecH(ExecutionHandler)
        ExecH -- Places/Manages Orders --> APIClients
        ExecH -- Reports Fills --> PortT(PortfolioTracker)
    end

    subgraph State & Persistence
        PortT -- Current State --> RiskM
        PortT -- Current State --> Strategies
        PortT -- Updates From --> ExecH
        PortT -- Reconciles With --> APIClients
        StateManager[StateManager] <-- Saves/Loads State --> PortT
        StateManager <-- Saves/Loads State --> DataH?(Potentially Cache)
        StateManager <-- Saves/Loads State --> ExecH?(Order History)
    end

    subgraph Exchange Interaction
        APIClients --> HLExAPI(HyperliquidAPI)
        APIClients --> BPExAPI(BackpackAPI)
        HLExAPI <-- WebSocket/REST --> HLEx(Hyperliquid Exchange)
        BPExAPI <-- WebSocket/REST --> BPEx(Backpack Exchange)
    end

    subgraph Safety & Validation Systems
        RiskM -- Checks --> CB(CircuitBreakerSystem)
        ExecH -- Checks --> CB
        PortT -- Initiates --> PSR(PositionReconciliationSystem)
        PSR -- Verifies State --> APIClients
        PSR -- Corrects State --> PortT
        Strategies -- Uses --> FRV(FundingRateValidator)
        CB -- Monitors Health --> APIClients
    end

    subgraph Utilities
        ConfigMgr[Config / Secrets] <-- Used By --> Main
        ConfigMgr <-- Used By --> All Components
        Logging(Logging Setup) <-- Used By --> All Components
        StateManager <-- Used By --> Components needing persistence
        SymbolMapper[SymbolMapper] <-- Used By --> ExecH
        SymbolMapper <-- Used By --> DataH
    end

    style Main fill:#f9f,stroke:#333,stroke-width:2px
    style Engine fill:#ccf,stroke:#333,stroke-width:2px
    style DataH fill:#cdf,stroke:#333,stroke-width:2px
    style Strategies fill:#cfc,stroke:#333,stroke-width:2px
    style OpportunityAnalysis fill:#cec,stroke:#333,stroke-width:1px
    style SignalQ fill:#fcf,stroke:#333,stroke-width:2px
    style RiskM fill:#fcc,stroke:#333,stroke-width:2px
    style ExecH fill:#ffc,stroke:#333,stroke-width:2px
    style PortT fill:#cff,stroke:#333,stroke-width:2px
    style APIClients fill:#eee,stroke:#333,stroke-width:1px
    style SafetySystems fill:#fde,stroke:#333,stroke-width:1px
    style ArbitrageOpportunity fill:#ddd,stroke:#666,stroke-width:1px,stroke-dasharray: 5 5
    style SizedOpportunity fill:#ddd,stroke:#666,stroke-width:1px,stroke-dasharray: 5 5
    style StateManager fill:#ddd,stroke:#333,stroke-width:1px
    style ConfigMgr fill:#ddd,stroke:#333,stroke-width:1px
    style Logging fill:#ddd,stroke:#333,stroke-width:1px
    style SymbolMapper fill:#ddd,stroke:#333,stroke-width:1px
```

## 3. Key Component Responsibilities & Details

*   **`main.py`**:
    *   **Entry Point:** Orchestrates application startup and shutdown.
    *   **Initialization:** Loads configuration (`Config`), sets up logging (`setup_logging`), initializes core components (`DataHandler`, `PortfolioTracker`, `ExecutionHandler`, `RiskManager`, `SignalQueue`, `Strategies`, `CircuitBreakerSystem`, etc.).
    *   **Lifecycle Management:** Starts component tasks (e.g., `DataHandler.run()`) and manages the main `asyncio` event loop. Handles OS signals (`SIGINT`, `SIGTERM`) for graceful shutdown.
    *   **Example Snippet (Conceptual Initialization):**
        ```python
        # main.py (Conceptual)
        async def main():
            config = Config("config.yaml", "secrets.yaml")
            setup_logging(config)
            # ... Initialize API clients, symbol mapper ...
            portfolio_tracker = PortfolioTracker(config, api_clients)
            data_handler = DataHandler(config, api_clients, symbol_mapper)
            signal_queue = PrioritySignalQueue(config)
            circuit_breakers = CircuitBreakerSystem(config, api_clients)
            risk_manager = RiskManager(config, portfolio_tracker, circuit_breakers)
            execution_handler = ExecutionHandler(config, api_clients, portfolio_tracker, circuit_breakers, symbol_mapper)
            strategy = FundingRateArbitrageStrategy(config, data_handler, signal_queue, risk_manager) # Simplified linkage

            engine = Engine(config, data_handler, [strategy], signal_queue, risk_manager, execution_handler) # Or similar orchestration

            await engine.run() # Start the main loop and component tasks
        ```

*   **`Engine` (`cyberdelta/core/engine.py`)**:
    *   **Orchestration Hub:** Intended to manage the overall flow, routing data to strategies and potentially coordinating signals to execution.
    *   **Current State:** Appears somewhat bypassed; the `FundingRateArbitrageStrategy` directly interacts with `DataHandler` and `SignalQueue`/`RiskManager`. The `Engine`'s role in active data routing seems minimal in the current implementation. It primarily manages the lifecycle of components.
    *   **Potential Role:** Could enforce a cleaner separation by receiving normalized data from `DataHandler` and explicitly passing it to registered strategies, then receiving signals/opportunities back for queuing/risk assessment.

*   **`DataHandler` (`cyberdelta/core/data_handler.py`)**:
    *   **Market Data Interface:** Connects to exchange WebSockets (Hyperliquid, Backpack).
    *   **Subscriptions:** Manages subscriptions to necessary data streams (tickers, order books, funding rates).
    *   **Normalization:** Parses exchange-specific data formats into standardized internal models (`Ticker`, `OrderBook`, `FundingRate` from `cyberdelta/core/models.py`).
    *   **Data Access:** Provides methods like `get_ticker(symbol, exchange)`, `get_funding_rate(symbol, exchange)`.
    *   **Data Freshness:** Tracks the timeliness of received data to ensure strategies act on current information.

*   **`FundingRateArbitrageStrategy` (`cyberdelta/strategies/funding_rate_arbitrage.py`)**:
    *   **Arbitrage Logic:** Contains the core algorithm for identifying funding rate arbitrage opportunities between Hyperliquid and Backpack.
    *   **Data Consumption:** Directly calls `DataHandler` methods to get required funding rates and prices.
    *   **Opportunity Generation:** Creates `ArbitrageOpportunity` objects when criteria (e.g., funding differential threshold, price spread) are met. Includes calculation of potential profit and utility score.
    *   **Signal Emission:** Converts `ArbitrageOpportunity` into `TradeSignal` and sends it to the `PrioritySignalQueue`.
    *   **Example Snippet (Conceptual Opportunity Check):**
        ```python
        # cyberdelta/strategies/funding_rate_arbitrage.py (Conceptual)
        async def check_opportunity(self):
            hl_rate = self.data_handler.get_funding_rate("BTC-PERP", "hyperliquid")
            bp_rate = self.data_handler.get_funding_rate("BTC-PERP", "backpack") # Assuming Backpack perp
            hl_price = self.data_handler.get_ticker("BTC-PERP", "hyperliquid").price
            bp_price = self.data_handler.get_ticker("BTC-PERP", "backpack").price # Assuming Backpack perp

            if hl_rate and bp_rate and hl_price and bp_price:
                # ... calculate differential, check thresholds ...
                if abs(hl_rate.funding_rate - bp_rate.funding_rate) > self.min_rate_diff:
                    opportunity = ArbitrageOpportunity(...)
                    trade_signal = self._create_signal_from_opportunity(opportunity)
                    await self.signal_queue.add_signal(trade_signal, opportunity.utility_score)
        ```

*   **`PrioritySignalQueue` (`cyberdelta/core/signal_queue.py`)**:
    *   **Signal Management:** Prioritizes incoming `TradeSignal` objects based on utility score (higher score = higher priority). Uses a min-heap with negative scores for max-heap behavior.
    *   **Expiration Handling:** Calculates signal expiration times (potentially based on confidence) and removes expired signals (`_clean_expired_signals`).
    *   **Queue Sizing:** Enforces `max_queue_size` by removing lowest-priority signals (`_trim_queue`).
    *   **Circuit Breaker Check:** Verifies `CircuitBreakerSystem` status before adding (`_check_circuit_breakers_pre_add`) or providing (`_check_circuit_breakers_post_get`) signals.
    *   **Interface:** Provides `add_signal`, `get_next_signal`, `peek_next_signal`.

*   **`RiskManager` (`cyberdelta/core/risk_manager.py`)**:
    *   **Pre-Trade Validation:** Receives prioritized signals from `SignalQueue`.
    *   **Checks:** Validates signals against:
        *   Portfolio constraints (max exposure per asset/total, available balance from `PortfolioTracker`).
        *   Risk limits defined in configuration (e.g., max drawdown).
        *   `CircuitBreakerSystem` status.
    *   **Sizing:** Calculates the appropriate order size based on risk parameters (e.g., Kelly Criterion fraction, max position size).
    *   **Output:** Passes validated and sized signals/orders (e.g., `SizedOpportunity` or directly parameters) to the `ExecutionHandler`.
    *   **Example Config Snippet:**
        ```yaml
        # config.yaml (Risk Section)
        risk_manager:
          max_total_exposure_usd: 10000
          max_position_size_usd: 5000
          kelly_fraction: 0.1
          max_drawdown_pct: 0.15 # 15%
          # ... other limits ...
        ```

*   **`ExecutionHandler` (`cyberdelta/core/execution_handler.py`)**:
    *   **Order Placement:** Translates validated signals/instructions into exchange-specific order requests using `API Clients`. Uses `SymbolMapper` for correct symbol formatting.
    *   **Order Lifecycle Management:** Places orders (e.g., `create_order`), monitors their status via WebSocket updates or polling, handles fills, cancellations, and potential errors/retries.
    *   **Fill Processing:** Updates `PortfolioTracker` upon receiving order fill notifications.
    *   **Circuit Breaker Check:** Verifies `CircuitBreakerSystem` status before placing orders.
    *   **Atomicity Concern:** Ensuring atomicity or consistency across legs of an arbitrage trade (e.g., placing both long and short orders successfully) is critical and complex.

*   **`PortfolioTracker` (`cyberdelta/core/portfolio_tracker.py`)**:
    *   **Internal State:** Maintains the application's view of current asset balances and open positions across all connected exchanges. Stores data using internal models (`Balance`, `Position`).
    *   **State Updates:** Updated primarily by fills reported from `ExecutionHandler`. Can also periodically fetch/reconcile state directly via `API Clients`.
    *   **Data Source:** Provides consistent state information to `RiskManager` and `Strategies`.
    *   **Persistence:** Relies on `StateManager` to save and load its state for recovery.

*   **`API Clients` (`cyberdelta/apis/`)**:
    *   **Exchange Interface:** Abstract base class (`BaseAPIClient`) and concrete implementations (`HyperliquidAPI`, `BackpackAPI`).
    *   **Functionality:** Handles authentication, WebSocket connections (for real-time data/updates), REST requests (for actions like placing orders, fetching balances), rate limiting logic, error handling, and parsing responses into internal models.

*   **`Safety Systems` (`cyberdelta/validation/`)**:
    *   **`CircuitBreakerSystem`:** Monitors exchange health (API errors, WebSocket disconnects), market conditions (volatility spikes), or portfolio metrics (drawdown). Can halt specific symbols, exchanges, or all trading activity. State is checked by `SignalQueue`, `RiskManager`, `ExecutionHandler`.
    *   **`PositionReconciliationSystem`:** Periodically compares `PortfolioTracker`'s state with actual balances/positions fetched from exchanges via `API Clients`. Logs discrepancies and potentially triggers alerts or corrective actions (manual intervention usually preferred).
    *   **`FundingRateValidator`:** (Less integrated currently) Tracks predicted vs. actual funding payments to verify strategy assumptions.

*   **`Utilities` (`cyberdelta/utils/`)**:
    *   **`Config`:** Loads and provides access to `config.yaml` and `secrets.yaml`.
    *   **`StateManager`:** Handles saving/loading component state (likely using JSON or Pickle) to files for persistence across restarts.
    *   **`SymbolMapper`:** Translates between internal canonical symbols (e.g., `BTC-USD-PERP`) and exchange-specific formats (e.g., `BTC-PERP` on Hyperliquid, `SOL-USD` on Backpack).
    *   **Logging:** Standardized logging setup (likely using `loguru`).

## 4. Architectural Observations & Potential Issues

*   **Data Flow Clarity:** The exact flow and transformation of data/objects between `Strategy -> SignalQueue -> RiskManager -> ExecutionHandler` needs rigorous definition. Is it `ArbitrageOpportunity -> TradeSignal -> SizedOpportunity -> OrderParams`? Ensuring consistency is key. The `Engine`'s role needs solidification or removal if bypassed.
*   **State Management:** Explicitly define which component owns which piece of state and how persistence/recovery works for each (Portfolio, open orders, data caches). Ensure `StateManager` interactions are robust.
*   **Execution Atomicity:** Arbitrage requires near-simultaneous execution on multiple legs. The current `ExecutionHandler` needs careful design to minimize legging risk (one side fills, the other fails). This might involve sophisticated order placement logic or retry mechanisms.
*   **Configuration Access:** Using `config.get("deeply.nested.key")` is functional but can be brittle. Consider using typed configuration models (e.g., Pydantic) for better validation and IDE support.
*   **Error Handling Granularity:** Ensure error handling is robust at API boundaries, within data processing, and during order execution. Distinguish between transient (retryable) and permanent errors.
*   **Testing Strategy:** Requires comprehensive unit tests for logic (strategy, risk), integration tests for component interactions (e.g., Signal -> Risk -> Execution), and potentially end-to-end tests against mock exchange servers.
*   **Backpack API:** Verify availability and behavior of Backpack perpetuals API; initial focus might need adjustment to Spot if Perps are not ready/stable.

*(This overview provides a detailed snapshot based on the codebase structure. Specific implementation details within each component are reviewed in subsequent sections.)*
