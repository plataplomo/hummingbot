# Current Workflow & Progress (As of April 6th, 2025) - Detailed

This document summarizes the current state of the CyberDeltaEngine implementation based on the prototypes created, providing more detail on component status and future work.

## Implemented Components (Prototypes - Detailed Status)

- **Configuration (`src/config/`)**: **Implemented.** `settings.py` loads configuration from `config.yaml` and secrets from `.env`. Provides access methods (`get_strategy_params`, `get_secret`, etc.) and basic error handling for missing files/keys.
- **Logging (`src/utils/logging_config.py`)**: **Implemented.** `setup_logging` function configures Python's logging based on settings. Supports console and rotating file output. Basic suppression for noisy libraries included.
- **Core Data Models (`src/core/models.py`)**: **Implemented.** Essential dataclasses defined (e.g., `Ticker`, `OrderBook`, `Order`, `Position`, `Balance`, `ArbitrageOpportunity`). May need refinement as exchange-specific details emerge.
- **API Base Class (`src/apis/base.py`)**: **Implemented.** `ExchangeAPI` ABC provides structure for REST requests (`_request`), WebSocket handling (connect, listen, route), and defines the required interface for specific clients. Includes basic `aiohttp` session management.
- **Hyperliquid API Client (`src/apis/hyperliquid.py`)**: **Partial Prototype.** Inherits `ExchangeAPI`. WS routing structure exists. `fetch_funding_rate` has a *placeholder* implementation using the assumed `/info` POST endpoint and assumed response structure. **TODO:** Implement actual REST/WS logic, parsing for all endpoints, and robust wallet signing (`_sign_request`). *Manual fix for extraneous tag might still be needed.*.
- **Data Handler (`src/core/data_handler.py`)**: **Prototype.** `DataHandler` class structure exists. `subscribe_to_streams` attempts subscriptions using *example* topic names. Message handlers (`_handle_ticker_message`) have *placeholder parsing logic*. **TODO:** Implement correct topic names per exchange, implement robust parsing logic for WS messages, integrate tightly with API client WebSocket routing, potentially add caching/data access improvements.
- **Portfolio Tracker (`src/core/portfolio_tracker.py`)**: **Prototype.** Holds state dictionaries. `update_*` methods exist with basic locking. `load_initial_state` calls API client methods which are currently *not implemented*. **TODO:** Implement actual state updates from WS user streams (ideal) or reliable polling, add PnL calculation logic and history.
- **Signal Generator (`src/core/signal_generator.py`)**: **Prototype.** `SignalGenerator` calculates `ArbitrageOpportunity` objects periodically. Uses *placeholder calculations* for NFD, costs, volatility, and Utility. Fetches data via *direct, suboptimal, unsafe API calls*. **TODO:** Implement correct mathematical formulas from docs, use `DataHandler` for efficient data access, estimate costs/slippage properly.
- **Risk Manager (`src/core/risk_manager.py`)**: **Prototype.** `RiskManager` structure exists. Performs checks using *placeholder logic* for dynamic VaR, Kelly sizing, margin constraints, and pre-trade risk. **TODO:** Implement actual VaR/CVaR calculations, portfolio Kelly sizing, margin requirement estimation, collateral transfer constraint checks.
- **Execution Handler (`src/core/execution_handler.py`)**: **Basic Prototype.** `ExecutionHandler` takes opportunities and places orders sequentially using *basic market orders*. **High legging risk.** **TODO:** Implement robust, near-atomic execution (parallel placement, limit orders, IOC/FOK), partial fill handling, slippage checks, error compensation logic.
- **Adaptation Loop (`src/core/adaptation_loop.py`)**: **Stub.** `AdaptationLoop` class structure exists. Runs periodically but uses *placeholder logic* for performance metric calculation and parameter adjustment. **TODO:** Implement actual performance metric calculation (requires PnL history from `PortfolioTracker`), implement mechanism to safely modify parameters in relevant live components (e.g., `RiskManager`).
- **Main Application (`src/main.py`)**: **Implemented.** Initializes components, handles startup/shutdown, runs component tasks (`DataHandler`, `PortfolioTracker`, `StrategyLoop`, `AdaptationLoop`), orchestrates the basic Signal -> Risk -> Execute flow. Error handling for component failures needs refinement.

## Current Workflow Diagram (Refined)

```mermaid
flowchart TD
    subgraph Main Orchestration [main.py: TradingBot]
        direction LR
        Init(Initialize Components) --> RunTasks(Run Async Tasks)
        RunTasks --> StrategyLoopTask[(Strategy Loop Task)]
        RunTasks --> DataHandlerTask[(Data Handler Task)]
        RunTasks --> PortfolioTrackerTask[(Portfolio Tracker Task)]
        RunTasks --> AdaptationLoopTask[(Adaptation Loop Task)]
        SignalHandler[Signal Handler (Ctrl+C)] -.-> StopBot(Stop Method)
        StrategyLoopTask -- Failure --> StopBot
        StopBot --> CancelTasks(Cancel Tasks)
        StopBot --> CloseAPIs(Close API Clients)
    end

    subgraph Strategy Loop [TaskSL: _strategy_loop]
       direction TB
       SL_Start{Start Cycle} --> SG_Run(Call SignalGenerator.run)
       SG_Run -- Yields List<Opp> --> Assess(Assess Opportunities?)
       Assess -- Yes --> RM_Assess(Call RiskManager.assess_and_filter)
       RM_Assess -- Returns ViableOpp[] --> ExecuteCheck{Any Viable?}
       ExecuteCheck -- Yes --> EH_Exec(Call ExecutionHandler.execute)
       EH_Exec --> Cooldown(Wait Cooldown)
       Cooldown --> SL_Start
       ExecuteCheck -- No --> SL_Start
       Assess -- No --> SL_Start
    end

    subgraph Data Handling [TaskDH: DataHandler]
        direction TB
        DH_Run(Run Method) --> DH_Sub(Subscribe to Streams)
        DH_Sub --> API_Sub(Call APIClient.subscribe)
        API_Sub --> WS_Listen(API Client WS Listener)
        WS_Listen -- WS Msg --> DH_Route(Call DH Message Handler)
        DH_Route -- Parses --> UpdateState(Update Internal State e.g., self.tickers)
        DH_Run -. Waits .-> StopEventDH(Stop Event)
    end

    subgraph State Tracking [TaskPT: PortfolioTracker]
         direction TB
         PT_Run(Run Method) --> PT_Load(Load Initial State)
         PT_Load --> API_Fetch(Call APIClient.get_balances etc.)
         API_Fetch --> PT_UpdateInitial(Update Internal State)
         PT_Run -. Waits .-> StopEventPT(Stop Event)
         # Ideal future: WS_UserStream --> PT_UpdateLive(Live Update State)
    end

    # Component Interactions
    SG_Run --> DataHandlerTask # Reads data
    RM_Assess --> PortfolioTrackerTask # Reads state
    EH_Exec --> PortfolioTrackerTask # Updates order state
    EH_Exec --> API_Clients # Places orders
    AdaptationLoopTask --> PortfolioTrackerTask # Reads state (for PnL)
    AdaptationLoopTask -.-> RM_Assess # Modifies params (future)

    API_Clients[API Clients] <--> Exchanges[(Exchanges HL, BP, PX)]

    style Init fill:#f9f
    style RunTasks fill:#f9f
    style StrategyLoopTask fill:#fff
    style DataHandlerTask fill:#ccf
    style PortfolioTrackerTask fill:#eee
    style AdaptationLoopTask fill:#ddf
```
*Diagram shows task orchestration and key interactions. Data reads/writes shown conceptually.* 

## Next Steps & Future Plans (Detailed)

1.  **Implement API Clients:**
    *   **Hyperliquid:** Implement public data endpoints (`fetch_ticker`, `fetch_order_book`, `fetch_trades`), implement `_sign_request` using wallet private key (e.g., via `eth_account`), implement authenticated endpoints (`get_balances`, `get_positions`, `place_order`, `cancel_order`, `withdraw`). Implement WS message parsing/routing.
    *   **Backpack:** Create `BackpackAPI` class. Implement ED25519 signing (`_sign_request`). Implement all required public and private REST endpoints. Implement WS connection, subscription (likely needs authentication), and message parsing.
    *   **Paradex:** Create `ParadexAPI` class. Implement StarkNet wallet signing (`_sign_request`). Implement relevant REST/WS endpoints.
2.  **Refine Data Handling:**
    *   Implement exchange-specific parsing logic in `DataHandler._handle_ticker_message`, `_handle_orderbook_message`, etc., populating internal state correctly.
    *   Implement subscriptions to necessary WS streams (tickers, order books, trades, **user data** like orders, fills, balances).
    *   Modify `SignalGenerator`, `RiskManager` to efficiently read cached/streamed data from `DataHandler` instead of direct polling.
3.  **Implement Core Calculations:**
    *   `SignalGenerator`: Implement basis volatility calculation, realistic cost estimation (fees from config, dynamic slippage based on order book data from `DataHandler`), use actual formulas from `.tex` docs.
    *   `RiskManager`: Implement portfolio VaR/CVaR calculation (requires position data, covariance matrix estimation), portfolio Kelly sizing, margin requirement estimation per exchange, collateral availability checks (considering transfers).
    *   `AdaptationLoop`: Implement Sharpe ratio or other metric calculation (requires PnL history in `PortfolioTracker`), implement safe mechanism to update live parameters in `RiskManager` or `SignalGenerator`.
4.  **Enhance Execution Handling:**
    *   Implement parallel leg placement for cross-exchange trades.
    *   Use limit orders with timeouts (IOC/FOK where appropriate) instead of just market orders.
    *   Monitor order status updates (via WS or polling) after placement.
    *   Implement logic for handling partial fills (e.g., adjust remaining legs, cancel).
    *   Implement basic slippage check post-fill.
    *   Develop robust error handling and compensation logic (e.g., market close stuck leg if counterpart fails).
5.  **Implement Collateral Management (`CollateralManager` component):**
    *   Create `CollateralManager` class.
    *   Implement logic to calculate target collateral levels per exchange.
    *   Implement dynamic path/bridge selection algorithm (using cost function from config).
    *   Integrate with bridge APIs (placeholder `BridgeAPI` client needed) for quotes/execution.
    *   Implement transfer execution logic via API clients (`withdraw`, potentially deposit confirmation checks).
    *   Implement transfer monitoring, timeout handling, retries, and contingency logic (borrowing).
    *   Integrate calls to `CollateralManager` from `RiskManager` (pre-trade check) and potentially a periodic rebalancing loop.
6.  **Refine Portfolio Tracking:**
    *   Implement realized/unrealized PnL calculation.
    *   Store historical PnL data needed for `AdaptationLoop`.
    *   Ensure state is accurately updated from WS user streams or reliable polling of `get_balances`, `get_positions`, `get_open_orders`.
7.  **Develop Testing Infrastructure:** Execute the plan in `docs/current_workflow/testing_plan.md` (setup tools, write unit/integration tests).
8.  **Build CLI/Monitoring Frontend:** Create a simple `asyncio`-compatible CLI or implement the web dashboard outlined in `docs/frontend_plan.md`.
9.  **Deployment Strategy:** Research and document server requirements, Dockerization process, process supervision (e.g., `systemd`, `supervisor`), and remote monitoring/alerting.
10. **ML Integration (Post-Validation):** Focus remains on validating the rule-based system first.

## Open Questions / Challenges (Expanded)

*   **API Specifics:** *Critical priority.* Need exact WS topic names, auth methods (HL/PX signing details, BP timestamp/window rules), rate limit details per endpoint, specific error code meanings, order placement parameter nuances (e.g., time-in-force options, post-only), withdrawal parameters (networks, fees, memos).
*   **Wallet Interaction:** Secure storage and use of private keys. Choosing appropriate libraries (`eth_account` for EVM, StarkNet libraries for Paradex). Handling nonce management for DEX transactions.
*   **Bridge Integration:** Which bridges offer reliable APIs? How to query executable quotes vs indicative? How to monitor cross-chain transaction finality? Handling bridge downtime/upgrades. Gas fee estimation for on-chain steps.
*   **Data Synchronization/Latency:** Measuring and potentially mitigating latency differences between exchanges. Handling stale data during NFD calculation. Impact of clock skew.
*   **Execution Atomicity:** Designing robust execution logic (e.g., using check-then-execute pattern, timeouts, rapid cancellation) to minimize risk of getting only one leg filled, especially during high volatility.
*   **Error Handling & Recovery:** Defining specific recovery procedures for different failures (API errors, disconnects, failed orders, stuck transfers, calculation errors). Implementing state persistence/recovery on restart.
*   **Resource Management:** Monitoring memory/CPU usage, managing number of concurrent `asyncio` tasks, optimizing `aiohttp` session usage. 