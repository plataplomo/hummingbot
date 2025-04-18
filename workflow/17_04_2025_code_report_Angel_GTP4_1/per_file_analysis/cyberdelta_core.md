# cyberdelta/core/ — Per-Folder Analysis

---

## backtesting.py
**Purpose:**
Implements a unified backtesting framework for trading strategies, supporting data loading, strategy execution, performance metrics calculation, and results visualization. Enables robust evaluation of strategies before live deployment.

```mermaid
flowchart TD
    A[Initialize Engine] --> B[Load Data]
    B --> C[Run Strategy]
    C --> D[Collect Results]
    D --> E[Save/Visualize Results]
```

```mermaid
sequenceDiagram
    participant Engine as BacktestEngine
    participant Strategy as BacktestStrategy
    participant Results as BacktestResultsHandler
    Engine->>Strategy: initialize/update
    Strategy-->>Engine: Trade signals
    Engine->>Results: Save/visualize
```

**Summary:**
- Inputs: Historical data, strategy instance, config.
- Outputs: Backtest results, metrics, visualizations.
- Dependencies: Pandas, Numpy, Decimal, ResultsHandler.
- Critical Path: Ensures strategies are robust before live trading.

---

## execution_handler.py
**Purpose:**
Executes trades reliably across multiple exchanges, handling order placement, monitoring, partial fills, compensation, retries, and circuit breaker integration. Central to safe and robust trade execution.

```mermaid
flowchart TD
    A[Receive Opportunity] --> B[Place Orders]
    B --> C[Monitor Status]
    C --> D[Handle Fills/Errors]
    D --> E[Update State/PNL]
```

```mermaid
sequenceDiagram
    participant Handler as ExecutionHandler
    participant API as ExchangeAPI
    participant Portfolio as PortfolioTracker
    Handler->>API: Place/monitor orders
    API-->>Handler: Order status/fills
    Handler->>Portfolio: Update positions
```

**Summary:**
- Inputs: Sized opportunities, config, portfolio state.
- Outputs: Executed trades, updated positions, error handling.
- Dependencies: ExchangeAPI, PortfolioTracker, CircuitBreakerSystem.
- Critical Path: Core to safe, reliable execution in production.

---

## signal_queue.py
**Purpose:**
Implements a priority queue for trade signals, supporting expiration, utility-based ordering, and integration with circuit breakers. Ensures only the best, valid signals are considered for execution.

```mermaid
flowchart TD
    A[Add Signal] --> B[Check Priority/Expiration]
    B --> C[Queue/Reject]
    C --> D[Pop/Process Signal]
```

```mermaid
sequenceDiagram
    participant Queue as PrioritySignalQueue
    participant Strategy as Strategy
    participant Handler as ExecutionHandler
    Strategy->>Queue: Add signal
    Queue-->>Handler: Provide next signal
```

**Summary:**
- Inputs: Trade signals, arbitrage opportunities.
- Outputs: Prioritized, valid signals for execution.
- Dependencies: Config, CircuitBreakerSystem.
- Critical Path: Ensures only actionable, safe signals are executed.

---

## strategy_manager.py
**Purpose:**
Manages the lifecycle, configuration, and execution of multiple trading strategies. Coordinates registration, enabling/disabling, data distribution, and signal collection.

```mermaid
flowchart TD
    A[Register Strategies] --> B[Enable/Disable]
    B --> C[Distribute Data]
    C --> D[Collect Signals]
    D --> E[Track Performance]
```

```mermaid
sequenceDiagram
    participant Manager as StrategyManager
    participant Strategy as Strategy
    participant Queue as PrioritySignalQueue
    Manager->>Strategy: Distribute data
    Strategy-->>Manager: Trade signals
    Manager->>Queue: Add signals
```

**Summary:**
- Inputs: Strategies, market data, config.
- Outputs: Enabled strategies, collected signals, performance metrics.
- Dependencies: ExecutionHandler, PortfolioTracker, RiskManager, PrioritySignalQueue.
- Critical Path: Central to orchestrating strategy operations.

---

## portfolio_tracker.py
**Purpose:**
Tracks and manages the current state of the portfolio, including balances, positions, orders, and P&L across exchanges. Supports reconciliation, exposure metrics, and integration with APIs.

```mermaid
flowchart TD
    A[Register API Clients] --> B[Track Balances/Positions]
    B --> C[Update State]
    C --> D[Reconcile/Report]
```

```mermaid
sequenceDiagram
    participant Tracker as PortfolioTracker
    participant API as ExchangeAPI
    participant Engine as Engine/Manager
    API-->>Tracker: Balances/positions/orders
    Tracker-->>Engine: Portfolio state, metrics
```

**Summary:**
- Inputs: API data, trades, config.
- Outputs: Portfolio state, exposure, P&L, reconciled data.
- Dependencies: ExchangeAPI, Config, Decimal.
- Critical Path: Accurate portfolio state is essential for risk and execution.

---

## risk_manager.py
**Purpose:**
Assesses and sizes trades based on risk parameters, validates opportunities, enforces position and exposure limits, and calculates risk metrics. Implements Kelly criterion and portfolio-level controls.

```mermaid
flowchart TD
    A[Validate Opportunity] --> B[Size Position]
    B --> C[Check Constraints]
    C --> D[Apply Controls]
    D --> E[Return Sized Opportunity]
```

```mermaid
sequenceDiagram
    participant Risk as RiskManager
    participant Portfolio as PortfolioTracker
    participant Handler as ExecutionHandler
    Handler->>Risk: Validate/size opportunity
    Risk-->>Handler: Sized opportunity
    Risk->>Portfolio: Query state
```

**Summary:**
- Inputs: Arbitrage opportunities, portfolio state, config.
- Outputs: Sized, validated opportunities for execution.
- Dependencies: PortfolioTracker, Config, Decimal.
- Critical Path: Prevents overexposure and enforces risk discipline.

---

## (Other files)
For brevity, additional files (e.g., data_handler.py, engine.py, models/, execution/, balance_monitor.py, results.py, symbol_mapper.py, trade_executor.py, backtesting/, data_manager.py) should be analyzed and documented in the same structured format as above, ensuring:
- Purpose summary
- Key logic flowchart
- Sequence diagram of interactions
- Brief summary of inputs, outputs, dependencies, and critical path
- Notation of any critical risks or rule compliance issues

---

## __init__.py
**Purpose:**
Initializes the core package and exposes key classes/functions for external use.

```mermaid
flowchart TD
    A[Import Key Symbols] --> B[Define __all__]
    B --> C[Expose API]
```

```mermaid
sequenceDiagram
    participant Init as __init__.py
    participant User as Importer
    User->>Init: Import symbol
    Init-->>User: Provide class/function
```

**Summary:**
- Inputs: None (package init).
- Outputs: Exposed API symbols.
- Dependencies: Internal core modules.
- Critical Path: Not runtime critical, but important for package structure. 