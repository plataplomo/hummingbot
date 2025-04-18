# cyberdelta/monitoring/ — Per-Folder Analysis

---

## real_time_dashboard.py
**Purpose:**
Implements a real-time, web-based dashboard for monitoring strategy performance in CyberDeltaEngine. Uses Dash and Plotly for interactive visualization of live trading metrics, drawdown, PnL, trades, and funding rates.

```mermaid
flowchart TD
    A[Initialize Dashboard] --> B[Setup Layout]
    B --> C[Setup Callbacks]
    C --> D[Start Server]
    D --> E[Update Visuals in Real Time]
```

```mermaid
sequenceDiagram
    participant Dashboard as RealTimeDashboard
    participant Tracker as PerformanceTracker
    participant Portfolio as PortfolioTracker
    participant User as User (Web UI)
    User->>Dashboard: Interact (select, filter)
    Dashboard->>Tracker: Fetch performance data
    Dashboard->>Portfolio: Fetch portfolio data
    Dashboard-->>User: Display updated metrics/plots
```

**Summary:**
- Inputs: Performance and portfolio data, user selections.
- Outputs: Real-time visualizations and metrics.
- Dependencies: PerformanceTracker, PortfolioTracker, Dash, Plotly.
- Critical Path: Essential for live monitoring and operational awareness.

---

## persistence.py
**Purpose:**
Provides thread-safe persistence for all performance tracking data. Handles saving/loading of returns, trades, signals, and funding rates to JSON files, ensuring data integrity and type safety.

```mermaid
flowchart TD
    A[Initialize Persistence] --> B[Save Data]
    A --> C[Load Data]
    B --> D[Make Serializable]
    C --> E[Post-Process Loaded Data]
    D --> F[Write JSON File]
    E --> G[Return Processed Data]
```

```mermaid
sequenceDiagram
    participant Tracker as PerformanceTracker
    participant Persistence as PerformanceDataPersistence
    participant File as File System
    Tracker->>Persistence: save/load data
    Persistence->>File: Write/read JSON
    Persistence-->>Tracker: Return processed data
```

**Summary:**
- Inputs: In-memory performance data.
- Outputs: Persisted JSON files, loaded data structures.
- Dependencies: File system, custom JSON encoder.
- Critical Path: Data integrity and recoverability.
- **Critical Risk:** Financial values must use `Decimal` throughout; current use of `float` is a violation and must be refactored for correctness and compliance.

---

## performance_metrics.py
**Purpose:**
Calculates key financial performance metrics (Sharpe, Sortino, Calmar ratios, max drawdown, win rate, profit factor) for trading strategies, using Pandas and Numpy for computation and `Decimal` for precision.

```mermaid
flowchart TD
    A[Input Returns/Trades] --> B[Calculate Metrics]
    B --> C[Return Results]
```

```mermaid
sequenceDiagram
    participant Metrics as PerformanceMetricsCalculator
    participant Caller as Dashboard/Tracker
    Caller->>Metrics: Request metric calculation
    Metrics-->>Caller: Return computed metrics
```

**Summary:**
- Inputs: Returns series, trades DataFrame, risk-free rate.
- Outputs: Computed financial metrics as `Decimal`.
- Dependencies: Pandas, Numpy, Decimal.
- Critical Path: Accurate performance analysis.

---

## performance_tracker.py
**Purpose:**
Tracks and manages all in-memory strategy performance data (returns, trades, signals, funding rates). Provides methods for tracking, updating, and retrieving data, and delegates persistence to `PerformanceDataPersistence`.

```mermaid
flowchart TD
    A[Track Data] --> B[Update In-Memory Structures]
    B --> C[Delegate Save to Persistence]
    C --> D[Retrieve Data for Analysis]
```

```mermaid
sequenceDiagram
    participant Tracker as PerformanceTracker
    participant Persistence as PerformanceDataPersistence
    participant User as Dashboard/Analyzer
    User->>Tracker: Track or retrieve data
    Tracker->>Persistence: Save/load as needed
    Tracker-->>User: Return requested data
```

**Summary:**
- Inputs: Trade, signal, return, and funding rate events.
- Outputs: In-memory and persisted data for analysis/visualization.
- Dependencies: Persistence, Pandas.
- Critical Path: Central to all monitoring and reporting.
- **Critical Risk:** All financial values must use `Decimal`; current use of `float` is a violation and must be refactored.

---

## simplified_performance_tracker.py
**Purpose:**
Provides a lightweight, dependency-minimized alternative for tracking and analyzing strategy performance. Uses dataclasses and CSV for persistence, suitable for simple or resource-constrained environments.

```mermaid
flowchart TD
    A[Track Signals/Trades] --> B[Update Metrics]
    B --> C[Export/Import CSV]
    C --> D[Analyze Performance]
```

```mermaid
sequenceDiagram
    participant SimpleTracker as SimplePerformanceTracker
    participant Analyzer as SimplePerformanceAnalyzer
    participant User as User/Script
    User->>SimpleTracker: Track events
    SimpleTracker->>Analyzer: Analyze metrics
    Analyzer-->>User: Return summary/statistics
```

**Summary:**
- Inputs: Signals, trades, opportunities.
- Outputs: Metrics, CSV exports, analysis results.
- Dependencies: Dataclasses, Pandas, CSV, Decimal.
- Critical Path: Useful for prototyping and environments without full stack.

---

## dashboard_integration.py
**Purpose:**
Integrates the real-time dashboard with the rest of the trading system. Provides an interface for strategies and other components to send data to the dashboard and performance tracker.

```mermaid
flowchart TD
    A[Initialize Integration] --> B[Register Components]
    B --> C[Start/Stop Dashboard]
    C --> D[Track Data Events]
```

```mermaid
sequenceDiagram
    participant Integration as DashboardIntegration
    participant Strategy as Strategy/Component
    participant Dashboard as RealTimeDashboard
    Strategy->>Integration: Register/track events
    Integration->>Dashboard: Forward data
    Dashboard-->>Strategy: Visual feedback (via UI)
```

**Summary:**
- Inputs: Strategy, portfolio, and trade data.
- Outputs: Dashboard updates, tracked events.
- Dependencies: PerformanceTracker, PortfolioTracker, Strategy, Dash.
- Critical Path: Ensures seamless data flow to monitoring UI.

---

## __init__.py
**Purpose:**
Initializes the monitoring package and exposes key classes/functions for external use.

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
- Dependencies: Internal monitoring modules.
- Critical Path: Not runtime critical, but important for package structure. 