# cyberdelta/visualization/ — Per-Folder Analysis

---

## performance_visualizer.py
**Purpose:**
Provides advanced tools for visualizing and analyzing strategy performance data, supporting both real-time dashboards and historical analysis. Includes chart generation (returns, drawdown, trades, funding rates), dashboard assembly, and performance metrics calculation.

```mermaid
flowchart TD
    A[Init Visualizer] --> B[Prepare Data]
    B --> C[Generate Charts]
    C --> D[Assemble Dashboard]
    D --> E[Calculate Metrics]
```

```mermaid
sequenceDiagram
    participant Visualizer as PerformanceVisualizer
    participant Dashboard as Dashboard/UI
    participant Data as Performance Data
    Dashboard->>Visualizer: Request chart/metrics
    Visualizer->>Data: Fetch/process data
    Visualizer-->>Dashboard: Return figures/metrics
```

**Summary:**
- Inputs: Performance data (returns, trades, funding rates), config.
- Outputs: Interactive charts, dashboards, computed metrics.
- Dependencies: Pandas, Plotly, Numpy, Dataclasses.
- Critical Path: Enables robust, interactive performance monitoring and analysis.
- **Note:** All financial fields use `Decimal` for accuracy, in compliance with project rules.

---

## simplified_visualizer.py
**Purpose:**
Provides lightweight, dependency-minimized visualization tools for performance data, using Matplotlib. Suitable for environments without web frameworks or for quick, script-based analysis.

```mermaid
flowchart TD
    A[Generate Example Data] --> B[Track Signals/Trades]
    B --> C[Analyze Performance]
    C --> D[Plot Metrics]
    D --> E[Export/Save Plots]
```

```mermaid
sequenceDiagram
    participant SimpleVis as SimpleVisualizer
    participant Tracker as SimplePerformanceTracker
    participant Analyzer as SimplePerformanceAnalyzer
    participant User as User/Script
    User->>SimpleVis: Request plot/report
    SimpleVis->>Tracker: Fetch data
    SimpleVis->>Analyzer: Analyze metrics
    SimpleVis-->>User: Return/save plots
```

**Summary:**
- Inputs: Performance data, tracker instance, config.
- Outputs: Static plots, performance summaries, saved images.
- Dependencies: Matplotlib, Pandas, Numpy, Decimal.
- Critical Path: Enables quick, scriptable performance analysis and reporting.
- **Note:** All financial fields use `Decimal` for accuracy, in compliance with project rules. 