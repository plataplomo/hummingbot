# cyberdelta/core/models/ — Per-Folder Analysis

---

## enums.py
**Purpose:**
Defines core enumerations for order sides, types, statuses, signal types, and time-in-force. Provides type-safe, self-documenting representations for use throughout the engine.

```mermaid
flowchart TD
    A[Define Enum Classes] --> B[Expose Enum Members]
    B --> C[Use Enums in Models/Logic]
```

```mermaid
sequenceDiagram
    participant Enums as Enums
    participant Models as Data Models
    Models->>Enums: Import/use enum members
    Enums-->>Models: Provide type-safe values
```

**Summary:**
- Inputs: Enum definitions.
- Outputs: Enum members for use in models and logic.
- Dependencies: Used throughout core models and engine logic.
- Critical Path: Ensures type safety and code clarity.

---

## market.py
**Purpose:**
Defines immutable data models for exchange state, market data, order books, funding rates, trades, and orders. Ensures precise, auditable, and type-safe representation of all market-facing data.

```mermaid
flowchart TD
    A[Define Market Models] --> B[Parse/Validate Fields]
    B --> C[Use in Engine/Analytics]
```

```mermaid
sequenceDiagram
    participant Market as MarketData/Order/Trade
    participant Engine as Engine/Portfolio
    Engine->>Market: Create/validate data
    Market-->>Engine: Provide validated, immutable data
```

**Summary:**
- Inputs: Exchange data, trading events.
- Outputs: Validated, immutable models for use in engine and analytics.
- Dependencies: Pydantic, Decimal, datetime, enums.
- Critical Path: Data integrity and auditability for all market-facing operations.
- **Note:** All financial fields use `Decimal` for accuracy, in compliance with project rules.

---

## portfolio.py
**Purpose:**
Defines models for portfolio state, including balances and positions. Used for risk management, PnL tracking, and reporting. Ensures all financial fields use `Decimal` for precision.

```mermaid
flowchart TD
    A[Define Portfolio Models] --> B[Parse/Validate Fields]
    B --> C[Use in Risk/Reporting]
```

```mermaid
sequenceDiagram
    participant Portfolio as Balance/Position
    participant Engine as Engine/RiskManager
    Engine->>Portfolio: Create/update models
    Portfolio-->>Engine: Provide validated state
```

**Summary:**
- Inputs: Exchange/account data, trading events.
- Outputs: Validated, immutable/mutable models for portfolio state.
- Dependencies: Pydantic, Decimal, datetime, enums.
- Critical Path: Accurate portfolio state is essential for risk and reporting.
- **Note:** All financial fields use `Decimal` for accuracy, in compliance with project rules.

---

## strategy.py
**Purpose:**
Defines models for trading strategy intent, signals, and arbitrage opportunities. Used by the strategy layer to communicate actionable ideas and instructions to execution and portfolio layers.

```mermaid
flowchart TD
    A[Define Strategy Models] --> B[Parse/Validate Fields]
    B --> C[Use in Signal/Execution]
```

```mermaid
sequenceDiagram
    participant Strategy as TradeSignal
    participant Engine as Engine/Execution
    Engine->>Strategy: Create/validate signals
    Strategy-->>Engine: Provide actionable signals
```

**Summary:**
- Inputs: Strategy logic, market data.
- Outputs: Validated, actionable signals for execution.
- Dependencies: Pydantic, Decimal, datetime, enums.
- Critical Path: Ensures only valid, precise signals are executed.
- **Note:** All financial fields use `Decimal` for accuracy, in compliance with project rules.

---

## __init__.py
**Purpose:**
Initializes the models package and exposes key classes/enums for external use.

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
    Init-->>User: Provide class/enum
```

**Summary:**
- Inputs: None (package init).
- Outputs: Exposed API symbols.
- Dependencies: Internal model modules.
- Critical Path: Not runtime critical, but important for package structure. 