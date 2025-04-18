# cyberdelta/validation/ — Per-Folder Analysis

---

## multi_tier_funding_provider.py
**Purpose:**
Implements a multi-tier funding rate provider that integrates data from multiple sources, applies confidence scoring, and exposes funding rates for use in trading and risk management. Supports source registration, caching, and fallback logic.

```mermaid
flowchart TD
    A[Register Sources] --> B[Request Funding Rate]
    B --> C[Fetch from Cache or Sources]
    C --> D[Integrate Data]
    D --> E[Score Confidence]
    E --> F[Return Rate & Confidence]
```

```mermaid
sequenceDiagram
    participant Provider as MultiTierFundingProvider
    participant Source as DataSource
    participant Consumer as Consumer (e.g., engine)
    Consumer->>Provider: get_funding_rate()
    Provider->>Source: Fetch data (primary/secondary/tertiary)
    Provider-->>Consumer: Return rate, confidence
```

**Summary:**
- Inputs: Source registration, funding rate requests.
- Outputs: Funding rate and confidence score.
- Dependencies: FundingData, IntegratedFundingData, ConfidenceFactors.
- Critical Path: Reliable funding data is essential for risk management and arbitrage.

---

## funding_data.py
**Purpose:**
Defines data structures and models for funding rate data, including source types, reliability, integrated data, validation metrics, and arbitrage opportunities. Ensures type safety and validation for all funding-related data.

```mermaid
flowchart TD
    A[Define Data Models] --> B[Validate Fields]
    B --> C[Use in Funding/Arb Logic]
```

```mermaid
sequenceDiagram
    participant Model as FundingData/ArbitrageOpportunity
    participant Provider as FundingProvider
    participant Validator as FundingRateValidator
    Provider->>Model: Create/validate data
    Validator->>Model: Validate/score data
    Model-->>Provider: Provide validated data
```

**Summary:**
- Inputs: Funding rate data from sources, arbitrage detection events.
- Outputs: Validated, structured data for use in trading and analytics.
- Dependencies: Pydantic, Decimal, datetime.
- Critical Path: Data integrity and validation for all funding-related operations.
- **Note:** All financial fields use `Decimal` for accuracy, in compliance with project rules.

---

## funding_rate_validator.py
**Purpose:**
Implements the system for validating funding rate predictions against actual payments, tracking accuracy metrics (RMSE, MAE, bias) and providing reports for model improvement.

```mermaid
flowchart TD
    A[Record Prediction/Payment] --> B[Calculate Metrics]
    B --> C[Generate Reports]
```

```mermaid
sequenceDiagram
    participant Validator as FundingRateValidator
    participant Engine as Trading Engine
    Engine->>Validator: Record prediction/payment
    Validator-->>Engine: Provide accuracy metrics
```

**Summary:**
- Inputs: Funding rate predictions, actual payment data.
- Outputs: Accuracy metrics, validation reports.
- Dependencies: Config, datetime, math.
- Critical Path: Ensures funding models are accurate and reliable.

---

## position_reconciliation.py
**Purpose:**
Validates and reconciles trading positions between exchange APIs, fill history, and local state. Detects discrepancies, provides alerts, and can auto-correct local state to match authoritative sources.

```mermaid
flowchart TD
    A[Check Positions] --> B[Compare Sources]
    B --> C[Detect Discrepancies]
    C --> D[Alert/Auto-Correct]
```

```mermaid
sequenceDiagram
    participant Reconciler as PositionReconciliationSystem
    participant Portfolio as PortfolioTracker
    participant Exchange as ExchangeAPI
    Reconciler->>Portfolio: Get local positions
    Reconciler->>Exchange: Get API positions
    Reconciler-->>Portfolio: Alert/correct discrepancies
```

**Summary:**
- Inputs: Position data from multiple sources.
- Outputs: Discrepancy alerts, corrections, reconciliation reports.
- Dependencies: PortfolioTracker, Config, Decimal.
- Critical Path: Prevents undetected position mismatches and risk.

---

## circuit_breaker.py
**Purpose:**
Implements a comprehensive circuit breaker system to halt trading operations under abnormal or dangerous conditions (e.g., excessive volatility, drawdown, API errors, liquidity issues). Supports custom breakers, state management, and recovery logic.

```mermaid
flowchart TD
    A[Monitor Conditions] --> B[Trip Breaker]
    B --> C[Block Operations]
    C --> D[Reset/Test Recovery]
```

```mermaid
sequenceDiagram
    participant Breaker as CircuitBreakerSystem
    participant Engine as Trading Engine
    Engine->>Breaker: Check/record events
    Breaker-->>Engine: Allow/block operations
    Engine->>Breaker: Reset/test recovery
```

**Summary:**
- Inputs: Trading events, market data, error signals.
- Outputs: Blocked operations, alerts, breaker status.
- Dependencies: Config, logging, datetime.
- Critical Path: Essential for system safety and loss prevention.

---

## __init__.py
**Purpose:**
Initializes the validation package and exposes key classes, data models, and systems for external use.

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
- Dependencies: Internal validation modules.
- Critical Path: Not runtime critical, but important for package structure. 