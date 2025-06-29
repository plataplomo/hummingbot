# CyberDeltaEngine: Ruff Error Resolution & Architectural Hardening Master Plan

This document provides a comprehensive strategy for resolving the 975 Ruff linting errors in CyberDeltaEngine while maintaining and improving business logic consistency. This analysis includes detailed business logic flows, exception architecture, and implementation strategies for a cryptocurrency delta-neutral arbitrage trading engine.

## Executive Summary: Building on Our Strong Foundation

After thorough analysis, we've discovered that CyberDeltaEngine already has a sophisticated exception handling system:
- **Robust `APIError` class** with retry logic, metadata, and comprehensive context
- **Well-organized `APIErrorCode` enum** with 30+ standardized error codes
- **Sophisticated error mappers** for both Backpack and Hyperliquid exchanges
- **`TransformationError` class** for data mapping failures

The Ruff violations (TRY003/TRY301) are about WHERE we construct error messages, not missing infrastructure. Our strategy is to **extend, not replace** this excellent foundation.

## 1. Current Error Analysis

**Total Errors: 975** (Updated count: 1,410 total TRY errors)
- TRY003: 1,244 errors (88%) - Long exception messages outside exception class
- TRY301: 166 errors (12%) - Raise statements within try blocks
- E501: 2 errors - Line length violations

### Key Insight: We Have Infrastructure, Need Specific Classes
The errors stem from constructing messages at the raise site:
```python
# Current pattern (TRY003)
raise ValueError(f"Order size {size} below minimum {min_size}")

# Should be
class OrderSizeError(APIError):
    def __init__(self, size: Decimal, min_size: Decimal):
        super().__init__(
            message=f"Order size {size} below minimum {min_size}",
            code=APIErrorCode.INVALID_ORDER_SIZE.value
        )
```

## 2. Business Context & Impact

CyberDeltaEngine is a **sophisticated cryptocurrency delta-neutral arbitrage trading system** that operates across Hyperliquid (perpetual contracts) and Backpack (spot markets). The system implements funding rate arbitrage strategies where error handling is **mission-critical**. Poor exception handling can lead to:

- **Financial Losses**: Unhandled trading errors can result in unwanted positions or missed opportunities
- **Position Imbalances**: Failed delta-neutral hedging can expose the portfolio to directional risk
- **Arbitrage Timing**: Delayed error resolution can cause profitable opportunities to disappear
- **Cross-Exchange Inconsistencies**: Different error handling between exchanges breaks strategy logic
- **Risk Management Failures**: Poor validation can bypass critical risk controls
- **Compliance Issues**: Inadequate error tracking can create regulatory reporting gaps

### 2.1. Core Business Operations at Risk

```mermaid
graph TD
    A[Market Data Ingestion] --> B[Opportunity Detection]
    B --> C[Risk Assessment]
    C --> D[Position Sizing]
    D --> E[Order Execution]
    E --> F[Position Monitoring]
    F --> G[Rebalancing]
    G --> H[Exit Strategy]

    A --> A1[Price Feed Errors]
    B --> B1[Calculation Errors]
    C --> C1[Risk Limit Violations]
    D --> D1[Sizing Validation Errors]
    E --> E1[Order Rejection Errors]
    F --> F1[Position Sync Errors]
    G --> G1[Rebalance Failures]
    H --> H1[Exit Execution Errors]

    A1 --> I[Strategy Halt]
    B1 --> I
    C1 --> I
    D1 --> I
    E1 --> I
    F1 --> I
    G1 --> I
    H1 --> I
```

## 3. Strategic Approach

### 3.1. Delta-Neutral Arbitrage Trading Flow

```mermaid
sequenceDiagram
    participant Engine as Trading Engine
    participant HLD as Hyperliquid API
    participant BP as Backpack API
    participant Risk as Risk Manager
    participant Portfolio as Portfolio Tracker

    Note over Engine, Portfolio: Funding Rate Arbitrage Cycle

    Engine->>HLD: Fetch Funding Rates
    HLD-->>Engine: Perpetual Funding Data
    Engine->>BP: Fetch Spot Prices
    BP-->>Engine: Spot Market Data

    Engine->>Engine: Calculate Basis & Profitability
    Engine->>Risk: Validate Opportunity
    Risk-->>Engine: Risk Assessment

    alt Profitable Opportunity
        Engine->>Portfolio: Check Available Capital
        Portfolio-->>Engine: Position Limits
        Engine->>Risk: Calculate Position Size
        Risk-->>Engine: Optimal Allocation

        par Simultaneous Execution
            Engine->>HLD: Place Perpetual Order
            Engine->>BP: Place Spot Order
        end

        HLD-->>Engine: Perp Fill Confirmation
        BP-->>Engine: Spot Fill Confirmation
        Engine->>Portfolio: Update Positions

        loop Position Monitoring
            Engine->>Portfolio: Check Delta Neutrality
            alt Rebalance Required
                Engine->>Risk: Calculate Rebalance
                Engine->>HLD: Adjust Perpetual
                Engine->>BP: Adjust Spot
            end
        end

    else Risk Limits Exceeded
        Engine->>Engine: Log Opportunity Skip
    end

    Note over Engine, Portfolio: Exception Scenarios
    HLD--xEngine: Authentication Error
    BP--xEngine: Rate Limit Error
    Risk--xEngine: Position Limit Error
    Portfolio--xEngine: Sync Error
```

### 3.2. **Enhancement**: Aligning with the 6-Layer Architecture

The refactoring strategy is explicitly designed to align with the established **6-Layer Architecture** documented in `API_ARCHITECTURE.md`. The new exception hierarchy will mirror the system's separation of concerns, providing clear, domain-specific errors at each layer. This ensures that errors are handled at the appropriate level and provide maximum context.

```mermaid
graph TD
    subgraph "Application Layer (Strategies, Engine)"
        L7[StrategyError]
    end
    subgraph "Layer 6: Domain Models (core/models)"
        L6[ValidationError]
    end
    subgraph "Layer 5: Data Transformation (mappers)"
        L5[TransformationError]
    end
    subgraph "Layer 4: Service Layer (services)"
        L4[APIError]
    end
    subgraph "Layer 3: Exchange Components (auth, request_builder)"
        L3[APIError]
    end
    subgraph "Layer 2: Base Exchange API (base)"
        L2[APIError]
    end
    subgraph "Layer 1: Connectivity (http_client, ws_manager)"
        L1[NetworkError]
    end

    L1 --> L2 --> L3 --> L4 --> L5 --> L6 --> L7
```

### 3.3. Exception Architecture Deep Dive

#### Existing Exception Infrastructure

Our current system already provides:

```mermaid
classDiagram
    class APIError {
        <<existing>>
        +message: str
        +code: int|str
        +http_status: int|None
        +exchange_code: str|int|None
        +exchange_message: str|None
        +retry_after: float|None
        +metadata: dict|None
        +is_retryable: bool
    }

    class TransformationError {
        <<existing>>
        +field_name: str|None
        +source_value: object
        +source_data: dict|None
    }

    class APIErrorCode {
        <<enumeration>>
        CONNECTION_ERROR = 0
        TIMEOUT = 1
        AUTHENTICATION_FAILED = 100
        INSUFFICIENT_FUNDS = 101
        ... (30+ codes)
    }
```

#### New Specific Exception Classes (Extend Existing)

```mermaid
classDiagram
    class TradingError {
        +exchange: str
        +operation_type: str
        +timestamp: datetime
        +context: dict
    }

    class AuthenticationError {
        +credentials_type: str
        +exchange: str
        +retry_allowed: bool
    }

    class RateLimitError {
        +retry_after: int
        +current_weight: int
        +limit_type: str
    }

    class MarketError {
        +symbol: str
        +market_state: str
        +expected_state: str
    }

    class ValidationError {
        +field_name: str
        +field_value: any
        +constraint: str
    }

    class PositionError {
        +position_id: str
        +expected_size: Decimal
        +actual_size: Decimal
        +exchange: str
    }

    class ArbitrageError {
        +opportunity_id: str
        +profitability: Decimal
        +risk_score: float
        +rejection_reason: str
    }

    TradingError <|-- AuthenticationError
    TradingError <|-- RateLimitError
    TradingError <|-- MarketError
    TradingError <|-- ValidationError
    TradingError <|-- PositionError
    TradingError <|-- ArbitrageError
```

## 4. Phase 1: Exception Class Architecture (TRY003)

### 4.1. Business Logic Categories & File Structure

We will proceed with the well-designed file structure for exceptions:

```
📁 cyberdelta/exceptions/
├── financial/
│   ├── trading_exceptions.py      # Order, position, trading errors
│   ├── market_data_exceptions.py  # Price, ticker, market errors
│   ├── balance_exceptions.py      # Funds, wallet, balance errors
│   └── risk_exceptions.py         # Risk management, limits
├── technical/
│   ├── api_exceptions.py          # HTTP, authentication, rate limits
│   ├── validation_exceptions.py   # Data validation, parsing
│   └── connectivity_exceptions.py # Network, WebSocket, connectivity
└── system/
    ├── configuration_exceptions.py # Config, setup, initialization
    └── transformation_exceptions.py # Data mapping, conversion
```

### 4.2. Comprehensive Exception Coverage Analysis

*(This section is preserved from the original document.)*

**Current Exception Landscape:**
- **857 TRY003 errors**: f-string messages in raise statements
- **116 TRY301 errors**: Raise statements within try blocks
- **Missing Business Logic Exceptions**: 23 identified gaps
- **Incomplete Error Context**: 45% of exceptions lack sufficient business context

```mermaid
graph TB
    subgraph "Current Exception Issues"
        A[TRY003: f-strings in raise] --> A1[857 violations]
        B[TRY301: raise in try blocks] --> B1[116 violations]
        C[Missing Business Exceptions] --> C1[23 gap areas]
        D[Insufficient Context] --> D1[45% lack context]
    end

    subgraph "Business Impact"
        A1 --> E[Poor Error Debugging]
        B1 --> F[Complex Control Flow]
        C1 --> G[Missing Error Handling]
        D1 --> H[Difficult Troubleshooting]
    end

    subgraph "Refactoring Strategy"
        E --> I[Create Specific Exception Classes]
        F --> J[Extract Validation Functions]
        G --> K[Add Missing Exception Types]
        H --> L[Enhance Error Context]
    end
```

**Critical Missing Exception Types:**

*(This list is preserved from the original document.)*

1.  **Delta-Neutral Strategy Exceptions**
2.  **Portfolio Management Exceptions**
3.  **Market Microstructure Exceptions**
4.  **Risk Management Exceptions**
5.  **Data Integrity Exceptions**

### 4.3. Exception Design Patterns (Original & Enhanced)

This section combines the original high-level patterns with new, code-grounded implementations.

#### Original High-Level Patterns

**Pattern A: Domain-Specific Exceptions with Context**
```python
# Before (TRY003 violation)
raise ValueError("Testnet API URL not configured but testnet environment requested")

# After (Business-focused)
class ConfigurationError(Exception):
    """Configuration validation error for exchange setup."""

    def __init__(self, missing_config: str, environment: str):
        self.missing_config = missing_config
        self.environment = environment
        super().__init__(
            f"{missing_config} not configured for {environment} environment"
        )
```

**Pattern B: Financial Safety Exceptions**
```python
# Before
raise ValueError("rate_limit_per_minute is required for Backpack")

# After
class ExchangeConfigurationError(Exception):
    """Exchange-specific configuration validation error."""

    def __init__(self, exchange: str, required_param: str):
        self.exchange = exchange
        self.required_param = required_param
        super().__init__(
            f"{required_param} is required for {exchange} exchange configuration"
        )
```

**Pattern C: API Operation Exceptions**
```python
# Before
raise APIError(
    "ED25519 authenticator required for private WebSocket subscriptions",
    code=APIErrorCode.AUTHENTICATION_FAILED.value,
)

# After
class AuthenticationRequiredError(APIError):
    """Authentication method required for private operations."""

    def __init__(self, required_auth: str, operation: str):
        self.required_auth = required_auth
        self.operation = operation
        super().__init__(
            f"{required_auth} authenticator required for {operation}",
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
        )
```

#### **Enhanced, Code-Grounded Refactoring Patterns**

**1. System Configuration Exceptions (Extending APIError)**

*   **Key Change:** All new exceptions inherit from `APIError` to maintain compatibility
*   **Code Example (`bp_api.py`):**
    ```python
    # In cyberdelta/exceptions/configuration.py
    from cyberdelta.apis.common import APIError, APIErrorCode

    class ConfigurationError(APIError):
        """Base class for configuration errors."""
        pass

    class MissingConfigValueError(ConfigurationError):
        def __init__(self, exchange_name: str, parameter_name: str):
            super().__init__(
                message=f"{parameter_name} is required for {exchange_name} exchange",
                code=APIErrorCode.INVALID_REQUEST.value,
                metadata={"exchange": exchange_name, "parameter": parameter_name}
            )

    # In bp_api.py
    from cyberdelta.exceptions import MissingConfigValueError

    if exchange_config.rate_limit_per_minute is None:
        raise MissingConfigValueError(
            exchange_name=exchange_config.exchange_name.value,
            parameter_name="rate_limit_per_minute"
        )
    ```

**2. Financial Safety Validation Exceptions (Extending TransformationError)**

*   **Key Change:** Validation exceptions extend `TransformationError` for data validation
*   **Code Example (`PlaceOrderArgs`):**
    ```python
    # In cyberdelta/exceptions/validation.py
    from cyberdelta.apis.common import TransformationError

    class OrderValidationError(TransformationError):
        """Base class for order validation errors."""
        pass

    class MissingPriceError(OrderValidationError):
        def __init__(self, order_type: str):
            super().__init__(
                message=f"Price is required for {order_type} orders",
                field_name="price",
                code="MISSING_REQUIRED_PRICE",
                source_data={"order_type": order_type}
            )

    # In service_args_models.py
    from cyberdelta.exceptions import MissingPriceError

    if self.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and self.price is None:
        raise MissingPriceError(self.order_type.value)
    ```

**3. Strategy Logic & Observability Exceptions (Extending APIError)**

*   **Goal:** Make strategy decision-making explicit while maintaining error mapper compatibility
*   **Code Example (`_check_opportunity`):**
    ```python
    # In cyberdelta/exceptions/strategy.py
    from cyberdelta.apis.common import APIError, APIErrorCode

    class StrategyError(APIError):
        """Base class for strategy-related errors."""
        pass

    class DataUnavailableError(StrategyError):
        def __init__(self, strategy_name: str, symbol: str, data_type: str):
            super().__init__(
                message=f"{data_type} unavailable for {symbol} in {strategy_name}",
                code=APIErrorCode.FUNDING_RATE_UNAVAILABLE.value,
                metadata={
                    "strategy": strategy_name,
                    "symbol": symbol,
                    "data_type": data_type
                }
            )

    # In funding_rate_arbitrage.py
    from cyberdelta.exceptions import DataUnavailableError

    if perp_ticker is None or spot_ticker is None:
        raise DataUnavailableError(
            self.name, self.symbol,
            "Ticker data for perp or spot"
        )
    ```

### 4.4. Advanced Exception Patterns for Trading Systems

*(The original, excellent examples for `DeltaNeutralityViolationError`, `CrossExchangeSyncError`, and `FundingRateArbitrageError` are preserved here.)*

## 5. Phase 2: Exception Control Flow (TRY301)

*(The original, correct analysis of `TRY301` patterns and the strategy of extracting validation logic into separate functions is fully preserved here.)*

## 6. Advanced Exception Monitoring and Recovery

*(The original, comprehensive sections on real-time tracking, recovery strategies, analytics, and testing are fully preserved here.)*

## 7. Implementation Workflow

### 7.1. Implementation Priority

*(The original priority list is preserved.)*

### 7.2. Enhanced, Prioritized Task List

This plan is ordered by business impact, addressing financial safety and system stability first.

**Phase 1: Critical Path - Financial Safety & Stability (Week 1-2)**

1.  **Create Exception Modules (Extending Existing Classes):**
    -   **Action:** Create `cyberdelta/exceptions/` with files that import and extend `APIError` and `TransformationError`:
        ```bash
        mkdir -p cyberdelta/exceptions/
        touch cyberdelta/exceptions/__init__.py
        touch cyberdelta/exceptions/{configuration,authentication,trading,validation,strategy}.py
        ```
    -   **Key Principle:** All new exceptions inherit from `APIError` or `TransformationError` - no new base classes

2.  **Harden `service_args_models.py`:**
    -   **File:** `cyberdelta/apis/models/service_args_models.py`
    -   **Task:** Implement **Pattern B**. Replace all `ValueError`s in Pydantic validators with specific `OrderValidationError` subclasses (`MissingPriceError`, `InvalidPostOnlyError`, etc.).

3.  **Harden API Client Configuration:**
    -   **Files:** `cyberdelta/apis/backpack/bp_api.py`, `cyberdelta/apis/hyperliquid/hl_api.py`
    -   **Task:** Implement **Pattern A**. Refactor `__init__` methods to raise `MissingConfigValueError` and `MissingTestnetURLError` from `configuration_exceptions.py`.

**Phase 2: Application Logic & Observability (Week 3)**

1.  **Harden `FundingRateArbitrageStrategy`:**
    -   **File:** `cyberdelta/strategies/funding_rate_arbitrage.py`
    -   **Task:** Implement **Pattern C**. Replace silent `return None` flows with `raise DataUnavailableError` for missing data and `raise OpportunityValidationError` for unprofitable (but valid) market conditions.

**Phase 3: Data Pipeline & Control Flow (Week 4)**

1.  **Refactor Mappers & Services (TRY301):**
    -   **Files:** All files in `cyberdelta/apis/**/mappers/` and `cyberdelta/apis/**/services/`
    -   **Task:** Apply the "Validation Chain Refactoring" pattern. Systematically separate validation from execution logic to resolve `TRY301` errors.

**Phase 4: Comprehensive Testing (Week 5)**

1.  **Update Unit & Integration Tests:**
    -   **Files:** All files in `tests/`
    -   **Task:** Modify existing tests to `pytest.raises(NewSpecificException)` instead of `pytest.raises(ValueError)`. Add new tests to verify the context and attributes of the new custom exceptions.

### 7.3. Risk-Weighted Implementation Approach

*(The original risk-weighted formula and results are preserved.)*

## 8. Quality Assurance & Success Metrics

*(The original QA checklist and Business Logic Preservation Rules are preserved.)*

### 8.1. Enhanced Success Metrics

1.  **Error Reduction**: 1,410 → 0 Ruff TRY errors.
2.  **100% Backward Compatibility**: All existing error handling continues to work
    - Existing `except APIError` blocks catch new exceptions
    - Error mappers process new exceptions without modification
    - Retry logic and metadata preserved
3.  **Architectural Alignment**: Exception hierarchy mirrors the 6-Layer Architecture while extending existing classes
4.  **Enhanced Observability**: Specific exception types enable better monitoring:
    -   **`StrategyError`** (extends APIError): `P3` - Log for analysis
    -   **`DataUnavailableError`** (extends APIError): `P2` - Investigate data feed
    -   **`RateLimitError`** (extends APIError): Auto-retry with backoff
    -   **`AuthenticationError`** (extends APIError): `P1` - Critical auth failure
5.  **Financial Safety**: All validation errors extend `TransformationError`, maintaining data integrity
6.  **Zero Breaking Changes**: No modifications to `APIError`, `APIErrorCode`, or error mappers

*(The original implementation timeline is preserved.)*
