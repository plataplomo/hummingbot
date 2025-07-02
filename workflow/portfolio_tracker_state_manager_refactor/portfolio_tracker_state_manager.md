# PortfolioTracker Refactor: From API-Aware Service to Pure State Manager

## Executive Summary

The current `PortfolioTracker` violates separation of concerns by directly making API calls to exchanges. This creates a circular import dependency between the core layer (which should contain business logic) and the APIs layer (which should handle external communication). The solution is to transform `PortfolioTracker` into a **pure state manager** that only receives data and manages portfolio state, while moving all API orchestration to a dedicated service layer.

## Current Architecture Problems

### 1. **Architectural Violation: Core Layer Depends on APIs Layer**
```python
# Current problematic dependency chain:
cyberdelta.core.portfolio_tracker → cyberdelta.apis.base.exchange_api
cyberdelta.apis.base.exchange_api → cyberdelta.core.models (9+ model imports)
# This creates circular dependency when core.__init__.py imports PortfolioTracker
```

### 2. **Single Responsibility Principle Violation**
Current PortfolioTracker does **TOO MUCH**:
- ✅ **Correct**: Managing portfolio state (balances, positions, orders)
- ✅ **Correct**: Calculating PnL, exposure, metrics
- ❌ **Wrong**: Making HTTP API calls to fetch balances (`_fetch_exchange_balances`)
- ❌ **Wrong**: Making HTTP API calls to fetch positions (`_fetch_exchange_positions`)
- ❌ **Wrong**: Making HTTP API calls to fetch orders (`_fetch_exchange_orders`)
- ❌ **Wrong**: Making HTTP API calls to fetch tickers (`_get_asset_price_in_base`)
- ❌ **Wrong**: Making HTTP API calls to fetch account summaries (`_fetch_exchange_account_summary`)
- ❌ **Wrong**: Orchestrating reconciliation timing and API call coordination

### 3. **Specific API Call Violations Found**

**Direct API Calls in PortfolioTracker:**
1. **Line 320**: `balances_data = await client.get_balances()`
2. **Line 570**: `positions_data_raw = await client.get_positions()`
3. **Line 625**: `orders_data: list[Order] = await client.get_open_orders()`
4. **Line 2250**: `ticker_direct = await client.get_ticker(symbol_direct)`
5. **Line 2280**: `ticker_inverse = await client.get_ticker(symbol_inverse)`
6. **Line 2425**: `summary = await client.get_account_summary()`

**API Client Management Violations:**
- **Line 67**: `api_clients: dict[str, ExchangeAPI]` - Core shouldn't know about ExchangeAPI
- **Line 164**: `register_api_client()` method - Core shouldn't register APIs
- **Line 310**: Direct access to `self.api_clients.get(exchange_id)`

## Target Architecture: Pure State Manager Pattern

### New Responsibility Separation

**PortfolioTracker (Pure State Manager)**:
- ✅ Store and manage portfolio state (balances, positions, orders)
- ✅ Calculate metrics (PnL, exposure, drawdown, total capital)
- ✅ Process trade executions and update positions
- ✅ Validate state consistency and business rules
- ✅ Provide query interface for portfolio data
- ✅ Handle state serialization/deserialization
- ❌ **NO** direct API calls
- ❌ **NO** knowledge of ExchangeAPI or HTTP clients
- ❌ **NO** network I/O operations

**New PortfolioOrchestrator (API Coordination Service)**:
- ✅ Own and manage ExchangeAPI clients
- ✅ Orchestrate data fetching from exchanges
- ✅ Handle reconciliation timing and scheduling
- ✅ Coordinate parallel API calls across exchanges
- ✅ Transform API responses and feed to PortfolioTracker
- ✅ Handle API errors and retry logic
- ✅ Manage rate limiting and API quotas

## Detailed Refactor Plan

### Phase 1: Create PortfolioOrchestrator Service

#### 1.1 Create New Service Module
**File**: `cyberdelta/core/services/portfolio_orchestrator.py`

```python
class PortfolioOrchestrator:
    """Orchestrates portfolio data fetching from exchanges and feeds PortfolioTracker."""

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTracker,
        api_clients: dict[str, ExchangeAPI] | None = None,
    ):
        self.app_settings = app_settings
        self.portfolio_tracker = portfolio_tracker
        self.api_clients: dict[str, ExchangeAPI] = api_clients or {}
        self.reconciliation_interval = 300  # Move from PortfolioTracker
        self.last_reconciliation_time: dict[str, datetime] = {}

    # Move all API-related methods from PortfolioTracker:
    async def fetch_and_update_balances(self, exchange_id: str) -> bool
    async def fetch_and_update_positions(self, exchange_id: str) -> bool
    async def fetch_and_update_orders(self, exchange_id: str) -> bool
    async def fetch_and_update_account_summary(self, exchange_id: str) -> bool
    async def orchestrate_full_reconciliation(self) -> None
    async def orchestrate_periodic_updates(self) -> None
```

#### 1.2 Move API Client Management
- Move `api_clients` dict from PortfolioTracker to PortfolioOrchestrator
- Move `register_api_client()` method
- Move `exchange_factories` if needed

### Phase 2: Transform PortfolioTracker to Pure State Manager

#### 2.1 Remove API Dependencies
**Remove from PortfolioTracker**:
- `from cyberdelta.apis.base.exchange_api import ExchangeAPI`
- `api_clients: dict[str, ExchangeAPI]` parameter and instance variable
- `register_api_client()` method
- All `client.get_*()` calls

#### 2.2 Create Pure Data Input Interface
**Add to PortfolioTracker**:
```python
# Pure data input methods (no API calls)
def update_balances(self, exchange_id: str, balances: dict[str, SpotBalance]) -> None
def update_positions(self, exchange_id: str, positions: list[DerivativePosition]) -> None
def update_orders(self, exchange_id: str, orders: list[Order]) -> None
def update_account_summary(self, exchange_id: str, summary: MarginAccountSummary) -> None
def update_ticker_data(self, exchange_id: str, symbol: str, ticker: Ticker) -> None
```

#### 2.3 Transform Existing Methods
**Current Method** → **New Pure Method**:
- `_fetch_exchange_balances()` → `_process_balances_update()` (no API calls)
- `_fetch_exchange_positions()` → `_process_positions_update()` (no API calls)
- `_fetch_exchange_orders()` → `_process_orders_update()` (no API calls)
- `_get_asset_price_in_base()` → Require ticker data to be provided externally

### Phase 3: Update Data Flow Architecture

#### 3.1 New Data Flow Pattern
```
PortfolioOrchestrator → ExchangeAPI.get_balances()
                    ↓
PortfolioOrchestrator.process_api_response()
                    ↓
PortfolioTracker.update_balances(exchange_id, clean_data)
                    ↓
PortfolioTracker internal state updated
```

#### 3.2 Remove Direct API Calls from Core Update Loop
**Current `update()` method**:
- Calls `self._fetch_exchange_*()` methods directly
- Manages reconciliation timing
- Coordinates parallel API calls

**New `update()` method**:
- Only processes already-fetched data
- Performs internal state calculations
- Triggers state consistency checks

### Phase 4: Update Integration Points

#### 4.1 Update Main Application Bootstrap
**File**: `main.py`
```python
# Current:
portfolio_tracker = PortfolioTracker(config, config.portfolio_tracker, api_clients=apis)

# New:
portfolio_tracker = PortfolioTracker(config, config.portfolio_tracker)
portfolio_orchestrator = PortfolioOrchestrator(config, portfolio_tracker, api_clients=apis)
```

#### 4.2 Update Engine Integration
**File**: `cyberdelta/core/engine.py`
- Replace direct PortfolioTracker.update() calls
- Integrate PortfolioOrchestrator.orchestrate_periodic_updates()
- Ensure proper startup sequence

#### 4.3 Update ExecutionHandler Integration
**File**: `cyberdelta/core/execution_handler.py`
- Keep existing `portfolio_tracker.process_trade()` calls (pure state updates)
- Remove any direct API client access through PortfolioTracker

### Phase 5: Handle Price Data Dependencies

#### 5.1 Create Price Data Service
**File**: `cyberdelta/core/services/price_data_service.py`
```python
class PriceDataService:
    """Manages ticker/price data fetching and caching."""

    async def get_ticker(self, exchange_id: str, symbol: str) -> Ticker | None
    async def get_price_in_base_currency(self, exchange_id: str, asset: str, base: str) -> Decimal | None
    def cache_ticker(self, exchange_id: str, symbol: str, ticker: Ticker) -> None
```

#### 5.2 Update Price-Dependent Calculations
- `get_total_capital()` - inject price data instead of fetching
- `get_exchange_exposure()` - inject price data instead of fetching
- `_calculate_position_unrealized_pnl()` - inject price data instead of fetching

### Phase 6: Testing Strategy

#### 6.1 Unit Test Updates
**PortfolioTracker Tests**:
- Remove all API client mocking
- Test pure state management functions
- Test data input validation
- Test calculation accuracy

**New PortfolioOrchestrator Tests**:
- Mock ExchangeAPI clients
- Test API error handling
- Test reconciliation timing
- Test parallel API coordination

#### 6.2 Integration Test Updates
- Test PortfolioOrchestrator → PortfolioTracker data flow
- Test end-to-end portfolio updates
- Test error propagation and recovery

### Phase 7: Performance Optimizations

#### 7.1 Async Data Processing
- Make PortfolioTracker state updates async where beneficial
- Implement proper locking for concurrent state updates
- Optimize batch processing of large data sets

#### 7.2 Memory Management
- Implement data retention policies
- Add configurable state cleanup
- Optimize large dictionary operations

## Implementation Timeline

### Week 1: Foundation
- Create PortfolioOrchestrator skeleton
- Move API client management
- Create pure data input interface on PortfolioTracker

### Week 2: Core Refactor
- Remove API dependencies from PortfolioTracker
- Transform fetch methods to pure processing methods
- Update data flow patterns

### Week 3: Integration
- Update main application bootstrap
- Fix Engine and ExecutionHandler integration
- Create PriceDataService

### Week 4: Testing & Polish
- Update all unit tests
- Fix integration tests
- Performance testing and optimization

## Benefits of This Refactor

### 1. **Architectural Correctness**
- ✅ Eliminates circular import dependency
- ✅ Proper separation of concerns (Core vs APIs layers)
- ✅ Single Responsibility Principle compliance

### 2. **Improved Testability**
- ✅ PortfolioTracker becomes easily unit testable (no API mocks needed)
- ✅ API logic isolated and independently testable
- ✅ Clear boundaries for integration testing

### 3. **Enhanced Maintainability**
- ✅ Clear data flow patterns
- ✅ Easier to debug state management vs API issues
- ✅ Simplified error handling boundaries

### 4. **Better Performance**
- ✅ API orchestration can be optimized independently
- ✅ State calculations can be optimized independently
- ✅ Better caching and batching opportunities

### 5. **Future Extensibility**
- ✅ Easy to add new data sources (WebSocket feeds, databases)
- ✅ Easy to add new portfolio calculation methods
- ✅ Clean integration points for additional services

## Risk Mitigation

### 1. **Backward Compatibility**
- Maintain existing public interface during transition
- Use adapter pattern if needed for external integrations
- Gradual migration approach

### 2. **Data Consistency**
- Implement proper state validation in pure methods
- Add comprehensive logging for data flow tracking
- Include state consistency checks

### 3. **Error Handling**
- Clear error boundaries between orchestrator and state manager
- Graceful degradation when API data is unavailable
- Proper error propagation to calling services

This refactor transforms PortfolioTracker from a problematic "god class" that violates architectural boundaries into a clean, focused state manager that properly separates concerns and eliminates the circular import issue.
