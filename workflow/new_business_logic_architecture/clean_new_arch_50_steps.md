# CyberDeltaEngine: 50-Step Implementation Plan

## Overview

This plan implements the clean architecture defined in `clean_new_arch.md` with **ZERO business logic violations** of `CODING_STANDARDS.md`. Each step follows the configuration-first principle with NO hardcoded values, NO assumptions, and NO defaults for critical operations.

### Key Principles Applied Throughout:
1. **Configuration-First**: ALL values from AppSettings, NO hardcoded numbers/strings
2. **Explicit Over Implicit**: NO type conversions, NO silent defaults
3. **Fail Fast**: NO graceful degradation, errors bubble up immediately
4. **Type Safety**: Symbol objects NOT strings, Enums NOT strings, Decimal NOT float
5. **Structured Logging**: ONLY use cyberdelta.logging helpers, NO raw logging

---

## Phase 1: Foundation & Infrastructure (Steps 1-10)

### Step 1: Create Module Structure
**Task**: Create the new module directories per clean_new_arch.md
```bash
cyberdelta/
├── logic/           # New business logic layer
├── application/     # Application orchestration  
├── infrastructure/  # Technical infrastructure
└── models/
    ├── portfolio/   # New portfolio models
    ├── risk/        # New risk models
    ├── trading/     # New trading models
    └── market/      # New market models
```
**Validation**: All directories exist with proper __init__.py files
**Dependencies**: None
**CODING_STANDARDS compliance**: Directory structure only, no code yet

### Step 2: Create Base Domain Models
**Task**: Implement the 4 new models from clean_new_arch.md
- `models/portfolio/state.py` - PortfolioState with NO defaults
- `models/trading/execution_request.py` - ExecutionRequest 
- `models/market/market_snapshot.py` - MarketSnapshot
- `models/risk/assessment.py` - RiskAssessment & PositionSize

**Validation**: 
- All models use Pydantic BaseModel
- All fields properly typed (Symbol not str, ExchangeName not str)
- NO default values for critical fields
- All Decimal fields, NO floats

**Dependencies**: Step 1
**CODING_STANDARDS compliance**: 
- Uses ExchangeName enum not strings
- Uses Symbol objects not strings
- All Decimal types for money/quantities
- NO defaults on critical fields

### Step 3: Implement Event Bus Infrastructure
**Task**: Create event distribution system in `application/event_bus.py`
```python
class DomainEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    version: int = 1

class EventBus:
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = {}
        # NO hardcoded retry counts or delays
```
**Validation**: Event bus can publish/subscribe with type safety
**Dependencies**: Step 2
**CODING_STANDARDS compliance**: NO hardcoded retry logic, all from config when used

### Step 4: Implement Service Registry
**Task**: Create dependency injection in `application/service_registry.py`
```python
class ServiceRegistry:
    def __init__(self):
        self._services: Dict[type, Any] = {}
    
    def register(self, interface: type, implementation: Any) -> None:
        # Validate implementation matches interface
        # NO defaults, explicit registration only
```
**Validation**: Registry enforces interface contracts
**Dependencies**: Step 3
**CODING_STANDARDS compliance**: Explicit registration only, no auto-discovery

### Step 5: Create Storage Interfaces
**Task**: Define storage protocols in `infrastructure/persistence/__init__.py`
```python
class StorageProtocol(Protocol):
    @abstractmethod
    async def save_state(self, state: PortfolioState) -> None:
        pass
    
    @abstractmethod  
    async def load_state(self) -> Optional[PortfolioState]:
        pass
```
**Validation**: Clean interface definitions with no implementation
**Dependencies**: Step 2
**CODING_STANDARDS compliance**: Just interfaces, no defaults

### Step 6: Implement File-Based Storage
**Task**: Create `infrastructure/persistence/file_repository.py`
```python
class FilePortfolioStorage(StorageProtocol):
    def __init__(self, config: AppSettings):
        self._state_file = Path(config.general.state_file)
        self._backup_dir = Path(config.general.state_backup_directory)
        # ALL paths from config, NO hardcoded paths
```
**Validation**: 
- Reads/writes JSON with proper error handling
- Uses ONLY configured paths
- Creates directories with proper permissions

**Dependencies**: Steps 2, 5
**CODING_STANDARDS compliance**: 
- All paths from config
- No hardcoded filenames
- Explicit error handling

### Step 7: Set Up Structured Logging
**Task**: Configure structlog in main.py using cyberdelta.logging
```python
import structlog
from cyberdelta.logging.logging_helpers import log_trading_event

# Configure processors from config
structlog.configure(
    processors=[...],  # From config.general.log_processors
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
```
**Validation**: All loggers use structured format
**Dependencies**: None
**CODING_STANDARDS compliance**: Uses existing logging infrastructure

### Step 8: Create Domain Event Types
**Task**: Define all events in `application/events.py`
```python
class OrderFilledEvent(DomainEvent):
    order_id: str
    symbol: Symbol  # NOT string!
    exchange: ExchangeName  # NOT string!
    fill_price: Decimal  # NOT float!
    fill_quantity: Decimal  # NOT float!
    # NO defaults for critical fields
```
**Validation**: All events properly typed with Symbol/ExchangeName
**Dependencies**: Steps 2, 3
**CODING_STANDARDS compliance**: Proper types throughout

### Step 9: Implement Configuration Validation
**Task**: Add startup validation in main.py
```python
def validate_configuration(config: AppSettings) -> None:
    # Verify all required settings present
    # Check path accessibility
    # Validate exchange configs
    # FAIL FAST if any issues
    if not config.general.state_file:
        raise ConfigurationError("state_file not configured")
```
**Validation**: Catches all config issues at startup
**Dependencies**: None
**CODING_STANDARDS compliance**: Fail fast, no silent defaults

### Step 10: Create Test Infrastructure
**Task**: Set up pytest fixtures with test configs
```python
@pytest.fixture
def test_config() -> AppSettings:
    # Load test configuration
    # ALL values explicit, no magic numbers
    return get_app_settings(config_path="tests/config/test_config.yaml")
```
**Validation**: Tests can run with known configuration
**Dependencies**: Steps 1-9
**CODING_STANDARDS compliance**: Test configs explicit, no hardcoded test data

### 🔍 Step 10 Checkpoint: CODING_STANDARDS Compliance Review

**Verification Checklist for Steps 1-10:**

1. **NO ASSUMPTIONS**:
   - [ ] No unit assumptions (all units explicit in field names)
   - [ ] No format assumptions (using Symbol/ExchangeName objects)
   - [ ] No timezone assumptions (using datetime with UTC)
   - [ ] No platform assumptions (using pathlib.Path)
   - [ ] No market behavior assumptions
   - [ ] No data structure assumptions

2. **NO HARDCODED VALUES**:
   - [ ] No hardcoded numbers in any code
   - [ ] No hardcoded strings (except for event names)
   - [ ] No hardcoded paths (all from config)
   - [ ] No hardcoded timeouts
   - [ ] No hardcoded retries
   - [ ] No hardcoded limits

3. **NO DEFAULTS FOR CRITICAL OPERATIONS**:
   - [ ] Models have no defaults for critical fields (prices, quantities, etc.)
   - [ ] All functions require explicit parameters
   - [ ] Storage paths must be configured
   - [ ] No fallback values anywhere

4. **TYPE SAFETY**:
   - [ ] Symbol objects used, not strings
   - [ ] ExchangeName enum used, not strings
   - [ ] Decimal used for all money/quantities
   - [ ] Proper Pydantic models for all data

5. **FAIL FAST**:
   - [ ] Configuration validation fails immediately on issues
   - [ ] No silent error handling
   - [ ] Explicit error messages

**Automated Verification Commands:**
```bash
# Check for hardcoded values
grep -r "Decimal(\"[0-9]" cyberdelta/logic/ cyberdelta/application/ cyberdelta/infrastructure/
grep -r "sleep([0-9]" cyberdelta/logic/ cyberdelta/application/ cyberdelta/infrastructure/
grep -r "= [0-9]\\+\\.[0-9]" cyberdelta/logic/ cyberdelta/application/ cyberdelta/infrastructure/

# Check for string symbols/exchanges
grep -r "symbol.*=.*['\"]" cyberdelta/logic/ --include="*.py" | grep -v "Symbol("
grep -r "exchange.*=.*['\"]" cyberdelta/logic/ --include="*.py" | grep -v "ExchangeName."

# Verify logging usage
grep -r "import logging" cyberdelta/logic/ cyberdelta/application/ cyberdelta/infrastructure/
grep -r "logger\." cyberdelta/logic/ --include="*.py" | grep -v "structlog"
```

**Required Actions Before Proceeding to Step 11:**
- Run all verification commands above
- Fix any violations found
- Document any exceptions with justification
- Get code review focusing on CODING_STANDARDS compliance

---

## Phase 2: Core Services Implementation (Steps 11-25)

### Step 11: Implement Portfolio Service Base
**Task**: Create `logic/portfolio/portfolio_service.py` skeleton
```python
class PortfolioService:
    def __init__(self, config: AppSettings, storage: StorageProtocol, event_bus: EventBus):
        self.config = config
        self._storage = storage
        self._event_bus = event_bus
        # Cache frequently used config values
        self._balance_tolerance = config.validation.balance_tolerance
        # NO hardcoded tolerances or limits
```
**Validation**: Service initializes with injected dependencies
**Dependencies**: Steps 1-10
**CODING_STANDARDS compliance**: All config injected, no defaults

### Step 12: Implement Portfolio State Management
**Task**: Add state methods to PortfolioService
```python
async def get_balance(self, asset: Symbol, exchange: ExchangeName) -> Optional[SpotBalance]:
    # Use Symbol object, not string
    # Use ExchangeName enum, not string
    key = f"{exchange.value}:{asset.value}"
    return self._cached_state.balances.get(key)
```
**Validation**: Type-safe state access methods work
**Dependencies**: Step 11
**CODING_STANDARDS compliance**: Proper types, no string symbols

### Step 13: Implement Portfolio Persistence
**Task**: Add save/load functionality with atomic writes
```python
async def _persist_state(self) -> None:
    # Use configured intervals
    if self._last_save + self.config.general.state_save_interval > datetime.now(UTC):
        return
    # Atomic write with temp file
    # Backup rotation from config.general.state_backup_count
```
**Validation**: State persists and recovers correctly
**Dependencies**: Steps 11-12
**CODING_STANDARDS compliance**: All intervals/counts from config

### Step 14: Implement Risk Service Base
**Task**: Create `logic/risk/risk_service.py`
```python
class RiskService:
    def __init__(self, config: AppSettings, portfolio_service: PortfolioService):
        self.config = config
        self._portfolio_service = portfolio_service
        # Extract risk settings - NO defaults
        self._max_position_usd = config.risk.global_risk.max_position_usd
        self._max_exposure_usd = config.risk.global_risk.max_total_exposure_usd
```
**Validation**: Service initializes with proper config
**Dependencies**: Steps 11-13
**CODING_STANDARDS compliance**: All limits from config

### Step 15: Implement Position Sizing
**Task**: Add position sizing to RiskService
```python
async def calculate_position_size(
    self,
    signal: TradeSignal,
    total_equity: Decimal,
    current_exposure: Decimal
) -> PositionSize:
    # Use config.risk.sizing.method ("simple" or "kelly")
    # Apply config.risk.sizing.min_position_size
    # Apply config.risk.sizing.max_position_size
    # NO hardcoded fractions or multipliers
```
**Validation**: Position sizing uses configured method
**Dependencies**: Step 14
**CODING_STANDARDS compliance**: All sizing params from config

### Step 16: Implement Risk Assessment
**Task**: Add signal assessment to RiskService
```python
async def assess_signal(self, signal: TradeSignal) -> RiskAssessment:
    # Check against config.risk.global_risk limits
    # Use config.risk.checkers thresholds
    # Return explicit violations, no silent filtering
    if position_size.value_usd > self._max_position_usd:
        limit_violations.append(f"Position ${position_size.value_usd} exceeds max ${self._max_position_usd}")
```
**Validation**: Assessment provides clear approve/reject with reasons
**Dependencies**: Steps 14-15
**CODING_STANDARDS compliance**: Explicit limit checking from config

### Step 17: Implement Market Data Service
**Task**: Create `logic/market/market_service.py`
```python
class MarketDataService:
    def __init__(self, config: AppSettings, api_clients: Dict[str, ExchangeAPI], event_bus: EventBus):
        self.config = config
        self._api_clients = api_clients
        self._cache_ttl = config.monitoring.cache_ttl_seconds
        # NO hardcoded cache durations
```
**Validation**: Service aggregates data from multiple exchanges
**Dependencies**: Steps 1-10
**CODING_STANDARDS compliance**: Cache TTL from config

### Step 18: Implement Market Data Aggregation
**Task**: Add aggregation logic to MarketDataService
```python
async def get_market_snapshot(self) -> MarketSnapshot:
    # Fetch from all enabled exchanges (config.exchanges)
    # Use configured timeouts per exchange
    # Return typed MarketSnapshot, not dict
    for exchange_name, exchange_config in self.config.exchanges.items():
        if exchange_config.enabled:
            timeout = exchange_config.request_timeout_seconds
            # Use timeout, no defaults
```
**Validation**: Returns properly typed MarketSnapshot
**Dependencies**: Step 17
**CODING_STANDARDS compliance**: Timeouts from config, typed returns

### Step 19: Implement Execution Engine
**Task**: Create `logic/trading/execution_engine.py`
```python
class ExecutionEngine:
    def __init__(self, config: AppSettings, api_clients: Dict[str, ExchangeAPI]):
        self.config = config
        self._max_slippage = config.execution.max_slippage_pct
        self._max_retries = config.execution.max_retries
        # NO hardcoded execution parameters
```
**Validation**: Engine handles order placement
**Dependencies**: Steps 1-10
**CODING_STANDARDS compliance**: All execution params from config

### Step 20: Implement Order Management
**Task**: Add order lifecycle to ExecutionEngine
```python
async def execute_request(self, request: ExecutionRequest) -> Order:
    # Check slippage against config.execution.max_slippage_pct
    # Retry using config.execution.max_retries
    # Use config.execution.retry_delay_base_sec
    for attempt in range(self._max_retries):
        try:
            # Place order
        except Exception as e:
            if attempt < self._max_retries - 1:
                await asyncio.sleep(self._retry_delay * (attempt + 1))
            else:
                raise  # Fail fast on final attempt
```
**Validation**: Orders placed with proper retry logic
**Dependencies**: Step 19
**CODING_STANDARDS compliance**: Retries/delays from config

### 🔍 Step 20 Checkpoint: CODING_STANDARDS Compliance Review

**Verification Checklist for Steps 11-20:**

1. **CONFIGURATION-FIRST ARCHITECTURE**:
   - [ ] All services receive AppSettings in constructor
   - [ ] Config values cached in __init__, not hardcoded in methods
   - [ ] No service has hardcoded parameters
   - [ ] All thresholds, limits, intervals from config

2. **TYPE SAFETY IN SERVICES**:
   - [ ] Symbol objects used in all method signatures
   - [ ] ExchangeName enum used, never strings
   - [ ] Decimal used for all financial calculations
   - [ ] Optional[] used appropriately, no implicit None

3. **NO IMPLICIT BEHAVIORS**:
   - [ ] No default retries (explicit from config)
   - [ ] No assumed timeouts (explicit from config)
   - [ ] No silent error handling
   - [ ] All errors bubble up with context

4. **STRUCTURED LOGGING**:
   - [ ] Using structlog.get_logger(__name__)
   - [ ] Using cyberdelta.logging helpers for events
   - [ ] No string formatting in log messages
   - [ ] Proper event names and context

5. **SERVICE BOUNDARIES**:
   - [ ] Services only do their specific domain task
   - [ ] No service has overlapping responsibilities
   - [ ] Clear interfaces between services
   - [ ] Proper dependency injection

**Code Quality Checks:**
```bash
# Verify all services have AppSettings injection
grep -r "def __init__" cyberdelta/logic/ --include="*.py" | grep -v "config: AppSettings"

# Check for float usage (should be Decimal)
grep -r "float(" cyberdelta/logic/ --include="*.py"
grep -r ": float" cyberdelta/logic/ --include="*.py" | grep -v "# NOT float"

# Verify no raw logging
grep -r "import logging" cyberdelta/logic/
grep -r "logging.getLogger" cyberdelta/logic/

# Check for hardcoded retry/timeout values
grep -r "for.*in range([0-9]" cyberdelta/logic/ --include="*.py"
grep -r "sleep([0-9]" cyberdelta/logic/ --include="*.py"
```

**Performance Verification:**
- [ ] No synchronous I/O in async methods
- [ ] Proper use of async/await throughout
- [ ] No blocking operations in services

---

### Step 21: Implement Signal Service
**Task**: Create `logic/signal/signal_service.py`
```python
class SignalService:
    def __init__(self, config: AppSettings, event_bus: EventBus):
        self.config = config
        self._event_bus = event_bus
        # Validation thresholds from config
        self._min_confidence = config.risk.checkers.thresholds.min_signal_confidence
```
**Validation**: Service validates and routes signals
**Dependencies**: Steps 1-10
**CODING_STANDARDS compliance**: Validation params from config

### Step 22: Implement Signal Validation
**Task**: Add validation logic to SignalService
```python
async def validate_signal(self, signal: TradeSignal) -> List[str]:
    violations = []
    # Check symbol is Symbol object
    if not isinstance(signal.symbol, Symbol):
        violations.append("Symbol must be Symbol type")
    # Check price sanity from config
    if signal.price < self.config.risk.checkers.thresholds.min_price:
        violations.append(f"Price {signal.price} below min {self.config.risk.checkers.thresholds.min_price}")
```
**Validation**: Comprehensive signal validation
**Dependencies**: Step 21
**CODING_STANDARDS compliance**: All thresholds from config

### Step 23: Implement Strategy Base Classes
**Task**: Create `logic/strategy/strategy_base.py`
```python
class BaseStrategy(ABC):
    def __init__(self, strategy_config: StrategyConfig):
        self.config = strategy_config
        # NO hardcoded strategy parameters
    
    @abstractmethod
    async def analyze(self, market_data: MarketSnapshot, portfolio: PortfolioState) -> Optional[TradeSignal]:
        # Must use typed inputs/outputs
        pass
```
**Validation**: Clean strategy interface
**Dependencies**: Steps 1-10
**CODING_STANDARDS compliance**: Config injected to strategies

### Step 24: Implement Strategy Service
**Task**: Create `logic/strategy/strategy_service.py`
```python
class StrategyService:
    def __init__(self, config: AppSettings, market_service: MarketDataService, 
                 portfolio_service: PortfolioService, signal_service: SignalService):
        self.config = config
        self._execution_interval = config.strategies.execution_interval_seconds
        # NO hardcoded intervals
```
**Validation**: Service orchestrates strategies
**Dependencies**: Steps 17, 11, 21, 23
**CODING_STANDARDS compliance**: Intervals from config

### Step 25: Implement Trading Service Coordination
**Task**: Create `logic/trading/trading_service.py`
```python
class TradingService:
    def __init__(self, config: AppSettings, execution_engine: ExecutionEngine,
                 portfolio_service: PortfolioService, event_bus: EventBus):
        self.config = config
        # Coordinate execution, portfolio updates, event publishing
        # NO business logic, just orchestration
```
**Validation**: Service coordinates trading flow
**Dependencies**: Steps 11, 19
**CODING_STANDARDS compliance**: Pure orchestration, config-driven

---

## Phase 3: Integration & Orchestration (Steps 26-35)

### Step 26: Implement Trading Engine
**Task**: Create `application/trading_engine.py`
```python
class TradingEngine:
    def __init__(self, config: AppSettings, event_bus: EventBus, 
                 # ... all services ...):
        self.config = config
        self._safe_mode = config.general.safe_mode
        self._reconciliation_interval = config.safety_systems.position_reconciliation.check_interval_sec
        # NO hardcoded operational parameters
```
**Validation**: Engine coordinates all services
**Dependencies**: Steps 11-25
**CODING_STANDARDS compliance**: All params from config

### Step 27: Wire Event Subscriptions
**Task**: Connect services via events in TradingEngine
```python
async def _setup_event_handlers(self):
    await self._event_bus.subscribe("market_data_update", self._handle_market_data)
    await self._event_bus.subscribe("trading_signal", self._handle_trading_signal)
    await self._event_bus.subscribe("order_filled", self._handle_order_filled)
    # All handlers use structured logging
```
**Validation**: Events flow between services
**Dependencies**: Step 26
**CODING_STANDARDS compliance**: Explicit event wiring

### Step 28: Implement Portfolio Reconciliation
**Task**: Add reconciliation to PortfolioService
```python
async def reconcile_with_exchanges(self) -> None:
    for exchange_name, api_client in self._api_clients.items():
        # Use config.state.reconciliation_timeout
        # Compare with tolerance from config.validation.balance_tolerance
        # Log all discrepancies, fail if critical
```
**Validation**: Portfolio syncs with exchange state
**Dependencies**: Steps 11-13
**CODING_STANDARDS compliance**: Tolerances from config

### Step 29: Implement Circuit Breakers
**Task**: Add safety systems to TradingEngine
```python
class CircuitBreaker:
    def __init__(self, config: SafetySystemsSettings):
        self._config = config.circuit_breakers
        self._failure_threshold = config.circuit_breakers.global_consecutive_failures
        self._cooldown_period = config.circuit_breakers.cooldown_period_seconds
        # NO hardcoded safety thresholds
```
**Validation**: Circuit breakers trip on configured thresholds
**Dependencies**: Step 26
**CODING_STANDARDS compliance**: All thresholds from config

### Step 30: Implement Health Monitoring
**Task**: Add monitoring to all services
```python
async def check_health(self) -> HealthStatus:
    # Check against config.monitoring.health_check_thresholds
    # Use config.monitoring.stale_data_threshold_seconds
    # Return explicit status, no assumptions
```
**Validation**: Health checks report accurate status
**Dependencies**: Steps 11-25
**CODING_STANDARDS compliance**: All thresholds from config

### 🔍 Step 30 Checkpoint: CODING_STANDARDS Compliance Review

**Verification Checklist for Steps 21-30:**

1. **EVENT-DRIVEN ARCHITECTURE**:
   - [ ] All events use proper domain types (Symbol, ExchangeName)
   - [ ] No string-based event data
   - [ ] Event handlers use structured logging
   - [ ] No hardcoded event names in subscriptions

2. **SAFETY SYSTEMS**:
   - [ ] Circuit breaker thresholds from config
   - [ ] Health check intervals from config
   - [ ] Reconciliation tolerances from config
   - [ ] All safety parameters configurable

3. **INTEGRATION PATTERNS**:
   - [ ] Services communicate only through defined interfaces
   - [ ] No direct service-to-service dependencies
   - [ ] Event bus handles all cross-service communication
   - [ ] No shared mutable state

4. **ERROR HANDLING**:
   - [ ] All async operations have proper error handling
   - [ ] Errors include context (not just message)
   - [ ] No catch-all exception handlers
   - [ ] Fail fast on critical errors

5. **MONITORING & OBSERVABILITY**:
   - [ ] All important operations logged with structure
   - [ ] Metrics use configured collection intervals
   - [ ] Health checks explicit about what they verify
   - [ ] No assumptions about "normal" behavior

**Integration Testing Requirements:**
```bash
# Verify event flow
grep -r "publish.*Event" cyberdelta/logic/ --include="*.py"
grep -r "subscribe(" cyberdelta/application/ --include="*.py"

# Check for shared state anti-patterns
grep -r "global " cyberdelta/logic/ cyberdelta/application/ --include="*.py"
grep -r "@classmethod" cyberdelta/logic/ --include="*.py" | grep -v "validator"

# Verify proper async patterns
grep -r "def.*async" cyberdelta/logic/ --include="*.py" | grep -v "async def"
grep -r "asyncio.run(" cyberdelta/logic/ --include="*.py"
```

---

### Step 31: Implement Alert System
**Task**: Create alert infrastructure
```python
class AlertService:
    def __init__(self, config: MonitoringSettings):
        self._enabled = config.notifications_enabled
        self._methods = config.alert_methods
        # Only alert if explicitly enabled
```
**Validation**: Alerts sent via configured channels
**Dependencies**: Step 30
**CODING_STANDARDS compliance**: Alert config explicit

### Step 32: Implement Metrics Collection
**Task**: Add metrics to all services
```python
class MetricsCollector:
    def __init__(self, config: AppSettings):
        self._metrics_interval = config.monitoring.metrics_collection_interval
        self._retention_days = config.monitoring.metrics_retention_days
        # NO hardcoded intervals or retention
```
**Validation**: Metrics collected at configured intervals
**Dependencies**: Steps 11-25
**CODING_STANDARDS compliance**: All timing from config

### Step 33: Implement State Snapshots
**Task**: Add snapshot functionality
```python
async def create_snapshot(self) -> None:
    # Use config.general.state_backup_directory
    # Rotate based on config.general.state_backup_count
    # Timestamp format from config
```
**Validation**: Snapshots created and rotated properly
**Dependencies**: Steps 11-13
**CODING_STANDARDS compliance**: All paths/counts from config

### Step 34: Implement Graceful Shutdown
**Task**: Add shutdown logic to TradingEngine
```python
async def shutdown(self) -> None:
    # Cancel orders if config.execution.cancel_on_shutdown
    # Save final state
    # Wait for config.general.shutdown_grace_period
    # NO assumptions about cleanup order
```
**Validation**: Clean shutdown with state preserved
**Dependencies**: Step 26
**CODING_STANDARDS compliance**: Shutdown behavior from config

### Step 35: Complete main.py Integration
**Task**: Wire everything in main.py
```python
async def main():
    # Load config ONCE
    config = get_app_settings()
    secrets = get_secrets_config()
    
    # Initialize all services with config
    # Start trading engine
    # Handle shutdown gracefully
```
**Validation**: Application starts and runs
**Dependencies**: Steps 1-34
**CODING_STANDARDS compliance**: Single config load point

---

## Phase 4: Strategies & Safety Systems (Steps 36-45)

### Step 36: Implement Example Momentum Strategy
**Task**: Create first strategy implementation
```python
class MomentumStrategy(BaseStrategy):
    async def analyze(self, market_data: MarketSnapshot, portfolio: PortfolioState) -> Optional[TradeSignal]:
        # Use config.strategies.momentum.price_change_threshold
        # Use config.strategies.momentum.lookback_period
        # Return typed TradeSignal or None
```
**Validation**: Strategy generates signals from config params
**Dependencies**: Step 23
**CODING_STANDARDS compliance**: All params from strategy config

### Step 37: Implement Strategy Registry
**Task**: Create `logic/strategy/strategy_registry.py`
```python
class StrategyRegistry:
    def __init__(self, config: AppSettings):
        self._enabled_strategies = config.strategies.enabled_strategies
        # Only load explicitly enabled strategies
```
**Validation**: Registry loads configured strategies
**Dependencies**: Steps 23-24
**CODING_STANDARDS compliance**: Explicit strategy enabling

### Step 38: Implement Position Limits
**Task**: Add position limit enforcement
```python
async def check_position_limits(self, symbol: Symbol, exchange: ExchangeName) -> List[str]:
    # Check config.risk.limits.max_positions_per_symbol
    # Check config.risk.limits.max_positions_total
    # Return explicit violations
```
**Validation**: Limits enforced from config
**Dependencies**: Step 14
**CODING_STANDARDS compliance**: All limits from config

### Step 39: Implement Drawdown Protection
**Task**: Add drawdown monitoring
```python
class DrawdownMonitor:
    def __init__(self, config: AppSettings):
        self._max_drawdown = config.risk.global_risk.max_drawdown_pct
        self._lookback_days = config.risk.global_risk.drawdown_lookback_days
        # NO hardcoded risk parameters
```
**Validation**: Drawdown calculated and enforced
**Dependencies**: Step 14
**CODING_STANDARDS compliance**: Parameters from config

### Step 40: Implement Order Validation
**Task**: Add comprehensive order validation
```python
async def validate_order(self, order: Order) -> List[str]:
    # Check min/max from config.exchanges[exchange].min_order_size
    # Check tick size from config.exchanges[exchange].tick_size
    # Check lot size from config.exchanges[exchange].lot_size
```
**Validation**: Orders validated against exchange rules
**Dependencies**: Step 19
**CODING_STANDARDS compliance**: All rules from config

## CHECKPOINT: Steps 31-40 CODING_STANDARDS Review

### Verification Against CODING_STANDARDS.md:

1. **NO ASSUMPTIONS** ✓
   - No assumptions about alert availability (Step 31: checks enabled flag)
   - No assumptions about metric retention (Step 32: from config)
   - No assumptions about snapshot format (Step 33: explicit paths)
   - No assumptions about shutdown order (Step 34: explicit sequence)
   - No assumptions about strategy availability (Step 37: explicit registry)
   - No assumptions about order validity (Step 40: comprehensive checks)

2. **NO HARDCODED VALUES** ✓
   - Alert methods from config (Step 31: alert_methods)
   - Metrics intervals from config (Step 32: metrics_collection_interval)
   - Backup counts from config (Step 33: state_backup_count)
   - Shutdown grace period from config (Step 34: shutdown_grace_period)
   - Strategy params from config (Step 36: momentum settings)
   - Position limits from config (Step 38: max_positions)
   - Drawdown parameters from config (Step 39: max_drawdown_pct)
   - Order validation rules from config (Step 40: min/max sizes)

3. **FAIL FAST PHILOSOPHY** ✓
   - Main.py validates config at startup (Step 35)
   - No silent strategy failures (Step 36)
   - Explicit limit violations returned (Step 38)
   - No graceful degradation in drawdown (Step 39)
   - Order validation fails explicitly (Step 40)

4. **SINGLE CONFIG LOAD POINT** ✓
   - Main.py loads config ONCE (Step 35)
   - All services receive config via injection
   - No service loads its own config
   - Secrets loaded separately but also once

5. **PATH AND FILE SAFETY** ✓
   - State snapshots use Path objects (Step 33)
   - Backup directory from config (Step 33)
   - Platform-independent path handling
   - No hardcoded file paths

### Architecture Validation:
- ✓ All strategies extend BaseStrategy with config injection
- ✓ Strategy registry explicitly loads enabled strategies only
- ✓ Position limits enforced at multiple levels
- ✓ Drawdown protection integrated with risk service
- ✓ Order validation uses exchange-specific rules from config

### CI/CD Checks to Add:
```bash
# Run these checks before proceeding to Step 41
grep -r "AlertService" cyberdelta/ | grep -v "config"
grep -r "metrics.*=" cyberdelta/ | grep -E "[0-9]+"
grep -r "shutdown" cyberdelta/ | grep -E "sleep\([0-9]"
grep -r "MomentumStrategy" cyberdelta/ | grep -E "Decimal\(\"[0-9]"
grep -r "position.*limit" cyberdelta/ | grep -v "config\."
rg "drawdown" cyberdelta/ --type py | grep -E "[0-9]+(\.[0-9]+)?"
```

### Step 41: Implement Fill Processing
**Task**: Create `logic/trading/fill_handler.py`
```python
class FillHandler:
    def __init__(self, config: AppSettings, portfolio_service: PortfolioService):
        self.config = config
        # Process fills, update portfolio
        # Calculate fees from config.exchanges[exchange].fee_structure
```
**Validation**: Fills processed accurately
**Dependencies**: Steps 11, 19
**CODING_STANDARDS compliance**: Fee calc from config

### Step 42: Implement PnL Calculation
**Task**: Add PnL tracking to PortfolioService
```python
async def calculate_pnl(self) -> PnLReport:
    # Use config.calculation.pnl_calculation_method
    # Include fees based on config.calculation.include_fees_in_pnl
    # Use config.calculation.base_currency for conversion
```
**Validation**: PnL calculated per config method
**Dependencies**: Step 11
**CODING_STANDARDS compliance**: Calc method from config

### Step 43: Implement Performance Metrics
**Task**: Add performance tracking
```python
class PerformanceTracker:
    def __init__(self, config: AppSettings):
        self._metrics_config = config.calculation.performance_metrics
        # Calculate only enabled metrics
        # Use configured time periods
```
**Validation**: Metrics match configuration
**Dependencies**: Step 32
**CODING_STANDARDS compliance**: Metrics selection from config

### Step 44: Implement Audit Logging
**Task**: Add audit trail using structured logging
```python
async def log_audit_event(self, event: AuditEvent) -> None:
    # Use cyberdelta.logging.logging_helpers
    # Include all configured audit fields
    # Never log sensitive data unless config.general.log_sensitive_data
```
**Validation**: Audit trail complete and compliant
**Dependencies**: Step 7
**CODING_STANDARDS compliance**: Uses structured logging

### Step 45: Implement Safe Mode
**Task**: Add paper trading support
```python
class SafeModeWrapper:
    def __init__(self, config: AppSettings, real_service: Any):
        self._safe_mode = config.general.safe_mode
        self._real_service = real_service
        # Intercept and simulate if safe_mode enabled
```
**Validation**: Safe mode prevents real trades
**Dependencies**: All services
**CODING_STANDARDS compliance**: Mode from config

---

## Phase 5: Testing & Documentation (Steps 46-50)

### Step 46: Create Integration Tests
**Task**: Write end-to-end tests
```python
@pytest.mark.integration
async def test_signal_to_execution_flow(test_config):
    # Use test configuration
    # Verify signal → risk → execution → portfolio flow
    # Assert using configured tolerances
```
**Validation**: Full flow works with test config
**Dependencies**: Steps 1-45
**CODING_STANDARDS compliance**: Test configs explicit

### Step 47: Create Unit Tests
**Task**: Write comprehensive unit tests
```python
@pytest.mark.asyncio
async def test_position_sizing(test_config):
    # Test with various config.risk.sizing settings
    # Verify bounds enforcement
    # No hardcoded test values
```
**Validation**: 90%+ code coverage
**Dependencies**: Steps 1-45
**CODING_STANDARDS compliance**: Tests use config

### Step 48: Create Performance Tests
**Task**: Add load and performance tests
```python
async def test_throughput(test_config, benchmark):
    # Use config.testing.performance_thresholds
    # Measure against configured SLAs
    # Fail if below thresholds
```
**Validation**: System meets performance targets
**Dependencies**: Steps 1-45
**CODING_STANDARDS compliance**: Thresholds from config

### Step 49: Generate API Documentation
**Task**: Document all public interfaces
```python
"""
PortfolioService: Manages portfolio state across all exchanges.

Configuration:
- config.general.state_file: Primary state persistence location
- config.validation.balance_tolerance: Balance reconciliation tolerance
- config.general.state_save_interval: Auto-save frequency

All parameters from AppSettings, no defaults accepted.
"""
```
**Validation**: All services documented
**Dependencies**: Steps 1-45
**CODING_STANDARDS compliance**: Docs emphasize config

### Step 50: Create Operational Runbook
**Task**: Write deployment and operation guide
```markdown
# Operational Runbook

## Configuration
ALL behavior controlled via config files:
- config/settings.yaml - Main configuration
- config/secrets.yaml - API credentials

## Monitoring
- Check logs for structured events
- Monitor metrics per config.monitoring settings
- Alerts sent per config.monitoring.alert_methods

## NO HARDCODED VALUES
System will fail to start if configuration incomplete.
This is intentional - no defaults for critical operations.
```
**Validation**: Operations team can run system
**Dependencies**: Steps 1-49
**CODING_STANDARDS compliance**: Emphasizes config-first

### 🔍 Step 50 Checkpoint: FINAL CODING_STANDARDS Compliance Review

**Final System-Wide Verification for Steps 41-50:**

1. **CALCULATION ACCURACY**:
   - [ ] All calculations use Decimal, never float
   - [ ] Fee calculations from exchange config
   - [ ] PnL methods from config
   - [ ] No hardcoded conversion rates

2. **TESTING STANDARDS**:
   - [ ] Test configurations explicit
   - [ ] No hardcoded test data
   - [ ] Performance thresholds from config
   - [ ] Tests verify config-driven behavior

3. **DOCUMENTATION**:
   - [ ] All config parameters documented
   - [ ] No default values mentioned
   - [ ] Emphasis on configuration-first
   - [ ] Clear operational guidelines

4. **AUDIT & COMPLIANCE**:
   - [ ] Audit logging uses structured format
   - [ ] Sensitive data handling per config
   - [ ] Complete transaction traceability
   - [ ] No data logged without config permission

5. **SAFE MODE OPERATIONS**:
   - [ ] Paper trading fully isolated
   - [ ] No accidental real trades
   - [ ] Mode clearly indicated in logs
   - [ ] All operations respect safe mode

**Final Validation Commands:**
```bash
# Full codebase scan for violations
./scripts/check_no_hardcoding.py cyberdelta/
./scripts/check_no_assumptions.py cyberdelta/

# Verify test quality
grep -r "assert.*==" tests/ --include="*.py" | grep "[0-9]" | grep -v "config"
grep -r "timeout.*=" tests/ --include="*.py" | grep -v "config"

# Documentation completeness
find cyberdelta/logic -name "*.py" -exec grep -L '"""' {} \;

# Final type safety check
mypy cyberdelta/logic/ --strict
```

**System Readiness Checklist:**
- [ ] All 50 steps completed
- [ ] All checkpoints passed
- [ ] Zero CODING_STANDARDS violations
- [ ] 90%+ test coverage
- [ ] Documentation complete
- [ ] Operational runbook ready

---

## Implementation Notes

### Critical Success Factors:
1. **NEVER add defaults** to critical operations
2. **ALWAYS use Symbol/ExchangeName** types, not strings  
3. **ALWAYS inject AppSettings** to every service
4. **NEVER use raw logging** - only cyberdelta.logging helpers
5. **FAIL FAST** on any configuration or validation issues

### Testing Each Step:
- Unit test with explicit test configs
- Integration test with full config
- Verify NO hardcoded values via grep checks
- Ensure all paths/values from configuration

### Rollback Plan:
Each step is atomic and can be rolled back independently. The system remains non-functional until Phase 3 completion, ensuring no partial implementations go live.

### Compliance Verification Process:
Every 10 steps, before proceeding:
1. Run all automated verification commands
2. Fix any violations found
3. Document exceptions with justification
4. Get code review focused on CODING_STANDARDS
5. Update compliance tracking document

This ensures continuous compliance throughout implementation rather than discovering violations at the end.