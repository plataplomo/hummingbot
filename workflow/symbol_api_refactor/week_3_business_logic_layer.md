# Week 3: Business Logic Layer Implementation Guide
**Phases 15-17 | Duration: 4 days | Focus: Core business logic operates with domain objects**

## 🎯 Week 3 Objectives

**PRIMARY GOAL**: Transform core business logic to operate entirely with domain objects

**BUILDING ON WEEKS 1-2**: Data flows now use domain objects - integrate with business operations

**CRITICAL SUCCESS FACTORS**:
- Core execution handler uses ExchangeSymbol for trade execution
- Portfolio tracker manages positions with domain objects
- Signal generation and processing uses domain objects
- Risk management calculations use domain objects
- Strategy layer operates with domain objects
- Complete business logic pipeline uses domain objects throughout

## 📅 Condensed Implementation Schedule

### **Day 1: PHASE 15 - Core Business Logic Layer**
**Impact**: Central business operations use domain objects
**Focus**: Execution, data handling, portfolio management

#### Morning Tasks (4 hours)

##### 15.1 Update Execution Handler
```bash
# File: cyberdelta/core/execution_handler.py
```

**Add Domain Imports**:
```python
from cyberdelta.core.symbols.models import ExchangeSymbol, create_exchange_symbol
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName
```

**Update Trade Signal Execution**:
```python
class ExecutionHandler:
    def __init__(self, symbol_service: SymbolService, ...):
        self.symbol_service = symbol_service
        # ... existing initialization

    async def execute_trade_signal(self, signal: TradeSignal) -> None:
        """Execute trade signal using domain objects."""

        # Signal now contains ExchangeSymbol
        logger.info("Executing trade signal",
                   symbol=signal.symbol.value,
                   exchange=signal.symbol.exchange_id,
                   side=signal.side)

        # Create order args with domain object
        order_args = PlaceOrderArgs(
            symbol=signal.symbol,  # Already ExchangeSymbol from signal
            side=signal.side,
            order_type=signal.order_type,
            quantity=signal.quantity,
            price=signal.price,
            time_in_force=signal.time_in_force,
        )

        # Route to appropriate exchange service
        if signal.symbol.exchange_id == ExchangeName.HYPERLIQUID:
            order = await self.hyperliquid_api.place_order(order_args)
        elif signal.symbol.exchange_id == ExchangeName.BACKPACK:
            order = await self.backpack_api.place_order(order_args)
        else:
            raise ValueError(f"Unsupported exchange: {signal.symbol.exchange_id}")

        # Order returned contains ExchangeSymbol
        logger.info("Order placed", order_id=order.exchange_order_id, symbol=order.symbol.value)

    async def execute_arbitrage_trades(self, opportunities: list[ArbitrageOpportunity]) -> None:
        """Execute arbitrage trades using domain objects."""

        for opportunity in opportunities:
            # Opportunity contains ExchangeSymbol objects for both legs
            buy_args = PlaceOrderArgs(
                symbol=opportunity.buy_symbol,  # ExchangeSymbol
                side=OrderSide.BUY,
                # ... rest
            )

            sell_args = PlaceOrderArgs(
                symbol=opportunity.sell_symbol,  # ExchangeSymbol
                side=OrderSide.SELL,
                # ... rest
            )

            # Execute both legs with domain objects
            await asyncio.gather(
                self._execute_order_on_exchange(buy_args),
                self._execute_order_on_exchange(sell_args)
            )
```

##### 15.2 Update Data Handler
```bash
# File: cyberdelta/core/data_handler.py
```

**Update Market Data Processing**:
```python
class DataHandler:
    async def process_ticker_update(self, ticker: Ticker) -> None:
        """Process ticker update with domain object."""

        # ticker.symbol is ExchangeSymbol
        logger.debug("Processing ticker",
                    symbol=ticker.symbol.value,
                    exchange=ticker.symbol.exchange_id,
                    price=ticker.last_price)

        # Store with domain object key
        await self.price_cache.set(
            key=f"{ticker.symbol.exchange_id.value}:{ticker.symbol.value}",
            value=ticker.last_price
        )

        # Emit signals with domain objects
        price_signal = PriceSignal(
            symbol=ticker.symbol,  # ExchangeSymbol
            price=ticker.last_price,
            timestamp=ticker.timestamp
        )

        await self.signal_queue.put(price_signal)

    async def process_order_book_update(self, order_book: OrderBook) -> None:
        """Process order book with domain object."""

        # order_book.symbol is ExchangeSymbol
        spread = order_book.best_ask - order_book.best_bid

        if spread > self.config.max_spread:
            logger.warning("Wide spread detected",
                          symbol=order_book.symbol.value,
                          spread=spread)

        # Update market state with domain object
        await self.market_state.update_order_book(order_book.symbol, order_book)
```

#### Afternoon Tasks (4 hours)

##### 15.3 Update Portfolio Tracker
```bash
# File: cyberdelta/core/portfolio_tracker.py
```

**Update Position Management**:
```python
class PortfolioTracker:
    def __init__(self, symbol_service: SymbolService, ...):
        self.symbol_service = symbol_service
        self.positions: dict[str, Position] = {}  # Key format: "exchange:symbol"

    def _position_key(self, symbol: ExchangeSymbol) -> str:
        """Create consistent position key from domain object."""
        return f"{symbol.exchange_id.value}:{symbol.value}"

    async def update_position(self, position: Position) -> None:
        """Update position using domain object."""

        # position.symbol is ExchangeSymbol
        position_key = self._position_key(position.symbol)

        logger.info("Updating position",
                   symbol=position.symbol.value,
                   exchange=position.symbol.exchange_id,
                   size=position.size)

        self.positions[position_key] = position

        # Emit position update signal with domain object
        signal = PositionUpdateSignal(
            symbol=position.symbol,  # ExchangeSymbol
            old_size=self.positions.get(position_key, Position()).size,
            new_size=position.size,
        )

        await self.signal_queue.put(signal)

    async def get_position(self, symbol: ExchangeSymbol) -> Position | None:
        """Get position using domain object."""
        position_key = self._position_key(symbol)
        return self.positions.get(position_key)

    async def calculate_portfolio_pnl(self) -> dict[ExchangeName, Decimal]:
        """Calculate PnL by exchange using domain objects."""
        pnl_by_exchange = {}

        for position in self.positions.values():
            exchange = position.symbol.exchange_id  # ExchangeSymbol.exchange_id

            if exchange not in pnl_by_exchange:
                pnl_by_exchange[exchange] = Decimal(0)

            pnl_by_exchange[exchange] += position.unrealized_pnl

        return pnl_by_exchange
```

##### 15.4 Update Signal Generator
```bash
# File: cyberdelta/core/signal_generator.py
```

**Update Signal Generation**:
```python
class SignalGenerator:
    async def generate_arbitrage_signals(self) -> list[ArbitrageSignal]:
        """Generate arbitrage signals using domain objects."""

        signals = []

        # Get all tickers (each contains ExchangeSymbol)
        hl_tickers = await self.hyperliquid_api.get_all_tickers()  # list[Ticker with ExchangeSymbol]
        bp_tickers = await self.backpack_api.get_all_tickers()    # list[Ticker with ExchangeSymbol]

        # Find matching symbols across exchanges using domain objects
        for hl_ticker in hl_tickers:
            for bp_ticker in bp_tickers:
                # Compare internal symbol values (BTC == BTC across exchanges)
                if (hl_ticker.symbol.internal_symbol and bp_ticker.symbol.internal_symbol and
                    hl_ticker.symbol.internal_symbol.base_asset == bp_ticker.symbol.internal_symbol.base_asset):

                    price_diff = abs(hl_ticker.last_price - bp_ticker.last_price)
                    threshold = hl_ticker.last_price * self.config.arbitrage_threshold

                    if price_diff > threshold:
                        # Create signal with domain objects
                        signal = ArbitrageSignal(
                            buy_symbol=hl_ticker.symbol if hl_ticker.last_price < bp_ticker.last_price else bp_ticker.symbol,
                            sell_symbol=bp_ticker.symbol if hl_ticker.last_price < bp_ticker.last_price else hl_ticker.symbol,
                            price_difference=price_diff,
                            opportunity_size=self._calculate_opportunity_size(hl_ticker.symbol, bp_ticker.symbol)
                        )
                        signals.append(signal)

        return signals
```

#### End of Day 1 Deliverable
- [x] Core business logic handlers use ExchangeSymbol
- [x] Trade execution operates with domain objects
- [x] Portfolio management uses domain objects
- [x] Signal generation creates domain objects

---

### **Day 2-3: PHASE 16 - Strategy Layer**
**Impact**: Trading strategies operate with domain objects
**Focus**: Strategy implementations and base classes

#### Day 2 Morning Tasks (4 hours)

##### 16.1 Update Funding Rate Arbitrage Strategy
```bash
# File: cyberdelta/strategies/funding_rate_arbitrage.py
```

**Update Strategy Implementation**:
```python
class FundingRateArbitrageStrategy(Strategy):
    def __init__(self, symbol_service: SymbolService, ...):
        super().__init__(symbol_service, ...)
        self.tracked_symbols: list[ExchangeSymbol] = []

    async def initialize(self, config: StrategyConfig) -> None:
        """Initialize strategy with domain objects."""

        # Convert string symbols to domain objects
        for symbol_str in config.symbols:
            # Parse string to appropriate exchange symbols
            hl_symbol = create_exchange_symbol(symbol_str, ExchangeName.HYPERLIQUID)
            bp_symbol = create_exchange_symbol(symbol_str, ExchangeName.BACKPACK)

            self.tracked_symbols.extend([hl_symbol, bp_symbol])

        logger.info("Strategy initialized",
                   symbol_count=len(self.tracked_symbols),
                   symbols=[s.value for s in self.tracked_symbols])

    async def analyze_opportunities(self) -> list[TradingOpportunity]:
        """Analyze opportunities using domain objects."""

        opportunities = []

        for symbol in self.tracked_symbols:
            # Get funding rates with domain objects
            if symbol.exchange_id == ExchangeName.HYPERLIQUID:
                funding_rate = await self.hyperliquid_api.get_funding_rate(
                    GetFundingRateArgs(symbol=symbol)  # ExchangeSymbol
                )
            elif symbol.exchange_id == ExchangeName.BACKPACK:
                funding_rate = await self.backpack_api.get_funding_rate(
                    GetFundingRateArgs(symbol=symbol)  # ExchangeSymbol
                )

            # funding_rate.symbol is ExchangeSymbol
            if abs(funding_rate.rate) > self.config.funding_threshold:
                opportunity = TradingOpportunity(
                    symbol=symbol,  # ExchangeSymbol
                    strategy_type="funding_arbitrage",
                    expected_return=self._calculate_funding_return(funding_rate),
                    confidence=self._calculate_confidence(funding_rate),
                )
                opportunities.append(opportunity)

        return opportunities

    async def generate_signals(self, opportunities: list[TradingOpportunity]) -> list[TradeSignal]:
        """Generate trade signals with domain objects."""

        signals = []

        for opportunity in opportunities:
            # opportunity.symbol is ExchangeSymbol
            position_size = await self._calculate_position_size(opportunity.symbol)

            signal = TradeSignal(
                symbol=opportunity.symbol,  # ExchangeSymbol
                side=OrderSide.BUY if opportunity.expected_return > 0 else OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity=position_size,
                price=await self._get_optimal_price(opportunity.symbol),
                strategy="funding_rate_arbitrage",
                confidence=opportunity.confidence,
            )
            signals.append(signal)

        return signals
```

#### Day 2 Afternoon Tasks (4 hours)

##### 16.2 Update Strategy Base Class
```bash
# File: cyberdelta/core/strategy.py
```

**Update Strategy Base**:
```python
class Strategy(ABC):
    def __init__(self, symbol_service: SymbolService, ...):
        self.symbol_service = symbol_service
        self.managed_symbols: list[ExchangeSymbol] = []

    @abstractmethod
    async def analyze_market_data(self, market_data: MarketData) -> list[TradingOpportunity]:
        """Analyze market data with domain objects."""
        # market_data contains objects with ExchangeSymbol
        pass

    @abstractmethod
    async def generate_signals(self, opportunities: list[TradingOpportunity]) -> list[TradeSignal]:
        """Generate signals with domain objects."""
        # Return signals with ExchangeSymbol
        pass

    async def get_symbol_for_exchange(self, internal_symbol: str, exchange: ExchangeName) -> ExchangeSymbol:
        """Get exchange-specific symbol using symbol service."""
        return self.symbol_service.get_exchange_symbol(internal_symbol, exchange.value)

    async def validate_symbol_availability(self, symbol: ExchangeSymbol) -> bool:
        """Validate symbol is available for trading."""
        # Use domain object for validation
        return await self._check_exchange_availability(symbol)
```

#### Day 3 All Day Tasks (8 hours)

##### 16.3 Update Strategy Manager
```bash
# File: cyberdelta/core/strategy_manager.py
```

**Update Strategy Management**:
```python
class StrategyManager:
    def __init__(self, symbol_service: SymbolService, ...):
        self.symbol_service = symbol_service
        self.strategies: list[Strategy] = []
        self.active_symbols: set[ExchangeSymbol] = set()

    async def register_strategy(self, strategy: Strategy, config: StrategyConfig) -> None:
        """Register strategy with domain object management."""

        await strategy.initialize(config)
        self.strategies.append(strategy)

        # Track symbols as domain objects
        for symbol in strategy.managed_symbols:
            self.active_symbols.add(symbol)

        logger.info("Strategy registered",
                   strategy=strategy.__class__.__name__,
                   symbols=[s.value for s in strategy.managed_symbols])

    async def process_market_update(self, market_data: MarketData) -> None:
        """Process market updates for all strategies."""

        # market_data contains domain objects
        affected_strategies = []

        for strategy in self.strategies:
            # Check if strategy manages this symbol
            if market_data.symbol in strategy.managed_symbols:
                affected_strategies.append(strategy)

        # Process in parallel
        await asyncio.gather(*[
            strategy.analyze_market_data(market_data)
            for strategy in affected_strategies
        ])

    async def execute_strategy_signals(self) -> None:
        """Execute all strategy signals with domain objects."""

        all_signals = []

        for strategy in self.strategies:
            opportunities = await strategy.analyze_opportunities()
            signals = await strategy.generate_signals(opportunities)
            all_signals.extend(signals)

        # Execute signals (each contains ExchangeSymbol)
        for signal in all_signals:
            await self.execution_handler.execute_trade_signal(signal)
```

##### 16.4 Update Additional Strategy Components
- Strategy factory
- Signal validation
- Risk integration

#### End of Day 2-3 Deliverable
- [x] All strategies use ExchangeSymbol throughout
- [x] Strategy base classes support domain objects
- [x] Strategy manager operates with domain objects
- [x] Signal generation and execution uses domain objects

---

### **Day 4: PHASE 17 - Risk Management Integration**
**Impact**: Risk calculations and constraints use domain objects
**Focus**: Position sizing, constraint validation, risk metrics

#### Morning Tasks (4 hours)

##### 17.1 Update Risk Manager
```bash
# File: cyberdelta/core/risk_manager.py
```

**Update Risk Calculations**:
```python
class RiskManager:
    def __init__(self, symbol_service: SymbolService, ...):
        self.symbol_service = symbol_service
        self.position_limits: dict[str, Decimal] = {}  # Key: "exchange:symbol"

    def _risk_key(self, symbol: ExchangeSymbol) -> str:
        """Create risk key from domain object."""
        return f"{symbol.exchange_id.value}:{symbol.value}"

    async def validate_trade_signal(self, signal: TradeSignal) -> RiskValidationResult:
        """Validate trade signal using domain objects."""

        # signal.symbol is ExchangeSymbol
        logger.debug("Validating trade signal",
                    symbol=signal.symbol.value,
                    exchange=signal.symbol.exchange_id,
                    quantity=signal.quantity)

        violations = []

        # Position size validation
        current_position = await self.portfolio_tracker.get_position(signal.symbol)
        new_position_size = (current_position.size if current_position else Decimal(0)) + signal.quantity

        risk_key = self._risk_key(signal.symbol)
        max_position = self.position_limits.get(risk_key, self.config.default_max_position)

        if abs(new_position_size) > max_position:
            violations.append(RiskViolation(
                rule="position_limit",
                symbol=signal.symbol,  # ExchangeSymbol
                current_value=abs(new_position_size),
                limit_value=max_position,
            ))

        # Concentration risk validation
        total_exposure = await self._calculate_symbol_exposure(signal.symbol)
        if total_exposure > self.config.max_symbol_concentration:
            violations.append(RiskViolation(
                rule="concentration_limit",
                symbol=signal.symbol,
                current_value=total_exposure,
                limit_value=self.config.max_symbol_concentration,
            ))

        return RiskValidationResult(
            approved=len(violations) == 0,
            violations=violations,
            adjusted_quantity=self._calculate_safe_quantity(signal) if violations else signal.quantity
        )

    async def calculate_position_size(self, signal: TradeSignal) -> Decimal:
        """Calculate optimal position size using domain objects."""

        # Get market data with domain object
        ticker = await self._get_ticker_for_symbol(signal.symbol)
        volatility = await self._calculate_volatility(signal.symbol)

        # Kelly criterion calculation
        win_rate = await self._get_strategy_win_rate(signal.strategy, signal.symbol)
        avg_win = await self._get_average_win(signal.strategy, signal.symbol)
        avg_loss = await self._get_average_loss(signal.strategy, signal.symbol)

        kelly_fraction = self._calculate_kelly_fraction(win_rate, avg_win, avg_loss)

        # Account balance for exchange
        account_balance = await self._get_account_balance(signal.symbol.exchange_id)

        # Calculate size
        raw_size = account_balance * kelly_fraction * self.config.kelly_multiplier

        # Apply position limits
        risk_key = self._risk_key(signal.symbol)
        max_position = self.position_limits.get(risk_key, self.config.default_max_position)

        return min(raw_size, max_position)
```

#### Afternoon Tasks (4 hours)

##### 17.2 Update Position Sizing Services
```bash
# File: cyberdelta/core/sizing/position_sizer.py
```

**Update Kelly Criterion Sizer**:
```python
class KellyCriterionSizer:
    async def calculate_size(self, signal: TradeSignal, portfolio_state: PortfolioState) -> Decimal:
        """Calculate position size using domain objects."""

        # signal.symbol is ExchangeSymbol
        historical_performance = await self._get_symbol_performance(
            symbol=signal.symbol,  # ExchangeSymbol
            strategy=signal.strategy,
            lookback_days=self.config.lookback_days
        )

        # Calculate Kelly parameters
        win_rate = historical_performance.win_rate
        avg_win_pct = historical_performance.avg_win_percentage
        avg_loss_pct = historical_performance.avg_loss_percentage

        # Kelly fraction
        kelly_f = (win_rate * avg_win_pct - (1 - win_rate) * avg_loss_pct) / avg_win_pct

        # Account balance for symbol's exchange
        exchange_balance = portfolio_state.get_exchange_balance(signal.symbol.exchange_id)

        # Calculate size with Kelly
        kelly_size = exchange_balance * kelly_f * self.config.kelly_multiplier

        # Apply risk constraints using domain object
        max_size = await self._get_max_position_size(signal.symbol)

        return min(kelly_size, max_size)
```

##### 17.3 Update Risk Validation Services
```bash
# File: cyberdelta/core/services/validation.py
```

**Update Constraint Validation**:
```python
class ValidationService:
    async def validate_symbol_constraints(self, symbol: ExchangeSymbol) -> ValidationResult:
        """Validate symbol constraints using domain object."""

        violations = []

        # Check if symbol is supported on exchange
        if not await self._is_symbol_supported(symbol):
            violations.append(ValidationViolation(
                rule="symbol_support",
                message=f"Symbol {symbol.value} not supported on {symbol.exchange_id.value}"
            ))

        # Check trading hours
        if not await self._is_trading_hours(symbol):
            violations.append(ValidationViolation(
                rule="trading_hours",
                message=f"Symbol {symbol.value} outside trading hours"
            ))

        # Check minimum order size
        min_order_size = await self._get_min_order_size(symbol)
        if symbol.lot_size and symbol.lot_size < min_order_size:
            violations.append(ValidationViolation(
                rule="min_order_size",
                message=f"Order size below minimum for {symbol.value}"
            ))

        return ValidationResult(
            valid=len(violations) == 0,
            violations=violations
        )
```

##### 17.4 Integration Testing
```bash
# Test complete business logic flow
pytest tests/integration/core/test_execution_handler.py -v
pytest tests/integration/strategies/test_funding_rate_arbitrage.py -v
pytest tests/unit/core/test_risk_manager_additional.py -v
```

#### End of Day 4 Deliverable
- [x] Risk management uses ExchangeSymbol throughout
- [x] Position sizing calculations use domain objects
- [x] Constraint validation operates with domain objects
- [x] Complete business logic pipeline uses domain objects

**🎯 WEEK 3 MILESTONE ACHIEVED**:
- Complete business logic layer operates with domain objects
- No string-based symbol operations in business logic
- Foundation ready for test infrastructure cleanup

---

## 🔍 Week 3 Success Criteria

### Must Pass Before Week 4
- [ ] **Core execution handler uses ExchangeSymbol** for all operations
- [ ] **Portfolio tracker manages positions** with domain objects
- [ ] **Signal generation and processing** uses domain objects throughout
- [ ] **All strategies operate** with ExchangeSymbol objects
- [ ] **Risk management calculations** use domain objects
- [ ] **Position sizing services** use domain objects
- [ ] **No string symbol operations** in business logic layer
- [ ] **mypy passes** for all updated files
- [ ] **Integration tests pass** for complete business flows

### Validation Commands
```bash
# Type checking for business logic
mypy cyberdelta/core/execution_handler.py
mypy cyberdelta/core/portfolio_tracker.py
mypy cyberdelta/core/signal_generator.py
mypy cyberdelta/core/risk_manager.py
mypy cyberdelta/strategies/
mypy cyberdelta/core/sizing/

# Test execution
pytest tests/unit/core/test_execution_handler.py -v
pytest tests/unit/core/test_portfolio_tracker.py -v
pytest tests/unit/core/test_risk_manager_additional.py -v
pytest tests/unit/strategies/test_funding_rate_arbitrage.py -v
pytest tests/integration/core/test_execution_handler.py -v
pytest tests/integration/strategies/test_funding_rate_arbitrage.py -v

# String usage audit (should return 0 results in business logic)
grep -r "symbol.*str" cyberdelta/core/ --exclude-dir=models
grep -r "symbol.*str" cyberdelta/strategies/
```

### Expected Metrics After Week 3
- **Data Layer**: 100% domain objects ✅ (from Weeks 1-2)
- **Business Logic Layer**: 100% domain objects ✅ (new)
- **Strategy Layer**: 100% domain objects ✅ (new)
- **Risk Management**: 100% domain objects ✅ (new)
- **Test Coverage**: All business logic tests passing
- **Architecture**: Clean separation of concerns with domain objects

---

## 🔄 Week 3 Integration Patterns

### Business Logic Pattern
```python
async def business_operation(self, input_with_domain: DomainInput) -> DomainOutput:
    """Business operation using domain objects throughout."""

    # Input validation with domain objects
    validation_result = await self.validator.validate(input_with_domain.symbol)
    if not validation_result.valid:
        raise ValidationError(validation_result.violations)

    # Business logic with domain objects
    result = await self._perform_operation(input_with_domain)

    # Output with domain objects
    return DomainOutput(
        symbol=input_with_domain.symbol,  # ExchangeSymbol
        result=result,
        timestamp=datetime.now(UTC)
    )
```

### Strategy Integration Pattern
```python
class MyStrategy(Strategy):
    async def analyze_and_signal(self) -> list[TradeSignal]:
        """Complete strategy flow with domain objects."""

        # Get market data (contains ExchangeSymbol objects)
        market_data = await self.data_handler.get_latest_data(self.managed_symbols)

        # Analyze opportunities
        opportunities = await self.analyze_market_data(market_data)

        # Generate signals with domain objects
        signals = await self.generate_signals(opportunities)

        # Risk validation with domain objects
        validated_signals = []
        for signal in signals:
            risk_result = await self.risk_manager.validate_trade_signal(signal)
            if risk_result.approved:
                signal.quantity = risk_result.adjusted_quantity
                validated_signals.append(signal)

        return validated_signals
```

### Risk Management Integration Pattern
```python
async def comprehensive_risk_check(self, signal: TradeSignal) -> RiskDecision:
    """Comprehensive risk validation with domain objects."""

    # Portfolio impact analysis
    portfolio_impact = await self._analyze_portfolio_impact(signal.symbol, signal.quantity)

    # Concentration risk check
    concentration_risk = await self._check_concentration_risk(signal.symbol)

    # Market risk assessment
    market_risk = await self._assess_market_risk(signal.symbol)

    # Combined risk decision
    return RiskDecision(
        symbol=signal.symbol,  # ExchangeSymbol
        approved=all([portfolio_impact.approved, concentration_risk.approved, market_risk.approved]),
        recommended_size=min(portfolio_impact.max_size, concentration_risk.max_size, market_risk.max_size),
        risk_score=self._calculate_combined_risk_score([portfolio_impact, concentration_risk, market_risk])
    )
```

---

## 🚨 Week 3 Critical Success Factors

### Day 1: **CORE FOUNDATION**
- Execution handler uses domain objects
- **EXPECT**: Trade execution works with ExchangeSymbol
- **GOAL**: Central business operations transformed

### Day 2-3: **STRATEGY TRANSFORMATION**
- All strategies use domain objects
- **EXPECT**: Strategy-generated signals contain ExchangeSymbol
- **GOAL**: Complete strategy pipeline uses domain objects

### Day 4: **RISK INTEGRATION**
- Risk management uses domain objects
- **EXPECT**: All risk calculations work with ExchangeSymbol
- **GOAL**: Complete business logic uses domain objects

---

## 🛠️ Week 3 Specialized Tools

### Business Logic Validation Script
```python
# validate_business_logic.py
def check_domain_usage_in_business_logic():
    """Verify business logic uses domain objects."""

    business_logic_files = [
        "cyberdelta/core/execution_handler.py",
        "cyberdelta/core/portfolio_tracker.py",
        "cyberdelta/core/signal_generator.py",
        "cyberdelta/core/risk_manager.py",
        "cyberdelta/strategies/",
    ]

    for file_path in business_logic_files:
        # Check for ExchangeSymbol usage
        assert_domain_object_usage(file_path)

        # Check for no string symbol operations
        assert_no_string_symbols(file_path)
```

### Signal Flow Testing
```bash
# Test complete signal flow with domain objects
function test_signal_flow() {
    echo "Testing complete signal flow..."

    # Generate signal -> Risk validation -> Execution
    pytest tests/integration/core/test_execution_handler.py::test_complete_signal_flow -v

    # Strategy signal generation
    pytest tests/integration/strategies/ -v -k "signal_generation"

    # Risk management integration
    pytest tests/unit/core/test_risk_manager_additional.py::test_domain_object_validation -v
}
```

---

## 📋 Week 3 Deliverables

### Code Changes (12+ core files)
#### Core Business Logic
- [ ] `cyberdelta/core/execution_handler.py` - Trade execution with ExchangeSymbol
- [ ] `cyberdelta/core/data_handler.py` - Data processing with domain objects
- [ ] `cyberdelta/core/portfolio_tracker.py` - Position management with ExchangeSymbol
- [ ] `cyberdelta/core/signal_generator.py` - Signal generation with domain objects

#### Strategy Layer
- [ ] `cyberdelta/strategies/funding_rate_arbitrage.py` - Strategy with ExchangeSymbol
- [ ] `cyberdelta/core/strategy.py` - Base strategy with domain objects
- [ ] `cyberdelta/core/strategy_manager.py` - Strategy management with ExchangeSymbol

#### Risk Management
- [ ] `cyberdelta/core/risk_manager.py` - Risk calculations with domain objects
- [ ] `cyberdelta/core/sizing/position_sizer.py` - Position sizing with ExchangeSymbol
- [ ] `cyberdelta/core/services/validation.py` - Validation with domain objects

### Documentation
- [ ] Business logic domain object patterns
- [ ] Strategy implementation guidelines
- [ ] Risk management integration guide
- [ ] Signal flow documentation

### Validation
- [ ] All business logic tests passing
- [ ] Integration tests for complete flows
- [ ] Strategy tests with domain objects
- [ ] Risk management tests passing
- [ ] mypy validation clean

**Week 3 completes the business logic transformation - only test cleanup remains in Week 4.** 🚀
