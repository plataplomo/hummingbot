# Week 3: Business Logic Layer Clean Architecture
**Duration: 4 days | Focus: Core business logic with Symbol domain objects**

## 🎯 Week 3 Objectives

**PRIMARY GOAL**: Implement Symbol usage throughout core business logic

**BUILDING ON WEEKS 1-2**: Data flows use Symbol objects - integrate with business operations

**NO BACKWARD COMPATIBILITY**: Clean Symbol architecture in all business logic

## 📊 Business Logic Components

### Core Components Using Symbols
1. **Execution Handler** - Trade execution with Symbol objects
2. **Portfolio Tracker** - Position management by Symbol
3. **Signal Generator** - Trading signals with Symbols
4. **Risk Manager** - Risk calculations per Symbol
5. **Strategy Layer** - Strategy decisions using Symbols

## 📅 Implementation Schedule

### **Day 1: Execution Handler**
**Focus**: Trade execution using Symbol objects

#### Update Execution Handler
```python
# File: cyberdelta/core/execution_handler.py
from typing import Any
from cyberdelta.core.symbols import exchanges, symbols, get_symbol_service
from cyberdelta.core.symbols.models import Symbol, BaseSymbol
from cyberdelta.enums.exchange_names import ExchangeName

class ExecutionHandler:
    def __init__(self, ...):
        # No symbol service injection needed - use registry
        self.hyperliquid_api = hyperliquid_api
        self.backpack_api = backpack_api
        
    async def execute_trade_signal(self, signal: TradeSignal) -> None:
        """Execute trade signal using Symbol domain object."""
        
        # Signal contains Symbol object
        logger.info("Executing trade signal",
                   symbol=signal.symbol.value,
                   exchange=signal.symbol.exchange,
                   side=signal.side)
        
        # Validate symbol for exchange
        if not self._is_symbol_tradeable(signal.symbol):
            raise ValueError(f"Symbol {signal.symbol.value} not tradeable")
        
        # Create order args with Symbol object
        order_args = PlaceOrderArgs(
            symbol=signal.symbol,  # Domain object
            side=signal.side,
            order_type=signal.order_type,
            quantity=signal.quantity,
            price=signal.price,
            time_in_force=signal.time_in_force,
        )
        
        # Route to appropriate exchange based on Symbol
        if signal.symbol.exchange == ExchangeName.HYPERLIQUID:
            order = await self.hyperliquid_api.place_order(order_args)
        elif signal.symbol.exchange == ExchangeName.BACKPACK:
            order = await self.backpack_api.place_order(order_args)
        else:
            raise ValueError(f"Unsupported exchange: {signal.symbol.exchange}")
        
        # Track execution
        await self._track_order_execution(order)
    
    def _is_symbol_tradeable(self, symbol: Symbol) -> bool:
        """Check if symbol is tradeable."""
        # Business logic using Symbol properties
        if symbol.exchange == ExchangeName.HYPERLIQUID:
            # Check Hyperliquid-specific constraints
            return True  # Simplified
        elif symbol.exchange == ExchangeName.BACKPACK:
            # Check Backpack-specific constraints
            return hasattr(symbol.metadata, 'symbol_id')
        return False
```

#### Arbitrage Execution
```python
async def execute_arbitrage_opportunity(self, opportunity: ArbitrageOpportunity) -> None:
    """Execute arbitrage using Symbol objects."""
    
    # Opportunity contains Symbol objects for both legs
    logger.info("Executing arbitrage",
               buy_symbol=opportunity.buy_symbol.value,
               buy_exchange=opportunity.buy_symbol.exchange,
               sell_symbol=opportunity.sell_symbol.value,
               sell_exchange=opportunity.sell_symbol.exchange)
    
    # Check symbols are equivalent
    service = get_symbol_service()
    if not service.are_equivalent(opportunity.buy_symbol, opportunity.sell_symbol):
        raise ValueError("Symbols are not equivalent for arbitrage")
    
    # Execute both legs
    buy_args = PlaceOrderArgs(
        symbol=opportunity.buy_symbol,  # Symbol object
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=opportunity.quantity
    )
    
    sell_args = PlaceOrderArgs(
        symbol=opportunity.sell_symbol,  # Symbol object
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=opportunity.quantity
    )
    
    # Execute atomically
    await asyncio.gather(
        self._execute_order_on_exchange(buy_args),
        self._execute_order_on_exchange(sell_args)
    )
```

### **Day 2: Portfolio Tracker**
**Focus**: Position and portfolio management with Symbols

#### Update Portfolio Tracker
```python
# File: cyberdelta/core/portfolio_tracker.py
from cyberdelta.core.symbols.models import Symbol, BaseSymbol

class PortfolioTracker:
    def __init__(self, ...):
        # Positions keyed by Symbol
        self.positions: dict[str, Position] = {}
        self.symbol_index: dict[str, Symbol] = {}
        
    def _position_key(self, symbol: Symbol) -> str:
        """Create position key from Symbol object."""
        return f"{symbol.exchange.value}:{symbol.value}"
    
    async def update_position(self, position: Position) -> None:
        """Update position with Symbol object."""
        
        # Position contains Symbol object
        position_key = self._position_key(position.symbol)
        
        logger.info("Updating position",
                   symbol=position.symbol.value,
                   exchange=position.symbol.exchange,
                   size=position.size,
                   pnl=position.unrealized_pnl)
        
        # Store position and index symbol
        self.positions[position_key] = position
        self.symbol_index[position_key] = position.symbol
        
        # Emit update event
        await self._emit_position_update(position)
    
    async def get_position(self, symbol: Symbol) -> Position | None:
        """Get position by Symbol object."""
        position_key = self._position_key(symbol)
        return self.positions.get(position_key)
    
    async def get_positions_by_exchange(self, exchange: ExchangeName) -> list[Position]:
        """Get all positions for an exchange."""
        positions = []
        for position in self.positions.values():
            if position.symbol.exchange == exchange:
                positions.append(position)
        return positions
    
    async def calculate_portfolio_metrics(self) -> PortfolioMetrics:
        """Calculate metrics using Symbol objects."""
        total_value = Decimal("0")
        positions_by_exchange = defaultdict(list)
        
        for position in self.positions.values():
            # Group by exchange using Symbol
            positions_by_exchange[position.symbol.exchange].append(position)
            
            # Calculate value
            position_value = position.size * position.mark_price
            total_value += position_value
        
        return PortfolioMetrics(
            total_value=total_value,
            positions_by_exchange=dict(positions_by_exchange),
            position_count=len(self.positions)
        )
```

### **Day 3: Signal Generator**
**Focus**: Trading signal generation with Symbols

#### Update Signal Generator
```python
# File: cyberdelta/core/signal_generator.py
from cyberdelta.core.symbols import get_symbol_service

class SignalGenerator:
    def __init__(self, ...):
        self.symbol_service = get_symbol_service()
        
    async def generate_arbitrage_signals(self) -> list[ArbitrageSignal]:
        """Generate arbitrage signals using Symbol objects."""
        
        signals = []
        
        # Get market data - all contain Symbol objects
        hl_tickers = await self.hyperliquid_api.get_all_tickers()
        bp_tickers = await self.backpack_api.get_all_tickers()
        
        # Find arbitrage opportunities
        for hl_ticker in hl_tickers:
            for bp_ticker in bp_tickers:
                # Check if symbols are equivalent
                if self.symbol_service.are_equivalent(hl_ticker.symbol, bp_ticker.symbol):
                    # Calculate spread using Symbol objects
                    spread = self._calculate_spread(hl_ticker, bp_ticker)
                    
                    if spread > self.config.min_arbitrage_spread:
                        signal = ArbitrageSignal(
                            buy_symbol=hl_ticker.symbol if hl_ticker.last_price < bp_ticker.last_price else bp_ticker.symbol,
                            sell_symbol=bp_ticker.symbol if hl_ticker.last_price < bp_ticker.last_price else hl_ticker.symbol,
                            spread=spread,
                            expected_profit=self._calculate_expected_profit(spread),
                            timestamp=datetime.now(UTC)
                        )
                        signals.append(signal)
        
        return signals
    
    async def generate_momentum_signals(self) -> list[MomentumSignal]:
        """Generate momentum signals with Symbol objects."""
        
        signals = []
        
        # Get all positions with Symbol objects
        positions = await self.portfolio_tracker.get_all_positions()
        
        for position in positions:
            # Get market data for Symbol
            ticker = await self._get_ticker_for_symbol(position.symbol)
            
            # Calculate momentum indicators
            momentum = self._calculate_momentum(position.symbol, ticker)
            
            if abs(momentum.score) > self.config.momentum_threshold:
                signal = MomentumSignal(
                    symbol=position.symbol,  # Symbol object
                    direction=OrderSide.BUY if momentum.score > 0 else OrderSide.SELL,
                    strength=abs(momentum.score),
                    indicators=momentum.indicators
                )
                signals.append(signal)
        
        return signals
```

### **Day 4: Risk Manager**
**Focus**: Risk calculations with Symbol objects

#### Update Risk Manager
```python
# File: cyberdelta/core/risk_manager.py
class RiskManager:
    def __init__(self, ...):
        self.position_limits: dict[str, PositionLimit] = {}
        
    def _limit_key(self, symbol: Symbol) -> str:
        """Create limit key from Symbol."""
        return f"{symbol.exchange.value}:{symbol.value}"
    
    async def check_position_risk(self, symbol: Symbol, proposed_size: Decimal) -> RiskCheck:
        """Check risk for Symbol position."""
        
        # Get current position
        current_position = await self.portfolio_tracker.get_position(symbol)
        current_size = current_position.size if current_position else Decimal("0")
        
        # Calculate new position
        new_size = current_size + proposed_size
        
        # Check limits
        limit_key = self._limit_key(symbol)
        limit = self.position_limits.get(limit_key)
        
        if limit and abs(new_size) > limit.max_size:
            return RiskCheck(
                approved=False,
                reason=f"Position size {new_size} exceeds limit {limit.max_size} for {symbol.value}"
            )
        
        # Check exchange-specific risks
        if symbol.exchange == ExchangeName.HYPERLIQUID:
            # Hyperliquid-specific risk checks
            leverage = self._calculate_leverage_hl(symbol, new_size)
            if leverage > self.config.max_leverage_hl:
                return RiskCheck(
                    approved=False,
                    reason=f"Leverage {leverage} exceeds max {self.config.max_leverage_hl}"
                )
        
        return RiskCheck(approved=True)
    
    async def calculate_portfolio_risk(self) -> PortfolioRisk:
        """Calculate portfolio-wide risk metrics."""
        
        positions = await self.portfolio_tracker.get_all_positions()
        
        # Group risk by exchange
        risk_by_exchange: dict[ExchangeName, ExchangeRisk] = {}
        
        for position in positions:
            exchange = position.symbol.exchange
            
            if exchange not in risk_by_exchange:
                risk_by_exchange[exchange] = ExchangeRisk(
                    total_exposure=Decimal("0"),
                    position_count=0
                )
            
            # Aggregate by exchange
            risk_by_exchange[exchange].total_exposure += abs(position.size * position.mark_price)
            risk_by_exchange[exchange].position_count += 1
        
        return PortfolioRisk(
            risk_by_exchange=risk_by_exchange,
            total_exposure=sum(r.total_exposure for r in risk_by_exchange.values())
        )
```

## 🎯 Week 3 Success Criteria

### Business Logic Integration ✅
- [ ] Execution handler uses Symbol objects throughout
- [ ] Portfolio tracker manages positions by Symbol
- [ ] Signal generator creates signals with Symbols
- [ ] Risk manager calculates risk per Symbol

### Clean Architecture ✅
- [ ] No string symbol manipulation in business logic
- [ ] Symbol objects flow through all components
- [ ] Exchange routing based on symbol.exchange
- [ ] Symbol equivalence checking works

### Type Safety ✅
- [ ] `mypy cyberdelta/core/` - 0 errors
- [ ] All Symbol usage properly typed
- [ ] Union type handled correctly
- [ ] No runtime type errors

## 🚨 Week 3 Key Patterns

### Exchange Routing
```python
# Route based on Symbol's exchange
if symbol.exchange == ExchangeName.HYPERLIQUID:
    # Hyperliquid logic
elif symbol.exchange == ExchangeName.BACKPACK:
    # Backpack logic
```

### Symbol-Based Keys
```python
# Create consistent keys from Symbols
def make_key(symbol: Symbol) -> str:
    return f"{symbol.exchange.value}:{symbol.value}"
```

### Equivalence Checking
```python
# Use symbol service for equivalence
service = get_symbol_service()
if service.are_equivalent(symbol1, symbol2):
    # Symbols represent same instrument
```

### Metadata Access
```python
# Type-safe metadata access
if symbol.exchange == ExchangeName.BACKPACK:
    symbol_id = symbol.metadata.symbol_id  # Type-safe
```

## 📊 Week 3 Metrics

- **Business Logic Components**: 100% Symbol usage ✅
- **String Operations**: 0 in business logic ✅
- **Type Safety**: Full coverage ✅
- **Exchange Handling**: Clean routing patterns ✅

**Week 3 integrates Symbol architecture into all business logic - ready for infrastructure!** 🚀