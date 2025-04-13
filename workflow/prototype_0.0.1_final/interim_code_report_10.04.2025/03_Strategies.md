# Code Report: CyberDeltaEngine - Strategies

## 1. Overview

Strategies encapsulate the specific logic for identifying trading opportunities based on market data and portfolio state. The engine can manage multiple strategies concurrently.

## 2. Base Strategy (`cyberdelta/core/strategy.py`)

**Purpose**: Defines the abstract base class `Strategy` that all specific strategy implementations must inherit from.

**Key Responsibilities**:
- Defining the core interface for a strategy (`process_data`, `on_start`, `on_stop`).
- Managing strategy parameters and state (enabled/disabled).
- Providing helper methods for accessing parameters (`get_param`).
- Basic tracking of signals generated and last signal time.

**Code Snippet (`Strategy` abstract methods)**:
```python
from abc import ABC, abstractmethod
# ... other imports ...

class Strategy(ABC):
    # ... __init__ and other methods ...

    @abstractmethod
    def process_data(self, data: MarketData) -> Optional[TradeSignal]:
        """
        Process new market data and potentially generate a trading signal.
        This is the core logic method for the strategy.

        Args:
            data: MarketData object containing market information.

        Returns:
            TradeSignal if a trading action should be taken, None otherwise.
        """
        pass

    def on_start(self) -> None:
        """Called when the strategy is started or enabled."""
        # Default implementation does nothing, subclasses can override
        pass

    def on_stop(self) -> None:
        """Called when the strategy is stopped or disabled."""
        # Default implementation does nothing, subclasses can override
        pass

    def update_historical_data(self, data: MarketData):
        """
        Allows the strategy to maintain its own historical view of relevant data.
        Called by the engine or data handler before process_data.
        """
        # Default implementation does nothing, subclasses can override
        pass
```

## 3. Funding Rate Arbitrage Strategy (`cyberdelta/strategies/funding_rate_arbitrage.py`)

**Purpose**: Implements the core funding rate arbitrage logic between two markets (Perp vs Spot, or Perp vs Perp).

**Key Responsibilities**:
- Identifying funding rate arbitrage opportunities by comparing funding rates and prices between exchanges (via `DataHandler`).
- Calculating the Net Funding Differential (NFD).
- Estimating expected profit considering potential trading costs (fees, slippage).
- Calculating basis (price difference) and its volatility.
- Determining optimal entry/exit points based on NFD, profit thresholds, and basis conditions.
- Generating `TradeSignal` objects for entering, exiting, or rebalancing positions.
- Interacting with `RiskManager` (if provided) to size positions.
- Maintaining historical basis data.
- Handling different configurations (Perp vs Spot, Perp vs Perp) via parameters.

**Code Snippet (`FundingRateArbitrageStrategy._check_opportunity`)**:
```python
    async def _check_opportunity(self) -> Optional[ArbitrageOpportunity]:
        """
        Check for funding rate arbitrage opportunity between specified markets.
        Considers funding rates, prices, basis, volatility, and estimated costs.

        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        now = datetime.now()

        # Determine perp and spot symbols based on configuration
        perp_symbol = self.symbol
        spot_symbol = self.symbol_mapping.get(self.symbol)
        perp_exchange = self.get_param('perp_exchange', 'hyperliquid')
        spot_exchange = self.get_param('spot_exchange', 'backpack') # Or perp_exchange2

        # 1. Get Required Data (Funding Rate, Tickers)
        funding_rate_data = await self.data_handler.get_funding_rate(perp_exchange, perp_symbol)
        perp_ticker = await self.data_handler.get_ticker(perp_exchange, perp_symbol)
        spot_ticker = await self.data_handler.get_ticker(spot_exchange, spot_symbol)

        if not funding_rate_data or not perp_ticker or not spot_ticker:
            logger.warning(f"[{self.name}] Missing data for opportunity check ({perp_symbol}/{spot_symbol})")
            return None

        funding_rate = funding_rate_data[0] # Assuming tuple (rate, timestamp)

        # 2. Calculate Basis and Volatility
        basis = perp_ticker.price - spot_ticker.price
        self._update_historical_basis(now, basis)
        basis_volatility = self._calculate_basis_volatility(self.symbol)

        # 3. Calculate Net Funding Differential (NFD)
        # Simplified for Perp-Spot; needs adjustment for Perp-Perp
        nfd = funding_rate

        # 4. Check Entry Conditions (Min NFD, Max Basis/Volatility - depending on config)
        if abs(nfd) < self.min_funding_differential:
            logger.debug(f"[{self.name}] NFD {nfd:.6f} below threshold {self.min_funding_differential:.6f}")
            return None
        # ... Add checks for max_basis_spread, min_spread etc. based on params ...

        # 5. Estimate Costs and Expected Profit
        # Use a placeholder size initially, RiskManager will refine it
        estimated_trade_size_usd = 1000.0
        total_costs = self._estimate_trade_costs(perp_symbol, spot_symbol, estimated_trade_size_usd)
        expected_profit = abs(estimated_trade_size_usd * nfd / 100) - total_costs # Simplified

        if expected_profit < self.min_profit_threshold:
            logger.debug(f"[{self.name}] Expected profit ${expected_profit:.2f} below threshold ${self.min_profit_threshold:.2f}")
            return None

        # 6. Calculate Utility Score (Example)
        utility_score = expected_profit - (self.risk_aversion * (basis_volatility ** 2))

        # 7. Create Opportunity Object
        # Determine long/short exchanges based on NFD sign
        long_exchange, short_exchange = (perp_exchange, spot_exchange) if nfd < 0 else (spot_exchange, perp_exchange)

        opportunity = ArbitrageOpportunity(
            symbol=self.symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            net_funding_differential=nfd,
            timestamp=now,
            expected_profit=expected_profit,
            utility_score=utility_score,
            basis_volatility=basis_volatility,
            # Add other relevant details: prices, individual funding rates etc.
        )

        logger.info(f"[{self.name}] Found opportunity: {opportunity}")
        return opportunity
```

## 4. Strategy Manager (`cyberdelta/core/strategy_manager.py`)

**Purpose**: Manages the lifecycle and execution of multiple strategies within the engine.

**Key Responsibilities**:
- Registering, enabling, disabling, and retrieving strategies.
- Potentially routing market data to appropriate strategies (although currently handled by the `Engine`).
- Providing an interface for querying strategy status and performance.

*Note: In the current architecture, the `Engine` class seems to handle most of the strategy management tasks. The `StrategyManager` might be underutilized or intended for future refactoring.* 