# CyberDeltaEngine: Code Review Report (v0.0.1) - Strategies

This section reviews the strategy infrastructure (base class) and the specific implementation of the funding rate arbitrage strategy planned for v0.0.1.

## 1. Base Class (`cyberdelta/core/strategy.py`)

*   **Responsibility:** Define an abstract base class (`Strategy`) providing a common interface and basic lifecycle management for all trading strategies within the engine.
*   **Implementation Details:**
    *   `Strategy(ABC)` includes:
        *   Initialization (`__init__`) taking `config`, `data_handler`, `portfolio_tracker`, and potentially `signal_emitter` (e.g., `SignalQueue` or `Engine`).
        *   Lifecycle methods: `start()` (called by Engine/Orchestrator), `stop()`, `run()` (main async loop for the strategy).
        *   State management: `enable()`, `disable()`, `is_enabled()`.
        *   Abstract methods requiring implementation by subclasses, potentially including:
            *   `_initialize()`: For strategy-specific setup after basic `__init__`.
            *   `_execute_cycle()` or similar: The core logic method called repeatedly by `run()`. This is where the strategy checks for opportunities or processes data.
            *   `_cleanup()`: Called during `stop()`.
    *   Provides basic parameter access (`get_param`, `set_param`).
    *   Might include basic state tracking (`last_execution_time`, `status`).
*   **Observations & Concerns:**
    *   **Data Input Method:** The original design might have assumed a `process_data(data: MarketData)` push model from the `Engine`. However, the current `FundingRateArbitrageStrategy` uses a *pull* model, directly accessing `DataHandler`. The base class needs to accommodate this or be adapted.
    *   **Signal Output Method:** How does a strategy emit a signal or opportunity? Does it call a method on a `signal_emitter` reference passed during `__init__`? This needs to be clearly defined in the base class or its contract.
    *   **Flexibility:** The base class should be flexible enough to support different types of strategies (event-driven, periodic polling, ML-based) and their varied data needs.
*   **Recommendations:**
    *   **Adopt Pull/Emitter Model:** Given the current implementation, adapt the base class to support a pull model where strategies fetch data from `DataHandler` as needed within their `_execute_cycle`. Define a clear mechanism for signal emission (e.g., require a `signal_emitter` dependency and define an `_emit_signal` method or similar).
    *   **Standardize `run` Loop:** Provide a standard `run` implementation in the base class that handles the main loop, calls `_execute_cycle` periodically (based on a configurable interval or event trigger), and manages startup/shutdown via `_initialize` and `_cleanup`.
    *   **Refine Abstract Methods:** Solidify the required abstract methods (`_execute_cycle`, `_initialize`, `_cleanup`) that concrete strategies must implement.

*   **Code Snippet (Conceptual Base Strategy):**
    ```python
    # cyberdelta/core/strategy.py (Conceptual)
    from abc import ABC, abstractmethod
    import asyncio
    import logging

    class Strategy(ABC):
        def __init__(self, name: str, config: Config, data_handler: DataHandler, portfolio_tracker: PortfolioTracker, signal_emitter: Any):
            self.name = name
            self.config = config
            self.data_handler = data_handler
            self.portfolio_tracker = portfolio_tracker
            self.signal_emitter = signal_emitter # e.g., SignalQueue instance
            self.logger = logging.getLogger(f"Strategy.{self.name}")
            self._enabled = False
            self._run_task = None
            self.cycle_interval = float(config.get(f"strategies.{self.name}.cycle_interval_seconds", 5.0))

        def enable(self): self._enabled = True
        def disable(self): self._enabled = False
        def is_enabled(self): return self._enabled

        @abstractmethod
        async def _initialize(self):
            """Strategy-specific initialization logic."""
            pass

        @abstractmethod
        async def _execute_cycle(self):
            """Core logic executed each cycle. Fetch data, check conditions, emit signals."""
            pass

        @abstractmethod
        async def _cleanup(self):
            """Strategy-specific cleanup logic on stop."""
            pass

        async def start(self):
            if not self._run_task:
                self.logger.info(f"Starting strategy...")
                self.enable()
                await self._initialize()
                self._run_task = asyncio.create_task(self.run(), name=f"StrategyRun_{self.name}")
                self.logger.info(f"Strategy started.")
            else:
                self.logger.warning(f"Strategy already running.")

        async def stop(self):
            if self._run_task:
                self.logger.info(f"Stopping strategy...")
                self.disable()
                self._run_task.cancel()
                try:
                    await self._run_task
                except asyncio.CancelledError:
                    self.logger.info("Run task cancelled.")
                await self._cleanup()
                self._run_task = None
                self.logger.info(f"Strategy stopped.")
            else:
                self.logger.warning(f"Strategy not running.")

        async def run(self):
            self.logger.info("Strategy run loop starting.")
            try:
                while self._enabled:
                    start_time = time.monotonic()
                    try:
                        await self._execute_cycle()
                    except Exception as e:
                        self.logger.error(f"Error during strategy cycle: {e}", exc_info=True)

                    # Wait for next cycle
                    elapsed = time.monotonic() - start_time
                    wait_time = max(0, self.cycle_interval - elapsed)
                    await asyncio.sleep(wait_time)
            except asyncio.CancelledError:
                self.logger.info("Strategy run loop cancelled.")
            finally:
                self.logger.info("Strategy run loop finished.")

        # Helper for emitting signals (example)
        async def _emit_signal(self, signal: TradeSignal, priority: float):
            if self.signal_emitter and hasattr(self.signal_emitter, 'add_signal'):
                 await self.signal_emitter.add_signal(signal, priority)
            else:
                 self.logger.error("No valid signal_emitter configured to emit signal.")
    ```

## 2. Funding Rate Arbitrage (`cyberdelta/strategies/funding_rate_arbitrage.py`)

*   **Responsibility:** Implement the delta-neutral funding rate arbitrage strategy between Hyperliquid (Perp) and Backpack (Spot/Perp) for v0.0.1.
*   **Implementation Details:**
    *   Inherits from `Strategy` base class.
    *   **Data Fetching:** Implements `_execute_cycle` to periodically fetch required data (funding rates, tickers/prices) for target symbols (`BTC-PERP`, etc.) directly from the `DataHandler` for both Hyperliquid and Backpack.
    *   **Opportunity Identification (`_check_opportunity`)**: Compares funding rates and prices. Calculates the Net Funding Differential (NFD) and basis (price spread).
    *   **Profit & Cost Estimation**: Estimates potential profit based on NFD. Calculates estimated costs including trading fees (fetched from config/exchange info) and slippage (using `_estimate_slippage` based on order book depth requested from `DataHandler`).
    *   **Utility Score**: Calculates a utility score for the opportunity, potentially factoring in profit magnitude, NFD stability (using `_calculate_basis_volatility`), and confidence.
    *   **Threshold Checks**: Compares estimated net profit and NFD against configurable thresholds (`min_profit_threshold_usd`, `min_funding_differential_pct`).
    *   **Signal Generation**: If thresholds are met, creates a `TradeSignal` object (or potentially `ArbitrageOpportunity` first, then converting). Populates signal metadata with details like calculated profit, utility score, confidence, involved exchanges, etc.
    *   **Signal Emission**: Calls `self._emit_signal` (inherited or implemented) to send the generated `TradeSignal` to the `SignalQueue` along with its calculated priority (utility score).
    *   **Configuration**: Reads parameters from `config.yaml` under `strategies.funding_rate_arbitrage` (e.g., target symbols, thresholds, exchange names).
*   **Code Snippet (Conceptual `_execute_cycle` and `_check_opportunity`):**
    ```python
    # cyberdelta/strategies/funding_rate_arbitrage.py (Conceptual)
    from cyberdelta.core.strategy import Strategy
    from cyberdelta.core.models import ArbitrageOpportunity, TradeSignal, SignalType, OrderSide
    from decimal import Decimal

    class FundingRateArbitrageStrategy(Strategy):
        async def _initialize(self):
            self.logger.info("Initializing FundingRateArbitrageStrategy...")
            # Load specific params from config
            self.target_symbol = self.config.get(f"strategies.{self.name}.target_symbol", "BTC-PERP")
            self.perp_exchange = self.config.get(f"strategies.{self.name}.perp_exchange", "hyperliquid")
            self.other_exchange = self.config.get(f"strategies.{self.name}.other_exchange", "backpack") # Spot or Perp
            self.min_nfd_pct = Decimal(self.config.get(f"strategies.{self.name}.min_nfd_pct", "0.0001")) # 0.01%
            self.min_profit_usd = Decimal(self.config.get(f"strategies.{self.name}.min_profit_usd", "1.0"))
            # ... load fee rates, slippage calculation params ...
            self.symbol_mapping = { # Example, move to config or SymbolMapper
                self.perp_exchange: self.target_symbol,
                self.other_exchange: self.target_symbol # Assuming same symbol name for simplicity
            }
            self.last_opportunity_ts = {}

        async def _execute_cycle(self):
            self.logger.debug("Executing funding rate check cycle...")
            opportunity = await self._check_opportunity(self.target_symbol)
            if opportunity:
                self.logger.info(f"Found opportunity: {opportunity.long_exchange} vs {opportunity.short_exchange} for {opportunity.symbol}. NFD: {opportunity.net_funding_differential:.6f}%")
                # Avoid spamming signals for the same opportunity too quickly
                opportunity_key = f"{opportunity.long_exchange}:{opportunity.short_exchange}"
                now = time.time()
                if now - self.last_opportunity_ts.get(opportunity_key, 0) > 30: # Cooldown period
                    trade_signal = self._create_signal_from_opportunity(opportunity)
                    if trade_signal:
                        priority = opportunity.utility_score # Utility score determines priority
                        await self._emit_signal(trade_signal, priority)
                        self.last_opportunity_ts[opportunity_key] = now
                else:
                     self.logger.debug(f"Opportunity {opportunity_key} within cooldown period.")

        async def _check_opportunity(self, symbol: str) -> ArbitrageOpportunity | None:
            # 1. Fetch Data (Using DataHandler)
            perp_sym = self.symbol_mapping[self.perp_exchange]
            other_sym = self.symbol_mapping[self.other_exchange]

            perp_funding = self.data_handler.get_funding_rate(self.perp_exchange, perp_sym)
            other_funding = self.data_handler.get_funding_rate(self.other_exchange, other_sym)
            perp_ticker = self.data_handler.get_ticker(self.perp_exchange, perp_sym)
            other_ticker = self.data_handler.get_ticker(self.other_exchange, other_sym)

            # Basic data validation
            if not all([perp_funding, other_funding, perp_ticker, other_ticker, perp_ticker.price, other_ticker.price]):
                self.logger.debug("Missing data for opportunity check.")
                return None
            # Check data freshness?

            # Ensure Decimal for rates (assuming DataHandler provides them correctly)
            perp_rate = perp_funding.funding_rate
            other_rate = other_funding.funding_rate
            if perp_rate is None or other_rate is None:
                 self.logger.debug("Missing funding rate values.")
                 return None

            # 2. Calculate NFD & Identify Direction
            # NFD = Rate on Long Leg - Rate on Short Leg
            # If Perp Rate > Other Rate, consider Long Other / Short Perp
            # If Other Rate > Perp Rate, consider Long Perp / Short Other
            nfd = perp_rate - other_rate
            if abs(nfd) < self.min_nfd_pct:
                self.logger.debug(f"NFD {nfd:.6f}% below threshold {self.min_nfd_pct:.6f}%")
                return None

            if nfd > 0: # Perp rate higher -> Long Other, Short Perp
                 long_exchange = self.other_exchange
                 short_exchange = self.perp_exchange
                 long_price = other_ticker.ask # Buy at Ask
                 short_price = perp_ticker.bid # Sell at Bid
                 actual_nfd = other_rate - perp_rate # NFD from perspective of Long leg
            else: # Other rate higher -> Long Perp, Short Other
                 long_exchange = self.perp_exchange
                 short_exchange = self.other_exchange
                 long_price = perp_ticker.ask # Buy at Ask
                 short_price = other_ticker.bid # Sell at Bid
                 actual_nfd = perp_rate - other_rate # NFD from perspective of Long leg

            # Price check (ensure prices are valid Decimals)
            if long_price is None or short_price is None or long_price <= 0 or short_price <= 0:
                 self.logger.debug("Invalid prices for opportunity check.")
                 return None

            # 3. Estimate Costs (Fees, Slippage)
            # Needs access to order book for slippage estimation
            # long_book = self.data_handler.get_orderbook(long_exchange, self.symbol_mapping[long_exchange])
            # short_book = self.data_handler.get_orderbook(short_exchange, self.symbol_mapping[short_exchange])
            # estimated_entry_slippage = self._estimate_slippage(long_book, OrderSide.BUY, TradeSignal.quantity) # Need hypothetical quantity?
            # estimated_exit_slippage = self._estimate_slippage(short_book, OrderSide.SELL, TradeSignal.quantity)
            # entry_cost_pct = fee_long + fee_short + estimated_entry_slippage + estimated_exit_slippage
            entry_cost_pct = Decimal("0.001") # Placeholder: 0.1% total cost (fees+slippage)

            # 4. Calculate Profit & Utility
            # Expected profit = NFD - Costs (annualized? or per period?)
            # For simplicity: Check if NFD > entry_cost_pct (crude check)
            if abs(actual_nfd) <= entry_cost_pct:
                 self.logger.debug(f"Estimated NFD {actual_nfd:.6f}% does not exceed costs {entry_cost_pct:.6f}%")
                 return None

            # Utility score - higher profit, lower volatility = better
            basis_volatility = self._calculate_basis_volatility(symbol) # Needs implementation
            utility = (abs(actual_nfd) - entry_cost_pct) / (basis_volatility**2 if basis_volatility > 0 else Decimal("1e-6"))

            # 5. Create Opportunity Object
            opportunity = ArbitrageOpportunity(
                 symbol=symbol,
                 long_exchange=long_exchange,
                 short_exchange=short_exchange,
                 long_price=long_price,
                 short_price=short_price,
                 long_funding_rate=perp_rate if long_exchange == self.perp_exchange else other_rate,
                 short_funding_rate=perp_rate if short_exchange == self.perp_exchange else other_rate,
                 net_funding_differential=actual_nfd, # NFD from Long's perspective
                 timestamp=datetime.now(UTC),
                 # optimal_size=... # Size calculation belongs to RiskManager
                 expected_profit = (abs(actual_nfd) - entry_cost_pct), # Simplified profit metric
                 confidence = 1.0 - basis_volatility, # Example confidence
                 basis_volatility=basis_volatility,
                 utility_score=float(utility), # Convert utility to float for queue
                 metadata={ ... } # Add context
            )
            return opportunity

        def _create_signal_from_opportunity(self, opportunity: ArbitrageOpportunity) -> TradeSignal | None:
             # This signal is INTENT - sizing happens in RiskManager
             # Quantity here might be placeholder (e.g., 1) or omitted
             signal = TradeSignal(
                 symbol=opportunity.symbol,
                 signal_type=SignalType.ENTER_LONG, # This needs adjustment - it's a pair trade
                 side=OrderSide.BUY, # Ambiguous for arbitrage signal - needs better representation
                 # Price/Quantity omitted - let RiskManager determine based on opportunity
                 timestamp=opportunity.timestamp,
                 confidence=opportunity.confidence,
                 source_strategy=self.name,
                 metadata={
                     'arbitrage_opportunity': opportunity.to_dict(), # Embed opportunity details
                     'utility_score': opportunity.utility_score,
                     'long_exchange': opportunity.long_exchange,
                     'short_exchange': opportunity.short_exchange,
                 }
             )
             # TODO: How to represent the two legs (long and short) in one signal or two?
             # Option 1: Emit one signal, ExecutionHandler interprets metadata.
             # Option 2: Emit two signals (ENTER_LONG on one exch, ENTER_SHORT on other).
             # Option 1 seems more manageable if ExecutionHandler handles pairs.
             self.logger.info(f"Creating signal for opportunity {opportunity.long_exchange} vs {opportunity.short_exchange}")
             return signal # Return ONE signal representing the pair trade intent

        def _estimate_slippage(self, orderbook, side, quantity) -> Decimal:
            # Needs implementation using order book data
            return Decimal("0.0002") # Placeholder 0.02%

        def _calculate_basis_volatility(self, symbol) -> Decimal:
             # Needs implementation using historical price data from DataHandler
             return Decimal("0.0005") # Placeholder 0.05%

        async def _cleanup(self):
            self.logger.info("Cleaning up FundingRateArbitrageStrategy...")
            # No specific cleanup needed currently
            pass
    ```

*   **Configuration Example (`config.yaml`):**
    ```yaml
    strategies:
      funding_rate_arbitrage:
        enabled: true
        cycle_interval_seconds: 5.0 # How often to check for opportunities
        target_symbol: "BTC-PERP" # Internal canonical symbol
        perp_exchange: "hyperliquid"
        other_exchange: "backpack" # Can be spot or perp, check API
        symbol_mapping: # Optional: If symbols differ greatly
          hyperliquid: "BTC-PERP"
          backpack: "SOL-USD" # Example difference
        min_funding_differential_pct: 0.0001 # Minimum abs(RateA - RateB) to consider (0.01%)
        min_profit_threshold_usd: 0.50 # Minimum estimated USD profit after costs (per unit? total? Clarify)
        # Cost calculation parameters
        fee_rate_perp: 0.0005 # 0.05%
        fee_rate_other: 0.0010 # 0.10%
        slippage_calculation_depth_usd: 1000 # Estimate slippage based on this USD depth in order book
        # Cooldown period in seconds before generating signal for same pair again
        opportunity_cooldown_seconds: 30
    ```

*   **Observations & Strengths:**
    *   Implements the core logic for identifying NFD opportunities.
    *   Parameterizes key thresholds and settings.
    *   Correctly uses `DataHandler` for data retrieval (pull model).
*   **Concerns & Areas for Improvement:**
    *   **Signal Representation:** How to represent a two-legged arbitrage trade (long one, short other) as a `TradeSignal`? The current example uses a single signal with metadata, requiring interpretation by downstream components (RiskManager/ExecutionHandler). This needs a clear convention.
    *   **Decimal Usage:** CRITICAL - All financial calculations (NFD, costs, profit, utility if possible) must use `Decimal` rigorously. Ensure `_check_opportunity` and helpers adhere to `decimal.mdc`.
    *   **Slippage/Volatility Calculation:** `_estimate_slippage` and `_calculate_basis_volatility` are placeholders and need proper implementation using `DataHandler` (order books, historical prices).
    *   **Data Freshness/Staleness:** Check data timestamps from `DataHandler` before calculating to avoid acting on stale information.
    *   **Signal Emission Flow:** Confirms strategy emits signal to `SignalQueue` (via `_emit_signal`). Ensures utility score is passed as priority.
    *   **Backpack Funding Rate:** Still dependent on the critical assumption that `DataHandler` can provide a timely funding rate for Backpack.
*   **Recommendations:**
    *   **Define Arbitrage Signal Standard:** Decide and document how a two-legged arbitrage trade is represented:
        *   **Option 1 (Preferred):** Single `TradeSignal` with `signal_type=ARBITRAGE` (add to enum), `side=NONE` (or similar), and detailed `metadata` including `long_exchange`, `short_exchange`, `symbol`, target NFD, etc. `RiskManager` and `ExecutionHandler` interpret this single signal to create the two required orders.
        *   **Option 2:** Emit two separate `TradeSignal` objects (one `ENTER_LONG`, one `ENTER_SHORT`) with linked IDs in metadata. More complex to manage downstream.
    *   **Implement Decimal Calculations:** Rigorously implement all financial math using `Decimal` in `_check_opportunity`, `_estimate_slippage`, etc.
    *   **Implement Helpers:** Develop robust implementations for `_estimate_slippage` (using order book data) and `_calculate_basis_volatility` (using historical price data).
    *   **Add Data Freshness Checks:** Before calculating NFD, check `get_last_update_time` from `DataHandler` for the relevant tickers/rates and skip calculation if data is too old (configure tolerance).
    *   **Unit Test Logic:** Create unit tests for `_check_opportunity` covering various data scenarios (valid opportunity, no opportunity, missing data, stale data).
    *   **Verify Backpack Funding:** Reiterate the need to confirm the Backpack funding rate source.
