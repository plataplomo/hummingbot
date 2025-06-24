
# Code Review Report: 03 - Strategies

**Report Date:** 2025-04-14
**Reviewer:** Angel (AI Assistant)
**Project:** CyberDeltaEngine
**Version Target:** v0.0.1
**Updated:** 2025-06-24

## UPDATE (2025-06-24): Current Strategy Implementation

### Key Observations:

1. **Strategy Configuration**:
   - Strategy now configured via Pydantic models in config.yaml
   - HyperLiquid Perpetual vs. Backpack Spot strategy is properly configured
   - Configurable parameters: funding_threshold, max_price_spread_pct, min_profit_usd

2. **StrategyManager Addition**:
   - New StrategyManager component handles strategy lifecycle
   - Better integration with execution_handler, portfolio_tracker, and risk_manager
   - Centralized strategy management and coordination

3. **Strategy Parameters**:
   - Strategies receive data_handler, portfolio_tracker, and risk_manager directly
   - Better dependency injection for testing and flexibility
   - Parameters passed as dictionary including exchange mappings

4. **Current Implementation Focus**:
   - Still focused on Perp/Spot arbitrage (no Perp/Perp implementation)
   - Funding rate arbitrage between HyperLiquid (perp) and Backpack (spot)
   - Symbol mapping handled through configuration

### Remaining Gaps:
- Perp/Perp variant not implemented
- Slippage estimation accuracy needs validation
- Basis volatility calculation still uses limited history

## 1. Overview

This section reviews the strategy definition and implementation within the CyberDeltaEngine, focusing on the base strategy class and the specific Funding Rate Arbitrage strategy intended for v0.0.1.

## 2. Base `Strategy` Class (`core/strategy.py`)

*   **Purpose:** Defines an abstract base class (`Strategy`) providing a common structure and interface for all trading strategies within the engine.
*   **Key Features:**
    *   **Abstract `process_data` Method:** Requires subclasses to implement the core logic for receiving `MarketData` and returning an optional `TradeSignal`.
    *   **Initialization:** Takes a unique `name`, the primary `symbol` it operates on, and optional `params`.
    *   **State:** Manages an `enabled` flag (controlled by `enable()`/`disable()` methods) used by the `Engine` for data routing.
    *   **Parameter Access:** Basic `get_param`/`set_param` methods for handling strategy-specific parameters.
    *   **Historical Data:** Includes a simple list (`_historical_data`) to cache recent `MarketData` for the strategy's symbol, managed by `update_historical_data`.
    *   **Lifecycle Hooks:** Provides `on_start`/`on_stop` methods (currently just logging).
*   **Strengths:** Enforces a consistent interface for strategies. Clearly separates strategy logic from engine mechanics. Provides basic state and parameter management.
*   **Weaknesses/Concerns:** The base class is quite minimal. It doesn't provide advanced features like built-in indicator calculations or sophisticated historical data handling, requiring subclasses to implement these if needed. Accessing the `_historical_data` cache within `process_data` relies on direct access to `self._historical_data`.

*   **Code Snippet (Abstract Method):**
    ```python
    # core/strategy.py L41-L52
    @abstractmethod
    def process_data(self, data: MarketData) -> TradeSignal | None: # Changed
        """
        Process new market data and optionally generate a trading signal

        Args:
            data: Market data to process

        Returns:
            Optional TradeSignal if a trade should be executed, None otherwise
        """
        pass
    ```

## 3. `FundingRateArbitrageStrategy` (`strategies/funding_rate_arbitrage.py`)

*   **Implementation Status:** Provides the concrete implementation for the v0.0.1 funding rate arbitrage strategy.
*   **Strategy Variant:** The code and docstrings clearly indicate this implementation focuses on a **Perpetual vs. Spot** arbitrage (specifically targeting `hyperliquid` as `perp_exchange` and `backpack` as `spot_exchange` by default, configurable via params). There is no evidence of a Perp/Perp variant in this file.
*   **Core Logic (`_check_opportunity`):**
    1.  Fetches the current funding rate for the perpetual symbol (`self.symbol`) from the `perp_exchange` via `DataHandler`.
    2.  Fetches current ticker prices for the perpetual symbol and its corresponding spot symbol (using `self.symbol_mapping`) from both exchanges via `DataHandler`.
    3.  Calculates the current basis (Perp Price - Spot Price) and updates an internal historical basis cache.
    4.  Calculates basis volatility (`_calculate_basis_volatility`) using the historical basis data (simple standard deviation).
    5.  Determines the Net Funding Differential (NFD), which for Perp/Spot is simply the perpetual's funding rate.
    6.  Checks if `abs(NFD)` exceeds the `min_funding_differential` parameter.
    7.  Estimates slippage (`_estimate_slippage`) based on order book data (requires `DataHandler` access) and fees. Calculates total estimated costs.
    8.  Calculates expected profit based on NFD, estimated costs, and a placeholder position size (actual sizing is done later by `RiskManager`).
    9.  Checks if `expected_profit` exceeds the `min_profit_threshold`.
    10. Calculates a `utility_score` (Profit - RiskAversion * BasisVolatility^2).
    11. Creates an `ArbitrageOpportunity` object containing market details, NFD, expected profit, utility score, etc. **Note:** This opportunity object does *not* contain the final trade size.
*   **Signal Generation (`process_data`, `_check_and_generate_signal`, `_generate_entry_signal`):**
    *   `process_data` periodically calls `_check_and_generate_signal`.
    *   `_check_and_generate_signal` calls `_check_opportunity`.
    *   If a valid `ArbitrageOpportunity` is returned, `_generate_entry_signal` creates a `TradeSignal` (e.g., `ENTER_ARB_OPPORTUNITY`). This signal contains the *unsized* `ArbitrageOpportunity` and the calculated `utility_score` in its metadata, intended for the `PrioritySignalQueue` and subsequent processing by the `RiskManager`.
*   **Rebalancing (`_should_rebalance`, `_generate_rebalance_signal`):** Includes logic to check if existing positions (obtained via `PortfolioTracker`) deviate significantly from a target (implicitly delta-neutral) and generate rebalancing signals if the `rebalance_threshold` is exceeded.
*   **Dependencies:** Requires `DataHandler`, `PortfolioTracker`, and potentially `RiskManager` (though direct interaction seems limited in favor of generating signals for it). Relies heavily on parameters defined in the main configuration (e.g., fee rates, thresholds) accessed via `self.get_param`.
*   **Strengths:** Implements the core logic for identifying Perp/Spot funding rate opportunities. Considers estimated costs (fees, slippage). Includes basic rebalancing logic. Decouples opportunity identification from final sizing (delegated to `RiskManager`). Uses `Decimal` for financial calculations.
*   **Weaknesses/Concerns:**
    *   **Perp/Perp Variant:** The request mentioned analyzing both Perp/Spot and Perp/Perp, but only Perp/Spot is implemented here.
    *   **Risk Manager Integration:** The flow relies on the `RiskManager` picking up the generated signal later. Robustness depends on that downstream processing.
    *   **Slippage Estimation (`_estimate_slippage`):** The accuracy of this estimation based on potentially thin order book data needs validation. Real-world slippage could differ significantly.
    *   **Basis Volatility (`_calculate_basis_volatility`):** Calculation uses a limited internal history cache. Might not capture true market volatility accurately.
    *   **Utility Score:** The formula is straightforward but potentially simplistic. Its effectiveness in prioritizing the best opportunities depends on the quality of profit/volatility inputs.
    *   **Rebalancing Trigger/Logic:** The exact trigger conditions and execution logic for rebalancing need thorough testing.

*   **Code Snippet (Opportunity Creation):**
    ```python
    # strategies/funding_rate_arbitrage.py L186-L201
    # Create opportunity object
    opportunity = ArbitrageOpportunity(
        symbol=self.symbol,
        long_exchange=self.perp_exchange if perp_side == "LONG" else self.spot_exchange,
        short_exchange=self.spot_exchange if perp_side == "LONG" else self.perp_exchange,
        long_price=perp_ticker.ask if perp_side == "LONG" else spot_ticker.ask,
        short_price=spot_ticker.bid if perp_side == "LONG" else perp_ticker.bid,
        long_funding_rate=Decimal(str(nfd)) if perp_side == "LONG" else Decimal("0"),
        short_funding_rate=Decimal(str(nfd)) if perp_side == "SHORT" else Decimal("0"),
        net_funding_differential=Decimal(str(nfd)),
        timestamp=now,
        expected_profit=Decimal(str(expected_profit)), # Based on placeholder size
        utility_score=float(utility_score),
        basis_volatility=float(basis_volatility),
        optimal_size=None, # Sizing done by RiskManager later
        confidence=None,
    )
    return opportunity # Returns unsized opportunity
    ```

## 4. Overall Assessment

The strategy implementation follows the base class structure well. The `FundingRateArbitrageStrategy` correctly identifies the Perp/Spot variant as the focus for v0.0.1 and implements the core logic for finding potential opportunities based on funding rates and estimated costs. It appropriately delegates final sizing and risk checks to the `RiskManager` by generating signals containing unsized opportunities. Key areas for focus are validating the cost/slippage estimations, ensuring the reliability of data inputs (funding rates, prices), and thoroughly testing the rebalancing logic. The absence of the Perp/Perp variant should be noted.
