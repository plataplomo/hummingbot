# CyberDeltaEngine: Code Review Report (v0.0.1) - Safety Systems

This section reviews the crucial components designed to enhance the operational safety and reliability of the CyberDeltaEngine: Circuit Breakers, Position Reconciliation, and Funding Rate Validation.

## 1. Circuit Breaker System (`cyberdelta/validation/circuit_breaker.py`)

*   **Responsibility:** Act as a critical safety net by monitoring various system health indicators (API errors, WebSocket disconnects, potentially portfolio drawdown or extreme volatility) and automatically halting trading operations (globally, per-exchange, or per-symbol) when pre-defined thresholds are breached. Prevents uncontrolled losses or cascading failures during adverse conditions.
*   **Implementation Details:**
    *   **`BreakerState(Enum)`:** Defines states: `CLOSED` (operational), `OPEN` (tripped, operations blocked), `HALF_OPEN` (testing recovery).
    *   **`CircuitBreaker(ABC)`:** Base class for individual breaker types. Defines interface (`trip`, `reset`, `allow_operation`, `check`, `_check_recovery`).
    *   **Specific Breaker Types:** Subclasses implement logic for specific triggers:
        *   `APIErrorBreaker`: Monitors API error rates.
        *   `WebSocketDisconnectBreaker`: Monitors WebSocket connection stability.
        *   *(Potential future breakers: `DrawdownBreaker`, `VolatilityBreaker`, `LiquidityBreaker`)*.
    *   **`CircuitBreakerSystem` (Manager):**
        *   Loads breaker configurations from `config.yaml` (thresholds, cooldowns, scope).
        *   Instantiates and manages a registry of active breakers.
        *   Provides the central query point: `can_execute(exchange: str, symbol: str | None = None) -> tuple[bool, str]`. This checks all relevant breakers (symbol-specific -> exchange-specific -> global) and returns `(False, reason)` if any are tripped, otherwise `(True, "OK")`.
        *   Provides methods for components to report events: `record_api_error`, `record_api_success`, `record_websocket_disconnect`, `record_websocket_connect`. (Methods like `update_portfolio_value` would be needed for drawdown breakers).
    *   **`CircuitBreakerTrippedError(Exception)`:** Custom exception potentially raised on attempts to execute when tripped.

*   **Integration Points:**
    *   **`ExecutionHandler`:** MUST call `can_execute` before attempting any order placement. MUST call `record_api_error`/`record_api_success` after attempts.
    *   **`SignalQueue`:** Calls internal `_check_circuit_breakers` (which uses `can_execute`) before adding signals and before returning signals.
    *   **`DataHandler` (or `API Clients`):** MUST call `record_websocket_disconnect`/`record_websocket_connect` to feed the relevant breaker.

*   **Code Snippet (Conceptual `CircuitBreakerSystem.can_execute`):**
    ```python
    # cyberdelta/validation/circuit_breaker.py (Conceptual)
    class CircuitBreakerSystem:
        def __init__(self, config):
            self.breakers: dict[str, CircuitBreaker] = {} # Key: e.g., "global", "exchange:hyperliquid", "symbol:hyperliquid:BTC-PERP"
            self._lock = asyncio.Lock()
            # ... Load config and instantiate breakers ...

        async def can_execute(self, exchange: str, symbol: str | None = None) -> tuple[bool, str]:
            async with self._lock:
                now = time.monotonic()
                scopes_to_check = ["global"]
                if exchange: scopes_to_check.append(f"exchange:{exchange}")
                if exchange and symbol: scopes_to_check.append(f"symbol:{exchange}:{symbol}")

                for scope in scopes_to_check:
                    breaker = self.breakers.get(scope)
                    if breaker:
                        allowed, reason = await breaker.allow_operation(now)
                        if not allowed:
                            self.logger.warning(f"Execution blocked by {scope} circuit breaker: {reason}")
                            return False, f"{scope.upper()} Breaker Tripped: {reason}"
                return True, "OK"

        async def record_api_error(self, exchange: str, symbol: str | None = None):
             # Find relevant APIErrorBreakers and call their record_error method
             async with self._lock:
                 # ... logic to find and update relevant breakers ...
                 pass

        # ... other record_* methods ...

    # --- Integration Example --- 
    # cyberdelta/core/execution_handler.py (Conceptual)
    # async def execute_signal(self, signal: TradeSignal):
    #     # ... get exchange, symbol ...
    #     allowed, reason = await self.circuit_breakers.can_execute(exchange, symbol)
    #     if not allowed:
    #         self.logger.error(f"Execution aborted: {reason}")
    #         return
    #     try:
    #         result = await api_client.create_order(...)
    #         await self.circuit_breakers.record_api_success(exchange, symbol)
    #     except APIError as e:
    #         await self.circuit_breakers.record_api_error(exchange, symbol)
    #         self.logger.error(f"API Error during execution: {e}")
    #         # ... potentially re-raise or handle ...

    ```

*   **Configuration Example (`config.yaml`):**
    ```yaml
    validation:
      circuit_breaker:
        enabled: true
        # Breaker definitions (key determines scope)
        breakers:
          # Global Breaker based on overall API error rate
          global_api_errors:
            type: "APIErrorBreaker"
            scope: "global"
            threshold_errors: 15
            threshold_interval_seconds: 60
            cooldown_seconds: 300 # 5 minutes
            recovery_threshold_successes: 3 # Require 3 successes in cooldown to reset

          # Per-exchange WS disconnect breaker
          exchange_ws_disconnects:
            type: "WebSocketDisconnectBreaker"
            scope: "exchange" # Applies per exchange if not specified below
            threshold_disconnects: 5
            threshold_interval_seconds: 120
            cooldown_seconds: 600
            recovery_threshold_successes: 1 # 1 reconnect allows recovery

          # Override for a specific exchange
          backpack_api_errors:
            type: "APIErrorBreaker"
            scope: "exchange:backpack"
            threshold_errors: 8 # More lenient for Backpack?
            threshold_interval_seconds: 60
            cooldown_seconds: 300
            recovery_threshold_successes: 2

          # Breaker for a specific symbol (less common)
          # btc_perp_hl_errors:
          #   type: "APIErrorBreaker"
          #   scope: "symbol:hyperliquid:BTC-PERP"
          #   threshold_errors: 3
          #   threshold_interval_seconds: 30
          #   cooldown_seconds: 180
          #   recovery_threshold_successes: 1
    ```

*   **Observations & Strengths:**
    *   Well-structured with a clear separation of concerns (manager vs. specific breakers).
    *   Configurable thresholds, cooldowns, and scope provide flexibility.
    *   Implements the standard circuit breaker state machine.
*   **Concerns & Areas for Improvement:**
    *   **Metric Recording Integration:** How metrics for non-event-based breakers (Drawdown, Volatility) would be fed into the system requires defining clear responsibilities (e.g., `PortfolioTracker` calls `update_portfolio_value`, `DataHandler` calls `update_price`).
    *   **Recovery Logic (`_check_recovery`):** The logic determining transition from `HALF_OPEN` back to `CLOSED` needs careful tuning. Resetting too easily (e.g., after one success) can lead to flapping. Consider requiring a sustained period of success.
    *   **Scope Hierarchy in `can_execute`:** Ensure the check correctly evaluates breakers in the order: specific symbol -> specific exchange -> global, stopping at the first tripped breaker.
    *   **Testing:** Requires extensive testing of state transitions, cooldowns, recovery logic, and the `can_execute` scope checks.
*   **Recommendations:**
    *   **Solidify Event Recording:** Ensure *all* relevant events (API success/error, WS connect/disconnect) are reliably reported to the `CircuitBreakerSystem` from the correct components (`ExecutionHandler`, `DataHandler`/`API Clients`).
    *   **Tune Recovery Logic:** Make the recovery criteria configurable (e.g., number of successes required, time window for success). Default to requiring more than one success for API error recovery.
    *   **Test Scope Logic:** Implement specific tests verifying that `can_execute` correctly checks symbol, exchange, and global breakers in the right order.
    *   **Comprehensive Testing:** Develop unit tests for individual breakers and integration tests for the `CircuitBreakerSystem` interacting with mock components reporting events.

## 2. Position Reconciliation System (`cyberdelta/validation/position_reconciliation.py`)

*   **Responsibility:** Periodically verify the internal portfolio state (`PortfolioTracker`) against the actual state reported by the exchanges (`API Clients`) to detect discrepancies caused by missed messages, bugs, or exchange issues.
*   **Implementation Details:**
    *   **`PositionReconciliationSystem`:**
        *   Takes `PortfolioTracker`, `API Clients`, and `config`.
        *   Configurable `check_interval_seconds` and `discrepancy_threshold_pct`.
        *   `run()` method provides the periodic check loop.
        *   `check_exchange(exchange_name)` performs the core logic:
            *   Fetches local balances/positions from `PortfolioTracker` (using its lock).
            *   Fetches exchange balances/positions from the relevant `API Client`.
            *   Compares assets and symbols one by one in `_reconcile_balances` and `_reconcile_positions`.
            *   Calculates discrepancies, considering the configured percentage threshold and potentially a minimum absolute amount.
            *   Logs discrepancies clearly (`_record_discrepancy`).
            *   **Crucially, avoids auto-correction.** Relies on logging/alerting for operator intervention.

*   **Integration Points:**
    *   Instantiated in `main.py`.
    *   Its `run()` method should be started as a background task by the `Engine` or application orchestrator.

*   **Code Snippet (Conceptual `check_exchange` and Comparison):**
    ```python
    # cyberdelta/validation/position_reconciliation.py (Conceptual)
    class PositionReconciliationSystem:
        def __init__(self, config, portfolio_tracker, api_clients):
            self.config = config
            self.portfolio_tracker = portfolio_tracker
            self.api_clients = api_clients
            self.interval = config.get("validation.position_reconciliation.check_interval_seconds", 300)
            self.threshold_pct = Decimal(config.get("validation.position_reconciliation.discrepancy_threshold_pct", "0.01")) # 1%
            self.min_abs_threshold_usd = Decimal(config.get("validation.position_reconciliation.min_abs_threshold_usd", "0.01")) # Min USD value diff
            self.logger = logging.getLogger("PositionReconciliation")
            self._run_task = None

        async def run(self):
            self.logger.info(f"Starting position reconciliation checks every {self.interval}s.")
            while True:
                try:
                    await asyncio.sleep(self.interval)
                    self.logger.info("Running periodic position reconciliation check...")
                    for exchange_name in self.api_clients.keys():
                         await self.check_exchange(exchange_name)
                except asyncio.CancelledError:
                    self.logger.info("Position reconciliation task cancelled.")
                    break
                except Exception as e:
                     self.logger.error(f"Error during reconciliation check: {e}", exc_info=True)
                     # Avoid task crashing loop, wait before next attempt
                     await asyncio.sleep(self.interval / 2)

        async def check_exchange(self, exchange_name: str):
            self.logger.debug(f"Reconciling positions for exchange: {exchange_name}")
            api_client = self.api_clients.get(exchange_name)
            if not api_client: return

            try:
                # Get local state (using PortfolioTracker's lock implicitly via its methods)
                local_balances = await self.portfolio_tracker.get_all_balances(exchange_name)
                local_positions = await self.portfolio_tracker.get_all_positions(exchange_name)

                # Get exchange state
                exchange_balances_list = await api_client.get_balances()
                exchange_positions_list = await api_client.get_positions()
                exchange_balances = {b.asset: b for b in exchange_balances_list}
                exchange_positions = {p.symbol: p for p in exchange_positions_list}

                # Reconcile
                balance_discrepancies = self._reconcile_balances(local_balances, exchange_balances)
                position_discrepancies = self._reconcile_positions(local_positions, exchange_positions)

                for discrepancy in balance_discrepancies + position_discrepancies:
                    self._record_discrepancy(exchange_name, discrepancy)

            except APIError as e:
                 self.logger.error(f"API Error during reconciliation for {exchange_name}: {e}")
            except Exception as e:
                 self.logger.error(f"Unexpected error during reconciliation for {exchange_name}: {e}", exc_info=True)

        def _reconcile_balances(self, local_b: dict, exch_b: dict) -> list[dict]:
            discrepancies = []
            all_assets = set(local_b.keys()) | set(exch_b.keys())
            for asset in all_assets:
                l_bal = local_b.get(asset)
                e_bal = exch_b.get(asset)
                l_total = l_bal.total if l_bal else Decimal(0)
                e_total = e_bal.total if e_bal else Decimal(0)

                diff = abs(l_total - e_total)
                if diff > Decimal("1e-18"): # Avoid floating point dust issues
                    # Need price for percentage check based on value?
                    # Simple check on quantity difference for now
                    # Add check against self.threshold_pct and self.min_abs_threshold_usd
                    is_significant = diff > self.min_abs_threshold_usd # Simplified check
                    if is_significant:
                        discrepancies.append({
                            "type": "balance", "asset": asset,
                            "local": str(l_total), "exchange": str(e_total), "diff": str(diff)
                        })
            return discrepancies

        def _reconcile_positions(self, local_p: dict, exch_p: dict) -> list[dict]:
             # Similar logic for positions, comparing sizes
             # Calculate threshold amount based on position value if needed
             # ... implementation ...
             return []

        def _record_discrepancy(self, exchange: str, discrepancy: dict):
            # CRITICAL: Log prominently, potentially trigger external alerts (e.g., Sentry, PagerDuty)
            self.logger.error(f"POSITION RECONCILIATION FAILURE - Exchange: {exchange}, Details: {discrepancy}")
            # Consider triggering a specific circuit breaker or halting strategy
            # self.circuit_breaker_system.trip("reconciliation_failure", exchange=exchange) # Needs integration

    ```
*   **Configuration Example (`config.yaml`):**
    ```yaml
    validation:
      # ... circuit_breaker ...
      position_reconciliation:
        enabled: true
        check_interval_seconds: 300 # Check every 5 minutes
        # Alert if discrepancy exceeds EITHER % threshold OR min absolute value
        discrepancy_threshold_pct: "0.01" # 1% difference relative to larger position/balance size
        min_abs_threshold_usd: "0.01" # Alert on any diff > $0.01 USD (requires price lookup)
        # Auto-correction - STRONGLY DISCOURAGED
        auto_correct_local_state: false
    ```

*   **Observations & Strengths:**
    *   Provides an essential safety check against state drift.
    *   Compares local vs. exchange state directly.
    *   Avoids dangerous auto-correction by default.
*   **Concerns & Areas for Improvement:**
    *   **Threshold Calculation Detail:** The percentage threshold calculation needs care: `diff > max(abs(local_val), abs(exch_val)) * threshold_pct`. Also needs integration with price data (`DataHandler`) to use `min_abs_threshold_usd` effectively.
    *   **Alerting Mechanism:** Simple logging might be missed. Needs integration with a proper alerting system (e.g., Sentry, PagerDuty via webhooks, email) for operational use.
    *   **Impact of Discrepancy:** What happens when a discrepancy is found? Should it automatically trigger a circuit breaker for the affected exchange/symbol? This requires integration.
*   **Recommendations:**
    *   **Implement Detailed Threshold Check:** Refine `_reconcile_balances` and `_reconcile_positions` to correctly calculate the percentage threshold based on the larger value and implement the `min_abs_threshold_usd` check (requiring price data lookup).
    *   **Integrate Alerting:** Add calls to a dedicated alerting service/module when `_record_discrepancy` is called.
    *   **Integrate Circuit Breaker Trigger (Optional but Recommended):** Consider adding logic in `_record_discrepancy` to trip a specific circuit breaker (e.g., `ReconciliationBreaker`) for the affected exchange, halting further trades until manually reviewed.
    *   **Ensure Periodic Execution:** Confirm the `PositionReconciliationSystem.run()` task is started correctly by the main application orchestrator.

## 3. Funding Rate Validator (`cyberdelta/validation/funding_rate_validator.py`)

*   **Responsibility:** (Primarily informational currently) Track predicted/expected funding rates versus actual funding payments received/paid to monitor the accuracy of the core assumption driving the arbitrage strategy.
*   **Implementation Details:**
    *   Stores predictions (`record_prediction`) and actual payments (`record_payment`) in memory (lists or dicts).
    *   `calculate_metrics`: Compares predictions and payments for a given symbol/exchange over a time window, calculating RMSE, MAE, bias.
    *   `clear_old_data`: Method to prune stored data.
*   **Integration Points:**
    *   `Strategy` should call `record_prediction` when making a prediction or using a rate.
    *   **Missing:** A mechanism to detect actual funding payments (e.g., specific transaction types in WebSocket user stream, periodic checks of transaction history) and call `record_payment`.
    *   `RiskManager` *could* potentially query `calculate_metrics` to adjust risk based on prediction accuracy, but this is not implemented.

*   **Observations & Strengths:**
    *   Addresses a key risk: inaccurate funding rate data invalidating the strategy.
    *   Uses standard metrics for assessing prediction accuracy.
*   **Concerns & Areas for Improvement:**
    *   **Payment Detection:** The lack of a mechanism to reliably detect and record actual funding payments makes the validator non-functional. This is the most critical missing piece.
    *   **Metric Usage:** The calculated metrics are currently unused. They don't influence trading decisions or risk management.
    *   **Data Storage:** In-memory storage is not suitable for long-running deployments; requires pruning or persistent storage.
*   **Recommendations:**
    *   **Implement Payment Detection:** **Priority 1:** Design and implement logic to detect actual funding payments. This likely involves:
        *   Enhancing `API Clients` to parse funding payment events from WebSocket user streams (if available).
        *   Alternatively, periodically fetching transaction history via REST and identifying funding payments.
        *   Ensuring the detected payments (with correct symbol, rate, amount, timestamp) are reliably passed to `FundingRateValidator.record_payment`.
    *   **Define Metric Usage (Future):** Decide *if* and *how* poor funding rate prediction accuracy should impact the system. Options:
        *   Alerting only.
        *   Triggering a specific circuit breaker.
        *   Adjusting risk parameters in `RiskManager` (e.g., reduce size if predictions are poor).
        *   Modifying strategy thresholds.
        Implement the chosen mechanism.
    *   **Implement Pruning/Persistence:** Ensure `clear_old_data` is called periodically (e.g., in a background task or triggered by `calculate_metrics`). For better analysis, consider logging prediction/payment pairs to a database or structured log file.
    *   **Strategy Integration:** Ensure the strategy consistently calls `record_prediction` when using a funding rate for decision-making.
