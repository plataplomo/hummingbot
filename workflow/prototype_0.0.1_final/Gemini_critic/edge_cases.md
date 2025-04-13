**(Slightly sharper tone)**

You want a prompt to make *another* AI think about edge cases for *this specific mess*? Fine. But understand this: generating edge cases isn't magic. It requires understanding the **system's boundaries, its failure points, its dependencies, and the hostile environment it operates in.** An AI can only generate meaningful edge cases if prompted with sufficient **context and constraints**.

Feeding it vague requests will get you generic, useless garbage like "what if price is negative?" or "what if network fails?". Useless.

Here's a prompt structure designed to force an AI (or *you*, if you had the discipline) to think **specifically and harshly** about *our* system's potential failures. Don't just copy-paste; **understand the *categories* of failure** it forces you to consider.

---

**Prompt Template for Generating Edge Cases (CyberDeltaEngine v0.0.1 - HL Perp vs BP Perp Focus)**

**System Context:**

You are analyzing `CyberDeltaEngine`, an asynchronous Python trading bot executing funding rate arbitrage between Hyperliquid (HL) perpetuals and Backpack (BP) perpetuals.

**Core Components:**
*   `Main/Engine`: Orchestrator.
*   `API Clients (HL, BP)`: Using `aiohttp`, WebSockets, handle Auth (EIP-712 for HL, ED25519 for BP), Rate Limits.
*   `DataHandler`: Consumes WS/REST data (Funding Rates, Mark Prices, Index Prices, Order Books, Tickers). Caches latest data. Checks staleness.
*   `PortfolioTracker`: Tracks positions (size, entry, PnL), balances (USD), orders on both HL & BP. Uses State Manager for persistence (atomic file save).
*   `SignalGenerator (FundingRateArbitrage)`: Calculates `NFD = FR[HL] - FR[BP]`, `Basis = Mark[HL] - Mark[BP]`, Basis Volatility. Generates LONG/SHORT signals based on NFD, basis checks, cost estimates.
*   `RiskManager (Simplified)`: Enforces hard caps (Max USD/Position, Max Total Exposure %, Max Portfolio Leverage, Max Exchange Concentration %). Performs basic margin/liquidation proximity checks for BOTH perp legs. Uses simple sizing (e.g., fixed fraction, TBD). **NO Kelly/VaR yet.**
*   `ExecutionHandler`: Executes dual-leg trades sequentially. Attempts compensation (reversing leg 1) if leg 2 fails, then **alerts for manual intervention** if compensation fails. Tracks execution state.
*   `BalanceMonitor`: Checks available margin collateral on HL & BP. Alerts if low. **NO auto-transfer.**
*   `StateManager`: Atomic JSON file save/load with checksums/backups.
*   `ValidationSystem`:
    *   `FundingRateValidator`: Compares API-provided rates (`/markPrices` for BP) vs actual payments (from fills/API). Logs metrics (RMSE, MAE).
    *   `PositionReconciler`: Compares `PortfolioTracker` state vs `APIClient.get_positions()` vs (optional) fills. Triggers safe mode on discrepancy.
*   `CircuitBreakerManager`: Manages CBs for API errors, volatility, drawdowns, etc., integrated into API calls and Execution Handler.

**Task:**

Generate a **comprehensive and brutal list of edge cases and potential failure scenarios** specifically for this system configuration (v0.0.1). Focus on realistic, high-impact failures. Categorize them clearly. For each edge case, briefly describe the scenario and the **expected *correct* system behavior** (e.g., specific error logged, circuit breaker tripped, safe mode entered, trade rejected, specific alert generated, state remains consistent). Avoid generic "network error" – be specific about *when* and *where* it occurs.

**Categorization REQUIRED:**

1.  **API Client Failures (HL & BP Separately):**
    *   Authentication (Expired keys, invalid signatures, clock skew affecting window)
    *   Rate Limits (Hitting hard limits, unexpected 429s despite internal limiter)
    *   Endpoint Errors (5xx Server Errors, 4xx Client Errors - bad request, not found, insufficient funds returned *during* critical ops)
    *   Data Fetching Errors (Timeout getting balances/positions/funding rates; partial/empty responses; garbage JSON)
    *   Order Placement Failures (Invalid quantity/price steps, price band violations, insufficient margin *at exchange*, order immediately rejected)
    *   Order Status Inconsistency (API says OPEN, but WS shows FILL; API query fails after placement)
    *   Cancel Order Failures (Order already filled/cancelled, ID not found, API error during cancel)
    *   Specific Endpoint Failures (`/markPrices` fails, `/position` fails, `/fundingRates` fails)
2.  **WebSocket Failures (HL & BP Separately):**
    *   Connection Drops (During startup, mid-trade, during data processing)
    *   Reconnection Failures (Persistent failure after backoff)
    *   Missed Messages / Gaps in Sequence
    *   Corrupted/Invalid Messages (Bad JSON, unexpected format)
    *   Latency Spikes (Messages delayed significantly)
    *   Missed Ping/Pong causing disconnect
    *   Subscription Failures
    *   Out-of-sync private data (e.g., `account.positionUpdate` lagging REST API)
3.  **Data Handling & State Issues:**
    *   Stale Data (Funding rates, prices, order books older than threshold)
    *   Normalization Errors (Different price/size precision between exchanges causing issues)
    *   Timestamp Mismatches (Micro vs Milli causing comparison errors)
    *   Cache Invalidation Failures
    *   `PortfolioTracker` state inconsistency (failure during fill update, race conditions - *if applicable*)
    *   `StateManager` Failures (File write error, disk full, corrupted state file on load, checksum mismatch, backup failure)
4.  **Strategy & Signal Generation:**
    *   Zero or near-zero volatility causing division errors.
    *   Extreme NFD or Basis values (potential data error).
    *   Calculation errors (e.g., `Decimal` precision).
    *   Failure to get required data (e.g., mark price missing for basis calc).
    *   Rapid signal flapping (generating opposite signals frequently).
5.  **Risk Management:**
    *   Failure to get accurate portfolio state (balances, positions, exposure) before sizing.
    *   Hitting hard limits exactly (max position size, max exposure).
    *   Margin calculation errors or discrepancies vs exchange.
    *   Liquidation price calculation errors or missing data.
    *   Incorrectly applying stricter limits for dual-perp vs perp-spot.
    *   Race condition where exposure increases between check and execution.
6.  **Execution Handler & Atomicity:**
    *   Leg 1 (e.g., HL) succeeds, Leg 2 (BP) fails (API error, timeout, rejection).
    *   Leg 1 succeeds, Leg 2 fails, **Compensation (Reversal) of Leg 1 ALSO fails.**
    *   Partial fill on Leg 1, followed by failure/timeout/partial fill on Leg 2.
    *   Significant latency between Leg 1 fill and Leg 2 placement causing price drift.
    *   Order status monitoring fails or returns inconsistent states.
    *   `ExecutionHandler` crashes mid-execution.
7.  **Safety Systems:**
    *   `FundingRateValidator` fails to match predictions/payments; DB error.
    *   `PositionReconciler` gets conflicting data from API vs Local; fails to reconcile; triggers Safe Mode incorrectly/correctly.
    *   `CircuitBreaker` trips unexpectedly (e.g., due to transient API flutter); fails to enter HALF-OPEN; fails to reset.
    *   Interaction failures: CB trips *during* reconciliation; Validation fails *during* execution.
8.  **System Level / Concurrency:**
    *   Resource exhaustion (memory, CPU, file handles).
    *   Unhandled exceptions causing task crashes.
    *   Deadlocks or race conditions between async tasks.
    *   Incorrect shutdown sequence leaving resources open.
    *   Clock Skew affecting timestamps across system/exchanges.

**Output Format:**

For each edge case:
*   **Scenario:** Clear description of the failure condition.
*   **Component(s) Affected:** Which parts of the system are involved.
*   **Expected Behavior:** How the system *should* react (log level, specific action, state change, alert).

**Focus:** Prioritize scenarios causing **financial loss, incorrect positions, system instability, or data inconsistency.**

---

**Now, feed that prompt structure (filled with the specifics of YOUR code's implementation details where possible) to your AI tool.** Or better yet, use it as a **checklist for your own brain** and your **test plan**. An AI might generate a list, but *you* need the deep understanding to know which ones are most critical and how to actually test them. Don't expect the AI to magically find everything or understand the subtleties without extremely precise context about *your* specific implementation choices.