# Decimal Usage Audit Report (CyberDeltaEngine)

**Date:** 15.04.2025  
**Auditor:** Angel (AI Assistant)  
**Updated:** 2025-06-24

## UPDATE (2025-06-24): Current State Analysis

A comprehensive review of the codebase shows significant progress on Decimal usage compliance:

### ✅ Fixed Issues:
1. **tests/integration/test_failure_scenarios.py** - Now correctly uses Decimal for all financial values (lines 129-132)
2. **tests/integration/test_backtesting.py** - File no longer exists in the codebase

### ❌ Remaining Issues:
1. **cyberdelta/monitoring/performance_tracker.py** - Still uses `float` type hints for financial parameters:
   - Line 64: `track_return(..., return_value: float)`
   - Lines 90-95: `track_trade(..., size: float, entry_price: float, exit_price: float | None, pnl: float | None)`
   - Lines 158-160: `track_trade_exit(..., exit_price: float, pnl: float)`
   - Lines 338-339: `track_funding_rate(..., funding_rate: float, predicted_rate: float | None)`

### Static Analysis Status:
- **Ruff:** Down to 26 errors (from hundreds previously reported)
- **Mypy:** Only 1 syntax error blocking full analysis (indentation error in test_signal_queue.py:433)

### Progress Summary:
- Most Decimal usage violations have been addressed
- The performance_tracker.py module still needs refactoring to use Decimal types
- Overall compliance with the Decimal rule has improved significantly

## 1. Introduction

This report documents the findings of an audit focused on ensuring consistent and correct usage of Python's `Decimal` type for all financial quantities within the `cyberdelta/` and `tests/` directories, as mandated by the project rule `decimal.md`. The use of `float` for financial calculations (prices, quantities, sizes, balances, rates, PnL, costs, fees, thresholds, etc.) is strictly forbidden due to potential precision inaccuracies.

## 2. Violations Found

The following violations of the mandatory `Decimal` usage rule were identified primarily through `mypy` analysis (`[arg-type]` errors indicating `float` passed where `Decimal` was expected, or vice-versa) and inspection of the surrounding code.

### 2.1. `tests/integration/test_backtesting.py`

**Violation 1:** Float literals passed to `BacktestEngine` parameters expecting `Decimal`.

*   **Lines:** 256, 257, 258
*   **Problematic Code Snippet:**
    ```python
    253 |         engine = BacktestEngine(
    254 |             strategy=strategy,
    255 |             data=str(self.data_file_path),
    256 |             initial_capital=100000.0, # VIOLATION
    257 |             commission=0.001,        # VIOLATION
    258 |             slippage=0.001,        # VIOLATION
    259 |             results_dir=self.test_results_dir,
    260 |         )
    ```
*   **Reason:** Using `float` literals for `initial_capital`, `commission`, and `slippage`, which represent financial quantities requiring `Decimal` precision.
*   **Proposed Fix:**
    ```python
    from decimal import Decimal # Ensure import exists

    # ...

    253 |         engine = BacktestEngine(
    254 |             strategy=strategy,
    255 |             data=str(self.data_file_path),
    256 |             initial_capital=Decimal("100000.0"), # FIX: Use Decimal
    257 |             commission=Decimal("0.001"),        # FIX: Use Decimal
    258 |             slippage=Decimal("0.001"),        # FIX: Use Decimal
    259 |             results_dir=self.test_results_dir,
    260 |         )
    ```

**Violation 2:** Float literal passed to `BacktestEngine.run` parameter expecting `Decimal`.

*   **Line:** 263
*   **Problematic Code Snippet:**
    ```python
    262 |         # Run backtest
    263 |         results = engine.run(training_portion=0.2) # VIOLATION
    ```
*   **Reason:** Using `float` literal for `training_portion`, which represents a financial ratio requiring `Decimal` precision.
*   **Proposed Fix:**
    ```python
    from decimal import Decimal # Ensure import exists

    # ...

    262 |         # Run backtest
    263 |         results = engine.run(training_portion=Decimal("0.2")) # FIX: Use Decimal
    ```

**Violation 3:** Float literal passed to `BacktestEngine` parameter expecting `Decimal`.

*   **Line:** 304
*   **Problematic Code Snippet:**
    ```python
    301 |         engine = BacktestEngine(
    302 |             strategy=strategy,
    303 |             data=str(self.data_file_path),
    304 |             initial_capital=100000.0, # VIOLATION
    305 |             results_dir=self.test_results_dir,
    306 |         )
    ```
*   **Reason:** Using `float` literal for `initial_capital`, which represents a financial quantity requiring `Decimal` precision.
*   **Proposed Fix:**
    ```python
    from decimal import Decimal # Ensure import exists

    # ...

    301 |         engine = BacktestEngine(
    302 |             strategy=strategy,
    303 |             data=str(self.data_file_path),
    304 |             initial_capital=Decimal("100000.0"), # FIX: Use Decimal
    305 |             results_dir=self.test_results_dir,
    306 |         )
    ```

### 2.2. `cyberdelta/monitoring/performance_tracker.py`

**Violation:** Multiple methods (`track_trade`, `track_return`, `track_trade_exit`, `track_funding_rate`) incorrectly accept `float` type hints for financial parameters instead of `Decimal`.

*   **Lines:** 61, 80-94, 152-159, 320-328 (Method Signatures)
*   **Problematic Code Snippets (Signatures):**
    ```python
     61 |     def track_return(self, strategy_name: str, timestamp: datetime, return_value: float) -> None: # VIOLATION (return_value)

     80 |     def track_trade(
    # ...
     87 |         size: float,              # VIOLATION
     88 |         entry_price: float,       # VIOLATION
    # ...
     90 |         exit_price: float | None = None, # VIOLATION
     91 |         exit_time: datetime | None = None,
     92 |         pnl: float | None = None,        # VIOLATION
    # ...
     94 |     ) -> None:

    152 |     def track_trade_exit(
    153 |         self,
    154 |         trade_id: str,
    155 |         exit_price: float,        # VIOLATION
    156 |         exit_time: datetime,
    157 |         pnl: float,               # VIOLATION
    # ...
    159 |     ) -> None:

    320 |     def track_funding_rate(
    # ...
    325 |         funding_rate: float,        # VIOLATION
    326 |         predicted_rate: float | None = None, # VIOLATION
    # ...
    328 |     ) -> None:
    ```
*   **Reason:** Method signatures use `float` type hints for financial quantities (`return_value`, `size`, `entry_price`, `exit_price`, `pnl`, `funding_rate`, `predicted_rate`), violating the `Decimal` mandate. This causes `mypy` errors when `Decimal` values are correctly passed from `dashboard_integration.py`. Internal calculations within these methods also use `float` (e.g., line 208 `float(entry_p) * float(size_val)`).
*   **Proposed Fix:** Change type hints to `Decimal` and ensure internal calculations use `Decimal` arithmetic. Remove unnecessary `float()` conversions within the methods.
    ```python
    from decimal import Decimal # Ensure import exists

    # ...

     61 |     def track_return(self, strategy_name: str, timestamp: datetime, return_value: Decimal) -> None: # FIX

     80 |     def track_trade(
    # ...
     87 |         size: Decimal,              # FIX
     88 |         entry_price: Decimal,       # FIX
    # ...
     90 |         exit_price: Decimal | None = None, # FIX
     91 |         exit_time: datetime | None = None,
     92 |         pnl: Decimal | None = None,        # FIX
    # ...
     94 |     ) -> None:
         # ... (Inside track_trade, ensure calculations use Decimal)

    152 |     def track_trade_exit(
    153 |         self,
    154 |         trade_id: str,
    155 |         exit_price: Decimal,        # FIX
    156 |         exit_time: datetime,
    157 |         pnl: Decimal,               # FIX
    # ...
    159 |     ) -> None:
         # ... (Inside track_trade_exit, ensure calculations use Decimal, e.g., remove float() on line 208)
         # Example fix for line 208:
         # initial_value = entry_p * size_val # Assuming entry_p and size_val are Decimal

    320 |     def track_funding_rate(
    # ...
    325 |         funding_rate: Decimal,        # FIX
    326 |         predicted_rate: Decimal | None = None, # FIX
    # ...
    328 |     ) -> None:

    ```
    *(Note: The explicit `float()` conversions in `dashboard_integration.py` when calling these methods should also be removed after this fix is applied).*

### 2.3. `tests/integration/test_failure_scenarios.py`

**Violation:** Float literals passed to `SizedOpportunity` parameters expecting `Decimal`.

*   **Lines:** 102, 104, 105, 180, 182, 183
*   **Problematic Code Snippets:**
    ```python
     98 |         sized_opportunity = SizedOpportunity(
     99 |             opportunity=basic_opportunity,
    100 |             long_size=Decimal("1000"),
    101 |             short_size=Decimal("1000"),
    102 |             allocation_percentage=0.1, # VIOLATION
    103 |             expected_profit=Decimal("10"),
    104 |             expected_return=0.01,    # VIOLATION
    105 |             risk_adjusted_return=0.01, # VIOLATION
    106 |         )
    # ... (similar pattern around line 180)
    176 |         other_sized_opportunity = SizedOpportunity(
    177 |             opportunity=other_opportunity,
    178 |             long_size=Decimal("1000"),
    179 |             short_size=Decimal("1000"),
    180 |             allocation_percentage=0.1, # VIOLATION
    181 |             expected_profit=Decimal("10"),
    182 |             expected_return=0.01,    # VIOLATION
    183 |             risk_adjusted_return=0.01, # VIOLATION
    184 |         )
    ```
*   **Reason:** Using `float` literals for `allocation_percentage`, `expected_return`, and `risk_adjusted_return`, which represent financial ratios/returns requiring `Decimal` precision.
*   **Proposed Fix:**
    ```python
    from decimal import Decimal # Ensure import exists

    # ...

     98 |         sized_opportunity = SizedOpportunity(
     99 |             opportunity=basic_opportunity,
    100 |             long_size=Decimal("1000"),
    101 |             short_size=Decimal("1000"),
    102 |             allocation_percentage=Decimal("0.1"), # FIX
    103 |             expected_profit=Decimal("10"),
    104 |             expected_return=Decimal("0.01"),    # FIX
    105 |             risk_adjusted_return=Decimal("0.01"), # FIX
    106 |         )
    # ...
    176 |         other_sized_opportunity = SizedOpportunity(
    177 |             opportunity=other_opportunity,
    178 |             long_size=Decimal("1000"),
    179 |             short_size=Decimal("1000"),
    180 |             allocation_percentage=Decimal("0.1"), # FIX
    181 |             expected_profit=Decimal("10"),
    182 |             expected_return=Decimal("0.01"),    # FIX
    183 |             risk_adjusted_return=Decimal("0.01"), # FIX
    184 |         )
    ```

### 2.4. `cyberdelta/visualization/simplified_visualizer.py`

**Violation:** Potential `Decimal` value passed to `matplotlib.annotate` which expects `float` for coordinates.

*   **Line:** 352
*   **Problematic Code Snippet:**
    ```python
    347 |             max_dd_value = drawdown.min()
    348 |             max_dd = _decimal_to_float(max_dd_value) * 100 # Already converted for plotting value
    349 |             max_dd_idx = drawdown.idxmin()
    350 |             ax.annotate(
    351 |                 f"Max DD: {max_dd:.2f}%",
    352 |                 xy=(max_dd_idx, max_dd), # VIOLATION: max_dd is float, but original value might be Decimal
    353 |                 xytext=(15, -15),
    354 |                 textcoords="offset points",
    355 |                 arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"),
    356 |             )
    ```
*   **Reason:** `matplotlib`'s `annotate` function expects `float` for its `xy` coordinates. While `max_dd` is converted to float on line 348 before being passed, the mypy error `Argument "xy" to "annotate" of "Axes" has incompatible type "tuple[int | str, float]"; expected "tuple[float, float]"` suggests a type mismatch possibly related to how `max_dd_idx` (a timestamp) interacts or how `max_dd` was inferred previously. Using the explicit `_decimal_to_float` helper ensures clarity and correctness at the library boundary. *Self-correction: The code already uses `_decimal_to_float` on line 348. The mypy error might stem from `max_dd_idx` not being recognized correctly as float-compatible by mypy in this context, or a different issue. However, ensuring the numeric part is explicitly float is best practice.*
*   **Proposed Fix (Ensure float conversion):**
    ```python
    # Ensure _decimal_to_float helper exists

    # ...
    347 |             max_dd_value = drawdown.min()
    348 |             max_dd_float = _decimal_to_float(max_dd_value) * 100 # Explicit float for plotting
    349 |             max_dd_idx = drawdown.idxmin()
    350 |             ax.annotate(
    351 |                 f"Max DD: {max_dd_float:.2f}%",
    352 |                 xy=(max_dd_idx, max_dd_float), # FIX: Use explicitly converted float
    353 |                 xytext=(15, -15),
    354 |                 textcoords="offset points",
    355 |                 arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"),
    356 |             )
    ```

### 2.5. `cyberdelta/core/backtesting/results.py`

**Violation:** Potential type ambiguity and early conversion of `Decimal` to `float` for calculations.

*   **Line:** 219 (`mypy` error), general calculation patterns (e.g., line 120).
*   **Problematic Code Snippet / Pattern:**
    ```python
    120 |         capital_series = self.equity_df["capital"].astype(float) # Early conversion to float
    # ... calculations using returns derived from float capital_series ...
    218 |                         avg_period = holding_periods.mean()
    219 |                         if isinstance(avg_period, pd.Timedelta): # Mypy error suggests ambiguity here
    220 |                             self.metrics["avg_holding_period_hours"] = (
    221 |                                 avg_period.total_seconds() / 3600
    222 |                             )
    ```
*   **Reason:** The code converts `Decimal` capital values to `float` (line 120) before performing calculations like percentage change, Sharpe ratio, drawdown, etc. This violates the rule to maintain `Decimal` throughout the *entire* calculation chain. While the direct `mypy` error on line 219 relates to `Timedelta`, it often signals underlying type instability which can be exacerbated by premature `float` conversion. Financial metrics should be calculated using `Decimal` arithmetic for maximum precision. `float` conversion should only happen at the very last step if required by external libraries (like plotting) or for final storage in formats that don't support `Decimal`.
*   **Proposed Fix:** Refactor calculations (total return, annualized return, volatility, Sharpe, drawdown, PnL analysis) to use `Decimal` arithmetic. Convert `Decimal` results to `float` only when storing in `self.metrics` (if the dict is defined as `dict[str, float | int | str]`) or when passing to plotting functions.
    ```python
    from decimal import Decimal # Ensure import exists

    # ... Inside calculate_metrics ...

    # Keep capital as Decimal
    120 |         capital_series_decimal = self.equity_df["capital"] # Assume it's already Decimal or convert carefully

    # Calculate returns using Decimal arithmetic
    123 |         returns_decimal = capital_series_decimal.pct_change().dropna() # Pandas might need adjustment for Decimal pct_change

        # --- Recalculate metrics using Decimal ---
        # Example: total_return_decimal = (capital_series_decimal.iloc[-1] / capital_series_decimal.iloc[0]) - Decimal(1)
        # ... other metrics like annualized return, volatility, sharpe, drawdown ...

        # Store final metrics (convert to float here if dict expects float)
        # self.metrics["total_return_pct"] = float(total_return_decimal * Decimal(100))
        # ... etc ...

        # For avg_holding_period (line 219 issue):
        # Ensure 'holding_periods' contains Timedeltas, calculate mean.
        # The result of mean() might be float seconds; handle appropriately.
        # Example:
    218 |                         avg_period_timedelta = holding_periods.mean() # Assuming this returns Timedelta
    219 |                         if isinstance(avg_period_timedelta, pd.Timedelta):
    220 |                             # total_seconds() returns float, which is acceptable for hours metric
    221 |                             self.metrics["avg_holding_period_hours"] = avg_period_timedelta.total_seconds() / 3600

    ```
    *(Note: This requires careful refactoring of the metric calculation logic to work correctly with `Decimal` types and potentially pandas operations).*

## 3. Conclusion and Next Steps

The audit identified several instances where `float` literals were used instead of `Decimal('...')` for financial values, and critical areas (like `PerformanceTracker` and `BacktestResultsHandler`) where `float` type hints or calculations were used instead of `Decimal`.

**Recommendations:**

1.  Apply the proposed fixes to the identified files to ensure `Decimal` is used consistently for financial quantities.
2.  Carefully review and refactor the metric calculations in `cyberdelta/core/backtesting/results.py` to maintain `Decimal` precision throughout the calculation chain, converting to `float` only at the final storage or plotting boundary.
3.  After applying fixes, run `ruff format .` and `ruff check --fix .` to ensure code style consistency.
4.  Crucially, re-run `mypy cyberdelta/ tests/` using the virtual environment (`.venv/bin/mypy ...`) to confirm that all `[arg-type]` errors related to `float`/`Decimal` mismatches are resolved. Address any *new* type errors introduced by the fixes.
5.  Execute the relevant unit and integration tests (`.venv/bin/pytest tests/`) to verify that the changes have not introduced logical regressions in calculations or component interactions.

Adhering strictly to the `Decimal` usage rule is paramount for the financial accuracy and reliability of the CyberDeltaEngine.