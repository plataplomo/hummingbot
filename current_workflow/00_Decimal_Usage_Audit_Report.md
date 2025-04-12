# Decimal Usage Audit Report

This report identifies instances where Python's standard `float` type is used for financial quantities instead of the more precise `Decimal` type, as mandated by the project's `decimal.mdc` rule.

## Critical Violations

### 1. `cyberdelta/core/portfolio_tracker.py`

**File Path:** `cyberdelta/core/portfolio_tracker.py` - Line 920-938
**Problematic Code Snippet:**
```python
def get_exchange_exposure(self, exchange_id: str) -> float:
    """
    Get the current exposure for an exchange.

    Args:
        exchange_id: Exchange identifier

    Returns:
        Current exposure in USD
    """
    if exchange_id not in self._positions:
        return 0.0

    exposure = 0.0
    for position in self._positions[exchange_id].values():
        if position.is_active():
            # Use mark_price for a more accurate exposure calculation
            exposure += position.mark_price * position.size

    return exposure
```

**Violation:** Function returns `float` for financial exposure calculation instead of `Decimal`, and initializes with float literals (`0.0`).

**Proposed Fix:**
```python
def get_exchange_exposure(self, exchange_id: str) -> Decimal:
    """
    Get the current exposure for an exchange.

    Args:
        exchange_id: Exchange identifier

    Returns:
        Current exposure in USD
    """
    if exchange_id not in self._positions:
        return Decimal("0.0")

    exposure = Decimal("0.0")
    for position in self._positions[exchange_id].values():
        if position.is_active():
            # Use mark_price for a more accurate exposure calculation
            if position.mark_price is not None and position.size is not None:
                # Ensure both values are Decimal before multiplication
                mark_price = position.mark_price if isinstance(position.mark_price, Decimal) else Decimal(str(position.mark_price))
                size = position.size if isinstance(position.size, Decimal) else Decimal(str(position.size))
                exposure += mark_price * size

    return exposure
```

### 2. `cyberdelta/core/risk_manager.py`

**File Path:** `cyberdelta/core/risk_manager.py` - Line 880-938
**Problematic Code Snippet:**
```python
def _get_validation_metrics(self, exchange: str, symbol: str) -> float:
    """
    # ... function docstring ...
    """
    # ...
    # Normalize metrics against acceptable thresholds (use floats for ratios)
    rmse_float = float(rmse)
    bias_float = float(bias)
    max_rmse_float = float(self.max_acceptable_rmse)
    max_bias_float = float(self.max_acceptable_bias)
    min_factor_float = float(self.min_validation_factor)
    
    # Calculate component scores (0.0 to 1.0)
    rmse_score = (
        max(0.0, 1.0 - (rmse_float / max_rmse_float)) if max_rmse_float > 0 else 1.0
    )
    
    bias_score = (
        max(0.0, 1.0 - (abs(bias_float) / max_bias_float)) if max_bias_float > 0 else 1.0
    )
    
    # Combine scores with weights
    combined_factor = (rmse_score * 0.6) + (bias_score * 0.4)
    
    # Ensure factor is within bounds [min_validation_factor, 1.0]
    final_factor = max(min_factor_float, combined_factor)
    final_factor = min(1.0, final_factor)
    
    # ... logging ...
    
    return final_factor  # Return float factor
```

**Violation:** Function returns a `float` for a financial model validation factor that affects sizing of trades. The entire calculation uses floats, which can lead to precision errors in the risk model.

**Proposed Fix:**
```python
def _get_validation_metrics(self, exchange: str, symbol: str) -> Decimal:
    """
    # ... function docstring ...
    """
    # ...
    # Convert to Decimal once instead of float
    rmse_dec = rmse if isinstance(rmse, Decimal) else Decimal(str(rmse))
    bias_dec = bias if isinstance(bias, Decimal) else Decimal(str(bias))
    max_rmse_dec = self.max_acceptable_rmse if isinstance(self.max_acceptable_rmse, Decimal) else Decimal(str(self.max_acceptable_rmse))
    max_bias_dec = self.max_acceptable_bias if isinstance(self.max_acceptable_bias, Decimal) else Decimal(str(self.max_acceptable_bias))
    min_factor_dec = self.min_validation_factor if isinstance(self.min_validation_factor, Decimal) else Decimal(str(self.min_validation_factor))
    
    # Calculate component scores (0.0 to 1.0) using Decimal
    rmse_score = (
        max(Decimal("0.0"), Decimal("1.0") - (rmse_dec / max_rmse_dec)) if max_rmse_dec > Decimal("0") else Decimal("1.0")
    )
    
    bias_score = (
        max(Decimal("0.0"), Decimal("1.0") - (abs(bias_dec) / max_bias_dec)) if max_bias_dec > Decimal("0") else Decimal("1.0")
    )
    
    # Combine scores with weights using Decimal
    combined_factor = (rmse_score * Decimal("0.6")) + (bias_score * Decimal("0.4"))
    
    # Ensure factor is within bounds [min_validation_factor, 1.0]
    final_factor = max(min_factor_dec, combined_factor)
    final_factor = min(Decimal("1.0"), final_factor)
    
    # ... logging (update formats to handle Decimal) ...
    
    return final_factor  # Return Decimal factor
```

### 3. `cyberdelta/core/backtesting.py`

**File Path:** `cyberdelta/core/backtesting.py` - Lines 74-76
**Problematic Code Snippet:**
```python
def __init__(
    self,
    data_source: DataSource,
    strategy: Strategy,
    initial_capital: float = 100000.0,
    commission: float = 0.001,  # 0.1% per trade
    slippage: float = 0.001,  # 0.1% slippage
    ...
```

**Violation:** Using `float` for financial parameters (initial_capital, commission, slippage) in the backtesting engine.

**Proposed Fix:**
```python
def __init__(
    self,
    data_source: DataSource,
    strategy: Strategy,
    initial_capital: Decimal = Decimal("100000.0"),
    commission: Decimal = Decimal("0.001"),  # 0.1% per trade
    slippage: Decimal = Decimal("0.001"),  # 0.1% slippage
    ...
```

### 4. `cyberdelta/core/backtesting.py`

**File Path:** `cyberdelta/core/backtesting.py` - Lines 153-157
**Problematic Code Snippet:**
```python
self.equity_curve: list[tuple[datetime, float]] = []
# ... more code ...
str, float | int | str
```

**Violation:** Using `float` for equity curve values which are financial data.

**Proposed Fix:**
```python
self.equity_curve: list[tuple[datetime, Decimal]] = []
# ... more code ...
str, Decimal | int | str
```

### 5. `cyberdelta/core/backtesting.py`

**File Path:** `cyberdelta/core/backtesting.py` - Lines 315-364
**Problematic Code Snippet:**
```python
# Convert Decimal to float/str for JSON if needed, but keep internal as Decimal
"price": float(signal.get("price", Decimal("0"))),
"size": float(trade_size_capital),  # Size in capital terms
"cost": float(transaction_cost),
"quantity": float(signal_size_decimal),  # Original quantity from signal
# ... more similar conversions ...
```

**Violation:** Converting Decimal financial values to `float` for storage, losing precision.

**Proposed Fix:**
```python
# Store as strings to preserve Decimal precision
"price": str(signal.get("price", Decimal("0"))),
"size": str(trade_size_capital),  # Size in capital terms
"cost": str(transaction_cost),
"quantity": str(signal_size_decimal),  # Original quantity from signal
# ... update all similar conversions ...
```

### 6. `cyberdelta/monitoring/performance_metrics.py` 

**File Path:** `cyberdelta/monitoring/performance_metrics.py` - Multiple functions
**Problematic Code Snippet:**
```python
def calculate_sharpe_ratio(
    returns: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252
) -> float:
    # ...
    return 0.0  # Return a concrete float value instead of np.nan
    # ...
    annualized_sharpe_ratio = float(sharpe_ratio * np.sqrt(periods_per_year))
```

**Violation:** Using `float` for financial metrics calculations and return values.

**Proposed Fix:**
```python
def calculate_sharpe_ratio(
    returns: pd.Series, risk_free_rate: Decimal = Decimal("0.0"), periods_per_year: int = 252
) -> Decimal:
    # ...
    return Decimal("0.0")  # Return a concrete Decimal value
    # ...
    # Convert numpy result to Decimal
    sqrt_periods = Decimal(str(np.sqrt(periods_per_year)))
    annualized_sharpe_ratio = Decimal(str(sharpe_ratio)) * sqrt_periods
```

### 7. `cyberdelta/monitoring/dashboard_integration.py`

**File Path:** `cyberdelta/monitoring/dashboard_integration.py` - Functions with float parameters
**Problematic Code Snippet:**
```python
def track_return(self, strategy_name: str, timestamp: datetime, return_value: float):
    # ...

def track_trade_entry(
    self, 
    strategy_name: str, 
    trade_id: str, 
    symbol: str, 
    side: str, 
    size: float,
    entry_price: float,
    timestamp: datetime,
    exit_price: float | None = None,
    exit_time: datetime | None = None,
    pnl: float | None = None,
):
    # ...
```

**Violation:** Using `float` for financial values in monitoring and dashboard integration.

**Proposed Fix:**
```python
def track_return(self, strategy_name: str, timestamp: datetime, return_value: Decimal):
    # ...

def track_trade_entry(
    self, 
    strategy_name: str, 
    trade_id: str, 
    symbol: str, 
    side: str, 
    size: Decimal,
    entry_price: Decimal,
    timestamp: datetime,
    exit_price: Decimal | None = None,
    exit_time: datetime | None = None,
    pnl: Decimal | None = None,
):
    # ...
```

## Additional Violations

### 8. `cyberdelta/core/models.py`

**File Path:** `cyberdelta/core/models.py` - ArbitrageOpportunity class (Lines 765-778)
**Problematic Code Snippet:**
```python
confidence: float | None = None,
basis_volatility: float | None = None,
utility_score: float | None = None,
# ...
self.confidence = confidence  # float or None
self.basis_volatility = basis_volatility  # float or None
self.utility_score = utility_score  # float or None
```

**Violation:** Using `float` for financial model parameters.

**Proposed Fix:**
```python
confidence: Decimal | None = None,
basis_volatility: Decimal | None = None,
utility_score: Decimal | None = None,
# ...
self.confidence = confidence  # Decimal or None
self.basis_volatility = basis_volatility  # Decimal or None
self.utility_score = utility_score  # Decimal or None
```

### 9. `cyberdelta/core/signal_generator.py`

**File Path:** `cyberdelta/core/signal_generator.py` - Lines 304-333
**Problematic Code Snippet:**
```python
# Use numpy for standard deviation, converting Decimals to floats for calculation
# Convert Decimal list to list of floats for numpy
rates_float = [float(r) for r in rates]
std_dev = np.std(rates_float)
# ...
basis_float = [float(b) for b in basis_values]
std_dev = np.std(basis_float)
```

**Violation:** Converting `Decimal` financial values to `float` for calculations.

**Proposed Fix:**
```python
# Convert to numpy array but maintain precision
rates_array = np.array([str(r) for r in rates], dtype=np.dtype('O'))
# Use Decimal's quantization for standard deviation calculation
values = [Decimal(str(v)) for v in rates]
n = len(values)
if n < 2:
    return Decimal("0.0001")
mean = sum(values) / n
std_dev = Decimal(str(math.sqrt(sum((x - mean) ** 2 for x in values) / (n - 1))))
```

### 10. `cyberdelta/core/risk_manager.py` 

**File Path:** `cyberdelta/core/risk_manager.py` - Conversion back to float for reporting (Lines 1363-1370)
**Problematic Code Snippet:**
```python
# Report floats for easier JSON serialization/external use
"total_capital_usd": float(total_capital),
"total_exposure_usd": float(total_exposure),
"total_exposure_pct": float((total_exposure / total_capital) * 100)
# ...
"max_total_exposure_limit_usd": float(self.max_total_exposure),
"max_total_exposure_limit_pct": float((self.max_total_exposure / total_capital) * 100)
```

**Violation:** Converting `Decimal` financial values to `float` for reporting and serialization.

**Proposed Fix:**
```python
# Report as strings to maintain precision
"total_capital_usd": str(total_capital),
"total_exposure_usd": str(total_exposure),
"total_exposure_pct": str((total_exposure / total_capital) * Decimal("100"))
# ...
"max_total_exposure_limit_usd": str(self.max_total_exposure),
"max_total_exposure_limit_pct": str((self.max_total_exposure / total_capital) * Decimal("100"))
```

### 11. `cyberdelta/monitoring/persistence.py`

**File Path:** `cyberdelta/monitoring/persistence.py` - Multiple locations
**Problematic Code Snippet:**
```python
processed_data: dict[str, dict[datetime, float]] = {}
# ...
# Assuming val is float or compatible
processed_data[strategy][datetime.fromisoformat(ts_str)] = float(
```

**Violation:** Using `float` to store financial time series data.

**Proposed Fix:**
```python
processed_data: dict[str, dict[datetime, Decimal]] = {}
# ...
# Convert to Decimal for precision
processed_data[strategy][datetime.fromisoformat(ts_str)] = Decimal(str(
```

### 12. `cyberdelta/visualization/simplified_visualizer.py`

**File Path:** `cyberdelta/visualization/simplified_visualizer.py` - Multiple locations
**Problematic Code Snippet:**
```python
# Convert Decimal values to float for plotting
trades_df["pnl"] = trades_df["pnl"].astype(float)
# ...
# Convert values to float arrays before multiplication to avoid type issues
drawdown_values = np.array(drawdown.values, dtype=float) * 100
```

**Violation:** Converting `Decimal` financial values to `float` for visualization.

**Special Case:** Visualization might be a valid exception since plotting libraries typically require `float`, but we should be mindful of when the conversion happens.

**Proposed Fix:**
```python
# Convert Decimal values to float only at the last moment for plotting
# (Keep conversion close to the plotting function call)
# Maintain intermediate calculations in Decimal
```

## Summary and Recommendations

This audit identified several critical areas where `float` is being used instead of `Decimal` for financial calculations in the CyberDeltaEngine codebase:

1. **Portfolio tracking functions:** Particularly `get_exchange_exposure()` in portfolio_tracker.py
2. **Risk management:** Validation metrics and calculations in risk_manager.py
3. **Financial model parameters:** Initial capital, commission, slippage in backtesting.py
4. **Performance metrics:** Sharpe ratios and other financial metrics in performance_metrics.py
5. **Data persistence:** Time series financial data in persistence.py
6. **Reporting and serialization:** Converting to float for reporting in various modules

**Recommendations:**

1. Update all function signatures to use `Decimal` instead of `float` for financial values
2. Replace all float literals with `Decimal` literals (using string format)
3. Update type hints to use `Decimal` for all financial quantities
4. Ensure conversions to `Decimal` use the string constructor `Decimal(str(value))` to avoid precision loss
5. For complex calculations with numpy, perform calculations in Decimal where possible, or minimize float usage by converting back to Decimal as soon as possible
6. Be cautious about the edge case of visualization libraries requiring float values - delay conversion to the latest possible point

After applying these fixes, run mypy with strict type checking to verify the changes have addressed the float/Decimal type errors. Additionally, run the test suite to ensure no logical regressions were introduced.

The goal is to maintain consistent and accurate financial calculations throughout the codebase, from input validation through computation to final reporting. 