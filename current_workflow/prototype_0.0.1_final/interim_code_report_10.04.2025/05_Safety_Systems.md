# Code Report: CyberDeltaEngine - Safety Systems

## 1. Overview

A critical aspect of the CyberDeltaEngine is its multi-layered safety framework, designed to prevent catastrophic failures, limit losses, and ensure data integrity. This framework consists of three main components implemented during Phase 3:

1.  **Funding Rate Validation**: Ensures the accuracy of predicted funding rates.
2.  **Position Reconciliation**: Verifies consistency between the system's view and the exchange's view of positions.
3.  **Circuit Breaker System**: Halts operations under potentially dangerous conditions.

## 2. Funding Rate Validation (`cyberdelta/validation/funding_rate_validator.py`)

**Purpose**: To track and validate the accuracy of predicted funding rates against actual funding payments received from exchanges.

**Key Features**:
- Records predictions from various sources (API, models) with confidence scores.
- Records actual payments received.
- Matches predictions to payments based on timestamps.
- Calculates accuracy metrics: Root Mean Square Error (RMSE), Mean Absolute Error (MAE), and Bias.
- Provides reporting capabilities for analysis.
- Uses an in-memory approach for simplicity and speed.

**Code Snippet (`FundingRateValidator.calculate_metrics`)**:
```python
    def calculate_metrics(self, exchange: Optional[str] = None, symbol: Optional[str] = None, days: int = 7) -> Dict[str, float]:
        """
        Calculate accuracy metrics for funding rate predictions.

        Args:
            exchange: Optional exchange filter.
            symbol: Optional symbol filter.
            days: Number of past days to consider.

        Returns:
            Dictionary containing RMSE, MAE, and Bias.
        """
        cutoff_time = datetime.now() - timedelta(days=days)
        errors = []
        matched_predictions = 0

        # Filter payments first
        relevant_payments = [
            p for p in self.payments
            if p['datetime'] >= cutoff_time and
               (exchange is None or p['exchange'] == exchange) and
               (symbol is None or p['symbol'] == symbol)
        ]

        # Filter predictions
        relevant_predictions = [
            pred for pred in self.predictions
            if pred['datetime'] >= cutoff_time and
               (exchange is None or pred['exchange'] == exchange) and
               (symbol is None or pred['symbol'] == symbol)
        ]

        # Match predictions to payments (simple nearest-prior prediction logic)
        for payment in relevant_payments:
            best_prediction = None
            min_time_diff = float('inf')

            for pred in relevant_predictions:
                # Find the latest prediction *before* the payment
                time_diff = payment['timestamp'] - pred['timestamp']
                if 0 <= time_diff < min_time_diff and \
                   pred['exchange'] == payment['exchange'] and \
                   pred['symbol'] == payment['symbol']:
                    min_time_diff = time_diff
                    best_prediction = pred

            if best_prediction:
                error = best_prediction['predicted_rate'] - payment['actual_rate']
                errors.append(error)
                matched_predictions += 1

        if not errors:
            return {'rmse': 0.0, 'mae': 0.0, 'bias': 0.0, 'matched_predictions': 0}

        squared_errors = [e**2 for e in errors]
        rmse = (sum(squared_errors) / len(errors)) ** 0.5
        mae = sum(abs(e) for e in errors) / len(errors)
        bias = sum(errors) / len(errors)

        return {
            'rmse': rmse,
            'mae': mae,
            'bias': bias,
            'matched_predictions': matched_predictions,
            'total_payments': len(relevant_payments)
         }
```

## 3. Position Reconciliation System (`cyberdelta/validation/position_reconciliation.py`)

**Purpose**: To ensure the trading system's internal view of positions aligns with the actual positions held on the exchanges.

**Key Features**:
- **Triple Source Verification**: Compares positions derived from:
    1.  Exchange API (Source of Truth)
    2.  Local Fill History
    3.  Portfolio Tracker's Internal State
- **Configurable Thresholds**: Allows setting tolerances for acceptable discrepancies (e.g., 1% difference).
- **Discrepancy Reporting**: Logs detailed information about any detected inconsistencies.
- **Scheduled Checks**: Runs periodically based on configuration.
- **Optional Auto-Correction**: Can be configured to automatically update the `PortfolioTracker` state to match the exchange API if discrepancies exceed thresholds.

**Code Snippet (Conceptual `_compare_positions`)**:
```python
    async def _compare_positions(self) -> List[Dict[str, Any]]:
        """Compare positions from the three sources."""
        discrepancies = []
        try:
            # 1. Fetch positions from all sources concurrently
            api_positions, fill_positions, local_positions = await asyncio.gather(
                self._fetch_api_positions(),
                self._fetch_fill_derived_positions(),
                self._fetch_local_positions()
            )

            # 2. Combine all unique exchange/symbol pairs
            all_keys = set(api_positions.keys()) | set(fill_positions.keys()) | set(local_positions.keys())

            # 3. Iterate and compare
            for key in all_keys: # key is typically (exchange, symbol)
                api_pos = api_positions.get(key)
                fill_pos = fill_positions.get(key)
                local_pos = local_positions.get(key)

                # Compare api_pos.size vs fill_pos.size vs local_pos.size
                # Calculate absolute and relative differences
                # Example check:
                if api_pos and local_pos:
                    diff = abs(api_pos.size - local_pos.size)
                    relative_diff = diff / abs(api_pos.size) if api_pos.size else 0

                    if relative_diff > self.threshold:
                        discrepancy_details = {
                            "key": key,
                            "source_api_size": api_pos.size,
                            "source_local_size": local_pos.size,
                            "difference": diff,
                            "relative_difference": relative_diff,
                            "timestamp": datetime.now()
                        }
                        discrepancies.append(discrepancy_details)
                        logger.warning(f"Position discrepancy detected: {discrepancy_details}")
                # ... add more comprehensive checks for all source combinations ...

            # 4. Handle Auto-Correction if enabled
            if self.auto_correct and discrepancies:
                await self._apply_corrections(discrepancies, api_positions)

        except Exception as e:
            logger.error(f"Error during position reconciliation: {e}", exc_info=True)

        return discrepancies
```

## 4. Circuit Breaker System (`cyberdelta/validation/circuit_breaker.py`)

**Purpose**: To automatically halt specific or all trading operations when potentially dangerous conditions are detected, preventing rapid losses or system instability.

**Key Features**:
- **State Pattern**: Implements CLOSED, OPEN (tripped), and HALF-OPEN (recovery testing) states.
- **Multiple Breaker Types**: Specialized breakers monitor different conditions:
    - `VolatilityBreaker`: High market price volatility.
    - `DrawdownBreaker`: Significant portfolio value loss.
    - `APIErrorBreaker`: Excessive API error rates.
    - `LiquidityBreaker`: Insufficient market liquidity.
- **Hierarchical Structure**: Breakers can be applied globally, per-exchange, or even per-symbol.
- **Configurable Thresholds**: Sensitivity can be tuned via `config.yaml`.
- **Cooldown & Recovery**: Automatic transition to HALF-OPEN after a cooldown period to test if conditions have normalized.
- **Central Management**: `CircuitBreakerSystem` coordinates all defined breakers.

**Code Snippet (`CircuitBreakerSystem.check_and_record`)**:
```python
    async def check_and_record(
        self,
        breaker_type: str,
        value: float,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None
    ):
        """
        Record a new value for a specific breaker type and check if it should trip.

        Args:
            breaker_type: The type of breaker (e.g., 'volatility', 'api_errors').
            value: The value to record (e.g., current volatility, number of recent errors).
            exchange: Optional exchange context.
            symbol: Optional symbol context.
        """
        # Find the relevant breaker(s) based on hierarchy (global, exchange, symbol)
        breakers_to_check = self._find_relevant_breakers(breaker_type, exchange, symbol)

        for breaker in breakers_to_check:
            if breaker.state == CircuitBreakerState.CLOSED:
                if breaker.check(value): # Check if value exceeds threshold
                    breaker.trip()
                    logger.critical(
                        f"Circuit breaker '{breaker.name}' tripped! Reason: {breaker.last_trip_reason}"
                    )
                    # Potentially trigger alerts
            elif breaker.state == CircuitBreakerState.OPEN:
                # Check if cooldown expired to transition to HALF_OPEN
                if datetime.now() >= breaker.recovery_timeout:
                    breaker.attempt_recovery()
                    logger.warning(f"Circuit breaker '{breaker.name}' attempting recovery (HALF_OPEN).")
            # HALF_OPEN state is handled when an operation is attempted

    def is_operation_allowed(
        self,
        operation_type: str, # e.g., 'place_order', 'fetch_data'
        exchange: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> bool:
        """
        Check if a specific operation is allowed based on relevant circuit breaker states.
        """
        # Find relevant breakers for the context (e.g., APIErrorBreaker for place_order)
        relevant_breakers = self._find_relevant_breakers_for_operation(operation_type, exchange, symbol)

        for breaker in relevant_breakers:
            if breaker.state == CircuitBreakerState.OPEN:
                logger.warning(f"Operation blocked by OPEN circuit breaker: {breaker.name}")
                return False
            elif breaker.state == CircuitBreakerState.HALF_OPEN:
                # Allow one test operation, record success/failure to close or re-trip
                logger.info(f"Operation allowed under HALF_OPEN circuit breaker: {breaker.name}. Testing recovery.")
                # Logic to track the outcome of this single operation is needed here
                # For simplicity now, assume test passes if operation is attempted.
                # Real implementation needs feedback mechanism.
                breaker.record_half_open_success() # Or failure based on operation outcome
                return True # Allow the test operation

        return True # Allowed if all relevant breakers are CLOSED
```

These safety systems provide essential layers of protection, increasing the robustness and reliability of the trading engine. 