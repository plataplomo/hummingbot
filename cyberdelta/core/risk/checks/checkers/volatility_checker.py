"""Volatility checker implementation with direct configuration access."""

import statistics
from decimal import Decimal
from typing import Any, Final

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.core.risk.exceptions.check_exceptions import VolatilityError
from cyberdelta.core.symbols import Symbol
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Volatility analysis constants
MIN_HISTORICAL_SAMPLES_FOR_VOLATILITY = 2  # Minimum samples needed for volatility calculation
MIN_HISTORICAL_SAMPLES_FOR_STABILITY = 3  # Minimum samples needed for stability analysis
MIN_SAMPLES_FOR_TREND_ANALYSIS = 6  # Minimum samples for trend analysis
TREND_ANALYSIS_WINDOW = 3  # Window size for trend analysis (hours)
VOLATILITY_TREND_THRESHOLD = 2.0  # Threshold for detecting concerning volatility trends
MIN_SAMPLES_FOR_REGIME_DETECTION = 10  # Minimum samples needed for regime detection
MIN_SAMPLES_FOR_STABILITY_SCORE = 5  # Minimum samples needed for stability scoring

# Volatility scoring constants
NEUTRAL_VOLATILITY_SCORE = 0.5  # Neutral score when volatility data is unavailable
OPTIMAL_VOLATILITY_MIN_MULTIPLIER = 2  # Multiplier for optimal minimum volatility
OPTIMAL_VOLATILITY_MAX_DIVISOR = 2  # Divisor for optimal maximum volatility
PERFECT_VOLATILITY_SCORE = 1.0  # Score for optimal volatility range
STABILITY_SCORE_PERFECT = 1.0  # Perfect stability score
MIN_STABILITY_MULTIPLIER = 0.5  # Minimum stability multiplier
MIN_VOLATILITY_SCORE = 0.0  # Minimum possible volatility score
MAX_VOLATILITY_SCORE = 1.0  # Maximum possible volatility score


class VolatilityChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates basis volatility and market stability."""

    CHECKER_NAME: Final[str] = "volatility"

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the volatility checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
        """
        super().__init__(app_settings, self.CHECKER_NAME)

        # Cache frequently accessed values for performance
        self._max_volatility = self.thresholds.max_volatility
        self._min_volatility = self.thresholds.min_volatility
        self._lookback_hours = self.checker_settings.volatility_lookback_hours

        # Additional thresholds not in the new config model (using defaults)
        self.max_basis_volatility = Decimal("0.1")  # 10% max basis volatility

        # Stability checks
        self.enable_stability_check = True
        self.max_volatility_spike = Decimal("5.0")  # 5x normal volatility spike threshold

        # Rolling window analysis
        self.enable_rolling_analysis = True
        self.rolling_window_hours = 6
        self.max_rolling_volatility = Decimal("0.15")  # 15% max rolling volatility

        # Market regime detection
        self.enable_regime_detection = True
        self.high_volatility_regime_threshold = Decimal("0.1")  # 10% high volatility regime

        # Historical data tracking
        self.volatility_history: dict[
            str, list[Decimal]
        ] = {}  # symbol.value -> list of recent volatility
        self.max_history_size = 168  # 1 week of hourly data

        # Correlation checks
        self.enable_correlation_check = True
        self.min_correlation_threshold = Decimal("0.5")  # 50% minimum correlation threshold

    @property
    def name(self) -> str:
        """Name of the checker."""
        return "volatility"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check volatility and basis stability.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        details: dict[str, Any] = {}

        # Get and validate opportunity data
        symbol, spread_decimal = self._extract_opportunity_data(opportunity, details)
        if isinstance(spread_decimal, CheckResult):
            return spread_decimal  # Return early failure

        # Get volatility data
        historical_volatility, current_volatility = self._get_volatility_data(opportunity, details)

        # Perform volatility checks
        check_result = self._perform_volatility_checks(
            symbol, spread_decimal, current_volatility, historical_volatility, details
        )
        if check_result:
            return check_result

        # Market regime and history updates
        self._handle_regime_and_history(symbol, current_volatility, historical_volatility, details)

        # Calculate volatility score
        volatility_score = self._calculate_volatility_score(
            current_volatility, historical_volatility
        )
        details["volatility_score"] = volatility_score

        return CheckResult.success(
            message=(
                f"Volatility check passed: {current_volatility:.4%}"
                if current_volatility
                else "Volatility check passed"
            ),
            details=details,
        )

    def _extract_opportunity_data(
        self, opportunity: ArbitrageOpportunity, details: dict[str, Any]
    ) -> tuple[Symbol, Decimal] | tuple[Symbol, CheckResult]:
        """Extract and validate opportunity data.

        Args:
            opportunity: The arbitrage opportunity to process
            details: Dictionary to update with extracted details

        Returns:
            Tuple of (symbol, spread_decimal) on success, or (symbol, CheckResult) on failure
        """
        symbol = opportunity.symbol
        spread_percentage = getattr(opportunity, "spread_percentage", None)

        if spread_percentage is None:
            return symbol, CheckResult.failure(
                message="Cannot check volatility: missing spread_percentage",
                details={"symbol": symbol.value if hasattr(symbol, "value") else str(symbol)},
            )

        # Convert spread to Decimal
        try:
            spread_decimal = self._to_decimal(spread_percentage)
        except (ValueError, TypeError) as e:
            return symbol, CheckResult.failure(
                message=f"Invalid spread_percentage format: {e!s}",
                details={"spread_percentage": str(spread_percentage)},
            )

        details.update({
            "symbol": symbol.value if hasattr(symbol, "value") else str(symbol),
            "current_spread": float(spread_decimal),
        })

        return symbol, spread_decimal

    def _get_volatility_data(
        self, opportunity: ArbitrageOpportunity, details: dict[str, Any]
    ) -> tuple[list[Decimal] | None, Decimal | None]:
        """Get historical and current volatility data.

        Args:
            opportunity: The arbitrage opportunity
            details: Dictionary to update with volatility details

        Returns:
            Tuple of (historical_volatility, current_volatility)
        """
        historical_volatility = self._get_historical_volatility(opportunity)
        current_volatility = self._calculate_current_volatility(opportunity, historical_volatility)

        details.update({
            "current_volatility": float(current_volatility) if current_volatility else None,
            "historical_data_points": len(historical_volatility) if historical_volatility else 0,
        })

        return historical_volatility, current_volatility

    def _perform_volatility_checks(
        self,
        symbol: Symbol,
        spread_decimal: Decimal,
        current_volatility: Decimal | None,
        historical_volatility: list[Decimal] | None,
        details: dict[str, Any],
    ) -> CheckResult | None:
        """Perform all volatility checks, return CheckResult if any fail.

        Args:
            symbol: Trading symbol
            spread_decimal: Current spread as Decimal
            current_volatility: Current volatility if available
            historical_volatility: Historical volatility data
            details: Dictionary to update with check details

        Returns:
            CheckResult if any check fails, None if all pass
        """
        # Check volatility bounds
        bounds_result = self._check_volatility_bounds(current_volatility, spread_decimal)
        if not bounds_result.passed:
            details.update(bounds_result.details or {})
            return CheckResult.failure(
                message=f"Volatility bounds check failed: {bounds_result.message}",
                details=details,
            )

        # Check basis volatility
        basis_result = self._check_basis_volatility(spread_decimal, historical_volatility)
        if not basis_result.passed:
            details.update(basis_result.details or {})
            details["basis_failure_reason"] = basis_result.message
            return CheckResult.failure(
                message="Basis volatility check failed",
                details=details,
            )

        # Check volatility stability
        if self.enable_stability_check and current_volatility:
            stability_result = self._check_volatility_stability(
                symbol, current_volatility, historical_volatility
            )
            if not stability_result.passed:
                details.update(stability_result.details or {})
                details["stability_failure_reason"] = stability_result.message
                return CheckResult.failure(
                    message="Volatility stability check failed",
                    details=details,
                )

        # Check rolling volatility
        if self.enable_rolling_analysis and historical_volatility:
            rolling_result = self._check_rolling_volatility(historical_volatility)
            if not rolling_result.passed:
                details.update(rolling_result.details or {})
                return CheckResult.failure(
                    message=f"Rolling volatility check failed: {rolling_result.message}",
                    details=details,
                )

        return None

    def _handle_regime_and_history(
        self,
        symbol: Symbol,
        current_volatility: Decimal | None,
        historical_volatility: list[Decimal] | None,
        details: dict[str, Any],
    ) -> None:
        """Handle market regime detection and volatility history updates."""
        # Market regime detection
        if self.enable_regime_detection and current_volatility:
            regime = self._detect_market_regime(current_volatility, historical_volatility)
            details["market_regime"] = regime

        # Update volatility history
        if current_volatility:
            self._update_volatility_history(symbol, current_volatility)

    def _to_decimal(self, value: Decimal | str | float) -> Decimal:
        """Convert value to Decimal with validation.

        Args:
            value: Value to convert to Decimal

        Returns:
            Decimal representation of the value

        Raises:
            VolatilityError: If value type cannot be converted
        """
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            return Decimal(value)
        if isinstance(value, float):
            return Decimal(str(value))
        # This should never be reached due to type constraints
        raise VolatilityError(
            VolatilityError.INVALID_TYPE_CONVERSION,
            metadata={"value_type": str(type(value))},
            checker_name="VolatilityChecker",
            check_type="volatility_conversion",
        )

    def _get_historical_volatility(self, opportunity: ArbitrageOpportunity) -> list[Decimal] | None:
        """Get historical volatility data for the opportunity.

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            List of historical volatility values or None if not available
        """
        symbol = getattr(opportunity, "symbol", None)
        if not symbol:
            return None

        # Check if we have historical data
        return self.volatility_history.get(symbol.value, [])

    def _calculate_current_volatility(
        self, opportunity: ArbitrageOpportunity, historical_data: list[Decimal] | None
    ) -> Decimal | None:
        """Calculate current volatility estimate.

        Args:
            opportunity: The arbitrage opportunity
            historical_data: Historical volatility data if available

        Returns:
            Current volatility estimate or None if not calculable
        """
        # Try to get volatility from opportunity data
        if opportunity.volatility is not None:
            return opportunity.volatility  # Already Decimal type from Pydantic validation

        # Try to get price data to calculate volatility
        # These fields are guaranteed to exist by Pydantic model
        long_price = opportunity.long_price
        short_price = opportunity.short_price

        if long_price and short_price:
            # Fields are already Decimal types from Pydantic validation
            # Simple volatility estimate based on price spread
            avg_price = (long_price + short_price) / 2
            price_spread = abs(long_price - short_price)

            if avg_price > 0:
                return price_spread / avg_price

        # Fallback to historical average if available
        if historical_data and len(historical_data) > 0:
            try:
                total = sum(historical_data, Decimal(0))
                count = Decimal(str(len(historical_data)))
                return total / count
            except (ValueError, TypeError):
                pass

        return None

    def _check_volatility_bounds(
        self, current_volatility: Decimal | None, spread: Decimal
    ) -> CheckResult:
        """Check if volatility is within acceptable bounds.

        Args:
            current_volatility: Current volatility or None
            spread: Current spread to use as proxy if volatility is None

        Returns:
            CheckResult indicating success or failure with details
        """
        # Use spread as proxy for volatility if current volatility is None
        volatility_proxy = spread if current_volatility is None else current_volatility

        if volatility_proxy < self._min_volatility:
            return CheckResult.failure(
                message=(
                    f"Volatility {volatility_proxy:.4%} below minimum threshold "
                    f"{self._min_volatility:.4%}"
                ),
                details={
                    "volatility": float(volatility_proxy),
                    "min_threshold": float(self._min_volatility),
                },
            )

        if volatility_proxy > self._max_volatility:
            return CheckResult.failure(
                message=(
                    f"Volatility {volatility_proxy:.4%} above maximum threshold "
                    f"{self._max_volatility:.4%}"
                ),
                details={
                    "volatility": float(volatility_proxy),
                    "max_threshold": float(self._max_volatility),
                },
            )

        return CheckResult.success("Volatility bounds check passed")

    def _check_basis_volatility(
        self, spread: Decimal, historical_data: list[Decimal] | None
    ) -> CheckResult:
        """Check basis volatility using spread data.

        Args:
            spread: Current spread
            historical_data: Historical volatility data if available

        Returns:
            CheckResult indicating success or failure with details
        """
        if spread > self.max_basis_volatility:
            return CheckResult.failure(
                message=(
                    f"Basis volatility {spread:.4%} exceeds maximum {self.max_basis_volatility:.4%}"
                ),
                details={
                    "basis_volatility": float(spread),
                    "max_basis_volatility": float(self.max_basis_volatility),
                },
            )

        # Check historical basis volatility if available
        if historical_data and len(historical_data) > MIN_HISTORICAL_SAMPLES_FOR_VOLATILITY:
            try:
                volatility_values = [float(v) for v in historical_data[-self._lookback_hours :]]
                historical_std = statistics.stdev(volatility_values)

                if historical_std > float(self.max_basis_volatility):
                    return CheckResult.failure(
                        message=(
                            f"Historical basis volatility {historical_std:.4%} exceeds maximum "
                            f"{self.max_basis_volatility:.4%}"
                        ),
                        details={
                            "historical_volatility": historical_std,
                            "max_basis_volatility": float(self.max_basis_volatility),
                            "sample_size": len(volatility_values),
                        },
                    )
            except (ValueError, statistics.StatisticsError):
                # Handle edge cases in statistics calculation
                pass

        return CheckResult.success("Basis volatility check passed")

    def _check_volatility_stability(
        self, symbol: Symbol, current_volatility: Decimal, historical_data: list[Decimal] | None
    ) -> CheckResult:
        """Check volatility stability and detect spikes.

        Args:
            symbol: Trading symbol
            current_volatility: Current volatility value
            historical_data: Historical volatility data

        Returns:
            CheckResult indicating success or failure with details
        """
        if not historical_data or len(historical_data) < MIN_HISTORICAL_SAMPLES_FOR_STABILITY:
            return CheckResult.success("Insufficient historical data for stability check")

        try:
            # Calculate historical average
            recent_history = historical_data[-min(24, len(historical_data)) :]  # Last 24 hours
            total = sum(recent_history, Decimal(0))
            count = Decimal(str(len(recent_history)))
            historical_avg = total / count

            # Check for volatility spike
            if historical_avg > 0:
                volatility_ratio = current_volatility / historical_avg

                if volatility_ratio > self.max_volatility_spike:
                    return CheckResult.failure(
                        message=(
                            f"Volatility spike detected: {volatility_ratio:.2f}x above "
                            "historical average"
                        ),
                        details={
                            "current_volatility": float(current_volatility),
                            "historical_average": float(historical_avg),
                            "volatility_ratio": float(volatility_ratio),
                            "max_spike_ratio": float(self.max_volatility_spike),
                        },
                    )

            # Check for volatility trend
            if len(recent_history) >= MIN_SAMPLES_FOR_TREND_ANALYSIS:
                recent_trend = recent_history[-TREND_ANALYSIS_WINDOW:]  # Last 3 hours
                # 3 hours before that
                earlier_trend = recent_history[
                    -MIN_SAMPLES_FOR_TREND_ANALYSIS:-TREND_ANALYSIS_WINDOW
                ]

                if (
                    len(recent_trend) == TREND_ANALYSIS_WINDOW
                    and len(earlier_trend) == TREND_ANALYSIS_WINDOW
                ):
                    recent_total = sum(recent_trend, Decimal(0))
                    recent_count = Decimal(str(len(recent_trend)))
                    recent_avg = recent_total / recent_count

                    earlier_total = sum(earlier_trend, Decimal(0))
                    earlier_count = Decimal(str(len(earlier_trend)))
                    earlier_avg = earlier_total / earlier_count

                    if earlier_avg > 0:
                        trend_ratio = recent_avg / earlier_avg

                        # Volatility doubled in recent hours
                        if trend_ratio > VOLATILITY_TREND_THRESHOLD:
                            return CheckResult.failure(
                                message=(
                                    f"Increasing volatility trend detected: "
                                    f"{trend_ratio:.2f}x increase"
                                ),
                                details={
                                    "recent_avg": float(recent_avg),
                                    "earlier_avg": float(earlier_avg),
                                    "trend_ratio": float(trend_ratio),
                                },
                            )

        except (ValueError, TypeError):
            # Handle edge cases in calculations
            pass

        return CheckResult.success("Volatility stability check passed")

    def _check_rolling_volatility(self, historical_data: list[Decimal]) -> CheckResult:
        """Check rolling window volatility.

        Args:
            historical_data: Historical volatility data

        Returns:
            CheckResult indicating success or failure with details
        """
        if len(historical_data) < self.rolling_window_hours:
            return CheckResult.success("Insufficient data for rolling volatility check")

        try:
            # Calculate rolling window volatility
            rolling_window = historical_data[-self.rolling_window_hours :]
            rolling_volatility_values = [float(v) for v in rolling_window]

            if len(rolling_volatility_values) > 1:
                rolling_std = statistics.stdev(rolling_volatility_values)
                rolling_avg = statistics.mean(rolling_volatility_values)

                if rolling_std > float(self.max_rolling_volatility):
                    return CheckResult.failure(
                        message=(
                            f"Rolling volatility {rolling_std:.4%} exceeds maximum "
                            f"{self.max_rolling_volatility:.4%}"
                        ),
                        details={
                            "rolling_volatility": rolling_std,
                            "rolling_average": rolling_avg,
                            "max_rolling_volatility": float(self.max_rolling_volatility),
                            "window_size": len(rolling_volatility_values),
                        },
                    )

        except (ValueError, statistics.StatisticsError):
            # Handle edge cases in statistics calculation
            pass

        return CheckResult.success("Rolling volatility check passed")

    def _detect_market_regime(
        self, current_volatility: Decimal, historical_data: list[Decimal] | None
    ) -> str:
        """Detect current market regime based on volatility.

        Args:
            current_volatility: Current volatility value
            historical_data: Historical volatility data if available

        Returns:
            Market regime string: 'high_volatility' or 'normal'
        """
        if current_volatility > self.high_volatility_regime_threshold:
            return "high_volatility"

        if historical_data and len(historical_data) > MIN_SAMPLES_FOR_REGIME_DETECTION:
            try:
                recent_samples = historical_data[-MIN_SAMPLES_FOR_REGIME_DETECTION:]
                recent_avg = sum(recent_samples) / MIN_SAMPLES_FOR_REGIME_DETECTION
                if recent_avg > self.high_volatility_regime_threshold:
                    return "high_volatility"
            except (ValueError, TypeError):
                pass

        return "normal"

    def _calculate_volatility_score(
        self, current_volatility: Decimal | None, historical_data: list[Decimal] | None
    ) -> float:
        """Calculate a volatility health score (0-1, higher is better).

        Args:
            current_volatility: Current volatility or None
            historical_data: Historical volatility data if available

        Returns:
            Volatility health score between 0 and 1
        """
        if current_volatility is None:
            return NEUTRAL_VOLATILITY_SCORE

        # Base score on how close to optimal volatility range
        optimal_min = self._min_volatility * OPTIMAL_VOLATILITY_MIN_MULTIPLIER
        optimal_max = self._max_volatility / OPTIMAL_VOLATILITY_MAX_DIVISOR

        if optimal_min <= current_volatility <= optimal_max:
            base_score = PERFECT_VOLATILITY_SCORE
        elif current_volatility < optimal_min:
            base_score = float(current_volatility / optimal_min)
        else:
            base_score = float(optimal_max / current_volatility)

        # Adjust based on stability
        if historical_data and len(historical_data) > MIN_SAMPLES_FOR_STABILITY_SCORE:
            try:
                recent_volatility = [
                    float(v) for v in historical_data[-MIN_SAMPLES_FOR_STABILITY_SCORE:]
                ]
                volatility_std = statistics.stdev(recent_volatility)
                volatility_mean = statistics.mean(recent_volatility)
                stability_score = STABILITY_SCORE_PERFECT - (volatility_std / volatility_mean)
                base_score *= max(MIN_STABILITY_MULTIPLIER, stability_score)
            except (ValueError, statistics.StatisticsError):
                pass

        return max(MIN_VOLATILITY_SCORE, min(MAX_VOLATILITY_SCORE, base_score))

    def _update_volatility_history(self, symbol: Symbol, volatility: Decimal) -> None:
        """Update volatility history for the symbol."""
        symbol_key = symbol.value
        if symbol_key not in self.volatility_history:
            self.volatility_history[symbol_key] = []

        self.volatility_history[symbol_key].append(volatility)

        # Maintain history size limit
        if len(self.volatility_history[symbol_key]) > self.max_history_size:
            self.volatility_history[symbol_key] = self.volatility_history[symbol_key][
                -self.max_history_size :
            ]

    def clear_volatility_history(self, symbol: Symbol | None = None) -> None:
        """Clear volatility history.

        Args:
            symbol: Symbol to clear history for, or None to clear all
        """
        if symbol:
            self.volatility_history.pop(symbol.value, None)
            self.logger.info("Cleared volatility history", symbol=symbol.value)
        else:
            self.volatility_history.clear()
            self.logger.info("Cleared all volatility history")

    def get_volatility_history(self, symbol: Symbol) -> list[Decimal]:
        """Get volatility history for a symbol.

        Args:
            symbol: Symbol to get history for

        Returns:
            List of historical volatility values
        """
        return self.volatility_history.get(symbol.value, []).copy()

    def set_volatility_thresholds(self, min_threshold: Decimal, max_threshold: Decimal) -> None:
        """Set volatility thresholds.

        Args:
            min_threshold: Minimum volatility threshold
            max_threshold: Maximum volatility threshold

        Raises:
            VolatilityError: If min_threshold is not less than max_threshold
        """
        if min_threshold >= max_threshold:
            msg = "min_threshold must be less than max_threshold"
            raise VolatilityError(
                msg,
                metadata={
                    "min_threshold": float(min_threshold),
                    "max_threshold": float(max_threshold),
                },
                checker_name="VolatilityChecker",
                check_type="volatility",
            )

        self._min_volatility = min_threshold
        self._max_volatility = max_threshold
        self.logger.info(
            "Set volatility thresholds",
            min_threshold=f"{min_threshold:.4%}",
            max_threshold=f"{max_threshold:.4%}",
        )

    def set_basis_volatility_threshold(self, max_basis_volatility: Decimal) -> None:
        """Set maximum basis volatility threshold.

        Args:
            max_basis_volatility: Maximum allowed basis volatility
        """
        self.max_basis_volatility = max_basis_volatility
        self.logger.info(
            "Set basis volatility threshold", max_basis_volatility=f"{max_basis_volatility:.4%}"
        )

    def _create_skip_result(self) -> CheckResult:
        """Create result for skipped check.

        Returns:
            CheckResult with skip status
        """
        return CheckResult.skip(
            message=f"{self.CHECKER_NAME} check skipped (disabled)",
        )

    def _create_error_result(self, error: Exception, execution_time: float) -> CheckResult:
        """Create result for failed check.

        Args:
            error: The exception that occurred
            execution_time: Time taken for the check in milliseconds

        Returns:
            CheckResult with error status and details
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
