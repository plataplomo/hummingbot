"""Funding rate checker implementation with direct configuration access."""

import statistics
from decimal import Decimal
from typing import Any, Final, Protocol

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.core.risk.exceptions.check_exceptions import FundingRateError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Funding rate stability analysis constants
MIN_HISTORICAL_RATES_FOR_STABILITY = 3  # Minimum data points for statistical stability analysis
MIN_RATES_FOR_CONSECUTIVE_ANALYSIS = 2  # Need at least 2 rates to calculate consecutive moves


# Protocol for funding rate validator
class FundingRateValidatorProtocol(Protocol):
    """Protocol for funding rate validator."""

    def get_symbol_metrics(self, exchange: str, symbol: str) -> dict[str, Any]:
        """Get symbol metrics."""
        ...


class FundingRateChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates funding rate stability and patterns."""

    CHECKER_NAME: Final[str] = "funding_rate"

    def __init__(
        self,
        app_settings: AppSettings,
        funding_rate_validator: FundingRateValidatorProtocol,
    ) -> None:
        """Initialize the funding rate checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
            funding_rate_validator: Funding rate validator protocol
        """
        super().__init__(
            app_settings, self.CHECKER_NAME, funding_rate_validator=funding_rate_validator
        )
        self.funding_rate_validator = funding_rate_validator

        # Cache frequently accessed values for performance
        self._max_funding_rate_spread = self.thresholds.max_funding_rate_spread
        self._max_funding_rate_volatility = self.thresholds.max_funding_rate_volatility
        self._min_funding_rate = self.thresholds.min_funding_rate
        self._max_funding_rate = self.thresholds.max_funding_rate

        # Stability checks
        self.enable_stability_check = self.checker_settings.enable_funding_rate_stability_check
        self.stability_lookback_hours = self.checker_settings.funding_rate_lookback_hours
        # Additional thresholds not in the new config model (using defaults)
        self.max_stability_coefficient = Decimal("0.5")  # 50% stability coefficient

        # Pattern checks (hardcoded as not in new config)
        self.enable_pattern_check = True
        self.max_consecutive_same_direction = 5

        # Confidence thresholds
        self._min_confidence_score = self.thresholds.min_funding_rate_confidence
        self.require_primary_source = self.checker_settings.require_primary_funding_source

    @property
    def name(self) -> str:
        """Name of the checker."""
        return "funding_rate"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check funding rate stability and patterns.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        details: dict[str, Any] = {}

        # Get exchange and symbol information
        long_exchange = getattr(opportunity, "long_exchange", None)
        short_exchange = getattr(opportunity, "short_exchange", None)
        symbol = getattr(opportunity, "symbol", None)

        if not long_exchange or not short_exchange or not symbol:
            return CheckResult.failure(
                message="Cannot check funding rate: missing exchange or symbol information",
                details={
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "symbol": symbol,
                },
            )

        # Get funding rate metrics for both exchanges
        try:
            long_metrics = self.funding_rate_validator.get_symbol_metrics(long_exchange, symbol)
            short_metrics = self.funding_rate_validator.get_symbol_metrics(short_exchange, symbol)
        except (ValueError, TypeError, AttributeError, KeyError, ConnectionError) as e:
            return CheckResult.error(
                message=f"Failed to get funding rate metrics: {e!s}",
                details={"exception": str(e)},
            )

        if not long_metrics or not short_metrics:
            return CheckResult.failure(
                message="Funding rate metrics not available",
                details={
                    "long_metrics_available": bool(long_metrics),
                    "short_metrics_available": bool(short_metrics),
                },
            )

        # Check funding rate bounds
        bounds_result = self._check_funding_rate_bounds(long_metrics, short_metrics)
        if not bounds_result.passed:
            details.update(bounds_result.details or {})
            return CheckResult.failure(
                message=f"Funding rate bounds check failed: {bounds_result.message}",
                details=details,
            )

        # Check funding rate spread
        spread_result = self._check_funding_rate_spread(long_metrics, short_metrics)
        if not spread_result.passed:
            details.update(spread_result.details or {})
            return CheckResult.failure(
                message=f"Funding rate spread check failed: {spread_result.message}",
                details=details,
            )

        # Check funding rate stability
        if self.enable_stability_check:
            stability_result = self._check_funding_rate_stability(long_metrics, short_metrics)
            if not stability_result.passed:
                details.update(stability_result.details or {})
                return CheckResult.failure(
                    message=f"Funding rate stability check failed: {stability_result.message}",
                    details=details,
                )

        # Check confidence scores
        confidence_result = self._check_confidence_scores(long_metrics, short_metrics)
        if not confidence_result.passed:
            details.update(confidence_result.details or {})
            return CheckResult.failure(
                message=f"Funding rate confidence check failed: {confidence_result.message}",
                details=details,
            )

        # Calculate overall funding rate health score
        long_rate = self._get_current_funding_rate(long_metrics)
        short_rate = self._get_current_funding_rate(short_metrics)
        spread = abs(long_rate - short_rate) if long_rate and short_rate else None

        details.update({
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "symbol": symbol,
            "long_funding_rate": float(long_rate) if long_rate else None,
            "short_funding_rate": float(short_rate) if short_rate else None,
            "funding_rate_spread": float(spread) if spread else None,
            "long_confidence": long_metrics.get("confidence", 0),
            "short_confidence": short_metrics.get("confidence", 0),
            "stability_check_passed": True,
            "bounds_check_passed": True,
            "spread_check_passed": True,
            "confidence_check_passed": True,
        })

        return CheckResult.success(
            message=(
                f"Funding rate check passed: spread={spread:.6f}"
                if spread
                else "Funding rate check passed"
            ),
            details=details,
        )

    def _check_funding_rate_bounds(
        self, long_metrics: dict[str, Any], short_metrics: dict[str, Any]
    ) -> CheckResult:
        """Check if funding rates are within reasonable bounds.

        Returns:
            CheckResult: Success if rates within bounds, failure otherwise.
        """
        for exchange_name, metrics in [("long", long_metrics), ("short", short_metrics)]:
            current_rate = self._get_current_funding_rate(metrics)
            if current_rate is None:
                return CheckResult.failure(
                    message=f"Current funding rate not available for {exchange_name} exchange",
                    details={f"{exchange_name}_metrics": metrics},
                )

            if current_rate < self._min_funding_rate:
                return CheckResult.failure(
                    message=(
                        f"{exchange_name} funding rate {current_rate:.6f} below minimum "
                        f"{self._min_funding_rate:.6f}"
                    ),
                    details={
                        f"{exchange_name}_funding_rate": float(current_rate),
                        "min_funding_rate": float(self._min_funding_rate),
                    },
                )

            if current_rate > self._max_funding_rate:
                return CheckResult.failure(
                    message=(
                        f"{exchange_name} funding rate {current_rate:.6f} above maximum "
                        f"{self._max_funding_rate:.6f}"
                    ),
                    details={
                        f"{exchange_name}_funding_rate": float(current_rate),
                        "max_funding_rate": float(self._max_funding_rate),
                    },
                )

        return CheckResult.success("Funding rate bounds check passed")

    def _check_funding_rate_spread(
        self, long_metrics: dict[str, Any], short_metrics: dict[str, Any]
    ) -> CheckResult:
        """Check if funding rate spread is within acceptable limits.

        Returns:
            CheckResult: Success if spread within limits, failure otherwise.
        """
        long_rate = self._get_current_funding_rate(long_metrics)
        short_rate = self._get_current_funding_rate(short_metrics)

        if long_rate is None or short_rate is None:
            return CheckResult.failure(
                message="Cannot calculate funding rate spread: missing rates",
                details={
                    "long_rate": float(long_rate) if long_rate else None,
                    "short_rate": float(short_rate) if short_rate else None,
                },
            )

        spread = abs(long_rate - short_rate)

        if spread > self._max_funding_rate_spread:
            return CheckResult.failure(
                message=(
                    f"Funding rate spread {spread:.6f} exceeds maximum "
                    f"{self._max_funding_rate_spread:.6f}"
                ),
                details={
                    "funding_rate_spread": float(spread),
                    "max_funding_rate_spread": float(self._max_funding_rate_spread),
                    "long_rate": float(long_rate),
                    "short_rate": float(short_rate),
                },
            )

        return CheckResult.success("Funding rate spread check passed")

    def _check_funding_rate_stability(
        self, long_metrics: dict[str, Any], short_metrics: dict[str, Any]
    ) -> CheckResult:
        """Check funding rate stability over time.

        Returns:
            CheckResult: Success if rates are stable, warning if volatile.
        """
        for exchange_name, metrics in [("long", long_metrics), ("short", short_metrics)]:
            # Get historical rates if available
            historical_rates = metrics.get("historical_rates", [])
            if not historical_rates or len(historical_rates) < MIN_HISTORICAL_RATES_FOR_STABILITY:
                # Skip stability check if insufficient data
                continue

            try:
                # Calculate volatility (standard deviation of rates)
                lookback = int(self.stability_lookback_hours)
                rates = [float(rate) for rate in historical_rates[-lookback:]]
                if len(rates) > 1:
                    volatility = Decimal(str(statistics.stdev(rates)))

                    if volatility > self._max_funding_rate_volatility:
                        return CheckResult.failure(
                            message=(
                                f"{exchange_name} funding rate volatility {volatility:.6f} "
                                f"exceeds maximum {self._max_funding_rate_volatility:.6f}"
                            ),
                            details={
                                f"{exchange_name}_volatility": float(volatility),
                                "max_volatility": float(self._max_funding_rate_volatility),
                                "sample_size": len(rates),
                            },
                        )

                # Check for excessive consecutive moves in same direction
                if self.enable_pattern_check:
                    consecutive_count = self._count_consecutive_moves(rates)
                    max_consecutive = int(self.max_consecutive_same_direction)
                    if consecutive_count > max_consecutive:
                        return CheckResult.failure(
                            message=(
                                f"{exchange_name} funding rate has {consecutive_count} "
                                f"consecutive moves in same direction"
                            ),
                            details={
                                f"{exchange_name}_consecutive_moves": consecutive_count,
                                "max_consecutive": max_consecutive,
                            },
                        )

            except (ValueError, statistics.StatisticsError) as e:
                # Handle edge cases in stability calculation
                self.logger.warning(
                    "Error calculating stability", exchange_name=exchange_name, error=str(e)
                )

        return CheckResult.success("Funding rate stability check passed")

    def _check_confidence_scores(
        self, long_metrics: dict[str, Any], short_metrics: dict[str, Any]
    ) -> CheckResult:
        """Check confidence scores for funding rate data.

        Returns:
            CheckResult: Success if confidence high, warning otherwise.
        """
        for exchange_name, metrics in [("long", long_metrics), ("short", short_metrics)]:
            confidence = metrics.get("confidence", 0)

            if confidence < float(self._min_confidence_score):
                return CheckResult.failure(
                    message=(
                        f"{exchange_name} funding rate confidence {confidence:.2f} "
                        f"below minimum {self._min_confidence_score:.2f}"
                    ),
                    details={
                        f"{exchange_name}_confidence": confidence,
                        "min_confidence": float(self._min_confidence_score),
                    },
                )

            # Check if primary source is required and available
            if self.require_primary_source:
                source_tier = metrics.get("source_tier", "unknown")
                if source_tier != "PRIMARY":
                    return CheckResult.failure(
                        message=(
                            f"{exchange_name} funding rate source is not primary: {source_tier}"
                        ),
                        details={
                            f"{exchange_name}_source_tier": source_tier,
                            "require_primary": self.require_primary_source,
                        },
                    )

        return CheckResult.success("Funding rate confidence check passed")

    def _get_current_funding_rate(self, metrics: dict[str, Any]) -> Decimal | None:
        """Extract current funding rate from metrics.

        Returns:
            Decimal | None: Current funding rate or None if not available.
        """
        current_rate = metrics.get("current_rate")
        if current_rate is None:
            return None

        try:
            return Decimal(str(current_rate))
        except (ValueError, TypeError):
            return None

    def _count_consecutive_moves(self, rates: list[float]) -> int:
        """Count consecutive moves in the same direction.

        Returns:
            int: Number of consecutive moves in same direction.
        """
        if len(rates) < MIN_RATES_FOR_CONSECUTIVE_ANALYSIS:
            return 0

        max_consecutive = 0
        current_consecutive = 1
        last_direction = 0

        for i in range(1, len(rates)):
            if len(rates) <= i:
                break

            current_move = rates[i] - rates[i - 1]
            if i == 1:
                last_direction = 1 if current_move > 0 else -1 if current_move < 0 else 0
                continue

            current_direction = 1 if current_move > 0 else -1 if current_move < 0 else 0

            if current_direction == last_direction and current_direction != 0:
                current_consecutive += 1
            else:
                max_consecutive = max(max_consecutive, current_consecutive)
                current_consecutive = 1

            last_direction = current_direction

        return max(max_consecutive, current_consecutive)

    def set_funding_rate_bounds(self, min_rate: Decimal, max_rate: Decimal) -> None:
        """Set funding rate bounds.

        Args:
            min_rate: Minimum allowed funding rate
            max_rate: Maximum allowed funding rate

        Raises:
            FundingRateError: If min_rate >= max_rate.
        """
        if min_rate >= max_rate:
            msg = "min_rate must be less than max_rate"
            raise FundingRateError(
                msg,
                metadata={"min_rate": float(min_rate), "max_rate": float(max_rate)},
                checker_name="FundingRateChecker",
                check_type="funding_rate",
            )

        self._min_funding_rate = min_rate
        self._max_funding_rate = max_rate
        self.logger.info(
            "Set funding rate bounds", min_rate=f"{min_rate:.6f}", max_rate=f"{max_rate:.6f}"
        )

    def set_spread_threshold(self, max_spread: Decimal) -> None:
        """Set maximum funding rate spread threshold.

        Args:
            max_spread: Maximum allowed funding rate spread
        """
        self._max_funding_rate_spread = max_spread
        self.logger.info("Set funding rate spread threshold", max_spread=f"{max_spread:.6f}")

    def set_confidence_threshold(self, min_confidence: Decimal) -> None:
        """Set minimum confidence score threshold.

        Args:
            min_confidence: Minimum required confidence score
        """
        self._min_confidence_score = min_confidence
        self.logger.info("Set confidence threshold", min_confidence=f"{min_confidence:.2f}")

    def enable_primary_source_requirement(self, require: bool) -> None:
        """Enable or disable primary source requirement.

        Args:
            require: Whether to require primary source
        """
        self.require_primary_source = require
        self.logger.info("Primary source requirement", require=require)

    def _create_skip_result(self) -> CheckResult:
        """Create result for skipped check.

        Returns:
            CheckResult: Skip result with appropriate message.
        """
        return CheckResult.skip(
            message=f"{self.CHECKER_NAME} check skipped (disabled)",
        )

    def _create_error_result(self, error: Exception, execution_time: float) -> CheckResult:
        """Create result for failed check.

        Returns:
            CheckResult: Error result with exception details.
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
