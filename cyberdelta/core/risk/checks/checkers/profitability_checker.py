"""Profitability checker implementation with direct configuration access."""

from decimal import Decimal
from typing import Any, Final

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class ProfitabilityChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates opportunity profitability meets minimum thresholds."""

    CHECKER_NAME: Final[str] = "profitability"

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the profitability checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
        """
        super().__init__(app_settings, self.CHECKER_NAME)

        # Cache frequently accessed values for performance
        self._min_profitability = self.thresholds.min_profitability

        # Additional thresholds not in the new config model (using defaults)
        self.min_profit_usd = Decimal("0.1")  # $0.10 minimum profit USD
        self.min_profit_percentage = Decimal("0.0001")  # 0.01% minimum profit percentage

        # Fee considerations
        self.include_fees = self.checker_settings.include_fees_in_profitability

        # Estimated fee percentage (not in new config, using default)
        self.estimated_fee_percentage = Decimal("0.001")  # 0.1% estimated fee

        # Risk-adjusted profitability (using base validation factor as proxy)
        self.risk_adjustment_factor = self.app_settings.risk.sizing.base_validation_factor

    @property
    def name(self) -> str:
        """Name of the checker."""
        return "profitability"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check that opportunity meets profitability requirements.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        details: dict[str, Any] = {}

        # Get and validate spread percentage
        spread_decimal = self._get_spread_percentage(opportunity)
        if isinstance(spread_decimal, CheckResult):
            return spread_decimal  # Return early failure

        # Calculate adjusted spreads
        _, risk_adjusted_spread = self._calculate_adjusted_spreads(spread_decimal, details)

        # Check spread thresholds
        threshold_result = self._check_spread_thresholds(
            spread_decimal, risk_adjusted_spread, details
        )
        if threshold_result:
            return threshold_result

        # Check optional profit metrics
        profit_result = self._check_profit_metrics(opportunity, details)
        if profit_result:
            return profit_result

        # Success case
        details["profitability_score"] = float(risk_adjusted_spread / self._min_profitability)

        return CheckResult.success(
            message=(
                f"Profitability check passed: {risk_adjusted_spread:.6f} >= "
                f"{self._min_profitability:.6f}"
            ),
            details=details,
        )

    def _get_spread_percentage(self, opportunity: ArbitrageOpportunity) -> Decimal | CheckResult:
        """Get and validate spread percentage from opportunity.

        Args:
            opportunity: The arbitrage opportunity to extract spread from

        Returns:
            Decimal | CheckResult: The spread percentage as Decimal if valid,
                                   or CheckResult with failure details if invalid
        """
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage is None:
            return CheckResult.failure(
                message="Cannot check profitability: spread_percentage is missing",
                details={"missing_field": "spread_percentage"},
            )

        # Convert to Decimal
        try:
            if isinstance(spread_percentage, str):
                return Decimal(spread_percentage)
            if isinstance(spread_percentage, (int, float)):
                return Decimal(str(spread_percentage))
            if isinstance(spread_percentage, Decimal):
                return spread_percentage
            return CheckResult.failure(
                message=f"Invalid spread_percentage type: {type(spread_percentage)}",
                details={"spread_percentage": str(spread_percentage)},
            )
        except (ValueError, TypeError) as e:
            return CheckResult.failure(
                message=f"Invalid spread_percentage value: {e!s}",
                details={"spread_percentage": str(spread_percentage)},
            )

    def _calculate_adjusted_spreads(
        self, spread_decimal: Decimal, details: dict[str, Any]
    ) -> tuple[Decimal, Decimal]:
        """Calculate effective and risk-adjusted spreads.

        Args:
            spread_decimal: Raw spread percentage as Decimal
            details: Dictionary to store calculation details

        Returns:
            tuple[Decimal, Decimal]: Tuple of (effective_spread, risk_adjusted_spread)
        """
        # Calculate effective spread after fees
        effective_spread = spread_decimal
        if self.include_fees:
            # Subtract estimated trading fees (2x for both sides)
            effective_spread = spread_decimal - (self.estimated_fee_percentage * 2)

        # Apply risk adjustment
        risk_adjusted_spread = effective_spread * self.risk_adjustment_factor

        # Store calculation details
        details.update({
            "raw_spread_percentage": float(spread_decimal),
            "effective_spread_percentage": float(effective_spread),
            "risk_adjusted_spread_percentage": float(risk_adjusted_spread),
            "estimated_fee_percentage": float(self.estimated_fee_percentage),
            "risk_adjustment_factor": float(self.risk_adjustment_factor),
            "include_fees": self.include_fees,
        })

        return effective_spread, risk_adjusted_spread

    def _check_spread_thresholds(
        self, spread_decimal: Decimal, risk_adjusted_spread: Decimal, details: dict[str, Any]
    ) -> CheckResult | None:
        """Check spread against minimum and maximum thresholds.

        Args:
            spread_decimal: Raw spread percentage
            risk_adjusted_spread: Risk-adjusted spread percentage
            details: Dictionary containing calculation details

        Returns:
            CheckResult | None: CheckResult with failure details if thresholds not met,
                               None if all thresholds pass
        """
        # Check minimum spread percentage
        if risk_adjusted_spread < self._min_profitability:
            return CheckResult.failure(
                message=(
                    f"Risk-adjusted spread {risk_adjusted_spread:.6f} below minimum "
                    f"{self._min_profitability:.6f}"
                ),
                details=details,
            )

        # Check for negative spread
        if spread_decimal < 0:
            return CheckResult.failure(
                message=f"Negative spread detected: {spread_decimal:.6f}",
                details=details,
            )

        # Check if spread is unrealistically high (possible data error)
        # Using a hardcoded value as this isn't in the new config
        max_realistic_spread = Decimal("0.1")  # 10% max realistic spread
        if spread_decimal > max_realistic_spread:
            return CheckResult.failure(
                message=(
                    f"Spread {spread_decimal:.6f} exceeds maximum realistic spread "
                    f"{max_realistic_spread:.6f}"
                ),
                details=details,
            )

        return None

    def _check_profit_metrics(
        self, opportunity: ArbitrageOpportunity, details: dict[str, Any]
    ) -> CheckResult | None:
        """Check optional profit metrics if available.

        Args:
            opportunity: The arbitrage opportunity with optional profit fields
            details: Dictionary to store profit metric details

        Returns:
            CheckResult | None: CheckResult with failure details if profit metrics fail,
                               None if all available metrics pass
        """
        # Check minimum profit in USD if available
        if opportunity.expected_profit is not None:
            try:
                profit_usd = opportunity.expected_profit  # Already Decimal from Pydantic
                details["expected_profit_usd"] = float(profit_usd)

                if profit_usd < self.min_profit_usd:
                    return CheckResult.failure(
                        message=(
                            f"Expected profit ${profit_usd:.2f} below minimum "
                            f"${self.min_profit_usd:.2f}"
                        ),
                        details=details,
                    )
            except (ValueError, TypeError):
                # If profit USD is invalid, we'll still check other criteria
                pass

        # Check minimum profit percentage if available
        if opportunity.expected_return_percentage is not None:
            try:
                # Already Decimal from Pydantic
                return_percentage = opportunity.expected_return_percentage
                details["expected_return_percentage"] = float(return_percentage)

                if return_percentage < self.min_profit_percentage:
                    return CheckResult.failure(
                        message=(
                            f"Expected return {return_percentage:.6f} below minimum "
                            f"{self.min_profit_percentage:.6f}"
                        ),
                        details=details,
                    )
            except (ValueError, TypeError):
                # If return percentage is invalid, we'll still check other criteria
                pass

        return None

    def set_min_spread_percentage(self, min_spread: Decimal) -> None:
        """Set minimum spread percentage threshold.

        Args:
            min_spread: Minimum spread percentage required
        """
        self._min_profitability = min_spread
        self.logger.info("Set minimum spread percentage", min_spread=f"{min_spread:.6f}")

    def set_fee_percentage(self, fee_percentage: Decimal) -> None:
        """Set estimated trading fee percentage.

        Args:
            fee_percentage: Estimated fee percentage per trade
        """
        self.estimated_fee_percentage = fee_percentage
        self.logger.info("Set estimated fee percentage", fee_percentage=f"{fee_percentage:.6f}")

    def set_risk_adjustment_factor(self, factor: Decimal) -> None:
        """Set risk adjustment factor.

        Args:
            factor: Risk adjustment factor (1.0 = no adjustment, <1.0 = more conservative)
        """
        self.risk_adjustment_factor = factor
        self.logger.info("Set risk adjustment factor", factor=f"{factor:.6f}")

    def enable_fee_adjustment(self) -> None:
        """Enable fee adjustment in profitability calculations."""
        self.include_fees = True
        self.logger.info("Enabled fee adjustment")

    def disable_fee_adjustment(self) -> None:
        """Disable fee adjustment in profitability calculations."""
        self.include_fees = False
        self.logger.info("Disabled fee adjustment")

    def _create_skip_result(self) -> CheckResult:
        """Create result for skipped check.

        Returns:
            CheckResult: A CheckResult with SKIPPED status
        """
        return CheckResult.skip(
            message=f"{self.CHECKER_NAME} check skipped (disabled)",
        )

    def _create_error_result(self, error: Exception, execution_time: float) -> CheckResult:
        """Create result for failed check.

        Args:
            error: The exception that caused the check to fail
            execution_time: Time taken for the check in milliseconds

        Returns:
            CheckResult: A CheckResult with ERROR status and error details
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
