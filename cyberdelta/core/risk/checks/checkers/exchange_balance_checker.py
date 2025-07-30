"""Exchange balance checker implementation with direct configuration access."""

import contextlib
from decimal import Decimal
from typing import Any, Final, Protocol

from cyberdelta.config import AppSettings
from cyberdelta.core.models import SpotBalance
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.core.risk.exceptions.check_exceptions import ExchangeBalanceError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Protocol for portfolio tracker
class PortfolioTrackerProtocol(Protocol):
    """Protocol for portfolio tracker."""

    def get_exchange_balances(self, exchange: str) -> list[SpotBalance]:
        """Get exchange balances."""
        ...

    def get_exchange_balance(self, exchange: str, asset: str) -> Decimal:
        """Get specific exchange balance."""
        ...

    def get_total_portfolio_value(self) -> Decimal:
        """Get total portfolio value."""
        ...

    def get_total_capital(self) -> Decimal:
        """Get total capital."""
        ...


class ExchangeBalanceChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates sufficient exchange balances for opportunities."""

    CHECKER_NAME: Final[str] = "balance"

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTrackerProtocol,
    ) -> None:
        """Initialize the exchange balance checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
            portfolio_tracker: Portfolio tracker for balance information
        """
        super().__init__(app_settings, self.CHECKER_NAME, portfolio_tracker=portfolio_tracker)
        self.portfolio_tracker = portfolio_tracker

        # Cache frequently accessed values for performance
        self._min_balance_ratio = self.thresholds.min_balance_ratio
        # Additional thresholds not in the new config model (using defaults)
        self.safety_margin = Decimal("0.05")  # 5% safety margin

        # Balance validation
        self.check_both_exchanges = self.checker_settings.check_both_exchanges
        # $10 minimum balance requirement (hardcoded as not in new config)
        self.min_usd_balance = Decimal("10.0")

        # Collateral assumptions (hardcoded as not in new config)
        self.collateral_asset = "USD"
        self.enable_multi_asset_support = False

        # Emergency thresholds (hardcoded as not in new config)
        self.emergency_balance_threshold = Decimal("0.02")  # 2% emergency threshold
        self.enable_emergency_check = True

        # Balance estimation
        self.enable_balance_estimation = True
        # Use sizing settings from AppSettings
        self.default_position_size = self.app_settings.risk.sizing.simple_fixed_usd

    @property
    def name(self) -> str:
        """Name of the checker.

        Returns:
            The string 'exchange_balance'
        """
        return "exchange_balance"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check exchange balance sufficiency for the opportunity.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        details: dict[str, Any] = {}

        # Get exchange information
        long_exchange = getattr(opportunity, "long_exchange", None)
        short_exchange = getattr(opportunity, "short_exchange", None)
        symbol = getattr(opportunity, "symbol", "unknown")

        if not long_exchange or not short_exchange:
            return CheckResult.failure(
                message="Cannot check exchange balance: missing exchange information",
                details={
                    "long_exchange": long_exchange,
                    "short_exchange": short_exchange,
                    "symbol": symbol,
                },
            )

        # Estimate required balance
        required_balance = self._estimate_required_balance(opportunity)

        details.update({
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "symbol": symbol,
            "estimated_required_balance": float(required_balance),
            "safety_margin": float(self.safety_margin),
            "min_balance_ratio": float(self._min_balance_ratio),
        })

        # Check long exchange balance
        long_balance_result = await self._check_exchange_balance(
            long_exchange,
            required_balance,
            "long",
        )
        if not long_balance_result.passed:
            details.update(long_balance_result.details or {})
            return CheckResult.failure(
                message=f"Long exchange balance check failed: {long_balance_result.message}",
                details=details,
            )

        # Check short exchange balance if different from long
        if self.check_both_exchanges and short_exchange != long_exchange:
            short_balance_result = await self._check_exchange_balance(
                short_exchange,
                required_balance,
                "short",
            )
            if not short_balance_result.passed:
                details.update(short_balance_result.details or {})
                return CheckResult.failure(
                    message=f"Short exchange balance check failed: {short_balance_result.message}",
                    details=details,
                )

        # Emergency balance check
        if self.enable_emergency_check:
            # Check emergency balances, avoiding duplicates
            exchanges_to_check = (
                [long_exchange, short_exchange]
                if short_exchange != long_exchange
                else [long_exchange]
            )
            emergency_result = await self._check_emergency_balances(exchanges_to_check)
            if not emergency_result.passed:
                details.update(emergency_result.details or {})
                return CheckResult.failure(
                    message=f"Emergency balance check failed: {emergency_result.message}",
                    details=details,
                )

        # Get actual balances for details
        long_balance = await self._get_exchange_balance(long_exchange)
        details["long_balance"] = float(long_balance) if long_balance else None

        if short_exchange != long_exchange:
            short_balance = await self._get_exchange_balance(short_exchange)
            details["short_balance"] = float(short_balance) if short_balance else None

        return CheckResult.success(
            message=f"Exchange balance check passed for {long_exchange}"
            + (f" and {short_exchange}" if short_exchange != long_exchange else ""),
            details=details,
        )

    def _estimate_required_balance(self, opportunity: ArbitrageOpportunity) -> Decimal:
        """Estimate required balance for the opportunity.

        Args:
            opportunity: The arbitrage opportunity to estimate balance for.

        Returns:
            Decimal: Estimated required balance including safety margin, never less than
                minimum USD balance.
        """
        # Try to get size from opportunity
        estimated_size = None

        # Check for size hints in opportunity
        if opportunity.optimal_size is not None:
            with contextlib.suppress(ValueError, TypeError):
                estimated_size = opportunity.optimal_size  # Already Decimal from Pydantic

        # Fallback to price-based estimation
        if estimated_size is None:
            long_price = getattr(opportunity, "long_price", None)
            short_price = getattr(opportunity, "short_price", None)

            if long_price and short_price:
                try:
                    long_decimal = Decimal(str(long_price))
                    short_decimal = Decimal(str(short_price))
                    avg_price = (long_decimal + short_decimal) / 2

                    # Estimate based on average price and default position size
                    if avg_price > 0:
                        estimated_size = self.default_position_size
                    else:
                        estimated_size = self.default_position_size
                except (ValueError, TypeError):
                    estimated_size = self.default_position_size
            else:
                estimated_size = self.default_position_size

        # Add safety margin
        required_balance = estimated_size * (Decimal(1) + self.safety_margin)

        # Ensure minimum balance
        return max(required_balance, self.min_usd_balance)

    async def _check_exchange_balance(
        self,
        exchange: str,
        required_balance: Decimal,
        exchange_type: str,
    ) -> CheckResult:
        """Check balance for a specific exchange.

        Args:
            exchange: Exchange identifier to check.
            required_balance: Required balance amount.
            exchange_type: Type of exchange ("long" or "short").

        Returns:
            CheckResult: Success if balance meets minimum threshold, failure otherwise.
        """
        try:
            balance = await self._get_exchange_balance(exchange)

            if balance is None:
                return CheckResult.failure(
                    message=f"Balance not available for {exchange}",
                    details={f"{exchange_type}_exchange": exchange},
                )

            # Check minimum balance threshold
            min_required = required_balance * self._min_balance_ratio

            if balance < min_required:
                return CheckResult.failure(
                    message=(
                        f"Insufficient balance on {exchange}: {balance:.2f} < {min_required:.2f}"
                    ),
                    details={
                        f"{exchange_type}_exchange": exchange,
                        f"{exchange_type}_balance": float(balance),
                        f"{exchange_type}_required": float(min_required),
                        "balance_ratio": (
                            float(balance / required_balance) if required_balance > 0 else 0
                        ),
                    },
                )

            # Check if balance is sufficient for full position
            if balance < required_balance:
                # Balance is above minimum but below full requirement
                return CheckResult.success(
                    message=(
                        f"Partial balance available on {exchange}: "
                        f"{balance:.2f} of {required_balance:.2f}"
                    ),
                    details={
                        f"{exchange_type}_exchange": exchange,
                        f"{exchange_type}_balance": float(balance),
                        f"{exchange_type}_required": float(required_balance),
                        "balance_ratio": float(balance / required_balance),
                        "partial_balance": True,
                    },
                )

            return CheckResult.success(
                message=f"Sufficient balance on {exchange}: {balance:.2f}",
                details={
                    f"{exchange_type}_exchange": exchange,
                    f"{exchange_type}_balance": float(balance),
                    f"{exchange_type}_required": float(required_balance),
                    "balance_ratio": float(balance / required_balance),
                },
            )

        except (ValueError, TypeError, ArithmeticError, AttributeError, KeyError) as e:
            return CheckResult.error(
                message=f"Error checking balance for {exchange}: {e!s}",
                details={
                    f"{exchange_type}_exchange": exchange,
                    "error": str(e),
                },
            )

    async def _check_emergency_balances(self, exchanges: list[str]) -> CheckResult:
        """Check for emergency balance thresholds.

        Args:
            exchanges: List of exchange identifiers to check.

        Returns:
            CheckResult: Failure if any exchange breaches emergency threshold, success otherwise.
        """
        for exchange in exchanges:
            try:
                balance = await self._get_exchange_balance(exchange)

                if balance is None:
                    continue

                # Get total capital to calculate emergency threshold
                total_capital = await self._get_total_capital()

                if total_capital and total_capital > 0:
                    balance_ratio = balance / total_capital

                    if balance_ratio < self.emergency_balance_threshold:
                        return CheckResult.failure(
                            message=(
                                f"Emergency balance threshold breached on {exchange}: "
                                f"{balance_ratio:.2%} < {self.emergency_balance_threshold:.2%}"
                            ),
                            details={
                                "exchange": exchange,
                                "balance": float(balance),
                                "total_capital": float(total_capital),
                                "balance_ratio": float(balance_ratio),
                                "emergency_threshold": float(self.emergency_balance_threshold),
                            },
                        )

            except Exception:
                self.logger.exception("Error checking emergency balance", exchange=exchange)

        return CheckResult.success("Emergency balance check passed")

    async def _get_exchange_balance(self, exchange: str) -> Decimal | None:
        """Get balance for a specific exchange.

        Args:
            exchange: Exchange identifier.

        Returns:
            Decimal | None: USD balance on the exchange, or None if unavailable.
        """
        try:
            # According to the protocol, get_exchange_balance returns Decimal
            return self.portfolio_tracker.get_exchange_balance(exchange, "USD")
        except Exception:
            self.logger.exception("Error getting balance", exchange=exchange)
            return None

    async def _get_total_capital(self) -> Decimal | None:
        """Get total capital across all exchanges.

        Returns:
            Decimal | None: Total capital amount, or None if unavailable.
        """
        try:
            total_capital = self.portfolio_tracker.get_total_capital()
            return Decimal(str(total_capital))
        except Exception:
            self.logger.exception("Error getting total capital")
            return None

    def set_balance_ratio_threshold(self, min_ratio: Decimal) -> None:
        """Set minimum balance ratio threshold.

        Args:
            min_ratio: Minimum balance ratio (0-1)

        Raises:
            ExchangeBalanceError: If min_ratio is not between 0 and 1.
        """
        if min_ratio < 0 or min_ratio > 1:
            msg = "min_ratio must be between 0 and 1"
            raise ExchangeBalanceError(
                msg,
                metadata={"min_ratio": float(min_ratio), "valid_range": "0-1"},
                checker_name="ExchangeBalanceChecker",
                check_type="exchange_balance",
            )

        self._min_balance_ratio = min_ratio
        self.logger.info("Set balance ratio threshold", min_ratio=f"{min_ratio:.2%}")

    def set_safety_margin(self, margin: Decimal) -> None:
        """Set safety margin for balance calculations.

        Args:
            margin: Safety margin (0-1)

        Raises:
            ExchangeBalanceError: If margin is not between 0 and 1.
        """
        if margin < 0 or margin > 1:
            msg = "margin must be between 0 and 1"
            raise ExchangeBalanceError(
                msg,
                metadata={"margin": float(margin), "valid_range": "0-1"},
                checker_name="ExchangeBalanceChecker",
                check_type="exchange_balance",
            )

        self.safety_margin = margin
        self.logger.info("Set safety margin", margin=f"{margin:.2%}")

    def set_minimum_usd_balance(self, min_balance: Decimal) -> None:
        """Set minimum USD balance requirement.

        Args:
            min_balance: Minimum balance in USD

        Raises:
            ExchangeBalanceError: If min_balance is negative.
        """
        if min_balance < 0:
            msg = "min_balance must be non-negative"
            raise ExchangeBalanceError(
                msg,
                metadata={"min_balance": float(min_balance)},
                checker_name="ExchangeBalanceChecker",
                check_type="exchange_balance",
            )

        self.min_usd_balance = min_balance
        self.logger.info("Set minimum USD balance", min_balance=f"${min_balance:.2f}")

    def set_emergency_threshold(self, threshold: Decimal) -> None:
        """Set emergency balance threshold.

        Args:
            threshold: Emergency threshold as ratio of total capital

        Raises:
            ExchangeBalanceError: If threshold is not between 0 and 1.
        """
        if threshold < 0 or threshold > 1:
            msg = "threshold must be between 0 and 1"
            raise ExchangeBalanceError(
                msg,
                metadata={"threshold": float(threshold), "valid_range": "0-1"},
                checker_name="ExchangeBalanceChecker",
                check_type="exchange_balance",
            )

        self.emergency_balance_threshold = threshold
        self.logger.info("Set emergency threshold", threshold=f"{threshold:.2%}")

    def enable_both_exchanges_check(self, enable: bool) -> None:
        """Enable or disable checking both exchanges.

        Args:
            enable: Whether to check both exchanges
        """
        self.check_both_exchanges = enable
        self.logger.info("Both exchanges check", enable=enable)

    def enable_emergency_checks(self, enable: bool) -> None:
        """Enable or disable emergency balance checks.

        Args:
            enable: Whether to enable emergency checks
        """
        self.enable_emergency_check = enable
        self.logger.info("Emergency checks", enable=enable)

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

        Args:
            error: The exception that occurred.
            execution_time: Time taken for the check in milliseconds.

        Returns:
            CheckResult: Error result with exception details.
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
