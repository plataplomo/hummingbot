"""Circuit breaker checker implementation with direct configuration access."""

from typing import Any, Final, Protocol

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.core.symbols import Symbol
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Protocol for circuit breaker system
class CircuitBreakerSystemProtocol(Protocol):
    """Protocol for circuit breaker system."""

    def check_circuit_state(self, symbol: Symbol, exchange: str) -> dict[str, Any]:
        """Check circuit breaker state."""
        ...

    def get_system_status(self) -> dict[str, Any]:
        """Get system status."""
        ...

    def can_execute(self, symbol: Symbol, exchange: str) -> bool:
        """Check if can execute trade."""
        ...

    def get_exchange_breaker(self, exchange: str) -> dict[str, Any]:
        """Get exchange circuit breaker info."""
        ...


class CircuitBreakerChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates circuit breaker status for exchanges."""

    CHECKER_NAME: Final[str] = "circuit_breaker"

    def __init__(
        self,
        app_settings: AppSettings,
        circuit_breaker_system: CircuitBreakerSystemProtocol,
    ) -> None:
        """Initialize the circuit breaker checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
            circuit_breaker_system: Circuit breaker system to check
        """
        super().__init__(
            app_settings, self.CHECKER_NAME, circuit_breaker_system=circuit_breaker_system
        )
        self.circuit_breaker_system = circuit_breaker_system

        # Configuration from AppSettings
        self.check_both_exchanges = self.checker_settings.check_both_exchanges
        # Hardcoded as not in new config model
        self.fail_on_half_open = False

    @property
    def name(self) -> str:
        """Name of the checker."""
        return "circuit_breaker"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check circuit breaker status for the opportunity's exchanges.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        details: dict[str, Any] = {}

        # Get exchanges from opportunity
        long_exchange = getattr(opportunity, "long_exchange", None)
        short_exchange = getattr(opportunity, "short_exchange", None)

        if not long_exchange or not short_exchange:
            return CheckResult.failure(
                message="Cannot check circuit breaker: missing exchange information",
                details={"long_exchange": long_exchange, "short_exchange": short_exchange},
            )

        exchanges_to_check = [long_exchange]
        if self.check_both_exchanges and short_exchange != long_exchange:
            exchanges_to_check.append(short_exchange)

        # Check each exchange
        failed_exchanges: list[str] = []
        half_open_exchanges: list[str] = []
        details.update({
            "checked_exchanges": exchanges_to_check,
            "check_both_exchanges": self.check_both_exchanges,
        })

        for exchange in exchanges_to_check:
            try:
                # can_execute expects symbol and exchange parameters based on Protocol
                can_execute = self.circuit_breaker_system.can_execute(opportunity.symbol, exchange)

                # Get breaker state for more detailed information
                breaker = self.circuit_breaker_system.get_exchange_breaker(exchange)
                breaker_state = breaker.get("state", "unknown")

                details[f"{exchange}_can_execute"] = can_execute
                details[f"{exchange}_state"] = breaker_state

                if not can_execute:
                    failed_exchanges.append(f"{exchange}: circuit breaker open")
                elif breaker_state == "HALF_OPEN" and self.fail_on_half_open:
                    half_open_exchanges.append(f"{exchange}: in half-open state")

            except (KeyError, AttributeError, ValueError, TypeError) as e:
                self.logger.exception("Error checking circuit breaker", exchange=exchange)
                failed_exchanges.append(f"{exchange}: error checking breaker - {e!s}")

        # Evaluate results
        if failed_exchanges:
            return CheckResult.failure(
                message="Circuit breaker check failed for exchanges: {}".format(
                    ", ".join(failed_exchanges),
                ),
                details=details,
            )

        if half_open_exchanges:
            return CheckResult.failure(
                message=f"Circuit breaker in half-open state: {', '.join(half_open_exchanges)}",
                details=details,
            )

        return CheckResult.success(
            message=(
                f"Circuit breaker check passed for exchanges: {', '.join(exchanges_to_check)}"
            ),
            details=details,
        )

    def set_check_both_exchanges(self, check_both: bool) -> None:
        """Set whether to check both exchanges.

        Args:
            check_both: Whether to check both long and short exchanges
        """
        self.check_both_exchanges = check_both
        self.logger.info("Set check_both_exchanges", check_both=check_both)

    def set_fail_on_half_open(self, fail_on_half_open: bool) -> None:
        """Set whether to fail on half-open circuit breaker state.

        Args:
            fail_on_half_open: Whether to fail when breaker is in half-open state
        """
        self.fail_on_half_open = fail_on_half_open
        self.logger.info("Set fail_on_half_open", fail_on_half_open=fail_on_half_open)

    def _create_skip_result(self) -> CheckResult:
        """Create result for skipped check.

        Returns:
            CheckResult indicating the check was skipped
        """
        return CheckResult.skip(
            message=f"{self.CHECKER_NAME} check skipped (disabled)",
        )

    def _create_error_result(self, error: Exception, execution_time: float) -> CheckResult:
        """Create result for failed check.

        Returns:
            CheckResult indicating the check failed with error details
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
