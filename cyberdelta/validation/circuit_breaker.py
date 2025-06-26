"""Circuit Breaker System for the CyberDeltaEngine.

This module provides circuit breakers that can halt trading operations
when abnormal conditions are detected, preventing cascading failures
and limiting potential losses.
"""

from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from typing import Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


# Add the custom exception class
class CircuitBreakerTrippedError(Exception):
    """Custom exception raised when a circuit breaker prevents an operation."""


class BreakerState(Enum):
    """State of a circuit breaker."""

    CLOSED = auto()  # Normal operation, allowing trades
    OPEN = auto()  # Tripped, blocking trades
    HALF_OPEN = auto()  # Testing if system has recovered


class CircuitBreaker(ABC):
    """Base abstract class for all circuit breakers.

    Circuit breakers monitor specific conditions and "trip" (open)
    when those conditions indicate problems.
    """

    def __init__(self, name: str, cooldown_seconds: int = 300) -> None:
        """Initialize the circuit breaker.

        Args:
            name: Identifier for this circuit breaker
            cooldown_seconds: Time to wait before testing if system has recovered

        """
        self.name = name
        self.cooldown_seconds = cooldown_seconds
        self.state = BreakerState.CLOSED
        self.trip_time: datetime | None = None
        self.trip_reason: str | None = None
        self.trip_count = 0
        self.last_reset_time: datetime | None = None

    def trip(self, reason: str) -> None:
        """Trip the circuit breaker, preventing further operations.

        Args:
            reason: Why the breaker was tripped

        """
        # Increment count even if already open, but only log/set time on first trip
        first_trip = self.state != BreakerState.OPEN
        self.state = BreakerState.OPEN
        self.trip_count += 1

        if first_trip:
            # Use timezone-aware datetime
            self.trip_time = datetime.now(UTC)
            self.trip_reason = reason
            logger.warning(
                "circuit_breaker_tripped",
                breaker_name=self.name,
                reason=reason,
                trip_count=self.trip_count,
                action="blocking_operations",
                message=f"Circuit breaker '{self.name}' tripped. Reason: {reason}",
            )
        # If already open, maybe update reason if different or just log repeated trip
        elif self.trip_reason != reason:
            logger.warning(
                "circuit_breaker_reason_updated",
                breaker_name=self.name,
                old_reason=self.trip_reason,
                new_reason=reason,
                trip_count=self.trip_count,
                action="reason_updated",
                message=f"Circuit breaker '{self.name}' trip reason updated to: {reason}",
            )
            self.trip_reason = reason
        else:
            logger.warning(
                "circuit_breaker_tripped_again",
                breaker_name=self.name,
                reason=reason,
                trip_count=self.trip_count,
                action="repeated_trip",
                message=(
                    f"Circuit breaker '{self.name}' tripped again for the same reason: {reason}"
                ),
            )
                # Optionally update trip_time on subsequent trips?
                # self.trip_time = datetime.now(timezone.utc)

    def reset(self) -> None:
        """Reset the circuit breaker to allow operations."""
        prev_state = self.state
        self.state = BreakerState.CLOSED
        self.last_reset_time = datetime.now(UTC)

        if prev_state == BreakerState.OPEN:
            trip_duration = (
                (self.last_reset_time - self.trip_time).total_seconds() if self.trip_time else 0
            )
            logger.info(
                "circuit_breaker_reset",
                breaker_name=self.name,
                trip_duration_seconds=round(trip_duration, 1),
                previous_reason=self.trip_reason,
                trip_count=self.trip_count,
                action="breaker_reset",
                message=(
                    f"Circuit breaker '{self.name}' reset after {trip_duration:.1f} seconds. "
                    f"Previous reason: {self.trip_reason}"
                ),
            )

        self.trip_time = None
        self.trip_reason = None

    def allow_operation(self) -> bool:
        """Check if operations are allowed through this breaker.

        Returns:
            True if the operation is allowed, False if blocked

        """
        now = datetime.now(UTC)
        logger.debug(
            "circuit_breaker_checking_operation",
            breaker_name=self.name,
            state=self.state.name,
            current_time=now.isoformat(),
            trip_time=self.trip_time.isoformat() if self.trip_time else None,
            cooldown_seconds=self.cooldown_seconds,
            action="checking_allow_operation",
            message=f"Breaker {self.name}: Checking allow_operation. State={self.state.name}",
        )
        if self.state == BreakerState.OPEN:
            if self.trip_time is None:
                logger.error(
                    "circuit_breaker_inconsistent_state",
                    breaker_name=self.name,
                    state=self.state.name,
                    trip_time=self.trip_time,
                    action="invalid_state_detected",
                    message=f"Breaker {self.name} is OPEN but trip_time is None!",
                )
                return False
            if (now - self.trip_time).total_seconds() >= self.cooldown_seconds:
                self.state = BreakerState.HALF_OPEN
                logger.info(
                    "circuit_breaker_entering_half_open",
                    breaker_name=self.name,
                    previous_state="OPEN",
                    new_state="HALF_OPEN",
                    cooldown_elapsed=True,
                    action="state_transition",
                    message=f"Circuit breaker '{self.name}' entering half-open state for testing",
                )

        # Only allow operations if closed or half-open
        return self.state != BreakerState.OPEN

    def test_recovery(self) -> bool:
        """Test if the system has recovered when in half-open state.

        This method should be called during a test operation when the breaker
        is in half-open state. If it returns True, the breaker will fully close.
        If False, it will reopen with the cooldown period.

        Returns:
            True if recovery was successful, False otherwise

        """
        # Only perform recovery test if in half-open state
        if self.state != BreakerState.HALF_OPEN:
            return False

        # Implement in subclasses to check specific recovery conditions
        recovery_successful = self._check_recovery()

        if recovery_successful:
            self.reset()
            return True
        # Re-trip the breaker with the same reason
        self.trip(f"Recovery failed: {self.trip_reason}")
        return False

    @abstractmethod
    def _check_recovery(self) -> bool:
        """Implement in subclasses to check if system has recovered.

        Returns:
            True if recovery was successful, False otherwise

        """

    @abstractmethod
    def check(
        self,
        *args: object,
        **kwargs: object,  # object required for flexible circuit breaker implementations
    ) -> None:
        """Check if the circuit breaker should trip.

        Implement in subclasses to evaluate specific conditions.
        Should call self.trip() if the breaker should trip.
        """

    def get_status(self) -> dict[str, Any]:
        """Get the current status of this breaker.

        Returns:
            Status as a dictionary

        """
        return {
            "name": self.name,
            "state": self.state.name,
            "trip_count": self.trip_count,
            "trip_time": self.trip_time.isoformat() if self.trip_time else None,
            "trip_reason": self.trip_reason,
            "cooldown_seconds": self.cooldown_seconds,
            "last_reset_time": self.last_reset_time.isoformat() if self.last_reset_time else None,
        }


class VolatilityBreaker(CircuitBreaker):
    """Circuit breaker that trips when asset volatility exceeds thresholds."""

    def __init__(
        self,
        name: str,
        lookback_periods: int = 12,
        volatility_threshold: float = 0.05,  # 5% volatility threshold
        cooldown_seconds: int = 300,
    ) -> None:
        """Initialize the volatility breaker.

        Args:
            name: Identifier for this breaker
            lookback_periods: Number of periods to consider for volatility
            volatility_threshold: Volatility threshold as decimal (e.g., 0.05 = 5%)
            cooldown_seconds: Time to wait before testing if system has recovered

        """
        super().__init__(name, cooldown_seconds)
        self.lookback_periods = lookback_periods
        self.volatility_threshold = volatility_threshold
        self.price_history: list[float] = []

    def add_price(self, price: float) -> None:
        """Add a price point to the history.

        Args:
            price: The current price

        """
        self.price_history.append(price)

        # Keep only the required number of periods
        while len(self.price_history) > self.lookback_periods:
            self.price_history.pop(0)

    def check(self, *args: object, **kwargs: object) -> None:
        """Check if volatility exceeds the threshold.

        Args:
            *args: Positional arguments (expects current_price as first arg if provided)
            **kwargs: Keyword arguments (supports current_price keyword)

        """
        # Extract current_price from args or kwargs
        current_price: float | None = None
        if args:
            current_price = args[0] if isinstance(args[0], int | float) else None
        elif "current_price" in kwargs:
            price_val = kwargs["current_price"]
            current_price = price_val if isinstance(price_val, int | float) else None
        # Only check if we have enough data and are not already tripped
        if self.state == BreakerState.OPEN:
            return

        # Add current price if provided
        if current_price is not None:
            self.add_price(current_price)

        # Need at least 2 prices to calculate volatility
        if len(self.price_history) < 2:
            return

        # Calculate volatility as standard deviation / mean
        mean = sum(self.price_history) / len(self.price_history)
        if mean == 0:
            return

        variance = sum((p - mean) ** 2 for p in self.price_history) / len(self.price_history)
        volatility = (variance**0.5) / mean

        if volatility > self.volatility_threshold:
            self.trip(
                f"Volatility of {volatility:.4f} exceeds threshold "
                f"of {self.volatility_threshold:.4f}",
            )

    def _check_recovery(self) -> bool:
        """Check if volatility has returned to acceptable levels.

        Returns:
            True if volatility is now below threshold, False otherwise

        """
        if len(self.price_history) < 2:
            return False

        mean = sum(self.price_history) / len(self.price_history)
        if mean == 0:
            return False

        variance = sum((p - mean) ** 2 for p in self.price_history) / len(self.price_history)
        volatility = (variance**0.5) / mean

        # Explicitly cast to bool for type safety
        return bool(volatility <= self.volatility_threshold)


class DrawdownBreaker(CircuitBreaker):
    """Circuit breaker that trips when drawdown exceeds thresholds."""

    def __init__(
        self,
        name: str,
        drawdown_threshold: float = 0.10,  # 10% drawdown threshold
        cooldown_seconds: int = 600,
    ) -> None:
        """Initialize the drawdown breaker.

        Args:
            name: Identifier for this breaker
            drawdown_threshold: Maximum allowable drawdown as decimal
            cooldown_seconds: Time to wait before testing if system has recovered

        """
        super().__init__(name, cooldown_seconds)
        self.drawdown_threshold = drawdown_threshold
        self.peak_value: float | None = None
        self.current_value: float | None = None

    def check(self, *args: object, **kwargs: object) -> None:
        """Check if drawdown exceeds the threshold.

        Args:
            *args: Positional arguments (expects current_value as first arg)
            **kwargs: Keyword arguments (supports current_value keyword)

        """
        # Extract current_value from args or kwargs
        current_value: float | None = None
        if args:
            current_value = args[0] if isinstance(args[0], int | float) else None
        elif "current_value" in kwargs:
            val = kwargs["current_value"]
            current_value = val if isinstance(val, int | float) else None

        if current_value is None:
            return
        # Update current value
        self.current_value = current_value

        # Update peak if this is a new high or initial value
        if self.peak_value is None or current_value > self.peak_value:
            self.peak_value = current_value
            return

        # Calculate drawdown
        if self.peak_value == 0:
            return

        drawdown = (self.peak_value - current_value) / self.peak_value

        # Trip if drawdown exceeds threshold
        if drawdown > self.drawdown_threshold:
            self.trip(
                f"Drawdown of {drawdown:.2%} exceeds threshold of {self.drawdown_threshold:.2%}",
            )

    def _check_recovery(self) -> bool:
        """Check if drawdown has returned to acceptable levels.

        Returns:
            True if drawdown is now below threshold, False otherwise

        """
        if self.peak_value is None or self.current_value is None or self.peak_value == 0:
            return False

        drawdown = (self.peak_value - self.current_value) / self.peak_value
        return drawdown <= self.drawdown_threshold


class APIErrorBreaker(CircuitBreaker):
    """Circuit breaker that trips when API errors exceed threshold in a time window."""

    def __init__(
        self,
        name: str,
        error_threshold: int = 3,  # Number of errors to trigger
        window_seconds: int = 60,  # Time window for errors
        cooldown_seconds: int = 300,
    ) -> None:
        """Initialize API error breaker.

        Args:
            name: Identifier for this breaker
            error_threshold: Number of errors to trigger the breaker
            window_seconds: Time window for counting errors
            cooldown_seconds: Time to wait before testing recovery

        """
        super().__init__(name, cooldown_seconds)
        self.error_threshold = error_threshold
        self.window_seconds = window_seconds
        self.errors: list[tuple[datetime, str]] = []
        self.success_count = 0
        self.consecutive_success_count = 0
        self.last_success_time: datetime | None = None

    def record_error(self, error_message: str) -> None:
        """Record an API error occurrence.

        Args:
            error_message: Description of the error

        """
        # Record error with current time
        now = datetime.now(UTC)
        self.errors.append((now, error_message))

        # Reset consecutive success counter since we had an error
        self.consecutive_success_count = 0

        # Prune errors outside the window
        cutoff = now - timedelta(seconds=self.window_seconds)
        self.errors = [e for e in self.errors if e[0] >= cutoff]

        # Check if breaker should trip after recording the error
        self.check()

    def record_success(self) -> None:
        """Record a successful API call.

        This helps track the ratio of successful to failed calls
        and can be used to determine if recovery is appropriate.
        """
        now = datetime.now(UTC)
        self.success_count += 1
        self.consecutive_success_count += 1
        self.last_success_time = now

        # If we're in half-open state and have enough consecutive successes,
        # this could help determine if recovery should happen
        if self.state == BreakerState.HALF_OPEN and self.consecutive_success_count >= max(
            3,
            self.error_threshold,
        ):
            # Consider this a strong signal for recovery
            logger.info(
                "api_breaker_consecutive_successes",
                breaker_name=self.name,
                consecutive_success_count=self.consecutive_success_count,
                state="HALF_OPEN",
                action="recovery_monitoring",
                message=(
                    f"APIErrorBreaker {self.name}: "
                    f"{self.consecutive_success_count} consecutive successful API calls "
                    f"while in half-open state"
                ),
            )

    def check(self, *args: object, **kwargs: object) -> None:
        """Check if the breaker should trip based on recent errors.

        Args:
            args: Positional arguments - first arg should be error_message (str | None)
            kwargs: Keyword arguments

        """
        # Extract error_message from args
        error_message: str | None = None
        if args:
            error_message = args[0] if isinstance(args[0], str | type(None)) else None

        # Record the error if provided
        if error_message is not None:
            self.record_error(error_message)

        # Don't check if the breaker is already open
        if self.state == BreakerState.OPEN:
            return

        now = datetime.now(UTC)
        # Filter to errors in the current window
        cutoff = now - timedelta(seconds=self.window_seconds)
        recent_errors = [e for e in self.errors if e[0] >= cutoff]

        # Trip if threshold exceeded
        if len(recent_errors) >= self.error_threshold:
            # Combine error messages
            combined_error = "\n".join([f"{e[0].isoformat()}: {e[1]}" for e in recent_errors[-3:]])
            self.trip(
                f"Detected {len(recent_errors)} API errors in {self.window_seconds}s window. "
                f"Recent errors: {combined_error}",
            )

    def _check_recovery(self) -> bool:
        """Check if API errors have subsided.

        Returns:
            True if recent error rate is acceptable, False otherwise

        """
        # We could just return True, assuming time has passed and we want to try again
        # But let's also check if there have been errors in the past window
        now = datetime.now(UTC)
        cutoff = now - timedelta(seconds=self.window_seconds)
        recent_errors = [e for e in self.errors if e[0] >= cutoff]

        # Check if the consecutive success count is promising
        success_recovery = self.consecutive_success_count >= max(3, self.error_threshold // 2)

        # Consider both error count and success streak
        if len(recent_errors) < self.error_threshold // 2:
            logger.info(
                "api_breaker_recovery_allowed",
                breaker_name=self.name,
                recent_error_count=len(recent_errors),
                consecutive_success_count=self.consecutive_success_count,
                action="recovery_check",
                message=(
                    f"APIErrorBreaker {self.name} recovery check: "
                    f"{len(recent_errors)} recent errors, "
                    f"{self.consecutive_success_count} consecutive successes - recovery allowed"
                ),
            )
            return True
        if success_recovery:
            logger.info(
                "api_breaker_recovery_allowed_with_errors",
                breaker_name=self.name,
                recent_error_count=len(recent_errors),
                consecutive_success_count=self.consecutive_success_count,
                action="recovery_check",
                message=(
                    f"APIErrorBreaker {self.name} recovery check: "
                    f"Despite {len(recent_errors)} recent errors, has "
                    f"{self.consecutive_success_count} consecutive successes - recovery allowed"
                ),
            )
            return True
        logger.info(
            "api_breaker_recovery_rejected",
            breaker_name=self.name,
            recent_error_count=len(recent_errors),
            consecutive_success_count=self.consecutive_success_count,
            action="recovery_check",
            message=(
                f"APIErrorBreaker {self.name} recovery check: "
                f"{len(recent_errors)} recent errors, "
                f"{self.consecutive_success_count} consecutive successes - recovery rejected"
            ),
        )
        return False


class LiquidityBreaker(CircuitBreaker):
    """Circuit breaker that trips when market liquidity drops below thresholds."""

    def __init__(
        self,
        name: str,
        min_liquidity: float,  # Minimum acceptable liquidity
        cooldown_seconds: int = 300,
    ) -> None:
        """Initialize the liquidity breaker.

        Args:
            name: Identifier for this breaker
            min_liquidity: Minimum acceptable liquidity (e.g., in USD)
            cooldown_seconds: Time to wait before testing if system has recovered

        """
        super().__init__(name, cooldown_seconds)
        self.min_liquidity = min_liquidity
        self.current_liquidity: float | None = None

    def check(self, *args: object, **kwargs: object) -> None:
        """Check if liquidity is below the threshold.

        Args:
            args: Positional arguments - first arg should be current_liquidity (float)
            kwargs: Keyword arguments

        """
        # Extract current_liquidity from args
        if not args or not isinstance(args[0], int | float):
            logger.error(
                "liquidity_breaker_missing_argument",
                breaker_name=self.name,
                required_argument="current_liquidity",
                action="argument_validation",
                message=(
                    f"LiquidityBreaker {self.name}: "
                    f"check() requires current_liquidity as first argument"
                ),
            )
            return

        current_liquidity = float(args[0])
        self.current_liquidity = current_liquidity

        if current_liquidity < self.min_liquidity:
            self.trip(
                f"Liquidity of {current_liquidity} below minimum threshold of {self.min_liquidity}",
            )

    def _check_recovery(self) -> bool:
        """Check if liquidity has returned to acceptable levels.

        Returns:
            True if liquidity is now above threshold, False otherwise

        """
        if self.current_liquidity is None:
            return False

        return self.current_liquidity >= self.min_liquidity


class CircuitBreakerSystem:
    """Manage a collection of circuit breakers and provide a central interface.

    This class provides a central interface for checking and controlling circuit breakers.
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize the circuit breaker system.

        Args:
            config: Application configuration

        """
        self.config = config
        self.breakers: dict[str, CircuitBreaker] = {}
        self.exchange_breakers: dict[
            str,
            dict[str, CircuitBreaker | dict[str, CircuitBreaker]],
        ] = {}
        self.last_status: dict[str, Any] = {}

        # Load configuration
        self._load_config()

    def _load_config(self) -> None:
        """Load circuit breaker configuration."""
        # Clear any existing breakers to allow for re-loading if called multiple times
        self.breakers.clear()
        self.exchange_breakers.clear()
        self.global_api_error_breaker: APIErrorBreaker | None = None  # Initialize

        # === Load Global API Error Breaker ===
        # Check if circuit breakers are enabled in safety_systems
        if not self.config.safety_systems.circuit_breakers.enabled:
            logger.info(
                "circuit_breakers_disabled",
                action="system_configuration",
                message="Circuit breakers are disabled in config",
            )
            return

        # Create a global API error breaker using the global settings
        global_failures = self.config.safety_systems.circuit_breakers.global_consecutive_failures
        global_timeout = self.config.safety_systems.circuit_breakers.global_reset_timeout_sec

        self.global_api_error_breaker = APIErrorBreaker(
            name="global/api_error",
            error_threshold=global_failures,
            window_seconds=60,  # Default window
            cooldown_seconds=global_timeout,
        )
        self.register_breaker(self.global_api_error_breaker)
        logger.info(
            "global_api_error_breaker_initialized",
            breaker_name=self.global_api_error_breaker.name,
            action="system_initialization",
            message=f"Initialized global API error breaker: {self.global_api_error_breaker.name}",
        )

        # === Load Exchange-Specific Breakers ===
        exchanges_config = self.config.exchanges
        exchange_failures = (
            self.config.safety_systems.circuit_breakers.exchange_consecutive_failures
        )
        exchange_timeout = self.config.safety_systems.circuit_breakers.exchange_reset_timeout_sec

        for exchange_id, exchange_config in exchanges_config.items():
            if not exchange_config.enabled:
                logger.debug(
                    "exchange_disabled_skipping_breakers",
                    exchange_id=exchange_id,
                    action="skipping_initialization",
                    message=f"Exchange '{exchange_id}' is disabled, skipping circuit breakers",
                )
                continue

            # Create an API error breaker for this exchange
            exchange_api_breaker = APIErrorBreaker(
                name=f"{exchange_id}/api_error",
                error_threshold=exchange_failures,
                window_seconds=60,  # Default window
                cooldown_seconds=exchange_timeout,
            )
            self.register_breaker(exchange_api_breaker)

            # Store in exchange_breakers structure
            exchange_level_breakers = self.exchange_breakers.setdefault(exchange_id, {})
            exchange_level_breakers["api_errors"] = exchange_api_breaker

            logger.info(
                "api_error_breaker_initialized",
                exchange_id=exchange_id,
                action="breaker_initialization",
                message=f"Initialized API error breaker for exchange '{exchange_id}'",
            )

        logger.info(
            "circuit_breaker_system_initialized",
            breaker_count=len(self.breakers),
            action="system_initialization",
            message=f"Circuit breaker system initialized with {len(self.breakers)} breakers.",
        )

    def register_breaker(self, breaker: CircuitBreaker) -> None:
        """Register a new circuit breaker with the system.

        Args:
            breaker: Circuit breaker to register

        """
        self.breakers[breaker.name] = breaker
        logger.debug(
            "circuit_breaker_registered",
            breaker_name=breaker.name,
            action="breaker_registration",
            message=f"Registered circuit breaker: {breaker.name}",
        )

    def get_breaker(self, name: str) -> CircuitBreaker | None:
        """Get a circuit breaker by name.

        Args:
            name: Name of the breaker

        Returns:
            The circuit breaker, or None if not found

        """
        return self.breakers.get(name)

    def get_exchange_breaker(
        self,
        exchange: str,
        breaker_type: str,
    ) -> CircuitBreaker | dict[str, CircuitBreaker] | None:
        """Get an exchange-specific circuit breaker or a dictionary of symbol-specific breakers.

        Args:
            exchange: Exchange identifier
            breaker_type: Type of breaker (e.g., 'api_errors', 'volatility')

        Returns:
            The circuit breaker, a dict of symbol-specific breakers, or None if not found

        """
        if exchange not in self.exchange_breakers:
            return None
        return self.exchange_breakers[exchange].get(breaker_type)

    def can_execute(self, exchange: str, symbol: str | None = None) -> tuple[bool, str | None]:
        """Check if an operation can be executed for an exchange/symbol or globally.

        Args:
            exchange: Exchange identifier or "global"
            symbol: Optional symbol for symbol-specific checks

        Returns:
            Tuple of (can_execute, reason_if_blocked)

        """
        # Check global breakers first
        global_check_result = self._check_global_breakers()
        if global_check_result[0] is False:
            return global_check_result

        # Check exchange-specific breakers
        exchange_check_result = self._check_exchange_breakers(exchange, symbol)
        if exchange_check_result[0] is False:
            return exchange_check_result

        return True, None  # Allowed if no breakers are OPEN

    def _check_global_breakers(self) -> tuple[bool, str | None]:
        """Check all global circuit breakers."""
        now = datetime.now(UTC)

        for breaker_name, breaker in self.breakers.items():
            if not breaker.allow_operation():
                reason = f"Global breaker '{breaker_name}' is OPEN due to: {breaker.trip_reason}"
                logger.warning(
                    "execution_blocked_by_global_breaker",
                    breaker_name=breaker_name,
                    reason=reason,
                    action="blocking_execution",
                    message=f"Execution blocked: {reason}",
                )
                return False, reason

            # Test recovery for HALF_OPEN global breakers
            if breaker.state == BreakerState.HALF_OPEN and breaker.trip_time is not None:
                cooldown_time = timedelta(seconds=breaker.cooldown_seconds)
                if now >= breaker.trip_time + cooldown_time:
                    if not breaker.test_recovery():
                        reason = f"Global breaker '{breaker_name}' failed recovery test."
                        logger.warning(
                            "execution_blocked_by_recovery_test",
                            breaker_name=breaker_name,
                            reason=reason,
                            action="blocking_execution",
                            message=f"Execution blocked: {reason}",
                        )
                        return False, reason
                    logger.info(
                        "global_breaker_recovered",
                        breaker_name=breaker_name,
                        new_state="CLOSED",
                        action="breaker_recovery",
                        message=f"Global breaker '{breaker_name}' recovered and is now CLOSED.",
                    )

        return True, None

    def _check_exchange_breakers(
        self,
        exchange: str,
        symbol: str | None,
    ) -> tuple[bool, str | None]:
        """Check exchange-specific circuit breakers."""
        if exchange not in self.exchange_breakers:
            return True, None

        for _breaker_type, breaker_item in self.exchange_breakers[exchange].items():
            breakers_to_check = self._get_breakers_to_check(breaker_item, symbol)

            for breaker in breakers_to_check:
                check_result = self._check_individual_breaker(breaker, exchange)
                if check_result[0] is False:
                    return check_result

        return True, None

    def _get_breakers_to_check(
        self,
        breaker_item: CircuitBreaker | dict[str, CircuitBreaker],
        symbol: str | None,
    ) -> list[CircuitBreaker]:
        """Get the list of breakers to check based on the breaker item type."""
        breakers_to_check: list[CircuitBreaker] = []

        if isinstance(breaker_item, CircuitBreaker):
            breakers_to_check.append(breaker_item)
        # This is a dict of symbol-specific breakers
        elif symbol and symbol in breaker_item:
            actual_breaker = breaker_item[symbol]
            breakers_to_check.append(actual_breaker)
        elif not symbol:
            for s_breaker in breaker_item.values():
                breakers_to_check.append(s_breaker)

        return breakers_to_check

    def _check_individual_breaker(
        self,
        breaker: CircuitBreaker,
        exchange: str,
    ) -> tuple[bool, str | None]:
        """Check an individual circuit breaker and handle recovery testing."""
        if not breaker.allow_operation():
            reason = (
                f"Exchange breaker '{breaker.name}' for {exchange} is OPEN due to: "
                f"{breaker.trip_reason}"
            )
            logger.warning(
                "execution_blocked_by_exchange_breaker",
                exchange=exchange,
                breaker_name=breaker.name,
                reason=reason,
                action="blocking_execution",
                message=f"Execution blocked for {exchange}: {reason}",
            )
            return False, reason

        # Test recovery for HALF_OPEN exchange breakers
        if breaker.state == BreakerState.HALF_OPEN and breaker.trip_time is not None:
            now = datetime.now(UTC)
            cooldown_time = timedelta(seconds=breaker.cooldown_seconds)
            if now >= breaker.trip_time + cooldown_time:
                if not breaker.test_recovery():
                    reason = (
                        f"Exchange breaker '{breaker.name}' for {exchange} failed recovery test."
                    )
                    logger.warning(
                        "execution_blocked_by_exchange_recovery",
                        exchange=exchange,
                        breaker_name=breaker.name,
                        reason=reason,
                        action="blocking_execution",
                        message=f"Execution blocked for {exchange}: {reason}",
                    )
                    return False, reason
                logger.info(
                    f"Exchange breaker '{breaker.name}' for {exchange} recovered "
                    f"and is now CLOSED.",
                )

        return True, None

    def record_api_error(self, exchange: str, error_message: str) -> None:
        """Record an API error for the specified exchange and globally.

        Args:
            exchange: Exchange identifier
            error_message: Description of the error

        """
        logger.debug(
            "recording_api_error",
            exchange=exchange,
            error_message=error_message,
            action="error_recording",
            message=f"Recording API error for {exchange}: {error_message}",
        )

        # Record with global breaker if it exists
        global_breaker = getattr(self, "global_api_error_breaker", None)
        if global_breaker and isinstance(global_breaker, APIErrorBreaker):
            global_breaker.record_error(error_message)
            # Check if global breaker tripped
            if not global_breaker.allow_operation():
                logger.critical(
                    f"Global API Error circuit breaker tripped: {global_breaker.trip_reason}",
                )

        # Record with exchange-specific breaker
        exchange_breaker = self.get_exchange_breaker(exchange, "api_errors")
        if exchange_breaker:  # If breaker exists, it's enabled (disabled ones are not loaded)
            # Make sure it's an APIErrorBreaker instance
            if isinstance(exchange_breaker, APIErrorBreaker):
                exchange_breaker.record_error(error_message)
                # Check if exchange-specific breaker tripped
                if not exchange_breaker.allow_operation():
                    logger.critical(
                        f"Exchange-specific API Error circuit breaker "
                        f"'{exchange_breaker.name}' tripped: "
                        f"{exchange_breaker.trip_reason}",
                    )
            else:
                logger.warning(
                    f"Retrieved breaker for '{exchange}/api_errors' is not an APIErrorBreaker. "
                    f"Type: {type(exchange_breaker)}. Cannot record API error.",
                )

    def record_api_success(self, exchange: str, context: str = "") -> None:
        """Record a successful API interaction for an exchange.

        This can help reset error counts or test recovery in HALF_OPEN state.

        Args:
            exchange: Exchange identifier
            context: Optional context about the successful operation

        """
        # Record in exchange-specific breaker
        exchange_breaker_name = f"{exchange}_api_errors"
        if exchange_breaker_name in self.breakers:
            breaker = self.breakers[exchange_breaker_name]
            if isinstance(breaker, APIErrorBreaker):
                # If breaker is open, consider transitioning it
                if breaker.state == BreakerState.OPEN:
                    # Only transition to HALF_OPEN if cooldown period has passed
                    now = datetime.now(UTC)
                    if (
                        breaker.trip_time is not None
                        and (now - breaker.trip_time).total_seconds() >= breaker.cooldown_seconds
                    ):
                        breaker.state = BreakerState.HALF_OPEN
                        logger.info(
                            f"API success recorded: {exchange_breaker_name} transitioning to "
                            f"HALF_OPEN state",
                        )

                # If breaker is in HALF_OPEN, test recovery
                if breaker.state == BreakerState.HALF_OPEN:
                    recovery_success = breaker.test_recovery()
                    if recovery_success:
                        logger.info(
                            f"API success confirmed recovery: {exchange_breaker_name} reset "
                            f"to CLOSED state",
                        )
                    else:
                        # Correctly formatted multi-line f-string
                        logger.info(
                            f"Exchange breaker {exchange_breaker_name} remains in HALF_OPEN after "
                            f"successful check.",
                        )
                        # Keep state HALF_OPEN, reset success count
                        # Commenting out potentially incorrect line
                        # self._recovery_success_counts[exchange_breaker_name] = 0
                else:
                    # Correctly formatted multi-line f-string
                    logger.info(
                        f"API success not sufficient for recovery: "
                        f"{exchange_breaker_name} remains in OPEN state",
                    )
            else:
                # Not OPEN or HALF_OPEN, success doesn't change state
                # Corrected escaping for apostrophe
                logger.debug(
                    f"API success recorded for {exchange} (breaker state {breaker.state.name}), "
                    f"no state change.",
                )

        # Also update global API breaker if present
        # Check if the attribute exists before accessing it
        global_breaker = getattr(self, "global_api_error_breaker", None)
        if global_breaker and isinstance(global_breaker, APIErrorBreaker):
            # Similar logic for global breaker
            if global_breaker.state == BreakerState.HALF_OPEN:
                recovery_success = global_breaker.test_recovery()
                if recovery_success:
                    logger.info(
                        "API success confirmed recovery: global_api_errors reset to CLOSED state",
                    )
                else:
                    # Correctly formatted multi-line f-string
                    logger.info(
                        f"API success not sufficient for recovery: "
                        f"global_api_errors remains in {global_breaker.state.name} state",
                    )
            else:
                # Not OPEN or HALF_OPEN, success doesn't change state
                logger.debug(
                    f"API success recorded for global breaker (state {global_breaker.state.name}), "
                    f"no state change.",
                )

    def record_critical_failure(self, exchange: str, error_message: str) -> None:
        """Record a critical failure that should immediately trip relevant breakers.

        Args:
            exchange: Exchange where the critical failure occurred
            error_message: Description of the critical failure

        """
        logger.error(
            "critical_failure_recorded",
            exchange=exchange,
            error_message=error_message,
            action="critical_failure",
            message=f"CRITICAL FAILURE for {exchange}: {error_message}",
        )

        # Trip the exchange-specific API breaker if it exists
        exchange_api_breaker_item = self.get_exchange_breaker(exchange, "api_errors")
        if exchange_api_breaker_item and isinstance(exchange_api_breaker_item, CircuitBreaker):
            exchange_api_breaker_item.trip(f"Critical failure: {error_message}")
        elif exchange_api_breaker_item:
            logger.warning(
                f"Critical failure on {exchange}, but api_errors breaker is a dict, "
                f"not tripping individual symbol breakers here.",
            )

        # Also trip the global API breaker to ensure all operations are affected
        global_api_breaker = getattr(self, "global_api_error_breaker", None)
        if global_api_breaker:
            global_api_breaker.trip(f"Critical failure on {exchange}: {error_message}")

        # If we have any exchange-specific volatility breakers, trip those too
        # as a critical failure might indicate market conditions are unstable
        for _breaker_key, breaker_item_val in self.exchange_breakers.get(
            exchange,
            {},
        ).items():  # Renamed breaker_type to _breaker_key
            if isinstance(breaker_item_val, VolatilityBreaker):
                breaker_item_val.trip(
                    f"Critical failure triggered volatility breaker: {error_message}",
                )
            elif isinstance(
                breaker_item_val,
                dict,
            ):  # It's a dict of symbol-specific VolatilityBreakers
                for sym_breaker in breaker_item_val.values():
                    if isinstance(sym_breaker, VolatilityBreaker):
                        sym_breaker.trip(
                            f"Critical failure triggered volatility breaker for symbol: "
                            f"{error_message}",
                        )

    def update_price(self, exchange: str, symbol: str, price: float) -> None:
        """Update price data for volatility monitoring.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            price: Current price

        """
        vol_breaker = self.get_exchange_breaker(exchange, f"{symbol}_volatility")
        if vol_breaker and isinstance(vol_breaker, VolatilityBreaker):
            vol_breaker.add_price(price)
            vol_breaker.check()

    def update_portfolio_value(self, exchange: str, value: float) -> None:
        """Update portfolio value for drawdown monitoring.

        Args:
            exchange: Exchange identifier
            value: Current portfolio value

        """
        draw_breaker = self.get_exchange_breaker(exchange, "drawdown")
        if draw_breaker and isinstance(draw_breaker, DrawdownBreaker):
            draw_breaker.check(value)

    def update_liquidity(self, exchange: str, symbol: str, liquidity: float) -> None:
        """Update market liquidity for monitoring.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            liquidity: Current market liquidity

        """
        liq_breaker = self.get_exchange_breaker(exchange, f"{symbol}_liquidity")
        if liq_breaker and isinstance(liq_breaker, LiquidityBreaker):
            liq_breaker.check(liquidity)

    def reset_breaker(self, name: str) -> bool:
        """Reset a circuit breaker by name.

        Args:
            name: Name of the breaker to reset

        Returns:
            True if the breaker was found and reset, False otherwise

        """
        breaker = self.breakers.get(name)
        if breaker:
            was_open = breaker.state == BreakerState.OPEN
            breaker.reset()
            # Specific handling for APIErrorBreaker to also reset its internal error tracking
            if isinstance(breaker, APIErrorBreaker):
                breaker.errors.clear()  # Corrected: use .errors attribute
            logger.info(
                f"Breaker '{name}' reset. "
                f"Previous state: {was_open}, New state: {breaker.state.name}",
            )
            # Log if it was previously open and now closed
            if was_open and breaker.state == BreakerState.CLOSED:
                logger.info(
                    "breaker_transition_complete",
                    breaker_name=name,
                    previous_state="OPEN",
                    new_state="CLOSED",
                    action="state_transition",
                    message=f"Breaker '{name}' successfully transitioned from OPEN to CLOSED.",
                )
            return True
        logger.warning(
            "breaker_not_found_for_reset",
            breaker_name=name,
            action="reset_attempt",
            message=f"Breaker '{name}' not found for reset.",
        )
        return False

    def reset_exchange_breakers(self, exchange: str) -> int:
        """Reset all circuit breakers for an exchange.

        Args:
            exchange: Exchange identifier

        Returns:
            Number of breakers reset

        """
        exchange_level_breakers_map = self.exchange_breakers.get(exchange)
        if not exchange_level_breakers_map:
            logger.info(
                "no_exchange_breakers_found",
                exchange=exchange,
                action="reset_attempt",
                message=f"No breakers found for exchange '{exchange}' to reset.",
            )
            return 0

        reset_count = 0
        # Iterate over a copy of values in case of future modifications during iteration
        for breaker_or_symbol_map_item in list(exchange_level_breakers_map.values()):
            if isinstance(breaker_or_symbol_map_item, CircuitBreaker):
                # This is a non-symbol-specific breaker for the exchange
                was_open = breaker_or_symbol_map_item.state == BreakerState.OPEN
                breaker_or_symbol_map_item.reset()
                if isinstance(breaker_or_symbol_map_item, APIErrorBreaker):
                    breaker_or_symbol_map_item.errors.clear()
                logger.info(
                    f"Exchange breaker '{breaker_or_symbol_map_item.name}' reset. "
                    f"Was open: {was_open}, New state: {breaker_or_symbol_map_item.state.name}",
                )
                reset_count += 1
            else:  # If not CircuitBreaker, it must be dict[str, CircuitBreaker]
                # This is a map of symbol-specific breakers (e.g., for volatility, liquidity)
                for symbol_key, specific_breaker_item in breaker_or_symbol_map_item.items():
                    # specific_breaker_item is known to be CircuitBreaker here due to the structure
                    was_open = specific_breaker_item.state == BreakerState.OPEN
                    specific_breaker_item.reset()
                    if isinstance(specific_breaker_item, APIErrorBreaker):
                        specific_breaker_item.errors.clear()
                    logger.info(
                        f"Symbol-specific breaker '{specific_breaker_item.name}' for exchange "
                        f"'{exchange}', symbol '{symbol_key}' reset. "
                        f"Was open: {was_open}, New state: {specific_breaker_item.state.name}",
                    )
                    reset_count += 1

        if reset_count > 0:
            logger.info(
                "exchange_breakers_reset_complete",
                exchange=exchange,
                reset_count=reset_count,
                action="reset_complete",
                message=f"Reset {reset_count} breakers for exchange '{exchange}'.",
            )
        else:
            logger.info(
                f"No breakers were actively reset for exchange '{exchange}' "
                f"(they might have been already closed or map was empty).",
            )
        return reset_count

    def update_critical_systems_status(self, status_updates: dict[str, bool]) -> None:
        """Update status for critical external systems (e.g., database, message queue).

        This method is intended to be called by monitoring components that check these systems.
        If a critical system is reported as down, relevant breakers might trip.

        Args:
            status_updates: A dictionary where keys are system names (e.g., "database",
                            "message_queue") and values are booleans (True for healthy,
                            False for unhealthy).

        """
        for system_name, is_healthy in status_updates.items():
            breaker_name = f"critical_system:{system_name}"
            breaker = self.get_breaker(breaker_name)

            if not breaker:  # Ensure breaker exists
                logger.warning(
                    f"No breaker found for critical system: {system_name}. Cannot update status.",
                )
                continue

            if not is_healthy:
                if breaker.state != BreakerState.OPEN:
                    breaker.trip(f"Critical system '{system_name}' reported as unhealthy.")
                    logger.critical(
                        f"Critical system breaker '{breaker_name}' tripped due to "
                        f"{system_name} unhealthiness.",
                    )
            elif breaker.state == BreakerState.OPEN or breaker.state == BreakerState.HALF_OPEN:
                # If the system is reported healthy and breaker was open/half-open,
                # attempt reset.
                # For critical systems, we might reset more assertively if health is confirmed.
                logger.info(
                    f"Critical system '{system_name}' reported as healthy. "
                    f"Resetting breaker '{breaker_name}'.",
                )
                breaker.reset()
                # For APIErrorBreaker types, ensure internal error counts are also cleared
                if isinstance(breaker, APIErrorBreaker):
                    breaker.errors.clear()  # Clear past errors

    def reset_all_breakers(self) -> int:
        """Reset all circuit breakers registered in the system.

        Returns:
            Number of breakers reset

        """
        reset_count = 0
        for breaker in self.breakers.values():
            current_state_before_reset = breaker.state
            breaker.reset()
            if isinstance(breaker, APIErrorBreaker):
                breaker.errors.clear()  # Corrected: use .errors attribute

            reset_count += 1
            logger.info(
                f"Breaker '{breaker.name}' reset. "
                f"Previous state: {current_state_before_reset.name}, "
                f"New state: {breaker.state.name}",
            )
        if reset_count > 0:
            logger.info(
                "all_breakers_reset_complete",
                reset_count=reset_count,
                action="mass_reset_complete",
                message=f"Reset {reset_count} total breakers.",
            )
        else:
            logger.info("No breakers found to reset.")
        return reset_count

    def check_all_breakers(self) -> None:
        """Check all circuit breakers and update their states based on their checks."""
        for breaker_key, breaker_instance in self.breakers.items():
            # Assuming breaker_instance is now the actual breaker, not a list/dict
            # The original code `for breaker in breaker:` suggested nesting which seems incorrect.
            # If `self.breakers[key]` can hold multiple breakers, the structure needs review.
            # For now, assume direct check on the registered instance.
            logger.debug(
                "checking_breaker",
                breaker_key=breaker_key,
                action="breaker_check",
                message=f"Checking breaker: {breaker_key}",
            )
            breaker_instance.check()

    def _create_breaker_from_config(
        self,
        breaker_name_or_key: str,
        breaker_specific_config: dict[str, Any],
        exchange_name_context: str,  # Used for default cooldown lookup if override not provided
        breaker_class: type[CircuitBreaker],
        symbol: str | None = None,
        default_cooldown_override: int | None = None,  # New parameter for explicit default
    ) -> CircuitBreaker | None:
        name = breaker_name_or_key
        try:
            # Determine cooldown
            cooldown = self._determine_cooldown(
                breaker_specific_config,
                default_cooldown_override,
                name,
            )

            # Create the appropriate breaker instance
            return self._instantiate_breaker(breaker_class, name, breaker_specific_config, cooldown)

        except KeyError as e:
            logger.error(
                "breaker_config_key_error",
                breaker_name=name,
                missing_key=str(e),
                config_details=breaker_specific_config,
                action="configuration_error",
                message=(
                    f"Configuration key error for "
                    f"{name} (expected key: {e}). Details: {breaker_specific_config}"
                ),
            )
            return None
        except ValueError as e:
            logger.error(
                "breaker_config_value_error",
                breaker_name=name,
                error=str(e),
                config_details=(breaker_specific_config),
                action="configuration_error",
                message=(
                    f"Configuration value error for "
                    f"{name} (e.g., type conversion failed: {e}). "
                    f"Details: {breaker_specific_config}"
                ),
            )
            return None
        except Exception as e:
            logger.error(
                "breaker_creation_error",
                breaker_name=name,
                breaker_class=breaker_class.__name__,
                error=str(e),
                config_details=breaker_specific_config,
                action="creation_error",
                message=(
                    f"Generic error creating breaker {name} of type {breaker_class.__name__}: "
                    f"{e} (Config: {breaker_specific_config})"
                ),
            )
            return None

    def _determine_cooldown(
        self,
        breaker_specific_config: dict[str, Any],
        default_cooldown_override: int | None,
        name: str,
    ) -> int:
        """Determine the cooldown value for a breaker from config."""
        # 1. From this specific breaker's config (`breaker_specific_config`)
        cooldown_raw = breaker_specific_config.get("cooldown_seconds")
        if cooldown_raw is None and default_cooldown_override is not None:
            cooldown_raw = default_cooldown_override

        # If still None, use a hardcoded default
        if cooldown_raw is None:
            cooldown_raw = 300  # Default 5 minutes

        if not isinstance(cooldown_raw, int | float | str):
            logger.error(
                "breaker_cooldown_type_error",
                breaker_name=name,
                cooldown_value=cooldown_raw,
                actual_type=type(cooldown_raw).__name__,
                default_cooldown=300,
                action="configuration_error",
                message=(
                    f"Cooldown value for breaker '{name}' is of an unexpected type: "
                    f"{cooldown_raw} (type: {type(cooldown_raw)}). Using system default 300s."
                ),
            )
            cooldown_raw = 300  # Fallback

        try:
            cooldown = int(float(str(cooldown_raw)))  # Robust parsing: str -> float -> int
        except ValueError:
            logger.error(
                "breaker_cooldown_parse_error",
                breaker_name=name,
                cooldown_value=str(cooldown_raw),
                default_cooldown=300,
                action="configuration_error",
                message=(
                    f"Could not parse cooldown value '{cooldown_raw}' for "
                    f"breaker '{name}'. Using system default 300s."
                ),
            )
            cooldown = 300

        return cooldown

    def _instantiate_breaker(
        self,
        breaker_class: type[CircuitBreaker],
        name: str,
        breaker_specific_config: dict[str, Any],
        cooldown: int,
    ) -> CircuitBreaker | None:
        """Instantiate the appropriate breaker based on class type."""
        if breaker_class == APIErrorBreaker:
            return self._create_api_error_breaker(name, breaker_specific_config, cooldown)
        if breaker_class == VolatilityBreaker:
            return self._create_volatility_breaker(name, breaker_specific_config, cooldown)
        if breaker_class == DrawdownBreaker:
            return self._create_drawdown_breaker(name, breaker_specific_config, cooldown)
        if breaker_class == LiquidityBreaker:
            return self._create_liquidity_breaker(name, breaker_specific_config, cooldown)
        logger.error(
            "unknown_breaker_class",
            breaker_class=breaker_class.__name__,
            config_key=name,
            action="creation_error",
            message=(
                f"Attempted to create unknown or unhandled breaker class: "
                f"{breaker_class.__name__} for "
                f"config key '{name}'"
            ),
        )
        return None

    def _create_api_error_breaker(
        self,
        name: str,
        breaker_specific_config: dict[str, Any],
        cooldown: int,
    ) -> APIErrorBreaker:
        """Create an API error breaker from config."""
        error_threshold = int(breaker_specific_config.get("error_threshold", 5))
        window_seconds_val = breaker_specific_config.get(
            "time_window_seconds",
            breaker_specific_config.get("window_seconds", 60),
        )
        window_seconds = int(window_seconds_val)
        return APIErrorBreaker(name, error_threshold, window_seconds, cooldown)

    def _create_volatility_breaker(
        self,
        name: str,
        breaker_specific_config: dict[str, Any],
        cooldown: int,
    ) -> VolatilityBreaker:
        """Create a volatility breaker from config."""
        lookback_periods = int(breaker_specific_config.get("lookback_periods", 12))
        volatility_threshold = float(
            breaker_specific_config.get("volatility_threshold", 0.05),
        )
        return VolatilityBreaker(name, lookback_periods, volatility_threshold, cooldown)

    def _create_drawdown_breaker(
        self,
        name: str,
        breaker_specific_config: dict[str, Any],
        cooldown: int,
    ) -> DrawdownBreaker:
        """Create a drawdown breaker from config."""
        drawdown_threshold_val = breaker_specific_config.get(
            "max_drawdown_percentage",
            breaker_specific_config.get("drawdown_threshold", 0.10),
        )
        drawdown_threshold = float(drawdown_threshold_val)
        return DrawdownBreaker(name, drawdown_threshold, cooldown)

    def _create_liquidity_breaker(
        self,
        name: str,
        breaker_specific_config: dict[str, Any],
        cooldown: int,
    ) -> LiquidityBreaker:
        """Create a liquidity breaker from config."""
        min_liquidity_val = breaker_specific_config.get(
            "min_liquidity_usd",
            breaker_specific_config.get("min_liquidity", 1000.0),
        )
        min_liquidity = float(min_liquidity_val)
        return LiquidityBreaker(name, min_liquidity, cooldown)
