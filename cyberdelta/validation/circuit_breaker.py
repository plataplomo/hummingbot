"""
Circuit Breaker System for the CyberDeltaEngine.

This module provides circuit breakers that can halt trading operations
when abnormal conditions are detected, preventing cascading failures
and limiting potential losses.
"""

import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from typing import Any

from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)


# Add the custom exception class
class CircuitBreakerTrippedError(Exception):
    """Custom exception raised when a circuit breaker prevents an operation."""

    pass


class BreakerState(Enum):
    """State of a circuit breaker."""

    CLOSED = auto()  # Normal operation, allowing trades
    OPEN = auto()  # Tripped, blocking trades
    HALF_OPEN = auto()  # Testing if system has recovered


class CircuitBreaker(ABC):
    """
    Base abstract class for all circuit breakers.

    Circuit breakers monitor specific conditions and "trip" (open)
    when those conditions indicate problems.
    """

    def __init__(self, name: str, cooldown_seconds: int = 300) -> None:
        """
        Initialize the circuit breaker.

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
        """
        Trip the circuit breaker, preventing further operations.

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
            logger.warning(f"Circuit breaker '{self.name}' tripped. Reason: {reason}")
        else:
            # If already open, maybe update reason if different or just log repeated trip
            if self.trip_reason != reason:
                logger.warning(f"Circuit breaker '{self.name}' trip reason updated to: {reason}")
                self.trip_reason = reason
            else:
                logger.warning(
                    f"Circuit breaker '{self.name}' tripped again for the same reason: {reason}"
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
                f"Circuit breaker '{self.name}' reset after {trip_duration:.1f} seconds. "
                f"Previous reason: {self.trip_reason}"
            )

        self.trip_time = None
        self.trip_reason = None

    def allow_operation(self) -> bool:
        """
        Check if operations are allowed through this breaker.

        Returns:
            True if the operation is allowed, False if blocked
        """
        now = datetime.now(UTC)
        logger.info(
            f"Breaker {self.name}: Checking allow_operation. State={self.state.name}, "
            f"Now={now}, TripTime={self.trip_time}, Cooldown={self.cooldown_seconds}"
        )
        if self.state == BreakerState.OPEN:
            if self.trip_time is None:
                logger.error(f"Breaker {self.name} is OPEN but trip_time is None!")
                return False
            if (now - self.trip_time).total_seconds() >= self.cooldown_seconds:
                self.state = BreakerState.HALF_OPEN
                logger.info(f"Circuit breaker '{self.name}' entering half-open state for testing")

        # Only allow operations if closed or half-open
        return self.state != BreakerState.OPEN

    def test_recovery(self) -> bool:
        """
        Test if the system has recovered when in half-open state.

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
        else:
            # Re-trip the breaker with the same reason
            self.trip(f"Recovery failed: {self.trip_reason}")
            return False

    @abstractmethod
    def _check_recovery(self) -> bool:
        """
        Implement in subclasses to check if system has recovered.

        Returns:
            True if recovery was successful, False otherwise
        """
        pass

    @abstractmethod
    def check(self, *args: Any, **kwargs: Any) -> None:
        """
        Check if the circuit breaker should trip.

        Implement in subclasses to evaluate specific conditions.
        Should call self.trip() if the breaker should trip.
        """
        pass

    def get_status(self) -> dict[str, Any]:
        """
        Get the current status of this breaker.

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
    """
    Circuit breaker that trips when asset volatility exceeds thresholds.
    """

    def __init__(
        self,
        name: str,
        lookback_periods: int = 12,
        volatility_threshold: float = 0.05,  # 5% volatility threshold
        cooldown_seconds: int = 300,
    ) -> None:
        """
        Initialize the volatility breaker.

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
        """
        Add a price point to the history.

        Args:
            price: The current price
        """
        self.price_history.append(price)

        # Keep only the required number of periods
        while len(self.price_history) > self.lookback_periods:
            self.price_history.pop(0)

    def check(self, current_price: float | None = None) -> None:
        """
        Check if volatility exceeds the threshold.

        Args:
            current_price: Current price to add to history before checking
        """
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
                f"of {self.volatility_threshold:.4f}"
            )

    def _check_recovery(self) -> bool:
        """
        Check if volatility has returned to acceptable levels.

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
    """
    Circuit breaker that trips when drawdown exceeds thresholds.
    """

    def __init__(
        self,
        name: str,
        drawdown_threshold: float = 0.10,  # 10% drawdown threshold
        cooldown_seconds: int = 600,
    ) -> None:
        """
        Initialize the drawdown breaker.

        Args:
            name: Identifier for this breaker
            drawdown_threshold: Maximum allowable drawdown as decimal
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        super().__init__(name, cooldown_seconds)
        self.drawdown_threshold = drawdown_threshold
        self.peak_value: float | None = None
        self.current_value: float | None = None

    def check(self, current_value: float) -> None:
        """
        Check if drawdown exceeds the threshold.

        Args:
            current_value: Current portfolio/asset value to check
        """
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
                f"Drawdown of {drawdown:.2%} exceeds threshold of {self.drawdown_threshold:.2%}"
            )

    def _check_recovery(self) -> bool:
        """
        Check if drawdown has returned to acceptable levels.

        Returns:
            True if drawdown is now below threshold, False otherwise
        """
        if self.peak_value is None or self.current_value is None or self.peak_value == 0:
            return False

        drawdown = (self.peak_value - self.current_value) / self.peak_value
        return drawdown <= self.drawdown_threshold


class APIErrorBreaker(CircuitBreaker):
    """
    Circuit breaker that trips when API errors exceed threshold in a time window.
    """

    def __init__(
        self,
        name: str,
        error_threshold: int = 3,  # Number of errors to trigger
        window_seconds: int = 60,  # Time window for errors
        cooldown_seconds: int = 300,
    ) -> None:
        """
        Initialize API error breaker.

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
        """
        Record an API error occurrence.

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

    def record_success(self) -> None:
        """
        Record a successful API call.

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
            3, self.error_threshold
        ):
            # Consider this a strong signal for recovery
            logger.info(
                f"APIErrorBreaker {self.name}: {self.consecutive_success_count} consecutive "
                f"successful API calls while in half-open state"
            )

    def check(self, error_message: str | None = None) -> None:
        """
        Check if the breaker should trip based on recent errors.

        Args:
            error_message: Optional error message to record during the check
        """
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
                f"Recent errors: {combined_error}"
            )

    def _check_recovery(self) -> bool:
        """
        Check if API errors have subsided.

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
                f"APIErrorBreaker {self.name} recovery check: {len(recent_errors)} recent errors, "
                f"{self.consecutive_success_count} consecutive successes - recovery allowed"
            )
            return True
        elif success_recovery:
            logger.info(
                f"APIErrorBreaker {self.name} recovery check: Despite {len(recent_errors)} recent errors, "
                f"has {self.consecutive_success_count} consecutive successes - recovery allowed"
            )
            return True
        else:
            logger.info(
                f"APIErrorBreaker {self.name} recovery check: {len(recent_errors)} recent errors, "
                f"{self.consecutive_success_count} consecutive successes - recovery rejected"
            )
            return False


class LiquidityBreaker(CircuitBreaker):
    """
    Circuit breaker that trips when market liquidity drops below thresholds.
    """

    def __init__(
        self,
        name: str,
        min_liquidity: float,  # Minimum acceptable liquidity
        cooldown_seconds: int = 300,
    ) -> None:
        """
        Initialize the liquidity breaker.

        Args:
            name: Identifier for this breaker
            min_liquidity: Minimum acceptable liquidity (e.g., in USD)
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        super().__init__(name, cooldown_seconds)
        self.min_liquidity = min_liquidity
        self.current_liquidity: float | None = None

    def check(self, current_liquidity: float) -> None:
        """
        Check if liquidity is below the threshold.

        Args:
            current_liquidity: Current market liquidity
        """
        self.current_liquidity = current_liquidity

        if current_liquidity < self.min_liquidity:
            self.trip(
                f"Liquidity of {current_liquidity} below minimum threshold of {self.min_liquidity}"
            )

    def _check_recovery(self) -> bool:
        """
        Check if liquidity has returned to acceptable levels.

        Returns:
            True if liquidity is now above threshold, False otherwise
        """
        if self.current_liquidity is None:
            return False

        return self.current_liquidity >= self.min_liquidity


class CircuitBreakerSystem:
    """
    Manages a collection of circuit breakers and provides a central
    interface for checking and controlling them.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize the circuit breaker system.

        Args:
            config: Application configuration
        """
        self.config = config
        self.breakers: dict[str, CircuitBreaker] = {}
        self.exchange_breakers: dict[str, dict[str, CircuitBreaker]] = {}
        self.last_status: dict[str, Any] = {}

        # Load configuration
        self._load_config()

    def _load_config(self) -> None:
        """Load circuit breaker configuration."""
        # === ADDED: Load Global API Error ===
        global_api_config_path = "validation.circuit_breaker.global.api_errors"
        global_api_config = self.config.get(global_api_config_path)

        if isinstance(global_api_config, dict):
            try:
                # Add type ignores for config.get() results
                threshold_val = global_api_config.get("error_threshold", 3)  # type: ignore [var-annotated]
                window_val = global_api_config.get("window_seconds", 60)  # type: ignore [var-annotated]
                cooldown_val = global_api_config.get("cooldown_seconds", 300)  # type: ignore [var-annotated]
                enabled_val = global_api_config.get("enabled", True)  # type: ignore [var-annotated]

                # Validate types
                threshold = (
                    int(threshold_val) if isinstance(threshold_val, (int, float, str)) else 3
                )
                window = int(window_val) if isinstance(window_val, (int, float, str)) else 60
                cooldown = int(cooldown_val) if isinstance(cooldown_val, (int, float, str)) else 300
                enabled = bool(enabled_val) if isinstance(enabled_val, bool) else True

                if enabled:
                    self.global_api_error_breaker = APIErrorBreaker(
                        name="global_api_errors",
                        error_threshold=threshold,
                        window_seconds=window,
                        cooldown_seconds=cooldown,
                    )
                    logger.info("Initialized global API error breaker.")
                else:
                    logger.info("Global API error breaker is disabled in config.")
            except (ValueError, TypeError) as e:
                logger.error(
                    f"Invalid config for global API error breaker at '{global_api_config_path}': {e}"
                )
        elif global_api_config is not None:
            logger.warning(
                f"Expected dict for global API config at '{global_api_config_path}', got {type(global_api_config)}. Skipping."
            )

        # Load exchange-specific breakers
        exchanges_config = self.config.get("exchanges")
        if not isinstance(exchanges_config, dict):
            logger.warning(
                "No 'exchanges' config found or it is not a dictionary. Cannot load exchange breakers."
            )
            return

        exchange_id: str
        exchange_config: dict[str, Any]
        # Ignore assignment type mismatch due to unknown items type
        for exchange_id, exchange_config in exchanges_config.items():  # type: ignore [assignment]
            if not isinstance(exchange_config, dict):
                logger.warning(f"Config for exchange '{exchange_id}' is not a dict. Skipping.")
                continue

            breakers_config_path = f"exchanges.{exchange_id}.validation.circuit_breaker"
            breakers_config = self.config.get(breakers_config_path)
            if not isinstance(breakers_config, dict):
                logger.debug(
                    f"No circuit breaker config found for exchange '{exchange_id}' at '{breakers_config_path}'."
                )
                continue

            # Example: Load API Error Breaker for the exchange
            api_error_config = breakers_config.get("api_errors")  # type: ignore [assignment]
            if isinstance(api_error_config, dict):
                try:
                    # Add type ignores for config.get() results
                    threshold_val = api_error_config.get("error_threshold", 5)  # type: ignore [var-annotated]
                    window_val = api_error_config.get("window_seconds", 120)  # type: ignore [var-annotated]
                    cooldown_val = api_error_config.get("cooldown_seconds", 600)  # type: ignore [var-annotated]
                    enabled_val = api_error_config.get("enabled", True)  # type: ignore [var-annotated]

                    threshold = (
                        int(threshold_val) if isinstance(threshold_val, (int, float, str)) else 5
                    )
                    window = int(window_val) if isinstance(window_val, (int, float, str)) else 120
                    cooldown = (
                        int(cooldown_val) if isinstance(cooldown_val, (int, float, str)) else 600
                    )
                    enabled = bool(enabled_val) if isinstance(enabled_val, bool) else True

                    if enabled:
                        breaker_name = f"{exchange_id}_api_errors"
                        api_breaker = APIErrorBreaker(
                            name=breaker_name,
                            error_threshold=threshold,
                            window_seconds=window,
                            cooldown_seconds=cooldown,
                        )
                        self.register_breaker(api_breaker)
                        # Ignore key type for setdefault
                        self.exchange_breakers.setdefault(exchange_id, {})["api_errors"] = (
                            api_breaker  # type: ignore [call-overload]
                        )
                        logger.info(f"Initialized API error breaker for {exchange_id}.")
                    else:
                        logger.info(f"API error breaker for {exchange_id} is disabled.")
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid config for {exchange_id} API error breaker: {e}")
            elif api_error_config is not None:
                # Ignore unknown type in logger argument
                logger.warning(
                    f"Expected dict for {exchange_id} API error breaker config, got {type(api_error_config)}. Skipping."  # type: ignore [arg-type]
                )

            # Example placeholder for VolatilityBreaker
            vol_config = breakers_config.get("volatility")  # type: ignore [assignment]
            if isinstance(vol_config, dict) and bool(vol_config.get("enabled", False)):  # type: ignore [union-attr]
                try:
                    # Add type ignores for config.get() results
                    lookback_val = vol_config.get("lookback_periods", 12)  # type: ignore [union-attr, var-annotated]
                    vol_threshold_val = vol_config.get("volatility_threshold", 0.05)  # type: ignore [union-attr, var-annotated]
                    cooldown_val = vol_config.get("cooldown_seconds", 300)  # type: ignore [union-attr, var-annotated]

                    lookback = (
                        int(lookback_val) if isinstance(lookback_val, (int, float, str)) else 12
                    )
                    vol_threshold = (
                        float(vol_threshold_val)
                        if isinstance(vol_threshold_val, (int, float, str))
                        else 0.05
                    )
                    cooldown = (
                        int(cooldown_val) if isinstance(cooldown_val, int | float | str) else 300
                    )

                    breaker_name = f"{exchange_id}_volatility"
                    vol_breaker = VolatilityBreaker(
                        name=breaker_name,
                        lookback_periods=lookback,
                        volatility_threshold=vol_threshold,
                        cooldown_seconds=cooldown,
                    )
                    self.register_breaker(vol_breaker)
                    # Ignore key type for setdefault
                    self.exchange_breakers.setdefault(exchange_id, {})["volatility"] = vol_breaker  # type: ignore [call-overload]
                    logger.info(f"Initialized Volatility breaker for {exchange_id}.")
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid config for {exchange_id} Volatility breaker: {e}")

            # Corrected Drawdown Breaker loading logic
            drawdown_config = breakers_config.get("drawdown")  # type: ignore [assignment]
            if isinstance(drawdown_config, dict) and bool(drawdown_config.get("enabled", False)):  # type: ignore [union-attr]
                try:
                    # DrawdownBreaker expects float threshold
                    # Add type ignores for config.get() results
                    threshold_raw = drawdown_config.get("drawdown_threshold", 0.10)  # type: ignore [union-attr, var-annotated]
                    cooldown_raw = drawdown_config.get("cooldown_seconds", 600)  # type: ignore [union-attr, var-annotated]

                    # Ignore unknown types for float/int conversion
                    threshold = float(threshold_raw)  # type: ignore [arg-type]
                    cooldown = int(cooldown_raw)  # type: ignore [arg-type]

                    breaker_name = f"{exchange_id}_drawdown"
                    dd_breaker = DrawdownBreaker(
                        name=breaker_name,
                        drawdown_threshold=threshold,  # Pass float
                        cooldown_seconds=cooldown,
                    )
                    self.register_breaker(dd_breaker)
                    # Ignore key type for setdefault
                    self.exchange_breakers.setdefault(exchange_id, {})["drawdown"] = dd_breaker  # type: ignore [call-overload]
                    logger.info(f"Initialized Drawdown breaker for {exchange_id}.")
                except (ValueError, TypeError) as e:
                    logger.error(f"Invalid config for {exchange_id} Drawdown breaker: {e}")

            # TODO: Add loading logic for Liquidity breaker

        logger.info(f"Initialized circuit breakers for {len(self.exchange_breakers)} exchanges")
        logger.info(f"Exchange Breakers Content: {self.exchange_breakers}")

    def register_breaker(self, breaker: CircuitBreaker) -> None:
        """
        Register a new circuit breaker with the system.

        Args:
            breaker: Circuit breaker to register
        """
        self.breakers[breaker.name] = breaker
        logger.debug(f"Registered circuit breaker: {breaker.name}")

    def get_breaker(self, name: str) -> CircuitBreaker | None:
        """
        Get a circuit breaker by name.

        Args:
            name: Name of the breaker

        Returns:
            The circuit breaker, or None if not found
        """
        return self.breakers.get(name)

    def get_exchange_breaker(self, exchange: str, breaker_type: str) -> CircuitBreaker | None:
        """
        Get an exchange-specific circuit breaker.

        Args:
            exchange: Exchange identifier
            breaker_type: Type of breaker (e.g., 'api_errors', 'BTC_volatility')

        Returns:
            The circuit breaker, or None if not found
        """
        if exchange not in self.exchange_breakers:
            return None

        return self.exchange_breakers[exchange].get(breaker_type)

    def can_execute(self, exchange: str, symbol: str | None = None) -> tuple[bool, str | None]:
        """
        Check if an operation can be executed for an exchange/symbol or globally.

        Args:
            exchange: Exchange identifier or "global"
            symbol: Optional symbol for symbol-specific checks

        Returns:
            Tuple of (can_execute, reason_if_blocked)
        """
        now = datetime.now(UTC)

        # Check global breakers
        for breaker_name, breaker in self.breakers.items():
            if not breaker.allow_operation():
                reason = f"Global breaker '{breaker_name}' is OPEN due to: {breaker.trip_reason}"
                logger.warning(f"Execution blocked: {reason}")
                return False, reason
            # Test recovery for HALF_OPEN global breakers
            if breaker.state == BreakerState.HALF_OPEN and breaker.trip_time is not None:
                cooldown_time = timedelta(seconds=breaker.cooldown_seconds)
                if now >= breaker.trip_time + cooldown_time:
                    if not breaker.test_recovery():
                        reason = f"Global breaker '{breaker_name}' failed recovery test."
                        logger.warning(f"Execution blocked: {reason}")
                        return False, reason
                    else:
                        logger.info(f"Global breaker '{breaker_name}' recovered and is now CLOSED.")

        # Check exchange-specific breakers
        if exchange in self.exchange_breakers:
            for breaker_type, breaker in self.exchange_breakers[exchange].items():
                breaker_name = f"exchange:{exchange}:{breaker_type}"

                if not breaker.allow_operation():
                    reason = (
                        f"Exchange breaker '{breaker_name}' for {exchange} is OPEN due to: "
                        f"{breaker.trip_reason}"
                    )
                    logger.warning(f"Execution blocked for {exchange}: {reason}")
                    return False, reason
                # Test recovery for HALF_OPEN exchange breakers
                if breaker.state == BreakerState.HALF_OPEN and breaker.trip_time is not None:
                    cooldown_time = timedelta(seconds=breaker.cooldown_seconds)
                    if now >= breaker.trip_time + cooldown_time:
                        if not breaker.test_recovery():
                            reason = (
                                f"Exchange breaker '{breaker_name}' for {exchange} failed "
                                f"recovery test."
                            )
                            logger.warning(f"Execution blocked for {exchange}: {reason}")
                            return False, reason
                        else:
                            logger.info(
                                f"Exchange breaker '{breaker_name}' for {exchange} recovered "
                                f"and is now CLOSED."
                            )

        # TODO: Consider symbol-specific breakers if implemented

        return True, None  # Allowed if no breakers are OPEN

    def record_api_error(self, exchange: str, error_message: str) -> None:
        """
        Record an API error for the specified exchange and globally.

        Args:
            exchange: Exchange identifier
            error_message: Description of the error
        """
        logger.debug(f"Recording API error for {exchange}: {error_message}")

        # Record with global breaker if it exists
        global_breaker = getattr(self, "global_api_error_breaker", None)
        if global_breaker and isinstance(global_breaker, APIErrorBreaker):
            global_breaker.record_error(error_message)
            # Check if global breaker tripped
            if not global_breaker.allow_operation():
                logger.critical(
                    f"Global API Error circuit breaker tripped: {global_breaker.trip_reason}"
                )

        # Record with exchange-specific breaker
        breaker = self.get_exchange_breaker(exchange, "api_errors")
        if isinstance(breaker, APIErrorBreaker):
            breaker.record_error(error_message)
            # Check if exchange breaker tripped
            if not breaker.allow_operation():
                logger.warning(
                    f"API Error circuit breaker for {exchange} tripped: {breaker.trip_reason}"
                )
        else:
            logger.debug(f"No APIErrorBreaker configured for exchange: {exchange}")

    def record_api_success(self, exchange: str, context: str = "") -> None:
        """
        Record a successful API interaction for an exchange.
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
                            f"HALF_OPEN state"
                        )

                # If breaker is in HALF_OPEN, test recovery
                if breaker.state == BreakerState.HALF_OPEN:
                    recovery_success = breaker.test_recovery()
                    if recovery_success:
                        logger.info(
                            f"API success confirmed recovery: {exchange_breaker_name} reset "
                            f"to CLOSED state"
                        )
                    else:
                        # Correctly formatted multi-line f-string
                        logger.info(
                            f"Exchange breaker {exchange_breaker_name} remains in HALF_OPEN after "
                            f"successful check."
                        )
                        # Keep state HALF_OPEN, reset success count
                        # Assumes _recovery_success_counts exists, which might be incorrect
                        # self._recovery_success_counts[exchange_breaker_name] = 0 # Commenting out potentially incorrect line
                else:
                    # Correctly formatted multi-line f-string
                    logger.info(
                        f"API success not sufficient for recovery: "
                        f"{exchange_breaker_name} remains in OPEN state"
                    )
            else:
                # Not OPEN or HALF_OPEN, success doesn't change state
                # Corrected escaping for apostrophe
                logger.debug(
                    f"API success recorded for {exchange} (breaker state {breaker.state.name}), "
                    f"no state change."
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
                        "API success confirmed recovery: global_api_errors reset to CLOSED state"
                    )
                else:
                    # Correctly formatted multi-line f-string
                    logger.info(
                        f"API success not sufficient for recovery: "
                        f"global_api_errors remains in {global_breaker.state.name} state"
                    )
            else:
                # Not OPEN or HALF_OPEN, success doesn't change state
                logger.debug(
                    f"API success recorded for global breaker (state {global_breaker.state.name}), "
                    f"no state change."
                )

    def record_critical_failure(self, exchange: str, error_message: str) -> None:
        """
        Record a critical failure that should immediately trip relevant breakers.

        Args:
            exchange: Exchange where the critical failure occurred
            error_message: Description of the critical failure
        """
        logger.error(f"CRITICAL FAILURE for {exchange}: {error_message}")

        # Trip the exchange-specific API breaker if it exists
        exchange_api_breaker = self.get_exchange_breaker(exchange, "api_errors")
        if exchange_api_breaker:
            exchange_api_breaker.trip(f"Critical failure: {error_message}")

        # Also trip the global API breaker to ensure all operations are affected
        global_api_breaker = getattr(self, "global_api_error_breaker", None)
        if global_api_breaker:
            global_api_breaker.trip(f"Critical failure on {exchange}: {error_message}")

        # If we have any exchange-specific volatility breakers, trip those too
        # as a critical failure might indicate market conditions are unstable
        for _breaker_name, breaker in self.exchange_breakers.get(exchange, {}).items():
            if isinstance(breaker, VolatilityBreaker):
                breaker.trip(f"Critical failure triggered volatility breaker: {error_message}")

    def update_price(self, exchange: str, symbol: str, price: float) -> None:
        """
        Update price data for volatility monitoring.

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
        """
        Update portfolio value for drawdown monitoring.

        Args:
            exchange: Exchange identifier
            value: Current portfolio value
        """
        draw_breaker = self.get_exchange_breaker(exchange, "drawdown")
        if draw_breaker and isinstance(draw_breaker, DrawdownBreaker):
            draw_breaker.check(value)

    def update_liquidity(self, exchange: str, symbol: str, liquidity: float) -> None:
        """
        Update market liquidity for monitoring.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            liquidity: Current market liquidity
        """
        liq_breaker = self.get_exchange_breaker(exchange, f"{symbol}_liquidity")
        if liq_breaker and isinstance(liq_breaker, LiquidityBreaker):
            liq_breaker.check(liquidity)

    def reset_breaker(self, name: str) -> bool:
        """
        Reset a circuit breaker by name.

        Args:
            name: Name of the breaker to reset

        Returns:
            True if the breaker was found and reset, False otherwise
        """
        breaker = self.get_breaker(name)
        if breaker:
            logger.info(
                f"Attempting to reset circuit breaker '{name}'. Current state: {breaker.state.name}"
            )
            breaker.reset()
            # Add check for state after reset
            if breaker.state == BreakerState.CLOSED:
                logger.info(
                    f"Circuit breaker '{name}' reset successfully. New state: {breaker.state.name}"
                )
                return True
            else:
                logger.error(
                    f"Circuit breaker '{name}' failed to reset. "
                    f"State is still {breaker.state.name}."
                )
                return False  # Return False if state didn't change to CLOSED
        else:
            logger.warning(f"Attempted to reset nonexistent circuit breaker: '{name}'")
            return False

    def reset_exchange_breakers(self, exchange: str) -> int:
        """
        Reset all circuit breakers for an exchange.

        Args:
            exchange: Exchange identifier

        Returns:
            Number of breakers reset
        """
        if exchange not in self.exchange_breakers:
            return 0

        reset_count = 0
        for breaker in self.exchange_breakers[exchange].values():
            breaker.reset()
            reset_count += 1
        return reset_count
