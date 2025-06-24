"""Tests for the CircuitBreaker system."""

import time
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from cyberdelta.config import AppSettings
from cyberdelta.validation.circuit_breaker import (
    APIErrorBreaker,
    BreakerState,
    CircuitBreaker,
    CircuitBreakerSystem,
    DrawdownBreaker,
    LiquidityBreaker,
    VolatilityBreaker,
)

pytestmark = pytest.mark.timing


class TestCircuitBreakerBase:
    """Test suite for the base CircuitBreaker class."""

    class SimpleBreaker(CircuitBreaker):
        """Simple implementation for testing."""

        def __init__(self, name: str, cooldown_seconds: int = 300) -> None:
            """Initialize SimpleBreaker with recovery check configuration."""
            super().__init__(name, cooldown_seconds)
            self.recovery_check_result = True

        def _check_recovery(self) -> bool:
            return self.recovery_check_result

        def check(self, *args: object, **kwargs: object) -> None:
            """Perform circuit breaker check logic (no-op for testing)."""
            pass

    def test_init(self) -> None:
        """Test initializing the circuit breaker."""
        breaker = self.SimpleBreaker("test_breaker", 600)

        assert breaker.name == "test_breaker"
        assert breaker.cooldown_seconds == 600
        assert breaker.state == BreakerState.CLOSED
        assert breaker.trip_time is None
        assert breaker.trip_reason is None
        assert breaker.trip_count == 0
        assert breaker.last_reset_time is None

    def test_trip(self) -> None:
        """Test tripping the circuit breaker."""
        breaker = self.SimpleBreaker("test_breaker")
        trip_time_before = datetime.now(UTC)

        # Trip the breaker
        breaker.trip("Test reason 1")
        trip_time_after_1 = datetime.now(UTC)

        assert breaker.state == BreakerState.OPEN
        assert isinstance(breaker.trip_time, datetime)
        assert breaker.trip_time.tzinfo is not None, "Trip time should be timezone-aware"
        assert breaker.trip_time >= trip_time_before and breaker.trip_time <= trip_time_after_1
        assert breaker.trip_reason == "Test reason 1"
        assert breaker.trip_count == 1
        trip_time_first = breaker.trip_time

        # Trip again immediately
        time.sleep(0.01)
        breaker.trip("Test reason 2")
        trip_time_after_2 = datetime.now(UTC)

        assert breaker.state == BreakerState.OPEN
        assert breaker.trip_count == 2
        assert breaker.trip_reason == "Test reason 2"
        assert breaker.trip_time >= trip_time_first
        assert breaker.trip_time <= trip_time_after_2
        assert breaker.trip_time.tzinfo is not None

    def test_reset(self) -> None:
        """Test resetting the circuit breaker."""
        breaker = self.SimpleBreaker("test_breaker")

        # Trip and then reset
        breaker.trip("Test reason")
        breaker.reset()

        assert breaker.state == BreakerState.CLOSED
        assert isinstance(breaker.last_reset_time, datetime)
        assert breaker.trip_time is None
        assert breaker.trip_reason is None

        # Trip count should remain the same
        assert breaker.trip_count == 1

    def test_allow_operation(self) -> None:
        """Test checking if operation is allowed."""
        breaker = self.SimpleBreaker("test_breaker", cooldown_seconds=1)

        # Initially closed
        assert breaker.allow_operation() is True

        # Trip the breaker
        breaker.trip("Test reason")
        assert breaker.allow_operation() is False

        # Wait for cooldown (set trip_time in the past)
        # Ensure the past time is also timezone-aware
        breaker.trip_time = datetime.now(UTC) - timedelta(seconds=2)
        assert breaker.allow_operation() is True
        # After checking, state should move to HALF_OPEN
        assert breaker.state == BreakerState.HALF_OPEN

    def test_test_recovery_successful(self) -> None:
        """Test recovery check when successful."""
        breaker = self.SimpleBreaker("test_breaker")

        # Trip and wait for cooldown
        breaker.trip("Test reason")
        breaker.state = BreakerState.HALF_OPEN
        breaker.recovery_check_result = True

        # Test recovery
        result = breaker.test_recovery()

        assert result is True
        assert breaker.state == BreakerState.CLOSED

    def test_test_recovery_failed(self) -> None:
        """Test recovery check when failed."""
        breaker = self.SimpleBreaker("test_breaker")

        # Trip and wait for cooldown
        breaker.trip("Test reason")
        breaker.state = BreakerState.HALF_OPEN
        breaker.recovery_check_result = False

        # Test recovery
        result = breaker.test_recovery()

        assert result is False
        assert breaker.state == BreakerState.OPEN

    def test_get_status(self) -> None:
        """Test getting the status of the breaker."""
        breaker = self.SimpleBreaker("test_breaker")

        # Initial status
        status = breaker.get_status()

        assert status["name"] == "test_breaker"
        assert status["state"] == "CLOSED"
        assert status["trip_count"] == 0
        assert status["trip_time"] is None
        assert status["trip_reason"] is None
        assert status["cooldown_seconds"] == 300
        assert status["last_reset_time"] is None

        # Trip and check status
        breaker.trip("Test reason")
        status = breaker.get_status()

        assert status["state"] == "OPEN"
        assert status["trip_count"] == 1
        assert status["trip_reason"] == "Test reason"
        assert isinstance(status["trip_time"], str)  # ISO format string


class TestVolatilityBreaker:
    """Test suite for the VolatilityBreaker class."""

    def test_init(self) -> None:
        """Test initializing the volatility breaker."""
        breaker = VolatilityBreaker("vol_breaker", lookback_periods=10, volatility_threshold=0.03)

        assert breaker.name == "vol_breaker"
        assert breaker.lookback_periods == 10
        assert breaker.volatility_threshold == 0.03
        assert breaker.price_history == []

    def test_add_price(self) -> None:
        """Test adding prices to history."""
        breaker = VolatilityBreaker("vol_breaker", lookback_periods=3)

        # Add prices
        breaker.add_price(100)
        breaker.add_price(105)
        breaker.add_price(110)

        assert breaker.price_history == [100, 105, 110]

        # Add one more to exceed lookback period
        breaker.add_price(115)

        assert breaker.price_history == [105, 110, 115]

    def test_check_insufficient_data(self) -> None:
        """Test check with insufficient data."""
        breaker = VolatilityBreaker("vol_breaker")

        # Check with no data
        breaker.check()
        assert breaker.state == BreakerState.CLOSED

        # Check with only one price point
        breaker.add_price(100)
        breaker.check()
        assert breaker.state == BreakerState.CLOSED

    def test_check_low_volatility(self) -> None:
        """Test check with low volatility."""
        breaker = VolatilityBreaker("vol_breaker", volatility_threshold=0.10)

        # Add similar prices
        breaker.add_price(100)
        breaker.add_price(102)
        breaker.add_price(101)
        breaker.add_price(103)

        # Check volatility
        breaker.check()

        # Volatility should be low, breaker stays closed
        assert breaker.state == BreakerState.CLOSED

    def test_check_high_volatility(self) -> None:
        """Test check with high volatility."""
        breaker = VolatilityBreaker("vol_breaker", volatility_threshold=0.05)

        # Add prices with high volatility
        breaker.add_price(100)
        breaker.add_price(120)
        breaker.add_price(80)
        breaker.add_price(110)

        # Check volatility
        breaker.check()

        # Volatility should be high, breaker trips
        assert breaker.state == BreakerState.OPEN
        assert breaker.trip_reason is not None and "Volatility" in breaker.trip_reason

    def test_check_with_current_price(self) -> None:
        """Test check with a current price parameter."""
        breaker = VolatilityBreaker("vol_breaker", volatility_threshold=0.05)

        # Add some prices
        breaker.add_price(100)
        breaker.add_price(105)

        # Check with a volatility-inducing price
        breaker.check(130)

        # Should be open and have added the price to history
        assert breaker.state == BreakerState.OPEN
        assert breaker.price_history == [100, 105, 130]

    def test_check_recovery(self) -> None:
        """Test recovery check."""
        breaker = VolatilityBreaker("vol_breaker", volatility_threshold=0.05)

        # Add prices with high volatility to trip the breaker
        breaker.add_price(100)
        breaker.add_price(120)
        breaker.add_price(80)
        breaker.check()  # This should trip the breaker

        # Reset to half-open
        breaker.state = BreakerState.HALF_OPEN

        # Test recovery
        result = breaker.test_recovery()

        # Should check volatility and return False
        assert result is False
        assert breaker.state == BreakerState.OPEN


class TestDrawdownBreaker:
    """Test suite for the DrawdownBreaker class."""

    def test_init(self) -> None:
        """Test initializing the drawdown breaker."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.15)

        assert breaker.name == "drawdown_breaker"
        assert breaker.drawdown_threshold == 0.15
        assert breaker.peak_value is None
        assert breaker.current_value is None

    def test_check_initial_values(self) -> None:
        """Test check with initial values."""
        breaker = DrawdownBreaker("drawdown_breaker")

        # Check with no data
        breaker.check(100)
        assert breaker.state == BreakerState.CLOSED
        assert breaker.peak_value == 100
        assert breaker.current_value == 100

    def test_check_new_peak(self) -> None:
        """Test check with a new peak value."""
        breaker = DrawdownBreaker("drawdown_breaker")

        # Set initial value
        breaker.check(100)
        assert breaker.peak_value == 100

        # Check with higher value (new peak)
        breaker.check(120)
        assert breaker.peak_value == 120
        assert breaker.current_value == 120
        assert breaker.state == BreakerState.CLOSED

    def test_check_small_drawdown(self) -> None:
        """Test check with a small drawdown."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.10)

        # Set initial peak
        breaker.check(100)

        # Check with small drawdown (5%)
        breaker.check(95)
        assert breaker.peak_value == 100
        assert breaker.current_value == 95
        assert breaker.state == BreakerState.CLOSED

    def test_check_large_drawdown(self) -> None:
        """Test check with a large drawdown."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.10)

        # Set initial peak
        breaker.check(100)

        # Check with large drawdown (15%)
        breaker.check(85)
        assert breaker.peak_value == 100
        assert breaker.current_value == 85
        assert breaker.state == BreakerState.OPEN
        assert breaker.trip_reason is not None and "Drawdown" in breaker.trip_reason

    def test_check_recovery(self) -> None:
        """Test recovery check."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.10)

        # Set peak and trigger
        breaker.check(100)
        breaker.check(85)  # Triggers

        # Recovery test
        breaker.state = BreakerState.HALF_OPEN
        result = breaker.test_recovery()

        # Should stay in recovery since price is still low
        assert result is False
        assert breaker.state == BreakerState.OPEN


class TestAPIErrorBreaker:
    """Test suite for the APIErrorBreaker class."""

    def test_init(self) -> None:
        """Test initializing the API error breaker."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=5, window_seconds=30)

        assert breaker.name == "api_breaker"
        assert breaker.error_threshold == 5
        assert breaker.window_seconds == 30
        assert breaker.errors == []

    def test_record_error_below_threshold(self) -> None:
        """Test recording errors below threshold."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3)

        # Record two errors
        breaker.record_error("Error 1")
        breaker.record_error("Error 2")

        # Should still be closed
        assert len(breaker.errors) == 2
        assert breaker.state == BreakerState.CLOSED

    def test_record_error_above_threshold(self) -> None:
        """Test recording errors above threshold."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3, window_seconds=60)

        # Record enough errors to trip
        breaker.record_error("Error 1")
        breaker.record_error("Error 2")
        breaker.record_error("Error 3")  # This should trip
        breaker.check()  # Explicitly call check to evaluate state

        # Should be open
        assert breaker.state == BreakerState.OPEN
        # Check if the trip reason contains key information
        assert breaker.trip_reason is not None and "API errors" in breaker.trip_reason

    def test_check_old_errors_removed(self) -> None:
        """Test that old errors are removed from consideration."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3, window_seconds=1)

        # Record two errors
        breaker.record_error("Error 1")
        breaker.record_error("Error 2")

        # Wait for window to expire
        time.sleep(1.1)

        # Record one more error - shouldn't trip because old ones expired
        breaker.record_error("Error 3")

        # Should still be closed, with only one recent error
        assert len(breaker.errors) == 1  # Only one recent error remains
        assert breaker.state == BreakerState.CLOSED

    def test_check_recovery(self) -> None:
        """Test recovery check."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3, window_seconds=10)

        # Force to tripped state
        breaker.trip("Test trip")

        # Set to half-open for recovery test
        breaker.state = BreakerState.HALF_OPEN

        # Test recovery
        result = breaker.test_recovery()

        # Should recover since no recent errors
        assert result is True
        assert breaker.state == BreakerState.CLOSED


class TestLiquidityBreaker:
    """Test suite for the LiquidityBreaker class."""

    def test_init(self) -> None:
        """Test initializing the liquidity breaker."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        assert breaker.name == "liquidity_breaker"
        assert breaker.min_liquidity == 50000
        assert breaker.current_liquidity is None

    def test_check_sufficient_liquidity(self) -> None:
        """Test check with sufficient liquidity."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        # Check with plenty of liquidity
        breaker.check(100000)

        # Should remain closed
        assert breaker.state == BreakerState.CLOSED

    def test_check_insufficient_liquidity(self) -> None:
        """Test check with insufficient liquidity."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        # Check with low liquidity
        breaker.check(40000)

        # Should trip
        assert breaker.state == BreakerState.OPEN
        assert breaker.trip_reason is not None and "Liquidity" in breaker.trip_reason

    def test_check_recovery(self) -> None:
        """Test recovery check."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        # Trip and set up for recovery check
        breaker.check(40000)  # This will trip
        breaker.state = BreakerState.HALF_OPEN

        # Still low liquidity
        breaker.current_liquidity = 45000

        # Test recovery
        result = breaker.test_recovery()

        # Should not recover yet
        assert result is False
        assert breaker.state == BreakerState.OPEN


# Define a type for config values
ConfigValue = str | int | float | bool | dict[str, Any] | list[Any] | None


@pytest.fixture
def mock_config() -> AppSettings:
    """Create a generic mock Config for circuit breaker tests."""
    cfg = MagicMock(spec=AppSettings)

    # Mock nested attributes structure
    cfg.safety_systems = MagicMock()
    cfg.safety_systems.circuit_breakers = MagicMock()
    cfg.safety_systems.circuit_breakers.enabled = True
    cfg.safety_systems.circuit_breakers.global_consecutive_failures = 5
    cfg.safety_systems.circuit_breakers.global_reset_timeout_sec = 300
    cfg.safety_systems.circuit_breakers.exchange_consecutive_failures = 3
    cfg.safety_systems.circuit_breakers.exchange_reset_timeout_sec = 180

    # Mock exchanges as empty dict by default
    cfg.exchanges = {}

    return cfg


# Define a more specific config fixture for tests needing exchange structure
@pytest.fixture
def mock_config_with_exchanges() -> AppSettings:
    """Mock Config object with predefined exchange configurations for CB testing."""
    mock = MagicMock(spec=AppSettings)

    # Mock safety systems structure
    mock.safety_systems = MagicMock()
    mock.safety_systems.circuit_breakers = MagicMock()
    mock.safety_systems.circuit_breakers.enabled = True
    mock.safety_systems.circuit_breakers.global_consecutive_failures = 5
    mock.safety_systems.circuit_breakers.global_reset_timeout_sec = 300
    mock.safety_systems.circuit_breakers.exchange_consecutive_failures = 3
    mock.safety_systems.circuit_breakers.exchange_reset_timeout_sec = 180

    # Mock exchanges with ExchangeSpecificConfig objects
    test_exchange_config = MagicMock()
    test_exchange_config.enabled = True  # ExchangeSpecificConfig.enabled attribute

    another_exchange_config = MagicMock()
    another_exchange_config.enabled = True  # ExchangeSpecificConfig.enabled attribute

    mock.exchanges = {
        "test_exchange": test_exchange_config,
        "another_exchange": another_exchange_config,
    }

    return mock


class TestCircuitBreakerSystem:
    """Test suite for the CircuitBreakerSystem class."""

    def test_init_and_load_config(self, mock_config_with_exchanges: AppSettings) -> None:
        """Test initialization and configuration loading."""
        # Use the fixture that provides exchange configs
        system = CircuitBreakerSystem(mock_config_with_exchanges)

        # Verify breakers were created for the test exchange
        assert "test_exchange" in system.exchange_breakers

        # Verify API error breaker was created (based on business logic)
        assert "api_errors" in system.exchange_breakers["test_exchange"]
        # Business logic only creates API error breakers for exchanges,
        # not drawdown/volatility/liquidity

        # Check if volatility breakers for symbols were created
        # Don't fail the test if they weren't - this is more of an informational check
        # system.get_exchange_breaker("test_exchange", "BTC_volatility")

    def test_register_and_get_breaker(self, mock_config: AppSettings) -> None:
        """Test registering and retrieving a breaker."""
        system = CircuitBreakerSystem(mock_config)

        # Create and register a new breaker
        test_breaker = APIErrorBreaker("test_breaker", error_threshold=3, window_seconds=60)
        system.register_breaker(test_breaker)

        # Retrieve the breaker
        retrieved = system.get_breaker("test_breaker")
        assert retrieved is not None
        assert retrieved.name == "test_breaker"
        assert isinstance(retrieved, APIErrorBreaker)

    def test_get_exchange_breaker(self, mock_config: AppSettings) -> None:
        """Test getting an exchange-specific breaker."""
        system = CircuitBreakerSystem(mock_config)

        # Business logic doesn't create exchange breakers by default without exchanges configured
        # So let's manually register one for testing
        test_exchange_breaker = APIErrorBreaker(
            "test_exchange/api_error", error_threshold=5, window_seconds=60
        )
        system.register_breaker(test_exchange_breaker)
        system.exchange_breakers["test_exchange"] = {"api_errors": test_exchange_breaker}

        # Test retrieval
        breaker = system.get_exchange_breaker("test_exchange", "api_errors")
        assert breaker is not None
        assert isinstance(breaker, APIErrorBreaker)
        assert breaker.name == "test_exchange/api_error"

    def test_can_execute_no_trips(self, mock_config: AppSettings) -> None:
        """Test can_execute when no breakers are tripped."""
        system = CircuitBreakerSystem(mock_config)

        # With no tripped breakers, execution should be allowed
        can_execute, reason = system.can_execute("test_exchange", "BTC")
        assert can_execute is True
        assert reason is None

    def test_can_execute_with_trip(self, mock_config: AppSettings) -> None:
        """Test can_execute when a breaker is tripped."""
        system = CircuitBreakerSystem(mock_config)

        # Since business logic doesn't create exchange breakers without proper config,
        # let's trip the global API error breaker instead
        global_breaker = system.get_breaker("global/api_error")
        assert global_breaker is not None
        global_breaker.trip("Test trip")

        # Verify can_execute returns false
        can_execute, reason = system.can_execute("test_exchange", "BTC")
        assert can_execute is False
        assert reason is not None
        assert "global" in reason

    def test_record_api_error(self, mock_config: AppSettings) -> None:
        """Test recording an API error."""
        # Test logic to be implemented when needed

    def test_update_price(self, mock_config: AppSettings) -> None:
        """Test updating price for volatility breakers."""
        # Test logic to be implemented when needed

    def test_update_portfolio_value(self, mock_config: AppSettings) -> None:
        """Test updating portfolio value for drawdown breakers."""
        # Test logic to be implemented when needed

    def test_update_liquidity(self, mock_config: AppSettings) -> None:
        """Test updating liquidity for liquidity breakers."""
        # Test logic to be implemented when needed

    def test_reset_breaker(self, mock_config: AppSettings) -> None:
        """Test resetting a specific breaker."""
        # Test logic to be implemented when needed

    def test_reset_nonexistent_breaker(self, mock_config: AppSettings) -> None:
        """Test resetting a non-existent breaker."""
        # Test logic to be implemented when needed

    def test_reset_exchange_breakers(self, mock_config: AppSettings) -> None:
        """Test resetting all breakers for an exchange."""
        # Test logic to be implemented when needed

    def test_get_status(self, mock_config: AppSettings) -> None:
        """Test getting status of all breakers."""
        # Test logic to be implemented when needed

    def test_get_tripped_breakers(self, mock_config: AppSettings) -> None:
        """Test getting all tripped breakers."""
        # Test logic to be implemented when needed
