"""
Tests for the CircuitBreaker system.
"""

import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone
import time

from cyberdelta.validation.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitBreakerSystem,
    VolatilityBreaker,
    DrawdownBreaker,
    APIErrorBreaker,
    LiquidityBreaker,
)


class TestCircuitBreakerBase:
    """Test suite for the base CircuitBreaker class."""

    class SimpleBreaker(CircuitBreaker):
        """Simple implementation for testing."""

        def __init__(self, name, cooldown_seconds=300):
            super().__init__(name, cooldown_seconds)
            self.recovery_check_result = True

        def _check_recovery(self):
            return self.recovery_check_result

        def check(self, *args, **kwargs):
            pass

    def test_init(self):
        """Test initializing the circuit breaker."""
        breaker = self.SimpleBreaker("test_breaker", 600)

        assert breaker.name == "test_breaker"
        assert breaker.cooldown_seconds == 600
        assert breaker.state == BreakerState.CLOSED
        assert breaker.trip_time is None
        assert breaker.trip_reason is None
        assert breaker.trip_count == 0
        assert breaker.last_reset_time is None

    def test_trip(self):
        """Test tripping the circuit breaker."""
        breaker = self.SimpleBreaker("test_breaker")
        trip_time_before = datetime.now(timezone.utc)

        # Trip the breaker
        breaker.trip("Test reason 1")
        trip_time_after_1 = datetime.now(timezone.utc)

        assert breaker.state == BreakerState.OPEN
        assert isinstance(breaker.trip_time, datetime)
        assert breaker.trip_time.tzinfo is not None, (
            "Trip time should be timezone-aware"
        )
        assert (
            breaker.trip_time >= trip_time_before
            and breaker.trip_time <= trip_time_after_1
        )
        assert breaker.trip_reason == "Test reason 1"
        assert breaker.trip_count == 1
        trip_time_first = breaker.trip_time

        # Trip again immediately
        time.sleep(0.01)
        breaker.trip("Test reason 2")
        trip_time_after_2 = datetime.now(timezone.utc)

        assert breaker.state == BreakerState.OPEN
        assert breaker.trip_count == 2
        assert breaker.trip_reason == "Test reason 2"
        assert breaker.trip_time >= trip_time_first
        assert breaker.trip_time <= trip_time_after_2
        assert breaker.trip_time.tzinfo is not None

    def test_reset(self):
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

    def test_allow_operation(self):
        """Test checking if operation is allowed."""
        breaker = self.SimpleBreaker("test_breaker", cooldown_seconds=1)

        # Initially closed
        assert breaker.allow_operation() is True

        # Trip the breaker
        breaker.trip("Test reason")
        assert breaker.allow_operation() is False

        # Wait for cooldown (set trip_time in the past)
        # Ensure the past time is also timezone-aware
        breaker.trip_time = datetime.now(timezone.utc) - timedelta(seconds=2)
        assert breaker.allow_operation() is True
        # After checking, state should move to HALF_OPEN
        assert breaker.state == BreakerState.HALF_OPEN

    def test_test_recovery_successful(self):
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

    def test_test_recovery_failed(self):
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

    def test_get_status(self):
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

    def test_init(self):
        """Test initializing the volatility breaker."""
        breaker = VolatilityBreaker(
            "vol_breaker", lookback_periods=10, volatility_threshold=0.03
        )

        assert breaker.name == "vol_breaker"
        assert breaker.lookback_periods == 10
        assert breaker.volatility_threshold == 0.03
        assert breaker.price_history == []

    def test_add_price(self):
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

    def test_check_insufficient_data(self):
        """Test check with insufficient data."""
        breaker = VolatilityBreaker("vol_breaker")

        # Check with no data
        breaker.check()
        assert breaker.state == BreakerState.CLOSED

        # Check with only one price point
        breaker.add_price(100)
        breaker.check()
        assert breaker.state == BreakerState.CLOSED

    def test_check_low_volatility(self):
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

    def test_check_high_volatility(self):
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
        assert "Volatility" in breaker.trip_reason

    def test_check_with_current_price(self):
        """Test check with a current price parameter."""
        breaker = VolatilityBreaker("vol_breaker", volatility_threshold=0.05)

        # Add some prices
        breaker.add_price(100)
        breaker.add_price(105)

        # Check with a current price that would trigger high volatility
        breaker.check(current_price=130)

        # Breaker should trip
        assert breaker.state == BreakerState.OPEN
        assert breaker.price_history == [100, 105, 130]

    def test_check_recovery(self):
        """Test recovery check."""
        breaker = VolatilityBreaker("vol_breaker", volatility_threshold=0.05)

        # Set up price history with high volatility
        breaker.price_history = [100, 120, 80, 110]

        # Recovery check should fail
        assert breaker._check_recovery() is False

        # Replace with less volatile prices
        breaker.price_history = [100, 102, 101, 103]

        # Recovery check should pass
        assert breaker._check_recovery() is True


class TestDrawdownBreaker:
    """Test suite for the DrawdownBreaker class."""

    def test_init(self):
        """Test initializing the drawdown breaker."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.15)

        assert breaker.name == "drawdown_breaker"
        assert breaker.drawdown_threshold == 0.15
        assert breaker.peak_value is None
        assert breaker.current_value is None

    def test_check_initial_values(self):
        """Test check with initial values."""
        breaker = DrawdownBreaker("drawdown_breaker")

        # First value becomes peak
        breaker.check(100)

        assert breaker.peak_value == 100
        assert breaker.current_value == 100
        assert breaker.state == BreakerState.CLOSED

    def test_check_new_peak(self):
        """Test check with a new peak value."""
        breaker = DrawdownBreaker("drawdown_breaker")

        # Set initial values
        breaker.check(100)

        # New peak
        breaker.check(120)

        assert breaker.peak_value == 120
        assert breaker.current_value == 120
        assert breaker.state == BreakerState.CLOSED

    def test_check_small_drawdown(self):
        """Test check with a small drawdown."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.10)

        # Set peak
        breaker.check(100)

        # Small drawdown (5%)
        breaker.check(95)

        assert breaker.peak_value == 100
        assert breaker.current_value == 95
        assert breaker.state == BreakerState.CLOSED

    def test_check_large_drawdown(self):
        """Test check with a large drawdown."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.10)

        # Set peak
        breaker.check(100)

        # Large drawdown (15%)
        breaker.check(85)

        assert breaker.peak_value == 100
        assert breaker.current_value == 85
        assert breaker.state == BreakerState.OPEN
        assert "Drawdown" in breaker.trip_reason

    def test_check_recovery(self):
        """Test recovery check."""
        breaker = DrawdownBreaker("drawdown_breaker", drawdown_threshold=0.10)

        # Set values with large drawdown
        breaker.peak_value = 100
        breaker.current_value = 85

        # Recovery check should fail
        assert breaker._check_recovery() is False

        # Update to smaller drawdown
        breaker.current_value = 95

        # Recovery check should pass
        assert breaker._check_recovery() is True


class TestAPIErrorBreaker:
    """Test suite for the APIErrorBreaker class."""

    def test_init(self):
        """Test initializing the API error breaker."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=5, window_seconds=30)

        assert breaker.name == "api_breaker"
        assert breaker.error_threshold == 5
        assert breaker.window_seconds == 30
        assert breaker.error_times == []

    def test_record_error_below_threshold(self):
        """Test recording errors below threshold."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3)

        # Record two errors
        breaker.record_error("Error 1")
        breaker.record_error("Error 2")

        assert len(breaker.error_times) == 2
        assert breaker.state == BreakerState.CLOSED

    def test_record_error_above_threshold(self):
        """Test recording errors above threshold."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3, window_seconds=60)

        # Record three errors to exceed threshold
        breaker.record_error("Error 1")
        breaker.record_error("Error 2")
        breaker.record_error("Error 3")

        assert len(breaker.error_times) == 3
        assert breaker.state == BreakerState.OPEN
        # Check the exact reason string format
        expected_reason = f"API errors exceeded threshold ({breaker.error_threshold} in {breaker.window_seconds}s)"
        assert breaker.trip_reason == expected_reason

    def test_check_old_errors_removed(self):
        """Test that old errors are removed from consideration."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3, window_seconds=1)

        # Add old errors (make them timezone-aware)
        old_time = datetime.now(timezone.utc) - timedelta(seconds=2)
        breaker.error_times = [old_time, old_time]

        # Check (should remove old errors)
        breaker.check()
        assert len(breaker.error_times) == 0
        assert breaker.state == BreakerState.CLOSED

        # Add one new error, still below threshold
        breaker.check("New error 1")
        assert len(breaker.error_times) == 1
        assert breaker.state == BreakerState.CLOSED

    def test_check_recovery(self):
        """Test recovery check."""
        breaker = APIErrorBreaker("api_breaker", error_threshold=3, window_seconds=10)

        # Add errors (make them timezone-aware)
        now = datetime.now(timezone.utc)
        breaker.error_times = [now, now, now]

        # Recovery check should fail with 3 errors
        assert breaker._check_recovery() is False

        # Reduce errors
        breaker.error_times = [now]

        # Recovery check should pass with 1 error
        assert breaker._check_recovery() is True


class TestLiquidityBreaker:
    """Test suite for the LiquidityBreaker class."""

    def test_init(self):
        """Test initializing the liquidity breaker."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        assert breaker.name == "liquidity_breaker"
        assert breaker.min_liquidity == 50000
        assert breaker.current_liquidity is None

    def test_check_sufficient_liquidity(self):
        """Test check with sufficient liquidity."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        # Check with high liquidity
        breaker.check(75000)

        assert breaker.current_liquidity == 75000
        assert breaker.state == BreakerState.CLOSED

    def test_check_insufficient_liquidity(self):
        """Test check with insufficient liquidity."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        # Check with low liquidity
        breaker.check(40000)

        assert breaker.current_liquidity == 40000
        assert breaker.state == BreakerState.OPEN
        assert "Liquidity" in breaker.trip_reason

    def test_check_recovery(self):
        """Test recovery check."""
        breaker = LiquidityBreaker("liquidity_breaker", min_liquidity=50000)

        # Set low liquidity
        breaker.current_liquidity = 40000

        # Recovery check should fail
        assert breaker._check_recovery() is False

        # Update to sufficient liquidity
        breaker.current_liquidity = 60000

        # Recovery check should pass
        assert breaker._check_recovery() is True


class TestCircuitBreakerSystem:
    """Test suite for the CircuitBreakerSystem class."""

    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        cfg = MagicMock(spec=Config)
        config_data = {
            # Define exchanges and enable them
            "exchanges": {
                "hyperliquid": {
                    "enabled": True,
                    "circuit_breakers": {  # Define some breakers for testing
                        "api_errors": {
                            "type": "api_error",
                            "error_threshold": 3,
                            "window_seconds": 60,
                        },
                        "BTC_volatility": {
                            "type": "volatility",
                            "lookback_periods": 10,
                            "volatility_threshold": 0.05,
                        },
                    },
                },
                "backpack": {
                    "enabled": True,
                    "circuit_breakers": {
                        "api_errors": {
                            "type": "api_error",
                            "error_threshold": 5,
                            "window_seconds": 120,
                        }
                    },
                },
            },
            # Global breaker config
            "circuit_breaker.global_api_error.enabled": True,
            "circuit_breaker.global_api_error.error_threshold": 10,
            "circuit_breaker.global_api_error.window_seconds": 300,
            "circuit_breaker.default_cooldown_seconds": 300,
            # Keys used directly by PositionReconciliationSystem tests
            "validation.position_reconciliation.threshold": 0.05,
            "validation.position_reconciliation.auto_correct": False,
            "validation.position_reconciliation.check_interval": 3600,
            "validation.position_reconciliation.use_fill_history": False,
        }

        # More robust side_effect for nested gets
        def config_get_side_effect(key, default=None):
            parts = key.split(".")
            # Direct key match
            if key in config_data:
                return config_data[key]
            # Nested lookup within 'exchanges' dict
            if len(parts) > 1 and parts[0] == "exchanges":
                try:
                    val = config_data["exchanges"]
                    for part in parts[1:]:
                        val = val[part]
                    return val
                except (KeyError, TypeError):
                    return default
            # Other nested lookups (e.g., circuit_breaker.global_...)
            try:
                val = config_data
                for part in parts:
                    val = val[part]
                return val
            except (KeyError, TypeError):
                return default

        cfg.get.side_effect = config_get_side_effect
        # Add direct attribute access for the top-level 'exchanges' dict if needed by CBSystem init
        cfg.exchanges = config_data["exchanges"]
        return cfg

    @pytest.fixture
    def breaker_system(self, config):
        """Create a CircuitBreakerSystem instance with mock config."""
        # Ensure the system is fully initialized
        system = CircuitBreakerSystem(config)
        system.load_configuration()  # Explicitly call load_configuration if it's not called in __init__
        return system

    def test_init_and_load_config(self, breaker_system):
        """Test initialization and configuration loading."""
        # Check that exchange breakers were created
        assert "hyperliquid" in breaker_system.exchange_breakers
        assert "backpack" in breaker_system.exchange_breakers

        # Check types of breakers created
        assert isinstance(
            breaker_system.exchange_breakers["hyperliquid"]["api_errors"],
            APIErrorBreaker,
        )
        assert isinstance(
            breaker_system.exchange_breakers["hyperliquid"]["BTC_volatility"],
            VolatilityBreaker,
        )
        assert isinstance(
            breaker_system.exchange_breakers["hyperliquid"]["drawdown"], DrawdownBreaker
        )

    def test_register_and_get_breaker(self, breaker_system):
        """Test registering and retrieving a breaker."""
        # Create and register a new breaker
        breaker = APIErrorBreaker("test_api_breaker", error_threshold=2)
        breaker_system.register_breaker(breaker)

        # Get the breaker back
        retrieved = breaker_system.get_breaker("test_api_breaker")

        assert retrieved is breaker
        assert breaker_system.get_breaker("nonexistent") is None

    def test_get_exchange_breaker(self, breaker_system):
        """Test getting an exchange-specific breaker."""
        # Get existing breaker
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")

        assert isinstance(api_breaker, APIErrorBreaker)
        assert api_breaker.name == "hyperliquid_api_errors"

        # Get nonexistent breaker
        assert breaker_system.get_exchange_breaker("nonexistent", "api_errors") is None
        assert breaker_system.get_exchange_breaker("hyperliquid", "nonexistent") is None

    def test_can_execute_no_trips(self, breaker_system):
        """Test can_execute when no breakers are tripped."""
        can_exec, reason = breaker_system.can_execute("hyperliquid", "BTC")

        assert can_exec is True
        assert reason is None

    def test_can_execute_with_trip(self, breaker_system):
        """Test can_execute when a breaker is tripped."""
        # Trip an API breaker
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")
        api_breaker.trip("Test trip")

        # Check can_execute
        can_exec, reason = breaker_system.can_execute("hyperliquid", "BTC")

        assert can_exec is False
        assert "Blocked by circuit breaker" in reason
        assert "Test trip" in reason

    def test_record_api_error(self, breaker_system):
        """Test recording an API error."""
        # Record 3 errors to trip the breaker
        breaker_system.record_api_error("hyperliquid", "Error 1")
        breaker_system.record_api_error("hyperliquid", "Error 2")
        breaker_system.record_api_error("hyperliquid", "Error 3")

        # Get the breaker
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")

        assert api_breaker.state == BreakerState.OPEN
        assert len(api_breaker.error_times) == 3

    def test_update_price(self, breaker_system):
        """Test updating price data."""
        # Update with highly volatile prices
        breaker_system.update_price("hyperliquid", "BTC", 100)
        breaker_system.update_price("hyperliquid", "BTC", 130)
        breaker_system.update_price("hyperliquid", "BTC", 80)

        # Get the breaker
        vol_breaker = breaker_system.get_exchange_breaker(
            "hyperliquid", "BTC_volatility"
        )

        assert len(vol_breaker.price_history) == 3
        # Depending on the threshold, this might trip

    def test_update_portfolio_value(self, breaker_system):
        """Test updating portfolio value."""
        # Set up a peak
        breaker_system.update_portfolio_value("hyperliquid", 100000)

        # Update with a large drawdown
        breaker_system.update_portfolio_value("hyperliquid", 80000)

        # Get the breaker
        draw_breaker = breaker_system.get_exchange_breaker("hyperliquid", "drawdown")

        assert draw_breaker.peak_value == 100000
        assert draw_breaker.current_value == 80000
        # Depending on the threshold, this might trip

    def test_update_liquidity(self, breaker_system):
        """Test updating market liquidity."""
        # Update with low liquidity
        breaker_system.update_liquidity("hyperliquid", "BTC", 10000)

        # Get the breaker
        liq_breaker = breaker_system.get_exchange_breaker(
            "hyperliquid", "BTC_liquidity"
        )

        assert liq_breaker.current_liquidity == 10000
        # Depending on the threshold, this might trip

    def test_reset_breaker(self, breaker_system):
        """Test resetting a specific breaker."""
        breaker_name = "hyperliquid_api_errors"
        # Trip a breaker
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")
        assert api_breaker is not None, (
            f"Breaker {breaker_name} not found in fixture setup"
        )
        api_breaker.trip("Test trip")
        assert api_breaker.state == BreakerState.OPEN

        # Reset by name
        success = breaker_system.reset_breaker(breaker_name)

        # Verify the system method returns True and breaker state is CLOSED
        assert success is True, f"reset_breaker({breaker_name}) returned False"
        assert api_breaker.state == BreakerState.CLOSED
        assert api_breaker.trip_reason is None  # Reason should be cleared on reset

    def test_reset_nonexistent_breaker(self, breaker_system):
        """Test resetting a nonexistent breaker."""
        success = breaker_system.reset_breaker("nonexistent")
        assert success is False

    def test_reset_exchange_breakers(self, breaker_system):
        """Test resetting all breakers for an exchange."""
        # Trip multiple breakers
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")
        vol_breaker = breaker_system.get_exchange_breaker(
            "hyperliquid", "BTC_volatility"
        )

        api_breaker.trip("API error")
        vol_breaker.trip("High volatility")

        # Reset all for the exchange
        reset_count = breaker_system.reset_exchange_breakers("hyperliquid")

        assert reset_count > 0
        assert api_breaker.state == BreakerState.CLOSED
        assert vol_breaker.state == BreakerState.CLOSED

        # Try to reset nonexistent exchange
        assert breaker_system.reset_exchange_breakers("nonexistent") == 0

    def test_get_status(self, breaker_system):
        """Test getting status of all breakers."""
        # Trip a breaker
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")
        api_breaker.trip("Test trip")

        # Get status
        status = breaker_system.get_status()

        assert "global_breakers" in status
        assert "exchange_breakers" in status
        assert "hyperliquid" in status["exchange_breakers"]
        assert "api_errors" in status["exchange_breakers"]["hyperliquid"]
        assert (
            status["exchange_breakers"]["hyperliquid"]["api_errors"]["state"] == "OPEN"
        )

    def test_get_tripped_breakers(self, breaker_system):
        """Test getting list of tripped breakers."""
        # Initially no tripped breakers
        assert len(breaker_system.get_tripped_breakers()) == 0

        # Trip some breakers
        api_breaker = breaker_system.get_exchange_breaker("hyperliquid", "api_errors")
        vol_breaker = breaker_system.get_exchange_breaker(
            "hyperliquid", "BTC_volatility"
        )

        api_breaker.trip("API error")
        vol_breaker.trip("High volatility")

        # Get tripped breakers
        tripped = breaker_system.get_tripped_breakers()

        assert len(tripped) == 2
        assert any(b["name"] == "hyperliquid_api_errors" for b in tripped)
        assert any(b["name"] == "hyperliquid_BTC_volatility" for b in tripped)
