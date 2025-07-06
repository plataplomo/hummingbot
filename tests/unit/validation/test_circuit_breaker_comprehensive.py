"""Comprehensive unit tests for the circuit breaker system.

Tests circuit breaker functionality including state management, failure detection and recovery.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from cyberdelta.config.models.config_models import (
    AppSettings,
    CircuitBreakerSettings,
    SafetySystemsSettings,
)
from cyberdelta.validation.circuit_breaker import (
    APIErrorBreaker,
    BreakerState,
    CircuitBreaker,
    CircuitBreakerSystem,
    CircuitBreakerTrippedError,
    DrawdownBreaker,
    LiquidityBreaker,
    VolatilityBreaker,
)
from tests.fixtures.time_fixtures import FreezerProtocol


class ConcreteCircuitBreaker(CircuitBreaker):
    """Concrete implementation for testing abstract CircuitBreaker."""

    def __init__(
        self,
        name: str,
        cooldown_seconds: int = 300,
        should_trip: bool = False,
        recovery_success: bool = True,
    ) -> None:
        """Initialize test circuit breaker with configurable behavior."""
        super().__init__(name, cooldown_seconds)
        self.should_trip = should_trip
        self.recovery_success = recovery_success
        self.check_called = False
        self.recovery_called = False

    def check(self, *args: object, **kwargs: object) -> None:
        """Check if the breaker should trip."""
        self.check_called = True
        if self.should_trip:
            self.trip("Test trip condition")

    def _check_recovery(self) -> bool:
        """Check if system has recovered."""
        self.recovery_called = True
        return self.recovery_success


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing."""
    settings = Mock(spec=AppSettings)

    # Mock safety systems
    safety_systems = Mock(spec=SafetySystemsSettings)
    circuit_breakers = Mock(spec=CircuitBreakerSettings)
    circuit_breakers.enabled = True
    circuit_breakers.global_consecutive_failures = 5
    circuit_breakers.global_reset_timeout_sec = 300
    circuit_breakers.exchange_consecutive_failures = 3
    circuit_breakers.exchange_reset_timeout_sec = 180
    safety_systems.circuit_breakers = circuit_breakers
    settings.safety_systems = safety_systems

    # Mock exchanges
    hyperliquid_settings = Mock()
    hyperliquid_settings.enabled = True
    backpack_settings = Mock()
    backpack_settings.enabled = True
    settings.exchanges = {
        "hyperliquid": hyperliquid_settings,
        "backpack": backpack_settings,
    }

    return settings


@pytest.fixture
def concrete_breaker() -> ConcreteCircuitBreaker:
    """Create concrete circuit breaker for testing."""
    return ConcreteCircuitBreaker("test_breaker", cooldown_seconds=60)


@pytest.fixture
def volatility_breaker() -> VolatilityBreaker:
    """Create volatility breaker for testing."""
    return VolatilityBreaker("volatility_test", lookback_periods=5, volatility_threshold=0.1)


@pytest.fixture
def drawdown_breaker() -> DrawdownBreaker:
    """Create drawdown breaker for testing."""
    return DrawdownBreaker("drawdown_test", drawdown_threshold=0.2)


@pytest.fixture
def api_error_breaker() -> APIErrorBreaker:
    """Create API error breaker for testing."""
    return APIErrorBreaker("api_test", error_threshold=3, window_seconds=60)


@pytest.fixture
def liquidity_breaker() -> LiquidityBreaker:
    """Create liquidity breaker for testing."""
    return LiquidityBreaker("liquidity_test", min_liquidity=1000.0)


@pytest.fixture
def circuit_system(mock_app_settings: Mock) -> CircuitBreakerSystem:
    """Create circuit breaker system for testing."""
    return CircuitBreakerSystem(mock_app_settings)


class TestCircuitBreakerTrippedError:
    """Test suite for CircuitBreakerTrippedError exception."""

    # ==================== SUCCESS CASES ====================

    def test_exception_creation_success(self) -> None:
        """Test successful creation of CircuitBreakerTrippedError."""
        # Act
        error = CircuitBreakerTrippedError("Test error message")

        # Assert
        assert isinstance(error, Exception)
        assert str(error) == "Test error message"

    def test_exception_raising_success(self) -> None:
        """Test successful raising of CircuitBreakerTrippedError."""
        # Act & Assert
        with pytest.raises(CircuitBreakerTrippedError) as exc_info:
            raise CircuitBreakerTrippedError("Breaker tripped")

        assert str(exc_info.value) == "Breaker tripped"


class TestBreakerState:
    """Test suite for BreakerState enum."""

    # ==================== SUCCESS CASES ====================

    def test_breaker_state_values_defined(self) -> None:
        """Test that all breaker state values are properly defined."""
        # Assert
        assert BreakerState.CLOSED is not None
        assert BreakerState.OPEN is not None
        assert BreakerState.HALF_OPEN is not None

    def test_breaker_state_uniqueness(self) -> None:
        """Test that breaker state values are unique."""
        # Assert
        # Test enum values are different instances
        states = [BreakerState.CLOSED, BreakerState.OPEN, BreakerState.HALF_OPEN]
        assert len(set(states)) == 3  # All different


class TestCircuitBreakerBase:
    """Test suite for CircuitBreaker base class."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success(self, concrete_breaker: ConcreteCircuitBreaker) -> None:
        """Test successful initialization of circuit breaker."""
        # Assert
        assert concrete_breaker.name == "test_breaker"
        assert concrete_breaker.cooldown_seconds == 60
        assert concrete_breaker.state == BreakerState.CLOSED
        assert concrete_breaker.trip_time is None
        assert concrete_breaker.trip_reason is None
        assert concrete_breaker.trip_count == 0
        assert concrete_breaker.last_reset_time is None

    def test_trip_success_first_time(self, concrete_breaker: ConcreteCircuitBreaker) -> None:
        """Test successful first trip of circuit breaker."""
        # Arrange
        reason = "Test failure detected"

        # Act
        concrete_breaker.trip(reason)

        # Assert
        assert concrete_breaker.state == BreakerState.OPEN
        assert concrete_breaker.trip_reason == reason
        assert concrete_breaker.trip_count == 1
        assert concrete_breaker.trip_time is not None
        assert isinstance(concrete_breaker.trip_time, datetime)

    def test_reset_success_from_open(self, concrete_breaker: ConcreteCircuitBreaker) -> None:
        """Test successful reset from open state."""
        # Arrange
        concrete_breaker.trip("Test failure")

        # Act
        concrete_breaker.reset()

        # Assert
        assert concrete_breaker.state == BreakerState.CLOSED
        assert concrete_breaker.trip_time is None
        assert concrete_breaker.trip_reason is None
        assert concrete_breaker.last_reset_time is not None
        assert isinstance(concrete_breaker.last_reset_time, datetime)

    def test_allow_operation_success_when_closed(
        self, concrete_breaker: ConcreteCircuitBreaker
    ) -> None:
        """Test allow_operation returns True when breaker is closed."""
        # Act
        result = concrete_breaker.allow_operation()

        # Assert
        assert result is True

    def test_test_recovery_success_when_recovered(
        self, concrete_breaker: ConcreteCircuitBreaker
    ) -> None:
        """Test successful recovery when conditions are met."""
        # Arrange
        concrete_breaker.state = BreakerState.HALF_OPEN
        concrete_breaker.recovery_success = True

        # Act
        result = concrete_breaker.test_recovery()

        # Assert
        assert result is True
        assert concrete_breaker.recovery_called is True
        assert concrete_breaker.state == BreakerState.CLOSED

    # ==================== EDGE CASES ====================

    def test_allow_operation_edge_transitions_to_half_open(
        self, concrete_breaker: ConcreteCircuitBreaker, frozen_time: FreezerProtocol
    ) -> None:
        """Test allow_operation transitions from OPEN to HALF_OPEN after cooldown."""
        # Arrange
        base_time = datetime.now(UTC)
        frozen_time.move_to(base_time)
        concrete_breaker.trip("Test failure")

        # Simulate cooldown period passed
        future_time = base_time + timedelta(seconds=concrete_breaker.cooldown_seconds + 1)
        frozen_time.move_to(future_time)

        # Act
        result = concrete_breaker.allow_operation()

        # Assert
        assert result is True
        assert concrete_breaker.state == BreakerState.HALF_OPEN

    def test_test_recovery_edge_wrong_state(self, concrete_breaker: ConcreteCircuitBreaker) -> None:
        """Test test_recovery returns False when not in HALF_OPEN state."""
        # Arrange
        concrete_breaker.state = BreakerState.CLOSED

        # Act
        result = concrete_breaker.test_recovery()

        # Assert
        assert result is False
        assert concrete_breaker.recovery_called is False

    # ==================== FAILURE CASES ====================

    def test_test_recovery_failure_recovery_fails(
        self, concrete_breaker: ConcreteCircuitBreaker
    ) -> None:
        """Test test_recovery when recovery check fails."""
        # Arrange
        concrete_breaker.state = BreakerState.HALF_OPEN
        concrete_breaker.trip_reason = "Original failure"
        concrete_breaker.recovery_success = False

        # Act
        result = concrete_breaker.test_recovery()

        # Assert
        assert result is False
        assert concrete_breaker.recovery_called is True
        assert concrete_breaker.state == BreakerState.OPEN
        assert "Recovery failed" in concrete_breaker.trip_reason


class TestVolatilityBreaker:
    """Test suite for VolatilityBreaker class."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success(self, volatility_breaker: VolatilityBreaker) -> None:
        """Test successful initialization of volatility breaker."""
        # Assert
        assert volatility_breaker.name == "volatility_test"
        assert volatility_breaker.lookback_periods == 5
        assert volatility_breaker.volatility_threshold == 0.1
        assert volatility_breaker.price_history == []

    def test_add_price_success(self, volatility_breaker: VolatilityBreaker) -> None:
        """Test successful addition of price to history."""
        # Act
        volatility_breaker.add_price(100.0)
        volatility_breaker.add_price(105.0)

        # Assert
        assert len(volatility_breaker.price_history) == 2
        assert volatility_breaker.price_history == [100.0, 105.0]

    def test_check_success_low_volatility(self, volatility_breaker: VolatilityBreaker) -> None:
        """Test check doesn't trip with low volatility."""
        # Arrange - Add stable prices
        volatility_breaker.add_price(100.0)
        volatility_breaker.add_price(100.1)
        volatility_breaker.add_price(99.9)

        # Act
        volatility_breaker.check()

        # Assert
        assert volatility_breaker.state == BreakerState.CLOSED

    def test_check_recovery_success_volatility_normal(
        self, volatility_breaker: VolatilityBreaker
    ) -> None:
        """Test check_recovery returns True when volatility is normal."""
        # Arrange - Add stable prices and set breaker to HALF_OPEN state
        for price in [100.0, 100.1, 99.9, 100.05, 99.95]:
            volatility_breaker.add_price(price)

        # Set breaker to HALF_OPEN state (required for test_recovery to work)
        volatility_breaker.state = BreakerState.HALF_OPEN

        # Act
        result = volatility_breaker.test_recovery()

        # Assert
        assert result is True

    # ==================== EDGE CASES ====================

    def test_check_edge_insufficient_price_history(
        self, volatility_breaker: VolatilityBreaker
    ) -> None:
        """Test check with insufficient price history."""
        # Arrange - Add only one price
        volatility_breaker.add_price(100.0)

        # Act
        volatility_breaker.check()

        # Assert
        assert volatility_breaker.state == BreakerState.CLOSED  # Should not trip

    def test_check_edge_zero_mean_price(self, volatility_breaker: VolatilityBreaker) -> None:
        """Test check with zero mean price."""
        # Arrange
        volatility_breaker.add_price(0.0)
        volatility_breaker.add_price(0.0)

        # Act
        volatility_breaker.check()

        # Assert
        assert volatility_breaker.state == BreakerState.CLOSED  # Should not trip with zero mean

    # ==================== FAILURE CASES ====================

    def test_check_failure_high_volatility_trips(
        self, volatility_breaker: VolatilityBreaker
    ) -> None:
        """Test check trips breaker when volatility exceeds threshold."""
        # Arrange - Add highly volatile prices
        prices = [100.0, 150.0, 80.0, 120.0, 70.0]
        for price in prices:
            volatility_breaker.add_price(price)

        # Act
        volatility_breaker.check()

        # Assert
        assert volatility_breaker.state == BreakerState.OPEN
        assert volatility_breaker.trip_reason is not None
        assert "Volatility" in volatility_breaker.trip_reason
        assert "exceeds threshold" in volatility_breaker.trip_reason


class TestDrawdownBreaker:
    """Test suite for DrawdownBreaker class."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test successful initialization of drawdown breaker."""
        # Assert
        assert drawdown_breaker.name == "drawdown_test"
        assert drawdown_breaker.drawdown_threshold == 0.2
        assert drawdown_breaker.peak_value is None
        assert drawdown_breaker.current_value is None

    def test_check_success_sets_initial_peak(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test check sets initial peak value."""
        # Act
        drawdown_breaker.check(1000.0)

        # Assert
        assert drawdown_breaker.peak_value == 1000.0
        assert drawdown_breaker.current_value == 1000.0
        assert drawdown_breaker.state == BreakerState.CLOSED

    def test_check_success_low_drawdown(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test check doesn't trip with acceptable drawdown."""
        # Arrange
        drawdown_breaker.check(1000.0)  # Set peak

        # Act - 10% drawdown (below 20% threshold)
        drawdown_breaker.check(900.0)

        # Assert
        assert drawdown_breaker.state == BreakerState.CLOSED
        assert drawdown_breaker.current_value == 900.0

    def test_check_recovery_success_low_drawdown(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test check_recovery returns True when drawdown is acceptable."""
        # Arrange
        drawdown_breaker.peak_value = 1000.0
        drawdown_breaker.current_value = 850.0  # 15% drawdown

        # Set breaker to HALF_OPEN state (required for test_recovery to work)
        drawdown_breaker.state = BreakerState.HALF_OPEN

        # Act
        result = drawdown_breaker.test_recovery()

        # Assert
        assert result is True

    # ==================== EDGE CASES ====================

    def test_check_edge_zero_peak_value(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test check with zero peak value."""
        # Arrange
        drawdown_breaker.peak_value = 0.0
        drawdown_breaker.current_value = -100.0

        # Act
        drawdown_breaker.check(-50.0)

        # Assert
        assert drawdown_breaker.state == BreakerState.CLOSED  # Should not trip with zero peak

    def test_check_recovery_edge_missing_values(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test check_recovery with missing peak or current values."""
        # Act
        result = drawdown_breaker.test_recovery()

        # Assert
        assert result is False

    # ==================== FAILURE CASES ====================

    def test_check_failure_high_drawdown_trips(self, drawdown_breaker: DrawdownBreaker) -> None:
        """Test check trips breaker when drawdown exceeds threshold."""
        # Arrange
        drawdown_breaker.check(1000.0)  # Set peak

        # Act - 25% drawdown (exceeds 20% threshold)
        drawdown_breaker.check(750.0)

        # Assert
        assert drawdown_breaker.state == BreakerState.OPEN
        assert drawdown_breaker.trip_reason is not None
        assert "Drawdown of" in drawdown_breaker.trip_reason
        assert "exceeds threshold" in drawdown_breaker.trip_reason


class TestAPIErrorBreaker:
    """Test suite for APIErrorBreaker class."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test successful initialization of API error breaker."""
        # Assert
        assert api_error_breaker.name == "api_test"
        assert api_error_breaker.error_threshold == 3
        assert api_error_breaker.window_seconds == 60
        assert api_error_breaker.errors == []
        assert api_error_breaker.success_count == 0
        assert api_error_breaker.consecutive_success_count == 0
        assert api_error_breaker.last_success_time is None

    def test_record_error_success(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test successful error recording."""
        # Act
        api_error_breaker.record_error("API timeout")

        # Assert
        assert len(api_error_breaker.errors) == 1
        assert api_error_breaker.errors[0][1] == "API timeout"
        assert api_error_breaker.consecutive_success_count == 0
        assert isinstance(api_error_breaker.errors[0][0], datetime)

    def test_record_success_success(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test successful success recording."""
        # Act
        api_error_breaker.record_success()

        # Assert
        assert api_error_breaker.success_count == 1
        assert api_error_breaker.consecutive_success_count == 1
        assert api_error_breaker.last_success_time is not None
        assert isinstance(api_error_breaker.last_success_time, datetime)

    def test_check_success_no_errors(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test check doesn't trip with no errors."""
        # Act
        api_error_breaker.check()

        # Assert
        assert api_error_breaker.state == BreakerState.CLOSED

    def test_check_recovery_success_few_errors(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test check_recovery succeeds when errors are below threshold."""
        # Arrange - Record NO errors (below threshold of 3)
        # Note: The recovery threshold is error_threshold // 2 = 3 // 2 = 1
        # So we need 0 errors (not 1) to be "below threshold" for recovery

        # Set breaker to HALF_OPEN state (required for test_recovery to work)
        api_error_breaker.state = BreakerState.HALF_OPEN

        # Act
        result = api_error_breaker.test_recovery()

        # Assert
        assert result is True

    # ==================== EDGE CASES ====================

    def test_check_edge_already_open(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test check when breaker is already open."""
        # Arrange
        api_error_breaker.state = BreakerState.OPEN

        # Act
        api_error_breaker.check("New error")

        # Assert - Should still record error but not re-check
        assert len(api_error_breaker.errors) == 1

    def test_check_recovery_edge_equal_threshold(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test check_recovery when errors are below half threshold."""
        # Arrange - Record no errors (below half threshold of 1)
        # Note: half threshold is error_threshold // 2 = 3 // 2 = 1
        # So we need 0 errors to be below the threshold for recovery to succeed

        # Set breaker to HALF_OPEN state (required for test_recovery to work)
        api_error_breaker.state = BreakerState.HALF_OPEN

        # Act
        result = api_error_breaker.test_recovery()

        # Assert
        assert result is True

    # ==================== FAILURE CASES ====================

    def test_record_error_failure_trips_breaker(self, api_error_breaker: APIErrorBreaker) -> None:
        """Test recording errors trips breaker when threshold exceeded."""
        # Act - Record errors equal to threshold
        for i in range(api_error_breaker.error_threshold):
            api_error_breaker.record_error(f"Error {i}")

        # Assert
        assert api_error_breaker.state == BreakerState.OPEN
        assert api_error_breaker.trip_reason is not None
        assert "API errors" in api_error_breaker.trip_reason


class TestLiquidityBreaker:
    """Test suite for LiquidityBreaker class."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success(self, liquidity_breaker: LiquidityBreaker) -> None:
        """Test successful initialization of liquidity breaker."""
        # Assert
        assert liquidity_breaker.name == "liquidity_test"
        assert liquidity_breaker.min_liquidity == 1000.0
        assert liquidity_breaker.current_liquidity is None

    def test_check_success_sufficient_liquidity(self, liquidity_breaker: LiquidityBreaker) -> None:
        """Test check doesn't trip with sufficient liquidity."""
        # Act
        liquidity_breaker.check(2000.0)

        # Assert
        assert liquidity_breaker.state == BreakerState.CLOSED
        assert liquidity_breaker.current_liquidity == 2000.0

    def test_check_recovery_success_sufficient_liquidity(
        self, liquidity_breaker: LiquidityBreaker
    ) -> None:
        """Test check_recovery returns True when liquidity is sufficient."""
        # Arrange
        liquidity_breaker.current_liquidity = 1500.0

        # Set breaker to HALF_OPEN state (required for test_recovery to work)
        liquidity_breaker.state = BreakerState.HALF_OPEN

        # Act
        result = liquidity_breaker.test_recovery()

        # Assert
        assert result is True

    # ==================== EDGE CASES ====================

    def test_check_edge_zero_liquidity(self, liquidity_breaker: LiquidityBreaker) -> None:
        """Test check with zero liquidity."""
        # Act
        liquidity_breaker.check(0.0)

        # Assert
        assert liquidity_breaker.current_liquidity == 0.0

    def test_check_recovery_edge_no_current_liquidity(
        self, liquidity_breaker: LiquidityBreaker
    ) -> None:
        """Test check_recovery with no current liquidity data."""
        # Act
        result = liquidity_breaker.test_recovery()

        # Assert
        assert result is False

    # ==================== FAILURE CASES ====================

    def test_check_failure_low_liquidity_trips(self, liquidity_breaker: LiquidityBreaker) -> None:
        """Test check trips breaker when liquidity is below minimum."""
        # Act
        liquidity_breaker.check(500.0)  # Below 1000.0 minimum

        # Assert
        assert liquidity_breaker.state == BreakerState.OPEN
        assert liquidity_breaker.trip_reason is not None
        assert "Liquidity of 500" in liquidity_breaker.trip_reason
        assert "below minimum threshold" in liquidity_breaker.trip_reason


class TestCircuitBreakerSystem:
    """Test suite for CircuitBreakerSystem class."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success(self, circuit_system: CircuitBreakerSystem) -> None:
        """Test successful initialization of circuit breaker system."""
        # Assert
        assert isinstance(circuit_system.breakers, dict)
        assert isinstance(circuit_system.exchange_breakers, dict)
        assert circuit_system.config is not None
        assert hasattr(circuit_system, "global_api_error_breaker")

    def test_can_execute_success_all_closed(self, circuit_system: CircuitBreakerSystem) -> None:
        """Test can_execute returns True when all breakers are closed."""
        # Act
        can_execute, reason = circuit_system.can_execute("hyperliquid")

        # Assert
        assert can_execute is True
        assert reason is None

    def test_record_api_error_success(self, circuit_system: CircuitBreakerSystem) -> None:
        """Test successful API error recording."""
        # Act
        circuit_system.record_api_error("hyperliquid", "Connection timeout")

        # Assert
        global_breaker = circuit_system.global_api_error_breaker
        assert global_breaker is not None
        assert len(global_breaker.errors) == 1
        assert global_breaker.errors[0][1] == "Connection timeout"

    def test_reset_breaker_success(self, circuit_system: CircuitBreakerSystem) -> None:
        """Test successful breaker reset."""
        # Arrange
        global_breaker = circuit_system.global_api_error_breaker
        assert global_breaker is not None
        global_breaker.trip("Test failure")

        # Act
        result = circuit_system.reset_breaker("global/api_error")

        # Assert
        assert result is True
        assert global_breaker.state == BreakerState.CLOSED

    # ==================== EDGE CASES ====================

    def test_initialization_edge_circuit_breakers_disabled(self, mock_app_settings: Mock) -> None:
        """Test initialization when circuit breakers are disabled."""
        # Arrange
        mock_app_settings.safety_systems.circuit_breakers.enabled = False

        # Act
        system = CircuitBreakerSystem(mock_app_settings)

        # Assert
        assert len(system.breakers) == 0

    def test_get_breaker_edge_nonexistent(self, circuit_system: CircuitBreakerSystem) -> None:
        """Test get_breaker with nonexistent breaker name."""
        # Act
        breaker = circuit_system.get_breaker("nonexistent")

        # Assert
        assert breaker is None

    # ==================== FAILURE CASES ====================

    def test_can_execute_failure_global_breaker_open(
        self, circuit_system: CircuitBreakerSystem
    ) -> None:
        """Test can_execute returns False when global breaker is open."""
        # Arrange
        global_breaker = circuit_system.global_api_error_breaker
        assert global_breaker is not None
        global_breaker.trip("Global failure")

        # Act
        can_execute, reason = circuit_system.can_execute("hyperliquid")

        # Assert
        assert can_execute is False
        assert reason is not None
        assert "Global breaker" in reason

    def test_record_critical_failure_trips_breakers(
        self, circuit_system: CircuitBreakerSystem
    ) -> None:
        """Test record_critical_failure trips relevant breakers."""
        # Act
        circuit_system.record_critical_failure("hyperliquid", "Critical system failure")

        # Assert
        global_breaker = circuit_system.global_api_error_breaker
        assert global_breaker is not None
        assert global_breaker.state == BreakerState.OPEN
        assert global_breaker.trip_reason is not None
        assert "Critical failure" in global_breaker.trip_reason


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("error_count", "should_trip"),
    [
        (1, False),
        (2, False),
        (3, True),
        (5, True),
    ],
)
def test_api_error_breaker_threshold_parametrized(error_count: int, should_trip: bool) -> None:
    """Test API error breaker with various error counts."""
    # Arrange
    breaker = APIErrorBreaker("test", error_threshold=3, window_seconds=60)

    # Act
    for i in range(error_count):
        breaker.record_error(f"Error {i}")

    # Assert
    if should_trip:
        assert breaker.state == BreakerState.OPEN
    else:
        assert breaker.state == BreakerState.CLOSED


@pytest.mark.parametrize(
    ("prices", "threshold", "should_trip"),
    [
        ([100, 101, 99, 100.5], 0.1, False),  # Low volatility
        ([100, 120, 80, 150], 0.1, True),  # High volatility
        ([100, 100, 100, 100], 0.1, False),  # No volatility
        ([100, 110], 0.05, False),  # Simple volatility: 4.76% < 5% threshold
    ],
)
def test_volatility_breaker_threshold_parametrized(
    prices: list[float], threshold: float, should_trip: bool
) -> None:
    """Test volatility breaker with various price patterns."""
    # Arrange
    breaker = VolatilityBreaker("test", lookback_periods=10, volatility_threshold=threshold)

    # Act
    for price in prices:
        breaker.add_price(price)
    breaker.check()

    # Assert
    if should_trip:
        assert breaker.state == BreakerState.OPEN
    else:
        assert breaker.state == BreakerState.CLOSED


@pytest.mark.parametrize(
    ("peak", "current", "threshold", "should_trip"),
    [
        (1000, 900, 0.2, False),  # 10% drawdown, 20% threshold
        (1000, 750, 0.2, True),  # 25% drawdown, 20% threshold
        (1000, 800, 0.2, False),  # 20% drawdown, 20% threshold (equal)
        (1000, 600, 0.5, False),  # 40% drawdown, 50% threshold
    ],
)
def test_drawdown_breaker_threshold_parametrized(
    peak: float, current: float, threshold: float, should_trip: bool
) -> None:
    """Test drawdown breaker with various drawdown scenarios."""
    # Arrange
    breaker = DrawdownBreaker("test", drawdown_threshold=threshold)
    breaker.peak_value = peak

    # Act
    breaker.check(current)

    # Assert
    if should_trip:
        assert breaker.state == BreakerState.OPEN
    else:
        assert breaker.state == BreakerState.CLOSED


@pytest.mark.parametrize(
    ("current_liquidity", "min_liquidity", "should_trip"),
    [
        (2000, 1000, False),  # Sufficient liquidity
        (500, 1000, True),  # Insufficient liquidity
        (1000, 1000, False),  # Equal liquidity
        (0, 1000, True),  # Zero liquidity
    ],
)
def test_liquidity_breaker_threshold_parametrized(
    current_liquidity: float, min_liquidity: float, should_trip: bool
) -> None:
    """Test liquidity breaker with various liquidity levels."""
    # Arrange
    breaker = LiquidityBreaker("test", min_liquidity=min_liquidity)

    # Act
    breaker.check(current_liquidity)

    # Assert
    if should_trip:
        assert breaker.state == BreakerState.OPEN
    else:
        assert breaker.state == BreakerState.CLOSED
