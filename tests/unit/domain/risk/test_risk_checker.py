"""Property-based tests for domain.risk.risk_checker module.

This module tests the critical risk checking utilities to ensure:
- Position limit enforcement maintains financial safety
- Signal validation logic catches dangerous trading conditions
- Maximum loss calculations preserve mathematical precision
- Configuration-driven behavior prevents hardcoded risk parameters
- Risk threshold validation maintains consistent behavior
- All combinations of risk scenarios are handled safely

SECURITY CRITICAL: Risk checking errors can lead to position sizes that
exceed safety limits, trades with excessive risk exposure, or failure to
detect dangerous market conditions that could result in catastrophic losses.
"""

from decimal import Decimal
from unittest.mock import MagicMock

from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.config.models import AppSettings
from cyberdelta.domain.risk.risk_checker import RiskChecker
from cyberdelta.models import TradeSignal
from cyberdelta.models.risk.assessment import PositionSize
from cyberdelta.symbols.models import Symbol


# =============================================================================
# HYPOTHESIS STRATEGIES FOR RISK CHECKER TESTING
# =============================================================================


def decimal_strategy(
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
    max_decimal_places: int = 8,
) -> SearchStrategy[Decimal]:
    """Generate Decimal values for financial testing.

    Args:
        min_value: Minimum decimal value (inclusive)
        max_value: Maximum decimal value (inclusive)
        max_decimal_places: Maximum number of decimal places

    Returns:
        Strategy generating valid Decimal values
    """
    min_val = min_value or Decimal(0)
    max_val = max_value or Decimal(1000000)

    return st.decimals(
        min_value=min_val,
        max_value=max_val,
        places=max_decimal_places,
        allow_nan=False,
        allow_infinity=False,
    )


def positive_decimal_strategy() -> SearchStrategy[Decimal]:
    """Generate positive decimal values for financial calculations.

    Returns:
        SearchStrategy generating positive Decimal values
    """
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal(1000000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid market prices.

    Returns:
        SearchStrategy generating realistic price Decimal values
    """
    return st.decimals(
        min_value=Decimal("0.01"),
        max_value=Decimal(100000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def position_value_strategy() -> SearchStrategy[Decimal]:
    """Generate position values for testing.

    Returns:
        SearchStrategy generating position value Decimal amounts
    """
    return st.decimals(
        min_value=Decimal(1),
        max_value=Decimal(100000),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    )


def exposure_strategy() -> SearchStrategy[Decimal]:
    """Generate exposure values for testing.

    Returns:
        SearchStrategy generating exposure Decimal amounts
    """
    return st.decimals(
        min_value=Decimal(0),
        max_value=Decimal(500000),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    )


def equity_strategy() -> SearchStrategy[Decimal]:
    """Generate total equity values for testing.

    Returns:
        SearchStrategy generating total equity Decimal amounts
    """
    return st.decimals(
        min_value=Decimal(1000),
        max_value=Decimal(1000000),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    )


def create_mock_app_settings(
    max_position_usd: Decimal = Decimal(10000),
    max_exposure_usd: Decimal = Decimal(50000),
    enable_profitability: bool = True,
    enable_price_sanity: bool = True,
    min_profitability: Decimal = Decimal("0.01"),
    min_price: Decimal = Decimal("0.01"),
    max_price: Decimal = Decimal(1000000),
) -> AppSettings:
    """Create a mock AppSettings for testing.

    Returns:
        Mocked AppSettings instance with risk configuration
    """
    settings = MagicMock(spec=AppSettings)

    # Mock the risk configuration structure
    global_risk = MagicMock()
    global_risk.max_position_usd = max_position_usd
    global_risk.max_total_exposure_usd = max_exposure_usd

    checkers = MagicMock()
    checkers.enable_profitability = enable_profitability
    checkers.enable_price_sanity = enable_price_sanity

    thresholds = MagicMock()
    thresholds.min_profitability = min_profitability
    thresholds.min_price = min_price
    thresholds.max_price = max_price
    checkers.thresholds = thresholds

    risk = MagicMock()
    risk.global_risk = global_risk
    risk.checkers = checkers

    settings.risk = risk

    return settings


def create_mock_position_size(
    value_usd: Decimal, quantity: Decimal = Decimal("1.0")
) -> PositionSize:
    """Create a mock PositionSize for testing.

    Returns:
        Mocked PositionSize instance
    """
    position_size = MagicMock(spec=PositionSize)
    position_size.value_usd = value_usd
    position_size.quantity = quantity
    return position_size


def create_mock_trade_signal(
    price: Decimal = Decimal(50000),
    take_profit: Decimal | None = None,
    stop_loss: Decimal | None = None,
) -> TradeSignal:
    """Create a mock TradeSignal for testing.

    Returns:
        Mocked TradeSignal instance
    """
    signal = MagicMock(spec=TradeSignal)
    signal.price = price
    signal.take_profit = take_profit
    signal.stop_loss = stop_loss

    # Create mock symbol
    mock_symbol = MagicMock(spec=Symbol)
    mock_symbol.value = "BTC"
    signal.symbol = mock_symbol

    return signal


# =============================================================================
# PROPERTY TESTS FOR BASIC LIMIT CHECKING
# =============================================================================


class TestRiskCheckerBasicLimitsProperties:
    """Property-based tests for basic limit checking logic."""

    @given(
        max_position_usd=position_value_strategy(),
        position_value=position_value_strategy(),
        current_exposure=exposure_strategy(),
        total_equity=equity_strategy(),
    )
    def test_position_size_limit_enforcement_property(
        self,
        max_position_usd: Decimal,
        position_value: Decimal,
        current_exposure: Decimal,
        total_equity: Decimal,
    ) -> None:
        """Property: Position size limits should be enforced correctly."""
        settings = create_mock_app_settings(max_position_usd=max_position_usd)
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(position_value)
        limit_violations: list[str] = []

        checker.check_basic_limits(position_size, current_exposure, total_equity, limit_violations)

        # Property: Violation should be recorded if position exceeds limit
        if position_value > max_position_usd:
            assert len(limit_violations) > 0
            assert any("exceeds max" in violation for violation in limit_violations)
            assert str(position_value) in " ".join(limit_violations)
            assert str(max_position_usd) in " ".join(limit_violations)

        # Property: All violation messages should be informative strings
        for violation in limit_violations:
            assert isinstance(violation, str)
            assert len(violation) > 0

    @given(
        max_exposure_usd=exposure_strategy(),
        position_value=position_value_strategy(),
        current_exposure=exposure_strategy(),
        total_equity=equity_strategy(),
    )
    def test_total_exposure_limit_enforcement_property(
        self,
        max_exposure_usd: Decimal,
        position_value: Decimal,
        current_exposure: Decimal,
        total_equity: Decimal,
    ) -> None:
        """Property: Total exposure limits should be enforced correctly."""
        settings = create_mock_app_settings(max_exposure_usd=max_exposure_usd)
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(position_value)
        limit_violations: list[str] = []

        checker.check_basic_limits(position_size, current_exposure, total_equity, limit_violations)

        # Calculate new exposure
        new_exposure = current_exposure + position_value

        # Property: Violation should be recorded if new exposure exceeds limit
        if new_exposure > max_exposure_usd:
            assert any("would exceed max" in violation for violation in limit_violations)
            assert str(new_exposure) in " ".join(limit_violations)
            assert str(max_exposure_usd) in " ".join(limit_violations)

        # Property: No exposure violation if within limits
        else:
            exposure_violations = [v for v in limit_violations if "would exceed max" in v]
            assert len(exposure_violations) == 0

    @given(
        position_value=position_value_strategy(),
        current_exposure=exposure_strategy(),
        total_equity=equity_strategy(),
    )
    def test_equity_sufficiency_check_property(
        self, position_value: Decimal, current_exposure: Decimal, total_equity: Decimal
    ) -> None:
        """Property: Equity sufficiency should be checked correctly."""
        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(position_value)
        limit_violations: list[str] = []

        checker.check_basic_limits(position_size, current_exposure, total_equity, limit_violations)

        # Property: Violation should be recorded if insufficient equity
        if total_equity < position_value:
            assert any("Insufficient equity" in violation for violation in limit_violations)
            assert str(total_equity) in " ".join(limit_violations)
            assert str(position_value) in " ".join(limit_violations)

        # Property: No equity violation if sufficient
        else:
            equity_violations = [v for v in limit_violations if "Insufficient equity" in v]
            assert len(equity_violations) == 0

    @given(
        max_position_usd=st.decimals(min_value=Decimal(1000), max_value=Decimal(10000), places=2),
        position_value=st.decimals(min_value=Decimal(500), max_value=Decimal(15000), places=2),
        current_exposure=st.decimals(min_value=Decimal(0), max_value=Decimal(5000), places=2),
        total_equity=st.decimals(min_value=Decimal(10000), max_value=Decimal(100000), places=2),
    )
    def test_multiple_violations_accumulation_property(
        self,
        max_position_usd: Decimal,
        position_value: Decimal,
        current_exposure: Decimal,
        total_equity: Decimal,
    ) -> None:
        """Property: Multiple violations should accumulate correctly."""
        settings = create_mock_app_settings(
            max_position_usd=max_position_usd,
            max_exposure_usd=Decimal(6000),  # Low exposure limit to trigger violations
        )
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(position_value)
        limit_violations: list[str] = []

        checker.check_basic_limits(position_size, current_exposure, total_equity, limit_violations)

        # Property: Each violation should be independent
        expected_violations = 0

        if position_value > max_position_usd:
            expected_violations += 1

        if current_exposure + position_value > Decimal(6000):
            expected_violations += 1

        if total_equity < position_value:
            expected_violations += 1

        # Property: Number of violations should match expected
        assert len(limit_violations) == expected_violations

        # Property: All violations should be unique messages
        assert len(limit_violations) == len(set(limit_violations))


# =============================================================================
# PROPERTY TESTS FOR SIGNAL LIMIT CHECKING
# =============================================================================


class TestRiskCheckerSignalLimitsProperties:
    """Property-based tests for signal limit checking logic."""

    @given(
        signal_price=price_strategy(),
        take_profit=price_strategy(),
        min_profitability=st.decimals(
            min_value=Decimal("0.001"), max_value=Decimal("0.1"), places=4
        ),
    )
    def test_profitability_check_property(
        self, signal_price: Decimal, take_profit: Decimal, min_profitability: Decimal
    ) -> None:
        """Property: Profitability checks should be enforced correctly."""
        settings = create_mock_app_settings(
            enable_profitability=True, min_profitability=min_profitability
        )
        checker = RiskChecker(settings)

        signal = create_mock_trade_signal(price=signal_price, take_profit=take_profit)
        limit_violations: list[str] = []

        checker.check_signal_limits(signal, limit_violations)

        # Calculate expected profit
        expected_profit = (take_profit - signal_price) / signal_price

        # Property: Violation should be recorded if profit below minimum
        if expected_profit < min_profitability:
            assert any("below minimum" in violation for violation in limit_violations)
            profitability_violations = [v for v in limit_violations if "below minimum" in v]
            assert len(profitability_violations) == 1
        else:
            # Property: No profitability violation if above minimum
            profitability_violations = [v for v in limit_violations if "below minimum" in v]
            assert len(profitability_violations) == 0

    @given(
        signal_price=price_strategy(),
        min_price=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100), places=2),
        max_price=st.decimals(min_value=Decimal(10000), max_value=Decimal(200000), places=2),
    )
    def test_price_sanity_check_property(
        self, signal_price: Decimal, min_price: Decimal, max_price: Decimal
    ) -> None:
        """Property: Price sanity checks should be enforced correctly."""
        assume(min_price < max_price)  # Ensure valid range

        settings = create_mock_app_settings(
            enable_price_sanity=True, min_price=min_price, max_price=max_price
        )
        checker = RiskChecker(settings)

        signal = create_mock_trade_signal(price=signal_price)
        limit_violations: list[str] = []

        checker.check_signal_limits(signal, limit_violations)

        # Property: Violation should be recorded if price outside range
        if signal_price < min_price or signal_price > max_price:
            assert any("outside valid range" in violation for violation in limit_violations)
            price_violations = [v for v in limit_violations if "outside valid range" in v]
            assert len(price_violations) == 1
            assert str(signal_price) in " ".join(limit_violations)
            assert str(min_price) in " ".join(limit_violations)
            assert str(max_price) in " ".join(limit_violations)
        else:
            # Property: No price violation if within range
            price_violations = [v for v in limit_violations if "outside valid range" in v]
            assert len(price_violations) == 0

    def test_disabled_checks_property(self) -> None:
        """Property: Disabled checks should not generate violations."""
        settings = create_mock_app_settings(enable_profitability=False, enable_price_sanity=False)
        checker = RiskChecker(settings)

        # Create signal with values that would violate if checks were enabled
        signal = create_mock_trade_signal(
            price=Decimal(999999),  # Very high price
            take_profit=Decimal("999999.01"),  # Tiny profit
        )
        limit_violations: list[str] = []

        checker.check_signal_limits(signal, limit_violations)

        # Property: No violations when checks are disabled
        assert len(limit_violations) == 0

    def test_missing_signal_data_handling_property(self) -> None:
        """Property: Missing signal data should be handled gracefully."""
        settings = create_mock_app_settings(enable_profitability=True)
        checker = RiskChecker(settings)

        # Create signal without take_profit
        signal = create_mock_trade_signal(price=Decimal(50000), take_profit=None)
        limit_violations: list[str] = []

        checker.check_signal_limits(signal, limit_violations)

        # Property: No profitability violation when data is missing
        profitability_violations = [v for v in limit_violations if "below minimum" in v]
        assert len(profitability_violations) == 0


# =============================================================================
# PROPERTY TESTS FOR MAX LOSS CALCULATION
# =============================================================================


class TestRiskCheckerMaxLossProperties:
    """Property-based tests for maximum loss calculation logic."""

    @given(
        position_quantity=positive_decimal_strategy(),
        entry_price=price_strategy(),
        stop_loss=price_strategy(),
    )
    def test_max_loss_calculation_property(
        self, position_quantity: Decimal, entry_price: Decimal, stop_loss: Decimal
    ) -> None:
        """Property: Max loss calculation should be mathematically correct."""
        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(
            value_usd=Decimal(10000), quantity=position_quantity
        )

        max_loss = checker.calculate_max_loss(position_size, entry_price, stop_loss)

        # Property: Result should not be None for valid inputs
        assert max_loss is not None

        # Property: Max loss should equal |entry_price - stop_loss| * quantity
        expected_loss = abs(entry_price - stop_loss) * position_quantity
        assert max_loss == expected_loss

        # Property: Result should be a Decimal
        assert isinstance(max_loss, Decimal)

        # Property: Result should be finite and non-negative
        assert max_loss.is_finite()
        assert max_loss >= Decimal(0)

    @given(
        position_quantity=positive_decimal_strategy(),
    )
    def test_max_loss_missing_data_handling_property(self, position_quantity: Decimal) -> None:
        """Property: Max loss calculation should handle missing data correctly."""
        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(
            value_usd=Decimal(10000), quantity=position_quantity
        )

        # Test missing entry price
        max_loss = checker.calculate_max_loss(position_size, None, Decimal(50000))
        assert max_loss is None

        # Test missing stop loss
        max_loss = checker.calculate_max_loss(position_size, Decimal(50000), None)
        assert max_loss is None

        # Test zero quantity
        zero_position = create_mock_position_size(value_usd=Decimal(10000), quantity=Decimal(0))
        max_loss = checker.calculate_max_loss(zero_position, Decimal(50000), Decimal(49000))
        assert max_loss is None

    @given(
        entry_price=st.decimals(min_value=Decimal(100), max_value=Decimal(10000), places=2),
        stop_loss_distance=st.decimals(min_value=Decimal(1), max_value=Decimal(50), places=2),
        quantity_multiplier=st.decimals(min_value=Decimal(1), max_value=Decimal(10), places=1),
    )
    def test_max_loss_scaling_property(
        self, entry_price: Decimal, stop_loss_distance: Decimal, quantity_multiplier: Decimal
    ) -> None:
        """Property: Max loss should scale linearly with position size."""
        # Ensure stop loss creates a valid scenario
        assume(stop_loss_distance < entry_price)

        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        base_quantity = Decimal("1.0")
        scaled_quantity = base_quantity * quantity_multiplier

        stop_loss = entry_price - stop_loss_distance  # Always create a loss scenario

        base_position = create_mock_position_size(value_usd=Decimal(10000), quantity=base_quantity)
        scaled_position = create_mock_position_size(
            value_usd=Decimal(10000), quantity=scaled_quantity
        )

        base_loss = checker.calculate_max_loss(base_position, entry_price, stop_loss)
        scaled_loss = checker.calculate_max_loss(scaled_position, entry_price, stop_loss)

        # Property: Loss should scale by the same factor as quantity
        assert base_loss is not None
        assert scaled_loss is not None

        if base_loss > 0:
            loss_ratio = scaled_loss / base_loss
            # Allow small rounding differences
            assert abs(loss_ratio - quantity_multiplier) < Decimal("0.000001")

    @given(
        base_price=price_strategy(),
        stop_distances=st.lists(
            st.decimals(min_value=Decimal(1), max_value=Decimal(1000), places=2),
            min_size=2,
            max_size=5,
        ),
        position_quantity=positive_decimal_strategy(),
    )
    def test_max_loss_proportional_to_stop_distance_property(
        self, base_price: Decimal, stop_distances: list[Decimal], position_quantity: Decimal
    ) -> None:
        """Property: Max loss should be proportional to stop loss distance."""
        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(
            value_usd=Decimal(10000), quantity=position_quantity
        )

        losses: list[Decimal] = []
        for distance in stop_distances:
            stop_loss = base_price - distance
            max_loss = checker.calculate_max_loss(position_size, base_price, stop_loss)
            assert max_loss is not None
            losses.append(max_loss)

        # Property: Losses should be ordered by stop distance
        for i in range(len(stop_distances) - 1):
            if stop_distances[i] < stop_distances[i + 1]:
                assert losses[i] < losses[i + 1]
            elif stop_distances[i] > stop_distances[i + 1]:
                assert losses[i] > losses[i + 1]


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestRiskCheckerIntegrationProperties:
    """Integration property tests across RiskChecker functionality."""

    @given(
        max_position_usd=position_value_strategy(),
        max_exposure_usd=exposure_strategy(),
        position_value=position_value_strategy(),
        current_exposure=exposure_strategy(),
        total_equity=equity_strategy(),
        signal_price=price_strategy(),
    )
    def test_configuration_driven_behavior_property(
        self,
        max_position_usd: Decimal,
        max_exposure_usd: Decimal,
        position_value: Decimal,
        current_exposure: Decimal,
        total_equity: Decimal,
        signal_price: Decimal,
    ) -> None:
        """Property: All behavior should be driven by configuration."""
        settings = create_mock_app_settings(
            max_position_usd=max_position_usd, max_exposure_usd=max_exposure_usd
        )
        checker = RiskChecker(settings)

        # Property: Configuration values should be accessible through config
        assert checker.config.risk.global_risk.max_position_usd == max_position_usd
        assert checker.config.risk.global_risk.max_total_exposure_usd == max_exposure_usd

        # Property: Risk checks should use these configured values
        position_size = create_mock_position_size(position_value)
        signal = create_mock_trade_signal(price=signal_price)

        limit_violations: list[str] = []
        checker.check_basic_limits(position_size, current_exposure, total_equity, limit_violations)
        checker.check_signal_limits(signal, limit_violations)

        # Property: All violation messages should reference configured limits
        for violation in limit_violations:
            if "exceeds max" in violation:
                assert str(max_position_usd) in violation
            elif "would exceed max" in violation:
                assert str(max_exposure_usd) in violation

    @given(
        scenarios=st.lists(
            st.tuples(
                position_value_strategy(), exposure_strategy(), equity_strategy(), price_strategy()
            ),
            min_size=3,
            max_size=10,
        )
    )
    def test_consistent_behavior_across_scenarios_property(
        self, scenarios: list[tuple[Decimal, Decimal, Decimal, Decimal]]
    ) -> None:
        """Property: Risk checker should behave consistently across scenarios."""
        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        for position_value, current_exposure, total_equity, signal_price in scenarios:
            position_size = create_mock_position_size(position_value)
            signal = create_mock_trade_signal(price=signal_price)

            limit_violations: list[str] = []
            checker.check_basic_limits(
                position_size, current_exposure, total_equity, limit_violations
            )
            checker.check_signal_limits(signal, limit_violations)

            # Property: All violations should be informative strings
            for violation in limit_violations:
                assert isinstance(violation, str)
                assert len(violation) > 10  # Should be descriptive

            # Property: Violation logic should be deterministic
            # Same inputs should produce same results
            repeat_violations: list[str] = []
            checker.check_basic_limits(
                position_size, current_exposure, total_equity, repeat_violations
            )
            checker.check_signal_limits(signal, repeat_violations)

            assert limit_violations == repeat_violations

    @given(
        entry_price=price_strategy(),
        position_quantity=positive_decimal_strategy(),
        stop_loss_multiplier=st.decimals(
            min_value=Decimal("0.9"), max_value=Decimal("0.99"), places=3
        ),
    )
    def test_comprehensive_risk_assessment_property(
        self, entry_price: Decimal, position_quantity: Decimal, stop_loss_multiplier: Decimal
    ) -> None:
        """Property: Comprehensive risk assessment should maintain mathematical consistency."""
        settings = create_mock_app_settings()
        checker = RiskChecker(settings)

        position_size = create_mock_position_size(
            value_usd=entry_price * position_quantity, quantity=position_quantity
        )

        stop_loss = entry_price * stop_loss_multiplier

        # Calculate max loss
        max_loss = checker.calculate_max_loss(position_size, entry_price, stop_loss)

        # Property: Max loss should be reasonable relative to position value
        assert max_loss is not None
        assert max_loss > Decimal(0)

        # Property: Max loss should not exceed position value for reasonable stops
        loss_percentage = max_loss / position_size.value_usd
        assert loss_percentage <= Decimal("0.2")  # Should not exceed 20% for this test scenario

        # Property: Mathematical relationship should hold
        expected_loss_per_unit = entry_price - stop_loss
        expected_total_loss = expected_loss_per_unit * position_quantity
        assert abs(max_loss - expected_total_loss) < Decimal("0.000001")
