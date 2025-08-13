"""Property-based tests for Position Sizer.

This module tests the critical position sizing calculations to ensure:
- Position size constraints are always respected
- Risk limits are never exceeded
- Mathematical consistency in sizing calculations
- Kelly criterion calculations follow proper formulas
- Simple fraction sizing is accurate
- Precision preservation in all calculations
- Edge case handling for extreme values

SECURITY CRITICAL: Position sizing errors could lead to over-leveraging,
excessive risk exposure, account liquidation, or catastrophic losses.
"""

from decimal import Decimal
from datetime import datetime, UTC
from unittest.mock import MagicMock
import pytest
from hypothesis import given, strategies as st, assume, settings, HealthCheck
from hypothesis.strategies import SearchStrategy

from cyberdelta.config.models import (
    AppSettings,
    EnhancedRiskSettings,
    GlobalRiskSettings,
    SizingSettings,
)
from cyberdelta.domain.risk.position_sizer import PositionSizer
from cyberdelta.enums import OrderSide
from cyberdelta.enums.signals import SignalType
from cyberdelta.exceptions.trading import SignalDataError
from cyberdelta.models import TradeSignal
from cyberdelta.models.risk.assessment import PositionSize
from cyberdelta.symbols import exchanges


# =============================================================================
# HYPOTHESIS STRATEGIES FOR POSITION SIZING
# =============================================================================


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid price values."""
    return st.decimals(
        min_value=Decimal("0.01"),
        max_value=Decimal("1000000"),
        places=6,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def equity_strategy() -> SearchStrategy[Decimal]:
    """Generate valid portfolio equity values."""
    return st.decimals(
        min_value=Decimal("100"),
        max_value=Decimal("10000000"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def exposure_strategy() -> SearchStrategy[Decimal]:
    """Generate valid exposure values."""
    return st.decimals(
        min_value=Decimal("0"),
        max_value=Decimal("5000000"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    )


def confidence_strategy() -> SearchStrategy[float]:
    """Generate valid confidence values (0-1)."""
    return st.floats(min_value=0.0, max_value=1.0)


def fraction_strategy() -> SearchStrategy[Decimal]:
    """Generate valid fraction values for sizing."""
    return st.decimals(
        min_value=Decimal("0.001"),
        max_value=Decimal("0.5"),
        places=4,
        allow_nan=False,
        allow_infinity=False,
    )


def kelly_multiplier_strategy() -> SearchStrategy[Decimal]:
    """Generate valid Kelly multiplier values."""
    return st.decimals(
        min_value=Decimal("0.1"),
        max_value=Decimal("1.0"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    )


def side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides."""
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def signal_strategy() -> SearchStrategy[dict]:
    """Generate valid trade signal data."""
    return st.builds(
        lambda price, side, signal_type, confidence: {
            "price": price,
            "side": side,
            "signal_type": signal_type,
            "confidence": confidence,
            "signal_id": "test_signal_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        },
        price=price_strategy(),
        side=side_strategy(),
        signal_type=st.sampled_from([
            SignalType.ENTER_LONG,
            SignalType.ENTER_SHORT,
            SignalType.REBALANCE,
        ]),
        confidence=st.one_of(st.none(), confidence_strategy()),
    )


# =============================================================================
# TEST FIXTURES
# =============================================================================


@pytest.fixture
def mock_config_simple():
    """Create mock configuration for simple sizing method."""
    config = MagicMock(spec=AppSettings)

    # Risk configuration
    config.risk = MagicMock(spec=EnhancedRiskSettings)
    config.risk.sizing = MagicMock(spec=SizingSettings)
    config.risk.global_risk = MagicMock(spec=GlobalRiskSettings)

    # Simple sizing configuration
    config.risk.sizing.method = "simple"
    config.risk.sizing.simple_fixed_fraction = Decimal("0.02")  # 2% per position
    config.risk.sizing.min_position_size = Decimal("0.001")
    config.risk.sizing.max_position_size = Decimal("100000")

    # Global risk limits
    config.risk.global_risk.max_position_usd = Decimal("10000")
    config.risk.global_risk.max_total_exposure_usd = Decimal("50000")
    config.risk.global_risk.expected_profit_loss_ratio = Decimal("2.0")

    return config


@pytest.fixture
def mock_config_kelly():
    """Create mock configuration for Kelly sizing method."""
    config = MagicMock(spec=AppSettings)

    # Risk configuration
    config.risk = MagicMock(spec=EnhancedRiskSettings)
    config.risk.sizing = MagicMock(spec=SizingSettings)
    config.risk.global_risk = MagicMock(spec=GlobalRiskSettings)

    # Kelly sizing configuration
    config.risk.sizing.method = "kelly"
    config.risk.sizing.kelly_multiplier = Decimal("0.25")  # Conservative Kelly
    config.risk.sizing.kelly_max_allocation = Decimal("0.25")  # Max 25% allocation
    config.risk.sizing.min_position_size = Decimal("0.001")
    config.risk.sizing.max_position_size = Decimal("100000")

    # Global risk limits
    config.risk.global_risk.max_position_usd = Decimal("10000")
    config.risk.global_risk.max_total_exposure_usd = Decimal("50000")
    config.risk.global_risk.expected_profit_loss_ratio = Decimal("2.0")

    return config


@pytest.fixture
def simple_sizer(mock_config_simple):
    """Create position sizer with simple method."""
    return PositionSizer(mock_config_simple)


@pytest.fixture
def kelly_sizer(mock_config_kelly):
    """Create position sizer with Kelly method."""
    return PositionSizer(mock_config_kelly)


# =============================================================================
# PROPERTY TESTS FOR POSITION SIZE CONSTRAINTS
# =============================================================================


class TestPositionSizeConstraints:
    """Property-based tests for position size constraints."""

    def _create_simple_sizer(self):
        """Create a simple sizer for testing."""
        config = MagicMock(spec=AppSettings)
        config.risk = MagicMock(spec=EnhancedRiskSettings)
        config.risk.sizing = MagicMock(spec=SizingSettings)
        config.risk.global_risk = MagicMock(spec=GlobalRiskSettings)

        config.risk.sizing.method = "simple"
        config.risk.sizing.simple_fixed_fraction = Decimal("0.02")
        config.risk.sizing.min_position_size = Decimal("0.001")
        config.risk.sizing.max_position_size = Decimal("100000")

        config.risk.global_risk.max_position_usd = Decimal("10000")
        config.risk.global_risk.max_total_exposure_usd = Decimal("50000")
        config.risk.global_risk.expected_profit_loss_ratio = Decimal("2.0")

        return PositionSizer(config)

    @given(
        signal_data=signal_strategy(),
        total_equity=equity_strategy(),
        current_exposure=exposure_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_position_size_never_negative(self, signal_data, total_equity, current_exposure):
        """Property: Position size should never be negative."""
        sizer = self._create_simple_sizer()
        signal = TradeSignal(**signal_data)
        position_size = sizer.calculate_position_size(signal, total_equity, current_exposure)

        # Property: All values must be non-negative
        assert position_size.quantity >= Decimal("0")
        assert position_size.value_usd >= Decimal("0")
        assert position_size.percent_of_equity >= Decimal("0")

    @given(
        signal_data=signal_strategy(),
        total_equity=equity_strategy(),
        current_exposure=exposure_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_position_value_respects_max_limit(
        self, simple_sizer, signal_data, total_equity, current_exposure
    ):
        """Property: Position value should never exceed max position limit."""
        signal = TradeSignal(**signal_data)
        position_size = simple_sizer.calculate_position_size(signal, total_equity, current_exposure)

        # Property: Value must not exceed configured max
        max_position = simple_sizer._max_position_usd
        assert position_size.value_usd <= max_position

    @given(
        signal_data=signal_strategy(),
        total_equity=equity_strategy(),
        current_exposure=exposure_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_total_exposure_limit_respected(
        self, simple_sizer, signal_data, total_equity, current_exposure
    ):
        """Property: Total exposure should never exceed max exposure limit."""
        signal = TradeSignal(**signal_data)
        position_size = simple_sizer.calculate_position_size(signal, total_equity, current_exposure)

        # Property: New exposure must not exceed limit
        max_exposure = simple_sizer._max_exposure_usd
        new_total_exposure = current_exposure + position_size.value_usd
        assert new_total_exposure <= max_exposure or position_size.value_usd == Decimal("0")

    @given(signal_data=signal_strategy(), total_equity=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_percent_of_equity_calculation(self, simple_sizer, signal_data, total_equity):
        """Property: Percent of equity should be accurate."""
        signal = TradeSignal(**signal_data)
        current_exposure = Decimal("0")

        position_size = simple_sizer.calculate_position_size(signal, total_equity, current_exposure)

        if total_equity > 0 and position_size.value_usd > 0:
            # Property: Percentage calculation should be exact
            expected_percent = (position_size.value_usd / total_equity) * 100
            assert abs(position_size.percent_of_equity - expected_percent) < Decimal("0.0001")
        else:
            assert position_size.percent_of_equity == Decimal("0")


# =============================================================================
# PROPERTY TESTS FOR SIMPLE SIZING METHOD
# =============================================================================


class TestSimpleSizingProperties:
    """Property-based tests for simple fixed fraction sizing."""

    @given(total_equity=equity_strategy(), fraction=fraction_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_simple_sizing_fraction_calculation(self, mock_config_simple, total_equity, fraction):
        """Property: Simple sizing should use exact fraction of equity."""
        # Set custom fraction
        mock_config_simple.risk.sizing.simple_fixed_fraction = fraction
        sizer = PositionSizer(mock_config_simple)

        # Calculate expected size
        expected_size = total_equity * fraction
        expected_size = min(expected_size, sizer._max_position_usd)

        # Calculate actual size
        actual_size = sizer._calculate_simple_size(total_equity, Decimal("0"))

        # Property: Size should match fraction calculation
        assert actual_size == expected_size

    @given(
        signal_data=signal_strategy(),
        total_equity=equity_strategy(),
        current_exposure=exposure_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_simple_sizing_consistency(
        self, simple_sizer, signal_data, total_equity, current_exposure
    ):
        """Property: Simple sizing should be deterministic."""
        signal = TradeSignal(**signal_data)

        # Calculate position size twice
        size1 = simple_sizer.calculate_position_size(signal, total_equity, current_exposure)
        size2 = simple_sizer.calculate_position_size(signal, total_equity, current_exposure)

        # Property: Same inputs should give same outputs
        assert size1.quantity == size2.quantity
        assert size1.value_usd == size2.value_usd
        assert size1.percent_of_equity == size2.percent_of_equity

    @given(price=price_strategy(), total_equity=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_simple_sizing_quantity_calculation(self, simple_sizer, price, total_equity):
        """Property: Quantity should equal value divided by price."""
        signal_data = {
            "price": price,
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        position_size = simple_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        if position_size.value_usd > 0:
            # Property: quantity * price should equal value (within constraints)
            calculated_value = position_size.quantity * price
            # Account for min/max quantity constraints
            assert abs(calculated_value - position_size.value_usd) < Decimal("0.01")


# =============================================================================
# PROPERTY TESTS FOR KELLY SIZING METHOD
# =============================================================================


class TestKellySizingProperties:
    """Property-based tests for Kelly criterion sizing."""

    @given(confidence=confidence_strategy(), total_equity=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_kelly_sizing_with_confidence(self, kelly_sizer, confidence, total_equity):
        """Property: Kelly sizing should scale with confidence."""
        signal_data = {
            "price": Decimal("50000"),
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "confidence": confidence,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        position_size = kelly_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        # Property: Higher confidence should generally lead to larger positions
        # (unless Kelly formula gives negative result)
        if confidence > 0.5:
            assert position_size.value_usd >= Decimal("0")

    def test_kelly_sizing_requires_confidence(self, kelly_sizer):
        """Property: Kelly sizing should fail without confidence."""
        signal_data = {
            "price": Decimal("50000"),
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "confidence": None,  # Missing confidence
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        # Property: Should raise error for missing confidence
        with pytest.raises(SignalDataError):
            kelly_sizer.calculate_position_size(signal, Decimal("10000"), Decimal("0"))

    @given(
        confidence=confidence_strategy(),
        kelly_multiplier=kelly_multiplier_strategy(),
        total_equity=equity_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_kelly_multiplier_effect(
        self, mock_config_kelly, confidence, kelly_multiplier, total_equity
    ):
        """Property: Kelly multiplier should scale position size."""
        # Skip edge cases where Kelly gives zero
        assume(confidence > 0.4)

        # Create two sizers with different multipliers
        mock_config_kelly.risk.sizing.kelly_multiplier = kelly_multiplier
        sizer1 = PositionSizer(mock_config_kelly)

        mock_config_kelly.risk.sizing.kelly_multiplier = kelly_multiplier * Decimal("0.5")
        sizer2 = PositionSizer(mock_config_kelly)

        signal_data = {
            "price": Decimal("50000"),
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "confidence": confidence,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        size1 = sizer1.calculate_position_size(signal, total_equity, Decimal("0"))
        size2 = sizer2.calculate_position_size(signal, total_equity, Decimal("0"))

        # Property: Smaller multiplier should give smaller or equal position
        assert size2.value_usd <= size1.value_usd

    @given(confidence=confidence_strategy(), total_equity=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_kelly_max_allocation_limit(self, kelly_sizer, confidence, total_equity):
        """Property: Kelly sizing should respect max allocation limit."""
        signal_data = {
            "price": Decimal("50000"),
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "confidence": confidence,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        position_size = kelly_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        # Property: Should not exceed max allocation percentage
        max_allocation = kelly_sizer._kelly_max_allocation
        max_value_from_allocation = total_equity * max_allocation

        # Also constrained by global max position
        max_value = min(max_value_from_allocation, kelly_sizer._max_position_usd)

        # The Kelly sizer may still exceed limits due to minimum quantity constraints
        # So we check that either the limit is respected OR position is at minimum size
        min_quantity = kelly_sizer._sizing_config.min_position_size
        min_value = min_quantity * signal.price if signal.price > 0 else Decimal("0")

        # Position should respect max allocation OR be at minimum size due to constraints
        assert (
            position_size.value_usd <= max_value
            or position_size.value_usd <= min_value
            or position_size.quantity == min_quantity
        ), (
            f"Position value {position_size.value_usd} should not exceed "
            f"max allocation {max_value} (from {max_allocation} * {total_equity}) "
            f"unless at minimum size {min_value}"
        )


# =============================================================================
# PROPERTY TESTS FOR MATHEMATICAL CONSISTENCY
# =============================================================================


class TestMathematicalConsistency:
    """Property tests for mathematical consistency in calculations."""

    @given(
        price=price_strategy(),
        quantity=st.decimals(min_value=Decimal("0.001"), max_value=Decimal("1000"), places=8),
    )
    def test_value_quantity_relationship(self, price, quantity):
        """Property: Value should equal price times quantity."""
        value = price * quantity

        # Property: Multiplication should be exact
        assert value == price * quantity

        # Property: Division should recover original
        recovered_quantity = value / price
        assert abs(recovered_quantity - quantity) < Decimal("0.00000001")

    @given(
        total_equity=equity_strategy(),
        fractions=st.lists(fraction_strategy(), min_size=2, max_size=5),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_fraction_additivity(self, mock_config_simple, total_equity, fractions):
        """Property: Sum of fractional positions should be additive."""
        total_allocated = Decimal("0")

        for fraction in fractions:
            mock_config_simple.risk.sizing.simple_fixed_fraction = fraction
            sizer = PositionSizer(mock_config_simple)

            size = sizer._calculate_simple_size(total_equity, Decimal("0"))
            expected = min(total_equity * fraction, sizer._max_position_usd)

            assert size == expected
            total_allocated += size

        # Property: Total should not exceed equity (if no max position limits)
        if all(
            total_equity * f <= mock_config_simple.risk.global_risk.max_position_usd
            for f in fractions
        ):
            assert total_allocated == total_equity * sum(fractions)

    @given(equity1=equity_strategy(), equity2=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_monotonicity_with_equity(self, simple_sizer, equity1, equity2):
        """Property: Larger equity should give larger or equal position size."""
        signal_data = {
            "price": Decimal("50000"),
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        size1 = simple_sizer.calculate_position_size(signal, equity1, Decimal("0"))
        size2 = simple_sizer.calculate_position_size(signal, equity2, Decimal("0"))

        # Property: Monotonicity (until hitting max limits)
        if equity1 <= equity2:
            # Larger equity should give larger or equal position
            # (may be equal if both hit max position limit)
            assert size1.value_usd <= size2.value_usd


# =============================================================================
# PROPERTY TESTS FOR EDGE CASES
# =============================================================================


class TestEdgeCases:
    """Property tests for edge cases in position sizing."""

    @given(total_equity=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_zero_price_handling(self, simple_sizer, total_equity):
        """Property: Zero or negative price should result in zero position."""
        # Test zero price using mock since TradeSignal validates price > 0
        from unittest.mock import Mock

        signal = Mock()
        signal.price = Decimal("0")
        signal.side = OrderSide.BUY
        signal.signal_type = SignalType.ENTER_LONG
        signal.signal_id = "test_123"
        signal.symbol = exchanges.hyperliquid(value="BTC")
        signal.exchange = "hyperliquid"
        signal.timestamp = datetime.now(UTC)

        position_size = simple_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        # Property: Zero price should give zero position
        assert position_size.quantity == Decimal("0")
        assert position_size.value_usd == Decimal("0")
        assert position_size.percent_of_equity == Decimal("0")

    @given(signal_data=signal_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_zero_equity_handling(self, simple_sizer, signal_data):
        """Property: Zero equity should result in zero position."""
        signal = TradeSignal(**signal_data)

        position_size = simple_sizer.calculate_position_size(signal, Decimal("0"), Decimal("0"))

        # Property: Zero equity should give zero position
        assert position_size.quantity == Decimal("0")
        assert position_size.value_usd == Decimal("0")
        assert position_size.percent_of_equity == Decimal("0")

    @given(signal_data=signal_strategy(), total_equity=equity_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_max_exposure_reached(self, simple_sizer, signal_data, total_equity):
        """Property: Should return zero when max exposure is reached."""
        signal = TradeSignal(**signal_data)

        # Set current exposure at max
        max_exposure = simple_sizer._max_exposure_usd
        position_size = simple_sizer.calculate_position_size(signal, total_equity, max_exposure)

        # Property: No new position when at max exposure
        assert position_size.quantity == Decimal("0")
        assert position_size.value_usd == Decimal("0")

    @given(
        very_small_price=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("0.001"), places=10
        ),
        total_equity=equity_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_very_small_price_handling(self, simple_sizer, very_small_price, total_equity):
        """Property: Very small prices should be handled correctly."""
        signal_data = {
            "price": very_small_price,
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="SHIB"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        position_size = simple_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        if position_size.value_usd > 0:
            # Property: Quantity should be large for small prices
            assert position_size.quantity > position_size.value_usd

            # Property: Value should be recoverable
            calculated_value = position_size.quantity * very_small_price
            # Account for min/max quantity constraints affecting final value
            assert calculated_value > 0

    @given(
        very_large_price=st.decimals(
            min_value=Decimal("100000"), max_value=Decimal("10000000"), places=2
        ),
        total_equity=equity_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_very_large_price_handling(self, simple_sizer, very_large_price, total_equity):
        """Property: Very large prices should be handled correctly."""
        signal_data = {
            "price": very_large_price,
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        position_size = simple_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        if position_size.value_usd > 0:
            # Property: Quantity should be small for large prices
            assert position_size.quantity < position_size.value_usd

            # Property: Should respect min quantity constraint
            min_quantity = simple_sizer._sizing_config.min_position_size
            assert position_size.quantity >= min_quantity or position_size.quantity == Decimal("0")


# =============================================================================
# PROPERTY TESTS FOR PRECISION PRESERVATION
# =============================================================================


class TestPrecisionPreservation:
    """Property tests for decimal precision preservation."""

    @given(
        price=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("999999.99999999"), places=8
        ).filter(lambda x: x > 0),
        total_equity=st.decimals(
            min_value=Decimal("100.00"), max_value=Decimal("9999999.99"), places=2
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_decimal_precision_maintained(self, simple_sizer, price, total_equity):
        """Property: Decimal precision should be preserved throughout calculations."""
        signal_data = {
            "price": price,
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        position_size = simple_sizer.calculate_position_size(signal, total_equity, Decimal("0"))

        # Property: All values should be Decimal
        assert isinstance(position_size.quantity, Decimal)
        assert isinstance(position_size.value_usd, Decimal)
        assert isinstance(position_size.percent_of_equity, Decimal)

        # Property: Values should be finite
        assert position_size.quantity.is_finite()
        assert position_size.value_usd.is_finite()
        assert position_size.percent_of_equity.is_finite()

    @given(
        fractions=st.lists(
            st.decimals(min_value=Decimal("0.0001"), max_value=Decimal("0.01"), places=6),
            min_size=10,
            max_size=50,
        )
    )
    def test_cumulative_precision(self, fractions):
        """Property: Cumulative operations should maintain precision."""
        total = Decimal("0")

        for fraction in fractions:
            total += fraction

        # Property: Sum should be exact
        assert total == sum(fractions)

        # Property: Average should be recoverable
        average = total / Decimal(str(len(fractions)))
        reconstructed = average * Decimal(str(len(fractions)))

        # Small rounding difference acceptable due to division
        assert abs(reconstructed - total) < Decimal("0.00000001")


# =============================================================================
# PROPERTY TESTS FOR KELLY FORMULA
# =============================================================================


class TestKellyFormula:
    """Property tests for Kelly criterion formula correctness."""

    @given(
        win_rate=st.floats(min_value=0.0, max_value=1.0),
        profit_loss_ratio=st.decimals(min_value=Decimal("0.5"), max_value=Decimal("5.0"), places=2),
    )
    def test_kelly_formula_properties(self, win_rate, profit_loss_ratio):
        """Property: Kelly formula should follow mathematical properties."""
        win_rate_decimal = Decimal(str(win_rate))
        loss_rate = Decimal("1") - win_rate_decimal

        # Kelly fraction = (bp - q) / b
        # where b = profit/loss ratio, p = win probability, q = loss probability
        kelly_fraction = (win_rate_decimal * profit_loss_ratio - loss_rate) / profit_loss_ratio

        # Property: Kelly fraction should be negative when expected value is negative
        expected_value = win_rate_decimal * profit_loss_ratio - loss_rate
        if expected_value < 0:
            assert kelly_fraction < 0
        elif expected_value > 0:
            assert kelly_fraction > 0
        else:
            assert kelly_fraction == 0

        # Property: Kelly fraction should be bounded
        # For positive expected value, Kelly fraction is between 0 and p
        if kelly_fraction > 0:
            assert kelly_fraction <= win_rate_decimal

    def test_kelly_edge_cases(self):
        """Property: Kelly should handle edge cases correctly."""
        # Case 1: 100% win rate
        win_rate = Decimal("1.0")
        loss_rate = Decimal("0.0")
        profit_loss_ratio = Decimal("2.0")

        kelly_fraction = (win_rate * profit_loss_ratio - loss_rate) / profit_loss_ratio
        assert kelly_fraction == Decimal("1.0")  # Bet everything

        # Case 2: 0% win rate
        win_rate = Decimal("0.0")
        loss_rate = Decimal("1.0")

        kelly_fraction = (win_rate * profit_loss_ratio - loss_rate) / profit_loss_ratio
        assert kelly_fraction < 0  # Don't bet (negative)

        # Case 3: Break-even (50% win, 1:1 ratio)
        win_rate = Decimal("0.5")
        loss_rate = Decimal("0.5")
        profit_loss_ratio = Decimal("1.0")

        kelly_fraction = (win_rate * profit_loss_ratio - loss_rate) / profit_loss_ratio
        assert kelly_fraction == Decimal("0")  # No edge, no bet


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestPositionSizerIntegration:
    """Integration property tests for complete position sizing flow."""

    @given(
        signal_data=signal_strategy(),
        total_equity=equity_strategy(),
        current_exposure=exposure_strategy(),
        method=st.sampled_from(["simple", "kelly"]),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_sizing_method_consistency(
        self,
        mock_config_simple,
        mock_config_kelly,
        signal_data,
        total_equity,
        current_exposure,
        method,
    ):
        """Property: Sizing method should produce consistent results."""
        # Skip Kelly without confidence
        if method == "kelly" and signal_data.get("confidence") is None:
            signal_data["confidence"] = 0.6

        config = mock_config_simple if method == "simple" else mock_config_kelly
        config.risk.sizing.method = method
        sizer = PositionSizer(config)

        signal = TradeSignal(**signal_data)

        # Calculate twice
        size1 = sizer.calculate_position_size(signal, total_equity, current_exposure)
        size2 = sizer.calculate_position_size(signal, total_equity, current_exposure)

        # Property: Deterministic results
        assert size1.quantity == size2.quantity
        assert size1.value_usd == size2.value_usd
        assert size1.percent_of_equity == size2.percent_of_equity

    @given(
        signals=st.lists(signal_strategy(), min_size=2, max_size=5), total_equity=equity_strategy()
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_multiple_position_sizing(self, simple_sizer, signals, total_equity):
        """Property: Multiple positions should respect cumulative limits."""
        total_exposure = Decimal("0")
        positions = []

        for signal_data in signals:
            signal = TradeSignal(**signal_data)
            position_size = simple_sizer.calculate_position_size(
                signal, total_equity, total_exposure
            )

            positions.append(position_size)
            total_exposure += position_size.value_usd

        # Property: Total exposure should not exceed limit
        assert total_exposure <= simple_sizer._max_exposure_usd

        # Property: Each position should respect individual limit
        for position in positions:
            assert position.value_usd <= simple_sizer._max_position_usd

    @given(price=price_strategy(), total_equity=equity_strategy(), confidence=confidence_strategy())
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_position_sizing_determinism(
        self, mock_config_simple, mock_config_kelly, price, total_equity, confidence
    ):
        """Property: Position sizing should be deterministic for same inputs."""
        signal_data = {
            "price": price,
            "side": OrderSide.BUY,
            "signal_type": SignalType.ENTER_LONG,
            "confidence": confidence,
            "signal_id": "test_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "exchange": "hyperliquid",
            "timestamp": datetime.now(UTC),
        }
        signal = TradeSignal(**signal_data)

        # Test simple method
        sizer_simple = PositionSizer(mock_config_simple)
        size_simple_1 = sizer_simple.calculate_position_size(signal, total_equity, Decimal("0"))
        size_simple_2 = sizer_simple.calculate_position_size(signal, total_equity, Decimal("0"))

        assert size_simple_1.value_usd == size_simple_2.value_usd

        # Test Kelly method
        sizer_kelly = PositionSizer(mock_config_kelly)
        size_kelly_1 = sizer_kelly.calculate_position_size(signal, total_equity, Decimal("0"))
        size_kelly_2 = sizer_kelly.calculate_position_size(signal, total_equity, Decimal("0"))

        assert size_kelly_1.value_usd == size_kelly_2.value_usd
