"""Property-based integration tests for validation service using Hypothesis.

Tests the complete ValidationService that orchestrates all validation rules
and ensures proper category ordering and rule execution.

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- Uses property-based testing for comprehensive coverage
- Tests complete validation flows
- Validates rule execution order and priority
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from cyberdelta.config.models import AppSettings
from cyberdelta.enums import (
    ExchangeName,
    OrderSide,
    OrderType,
    TimeInForce,
    TradingState,
    ValidationCategory,
)
from cyberdelta.infrastructure.validation.validation_service import ValidationService
from cyberdelta.models.market.order import Order
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.models.spot_balance import SpotBalance
from cyberdelta.models.validation import ValidationResult
from tests.common_symbols import BTC_USDC_BP


# --- Hypothesis Strategies ---


@st.composite
def decimal_strategy(
    draw: st.DrawFn, min_value: float = 0.000001, max_value: float = 100000.0
) -> Decimal:
    """Generate valid Decimal values for financial calculations.

    Returns:
        Decimal: A valid decimal value for financial calculations.
    """
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
            exclude_min=True,
        )
    )
    return Decimal(str(value))


@st.composite
def balance_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic balance values.

    Returns:
        Decimal: A realistic balance value.
    """
    return draw(decimal_strategy(min_value=0.0, max_value=1000000.0))


@st.composite
def price_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic price values.

    Returns:
        Decimal: A realistic price value.
    """
    return draw(decimal_strategy(min_value=0.01, max_value=100000.0))


@st.composite
def quantity_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic quantity values.

    Returns:
        Decimal: A realistic quantity value.
    """
    return draw(decimal_strategy(min_value=0.000001, max_value=1000.0))


@st.composite
def config_limits_strategy(draw: st.DrawFn) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Generate valid configuration limits (min_trade, max_trade, max_position, max_exposure).

    Returns:
        tuple[Decimal, Decimal, Decimal, Decimal]: Configuration limits tuple.
    """
    min_trade = draw(decimal_strategy(min_value=1.0, max_value=1000.0))
    max_trade = draw(decimal_strategy(min_value=float(min_trade), max_value=100000.0))
    max_position = draw(decimal_strategy(min_value=float(max_trade), max_value=500000.0))
    max_exposure = draw(decimal_strategy(min_value=float(max_position), max_value=1000000.0))
    return min_trade, max_trade, max_position, max_exposure


@st.composite
def exchange_limits_strategy(draw: st.DrawFn) -> tuple[Decimal, Decimal, float, float]:
    """Generate valid exchange limits (min_order, max_order, tick_size, lot_size).

    Returns:
        tuple[Decimal, Decimal, float, float]: Exchange limits tuple.
    """
    min_order = draw(decimal_strategy(min_value=1.0, max_value=100.0))
    max_order = draw(decimal_strategy(min_value=float(min_order), max_value=50000.0))
    tick_size = draw(st.sampled_from([0.01, 0.001, 0.0001, 0.1, 1.0]))
    lot_size = draw(st.sampled_from([0.001, 0.0001, 0.01, 0.1, 1.0]))
    return min_order, max_order, tick_size, lot_size


# --- Helper Functions ---


def create_mock_config(
    min_trade_value: Decimal = Decimal("100.00"),
    max_trade_value: Decimal = Decimal("50000.00"),
    max_position_usd: Decimal = Decimal("25000.00"),
    max_total_exposure_usd: Decimal = Decimal("100000.00"),
) -> Mock:
    """Create comprehensive mock AppSettings for testing.

    Returns:
        Mock: Mock AppSettings object for testing.
    """
    config = Mock(spec=AppSettings)

    # Mock validation config
    validation_config = Mock()
    validation_config.min_trade_value = min_trade_value
    validation_config.max_trade_value = max_trade_value
    config.validation = validation_config

    # Mock risk config
    risk_config = Mock()
    global_risk = Mock()
    global_risk.max_position_usd = max_position_usd
    global_risk.max_total_exposure_usd = max_total_exposure_usd
    risk_config.global_risk = global_risk
    config.risk = risk_config

    # Mock exchange configs
    backpack_config = Mock()
    backpack_config.tick_size = 0.01
    backpack_config.lot_size = 0.001
    backpack_config.min_order_size = Decimal("1.00")  # Lower minimum for tests
    backpack_config.max_order_size = Decimal("25000.00")

    config.exchanges = {"backpack": backpack_config}

    return config


def create_mock_portfolio_state(
    usdc_balance: Decimal = Decimal("15000.00"),
    btc_balance: Decimal = Decimal("2.0"),
) -> Mock:
    """Create mock portfolio state with realistic balances and positions.

    Returns:
        Mock: Mock portfolio state object.
    """
    portfolio_state = Mock(spec=PortfolioState)

    # Mock balances
    usdc_bal = Mock(spec=SpotBalance)
    usdc_bal.available_quantity = usdc_balance

    btc_bal = Mock(spec=SpotBalance)
    btc_bal.available_quantity = btc_balance

    portfolio_state.balances = {
        "backpack:USDC": usdc_bal,
        "backpack:BTC": btc_bal,
    }

    # Mock positions (empty for most tests)
    portfolio_state.positions = {}

    return portfolio_state


def create_valid_order(
    quantity: Decimal = Decimal("0.100"),
    price: Decimal = Decimal("10000.00"),
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.LIMIT,
) -> Order:
    """Create a valid test order that should pass all validations.

    Returns:
        Order: A valid test order.
    """
    return Order(
        symbol=BTC_USDC_BP,
        side=side,
        quantity_requested=quantity,
        price=price,
        order_type=order_type,
        time_in_force=TimeInForce.GTC,
        exchange=ExchangeName.BACKPACK,
        exchange_order_id="test_valid_order",
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name="test_strategy",
        signal_id="test_signal",
    )


class TestValidationServiceProperties:
    """Property-based integration tests for ValidationService."""

    def test_service_initialization(self) -> None:
        """Test that service initializes with all validation rules registered."""
        config = create_mock_config()
        validation_service = ValidationService(config)

        # Check that rules are registered in registry
        all_rules = validation_service.registry.get_rules()
        assert len(all_rules) > 0

        # Check that all categories have rules
        rules_by_category = validation_service.registry.get_rules_by_category()
        assert ValidationCategory.PRECISION in rules_by_category
        assert ValidationCategory.LIMITS in rules_by_category
        assert ValidationCategory.BALANCE in rules_by_category
        assert ValidationCategory.RISK in rules_by_category
        assert ValidationCategory.MARKET in rules_by_category

        # Check rule counts
        precision_rules = validation_service.registry.get_rules(ValidationCategory.PRECISION)
        assert len(precision_rules) == 2  # PricePrecisionRule, QuantityPrecisionRule

        limits_rules = validation_service.registry.get_rules(ValidationCategory.LIMITS)
        assert len(limits_rules) == 1  # OrderValueLimitsRule

        balance_rules = validation_service.registry.get_rules(ValidationCategory.BALANCE)
        assert len(balance_rules) == 1  # BalanceValidationRule

        risk_rules = validation_service.registry.get_rules(ValidationCategory.RISK)
        assert len(risk_rules) == 2  # MaxPositionRule, MaxExposureRule

        market_rules = validation_service.registry.get_rules(ValidationCategory.MARKET)
        assert len(market_rules) == 2  # MarketStatusRule, LiquidityRule

    @given(
        config_limits=config_limits_strategy(),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(
        max_examples=50,
        deadline=timedelta(seconds=1),
        suppress_health_check=[HealthCheck.filter_too_much],
    )
    @pytest.mark.asyncio
    async def test_valid_orders_within_all_limits_pass(
        self,
        config_limits: tuple[Decimal, Decimal, Decimal, Decimal],
        order_price: Decimal,
        order_quantity: Decimal,
    ) -> None:
        """Property: Orders within all configured limits should pass validation."""
        min_trade, max_trade, max_position, max_exposure = config_limits

        order_value = order_price * order_quantity

        # Only test orders that should be valid
        assume(min_trade <= order_value <= max_trade)
        assume(order_value <= max_position)  # Single order won't exceed position limit

        config = create_mock_config(
            min_trade_value=min_trade,
            max_trade_value=max_trade,
            max_position_usd=max_position,
            max_total_exposure_usd=max_exposure,
        )

        # Ensure exchange limits accommodate the order value
        # Set exchange max to be at least as large as max_trade to avoid conflicts
        exchange_max = max(max_trade, Decimal("50000.00"))
        config.exchanges["backpack"].max_order_size = exchange_max

        validation_service = ValidationService(config)

        # Create order with aligned precision
        aligned_quantity = (order_quantity // Decimal("0.001")) * Decimal("0.001")
        aligned_price = (order_price // Decimal("0.01")) * Decimal("0.01")

        # Ensure aligned values are positive
        if aligned_quantity <= 0:
            aligned_quantity = Decimal("0.001")
        if aligned_price <= 0:
            aligned_price = Decimal("0.01")

        # Recalculate order value with aligned values and ensure it's still within limits
        aligned_order_value = aligned_price * aligned_quantity

        # Skip this test case if alignment changed the value too much
        assume(min_trade <= aligned_order_value <= max_trade)
        assume(aligned_order_value <= max_position)

        # Create portfolio state with balance matching aligned order value
        portfolio_state = create_mock_portfolio_state(
            usdc_balance=aligned_order_value * Decimal(2)  # Sufficient balance for aligned value
        )

        order = create_valid_order(
            quantity=aligned_quantity, price=aligned_price, side=OrderSide.BUY
        )

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Property: Should be valid when within all limits
        assert result.is_valid, (
            f"Order with aligned value {aligned_order_value} should be valid within limits "
            f"[{min_trade}, {max_trade}] and sufficient balance. Violations: {result.violations}"
        )

    @given(
        insufficient_balance=balance_strategy(),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_insufficient_balance_always_fails(
        self, insufficient_balance: Decimal, order_price: Decimal, order_quantity: Decimal
    ) -> None:
        """Property: Orders with insufficient balance should always fail validation."""
        order_value = order_price * order_quantity

        # Ensure balance is insufficient
        assume(insufficient_balance < order_value)

        config = create_mock_config()
        validation_service = ValidationService(config)
        portfolio_state = create_mock_portfolio_state(usdc_balance=insufficient_balance)

        # Create order with aligned precision to avoid precision validation failures
        aligned_quantity = (order_quantity // Decimal("0.001")) * Decimal("0.001")
        aligned_price = (order_price // Decimal("0.01")) * Decimal("0.01")

        # Ensure aligned values are positive
        if aligned_quantity <= 0:
            aligned_quantity = Decimal("0.001")
        if aligned_price <= 0:
            aligned_price = Decimal("0.01")

        order = create_valid_order(
            quantity=aligned_quantity, price=aligned_price, side=OrderSide.BUY
        )

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Property: Should always fail with insufficient balance
        assert not result.is_valid, (
            f"Order costing {order_value} should fail with balance {insufficient_balance}"
        )
        assert any("balance" in v.lower() or "insufficient" in v.lower() for v in result.violations)

    @given(
        trading_state=st.sampled_from([
            TradingState.ACTIVE,
            TradingState.REDUCING,
            TradingState.HALTED,
            TradingState.RECONCILING,
        ]),
        is_reduce_only=st.booleans(),
        is_reconciling=st.booleans(),
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_trading_state_behavior_properties(
        self, trading_state: TradingState, is_reduce_only: bool, is_reconciling: bool
    ) -> None:
        """Property: Validation behavior should be consistent across trading states."""
        config = create_mock_config()
        validation_service = ValidationService(config)
        portfolio_state = create_mock_portfolio_state()

        order = create_valid_order()

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=trading_state,
            is_reconciling=is_reconciling,
            is_reduce_only=is_reduce_only,
        )

        # Property: Reconciliation mode should skip most checks
        if is_reconciling:
            # During reconciliation, balance and risk checks should be skipped
            if not result.is_valid:
                violations_text = " ".join(result.violations).lower()
                assert "balance" not in violations_text
                assert "insufficient" not in violations_text

        # Property: Halted state should block non-reduce orders
        elif trading_state == TradingState.HALTED and not is_reduce_only:
            assert not result.is_valid
            violations_text = " ".join(result.violations).lower()
            assert "halted" in violations_text

        # Property: Reduce-only orders should bypass some restrictions
        elif is_reduce_only and trading_state == TradingState.HALTED:
            violations_text = " ".join(result.violations).lower() if result.violations else ""
            assert "halted" not in violations_text  # Should not be blocked by halt

    @given(
        misaligned_quantity=st.floats(
            min_value=1.0001, max_value=2.0, allow_nan=False, allow_infinity=False
        ),
        misaligned_price=st.floats(
            min_value=10000.005, max_value=10000.995, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_precision_violations_cause_early_termination(
        self, misaligned_quantity: float, misaligned_price: float
    ) -> None:
        """Property: Precision violations should cause early termination (fail-fast)."""
        config = create_mock_config()
        validation_service = ValidationService(config)
        portfolio_state = create_mock_portfolio_state()

        # Create order with precision violations
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal(str(misaligned_quantity)),  # Not aligned to 0.001 lot size
            price=Decimal(str(misaligned_price)),  # Not aligned to 0.01 tick size
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_bad_precision",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Property: Should fail with precision violations
        assert not result.is_valid
        assert len(result.violations) >= 1

        # Should have precision violations
        precision_violations = [
            v for v in result.violations if "precision" in v.lower() or "aligned" in v.lower()
        ]
        assert len(precision_violations) >= 1

    @pytest.mark.asyncio
    async def test_market_orders_skip_price_validation(self) -> None:
        """Property: Market orders should skip price-related validations."""
        config = create_mock_config()
        validation_service = ValidationService(config)
        portfolio_state = create_mock_portfolio_state()

        market_order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.100"),  # Valid quantity
            price=None,  # Market order
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_market_order",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await validation_service.validate_order(
            order=market_order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Market orders should skip price-related validations
        # but still validate quantity precision and other rules
        if not result.is_valid:
            violations_text = " ".join(result.violations).lower()
            assert "price" not in violations_text or "must have a price" not in violations_text

    @pytest.mark.asyncio
    async def test_no_portfolio_state_always_fails(self) -> None:
        """Property: Orders should always fail when portfolio state is unavailable."""
        config = create_mock_config()
        validation_service = ValidationService(config)

        order = create_valid_order()

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=None,  # No portfolio state
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Property: Should always fail without portfolio state
        assert not result.is_valid
        assert len(result.violations) >= 1

    @pytest.mark.asyncio
    async def test_validation_result_structure_consistency(self) -> None:
        """Property: ValidationResult should always have consistent structure."""
        config = create_mock_config()
        validation_service = ValidationService(config)
        portfolio_state = create_mock_portfolio_state()

        order = create_valid_order()

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Property: Result should always have consistent structure
        assert isinstance(result, ValidationResult)
        assert isinstance(result.is_valid, bool)
        assert isinstance(result.violations, list)
        assert result.validation_type == "PRE_TRADE_RISK_CHECK"

        # If invalid, should have violations
        if not result.is_valid:
            assert len(result.violations) > 0
            assert all(isinstance(v, str) for v in result.violations)

    @given(
        exchange_limits=exchange_limits_strategy(),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_exchange_specific_validation_respected(
        self,
        exchange_limits: tuple[Decimal, Decimal, float, float],
        order_price: Decimal,
        order_quantity: Decimal,
    ) -> None:
        """Property: Exchange-specific limits should be properly enforced."""
        min_order, max_order, tick_size, lot_size = exchange_limits

        # Create config with exchange limits
        config = create_mock_config()
        backpack_config = Mock()
        backpack_config.tick_size = tick_size
        backpack_config.lot_size = lot_size
        backpack_config.min_order_size = min_order
        backpack_config.max_order_size = max_order
        config.exchanges = {"backpack": backpack_config}

        validation_service = ValidationService(config)
        portfolio_state = create_mock_portfolio_state(
            usdc_balance=Decimal("1000000.00")  # Large balance to avoid balance issues
        )

        # Align order to exchange precision
        aligned_quantity = (order_quantity // Decimal(str(lot_size))) * Decimal(str(lot_size))
        aligned_price = (order_price // Decimal(str(tick_size))) * Decimal(str(tick_size))

        # Ensure we have valid aligned values
        if aligned_quantity <= 0:
            aligned_quantity = Decimal(str(lot_size))
        if aligned_price <= 0:
            aligned_price = Decimal(str(tick_size))

        order_value = aligned_price * aligned_quantity

        order = create_valid_order(quantity=aligned_quantity, price=aligned_price)

        result = await validation_service.validate_order(
            order=order,
            portfolio_state=portfolio_state,
            market_snapshot=None,
            trading_state=TradingState.ACTIVE,
            is_reconciling=False,
            is_reduce_only=False,
        )

        # Property: Orders should respect exchange-specific limits
        if order_value < min_order:
            assert not result.is_valid
            assert any("below" in v.lower() and "minimum" in v.lower() for v in result.violations)
        elif order_value > max_order:
            assert not result.is_valid
            assert any("exceeds" in v.lower() and "maximum" in v.lower() for v in result.violations)
        # If within exchange limits, other validations may still apply
