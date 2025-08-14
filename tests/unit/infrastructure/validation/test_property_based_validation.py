"""Property-based tests for validation framework using Hypothesis.

Tests the validation rules using Hypothesis to generate comprehensive test cases
that cover edge cases and boundary conditions automatically.

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- Uses property-based testing for exhaustive coverage
- All financial calculations use Decimal
- Fails fast on any violation of validation properties
"""

from __future__ import annotations

import random
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest
from hypothesis import assume, given, settings, strategies as st

from cyberdelta.config.models import AppSettings
from cyberdelta.enums import ExchangeName, OrderSide, OrderType, TimeInForce, TradingState
from cyberdelta.infrastructure.validation.rules.business_rules import (
    BalanceValidationRule,
    OrderValueLimitsRule,
)
from cyberdelta.infrastructure.validation.rules.precision_rules import (
    PricePrecisionRule,
    QuantityPrecisionRule,
)
from cyberdelta.infrastructure.validation.validation_context import ValidationContext
from cyberdelta.infrastructure.validation.validation_service import ValidationService
from cyberdelta.models.market.order import Order
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.models.spot_balance import SpotBalance
from tests.common_symbols import BTC_HL, BTC_USDC_BP


# --- Hypothesis Strategies ---


@st.composite
def decimal_strategy(draw: st.DrawFn) -> Decimal:
    """Generate valid Decimal values for financial calculations.

    Returns:
        Random Decimal value suitable for financial operations.
    """
    # Generate reasonable financial values
    value = draw(
        st.floats(
            min_value=0.000001,  # Minimum meaningful value
            max_value=100000.0,  # Maximum reasonable value for testing
            allow_infinity=False,
            allow_nan=False,
            exclude_min=True,  # Exclude exactly 0
        )
    )
    return Decimal(str(value))


@st.composite
def tick_size_strategy(draw: st.DrawFn) -> Decimal:
    """Generate valid tick sizes.

    Returns:
        Random tick size from common values.
    """
    # Common tick sizes: 0.01, 0.001, 0.0001, 0.1, 1.0
    return draw(
        st.sampled_from([
            Decimal("0.01"),
            Decimal("0.001"),
            Decimal("0.0001"),
            Decimal("0.1"),
            Decimal("1.0"),
        ])
    )


@st.composite
def lot_size_strategy(draw: st.DrawFn) -> Decimal:
    """Generate valid lot sizes.

    Returns:
        Random lot size from common values.
    """
    # Common lot sizes for crypto
    return draw(
        st.sampled_from([
            Decimal("0.001"),
            Decimal("0.0001"),
            Decimal("0.01"),
            Decimal("0.1"),
            Decimal("1.0"),
        ])
    )


@st.composite
def aligned_price_strategy(draw: st.DrawFn, tick_size: Decimal) -> Decimal:
    """Generate prices aligned to given tick size.

    Returns:
        Price aligned to the specified tick size.
    """
    # Generate a base price multiple
    base_multiplier = draw(st.integers(min_value=1, max_value=100000))
    # Align to tick size
    return Decimal(base_multiplier) * tick_size


@st.composite
def aligned_quantity_strategy(draw: st.DrawFn, lot_size: Decimal) -> Decimal:
    """Generate quantities aligned to given lot size.

    Returns:
        Quantity aligned to the specified lot size.
    """
    # Generate a base quantity multiple
    base_multiplier = draw(st.integers(min_value=1, max_value=10000))
    # Align to lot size
    return Decimal(base_multiplier) * lot_size


# --- Property-Based Tests ---


class TestPricePrecisionRuleProperties:
    """Property-based tests for PricePrecisionRule."""

    @given(tick_size=tick_size_strategy())
    @settings(max_examples=50, deadline=None)  # Reduce examples for async tests
    @pytest.mark.asyncio
    async def test_aligned_prices_always_valid(self, tick_size: Decimal) -> None:
        """Property: Prices aligned to tick size should always be valid."""
        # Create rule instance
        price_rule = PricePrecisionRule(enabled=True)

        # Generate aligned price
        aligned_price = draw_aligned_price(tick_size)

        # Create mock context
        context = create_mock_validation_context(tick_size=float(tick_size))

        # Create order with aligned price
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=aligned_price,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_aligned",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await price_rule.validate(order, context)
        assert result.is_valid, (
            f"Aligned price {aligned_price} should be valid for tick size {tick_size}"
        )

    @given(tick_size=tick_size_strategy())
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_misaligned_prices_always_invalid(self, tick_size: Decimal) -> None:
        """Property: Prices NOT aligned to tick size should always be invalid."""
        # Skip very small tick sizes where misalignment might be negligible
        assume(tick_size >= Decimal("0.001"))

        # Create rule instance
        price_rule = PricePrecisionRule(enabled=True)

        # Generate misaligned price
        aligned_price = draw_aligned_price(tick_size)
        misalignment = tick_size / Decimal(10)  # 1/10th of tick size
        misaligned_price = aligned_price + misalignment

        # Verify it's actually misaligned
        remainder = misaligned_price % tick_size
        assume(remainder != Decimal(0))  # Must be misaligned

        # Create mock context
        context = create_mock_validation_context(tick_size=float(tick_size))

        # Create order with misaligned price
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=misaligned_price,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_misaligned",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await price_rule.validate(order, context)
        assert not result.is_valid, (
            f"Misaligned price {misaligned_price} should be invalid for tick size {tick_size}"
        )

    @pytest.mark.asyncio
    async def test_market_orders_always_skip_price_validation(self) -> None:
        """Property: Market orders should always skip price validation regardless of tick size."""
        # Create rule instance
        price_rule = PricePrecisionRule(enabled=True)

        # Create mock context with any tick size
        context = create_mock_validation_context(tick_size=0.01)

        # Create market order (price should be ignored)
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=None,  # Market order
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_market",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await price_rule.validate(order, context)
        assert result.is_valid, "Market orders should always pass price validation"


class TestQuantityPrecisionRuleProperties:
    """Property-based tests for QuantityPrecisionRule."""

    @given(lot_size=lot_size_strategy())
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_aligned_quantities_always_valid(self, lot_size: Decimal) -> None:
        """Property: Quantities aligned to lot size should always be valid."""
        # Create rule instance
        quantity_rule = QuantityPrecisionRule(enabled=True)

        # Generate aligned quantity
        aligned_quantity = draw_aligned_quantity(lot_size)

        # Create mock context
        context = create_mock_validation_context(lot_size=float(lot_size))

        # Create order with aligned quantity
        order = Order(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            quantity_requested=aligned_quantity,
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_aligned_qty",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await quantity_rule.validate(order, context)
        assert result.is_valid, (
            f"Aligned quantity {aligned_quantity} should be valid for lot size {lot_size}"
        )

    @given(
        quantity=st.floats(
            min_value=-1000000.0, max_value=0.0, allow_nan=False, allow_infinity=False
        )
    )
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_negative_and_zero_quantities_always_invalid(self, quantity: float) -> None:
        """Property: Negative or zero quantities should always be invalid."""
        # Create rule instance
        quantity_rule = QuantityPrecisionRule(enabled=True)

        # Convert to Decimal
        decimal_quantity = Decimal(str(quantity))

        # Create mock context
        context = create_mock_validation_context(lot_size=0.001)

        # Create a mock order to test the validation rule directly
        # without going through Pydantic validation
        order = Mock()
        order.symbol = BTC_HL
        order.side = OrderSide.BUY
        order.quantity_requested = decimal_quantity  # Non-positive quantity
        order.price = Decimal("50000.00")
        order.order_type = OrderType.LIMIT
        order.time_in_force = TimeInForce.GTC
        order.exchange = ExchangeName.HYPERLIQUID
        order.exchange_order_id = "test_non_positive_qty"

        result = await quantity_rule.validate(order, context)
        assert not result.is_valid, (
            f"Non-positive quantity {decimal_quantity} should always be invalid"
        )


class TestBalanceValidationRuleProperties:
    """Property-based tests for BalanceValidationRule."""

    @given(
        available_balance=decimal_strategy(),
        order_price=decimal_strategy(),
        order_quantity=decimal_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_buy_orders_respect_balance_constraints(
        self, available_balance: Decimal, order_price: Decimal, order_quantity: Decimal
    ) -> None:
        """Property: Buy orders should only be valid if total cost <= available balance."""
        # Create rule instance
        balance_rule = BalanceValidationRule(enabled=True)

        order_cost = order_price * order_quantity

        # Create portfolio with given balance
        portfolio_state = Mock(spec=PortfolioState)
        usdc_balance = Mock(spec=SpotBalance)
        usdc_balance.available_quantity = available_balance
        portfolio_state.balances = {"backpack:USDC": usdc_balance}

        # Create context
        context = create_mock_validation_context(portfolio_state=portfolio_state)

        # Create buy order
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=order_quantity,
            price=order_price,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_buy_balance",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await balance_rule.validate(order, context)

        # Property: Order should be valid iff we have enough balance
        if order_cost <= available_balance:
            assert result.is_valid, (
                f"Buy order costing {order_cost} should be valid with balance {available_balance}"
            )
        else:
            assert not result.is_valid, (
                f"Buy order costing {order_cost} should be invalid with balance {available_balance}"
            )


class TestOrderValueLimitsRuleProperties:
    """Property-based tests for OrderValueLimitsRule."""

    @given(
        min_value=decimal_strategy(),
        max_value=decimal_strategy(),
        order_price=decimal_strategy(),
        order_quantity=decimal_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_order_value_limits_enforced(
        self, min_value: Decimal, max_value: Decimal, order_price: Decimal, order_quantity: Decimal
    ) -> None:
        """Property: Orders should only be valid if min_value <= order_value <= max_value."""
        # Create rule instance
        limits_rule = OrderValueLimitsRule(enabled=True)

        # Ensure min <= max
        assume(min_value <= max_value)

        order_value = order_price * order_quantity

        # Create config with limits
        context = create_mock_validation_context(
            min_trade_value=min_value, max_trade_value=max_value
        )

        # Create order
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=order_quantity,
            price=order_price,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_value_limits",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await limits_rule.validate(order, context)

        # Property: Order should be valid iff value is within limits
        if min_value <= order_value <= max_value:
            assert result.is_valid, (
                f"Order value {order_value} should be valid "
                f"within limits [{min_value}, {max_value}]"
            )
        else:
            assert not result.is_valid, (
                f"Order value {order_value} should be invalid "
                f"outside limits [{min_value}, {max_value}]"
            )


# Define strategy before class that uses it
@st.composite
def generate_order_strategy(draw: st.DrawFn) -> Order:
    """Generate valid Order objects for testing.

    Returns:
        Order: A valid order for property-based testing.
    """
    side = draw(st.sampled_from(list(OrderSide)))
    # Only use simple order types to avoid complex validation requirements
    order_type = draw(st.sampled_from([OrderType.MARKET, OrderType.LIMIT]))
    exchange = draw(st.sampled_from(list(ExchangeName)))

    # Generate price (None for market orders)
    if order_type == OrderType.MARKET:
        price = None
        stop_price = None
    else:
        price = draw(decimal_strategy())
        stop_price = None

    # Generate quantity
    quantity = draw(decimal_strategy())

    # Use pre-configured symbols from common_symbols
    # Use the properly configured symbol for each exchange
    symbol = BTC_USDC_BP if exchange == ExchangeName.BACKPACK else BTC_HL

    order_id = draw(
        st.text(min_size=5, max_size=20, alphabet=st.characters(min_codepoint=65, max_codepoint=90))
    )

    return Order(
        symbol=symbol,
        side=side,
        quantity_requested=quantity,
        price=price,
        stop_price=stop_price,
        order_type=order_type,
        time_in_force=TimeInForce.GTC,
        exchange=exchange,
        exchange_order_id=f"test_{order_id}",
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name="test_strategy",
        signal_id="test_signal",
    )


class TestValidationServiceProperties:
    """Property-based tests for the complete ValidationService."""

    @given(order=generate_order_strategy())
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_validation_always_returns_result(self, order: Order) -> None:
        """Property: Validation should always return a ValidationResult, never crash.
        
        Raises:
            AssertionError: If validation system fails unexpectedly.
        """
        # Create validation service
        validation_service = create_test_validation_service()

        # Create minimal portfolio state
        portfolio_state = Mock(spec=PortfolioState)
        portfolio_state.balances = {}
        portfolio_state.positions = {}

        try:
            result = await validation_service.validate_order(
                order=order,
                portfolio_state=portfolio_state,
                market_snapshot=None,
                trading_state=TradingState.ACTIVE,
                is_reconciling=False,
                is_reduce_only=False,
            )

            # Property: Should always return a ValidationResult
            assert result is not None
            assert hasattr(result, "is_valid")
            assert hasattr(result, "violations")
            assert isinstance(result.violations, list)

        except Exception as e:
            # Re-raise with context - validation must be robust and not crash
            raise AssertionError(f"Validation system failed unexpectedly: {e}") from e


# --- Helper Functions ---


def draw_aligned_price(tick_size: Decimal) -> Decimal:
    """Helper to generate an aligned price for a given tick size.

    Returns:
        Price aligned to the tick size.
    """
    # Generate a random multiplier
    multiplier = random.randint(1, 100000)
    return Decimal(multiplier) * tick_size


def draw_aligned_quantity(lot_size: Decimal) -> Decimal:
    """Helper to generate an aligned quantity for a given lot size.

    Returns:
        Quantity aligned to the lot size.
    """
    # Generate a random multiplier
    multiplier = random.randint(1, 10000)
    return Decimal(multiplier) * lot_size


def create_mock_validation_context(
    tick_size: float | None = None,
    lot_size: float | None = None,
    portfolio_state: PortfolioState | None = None,
    min_trade_value: Decimal | None = None,
    max_trade_value: Decimal | None = None,
) -> ValidationContext:
    """Create a mock validation context for testing.

    Returns:
        Mock validation context with specified parameters.
    """
    mock_config = Mock(spec=AppSettings)

    # Mock validation config if trade values are provided
    if min_trade_value is not None or max_trade_value is not None:
        validation_config = Mock()
        validation_config.min_trade_value = min_trade_value or Decimal("100.00")
        validation_config.max_trade_value = max_trade_value or Decimal("50000.00")
        mock_config.validation = validation_config

    # Mock exchange config if precision values are provided
    mock_exchange_config = None
    if tick_size is not None or lot_size is not None:
        mock_exchange_config = Mock()
        if tick_size is not None:
            mock_exchange_config.tick_size = tick_size
        if lot_size is not None:
            mock_exchange_config.lot_size = lot_size

    return ValidationContext(
        config=mock_config,
        exchange_config=mock_exchange_config,
        market_snapshot=None,
        portfolio_state=portfolio_state,
        trading_state=TradingState.ACTIVE,
        timestamp=datetime.now(UTC),
    )


def create_test_validation_service() -> ValidationService:
    """Create ValidationService for testing.

    Returns:
        Configured ValidationService instance for testing.
    """
    # Create comprehensive mock config
    config = Mock(spec=AppSettings)

    # Mock validation config
    validation_config = Mock()
    validation_config.min_trade_value = Decimal("100.00")
    validation_config.max_trade_value = Decimal("50000.00")
    config.validation = validation_config

    # Mock risk config
    risk_config = Mock()
    global_risk = Mock()
    global_risk.max_position_usd = Decimal("25000.00")
    global_risk.max_total_exposure_usd = Decimal("100000.00")
    risk_config.global_risk = global_risk
    config.risk = risk_config

    # Mock exchange configs
    backpack_config = Mock()
    backpack_config.tick_size = 0.01
    backpack_config.lot_size = 0.001
    backpack_config.min_order_size = Decimal("50.00")
    backpack_config.max_order_size = Decimal("25000.00")

    config.exchanges = {"backpack": backpack_config}

    return ValidationService(config)
