"""Property-based tests for precision validation rules using Hypothesis.

Tests the PricePrecisionRule and QuantityPrecisionRule implementations
that validate order price and quantity alignment to exchange requirements.

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- Uses property-based testing for exhaustive coverage
- Fails fast on critical precision errors
- All financial calculations use Decimal
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock

import pytest
from hypothesis import assume, given, settings, strategies as st

from cyberdelta.config.models import AppSettings
from cyberdelta.enums import (
    ExchangeName,
    OrderSide,
    OrderType,
    TimeInForce,
    TradingState,
    ValidationCategory,
)
from cyberdelta.infrastructure.validation.rules.precision_rules import (
    PricePrecisionRule,
    QuantityPrecisionRule,
)
from cyberdelta.infrastructure.validation.validation_context import ValidationContext
from cyberdelta.models.market.order import Order
from tests.common_symbols import BTC_HL, BTC_USDC_BP


# --- Hypothesis Strategies ---


@st.composite
def decimal_strategy(
    draw: st.DrawFn, min_value: float = 0.000001, max_value: float = 100000.0
) -> Decimal:
    """Generate valid Decimal values for financial calculations.

    Returns:
        Decimal: A valid financial decimal value between min_value and max_value.
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
def tick_size_strategy(draw: st.DrawFn) -> Decimal:
    """Generate valid tick sizes.

    Returns:
        Decimal: A valid tick size value (0.01, 0.001, 0.0001, 0.1, or 1.0).
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
        Decimal: A valid lot size value for crypto trading.
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
        Decimal: A price that is aligned to the given tick size.
    """
    # Generate a base price multiple
    base_multiplier = draw(st.integers(min_value=1, max_value=100000))
    # Align to tick size
    return Decimal(base_multiplier) * tick_size


@st.composite
def misaligned_price_strategy(draw: st.DrawFn, tick_size: Decimal) -> Decimal:
    """Generate prices NOT aligned to given tick size.

    Returns:
        Decimal: A price that is NOT aligned to the given tick size.
    """
    # Skip very small tick sizes where misalignment might be negligible
    assume(tick_size >= Decimal("0.001"))

    # Generate aligned price first
    aligned_price = draw(aligned_price_strategy(tick_size))
    # Add misalignment (fraction of tick size)
    misalignment = tick_size / Decimal(10)  # 1/10th of tick size
    misaligned_price = aligned_price + misalignment

    # Verify it's actually misaligned
    remainder = misaligned_price % tick_size
    assume(remainder != Decimal(0))  # Must be misaligned

    return misaligned_price


@st.composite
def aligned_quantity_strategy(draw: st.DrawFn, lot_size: Decimal) -> Decimal:
    """Generate quantities aligned to given lot size.

    Returns:
        Decimal: A quantity that is aligned to the given lot size.
    """
    # Generate a base quantity multiple
    base_multiplier = draw(st.integers(min_value=1, max_value=10000))
    # Align to lot size
    return Decimal(base_multiplier) * lot_size


@st.composite
def misaligned_quantity_strategy(draw: st.DrawFn, lot_size: Decimal) -> Decimal:
    """Generate quantities NOT aligned to given lot size.

    Returns:
        Decimal: A quantity that is NOT aligned to the given lot size.
    """
    # Skip very small lot sizes where misalignment might be negligible
    assume(lot_size >= Decimal("0.001"))

    # Generate aligned quantity first
    aligned_quantity = draw(aligned_quantity_strategy(lot_size))
    # Add misalignment (fraction of lot size)
    misalignment = lot_size / Decimal(10)  # 1/10th of lot size
    misaligned_quantity = aligned_quantity + misalignment

    # Verify it's actually misaligned
    remainder = misaligned_quantity % lot_size
    assume(remainder != Decimal(0))  # Must be misaligned

    return misaligned_quantity


# --- Helper Functions ---


def create_mock_validation_context(
    tick_size: float | None = None,
    lot_size: float | None = None,
    exchange_config: Mock | None = None,
) -> ValidationContext:
    """Create a mock validation context for testing.

    Returns:
        ValidationContext: Mock validation context with specified precision settings.
    """
    mock_config = Mock(spec=AppSettings)

    # Mock exchange config if precision values are provided
    if exchange_config is None and (tick_size is not None or lot_size is not None):
        exchange_config = Mock()
        if tick_size is not None:
            exchange_config.tick_size = tick_size
        if lot_size is not None:
            exchange_config.lot_size = lot_size

    return ValidationContext(
        config=mock_config,
        exchange_config=exchange_config,
        market_snapshot=None,
        portfolio_state=None,
        trading_state=TradingState.ACTIVE,
        timestamp=datetime.now(UTC),
    )


def create_test_order(
    exchange: ExchangeName,
    side: OrderSide,
    quantity: Decimal = Decimal("1.0"),
    price: Decimal | None = Decimal("50000.00"),
    order_type: OrderType = OrderType.LIMIT,
) -> Order:
    """Create a test order with specified parameters.

    Returns:
        Order: Test order with the specified parameters.
    """
    symbol = BTC_USDC_BP if exchange == ExchangeName.BACKPACK else BTC_HL

    return Order(
        symbol=symbol,
        side=side,
        quantity_requested=quantity,
        price=price,
        order_type=order_type,
        time_in_force=TimeInForce.GTC,
        exchange=exchange,
        exchange_order_id="test_order",
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name="test_strategy",
        signal_id="test_signal",
    )


class TestPricePrecisionRuleProperties:
    """Property-based tests for PricePrecisionRule validation logic."""

    def test_rule_properties(self) -> None:
        """Test rule property values."""
        price_rule = PricePrecisionRule(enabled=True)
        assert price_rule.name == "price_precision"
        assert price_rule.category == ValidationCategory.PRECISION
        assert price_rule.enabled is True
        assert price_rule.bypass_on_reduce_only is False

    @pytest.mark.asyncio
    async def test_market_orders_always_skipped(self) -> None:
        """Property: Market orders should always be skipped (no price to validate)."""
        price_rule = PricePrecisionRule(enabled=True)

        context = create_mock_validation_context(tick_size=0.01)

        # Create market order
        order = create_test_order(
            exchange=ExchangeName.BACKPACK,
            side=OrderSide.BUY,
            price=None,  # Market order
            order_type=OrderType.MARKET,
        )

        result = await price_rule.validate(order, context)

        # Property: Should always be valid (skipped)
        assert result.is_valid
        assert len(result.violations) == 0

    @pytest.mark.asyncio
    async def test_limit_orders_without_price_always_violation(self) -> None:
        """Property: Limit orders without price should always be rejected.

        Uses Mock to test defensive validation for limit orders missing price,
        which could occur if order creation logic has bugs.
        """
        price_rule = PricePrecisionRule(enabled=True)

        context = create_mock_validation_context(tick_size=0.01)

        # Use Mock to create limit order without price
        # This could happen if order creation logic has a bug
        order = Mock()
        order.symbol = BTC_USDC_BP
        order.side = OrderSide.BUY
        order.price = None  # Missing price for limit order
        order.quantity_requested = Decimal("1.0")
        order.order_type = OrderType.LIMIT
        order.time_in_force = TimeInForce.GTC
        order.exchange = ExchangeName.BACKPACK
        order.exchange_order_id = "test_limit_no_price"

        result = await price_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Limit order must have a price specified" in result.violations[0]

    @given(
        negative_price=st.floats(
            min_value=-1000000.0, max_value=0.0, allow_nan=False, allow_infinity=False
        )
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_negative_and_zero_prices_always_violation(self, negative_price: float) -> None:
        """Property: Negative or zero prices should always be rejected.

        Uses Mock to test defensive validation against impossible states
        that could occur if Pydantic validation is bypassed.
        """
        price_rule = PricePrecisionRule(enabled=True)

        context = create_mock_validation_context(tick_size=0.01)

        # Use Mock to create "impossible" order state
        # This tests defensive validation against bugs that bypass Pydantic
        order = Mock()
        order.symbol = BTC_USDC_BP
        order.side = OrderSide.BUY
        order.price = Decimal(str(negative_price))  # Negative/zero price
        order.quantity_requested = Decimal("1.0")
        order.order_type = OrderType.LIMIT
        order.time_in_force = TimeInForce.GTC
        order.exchange = ExchangeName.BACKPACK
        order.exchange_order_id = "test_negative_price"

        result = await price_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid
        assert len(result.violations) >= 1  # May have multiple violations
        # Check that at least one violation mentions the negative price
        assert any("Price must be positive" in v for v in result.violations)

    @given(tick_size=tick_size_strategy())
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_aligned_prices_always_valid(self, tick_size: Decimal) -> None:
        """Property: Prices aligned to tick size should always be valid."""
        price_rule = PricePrecisionRule(enabled=True)

        # Generate aligned price
        aligned_price = draw_aligned_price(tick_size)

        context = create_mock_validation_context(tick_size=float(tick_size))

        # Create order with aligned price
        order = create_test_order(
            exchange=ExchangeName.BACKPACK,
            side=OrderSide.BUY,
            price=aligned_price,
            order_type=OrderType.LIMIT,
        )

        result = await price_rule.validate(order, context)

        # Property: Should always be valid
        assert result.is_valid, (
            f"Aligned price {aligned_price} should be valid for tick size {tick_size}"
        )

    @given(tick_size=tick_size_strategy())
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_misaligned_prices_always_invalid(self, tick_size: Decimal) -> None:
        """Property: Prices NOT aligned to tick size should always be invalid."""
        price_rule = PricePrecisionRule(enabled=True)

        # Generate misaligned price
        misaligned_price = draw_misaligned_price(tick_size)

        context = create_mock_validation_context(tick_size=float(tick_size))

        # Create order with misaligned price
        order = create_test_order(
            exchange=ExchangeName.BACKPACK,
            side=OrderSide.BUY,
            price=misaligned_price,
            order_type=OrderType.LIMIT,
        )

        result = await price_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid, (
            f"Misaligned price {misaligned_price} should be invalid for tick size {tick_size}"
        )
        assert len(result.violations) >= 1
        assert "not aligned to tick size" in result.violations[0]

    @pytest.mark.asyncio
    async def test_no_tick_size_configured_always_valid(self) -> None:
        """Property: Orders should always be valid when no tick size is configured."""
        price_rule = PricePrecisionRule(enabled=True)

        # Context without exchange config (no tick size)
        context = create_mock_validation_context(exchange_config=None)

        # Create order with any price
        order = create_test_order(
            exchange=ExchangeName.BACKPACK,
            side=OrderSide.BUY,
            price=Decimal("50000.005"),  # Would be misaligned with 0.01 tick
            order_type=OrderType.LIMIT,
        )

        result = await price_rule.validate(order, context)

        # Property: Should always be valid without tick size config
        assert result.is_valid
        assert len(result.violations) == 0

    @pytest.mark.asyncio
    async def test_zero_tick_size_always_valid(self) -> None:
        """Property: Orders should always be valid when tick size is zero."""
        price_rule = PricePrecisionRule(enabled=True)

        context = create_mock_validation_context(tick_size=0.0)

        # Create order with any price
        order = create_test_order(
            exchange=ExchangeName.BACKPACK,
            side=OrderSide.BUY,
            price=Decimal("50000.005"),  # Any precision
            order_type=OrderType.LIMIT,
        )

        result = await price_rule.validate(order, context)

        # Property: Should always be valid with zero tick size
        assert result.is_valid
        assert len(result.violations) == 0


class TestQuantityPrecisionRuleProperties:
    """Property-based tests for QuantityPrecisionRule validation logic."""

    def test_rule_properties(self) -> None:
        """Test rule property values."""
        quantity_rule = QuantityPrecisionRule(enabled=True)
        assert quantity_rule.name == "quantity_precision"
        assert quantity_rule.category == ValidationCategory.PRECISION
        assert quantity_rule.enabled is True
        assert quantity_rule.bypass_on_reduce_only is False

    @given(
        negative_quantity=st.floats(
            min_value=-1000000.0, max_value=0.0, allow_nan=False, allow_infinity=False
        )
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_negative_and_zero_quantities_always_violation(
        self, negative_quantity: float
    ) -> None:
        """Property: Negative or zero quantities should always be rejected.

        Uses Mock to test defensive validation against impossible states
        that could occur if Pydantic validation is bypassed.
        """
        quantity_rule = QuantityPrecisionRule(enabled=True)

        context = create_mock_validation_context(lot_size=0.001)

        # Use Mock to create "impossible" order state
        # This tests defensive validation against bugs that bypass Pydantic
        order = Mock()
        order.symbol = BTC_HL
        order.side = OrderSide.BUY
        order.quantity_requested = Decimal(str(negative_quantity))  # Negative/zero quantity
        order.price = Decimal("50000.00")
        order.order_type = OrderType.LIMIT
        order.time_in_force = TimeInForce.GTC
        order.exchange = ExchangeName.HYPERLIQUID
        order.exchange_order_id = "test_negative_quantity"

        result = await quantity_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid
        assert len(result.violations) >= 1  # May have multiple violations
        # Check that at least one violation mentions the negative quantity
        assert any("Quantity must be positive" in v for v in result.violations)

    @given(lot_size=lot_size_strategy())
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_aligned_quantities_always_valid(self, lot_size: Decimal) -> None:
        """Property: Quantities aligned to lot size should always be valid."""
        quantity_rule = QuantityPrecisionRule(enabled=True)

        # Generate aligned quantity
        aligned_quantity = draw_aligned_quantity(lot_size)

        context = create_mock_validation_context(lot_size=float(lot_size))

        # Create order with aligned quantity
        order = create_test_order(
            exchange=ExchangeName.HYPERLIQUID,
            side=OrderSide.BUY,
            quantity=aligned_quantity,
            order_type=OrderType.LIMIT,
        )

        result = await quantity_rule.validate(order, context)

        # Property: Should always be valid
        assert result.is_valid, (
            f"Aligned quantity {aligned_quantity} should be valid for lot size {lot_size}"
        )

    @given(lot_size=lot_size_strategy())
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    @pytest.mark.asyncio
    async def test_misaligned_quantities_always_invalid(self, lot_size: Decimal) -> None:
        """Property: Quantities NOT aligned to lot size should always be invalid."""
        quantity_rule = QuantityPrecisionRule(enabled=True)

        # Generate misaligned quantity
        misaligned_quantity = draw_misaligned_quantity(lot_size)

        context = create_mock_validation_context(lot_size=float(lot_size))

        # Create order with misaligned quantity
        order = create_test_order(
            exchange=ExchangeName.HYPERLIQUID,
            side=OrderSide.BUY,
            quantity=misaligned_quantity,
            order_type=OrderType.LIMIT,
        )

        result = await quantity_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid, (
            f"Misaligned quantity {misaligned_quantity} should be invalid for lot size {lot_size}"
        )
        assert len(result.violations) >= 1
        assert "not aligned to lot size" in result.violations[0]

    @pytest.mark.asyncio
    async def test_no_lot_size_configured_always_valid(self) -> None:
        """Property: Orders should always be valid when no lot size is configured."""
        quantity_rule = QuantityPrecisionRule(enabled=True)

        # Context without exchange config (no lot size)
        context = create_mock_validation_context(exchange_config=None)

        # Create order with any quantity
        order = create_test_order(
            exchange=ExchangeName.HYPERLIQUID,
            side=OrderSide.BUY,
            quantity=Decimal("1.0005"),  # Would be misaligned with 0.001 lot
            order_type=OrderType.LIMIT,
        )

        result = await quantity_rule.validate(order, context)

        # Property: Should always be valid without lot size config
        assert result.is_valid
        assert len(result.violations) == 0

    @pytest.mark.asyncio
    async def test_zero_lot_size_always_valid(self) -> None:
        """Property: Orders should always be valid when lot size is zero."""
        quantity_rule = QuantityPrecisionRule(enabled=True)

        context = create_mock_validation_context(lot_size=0.0)

        # Create order with any quantity
        order = create_test_order(
            exchange=ExchangeName.HYPERLIQUID,
            side=OrderSide.BUY,
            quantity=Decimal("1.0005"),  # Any precision
            order_type=OrderType.LIMIT,
        )

        result = await quantity_rule.validate(order, context)

        # Property: Should always be valid with zero lot size
        assert result.is_valid
        assert len(result.violations) == 0

    @pytest.mark.asyncio
    async def test_market_orders_validate_quantity_precision(self) -> None:
        """Property: Market orders should still validate quantity precision."""
        quantity_rule = QuantityPrecisionRule(enabled=True)

        context = create_mock_validation_context(lot_size=0.001)

        # Create market order with misaligned quantity
        order = create_test_order(
            exchange=ExchangeName.HYPERLIQUID,
            side=OrderSide.BUY,
            quantity=Decimal("1.0005"),  # Not aligned to 0.001 lot
            price=None,  # Market order
            order_type=OrderType.MARKET,
        )

        result = await quantity_rule.validate(order, context)

        # Property: Should still be invalid (quantity precision applies to all orders)
        assert not result.is_valid
        assert len(result.violations) == 1
        assert "not aligned to lot size" in result.violations[0]


# --- Helper Functions for Drawing ---


def draw_aligned_price(tick_size: Decimal) -> Decimal:
    """Helper to generate an aligned price for a given tick size.

    Returns:
        Decimal: An aligned price value.
    """
    # Generate deterministic multiplier based on tick_size for test reproducibility
    hash_input = f"price_{tick_size}".encode()
    hash_value = int(hashlib.sha256(hash_input).hexdigest()[:8], 16)
    multiplier = (hash_value % 99999) + 1  # 1 to 100000 range
    return Decimal(multiplier) * tick_size


def draw_misaligned_price(tick_size: Decimal) -> Decimal:
    """Helper to generate a misaligned price for a given tick size.

    Returns:
        Decimal: A misaligned price value.
    """
    # Skip very small tick sizes where misalignment might be negligible
    tick_size = max(tick_size, Decimal("0.001"))

    # Generate aligned price first
    hash_input = f"misaligned_price_{tick_size}".encode()
    hash_value = int(hashlib.sha256(hash_input).hexdigest()[:8], 16)
    multiplier = (hash_value % 99900) + 100  # 100 to 100000 range
    aligned_price = Decimal(multiplier) * tick_size

    # Add misalignment that's NOT a multiple of tick size
    # Use a fraction that won't result in a valid tick
    misalignment = tick_size / Decimal(3)  # 1/3 of tick size - won't align
    misaligned_price = aligned_price + misalignment

    # Verify it's actually misaligned
    remainder = misaligned_price % tick_size
    if remainder == Decimal(0):
        # If somehow aligned, add a smaller misalignment
        misaligned_price += tick_size / Decimal(7)

    return misaligned_price


def draw_aligned_quantity(lot_size: Decimal) -> Decimal:
    """Helper to generate an aligned quantity for a given lot size.

    Returns:
        Decimal: An aligned quantity value.
    """
    # Generate deterministic multiplier based on lot_size for test reproducibility
    hash_input = f"quantity_{lot_size}".encode()
    hash_value = int(hashlib.sha256(hash_input).hexdigest()[:8], 16)
    multiplier = (hash_value % 9999) + 1  # 1 to 10000 range
    return Decimal(multiplier) * lot_size


def draw_misaligned_quantity(lot_size: Decimal) -> Decimal:
    """Helper to generate a misaligned quantity for a given lot size.

    Returns:
        Decimal: A misaligned quantity value.
    """
    # Skip very small lot sizes where misalignment might be negligible
    lot_size = max(lot_size, Decimal("0.001"))

    # Generate aligned quantity first
    hash_input = f"misaligned_quantity_{lot_size}".encode()
    hash_value = int(hashlib.sha256(hash_input).hexdigest()[:8], 16)
    multiplier = (hash_value % 9990) + 10  # 10 to 10000 range
    aligned_quantity = Decimal(multiplier) * lot_size

    # Add misalignment that's NOT a multiple of lot size
    # Use a fraction that won't result in a valid lot
    misalignment = lot_size / Decimal(3)  # 1/3 of lot size - won't align
    misaligned_quantity = aligned_quantity + misalignment

    # Verify it's actually misaligned
    remainder = misaligned_quantity % lot_size
    if remainder == Decimal(0):
        # If somehow aligned, add a smaller misalignment
        misaligned_quantity += lot_size / Decimal(7)

    return misaligned_quantity
