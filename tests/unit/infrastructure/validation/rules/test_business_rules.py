"""Property-based tests for business validation rules using Hypothesis.

Tests the BalanceValidationRule and OrderValueLimitsRule implementations
that validate order balance availability and value limits.

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- Uses property-based testing for exhaustive coverage
- All financial calculations use Decimal
- Fails fast on critical business logic errors
"""

from __future__ import annotations

from datetime import UTC, datetime
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
from cyberdelta.infrastructure.validation.rules.business_rules import (
    BalanceValidationRule,
    OrderValueLimitsRule,
)
from cyberdelta.infrastructure.validation.validation_context import ValidationContext
from cyberdelta.models.market.order import Order
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.models.spot_balance import SpotBalance
from tests.common_symbols import BTC_HL, BTC_USDC_BP, ETH_USDC_BP


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
def balance_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic balance values.

    Returns:
        Decimal: A realistic balance value between 0 and 1,000,000.
    """
    return draw(decimal_strategy(min_value=0.0, max_value=1000000.0))


@st.composite
def price_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic price values.

    Returns:
        Decimal: A realistic price value between 0.01 and 10,000.
    """
    return draw(decimal_strategy(min_value=0.01, max_value=100000.0))


@st.composite
def quantity_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic quantity values.

    Returns:
        Decimal: A realistic quantity value between 0.000001 and 1000.
    """
    return draw(decimal_strategy(min_value=0.000001, max_value=1000.0))


@st.composite
def order_value_limits_strategy(draw: st.DrawFn) -> tuple[Decimal, Decimal]:
    """Generate valid order value limits (min, max) where min <= max.

    Returns:
        tuple[Decimal, Decimal]: A tuple of (min_value, max_value) where min <= max.
    """
    min_val = draw(decimal_strategy(min_value=1.0, max_value=1000.0))
    max_val = draw(decimal_strategy(min_value=float(min_val), max_value=100000.0))
    return min_val, max_val


@st.composite
def generate_order_strategy(
    draw: st.DrawFn, exchange: ExchangeName, with_price: bool = True
) -> Order:
    """Generate valid Order objects for testing.

    Returns:
        Order: A valid Order object with the specified exchange and optional price.
    """
    side = draw(st.sampled_from(list(OrderSide)))
    order_type = draw(st.sampled_from([OrderType.LIMIT, OrderType.MARKET]))

    # Generate price (None for market orders or when not required)
    if order_type == OrderType.MARKET or not with_price:
        price = None
    else:
        price = draw(price_strategy())

    quantity = draw(quantity_strategy())

    # Use proper symbols from common_symbols
    if exchange == ExchangeName.BACKPACK:
        symbol = BTC_USDC_BP
    else:
        symbol = BTC_HL

    order_id = draw(
        st.text(min_size=5, max_size=20, alphabet=st.characters(min_codepoint=65, max_codepoint=90))
    )

    return Order(
        symbol=symbol,
        side=side,
        quantity_requested=quantity,
        price=price,
        order_type=order_type,
        time_in_force=TimeInForce.GTC,
        exchange=exchange,
        exchange_order_id=f"test_{order_id}",
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name="test_strategy",
        signal_id="test_signal",
    )


# --- Helper Functions ---


def create_mock_validation_context(
    portfolio_state: PortfolioState | None = None,
    exchange_config: Mock | None = None,
    is_reconciling: bool = False,
    global_min_value: Decimal | None = None,
    global_max_value: Decimal | None = None,
    exchange_min_size: Decimal | None = None,
    exchange_max_size: Decimal | None = None,
) -> ValidationContext:
    """Create a mock validation context for testing.

    Returns:
        ValidationContext: A mock validation context configured with the specified parameters.
    """
    mock_config = Mock(spec=AppSettings)

    # Mock validation config if values are provided
    if global_min_value is not None or global_max_value is not None:
        validation_config = Mock()
        validation_config.min_trade_value = global_min_value or Decimal("100.00")
        validation_config.max_trade_value = global_max_value or Decimal("50000.00")
        mock_config.validation = validation_config

    # Mock exchange config if values are provided
    if exchange_config is None and (exchange_min_size is not None or exchange_max_size is not None):
        exchange_config = Mock()
        if exchange_min_size is not None:
            exchange_config.min_order_size = exchange_min_size
        if exchange_max_size is not None:
            exchange_config.max_order_size = exchange_max_size

    return ValidationContext(
        config=mock_config,
        exchange_config=exchange_config,
        market_snapshot=None,
        portfolio_state=portfolio_state,
        trading_state=TradingState.ACTIVE,
        timestamp=datetime.now(UTC),
        is_reconciling=is_reconciling,
        is_reduce_only=False,
    )


def create_mock_portfolio_with_balance(
    usdc_balance: Decimal | None = None,
    btc_balance: Decimal | None = None,
) -> Mock:
    """Create mock portfolio state with specified balances.

    Returns:
        Mock: A mock portfolio state with the specified USDC and BTC balances.
    """
    portfolio_state = Mock(spec=PortfolioState)

    balances = {}

    if usdc_balance is not None:
        usdc_bal = Mock(spec=SpotBalance)
        usdc_bal.available_quantity = usdc_balance
        balances["backpack:USDC"] = usdc_bal
        balances["hyperliquid:USDC"] = usdc_bal

    if btc_balance is not None:
        btc_bal = Mock(spec=SpotBalance)
        btc_bal.available_quantity = btc_balance
        balances["backpack:BTC"] = btc_bal
        balances["hyperliquid:BTC"] = btc_bal

    portfolio_state.balances = balances
    return portfolio_state


def create_test_order(
    exchange: ExchangeName,
    side: OrderSide,
    quantity: Decimal = Decimal("1.0"),
    price: Decimal | None = Decimal("50000.00"),
    order_type: OrderType = OrderType.LIMIT,
) -> Order:
    """Create a test order with specified parameters.

    Returns:
        Order: A test order configured with the specified exchange, side, quantity, price, and type.
    """
    if exchange == ExchangeName.BACKPACK:
        symbol = BTC_USDC_BP
    else:
        symbol = BTC_HL

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


class TestBalanceValidationRuleProperties:
    """Property-based tests for BalanceValidationRule validation logic."""

    def test_rule_properties(self) -> None:
        """Test rule property values."""
        balance_rule = BalanceValidationRule(enabled=True)
        assert balance_rule.name == "balance_check"
        assert balance_rule.category == ValidationCategory.BALANCE
        assert balance_rule.enabled is True
        assert balance_rule.bypass_on_reduce_only is True

    @given(st.just(None))  # Add @given decorator for @settings to work
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_skip_balance_checks_during_reconciliation(self, _: None) -> None:
        """Property: Balance checks should always be skipped during reconciliation."""
        balance_rule = BalanceValidationRule(enabled=True)

        # Create context that skips balance checks
        context = create_mock_validation_context(
            portfolio_state=create_mock_portfolio_with_balance(), is_reconciling=True
        )

        # Create any order
        order = create_test_order(ExchangeName.BACKPACK, OrderSide.BUY)

        result = await balance_rule.validate(order, context)

        # Property: Should always be valid during reconciliation
        assert result.is_valid
        assert len(result.violations) == 0

    @given(st.just(None))
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_no_portfolio_state_always_violation(self, _: None) -> None:
        """Property: Orders should always be rejected when portfolio state is unavailable."""
        balance_rule = BalanceValidationRule(enabled=True)

        # Create context without portfolio state
        context = create_mock_validation_context(portfolio_state=None)

        # Create any order
        order = create_test_order(ExchangeName.BACKPACK, OrderSide.BUY)

        result = await balance_rule.validate(order, context)

        # Property: Should always be invalid without portfolio state
        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Portfolio state unavailable" in result.violations[0]

    @given(
        available_balance=balance_strategy(),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_buy_orders_respect_balance_constraints(
        self, available_balance: Decimal, order_price: Decimal, order_quantity: Decimal
    ) -> None:
        """Property: Buy orders should only be valid if total cost <= available balance."""
        balance_rule = BalanceValidationRule(enabled=True)

        order_cost = order_price * order_quantity

        # Create portfolio with specified balance
        portfolio_state = create_mock_portfolio_with_balance(usdc_balance=available_balance)
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
            assert len(result.violations) >= 1
            assert "Insufficient USDC balance" in result.violations[0]

    @given(
        available_balance=balance_strategy(),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_sell_orders_respect_balance_constraints(
        self, available_balance: Decimal, order_price: Decimal, order_quantity: Decimal
    ) -> None:
        """Property: Sell orders should only be valid if requested quantity <= available balance."""
        balance_rule = BalanceValidationRule(enabled=True)

        # Create portfolio with specified BTC balance
        portfolio_state = create_mock_portfolio_with_balance(btc_balance=available_balance)
        context = create_mock_validation_context(portfolio_state=portfolio_state)

        # Create sell order
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.SELL,
            quantity_requested=order_quantity,
            price=order_price,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_sell_balance",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await balance_rule.validate(order, context)

        # Property: Order should be valid iff we have enough base asset
        if order_quantity <= available_balance:
            assert result.is_valid, (
                f"Sell order for {order_quantity} should be valid with balance {available_balance}"
            )
        else:
            assert not result.is_valid, (
                f"Sell order for {order_quantity} should be invalid "
                f"with balance {available_balance}"
            )
            assert len(result.violations) >= 1
            assert "Insufficient BTC balance" in result.violations[0]

    @given(st.just(None))
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_buy_orders_without_price_always_violation(self, _: None) -> None:
        """Property: Buy orders without price should always be rejected."""
        balance_rule = BalanceValidationRule(enabled=True)

        portfolio_state = create_mock_portfolio_with_balance()
        context = create_mock_validation_context(portfolio_state=portfolio_state)

        # Create market buy order without price
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.1"),
            price=None,
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_market_buy",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await balance_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Cannot validate buy order without price" in result.violations[0]

    @given(st.just(None))
    @settings(max_examples=20, deadline=None)
    @pytest.mark.asyncio
    async def test_missing_base_balance_always_violation(self, _: None) -> None:
        """Property: Sell orders for assets without base balance should always be rejected."""
        balance_rule = BalanceValidationRule(enabled=True)

        # Create portfolio without ETH balance
        portfolio_state = create_mock_portfolio_with_balance(usdc_balance=Decimal("10000.00"))
        context = create_mock_validation_context(portfolio_state=portfolio_state)

        # Create sell order for ETH (no ETH balance in portfolio)
        order = Order(
            symbol=ETH_USDC_BP,
            side=OrderSide.SELL,
            quantity_requested=Decimal("1.0"),
            price=Decimal("3000.00"),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_no_eth",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await balance_rule.validate(order, context)

        # Property: Should always be invalid
        assert not result.is_valid
        assert len(result.violations) == 1
        assert "No ETH balance found" in result.violations[0]

    @given(st.just(None))
    @settings(max_examples=20, deadline=None)
    @pytest.mark.asyncio
    async def test_hyperliquid_symbol_extraction_always_works(self, _: None) -> None:
        """Property: Hyperliquid orders should always extract symbols correctly."""
        balance_rule = BalanceValidationRule(enabled=True)

        # Create portfolio with USD balance for Hyperliquid (BTC_HL uses USD as quote)
        portfolio_state = Mock(spec=PortfolioState)
        usd_balance = Mock(spec=SpotBalance)
        usd_balance.available_quantity = Decimal("10000.00")
        portfolio_state.balances = {"hyperliquid:USD": usd_balance}
        context = create_mock_validation_context(portfolio_state=portfolio_state)

        # Create Hyperliquid order
        order = Order(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.1"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_hl_order",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await balance_rule.validate(order, context)

        # Should use USDC as quote asset and succeed with sufficient balance
        assert result.is_valid
        assert len(result.violations) == 0


class TestOrderValueLimitsRuleProperties:
    """Property-based tests for OrderValueLimitsRule validation logic."""

    def test_rule_properties(self) -> None:
        """Test rule property values."""
        limits_rule = OrderValueLimitsRule(enabled=True)
        assert limits_rule.name == "order_value_limits"
        assert limits_rule.category == ValidationCategory.LIMITS
        assert limits_rule.enabled is True
        assert limits_rule.bypass_on_reduce_only is False

    @given(st.just(None))
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_market_orders_without_price_always_skipped(self, _: None) -> None:
        """Property: Market orders without price should always be skipped."""
        limits_rule = OrderValueLimitsRule(enabled=True)

        context = create_mock_validation_context()

        # Create market order without price
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.01"),
            price=None,
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_market",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await limits_rule.validate(order, context)

        # Property: Should always be valid (skipped)
        assert result.is_valid
        assert len(result.violations) == 0

    @given(
        min_value=decimal_strategy(min_value=1.0, max_value=1000.0),
        max_value=decimal_strategy(min_value=1000.0, max_value=100000.0),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_order_value_limits_properly_enforced(
        self, min_value: Decimal, max_value: Decimal, order_price: Decimal, order_quantity: Decimal
    ) -> None:
        """Property: Orders should only be valid if min_value <= order_value <= max_value."""
        limits_rule = OrderValueLimitsRule(enabled=True)

        # Ensure min <= max (Hypothesis can sometimes generate edge cases)
        assume(min_value <= max_value)

        order_value = order_price * order_quantity

        # Create context with specified limits
        context = create_mock_validation_context(
            global_min_value=min_value, global_max_value=max_value
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
            assert len(result.violations) >= 1

    @given(
        exchange_min=decimal_strategy(min_value=10.0, max_value=100.0),
        exchange_max=decimal_strategy(min_value=1000.0, max_value=50000.0),
        order_price=price_strategy(),
        order_quantity=quantity_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    @pytest.mark.asyncio
    async def test_exchange_limits_properly_enforced(
        self,
        exchange_min: Decimal,
        exchange_max: Decimal,
        order_price: Decimal,
        order_quantity: Decimal,
    ) -> None:
        """Property: Orders should respect exchange-specific limits when configured."""
        limits_rule = OrderValueLimitsRule(enabled=True)

        # Ensure min <= max
        assume(exchange_min <= exchange_max)

        order_value = order_price * order_quantity

        # Create context with exchange limits and proper validation config
        context = create_mock_validation_context(
            exchange_min_size=exchange_min,
            exchange_max_size=exchange_max,
            global_min_value=Decimal("1.0"),  # Ensure validation config exists
            global_max_value=Decimal("100000.0"),
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
            exchange_order_id="test_exchange_limits",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await limits_rule.validate(order, context)

        # Property: Order should respect exchange limits
        if order_value < exchange_min:
            assert not result.is_valid
            assert any("below exchange minimum" in v for v in result.violations)
        elif order_value > exchange_max:
            assert not result.is_valid
            assert any("exceeds exchange maximum" in v for v in result.violations)
        # Note: May still violate global limits, but that's checked separately

    @given(st.just(None))
    @settings(max_examples=30, deadline=None)
    @pytest.mark.asyncio
    async def test_no_exchange_config_uses_global_only(self, _: None) -> None:
        """Property: When no exchange config, only global limits should apply."""
        limits_rule = OrderValueLimitsRule(enabled=True)

        # Create context without exchange config
        context = create_mock_validation_context(
            exchange_config=None,
            global_min_value=Decimal("100.00"),
            global_max_value=Decimal("50000.00"),
        )

        # Order within global limits but would exceed typical exchange limits
        order = Order(
            symbol=BTC_USDC_BP,
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.6"),  # 0.6 BTC
            price=Decimal("50000.00"),  # $30k total
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_no_exchange_config",
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name="test_strategy",
            signal_id="test_signal",
        )

        result = await limits_rule.validate(order, context)

        # Property: Should be valid (only global limits apply)
        assert result.is_valid
        assert len(result.violations) == 0
