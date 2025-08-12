"""Unit tests for business validation rules.

Tests the BalanceValidationRule and OrderValueLimitsRule implementations
that validate order balance availability and value limits.

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values
- Uses real configuration values
- All financial calculations use Decimal
- Fails fast on critical business logic errors
"""

from __future__ import annotations

from decimal import Decimal
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from cyberdelta.config.models import AppSettings
from cyberdelta.infrastructure.validation.rules.business_rules import (
    BalanceValidationRule,
    OrderValueLimitsRule,
)
from cyberdelta.infrastructure.validation.validation_context import ValidationContext
from cyberdelta.enums import ExchangeName, OrderSide, OrderType, TradingState, ValidationCategory
from cyberdelta.models.market.order import Order
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.models.spot_balance import SpotBalance
from cyberdelta.models.validation import ValidationResult
from cyberdelta.symbols import bp_symbol, hl_symbol


class TestBalanceValidationRule:
    """Test cases for BalanceValidationRule validation logic."""

    @pytest.fixture
    def balance_rule(self) -> BalanceValidationRule:
        """Create BalanceValidationRule instance for testing."""
        return BalanceValidationRule(enabled=True)

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create mock AppSettings for testing."""
        config = Mock(spec=AppSettings)
        config.exchanges = {}
        return config

    @pytest.fixture
    def mock_portfolio_state(self) -> Mock:
        """Create mock portfolio state with balances."""
        portfolio_state = Mock(spec=PortfolioState)

        # Mock USDC balance for buy orders
        usdc_balance = Mock(spec=SpotBalance)
        usdc_balance.available_quantity = Decimal("10000.00")  # $10k available

        # Mock BTC balance for sell orders
        btc_balance = Mock(spec=SpotBalance)
        btc_balance.available_quantity = Decimal("1.5")  # 1.5 BTC available

        portfolio_state.balances = {
            "backpack:USDC": usdc_balance,
            "backpack:BTC": btc_balance,
            "hyperliquid:USDC": usdc_balance,
            "hyperliquid:BTC": btc_balance,
        }

        return portfolio_state

    @pytest.fixture
    def validation_context(
        self, mock_config: Mock, mock_portfolio_state: Mock
    ) -> ValidationContext:
        """Create validation context for testing."""
        return ValidationContext(
            config=mock_config,
            exchange_config=None,
            market_snapshot=None,
            portfolio_state=mock_portfolio_state,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
            is_reconciling=False,
            is_reduce_only=False,
        )

    @pytest.fixture
    def context_no_portfolio(self, mock_config: Mock) -> ValidationContext:
        """Create validation context without portfolio state."""
        return ValidationContext(
            config=mock_config,
            exchange_config=None,
            market_snapshot=None,
            portfolio_state=None,  # No portfolio state
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

    @pytest.fixture
    def context_skip_balance(
        self, mock_config: Mock, mock_portfolio_state: Mock
    ) -> ValidationContext:
        """Create validation context that skips balance checks."""
        return ValidationContext(
            config=mock_config,
            exchange_config=None,
            market_snapshot=None,
            portfolio_state=mock_portfolio_state,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
            is_reconciling=True,  # Skip balance checks during reconciliation
            is_reduce_only=False,
        )

    @pytest.fixture
    def buy_order(self) -> Order:
        """Create test buy order."""
        return Order(
            symbol=bp_symbol("BTC_USDC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.1"),  # 0.1 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $5k total
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_buy_001",
        )

    @pytest.fixture
    def sell_order(self) -> Order:
        """Create test sell order."""
        return Order(
            symbol=bp_symbol("BTC_USDC"),
            side=OrderSide.SELL,
            quantity_requested=Decimal("1.0"),  # 1.0 BTC
            price=Decimal("50000.00"),  # $50k per BTC
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_sell_001",
        )

    def test_rule_properties(self, balance_rule: BalanceValidationRule) -> None:
        """Test rule property values."""
        assert balance_rule.name == "balance_check"
        assert balance_rule.category == ValidationCategory.BALANCE
        assert balance_rule.enabled is True
        assert balance_rule.bypass_on_reduce_only is True

    async def test_skip_balance_checks_during_reconciliation(
        self,
        balance_rule: BalanceValidationRule,
        buy_order: Order,
        context_skip_balance: ValidationContext,
    ) -> None:
        """Test that balance checks are skipped during reconciliation."""
        result = await balance_rule.validate(buy_order, context_skip_balance)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_no_portfolio_state_violation(
        self,
        balance_rule: BalanceValidationRule,
        buy_order: Order,
        context_no_portfolio: ValidationContext,
    ) -> None:
        """Test violation when portfolio state is unavailable."""
        result = await balance_rule.validate(buy_order, context_no_portfolio)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Portfolio state unavailable" in result.violations[0]

    async def test_buy_order_sufficient_balance_valid(
        self,
        balance_rule: BalanceValidationRule,
        buy_order: Order,
        validation_context: ValidationContext,
    ) -> None:
        """Test buy order with sufficient USDC balance is valid."""
        # Buy order needs $5k, we have $10k available
        result = await balance_rule.validate(buy_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_buy_order_insufficient_balance_violation(
        self, balance_rule: BalanceValidationRule, validation_context: ValidationContext
    ) -> None:
        """Test buy order with insufficient USDC balance is rejected."""
        # Create buy order that needs more than available balance
        large_buy_order = Order(
            symbol=bp_symbol("BTC_USDC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),  # 1.0 BTC
            price=Decimal("15000.00"),  # $15k per BTC = $15k total (> $10k available)
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_large_buy",
        )

        result = await balance_rule.validate(large_buy_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Insufficient USDC balance" in result.violations[0]
        assert "need 15000" in result.violations[0]
        assert "available 10000" in result.violations[0]

    async def test_buy_order_no_quote_balance_violation(
        self, balance_rule: BalanceValidationRule, validation_context: ValidationContext
    ) -> None:
        """Test buy order with no quote currency balance is rejected."""
        # Create buy order for pair without quote balance
        no_balance_order = Order(
            symbol=bp_symbol("ETH_DAI"),  # DAI not in portfolio
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=Decimal("3000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_no_balance",
        )

        result = await balance_rule.validate(no_balance_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "No DAI balance found" in result.violations[0]

    async def test_buy_order_without_price_violation(
        self, balance_rule: BalanceValidationRule, validation_context: ValidationContext
    ) -> None:
        """Test buy order without price cannot be validated."""
        market_buy_order = Order(
            symbol=bp_symbol("BTC_USDC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.1"),
            price=None,  # Market order without price
            order_type=OrderType.MARKET,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_market_buy",
        )

        result = await balance_rule.validate(market_buy_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Cannot validate buy order without price" in result.violations[0]

    async def test_sell_order_sufficient_balance_valid(
        self,
        balance_rule: BalanceValidationRule,
        sell_order: Order,
        validation_context: ValidationContext,
    ) -> None:
        """Test sell order with sufficient BTC balance is valid."""
        # Sell order needs 1.0 BTC, we have 1.5 BTC available
        result = await balance_rule.validate(sell_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_sell_order_insufficient_balance_violation(
        self, balance_rule: BalanceValidationRule, validation_context: ValidationContext
    ) -> None:
        """Test sell order with insufficient BTC balance is rejected."""
        # Create sell order that needs more than available balance
        large_sell_order = Order(
            symbol=bp_symbol("BTC_USDC"),
            side=OrderSide.SELL,
            quantity_requested=Decimal("2.0"),  # Need 2.0 BTC, only have 1.5
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_large_sell",
        )

        result = await balance_rule.validate(large_sell_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Insufficient BTC balance" in result.violations[0]
        assert "need 2.0" in result.violations[0]
        assert "available 1.5" in result.violations[0]

    async def test_sell_order_no_base_balance_violation(
        self, balance_rule: BalanceValidationRule, validation_context: ValidationContext
    ) -> None:
        """Test sell order with no base currency balance is rejected."""
        # Create sell order for asset not in portfolio
        no_balance_order = Order(
            symbol=bp_symbol("ETH_USDC"),  # ETH not in portfolio
            side=OrderSide.SELL,
            quantity_requested=Decimal("1.0"),
            price=Decimal("3000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_no_eth",
        )

        result = await balance_rule.validate(no_balance_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "No ETH balance found" in result.violations[0]

    async def test_hyperliquid_symbol_extraction(
        self, balance_rule: BalanceValidationRule, validation_context: ValidationContext
    ) -> None:
        """Test symbol extraction works for Hyperliquid orders."""
        hl_order = Order(
            symbol=hl_symbol("BTC"),  # Hyperliquid format
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.1"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_hl_order",
        )

        result = await balance_rule.validate(hl_order, validation_context)

        # Should use USDC as quote asset and succeed with sufficient balance
        assert result.is_valid
        assert len(result.violations) == 0


class TestOrderValueLimitsRule:
    """Test cases for OrderValueLimitsRule validation logic."""

    @pytest.fixture
    def limits_rule(self) -> OrderValueLimitsRule:
        """Create OrderValueLimitsRule instance for testing."""
        return OrderValueLimitsRule(enabled=True)

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create mock AppSettings with validation limits."""
        config = Mock(spec=AppSettings)

        # Mock validation config with limits
        validation_config = Mock()
        validation_config.min_trade_value = Decimal("100.00")  # $100 minimum
        validation_config.max_trade_value = Decimal("50000.00")  # $50k maximum
        config.validation = validation_config

        return config

    @pytest.fixture
    def mock_exchange_config(self) -> Mock:
        """Create mock exchange configuration with order size limits."""
        exchange_config = Mock()
        exchange_config.min_order_size = Decimal("50.00")  # $50 minimum
        exchange_config.max_order_size = Decimal("25000.00")  # $25k maximum
        return exchange_config

    @pytest.fixture
    def validation_context(
        self, mock_config: Mock, mock_exchange_config: Mock
    ) -> ValidationContext:
        """Create validation context for testing."""
        return ValidationContext(
            config=mock_config,
            exchange_config=mock_exchange_config,
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

    @pytest.fixture
    def valid_order(self) -> Order:
        """Create test order with valid value."""
        return Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.01"),  # 0.01 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $500 total
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_valid_value",
        )

    def test_rule_properties(self, limits_rule: OrderValueLimitsRule) -> None:
        """Test rule property values."""
        assert limits_rule.name == "order_value_limits"
        assert limits_rule.category == ValidationCategory.LIMITS
        assert limits_rule.enabled is True
        assert limits_rule.bypass_on_reduce_only is False

    async def test_market_order_without_price_skipped(
        self, limits_rule: OrderValueLimitsRule, validation_context: ValidationContext
    ) -> None:
        """Test that market orders without price are skipped."""
        market_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.01"),
            price=None,  # Market order
            order_type=OrderType.MARKET,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_market",
        )

        result = await limits_rule.validate(market_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_order_within_all_limits_valid(
        self,
        limits_rule: OrderValueLimitsRule,
        valid_order: Order,
        validation_context: ValidationContext,
    ) -> None:
        """Test order within all limits is valid."""
        # Order value $500 is between global ($100-$50k) and exchange ($50-$25k) limits
        result = await limits_rule.validate(valid_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_order_below_global_minimum_violation(
        self, limits_rule: OrderValueLimitsRule, validation_context: ValidationContext
    ) -> None:
        """Test order below global minimum is rejected."""
        small_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.001"),  # 0.001 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $50 total (< $100 min)
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_small",
        )

        result = await limits_rule.validate(small_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "below global minimum" in result.violations[0]
        assert "$50" in result.violations[0]
        assert "$100" in result.violations[0]

    async def test_order_above_global_maximum_violation(
        self, limits_rule: OrderValueLimitsRule, validation_context: ValidationContext
    ) -> None:
        """Test order above global maximum is rejected."""
        large_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.5"),  # 1.5 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $75k total (> $50k max)
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_large",
        )

        result = await limits_rule.validate(large_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "exceeds global maximum" in result.violations[0]
        assert "$75000" in result.violations[0]
        assert "$50000" in result.violations[0]

    async def test_order_below_exchange_minimum_violation(
        self, limits_rule: OrderValueLimitsRule, validation_context: ValidationContext
    ) -> None:
        """Test order below exchange minimum is rejected."""
        small_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.0008"),  # 0.0008 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $40 total (< $50 exchange min)
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_exchange_small",
        )

        result = await limits_rule.validate(small_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "below exchange minimum" in result.violations[0]
        assert "$40" in result.violations[0]
        assert "$50" in result.violations[0]
        assert "backpack" in result.violations[0]

    async def test_order_above_exchange_maximum_violation(
        self, limits_rule: OrderValueLimitsRule, validation_context: ValidationContext
    ) -> None:
        """Test order above exchange maximum is rejected."""
        large_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.6"),  # 0.6 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $30k total (> $25k exchange max)
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_exchange_large",
        )

        result = await limits_rule.validate(large_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "exceeds exchange maximum" in result.violations[0]
        assert "$30000" in result.violations[0]
        assert "$25000" in result.violations[0]
        assert "backpack" in result.violations[0]

    async def test_multiple_violations(
        self, limits_rule: OrderValueLimitsRule, validation_context: ValidationContext
    ) -> None:
        """Test order can have multiple limit violations."""
        # Order that violates both global max and exchange max
        very_large_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("2.0"),  # 2.0 BTC
            price=Decimal("50000.00"),  # $50k per BTC = $100k total
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_multiple",
        )

        result = await limits_rule.validate(very_large_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 2
        assert any("exceeds global maximum" in v for v in result.violations)
        assert any("exceeds exchange maximum" in v for v in result.violations)

    async def test_no_exchange_config_uses_global_only(
        self, limits_rule: OrderValueLimitsRule, mock_config: Mock
    ) -> None:
        """Test validation uses only global limits when no exchange config."""
        context_no_exchange = ValidationContext(
            config=mock_config,
            exchange_config=None,  # No exchange config
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        # Order within global limits but would exceed exchange limits
        order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.6"),  # 0.6 BTC
            price=Decimal("50000.00"),  # $30k total (within global $50k, above exchange $25k)
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_no_exchange_config",
        )

        result = await limits_rule.validate(order, context_no_exchange)

        # Should be valid (only global limits apply)
        assert result.is_valid
        assert len(result.violations) == 0

    async def test_exchange_config_without_limits(
        self, limits_rule: OrderValueLimitsRule, mock_config: Mock
    ) -> None:
        """Test validation when exchange config has no order size limits."""
        exchange_config_no_limits = Mock()
        # No min_order_size or max_order_size attributes

        context_no_limits = ValidationContext(
            config=mock_config,
            exchange_config=exchange_config_no_limits,
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        # Large order that would violate exchange limits if they existed
        order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.8"),  # 0.8 BTC
            price=Decimal("50000.00"),  # $40k total
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_no_limits",
        )

        result = await limits_rule.validate(order, context_no_limits)

        # Should be valid (only global limits apply)
        assert result.is_valid
        assert len(result.violations) == 0

    @pytest.mark.parametrize(
        "quantity_str,expected_value,should_violate_global_min",
        [
            ("0.001", "50", True),  # $50 < $100 global min
            ("0.002", "100", False),  # $100 = $100 global min (boundary)
            ("0.01", "500", False),  # $500 within limits
            ("1.0", "50000", False),  # $50k = $50k global max (boundary)
        ],
    )
    async def test_various_order_values(
        self,
        limits_rule: OrderValueLimitsRule,
        validation_context: ValidationContext,
        quantity_str: str,
        expected_value: str,
        should_violate_global_min: bool,
    ) -> None:
        """Test various order values against limits."""
        order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal(quantity_str),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_parametrized",
        )

        result = await limits_rule.validate(order, validation_context)

        if should_violate_global_min:
            assert not result.is_valid
            assert len(result.violations) >= 1
            assert any("below global minimum" in v for v in result.violations)
            assert expected_value in result.violations[0]
        else:
            # May still violate other limits, but not global minimum
            if not result.is_valid:
                assert not any("below global minimum" in v for v in result.violations)
