"""Unit tests for precision validation rules.

Tests the PricePrecisionRule and QuantityPrecisionRule implementations
that validate order price and quantity alignment to exchange requirements.

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values
- Uses real exchange constraints from configuration
- Fails fast on critical precision errors
- All financial calculations use Decimal
"""

from __future__ import annotations

from decimal import Decimal
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from cyberdelta.config.models import AppSettings
from cyberdelta.infrastructure.validation.rules.precision_rules import (
    PricePrecisionRule,
    QuantityPrecisionRule,
)
from cyberdelta.infrastructure.validation.validation_context import ValidationContext
from cyberdelta.enums import ExchangeName, OrderSide, OrderType, TradingState, ValidationCategory
from cyberdelta.models.market.order import Order
from cyberdelta.models.validation import ValidationResult
from cyberdelta.symbols import bp_symbol, hl_symbol


class TestPricePrecisionRule:
    """Test cases for PricePrecisionRule validation logic."""

    @pytest.fixture
    def price_rule(self) -> PricePrecisionRule:
        """Create PricePrecisionRule instance for testing."""
        return PricePrecisionRule(enabled=True)

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create mock AppSettings for testing."""
        config = Mock(spec=AppSettings)
        config.exchanges = {}
        return config

    @pytest.fixture
    def mock_exchange_config(self) -> Mock:
        """Create mock exchange configuration with tick size."""
        exchange_config = Mock()
        exchange_config.tick_size = 0.01  # $0.01 tick size
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
    def limit_order(self) -> Order:
        """Create test limit order."""
        return Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.00"),  # Aligned to $0.01 tick
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_order_001",
        )

    @pytest.fixture
    def market_order(self) -> Order:
        """Create test market order."""
        return Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=None,  # Market orders don't have price
            order_type=OrderType.MARKET,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_order_002",
        )

    def test_rule_properties(self, price_rule: PricePrecisionRule) -> None:
        """Test rule property values."""
        assert price_rule.name == "price_precision"
        assert price_rule.category == ValidationCategory.PRECISION
        assert price_rule.enabled is True
        assert price_rule.bypass_on_reduce_only is False

    async def test_market_order_skipped(
        self,
        price_rule: PricePrecisionRule,
        market_order: Order,
        validation_context: ValidationContext,
    ) -> None:
        """Test that market orders are skipped (no price to validate)."""
        result = await price_rule.validate(market_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_limit_order_without_price_violation(
        self, price_rule: PricePrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that limit orders without price are rejected."""
        limit_order_no_price = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=None,  # Invalid: limit order must have price
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_order_003",
        )

        result = await price_rule.validate(limit_order_no_price, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Limit order must have a price specified" in result.violations[0]

    async def test_negative_price_violation(
        self, price_rule: PricePrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that negative prices are rejected."""
        negative_price_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=Decimal("-100.00"),  # Invalid: negative price
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_order_004",
        )

        result = await price_rule.validate(negative_price_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Price must be positive" in result.violations[0]
        assert "BTC" in result.violations[0]

    async def test_zero_price_violation(
        self, price_rule: PricePrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that zero prices are rejected."""
        zero_price_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=Decimal("0.00"),  # Invalid: zero price
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_order_005",
        )

        result = await price_rule.validate(zero_price_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Price must be positive" in result.violations[0]

    async def test_price_aligned_to_tick_size_valid(
        self,
        price_rule: PricePrecisionRule,
        limit_order: Order,
        validation_context: ValidationContext,
    ) -> None:
        """Test that prices aligned to tick size are valid."""
        # Order with price aligned to $0.01 tick size
        result = await price_rule.validate(limit_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_price_not_aligned_to_tick_size_violation(
        self, price_rule: PricePrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that prices not aligned to tick size are rejected."""
        misaligned_order = Order(
            symbol=bp_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.005"),  # Not aligned to $0.01 tick
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.BACKPACK,
            exchange_order_id="test_order_006",
        )

        result = await price_rule.validate(misaligned_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "not aligned to tick size" in result.violations[0]
        assert "50000.005" in result.violations[0]
        assert "0.01" in result.violations[0]

    async def test_no_tick_size_configured_valid(
        self, price_rule: PricePrecisionRule, limit_order: Order, mock_config: Mock
    ) -> None:
        """Test that orders are valid when no tick size is configured."""
        # Context without exchange config (no tick size)
        context_no_tick = ValidationContext(
            config=mock_config,
            exchange_config=None,  # No exchange config
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        result = await price_rule.validate(limit_order, context_no_tick)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_zero_tick_size_valid(
        self, price_rule: PricePrecisionRule, limit_order: Order, mock_config: Mock
    ) -> None:
        """Test that orders are valid when tick size is zero."""
        mock_exchange_config = Mock()
        mock_exchange_config.tick_size = 0.0  # Zero tick size

        context_zero_tick = ValidationContext(
            config=mock_config,
            exchange_config=mock_exchange_config,
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        result = await price_rule.validate(limit_order, context_zero_tick)

        assert result.is_valid
        assert len(result.violations) == 0

    def test_precision_alignment_check(self, price_rule: PricePrecisionRule) -> None:
        """Test the precision alignment helper method."""
        tick_size = Decimal("0.01")

        # Test aligned prices
        assert price_rule._is_precision_valid(Decimal("100.00"), tick_size)
        assert price_rule._is_precision_valid(Decimal("100.01"), tick_size)
        assert price_rule._is_precision_valid(Decimal("99.99"), tick_size)

        # Test misaligned prices
        assert not price_rule._is_precision_valid(Decimal("100.005"), tick_size)
        assert not price_rule._is_precision_valid(Decimal("100.001"), tick_size)

    def test_tick_alignment_helper(self, price_rule: PricePrecisionRule) -> None:
        """Test the tick alignment helper method."""
        tick_size = Decimal("0.01")

        # Test price alignment (rounds down)
        aligned = price_rule._align_to_tick(Decimal("100.005"), tick_size)
        assert aligned == Decimal("100.00")

        aligned = price_rule._align_to_tick(Decimal("100.019"), tick_size)
        assert aligned == Decimal("100.01")


class TestQuantityPrecisionRule:
    """Test cases for QuantityPrecisionRule validation logic."""

    @pytest.fixture
    def quantity_rule(self) -> QuantityPrecisionRule:
        """Create QuantityPrecisionRule instance for testing."""
        return QuantityPrecisionRule(enabled=True)

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create mock AppSettings for testing."""
        config = Mock(spec=AppSettings)
        config.exchanges = {}
        return config

    @pytest.fixture
    def mock_exchange_config(self) -> Mock:
        """Create mock exchange configuration with lot size."""
        exchange_config = Mock()
        exchange_config.lot_size = 0.001  # 0.001 BTC lot size
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
        """Create test order with valid quantity."""
        return Order(
            symbol=hl_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.000"),  # Aligned to 0.001 lot size
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_order_007",
        )

    def test_rule_properties(self, quantity_rule: QuantityPrecisionRule) -> None:
        """Test rule property values."""
        assert quantity_rule.name == "quantity_precision"
        assert quantity_rule.category == ValidationCategory.PRECISION
        assert quantity_rule.enabled is True
        assert quantity_rule.bypass_on_reduce_only is False

    async def test_negative_quantity_violation(
        self, quantity_rule: QuantityPrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that negative quantities are rejected."""
        negative_quantity_order = Order(
            symbol=hl_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("-1.0"),  # Invalid: negative quantity
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_order_008",
        )

        result = await quantity_rule.validate(negative_quantity_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Quantity must be positive" in result.violations[0]
        assert "BTC" in result.violations[0]

    async def test_zero_quantity_violation(
        self, quantity_rule: QuantityPrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that zero quantities are rejected."""
        zero_quantity_order = Order(
            symbol=hl_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("0.000"),  # Invalid: zero quantity
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_order_009",
        )

        result = await quantity_rule.validate(zero_quantity_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "Quantity must be positive" in result.violations[0]

    async def test_quantity_aligned_to_lot_size_valid(
        self,
        quantity_rule: QuantityPrecisionRule,
        valid_order: Order,
        validation_context: ValidationContext,
    ) -> None:
        """Test that quantities aligned to lot size are valid."""
        result = await quantity_rule.validate(valid_order, validation_context)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_quantity_not_aligned_to_lot_size_violation(
        self, quantity_rule: QuantityPrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that quantities not aligned to lot size are rejected."""
        misaligned_order = Order(
            symbol=hl_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0005"),  # Not aligned to 0.001 lot
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_order_010",
        )

        result = await quantity_rule.validate(misaligned_order, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "not aligned to lot size" in result.violations[0]
        assert "1.0005" in result.violations[0]
        assert "0.001" in result.violations[0]

    async def test_no_lot_size_configured_valid(
        self, quantity_rule: QuantityPrecisionRule, valid_order: Order, mock_config: Mock
    ) -> None:
        """Test that orders are valid when no lot size is configured."""
        # Context without exchange config (no lot size)
        context_no_lot = ValidationContext(
            config=mock_config,
            exchange_config=None,  # No exchange config
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        result = await quantity_rule.validate(valid_order, context_no_lot)

        assert result.is_valid
        assert len(result.violations) == 0

    async def test_zero_lot_size_valid(
        self, quantity_rule: QuantityPrecisionRule, valid_order: Order, mock_config: Mock
    ) -> None:
        """Test that orders are valid when lot size is zero."""
        mock_exchange_config = Mock()
        mock_exchange_config.lot_size = 0.0  # Zero lot size

        context_zero_lot = ValidationContext(
            config=mock_config,
            exchange_config=mock_exchange_config,
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        result = await quantity_rule.validate(valid_order, context_zero_lot)

        assert result.is_valid
        assert len(result.violations) == 0

    def test_precision_alignment_check(self, quantity_rule: QuantityPrecisionRule) -> None:
        """Test the precision alignment helper method."""
        lot_size = Decimal("0.001")

        # Test aligned quantities
        assert quantity_rule._is_precision_valid(Decimal("1.000"), lot_size)
        assert quantity_rule._is_precision_valid(Decimal("0.001"), lot_size)
        assert quantity_rule._is_precision_valid(Decimal("10.250"), lot_size)

        # Test misaligned quantities
        assert not quantity_rule._is_precision_valid(Decimal("1.0005"), lot_size)
        assert not quantity_rule._is_precision_valid(Decimal("0.0001"), lot_size)

    def test_lot_alignment_helper(self, quantity_rule: QuantityPrecisionRule) -> None:
        """Test the lot alignment helper method."""
        lot_size = Decimal("0.001")

        # Test quantity alignment (rounds down)
        aligned = quantity_rule._align_to_lot(Decimal("1.0005"), lot_size)
        assert aligned == Decimal("1.000")

        aligned = quantity_rule._align_to_lot(Decimal("0.0019"), lot_size)
        assert aligned == Decimal("0.001")

    async def test_market_order_quantity_validation(
        self, quantity_rule: QuantityPrecisionRule, validation_context: ValidationContext
    ) -> None:
        """Test that market orders also validate quantity precision."""
        market_order_misaligned = Order(
            symbol=hl_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal("1.0005"),  # Not aligned to 0.001 lot
            price=None,  # Market order
            order_type=OrderType.MARKET,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_order_011",
        )

        result = await quantity_rule.validate(market_order_misaligned, validation_context)

        assert not result.is_valid
        assert len(result.violations) == 1
        assert "not aligned to lot size" in result.violations[0]

    @pytest.mark.parametrize(
        "quantity_str,lot_size_str,should_be_valid",
        [
            # Valid alignments
            ("1.000", "0.001", True),
            ("0.500", "0.001", True),
            ("10.000", "0.010", True),
            ("5.50", "0.50", True),
            # Invalid alignments
            ("1.0005", "0.001", False),
            ("0.9999", "0.001", False),
            ("10.005", "0.010", False),
            ("5.25", "0.50", False),
        ],
    )
    async def test_various_quantity_alignments(
        self,
        quantity_rule: QuantityPrecisionRule,
        mock_config: Mock,
        quantity_str: str,
        lot_size_str: str,
        should_be_valid: bool,
    ) -> None:
        """Test various quantity and lot size combinations."""
        mock_exchange_config = Mock()
        mock_exchange_config.lot_size = float(lot_size_str)

        context = ValidationContext(
            config=mock_config,
            exchange_config=mock_exchange_config,
            market_snapshot=None,
            portfolio_state=None,
            trading_state=TradingState.ACTIVE,
            timestamp=datetime.now(UTC),
        )

        order = Order(
            symbol=hl_symbol("BTC"),
            side=OrderSide.BUY,
            quantity_requested=Decimal(quantity_str),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            exchange=ExchangeName.HYPERLIQUID,
            exchange_order_id="test_parametrized",
        )

        result = await quantity_rule.validate(order, context)

        assert result.is_valid == should_be_valid
        if not should_be_valid:
            assert len(result.violations) == 1
            assert "not aligned to lot size" in result.violations[0]
