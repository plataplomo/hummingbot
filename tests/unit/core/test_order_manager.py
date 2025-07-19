"""Unit tests for the OrderManager component.

Tests order state management functionality including fill application and status transitions.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.order_manager import OrderManager
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


pytestmark = pytest.mark.timing


@pytest.fixture
def sample_order() -> Order:
    """Create a sample order for testing."""
    return Order(
        client_order_id="TEST-ORDER-001",
        exchange_order_id="EXCHANGE-001",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.IOC,
        quantity_requested=Decimal("1.0"),
        price=Decimal("50000.0"),
        status=OrderStatus.NEW,
        exchange="test_exchange",
        created_at=datetime.now(UTC),
        quantity_filled=Decimal(0),
        average_fill_price=None,
        trades=[],
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


@pytest.fixture
def sample_trade() -> Trade:
    """Create a sample trade for testing."""
    return Trade(
        id="TRADE-001",
        order_id="EXCHANGE-001",
        client_order_id="TEST-ORDER-001",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        exchange="test_exchange",
        price=Decimal("50000.0"),
        quantity=Decimal("0.5"),
        fee=Decimal("0.5"),
        fee_asset="USDT",
        executed_at=datetime.now(UTC),
    )


class TestOrderManagerApplyFill:
    """Test suite for OrderManager.apply_fill method."""

    # ==================== SUCCESS CASES ====================

    def test_apply_fill_success_first_fill(self, sample_order: Order, sample_trade: Trade) -> None:
        """Test successful application of first fill to an order."""
        # Arrange
        assert sample_order.quantity_filled == Decimal(0)
        assert sample_order.average_fill_price is None
        assert len(sample_order.trades) == 0

        # Act
        OrderManager.apply_fill(sample_order, sample_trade)

        # Assert
        assert sample_order.quantity_filled == Decimal("0.5")
        assert sample_order.average_fill_price == Decimal("50000.0")
        assert len(sample_order.trades) == 1
        assert sample_order.trades[0] == sample_trade
        assert sample_order.status == OrderStatus.PARTIALLY_FILLED

    def test_apply_fill_success_complete_fill(self, sample_order: Order) -> None:
        """Test successful application of fill that completes the order."""
        # Arrange
        full_trade = Trade(
            id="TRADE-002",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            fee=Decimal("1.0"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        OrderManager.apply_fill(sample_order, full_trade)

        # Assert
        assert sample_order.quantity_filled == Decimal("1.0")
        assert sample_order.average_fill_price == Decimal("50000.0")
        assert sample_order.status == OrderStatus.FILLED

    def test_apply_fill_success_multiple_fills(self, sample_order: Order) -> None:
        """Test successful application of multiple fills with correct average price calculation."""
        # Arrange
        trade1 = Trade(
            id="TRADE-001",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.3"),
            fee=Decimal("0.3"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )
        trade2 = Trade(
            id="TRADE-002",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("51000.0"),
            quantity=Decimal("0.7"),
            fee=Decimal("0.7"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        OrderManager.apply_fill(sample_order, trade1)
        OrderManager.apply_fill(sample_order, trade2)

        # Assert
        assert sample_order.quantity_filled == Decimal("1.0")
        # Average price = (0.3 * 50000 + 0.7 * 51000) / 1.0 = 50700
        assert sample_order.average_fill_price == Decimal("50700.0")
        assert len(sample_order.trades) == 2
        assert sample_order.status == OrderStatus.FILLED

    # ==================== EDGE CASES ====================

    def test_apply_fill_edge_tiny_overfill_snapped(self, sample_order: Order) -> None:
        """Test handling of tiny overfill within tolerance."""
        # Arrange
        overfill_trade = Trade(
            id="TRADE-003",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("1.0000000001"),  # Tiny overfill
            fee=Decimal("1.0"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(sample_order, overfill_trade)

        # Assert - should snap to requested quantity
        assert sample_order.quantity_filled == Decimal("1.0")
        assert sample_order.status == OrderStatus.FILLED
        mock_logger.warning.assert_called_once()
        assert "order_slight_overfill_snapped" in str(mock_logger.warning.call_args)

    def test_apply_fill_edge_large_overfill_error(self, sample_order: Order) -> None:
        """Test handling of significant overfill."""
        # Arrange
        overfill_trade = Trade(
            id="TRADE-004",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("1.5"),  # 50% overfill
            fee=Decimal("1.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(sample_order, overfill_trade)

        # Assert - should snap to requested quantity with error log
        assert sample_order.quantity_filled == Decimal("1.0")
        assert sample_order.status == OrderStatus.FILLED
        mock_logger.error.assert_called_once()
        assert "order_overfill_error" in str(mock_logger.error.call_args)

    def test_apply_fill_edge_order_already_canceled(
        self, sample_order: Order, sample_trade: Trade
    ) -> None:
        """Test applying fill to canceled order - status should not change."""
        # Arrange
        sample_order.status = OrderStatus.CANCELED

        # Act
        OrderManager.apply_fill(sample_order, sample_trade)

        # Assert - fill is applied but status remains CANCELED
        assert sample_order.quantity_filled == Decimal("0.5")
        assert sample_order.average_fill_price == Decimal("50000.0")
        assert sample_order.status == OrderStatus.CANCELED

    def test_apply_fill_edge_order_already_rejected(
        self, sample_order: Order, sample_trade: Trade
    ) -> None:
        """Test applying fill to rejected order - status should not change."""
        # Arrange
        sample_order.status = OrderStatus.REJECTED

        # Act
        OrderManager.apply_fill(sample_order, sample_trade)

        # Assert - fill is applied but status remains REJECTED
        assert sample_order.quantity_filled == Decimal("0.5")
        assert sample_order.status == OrderStatus.REJECTED

    def test_apply_fill_edge_zero_quantity_trade(self, sample_order: Order) -> None:
        """Test that zero quantity trades are not allowed (business logic validation)."""
        # Arrange & Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            Trade(
                id="TRADE-005",
                order_id="EXCHANGE-001",
                client_order_id="TEST-ORDER-001",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                exchange="test_exchange",
                price=Decimal("50000.0"),
                quantity=Decimal(0),  # This should be invalid per business logic
                fee=Decimal(0),
                fee_asset="USDT",
                executed_at=datetime.now(UTC),
            )

        # Verify the validation error is about quantity
        error_msg = str(exc_info.value)
        assert "quantity" in error_msg.lower()
        assert "greater than" in error_msg.lower()

    def test_apply_fill_edge_different_prices_average_calculation(
        self, sample_order: Order
    ) -> None:
        """Test correct average price calculation with vastly different prices."""
        # Arrange
        trade1 = Trade(
            id="TRADE-001",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("10000.0"),  # Very low price
            quantity=Decimal("0.5"),
            fee=Decimal("0.05"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )
        trade2 = Trade(
            id="TRADE-002",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("90000.0"),  # Very high price
            quantity=Decimal("0.5"),
            fee=Decimal("0.45"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        OrderManager.apply_fill(sample_order, trade1)
        OrderManager.apply_fill(sample_order, trade2)

        # Assert
        assert sample_order.quantity_filled == Decimal("1.0")
        # Average = (0.5 * 10000 + 0.5 * 90000) / 1.0 = 50000
        assert sample_order.average_fill_price == Decimal("50000.0")

    # ==================== FAILURE CASES ====================

    def test_apply_fill_failure_mismatched_order_id(self, sample_order: Order) -> None:
        """Test warning when trade order_id doesn't match."""
        # Arrange
        mismatched_trade = Trade(
            id="TRADE-006",
            order_id="DIFFERENT-ORDER",  # Mismatched
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(sample_order, mismatched_trade)

        # Assert - fill is still applied but warning is logged
        assert sample_order.quantity_filled == Decimal("0.5")
        mock_logger.warning.assert_called()
        warning_calls = mock_logger.warning.call_args_list
        assert any("trade_order_id_mismatch" in str(call) for call in warning_calls)

    def test_apply_fill_failure_mismatched_client_order_id(self, sample_order: Order) -> None:
        """Test warning when trade client_order_id doesn't match."""
        # Arrange
        mismatched_trade = Trade(
            id="TRADE-007",
            order_id="EXCHANGE-001",
            client_order_id="DIFFERENT-CLIENT-ORDER",  # Mismatched
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(sample_order, mismatched_trade)

        # Assert - fill is still applied but warning is logged
        assert sample_order.quantity_filled == Decimal("0.5")
        mock_logger.warning.assert_called()
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("trade_client_order_id_mismatch" in call for call in warning_calls)

    def test_apply_fill_failure_mismatched_symbol(self, sample_order: Order) -> None:
        """Test warning when trade symbol doesn't match."""
        # Arrange
        mismatched_trade = Trade(
            id="TRADE-008",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="ETH-PERP",  # Different symbol
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("3000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(sample_order, mismatched_trade)

        # Assert - fill is still applied but warning is logged
        assert sample_order.quantity_filled == Decimal("0.5")
        mock_logger.warning.assert_called()
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("trade_details_mismatch" in call for call in warning_calls)

    def test_apply_fill_failure_mismatched_side(self, sample_order: Order) -> None:
        """Test warning when trade side doesn't match."""
        # Arrange
        mismatched_trade = Trade(
            id="TRADE-009",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.SELL,  # Different side
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(sample_order, mismatched_trade)

        # Assert - fill is still applied but warning is logged
        assert sample_order.quantity_filled == Decimal("0.5")
        mock_logger.warning.assert_called()
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("trade_details_mismatch" in call for call in warning_calls)


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("initial_status", "fill_fraction", "expected_status"),
    [
        # Success cases - status transitions
        (OrderStatus.NEW, Decimal("0.5"), OrderStatus.PARTIALLY_FILLED),
        (OrderStatus.NEW, Decimal("1.0"), OrderStatus.FILLED),
        (OrderStatus.PARTIALLY_FILLED, Decimal("0.5"), OrderStatus.PARTIALLY_FILLED),
        (OrderStatus.PARTIALLY_FILLED, Decimal("1.0"), OrderStatus.FILLED),
        # Edge cases - terminal states don't change
        (OrderStatus.CANCELED, Decimal("0.5"), OrderStatus.CANCELED),
        (OrderStatus.REJECTED, Decimal("0.5"), OrderStatus.REJECTED),
        (OrderStatus.EXPIRED, Decimal("0.5"), OrderStatus.EXPIRED),
        (OrderStatus.FAILED, Decimal("0.5"), OrderStatus.FAILED),
        # Edge case - already filled
        (OrderStatus.FILLED, Decimal("0.1"), OrderStatus.FILLED),
    ],
)
def test_order_status_transitions(
    sample_order: Order,
    initial_status: OrderStatus,
    fill_fraction: Decimal,
    expected_status: OrderStatus,
) -> None:
    """Test various order status transitions based on fills."""
    # Arrange
    sample_order.status = initial_status
    if initial_status == OrderStatus.PARTIALLY_FILLED:
        # Pre-fill some quantity and set average fill price
        # Set both fields using model_copy to avoid validation errors
        sample_order.__dict__.update({
            "quantity_filled": Decimal("0.3"),  # Pre-fill 30%
            "average_fill_price": Decimal("50000.0"),
        })
        # For partially filled orders, fill_fraction represents additional fill amount
        fill_quantity = sample_order.quantity_requested * fill_fraction
    elif initial_status == OrderStatus.FILLED:
        # Pre-fill the entire order
        sample_order.__dict__.update({
            "quantity_filled": Decimal("1.0"),  # Fully filled
            "average_fill_price": Decimal("50000.0"),
        })
        # For filled orders, fill_fraction represents additional fill amount (overfill)
        fill_quantity = sample_order.quantity_requested * fill_fraction
    else:
        fill_quantity = sample_order.quantity_requested * fill_fraction

    trade = Trade(
        id=f"TRADE-PARAM-{initial_status.value}",
        order_id="EXCHANGE-001",
        client_order_id="TEST-ORDER-001",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        exchange="test_exchange",
        price=Decimal("50000.0"),
        quantity=fill_quantity,
        fee=Decimal("0.1"),
        fee_asset="USDT",
        executed_at=datetime.now(UTC),
    )

    # Act
    OrderManager.apply_fill(sample_order, trade)

    # Assert
    assert sample_order.status == expected_status


@pytest.mark.parametrize(
    ("prices", "quantities", "expected_avg_price"),
    [
        # Simple cases
        ([Decimal(100)], [Decimal(1)], Decimal(100)),
        (
            [Decimal(100), Decimal(200)],
            [Decimal(1), Decimal(1)],
            Decimal(100),
        ),  # Second trade is overfill
        # Weighted average cases - using smaller quantities to avoid overfill
        ([Decimal(100), Decimal(200)], [Decimal("0.75"), Decimal("0.25")], Decimal(125)),
        (
            [Decimal(100), Decimal(200), Decimal(300)],
            [Decimal("0.25"), Decimal("0.25"), Decimal("0.5")],
            Decimal(225),  # (100*0.25 + 200*0.25 + 300*0.5) / 1.0
        ),
        # Edge case - very small quantities
        (
            [Decimal(100), Decimal(200)],
            [Decimal("0.0001"), Decimal("0.0002")],
            Decimal("166.6666666666666666666666667"),
        ),
        # Edge case - very large prices
        ([Decimal(1000000), Decimal(2000000)], [Decimal("0.5"), Decimal("0.5")], Decimal(1500000)),
    ],
)
def test_average_price_calculation(
    sample_order: Order,
    prices: list[Decimal],
    quantities: list[Decimal],
    expected_avg_price: Decimal,
) -> None:
    """Test average fill price calculation across multiple trades."""
    # Arrange & Act
    for i, (price, quantity) in enumerate(zip(prices, quantities, strict=False)):
        trade = Trade(
            id=f"TRADE-AVG-{i}",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=price,
            quantity=quantity,
            fee=Decimal("0.01"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )
        OrderManager.apply_fill(sample_order, trade)

    # Assert
    assert sample_order.average_fill_price == expected_avg_price


class TestOrderManagerEdgeCases:
    """Additional edge case tests for OrderManager."""

    def test_apply_fill_edge_order_with_no_exchange_order_id(self) -> None:
        """Test applying fill to order without exchange_order_id."""
        # Arrange
        order = Order(
            client_order_id="TEST-ORDER-NO-EXCHANGE-ID",
            exchange_order_id=None,  # No exchange ID
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.IOC,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.NEW,
            exchange="test_exchange",
            created_at=datetime.now(UTC),
            quantity_filled=Decimal(0),
            average_fill_price=None,
            trades=[],
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        trade = Trade(
            id="TRADE-NO-EXCHANGE",
            order_id="SOME-EXCHANGE-ID",
            client_order_id="TEST-ORDER-NO-EXCHANGE-ID",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(order, trade)

        # Assert - fill is applied without warning about order_id mismatch
        assert order.quantity_filled == Decimal("0.5")
        # Should only have one warning about client_order_id match, not order_id
        warning_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "trade_order_id_mismatch" in str(call)
        ]
        assert len(warning_calls) == 0

    def test_apply_fill_edge_trade_with_same_side(self) -> None:
        """Test applying trade with matching side - no warning expected."""
        # Arrange
        order = Order(
            client_order_id="TEST-ORDER-001",
            exchange_order_id="EXCHANGE-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.IOC,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.NEW,
            exchange="test_exchange",
            created_at=datetime.now(UTC),
            quantity_filled=Decimal(0),
            average_fill_price=None,
            trades=[],
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        trade = Trade(
            id="TRADE-NO-SIDE",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,  # Will test side mismatch differently
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        with patch("cyberdelta.core.order_manager.logger") as mock_logger:
            OrderManager.apply_fill(order, trade)

        # Assert - fill is applied without any warnings since sides match
        assert order.quantity_filled == Decimal("0.5")
        mock_logger.warning.assert_not_called()

    def test_apply_fill_edge_updated_at_with_timezone(self) -> None:
        """Test that updated_at respects order's timezone."""
        # Arrange
        utc_plus_5 = timezone(timedelta(hours=5))

        order = Order(
            client_order_id="TEST-ORDER-TZ",
            exchange_order_id="EXCHANGE-001",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.IOC,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.NEW,
            exchange="test_exchange",
            created_at=datetime.now(utc_plus_5),  # Non-UTC timezone
            quantity_filled=Decimal(0),
            average_fill_price=None,
            trades=[],
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        trade = Trade(
            id="TRADE-TZ",
            order_id="EXCHANGE-001",
            client_order_id="TEST-ORDER-TZ",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            exchange="test_exchange",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            fee=Decimal("0.5"),
            fee_asset="USDT",
            executed_at=datetime.now(UTC),
        )

        # Act
        OrderManager.apply_fill(order, trade)

        # Assert - updated_at should have same timezone as created_at
        assert order.updated_at is not None
        assert order.updated_at.tzinfo == order.created_at.tzinfo
