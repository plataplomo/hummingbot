"""Additional comprehensive unit tests for OrderManager.

Tests additional edge cases and scenarios for the apply_fill method,
focusing on order state mutations, fill reconciliation, average fill price calculations,
status transitions, and overfill handling.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.order_manager import OrderManager
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


def create_trade(
    trade_id: str = "T1",
    symbol: str = "BTC-PERP",
    side: OrderSide = OrderSide.BUY,
    price: Decimal = Decimal("50000.0"),
    quantity: Decimal = Decimal("0.5"),
    order_id: str = "EX123456",
    exchange: str = "hyperliquid",
    client_order_id: str | None = None,
    fee: Decimal = Decimal("0.0"),
    fee_asset: str = "USDC",
    is_maker: bool = False,
) -> Trade:
    """Helper function to create a Trade with valid defaults.
    
    Returns:
        Trade: A trade instance with the specified parameters.
    """
    return Trade(
        id=trade_id,
        symbol=symbol,
        executed_at=datetime.now(UTC),
        side=side,
        order_id=order_id,
        exchange=exchange,
        price=price,
        quantity=quantity,
        client_order_id=client_order_id,
        fee=fee,
        fee_asset=fee_asset,
        is_maker=is_maker,
    )


@pytest.fixture
def base_order() -> Order:
    """Create a base order for testing.
    
    Returns:
        Order: A base order instance for testing.
    """
    return Order(
        exchange="hyperliquid",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("1.0"),
        time_in_force=TimeInForce.GTC,
        client_order_id=str(uuid4()),
        exchange_order_id="EX123456",
        price=Decimal("50000.0"),
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


class TestOrderManagerApplyFillValidation:
    """Test suite for apply_fill validation scenarios."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_matching_ids(self, mock_logger: Mock, base_order: Order) -> None:
        """Test successful fill application with matching IDs."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
            client_order_id=base_order.client_order_id,
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("0.5")
        assert base_order.average_fill_price == Decimal("50000.0")
        assert base_order.status == OrderStatus.PARTIALLY_FILLED
        assert len(base_order.trades) == 1
        assert base_order.trades[0] == trade

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_no_client_id(self, mock_logger: Mock, base_order: Order) -> None:
        """Test successful fill application when trade has no client ID."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            order_id="UNKNOWN",  # Different order_id than the order
            client_order_id=None,
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("0.5")
        assert base_order.average_fill_price == Decimal("50000.0")
        # Should get warning about order_id mismatch but not client_order_id
        mock_logger.warning.assert_any_call(
            "trade_order_id_mismatch",
            trade_order_id="UNKNOWN",
            order_exchange_id="EX123456",
            order_client_id=base_order.client_order_id,
            symbol="BTC-PERP",
            action="validation_warning",
            message="Trade order_id does not match order.exchange_order_id",
        )
        assert not any(
            call[0][0] == "trade_client_order_id_mismatch"
            for call in mock_logger.warning.call_args_list
        )

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_mismatched_order_id(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test fill application with mismatched order ID."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            order_id="DIFFERENT123",
            client_order_id=base_order.client_order_id,
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        # Should still apply the fill despite mismatch
        assert base_order.quantity_filled == Decimal("0.5")
        # Should log warning about mismatch
        mock_logger.warning.assert_any_call(
            "trade_order_id_mismatch",
            trade_order_id="DIFFERENT123",
            order_exchange_id="EX123456",
            order_client_id=base_order.client_order_id,
            symbol="BTC-PERP",
            action="validation_warning",
            message="Trade order_id does not match order.exchange_order_id",
        )

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_mismatched_client_order_id(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test fill application with mismatched client order ID."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
            client_order_id="DIFFERENT_CLIENT_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        # Should still apply the fill despite mismatch
        assert base_order.quantity_filled == Decimal("0.5")
        # Should log warning about mismatch
        mock_logger.warning.assert_any_call(
            "trade_client_order_id_mismatch",
            trade_client_order_id="DIFFERENT_CLIENT_ID",
            order_client_order_id=base_order.client_order_id,
            order_exchange_id="EX123456",
            symbol="BTC-PERP",
            action="validation_warning",
            message="Trade client_order_id does not match order.client_order_id",
        )

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_mismatched_symbol(self, mock_logger: Mock, base_order: Order) -> None:
        """Test fill application with mismatched symbol."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            symbol="ETH-PERP",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        # Should still apply the fill despite mismatch
        assert base_order.quantity_filled == Decimal("0.5")
        # Should log warning about mismatch
        mock_logger.warning.assert_any_call(
            "trade_details_mismatch",
            trade_symbol="ETH-PERP",
            order_symbol="BTC-PERP",
            trade_side="BUY",
            order_side="BUY",
            order_client_id=base_order.client_order_id,
            action="validation_warning",
            message="Trade details mismatch order details",
        )

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_mismatched_side(self, mock_logger: Mock, base_order: Order) -> None:
        """Test fill application with mismatched side."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            side=OrderSide.SELL,
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        # Should still apply the fill despite mismatch
        assert base_order.quantity_filled == Decimal("0.5")
        # Should log warning about mismatch
        mock_logger.warning.assert_any_call(
            "trade_details_mismatch",
            trade_symbol="BTC-PERP",
            order_symbol="BTC-PERP",
            trade_side="SELL",
            order_side="BUY",
            order_client_id=base_order.client_order_id,
            action="validation_warning",
            message="Trade details mismatch order details",
        )


class TestOrderManagerFillCalculations:
    """Test suite for fill calculation scenarios."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_single_fill(self, mock_logger: Mock, base_order: Order) -> None:
        """Test applying a single fill to an empty order."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("0.5")
        assert base_order.average_fill_price == Decimal("50000.0")
        assert base_order.status == OrderStatus.PARTIALLY_FILLED

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_multiple_fills_same_price(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test applying multiple fills at the same price."""
        # Arrange
        trade1 = Trade(
            id="T1",
            symbol="BTC-PERP",
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            exchange="hyperliquid",
            price=Decimal("50000.0"),
            quantity=Decimal("0.3"),
        )
        trade2 = Trade(
            id="T2",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.3"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade1)
        OrderManager.apply_fill(base_order, trade2)

        # Assert
        assert base_order.quantity_filled == Decimal("0.6")
        assert base_order.average_fill_price == Decimal("50000.0")
        assert len(base_order.trades) == 2

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_multiple_fills_different_prices(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test applying multiple fills at different prices."""
        # Arrange
        trade1 = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )
        trade2 = Trade(
            id="T2",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("51000.0"),
            quantity=Decimal("0.5"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade1)
        OrderManager.apply_fill(base_order, trade2)

        # Assert
        assert base_order.quantity_filled == Decimal("1.0")
        # Average price should be (50000*0.5 + 51000*0.5) / 1.0 = 50500
        assert base_order.average_fill_price == Decimal("50500.0")
        assert base_order.status == OrderStatus.FILLED

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_complete_fill(self, mock_logger: Mock, base_order: Order) -> None:
        """Test applying a fill that completes the order."""
        # Arrange
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("1.0")
        assert base_order.average_fill_price == Decimal("50000.0")
        assert base_order.status == OrderStatus.FILLED

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_very_small_quantity(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test applying a very small fill quantity."""
        # Arrange
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.00000001"),  # Very small quantity
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("0.00000001")
        assert base_order.average_fill_price == Decimal("50000.0")
        assert base_order.status == OrderStatus.PARTIALLY_FILLED

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_near_complete_fill(self, mock_logger: Mock, base_order: Order) -> None:
        """Test applying a fill that almost completes the order."""
        # Arrange
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.9999999999"),  # Just under requested
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("0.9999999999")
        assert base_order.status == OrderStatus.FILLED  # Should be FILLED due to tolerance


class TestOrderManagerOverfillHandling:
    """Test suite for overfill scenarios."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_slight_overfill_within_tolerance(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test handling slight overfill within tolerance."""
        # Arrange
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0000000005"),  # Slightly over, within tolerance
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("1.0")  # Snapped to requested
        assert base_order.status == OrderStatus.FILLED
        # Should log warning about snapping
        mock_logger.warning.assert_any_call(
            "order_slight_overfill_snapped",
            client_order_id=base_order.client_order_id,
            exchange_order_id="EX123456",
            symbol="BTC-PERP",
            quantity_filled=float(Decimal("1.0000000005")),
            quantity_requested=1.0,
            overfill_amount=float(Decimal("0.0000000005")),
            tolerance=float(Decimal("1e-9")),
            action="snapping_to_requested",
            message=(
                f"Order {base_order.client_order_id}: Snapping slightly "
                f"overfilled qty 1.0000000005 to requested 1.0."
            ),
        )

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_significant_overfill(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test handling significant overfill."""
        # Arrange
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.5"),  # 50% overfill
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("1.0")  # Snapped to requested
        assert base_order.status == OrderStatus.FILLED
        # Should log error about overfill
        mock_logger.error.assert_called_once()

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_success_overfill_with_existing_fills(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test overfill handling when order already has partial fills."""
        # Arrange
        # First fill
        trade1 = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.6"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )
        OrderManager.apply_fill(base_order, trade1)

        # Second fill that would cause overfill
        trade2 = Trade(
            id="T2",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("51000.0"),
            quantity=Decimal("0.5"),  # Total would be 1.1
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade2)

        # Assert
        assert base_order.quantity_filled == Decimal("1.0")  # Snapped to requested
        assert base_order.status == OrderStatus.FILLED
        # Average price calculation:
        # First fill: 0.6 * 50000 = 30000
        # Second fill: 0.4 * 51000 = 20400 (only 0.4 counted due to snap)
        # Total weighted average: 50400 / 1.0 = 50400
        assert base_order.average_fill_price == Decimal("50400.0")

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_apply_fill_edge_overfill_average_price_calculation(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test average price calculation is correct after overfill snapping."""
        # Arrange
        # First apply a trade to set order to near completion
        first_trade = Trade(
            id="T0",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_id="EX123456",
            price=Decimal("49000.0"),
            quantity=Decimal("0.9"),
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )
        OrderManager.apply_fill(base_order, first_trade)

        # Trade that would overfill
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("52000.0"),
            quantity=Decimal("0.2"),  # Would make total 1.1
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("1.0")
        # Average price: (0.9 * 49000 + 0.1 * 52000) / 1.0 = 49300
        assert base_order.average_fill_price == Decimal("49300.0")


class TestOrderManagerStatusTransitions:
    """Test suite for order status transitions."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_status_transition_success_new_to_partially_filled(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test transition from NEW to PARTIALLY_FILLED."""
        # Arrange
        base_order.status = OrderStatus.NEW
        trade = create_trade(
            trade_id="T123456",
            quantity=Decimal("0.5"),
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.status == OrderStatus.PARTIALLY_FILLED
        mock_logger.info.assert_any_call(
            "order_status_transition_to_partially_filled",
            client_order_id=base_order.client_order_id,
            exchange_order_id="EX123456",
            symbol="BTC-PERP",
            previous_status="NEW",
            new_status="PARTIALLY_FILLED",
            quantity_filled=0.5,
            quantity_requested=1.0,
            fill_percentage=50.0,
            average_fill_price=50000.0,
            trade_id="T123456",
            trade_price=50000.0,
            trade_quantity=0.5,
            message=(
                f"Order {base_order.client_order_id} partially filled (0.5/1.0) with trade T123456"
            ),
        )

    @patch("cyberdelta.core.order_manager.logger")
    def test_status_transition_success_partially_filled_to_filled(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test transition from PARTIALLY_FILLED to FILLED."""
        # Arrange
        # First apply a partial fill to get order to PARTIALLY_FILLED state
        first_trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_id="EX123456",
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )
        OrderManager.apply_fill(base_order, first_trade)

        # Now apply second fill
        trade = Trade(
            id="T2",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.5"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.status == OrderStatus.FILLED
        mock_logger.info.assert_any_call(
            "order_status_transition_to_filled",
            client_order_id=base_order.client_order_id,
            exchange_order_id="EX123456",
            symbol="BTC-PERP",
            previous_status="PARTIALLY_FILLED",
            new_status="FILLED",
            quantity_filled=1.0,
            quantity_requested=1.0,
            average_fill_price=50000.0,
            trade_id="T2",
            trade_price=50000.0,
            trade_quantity=0.5,
            message=f"Order {base_order.client_order_id} fully filled with trade T2",
        )

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_status_transition_edge_canceled_order_receives_fill(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test fill applied to CANCELED order doesn't change status."""
        # Arrange
        base_order.status = OrderStatus.CANCELED
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.status == OrderStatus.CANCELED  # Status unchanged
        assert base_order.quantity_filled == Decimal("0.5")  # Fill still applied

    @patch("cyberdelta.core.order_manager.logger")
    def test_status_transition_edge_rejected_order_receives_fill(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test fill applied to REJECTED order doesn't change status."""
        # Arrange
        base_order.status = OrderStatus.REJECTED
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.status == OrderStatus.REJECTED  # Status unchanged
        assert base_order.quantity_filled == Decimal("0.5")  # Fill still applied

    @patch("cyberdelta.core.order_manager.logger")
    def test_status_transition_edge_expired_order_receives_fill(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test fill applied to EXPIRED order doesn't change status."""
        # Arrange
        base_order.status = OrderStatus.EXPIRED
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.status == OrderStatus.EXPIRED  # Status unchanged
        assert base_order.quantity_filled == Decimal("0.5")  # Fill still applied

    @patch("cyberdelta.core.order_manager.logger")
    def test_status_transition_edge_failed_order_receives_fill(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test fill applied to FAILED order doesn't change status."""
        # Arrange
        base_order.status = OrderStatus.FAILED
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.status == OrderStatus.FAILED  # Status unchanged
        assert base_order.quantity_filled == Decimal("0.5")  # Fill still applied


class TestOrderManagerTimestampHandling:
    """Test suite for timestamp handling."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_timestamp_success_with_timezone(self, mock_logger: Mock, base_order: Order) -> None:
        """Test updated_at uses order's timezone."""
        # Arrange
        tz = base_order.created_at.tzinfo
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.updated_at is not None
        assert base_order.updated_at.tzinfo == tz

    @patch("cyberdelta.core.order_manager.logger")
    def test_timestamp_success_without_timezone(self, mock_logger: Mock, base_order: Order) -> None:
        """Test updated_at defaults to UTC when order has no timezone."""
        # Arrange
        base_order.created_at = base_order.created_at.replace(tzinfo=None)
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.updated_at is not None
        assert base_order.updated_at.tzinfo == UTC

    # ==================== EDGE CASES ====================

    @pytest.mark.timing
    @patch("cyberdelta.core.order_manager.logger")
    def test_timestamp_edge_multiple_fills_updates_timestamp(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test that each fill updates the timestamp."""
        # Arrange
        trade1 = Trade(
            id="T1",
            symbol="BTC-PERP",
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            exchange="hyperliquid",
            price=Decimal("50000.0"),
            quantity=Decimal("0.3"),
        )
        trade2 = Trade(
            id="T2",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.3"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade1)
        first_update = base_order.updated_at

        # Small delay to ensure different timestamp
        import time  # noqa: PLC0415

        time.sleep(0.001)

        OrderManager.apply_fill(base_order, trade2)
        second_update = base_order.updated_at

        # Assert
        assert first_update is not None
        assert second_update is not None
        assert second_update >= first_update


class TestOrderManagerTradeAccumulation:
    """Test suite for trade accumulation."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_trade_accumulation_success_single_trade(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test single trade is added to trades list."""
        # Arrange
        trade = create_trade(
            trade_id="T123456",
            order_id=base_order.exchange_order_id or "DEFAULT_ORDER_ID",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert len(base_order.trades) == 1
        assert base_order.trades[0] == trade

    @patch("cyberdelta.core.order_manager.logger")
    def test_trade_accumulation_success_multiple_trades(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test multiple trades are accumulated in order."""
        # Arrange
        trades: list[Trade] = []
        for i in range(5):
            trade = Trade(
                id=f"T{i}",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("0.2"),
                order_id="EX123456",
                executed_at=datetime.now(UTC),
                exchange="hyperliquid",
            )
            trades.append(trade)

        # Act
        for trade in trades:
            OrderManager.apply_fill(base_order, trade)

        # Assert
        assert len(base_order.trades) == 5
        assert base_order.trades == trades
        assert base_order.quantity_filled == Decimal("1.0")
        assert base_order.status == OrderStatus.FILLED

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_trade_accumulation_edge_duplicate_trade_ids(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test handling of trades with duplicate IDs."""
        # Arrange
        trade1 = Trade(
            id="T1",  # Same ID
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.3"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )
        trade2 = Trade(
            id="T1",  # Same ID
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("51000.0"),
            quantity=Decimal("0.3"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade1)
        OrderManager.apply_fill(base_order, trade2)

        # Assert
        # Both trades should be added despite duplicate IDs
        assert len(base_order.trades) == 2
        assert base_order.quantity_filled == Decimal("0.6")


class TestOrderManagerSpecialCases:
    """Test suite for special cases and edge scenarios."""

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.order_manager.logger")
    def test_special_case_edge_minimal_quantity_trade(
        self, mock_logger: Mock, base_order: Order
    ) -> None:
        """Test handling of minimal quantity trade."""
        # Arrange
        # Use smallest possible quantity that's still positive
        min_quantity = Decimal("1e-18")
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_id="EX123456",
            price=Decimal("50000.0"),
            quantity=min_quantity,  # Minimal positive quantity
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == min_quantity
        assert base_order.average_fill_price == Decimal("50000.0")
        assert base_order.status == OrderStatus.PARTIALLY_FILLED  # Status changed

    @patch("cyberdelta.core.order_manager.logger")
    def test_special_case_edge_very_high_price(self, mock_logger: Mock, base_order: Order) -> None:
        """Test handling of very high price trades."""
        # Arrange
        trade = Trade(
            id="T1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal("99999999.99999999"),  # Very high price
            quantity=Decimal("0.5"),
            order_id="EX123456",
            executed_at=datetime.now(UTC),
            exchange="hyperliquid",
        )

        # Act
        OrderManager.apply_fill(base_order, trade)

        # Assert
        assert base_order.quantity_filled == Decimal("0.5")
        assert base_order.average_fill_price == Decimal("99999999.99999999")
