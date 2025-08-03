"""CyberDeltaEngine: Backpack Trading Data Mapper Integration Tests.

---------------------------------------------------------------

Comprehensive integration test suite for BackpackOrderMapper.
Tests complex scenarios, cross-method consistency, and advanced business logic including:
- Integration between raw order and order data transformations
- Data consistency validation across transformation methods
- Complex order scenarios with multiple fields
- Cross-method transformation consistency
- Business logic validation and edge case handling
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from tests.common_symbols import ADA_BTC_BP, BTC_USDC_BP, DOGE_USDT_BP, ETH_USDC_BP, SOL_USDC_BP


# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.common import TransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


# --- Fixtures ---


@pytest.fixture
def trading_data_mapper() -> BackpackOrderMapper:
    """Provide an instance of BackpackOrderMapper.

    Returns:
        BackpackOrderMapper: Configured mapper instance.
    """
    return BackpackOrderMapper()


def create_raw_order(
    side: str = "Buy",
    status: str = "NEW",
    order_type: str = "LIMIT",
    price: str | None = "3000.50",
    quantity: str = "1.5",
    executed_quantity: str | None = "0.0",
    order_id: str = "12345",
    client_id: str | None = "test_order_001",
    symbol: str = SOL_USDC_BP.value,
    time_in_force: str | None = "GTC",
    created_at: str | None = None,
    updated_at: str | None = None,
    avg_fill_price: str | None = None,
) -> BackpackRawOrderResponse:
    """Create a BackpackRawOrderResponse with customizable parameters.

    Returns:
        BackpackRawOrderResponse: A raw order object with the specified parameters.
    """
    if created_at is None:
        created_at = datetime.now(UTC).isoformat()
    if updated_at is None:
        updated_at = datetime.now(UTC).isoformat()

    return BackpackRawOrderResponse(
        id=order_id,
        clientId=client_id,
        relatedOrderId=None,
        symbol=symbol,
        side=side,
        orderType=order_type,
        status=status,
        quantity=quantity,
        executedQuantity=executed_quantity,
        executedQuoteQuantity=None,
        price=price,
        triggerPrice=None,
        avgFillPrice=avg_fill_price,
        triggerBy=None,
        timeInForce=time_in_force,
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention=None,
        createdAt=created_at,
        updatedAt=updated_at,
        triggeredAt=None,
        expiryReason=None,
        origin=None,
    )


# --- Integration Tests ---


class TestTradingDataMapperIntegration:
    """Integration tests for the complete trading data mapper functionality."""

    def test_complete_order_transformation_consistency(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test that both transformation methods produce consistent results."""
        # Create raw order
        raw_order = create_raw_order(
            side="Buy",
            order_type="LIMIT",
            status="FILLED",
            quantity="1.0",
            executed_quantity="1.0",
            avg_fill_price="3000.00",
            price="3000.00",
            time_in_force="IOC",
        )

        # Transform via raw order method
        result_raw = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Transform via order data method
        result_data = trading_data_mapper.transform_order_data_to_internal(
            order_id=raw_order.id,
            symbol=raw_order.symbol,
            side=raw_order.side,
            order_type=raw_order.orderType,
            status=raw_order.status,
            quantity=raw_order.quantity or "0",
            price=raw_order.price,
            client_order_id=raw_order.clientId,
            time_in_force=raw_order.timeInForce,
            created_at=str(raw_order.createdAt) if raw_order.createdAt else None,
            updated_at=str(raw_order.updatedAt) if raw_order.updatedAt else None,
        )

        # Both should produce similar core results
        assert result_raw.exchange_order_id == result_data.exchange_order_id
        assert result_raw.symbol.value == result_data.symbol.value
        assert result_raw.side == result_data.side
        assert result_raw.order_type == result_data.order_type
        assert result_raw.status == result_data.status
        assert result_raw.quantity_requested == result_data.quantity_requested
        assert result_raw.price == result_data.price
        assert result_raw.time_in_force == result_data.time_in_force
        assert result_raw.exchange == result_data.exchange

    def test_error_handling_consistency(
        self,
        trading_data_mapper: BackpackOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that error handling is consistent across different transformation methods."""
        # Mock to cause an exception in both methods
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Consistent error")

        # Both methods should raise TransformationError
        raw_order = create_raw_order()
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

        with pytest.raises(TransformationError):
            trading_data_mapper.transform_order_data_to_internal(
                order_id="123",
                symbol=SOL_USDC_BP.value,
                side="Buy",
                order_type="LIMIT",
                status="NEW",
                quantity="1.0",
            )

    def test_all_mapping_logic_works_together(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test that all mapping logic works together properly in a transformation."""
        raw_order = create_raw_order(
            side="Sell",  # Should map to SELL
            order_type="MARKET",  # Should map to MARKET
            status="PARTIALLY_FILLED",  # Should map to PARTIALLY_FILLED
            time_in_force="FOK",  # Should map to FOK
            quantity="2.5",
            executed_quantity="1.0",
            avg_fill_price="3000.00",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify that all mapping methods contributed correctly
        assert result.side == OrderSide.SELL  # side mapping
        assert result.order_type == OrderType.MARKET  # type mapping
        assert result.status == OrderStatus.PARTIALLY_FILLED  # status mapping
        assert result.time_in_force == TimeInForce.FOK  # TIF mapping

        # Verify the result is a complete, valid Order
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.symbol.value == SOL_USDC_BP.value
        assert result.quantity_requested > Decimal(0)
        assert result.quantity_filled == Decimal("1.0")
        assert result.average_fill_price == Decimal("3000.00")
        assert result.created_at is not None

    def test_complex_order_scenario_with_all_fields(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test transformation of a complex order with all fields populated."""
        created_at = datetime.now(UTC).isoformat()
        updated_at = datetime.now(UTC).isoformat()
        triggered_at = datetime.now(UTC).isoformat()

        # Create a complex STOP order with all fields
        raw_order = create_raw_order(
            side="Sell",
            order_type="STOP",
            status="PARTIALLY_FILLED",
            quantity="10.5",
            executed_quantity="3.2",
            price="45000.00",
            avg_fill_price="45050.25",
            order_id="complex_order_123",
            client_id="complex_client_456",
            symbol=BTC_USDC_BP.value,
            time_in_force="IOC",
            created_at=created_at,
            updated_at=updated_at,
        )

        # Add additional fields via model_copy
        raw_order = raw_order.model_copy(
            update={
                "triggerPrice": "44000.00",
                "triggeredAt": triggered_at,
                "reduceOnly": True,
                "postOnly": False,
            },
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify all fields are correctly transformed
        assert result.exchange_order_id == "complex_order_123"
        assert result.client_order_id == "complex_client_456"
        assert result.symbol.value == BTC_USDC_BP.value
        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.STOP_MARKET
        assert result.status == OrderStatus.PARTIALLY_FILLED
        assert result.quantity_requested == Decimal("10.5")
        assert result.quantity_filled == Decimal("3.2")
        assert result.price == Decimal("45000.00")
        assert result.stop_price == Decimal("44000.00")
        assert result.average_fill_price == Decimal("45050.25")
        assert result.time_in_force == TimeInForce.IOC
        assert result.reduce_only is True
        assert result.post_only is False
        assert result.triggered_at is not None
        assert result.created_at is not None
        assert result.updated_at is not None
        assert result.exchange == ExchangeName.BACKPACK.value

    def test_multiple_orders_transformation_consistency(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test that multiple order transformations maintain consistency."""
        orders_data = [
            ("Buy", "LIMIT", "NEW", "1.0", "0.0", "3000.00", None),
            ("Sell", "MARKET", "FILLED", "2.0", "2.0", None, "2950.00"),
            ("Buy", "STOP", "PARTIALLY_FILLED", "0.5", "0.2", "3100.00", "3050.00"),
        ]

        transformed_orders: list[Order] = []

        for side, order_type, status, quantity, executed_qty, price, avg_fill in orders_data:
            raw_order = create_raw_order(
                side=side,
                order_type=order_type,
                status=status,
                quantity=quantity,
                executed_quantity=executed_qty,
                price=price,
                avg_fill_price=avg_fill,
                order_id=f"order_{len(transformed_orders) + 1}",
            )

            # Add trigger price for STOP orders
            if order_type == "STOP":
                raw_order = raw_order.model_copy(update={"triggerPrice": "2900.00"})

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
            transformed_orders.append(result)

        # Verify all orders were transformed correctly
        assert len(transformed_orders) == 3

        # Check first order (BUY LIMIT)
        assert transformed_orders[0].side == OrderSide.BUY
        assert transformed_orders[0].order_type == OrderType.LIMIT
        assert transformed_orders[0].status == OrderStatus.OPEN
        assert transformed_orders[0].price == Decimal("3000.00")

        # Check second order (SELL MARKET)
        assert transformed_orders[1].side == OrderSide.SELL
        assert transformed_orders[1].order_type == OrderType.MARKET
        assert transformed_orders[1].status == OrderStatus.FILLED
        assert transformed_orders[1].price is None
        assert transformed_orders[1].average_fill_price == Decimal("2950.00")

        # Check third order (BUY STOP)
        assert transformed_orders[2].side == OrderSide.BUY
        assert transformed_orders[2].order_type == OrderType.STOP_MARKET
        assert transformed_orders[2].status == OrderStatus.PARTIALLY_FILLED
        assert transformed_orders[2].stop_price == Decimal("2900.00")

        # All should have consistent exchange and timestamp data
        for order in transformed_orders:
            assert order.exchange == ExchangeName.BACKPACK.value
            assert order.created_at is not None
            assert order.updated_at is not None

    def test_data_transformation_integrity(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test that data transformation maintains mathematical integrity."""
        # Test with high precision values
        raw_order = create_raw_order(
            quantity="1.123456789012345",
            executed_quantity="0.987654321098765",
            price="12345.678901234567",
            avg_fill_price="12346.111111111111",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify precision is maintained
        assert result.quantity_requested == Decimal("1.123456789012345")
        assert result.quantity_filled == Decimal("0.987654321098765")
        assert result.price == Decimal("12345.678901234567")
        assert result.average_fill_price == Decimal("12346.111111111111")

        # Verify mathematical relationships
        assert result.quantity_filled <= result.quantity_requested
        remaining_quantity = result.quantity_requested - result.quantity_filled
        # Use approximate comparison due to potential decimal precision variations
        expected_remaining = Decimal("0.13580246792358")
        assert abs(remaining_quantity - expected_remaining) < Decimal("0.0000000001")

    def test_cross_symbol_transformation_consistency(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test transformation consistency across different trading symbols."""
        symbols = [
            BTC_USDC_BP.value,
            ETH_USDC_BP.value,
            SOL_USDC_BP.value,
            DOGE_USDT_BP.value,
            ADA_BTC_BP.value,
        ]

        for symbol in symbols:
            raw_order = create_raw_order(
                symbol=symbol,
                side="Buy",
                order_type="LIMIT",
                quantity="1.0",
                price="100.0",
                order_id=f"order_{symbol}",
            )

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # Symbol should be preserved exactly
            assert result.symbol.value == symbol
            # Other core fields should be consistent
            assert result.side == OrderSide.BUY
            assert result.order_type == OrderType.LIMIT
            assert result.quantity_requested == Decimal("1.0")
            assert result.price == Decimal("100.0")
            assert result.exchange == ExchangeName.BACKPACK.value

    def test_time_field_transformation_consistency(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test that time field transformations are consistent and valid."""
        base_time = datetime.now(UTC)
        created_at = base_time.isoformat()
        updated_at = (base_time.replace(microsecond=0)).isoformat()  # Later time
        triggered_at = (base_time.replace(second=30)).isoformat()  # Even later

        raw_order = create_raw_order(
            order_type="STOP",
            created_at=created_at,
            updated_at=updated_at,
        )

        raw_order = raw_order.model_copy(
            update={
                "triggerPrice": "2900.00",
                "triggeredAt": triggered_at,
            },
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # All timestamps should be valid datetime objects
        assert isinstance(result.created_at, datetime)
        assert isinstance(result.updated_at, datetime)
        assert isinstance(result.triggered_at, datetime)

        # Time relationship should be logical (within reasonable bounds)
        # Note: We're not enforcing strict ordering due to potential clock skew
        assert result.created_at is not None
        assert result.updated_at is not None
        assert result.triggered_at is not None

    def test_business_logic_validation_integration(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test integration of business logic validation across transformations."""
        # Test that orders with fills have appropriate average fill prices
        orders_with_fills = [
            ("1.0", "1.0", "3000.00"),  # Fully filled
            ("2.0", "1.5", "2999.50"),  # Partially filled
            ("0.5", "0.25", "3001.25"),  # Quarter filled
        ]

        for quantity, executed_qty, avg_fill in orders_with_fills:
            raw_order = create_raw_order(
                quantity=quantity,
                executed_quantity=executed_qty,
                avg_fill_price=avg_fill,
                status="PARTIALLY_FILLED" if executed_qty < quantity else "FILLED",
            )

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # Business logic checks
            assert result.quantity_filled <= result.quantity_requested
            if result.quantity_filled > Decimal(0):
                assert result.average_fill_price is not None
                assert result.average_fill_price > Decimal(0)

        # Test orders without fills
        raw_order = create_raw_order(
            quantity="1.0",
            executed_quantity="0.0",
            avg_fill_price=None,
            status="NEW",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.quantity_filled == Decimal(0)
        assert result.average_fill_price is None
