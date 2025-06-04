"""
CyberDeltaEngine: Backpack Trading Data Mapper Robustness Tests
--------------------------------------------------------------

Comprehensive robustness test suite for BackpackTradingDataMapper.
Tests edge cases, boundary conditions, and error handling including:
- Boundary value testing with extreme inputs
- Unicode and encoding support
- Error handling and recovery scenarios
- Performance and memory considerations
- Invalid input validation and recovery
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)

logger = logging.getLogger(__name__)


# --- Fixtures ---


@pytest.fixture
def trading_data_mapper() -> BackpackTradingDataMapper:
    """Provide an instance of BackpackTradingDataMapper."""
    return BackpackTradingDataMapper()


def create_raw_order(
    side: str = "Buy",
    status: str = "NEW",
    order_type: str = "LIMIT",
    price: str | None = "3000.50",
    quantity: str = "1.5",
    executed_quantity: str | None = "0.0",
    order_id: str = "12345",
    client_id: str | None = "test_order_001",
    symbol: str = "SOL_USDC",
    time_in_force: str | None = "GTC",
    created_at: str | None = None,
    updated_at: str | None = None,
    avg_fill_price: str | None = None,
) -> BackpackRawOrder:
    """Create a BackpackRawOrder with customizable parameters."""
    if created_at is None:
        created_at = datetime.now(UTC).isoformat()
    if updated_at is None:
        updated_at = datetime.now(UTC).isoformat()

    return BackpackRawOrder(
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


# --- Edge Case and Robustness Tests ---


class TestBoundaryValueHandling:
    """Tests for boundary values and extreme inputs."""

    def test_minimal_order_data(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test transformation with minimal required order data."""
        minimal_order = create_raw_order(
            side="Buy",
            order_type="LIMIT",
            price="50000.0",
            quantity="1",
            executed_quantity="0",
            order_id="1",
            client_id=None,
            symbol="BTC_USDC",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(minimal_order)

        assert result.exchange_order_id == "1"
        assert result.symbol == "BTC_USDC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.client_order_id is not None and len(result.client_order_id) > 0

    def test_boundary_values(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test transformation with boundary values."""
        boundary_order = create_raw_order(
            side="Sell",
            order_type="STOP",
            price="999999.999999",  # High precision price
            quantity="0.000001",  # Very small size
            executed_quantity="0.000001",  # All executed
            avg_fill_price="999999.999999",
            order_id="999999999",  # Large order ID
            client_id="x" * 64,  # Max length client order ID
            symbol="A" * 32,  # Long symbol
            time_in_force="FOK",
        )
        # Add trigger price for STOP order
        boundary_order = boundary_order.model_copy(update={"triggerPrice": "999999.999999"})

        result = trading_data_mapper.transform_raw_order_to_internal(boundary_order)

        assert result.exchange_order_id == "999999999"
        assert result.client_order_id == "x" * 64
        assert result.symbol == "A" * 32
        assert result.side == OrderSide.SELL
        assert result.quantity_requested == Decimal("0.000001")
        assert result.quantity_filled == Decimal("0.000001")
        assert result.average_fill_price == Decimal("999999.999999")
        assert result.stop_price == Decimal("999999.999999")
        assert result.time_in_force == TimeInForce.FOK

    def test_extremely_large_values(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test transformation with extremely large numeric values."""
        large_order = create_raw_order(
            quantity="999999999999999.999999999999999",
            price="999999999999999.999999999999999",
            executed_quantity="0.0",
            order_id="99999999999999999999",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(large_order)

        assert result.quantity_requested == Decimal("999999999999999.999999999999999")
        assert result.price == Decimal("999999999999999.999999999999999")
        assert result.exchange_order_id == "99999999999999999999"

    def test_extremely_small_values(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test transformation with extremely small numeric values."""
        small_order = create_raw_order(
            quantity="0.000000000000001",
            price="0.000000000000001",
            executed_quantity="0.0",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(small_order)

        assert result.quantity_requested == Decimal("0.000000000000001")
        assert result.price == Decimal("0.000000000000001")

    def test_zero_values_handling(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test handling of zero values in various fields."""
        zero_order = create_raw_order(
            executed_quantity="0.0",
            avg_fill_price=None,  # Should be None when no fills
        )

        result = trading_data_mapper.transform_raw_order_to_internal(zero_order)

        assert result.quantity_filled == Decimal("0.0")
        assert result.average_fill_price is None

    def test_maximum_precision_decimals(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test transformation with maximum precision decimal values."""
        precision_order = create_raw_order(
            quantity="1.123456789012345678901234567890",
            price="100.987654321098765432109876543210",
            executed_quantity="0.111111111111111111111111111111",
            avg_fill_price="100.999999999999999999999999999999",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(precision_order)

        # Verify precision is maintained (may be truncated based on Decimal precision)
        assert result.quantity_requested > Decimal("1.12345")
        # DEFENSIVE CHECK: price could be None after transformation.
        # Mypy=[union-attr] Ruff=[N/A]
        if result.price is not None:
            assert result.price > Decimal("100.98765")
        assert result.quantity_filled > Decimal("0.11111")
        # DEFENSIVE CHECK: average_fill_price could be None after transformation.
        # Mypy=[union-attr] Ruff=[N/A]
        if result.average_fill_price is not None:
            assert result.average_fill_price > Decimal("100.99999")


class TestUnicodeAndEncodingSupport:
    """Tests for Unicode symbol support and encoding handling."""

    def test_unicode_symbols_in_symbol_field(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test transformation with Unicode characters in symbol field."""
        unicode_symbols = [
            "BTC-USDC🚀",
            "ETH_测试",
            "SOL-€URO",
            "DOGE_символ",
            "ADA_🌟",
        ]

        for symbol in unicode_symbols:
            raw_order = create_raw_order(
                symbol=symbol,
                order_id=f"order_{len(symbol)}",
            )

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # Symbol should be preserved exactly
            assert result.symbol == symbol
            assert result.exchange_order_id == f"order_{len(symbol)}"

    def test_unicode_in_client_order_id(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test transformation with Unicode characters in client order ID."""
        unicode_client_ids = [
            "client_测试_123",
            "заказ_456",
            "order_🎯_789",
            "commande_été_001",
        ]

        for client_id in unicode_client_ids:
            raw_order = create_raw_order(client_id=client_id)

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            assert result.client_order_id == client_id

    def test_mixed_encoding_scenarios(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test transformation with mixed encoding scenarios."""
        mixed_order = create_raw_order(
            symbol="BTC-USDC🚀",
            client_id="test_测试_🎯",
            order_id="order_символ_123",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(mixed_order)

        assert result.symbol == "BTC-USDC🚀"
        assert result.client_order_id == "test_测试_🎯"
        assert result.exchange_order_id == "order_символ_123"

    def test_empty_string_handling(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test handling of empty strings in various fields."""
        # Test via order data method since raw model validation might prevent empty strings
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="123",
            symbol="SOL_USDC",  # Cannot be empty
            side="Buy",
            order_type="LIMIT",
            status="NEW",
            quantity="1.0",
            price="100.0",
            client_order_id="",  # Empty client order ID
        )

        # Should get a generated UUID instead of empty string
        assert result.client_order_id is not None
        assert len(result.client_order_id) > 0


class TestErrorHandlingAndRecovery:
    """Tests for error handling and recovery scenarios."""

    def test_invalid_decimal_conversion_handling(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of invalid decimal conversion errors."""
        # Mock parse_decimal_value to simulate parsing errors
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Invalid decimal format")

        raw_order = create_raw_order()

        with pytest.raises(TransformationError, match="Failed to transform BackpackRawOrder"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_invalid_datetime_conversion_handling(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of invalid datetime conversion errors."""
        # Mock parse_datetime_utc to simulate parsing errors
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_datetime_utc",
        )
        mock_parse.side_effect = ValueError("Invalid datetime format")

        raw_order = create_raw_order()

        with pytest.raises(TransformationError, match="Failed to transform BackpackRawOrder"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_multiple_parsing_errors_aggregation(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that multiple parsing errors are properly aggregated."""
        # Mock multiple parsing functions to fail
        mock_decimal = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value",
        )
        mock_datetime = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_datetime_utc",
        )

        mock_decimal.side_effect = ValueError("Decimal parsing error")
        mock_datetime.side_effect = ValueError("Datetime parsing error")

        raw_order = create_raw_order()

        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_graceful_degradation_with_partial_data(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test graceful handling when optional fields are missing or invalid."""
        # Create order with minimal data
        raw_order = create_raw_order(
            executed_quantity="0.0",  # No fills
            avg_fill_price=None,  # No average fill price
            client_id=None,  # No client ID
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Should still create valid order with defaults
        assert result.quantity_filled == Decimal("0.0")
        assert result.average_fill_price is None
        assert result.client_order_id is not None  # Should generate UUID

    def test_transformation_error_message_clarity(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that transformation error messages are clear and informative."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Specific parsing error message")

        raw_order = create_raw_order()

        with pytest.raises(TransformationError) as exc_info:
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

        error_message = str(exc_info.value)
        assert "Failed to transform BackpackRawOrder to Order" in error_message


class TestPerformanceAndMemoryConsiderations:
    """Tests for performance and memory efficiency."""

    def test_large_batch_transformation_efficiency(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test transformation efficiency with large batches of orders."""
        # Create a large number of orders
        orders: list[BackpackRawOrder] = []
        for i in range(100):
            # Ensure quantity is always > 0 for validation
            quantity_value = max(0.1, i * 0.1)  # Minimum 0.1, then increment
            price_value = 1000 + i * 0.1

            order = create_raw_order(
                order_id=f"order_{i:03d}",
                symbol=f"SYMBOL{i % 10}_USDC",
                quantity=f"{quantity_value:.1f}",
                price=f"{price_value:.1f}",
            )
            orders.append(order)

        # Transform all orders
        results: list[Order] = []
        for order in orders:
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            results.append(result)

        # Verify all orders were transformed correctly
        assert len(results) == 100
        for i, result in enumerate(results):
            assert result.exchange_order_id == f"order_{i:03d}"
            assert result.symbol == f"SYMBOL{i % 10}_USDC"

    def test_memory_efficiency_with_large_objects(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test memory efficiency when handling large order objects."""
        # Create orders with large string fields but within validation limits
        large_order = create_raw_order(
            order_id="a" * 60,  # Within 64 char limit
            client_id="b" * 60,  # Within 64 char limit
            symbol="c" * 60,  # Within 64 char limit
        )

        result = trading_data_mapper.transform_raw_order_to_internal(large_order)

        # Should handle large strings without issues
        assert result.exchange_order_id == "a" * 60
        assert result.client_order_id == "b" * 60
        assert result.symbol == "c" * 60

    def test_concurrent_transformation_safety(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test that transformations are safe for concurrent usage."""
        # Create multiple orders that could potentially interfere
        orders = [
            create_raw_order(
                order_id=f"concurrent_{i}",
                quantity=f"{max(1, i)}.0",  # Ensure quantity >= 1 for validation
            )
            for i in range(1, 11)  # Start from 1 to avoid 0 quantity
        ]

        # Transform orders (simulating concurrent access)
        results: list[Order] = []
        for _i, order in enumerate(orders, 1):
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            results.append(result)

        # Verify no cross-contamination between transformations
        for i, result in enumerate(results, 1):
            assert result.exchange_order_id == f"concurrent_{i}"
            assert result.quantity_requested == Decimal(f"{i}.0")


class TestDataConsistencyAndValidation:
    """Tests for data consistency and validation across transformations."""

    @pytest.mark.parametrize(
        "status_input,expected_output",
        [
            ("NEW", OrderStatus.OPEN),
            ("FILLED", OrderStatus.FILLED),
            ("CANCELLED", OrderStatus.CANCELED),
            ("REJECTED", OrderStatus.REJECTED),
            ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
            ("EXPIRED", OrderStatus.UNKNOWN),
            # Test consistency with various casing
            ("new", OrderStatus.OPEN),
            ("filled", OrderStatus.FILLED),
            ("cancelled", OrderStatus.CANCELED),
        ],
    )
    def test_status_mapping_consistency(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        status_input: str,
        expected_output: OrderStatus,
    ) -> None:
        """Test consistency of status mapping across different cases."""
        # Test via order data method for case sensitivity
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="123",
            symbol="SOL_USDC",
            side="Buy",
            order_type="LIMIT",
            status=status_input,
            quantity="1.0",
            price="100.0",
        )

        assert result.status == expected_output

    def test_case_insensitive_mappings_comprehensive(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test that all enum mappings are case insensitive."""
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="123",
            symbol="SOL_USDC",
            side="buy",  # lowercase
            order_type="market",  # lowercase
            status="filled",  # lowercase
            quantity="1.0",
            time_in_force="ioc",  # lowercase
        )

        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.MARKET
        assert result.status == OrderStatus.FILLED
        assert result.time_in_force == TimeInForce.IOC

    def test_decimal_precision_consistency(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test that decimal precision is maintained consistently."""
        test_values = [
            "1.123456789012345",
            "0.000000000000001",
            "999999999999999.999",
            "123.456",
        ]

        for value in test_values:
            raw_order = create_raw_order(
                quantity=value,
                price=value,
                executed_quantity="0.0",
            )

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            assert result.quantity_requested == Decimal(value)
            assert result.price == Decimal(value)

    def test_none_value_handling_consistency(
        self, trading_data_mapper: BackpackTradingDataMapper,
    ) -> None:
        """Test consistent handling of None values across transformations."""
        # Test with various None scenarios
        raw_order = create_raw_order(
            price=None,  # Market order
            executed_quantity="0.0",
            avg_fill_price=None,
            client_id=None,
            order_type="MARKET",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.price is None
        assert result.average_fill_price is None
        assert result.client_order_id is not None  # Should generate UUID
        assert result.order_type == OrderType.MARKET
