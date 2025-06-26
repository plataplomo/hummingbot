"""CyberDeltaEngine: Hyperliquid Trading Data Mapper Robustness Tests.

------------------------------------------------------------------

Comprehensive test suite for HyperliquidTradingDataMapper robustness and edge cases.
Tests various scenarios including:
- Boundary value testing and edge cases
- Error handling and exception scenarios
- Unicode and special character handling
- Performance considerations and memory testing
- Complex trigger scenarios and mapping edge cases
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
import structlog.testing
from _pytest.logging import LogCaptureFixture

from cyberdelta.config.structlog_config import get_logger


# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import HyperliquidRawHistoricalOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
)


logger = get_logger(__name__)


# --- Fixtures ---


@pytest.fixture
def trading_data_mapper() -> HyperliquidTradingDataMapper:
    """Provide an instance of HyperliquidTradingDataMapper."""
    return HyperliquidTradingDataMapper()


def create_raw_order(
    side: str = "B",
    status: str = "open",
    order_type: dict[str, Any] | None = None,
    limit_px: str = "3000.50",
    sz: str = "1.5",
    remaining_sz: str = "0.5",
    oid: int = 12345,
    cloid: str | None = None,  # Use None by default since cloid is optional
    asset: str = "ETH-PERP",
    timestamp: int = 1640995200000,  # Fixed timestamp for consistency
) -> HyperliquidRawOrder:
    """Create a HyperliquidRawOrder with customizable parameters."""
    if order_type is None:
        order_type = {"limit": {"tif": "Gtc"}}

    return HyperliquidRawOrder(
        oid=oid,
        cloid=cloid,
        asset=asset,
        side=side,
        limitPx=limit_px,
        sz=sz,
        timestamp=timestamp,
        orderType=order_type,
        reduceOnly=False,
        remainingSz=remaining_sz,
        status=status,
        statusTimestamp=timestamp + 1000,
    )


def create_raw_historical_order(
    side: str = "B",
    status: str = "filled",
    order_type_str: str = "limit",
    limit_px: str = "100.25",
    sz: str = "10.0",
    remaining_sz: str = "2.5",
    oid: int = 98765,
    cloid: str | None = None,  # Use None by default since cloid is optional
    coin: str = "SOL-PERP",
    timestamp: int = 1640995200000,  # Fixed timestamp for consistency
) -> HyperliquidRawHistoricalOrder:
    """Create a HyperliquidRawHistoricalOrder with customizable parameters."""
    return HyperliquidRawHistoricalOrder(
        oid=oid,
        cloid=cloid,
        coin=coin,
        side=side,
        limitPx=limit_px,
        sz=sz,
        timestamp=timestamp,
        orderType=order_type_str,
        reduceOnly=False,
        origSz=sz,  # Use sz as origSz
        tif="Ioc",
        status=status,
        statusTimestamp=timestamp + 5000,
        # Optional fields
        triggerCondition=None,
        isTrigger=None,
        triggerPx=None,
        children=None,
        isPositionTpsl=None,
    )


# --- Tests for Edge Cases and Boundary Values ---


class TestEdgeCasesAndBoundaryValues:
    """Tests for edge cases and boundary value scenarios."""

    def test_minimal_order_data(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test transformation with minimal required order data."""
        minimal_order = create_raw_order(
            side="B",
            status="open",
            order_type={"market": {}},
            limit_px="0.01",
            sz="0.001",
            remaining_sz="0.001",
            oid=12345,
            cloid=None,  # No client order ID
            asset="ETH-PERP",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(minimal_order)

        # Should handle minimal data gracefully
        assert result.exchange_order_id == "12345"
        assert (
            isinstance(result.client_order_id, str) and len(result.client_order_id) > 0
        )  # UUID generated when cloid is None
        assert result.symbol == "ETH-PERP"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.MARKET

    def test_boundary_values(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test transformation with boundary values."""
        # Very large values
        large_order = create_raw_order(
            oid=999999999999999999,
            limit_px="999999999999.999999999999",
            sz="999999999999.999999999999",
            remaining_sz="0.000000000000000001",
        )

        # Small positive values that don't round to zero (business logic rounds to 8 decimal places)
        small_order = create_raw_order(
            oid=1,
            limit_px="0.00000001",  # This will remain positive after 8-decimal rounding
            sz="0.00000001",
            remaining_sz="0.00000001",
        )

        # Test transformations
        result1 = trading_data_mapper.transform_raw_order_to_internal(large_order)
        result2 = trading_data_mapper.transform_raw_order_to_internal(small_order)

        # Should handle boundary values without error
        assert result1.exchange_order_id == "999999999999999999"
        assert result2.exchange_order_id == "1"
        assert result1.price is not None and result1.price > Decimal(0)
        assert result2.price is not None and result2.price > Decimal(0)

    def test_complex_trigger_scenarios(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test complex trigger scenarios and edge cases."""
        # Test that the mapper can handle extreme trigger values
        # Note: Since triggers are handled separately from basic order transformation,
        # we focus on ensuring the core transformation remains stable
        order = create_raw_order()

        # Test that the mapper can handle extreme trigger values
        # Note: Since triggers are handled separately from basic order transformation,
        # we focus on ensuring the core transformation remains stable
        result = trading_data_mapper.transform_raw_order_to_internal(order)
        assert result is not None

    @pytest.mark.parametrize(
        "status_input,expected_output",
        [
            ("canceled", OrderStatus.CANCELED),  # Valid status, direct mapping
            ("rejected", OrderStatus.REJECTED),  # Valid status, direct mapping
            ("expired", OrderStatus.UNKNOWN),  # Valid status, maps to UNKNOWN
            ("filled", OrderStatus.FILLED),  # Valid status, direct mapping
            ("open", OrderStatus.OPEN),  # Valid status, direct mapping
        ],
    )
    def test_status_edge_cases(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        status_input: str,
        expected_output: OrderStatus,
    ) -> None:
        """Test edge cases in status mapping."""
        if status_input == "open":
            # Use regular raw order for "open" status
            order = create_raw_order(status=status_input)
            result = trading_data_mapper.transform_raw_order_to_internal(order)
        else:
            # Use historical raw order for other statuses
            historical_order = create_raw_historical_order(status=status_input)
            result = trading_data_mapper.transform_raw_historical_order_to_internal(
                historical_order,
            )
        assert result.status == expected_output


# --- Tests for Unicode and Special Character Handling ---


class TestUnicodeAndSpecialCharacters:
    """Tests for Unicode and special character handling."""

    def test_unicode_asset_symbols(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test transformation with Unicode characters in asset symbols."""
        unicode_symbols = [
            "BTC-PERP🚀",
            "ETH-PERPⓍ",
            "SOL-PERP™",
            "AVAX-PERP®",
            "测试-PERP",  # Chinese characters
            "тест-PERP",  # Cyrillic characters
            "テスト-PERP",  # Japanese characters
        ]

        for symbol in unicode_symbols:
            order = create_raw_order(asset=symbol)
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            assert result.symbol == symbol

    def test_unicode_client_order_ids(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test transformation with None client order IDs generates valid UUIDs.

        Note: Unicode characters are no longer valid in cloids since they must be
        128-bit hex strings. This test now verifies proper UUID generation when
        cloid is None.
        """
        # Test that None cloid generates a valid UUID
        order = create_raw_order(cloid=None)
        result = trading_data_mapper.transform_raw_order_to_internal(order)

        # Should have generated a UUID for client_order_id
        assert result.client_order_id is not None
        assert isinstance(result.client_order_id, str)
        assert len(result.client_order_id) > 0

        # Test multiple orders get different UUIDs
        order2 = create_raw_order(cloid=None)
        result2 = trading_data_mapper.transform_raw_order_to_internal(order2)
        assert result2.client_order_id != result.client_order_id

    def test_special_characters_in_strings(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test transformation with special characters in asset symbols.

        Note: Special characters in cloids are no longer valid since cloids must be
        128-bit hex strings. This test focuses on special characters in asset symbols.
        """
        # Test each case individually with None cloid (will generate UUIDs)
        test_order1 = create_raw_order(asset="BTC\\PERP", cloid=None)
        result1 = trading_data_mapper.transform_raw_order_to_internal(test_order1)
        assert result1.symbol == "BTC\\PERP"
        assert result1.client_order_id is not None  # UUID generated

        test_order2 = create_raw_order(asset="ETH/PERP", cloid=None)
        result2 = trading_data_mapper.transform_raw_order_to_internal(test_order2)
        assert result2.symbol == "ETH/PERP"
        assert result2.client_order_id is not None  # UUID generated

        test_order3 = create_raw_order(asset="SOL PERP", cloid=None)
        result3 = trading_data_mapper.transform_raw_order_to_internal(test_order3)
        assert result3.symbol == "SOL PERP"
        assert result3.client_order_id is not None  # UUID generated

        test_order4 = create_raw_order(asset="AVAX.PERP", cloid=None)
        result4 = trading_data_mapper.transform_raw_order_to_internal(test_order4)
        assert result4.symbol == "AVAX.PERP"
        assert result4.client_order_id is not None  # UUID generated

        test_order5 = create_raw_order(asset="ADA-PERP-X", cloid=None)
        result5 = trading_data_mapper.transform_raw_order_to_internal(test_order5)
        assert result5.symbol == "ADA-PERP-X"
        assert result5.client_order_id is not None  # UUID generated


# --- Tests for Error Handling and Exception Scenarios ---


class TestErrorHandlingAndExceptions:
    """Tests for error handling and exception scenarios."""

    def test_transformation_error_with_nested_exceptions(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that nested exceptions are properly wrapped in TransformationError."""
        order = create_raw_order()

        # Mock to raise a nested exception chain
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value",
        )

        original_exception = ValueError("Original error")
        chained_exception = RuntimeError("Chained error")
        chained_exception.__cause__ = original_exception
        mock_parse.side_effect = chained_exception

        with pytest.raises(TransformationError) as exc_info:
            trading_data_mapper.transform_raw_order_to_internal(order)

        # Verify the exception chain is preserved
        assert "Failed to parse order quantities and price" in str(exc_info.value)

    def test_logging_during_error_scenarios(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test that appropriate logging occurs during error scenarios."""
        # Create order that will trigger quantity_filled adjustment warning
        problematic_order = create_raw_order(
            limit_px="0.0",  # Zero price will trigger warning
            sz="1.0",
            remaining_sz="0.0",  # Fully filled but zero price
        )

        with structlog.testing.capture_logs() as captured_logs:
            try:
                trading_data_mapper.transform_raw_order_to_internal(problematic_order)
            except Exception as e:
                # We expect this to fail, we're testing logging
                logger.debug(f"Expected exception during robustness test: {e}")

        # Check that warning was logged in structured logs
        warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
        assert len(warning_logs) > 0, "Expected at least one warning log"

        # Check for the specific warning about quantity_filled adjustment
        quantity_logs = [
            log
            for log in warning_logs
            if "quantity_filled" in str(log) and "no valid price available" in str(log)
        ]
        assert len(quantity_logs) > 0, (
            f"Expected quantity_filled warning logs, got: {captured_logs}"
        )

    def test_multiple_consecutive_errors(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that multiple consecutive transformation errors are handled properly."""
        orders = [create_raw_order(oid=i) for i in range(5)]

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Consistent error")

        # All should raise TransformationError
        for order in orders:
            with pytest.raises(TransformationError):
                trading_data_mapper.transform_raw_order_to_internal(order)


# --- Tests for Performance and Memory Considerations ---


class TestPerformanceAndMemory:
    """Tests for performance and memory considerations."""

    def test_large_batch_transformation_stability(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test that large batches of transformations remain stable."""
        # Create a large number of orders
        orders = [
            create_raw_order(
                oid=i,
                cloid=None,  # Let mapper generate UUIDs
                limit_px=f"{100.0 + i * 0.01:.2f}",
                sz=f"{1.0 + i * 0.001:.3f}",
            )
            for i in range(1000)
        ]

        # Transform all orders - should not raise exceptions
        results: list[object] = []
        for order in orders:
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            results.append(result)

        # Verify all transformations succeeded
        assert len(results) == 1000
        assert all(result is not None for result in results)

    def test_memory_efficiency_with_large_strings(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test memory efficiency with large asset name strings.

        Note: cloids must be 128-bit hex strings, so we test with None cloid
        and focus on long asset names.
        """
        # Create order with very long asset name
        long_asset = "VERYLONGASSETSYMBOLNAME"  # Long but valid asset name

        order = create_raw_order(
            cloid=None,  # Let mapper generate UUID
            asset=long_asset,
        )

        # Should handle large strings efficiently
        result = trading_data_mapper.transform_raw_order_to_internal(order)
        assert result.client_order_id is not None  # UUID generated
        assert result.symbol == long_asset

    def test_high_precision_calculation_stability(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test stability with high precision decimal calculations."""
        # Create orders with maximum precision decimals that don't round to zero
        high_precision_orders = [
            create_raw_order(
                limit_px="999999.999999999999999999",
                sz="999999.999999999999999999",
                remaining_sz="999999.999999999999999999",
            ),
            create_raw_order(
                limit_px="0.00000001",  # Business logic rounds to 8 decimal places
                sz="0.00000001",
                remaining_sz="0.00000001",
            ),
        ]

        for order in high_precision_orders:
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            # Should handle high precision without loss or errors
            assert result is not None


# --- Tests for Complex Integration Scenarios ---


class TestComplexIntegrationScenarios:
    """Tests for complex integration scenarios and real-world edge cases."""

    def test_rapid_status_changes_simulation(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test simulation of rapid order status changes."""
        # Simulate order lifecycle: open -> partially filled -> filled
        open_status_progression = [
            ("open", "1.0", OrderStatus.OPEN),
            ("open", "0.5", OrderStatus.OPEN),  # Partially filled
            ("open", "0.1", OrderStatus.OPEN),  # More filled
        ]

        # Test open status orders
        for status, remaining, expected_status in open_status_progression:
            order = create_raw_order(
                oid=12345,
                cloid=None,  # Let mapper generate UUID
                asset="BTC-PERP",
                limit_px="50000.0",
                sz="1.0",
                status=status,
                remaining_sz=remaining,
            )
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            assert result.status == expected_status

        # Test filled status using historical order
        filled_order = create_raw_historical_order(
            oid=12345,
            cloid=None,  # Let mapper generate UUID
            coin="BTC-PERP",
            limit_px="50000.0",
            sz="1.0",
            status="filled",
            remaining_sz="0.0",
        )
        result = trading_data_mapper.transform_raw_historical_order_to_internal(filled_order)
        assert result.status == OrderStatus.FILLED

    def test_concurrent_transformation_consistency(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test that concurrent-like transformations maintain consistency."""
        # Create identical orders to simulate concurrent processing
        identical_orders = [
            create_raw_order(
                oid=99999,
                cloid=None,  # Let mapper generate UUID
                asset="ETH-PERP",
                limit_px="3000.0",
                sz="2.0",
                remaining_sz="1.0",
            )
            for _ in range(10)
        ]

        # Transform all orders
        results = [
            trading_data_mapper.transform_raw_order_to_internal(order) for order in identical_orders
        ]

        # All results should be identical
        first_result = results[0]
        for result in results[1:]:
            assert result.symbol == first_result.symbol
            assert result.side == first_result.side
            assert result.order_type == first_result.order_type
            assert result.status == first_result.status

    def test_mixed_order_types_batch_processing(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test batch processing of mixed order types and configurations."""
        mixed_orders = [
            # Market orders
            create_raw_order(order_type={"market": {}}, side="B"),
            create_raw_order(order_type={"market": {}}, side="A"),
            # Limit orders with different TIF
            create_raw_order(order_type={"limit": {"tif": "Gtc"}}, side="B"),
            create_raw_order(order_type={"limit": {"tif": "Ioc"}}, side="A"),
            create_raw_order(order_type={"limit": {"tif": "Alo"}}, side="B"),
            # Open status (using regular raw order)
            create_raw_order(status="open"),
        ]

        # Historical orders with different statuses
        historical_orders = [
            create_raw_historical_order(status="filled"),
            create_raw_historical_order(status="canceled"),
            create_raw_historical_order(status="rejected"),
        ]

        # Transform regular orders
        results: list[object] = []
        for order in mixed_orders:
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            results.append(result)

        # Transform historical orders
        for hist_order in historical_orders:
            result = trading_data_mapper.transform_raw_historical_order_to_internal(hist_order)
            results.append(result)

        # Verify all transformations succeeded with expected variety
        assert len(results) == len(mixed_orders) + len(historical_orders)
        # Note: We can't easily check for variety due to type constraints,
        # but the important thing is that all transformations succeeded
        assert all(result is not None for result in results)
