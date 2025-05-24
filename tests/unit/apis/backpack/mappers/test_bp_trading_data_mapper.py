"""
Unit tests for the Backpack Trading Data Mapper.
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
from cyberdelta.apis.exchange_names import ExchangeName
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


@pytest.fixture
def base_timestamp() -> str:
    """Provide a consistent timestamp string for tests."""
    return datetime.now(UTC).isoformat()


def create_raw_order(
    side: str = "Buy",  # Valid value from BP_EXTENDED_ORDER_SIDES
    status: str = "NEW",  # Valid value from BP_ORDER_STATUSES
    order_type: str = "LIMIT",  # Valid value from BP_ORDER_TYPES
    price: str | None = "3000.50",
    quantity: str = "1.5",
    executed_quantity: str
    | None = "0.0",  # Changed from "0.5" to "0.0" to avoid business logic validation
    order_id: str = "12345",
    client_id: str | None = "test_order_001",
    symbol: str = "SOL_USDC",
    time_in_force: str | None = "GTC",
    created_at: str | None = None,
    updated_at: str | None = None,
    avg_fill_price: str | None = None,  # Add parameter for average fill price
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
        avgFillPrice=avg_fill_price,  # Use the parameter
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


# --- Parameterized Tests for Order Side Mapping ---


@pytest.mark.parametrize(
    "bp_side,expected_side",
    [
        ("Buy", OrderSide.BUY),
        ("Sell", OrderSide.SELL),
        ("buy", OrderSide.BUY),  # Case insensitive
        ("sell", OrderSide.SELL),
        ("Bid", OrderSide.BUY),  # Changed from "BUY" to "Bid" (valid in BP_EXTENDED_ORDER_SIDES)
        ("Ask", OrderSide.SELL),  # Changed from "SELL" to "Ask" (valid in BP_EXTENDED_ORDER_SIDES)
    ],
)
class TestOrderSideMapping:
    """Tests for order side mapping through public transformation methods."""

    def test_raw_order_side_mapping(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        bp_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via raw order transformation."""
        raw_order = create_raw_order(side=bp_side)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.side == expected_side

    def test_order_data_side_mapping(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        bp_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via order data transformation."""
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="12345",
            symbol="SOL_USDC",
            side=bp_side,
            order_type="LIMIT",
            status="NEW",
            quantity="1.0",
            price="100.0",  # Add required price for LIMIT orders
        )
        assert result.side == expected_side


@pytest.mark.parametrize(
    "invalid_side",
    ["X", "", "b", "s", "invalid", "Long", "Short"],
)
def test_invalid_order_side_raises_error(
    trading_data_mapper: BackpackTradingDataMapper,
    invalid_side: str,
) -> None:
    """Test that invalid order sides raise TransformationError."""
    with pytest.raises(TransformationError, match="Unknown Backpack order side"):
        trading_data_mapper.transform_order_data_to_internal(
            order_id="12345",
            symbol="SOL_USDC",
            side=invalid_side,
            order_type="LIMIT",
            status="NEW",
            quantity="1.0",
            price="100.0",  # Add required price for LIMIT orders
        )


# --- Parameterized Tests for Order Status Mapping ---


@pytest.mark.parametrize(
    "bp_status,expected_status",
    [
        ("NEW", OrderStatus.OPEN),
        ("FILLED", OrderStatus.FILLED),
        ("CANCELLED", OrderStatus.CANCELED),
        ("REJECTED", OrderStatus.REJECTED),
        ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
        (
            "EXPIRED",
            OrderStatus.UNKNOWN,
        ),  # Changed from "UNKNOWN_STATUS" to "EXPIRED" (valid in BP_ORDER_STATUSES)
    ],
)
def test_order_status_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
    bp_status: str,
    expected_status: OrderStatus,
) -> None:
    """Test order status mapping with valid and invalid statuses."""
    raw_order = create_raw_order(status=bp_status)
    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
    assert result.status == expected_status


# --- Parameterized Tests for Order Type Mapping ---


@pytest.mark.parametrize(
    "bp_type,expected_type",
    [
        ("LIMIT", OrderType.LIMIT),
        ("MARKET", OrderType.MARKET),
        (
            "TAKE_PROFIT",
            OrderType.LIMIT,
        ),  # Changed from "TRAILING_STOP" to "TAKE_PROFIT" to avoid stop_price requirement
    ],
)
def test_order_type_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
    bp_type: str,
    expected_type: OrderType,
) -> None:
    """Test order type mapping with various types."""
    # Create order with appropriate parameters based on type
    if bp_type in ["LIMIT", "TAKE_PROFIT"]:
        # LIMIT orders require price
        raw_order = create_raw_order(order_type=bp_type, price="3000.50")
    else:
        # MARKET orders don't require price
        raw_order = create_raw_order(order_type=bp_type, price=None)

    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
    assert result.order_type == expected_type


@pytest.mark.parametrize(
    "bp_type,expected_type",
    [
        ("STOP", OrderType.STOP_MARKET),
        ("TRAILING_STOP", OrderType.STOP_MARKET),
    ],
)
def test_stop_order_type_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
    bp_type: str,
    expected_type: OrderType,
) -> None:
    """Test STOP order type mapping with required stop_price."""
    raw_order = create_raw_order(order_type=bp_type, price="3000.50")
    raw_order = raw_order.model_copy(update={"triggerPrice": "2900.00"})

    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
    assert result.order_type == expected_type
    assert result.stop_price == Decimal("2900.00")


# --- Parameterized Tests for Time In Force Mapping ---


@pytest.mark.parametrize(
    "bp_tif,expected_tif",
    [
        ("GTC", TimeInForce.GTC),
        ("IOC", TimeInForce.IOC),
        ("FOK", TimeInForce.FOK),
        ("gtc", TimeInForce.GTC),  # Case insensitive
        ("ioc", TimeInForce.IOC),
        ("fok", TimeInForce.FOK),
        ("unknown", TimeInForce.GTC),  # Unknown defaults to GTC
        (None, TimeInForce.GTC),  # None defaults to GTC
    ],
)
def test_time_in_force_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
    bp_tif: str | None,
    expected_tif: TimeInForce,
) -> None:
    """Test time in force mapping through order transformation."""
    raw_order = create_raw_order(time_in_force=bp_tif)
    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
    assert result.time_in_force == expected_tif


# --- Comprehensive Transformation Tests ---


class TestTransformRawOrderToInternal:
    """Tests for the transform_raw_order_to_internal method."""

    def test_transform_raw_order_buy_limit_happy_path(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test successful transformation of a BUY limit order."""
        raw_order = create_raw_order(
            side="Buy",
            order_type="LIMIT",  # Changed from "Limit" to "LIMIT" (valid in BP_ORDER_TYPES)
            price="3000.50",
            quantity="1.5",
            executed_quantity="0.5",
            avg_fill_price="3001.00",  # Add average fill price to satisfy business logic validation
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "12345"
        assert result.symbol == "SOL_USDC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.quantity_requested == Decimal("1.5")
        assert result.quantity_filled == Decimal("0.5")
        assert result.price == Decimal("3000.50")
        assert result.average_fill_price == Decimal(
            "3001.00"
        )  # Add assertion for average fill price
        assert result.time_in_force == TimeInForce.GTC
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.client_order_id == "test_order_001"
        assert result.created_at is not None
        assert result.updated_at is not None
        assert result.reduce_only is False
        assert result.post_only is False

    def test_transform_raw_order_sell_market_happy_path(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test successful transformation of a SELL market order."""
        raw_order = create_raw_order(
            side="Sell",
            order_type="MARKET",
            price=None,  # Market orders may not have price
            quantity="0.1",
            executed_quantity="0.1",  # Fully executed
            avg_fill_price="50000.00",  # Add average fill price since quantity_filled > 0
            order_id="67890",
            client_id=None,
            symbol="BTC_USDC",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "67890"
        assert result.symbol == "BTC_USDC"
        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.MARKET
        assert result.status == OrderStatus.OPEN
        assert result.quantity_requested == Decimal("0.1")
        assert result.quantity_filled == Decimal("0.1")
        assert result.price is None  # Market orders should have price=None
        assert result.average_fill_price == Decimal(
            "50000.00"
        )  # Add assertion for average fill price
        assert result.time_in_force == TimeInForce.GTC
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.client_order_id is not None and len(result.client_order_id) > 0

    def test_transform_raw_order_with_stop_price(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with stop price."""
        raw_order = create_raw_order(
            order_type="STOP",
            price="3000.00",
        )
        # Add trigger price to the raw order
        raw_order = raw_order.model_copy(update={"triggerPrice": "2900.00"})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.order_type == OrderType.STOP_MARKET
        assert result.stop_price == Decimal("2900.00")

    def test_transform_raw_order_with_average_fill_price(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with average fill price."""
        raw_order = create_raw_order(
            executed_quantity="1.0",
        )
        # Add average fill price to the raw order
        raw_order = raw_order.model_copy(update={"avgFillPrice": "3001.25"})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.average_fill_price == Decimal("3001.25")

    def test_transform_raw_order_missing_quantity_raises_error(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing quantity raises TransformationError."""
        # Mock parse_decimal_value to return None for the quantity field
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.return_value = None

        raw_order = create_raw_order()
        with pytest.raises(TransformationError, match="quantity_requested is required"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_missing_created_at_raises_error(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing created_at raises TransformationError."""
        # Mock parse_datetime_utc to return None for the createdAt field
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_datetime_utc"
        )
        mock_parse.return_value = None

        raw_order = create_raw_order()
        with pytest.raises(TransformationError, match="createdAt is required"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_parsing_exception_raises_transformation_error(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that parsing exceptions are wrapped in TransformationError."""
        # Mock parse_decimal_value to raise an exception
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Mock parsing error")

        raw_order = create_raw_order()
        with pytest.raises(
            TransformationError, match="Failed to transform BackpackRawOrder to Order"
        ):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_with_triggered_at(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with triggered timestamp."""
        triggered_time = datetime.now(UTC).isoformat()
        raw_order = create_raw_order(order_type="STOP")
        raw_order = raw_order.model_copy(
            update={
                "triggeredAt": triggered_time,
                "triggerPrice": "2900.00",  # Add trigger price for STOP order
            }
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.triggered_at is not None
        assert result.triggered_at.isoformat() == triggered_time  # Compare ISO strings directly
        assert result.stop_price == Decimal("2900.00")  # Add assertion for stop price

    def test_transform_raw_order_reduce_only_and_post_only(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with reduce_only and post_only flags."""
        raw_order = create_raw_order()
        raw_order = raw_order.model_copy(update={"reduceOnly": True, "postOnly": True})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.reduce_only is True
        assert result.post_only is True


# --- Tests for transform_order_data_to_internal ---


class TestTransformOrderDataToInternal:
    """Tests for the transform_order_data_to_internal method."""

    def test_transform_order_data_happy_path(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test successful transformation of order data."""
        created_at = datetime.now(UTC).isoformat()
        updated_at = datetime.now(UTC).isoformat()

        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="98765",
            symbol="ETH_USDC",
            side="Sell",
            order_type="MARKET",
            status="FILLED",
            quantity="2.0",
            price=None,  # Market order
            client_order_id="client_123",
            time_in_force="IOC",
            created_at=created_at,
            updated_at=updated_at,
        )

        assert isinstance(result, Order)
        assert result.exchange_order_id == "98765"
        assert result.symbol == "ETH_USDC"
        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.MARKET
        assert result.status == OrderStatus.FILLED
        assert result.quantity_requested == Decimal("2.0")
        assert result.quantity_filled == Decimal("0")  # Default when not provided
        assert result.price is None
        assert result.time_in_force == TimeInForce.IOC
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.client_order_id == "client_123"
        assert result.created_at is not None
        assert result.updated_at is not None

    def test_transform_order_data_minimal_params(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with minimal required parameters."""
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="123",
            symbol="SOL_USDC",
            side="Buy",
            order_type="LIMIT",
            status="NEW",
            quantity="1.0",
            price="3000.00",  # Add price for LIMIT order
        )

        assert isinstance(result, Order)
        assert result.exchange_order_id == "123"
        assert result.symbol == "SOL_USDC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.quantity_requested == Decimal("1.0")
        assert result.quantity_filled == Decimal("0")
        assert result.price == Decimal("3000.00")  # Update assertion for price
        assert result.time_in_force == TimeInForce.GTC  # Default
        assert (
            result.client_order_id is not None and len(result.client_order_id) > 0
        )  # Changed assertion to expect UUID
        assert result.created_at is not None  # Should be set to current time

    def test_transform_order_data_with_price(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with price parameter."""
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="456",
            symbol="BTC_USDC",
            side="Buy",
            order_type="LIMIT",
            status="NEW",
            quantity="0.1",
            price="50000.00",
        )

        assert result.price == Decimal("50000.00")

    def test_transform_order_data_missing_quantity_raises_error(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing quantity raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.return_value = None

        with pytest.raises(TransformationError, match="quantity_requested is required"):
            trading_data_mapper.transform_order_data_to_internal(
                order_id="123",
                symbol="SOL_USDC",
                side="Buy",
                order_type="LIMIT",
                status="NEW",
                quantity="1.0",
            )

    def test_transform_order_data_exception_wrapping(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that exceptions are properly wrapped in TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Mock parsing error")

        with pytest.raises(
            TransformationError, match="Failed to transform Backpack order data to Order"
        ):
            trading_data_mapper.transform_order_data_to_internal(
                order_id="123",
                symbol="SOL_USDC",
                side="Buy",
                order_type="LIMIT",
                status="NEW",
                quantity="1.0",
            )


# --- Integration Tests ---


class TestTradingDataMapperIntegration:
    """Integration tests for the complete trading data mapper functionality."""

    def test_complete_order_transformation_consistency(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test that both transformation methods produce consistent results."""
        # Create raw order
        raw_order = create_raw_order(
            side="Buy",
            order_type="LIMIT",
            status="FILLED",
            quantity="1.0",
            executed_quantity="1.0",  # Fully filled
            avg_fill_price="3000.00",  # Add average fill price since quantity_filled > 0
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
            quantity=raw_order.quantity,
            price=raw_order.price,
            client_order_id=raw_order.clientId,
            time_in_force=raw_order.timeInForce,
            created_at=str(raw_order.createdAt) if raw_order.createdAt else None,
            updated_at=str(raw_order.updatedAt) if raw_order.updatedAt else None,
        )

        # Both should produce similar core results
        assert result_raw.exchange_order_id == result_data.exchange_order_id
        assert result_raw.symbol == result_data.symbol
        assert result_raw.side == result_data.side
        assert result_raw.order_type == result_data.order_type
        assert result_raw.status == result_data.status
        assert result_raw.quantity_requested == result_data.quantity_requested
        assert result_raw.price == result_data.price
        assert result_raw.time_in_force == result_data.time_in_force
        assert result_raw.exchange == result_data.exchange

    def test_error_handling_consistency(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that error handling is consistent across different transformation methods."""
        # Mock to cause an exception in both methods
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.bp_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Consistent error")

        # Both methods should raise TransformationError
        raw_order = create_raw_order()
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

        with pytest.raises(TransformationError):
            trading_data_mapper.transform_order_data_to_internal(
                order_id="123",
                symbol="SOL_USDC",
                side="Buy",
                order_type="LIMIT",
                status="NEW",
                quantity="1.0",
            )

    def test_all_mapping_logic_works_together(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test that all mapping logic works together properly in a transformation."""
        raw_order = create_raw_order(
            side="Sell",  # Should map to SELL
            order_type="MARKET",  # Should map to MARKET
            status="PARTIALLY_FILLED",  # Should map to PARTIALLY_FILLED
            time_in_force="FOK",  # Should map to FOK
            quantity="2.5",
            executed_quantity="1.0",
            avg_fill_price="3000.00",  # Add average fill price since quantity_filled > 0
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify that all mapping methods contributed correctly
        assert result.side == OrderSide.SELL  # side mapping
        assert result.order_type == OrderType.MARKET  # type mapping
        assert result.status == OrderStatus.PARTIALLY_FILLED  # status mapping
        assert result.time_in_force == TimeInForce.FOK  # TIF mapping

        # Verify the result is a complete, valid Order
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.symbol == "SOL_USDC"
        assert result.quantity_requested > Decimal("0")
        assert result.quantity_filled == Decimal("1.0")
        assert result.average_fill_price == Decimal(
            "3000.00"
        )  # Add assertion for average fill price
        assert result.created_at is not None


# --- Edge Case and Robustness Tests ---


class TestEdgeCasesAndRobustness:
    """Tests for edge cases and robustness of the trading data mapper."""

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
            order_type="STOP",  # Valid value from BP_ORDER_TYPES
            price="999999.999999",  # High precision price
            quantity="0.000001",  # Very small size
            executed_quantity="0.000001",  # All executed
            avg_fill_price="999999.999999",  # Add average fill price since quantity_filled > 0
            order_id="999999999",  # Large order ID
            client_id="x" * 64,  # Max length client order ID
            symbol="A" * 32,  # Long symbol
            time_in_force="FOK",
        )
        # Add trigger price for STOP order to satisfy business logic validation
        boundary_order = boundary_order.model_copy(update={"triggerPrice": "999999.999999"})

        result = trading_data_mapper.transform_raw_order_to_internal(boundary_order)

        assert result.exchange_order_id == "999999999"
        assert result.client_order_id == "x" * 64
        assert result.symbol == "A" * 32
        assert result.side == OrderSide.SELL
        assert result.quantity_requested == Decimal("0.000001")
        assert result.quantity_filled == Decimal("0.000001")
        assert result.average_fill_price == Decimal(
            "999999.999999"
        )  # Add assertion for average fill price
        assert result.stop_price == Decimal("999999.999999")  # Add assertion for stop price
        assert result.time_in_force == TimeInForce.FOK

    @pytest.mark.parametrize(
        "status_input,expected_output",
        [
            ("NEW", OrderStatus.OPEN),
            ("FILLED", OrderStatus.FILLED),
            ("CANCELLED", OrderStatus.CANCELED),
            ("CANCELLED", OrderStatus.CANCELED),
            ("REJECTED", OrderStatus.REJECTED),
            ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
            ("NEW", OrderStatus.OPEN),
        ],
    )
    def test_status_edge_cases(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        status_input: str,
        expected_output: OrderStatus,
    ) -> None:
        """Test various status edge cases."""
        raw_order = create_raw_order(status=status_input)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.status == expected_output

    def test_case_insensitive_mappings(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test that all enum mappings are case insensitive."""
        # Test case insensitive mapping through the order data transformation method
        # which doesn't go through raw model validation
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="123",
            symbol="SOL_USDC",
            side="buy",  # lowercase - should map to BUY
            order_type="market",  # Changed from "limit" to "market" to avoid price requirement
            status="filled",  # lowercase - should map to FILLED
            quantity="1.0",
            time_in_force="ioc",  # lowercase - should map to IOC
        )

        # Verify that all mapping methods contributed correctly
        assert result.side == OrderSide.BUY  # side mapping
        assert result.order_type == OrderType.MARKET  # type mapping (changed from LIMIT to MARKET)
        assert result.status == OrderStatus.FILLED  # status mapping
        assert result.time_in_force == TimeInForce.IOC  # TIF mapping

    def test_stop_order_type_mapping(self, trading_data_mapper: BackpackTradingDataMapper) -> None:
        """Test STOP order type mapping with required stop_price."""
        raw_order = create_raw_order(order_type="STOP", price="3000.50")
        raw_order = raw_order.model_copy(update={"triggerPrice": "2900.00"})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.order_type == OrderType.STOP_MARKET
        assert result.stop_price == Decimal("2900.00")
