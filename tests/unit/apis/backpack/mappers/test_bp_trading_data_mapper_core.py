"""
CyberDeltaEngine: Backpack Trading Data Mapper Core Tests
---------------------------------------------------------

Comprehensive test suite for BackpackTradingDataMapper core transformations.
Tests fundamental transformation methods and mapping logic including:
- Order side, status, type, and time-in-force mappings
- Raw order to internal order transformations
- Order data to internal order transformations
- Core business logic validation
- Basic error handling scenarios
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


# --- Parameterized Tests for Order Side Mapping ---


@pytest.mark.parametrize(
    "bp_side,expected_side",
    [
        ("Buy", OrderSide.BUY),
        ("Sell", OrderSide.SELL),
        ("buy", OrderSide.BUY),  # Case insensitive
        ("sell", OrderSide.SELL),
        ("Bid", OrderSide.BUY),
        ("Ask", OrderSide.SELL),
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
            price="100.0",
        )
        assert result.side == expected_side


@pytest.mark.parametrize(
    "invalid_side",
    ["X", "", "b", "s", "invalid", "Long", "Short", "UP", "DOWN"],
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
            price="100.0",
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
        ("EXPIRED", OrderStatus.UNKNOWN),
        # Remove invalid statuses that aren't supported by BackpackRawOrder validation
    ],
)
def test_order_status_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
    bp_status: str,
    expected_status: OrderStatus,
) -> None:
    """Test order status mapping with valid and extended statuses."""
    raw_order = create_raw_order(status=bp_status)
    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
    assert result.status == expected_status


# Add separate test for unsupported statuses using order_data method
def test_unsupported_order_status_via_data_method(
    trading_data_mapper: BackpackTradingDataMapper,
) -> None:
    """Test unsupported order statuses via order data method."""
    # Test "pending" which actually maps to OPEN according to the mapper
    result_pending = trading_data_mapper.transform_order_data_to_internal(
        order_id="12345",
        symbol="SOL_USDC",
        side="Buy",
        order_type="LIMIT",
        status="PENDING",
        quantity="1.0",
        price="100.0",
    )
    assert result_pending.status == OrderStatus.OPEN

    # Test truly unsupported statuses that map to UNKNOWN
    unsupported_statuses = ["OPEN", "ACTIVE", "INVALID", "NONSENSE"]
    for status in unsupported_statuses:
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="12345",
            symbol="SOL_USDC",
            side="Buy",
            order_type="LIMIT",
            status=status,
            quantity="1.0",
            price="100.0",
        )
        assert result.status == OrderStatus.UNKNOWN


@pytest.mark.parametrize(
    "invalid_status",
    ["UNKNOWN_STATUS", "INVALID", "", "xyz", "123"],
)
def test_invalid_order_status_defaults_to_unknown(
    trading_data_mapper: BackpackTradingDataMapper,
    invalid_status: str,
) -> None:
    """Test that invalid order statuses default to UNKNOWN."""
    result = trading_data_mapper.transform_order_data_to_internal(
        order_id="12345",
        symbol="SOL_USDC",
        side="Buy",
        order_type="LIMIT",
        status=invalid_status,
        quantity="1.0",
        price="100.0",
    )
    assert result.status == OrderStatus.UNKNOWN


# --- Parameterized Tests for Order Type Mapping ---


@pytest.mark.parametrize(
    "bp_type,expected_type",
    [
        ("LIMIT", OrderType.LIMIT),
        ("MARKET", OrderType.MARKET),
        ("TAKE_PROFIT", OrderType.LIMIT),
        # Remove invalid order types that aren't supported by BackpackRawOrder validation
    ],
)
def test_order_type_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
    bp_type: str,
    expected_type: OrderType,
) -> None:
    """Test order type mapping with various types."""
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
        # Remove STOP_MARKET as it's not a valid orderType in BackpackRawOrder
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
        # Remove empty string as it causes validation error
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


# Test empty string TIF separately using order data method
def test_empty_time_in_force_mapping(
    trading_data_mapper: BackpackTradingDataMapper,
) -> None:
    """Test empty string time in force mapping via order data method."""
    result = trading_data_mapper.transform_order_data_to_internal(
        order_id="12345",
        symbol="SOL_USDC",
        side="Buy",
        order_type="LIMIT",
        status="NEW",
        quantity="1.0",
        price="100.0",
        time_in_force="",  # Empty string
    )
    assert result.time_in_force == TimeInForce.GTC


# --- Comprehensive Transformation Tests ---


class TestTransformRawOrderToInternal:
    """Tests for the transform_raw_order_to_internal method."""

    def test_transform_raw_order_buy_limit_happy_path(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test successful transformation of a BUY limit order."""
        raw_order = create_raw_order(
            side="Buy",
            order_type="LIMIT",
            price="3000.50",
            quantity="1.5",
            executed_quantity="0.5",
            avg_fill_price="3001.00",
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
        assert result.average_fill_price == Decimal("3001.00")
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
            price=None,
            quantity="0.1",
            executed_quantity="0.1",
            avg_fill_price="50000.00",
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
        assert result.price is None
        assert result.average_fill_price == Decimal("50000.00")
        assert result.time_in_force == TimeInForce.GTC
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.client_order_id is not None and len(result.client_order_id) > 0

    def test_transform_raw_order_with_stop_price(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with stop price."""
        raw_order = create_raw_order(order_type="STOP", price="3000.00")
        raw_order = raw_order.model_copy(update={"triggerPrice": "2900.00"})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.order_type == OrderType.STOP_MARKET
        assert result.stop_price == Decimal("2900.00")

    def test_transform_raw_order_with_average_fill_price(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with average fill price."""
        raw_order = create_raw_order(executed_quantity="1.0")
        raw_order = raw_order.model_copy(update={"avgFillPrice": "3001.25"})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.average_fill_price == Decimal("3001.25")

    def test_transform_raw_order_missing_quantity_raises_error(
        self,
        trading_data_mapper: BackpackTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing quantity raises TransformationError."""
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
                "triggerPrice": "2900.00",
            }
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.triggered_at is not None
        assert result.triggered_at.isoformat() == triggered_time
        assert result.stop_price == Decimal("2900.00")

    def test_transform_raw_order_reduce_only_and_post_only(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with reduce_only and post_only flags."""
        raw_order = create_raw_order()
        raw_order = raw_order.model_copy(update={"reduceOnly": True, "postOnly": True})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.reduce_only is True
        assert result.post_only is True

    def test_transform_raw_order_high_precision_values(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with high precision decimal values."""
        raw_order = create_raw_order(
            quantity="1.123456789012345",
            price="50000.987654321098765",
            executed_quantity="0.123456789012345",
            avg_fill_price="50001.111111111111111",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.quantity_requested == Decimal("1.123456789012345")
        assert result.price == Decimal("50000.987654321098765")
        assert result.quantity_filled == Decimal("0.123456789012345")
        assert result.average_fill_price == Decimal("50001.111111111111111")

    def test_transform_raw_order_zero_values_handled(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with zero values."""
        raw_order = create_raw_order(
            executed_quantity="0.0",  # No fills yet
            avg_fill_price=None,  # No average fill price when no fills
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.quantity_filled == Decimal("0.0")
        assert result.average_fill_price is None


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
            price=None,
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
        assert result.quantity_filled == Decimal("0")
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
            price="3000.00",
        )

        assert isinstance(result, Order)
        assert result.exchange_order_id == "123"
        assert result.symbol == "SOL_USDC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.quantity_requested == Decimal("1.0")
        assert result.quantity_filled == Decimal("0")
        assert result.price == Decimal("3000.00")
        assert result.time_in_force == TimeInForce.GTC
        assert result.client_order_id is not None and len(result.client_order_id) > 0
        assert result.created_at is not None

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

    def test_transform_order_data_case_insensitive_mappings(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with case insensitive enum values."""
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="789",
            symbol="ADA_USDC",
            side="sell",  # lowercase
            order_type="market",  # lowercase
            status="filled",  # lowercase
            quantity="100.0",
            time_in_force="ioc",  # lowercase
        )

        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.MARKET
        assert result.status == OrderStatus.FILLED
        assert result.time_in_force == TimeInForce.IOC

    def test_transform_order_data_with_optional_fields(
        self, trading_data_mapper: BackpackTradingDataMapper
    ) -> None:
        """Test transformation with all optional fields provided."""
        created_at = datetime.now(UTC).isoformat()
        updated_at = datetime.now(UTC).isoformat()

        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="999",
            symbol="DOGE_USDC",
            side="Buy",
            order_type="LIMIT",
            status="NEW",
            quantity="1000.0",
            price="0.10",
            client_order_id="custom_client_id",
            time_in_force="FOK",
            created_at=created_at,
            updated_at=updated_at,
        )

        assert result.exchange_order_id == "999"
        assert result.symbol == "DOGE_USDC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.quantity_requested == Decimal("1000.0")
        assert result.price == Decimal("0.10")
        assert result.client_order_id == "custom_client_id"
        assert result.time_in_force == TimeInForce.FOK
        assert result.quantity_filled == Decimal("0")
        assert result.average_fill_price is None
        assert result.reduce_only is False
        assert result.post_only is False
