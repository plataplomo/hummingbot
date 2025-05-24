"""
Unit tests for the Hyperliquid Trading Data Mapper.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from _pytest.logging import LogCaptureFixture

# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import HyperliquidRawHistoricalOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)

logger = logging.getLogger(__name__)


# --- Fixtures ---


@pytest.fixture
def trading_data_mapper() -> HyperliquidTradingDataMapper:
    """Provide an instance of HyperliquidTradingDataMapper."""
    return HyperliquidTradingDataMapper()


@pytest.fixture
def base_timestamp() -> int:
    """Provide a consistent timestamp for tests."""
    return int(datetime.now(UTC).timestamp() * 1000)


def create_raw_order(
    side: str = "B",
    status: str = "open",
    order_type: dict[str, Any] | None = None,
    limit_px: str = "3000.50",
    sz: str = "1.5",
    remaining_sz: str = "0.5",
    oid: int = 12345,
    cloid: str | None = "test_order_001",
    asset: str = "ETH-PERP",
    timestamp: int | None = None,
) -> HyperliquidRawOrder:
    """Create a HyperliquidRawOrder with customizable parameters."""
    if timestamp is None:
        timestamp = int(datetime.now(UTC).timestamp() * 1000)
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
    order_type: dict[str, Any] | None = None,
    limit_px: str = "100.25",
    sz: str = "10.0",
    remaining_sz: str = "2.5",
    oid: int = 98765,
    cloid: str | None = "test_historical_001",
    asset: str = "SOL-PERP",
    timestamp: int | None = None,
) -> HyperliquidRawHistoricalOrder:
    """Create a HyperliquidRawHistoricalOrder with customizable parameters."""
    if timestamp is None:
        timestamp = int(datetime.now(UTC).timestamp() * 1000)
    if order_type is None:
        order_type = {"limit": {"tif": "Ioc"}}

    return HyperliquidRawHistoricalOrder(
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
        statusTimestamp=timestamp + 5000,
    )


@pytest.fixture
def hyperliquid_raw_trigger_info_stop_loss_fixture() -> HyperliquidRawTriggerInfo:
    """Provides a valid HyperliquidRawTriggerInfo for a stop loss."""
    return HyperliquidRawTriggerInfo(
        triggerPx="2900.00",
        isMarket=True,
        tpsl="sl",
    )


@pytest.fixture
def hyperliquid_raw_trigger_info_take_profit_fixture() -> HyperliquidRawTriggerInfo:
    """Provides a valid HyperliquidRawTriggerInfo for a take profit."""
    return HyperliquidRawTriggerInfo(
        triggerPx="3200.00",
        isMarket=False,
        tpsl="tp",
    )


# --- Parameterized Tests for Order Side Mapping ---


@pytest.mark.parametrize(
    "hl_side,expected_side",
    [
        ("B", OrderSide.BUY),
        ("A", OrderSide.SELL),
    ],
)
class TestOrderSideMapping:
    """Tests for order side mapping through public transformation methods."""

    def test_raw_order_side_mapping(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hl_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via raw order transformation."""
        raw_order = create_raw_order(side=hl_side)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.side == expected_side

    def test_historical_order_side_mapping(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hl_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via historical order transformation."""
        raw_order = create_raw_historical_order(side=hl_side)
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.side == expected_side


@pytest.mark.parametrize(
    "invalid_side",
    ["X", "", "b", "buy", "sell", "invalid"],
)
def test_invalid_order_side_raises_error(
    trading_data_mapper: HyperliquidTradingDataMapper,
    invalid_side: str,
) -> None:
    """Test that invalid order sides raise TransformationError."""
    raw_order = create_raw_order(side=invalid_side)
    with pytest.raises(
        TransformationError, match=f"Unknown Hyperliquid order side: '{invalid_side}'"
    ):
        trading_data_mapper.transform_raw_order_to_internal(raw_order)


# --- Parameterized Tests for Order Status Mapping ---


@pytest.mark.parametrize(
    "hl_status,expected_status",
    [
        ("open", OrderStatus.OPEN),
        ("filled", OrderStatus.FILLED),
        ("cancelled", OrderStatus.CANCELED),
        ("canceled", OrderStatus.CANCELED),
        ("rejected", OrderStatus.REJECTED),
        ("partially_filled", OrderStatus.PARTIALLY_FILLED),
        ("OPEN", OrderStatus.OPEN),  # Case insensitive
        ("FILLED", OrderStatus.FILLED),
        ("unknown_status", OrderStatus.UNKNOWN),
        ("", OrderStatus.UNKNOWN),
    ],
)
def test_order_status_mapping(
    trading_data_mapper: HyperliquidTradingDataMapper,
    hl_status: str,
    expected_status: OrderStatus,
) -> None:
    """Test order status mapping via historical order transformation."""
    raw_order = create_raw_historical_order(status=hl_status)
    result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
    assert result.status == expected_status


# --- Parameterized Tests for Order Type Mapping ---


@pytest.mark.parametrize(
    "order_type_dict,trigger_tpsl,expected_type",
    [
        ({"limit": {"tif": "Gtc"}}, None, OrderType.LIMIT),
        ({"market": {}}, None, OrderType.MARKET),
        ({"limit": {"tif": "Gtc"}}, "sl", OrderType.STOP_LIMIT),
        ({"limit": {"tif": "Gtc"}}, "tp", OrderType.TAKE_PROFIT_LIMIT),
        ({"market": {}}, "sl", OrderType.STOP_MARKET),
        ({"market": {}}, "tp", OrderType.TAKE_PROFIT_MARKET),
    ],
)
def test_order_type_mapping(
    trading_data_mapper: HyperliquidTradingDataMapper,
    order_type_dict: dict[str, Any],
    trigger_tpsl: str | None,
    expected_type: OrderType,
) -> None:
    """Test order type mapping with and without triggers."""
    raw_order = create_raw_order(order_type=order_type_dict)

    trigger = None
    if trigger_tpsl:
        trigger = HyperliquidRawTriggerInfo(
            triggerPx="2900.00",
            isMarket=True,
            tpsl=trigger_tpsl,
        )

    result = trading_data_mapper.transform_raw_order_to_internal(raw_order, trigger)
    assert result.order_type == expected_type


@pytest.mark.parametrize(
    "unknown_order_type",
    [
        {"unknown": {}},
        {},
        {"invalid_type": {"some": "data"}},
    ],
)
def test_unknown_order_type_defaults_to_limit(
    trading_data_mapper: HyperliquidTradingDataMapper,
    unknown_order_type: dict[str, Any],
    caplog: LogCaptureFixture,
) -> None:
    """Test that unknown order types default to LIMIT with warning."""
    raw_order = create_raw_order(order_type=unknown_order_type)
    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

    assert result.order_type == OrderType.LIMIT
    assert "Unknown orderType structure" in caplog.text
    assert "Defaulting to LIMIT" in caplog.text


# --- Parameterized Tests for Time In Force Mapping ---


@pytest.mark.parametrize(
    "order_type_dict,expected_tif",
    [
        ({"limit": {"tif": "Gtc"}}, TimeInForce.GTC),
        ({"limit": {"tif": "Ioc"}}, TimeInForce.IOC),
        ({"limit": {"tif": "Alo"}}, TimeInForce.ALO),
        ({"limit": {"tif": "GTC"}}, TimeInForce.GTC),  # Case insensitive
        ({"limit": {"tif": "IOC"}}, TimeInForce.IOC),
        ({"limit": {"tif": "ALO"}}, TimeInForce.ALO),
        ({"market": {}}, TimeInForce.GTC),  # Market orders default to GTC
        ({"limit": {}}, TimeInForce.GTC),  # No TIF defaults to GTC
        ({"limit": {"tif": "unknown"}}, TimeInForce.GTC),  # Unknown TIF defaults to GTC
        ({"limit": {"tif": 123}}, TimeInForce.GTC),  # Non-string TIF defaults to GTC
        ({"unknown": {}}, TimeInForce.GTC),  # Unknown order type defaults to GTC
    ],
)
def test_time_in_force_mapping(
    trading_data_mapper: HyperliquidTradingDataMapper,
    order_type_dict: dict[str, Any],
    expected_tif: TimeInForce,
) -> None:
    """Test time in force mapping through order transformation."""
    raw_order = create_raw_order(order_type=order_type_dict)
    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
    assert result.time_in_force == expected_tif


# --- Comprehensive Transformation Tests ---


class TestTransformRawOrderToInternal:
    """Tests for the transform_raw_order_to_internal method."""

    def test_transform_raw_order_buy_limit_happy_path(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test successful transformation of a BUY limit order."""
        raw_order = create_raw_order(
            side="B",
            order_type={"limit": {"tif": "Gtc"}},
            limit_px="3000.50",
            sz="1.5",
            remaining_sz="0.5",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "12345"
        assert result.symbol == "ETH-PERP"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.quantity_requested == Decimal("1.5")
        assert result.quantity_filled == Decimal("1.0")  # 1.5 - 0.5
        assert result.price == Decimal("3000.50")
        assert result.time_in_force == TimeInForce.GTC
        assert result.exchange == ExchangeName.HYPERLIQUID.value
        assert result.client_order_id == "test_order_001"
        assert result.created_at is not None
        assert result.updated_at is not None
        assert result.stop_price is None
        assert result.trigger_by is None
        # For filled quantity > 0, we need a valid average_fill_price
        assert result.average_fill_price == Decimal("3000.50")  # Uses the limit price

    def test_transform_raw_order_sell_market_happy_path(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test successful transformation of a SELL market order."""
        raw_order = create_raw_order(
            side="A",
            order_type={"market": {}},
            limit_px="0",  # Market orders typically have 0 limit price
            sz="0.1",
            remaining_sz="0.1",  # All remaining for open market order
            oid=67890,
            cloid=None,
            asset="BTC-PERP",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "67890"
        assert result.symbol == "BTC-PERP"
        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.MARKET
        assert result.status == OrderStatus.OPEN
        assert result.quantity_requested == Decimal("0.1")
        assert result.quantity_filled == Decimal("0.0")  # 0.1 - 0.1 (all remaining)
        assert result.price == Decimal("0")
        assert result.time_in_force == TimeInForce.GTC
        assert result.exchange == ExchangeName.HYPERLIQUID.value
        assert result.client_order_id == ""  # None should become empty string
        # No quantity filled, so no average_fill_price required

    def test_transform_raw_order_with_trigger(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation with trigger information."""
        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(
            raw_order, hyperliquid_raw_trigger_info_stop_loss_fixture
        )

        assert result.order_type == OrderType.STOP_LIMIT
        assert result.stop_price == Decimal("2900.00")
        assert result.trigger_by is None  # No trigger_type in fixture

    def test_transform_raw_order_with_trigger_mark_price(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with trigger that has mark price trigger type."""
        trigger = mocker.Mock()
        trigger.trigger_px = "2900.00"
        trigger.tpsl = "sl"
        trigger.trigger_type = "mark"

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order, trigger)

        assert result.trigger_by == TriggerType.MARK_PRICE

    def test_transform_raw_order_with_trigger_last_price(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with trigger that has last price trigger type."""
        trigger = mocker.Mock()
        trigger.trigger_px = "2900.00"
        trigger.tpsl = "sl"
        trigger.trigger_type = "last"

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order, trigger)

        assert result.trigger_by == TriggerType.LAST_PRICE

    def test_transform_raw_order_missing_size_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing size raises TransformationError."""
        # Mock parse_decimal_value to return None for the sz field
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.return_value = None

        raw_order = create_raw_order()
        with pytest.raises(TransformationError, match="quantity_requested \\(sz\\) is required"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_missing_timestamp_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing timestamp raises TransformationError."""
        # Mock parse_datetime_utc to return None for the timestamp field
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_datetime_utc"
        )
        mock_parse.return_value = None

        raw_order = create_raw_order()
        with pytest.raises(TransformationError, match="created_at \\(timestamp\\) is required"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_parsing_exception_raises_transformation_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that parsing exceptions are wrapped in TransformationError."""
        # Mock parse_decimal_value to raise an exception
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Mock parsing error")

        raw_order = create_raw_order()
        with pytest.raises(
            TransformationError, match="Failed to transform HyperliquidRawOrder to Order"
        ):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_edge_case_none_remaining_sz(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of None remaining_sz (should default to 0)."""
        # Mock parse_decimal_value to return None only for remaining_sz
        original_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )

        def mock_parse_side_effect(
            value: object, allow_none: bool = False, field_name: str = ""
        ) -> Decimal | None:
            if field_name == "remainingSz":
                return None
            if field_name == "sz":
                return Decimal("1.5")
            if field_name == "limitPx":
                return Decimal("3000.50")
            # Return a sensible default instead of calling the mock recursively
            return Decimal("0.0")

        original_parse.side_effect = mock_parse_side_effect

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.quantity_filled == Decimal("1.5")  # 1.5 - 0.0 (default)

    def test_transform_raw_order_edge_case_none_limit_px(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of None limit_px (should work for market orders)."""
        # Mock parse_decimal_value to return None for limit_px
        original_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )

        def mock_parse_side_effect(
            value: object, allow_none: bool = False, field_name: str = ""
        ) -> Decimal | None:
            if field_name == "limitPx":
                return None
            if field_name == "sz":
                return Decimal("1.5")
            if field_name == "remainingSz":
                return Decimal("0.5")
            # Return a sensible default instead of calling the mock recursively
            return Decimal("0.0")

        original_parse.side_effect = mock_parse_side_effect

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.price is None


# --- Tests for transform_raw_historical_order_to_internal ---


class TestTransformRawHistoricalOrderToInternal:
    """Tests for the transform_raw_historical_order_to_internal method."""

    def test_transform_raw_historical_order_happy_path(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test successful transformation of a historical order."""
        raw_order = create_raw_historical_order()
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "98765"
        assert result.symbol == "SOL-PERP"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.FILLED
        assert result.quantity_requested == Decimal("10.0")
        assert result.quantity_filled == Decimal("7.5")  # 10.0 - 2.5
        assert result.price == Decimal("100.25")
        assert result.time_in_force == TimeInForce.IOC
        assert result.exchange == ExchangeName.HYPERLIQUID.value
        assert result.client_order_id == "test_historical_001"
        assert result.created_at is not None
        assert result.updated_at is not None
        # For filled quantity > 0, we need a valid average_fill_price
        assert result.average_fill_price == Decimal("100.25")  # Uses the limit price

    def test_transform_raw_historical_order_with_trigger(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_take_profit_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation of historical order with trigger."""
        raw_order = create_raw_historical_order()
        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            raw_order, hyperliquid_raw_trigger_info_take_profit_fixture
        )

        assert result.order_type == OrderType.TAKE_PROFIT_LIMIT
        assert result.stop_price == Decimal("3200.00")

    def test_transform_raw_historical_order_missing_size_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing size in historical order raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.return_value = None

        raw_order = create_raw_historical_order()
        with pytest.raises(TransformationError, match="quantity_requested \\(sz\\) is required"):
            trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

    def test_transform_raw_historical_order_missing_timestamp_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing timestamp in historical order raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_datetime_utc"
        )
        mock_parse.return_value = None

        raw_order = create_raw_historical_order()
        with pytest.raises(TransformationError, match="created_at \\(timestamp\\) is required"):
            trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

    def test_transform_raw_historical_order_no_status_timestamp_uses_created_at(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that when status_timestamp is missing, updated_at uses created_at."""
        mock_created_at = datetime.now(UTC)
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_datetime_utc"
        )

        def mock_parse_side_effect(value: object, field_name: str = "") -> datetime | None:
            if field_name == "timestamp":
                return mock_created_at
            if field_name == "statusTimestamp":
                return None  # Simulate missing status timestamp
            return mock_created_at

        mock_parse.side_effect = mock_parse_side_effect

        raw_order = create_raw_historical_order()
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.created_at == mock_created_at
        assert result.updated_at == mock_created_at

    def test_transform_raw_historical_order_edge_case_no_cloid(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of historical order without client order ID."""

        # Mock getattr to return None for cloid
        def mock_getattr(obj: object, attr: str, default: object = None) -> object:
            if attr == "cloid":
                return default
            return getattr(obj, attr, default)

        mocker.patch("builtins.getattr", side_effect=mock_getattr)

        raw_order = create_raw_historical_order()
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.client_order_id == ""

    def test_transform_raw_historical_order_exception_wrapping(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that exceptions are properly wrapped in TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Mock parsing error")

        raw_order = create_raw_historical_order()
        with pytest.raises(
            TransformationError, match="Failed to transform HyperliquidRawHistoricalOrder to Order"
        ):
            trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

    def test_transform_raw_historical_order_edge_case_zero_remaining_sz(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of historical order with missing remaining_sz attribute."""
        # Mock getattr to return "0" for remaining_sz (simulating it's missing and defaults to "0")
        original_getattr = getattr

        def mock_getattr(obj: object, attr: str, default: object = None) -> object:
            if attr == "remaining_sz":
                return "0"
            return original_getattr(obj, attr, default)

        mocker.patch("builtins.getattr", side_effect=mock_getattr)

        raw_order = create_raw_historical_order()
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.quantity_filled == Decimal("10.0")  # All filled since remaining is 0


# --- Integration Tests ---


class TestTradingDataMapperIntegration:
    """Integration tests for the complete trading data mapper functionality."""

    def test_complete_order_lifecycle_transformation(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation of orders through different lifecycle stages."""
        # Transform live order
        live_order_raw = create_raw_order()
        live_order = trading_data_mapper.transform_raw_order_to_internal(
            live_order_raw, hyperliquid_raw_trigger_info_stop_loss_fixture
        )

        # Transform historical order
        historical_order_raw = create_raw_historical_order()
        historical_order = trading_data_mapper.transform_raw_historical_order_to_internal(
            historical_order_raw
        )

        # Both should be valid orders
        assert isinstance(live_order, Order)
        assert isinstance(historical_order, Order)

        # Live order should have trigger information
        assert live_order.order_type == OrderType.STOP_LIMIT
        assert live_order.stop_price is not None

        # Historical order should not have trigger info
        assert historical_order.order_type == OrderType.LIMIT
        assert historical_order.stop_price is None

        # Both should have valid timestamps
        assert live_order.created_at is not None
        assert live_order.updated_at is not None
        assert historical_order.created_at is not None
        assert historical_order.updated_at is not None

    def test_error_handling_consistency(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that error handling is consistent across different transformation methods."""
        # Mock to cause an exception in both methods
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Consistent error")

        # Both methods should raise TransformationError
        raw_order = create_raw_order()
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

        historical_raw_order = create_raw_historical_order()
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_historical_order_to_internal(historical_raw_order)

    def test_all_mapping_logic_works_together(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that all mapping logic works together properly in a transformation."""
        raw_order = create_raw_order(
            side="B",  # Should map to BUY
            order_type={"limit": {"tif": "Gtc"}},  # Should map to LIMIT, GTC
            limit_px="3000.50",
            sz="1.5",
            remaining_sz="0.5",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify that all mapping methods contributed correctly
        assert result.side == OrderSide.BUY  # side mapping
        assert result.status == OrderStatus.OPEN  # status mapping
        assert result.order_type == OrderType.LIMIT  # type mapping
        assert result.time_in_force == TimeInForce.GTC  # TIF mapping

        # Verify the result is a complete, valid Order
        assert result.exchange == ExchangeName.HYPERLIQUID.value
        assert result.symbol == "ETH-PERP"
        assert result.quantity_requested > Decimal("0")
        assert result.price is not None
        assert result.created_at is not None


# --- Edge Case and Robustness Tests ---


class TestEdgeCasesAndRobustness:
    """Tests for edge cases and robustness of the trading data mapper."""

    def test_minimal_order_data(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test transformation with minimal required order data."""
        minimal_order = create_raw_order(
            side="B",
            order_type={"limit": {"tif": "Gtc"}},  # Use limit order type
            limit_px="50000.0",  # Positive price for limit order
            sz="1",
            remaining_sz="1",  # All remaining (no fills)
            oid=1,
            cloid=None,
            asset="BTC",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(minimal_order)

        assert result.exchange_order_id == "1"
        assert result.symbol == "BTC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.client_order_id == ""  # None should become empty string

    def test_boundary_values(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test transformation with boundary values."""
        boundary_order = create_raw_order(
            side="A",
            order_type={"limit": {"tif": "Alo"}},
            limit_px="999999.999999",  # High precision price
            sz="0.000001",  # Very small size
            remaining_sz="0.000001",  # All remaining
            oid=999999999,  # Large order ID
            cloid="x" * 64,  # Max length client order ID
            asset="A" * 64,  # Max length asset
        )

        result = trading_data_mapper.transform_raw_order_to_internal(boundary_order)

        assert result.exchange_order_id == "999999999"
        assert result.client_order_id == "x" * 64
        assert result.symbol == "A" * 64
        assert result.side == OrderSide.SELL
        assert result.quantity_requested == Decimal("0.000001")
        assert result.quantity_filled == Decimal("0")  # All remaining, so nothing filled
        assert result.time_in_force == TimeInForce.ALO

    def test_complex_trigger_scenarios(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test complex trigger scenarios with various attributes."""
        # Test trigger with missing attributes
        minimal_trigger = mocker.Mock()
        minimal_trigger.trigger_px = "100.0"
        # No tpsl or trigger_type attributes

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order, minimal_trigger)

        assert result.stop_price == Decimal("100.0")
        assert result.trigger_by is None  # Should handle missing trigger_type gracefully

    @pytest.mark.parametrize(
        "status_input,expected_output",
        [
            ("CANCELLED", OrderStatus.CANCELED),
            ("rejected", OrderStatus.REJECTED),
            ("Partially_Filled", OrderStatus.PARTIALLY_FILLED),
            ("weird_status", OrderStatus.UNKNOWN),
            ("", OrderStatus.UNKNOWN),
        ],
    )
    def test_status_edge_cases(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        status_input: str,
        expected_output: OrderStatus,
    ) -> None:
        """Test various status edge cases."""
        raw_order = create_raw_historical_order(status=status_input)
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.status == expected_output
