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
def hyperliquid_raw_order_buy_limit_fixture() -> HyperliquidRawOrder:
    """Provides a valid HyperliquidRawOrder for a BUY limit order."""
    timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)
    return HyperliquidRawOrder(
        oid=12345,
        cloid="test_buy_limit_001",
        asset="ETH-PERP",
        side="B",
        limitPx="3000.50",
        sz="1.5",
        timestamp=timestamp_ms,
        orderType={"limit": {"tif": "Gtc"}},
        reduceOnly=False,
        remainingSz="0.5",
        status="open",
        statusTimestamp=timestamp_ms + 1000,
    )


@pytest.fixture
def hyperliquid_raw_order_sell_market_fixture() -> HyperliquidRawOrder:
    """Provides a valid HyperliquidRawOrder for a SELL market order."""
    timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)
    return HyperliquidRawOrder(
        oid=67890,
        cloid=None,
        asset="BTC-PERP",
        side="A",
        limitPx="0",  # Market orders typically have 0 limit price
        sz="0.1",
        timestamp=timestamp_ms,
        orderType={"market": {}},
        reduceOnly=True,
        remainingSz="0.1",  # All remaining for open market order
        status="open",  # Only "open" is valid for HyperliquidRawOrder
        statusTimestamp=timestamp_ms + 2000,
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


@pytest.fixture
def hyperliquid_raw_historical_order_fixture() -> HyperliquidRawHistoricalOrder:
    """Provides a valid HyperliquidRawHistoricalOrder."""
    timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)
    return HyperliquidRawHistoricalOrder(
        oid=98765,
        cloid="test_historical_001",
        asset="SOL-PERP",
        side="B",
        limitPx="100.25",
        sz="10.0",
        timestamp=timestamp_ms,
        orderType={"limit": {"tif": "Ioc"}},
        reduceOnly=False,
        remainingSz="2.5",
        status="filled",  # "filled" is valid for HyperliquidRawHistoricalOrder
        statusTimestamp=timestamp_ms + 5000,
    )


# --- Tests for _map_side_to_internal ---


class TestMapSideToInternal:
    """Tests for the _map_side_to_internal method."""

    def test_map_side_buy(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'B' to OrderSide.BUY."""
        # Access the protected method using getattr to avoid linter warnings
        map_side_method = trading_data_mapper._map_side_to_internal
        result = map_side_method("B")
        assert result == OrderSide.BUY

    def test_map_side_sell(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'A' to OrderSide.SELL."""
        map_side_method = trading_data_mapper._map_side_to_internal
        result = map_side_method("A")
        assert result == OrderSide.SELL

    def test_map_side_invalid(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping invalid side raises TransformationError."""
        map_side_method = trading_data_mapper._map_side_to_internal
        with pytest.raises(TransformationError, match="Unknown Hyperliquid order side: 'X'"):
            map_side_method("X")

    def test_map_side_empty_string(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping empty string raises TransformationError."""
        map_side_method = trading_data_mapper._map_side_to_internal
        with pytest.raises(TransformationError, match="Unknown Hyperliquid order side: ''"):
            map_side_method("")

    def test_map_side_lowercase(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping lowercase side strings raises TransformationError."""
        map_side_method = trading_data_mapper._map_side_to_internal
        with pytest.raises(TransformationError, match="Unknown Hyperliquid order side: 'b'"):
            map_side_method("b")


# --- Tests for _map_status_to_internal ---


class TestMapStatusToInternal:
    """Tests for the _map_status_to_internal method."""

    def test_map_status_open(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'open' to OrderStatus.OPEN."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("open")
        assert result == OrderStatus.OPEN

    def test_map_status_filled(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'filled' to OrderStatus.FILLED."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("filled")
        assert result == OrderStatus.FILLED

    def test_map_status_cancelled(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'cancelled' to OrderStatus.CANCELED."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("cancelled")
        assert result == OrderStatus.CANCELED

    def test_map_status_canceled(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'canceled' (US spelling) to OrderStatus.CANCELED."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("canceled")
        assert result == OrderStatus.CANCELED

    def test_map_status_rejected(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping 'rejected' to OrderStatus.REJECTED."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("rejected")
        assert result == OrderStatus.REJECTED

    def test_map_status_partially_filled(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test mapping 'partially_filled' to OrderStatus.PARTIALLY_FILLED."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("partially_filled")
        assert result == OrderStatus.PARTIALLY_FILLED

    def test_map_status_case_insensitive(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that status mapping is case-insensitive."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("OPEN")
        assert result == OrderStatus.OPEN

    def test_map_status_unknown(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping unknown status returns OrderStatus.UNKNOWN."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("unknown_status")
        assert result == OrderStatus.UNKNOWN

    def test_map_status_empty_string(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test mapping empty string returns OrderStatus.UNKNOWN."""
        map_status_method = trading_data_mapper._map_status_to_internal
        result = map_status_method("")
        assert result == OrderStatus.UNKNOWN


# --- Tests for _map_type_to_internal ---


class TestMapTypeToInternal:
    """Tests for the _map_type_to_internal method."""

    def test_map_type_limit_no_trigger(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test mapping limit order without trigger."""
        order_type: dict[str, Any] = {"limit": {"tif": "Gtc"}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, None)
        assert result == OrderType.LIMIT

    def test_map_type_market_no_trigger(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test mapping market order without trigger."""
        order_type: dict[str, Any] = {"market": {}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, None)
        assert result == OrderType.MARKET

    def test_map_type_limit_with_stop_loss_trigger(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test mapping limit order with stop loss trigger."""
        order_type: dict[str, Any] = {"limit": {"tif": "Gtc"}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, hyperliquid_raw_trigger_info_stop_loss_fixture)
        assert result == OrderType.STOP_LIMIT

    def test_map_type_limit_with_take_profit_trigger(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_take_profit_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test mapping limit order with take profit trigger."""
        order_type: dict[str, Any] = {"limit": {"tif": "Gtc"}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, hyperliquid_raw_trigger_info_take_profit_fixture)
        assert result == OrderType.TAKE_PROFIT_LIMIT

    def test_map_type_market_with_stop_loss_trigger(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test mapping market order with stop loss trigger."""
        order_type: dict[str, Any] = {"market": {}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, hyperliquid_raw_trigger_info_stop_loss_fixture)
        assert result == OrderType.STOP_MARKET

    def test_map_type_market_with_take_profit_trigger(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_trigger_info_take_profit_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test mapping market order with take profit trigger."""
        order_type: dict[str, Any] = {"market": {}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, hyperliquid_raw_trigger_info_take_profit_fixture)
        assert result == OrderType.TAKE_PROFIT_MARKET

    def test_map_type_unknown_defaults_to_limit(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test mapping unknown order type defaults to LIMIT with warning."""
        order_type: dict[str, Any] = {"unknown": {}}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, None)
        assert result == OrderType.LIMIT
        assert "Unknown orderType structure" in caplog.text
        assert "Defaulting to LIMIT" in caplog.text

    def test_map_type_empty_dict_defaults_to_limit(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test mapping empty order type dict defaults to LIMIT with warning."""
        order_type: dict[str, Any] = {}
        map_type_method = trading_data_mapper._map_type_to_internal
        result = map_type_method(order_type, None)
        assert result == OrderType.LIMIT
        assert "Unknown orderType structure" in caplog.text


# --- Tests for _map_time_in_force ---


class TestMapTimeInForce:
    """Tests for the _map_time_in_force method."""

    def test_map_tif_gtc(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping GTC time in force."""
        order_type: dict[str, Any] = {"limit": {"tif": "Gtc"}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC

    def test_map_tif_ioc(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping IOC time in force."""
        order_type: dict[str, Any] = {"limit": {"tif": "Ioc"}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.IOC

    def test_map_tif_alo(self, trading_data_mapper: HyperliquidTradingDataMapper) -> None:
        """Test mapping ALO time in force."""
        order_type: dict[str, Any] = {"limit": {"tif": "Alo"}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.ALO

    def test_map_tif_case_insensitive(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that TIF mapping is case-insensitive."""
        order_type: dict[str, Any] = {"limit": {"tif": "GTC"}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC

    def test_map_tif_market_order_defaults_to_gtc(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that market orders default to GTC."""
        order_type: dict[str, Any] = {"market": {}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC

    def test_map_tif_no_limit_defaults_to_gtc(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that order types without limit default to GTC."""
        order_type: dict[str, Any] = {"unknown": {}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC

    def test_map_tif_empty_limit_defaults_to_gtc(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that limit orders without tif default to GTC."""
        order_type: dict[str, Any] = {"limit": {}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC

    def test_map_tif_unknown_value_defaults_to_gtc(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that unknown TIF values default to GTC."""
        order_type: dict[str, Any] = {"limit": {"tif": "unknown"}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC

    def test_map_tif_non_string_value_defaults_to_gtc(
        self, trading_data_mapper: HyperliquidTradingDataMapper
    ) -> None:
        """Test that non-string TIF values default to GTC."""
        order_type: dict[str, Any] = {"limit": {"tif": 123}}
        map_tif_method = trading_data_mapper._map_time_in_force
        result = map_tif_method(order_type)
        assert result == TimeInForce.GTC


# --- Tests for transform_raw_order_to_internal ---


class TestTransformRawOrderToInternal:
    """Tests for the transform_raw_order_to_internal method."""

    def test_transform_raw_order_buy_limit_happy_path(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
    ) -> None:
        """Test successful transformation of a BUY limit order."""
        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture
        )

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
        assert result.client_order_id == "test_buy_limit_001"
        assert result.created_at is not None
        assert result.updated_at is not None
        assert result.stop_price is None
        assert result.trigger_by is None
        # For filled quantity > 0, we need a valid average_fill_price
        assert result.average_fill_price == Decimal("3000.50")  # Uses the limit price

    def test_transform_raw_order_sell_market_happy_path(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_sell_market_fixture: HyperliquidRawOrder,
    ) -> None:
        """Test successful transformation of a SELL market order."""
        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_sell_market_fixture
        )

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
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation with trigger information."""
        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture, hyperliquid_raw_trigger_info_stop_loss_fixture
        )

        assert result.order_type == OrderType.STOP_LIMIT
        assert result.stop_price == Decimal("2900.00")
        assert result.trigger_by is None  # No trigger_type in fixture

    def test_transform_raw_order_with_trigger_mark_price(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with trigger that has mark price trigger type."""
        trigger = mocker.Mock()
        trigger.trigger_px = "2900.00"
        trigger.tpsl = "sl"
        trigger.trigger_type = "mark"

        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture, trigger
        )

        assert result.trigger_by == TriggerType.MARK_PRICE

    def test_transform_raw_order_with_trigger_last_price(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with trigger that has last price trigger type."""
        trigger = mocker.Mock()
        trigger.trigger_px = "2900.00"
        trigger.tpsl = "sl"
        trigger.trigger_type = "last"

        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture, trigger
        )

        assert result.trigger_by == TriggerType.LAST_PRICE

    def test_transform_raw_order_missing_size_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing size raises TransformationError."""
        # Mock parse_decimal_value to return None for the sz field
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.return_value = None

        with pytest.raises(TransformationError, match="quantity_requested \\(sz\\) is required"):
            trading_data_mapper.transform_raw_order_to_internal(
                hyperliquid_raw_order_buy_limit_fixture
            )

    def test_transform_raw_order_missing_timestamp_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing timestamp raises TransformationError."""
        # Mock parse_datetime_utc to return None for the timestamp field
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_datetime_utc"
        )
        mock_parse.return_value = None

        with pytest.raises(TransformationError, match="created_at \\(timestamp\\) is required"):
            trading_data_mapper.transform_raw_order_to_internal(
                hyperliquid_raw_order_buy_limit_fixture
            )

    def test_transform_raw_order_parsing_exception_raises_transformation_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that parsing exceptions are wrapped in TransformationError."""
        # Mock parse_decimal_value to raise an exception
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Mock parsing error")

        with pytest.raises(
            TransformationError, match="Failed to transform HyperliquidRawOrder to Order"
        ):
            trading_data_mapper.transform_raw_order_to_internal(
                hyperliquid_raw_order_buy_limit_fixture
            )

    def test_transform_raw_order_edge_case_none_remaining_sz(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of None remaining_sz (should default to 0)."""
        # Mock parse_decimal_value to return None only for remaining_sz
        original_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )

        def mock_parse_side_effect(
            value: Any, allow_none: bool = False, field_name: str = ""
        ) -> Decimal | None:
            if field_name == "remainingSz":
                return None
            if field_name == "sz":
                return Decimal("1.5")
            if field_name == "limitPx":
                return Decimal("3000.50")
            return original_parse.return_value

        original_parse.side_effect = mock_parse_side_effect

        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture
        )
        assert result.quantity_filled == Decimal("1.5")  # 1.5 - 0.0 (default)

    def test_transform_raw_order_edge_case_none_limit_px(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of None limit_px (should work for market orders)."""
        # Mock parse_decimal_value to return None for limit_px
        original_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )

        def mock_parse_side_effect(
            value: Any, allow_none: bool = False, field_name: str = ""
        ) -> Decimal | None:
            if field_name == "limitPx":
                return None
            if field_name == "sz":
                return Decimal("1.5")
            if field_name == "remainingSz":
                return Decimal("0.5")
            return original_parse.return_value

        original_parse.side_effect = mock_parse_side_effect

        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture
        )
        assert result.price is None


# --- Tests for transform_raw_historical_order_to_internal ---


class TestTransformRawHistoricalOrderToInternal:
    """Tests for the transform_raw_historical_order_to_internal method."""

    def test_transform_raw_historical_order_happy_path(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
    ) -> None:
        """Test successful transformation of a historical order."""
        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            hyperliquid_raw_historical_order_fixture
        )

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
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        hyperliquid_raw_trigger_info_take_profit_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation of historical order with trigger."""
        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            hyperliquid_raw_historical_order_fixture,
            hyperliquid_raw_trigger_info_take_profit_fixture,
        )

        assert result.order_type == OrderType.TAKE_PROFIT_LIMIT
        assert result.stop_price == Decimal("3200.00")

    def test_transform_raw_historical_order_missing_size_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing size in historical order raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.return_value = None

        with pytest.raises(TransformationError, match="quantity_requested \\(sz\\) is required"):
            trading_data_mapper.transform_raw_historical_order_to_internal(
                hyperliquid_raw_historical_order_fixture
            )

    def test_transform_raw_historical_order_missing_timestamp_raises_error(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing timestamp in historical order raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_datetime_utc"
        )
        mock_parse.return_value = None

        with pytest.raises(TransformationError, match="created_at \\(timestamp\\) is required"):
            trading_data_mapper.transform_raw_historical_order_to_internal(
                hyperliquid_raw_historical_order_fixture
            )

    def test_transform_raw_historical_order_no_status_timestamp_uses_created_at(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that when status_timestamp is missing, updated_at uses created_at."""
        mock_created_at = datetime.now(UTC)
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_datetime_utc"
        )

        def mock_parse_side_effect(value: Any, field_name: str = "") -> datetime | None:
            if field_name == "timestamp":
                return mock_created_at
            if field_name == "statusTimestamp":
                return None  # Simulate missing status timestamp
            return mock_created_at

        mock_parse.side_effect = mock_parse_side_effect

        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            hyperliquid_raw_historical_order_fixture
        )
        assert result.created_at == mock_created_at
        assert result.updated_at == mock_created_at

    def test_transform_raw_historical_order_edge_case_no_cloid(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of historical order without client order ID."""

        # Mock getattr to return None for cloid
        def mock_getattr(obj: Any, attr: str, default: Any = None) -> Any:
            if attr == "cloid":
                return default
            return getattr(obj, attr, default)

        mocker.patch("builtins.getattr", side_effect=mock_getattr)

        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            hyperliquid_raw_historical_order_fixture
        )
        assert result.client_order_id == ""

    def test_transform_raw_historical_order_exception_wrapping(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that exceptions are properly wrapped in TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Mock parsing error")

        with pytest.raises(
            TransformationError, match="Failed to transform HyperliquidRawHistoricalOrder to Order"
        ):
            trading_data_mapper.transform_raw_historical_order_to_internal(
                hyperliquid_raw_historical_order_fixture
            )

    def test_transform_raw_historical_order_edge_case_zero_remaining_sz(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of historical order with missing remaining_sz attribute."""
        # Mock getattr to return "0" for remaining_sz (simulating it's missing and defaults to "0")
        original_getattr = getattr

        def mock_getattr(obj: Any, attr: str, default: Any = None) -> Any:
            if attr == "remaining_sz":
                return "0"
            return original_getattr(obj, attr, default)

        mocker.patch("builtins.getattr", side_effect=mock_getattr)

        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            hyperliquid_raw_historical_order_fixture
        )
        assert result.quantity_filled == Decimal("10.0")  # All filled since remaining is 0


# --- Integration Tests ---


class TestTradingDataMapperIntegration:
    """Integration tests for the complete trading data mapper functionality."""

    def test_complete_order_lifecycle_transformation(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
    ) -> None:
        """Test transformation of orders through different lifecycle stages."""
        # Transform live order
        live_order = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture, hyperliquid_raw_trigger_info_stop_loss_fixture
        )

        # Transform historical order
        historical_order = trading_data_mapper.transform_raw_historical_order_to_internal(
            hyperliquid_raw_historical_order_fixture
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
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        hyperliquid_raw_historical_order_fixture: HyperliquidRawHistoricalOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test that error handling is consistent across different transformation methods."""
        # Mock to cause an exception in both methods
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper.parse_decimal_value"
        )
        mock_parse.side_effect = ValueError("Consistent error")

        # Both methods should raise TransformationError
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(
                hyperliquid_raw_order_buy_limit_fixture
            )

        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_historical_order_to_internal(
                hyperliquid_raw_historical_order_fixture
            )

    def test_all_helper_methods_work_together(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
    ) -> None:
        """Test that all helper methods work together properly in a transformation."""
        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture
        )

        # Verify that all helper methods contributed correctly
        assert result.side == OrderSide.BUY  # _map_side_to_internal
        assert result.status == OrderStatus.OPEN  # _map_status_to_internal
        assert result.order_type == OrderType.LIMIT  # _map_type_to_internal
        assert result.time_in_force == TimeInForce.GTC  # _map_time_in_force

        # Verify the result is a complete, valid Order
        assert result.exchange == ExchangeName.HYPERLIQUID.value
        assert result.symbol == "ETH-PERP"
        assert result.quantity_requested > Decimal("0")
        assert result.price is not None
        assert result.created_at is not None


# --- Edge Case and Robustness Tests ---


class TestEdgeCasesAndRobustness:
    """Tests for edge cases and robustness of the trading data mapper."""

    def test_minimal_order_data(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test transformation with minimal required order data."""
        timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)
        minimal_order = HyperliquidRawOrder(
            oid=1,
            cloid=None,
            asset="BTC",
            side="B",
            limitPx="50000.0",  # Positive price for limit order
            sz="1",
            timestamp=timestamp_ms,
            orderType={"limit": {"tif": "Gtc"}},  # Use limit order type
            reduceOnly=False,
            remainingSz="1",  # All remaining (no fills)
            status="open",  # Only valid status for HyperliquidRawOrder
            statusTimestamp=timestamp_ms,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(minimal_order)

        assert result.exchange_order_id == "1"
        assert result.symbol == "BTC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.status == OrderStatus.OPEN
        assert result.client_order_id == ""  # None should become empty string

    def test_boundary_values(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test transformation with boundary values."""
        timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)
        boundary_order = HyperliquidRawOrder(
            oid=999999999,  # Large order ID
            cloid="x" * 64,  # Max length client order ID
            asset="A" * 64,  # Max length asset
            side="A",
            limitPx="999999.999999",  # High precision price
            sz="0.000001",  # Very small size
            timestamp=timestamp_ms,
            orderType={"limit": {"tif": "Alo"}},
            reduceOnly=True,
            remainingSz="0.000001",  # All remaining
            status="open",  # Only valid status for HyperliquidRawOrder
            statusTimestamp=timestamp_ms,
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
        hyperliquid_raw_order_buy_limit_fixture: HyperliquidRawOrder,
        mocker: MockerFixture,
    ) -> None:
        """Test complex trigger scenarios with various attributes."""
        # Test trigger with missing attributes
        minimal_trigger = mocker.Mock()
        minimal_trigger.trigger_px = "100.0"
        # No tpsl or trigger_type attributes

        result = trading_data_mapper.transform_raw_order_to_internal(
            hyperliquid_raw_order_buy_limit_fixture, minimal_trigger
        )

        assert result.stop_price == Decimal("100.0")
        assert result.trigger_by is None  # Should handle missing trigger_type gracefully

    def test_status_edge_cases(
        self,
        trading_data_mapper: HyperliquidTradingDataMapper,
    ) -> None:
        """Test various status edge cases."""
        test_cases = [
            ("CANCELLED", OrderStatus.CANCELED),
            ("rejected", OrderStatus.REJECTED),
            ("Partially_Filled", OrderStatus.PARTIALLY_FILLED),
            ("weird_status", OrderStatus.UNKNOWN),
            ("", OrderStatus.UNKNOWN),
        ]

        map_status_method = trading_data_mapper._map_status_to_internal
        for status_input, expected_output in test_cases:
            result = map_status_method(status_input)
            assert result == expected_output, f"Failed for status: {status_input}"
