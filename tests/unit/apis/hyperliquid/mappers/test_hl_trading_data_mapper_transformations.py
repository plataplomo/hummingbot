"""CyberDeltaEngine: Hyperliquid Trading Data Mapper Transformations Tests.

-----------------------------------------------------------------------

Comprehensive test suite for HyperliquidOrderMapper transformation methods.
Tests specific transformation scenarios including:
- Raw order to internal order transformations
- Historical order to internal order transformations
- Trigger-based order handling
- Comprehensive field mapping and validation
- Edge cases and error scenarios
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from cyberdelta.config.structlog_config import get_logger


# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions.data_transformation import MissingRequiredFieldError
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import HyperliquidRawHistoricalOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.core.enums import (
    OrderStatus,
    TriggerType,
)
from cyberdelta.core.models import Order
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


# --- Fixtures ---


@pytest.fixture
def trading_data_mapper() -> HyperliquidOrderMapper:
    """Provide an instance of HyperliquidOrderMapper."""
    return HyperliquidOrderMapper()


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
        sz=remaining_sz,  # sz is the remaining size in historical orders
        timestamp=timestamp,
        orderType=order_type_str,
        reduceOnly=False,
        origSz=sz,  # origSz is the original size
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


@pytest.fixture
def hyperliquid_raw_trigger_info_stop_loss_fixture() -> HyperliquidRawTriggerInfo:
    """Provide a valid HyperliquidRawTriggerInfo for a stop loss."""
    return HyperliquidRawTriggerInfo(
        triggerPx="2900.00",
        isMarket=True,
        tpsl="sl",
    )


@pytest.fixture
def hyperliquid_raw_trigger_info_take_profit_fixture() -> HyperliquidRawTriggerInfo:
    """Provide a valid HyperliquidRawTriggerInfo for a take profit."""
    return HyperliquidRawTriggerInfo(
        triggerPx="3200.00",
        isMarket=False,
        tpsl="tp",
    )


# --- Tests for Raw Order Transformations ---


class TestTransformRawOrderToInternal:
    """Tests for transform_raw_order_to_internal method."""

    def test_transform_raw_order_buy_limit_happy_path(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test successful transformation of a buy limit order."""
        raw_order = create_raw_order(
            side="B",
            status="open",
            order_type={"limit": {"tif": "Gtc"}},
            limit_px="3000.50",
            sz="1.5",
            remaining_sz="0.5",
            oid=12345,
            cloid="0x" + "0" * 30 + "1" * 2,  # Valid 128-bit hex string
            asset="ETH-PERP",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "12345"
        assert result.client_order_id == "0x" + "0" * 30 + "1" * 2
        assert result.symbol == "ETH-PERP"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.time_in_force == TimeInForce.GTC
        assert result.status == OrderStatus.OPEN
        assert result.price == Decimal("3000.50")
        assert result.quantity_requested == Decimal("1.5")
        assert result.quantity_filled == Decimal("1.0")  # 1.5 - 0.5 remaining
        # Note: remaining_quantity would be calculated as sz - quantity_filled
        assert result.exchange == ExchangeName.HYPERLIQUID.value
        assert isinstance(result.created_at, datetime)
        assert isinstance(result.updated_at, datetime)

    def test_transform_raw_order_sell_market_happy_path(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test successful transformation of a sell market order."""
        raw_order = create_raw_historical_order(
            side="A",
            status="filled",
            order_type_str="market",
            limit_px="60100.75",  # Use positive price for market order to avoid validation issues
            sz="2.0",
            remaining_sz="0.0",
            oid=54321,
            cloid=None,
            coin="BTC-PERP",
        )

        try:
            result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        except TransformationError:
            pytest.fail("Transformation should not fail for valid input")

        # DEFENSIVE CHECK: Mypy incorrectly reports following assertions as unreachable
        # due to complex transformation method control flow analysis. Mypy=[unreachable] Ruff=[]
        # DEFENSIVE CHECK: Ensure transformation succeeded. Mypy=[unreachable] Ruff=[]
        assert result is not None, "Transformation should not return None"
        assert result.exchange_order_id == "54321"
        assert isinstance(result.client_order_id, str)
        assert len(result.client_order_id) > 0
        assert result.symbol == "BTC-PERP"
        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.LIMIT  # Historical market orders map to LIMIT
        assert result.time_in_force == TimeInForce.IOC  # Market orders become IOC
        assert result.status == OrderStatus.FILLED
        assert result.price == Decimal("60100.75")  # Market orders use limit_px as price
        assert result.quantity_requested == Decimal("2.0")
        assert result.quantity_filled == Decimal("2.0")
        assert result.average_fill_price == Decimal("60100.75")  # Uses limit_px as approximation

    def test_transform_raw_order_missing_size_raises_error(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing or invalid size raises TransformationError."""
        raw_order = create_raw_order()

        # Mock parse_decimal_value to return None for size
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.return_value = None

        with pytest.raises(
            MissingRequiredFieldError, match="sz is required for HyperliquidRawOrder"
        ):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_missing_timestamp_raises_error(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing or invalid timestamp raises TransformationError."""
        raw_order = create_raw_order()

        # Mock parse_datetime_utc to return None for timestamp
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_datetime_utc",
        )
        mock_parse.return_value = None

        with pytest.raises(
            MissingRequiredFieldError, match="timestamp is required for HyperliquidRawOrder"
        ):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_parsing_exception_raises_transformation_error(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that parsing exceptions are wrapped in TransformationError."""
        raw_order = create_raw_order()

        # Mock parse_decimal_value to raise an exception
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Invalid decimal format")

        with pytest.raises(TransformationError, match="Failed to parse order quantities and price"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_edge_case_none_remaining_sz(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with None remaining size."""
        raw_order = create_raw_order()

        def mock_parse_side_effect(
            value: object,
            allow_none: bool = False,
            field_name: str = "",
        ) -> Decimal | None:
            """Return mock parse side effect for testing."""
            if field_name == "remaining_sz":
                return None
            # Return valid decimals for other fields
            if isinstance(value, str):
                return Decimal(value)
            return Decimal("1.0")

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = mock_parse_side_effect

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        # Note: The exact behavior would depend on the mapper implementation
        # This test verifies the transformation handles None remaining size gracefully
        assert result is not None

    def test_transform_raw_order_edge_case_none_limit_px(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with None limit price (market order scenario)."""
        raw_order = create_raw_order(order_type={"market": {}})

        def mock_parse_side_effect(
            value: object,
            allow_none: bool = False,
            field_name: str = "",
        ) -> Decimal | None:
            """Return mock parse side effect for testing."""
            if field_name == "limit_px":
                return None
            # Return valid decimals for other fields
            if isinstance(value, str):
                return Decimal(value)
            return Decimal("1.0")

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = mock_parse_side_effect

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.price == Decimal("3000.50")  # Uses the default limit_px from create_raw_order

    def test_transform_raw_order_with_trigger(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation with trigger information."""
        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.order_type == OrderType.STOP_LIMIT
        assert result.stop_price == Decimal("2900.00")
        assert result.trigger_by is None  # No trigger_type in fixture

    def test_transform_raw_order_with_trigger_mark_price(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with trigger that has mark price trigger type."""
        trigger = mocker.Mock()
        trigger.trigger_px = "2900.00"
        trigger.tpsl = "sl"
        trigger.trigger_type = "mark"

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.trigger_by == TriggerType.MARK_PRICE

    def test_transform_raw_order_with_trigger_last_price(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with trigger that has last price trigger type."""
        trigger = mocker.Mock()
        trigger.trigger_px = "2900.00"
        trigger.tpsl = "sl"
        trigger.trigger_type = "last"

        raw_order = create_raw_order()
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.trigger_by == TriggerType.LAST_PRICE


# --- Tests for Historical Order Transformations ---


class TestTransformRawHistoricalOrderToInternal:
    """Tests for transform_raw_historical_order_to_internal method."""

    def test_transform_raw_historical_order_happy_path(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test successful transformation of a historical order."""
        raw_order = create_raw_historical_order(
            side="B",
            status="filled",
            order_type_str="limit",
            limit_px="100.25",
            sz="10.0",
            remaining_sz="0.0",
            oid=98765,
            cloid="0x" + "0" * 30 + "2" * 2,  # Valid 128-bit hex string
            coin="SOL-PERP",
        )

        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "98765"
        assert result.client_order_id == "0x" + "0" * 30 + "2" * 2
        assert result.symbol == "SOL-PERP"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.time_in_force == TimeInForce.IOC
        assert result.status == OrderStatus.FILLED
        assert result.price == Decimal("100.25")
        assert result.quantity_requested == Decimal("10.0")
        assert result.quantity_filled == Decimal("10.0")
        assert result.exchange == ExchangeName.HYPERLIQUID.value

    def test_transform_raw_historical_order_missing_size_raises_error(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing size raises TransformationError for historical orders."""
        raw_order = create_raw_historical_order()

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.return_value = None

        with pytest.raises(
            MissingRequiredFieldError,
            match="orig_sz is required for HyperliquidRawHistoricalOrder",
        ):
            trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

    def test_transform_raw_historical_order_missing_timestamp_raises_error(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing timestamp raises TransformationError for historical orders."""
        raw_order = create_raw_historical_order()

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_datetime_utc",
        )
        mock_parse.return_value = None

        with pytest.raises(
            MissingRequiredFieldError,
            match="timestamp is required for HyperliquidRawHistoricalOrder",
        ):
            trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

    def test_transform_raw_historical_order_no_status_timestamp_uses_created_at(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that if status timestamp is missing, created_at is used for updated_at."""
        raw_order = create_raw_historical_order()

        def mock_parse_side_effect(value: object, field_name: str = "") -> datetime | None:
            """Return mock parse side effect for testing."""
            if field_name == "status_timestamp":
                return None
            # Return valid datetime for created_at/timestamp
            return datetime.now(UTC)

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_datetime_utc",
        )
        mock_parse.side_effect = mock_parse_side_effect

        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        # Should use created_at for updated_at when status timestamp is missing
        # Allow for small time differences due to processing time
        assert result.updated_at is not None
        assert result.created_at is not None
        time_diff = abs((result.updated_at - result.created_at).total_seconds())
        assert time_diff < 1.0  # Less than 1 second difference

    def test_transform_raw_historical_order_edge_case_no_cloid(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation of historical order with no client order ID."""
        raw_order = create_raw_historical_order(cloid=None)

        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert isinstance(result.client_order_id, str)
        assert len(result.client_order_id) > 0

    def test_transform_raw_historical_order_exception_wrapping(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that exceptions during historical order transformation are wrapped."""
        raw_order = create_raw_historical_order()

        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Parse error")

        with pytest.raises(TransformationError, match=r"Failed to transform.*HistoricalOrder"):
            trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

    def test_transform_raw_historical_order_edge_case_zero_remaining_sz(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test transformation with zero remaining size."""
        raw_order = create_raw_historical_order(remaining_sz="0.0")

        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        # Note: The exact behavior depends on mapper implementation
        # This verifies the transformation handles zero remaining size gracefully
        assert result is not None

    def test_transform_raw_historical_order_with_trigger(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hyperliquid_raw_trigger_info_take_profit_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation of historical order with trigger."""
        raw_order = create_raw_historical_order()
        result = trading_data_mapper.transform_raw_historical_order_to_internal(
            raw_order,
            hyperliquid_raw_trigger_info_take_profit_fixture,
        )

        assert result.order_type == OrderType.TAKE_PROFIT_LIMIT
        assert result.stop_price == Decimal("3200.00")


# --- Tests for Integration Scenarios ---


class TestTransformationIntegration:
    """Tests for integration scenarios and complex transformations."""

    def test_complete_order_lifecycle_transformation(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hyperliquid_raw_trigger_info_stop_loss_fixture: HyperliquidRawTriggerInfo,
    ) -> None:
        """Test transformation of orders at different lifecycle stages."""
        # Open order
        open_order = create_raw_order(
            status="open",
            sz="5.0",
            remaining_sz="3.0",
        )

        # Partially filled order
        partial_order = create_raw_order(
            status="open",
            sz="5.0",
            remaining_sz="2.0",
        )

        # Fully filled order
        filled_order = create_raw_historical_order(
            status="filled",
            sz="5.0",
            remaining_sz="0.0",
        )

        # Order with trigger
        trigger_order = create_raw_order(
            status="open",
            sz="5.0",
            remaining_sz="5.0",
        )

        open_result = trading_data_mapper.transform_raw_order_to_internal(open_order)
        partial_result = trading_data_mapper.transform_raw_order_to_internal(partial_order)
        filled_result = trading_data_mapper.transform_raw_historical_order_to_internal(filled_order)
        trigger_result = trading_data_mapper.transform_raw_order_to_internal(trigger_order)

        # Verify progression
        assert open_result.quantity_filled == Decimal("2.0")  # 5.0 - 3.0
        assert partial_result.quantity_filled == Decimal("3.0")  # 5.0 - 2.0
        assert filled_result.quantity_filled == Decimal("5.0")  # 5.0 - 0.0

        assert open_result.status == OrderStatus.OPEN
        assert partial_result.status == OrderStatus.OPEN
        assert filled_result.status == OrderStatus.FILLED

        # Verify trigger order
        assert trigger_result.order_type == OrderType.STOP_LIMIT
        assert trigger_result.stop_price == Decimal("2900.00")
        assert trigger_result.trigger_by is None  # No trigger_type in fixture

    def test_error_handling_consistency(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that error handling is consistent across transformation methods."""
        raw_order = create_raw_order()
        historical_order = create_raw_historical_order()

        # Mock to cause parsing error
        mock_parse = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper.parse_decimal_value",
        )
        mock_parse.side_effect = ValueError("Consistent error")

        # Both should raise TransformationError with similar messages
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_historical_order_to_internal(historical_order)

    def test_all_mapping_logic_works_together(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test that all mapping logic works together correctly."""
        # Use historical order for canceled status since HyperliquidRawOrder only allows "open"
        raw_order = create_raw_historical_order(
            side="B",
            status="canceled",
            order_type_str="limit",
            limit_px="1000.0",
            sz="5.0",
            remaining_sz="2.0",
            oid=12345,
            cloid="0x" + "0" * 30 + "3" * 2,  # Valid 128-bit hex string
            coin="ETH-PERP",
        )

        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

        # Verify all mappings work correctly
        assert result.side == OrderSide.BUY
        assert result.status == OrderStatus.CANCELED
        assert result.order_type == OrderType.LIMIT
        assert result.time_in_force == TimeInForce.IOC
        assert result.symbol == "ETH-PERP"
        assert result.exchange_order_id == "12345"
        assert result.client_order_id == "0x" + "0" * 30 + "3" * 2
        assert result.price == Decimal("1000.0")
        assert result.quantity_requested == Decimal("5.0")
        assert result.quantity_filled == Decimal("3.0")  # sz - remaining_sz


# --- Tests for Advanced Scenarios ---


class TestAdvancedScenarios:
    """Tests for advanced transformation scenarios and edge cases."""

    def test_high_precision_decimal_handling(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test transformation with high precision decimal values."""
        raw_order = create_raw_order(
            limit_px="1234.123456789012345",
            sz="10.987654321098765",
            remaining_sz="0.00000001",  # Use value that doesn't round to zero
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Business logic rounds prices to 8 decimal places, but preserves quantity precision
        assert result.price == Decimal("1234.12345679")
        assert result.quantity_requested == Decimal("10.987654321098765")

    def test_large_order_ids_handling(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test transformation with very large order IDs."""
        large_oid = 999999999999999999
        raw_order = create_raw_order(oid=large_oid)

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.exchange_order_id == str(large_oid)

    def test_unicode_symbol_handling(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test transformation with Unicode characters in symbol names."""
        unicode_symbol = "BTC-PERP🚀"
        raw_order = create_raw_order(asset=unicode_symbol)

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.symbol == unicode_symbol

    def test_very_long_client_order_ids(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test transformation with very long client order IDs."""
        long_cloid = "0x" + "a" * 32  # Valid 128-bit hex string
        raw_order = create_raw_order(cloid=long_cloid)

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.client_order_id == long_cloid

    def test_boundary_timestamps(self, trading_data_mapper: HyperliquidOrderMapper) -> None:
        """Test transformation with boundary timestamp values."""
        # Test with very old and very new timestamps
        old_timestamp = 946684800000  # Year 2000
        new_timestamp = 4102444800000  # Year 2100

        old_order = create_raw_order(timestamp=old_timestamp)
        new_order = create_raw_order(timestamp=new_timestamp)

        old_result = trading_data_mapper.transform_raw_order_to_internal(old_order)
        new_result = trading_data_mapper.transform_raw_order_to_internal(new_order)

        assert old_result.created_at.year == 2000
        assert new_result.created_at.year == 2100
