"""CyberDeltaEngine: Hyperliquid Trading Data Mapper Core Tests.

-----------------------------------------------------------

Comprehensive test suite for HyperliquidOrderMapper core transformation methods.
Tests fundamental transformation logic including:
- Order side mapping (B/A -> BUY/SELL)
- Order status mapping (open/filled/canceled -> OPEN/FILLED/CANCELED)
- Order type detection and mapping (limit/market/stop/take_profit)
- Time-in-force mapping (Gtc/Ioc/Alo -> GTC/IOC/ALO)
- Core validation and enum transformations
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
import structlog.testing
from _pytest.logging import LogCaptureFixture
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import HyperliquidRawHistoricalOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.parsing import EmptyStringError


logger = get_logger(__name__)


# --- Fixtures ---


@pytest.fixture
def trading_data_mapper() -> HyperliquidOrderMapper:
    """Provide an instance of HyperliquidOrderMapper.

    Returns:
        HyperliquidOrderMapper: Mapper instance for trading data transformations.
    """
    return HyperliquidOrderMapper()


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
    """Create a HyperliquidRawOrder with customizable parameters.

    Returns:
        HyperliquidRawOrder: Raw order object with the specified parameters.
    """
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
    sz: str = "2.5",  # remaining size - should be smaller than original
    remaining_sz: str = "10.0",  # original size - should be larger
    oid: int = 98765,
    cloid: str | None = None,  # Use None by default since cloid is optional
    asset: str = "SOL-PERP",
    timestamp: int = 1640995200000,  # Fixed timestamp for consistency
) -> HyperliquidRawHistoricalOrder:
    """Create a HyperliquidRawHistoricalOrder with customizable parameters.

    Returns:
        HyperliquidRawHistoricalOrder: Raw historical order object with the specified parameters.
    """
    if order_type is None:
        order_type = {"limit": {"tif": "Ioc"}}

    # Convert order_type dict to appropriate string format for historical orders
    if "limit" in order_type:
        order_type_str = "limit"
        tif_value = order_type["limit"].get("tif", "Gtc")
        # Handle non-string TIF values that should default to GTC
        if not isinstance(tif_value, str) or tif_value.lower() not in ["gtc", "ioc", "alo"]:
            tif = "Gtc"
        else:
            tif = tif_value
    elif "market" in order_type:
        order_type_str = "market"
        tif = "Ioc"  # Market orders default to IOC
    else:
        order_type_str = next(iter(order_type.keys())) if order_type else "limit"
        tif = "Gtc"  # Default to GTC for unknown order types

    return HyperliquidRawHistoricalOrder(
        oid=oid,
        cloid=cloid,
        coin=asset,  # Changed from asset to coin
        side=side,
        limitPx=limit_px,
        sz=sz,
        timestamp=timestamp,
        orderType=order_type_str,  # Use the string format
        reduceOnly=False,
        origSz=remaining_sz,  # Use remaining_sz parameter as original size
        tif=tif,  # Use the extracted TIF
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
    """Provide a valid HyperliquidRawTriggerInfo for a stop loss.

    Returns:
        HyperliquidRawTriggerInfo: Trigger info configured for stop loss orders.
    """
    return HyperliquidRawTriggerInfo(
        triggerPx="2900.00",
        isMarket=True,
        tpsl="sl",
    )


@pytest.fixture
def hyperliquid_raw_trigger_info_take_profit_fixture() -> HyperliquidRawTriggerInfo:
    """Provide a valid HyperliquidRawTriggerInfo for a take profit.

    Returns:
        HyperliquidRawTriggerInfo: Trigger info configured for take profit orders.
    """
    return HyperliquidRawTriggerInfo(
        triggerPx="3200.00",
        isMarket=False,
        tpsl="tp",
    )


# --- Parameterized Tests for Order Side Mapping ---


@pytest.mark.parametrize(
    ("hl_side", "expected_side"),
    [
        ("B", OrderSide.BUY),
        ("A", OrderSide.SELL),
    ],
)
class TestOrderSideMapping:
    """Tests for order side mapping through public transformation methods."""

    def test_raw_order_side_mapping(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hl_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via raw order transformation."""
        raw_order = create_raw_order(side=hl_side)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.side == expected_side

    def test_historical_order_side_mapping(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hl_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via historical order transformation."""
        raw_order = create_raw_historical_order(side=hl_side)
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.side == expected_side


@pytest.mark.parametrize(
    ("invalid_side", "expected_exception"),
    [
        ("X", ValidationError),
        ("", EmptyStringError),
        ("b", ValidationError),
        ("buy", ValidationError),
        ("sell", ValidationError),
        ("invalid", ValidationError),
    ],
)
def test_invalid_order_side_raises_error(
    trading_data_mapper: HyperliquidOrderMapper,
    invalid_side: str,
    expected_exception: type,
) -> None:
    """Test that invalid order sides raise appropriate validation errors at the Raw model level."""
    # Raw model validation should catch invalid sides before they reach the mapper
    with pytest.raises(expected_exception):
        create_raw_order(side=invalid_side)


# --- Parameterized Tests for Order Status Mapping ---


@pytest.mark.parametrize(
    ("hl_status", "expected_status"),
    [
        ("open", OrderStatus.OPEN),
        ("filled", OrderStatus.FILLED),
        ("canceled", OrderStatus.CANCELED),
        ("rejected", OrderStatus.REJECTED),
        ("expired", OrderStatus.UNKNOWN),
    ],
)
class TestOrderStatusMapping:
    """Tests for order status mapping through public transformation methods."""

    def test_raw_order_status_mapping(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hl_status: str,
        expected_status: OrderStatus,
    ) -> None:
        """Test order status mapping via raw order transformation."""
        if hl_status == "open":
            # Use raw order for open status
            raw_order = create_raw_order(status=hl_status)
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        else:
            # Use historical order for non-open statuses since HyperliquidRawOrder
            # only allows "open"
            historical_order = create_raw_historical_order(status=hl_status)
            result = trading_data_mapper.transform_raw_historical_order_to_internal(
                historical_order,
            )

        assert result.status == expected_status

    def test_historical_order_status_mapping(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        hl_status: str,
        expected_status: OrderStatus,
    ) -> None:
        """Test order status mapping via historical order transformation."""
        raw_order = create_raw_historical_order(status=hl_status)
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.status == expected_status


# --- Parameterized Tests for Order Type Mapping ---


@pytest.mark.parametrize(
    ("order_type_dict", "expected_type"),
    [
        ({"limit": {"tif": "Gtc"}}, OrderType.LIMIT),
        (
            {"market": {}},
            OrderType.MARKET,
        ),  # Business logic correctly maps market orders for raw orders
    ],
)
class TestOrderTypeMapping:
    """Tests for order type mapping through public transformation methods."""

    def test_order_type_mapping_via_raw_order(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        order_type_dict: dict[str, Any],
        expected_type: OrderType,
    ) -> None:
        """Test order type mapping via raw order transformation."""
        raw_order = create_raw_order(order_type=order_type_dict)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.order_type == expected_type

    def test_order_type_mapping_via_historical_order(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        order_type_dict: dict[str, Any],
        expected_type: OrderType,
    ) -> None:
        """Test order type mapping via historical order transformation."""
        # Historical orders handle types differently than raw orders
        if "market" in order_type_dict:
            # For historical orders, market orders are treated as limit IOC
            expected_type = OrderType.LIMIT

        raw_order = create_raw_historical_order(order_type=order_type_dict)
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.order_type == expected_type


@pytest.mark.parametrize(
    ("order_type_dict", "trigger_tpsl", "expected_type"),
    [
        ({"limit": {"tif": "Gtc"}}, None, OrderType.LIMIT),
        ({"market": {}}, None, OrderType.MARKET),
        (
            {"trigger": {"triggerPx": "2950.0", "isMarket": False, "tpsl": "sl"}},
            None,
            OrderType.STOP_LIMIT,
        ),
        (
            {"trigger": {"triggerPx": "3050.0", "isMarket": False, "tpsl": "tp"}},
            None,
            OrderType.TAKE_PROFIT_LIMIT,
        ),
        (
            {"trigger": {"triggerPx": "2950.0", "isMarket": True, "tpsl": "sl"}},
            None,
            OrderType.STOP_MARKET,
        ),
        (
            {"trigger": {"triggerPx": "3050.0", "isMarket": True, "tpsl": "tp"}},
            None,
            OrderType.TAKE_PROFIT_MARKET,
        ),
    ],
)
def test_order_type_mapping_with_triggers(
    trading_data_mapper: HyperliquidOrderMapper,
    order_type_dict: dict[str, Any],
    trigger_tpsl: str | None,
    expected_type: OrderType,
) -> None:
    """Test order type mapping with and without triggers."""
    raw_order = create_raw_order(order_type=order_type_dict)

    result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
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
    trading_data_mapper: HyperliquidOrderMapper,
    unknown_order_type: dict[str, Any],
    caplog: LogCaptureFixture,
) -> None:
    """Test that unknown order types default to LIMIT and log a warning."""
    raw_order = create_raw_order(order_type=unknown_order_type)

    with structlog.testing.capture_logs() as captured_logs:
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

    # Should default to LIMIT
    assert result.order_type == OrderType.LIMIT

    # Should log a warning about unknown order type in structured logs
    warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
    assert len(warning_logs) > 0, "Expected at least one warning log"

    # Check for the specific warning about unknown order type
    unknown_type_logs = [log for log in warning_logs if "Unknown orderType structure" in str(log)]
    assert len(unknown_type_logs) > 0, f"Expected unknown order type logs, got: {captured_logs}"


# --- Parameterized Tests for Time-in-Force Mapping ---


@pytest.mark.parametrize(
    ("order_type_dict", "expected_tif"),
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
class TestTimeInForceMapping:
    """Tests for time-in-force mapping through public transformation methods."""

    def test_time_in_force_mapping_via_raw_order(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        order_type_dict: dict[str, Any],
        expected_tif: TimeInForce,
    ) -> None:
        """Test time-in-force mapping via raw order transformation."""
        raw_order = create_raw_order(order_type=order_type_dict)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.time_in_force == expected_tif

    def test_time_in_force_mapping_via_historical_order(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        order_type_dict: dict[str, Any],
        expected_tif: TimeInForce,
    ) -> None:
        """Test time-in-force mapping via historical order transformation."""
        # For historical orders, the business logic differs from raw orders:
        # - Market orders are treated as limit IOC
        # - Empty limit orders and unknown types default to GTC
        # - Invalid/unknown TIF values default to GTC
        if "market" in order_type_dict:
            expected_tif = TimeInForce.IOC
        elif "limit" in order_type_dict:
            tif = order_type_dict["limit"].get("tif", "")
            if isinstance(tif, str) and tif.lower() in ["gtc", "ioc", "alo"]:
                # Keep the expected TIF as is for valid values
                pass
            else:
                # For empty, unknown, or non-string TIF values, business logic defaults to GTC
                expected_tif = TimeInForce.GTC

        raw_order = create_raw_historical_order(order_type=order_type_dict)
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)
        assert result.time_in_force == expected_tif


# --- Tests for Core Validation Logic ---


class TestCoreValidationLogic:
    """Tests for core validation logic in trading data transformations."""

    def test_consistent_transformation_across_methods(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test that transformation methods produce consistent results."""
        # Use historical order for filled status since HyperliquidRawOrder only allows "open"
        # For historical orders: sz=remaining_size, origSz=original_size
        # For filled orders: sz="0.0", origSz="5.0" so quantity_filled = 5.0 - 0.0 = 5.0
        raw_order = create_raw_historical_order(
            side="B",
            status="filled",
            order_type={"limit": {"tif": "Gtc"}},
            limit_px="1000.0",
            sz="0.0",  # remaining size for filled order
            remaining_sz="5.0",  # This will be used as origSz
        )

        # Transform using historical order method
        result = trading_data_mapper.transform_raw_historical_order_to_internal(raw_order)

        # Verify consistent field mapping
        assert result.side == OrderSide.BUY
        assert result.status == OrderStatus.FILLED
        assert result.order_type == OrderType.LIMIT
        assert result.time_in_force == TimeInForce.GTC
        assert result.price == Decimal("1000.0")
        assert result.quantity_requested == Decimal("5.0")
        assert result.quantity_filled == Decimal("5.0")

    def test_symbol_consistency_across_transformations(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test that symbol names are consistently handled across different transformations."""
        # Use symbols that comply with 20-character domain model limit
        test_symbols = [
            "ETH-PERP",
            "BTC-PERP",
            "SOL-PERP",
            "AVAX-PERP",
            "LONGNAME-PERP",  # Updated to stay within 20-char limit
        ]

        for symbol in test_symbols:
            raw_order = create_raw_order(asset=symbol)
            historical_order = create_raw_historical_order(asset=symbol)

            raw_result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
            historical_result = trading_data_mapper.transform_raw_historical_order_to_internal(
                historical_order,
            )

            # Compare ExchangeSymbol domain objects properly
            assert raw_result.symbol.value == symbol
            assert historical_result.symbol.value == symbol
            assert raw_result.symbol.exchange.value == "hyperliquid"
            assert historical_result.symbol.exchange.value == "hyperliquid"

    def test_decimal_precision_handling(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test that decimal precision is maintained consistently."""
        high_precision_price = "1234.123456789012345"
        high_precision_size = "10.987654321098765"

        raw_order = create_raw_order(
            limit_px=high_precision_price,
            sz=high_precision_size,
            remaining_sz="0.000000000000001",
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Business logic: price fields rounded to 8 decimal places, quantities preserve precision
        assert result.price == Decimal("1234.12345679")
        assert result.quantity_requested == Decimal("10.987654321098765")

    def test_exchange_assignment_consistency(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test that exchange name is consistently assigned."""
        raw_order = create_raw_order()
        historical_order = create_raw_historical_order()

        raw_result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        historical_result = trading_data_mapper.transform_raw_historical_order_to_internal(
            historical_order,
        )

        assert raw_result.exchange == ExchangeName.HYPERLIQUID.value
        assert historical_result.exchange == ExchangeName.HYPERLIQUID.value
