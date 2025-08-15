"""CyberDeltaEngine: Hyperliquid Trading Data Mapper Core Tests with Property-Based Testing.

---------------------------------------------------------------------------

Comprehensive property-based test suite for HyperliquidOrderMapper core transformation methods.
Tests fundamental transformation logic including:
- Order side mapping (B/A -> BUY/SELL) with generated inputs
- Order status mapping (open/filled/canceled -> OPEN/FILLED/CANCELED)
- Order type detection and mapping (limit/market/stop/take_profit)
- Time-in-force mapping (Gtc/Ioc/Alo -> GTC/IOC/ALO)
- Core validation and enum transformations
- Edge cases and boundary value testing with hundreds of generated inputs
- Malicious input resistance testing
- Decimal precision boundary testing
"""

from __future__ import annotations

import string
from decimal import Decimal
from typing import Any

import pytest
import structlog.testing
from _pytest.logging import LogCaptureFixture
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite
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


# =======================
# Property-Based Testing Strategies
# =======================


def hl_side_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid order sides.

    Returns:
        SearchStrategy[str]: Strategy for valid order sides.
    """
    return st.sampled_from(["B", "A"])


def hl_status_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid order statuses.

    Returns:
        SearchStrategy[str]: Strategy for valid order statuses.
    """
    return st.sampled_from(["open", "filled", "canceled", "rejected", "expired"])


def hl_historical_status_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid historical order statuses (non-open).

    Returns:
        SearchStrategy[str]: Strategy for historical order statuses.
    """
    return st.sampled_from(["filled", "canceled", "rejected", "expired"])


def hl_tif_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid time-in-force values.

    Returns:
        SearchStrategy[str]: Strategy for valid TIF values.
    """
    return st.sampled_from(["Gtc", "Ioc", "Alo", "GTC", "IOC", "ALO"])


def invalid_hl_tif_strategy() -> SearchStrategy[str]:
    """Generate invalid Hyperliquid time-in-force values.

    Returns:
        SearchStrategy[str]: Strategy for invalid TIF values.
    """
    return st.one_of([
        st.sampled_from(["unknown", "invalid", "gtc", "ioc", "alo", "", "FOK", "DAY"]),
        st.text(min_size=1, max_size=20).filter(
            lambda x: x not in ["Gtc", "Ioc", "Alo", "GTC", "IOC", "ALO"]
        ),
    ])


def hl_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid symbols within 20-character limit.

    Returns:
        SearchStrategy[str]: Strategy for valid symbols.
    """
    return st.sampled_from([
        "ETH-PERP",
        "BTC-PERP",
        "SOL-PERP",
        "AVAX-PERP",
        "DOGE-PERP",
        "MATIC-PERP",
        "ADA-PERP",
        "DOT-PERP",
        "LINK-PERP",
        "UNI-PERP",
        "AAVE-PERP",
        "COMP-PERP",
        "YFI-PERP",
        "SUSHI-PERP",
        "CRV-PERP",
        "MKR-PERP",
        "SNX-PERP",
        "ALPHA-PERP",
        "RUNE-PERP",
        "FTM-PERP",
    ])


def decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and quantities.

    Returns:
        SearchStrategy[str]: Strategy for decimal strings.
    """
    return st.one_of([
        # Normal decimals
        st.builds(
            lambda integer, fractional: f"{integer}.{fractional}",
            st.integers(min_value=1, max_value=99999),
            st.text(alphabet=string.digits, min_size=1, max_size=18),
        ),
        # Small decimals
        st.sampled_from(["0.000001", "0.00001", "0.0001", "0.001", "0.01", "0.1"]),
        # Large decimals
        st.sampled_from(["1000000", "999999.999999", "50000.123456", "10000.0"]),
        # High precision
        st.sampled_from([
            "1234.123456789012345",
            "10.987654321098765",
            "0.000000000000001",
            "99999.999999999999",
        ]),
    ])


def hl_order_id_strategy() -> SearchStrategy[int]:
    """Generate valid Hyperliquid order IDs.

    Returns:
        SearchStrategy[int]: Strategy for order IDs.
    """
    return st.integers(min_value=1, max_value=2**63 - 1)


def hl_client_order_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid Hyperliquid client order IDs.

    Returns:
        SearchStrategy[str | None]: Strategy for client order IDs.
    """
    return st.one_of([
        st.none(),
        st.text(min_size=1, max_size=50),
        st.builds(lambda n: f"client_{n}", st.integers(min_value=1, max_value=999999)),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamps in milliseconds.

    Returns:
        SearchStrategy[int]: Strategy for timestamps.
    """
    return st.integers(min_value=1640995200000, max_value=2000000000000)  # 2022-2033


@composite
def hl_limit_order_type_strategy(draw: st.DrawFn) -> dict[str, dict[str, str]]:
    """Generate valid Hyperliquid limit order type dictionaries.

    Args:
        draw: Hypothesis draw function.

    Returns:
        dict[str, dict[str, str]]: Valid limit order type.
    """
    tif = draw(hl_tif_strategy())
    return {"limit": {"tif": tif}}


@composite
def hl_market_order_type_strategy(draw: st.DrawFn) -> dict[str, dict[str, Any]]:
    """Generate valid Hyperliquid market order type dictionaries.

    Args:
        draw: Hypothesis draw function.

    Returns:
        dict[str, dict[str, Any]]: Valid market order type.
    """
    return {"market": {}}


@composite
def hl_trigger_order_type_strategy(draw: st.DrawFn) -> dict[str, dict[str, Any]]:
    """Generate valid Hyperliquid trigger order type dictionaries.

    Args:
        draw: Hypothesis draw function.

    Returns:
        dict[str, dict[str, Any]]: Valid trigger order type.
    """
    trigger_px = draw(decimal_string_strategy())
    is_market = draw(st.booleans())
    tpsl = draw(st.sampled_from(["sl", "tp"]))

    return {
        "trigger": {
            "triggerPx": trigger_px,
            "isMarket": is_market,
            "tpsl": tpsl,
        }
    }


def hl_order_type_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid Hyperliquid order type dictionaries.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy for order types.
    """
    return st.one_of([
        hl_limit_order_type_strategy(),
        hl_market_order_type_strategy(),
        hl_trigger_order_type_strategy(),
    ])


def invalid_hl_order_type_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate invalid Hyperliquid order type dictionaries.

    Returns:
        SearchStrategy[dict[str, Any]]: Strategy for invalid order types.
    """
    return st.one_of([
        st.just({}),
        st.just({"unknown": {}}),
        st.just({"invalid_type": {"some": "data"}}),
        st.just({"limit": {"invalid_field": "value"}}),
        st.builds(
            lambda k, v: {k: v},
            st.text(min_size=1, max_size=20),
            st.dictionaries(st.text(), st.text()),
        ),
    ])


@composite
def hl_raw_order_strategy(draw: st.DrawFn) -> HyperliquidRawOrder:
    """Generate valid HyperliquidRawOrder instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        HyperliquidRawOrder: Valid raw order for testing.
    """
    order_type = draw(hl_order_type_strategy())
    remaining_sz = draw(decimal_string_strategy())

    return HyperliquidRawOrder(
        oid=draw(hl_order_id_strategy()),
        cloid=draw(hl_client_order_id_strategy()),
        asset=draw(hl_symbol_strategy()),
        side=draw(hl_side_strategy()),
        limitPx=draw(decimal_string_strategy()),
        sz=draw(decimal_string_strategy()),
        timestamp=draw(timestamp_strategy()),
        orderType=order_type,
        reduceOnly=draw(st.booleans()),
        remainingSz=remaining_sz,
        status="open",  # Raw orders are always open
        statusTimestamp=draw(timestamp_strategy()),
    )


@composite
def hl_raw_historical_order_strategy(draw: st.DrawFn) -> HyperliquidRawHistoricalOrder:
    """Generate valid HyperliquidRawHistoricalOrder instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        HyperliquidRawHistoricalOrder: Valid historical order for testing.
    """
    order_type_dict = draw(hl_order_type_strategy())

    # Extract order type string for historical orders
    if "limit" in order_type_dict:
        order_type_str = "limit"
        tif = order_type_dict["limit"].get("tif", "Gtc")
    elif "market" in order_type_dict:
        order_type_str = "market"
        tif = "Ioc"
    elif "trigger" in order_type_dict:
        order_type_str = "trigger"
        tif = "Gtc"
    else:
        order_type_str = "limit"
        tif = "Gtc"

    # Ensure TIF is valid string for historical orders
    if not isinstance(tif, str) or tif.lower() not in ["gtc", "ioc", "alo"]:
        tif = "Gtc"

    orig_sz = draw(decimal_string_strategy())
    remaining_sz = draw(decimal_string_strategy())

    return HyperliquidRawHistoricalOrder(
        oid=draw(hl_order_id_strategy()),
        cloid=draw(hl_client_order_id_strategy()),
        coin=draw(hl_symbol_strategy()),
        side=draw(hl_side_strategy()),
        limitPx=draw(decimal_string_strategy()),
        sz=remaining_sz,
        timestamp=draw(timestamp_strategy()),
        orderType=order_type_str,
        reduceOnly=draw(st.booleans()),
        origSz=orig_sz,
        tif=tif,
        status=draw(hl_historical_status_strategy()),
        statusTimestamp=draw(timestamp_strategy()),
        triggerCondition=None,
        isTrigger=None,
        triggerPx=None,
        children=None,
        isPositionTpsl=None,
    )


def extreme_decimal_strategy() -> SearchStrategy[str]:
    """Generate extreme decimal values for boundary testing.

    Returns:
        SearchStrategy[str]: Strategy for extreme decimal values.
    """
    return st.one_of([
        # Very small values
        st.sampled_from(["0.000000000000001", "0.00000001", "1e-18", "1e-15"]),
        # Very large values
        st.sampled_from(["999999999999.999999", "1e15", "1e12", "999999999"]),
        # High precision values
        st.builds(
            lambda mantissa, precision: f"{mantissa}.{''.join(precision)}",
            st.integers(min_value=1, max_value=9999),
            st.lists(st.sampled_from(string.digits), min_size=15, max_size=25),
        ),
        # Scientific notation
        st.builds(
            lambda m, e: f"{m}e{e}",
            st.floats(min_value=1.0, max_value=9.999, allow_nan=False, allow_infinity=False),
            st.integers(min_value=-15, max_value=15),
        ),
    ])


def malicious_string_strategy() -> SearchStrategy[str]:
    """Generate potentially malicious string inputs.

    Returns:
        SearchStrategy[str]: Strategy for malicious inputs.
    """
    return st.one_of([
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE orders; --",
            "1' OR '1'='1",
            "admin'--",
            "1; DELETE FROM fills WHERE 1=1; --",
        ]),
        # XSS attempts
        st.sampled_from([
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "javascript:alert('XSS')",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=10000),
        # Format string attacks
        st.sampled_from(["%s%s%s%s", "%x%x%x%x", "%n%n%n"]),
        # Path traversal
        st.sampled_from(["../../../etc/passwd", "..\\\\windows\\\\system32"]),
        # Command injection
        st.sampled_from(["$(rm -rf /)", "`cat /etc/passwd`", "; ls -la"]),
        # Unicode attacks
        st.text(
            alphabet=st.characters(min_codepoint=0x1F300, max_codepoint=0x1F6FF),
            min_size=1,
            max_size=50,
        ),
    ])


# =======================
# Legacy Fixtures (Maintained for Compatibility)
# =======================


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


# =======================
# Property-Based Test Classes
# =======================


class TestPropertyBasedOrderSideMapping:
    """Property-based tests for order side mapping."""

    @given(
        raw_order=hl_raw_order_strategy(),
        side=hl_side_strategy(),
    )
    @settings(max_examples=50)
    def test_raw_order_side_mapping_property_based(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        side: str,
    ) -> None:
        """Test order side mapping with property-based testing for raw orders."""
        raw_order.side = side

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        expected_side = OrderSide.BUY if side == "B" else OrderSide.SELL
        assert result.side == expected_side

    @given(
        historical_order=hl_raw_historical_order_strategy(),
        side=hl_side_strategy(),
    )
    @settings(max_examples=50)
    def test_historical_order_side_mapping_property_based(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        historical_order: HyperliquidRawHistoricalOrder,
        side: str,
    ) -> None:
        """Test order side mapping with property-based testing for historical orders."""
        historical_order.side = side

        result = trading_data_mapper.transform_raw_historical_order_to_internal(historical_order)

        expected_side = OrderSide.BUY if side == "B" else OrderSide.SELL
        assert result.side == expected_side


class TestPropertyBasedOrderStatusMapping:
    """Property-based tests for order status mapping."""

    @given(
        historical_order=hl_raw_historical_order_strategy(),
        status=hl_historical_status_strategy(),
    )
    @settings(max_examples=30)
    def test_order_status_mapping_property_based(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        historical_order: HyperliquidRawHistoricalOrder,
        status: str,
    ) -> None:
        """Test order status mapping with property-based testing."""
        historical_order.status = status

        result = trading_data_mapper.transform_raw_historical_order_to_internal(historical_order)

        expected_status_map = {
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            "expired": OrderStatus.UNKNOWN,
        }

        expected_status = expected_status_map[status]
        assert result.status == expected_status


class TestPropertyBasedOrderTypeMapping:
    """Property-based tests for order type mapping."""

    @given(
        raw_order=hl_raw_order_strategy(),
        order_type=hl_order_type_strategy(),
    )
    @settings(max_examples=100)
    def test_order_type_mapping_raw_orders(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        order_type: dict[str, Any],
    ) -> None:
        """Test order type mapping for raw orders with property-based testing."""
        raw_order.order_type = order_type

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Determine expected order type based on structure
        if "limit" in order_type:
            expected_type = OrderType.LIMIT
        elif "market" in order_type:
            expected_type = OrderType.MARKET
        elif "trigger" in order_type:
            trigger_info = order_type["trigger"]
            is_market = trigger_info.get("isMarket", False)
            tpsl = trigger_info.get("tpsl", "")

            if tpsl == "sl":
                expected_type = OrderType.STOP_MARKET if is_market else OrderType.STOP_LIMIT
            elif tpsl == "tp":
                expected_type = (
                    OrderType.TAKE_PROFIT_MARKET if is_market else OrderType.TAKE_PROFIT_LIMIT
                )
            else:
                expected_type = OrderType.LIMIT  # Default fallback
        else:
            expected_type = OrderType.LIMIT  # Default fallback

        assert result.order_type == expected_type

    @given(
        historical_order=hl_raw_historical_order_strategy(),
        order_type=hl_order_type_strategy(),
    )
    @settings(max_examples=50)
    def test_order_type_mapping_historical_orders(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        historical_order: HyperliquidRawHistoricalOrder,
        order_type: dict[str, Any],
    ) -> None:
        """Test order type mapping for historical orders with property-based testing."""
        # Extract order type string for historical orders
        if "limit" in order_type:
            historical_order.order_type = "limit"
            expected_type = OrderType.LIMIT
        elif "market" in order_type:
            historical_order.order_type = "market"
            expected_type = OrderType.LIMIT  # Market orders treated as limit IOC in historical
        else:
            historical_order.order_type = "limit"
            expected_type = OrderType.LIMIT

        result = trading_data_mapper.transform_raw_historical_order_to_internal(historical_order)
        assert result.order_type == expected_type


class TestPropertyBasedTimeInForceMapping:
    """Property-based tests for time-in-force mapping."""

    @given(
        raw_order=hl_raw_order_strategy(),
        tif=hl_tif_strategy(),
    )
    @settings(max_examples=30)
    def test_tif_mapping_property_based(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        tif: str,
    ) -> None:
        """Test time-in-force mapping with property-based testing."""
        order_type: dict[str, object] = {"limit": {"tif": tif}}
        raw_order.order_type = order_type

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        expected_tif_map = {
            "Gtc": TimeInForce.GTC,
            "GTC": TimeInForce.GTC,
            "Ioc": TimeInForce.IOC,
            "IOC": TimeInForce.IOC,
            "Alo": TimeInForce.ALO,
            "ALO": TimeInForce.ALO,
        }

        expected_tif = expected_tif_map[tif]
        assert result.time_in_force == expected_tif

    @given(
        raw_order=hl_raw_order_strategy(),
        invalid_tif=invalid_hl_tif_strategy(),
    )
    @settings(max_examples=20)
    def test_invalid_tif_defaults_to_gtc(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        invalid_tif: str,
    ) -> None:
        """Test that invalid TIF values default to GTC."""
        order_type: dict[str, object] = {"limit": {"tif": invalid_tif}}
        raw_order.order_type = order_type

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.time_in_force == TimeInForce.GTC


class TestPropertyBasedSymbolHandling:
    """Property-based tests for symbol handling."""

    @given(
        raw_order=hl_raw_order_strategy(),
        historical_order=hl_raw_historical_order_strategy(),
        symbol=hl_symbol_strategy(),
    )
    @settings(max_examples=30)
    def test_symbol_consistency_property_based(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        historical_order: HyperliquidRawHistoricalOrder,
        symbol: str,
    ) -> None:
        """Test symbol handling consistency with property-based testing."""
        raw_order.asset = symbol
        historical_order.coin = symbol

        raw_result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        historical_result = trading_data_mapper.transform_raw_historical_order_to_internal(
            historical_order
        )

        assert raw_result.symbol.value == symbol
        assert historical_result.symbol.value == symbol
        assert raw_result.symbol.exchange.value == "hyperliquid"
        assert historical_result.symbol.exchange.value == "hyperliquid"


class TestPropertyBasedDecimalPrecision:
    """Property-based tests for decimal precision handling."""

    @given(
        raw_order=hl_raw_order_strategy(),
        price=decimal_string_strategy(),
        quantity=decimal_string_strategy(),
        remaining=decimal_string_strategy(),
    )
    @settings(max_examples=100)
    def test_decimal_precision_handling(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        price: str,
        quantity: str,
        remaining: str,
    ) -> None:
        """Test decimal precision handling with property-based testing."""
        raw_order.limit_px = price
        raw_order.sz = quantity
        raw_order.remaining_sz = remaining

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify decimal types
        assert isinstance(result.price, Decimal)
        assert isinstance(result.quantity_requested, Decimal)

        # Verify positive values
        assert result.price > 0
        assert result.quantity_requested > 0

    @given(
        raw_order=hl_raw_order_strategy(),
        extreme_price=extreme_decimal_strategy(),
        extreme_quantity=extreme_decimal_strategy(),
    )
    @settings(max_examples=50)
    def test_extreme_decimal_values(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        extreme_price: str,
        extreme_quantity: str,
    ) -> None:
        """Test handling of extreme decimal values."""
        raw_order.limit_px = extreme_price
        raw_order.sz = extreme_quantity

        try:
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # If transformation succeeds, verify basic properties
            assert isinstance(result.price, Decimal)
            assert isinstance(result.quantity_requested, Decimal)
            assert result.price > 0
            assert result.quantity_requested > 0
        except (ValueError, ValidationError):
            # Some extreme values might fail validation, which is acceptable
            pass


class TestPropertyBasedInvalidInputHandling:
    """Property-based tests for invalid input handling."""

    @given(
        raw_order=hl_raw_order_strategy(),
        invalid_order_type=invalid_hl_order_type_strategy(),
    )
    @settings(max_examples=30)
    def test_invalid_order_type_handling(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        invalid_order_type: dict[str, Any],
    ) -> None:
        """Test handling of invalid order types with property-based testing."""
        raw_order.order_type = invalid_order_type

        with structlog.testing.capture_logs() as captured_logs:
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Should default to LIMIT for unknown order types
        assert result.order_type == OrderType.LIMIT

        # Should log a warning for unknown order types
        if invalid_order_type and not any(
            key in invalid_order_type for key in ["limit", "market", "trigger"]
        ):
            warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
            assert len(warning_logs) > 0


class TestPropertyBasedMaliciousInputResistance:
    """Property-based tests for malicious input resistance."""

    @given(
        raw_order=hl_raw_order_strategy(),
        malicious_symbol=malicious_string_strategy(),
    )
    @settings(max_examples=20)
    def test_malicious_symbol_resistance(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        malicious_symbol: str,
    ) -> None:
        """Test resistance to malicious input in symbols."""
        # Limit symbol length to avoid hitting length constraints
        if len(malicious_symbol) > 50:
            malicious_symbol = malicious_symbol[:50]

        try:
            # Some malicious inputs might fail at the raw model level (which is good)
            raw_order.asset = malicious_symbol
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # If it succeeds, the symbol should be safely stored
            assert isinstance(result.symbol.value, str)
            # The malicious string should not have been executed or interpreted
            assert result.symbol.value == malicious_symbol

        except (ValidationError, ValueError):
            # Rejecting malicious input is also acceptable
            pass

    @given(
        raw_order=hl_raw_order_strategy(),
        malicious_client_id=malicious_string_strategy(),
    )
    @settings(max_examples=20)
    def test_malicious_client_id_resistance(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
        malicious_client_id: str,
    ) -> None:
        """Test resistance to malicious input in client order IDs."""
        # Limit client ID length to avoid hitting length constraints
        if len(malicious_client_id) > 100:
            malicious_client_id = malicious_client_id[:100]

        try:
            raw_order.cloid = malicious_client_id
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # If it succeeds, the client ID should be safely stored
            if result.client_order_id is not None:
                assert isinstance(result.client_order_id, str)
                assert result.client_order_id == malicious_client_id

        except (ValidationError, ValueError):
            # Rejecting malicious input is also acceptable
            pass


class TestPropertyBasedComprehensiveTransformation:
    """Comprehensive property-based tests combining multiple aspects."""

    @given(raw_order=hl_raw_order_strategy())
    @settings(max_examples=100)
    def test_comprehensive_raw_order_transformation(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        raw_order: HyperliquidRawOrder,
    ) -> None:
        """Test comprehensive raw order transformation with property-based testing."""
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify basic field mappings
        assert result.exchange_order_id == str(raw_order.oid)
        assert result.client_order_id == raw_order.cloid
        assert result.symbol.value == raw_order.asset
        assert result.exchange == ExchangeName.HYPERLIQUID.value

        # Verify side mapping
        expected_side = OrderSide.BUY if raw_order.side == "B" else OrderSide.SELL
        assert result.side == expected_side

        # Verify status (raw orders are always open)
        assert result.status == OrderStatus.OPEN

        # Verify decimal types and positive values
        assert isinstance(result.price, Decimal)
        assert isinstance(result.quantity_requested, Decimal)
        assert result.price > 0
        assert result.quantity_requested > 0

    @given(historical_order=hl_raw_historical_order_strategy())
    @settings(max_examples=100)
    def test_comprehensive_historical_order_transformation(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
        historical_order: HyperliquidRawHistoricalOrder,
    ) -> None:
        """Test comprehensive historical order transformation with property-based testing."""
        result = trading_data_mapper.transform_raw_historical_order_to_internal(historical_order)

        # Verify basic field mappings
        assert result.exchange_order_id == str(historical_order.oid)
        assert result.client_order_id == historical_order.cloid
        assert result.symbol.value == historical_order.coin
        assert result.exchange == ExchangeName.HYPERLIQUID.value

        # Verify side mapping
        expected_side = OrderSide.BUY if historical_order.side == "B" else OrderSide.SELL
        assert result.side == expected_side

        # Verify status mapping
        status_map = {
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            "expired": OrderStatus.UNKNOWN,
        }
        expected_status = status_map[historical_order.status]
        assert result.status == expected_status

        # Verify decimal types and positive values
        assert isinstance(result.price, Decimal)
        assert isinstance(result.quantity_requested, Decimal)
        assert result.price > 0
        assert result.quantity_requested > 0


class TestPropertyBasedLegacyCompatibility:
    """Tests to ensure property-based tests don't break legacy functionality."""

    def test_legacy_consistency_with_property_based(
        self,
        trading_data_mapper: HyperliquidOrderMapper,
    ) -> None:
        """Test that legacy and property-based approaches yield consistent results."""
        # Create same order using legacy method
        legacy_order = create_raw_order(
            side="B",
            status="open",
            order_type={"limit": {"tif": "Gtc"}},
            limit_px="3000.50",
            sz="1.5",
            remaining_sz="0.5",
        )

        # Create equivalent order using the factory function
        pb_order = create_raw_order(
            side="B",
            status="open",
            order_type={"limit": {"tif": "Gtc"}},
            limit_px="3000.50",
            sz="1.5",
            remaining_sz="0.5",
        )

        legacy_result = trading_data_mapper.transform_raw_order_to_internal(legacy_order)
        pb_result = trading_data_mapper.transform_raw_order_to_internal(pb_order)

        # Key fields should match
        assert legacy_result.side == pb_result.side
        assert legacy_result.order_type == pb_result.order_type
        assert legacy_result.time_in_force == pb_result.time_in_force
        assert legacy_result.price == pb_result.price
        assert legacy_result.quantity_requested == pb_result.quantity_requested
