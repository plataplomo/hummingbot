"""CyberDeltaEngine: Backpack Trading Data Mapper Core Tests with Property-Based Testing.

---------------------------------------------------------

Comprehensive property-based test suite for BackpackTradingDataMapper core transformations.
Tests fundamental transformation methods and mapping logic including:
- Order side, status, type, and time-in-force mappings
- Raw order to internal order transformations
- Order data to internal order transformations
- Core business logic validation
- Edge cases and security boundaries
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.config.structlog_config import get_logger
from tests.common_symbols import ADA_USDC_BP, BTC_USDC_BP, DOGE_USDC_BP, ETH_USDC_BP, SOL_USDC_BP


# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
import string

from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.common import TransformationError
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Order


logger = get_logger(__name__)


# =======================
# Helper Functions
# =======================


def _format_decimal_string(i: int, d: str) -> str:
    """Format integer and decimal string into a decimal representation.

    Args:
        i: Integer part of the decimal
        d: Decimal part as string

    Returns:
        Formatted decimal string
    """
    return f"{i}.{d}"


def _format_high_precision_decimal(i: int, d: list[str]) -> str:
    """Format integer and digit list into high precision decimal.

    Args:
        i: Integer part of the decimal
        d: List of digit strings for decimal part

    Returns:
        Formatted high precision decimal string
    """
    return f"{i}.{''.join(d)}"


def _datetime_to_isoformat(dt: datetime) -> str:
    """Convert datetime to ISO format string.

    Args:
        dt: Datetime object to convert

    Returns:
        ISO format string representation
    """
    return dt.isoformat()


def _create_symbol_dict(s: str) -> dict[str, str]:
    """Create symbol dictionary with value key.

    Args:
        s: Symbol string value

    Returns:
        Dictionary with symbol value
    """
    return {"value": s}


# =======================
# Strategy Builders
# =======================


def bp_side_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order sides.

    Returns:
        SearchStrategy[str]: Strategy for Backpack side strings.
    """
    return st.one_of([
        st.sampled_from(["Buy", "Sell", "Bid", "Ask"]),
        # Case variations
        st.sampled_from(["buy", "sell", "bid", "ask", "BUY", "SELL", "BID", "ASK"]),
    ])


def bp_status_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order statuses.

    Returns:
        SearchStrategy[str]: Strategy for Backpack status strings.
    """
    return st.sampled_from([
        "NEW",
        "FILLED",
        "CANCELLED",
        "REJECTED",
        "PARTIALLY_FILLED",
        "EXPIRED",
        "PENDING",
    ])


def bp_order_type_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order types.

    Returns:
        SearchStrategy[str]: Strategy for Backpack order type strings.
    """
    return st.sampled_from([
        "LIMIT",
        "MARKET",
        "TAKE_PROFIT",
        "STOP",
        "TRAILING_STOP",
    ])


def bp_time_in_force_strategy() -> SearchStrategy[str | None]:
    """Generate valid Backpack time in force values.

    Returns:
        SearchStrategy[str | None]: Strategy for time in force strings.
    """
    return st.one_of([
        st.none(),
        st.sampled_from(["GTC", "IOC", "FOK", "gtc", "ioc", "fok"]),
    ])


def decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and quantities.

    Returns:
        SearchStrategy[str]: Strategy for decimal strings.
    """
    return st.one_of([
        # Normal values
        st.builds(
            _format_decimal_string,
            st.integers(min_value=0, max_value=999999),
            st.text(alphabet=string.digits, min_size=1, max_size=8),
        ),
        # High precision values
        st.builds(
            _format_high_precision_decimal,
            st.integers(min_value=0, max_value=999),
            st.lists(st.sampled_from(string.digits), min_size=10, max_size=18),
        ),
        # Edge cases
        st.sampled_from([
            "0.0",
            "0.00",
            "0.001",
            "0.0001",
            "1.0",
            "10.0",
            "100.0",
            "1000.0",
            "0.123456789012345",
            "999999.999999999",
        ]),
    ])


def symbol_string_strategy() -> SearchStrategy[str]:
    """Generate valid symbol strings.

    Returns:
        SearchStrategy[str]: Strategy for symbol strings.
    """
    return st.sampled_from([
        SOL_USDC_BP.value,
        BTC_USDC_BP.value,
        ETH_USDC_BP.value,
        ADA_USDC_BP.value,
        DOGE_USDC_BP.value,
    ])


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid order ID strings.

    Returns:
        SearchStrategy[str]: Strategy for order IDs.
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
        min_size=1,
        max_size=50,
    )


def client_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client ID strings.

    Returns:
        SearchStrategy[str | None]: Strategy for client IDs.
    """
    return st.one_of([
        st.none(),
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd", "Pc"]),
            min_size=1,
            max_size=50,
        ),
    ])


def timestamp_string_strategy() -> SearchStrategy[str]:
    """Generate valid ISO timestamp strings.

    Returns:
        SearchStrategy[str]: Strategy for timestamp strings.
    """
    return st.builds(
        _datetime_to_isoformat,
        st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 1, 1),
            timezones=st.just(UTC),
        ),
    )


@composite
def raw_order_strategy(draw: st.DrawFn) -> BackpackRawOrderResponse:
    """Generate valid BackpackRawOrderResponse instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawOrderResponse: Valid raw order response.
    """
    order_type = draw(bp_order_type_strategy())

    # Determine if price is required
    needs_price = order_type in ["LIMIT", "TAKE_PROFIT", "STOP", "TRAILING_STOP"]
    price = draw(decimal_string_strategy()) if needs_price else None

    # Determine if trigger price is needed
    needs_trigger = order_type in ["STOP", "TRAILING_STOP"]
    trigger_price = draw(decimal_string_strategy()) if needs_trigger else None

    created_at = draw(timestamp_string_strategy())
    updated_at = draw(timestamp_string_strategy())

    return BackpackRawOrderResponse(
        id=draw(order_id_strategy()),
        clientId=draw(client_id_strategy()),
        relatedOrderId=None,
        symbol=draw(symbol_string_strategy()),
        side=draw(bp_side_strategy()),
        orderType=order_type,
        status=draw(bp_status_strategy()),
        quantity=draw(decimal_string_strategy()),
        executedQuantity=draw(st.one_of(st.none(), decimal_string_strategy())),
        executedQuoteQuantity=None,
        price=price,
        triggerPrice=trigger_price,
        avgFillPrice=draw(st.one_of(st.none(), decimal_string_strategy())),
        triggerBy=None,
        timeInForce=draw(bp_time_in_force_strategy()),
        reduceOnly=draw(st.booleans()),
        postOnly=draw(st.booleans()),
        selfTradePrevention=None,
        createdAt=created_at,
        updatedAt=updated_at,
        triggeredAt=draw(st.one_of(st.none(), timestamp_string_strategy())),
        expiryReason=None,
        origin=None,
    )


# =======================
# Fixtures
# =======================


@pytest.fixture
def trading_data_mapper() -> BackpackOrderMapper:
    """Provide an instance of BackpackOrderMapper.

    Returns:
        BackpackOrderMapper: Instance of the order mapper for testing.
    """
    return BackpackOrderMapper()


@pytest.fixture
def base_timestamp() -> str:
    """Provide a consistent timestamp string for tests.

    Returns:
        str: ISO format timestamp string for test consistency.
    """
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
    symbol: str = SOL_USDC_BP.value,
    time_in_force: str | None = "GTC",
    created_at: str | None = None,
    updated_at: str | None = None,
    avg_fill_price: str | None = None,
) -> BackpackRawOrderResponse:
    """Create a BackpackRawOrderResponse with customizable parameters.

    Returns:
        BackpackRawOrderResponse: A raw order response instance with the specified parameters.
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


# =======================
# Property-Based Tests
# =======================


class TestOrderSideMapping:
    """Property-based tests for order side mapping."""

    @given(side=bp_side_strategy())
    def test_valid_side_mapping(
        self,
        trading_data_mapper: BackpackOrderMapper,
        side: str,
    ) -> None:
        """Test that all valid sides map correctly."""
        raw_order = create_raw_order(side=side)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Check mapping
        if side.upper() in ["BUY", "BID"]:
            assert result.side == OrderSide.BUY
        elif side.upper() in ["SELL", "ASK"]:
            assert result.side == OrderSide.SELL
        else:
            # Unknown sides should still be handled
            assert result.side in [OrderSide.BUY, OrderSide.SELL]

    @given(
        invalid_side=st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu"]),
            min_size=1,
            max_size=10,
        ).filter(lambda s: s.upper() not in ["BUY", "SELL", "BID", "ASK"])
    )
    def test_invalid_side_handling(
        self,
        trading_data_mapper: BackpackOrderMapper,
        invalid_side: str,
    ) -> None:
        """Test that invalid sides raise TransformationError."""
        with pytest.raises(TransformationError) as exc_info:
            trading_data_mapper.transform_order_data_to_internal(
                order_id="12345",
                symbol=SOL_USDC_BP,
                side=invalid_side,
                order_type="LIMIT",
                status="NEW",
                quantity="1.0",
                price="100.0",
            )
        assert "Unknown order side" in str(exc_info.value)

    # Legacy parametrized tests
    @pytest.mark.parametrize(
        ("bp_side", "expected_side"),
        [
            ("Buy", OrderSide.BUY),
            ("Sell", OrderSide.SELL),
            ("buy", OrderSide.BUY),
            ("sell", OrderSide.SELL),
            ("Bid", OrderSide.BUY),
            ("Ask", OrderSide.SELL),
        ],
    )
    def test_raw_order_side_mapping(
        self,
        trading_data_mapper: BackpackOrderMapper,
        bp_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order side mapping via raw order transformation."""
        raw_order = create_raw_order(side=bp_side)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.side == expected_side


class TestOrderStatusMapping:
    """Property-based tests for order status mapping."""

    @given(status=bp_status_strategy())
    def test_status_mapping(
        self,
        trading_data_mapper: BackpackOrderMapper,
        status: str,
    ) -> None:
        """Test that all statuses map to valid OrderStatus."""
        raw_order = create_raw_order(status=status)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Check mapping
        expected_mapping = {
            "NEW": OrderStatus.OPEN,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "EXPIRED": OrderStatus.UNKNOWN,
            "PENDING": OrderStatus.OPEN,
        }

        expected = expected_mapping.get(status, OrderStatus.UNKNOWN)
        assert result.status == expected

    @given(
        unknown_status=st.text(
            alphabet=st.characters(whitelist_categories=["Lu"]),
            min_size=3,
            max_size=20,
        ).filter(
            lambda s: s
            not in [
                "NEW",
                "FILLED",
                "CANCELLED",
                "REJECTED",
                "PARTIALLY_FILLED",
                "EXPIRED",
                "PENDING",
            ]
        )
    )
    def test_unknown_status_defaults_to_unknown(
        self,
        trading_data_mapper: BackpackOrderMapper,
        unknown_status: str,
    ) -> None:
        """Test that unknown statuses default to UNKNOWN."""
        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="12345",
            symbol=SOL_USDC_BP,
            side="Buy",
            order_type="LIMIT",
            status=unknown_status,
            quantity="1.0",
            price="100.0",
        )
        assert result.status == OrderStatus.UNKNOWN

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("bp_status", "expected_status"),
        [
            ("NEW", OrderStatus.OPEN),
            ("FILLED", OrderStatus.FILLED),
            ("CANCELLED", OrderStatus.CANCELED),
            ("REJECTED", OrderStatus.REJECTED),
            ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
            ("EXPIRED", OrderStatus.UNKNOWN),
        ],
    )
    def test_order_status_mapping(
        self,
        trading_data_mapper: BackpackOrderMapper,
        bp_status: str,
        expected_status: OrderStatus,
    ) -> None:
        """Test order status mapping with valid and extended statuses."""
        raw_order = create_raw_order(status=bp_status)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.status == expected_status


class TestOrderTypeMapping:
    """Property-based tests for order type mapping."""

    @given(order_type=bp_order_type_strategy())
    def test_order_type_mapping(
        self,
        trading_data_mapper: BackpackOrderMapper,
        order_type: str,
    ) -> None:
        """Test that all order types map correctly."""
        # Set price based on order type
        price = "3000.50" if order_type in ["LIMIT", "TAKE_PROFIT"] else None

        raw_order = create_raw_order(order_type=order_type, price=price)

        # Add trigger price for stop orders
        if order_type in ["STOP", "TRAILING_STOP"]:
            raw_order = raw_order.model_copy(update={"triggerPrice": "2900.00"})

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Check mapping
        expected_mapping = {
            "LIMIT": OrderType.LIMIT,
            "MARKET": OrderType.MARKET,
            "TAKE_PROFIT": OrderType.LIMIT,
            "STOP": OrderType.STOP_MARKET,
            "TRAILING_STOP": OrderType.STOP_MARKET,
        }

        expected = expected_mapping.get(order_type, OrderType.LIMIT)
        assert result.order_type == expected

        # Check stop price for stop orders
        if order_type in ["STOP", "TRAILING_STOP"]:
            assert result.stop_price == Decimal("2900.00")

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("bp_type", "expected_type"),
        [
            ("LIMIT", OrderType.LIMIT),
            ("MARKET", OrderType.MARKET),
            ("TAKE_PROFIT", OrderType.LIMIT),
        ],
    )
    def test_order_type_mapping_legacy(
        self,
        trading_data_mapper: BackpackOrderMapper,
        bp_type: str,
        expected_type: OrderType,
    ) -> None:
        """Test order type mapping with various types."""
        if bp_type in ["LIMIT", "TAKE_PROFIT"]:
            raw_order = create_raw_order(order_type=bp_type, price="3000.50")
        else:
            raw_order = create_raw_order(order_type=bp_type, price=None)

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.order_type == expected_type


class TestTimeInForceMapping:
    """Property-based tests for time in force mapping."""

    @given(tif=bp_time_in_force_strategy())
    def test_time_in_force_mapping(
        self,
        trading_data_mapper: BackpackOrderMapper,
        tif: str | None,
    ) -> None:
        """Test that all time in force values map correctly."""
        raw_order = create_raw_order(time_in_force=tif)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        if tif is None or tif.upper() not in ["GTC", "IOC", "FOK"]:
            # Default to GTC for unknown or None
            assert result.time_in_force == TimeInForce.GTC
        else:
            # Map known values
            expected_mapping = {
                "GTC": TimeInForce.GTC,
                "IOC": TimeInForce.IOC,
                "FOK": TimeInForce.FOK,
            }
            assert result.time_in_force == expected_mapping[tif.upper()]

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("bp_tif", "expected_tif"),
        [
            ("GTC", TimeInForce.GTC),
            ("IOC", TimeInForce.IOC),
            ("FOK", TimeInForce.FOK),
            ("gtc", TimeInForce.GTC),
            ("ioc", TimeInForce.IOC),
            ("fok", TimeInForce.FOK),
            ("unknown", TimeInForce.GTC),
            (None, TimeInForce.GTC),
        ],
    )
    def test_time_in_force_mapping_legacy(
        self,
        trading_data_mapper: BackpackOrderMapper,
        bp_tif: str | None,
        expected_tif: TimeInForce,
    ) -> None:
        """Test time in force mapping through order transformation."""
        raw_order = create_raw_order(time_in_force=bp_tif)
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result.time_in_force == expected_tif


class TestTransformRawOrderToInternal:
    """Property-based tests for raw order transformation."""

    @given(raw_order=raw_order_strategy())
    @settings(max_examples=100)
    def test_transform_raw_order_properties(
        self,
        trading_data_mapper: BackpackOrderMapper,
        raw_order: BackpackRawOrderResponse,
    ) -> None:
        """Test that any valid raw order transforms correctly."""
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Verify basic properties
        assert isinstance(result, Order)
        assert result.exchange_order_id == raw_order.id
        assert result.symbol.value == raw_order.symbol
        assert result.exchange == ExchangeName.BACKPACK.value

        # Verify quantities are decimal
        assert isinstance(result.quantity_requested, Decimal)
        if raw_order.executedQuantity:
            assert isinstance(result.quantity_filled, Decimal)

        # Verify timestamps
        assert result.created_at is not None
        assert result.updated_at is not None

    @given(
        quantity=decimal_string_strategy(),
        price=decimal_string_strategy(),
        executed_quantity=decimal_string_strategy(),
        avg_fill_price=decimal_string_strategy(),
    )
    def test_high_precision_values(
        self,
        trading_data_mapper: BackpackOrderMapper,
        quantity: str,
        price: str,
        executed_quantity: str,
        avg_fill_price: str,
    ) -> None:
        """Test transformation with high precision decimal values."""
        raw_order = create_raw_order(
            quantity=quantity,
            price=price,
            executed_quantity=executed_quantity,
            avg_fill_price=avg_fill_price,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.quantity_requested == Decimal(quantity)
        assert result.price == Decimal(price)
        assert result.quantity_filled == Decimal(executed_quantity)
        assert result.average_fill_price == Decimal(avg_fill_price)

    @given(
        reduce_only=st.booleans(),
        post_only=st.booleans(),
    )
    def test_order_flags(
        self,
        trading_data_mapper: BackpackOrderMapper,
        reduce_only: bool,
        post_only: bool,
    ) -> None:
        """Test transformation with order flags."""
        raw_order = create_raw_order()
        raw_order = raw_order.model_copy(
            update={
                "reduceOnly": reduce_only,
                "postOnly": post_only,
            }
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.reduce_only == reduce_only
        assert result.post_only == post_only

    def test_transform_raw_order_buy_limit_happy_path(
        self,
        trading_data_mapper: BackpackOrderMapper,
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
        assert result.symbol == SOL_USDC_BP
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


class TestTransformOrderDataToInternal:
    """Property-based tests for order data transformation."""

    @given(
        order_id=order_id_strategy(),
        symbol=st.builds(_create_symbol_dict, symbol_string_strategy()),
        side=bp_side_strategy(),
        order_type=bp_order_type_strategy(),
        status=bp_status_strategy(),
        quantity=decimal_string_strategy(),
        price=st.one_of(st.none(), decimal_string_strategy()),
        client_order_id=client_id_strategy(),
        time_in_force=bp_time_in_force_strategy(),
    )
    @settings(max_examples=50)
    def test_order_data_transformation_properties(
        self,
        trading_data_mapper: BackpackOrderMapper,
        order_id: str,
        symbol: dict[str, str],
        side: str,
        order_type: str,
        status: str,
        quantity: str,
        price: str | None,
        client_order_id: str | None,
        time_in_force: str | None,
    ) -> None:
        """Test order data transformation with various inputs."""
        # Use valid symbol
        symbol_obj = SOL_USDC_BP

        # Skip invalid side combinations
        if side.upper() not in ["BUY", "SELL", "BID", "ASK"]:
            assume(False)

        # Add price for limit orders
        if order_type in ["LIMIT", "TAKE_PROFIT"] and price is None:
            price = "100.0"

        result = trading_data_mapper.transform_order_data_to_internal(
            order_id=order_id,
            symbol=symbol_obj,
            side=side,
            order_type=order_type,
            status=status,
            quantity=quantity,
            price=price,
            client_order_id=client_order_id,
            time_in_force=time_in_force,
        )

        assert isinstance(result, Order)
        assert result.exchange_order_id == order_id
        assert result.symbol == symbol_obj
        assert result.quantity_requested == Decimal(quantity)
        assert result.exchange == ExchangeName.BACKPACK.value

    def test_transform_order_data_happy_path(
        self,
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Test successful transformation of order data."""
        created_at = datetime.now(UTC).isoformat()
        updated_at = datetime.now(UTC).isoformat()

        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="98765",
            symbol=ETH_USDC_BP,
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
        assert result.symbol == ETH_USDC_BP
        assert result.side == OrderSide.SELL
        assert result.order_type == OrderType.MARKET
        assert result.status == OrderStatus.FILLED
        assert result.quantity_requested == Decimal("2.0")
        assert result.quantity_filled == Decimal(0)
        assert result.price is None
        assert result.time_in_force == TimeInForce.IOC
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.client_order_id == "client_123"
        assert result.created_at is not None
        assert result.updated_at is not None


class TestEdgeCases:
    """Property-based tests for edge cases."""

    @given(
        zero_quantity=st.sampled_from(["0", "0.0", "0.00", "0.000"]),
    )
    def test_zero_quantity_handling(
        self,
        trading_data_mapper: BackpackOrderMapper,
        zero_quantity: str,
    ) -> None:
        """Test handling of zero quantities."""
        raw_order = create_raw_order(
            executed_quantity=zero_quantity,
            avg_fill_price=None,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        assert result.quantity_filled == Decimal(0)
        assert result.average_fill_price is None

    @given(
        malicious_input=st.one_of([
            st.sampled_from(["'; DROP TABLE orders; --", "1' OR '1'='1"]),
            st.sampled_from(["<script>alert('XSS')</script>"]),
            st.text(alphabet="A", min_size=1000, max_size=1500),
        ])
    )
    def test_malicious_input_resistance(
        self,
        trading_data_mapper: BackpackOrderMapper,
        malicious_input: str,
    ) -> None:
        """Test resistance to malicious inputs."""
        try:
            # Try as order ID
            result = trading_data_mapper.transform_order_data_to_internal(
                order_id=malicious_input,
                symbol=SOL_USDC_BP,
                side="Buy",
                order_type="LIMIT",
                status="NEW",
                quantity="1.0",
                price="100.0",
            )
            # Should safely store the malicious string
            assert result.exchange_order_id == malicious_input
        except (TransformationError, ValueError):
            # Rejecting malicious input is also acceptable
            pass

    def test_transform_raw_order_missing_quantity_raises_error(
        self,
        trading_data_mapper: BackpackOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing quantity raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_decimal_safely",
        )
        mock_parse.return_value = None

        raw_order = create_raw_order()
        with pytest.raises(TransformationError, match="quantity_requested is required"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

    def test_transform_raw_order_missing_created_at_raises_error(
        self,
        trading_data_mapper: BackpackOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that missing created_at raises TransformationError."""
        mock_parse = mocker.patch(
            "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_datetime_utc",
        )
        mock_parse.return_value = None

        raw_order = create_raw_order()
        with pytest.raises(TransformationError, match="createdAt is required"):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)
