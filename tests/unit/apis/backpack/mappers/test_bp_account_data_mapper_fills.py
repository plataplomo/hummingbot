"""CyberDeltaEngine: Backpack Account Data Mapper Fill Tests with Property-Based Testing.

--------------------------------------------------------

Comprehensive property-based test suite for BackpackTransactionMapper transformations.
Tests all public transformation methods with various scenarios including:
- Happy path transformations for fills and trades
- Property-based testing with Hypothesis for robust coverage
- Error handling and edge cases
- Side mapping functionality through public API
- WebSocket fill event transformations
- Boundary value testing
- Data integrity validation across all transformation methods
"""

import string
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal
from unittest.mock import patch

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionUpdate
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
)
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import DateTimeParsingError
from cyberdelta.models import DerivativePosition, Fill
from cyberdelta.symbols import exchanges
from cyberdelta.utils.parsing import parse_decimal_value
from tests.common_symbols import SOL_USDC_BP


@pytest.fixture
def mapper() -> BackpackTransactionMapper:
    """Fixture providing a BackpackTransactionMapper instance.

    Returns:
        BackpackTransactionMapper: Configured mapper instance for testing.
    """
    return BackpackTransactionMapper()


@pytest.fixture
def position_mapper() -> BackpackPositionMapper:
    """Fixture providing a BackpackPositionMapper instance.

    Returns:
        BackpackPositionMapper: Configured mapper instance for testing.
    """
    return BackpackPositionMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string.

    Returns:
        str: ISO format timestamp string for consistent testing.
    """
    return "2024-01-15T10:30:00Z"


# =======================
# Strategy Builders
# =======================


def decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and quantities.

    Returns:
        SearchStrategy[str]: Strategy for decimal strings.
    """
    return st.one_of([
        # Normal values
        st.builds(
            lambda i, d: f"{i}.{d}",
            st.integers(min_value=1, max_value=999999),
            st.text(alphabet=string.digits, min_size=1, max_size=8),
        ),
        # High precision values
        st.builds(
            lambda i, d: f"{i}.{''.join(d)}",
            st.integers(min_value=1, max_value=999),
            st.lists(st.sampled_from(string.digits), min_size=6, max_size=18),
        ),
        # Edge cases
        st.sampled_from([
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
        "SOL-USDC",
        "BTC-USDC",
        "ETH-USDC",
        "DOGE-USDC",
        "ADA-USDC",
        "MATIC-USDC",
    ])


def fee_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid fee symbols.

    Returns:
        SearchStrategy[str]: Strategy for fee symbols.
    """
    return st.sampled_from(["USDC", "USDT", "USD", "BTC", "ETH", "SOL"])


def order_side_strategy() -> SearchStrategy[str]:
    """Generate valid order sides.

    Returns:
        SearchStrategy[str]: Strategy for order sides.
    """
    return st.sampled_from(["Bid", "Ask"])


def trade_id_strategy() -> SearchStrategy[int]:
    """Generate valid trade IDs.

    Returns:
        SearchStrategy[int]: Strategy for trade IDs.
    """
    return st.integers(min_value=1, max_value=10**18)


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid order IDs.

    Returns:
        SearchStrategy[str]: Strategy for order IDs.
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
        min_size=1,
        max_size=100,
    )


def client_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client IDs.

    Returns:
        SearchStrategy[str | None]: Strategy for client IDs.
    """
    return st.one_of([
        st.none(),
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
            min_size=1,
            max_size=64,
        ),
    ])


def timestamp_strategy() -> SearchStrategy[str]:
    """Generate valid ISO timestamp strings.

    Returns:
        SearchStrategy[str]: Strategy for timestamp strings.
    """
    return st.builds(
        lambda dt: dt.isoformat(),
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 1, 1, tzinfo=UTC),
        ),
    )


@composite
def raw_fill_strategy(draw: st.DrawFn) -> BackpackRawFillResponse:
    """Generate valid BackpackRawFillResponse instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawFillResponse: Valid raw fill response.
    """
    return BackpackRawFillResponse(
        fee=draw(decimal_string_strategy()),
        feeSymbol=draw(fee_symbol_strategy()),
        isMaker=draw(st.booleans()),
        orderId=draw(order_id_strategy()),
        price=draw(decimal_string_strategy()),
        quantity=draw(decimal_string_strategy()),
        side=draw(order_side_strategy()),
        symbol=draw(symbol_string_strategy()),
        timestamp=draw(timestamp_strategy()),
        tradeId=draw(trade_id_strategy()),
        clientId=draw(client_id_strategy()),
        systemOrderType=None,
    )


@composite
def raw_trade_strategy(draw: st.DrawFn) -> BackpackRawPublicTrade:
    """Generate valid BackpackRawPublicTrade instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawPublicTrade: Valid raw trade response.
    """
    return BackpackRawPublicTrade(
        id=draw(st.text(min_size=1, max_size=50)),
        symbol=draw(symbol_string_strategy()),
        price=draw(decimal_string_strategy()),
        qty=draw(decimal_string_strategy()),
        time=draw(timestamp_strategy()),
        orderId=draw(order_id_strategy()),
    )


@composite
def raw_position_update_strategy(draw: st.DrawFn) -> BackpackRawPositionUpdate:
    """Generate valid BackpackRawPositionUpdate instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawPositionUpdate: Valid raw position update.
    """
    return BackpackRawPositionUpdate(
        e="positionUpdate",
        E=draw(st.integers(min_value=1600000000000, max_value=2000000000000)),
        s=draw(symbol_string_strategy()),
        b=draw(st.one_of(st.none(), decimal_string_strategy())),
        B=draw(st.one_of(st.none(), decimal_string_strategy())),
        l=draw(st.one_of(st.none(), decimal_string_strategy())),
        f=draw(st.one_of(st.none(), decimal_string_strategy())),
        M=draw(st.one_of(st.none(), decimal_string_strategy())),
        m=draw(st.one_of(st.none(), decimal_string_strategy())),
        q=draw(st.one_of(st.none(), decimal_string_strategy())),
        Q=draw(st.one_of(st.none(), decimal_string_strategy())),
        n=draw(st.one_of(st.none(), decimal_string_strategy())),
    )


def create_raw_fill(
    fee: str = "0.05",
    fee_symbol: str = "USDC",
    is_maker: bool = True,
    order_id: str = "order123",
    price: str = "100.50",
    quantity: str = "10.0",
    side: str = "Bid",  # Changed from "Buy" to "Bid" to match BP_ORDER_SIDES validation
    symbol: str = SOL_USDC_BP.value,
    timestamp: str = "2024-01-15T10:30:00Z",
    trade_id: int = 123456,
    client_id: str | None = None,
) -> BackpackRawFillResponse:
    """Create BackpackRawFillResponse instances for testing fill transformations.

    Returns:
        BackpackRawFillResponse instance configured with test data.
    """
    return BackpackRawFillResponse(
        fee=fee,
        feeSymbol=fee_symbol,
        isMaker=is_maker,
        orderId=order_id,
        price=price,
        quantity=quantity,
        side=side,
        symbol=symbol,
        timestamp=timestamp,
        tradeId=trade_id,
        clientId=client_id,
        systemOrderType=None,
    )


def create_raw_trade(
    trade_id: str = "trade123",
    symbol: str = SOL_USDC_BP.value,
    price: str = "100.50",
    qty: str = "10.0",
    time: str = "2024-01-15T10:30:00Z",
    order_id: str = "order123",
) -> BackpackRawPublicTrade:
    """Create BackpackRawPublicTrade instances for testing trade transformations.

    Returns:
        BackpackRawPublicTrade instance configured with test data.
    """
    return BackpackRawPublicTrade(
        id=trade_id,
        symbol=symbol,
        price=price,
        qty=qty,
        time=time,
        orderId=order_id,
    )


def create_raw_position_update(
    event_type: Literal["positionUpdate"] = "positionUpdate",
    event_time: int = 1678886400000,
    symbol: str = SOL_USDC_BP.value,
    break_event_price: str | None = "100.25",
    entry_price: str | None = "100.00",
    liquidation_price: str | None = "90.00",
    initial_margin_fraction: str | None = "0.05",
    mark_price: str | None = "100.50",
    maintenance_margin_fraction: str | None = "0.02",
    net_quantity: str | None = "10.0",
    net_exposure_quantity: str | None = "10.0",
    net_exposure_notional: str | None = "1000.0",
) -> BackpackRawPositionUpdate:
    """Create BackpackRawPositionUpdate instances for testing position updates.

    Returns:
        BackpackRawPositionUpdate instance configured with test data.
    """
    return BackpackRawPositionUpdate(
        e=event_type,
        E=event_time,
        s=symbol,
        b=break_event_price,
        B=entry_price,
        l=liquidation_price,
        f=initial_margin_fraction,
        M=mark_price,
        m=maintenance_margin_fraction,
        q=net_quantity,
        Q=net_exposure_quantity,
        n=net_exposure_notional,
    )


# =======================
# Property-Based Tests
# =======================


class TestFillTransformationProperties:
    """Property-based tests for fill transformation functionality."""

    @given(raw_fill=raw_fill_strategy())
    @settings(max_examples=100)
    def test_transform_raw_fill_properties(
        self,
        mapper: BackpackTransactionMapper,
        raw_fill: BackpackRawFillResponse,
    ) -> None:
        """Test fill transformation with various property combinations."""
        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Result can be None for zero price/quantity
        if result is not None:
            # Verify basic properties
            assert isinstance(result, Fill)
            assert result.id == str(raw_fill.trade_id)
            assert result.symbol == exchanges.backpack(raw_fill.symbol)
            assert result.exchange == ExchangeName.BACKPACK.value

            # Verify types
            assert isinstance(result.price, Decimal)
            assert isinstance(result.quantity, Decimal)
            assert isinstance(result.fee, Decimal)

            # Verify positive values
            assert result.price > 0
            assert result.quantity > 0
            assert result.fee >= 0

            # Verify side mapping
            if raw_fill.side == "Bid":
                assert result.side == OrderSide.BUY
            elif raw_fill.side == "Ask":
                assert result.side == OrderSide.SELL

            # Verify fee details
            assert result.fee_asset == raw_fill.fee_symbol
            assert result.order_id == raw_fill.order_id

            # Verify client ID if present
            if raw_fill.client_id:
                assert result.client_order_id == raw_fill.client_id
            else:
                assert result.client_order_id is None

            # Verify timestamps
            assert result.executed_at is not None
            assert isinstance(result.executed_at, datetime)

    @given(
        price=decimal_string_strategy(),
        quantity=decimal_string_strategy(),
        fee=decimal_string_strategy(),
        side=order_side_strategy(),
    )
    def test_decimal_precision_preservation(
        self,
        mapper: BackpackTransactionMapper,
        price: str,
        quantity: str,
        fee: str,
        side: str,
    ) -> None:
        """Test that decimal precision is preserved during transformation."""
        raw_fill = create_raw_fill(
            price=price,
            quantity=quantity,
            fee=fee,
            side=side,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            # Verify exact decimal preservation
            assert result.price == Decimal(price)
            assert result.quantity == Decimal(quantity)
            assert result.fee == Decimal(fee)

    @given(
        is_maker=st.booleans(),
        trade_id=trade_id_strategy(),
        symbol=symbol_string_strategy(),
    )
    def test_metadata_preservation(
        self,
        mapper: BackpackTransactionMapper,
        is_maker: bool,
        trade_id: int,
        symbol: str,
    ) -> None:
        """Test that metadata is preserved during transformation."""
        raw_fill = create_raw_fill(
            is_maker=is_maker,
            trade_id=trade_id,
            symbol=symbol,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.id == str(trade_id)
            assert result.symbol == exchanges.backpack(symbol)
            assert result.bp_details is not None
            assert result.maker_taker is not None

    @given(
        zero_value=st.sampled_from(["0", "0.0", "0.00", "0.000"]),
        field=st.sampled_from(["price", "quantity"]),
    )
    def test_zero_value_handling(
        self,
        mapper: BackpackTransactionMapper,
        zero_value: str,
        field: str,
    ) -> None:
        """Test that zero values are handled correctly."""
        if field == "price":
            raw_fill = create_raw_fill(price=zero_value)
        elif field == "quantity":
            raw_fill = create_raw_fill(quantity=zero_value)
        else:
            raw_fill = create_raw_fill()

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Should return None for zero price or quantity
        assert result is None

    @given(
        client_id=client_id_strategy(),
        order_id=order_id_strategy(),
    )
    def test_id_handling(
        self,
        mapper: BackpackTransactionMapper,
        client_id: str | None,
        order_id: str,
    ) -> None:
        """Test that various ID formats are handled correctly."""
        raw_fill = create_raw_fill(
            client_id=client_id,
            order_id=order_id,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.order_id == order_id
            if client_id:
                assert result.client_order_id == client_id
            else:
                assert result.client_order_id is None


class TestFillTransformation:
    """Test cases for fill transformation functionality."""

    def test_transform_raw_fill_to_internal_happy_path(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFillResponse to internal Fill."""
        raw_fill = create_raw_fill(
            fee="0.05",
            fee_symbol="USDC",
            is_maker=True,
            order_id="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp=test_timestamp,
            trade_id=123456,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert isinstance(result, Fill)
        assert result.id == "123456"
        assert result.symbol == exchanges.backpack("SOL-USDC")
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.side == OrderSide.BUY
        assert result.fee == Decimal("0.05")
        assert result.fee_asset == "USDC"
        assert result.order_id == "order123"
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.executed_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_fill_sell_side(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test fill transformation with sell side."""
        raw_fill = create_raw_fill(side="Ask", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Fill object but got None"
        assert result.side == OrderSide.SELL

    def test_transform_raw_fill_with_client_id(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test fill transformation with client order ID."""
        raw_fill = create_raw_fill(client_id="client123", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Fill object but got None"
        assert result.client_order_id == "client123"

    def test_transform_raw_fill_zero_price_returns_none(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test that zero price returns None."""
        raw_fill = create_raw_fill(price="0.0", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert result is None

    def test_transform_raw_fill_zero_quantity_returns_none(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test that zero quantity returns None."""
        raw_fill = create_raw_fill(quantity="0.0", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert result is None

    def test_transform_raw_fill_negative_values(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test fill transformation with negative values."""
        raw_fill = create_raw_fill(
            price="-100.50",
            quantity="-10.0",
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert result is None  # Should return None for invalid values

    def test_transform_raw_fill_transformation_error(
        self,
        mapper: BackpackTransactionMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw fill
        raw_fill = create_raw_fill()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                DataTransformationError,
                match="Failed to transform BackpackRawFillResponse to Fill",
            ):
                mapper.transform_raw_fill_to_internal(raw_fill)

    def test_transform_raw_fill_boundary_values(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test fill transformation with boundary decimal values."""
        raw_fill = create_raw_fill(
            price="0.000001",  # Very small price
            quantity="999999999.999999",  # Very large quantity
            fee="0.000000001",  # Very small fee
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Fill object but got None"
        assert result.price == Decimal("0.000001")
        assert result.quantity == Decimal("999999999.999999")
        assert result.fee == Decimal("0.000000001")

    @pytest.mark.parametrize(
        ("side_input", "expected_side"),
        [
            ("Bid", OrderSide.BUY),
            ("Ask", OrderSide.SELL),
        ],
    )
    def test_transform_raw_fill_side_mapping(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
        side_input: str,
        expected_side: OrderSide,
    ) -> None:
        """Test side mapping through fill transformation."""
        raw_fill = create_raw_fill(side=side_input, timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Fill object but got None"
        assert result.side == expected_side


class TestFillTransformationPrivate:
    """Test cases for private fill transformation functionality."""

    def test_transform_raw_fill_to_internal_returns_none(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test that BackpackRawPublicTrade transformation returns None due to missing side info."""
        raw_trade = create_raw_trade(
            trade_id="trade123",
            symbol="SOL-USDC",
            price="100.50",
            qty="10.0",
            time=test_timestamp,
            order_id="order123",
        )

        result = mapper.transform_raw_fill_to_internal_public(raw_trade)

        # Backpack REST API for trades lacks side information, so mapper returns None
        assert result is None

    def test_transform_raw_trade_missing_price_raises_error(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test trade transformation with missing price raises TransformationError."""
        # Create a valid raw trade first
        raw_trade = create_raw_trade(time=test_timestamp)

        # Mock parse_decimal_value to return None for price
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing fill parsing edge cases."""
                if field_name == "price":
                    return None
                # For other fields, call the real function
                if allow_none:
                    return parse_decimal_value(value, allow_none=True, field_name=field_name)
                return parse_decimal_value(value, allow_none=False, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                DataTransformationError,
                match="Failed to transform BackpackRawPublicTrade to Fill",
            ):
                mapper.transform_raw_fill_to_internal_public(raw_trade)

    def test_transform_raw_trade_missing_quantity_raises_error(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test trade transformation with missing quantity raises TransformationError."""
        # Create a valid raw trade first
        raw_trade = create_raw_trade(time=test_timestamp)

        # Mock parse_decimal_value to return None for quantity
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing fill quantity validation."""
                if field_name == "quantity":
                    return None
                # For other fields, call the real function
                if allow_none:
                    return parse_decimal_value(value, allow_none=True, field_name=field_name)
                return parse_decimal_value(value, allow_none=False, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                DataTransformationError,
                match="Failed to transform BackpackRawPublicTrade to Fill",
            ):
                mapper.transform_raw_fill_to_internal_public(raw_trade)


class TestWebSocketFillTransformationProperties:
    """Property-based tests for WebSocket fill event transformation."""

    @given(raw_fill=raw_fill_strategy())
    @settings(max_examples=50)
    def test_ws_fill_event_properties(
        self,
        mapper: BackpackTransactionMapper,
        raw_fill: BackpackRawFillResponse,
    ) -> None:
        """Test WebSocket fill event transformation with various inputs."""
        result = mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        # Should produce same result as regular fill transformation
        expected = mapper.transform_raw_fill_to_internal(raw_fill)

        if expected is None:
            assert result is None
        else:
            assert result is not None
            assert result.id == expected.id
            assert result.symbol == expected.symbol
            assert result.price == expected.price
            assert result.quantity == expected.quantity
            assert result.side == expected.side
            assert result.fee == expected.fee
            assert result.fee_asset == expected.fee_asset
            assert result.order_id == expected.order_id
            assert result.exchange == expected.exchange

    @given(
        symbol=symbol_string_strategy(),
        side=order_side_strategy(),
    )
    def test_ws_fill_event_symbol_side_consistency(
        self,
        mapper: BackpackTransactionMapper,
        symbol: str,
        side: str,
    ) -> None:
        """Test WebSocket fill event symbol and side consistency."""
        raw_fill = create_raw_fill(symbol=symbol, side=side)

        result = mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        if result is not None:
            assert result.symbol == exchanges.backpack(symbol)
            if side == "Bid":
                assert result.side == OrderSide.BUY
            elif side == "Ask":
                assert result.side == OrderSide.SELL


class TestWebSocketFillTransformation:
    """Test cases for WebSocket fill event transformation functionality."""

    def test_transform_ws_fill_event_to_internal_fill_happy_path(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of WebSocket fill event to internal Fill."""
        raw_fill = create_raw_fill(
            fee="0.05",
            fee_symbol="USDC",
            is_maker=True,
            order_id="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp=test_timestamp,
            trade_id=123456,
        )

        result = mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        assert isinstance(result, Fill)
        assert result.id == "123456"
        assert result.symbol == exchanges.backpack("SOL-USDC")
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.side == OrderSide.BUY
        assert result.fee == Decimal("0.05")
        assert result.fee_asset == "USDC"
        assert result.order_id == "order123"
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None

    def test_transform_ws_fill_event_zero_values_returns_none(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test that WebSocket fill with zero values returns None."""
        raw_fill = create_raw_fill(
            price="0.0",
            quantity="0.0",
            timestamp=test_timestamp,
        )

        result = mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        assert result is None


class TestWebSocketPositionUpdateTransformationProperties:
    """Property-based tests for WebSocket position update transformation."""

    @given(raw_position=raw_position_update_strategy())
    @settings(max_examples=50)
    def test_ws_position_update_properties(
        self,
        position_mapper: BackpackPositionMapper,
        raw_position: BackpackRawPositionUpdate,
    ) -> None:
        """Test WebSocket position update transformation with various inputs."""
        # Skip if net_quantity is None (required for position)
        if raw_position.net_quantity is None:
            assume(False)

        result = position_mapper.transform_ws_position_update_to_internal_position(raw_position)

        assert isinstance(result, DerivativePosition)
        assert result.symbol == exchanges.backpack(raw_position.symbol)
        assert result.exchange == ExchangeName.BACKPACK

        # Verify types
        assert isinstance(result.size, Decimal)

        # Verify side mapping based on position sign
        assert raw_position.net_quantity is not None
        quantity = Decimal(raw_position.net_quantity)
        if quantity > 0:
            assert result.side == OrderSide.BUY
        elif quantity < 0:
            assert result.side == OrderSide.SELL
        else:
            # Zero quantity positions might be handled specially
            assert result.side in [OrderSide.BUY, OrderSide.SELL]

        assert result.size == quantity

        # Verify optional fields
        if raw_position.entry_price:  # entry_price
            assert result.entry_price == Decimal(raw_position.entry_price)
        if raw_position.mark_price:  # mark_price
            assert result.mark_price == Decimal(raw_position.mark_price)
        if raw_position.liquidation_price:  # liquidation_price
            assert result.liquidation_price == Decimal(raw_position.liquidation_price)

        # Position updates don't include PnL
        assert result.unrealized_pnl is None
        assert result.realized_pnl is None
        assert result.bp_details is not None

    @given(
        net_quantity=st.builds(
            lambda sign, magnitude: f"{sign}{magnitude}",
            st.sampled_from(["", "-"]),
            decimal_string_strategy(),
        )
    )
    def test_position_side_mapping(
        self,
        position_mapper: BackpackPositionMapper,
        net_quantity: str,
    ) -> None:
        """Test position side mapping based on quantity sign."""
        raw_position = create_raw_position_update(net_quantity=net_quantity)

        result = position_mapper.transform_ws_position_update_to_internal_position(raw_position)

        quantity = Decimal(net_quantity)
        if quantity > 0:
            assert result.side == OrderSide.BUY
        elif quantity < 0:
            assert result.side == OrderSide.SELL
        else:
            # Zero positions are edge case
            assert result.side in [OrderSide.BUY, OrderSide.SELL]

        assert result.size == quantity


class TestWebSocketPositionUpdateTransformation:
    """Test cases for WebSocket position update transformation functionality."""

    def test_transform_ws_position_update_to_internal_position_happy_path(
        self,
        position_mapper: BackpackPositionMapper,
    ) -> None:
        """Test successful transformation of WebSocket position update to DerivativePosition."""
        raw_position_update = create_raw_position_update(
            symbol="SOL-USDC",
            break_event_price="100.25",
            entry_price="100.00",
            liquidation_price="90.00",
            mark_price="100.50",
            net_quantity="10.0",
            net_exposure_quantity="10.0",
            net_exposure_notional="1000.0",
        )

        result = position_mapper.transform_ws_position_update_to_internal_position(
            raw_position_update
        )

        assert isinstance(result, DerivativePosition)
        assert result.symbol == exchanges.backpack("SOL-USDC")
        assert result.side == OrderSide.BUY  # Long -> BUY
        assert result.size == Decimal("10.0")
        assert result.entry_price == Decimal("100.00")
        assert result.mark_price == Decimal("100.50")
        assert result.liquidation_price == Decimal("90.00")
        assert result.exchange == ExchangeName.BACKPACK
        # Position updates don't include PnL information
        assert result.unrealized_pnl is None
        assert result.realized_pnl is None
        assert result.bp_details is not None
        assert result.bp_details.imf_base == Decimal("0.05")
        assert result.bp_details.mmf_base == Decimal("0.02")

    def test_transform_ws_position_update_short_position(
        self,
        position_mapper: BackpackPositionMapper,
    ) -> None:
        """Test WebSocket position update transformation for short position."""
        raw_position_update = create_raw_position_update(
            net_quantity="-10.0",  # Short position
        )

        result = position_mapper.transform_ws_position_update_to_internal_position(
            raw_position_update
        )

        assert isinstance(result, DerivativePosition)
        assert result.side == OrderSide.SELL  # Short -> SELL
        assert result.size == Decimal("-10.0")

    def test_transform_ws_position_update_transformation_error(
        self,
        position_mapper: BackpackPositionMapper,
    ) -> None:
        """Test that position update transformation errors are properly wrapped."""
        raw_position_update = create_raw_position_update()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_position_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                DataTransformationError,
                match="Failed to transform BackpackRawPositionUpdate to DerivativePosition",
            ):
                position_mapper.transform_ws_position_update_to_internal_position(
                    raw_position_update
                )


class TestErrorHandlingProperties:
    """Property-based tests for error handling and edge cases."""

    @given(
        malicious_string=st.one_of([
            st.sampled_from([
                "'; DROP TABLE fills; --",
                "1' OR '1'='1",
                "<script>alert('XSS')</script>",
                "$(rm -rf /)",
                "../../../etc/passwd",
            ]),
            st.text(alphabet="A", min_size=1000, max_size=10000),
        ]),
        field=st.sampled_from(["order_id", "symbol", "fee_symbol"]),
    )
    def test_malicious_input_resistance(
        self,
        mapper: BackpackTransactionMapper,
        malicious_string: str,
        field: str,
    ) -> None:
        """Test resistance to malicious inputs."""
        try:
            if field == "order_id":
                raw_fill = create_raw_fill(order_id=malicious_string)
            elif field == "symbol":
                raw_fill = create_raw_fill(symbol=malicious_string)
            elif field == "fee_symbol":
                raw_fill = create_raw_fill(fee_symbol=malicious_string)
            else:
                raw_fill = create_raw_fill()
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                # Should safely store the malicious string
                if field == "order_id":
                    assert result.order_id == malicious_string
                elif field == "symbol":
                    assert result.symbol == exchanges.backpack(malicious_string)
                elif field == "fee_symbol":
                    assert result.fee_asset == malicious_string
        except (DataTransformationError, ValueError, TypeFieldError):
            # Rejecting malicious input is also acceptable
            pass

    @given(
        negative_value=st.builds(
            lambda n: f"-{n}",
            decimal_string_strategy(),
        ),
        field=st.sampled_from(["price", "quantity", "fee"]),
    )
    def test_negative_value_handling(
        self,
        mapper: BackpackTransactionMapper,
        negative_value: str,
        field: str,
    ) -> None:
        """Test handling of negative values."""
        if field == "price":
            kwargs = {"price": negative_value}
        elif field == "quantity":
            kwargs = {"quantity": negative_value}
        elif field == "fee":
            kwargs = {"fee": negative_value}
        else:
            kwargs = {}

        # Mock parse_decimal_value to allow negative values through validation
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = lambda v, **kwargs: Decimal(str(v)) if v else None

            raw_fill = create_raw_fill(**kwargs)  # type: ignore
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # Should return None for negative values
            assert result is None

    @given(
        unicode_string=st.text(
            alphabet=st.characters(min_codepoint=0x1F300, max_codepoint=0x1F6FF),
            min_size=1,
            max_size=10,
        ),
        field=st.sampled_from(["symbol", "fee_symbol"]),
    )
    def test_unicode_handling(
        self,
        mapper: BackpackTransactionMapper,
        unicode_string: str,
        field: str,
    ) -> None:
        """Test handling of unicode characters."""
        try:
            if field == "symbol":
                raw_fill = create_raw_fill(symbol=unicode_string)
            elif field == "fee_symbol":
                raw_fill = create_raw_fill(fee_symbol=unicode_string)
            else:
                raw_fill = create_raw_fill()
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                if field == "symbol":
                    assert result.symbol == exchanges.backpack(unicode_string)
                elif field == "fee_symbol":
                    assert result.fee_asset == unicode_string
        except (DataTransformationError, ValueError, TypeFieldError):
            # Some unicode might not be valid, which is acceptable
            pass

    @given(
        long_id=st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
            min_size=65,
            max_size=200,
        )
    )
    def test_long_client_id_handling(
        self,
        mapper: BackpackTransactionMapper,
        long_id: str,
    ) -> None:
        """Test handling of very long client IDs."""
        # Client IDs over 64 characters should raise an error
        with pytest.raises(DataTransformationError):
            raw_fill = create_raw_fill(client_id=long_id)
            mapper.transform_raw_fill_to_internal(raw_fill)

    @given(extreme_trade_id=st.integers(min_value=10**15, max_value=10**18))
    def test_extreme_trade_id_handling(
        self,
        mapper: BackpackTransactionMapper,
        extreme_trade_id: int,
    ) -> None:
        """Test handling of extreme trade ID values."""
        raw_fill = create_raw_fill(trade_id=extreme_trade_id)
        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.id == str(extreme_trade_id)
            assert int(result.id) == extreme_trade_id


class TestErrorHandling:
    """Test cases for error handling and edge cases."""

    def test_invalid_side_raises_transformation_error(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test that invalid side values are handled by raw model validation."""
        # This test checks that field validation catches invalid sides
        # before they reach the mapper - the side field has a max length of 3 chars
        with pytest.raises(TypeFieldError):  # Field validation error for length
            create_raw_fill(side="InvalidSide", timestamp=test_timestamp)

    def test_malformed_timestamp_handled_gracefully(
        self,
        mapper: BackpackTransactionMapper,
    ) -> None:
        """Test that malformed timestamps are handled by raw model validation."""
        # This test checks that field validation catches invalid timestamps
        # before they reach the mapper
        with pytest.raises(DateTimeParsingError):  # DateTimeParsingError from datetime parsing
            create_raw_fill(timestamp="not-a-timestamp")

    def test_edge_case_unicode_symbols(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test transformation with Unicode symbols."""
        raw_fill = create_raw_fill(
            symbol="SOL-USDC🚀",  # Unicode symbol
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Fill object but got None"
        assert result.symbol == exchanges.backpack("SOL-USDC🚀")

    def test_very_long_trade_ids(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test transformation with very long trade IDs."""
        long_id = 999999999999999999  # Very large trade ID
        raw_fill = create_raw_fill(trade_id=long_id, timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Fill object but got None"
        assert result.id == str(long_id)
