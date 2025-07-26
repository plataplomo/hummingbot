"""CyberDeltaEngine: Backpack Account Data Mapper Fill Tests.

--------------------------------------------------------

Comprehensive test suite for BackpackTransactionMapper fill and trade transformation methods.
Tests all public transformation methods with various scenarios including:
- Happy path transformations for fills and trades
- Error handling and edge cases
- Side mapping functionality through public API
- WebSocket fill event transformations
- Boundary value testing
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionUpdate
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
)
from cyberdelta.core.models import DerivativePosition, Trade
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import DateTimeParsingError
from cyberdelta.utils.parsing import parse_decimal_value


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


def create_raw_fill(
    fee: str = "0.05",
    fee_symbol: str = "USDC",
    is_maker: bool = True,
    order_id: str = "order123",
    price: str = "100.50",
    quantity: str = "10.0",
    side: str = "Bid",  # Changed from "Buy" to "Bid" to match BP_ORDER_SIDES validation
    symbol: str = "SOL-USDC",
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
    symbol: str = "SOL-USDC",
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
    symbol: str = "SOL_USDC",
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


class TestFillTransformation:
    """Test cases for fill transformation functionality."""

    def test_transform_raw_fill_to_internal_happy_path(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFillResponse to internal Trade."""
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

        assert isinstance(result, Trade)
        assert result.id == "123456"
        assert result.symbol == "SOL-USDC"
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
        assert result is not None, "Expected Trade object but got None"
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
        assert result is not None, "Expected Trade object but got None"
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
                match="Failed to transform BackpackRawFillResponse to Trade",
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
        assert result is not None, "Expected Trade object but got None"
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
        assert result is not None, "Expected Trade object but got None"
        assert result.side == expected_side


class TestTradeTransformation:
    """Test cases for trade transformation functionality."""

    def test_transform_raw_trade_to_internal_returns_none(
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

        result = mapper.transform_raw_trade_to_internal(raw_trade)

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
                match="Failed to transform BackpackRawPublicTrade to PublicTrade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

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
                match="Failed to transform BackpackRawPublicTrade to PublicTrade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)


class TestWebSocketFillTransformation:
    """Test cases for WebSocket fill event transformation functionality."""

    def test_transform_ws_fill_event_to_internal_trade_happy_path(
        self,
        mapper: BackpackTransactionMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of WebSocket fill event to internal Trade."""
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

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert isinstance(result, Trade)
        assert result.id == "123456"
        assert result.symbol == "SOL-USDC"
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

        result = mapper.transform_ws_fill_event_to_internal_trade(raw_fill)

        assert result is None


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
        assert result.symbol == "SOL-USDC"
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
        assert result is not None, "Expected Trade object but got None"
        assert result.symbol == "SOL-USDC🚀"

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
        assert result is not None, "Expected Trade object but got None"
        assert result.id == str(long_id)
