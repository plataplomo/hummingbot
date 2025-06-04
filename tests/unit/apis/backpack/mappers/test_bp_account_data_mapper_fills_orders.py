"""CyberDeltaEngine: Backpack Account Data Mapper Fill and Order Tests
-------------------------------------------------------------------

Comprehensive test suite for BackpackAccountDataMapper fill and order methods.
Tests fill and order transformation methods with various scenarios including:
- Fill transformations to Trade objects
- Order transformations with status/side/type mapping
- Trade transformations (REST API limitations)
- Error handling and edge cases
- Enum mapping validation
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill, BackpackRawTrade
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import Order, Trade
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def mapper() -> BackpackAccountDataMapper:
    """Fixture providing a BackpackAccountDataMapper instance."""
    return BackpackAccountDataMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string."""
    return "2024-01-15T10:30:00Z"


def create_raw_fill(
    fee: str = "0.05",
    fee_symbol: str = "USDC",
    is_maker: bool = True,
    order_id: str = "order123",
    price: str = "100.50",
    quantity: str = "10.0",
    side: str = "Buy",
    symbol: str = "SOL-USDC",
    timestamp: str = "2024-01-15T10:30:00Z",
    trade_id: int = 123456,
    client_id: str | None = None,
) -> BackpackRawFill:
    """Helper function to create BackpackRawFill instances for testing."""
    return BackpackRawFill(
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
    )


def create_raw_order(
    id: str = "order123",
    symbol: str = "SOL-USDC",
    side: str = "Buy",
    order_type: str = "LIMIT",
    quantity: str = "10.0",
    price: str = "100.50",
    status: str = "NEW",
    time_in_force: str = "GTC",
    created_at: str = "2024-01-15T10:30:00Z",
    executed_quantity: str = "0.0",
    avg_fill_price: str | None = None,
    trigger_price: str | None = None,
    trigger_by: str | None = None,
) -> BackpackRawOrder:
    """Helper function to create BackpackRawOrder instances for testing."""
    return BackpackRawOrder(
        clientId=None,
        id=id,
        symbol=symbol,
        side=side,
        orderType=order_type,
        quantity=quantity,
        price=price,
        status=status,
        timeInForce=time_in_force,
        createdAt=created_at,
        executedQuantity=executed_quantity,
        executedQuoteQuantity="0.0",
        relatedOrderId=None,
        avgFillPrice=avg_fill_price,
        triggerPrice=trigger_price,
        triggerBy=trigger_by,
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention="NONE",
        updatedAt=None,
        triggeredAt=None,
        expiryReason=None,
        origin="API",
    )


def create_raw_trade(
    id: str = "trade123",
    symbol: str = "SOL-USDC",
    price: str = "100.50",
    qty: str = "10.0",
    time: str = "2024-01-15T10:30:00Z",
    order_id: str = "order123",
    is_buyer: bool = True,
) -> BackpackRawTrade:
    """Helper function to create BackpackRawTrade instances for testing."""
    return BackpackRawTrade(
        id=id,
        symbol=symbol,
        price=price,
        qty=qty,
        time=time,
        orderId=order_id,
    )


class TestFillTransformation:
    """Test cases for fill transformation functionality."""

    def test_transform_raw_fill_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFill to internal Trade."""
        raw_fill = create_raw_fill(
            fee="0.05",
            fee_symbol="USDC",
            is_maker=True,
            order_id="order123",
            price="100.50",
            quantity="10.0",
            side="Buy",
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
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test fill transformation with sell side."""
        raw_fill = create_raw_fill(side="Sell", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.side == OrderSide.SELL

    def test_transform_raw_fill_transformation_error(
        self, mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw fill
        raw_fill = create_raw_fill()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError, match="Failed to transform BackpackRawFill to Trade",
            ):
                mapper.transform_raw_fill_to_internal(raw_fill)

    def test_transform_raw_fill_zero_price_returns_none(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test that fill transformation with zero price returns None."""
        raw_fill = create_raw_fill(price="0.0", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert result is None

    def test_transform_raw_fill_zero_quantity_returns_none(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test that fill transformation with zero quantity returns None."""
        raw_fill = create_raw_fill(quantity="0.0", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert result is None

    def test_transform_raw_fill_with_client_id(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test fill transformation with client ID."""
        raw_fill = create_raw_fill(client_id="client123", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.client_order_id == "client123"

    def test_transform_raw_fill_high_precision_values(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test fill transformation with high precision decimal values."""
        raw_fill = create_raw_fill(
            price="100.123456789012345",
            quantity="10.987654321098765",
            fee="0.012345678901234",
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.price == Decimal("100.123456789012345")
        assert result.quantity == Decimal("10.987654321098765")
        assert result.fee == Decimal("0.012345678901234")


class TestOrderTransformation:
    """Test cases for order transformation functionality."""

    def test_transform_raw_order_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawOrder to internal Order."""
        raw_order = create_raw_order(
            id="order123",
            symbol="SOL-USDC",
            side="Buy",
            order_type="LIMIT",
            quantity="10.0",
            price="100.50",
            status="NEW",
            time_in_force="GTC",
            created_at=test_timestamp,
        )

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert isinstance(result, Order)
        assert result.exchange_order_id == "order123"
        assert result.symbol == "SOL-USDC"
        assert result.side == OrderSide.BUY
        assert result.order_type == OrderType.LIMIT
        assert result.quantity_requested == Decimal("10.0")
        assert result.price == Decimal("100.50")
        assert result.status == OrderStatus.NEW
        assert result.time_in_force == TimeInForce.GTC
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.created_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    @pytest.mark.parametrize(
        "bp_side,expected_side",
        [
            ("Buy", OrderSide.BUY),
            ("Sell", OrderSide.SELL),
            ("Bid", OrderSide.BUY),
            ("Ask", OrderSide.SELL),
        ],
    )
    def test_transform_raw_order_side_mapping(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
        bp_side: str,
        expected_side: OrderSide,
    ) -> None:
        """Test order transformation with different side values."""
        raw_order = create_raw_order(side=bp_side, created_at=test_timestamp)

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.side == expected_side

    @pytest.mark.parametrize(
        "bp_status,expected_status",
        [
            ("NEW", OrderStatus.NEW),
            ("FILLED", OrderStatus.FILLED),
            ("CANCELLED", OrderStatus.CANCELED),
            ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
            ("REJECTED", OrderStatus.REJECTED),
            ("EXPIRED", OrderStatus.EXPIRED),
        ],
    )
    def test_transform_raw_order_status_mapping(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
        bp_status: str,
        expected_status: OrderStatus,
    ) -> None:
        """Test order transformation with different status values."""
        raw_order = create_raw_order(status=bp_status, created_at=test_timestamp)

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.status == expected_status

    @pytest.mark.parametrize(
        "bp_type,expected_type",
        [
            ("LIMIT", OrderType.LIMIT),
            ("MARKET", OrderType.MARKET),
            ("STOP", OrderType.STOP_MARKET),
            ("TAKE_PROFIT", OrderType.TAKE_PROFIT_MARKET),
        ],
    )
    def test_transform_raw_order_type_mapping(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
        bp_type: str,
        expected_type: OrderType,
    ) -> None:
        """Test order transformation with different type values."""
        # For STOP orders, provide a trigger price since it's required
        trigger_price = "99.00" if bp_type == "STOP" else None
        raw_order = create_raw_order(
            order_type=bp_type, created_at=test_timestamp, trigger_price=trigger_price,
        )

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.order_type == expected_type

    @pytest.mark.parametrize(
        "bp_tif,expected_tif",
        [
            ("GTC", TimeInForce.GTC),
            ("IOC", TimeInForce.IOC),
            ("FOK", TimeInForce.FOK),
        ],
    )
    def test_transform_raw_order_tif_mapping(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
        bp_tif: str,
        expected_tif: TimeInForce,
    ) -> None:
        """Test order transformation with different time-in-force values."""
        raw_order = create_raw_order(time_in_force=bp_tif, created_at=test_timestamp)

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.time_in_force == expected_tif

    def test_transform_raw_order_with_executed_quantity(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test order transformation with executed quantity."""
        raw_order = create_raw_order(
            executed_quantity="5.0",
            avg_fill_price="100.25",
            created_at=test_timestamp,
        )

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.quantity_filled == Decimal("5.0")
        assert result.average_fill_price == Decimal("100.25")

    def test_transform_raw_order_with_trigger_price(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test order transformation with trigger price for stop orders."""
        raw_order = create_raw_order(
            order_type="STOP",
            trigger_price="99.00",
            created_at=test_timestamp,
        )

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.order_type == OrderType.STOP_MARKET
        assert result.bp_details is not None

    def test_transform_raw_order_high_precision_values(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test order transformation with high precision values."""
        raw_order = create_raw_order(
            quantity="10.123456789012345",
            price="100.987654321098765",
            executed_quantity="5.555666777888999",
            avg_fill_price="100.987654321098765",  # Required when executed_quantity > 0
            created_at=test_timestamp,
        )

        result = mapper.transform_raw_order_to_internal(raw_order)

        assert result.quantity_requested == Decimal("10.123456789012345")
        assert result.price == Decimal("100.987654321098765")
        assert result.quantity_filled == Decimal("5.555666777888999")
        assert result.average_fill_price == Decimal("100.987654321098765")


class TestTradeTransformation:
    """Test cases for trade transformation functionality."""

    def test_transform_raw_trade_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test that BackpackRawTrade transformation returns None due to missing side info."""
        raw_trade = create_raw_trade(
            id="trade123",
            symbol="SOL-USDC",
            price="100.50",
            qty="10.0",
            time=test_timestamp,
            order_id="order123",
            is_buyer=True,
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        # Backpack REST API for trades lacks side information, so mapper returns None
        assert result is None

    def test_transform_raw_trade_missing_price(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test trade transformation with missing price returns None."""
        # Create a valid raw trade first
        raw_trade = create_raw_trade(time=test_timestamp)

        # Mock parse_decimal_value to return None for price
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str, allow_none: bool = False, field_name: str = "",
            ) -> Decimal | None:
                """Helper function for side effect."""
                if field_name == "price":
                    return None
                # For other fields, call the real function
                from cyberdelta.utils.parsing import parse_decimal_value as real_parse

                return real_parse(value, allow_none=allow_none, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError, match="price missing/invalid in BackpackRawTrade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_missing_quantity(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test trade transformation with missing quantity returns None."""
        # Create a valid raw trade first
        raw_trade = create_raw_trade(time=test_timestamp)

        # Mock parse_decimal_value to return None for quantity
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str, allow_none: bool = False, field_name: str = "",
            ) -> Decimal | None:
                """Helper function for side effect."""
                if field_name == "quantity":
                    return None
                # For other fields, call the real function
                from cyberdelta.utils.parsing import parse_decimal_value as real_parse

                return real_parse(value, allow_none=allow_none, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError, match="quantity missing/invalid in BackpackRawTrade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_high_precision_values(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test trade transformation with high precision values (should still return None)."""
        raw_trade = create_raw_trade(
            price="100.123456789012345",
            qty="10.987654321098765",
            time=test_timestamp,
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        # Should still return None due to missing side information
        assert result is None

    def test_transform_raw_trade_with_very_long_id(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str,
    ) -> None:
        """Test trade transformation with very long trade ID (should still return None)."""
        long_id = "a" * 64  # Maximum allowed length
        raw_trade = create_raw_trade(id=long_id, time=test_timestamp)

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        # Should still return None due to missing side information
        assert result is None
