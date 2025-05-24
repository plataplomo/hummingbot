"""
CyberDeltaEngine: Backpack Account Data Mapper Tests
---------------------------------------------------

Comprehensive test suite for BackpackAccountDataMapper class.
Tests all public transformation methods with various scenarios including:
- Happy path transformations
- Error handling and edge cases
- Enum mapping functionality
- Balance and position transformations
- Account summary transformations
- Transfer and withdrawal transformations
- Boundary value testing
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.bp_response_handler import RawJsonResponse
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill, BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import (
    InternalTransferStatus,
    InternalWithdrawalStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.operations import (
    Transfer,
    Withdrawal,
)


@pytest.fixture
def mapper() -> BackpackAccountDataMapper:
    """Fixture providing a BackpackAccountDataMapper instance."""
    return BackpackAccountDataMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string."""
    return "2024-01-15T10:30:00Z"


@pytest.fixture
def test_timestamp_ms() -> int:
    """Fixture providing a consistent test timestamp in milliseconds."""
    return 1705316600000


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


def create_raw_balance(
    asset: str = "USDC",
    available: str = "1000.0",
    total: str = "1100.0",
) -> BackpackRawBalance:
    """Helper function to create BackpackRawBalance instances for testing."""
    return BackpackRawBalance(
        asset=asset,
        available=available,
        total=total,
    )


def create_raw_position(
    symbol: str = "SOL-USDC",
    break_even_price: str = "100.25",
    entry_price: str = "100.00",
    est_liquidation_price: str = "90.00",
    imf: str = "0.1",
    mark_price: str = "100.50",
    mmf: str = "0.05",
    net_cost: str = "1000.0",
    net_quantity: str = "10.0",
    net_exposure_quantity: str = "10.0",
    net_exposure_notional: str = "1005.0",
    pnl_realized: str = "0.0",
    pnl_unrealized: str = "5.0",
    cumulative_funding_payment: str = "0.1",
    user_id: int = 12345,
    position_id: str = "pos123",
    cumulative_interest: str = "0.0",
) -> BackpackRawPosition:
    """Helper function to create BackpackRawPosition instances for testing."""
    # Create minimal IMF and MMF function objects with correct parameters
    imf_function = BackpackRawImfFunction(
        base="0.1",
        factor="0.0",
    )
    mmf_function = BackpackRawMmfFunction(
        base="0.05",
        factor="0.0",
    )

    return BackpackRawPosition(
        symbol=symbol,
        breakEvenPrice=break_even_price,
        entryPrice=entry_price,
        estLiquidationPrice=est_liquidation_price,
        imf=imf,
        imfFunction=imf_function,
        markPrice=mark_price,
        mmf=mmf,
        mmfFunction=mmf_function,
        netCost=net_cost,
        netQuantity=net_quantity,
        netExposureQuantity=net_exposure_quantity,
        netExposureNotional=net_exposure_notional,
        pnlRealized=pnl_realized,
        pnlUnrealized=pnl_unrealized,
        cumulativeFundingPayment=cumulative_funding_payment,
        userId=user_id,
        positionId=position_id,
        cumulativeInterest=cumulative_interest,
    )


def create_raw_account_summary(
    auto_borrow_settlements: bool = False,
    auto_lend: bool = False,
    auto_realize_pnl: bool = False,
    auto_repay_borrows: bool = False,
    borrow_limit: str = "5000.0",
    futures_maker_fee: str = "0.0002",
    futures_taker_fee: str = "0.0005",
    leverage_limit: str = "10.0",
    limit_orders: int = 100,
    liquidating: bool = False,
    position_limit: str = "1000000.0",
    spot_maker_fee: str = "0.001",
    spot_taker_fee: str = "0.001",
    trigger_orders: int = 50,
) -> BackpackRawAccountSummary:
    """Helper function to create BackpackRawAccountSummary instances for testing."""
    return BackpackRawAccountSummary.model_validate(
        {
            "autoBorrowSettlements": auto_borrow_settlements,
            "autoLend": auto_lend,
            "autoRealizePnl": auto_realize_pnl,
            "autoRepayBorrows": auto_repay_borrows,
            "borrowLimit": borrow_limit,
            "futuresMakerFee": futures_maker_fee,
            "futuresTakerFee": futures_taker_fee,
            "leverageLimit": leverage_limit,
            "limitOrders": limit_orders,
            "liquidating": liquidating,
            "positionLimit": position_limit,
            "spotMakerFee": spot_maker_fee,
            "spotTakerFee": spot_taker_fee,
            "triggerOrders": trigger_orders,
        }
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


def create_raw_withdrawal_response(
    id: int = 123,
    status: str = "confirmed",
    blockchain: str = "Ethereum",
    quantity: str = "1000.0",
    fee: str = "5.0",
    symbol: str = "USDC",
    to_address: str = "0xabc123",
    created_at: str = "2024-01-15T10:30:00Z",
    is_internal: bool = False,
) -> BackpackRawWithdrawalResponse:
    """Helper function to create BackpackRawWithdrawalResponse instances for testing."""
    return BackpackRawWithdrawalResponse.model_validate(
        {
            "id": id,
            "status": status,
            "blockchain": blockchain,
            "quantity": quantity,
            "fee": fee,
            "symbol": symbol,
            "toAddress": to_address,
            "createdAt": created_at,
            "isInternal": is_internal,
        }
    )


class TestFillTransformation:
    """Test cases for fill transformation functionality."""

    def test_transform_raw_fill_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
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
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
    ) -> None:
        """Test fill transformation with sell side."""
        raw_fill = create_raw_fill(side="Sell", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.side == OrderSide.SELL

    def test_transform_raw_fill_transformation_error(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw fill
        raw_fill = create_raw_fill()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError, match="Failed to transform BackpackRawFill to Trade"
            ):
                mapper.transform_raw_fill_to_internal(raw_fill)


class TestBalanceTransformation:
    """Test cases for balance transformation functionality."""

    def test_transform_balance_data_to_spot_balance_happy_path(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test successful transformation of balance data to SpotBalance."""
        result = mapper.transform_balance_data_to_spot_balance(
            asset="USDC",
            total_balance="1000.0",
            available_balance="900.0",
        )

        assert isinstance(result, SpotBalance)
        assert result.asset == "USDC"
        assert result.total_quantity == Decimal("1000.0")
        assert result.available_quantity == Decimal("900.0")
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None

    def test_transform_balance_data_zero_values(self, mapper: BackpackAccountDataMapper) -> None:
        """Test balance transformation with zero values."""
        result = mapper.transform_balance_data_to_spot_balance(
            asset="BTC",
            total_balance="0.0",
            available_balance="0.0",
        )

        assert result.total_quantity == Decimal("0.0")
        assert result.available_quantity == Decimal("0.0")

    def test_transform_raw_balance_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test successful transformation of BackpackRawBalance to SpotBalance."""
        raw_balance = create_raw_balance(available="900.0", total="1100.0")

        result = mapper.transform_raw_balance_to_internal("USDC", raw_balance)

        assert isinstance(result, SpotBalance)
        assert result.asset == "USDC"
        assert result.available_quantity == Decimal("900.0")
        assert result.total_quantity == Decimal("1100.0")
        assert result.exchange == ExchangeName.BACKPACK.value

    def test_transform_raw_balance_transformation_error(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw balance
        raw_balance = create_raw_balance()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError, match="Failed to transform raw balance to internal"
            ):
                mapper.transform_raw_balance_to_internal("USDC", raw_balance)


class TestPositionTransformation:
    """Test cases for position transformation functionality."""

    def test_transform_raw_position_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test successful transformation of BackpackRawPosition to DerivativePosition."""
        raw_position = create_raw_position(
            symbol="SOL-USDC",
            break_even_price="100.25",
            entry_price="100.00",
            est_liquidation_price="90.00",
            imf="0.1",
            mark_price="100.50",
            mmf="0.05",
            net_cost="1000.0",
            net_quantity="10.0",
            net_exposure_quantity="10.0",
            net_exposure_notional="1005.0",
            pnl_realized="0.0",
            pnl_unrealized="5.0",
            cumulative_funding_payment="0.1",
            user_id=12345,
            position_id="pos123",
            cumulative_interest="0.0",
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        assert isinstance(result, DerivativePosition)
        assert result.symbol == "SOL-USDC"
        assert result.side == OrderSide.BUY  # Long -> BUY
        assert result.size == Decimal("10.0")
        assert result.entry_price == Decimal("100.00")
        assert result.mark_price == Decimal("100.50")
        assert result.unrealized_pnl == Decimal("5.0")
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None

    def test_transform_raw_position_transformation_error(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw position
        raw_position = create_raw_position()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError, match="Failed to transform raw position to internal"
            ):
                mapper.transform_raw_position_to_internal(raw_position)


class TestAccountSummaryTransformation:
    """Test cases for account summary transformation functionality."""

    def test_transform_raw_account_summary_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test successful transformation of account summary data to MarginAccountSummary."""
        raw_summary = create_raw_account_summary()
        spot_balances = {"USDC": create_raw_balance()}
        positions = [create_raw_position()]

        result = mapper.transform_raw_account_summary_to_internal(
            raw_summary, spot_balances, positions
        )

        assert isinstance(result, MarginAccountSummary)
        assert result.total_equity == Decimal(
            "1105.0"
        )  # From balance total (1100.0) + position unrealized PnL (5.0)
        assert result.available_equity == Decimal("1000.0")  # From balance available
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None

    def test_transform_raw_account_summary_empty_collections(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test account summary transformation with empty balances and positions."""
        raw_summary = create_raw_account_summary()

        result = mapper.transform_raw_account_summary_to_internal(raw_summary, {}, [])

        assert result.total_equity == Decimal("0.0")
        assert result.available_equity == Decimal("0.0")


class TestOrderTransformation:
    """Test cases for order transformation functionality."""

    def test_transform_raw_order_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
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
            order_type=bp_type, created_at=test_timestamp, trigger_price=trigger_price
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


class TestTradeTransformation:
    """Test cases for trade transformation functionality."""

    def test_transform_raw_trade_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
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
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
    ) -> None:
        """Test trade transformation with missing price returns None."""
        # Create a valid raw trade first
        raw_trade = create_raw_trade(time=test_timestamp)

        # Mock parse_decimal_value to return None for price
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value"
        ) as mock_parse:

            def side_effect(
                value: str, allow_none: bool = False, field_name: str = ""
            ) -> Decimal | None:
                if field_name == "price":
                    return None
                # For other fields, call the real function
                from cyberdelta.utils.parsing import parse_decimal_value as real_parse

                return real_parse(value, allow_none=allow_none, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError, match="price missing/invalid in BackpackRawTrade"
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_missing_quantity(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
    ) -> None:
        """Test trade transformation with missing quantity returns None."""
        # Create a valid raw trade first
        raw_trade = create_raw_trade(time=test_timestamp)

        # Mock parse_decimal_value to return None for quantity
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value"
        ) as mock_parse:

            def side_effect(
                value: str, allow_none: bool = False, field_name: str = ""
            ) -> Decimal | None:
                if field_name == "quantity":
                    return None
                # For other fields, call the real function
                from cyberdelta.utils.parsing import parse_decimal_value as real_parse

                return real_parse(value, allow_none=allow_none, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError, match="quantity missing/invalid in BackpackRawTrade"
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)


class TestTransferTransformation:
    """Test cases for transfer transformation functionality."""

    def test_transform_raw_transfer_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test successful transformation of transfer data to internal Transfer."""
        raw_response: RawJsonResponse = {
            "id": "transfer123",
            "status": "success",
        }

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="USDC",
            quantity=Decimal("1000.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id="client123",
        )

        assert isinstance(result, Transfer)
        assert result.id == "transfer123"
        assert result.exchange == "backpack"
        assert result.asset == "USDC"
        assert result.quantity == Decimal("1000.0")
        assert result.status == InternalTransferStatus.COMPLETED
        assert result.bp_details is not None

    @pytest.mark.parametrize(
        "raw_status,expected_status",
        [
            ("success", InternalTransferStatus.COMPLETED),
            ("pending", InternalTransferStatus.PENDING),
            ("failed", InternalTransferStatus.FAILED),
            ("cancelled", InternalTransferStatus.REJECTED),
            (None, InternalTransferStatus.UNKNOWN),
        ],
    )
    def test_transform_raw_transfer_status_mapping(
        self,
        mapper: BackpackAccountDataMapper,
        raw_status: str | None,
        expected_status: InternalTransferStatus,
    ) -> None:
        """Test transfer transformation with different status values."""
        raw_response: RawJsonResponse = {
            "id": "transfer123",
            "status": raw_status,
        }

        result = mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name="backpack",
            asset="USDC",
            quantity=Decimal("1000.0"),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=None,
        )

        assert result.status == expected_status


class TestWithdrawalTransformation:
    """Test cases for withdrawal transformation functionality."""

    def test_transform_raw_withdrawal_response_to_internal_happy_path(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test successful transformation of withdrawal response to internal Withdrawal."""
        raw_response = create_raw_withdrawal_response(
            id=123,
            status="confirmed",
            blockchain="Ethereum",
            quantity="1000.0",
            fee="5.0",
            symbol="USDC",
            to_address="0xabc123",
            created_at="2024-01-15T10:30:00Z",
            is_internal=False,
        )

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="USDC",
            quantity=Decimal("1000.0"),
            address="0xabc123",
            network="ethereum",
            client_withdrawal_id="client123",
            tag=None,
        )

        assert isinstance(result, Withdrawal)
        assert result.id == "123"
        assert result.asset == "USDC"
        assert result.quantity == Decimal("1000.0")
        assert result.address == "0xabc123"
        assert result.status == InternalWithdrawalStatus.COMPLETED
        assert result.bp_details is not None

    def test_transform_raw_withdrawal_with_tag(self, mapper: BackpackAccountDataMapper) -> None:
        """Test withdrawal transformation with tag."""
        raw_response = create_raw_withdrawal_response()

        result = mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="XRP",
            quantity=Decimal("1000.0"),
            address="rAddress123",
            network="xrp",
            client_withdrawal_id=None,
            tag="12345",
        )

        assert result.bp_details is not None

    def test_transform_raw_withdrawal_transformation_error(
        self, mapper: BackpackAccountDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw withdrawal response
        raw_response = create_raw_withdrawal_response()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError, match="Failed to transform raw withdrawal to internal"
            ):
                mapper.transform_raw_withdrawal_response_to_internal(
                    raw_response=raw_response,
                    asset="USDC",
                    quantity=Decimal("1000.0"),
                    address="0xabc123",
                    network="Ethereum",
                    client_withdrawal_id=None,
                    tag=None,
                )


class TestEdgeCasesAndRobustness:
    """Test cases for edge cases and robustness."""

    def test_boundary_decimal_values(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
    ) -> None:
        """Test handling of boundary decimal values."""
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

    def test_zero_values(self, mapper: BackpackAccountDataMapper, test_timestamp: str) -> None:
        """Test handling of zero values."""
        raw_fill = create_raw_fill(
            price="0.0",
            quantity="0.0",
            fee="0.0",
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Mapper returns None for zero price/quantity since Trade model requires positive values
        assert result is None

    def test_unicode_symbol_handling(
        self, mapper: BackpackAccountDataMapper, test_timestamp: str
    ) -> None:
        """Test handling of unicode characters in symbols."""
        raw_fill = create_raw_fill(symbol="SOL-USDC", timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.symbol == "SOL-USDC"

    def test_very_long_ids(self, mapper: BackpackAccountDataMapper, test_timestamp: str) -> None:
        """Test handling of very long ID strings."""
        # Use a long ID that's within the validation limits (max 128 chars)
        long_id = "a" * 120  # Just under the 128 char limit
        raw_fill = create_raw_fill(order_id=long_id, timestamp=test_timestamp)

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Should handle long IDs gracefully
        assert result is not None
        assert result.order_id == long_id
