"""CyberDeltaEngine: Backpack Account Data Mapper Balance and Position Tests.

--------------------------------------------------------------------------

Comprehensive test suite for BackpackAccountDataMapper balance and position methods.
Tests balance and position transformation methods with various scenarios including:
- Balance transformations for spot balances
- Position transformations for derivative positions
- Account summary transformations
- Error handling and edge cases
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import DerivativePosition, MarginAccountSummary, SpotBalance
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName


pytestmark = pytest.mark.timing


@pytest.fixture
def mapper() -> BackpackAccountDataMapper:
    """Fixture providing a BackpackAccountDataMapper instance.

    Returns:
        BackpackAccountDataMapper: Mapper instance for testing.
    """
    return BackpackAccountDataMapper()


def create_raw_balance(
    available: str = "1000.0",
    locked: str = "50.0",
    staked: str = "50.0",
) -> BackpackRawBalance:
    """Create BackpackRawBalance instances for testing.

    Returns:
        BackpackRawBalance: Raw balance object for testing.
    """
    return BackpackRawBalance(
        available=available,
        locked=locked,
        staked=staked,
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
    """Create BackpackRawPosition instances for testing.

    Returns:
        BackpackRawPosition: Raw position object for testing.
    """
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
        subaccountId=0,  # Add missing required field
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
    """Create BackpackRawAccountSummary instances for testing.

    Returns:
        BackpackRawAccountSummary: Raw account summary object for testing.
    """
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
        },
    )


class TestBalanceTransformation:
    """Test cases for balance transformation functionality."""

    def test_transform_balance_data_to_spot_balance_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
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
        assert isinstance(result.timestamp, datetime)

    def test_transform_balance_data_zero_values(self, mapper: BackpackAccountDataMapper) -> None:
        """Test balance transformation with zero values."""
        result = mapper.transform_balance_data_to_spot_balance(
            asset="BTC",
            total_balance="0.0",
            available_balance="0.0",
        )

        assert result.total_quantity == Decimal("0.0")
        assert result.available_quantity == Decimal("0.0")
        assert result.asset == "BTC"

    def test_transform_balance_data_transformation_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Mock parse_decimal_value to raise an error
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError,
                match="Failed to transform balance data to SpotBalance",
            ):
                mapper.transform_balance_data_to_spot_balance(
                    asset="USDC",
                    total_balance="invalid",
                    available_balance="900.0",
                )

    def test_transform_raw_balance_to_internal_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test successful transformation of BackpackRawBalance to SpotBalance."""
        raw_balance = create_raw_balance(available="900.0", locked="100.0", staked="100.0")

        result = mapper.transform_raw_balance_to_internal("USDC", raw_balance)

        assert isinstance(result, SpotBalance)
        assert result.asset == "USDC"
        assert result.available_quantity == Decimal("900.0")
        assert result.total_quantity == Decimal("1100.0")  # 900 + 100 + 100
        assert result.exchange == ExchangeName.BACKPACK
        assert result.bp_details is not None

    def test_transform_raw_balance_different_assets(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test balance transformation with different asset types."""
        assets = ["BTC", "ETH", "SOL", "AVAX"]

        for asset in assets:
            raw_balance = create_raw_balance(available="500.0", locked="50.0", staked="50.0")
            result = mapper.transform_raw_balance_to_internal(asset, raw_balance)

            assert result.asset == asset.upper()
            assert result.available_quantity == Decimal("500.0")
            assert result.total_quantity == Decimal("600.0")  # 500 + 50 + 50

    def test_transform_raw_balance_missing_locked_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that missing locked balance raises TransformationError."""
        raw_balance = create_raw_balance()

        # Mock parse_decimal_value to return None for locked
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing balance validation."""
                if field_name.endswith("_locked"):
                    return None
                # For other fields, return a valid decimal
                return Decimal(value)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError,
                match="Locked quantity missing/invalid for USDC in BackpackRawBalance",
            ):
                mapper.transform_raw_balance_to_internal("USDC", raw_balance)

    def test_transform_raw_balance_missing_available_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that missing available raises TransformationError."""
        raw_balance = create_raw_balance()

        # Mock parse_decimal_value to return None for available
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return Decimal conversion for testing available balance validation."""
                if field_name.endswith("_available"):
                    return None
                # For other fields, return a valid decimal
                return Decimal(value)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError,
                match="Available quantity missing/invalid for USDC in BackpackRawBalance",
            ):
                mapper.transform_raw_balance_to_internal("USDC", raw_balance)

    def test_transform_raw_balance_missing_staked_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that missing staked balance raises TransformationError."""
        raw_balance = create_raw_balance()

        # Mock parse_decimal_value to return None for staked
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing balance validation."""
                if field_name.endswith("_staked"):
                    return None
                # For other fields, return a valid decimal
                return Decimal(value)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError,
                match="Staked quantity missing/invalid for USDC in BackpackRawBalance",
            ):
                mapper.transform_raw_balance_to_internal("USDC", raw_balance)

    def test_transform_raw_balance_boundary_values(self, mapper: BackpackAccountDataMapper) -> None:
        """Test balance transformation with boundary decimal values."""
        raw_balance = create_raw_balance(
            available="0.000001",  # Very small available
            locked="500000000.0",  # Large locked
            staked="499999999.999998",  # Large staked (total will be 999999999.999999)
        )

        result = mapper.transform_raw_balance_to_internal("USDC", raw_balance)

        assert result.available_quantity == Decimal("0.000001")
        assert result.total_quantity == Decimal("999999999.999999")

    def test_transform_raw_balance_high_precision_decimals(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test balance transformation with high precision decimal values."""
        raw_balance = create_raw_balance(
            available="123.123456789012345",
            locked="200.0",
            staked="133.864197532086420",  # Total will be 456.987654321098765
        )

        result = mapper.transform_raw_balance_to_internal("USDC", raw_balance)

        assert result.available_quantity == Decimal("123.123456789012345")
        assert result.total_quantity == Decimal("456.987654321098765")


class TestPositionTransformation:
    """Test cases for position transformation functionality."""

    def test_transform_raw_position_to_internal_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
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
        assert result.liquidation_price == Decimal("90.00")
        assert result.unrealized_pnl == Decimal("5.0")
        assert result.realized_pnl == Decimal("0.0")
        assert result.exchange == ExchangeName.BACKPACK
        assert result.bp_details is not None
        assert result.bp_details.imf_base == Decimal("0.1")
        assert result.bp_details.mmf_base == Decimal("0.05")

    def test_transform_raw_position_short_position(self, mapper: BackpackAccountDataMapper) -> None:
        """Test position transformation for short position."""
        raw_position = create_raw_position(
            net_quantity="-10.0",  # Short position
            pnl_unrealized="-2.5",
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        assert result.side == OrderSide.SELL  # Short -> SELL
        assert result.size == Decimal("-10.0")
        assert result.unrealized_pnl == Decimal("-2.5")

    def test_transform_raw_position_zero_size_no_entry_price(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test position transformation with zero size sets entry price to None."""
        raw_position = create_raw_position(
            net_quantity="0.0",  # Zero position
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        assert result.size == Decimal("0.0")
        assert result.entry_price is None
        # Side is determined by size comparison, zero defaults to SELL
        assert result.side == OrderSide.SELL

    def test_transform_raw_position_missing_net_quantity_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that missing net_quantity raises TransformationError."""
        raw_position = create_raw_position()

        # Mock parse_decimal_value to return None for net_quantity
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return Decimal conversion for testing position quantity validation."""
                if field_name == "net_quantity":
                    return None
                # For other fields, return a valid decimal if possible
                try:
                    return Decimal(str(value)) if value else None
                except Exception:
                    return None

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError,
                match="net_quantity missing/invalid in BackpackRawPosition",
            ):
                mapper.transform_raw_position_to_internal(raw_position)

    def test_transform_raw_position_transformation_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        raw_position = create_raw_position()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError,
                match="Failed to transform raw position to internal",
            ):
                mapper.transform_raw_position_to_internal(raw_position)

    def test_transform_raw_position_negative_values(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test position transformation with negative cost and PnL values."""
        raw_position = create_raw_position(
            net_cost="-1000.0",  # Negative cost (short position)
            pnl_realized="-10.0",  # Realized loss
            pnl_unrealized="-5.0",  # Unrealized loss
            cumulative_funding_payment="-0.5",  # Negative funding
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        assert result.realized_pnl == Decimal("-10.0")
        assert result.unrealized_pnl == Decimal("-5.0")
        # DEFENSIVE CHECK: bp_details could be None after transformation.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result.bp_details is not None, "Expected bp_details but got None"
        assert result.bp_details.cumulative_funding == Decimal("-0.5")

    def test_transform_raw_position_large_values(self, mapper: BackpackAccountDataMapper) -> None:
        """Test position transformation with large position values."""
        raw_position = create_raw_position(
            net_quantity="1000000.0",  # Large position
            entry_price="50000.0",  # High price
            net_cost="50000000000.0",  # Large cost
            pnl_unrealized="1000000.0",  # Large PnL
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        assert result.size == Decimal("1000000.0")
        assert result.entry_price == Decimal("50000.0")
        assert result.unrealized_pnl == Decimal("1000000.0")

    def test_transform_raw_position_high_precision_decimals(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test position transformation with high precision decimal values."""
        raw_position = create_raw_position(
            net_quantity="10.123456789012345",
            entry_price="100.987654321098765",
            pnl_unrealized="5.555555555555555",
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        assert result.size == Decimal("10.123456789012345")
        assert result.entry_price == Decimal("100.987654321098765")
        assert result.unrealized_pnl == Decimal("5.555555555555555")

    def test_transform_raw_position_optional_fields_none(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test position transformation when optional fields can be parsed as None."""
        # The raw model doesn't allow empty strings, but parse_decimal_value can return None
        # for some cases. Let's test the actual scenario where parsing results in None values.
        raw_position = create_raw_position()

        # Mock parse_decimal_value to return None for specific optional fields
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing position data validation."""
                # Return valid values for required fields
                if field_name == "net_quantity":
                    return Decimal("10.0")
                # Return None for mark_price and liquidation_price (these are
                # optional in the mapper)
                if not field_name:  # When field_name is empty string, parse normally
                    try:
                        return Decimal(str(value)) if value else None
                    except Exception:
                        return None
                # For raw.mark_price and raw.est_liquidation_price calls
                if "mark_price" in str(value) or "est_liquidation_price" in str(value):
                    return None
                # For other fields, try to parse normally
                try:
                    return Decimal(str(value)) if value else None
                except Exception:
                    return None

            mock_parse.side_effect = side_effect

            result = mapper.transform_raw_position_to_internal(raw_position)

            assert result.size == Decimal("10.0")
            # Entry price should be parsed normally since size > 0
            assert result.entry_price is not None
            # These tests show that the method handles None values gracefully
            assert isinstance(result, DerivativePosition)

    def test_edge_case_very_long_position_ids(self, mapper: BackpackAccountDataMapper) -> None:
        """Test transformation with long position IDs (within valid limits)."""
        # Create a 60-character position ID (under the 64 char limit but still long)
        long_position_id = "pos_" + "a" * 56  # 4 + 56 = 60 chars total
        raw_position = create_raw_position(position_id=long_position_id)

        result = mapper.transform_raw_position_to_internal(raw_position)

        # Position ID is not directly exposed but should not cause errors
        assert result.symbol == "SOL-USDC"


class TestAccountSummaryTransformation:
    """Test cases for account summary transformation functionality."""

    def test_transform_raw_account_summary_to_internal_happy_path(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test successful transformation of account summary data to MarginAccountSummary."""
        raw_summary = create_raw_account_summary()
        spot_balances = {"USDC": create_raw_balance()}
        positions = [create_raw_position()]

        result = mapper.transform_raw_account_summary_to_internal(
            raw_summary,
            spot_balances,
            positions,
        )

        assert isinstance(result, MarginAccountSummary)
        # Total equity = balance total (1100.0) + position unrealized PnL (5.0)
        assert result.total_equity == Decimal("1105.0")
        assert result.available_equity == Decimal("1000.0")  # From balance available
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.bp_details is not None
        assert isinstance(result.timestamp, datetime)

    def test_transform_raw_account_summary_empty_collections(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test account summary transformation with empty balances and positions."""
        raw_summary = create_raw_account_summary()

        result = mapper.transform_raw_account_summary_to_internal(raw_summary, {}, [])

        assert result.total_equity == Decimal("0.0")
        assert result.available_equity == Decimal("0.0")
        assert result.total_position_notional == Decimal("0.0")
        assert result.total_unrealized_pnl == Decimal("0.0")

    def test_transform_raw_account_summary_multiple_balances(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test account summary with multiple USD-like balances."""
        raw_summary = create_raw_account_summary()
        spot_balances = {
            "USDC": create_raw_balance(available="900.0", locked="100.0", staked="0"),
            "USDT": create_raw_balance(available="450.0", locked="50.0", staked="0"),
            "BTC": create_raw_balance(available="1.8", locked="0.2", staked="0"),  # Non-USD
        }

        result = mapper.transform_raw_account_summary_to_internal(raw_summary, spot_balances, [])

        # Only USDC and USDT should be counted (USD-like assets)
        assert result.total_equity == Decimal("1500.0")  # 1000 + 500
        assert result.available_equity == Decimal("1350.0")  # 900 + 450

    def test_transform_raw_account_summary_multiple_positions(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test account summary with multiple positions."""
        raw_summary = create_raw_account_summary()
        spot_balances = {"USDC": create_raw_balance(available="900.0", locked="100.0", staked="0")}
        positions = [
            create_raw_position(
                symbol="SOL-USDC",
                net_quantity="10.0",
                entry_price="100.0",
                pnl_unrealized="50.0",
            ),
            create_raw_position(
                symbol="BTC-USDC",
                net_quantity="-0.1",
                entry_price="50000.0",
                pnl_unrealized="-25.0",
            ),
        ]

        result = mapper.transform_raw_account_summary_to_internal(
            raw_summary,
            spot_balances,
            positions,
        )

        # Total equity = 1000 (balance) + 50 - 25 (unrealized PnL) = 1025
        assert result.total_equity == Decimal("1025.0")
        assert result.total_unrealized_pnl == Decimal("25.0")  # 50 - 25
        # Position notional = |10 * 100| + |-0.1 * 50000| = 1000 + 5000 = 6000
        assert result.total_position_notional == Decimal("6000.0")

    def test_transform_raw_account_summary_none_unrealized_pnl_handled(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test account summary when positions have None unrealized PnL."""
        raw_summary = create_raw_account_summary()
        spot_balances = {"USDC": create_raw_balance(available="900.0", locked="100.0", staked="0")}

        # Create a position where the raw data has no unrealized PnL (parsing to None)
        position = create_raw_position(
            pnl_unrealized="0.0",
        )  # Use 0 which effectively means no PnL impact

        # Mock the position transformation to return a position with None unrealized_pnl
        with patch.object(mapper, "transform_raw_position_to_internal") as mock_transform:
            mock_position = DerivativePosition(
                exchange=ExchangeName.BACKPACK,
                symbol="SOL-USDC",
                timestamp=datetime.now(UTC),
                side=OrderSide.BUY,
                size=Decimal("10.0"),
                entry_price=Decimal("100.0"),
                unrealized_pnl=None,  # None unrealized PnL
            )
            mock_transform.return_value = mock_position

            result = mapper.transform_raw_account_summary_to_internal(
                raw_summary,
                spot_balances,
                [position],
            )

            # Should handle None unrealized_pnl gracefully (no PnL added)
            assert result.total_equity == Decimal("1000.0")  # Only balance, no PnL added
            assert result.total_unrealized_pnl == Decimal("0.0")

    def test_transform_raw_account_summary_none_entry_price_handled(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test account summary when positions have zero size (which sets entry_price to None)."""
        raw_summary = create_raw_account_summary()
        spot_balances = {"USDC": create_raw_balance(available="900.0", locked="100.0", staked="0")}

        # Create position with zero size, which should result in None entry_price and no PnL
        position = create_raw_position(net_quantity="0.0", pnl_unrealized="0.0")

        result = mapper.transform_raw_account_summary_to_internal(
            raw_summary,
            spot_balances,
            [position],
        )

        # Should handle None entry_price gracefully (no notional calculated for zero size)
        assert result.total_equity == Decimal("1000.0")  # Only balance, no PnL from zero position
        assert result.total_position_notional == Decimal(
            "0.0",
        )  # No notional for zero size position


class TestErrorHandling:
    """Test cases for error handling and edge cases."""

    def test_edge_case_unicode_asset_names(self, mapper: BackpackAccountDataMapper) -> None:
        """Test transformation with Unicode asset names."""
        raw_balance = create_raw_balance()

        result = mapper.transform_raw_balance_to_internal("USDC🚀", raw_balance)

        assert result.asset == "USDC🚀"

    def test_edge_case_high_user_ids(self, mapper: BackpackAccountDataMapper) -> None:
        """Test transformation with very high user IDs."""
        high_user_id = 999999999999999999  # Very large user ID
        raw_position = create_raw_position(user_id=high_user_id)

        result = mapper.transform_raw_position_to_internal(raw_position)

        # User ID is not directly exposed but should not cause errors
        assert result.symbol == "SOL-USDC"

    def test_balance_data_none_total_raises_error(self, mapper: BackpackAccountDataMapper) -> None:
        """Test that None total balance raises TransformationError."""
        # Mock parse_decimal_value to return None for total_balance
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing account summary validation."""
                if field_name == "total_balance":
                    return None
                return Decimal("900.0")  # Available balance

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError,
                match="Total and available balances are required",
            ):
                mapper.transform_balance_data_to_spot_balance(
                    asset="USDC",
                    total_balance="invalid",
                    available_balance="900.0",
                )

    def test_balance_data_none_available_raises_error(
        self,
        mapper: BackpackAccountDataMapper,
    ) -> None:
        """Test that None available balance raises TransformationError."""
        # Mock parse_decimal_value to return None for available_balance
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return appropriate Decimal conversion for testing balance parsing edge cases."""
                if field_name == "available_balance":
                    return None
                return Decimal("1000.0")  # Total balance

            mock_parse.side_effect = side_effect

            with pytest.raises(
                TransformationError,
                match="Total and available balances are required",
            ):
                mapper.transform_balance_data_to_spot_balance(
                    asset="USDC",
                    total_balance="1000.0",
                    available_balance="invalid",
                )
