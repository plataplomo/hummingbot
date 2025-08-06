"""CyberDeltaEngine: Hyperliquid Position & Transaction Mapper Tests.

--------------------------------------------------------------------------

Comprehensive test suite for HyperliquidPositionMapper and HyperliquidTransactionMapper.

Tests complex scenarios and advanced business logic including:
- Derivative position transformations with various leverage types (PositionMapper)
- Fill/trade transformations with detailed trade data (TransactionMapper)
- Integration scenarios with multiple positions
- Side determination and position sizing logic
- Cross-method transformation consistency
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

# Third-party imports for type checking only
# Project-specific imports
from cyberdelta.apis.hyperliquid.mappers.account.hl_position_mapper import HyperliquidPositionMapper
from cyberdelta.apis.hyperliquid.mappers.account.hl_transaction_mapper import (
    HyperliquidTransactionMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import (
    DerivativePosition,
    Fill,
    HyperliquidPositionDetails,
)
from cyberdelta.models.market.fill import HyperliquidFillDetails
from tests.common_symbols import BTC_HL, ETH_HL


# Aliases for shorter method calls
PositionMapper = HyperliquidPositionMapper
TransactionMapper = HyperliquidTransactionMapper

# --- Fixtures ---


@pytest.fixture
def position_mapper() -> HyperliquidPositionMapper:
    """Provide an instance of HyperliquidPositionMapper.

    Returns:
        HyperliquidPositionMapper: Instance of the position mapper for testing.
    """
    return HyperliquidPositionMapper()


@pytest.fixture
def transaction_mapper() -> HyperliquidTransactionMapper:
    """Provide an instance of HyperliquidTransactionMapper.

    Returns:
        HyperliquidTransactionMapper: Instance of the transaction mapper for testing.
    """
    return HyperliquidTransactionMapper()


@pytest.fixture
def raw_user_state_with_positions() -> HyperliquidRawClearinghouseState:
    """Fixture for raw user state with ETH and BTC derivative positions.

    Returns:
        HyperliquidRawClearinghouseState: Raw clearinghouse state with sample positions.
    """
    eth_position_asset = HyperliquidRawAssetPosition(
        asset="ETH-PERP",
        position=HyperliquidRawPositionInfo(
            coin="ETH-PERP",
            szi="1.0",
            entryPx="3000.0",
            leverage=HyperliquidRawLeverage(type="cross", value=10),
            liquidationPx="2700.0",
            marginUsed="300.0",
            maxLeverage=50,
            positionValue="3000.0",
            returnOnEquity="0.05",
            unrealizedPnl="150.0",
            cumFunding=None,
        ),
        type=None,
    )
    btc_position_asset = HyperliquidRawAssetPosition(
        asset="BTC-PERP",
        position=HyperliquidRawPositionInfo(
            coin="BTC-PERP",
            szi="-0.5",  # Short position
            entryPx="60000.0",
            leverage=HyperliquidRawLeverage(type="isolated", value=5),
            liquidationPx="63000.0",
            marginUsed="6000.0",
            maxLeverage=20,
            positionValue="30000.0",
            returnOnEquity="-0.02",
            unrealizedPnl="-600.0",
            cumFunding=None,
        ),
        type=None,
    )

    updated_asset_positions_data = [
        eth_position_asset.model_dump(by_alias=False),
        btc_position_asset.model_dump(by_alias=False),
    ]

    updated_data_python_names = {
        "asset_positions": updated_asset_positions_data,
        "cross_maintenance_margin_used": "50.0",
        "cross_margin_summary": {
            "account_value": "10000.0",
            "total_margin_used": "1000.0",
            "total_ntl_pos": "9000.0",
            "total_raw_usd": "8000.0",
        },
        "margin_summary": {
            "account_value": "10000.0",
            "total_margin_used": "1000.0",
            "total_ntl_pos": "9000.0",
            "total_raw_usd": "8000.0",
        },
        "isolated_maintenance_margin_used": "100.0",
        "isolated_margin_summary": {
            "account_value": "0",
            "total_margin_used": "0",
            "total_ntl_pos": "0",
            "total_raw_usd": "0",
        },
        "withdrawable": "7000.0",
    }
    return HyperliquidRawClearinghouseState.model_validate(updated_data_python_names)


@pytest.fixture
def hyperliquid_raw_fill_buy_fixture() -> HyperliquidRawFill:
    """Return a valid HyperliquidRawFill for a BUY trade."""
    return HyperliquidRawFill(
        tid=12345,
        coin="ETH-PERP",
        px="3005.50",
        sz="0.5",
        time=int(datetime.now(UTC).timestamp() * 1000 - 10000),  # 10 seconds ago
        side="B",
        oid=67890,
        startPosition="0.0",
        dir="Open Long",
        hash="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        fee="1.50275",
        isMaker=False,
        liquidationMarkPx=None,
        cloid="my_buy_order_1",
    )


@pytest.fixture
def hyperliquid_raw_fill_sell_maker_fixture() -> HyperliquidRawFill:
    """Return a valid HyperliquidRawFill for a SELL MAKER trade."""
    return HyperliquidRawFill(
        tid=54321,
        coin="BTC-PERP",
        px="60000.00",
        sz="0.01",
        time=int(datetime.now(UTC).timestamp() * 1000 - 5000),  # 5 seconds ago
        side="A",  # Sell
        oid=98760,
        startPosition="0.1",  # Had a long position before this sell
        dir="Close Long",
        hash="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        fee="0.00",  # Maker trade, zero fee
        isMaker=True,
        liquidationMarkPx="50000.00",
        cloid="",  # Empty string instead of None
    )


# --- Tests for derivative position transformations ---


class TestMapRawClearinghouseStateToDerivativePositions:
    """Tests for transform_raw_clearinghouse_state_to_derivative_positions method."""

    def test_empty_derivative_positions(self) -> None:
        """Test mapping when raw state has no derivative positions."""
        empty_margin_summary = HyperliquidRawMarginSummary(
            accountValue="0",
            totalRawUsd="0",
            totalMarginUsed="0",
            totalNtlPos="0",
        )
        raw_user_state_empty = HyperliquidRawClearinghouseState(
            assetPositions=[],
            marginSummary=empty_margin_summary,
            crossMaintenanceMarginUsed="0",
            crossMarginSummary=empty_margin_summary,
            isolatedMaintenanceMarginUsed="0",
            isolatedMarginSummary=empty_margin_summary,
            withdrawable="0",
            time=1640995200000,
        )

        position_mapper = PositionMapper()
        positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_user_state_empty,
        )
        assert isinstance(positions, dict)
        assert not positions

    def test_populated_derivative_positions(
        self,
        raw_user_state_with_positions: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test mapping with ETH long and BTC short positions."""
        position_mapper = PositionMapper()
        positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_user_state_with_positions,
        )

        assert isinstance(positions, dict)
        assert len(positions) == 2
        assert "ETH-PERP" in positions
        assert "BTC-PERP" in positions

        # Validate ETH position (long)
        eth_pos = positions["ETH-PERP"]
        assert isinstance(eth_pos, DerivativePosition)
        assert eth_pos.exchange == ExchangeName.HYPERLIQUID.value
        assert eth_pos.symbol == ETH_HL
        assert eth_pos.side == OrderSide.BUY
        assert eth_pos.size == Decimal("1.0")
        assert eth_pos.entry_price == Decimal("3000.0")
        assert isinstance(eth_pos.timestamp, datetime)
        assert eth_pos.unrealized_pnl == Decimal("150.0")
        assert eth_pos.liquidation_price == Decimal("2700.0")
        assert eth_pos.hl_details is not None
        assert isinstance(eth_pos.hl_details, HyperliquidPositionDetails)
        assert eth_pos.hl_details.leverage_type == "cross"
        assert eth_pos.hl_details.leverage_value == 10
        assert eth_pos.hl_details.max_leverage == 50
        assert eth_pos.hl_details.margin_used == Decimal("300.0")
        assert eth_pos.bp_details is None

        # Validate BTC position (short)
        btc_pos = positions["BTC-PERP"]
        assert isinstance(btc_pos, DerivativePosition)
        assert btc_pos.exchange == ExchangeName.HYPERLIQUID.value
        assert btc_pos.symbol == BTC_HL
        assert btc_pos.side == OrderSide.SELL
        assert btc_pos.size == Decimal("-0.5")
        assert btc_pos.entry_price == Decimal("60000.0")
        assert isinstance(btc_pos.timestamp, datetime)
        assert btc_pos.unrealized_pnl == Decimal("-600.0")
        assert btc_pos.liquidation_price == Decimal("63000.0")
        assert btc_pos.hl_details is not None
        assert isinstance(btc_pos.hl_details, HyperliquidPositionDetails)
        assert btc_pos.hl_details.leverage_type == "isolated"
        assert btc_pos.hl_details.leverage_value == 5
        assert btc_pos.hl_details.max_leverage == 20
        assert btc_pos.hl_details.margin_used == Decimal("6000.0")
        assert btc_pos.bp_details is None

    def test_position_side_determination_logic(self) -> None:
        """Test that position side is correctly determined from size."""
        test_cases = [
            ("10.0", OrderSide.BUY),  # Positive size = BUY (long)
            ("-5.0", OrderSide.SELL),  # Negative size = SELL (short)
            ("0.0", OrderSide.BUY),  # Zero size = BUY (default)
            ("0.000001", OrderSide.BUY),  # Very small positive = BUY
            ("-0.000001", OrderSide.SELL),  # Very small negative = SELL
        ]

        for size_str, expected_side in test_cases:
            # For zero positions, entry price will be set to None by the mapper
            entry_px = "0.0" if size_str == "0.0" else "1000.0"
            position_info = HyperliquidRawPositionInfo(
                coin="TEST-PERP",
                szi=size_str,
                entryPx=entry_px,
                leverage=HyperliquidRawLeverage(type="cross", value=1),
                liquidationPx="900.0",
                marginUsed="100.0",
                maxLeverage=10,
                positionValue="1000.0",
                returnOnEquity="0.0",
                unrealizedPnl="0.0",
                cumFunding=None,
            )
            asset_position = HyperliquidRawAssetPosition(
                asset="TEST-PERP",
                position=position_info,
                type=None,
            )

            raw_state = HyperliquidRawClearinghouseState(
                assetPositions=[asset_position],
                marginSummary=HyperliquidRawMarginSummary(
                    accountValue="1000.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                crossMaintenanceMarginUsed="0.0",
                crossMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="1000.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                isolatedMaintenanceMarginUsed="0.0",
                isolatedMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="0.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="0.0",
                    totalNtlPos="0.0",
                ),
                withdrawable="900.0",
                time=1640995200000,
            )

            position_mapper = PositionMapper()
            positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                raw_state,
            )

            position = positions["TEST-PERP"]
            assert position.side == expected_side

            # Verify entry price handling for zero positions
            if size_str == "0.0":
                assert position.entry_price is None
            else:
                assert position.entry_price is not None

    def test_leverage_type_variations(self) -> None:
        """Test positions with different leverage types."""
        leverage_types = [
            ("cross", 10),
            ("isolated", 5),
            ("cross", 1),
            ("isolated", 20),
        ]

        for leverage_type, leverage_value in leverage_types:
            position_info = HyperliquidRawPositionInfo(
                coin="TEST-PERP",
                szi="1.0",
                entryPx="1000.0",
                leverage=HyperliquidRawLeverage(type=leverage_type, value=leverage_value),
                liquidationPx="900.0",
                marginUsed="100.0",
                maxLeverage=50,
                positionValue="1000.0",
                returnOnEquity="0.1",
                unrealizedPnl="50.0",
                cumFunding=None,
            )
            asset_position = HyperliquidRawAssetPosition(
                asset="TEST-PERP",
                position=position_info,
                type=None,
            )

            raw_state = HyperliquidRawClearinghouseState(
                assetPositions=[asset_position],
                marginSummary=HyperliquidRawMarginSummary(
                    accountValue="1050.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                crossMaintenanceMarginUsed="0.0",
                crossMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="1050.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                isolatedMaintenanceMarginUsed="0.0",
                isolatedMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="0.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="0.0",
                    totalNtlPos="0.0",
                ),
                withdrawable="950.0",
                time=1640995200000,
            )

            position_mapper = PositionMapper()
            positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                raw_state,
            )

            position = positions["TEST-PERP"]
            assert position.hl_details is not None
            assert position.hl_details.leverage_type == leverage_type
            assert position.hl_details.leverage_value == leverage_value
            assert position.hl_details.max_leverage == 50

    def test_high_precision_position_values(self) -> None:
        """Test position transformation with high precision decimal values."""
        position_info = HyperliquidRawPositionInfo(
            coin="PRECISION-PERP",
            szi="1.123456789012345",
            entryPx="1000.987654321098765",
            leverage=HyperliquidRawLeverage(type="cross", value=10),
            liquidationPx="900.111111111111111",
            marginUsed="100.555555555555555",
            maxLeverage=50,
            positionValue="1000.999999999999999",
            returnOnEquity="0.123456789012345",
            unrealizedPnl="50.987654321098765",
            cumFunding=None,
        )
        asset_position = HyperliquidRawAssetPosition(
            asset="PRECISION-PERP",
            position=position_info,
            type=None,
        )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=[asset_position],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="1051.0",
                totalRawUsd="0.0",
                totalMarginUsed="100.555555555555555",
                totalNtlPos="1000.999999999999999",
            ),
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="1051.0",
                totalRawUsd="0.0",
                totalMarginUsed="100.555555555555555",
                totalNtlPos="1000.999999999999999",
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="950.0",
            time=1640995200000,
        )

        position_mapper = PositionMapper()
        positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        position = positions["PRECISION-PERP"]

        # Verify precision is maintained (8 decimal places for most fields)
        assert position.size == Decimal("1.12345679")
        assert position.entry_price == Decimal("1000.98765432")
        assert position.liquidation_price == Decimal("900.11111111")
        assert position.unrealized_pnl == Decimal("50.98765432")
        assert position.hl_details is not None
        assert position.hl_details.margin_used == Decimal("100.555555555555555")

    def test_position_timestamp_generation(
        self,
        raw_user_state_with_positions: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that position transformations generate appropriate timestamps."""
        position_mapper = PositionMapper()
        positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_user_state_with_positions,
        )

        for position in positions.values():
            assert position.timestamp is not None
            assert isinstance(position.timestamp, datetime)

            # Timestamp should be very recent
            current_time = datetime.now(UTC)
            time_diff = current_time - position.timestamp
            assert time_diff.total_seconds() < 5


# --- Tests for fill/trade transformations ---


class TestTransformRawFillToInternal:
    """Tests for transform_raw_fill_to_internal method."""

    def test_buy_taker_transformation(
        self,
        hyperliquid_raw_fill_buy_fixture: HyperliquidRawFill,
    ) -> None:
        """Test transforming a raw BUY TAKER fill to an internal Fill model."""
        raw_fill = hyperliquid_raw_fill_buy_fixture
        transaction_mapper = TransactionMapper()
        trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)

        assert isinstance(trade, Fill)
        assert trade.id == str(raw_fill.tid)
        assert trade.symbol.value == raw_fill.coin
        assert isinstance(trade.executed_at, datetime)
        assert trade.executed_at.timestamp() * 1000 == raw_fill.time
        assert trade.side == OrderSide.BUY
        assert trade.order_id == str(raw_fill.oid)
        assert trade.exchange == ExchangeName.HYPERLIQUID.value
        assert trade.client_order_id == raw_fill.cloid
        assert trade.price == Decimal(raw_fill.px)
        assert trade.quantity == Decimal(raw_fill.sz)
        assert trade.fee == Decimal(raw_fill.fee)
        assert trade.fee_asset == raw_fill.coin
        assert trade.is_maker == raw_fill.is_maker

        assert trade.hl_details is not None
        assert isinstance(trade.hl_details, HyperliquidFillDetails)
        assert trade.hl_details.fill_hash == raw_fill.hash
        assert trade.hl_details.liquidation_mark_px is None
        assert trade.hl_details.start_position == Decimal(raw_fill.start_position)
        assert trade.hl_details.dir == raw_fill.dir
        assert trade.bp_details is None

    def test_sell_maker_transformation(
        self,
        hyperliquid_raw_fill_sell_maker_fixture: HyperliquidRawFill,
    ) -> None:
        """Test transforming a raw SELL MAKER fill to an internal Fill model."""
        raw_fill = hyperliquid_raw_fill_sell_maker_fixture
        transaction_mapper = TransactionMapper()
        trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)

        assert isinstance(trade, Fill)
        assert trade.id == str(raw_fill.tid)
        assert trade.symbol.value == raw_fill.coin
        assert isinstance(trade.executed_at, datetime)
        assert trade.executed_at.timestamp() * 1000 == raw_fill.time
        assert trade.side == OrderSide.SELL
        assert trade.order_id == str(raw_fill.oid)
        assert trade.exchange == ExchangeName.HYPERLIQUID.value
        assert trade.client_order_id is None  # Empty string becomes None
        assert trade.price == Decimal(raw_fill.px)
        assert trade.quantity == Decimal(raw_fill.sz)
        assert trade.fee == Decimal(raw_fill.fee)
        assert trade.fee_asset == raw_fill.coin
        assert trade.is_maker == raw_fill.is_maker

        assert trade.hl_details is not None
        assert isinstance(trade.hl_details, HyperliquidFillDetails)
        assert trade.hl_details.fill_hash == raw_fill.hash
        assert raw_fill.liquidation_mark_px is not None
        assert trade.hl_details.liquidation_mark_px == Decimal(raw_fill.liquidation_mark_px)
        assert trade.hl_details.start_position == Decimal(raw_fill.start_position)
        assert trade.hl_details.dir == raw_fill.dir
        assert trade.bp_details is None

    def test_fill_side_mapping(self) -> None:
        """Test that fill sides are correctly mapped to order sides."""
        test_cases = [
            ("B", OrderSide.BUY),
            ("A", OrderSide.SELL),
        ]

        for fill_side, expected_order_side in test_cases:
            raw_fill = HyperliquidRawFill(
                tid=12345,
                coin="TEST-PERP",
                px="1000.0",
                sz="1.0",
                time=int(datetime.now(UTC).timestamp() * 1000),
                side=fill_side,
                oid=67890,
                startPosition="0.0",
                dir="Open",
                hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                fee="1.0",
                isMaker=False,
                liquidationMarkPx=None,
                cloid="test",
            )

            transaction_mapper = TransactionMapper()
            trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)
            assert trade.side == expected_order_side

    def test_fill_with_high_precision_values(self) -> None:
        """Test fill transformation with high precision decimal values."""
        raw_fill = HyperliquidRawFill(
            tid=99999,
            coin="PRECISION-PERP",
            px="1000.123456789012345",
            sz="0.987654321098765",
            time=int(datetime.now(UTC).timestamp() * 1000),
            side="B",
            oid=88888,
            startPosition="5.111111111111111",
            dir="Reduce Long",
            hash="0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            fee="1.555555555555555",
            isMaker=True,
            liquidationMarkPx="950.999999999999999",
            cloid="precision_test",
        )

        transaction_mapper = TransactionMapper()
        trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)

        # Verify precision is maintained (8 decimal places for most fields)
        assert trade.price == Decimal("1000.12345679")
        assert trade.quantity == Decimal("0.98765432")
        assert trade.fee == Decimal("1.55555556")
        assert trade.hl_details is not None
        assert trade.hl_details.start_position == Decimal("5.11111111")
        assert trade.hl_details.liquidation_mark_px == Decimal(951)

    def test_fill_with_zero_fee(self) -> None:
        """Test fill transformation with zero fee (maker trades)."""
        raw_fill = HyperliquidRawFill(
            tid=11111,
            coin="ZERO-FEE-PERP",
            px="2000.0",
            sz="0.5",
            time=int(datetime.now(UTC).timestamp() * 1000),
            side="A",
            oid=22222,
            startPosition="1.0",
            dir="Close Long",
            hash="0xfeeddead1234567890abcdef1234567890abcdef1234567890abcdef12345678",
            fee="0.0",
            isMaker=True,
            liquidationMarkPx=None,
            cloid="zero_fee_test",
        )

        transaction_mapper = TransactionMapper()
        trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)

        assert trade.fee == Decimal("0.0")
        assert trade.is_maker is True

    def test_fill_timestamp_conversion(self) -> None:
        """Test that fill timestamps are correctly converted from milliseconds."""
        # Use a specific timestamp for testing
        test_timestamp_ms = 1640995200000  # January 1, 2022 00:00:00 UTC
        expected_datetime = datetime.fromtimestamp(test_timestamp_ms / 1000, tz=UTC)

        raw_fill = HyperliquidRawFill(
            tid=33333,
            coin="TIMESTAMP-PERP",
            px="1500.0",
            sz="1.0",
            time=test_timestamp_ms,
            side="B",
            oid=44444,
            startPosition="0.0",
            dir="Open Long",
            hash="0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba",
            fee="1.5",
            isMaker=False,
            liquidationMarkPx=None,
            cloid="timestamp_test",
        )

        transaction_mapper = TransactionMapper()
        trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)

        assert trade.executed_at == expected_datetime

    def test_fill_client_order_id_handling(self) -> None:
        """Test handling of various client order ID values."""
        test_cases = [
            ("valid_client_id", "valid_client_id"),
            ("", None),  # Empty string becomes None
            ("a" * 60, "a" * 60),  # Long client ID within 64 char limit
        ]

        for input_cloid, expected_cloid in test_cases:
            raw_fill = HyperliquidRawFill(
                tid=55555,
                coin="CLOID-PERP",
                px="1000.0",
                sz="1.0",
                time=int(datetime.now(UTC).timestamp() * 1000),
                side="B",
                oid=66666,
                startPosition="0.0",
                dir="Open Long",
                hash="0xdeadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678",
                # Valid 66-char hash
                fee="1.0",
                isMaker=False,
                liquidationMarkPx=None,
                cloid=input_cloid,
            )

            transaction_mapper = TransactionMapper()
            trade = transaction_mapper.transform_raw_fill_to_internal(raw_fill)
            assert trade.client_order_id == expected_cloid


# --- Integration tests for complex scenarios ---


class TestPositionAndFillIntegration:
    """Integration tests combining position and trade transformations."""

    def test_position_and_trade_consistency(
        self,
        raw_user_state_with_positions: HyperliquidRawClearinghouseState,
        hyperliquid_raw_fill_buy_fixture: HyperliquidRawFill,
    ) -> None:
        """Test that position and trade transformations are consistent."""
        # Get positions
        position_mapper = PositionMapper()
        positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_user_state_with_positions,
        )

        # Get trade
        transaction_mapper = TransactionMapper()
        trade = transaction_mapper.transform_raw_fill_to_internal(hyperliquid_raw_fill_buy_fixture)

        # Both should have consistent exchange assignments
        for position in positions.values():
            assert position.exchange == trade.exchange == ExchangeName.HYPERLIQUID.value

        # Both should have recent timestamps
        current_time = datetime.now(UTC)
        for position in positions.values():
            assert (current_time - position.timestamp).total_seconds() < 10

        assert (current_time - trade.executed_at).total_seconds() < 20000  # Within test range

    def test_multiple_positions_transformation_efficiency(
        self,
    ) -> None:
        """Test transformation efficiency with multiple positions."""
        # Create multiple positions
        asset_positions: list[HyperliquidRawAssetPosition] = []
        for i in range(10):
            symbol = f"TEST{i}-PERP"
            position_info = HyperliquidRawPositionInfo(
                coin=symbol,
                szi=str(1.0 + i * 0.1),
                entryPx=str(1000.0 + i * 100),
                leverage=HyperliquidRawLeverage(
                    type="cross" if i % 2 == 0 else "isolated",
                    value=5 + i,
                ),
                liquidationPx=str(900.0 + i * 90),
                marginUsed=str(100.0 + i * 10),
                maxLeverage=50,
                positionValue=str(1000.0 + i * 110),
                returnOnEquity=str(0.01 * i),
                unrealizedPnl=str(10.0 + i * 5),
                cumFunding=None,
            )
            asset_positions.append(
                HyperliquidRawAssetPosition(asset=symbol, position=position_info, type=None),
            )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=asset_positions,
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="15000.0",
                totalRawUsd="0.0",
                totalMarginUsed="1450.0",
                totalNtlPos="11000.0",
            ),
            crossMaintenanceMarginUsed="100.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="15000.0",
                totalRawUsd="0.0",
                totalMarginUsed="1450.0",
                totalNtlPos="11000.0",
            ),
            isolatedMaintenanceMarginUsed="50.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="13500.0",
            time=1640995200000,
        )

        # Transform all positions
        position_mapper = PositionMapper()
        positions = position_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        # Verify all positions were transformed correctly
        assert len(positions) == 10

        for i, (symbol, position) in enumerate(positions.items()):
            assert symbol == f"TEST{i}-PERP"
            assert position.symbol.value == symbol
            assert position.exchange == ExchangeName.HYPERLIQUID.value
            assert position.hl_details is not None
            assert position.hl_details.leverage_type in ["cross", "isolated"]
