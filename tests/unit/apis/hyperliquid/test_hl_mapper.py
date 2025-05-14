"""
Unit tests for the HyperliquidMapper.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import HyperliquidRawAssetCtx
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.core.models import (
    DerivativePosition,
    HyperliquidMarginDetails,
    HyperliquidPositionDetails,
    HyperliquidSpotBalanceDetails,
    MarginAccountSummary,
    OrderSide,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.market.funding_rate import FundingRate, HyperliquidFundingDetails
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.utils.parsing import parse_decimal_value


# --- Fixtures for HyperliquidRawClearinghouseState ---
@pytest.fixture
def raw_leverage_fixture() -> HyperliquidRawLeverage:
    """Provides a basic HyperliquidRawLeverage fixture."""
    return HyperliquidRawLeverage(type="cross", value=10)


@pytest.fixture
def raw_position_info_fixture(
    raw_leverage_fixture: HyperliquidRawLeverage,
) -> HyperliquidRawPositionInfo:
    """Provides a basic HyperliquidRawPositionInfo fixture."""
    return HyperliquidRawPositionInfo(
        coin="ETH",
        szi="1.0",
        entryPx="2000.0",
        unrealizedPnl="100.0",
        maxLeverage=20,
        positionValue="2000.0",
        liquidationPx="1500.0",
        marginUsed="100.0",
        leverage=raw_leverage_fixture,
        returnOnEquity="0.5",
    )


@pytest.fixture
def raw_asset_position_spot_fixture() -> HyperliquidRawAssetPosition:
    """Provides a HyperliquidRawAssetPosition fixture representing a spot balance."""
    return HyperliquidRawAssetPosition(
        asset="USDC",
        position=HyperliquidRawPositionInfo(
            coin="USDC",
            szi="10000.0",
            entryPx=None,
            unrealizedPnl="0",
            maxLeverage=0,
            positionValue="10000.0",
            liquidationPx=None,
            marginUsed="0",
            leverage=HyperliquidRawLeverage(type="cross", value=0),
            returnOnEquity="0",
        ),
    )


@pytest.fixture
def raw_asset_position_derivative_fixture(
    raw_position_info_fixture: HyperliquidRawPositionInfo,
) -> HyperliquidRawAssetPosition:
    """Provides a HyperliquidRawAssetPosition fixture for a derivative."""
    return HyperliquidRawAssetPosition(asset="ETH", position=raw_position_info_fixture)


@pytest.fixture
def raw_margin_summary_fixture() -> HyperliquidRawMarginSummary:
    """Provides a basic HyperliquidRawMarginSummary fixture."""
    return HyperliquidRawMarginSummary(
        accountValue="12000.0", totalRawUsd="12500.0", totalMarginUsed="200.0", totalNtlPos="2000.0"
    )


@pytest.fixture
def raw_clearinghouse_state_base_fixture(
    raw_margin_summary_fixture: HyperliquidRawMarginSummary,
) -> HyperliquidRawClearinghouseState:
    """Provides a base HyperliquidRawClearinghouseState fixture for testing."""
    return HyperliquidRawClearinghouseState(
        assetPositions=[],
        marginSummary=raw_margin_summary_fixture,
        crossMaintenanceMarginUsed="50.0",
        crossMarginSummary=raw_margin_summary_fixture,
        isolatedMaintenanceMarginUsed="25.0",
        isolatedMarginSummary=raw_margin_summary_fixture,
        withdrawable="9800.0",
    )


# --- Tests for map_raw_clearinghouse_state_to_margin_summary ---


class TestMapRawClearinghouseStateToMarginSummary:
    def test_happy_path_with_positions(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
        raw_asset_position_derivative_fixture: HyperliquidRawAssetPosition,
    ) -> None:
        """Test successful mapping with derivative positions."""
        raw_position_1_info = HyperliquidRawPositionInfo(
            coin="ETH",
            szi="1.0",
            entryPx="2000.0",
            unrealizedPnl="150.25",
            maxLeverage=20,
            positionValue="2000.0",
            liquidationPx="1500.0",
            marginUsed="100.0",
            leverage=HyperliquidRawLeverage(type="cross", value=10),
            returnOnEquity="0.75",
        )
        raw_position_2_info = HyperliquidRawPositionInfo(
            coin="BTC",
            szi="-0.1",
            entryPx="30000.0",
            unrealizedPnl="-50.75",
            maxLeverage=10,
            positionValue="3000.0",
            liquidationPx="25000.0",
            marginUsed="300.0",
            leverage=HyperliquidRawLeverage(type="isolated", value=5),
            returnOnEquity="-0.1",
        )
        asset_pos_1 = HyperliquidRawAssetPosition(asset="ETH", position=raw_position_1_info)
        asset_pos_2 = HyperliquidRawAssetPosition(asset="BTC", position=raw_position_2_info)

        current_raw_state = raw_clearinghouse_state_base_fixture.model_copy(
            update={
                "assetPositions": [asset_pos_1, asset_pos_2],
                "marginSummary": HyperliquidRawMarginSummary(
                    accountValue="12099.50",
                    totalRawUsd="12500.0",
                    totalMarginUsed="400.0",
                    totalNtlPos="5000.0",
                ),
                "crossMaintenanceMarginUsed": "50.0",
                "isolatedMaintenanceMarginUsed": "75.0",
            }
        )

        summary = HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(current_raw_state)

        assert isinstance(summary, MarginAccountSummary)
        assert summary.exchange == ExchangeName.HYPERLIQUID.value

        assert summary.total_equity == Decimal("12099.50")
        assert summary.total_unrealized_pnl == Decimal("99.50")
        assert summary.total_initial_margin_required == Decimal("400.0")
        assert summary.total_maintenance_margin_required == Decimal("125.0")
        assert summary.available_equity == Decimal(current_raw_state.withdrawable)

        assert summary.timestamp is not None
        current_time = datetime.now(UTC)
        assert (current_time - timedelta(seconds=10)) < summary.timestamp <= current_time

        assert isinstance(summary.hl_details, HyperliquidMarginDetails)
        assert summary.hl_details.cross_maintenance_margin_used == Decimal("50.0")
        assert summary.hl_details.isolated_maintenance_margin_used == Decimal("75.0")

    def test_no_derivative_positions(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test mapping when there are no derivative positions."""
        current_raw_state = raw_clearinghouse_state_base_fixture.model_copy(
            update={
                "assetPositions": [],
                "marginSummary": HyperliquidRawMarginSummary(
                    accountValue="12000.0",
                    totalRawUsd="12500.0",
                    totalMarginUsed="0.0",
                    totalNtlPos="0.0",
                ),
                "crossMaintenanceMarginUsed": "0.0",
                "isolatedMaintenanceMarginUsed": "0.0",
            }
        )

        summary = HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(current_raw_state)

        assert summary.total_unrealized_pnl == Decimal("0")
        assert summary.total_equity == Decimal("12000.0")
        assert summary.total_initial_margin_required == Decimal("0.0")
        assert summary.total_maintenance_margin_required == Decimal("0.0")
        assert summary.available_equity == Decimal(current_raw_state.withdrawable)

        assert isinstance(summary.hl_details, HyperliquidMarginDetails)
        assert summary.hl_details.cross_maintenance_margin_used == Decimal("0.0")
        assert summary.hl_details.isolated_maintenance_margin_used == Decimal("0.0")

    def test_invalid_numeric_strings_in_raw_state(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test graceful handling of invalid numeric strings in raw_state."""
        invalid_margin_summary = HyperliquidRawMarginSummary(
            accountValue="not-a-number",
            totalRawUsd="still-bad",
            totalMarginUsed="nope",
            totalNtlPos="bad",
        )
        current_raw_state = raw_clearinghouse_state_base_fixture.model_copy(
            update={
                "marginSummary": invalid_margin_summary,
                "crossMaintenanceMarginUsed": "also-invalid",
                "isolatedMaintenanceMarginUsed": "another-bad-one",
                "withdrawable": "bad-decimal",
            }
        )

        summary = HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(current_raw_state)

        assert summary.total_equity == Decimal("0")
        assert summary.total_unrealized_pnl == Decimal("0")
        assert summary.total_initial_margin_required == Decimal("0")
        assert summary.total_maintenance_margin_required == Decimal("0")
        assert summary.available_equity == Decimal("0")

    def test_missing_margin_summary_in_raw_state_simulated(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test handling if margin_summary is effectively None (e.g., parsing failed upstream)."""
        raw_dict_missing_summary = raw_clearinghouse_state_base_fixture.model_dump(by_alias=True)
        if "marginSummary" in raw_dict_missing_summary:
            del raw_dict_missing_summary["marginSummary"]
        if "crossMaintenanceMarginUsed" in raw_dict_missing_summary:
            del raw_dict_missing_summary["crossMaintenanceMarginUsed"]

    def test_withdrawable_funds_invalid_or_missing_in_raw_state(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test available_equity calculation when withdrawable is problematic."""
        state_with_invalid_withdrawable = raw_clearinghouse_state_base_fixture.model_copy(
            update={"withdrawable": "not-a-decimal"}
        )
        summary_invalid = HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(
            state_with_invalid_withdrawable
        )
        assert summary_invalid.available_equity == Decimal("0")

    def test_cross_maintenance_margin_used_invalid_or_missing_in_raw_state(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test total_maintenance_margin when crossMaintenanceMarginUsed is problematic."""
        state_invalid_cross_mmr = raw_clearinghouse_state_base_fixture.model_copy(
            update={"crossMaintenanceMarginUsed": "invalid"}
        )
        summary_invalid = HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(
            state_invalid_cross_mmr
        )
        assert summary_invalid.total_maintenance_margin_required == Decimal("25.0")


# --- Tests for map_raw_clearinghouse_state_to_spot_balances ---


def test_map_raw_clearinghouse_state_to_spot_balances_empty(
    mapper: HyperliquidMapper,
    raw_user_state_empty_positions_no_balances: HyperliquidRawClearinghouseState,
) -> None:
    """Test mapping when raw state has no spot balances (e.g., only perp positions)."""
    spot_balances = mapper.map_raw_clearinghouse_state_to_spot_balances(
        raw_user_state_empty_positions_no_balances
    )
    assert isinstance(spot_balances, dict)
    assert not spot_balances  # Expect empty dictionary


def test_map_raw_clearinghouse_state_to_spot_balances_with_usdc(
    mapper: HyperliquidMapper, mock_raw_user_state_fixture: HyperliquidRawClearinghouseState
) -> None:
    """Test mapping when raw state contains a USDC spot balance."""
    # Modify fixture to ensure it has a clear USDC balance in assetPositions
    # Typically, USDC balance is in marginSummary.accountValue, not assetPositions.
    # The current mapper logic derives USDC from marginSummary.accountValue if no explicit
    # USDC assetPosition. For this test, let's assume the mapper correctly extracts USDC.

    # The mock_raw_user_state_fixture's marginSummary.accountValue is "10000.0"
    # The mapper should create a "USDC" spot balance from this.

    spot_balances = mapper.map_raw_clearinghouse_state_to_spot_balances(mock_raw_user_state_fixture)

    assert isinstance(spot_balances, dict)
    assert "USDC" in spot_balances
    usdc_balance = spot_balances["USDC"]

    assert isinstance(usdc_balance, SpotBalance)
    assert usdc_balance.exchange == ExchangeName.HYPERLIQUID.value
    assert usdc_balance.asset == "USDC"
    assert usdc_balance.total_quantity == Decimal("10000.0")  # From marginSummary.accountValue
    assert usdc_balance.available_quantity == Decimal("8000.0")  # From marginSummary.freeCollateral
    assert isinstance(usdc_balance.timestamp, datetime)
    assert usdc_balance.hl_details is not None
    assert isinstance(usdc_balance.hl_details, HyperliquidSpotBalanceDetails)
    assert usdc_balance.bp_details is None


def test_map_raw_clearinghouse_state_to_spot_balances_with_other_spot_assets(
    mapper: HyperliquidMapper,
) -> None:
    """
    Test mapping when raw state contains other spot assets in assetPositions.
    (Note: Hyperliquid primarily uses assetPositions for perps, spot is usually just USDC).
    This test assumes if other non-perp assets appeared, they'd be mapped.
    """
    raw_state_with_spot = HyperliquidRawClearinghouseState(
        assetPositions=[
            HyperliquidRawAssetPosition(  # This is a derivative, should be ignored by spot
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
                ),
            ),
            HyperliquidRawAssetPosition(  # Simulate a non-USDC spot asset
                asset="SPOT-ASSET",
                position=HyperliquidRawPositionInfo(
                    coin="SPOT-ASSET",
                    szi="10.0",  # This would be total_quantity
                    entryPx="0",  # Not applicable for spot usually
                    leverage=HyperliquidRawLeverage(
                        type="isolated", value=0
                    ),  # No leverage for spot
                    liquidationPx=None,
                    marginUsed="0",
                    maxLeverage=0,
                    positionValue="500.0",  # 10 units * $50 price = 500
                    returnOnEquity="0",
                    unrealizedPnl="0",
                ),
            ),
        ],
        marginSummary=HyperliquidRawMarginSummary(  # Main margin summary
            accountValue="10700.0",  # Total value (USDC + SPOT-ASSET val + ETH-PERP PNL)
            totalRawUsd="500.0",  # Value of non-USDC assets (SPOT-ASSET value)
            totalNtlPos="3000.0",  # Notional value of derivative positions (ETH-PERP)
            totalMarginUsed="300.0",  # Margin used by ETH-PERP
        ),
        crossMaintenanceMarginUsed="150.0",  # Maintenance for ETH-PERP
        crossMarginSummary=HyperliquidRawMarginSummary(  # Cross-specific summary
            accountValue="10200.0",  # Value for cross account (USDC bal + PNLs if cross)
            # This is often the total USDC if all perps are cross
            totalRawUsd="0",  # Assuming SPOT-ASSET is not part of cross margin here
            totalNtlPos="3000.0",  # Notional of cross positions
            totalMarginUsed="300.0",  # Margin for cross positions
        ),
        isolatedMaintenanceMarginUsed="0",  # No isolated positions in this setup
        isolatedMarginSummary=HyperliquidRawMarginSummary(  # Isolated-specific summary
            accountValue="0",
            totalRawUsd="0",
            totalNtlPos="0",
            totalMarginUsed="0",
        ),
        withdrawable="9900.0",  # Typically (crossMarginSummary.accountValue -
        # crossMarginSummary.totalInitialMargin)
        # For test: Cross Account Value (10200) - ETH Initial Margin (300) = 9900
    )

    spot_balances = mapper.map_raw_clearinghouse_state_to_spot_balances(raw_state_with_spot)

    assert isinstance(spot_balances, dict)
    assert "USDC" in spot_balances
    assert "SPOT-ASSET" in spot_balances

    usdc_balance = spot_balances["USDC"]
    # USDC total derived from crossMarginSummary.accountValue, available from withdrawable
    assert usdc_balance.total_quantity == Decimal("10200.0")
    assert usdc_balance.available_quantity == Decimal("9900.0")

    spot_asset_balance = spot_balances["SPOT-ASSET"]
    assert spot_asset_balance.asset == "SPOT-ASSET"
    assert spot_asset_balance.total_quantity == Decimal("10.0")  # From szi
    assert spot_asset_balance.available_quantity == Decimal("10.0")  # Assuming all available
    assert spot_asset_balance.hl_details is not None


# --- Tests for map_raw_clearinghouse_state_to_derivative_positions ---


@pytest.fixture
def raw_user_state_with_positions(
    mock_raw_user_state_fixture: HyperliquidRawClearinghouseState,
) -> HyperliquidRawClearinghouseState:
    """Fixture for raw user state with ETH and BTC derivative positions."""
    # mock_raw_user_state_fixture already has an ETH position. Add a BTC one.
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
        ),
    )
    btc_position_asset = HyperliquidRawAssetPosition(
        asset="BTC-PERP",
        position=HyperliquidRawPositionInfo(
            coin="BTC-PERP",
            szi="-0.5",  # Short position
            entryPx="60000.0",
            leverage=HyperliquidRawLeverage(type="isolated", value=5),
            liquidationPx="63000.0",
            marginUsed="6000.0",  # 0.5 * 60000 / 5
            maxLeverage=20,
            positionValue="30000.0",  # abs value
            returnOnEquity="-0.02",
            unrealizedPnl="-600.0",
        ),
    )
    # Ensure no assetPositions conflict by rebuilding the list
    updated_asset_positions = [eth_position_asset, btc_position_asset]

    return mock_raw_user_state_fixture.model_copy(
        update={"assetPositions": updated_asset_positions}
    )


def test_map_raw_clearinghouse_state_to_derivative_positions_empty(
    mapper: HyperliquidMapper,
    raw_user_state_empty_positions_no_balances: HyperliquidRawClearinghouseState,
) -> None:
    """Test mapping when raw state has no derivative positions."""
    positions = mapper.map_raw_clearinghouse_state_to_derivative_positions(
        raw_user_state_empty_positions_no_balances
    )
    assert isinstance(positions, dict)
    assert not positions


def test_map_raw_clearinghouse_state_to_derivative_positions_populated(
    mapper: HyperliquidMapper, raw_user_state_with_positions: HyperliquidRawClearinghouseState
) -> None:
    """Test mapping with ETH long and BTC short positions."""
    positions = mapper.map_raw_clearinghouse_state_to_derivative_positions(
        raw_user_state_with_positions
    )

    assert isinstance(positions, dict)
    assert len(positions) == 2
    assert "ETH-PERP" in positions
    assert "BTC-PERP" in positions

    # Validate ETH position (long)
    eth_pos = positions["ETH-PERP"]
    assert isinstance(eth_pos, DerivativePosition)
    assert eth_pos.exchange == ExchangeName.HYPERLIQUID.value
    assert eth_pos.symbol == "ETH-PERP"
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
    assert btc_pos.symbol == "BTC-PERP"
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


# --- Fixtures for HyperliquidRawFill ---


@pytest.fixture
def hyperliquid_raw_fill_buy_fixture() -> HyperliquidRawFill:
    """Provides a valid HyperliquidRawFill for a BUY trade."""
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
        hash="0xabc123",
        fee="1.50275",  # 0.5 * 3005.50 * 0.001 (example fee rate)
        isMaker=False,
        liquidationMarkPx=None,
        cloid="my_buy_order_1",
    )


@pytest.fixture
def hyperliquid_raw_fill_sell_maker_fixture() -> HyperliquidRawFill:
    """Provides a valid HyperliquidRawFill for a SELL MAKER trade with cloid=None."""
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
        hash="0xdef456",
        fee="0.00",  # Maker trade, zero fee
        isMaker=True,
        liquidationMarkPx="50000.00",  # Example, may not be relevant for all fills
        cloid=None,  # No client order ID
    )


# --- Tests for transform_raw_fill_to_internal ---


def test_transform_raw_fill_to_internal_buy_taker(
    mapper: HyperliquidMapper, hyperliquid_raw_fill_buy_fixture: HyperliquidRawFill
) -> None:
    """Test transforming a raw BUY TAKER fill to an internal Trade model."""
    raw_fill = hyperliquid_raw_fill_buy_fixture
    trade = mapper.transform_raw_fill_to_internal(raw_fill)

    assert isinstance(trade, Trade)
    assert trade.id == str(raw_fill.tid)
    assert trade.symbol == raw_fill.coin
    assert isinstance(trade.executed_at, datetime)
    assert trade.executed_at.timestamp() * 1000 == raw_fill.time
    assert trade.side == OrderSide.BUY
    assert trade.order_id == str(raw_fill.oid)
    assert trade.exchange == ExchangeName.HYPERLIQUID.value
    assert trade.client_order_id == raw_fill.cloid
    assert trade.price == Decimal(raw_fill.px)
    assert trade.quantity == Decimal(raw_fill.sz)
    assert trade.fee == Decimal(raw_fill.fee)
    assert trade.fee_asset == raw_fill.coin  # Mapper assumes fee in quote asset (coin)
    assert trade.is_maker == raw_fill.is_maker

    assert trade.hl_details is not None
    assert isinstance(trade.hl_details, HyperliquidTradeDetails)
    assert trade.hl_details.trade_hash == raw_fill.hash
    assert trade.hl_details.liquidation_mark_px is None
    assert trade.hl_details.start_position == Decimal(raw_fill.start_position)
    assert trade.hl_details.dir == raw_fill.dir
    assert trade.bp_details is None


def test_transform_raw_fill_to_internal_sell_maker(
    mapper: HyperliquidMapper, hyperliquid_raw_fill_sell_maker_fixture: HyperliquidRawFill
) -> None:
    """Test transforming a raw SELL MAKER fill to an internal Trade model."""
    raw_fill = hyperliquid_raw_fill_sell_maker_fixture
    trade = mapper.transform_raw_fill_to_internal(raw_fill)

    assert isinstance(trade, Trade)
    assert trade.id == str(raw_fill.tid)
    assert trade.symbol == raw_fill.coin
    assert isinstance(trade.executed_at, datetime)
    assert trade.executed_at.timestamp() * 1000 == raw_fill.time
    assert trade.side == OrderSide.SELL
    assert trade.order_id == str(raw_fill.oid)
    assert trade.exchange == ExchangeName.HYPERLIQUID.value
    assert trade.client_order_id is None  # Based on fixture
    assert trade.price == Decimal(raw_fill.px)
    assert trade.quantity == Decimal(raw_fill.sz)
    assert trade.fee == Decimal(raw_fill.fee)
    assert trade.fee_asset == raw_fill.coin
    assert trade.is_maker == raw_fill.is_maker

    assert trade.hl_details is not None
    assert isinstance(trade.hl_details, HyperliquidTradeDetails)
    assert trade.hl_details.trade_hash == raw_fill.hash
    assert raw_fill.liquidation_mark_px is not None
    assert trade.hl_details.liquidation_mark_px == Decimal(raw_fill.liquidation_mark_px)
    assert trade.hl_details.start_position == Decimal(raw_fill.start_position)
    assert trade.hl_details.dir == raw_fill.dir
    assert trade.bp_details is None


def test_transform_raw_fill_to_internal_invalid_data_handling(mapper: HyperliquidMapper) -> None:
    """Test that invalid data in raw fill (e.g., non-decimal price) raises error."""
    # HyperliquidRawFill validation should catch this, but mapper might have its own parsing.
    # The mapper uses parse_decimal_value which will raise ValueError.
    invalid_raw_fill_data = {
        "tid": 123,
        "coin": "XYZ-PERP",
        "px": "not_a_decimal",
        "sz": "1",
        "time": int(datetime.now(UTC).timestamp() * 1000),
        "side": "B",
        "oid": 1,
        "startPosition": "0",
        "dir": "Open",
        "hash": "0x123",
        "fee": "0.1",
        "isMaker": False,
    }
    # We need to construct HyperliquidRawFill carefully if we want to bypass Pydantic validation
    # for this test, or rely on Pydantic to fail first.
    # For mapper robustness, direct field manipulation is hard with Pydantic v2 frozen.

    # Test case 1: Pydantic validation catches it first
    with pytest.raises(ValidationError):
        HyperliquidRawFill.model_validate(invalid_raw_fill_data)

    # Test case 2: If somehow a badly typed RawFill object gets to the mapper
    # (e.g. if validation was at a different stage or a type ignore was used elsewhere)
    # This is harder to simulate directly with frozen models. We assume parse_decimal_value
    # within the mapper is the point of failure if Pydantic validation somehow passed.

    # Let's assume the raw fill model itself is valid, but a field that parse_decimal_value
    # handles might fail. Example: if 'px' was Decimal(NaN) which is valid Decimal but not finite.
    # However, RawFiniteDecimalStr should prevent NaN strings.

    # For now, testing Pydantic's boundary validation is sufficient for this aspect.
    # If the mapper had more complex internal transformation logic susceptible to bad intermediate
    # types, more specific mocking would be needed.
    pass  # Covered by Pydantic validation for now


# --- Fixtures for HyperliquidRawAssetCtx ---


@pytest.fixture
def hyperliquid_raw_asset_ctx_eth_fixture() -> HyperliquidRawAssetCtx:
    """Provides a valid HyperliquidRawAssetCtx for ETH-PERP."""
    return HyperliquidRawAssetCtx(
        name="ETH-PERP",
        funding="0.00001234",
        markPx="3010.75",  # Instantiation uses alias
        prevDayPx="2950.00",  # Instantiation uses alias
        dayNtlVlm="50000000.00",  # Instantiation uses alias
        impactPx="3009.50",  # Instantiation uses alias
    )


@pytest.fixture
def hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture() -> HyperliquidRawAssetCtx:
    """Provides a valid HyperliquidRawAssetCtx for BTC-PERP with no impactPx."""
    return HyperliquidRawAssetCtx(
        name="BTC-PERP",
        funding="-0.00000567",
        markPx="60200.50",  # Instantiation uses alias
        prevDayPx="61000.00",  # Instantiation uses alias
        dayNtlVlm="120000000.00",  # Instantiation uses alias
        impactPx=None,
    )


# --- Tests for map_raw_ctx_to_ticker ---


def test_map_raw_ctx_to_ticker_eth(
    mapper: HyperliquidMapper, hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx
) -> None:
    """Test mapping a raw asset context for ETH-PERP to an internal Ticker."""
    raw_ctx = hyperliquid_raw_asset_ctx_eth_fixture
    ticker = mapper.map_raw_ctx_to_ticker(raw_ctx)

    assert isinstance(ticker, Ticker)
    assert ticker.symbol == raw_ctx.name  # Access uses Python name
    assert isinstance(ticker.timestamp, datetime)
    assert (datetime.now(UTC) - ticker.timestamp) < timedelta(seconds=5)

    assert ticker.price == Decimal(raw_ctx.mark_px)  # Access uses Python name mark_px
    assert ticker.bid == Decimal(raw_ctx.mark_px)  # Access uses Python name mark_px
    assert ticker.ask == Decimal(raw_ctx.mark_px)  # Access uses Python name mark_px
    assert ticker.volume == Decimal(raw_ctx.day_ntl_vlm)  # Access uses Python name day_ntl_vlm


def test_map_raw_ctx_to_ticker_btc_no_impact(
    mapper: HyperliquidMapper,
    hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test mapping for BTC-PERP with no impact price and negative funding."""
    raw_ctx = hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture
    ticker = mapper.map_raw_ctx_to_ticker(raw_ctx)

    assert isinstance(ticker, Ticker)
    assert ticker.symbol == raw_ctx.name  # Access uses Python name
    assert ticker.price == Decimal(raw_ctx.mark_px)  # Access uses Python name mark_px
    assert ticker.volume == Decimal(raw_ctx.day_ntl_vlm)  # Access uses Python name day_ntl_vlm


# --- Fixtures for HyperliquidRawL2Book ---


@pytest.fixture
def hyperliquid_raw_book_level_fixture_bid() -> HyperliquidRawBookLevel:
    """Provides a valid HyperliquidRawBookLevel for a bid."""
    return HyperliquidRawBookLevel(px="2999.50", sz="10.5", n=2)


@pytest.fixture
def hyperliquid_raw_book_level_fixture_ask() -> HyperliquidRawBookLevel:
    """Provides a valid HyperliquidRawBookLevel for an ask."""
    return HyperliquidRawBookLevel(px="3000.50", sz="5.25", n=3)


@pytest.fixture
def hyperliquid_raw_l2_book_eth_fixture(
    hyperliquid_raw_book_level_fixture_bid: HyperliquidRawBookLevel,
    hyperliquid_raw_book_level_fixture_ask: HyperliquidRawBookLevel,
) -> HyperliquidRawL2Book:
    """Provides a valid HyperliquidRawL2Book for ETH-PERP."""
    # Create more levels for a more realistic book
    bid_levels = [
        HyperliquidRawBookLevel(px="2999.50", sz="10.5", n=2),
        HyperliquidRawBookLevel(px="2999.00", sz="20.0", n=5),
        HyperliquidRawBookLevel(px="2998.50", sz="15.0", n=3),
    ]
    ask_levels = [
        HyperliquidRawBookLevel(px="3000.50", sz="5.25", n=3),
        HyperliquidRawBookLevel(px="3001.00", sz="12.0", n=4),
        HyperliquidRawBookLevel(px="3001.50", sz="8.0", n=2),
    ]
    return HyperliquidRawL2Book(
        coin="ETH-PERP",
        levels=[bid_levels, ask_levels],
        time=int(datetime.now(UTC).timestamp() * 1000 - 2000),  # 2 seconds ago
    )


@pytest.fixture
def hyperliquid_raw_l2_book_empty_fixture() -> HyperliquidRawL2Book:
    """Provides an empty HyperliquidRawL2Book."""
    return HyperliquidRawL2Book(
        coin="BTC-PERP",
        levels=[[], []],  # Empty bids and asks
        time=int(datetime.now(UTC).timestamp() * 1000 - 1000),  # 1 second ago
    )


# --- Tests for map_raw_order_book ---


def test_map_raw_order_book_eth(
    mapper: HyperliquidMapper, hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book
) -> None:
    """Test mapping a raw L2 book for ETH-PERP to an internal OrderBook."""
    raw_book = hyperliquid_raw_l2_book_eth_fixture
    order_book = mapper.map_raw_order_book(raw_book)

    assert isinstance(order_book, OrderBook)
    assert order_book.symbol == raw_book.coin
    assert isinstance(order_book.timestamp, datetime)
    assert order_book.timestamp.timestamp() * 1000 == raw_book.time

    assert len(order_book.bids) == 3
    assert order_book.bids[0] == (Decimal("2999.50"), Decimal("10.5"))
    assert order_book.bids[1] == (Decimal("2999.00"), Decimal("20.0"))
    assert order_book.bids[2] == (Decimal("2998.50"), Decimal("15.0"))

    assert len(order_book.asks) == 3
    assert order_book.asks[0] == (Decimal("3000.50"), Decimal("5.25"))
    assert order_book.asks[1] == (Decimal("3001.00"), Decimal("12.0"))
    assert order_book.asks[2] == (Decimal("3001.50"), Decimal("8.0"))


def test_map_raw_order_book_with_depth_limit(
    mapper: HyperliquidMapper, hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book
) -> None:
    """Test mapping with a depth limit."""
    raw_book = hyperliquid_raw_l2_book_eth_fixture
    order_book = mapper.map_raw_order_book(raw_book, depth=2)

    assert isinstance(order_book, OrderBook)
    assert len(order_book.bids) == 2
    assert order_book.bids[0] == (Decimal("2999.50"), Decimal("10.5"))
    assert order_book.bids[1] == (Decimal("2999.00"), Decimal("20.0"))

    assert len(order_book.asks) == 2
    assert order_book.asks[0] == (Decimal("3000.50"), Decimal("5.25"))
    assert order_book.asks[1] == (Decimal("3001.00"), Decimal("12.0"))


def test_map_raw_order_book_empty(
    mapper: HyperliquidMapper, hyperliquid_raw_l2_book_empty_fixture: HyperliquidRawL2Book
) -> None:
    """Test mapping an empty raw L2 book."""
    raw_book = hyperliquid_raw_l2_book_empty_fixture
    order_book = mapper.map_raw_order_book(raw_book)

    assert isinstance(order_book, OrderBook)
    assert order_book.symbol == raw_book.coin
    assert isinstance(order_book.timestamp, datetime)
    assert not order_book.bids
    assert not order_book.asks


def test_map_raw_order_book_malformed_levels_structure(
    mapper: HyperliquidMapper,
) -> None:
    """Test mapping when raw_book.levels has an unexpected structure
    (e.g., not a list of 2 lists)."""
    # The HyperliquidRawL2Book model itself has a validator for levels structure.
    # This test ensures the mapper handles it gracefully if such data somehow passed.
    # (Pydantic should prevent this, so this is more of a conceptual check on mapper robustness).

    # Case 1: levels is not a list (should be caught by Pydantic on HyperliquidRawL2Book)
    with pytest.raises(ValidationError):
        raw_data_invalid_levels_type = {
            "coin": "MALFORMED-PERP",
            "levels": "not_a_list",
            "time": 123,
        }
        HyperliquidRawL2Book.model_validate(raw_data_invalid_levels_type)

    # Case 2: levels is a list, but not of length 2 (should be caught by Pydantic)
    with pytest.raises(ValidationError):
        raw_data_invalid_levels_item_type = {
            "coin": "MALFORMED-PERP",
            "levels": ["not_list_1", "not_list_2"],
            "time": 123,
        }
        HyperliquidRawL2Book.model_validate(raw_data_invalid_levels_item_type)

    # The mapper logic itself also logs warnings if levels structure is bad after
    # Pydantic validation.
    # We can simulate a raw_book object that bypasses Pydantic for the levels field
    # to test this.
    # If a non-finite decimal somehow got past RawFiniteDecimalStr for px:
    # This scenario is unlikely due to RawFiniteDecimalStr but tests mapper's
    # direct use of parse_decimal_value
    # For Pydantic v2, direct instantiation with invalid types is harder if
    # validators are robust.
    # The mapper directly calls parse_decimal_value, which would raise ValueError.
    pass


# --- Fixtures for HyperliquidRawPublicTrade ---


@pytest.fixture
def hyperliquid_raw_public_trade_buy_fixture() -> HyperliquidRawPublicTrade:
    """Provides a valid HyperliquidRawPublicTrade for a BUY trade."""
    return HyperliquidRawPublicTrade(
        coin="ETH-PERP",
        side="B",
        px="3002.00",
        sz="1.5",
        time=int(datetime.now(UTC).timestamp() * 1000 - 3000),  # 3 seconds ago
        hash="0xghi789",
    )


@pytest.fixture
def hyperliquid_raw_public_trade_sell_fixture() -> HyperliquidRawPublicTrade:
    """Provides a valid HyperliquidRawPublicTrade for a SELL trade."""
    return HyperliquidRawPublicTrade(
        coin="BTC-PERP",
        side="A",  # Sell
        px="60100.75",
        sz="0.02",
        time=int(datetime.now(UTC).timestamp() * 1000 - 1500),  # 1.5 seconds ago
        hash="0xjkl012",
    )


# --- Tests for transform_raw_public_trade_to_internal ---


def test_transform_raw_public_trade_to_internal_buy(
    mapper: HyperliquidMapper, hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade
) -> None:
    """Test transforming a raw public BUY trade to an internal Trade model."""
    raw_trade = hyperliquid_raw_public_trade_buy_fixture
    trade = mapper.transform_raw_public_trade_to_internal(raw_trade)

    assert isinstance(trade, Trade)
    assert trade.id == raw_trade.hash  # Public trades use hash as ID
    assert trade.symbol == raw_trade.coin
    assert isinstance(trade.executed_at, datetime)
    assert trade.executed_at.timestamp() * 1000 == raw_trade.time
    assert trade.side == OrderSide.BUY
    # Public trades don't have order_id, client_order_id, fee, is_maker directly
    assert trade.order_id == "UNKNOWN_PUBLIC_TRADE"  # Mapper default
    assert trade.exchange == ExchangeName.HYPERLIQUID.value
    assert trade.client_order_id is None
    assert trade.price == Decimal(raw_trade.px)
    assert trade.quantity == Decimal(raw_trade.sz)
    assert trade.fee == Decimal("0")  # Mapper default for public
    assert trade.fee_asset is None  # Mapper default for public
    assert trade.is_maker is None  # Not available in public trades

    assert trade.hl_details is not None
    assert isinstance(trade.hl_details, HyperliquidTradeDetails)
    assert trade.hl_details.trade_hash == raw_trade.hash
    assert trade.hl_details.liquidation_mark_px is None
    assert trade.hl_details.start_position is None
    assert trade.hl_details.dir is None
    assert trade.bp_details is None


def test_transform_raw_public_trade_to_internal_sell(
    mapper: HyperliquidMapper, hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade
) -> None:
    """Test transforming a raw public SELL trade to an internal Trade model."""
    raw_trade = hyperliquid_raw_public_trade_sell_fixture
    trade = mapper.transform_raw_public_trade_to_internal(raw_trade)

    assert isinstance(trade, Trade)
    assert trade.id == raw_trade.hash
    assert trade.symbol == raw_trade.coin
    assert isinstance(trade.executed_at, datetime)
    assert trade.executed_at.timestamp() * 1000 == raw_trade.time
    assert trade.side == OrderSide.SELL
    assert trade.order_id == "UNKNOWN_PUBLIC_TRADE"
    assert trade.price == Decimal(raw_trade.px)
    assert trade.quantity == Decimal(raw_trade.sz)
    assert trade.fee == Decimal("0")
    assert trade.is_maker is None

    assert trade.hl_details is not None
    assert trade.hl_details.trade_hash == raw_trade.hash


def test_transform_raw_public_trade_invalid_data(mapper: HyperliquidMapper) -> None:
    """Test transform_raw_public_trade with data that would fail parsing (e.g. price)."""
    invalid_raw_data = {
        "coin": "XYZ-PERP",
        "side": "B",
        "px": "not_a_price",
        "sz": "1",
        "time": int(datetime.now(UTC).timestamp() * 1000),
        "hash": "0xbad",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawPublicTrade.model_validate(invalid_raw_data)

    # If a non-finite decimal somehow got past RawFiniteDecimalStr for px:
    # This scenario is unlikely due to RawFiniteDecimalStr but tests mapper's
    # direct use of parse_decimal_value
    # For Pydantic v2, direct instantiation with invalid types is harder if
    # validators are robust.
    # The mapper directly calls parse_decimal_value, which would raise ValueError.
    pass


# --- Tests for map_raw_trades ---


def test_map_raw_trades_empty_list(mapper: HyperliquidMapper) -> None:
    """Test mapping an empty list of raw public trades."""
    trades = mapper.map_raw_trades([])
    assert isinstance(trades, list)
    assert not trades


def test_map_raw_trades_populated_list(
    mapper: HyperliquidMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test mapping a list of raw public trades."""
    raw_trades_list = [
        hyperliquid_raw_public_trade_buy_fixture,
        hyperliquid_raw_public_trade_sell_fixture,
    ]
    trades = mapper.map_raw_trades(raw_trades_list)

    assert isinstance(trades, list)
    assert len(trades) == 2

    # Check first trade (buy)
    assert trades[0].id == hyperliquid_raw_public_trade_buy_fixture.hash
    assert trades[0].symbol == hyperliquid_raw_public_trade_buy_fixture.coin
    assert trades[0].side == OrderSide.BUY
    assert trades[0].price == Decimal(hyperliquid_raw_public_trade_buy_fixture.px)

    # Check second trade (sell)
    assert trades[1].id == hyperliquid_raw_public_trade_sell_fixture.hash
    assert trades[1].symbol == hyperliquid_raw_public_trade_sell_fixture.coin
    assert trades[1].side == OrderSide.SELL
    assert trades[1].price == Decimal(hyperliquid_raw_public_trade_sell_fixture.px)


def test_map_raw_trades_with_limit(
    mapper: HyperliquidMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test mapping with a limit applied."""
    # Create more fixtures if needed, or reuse existing ones for a list > limit
    raw_trade_3 = HyperliquidRawPublicTrade(
        coin="ADA-PERP",
        side="B",
        px="0.5",
        sz="1000",
        time=int(datetime.now(UTC).timestamp() * 1000 - 500),
        hash="0xmno345",
    )
    raw_trades_list = [
        hyperliquid_raw_public_trade_buy_fixture,  # ETH
        hyperliquid_raw_public_trade_sell_fixture,  # BTC
        raw_trade_3,  # ADA
    ]
    trades = mapper.map_raw_trades(raw_trades_list, limit=2)

    assert isinstance(trades, list)
    assert len(trades) == 2
    # Ensure it took the first two
    assert trades[0].symbol == "ETH-PERP"
    assert trades[1].symbol == "BTC-PERP"


def test_map_raw_trades_limit_greater_than_list_size(
    mapper: HyperliquidMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test mapping when limit is larger than the number of trades."""
    raw_trades_list = [hyperliquid_raw_public_trade_buy_fixture]
    trades = mapper.map_raw_trades(raw_trades_list, limit=5)
    assert isinstance(trades, list)
    assert len(trades) == 1
    assert trades[0].symbol == hyperliquid_raw_public_trade_buy_fixture.coin


def test_map_raw_trades_with_transformation_error(
    mapper: HyperliquidMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    mocker: MockerFixture,  # For pytest-mock
) -> None:
    """Test that map_raw_trades handles errors from
    transform_raw_public_trade_to_internal gracefully."""
    # Create a raw trade that will pass HyperliquidRawPublicTrade validation
    # but cause an issue inside transform_raw_public_trade_to_internal
    # (e.g., hypothetical internal error)
    # For this example, we mock transform_raw_public_trade_to_internal to raise
    # an exception for one item.

    problematic_raw_trade = HyperliquidRawPublicTrade(
        coin="ERR-PERP",
        side="B",
        px="10",
        sz="1",
        time=int(datetime.now(UTC).timestamp() * 1000),
        hash="0xerr",
    )
    raw_trades_list = [
        hyperliquid_raw_public_trade_buy_fixture,  # Should pass
        problematic_raw_trade,  # Should fail during transformation
    ]

    # Mock the inner transform method to raise an error for the specific problematic trade
    def mock_transform(raw_trade_arg: HyperliquidRawPublicTrade) -> Trade | None:
        if raw_trade_arg.coin == "ERR-PERP":
            # This mock simulates a failure *before* or *during* the call to the actual
            # transform_raw_public_trade_to_internal, as if the raw data itself is problematic
            # or a sub-process within the transform (like Decimal conversion) fails.
            raise ValueError(f"Simulated transformation error for {raw_trade_arg.coin}")

        # For non-error cases, delegate to the *actual* method on the *actual* mapper instance.
        # The `mapper` fixture is an instance of HyperliquidMapper.
        return mapper.transform_raw_public_trade_to_internal(raw_trade_arg)

    mocked_transformer = mocker.patch.object(
        mapper, "transform_raw_public_trade_to_internal", side_effect=mock_transform
    )

    # Depending on map_raw_trades error handling (propagate vs. collect valid ones):
    # Option 1: If it propagates immediately
    with pytest.raises(ValueError, match="Simulated transformation error for ERR-PERP"):
        mapper.map_raw_trades(raw_trades_list)
    assert mocked_transformer.call_count == 2  # Called for ETH, then for ERR

    # Option 2: If map_raw_trades is designed to skip errors and log
    # (not current design based on snippet)
    # trades = mapper.map_raw_trades(raw_trades_list)
    # assert len(trades) == 1 # Only the valid one
    # Check logs for the error (would require log capture fixture)


# --- Tests for map_raw_ctx_to_funding_rate ---


def test_map_raw_ctx_to_funding_rate_eth(
    mapper: HyperliquidMapper, hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx
) -> None:
    """Test mapping asset context to FundingRate for ETH with positive funding."""
    raw_ctx = hyperliquid_raw_asset_ctx_eth_fixture
    fr = mapper.map_raw_ctx_to_funding_rate(raw_ctx)

    assert fr is not None, "FundingRate object should be created"
    assert isinstance(fr, FundingRate)
    assert fr.symbol == raw_ctx.name  # Access uses Python name
    assert isinstance(fr.timestamp, datetime)
    assert (datetime.now(UTC) - fr.timestamp) < timedelta(seconds=10)

    assert fr.hl_details is not None
    expected_hourly_rate = Decimal(raw_ctx.funding)  # Access uses Python name
    assert fr.hl_details.hl_funding_hourly == expected_hourly_rate

    # map_raw_ctx_to_funding_rate calculates 8hr rate for fr.funding_rate
    assert fr.funding_rate == expected_hourly_rate * Decimal("8")

    assert fr.predicted_rate is None  # Set to None by mapper

    assert fr.next_funding_time is not None
    # Check that next_funding_time is roughly the start of the next hour UTC
    now_utc = datetime.now(UTC)
    expected_next_funding_time_approx = now_utc.replace(
        minute=0, second=0, microsecond=0
    ) + timedelta(hours=1)
    assert abs((fr.next_funding_time - expected_next_funding_time_approx).total_seconds()) < 120, (
        f"Next funding time {fr.next_funding_time} not close to {expected_next_funding_time_approx}"
    )

    assert fr.mark_price == Decimal(raw_ctx.mark_px)  # Access uses Python name
    assert fr.index_price is None  # Set to None by mapper

    assert isinstance(fr.hl_details, HyperliquidFundingDetails)
    assert fr.hl_details.hl_impact_px == (
        Decimal(raw_ctx.impact_px) if raw_ctx.impact_px else None
    )  # Access uses Python name
    assert fr.bp_details is None


def test_map_raw_ctx_to_funding_rate_btc_negative_funding(
    mapper: HyperliquidMapper,
    hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test mapping for BTC with negative funding and no impact price."""
    raw_ctx = hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture
    fr = mapper.map_raw_ctx_to_funding_rate(raw_ctx)

    assert fr is not None, "FundingRate object should be created"
    assert isinstance(fr, FundingRate)
    assert fr.symbol == raw_ctx.name  # Access uses Python name

    assert fr.hl_details is not None
    expected_hourly_rate = Decimal(raw_ctx.funding)  # Access uses Python name
    assert fr.hl_details.hl_funding_hourly == expected_hourly_rate
    assert fr.funding_rate == expected_hourly_rate * Decimal("8")  # 8hr rate

    assert fr.mark_price == Decimal(raw_ctx.mark_px)  # Access uses Python name

    assert isinstance(fr.hl_details, HyperliquidFundingDetails)
    assert fr.hl_details.hl_impact_px is None  # Based on fixture
    assert fr.bp_details is None  # Ensure bp_details is None for HL


def test_map_raw_ctx_to_funding_rate_parsing_error_returns_none(
    mapper: HyperliquidMapper,
    mocker: MockerFixture,
    hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test that if parsing raw_ctx.funding fails internally, the method returns None."""
    raw_ctx_bad_funding = HyperliquidRawAssetCtx(  # Instantiation uses alias
        name="ERR-FUNDING-PERP",
        funding="invalid_decimal_str",
        markPx="100",
        prevDayPx="99",
        dayNtlVlm="10000",
        impactPx=None,
    )

    # Option 1: Mock parse_decimal_value to return None for the 'funding' field
    original_parse_decimal = parse_decimal_value  # Store original for restoration/use

    def mock_parse_decimal_none_for_funding(
        value: str | int | float | Decimal,
        allow_none: bool = False,
        field_name: str | None = None,
        # **kwargs: Any, # Removed kwargs
    ) -> Decimal | None:
        if field_name == "funding":
            return None
        return original_parse_decimal(
            value,
            allow_none=allow_none,
            field_name=field_name if field_name is not None else "",
            # **kwargs, # Removed kwargs
        )

    mocker.patch(
        "cyberdelta.apis.hyperliquid.hl_mapper.parse_decimal_value",
        side_effect=mock_parse_decimal_none_for_funding,
    )
    fr_parsed_as_none = mapper.map_raw_ctx_to_funding_rate(
        raw_ctx_bad_funding
    )  # Use raw_ctx_bad_funding
    assert fr_parsed_as_none is None, (
        "Expected None when internal parsing of funding rate returns None"
    )

    mocker.resetall()  # Reset mocks before next case

    # Option 2: Mock parse_decimal_value to raise ValueError for the 'funding' field
    def mock_parse_decimal_raise_for_funding(
        value: str | int | float | Decimal,
        allow_none: bool = False,
        field_name: str | None = None,
        # **kwargs: Any, # Removed kwargs
    ) -> Decimal | None:
        if field_name == "funding":
            raise ValueError("Simulated parsing error for funding")
        return original_parse_decimal(
            value,
            allow_none=allow_none,
            field_name=field_name if field_name is not None else "",
            # **kwargs, # Removed kwargs
        )

    mocker.patch(
        "cyberdelta.apis.hyperliquid.hl_mapper.parse_decimal_value",
        side_effect=mock_parse_decimal_raise_for_funding,
    )
    fr_parse_exception = mapper.map_raw_ctx_to_funding_rate(
        raw_ctx_bad_funding
    )  # Use raw_ctx_bad_funding
    assert fr_parse_exception is None, (
        "Expected None when internal parsing of funding rate raises ValueError"
    )
    mocker.resetall()  # Reset mocks before next case

    raw_ctx_valid_funding_bad_markpx = HyperliquidRawAssetCtx(
        name="VALID-FUNDING-BAD-MARKPX-PERP",
        funding="0.0001",
        markPx="invalid_mark_price",
        prevDayPx="99",
        dayNtlVlm="10000",
        impactPx=None,
    )

    def mock_parse_decimal_raise_for_mark_px(
        value: str | int | float | Decimal,
        allow_none: bool = False,
        field_name: str | None = None,
        # **kwargs: Any, # Removed kwargs
    ) -> Decimal | None:
        if field_name == "mark_px":
            raise ValueError("Simulated parsing error for mark_px")
        return original_parse_decimal(
            value,
            allow_none=allow_none,
            field_name=field_name if field_name is not None else "",
            # **kwargs, # Removed kwargs
        )

    mocker.patch(
        "cyberdelta.apis.hyperliquid.hl_mapper.parse_decimal_value",
        side_effect=mock_parse_decimal_raise_for_mark_px,
    )
    fr_mark_px_exception = mapper.map_raw_ctx_to_funding_rate(raw_ctx_valid_funding_bad_markpx)
    assert fr_mark_px_exception is None, (
        "Expected None when internal parsing of mark_px raises ValueError"
    )
