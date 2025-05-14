"""
Unit tests for the HyperliquidMapper.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.core.models import (
    HyperliquidMarginDetails,
    MarginAccountSummary,
)


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


# TODO: Add tests for other HyperliquidMapper methods:
# - map_raw_ctx_to_ticker
# - map_raw_order_book
# - transform_raw_public_trade_to_internal
# - map_raw_trades
# - map_raw_ctx_to_funding_rate
# - map_raw_clearinghouse_state_to_derivative_positions
# - map_raw_clearinghouse_state_to_spot_balances
# - transform_raw_fill_to_internal

# Remember to create fixtures for raw models like HyperliquidRawAssetCtx,
# HyperliquidRawL2Book, HyperliquidRawPublicTrade, HyperliquidRawFill.
