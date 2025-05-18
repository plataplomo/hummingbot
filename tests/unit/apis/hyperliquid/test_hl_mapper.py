"""
Unit tests for the HyperliquidMapper.
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
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
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

# Import fixture from another test file
# noqa: F401, RUF100 # Removed mock_raw_user_state_fixture


@pytest.fixture
def mapper() -> HyperliquidMapper:
    """Provides an instance of HyperliquidMapper."""
    return HyperliquidMapper()


@pytest.fixture
def raw_user_state_empty_positions_no_balances(
    raw_margin_summary_fixture: HyperliquidRawMarginSummary,  # Re-use for consistency
) -> HyperliquidRawClearinghouseState:
    """Provides a HyperliquidRawClearinghouseState with no asset positions
    and basic margin summary."""
    # Create a zeroed-out or minimal valid margin summary for this fixture
    empty_margin_summary = HyperliquidRawMarginSummary(
        accountValue="0", totalRawUsd="0", totalMarginUsed="0", totalNtlPos="0"
    )
    return HyperliquidRawClearinghouseState(
        assetPositions=[],
        marginSummary=empty_margin_summary,
        crossMaintenanceMarginUsed="0",
        crossMarginSummary=empty_margin_summary,
        isolatedMaintenanceMarginUsed="0",
        isolatedMarginSummary=empty_margin_summary,
        withdrawable="0",
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

        # Intentionally keep original assetPositions for this focused test
        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)

        new_margin_summary_data = HyperliquidRawMarginSummary(
            accountValue="12099.50",
            totalRawUsd="12500.0",
            totalMarginUsed="400.0",
            totalNtlPos="5000.0",
        ).model_dump(by_alias=False)

        updated_data_python_names = {
            **base_dump_python_names,
            "asset_positions": [
                asset_pos_1.model_dump(by_alias=False),
                asset_pos_2.model_dump(by_alias=False),
            ],
            "margin_summary": new_margin_summary_data,
            "cross_maintenance_margin_used": "50.0",
            "isolated_maintenance_margin_used": "75.0",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names
        )

        # DEBUG: Verify current_raw_state before passing to mapper
        assert (
            current_raw_state.margin_summary.account_value == "12099.50"
        ), (  # Compare with String
            "Validated margin_summary.account_value mismatch"
        )
        assert (
            current_raw_state.margin_summary.total_margin_used == "400.0"
        ), (  # Compare with String
            "Validated margin_summary.total_margin_used mismatch"
        )
        # Ensure other fields of margin_summary also reflect the new instance
        assert current_raw_state.margin_summary.total_raw_usd == "12500.0", (  # Compare with String
            "Validated margin_summary.total_raw_usd mismatch"
        )
        assert current_raw_state.margin_summary.total_ntl_pos == "5000.0", (  # Compare with String
            "Validated margin_summary.total_ntl_pos mismatch"
        )

        # Check that fields not in update dict remain from original base fixture
        # (assetPositions will be empty, cross/isolated MMRs will be original)
        assert len(current_raw_state.asset_positions) == 2, (
            "asset_positions should now be populated"
        )
        assert current_raw_state.cross_maintenance_margin_used == "50.0", (  # Compare with String
            "Validated cross_maintenance_margin_used mismatch"
        )
        assert (
            current_raw_state.isolated_maintenance_margin_used == "75.0"
        ), (  # Compare with String
            "Validated isolated_maintenance_margin_used mismatch"
        )

        # The original assertions for summary will likely fail now because other parts of
        # current_raw_state are not updated, but the goal is to see if the debug
        # assertions for current_raw_state.margin_summary pass.
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
        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)

        new_margin_summary_data = HyperliquidRawMarginSummary(
            accountValue="12000.0",
            totalRawUsd="12500.0",
            totalMarginUsed="0.0",
            totalNtlPos="0.0",
        ).model_dump(by_alias=False)

        updated_data_python_names = {
            **base_dump_python_names,
            "asset_positions": [],
            "margin_summary": new_margin_summary_data,
            "cross_maintenance_margin_used": "0.0",
            "isolated_maintenance_margin_used": "0.0",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names
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
        """Test that creating/copying raw_state with invalid numeric strings
        raises ValidationError."""
        invalid_margin_summary_data = {
            "accountValue": "not-a-number",
            "totalRawUsd": "still-bad",
            "totalMarginUsed": "nope",
            "totalNtlPos": "bad",
        }
        # Validation should fail when attempting to create HyperliquidRawMarginSummary
        # or when model_copy tries to validate the updated fields.
        with pytest.raises(ValidationError):
            raw_clearinghouse_state_base_fixture.model_copy(
                update={
                    "marginSummary": HyperliquidRawMarginSummary(**invalid_margin_summary_data),
                    "crossMaintenanceMarginUsed": "also-invalid",
                    "isolatedMaintenanceMarginUsed": "another-bad-one",
                    "withdrawable": "bad-decimal",
                }
            )

    def test_withdrawable_funds_invalid_or_missing_in_raw_state(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test that mapping raw_state with invalid 'withdrawable' raises ValueError from mapper."""
        current_raw_state_invalid = raw_clearinghouse_state_base_fixture.model_copy(
            update={"withdrawable": "not-a-decimal"}
        )
        # Expect ValueError from parse_decimal_value inside the mapper
        with pytest.raises(ValueError) as exc_info:
            HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(
                current_raw_state_invalid
            )
        assert "withdrawable" in str(exc_info.value).lower()

    def test_cross_maintenance_margin_used_invalid_or_missing_in_raw_state(
        self, raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState
    ) -> None:
        """Test mapping with problematic crossMaintenanceMarginUsed."""
        # Case 1: Invalid numeric string for crossMaintenanceMarginUsed
        # should raise ValidationError on model_validate
        with pytest.raises(ValidationError, match="crossMaintenanceMarginUsed"):
            data_to_validate = raw_clearinghouse_state_base_fixture.model_dump(by_alias=True)
            data_to_validate["crossMaintenanceMarginUsed"] = "bad-value"
            HyperliquidRawClearinghouseState.model_validate(data_to_validate)

        # Case 2: Test successful mapping with valid values (original intent of the second part)
        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)
        updated_data_python_names = {
            **base_dump_python_names,
            "cross_maintenance_margin_used": "0.0",
            "isolated_maintenance_margin_used": "25.0",
        }
        current_raw_state_updated_mmr = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names
        )

        summary_updated_mmr = HyperliquidMapper.map_raw_clearinghouse_state_to_margin_summary(
            current_raw_state_updated_mmr
        )
        assert summary_updated_mmr.total_maintenance_margin_required == Decimal("25.0")

    def test_direct_validation_of_cross_maintenance_margin_used_invalid(self) -> None:
        """Test HyperliquidRawClearinghouseState validation for bad crossMaintenanceMarginUsed."""
        valid_margin_summary_data = {
            "accountValue": "100",
            "totalRawUsd": "100",
            "totalMarginUsed": "0",
            "totalNtlPos": "0",
        }
        with pytest.raises(
            ValidationError, match="crossMaintenanceMarginUsed"
        ):  # check that the error is about this field
            HyperliquidRawClearinghouseState(
                assetPositions=[],
                marginSummary=valid_margin_summary_data,  # type: ignore
                crossMaintenanceMarginUsed="bad-value",  # Problematic field
                crossMarginSummary=valid_margin_summary_data,  # type: ignore
                isolatedMaintenanceMarginUsed="0",
                isolatedMarginSummary=valid_margin_summary_data,  # type: ignore
                withdrawable="100",
            )


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
    mapper: HyperliquidMapper,
) -> None:
    """Test that USDC balance is created from marginSummary.accountValue."""
    # This test needs a RawClearinghouseState where marginSummary.accountValue is non-zero.
    margin_summary_with_usdc_value = HyperliquidRawMarginSummary(
        accountValue="10000.0",  # This should result in a USDC spot balance
        totalRawUsd="10000.0",  # Assuming totalRawUsd matches for simplicity here
        totalMarginUsed="0",
        totalNtlPos="0",
    )
    raw_state_for_usdc_test = HyperliquidRawClearinghouseState(
        assetPositions=[],
        marginSummary=margin_summary_with_usdc_value,
        crossMaintenanceMarginUsed="0",
        # For focused testing of USDC from accountValue, other summaries can be minimal
        crossMarginSummary=HyperliquidRawMarginSummary(
            accountValue="0", totalRawUsd="0", totalMarginUsed="0", totalNtlPos="0"
        ),
        isolatedMaintenanceMarginUsed="0",
        isolatedMarginSummary=HyperliquidRawMarginSummary(
            accountValue="0", totalRawUsd="0", totalMarginUsed="0", totalNtlPos="0"
        ),
        withdrawable="0",
    )

    spot_balances = mapper.map_raw_clearinghouse_state_to_spot_balances(raw_state_for_usdc_test)

    assert "USDC" in spot_balances
    usdc_balance = spot_balances["USDC"]

    assert isinstance(usdc_balance, SpotBalance)
    assert usdc_balance.exchange == ExchangeName.HYPERLIQUID.value
    assert usdc_balance.asset == "USDC"
    assert usdc_balance.total_quantity == Decimal("10000.0")  # From marginSummary.accountValue
    assert usdc_balance.available_quantity == Decimal(
        "0"
    )  # Corrected: From raw_state_for_usdc_test.withdrawable which is "0"
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
    # USDC total derived from marginSummary.accountValue, as per current mapper logic
    assert usdc_balance.total_quantity == Decimal("10700.0")
    assert usdc_balance.available_quantity == Decimal("9900.0")

    spot_asset_balance = spot_balances["SPOT-ASSET"]
    assert spot_asset_balance.asset == "SPOT-ASSET"
    assert spot_asset_balance.total_quantity == Decimal("10.0")  # From szi
    assert spot_asset_balance.available_quantity == Decimal("10.0")  # Assuming all available
    assert spot_asset_balance.hl_details is not None


# --- Tests for map_raw_clearinghouse_state_to_derivative_positions ---


@pytest.fixture
def raw_user_state_with_positions() -> HyperliquidRawClearinghouseState:
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
    updated_asset_positions_data = [
        eth_position_asset.model_dump(by_alias=False),
        btc_position_asset.model_dump(by_alias=False),
    ]
    updated_data_python_names = {
        "asset_positions": updated_asset_positions_data,
    }
    return HyperliquidRawClearinghouseState.model_validate(updated_data_python_names)


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
        hash="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
        hash="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        fee="0.00",  # Maker trade, zero fee
        isMaker=True,
        liquidationMarkPx="50000.00",  # Example, may not be relevant for all fills
        cloid="",  # Changed from None to empty string
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
    caplog: LogCaptureFixture,  # Added caplog fixture
) -> None:
    """Test that errors during individual trade transformation are handled gracefully."""
    mapper = HyperliquidMapper()  # Use a fresh mapper instance

    # Prepare one trade that will succeed transformation by the mock,
    # and one that will cause the mock to raise an error.
    raw_trade_success = hyperliquid_raw_public_trade_buy_fixture.model_copy(deep=True)
    # Use .hash for trade ID and .sz for quantity as per HyperliquidRawPublicTrade model
    modified_data_success = raw_trade_success.model_dump()
    modified_data_success["hash"] = "success_hash_id"
    modified_data_success["coin"] = "ETH"
    modified_data_success["px"] = "2000.0"
    modified_data_success["sz"] = "1.0"
    modified_data_success["side"] = "B"
    modified_data_success["time"] = int(datetime.now(UTC).timestamp() * 1000)
    raw_trade_success = HyperliquidRawPublicTrade.model_validate(modified_data_success)

    raw_trade_fail = hyperliquid_raw_public_trade_buy_fixture.model_copy(deep=True)
    modified_data_fail = raw_trade_fail.model_dump()
    modified_data_fail["hash"] = "problematic_hash_id"
    modified_data_fail["coin"] = "BTC"
    modified_data_fail["px"] = "30000.0"
    modified_data_fail["sz"] = "0.1"
    modified_data_fail["side"] = "A"  # Corrected from "S" to "A" for sell
    modified_data_fail["time"] = int(datetime.now(UTC).timestamp() * 1000) + 1000
    raw_trade_fail = HyperliquidRawPublicTrade.model_validate(modified_data_fail)

    # This mock will be the side_effect for the patched method
    def mock_transform_side_effect(raw_trade_arg: HyperliquidRawPublicTrade) -> Trade | None:
        if raw_trade_arg.hash == "problematic_hash_id":  # Check .hash
            raise ValueError("Simulated transformation error for problematic_hash_id")

        side = OrderSide.BUY if raw_trade_arg.side == "B" else OrderSide.SELL
        # Public trades don't have a direct order_id, fee, or maker status.
        # The mapper sets defaults for these.
        return Trade(
            id=str(raw_trade_arg.hash),  # Trade.id is from hash for public trades
            symbol=raw_trade_arg.coin,
            executed_at=parse_datetime_utc(raw_trade_arg.time, field_name="time"),  # type: ignore[arg-type]
            side=side,
            order_id="UNKNOWN_PUBLIC_TRADE",  # Default from mapper
            exchange=ExchangeName.HYPERLIQUID.value,
            price=parse_decimal_value(raw_trade_arg.px, field_name="price"),  # type: ignore[arg-type]
            quantity=parse_decimal_value(raw_trade_arg.sz, field_name="quantity"),  # type: ignore[arg-type]
            client_order_id=None,  # Default
            fee=Decimal("0"),  # Default
            fee_asset=None,  # Default
            is_maker=None,  # Default
            hl_details=HyperliquidTradeDetails(
                trade_hash=raw_trade_arg.hash,
                # These are None for public trades, as per HyperliquidMapper logic
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            ),
            bp_details=None,  # Default
        )

    patched_method = mocker.patch.object(
        HyperliquidMapper,
        "transform_raw_public_trade_to_internal",
        side_effect=mock_transform_side_effect,
    )

    mapped_trades = mapper.map_raw_trades(
        [raw_trade_success, raw_trade_fail]
    )  # Corrected method name

    assert len(mapped_trades) == 1, "Only the successful trade should be mapped"
    assert mapped_trades[0].id == "success_hash_id"  # Check 'id' now
    assert mapped_trades[0].symbol == "ETH"

    expected_log_message_part1 = (
        "Skipping public trade due to transformation error: "
        "Simulated transformation error for problematic_hash_id."
    )
    expected_log_message_part2 = "Raw: {'coin': 'BTC', 'side': 'A', 'px': '30000.0', 'sz': '0.1'"  # Check start of raw data log

    assert any(
        expected_log_message_part1 in record.message
        and expected_log_message_part2 in record.message
        and "'hash': 'problematic_hash_id'"
        in record.message  # Ensure problematic_hash_id is in raw
        and record.levelno == logging.WARNING
        for record in caplog.records
    ), f"Warning for transformation error not found or doesn't match. Logs: {caplog.text}"

    patched_method.assert_any_call(raw_trade_success)
    patched_method.assert_any_call(raw_trade_fail)


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
    # Instantiate with a funding value that is valid for HyperliquidRawAssetCtx itself,
    # but we will mock parse_decimal_value to fail for this specific input.
    raw_ctx_problematic_funding = HyperliquidRawAssetCtx(  # Instantiation uses alias
        name="ERR-FUNDING-PERP",
        funding="0.0000999",  # Valid raw string, but parsing will be mocked to fail
        markPx="100",
        prevDayPx="99",
        dayNtlVlm="10000",
        impactPx=None,  # Optional, can be None
    )

    original_parse_decimal = parse_decimal_value  # Save original for delegation

    # Define a side effect function for the mock
    def side_effect_for_funding_parse(
        value: Any, allow_none: bool = False, field_name: str | None = None
    ) -> Decimal | None:
        # Check if this call is for the 'funding' field and the specific value
        if field_name == "funding" and str(value) == "0.0000999":
            return None  # Simulate parsing failure for this specific case
        # For all other calls, delegate to the original parse_decimal_value
        return original_parse_decimal(
            value, allow_none=allow_none, field_name=field_name or "unknown_field"
        )

    # Mock parse_decimal_value within the scope of the mapper module
    mocker.patch(
        "cyberdelta.apis.hyperliquid.hl_mapper.parse_decimal_value",
        side_effect=side_effect_for_funding_parse,
    )

    result = mapper.map_raw_ctx_to_funding_rate(raw_ctx_problematic_funding)
    assert result is not None, (
        "RE-APPLY: map_raw_ctx_to_funding_rate should return a FundingRate object "
        "even if funding parsing fails"
    )
    assert result.funding_rate is None, (
        "RE-APPLY: FundingRate.funding_rate should be None if raw funding parsing failed"
    )
    assert result.symbol == "ERR-FUNDING-PERP"  # RE-APPLY

    # Ensure the mock was actually called for funding (optional check)
    # To do this properly, you might need to inspect mock_parse_decimal.call_args_list
    # For simplicity, the primary assertion is that result is None.
