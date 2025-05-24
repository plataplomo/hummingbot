"""
Unit tests for the Hyperliquid Account Data Mapper.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError

# Third-party imports for type checking only
if TYPE_CHECKING:
    pass

# Project-specific imports
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
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
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails

# Fixtures for Account Data Mapper tests


@pytest.fixture
def account_data_mapper() -> HyperliquidAccountDataMapper:
    """Provide an instance of HyperliquidAccountDataMapper."""
    return HyperliquidAccountDataMapper()


@pytest.fixture
def raw_user_state_empty_positions_no_balances(
    # raw_margin_summary_fixture: HyperliquidRawMarginSummary,  # ARG001: Removed
) -> HyperliquidRawClearinghouseState:
    """Provide a HyperliquidRawClearinghouseState with no asset positions
    and basic margin summary.
    """
    # Create a zeroed-out or minimal valid margin summary for this fixture
    empty_margin_summary = HyperliquidRawMarginSummary(
        accountValue="0",
        totalRawUsd="0",
        totalMarginUsed="0",
        totalNtlPos="0",
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
    """Provide a basic HyperliquidRawLeverage fixture."""
    return HyperliquidRawLeverage(type="cross", value=10)


@pytest.fixture
def raw_position_info_fixture(
    raw_leverage_fixture: HyperliquidRawLeverage,
) -> HyperliquidRawPositionInfo:
    """Provide a basic HyperliquidRawPositionInfo fixture."""
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
    """Provide a HyperliquidRawAssetPosition fixture representing a spot balance."""
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
    """Provide a HyperliquidRawAssetPosition fixture for a derivative."""
    return HyperliquidRawAssetPosition(asset="ETH", position=raw_position_info_fixture)


@pytest.fixture
def raw_margin_summary_fixture() -> HyperliquidRawMarginSummary:
    """Provide a basic HyperliquidRawMarginSummary fixture."""
    return HyperliquidRawMarginSummary(
        accountValue="12000.0",
        totalRawUsd="12500.0",
        totalMarginUsed="200.0",
        totalNtlPos="2000.0",
    )


@pytest.fixture
def raw_clearinghouse_state_base_fixture(
    raw_margin_summary_fixture: HyperliquidRawMarginSummary,
) -> HyperliquidRawClearinghouseState:
    """Provide a base HyperliquidRawClearinghouseState fixture for testing."""
    return HyperliquidRawClearinghouseState(
        assetPositions=[],
        marginSummary=raw_margin_summary_fixture,
        crossMaintenanceMarginUsed="50.0",
        crossMarginSummary=raw_margin_summary_fixture,
        isolatedMaintenanceMarginUsed="25.0",
        isolatedMarginSummary=raw_margin_summary_fixture,
        withdrawable="9800.0",
    )


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
    # Add all required fields for HyperliquidRawClearinghouseState
    updated_data_python_names: dict[str, Any] = {
        "asset_positions": updated_asset_positions_data,
        "cross_maintenance_margin_used": "50.0",  # Example value
        "cross_margin_summary": {  # Example structure
            "account_value": "10000.0",
            "total_margin_used": "1000.0",
            "total_ntl_pos": "9000.0",
            "total_raw_usd": "8000.0",
        },
        "margin_summary": {  # Example structure, often same as cross for overall
            "account_value": "10000.0",
            "total_margin_used": "1000.0",
            "total_ntl_pos": "9000.0",
            "total_raw_usd": "8000.0",
        },
        "isolated_maintenance_margin_used": "100.0",  # Example value
        "isolated_margin_summary": {  # Example structure
            "account_value": "0",  # Can be 0 if no isolated positions or specific context
            "total_margin_used": "0",
            "total_ntl_pos": "0",
            "total_raw_usd": "0",
        },
        "withdrawable": "7000.0",  # Example value
    }
    return HyperliquidRawClearinghouseState.model_validate(updated_data_python_names)


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


# --- Tests for map_raw_clearinghouse_state_to_margin_summary ---


class TestMapRawClearinghouseStateToMarginSummary:
    """Tests for map_raw_clearinghouse_state_to_margin_summary."""

    def test_happy_path_with_positions(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
        # raw_asset_position_derivative_fixture: HyperliquidRawAssetPosition, # ARG002: Removed
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
            "asset_positions": [asset_pos_1, asset_pos_2],  # Use model objects directly
            "margin_summary": new_margin_summary_data,
            "cross_maintenance_margin_used": "50.0",
            "isolated_maintenance_margin_used": "75.0",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names,
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
        assert current_raw_state.margin_summary.total_raw_usd == "12500.0", (
            "Validated margin_summary.total_raw_usd mismatch"
        )
        assert current_raw_state.margin_summary.total_ntl_pos == "5000.0", (
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
        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            current_raw_state
        )

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

    def test_map_raw_clearinghouse_state_to_margin_summary_no_derivatives(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test mapping when there are no derivative positions."""
        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(
            by_alias=False,
        )

        new_margin_summary_data = HyperliquidRawMarginSummary(
            accountValue="12000.0",
            totalRawUsd="12500.0",
            totalMarginUsed="0.0",
            totalNtlPos="0.0",
        ).model_dump(by_alias=False)

        empty_asset_positions_list: list[dict[str, Any]] = []  # Explicitly typed empty list
        updated_data_python_names: dict[str, Any] = {
            **base_dump_python_names,
            "asset_positions": empty_asset_positions_list,
            "margin_summary": new_margin_summary_data,
            "cross_maintenance_margin_used": "0.0",
            "isolated_maintenance_margin_used": "0.0",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names,
        )

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            current_raw_state,
        )

        assert summary.total_unrealized_pnl == Decimal("0")
        assert summary.total_equity == Decimal("12000.0")
        assert summary.total_initial_margin_required == Decimal("0.0")
        assert summary.total_maintenance_margin_required == Decimal("0.0")
        assert summary.available_equity == Decimal(current_raw_state.withdrawable)

        assert isinstance(summary.hl_details, HyperliquidMarginDetails)
        assert summary.hl_details.cross_maintenance_margin_used == Decimal("0.0")
        assert summary.hl_details.isolated_maintenance_margin_used == Decimal("0.0")

    def test_invalid_numeric_strings_in_raw_state(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that creating/copying raw_state with invalid numeric strings
        raises ValidationError.
        """
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
                },
            )

    def test_withdrawable_funds_invalid_or_missing_in_raw_state(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that mapping raw_state with invalid 'withdrawable' raises ValueError from mapper."""
        current_raw_state_invalid = raw_clearinghouse_state_base_fixture.model_copy(
            update={"withdrawable": "not-a-decimal"},
        )
        # Expect ValueError from parse_decimal_value inside the mapper
        with pytest.raises(ValueError) as exc_info:
            HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
                current_raw_state_invalid,
            )
        assert "withdrawable" in str(exc_info.value).lower()

    def test_cross_maintenance_margin_used_invalid_or_missing_in_raw_state(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
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
            updated_data_python_names,
        )

        summary_updated_mmr = (
            HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
                current_raw_state_updated_mmr,
            )
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
            ValidationError,
            match="crossMaintenanceMarginUsed",
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
    account_data_mapper: HyperliquidAccountDataMapper,
    raw_user_state_empty_positions_no_balances: HyperliquidRawClearinghouseState,
) -> None:
    """Test mapping when raw state has no spot balances (e.g., only perp positions)."""
    spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
        raw_user_state_empty_positions_no_balances,
    )
    assert isinstance(spot_balances, dict)
    assert not spot_balances  # Expect empty dictionary


def test_map_raw_clearinghouse_state_to_spot_balances_with_usdc(
    account_data_mapper: HyperliquidAccountDataMapper,
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
            accountValue="0",
            totalRawUsd="0",
            totalMarginUsed="0",
            totalNtlPos="0",
        ),
        isolatedMaintenanceMarginUsed="0",
        isolatedMarginSummary=HyperliquidRawMarginSummary(
            accountValue="0",
            totalRawUsd="0",
            totalMarginUsed="0",
            totalNtlPos="0",
        ),
        withdrawable="0",
    )

    spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
        raw_state_for_usdc_test
    )

    assert "USDC" in spot_balances
    usdc_balance = spot_balances["USDC"]

    assert isinstance(usdc_balance, SpotBalance)
    assert usdc_balance.exchange == ExchangeName.HYPERLIQUID.value
    assert usdc_balance.asset == "USDC"
    assert usdc_balance.total_quantity == Decimal("10000.0")  # From marginSummary.accountValue
    assert usdc_balance.available_quantity == Decimal(
        "0",
    )  # Corrected: From raw_state_for_usdc_test.withdrawable which is "0"
    assert isinstance(usdc_balance.timestamp, datetime)
    assert usdc_balance.hl_details is not None
    assert isinstance(usdc_balance.hl_details, HyperliquidSpotBalanceDetails)
    assert usdc_balance.bp_details is None


def test_map_raw_clearinghouse_state_to_spot_balances_with_other_spot_assets() -> None:
    """Test mapping when raw state contains other spot assets in assetPositions.
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
                        type="isolated",
                        value=0,
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

    spot_balances = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_spot_balances(
        raw_state_with_spot
    )

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


def test_map_raw_clearinghouse_state_to_derivative_positions_empty(
    raw_user_state_empty_positions_no_balances: HyperliquidRawClearinghouseState,
) -> None:
    """Test mapping when raw state has no derivative positions."""
    positions = (
        HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_user_state_empty_positions_no_balances,
        )
    )
    assert isinstance(positions, dict)
    assert not positions


def test_map_raw_clearinghouse_state_to_derivative_positions_populated(
    raw_user_state_with_positions: HyperliquidRawClearinghouseState,
) -> None:
    """Test mapping with ETH long and BTC short positions."""
    positions = (
        HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_user_state_with_positions,
        )
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


# --- Tests for transform_raw_fill_to_internal ---


def test_transform_raw_fill_to_internal_buy_taker(
    hyperliquid_raw_fill_buy_fixture: HyperliquidRawFill,
) -> None:
    """Test transforming a raw BUY TAKER fill to an internal Trade model."""
    raw_fill = hyperliquid_raw_fill_buy_fixture
    trade = HyperliquidAccountDataMapper.transform_raw_fill_to_internal(raw_fill)

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
    hyperliquid_raw_fill_sell_maker_fixture: HyperliquidRawFill,
) -> None:
    """Test transforming a raw SELL MAKER fill to an internal Trade model."""
    raw_fill = hyperliquid_raw_fill_sell_maker_fixture
    trade = HyperliquidAccountDataMapper.transform_raw_fill_to_internal(raw_fill)

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


def test_transform_raw_fill_to_internal_invalid_data_handling() -> None:
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
    # Covered by Pydantic validation for now
