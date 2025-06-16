"""CyberDeltaEngine: Hyperliquid Account Data Mapper Core Tests.

-----------------------------------------------------------

Comprehensive test suite for HyperliquidAccountDataMapper core transformations.
Tests fundamental transformation methods and business logic including:
- Margin summary transformations from clearinghouse state
- Spot balance transformations and USDC handling
- Core business logic validation
- Basic transformation scenarios
- Numeric parsing and validation
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

# Third-party imports for type checking only
# Project-specific imports
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.core.models import (
    HyperliquidMarginDetails,
    HyperliquidSpotBalanceDetails,
    MarginAccountSummary,
    SpotBalance,
)
from cyberdelta.enums.exchange_names import ExchangeName

# --- Fixtures ---


@pytest.fixture
def account_data_mapper() -> HyperliquidAccountDataMapper:
    """Provide an instance of HyperliquidAccountDataMapper."""
    return HyperliquidAccountDataMapper()


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
        time=1640995200000,
    )


@pytest.fixture
def raw_user_state_empty_positions_no_balances() -> HyperliquidRawClearinghouseState:
    """Provide a HyperliquidRawClearinghouseState with no asset positions.

    Includes basic margin summary.
    """
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
        time=1640995200000,
    )


# --- Tests for margin summary transformations ---


class TestMapRawClearinghouseStateToMarginSummary:
    """Tests for transform_raw_clearinghouse_state_to_margin_summary method."""

    def test_happy_path_with_positions(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
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

        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)

        new_margin_summary_data = HyperliquidRawMarginSummary(
            accountValue="12099.50",
            totalRawUsd="12500.0",
            totalMarginUsed="400.0",
            totalNtlPos="5000.0",
        ).model_dump(by_alias=False)

        updated_data_python_names = {
            **base_dump_python_names,
            "asset_positions": [asset_pos_1, asset_pos_2],
            "margin_summary": new_margin_summary_data,
            "cross_maintenance_margin_used": "50.0",
            "isolated_maintenance_margin_used": "75.0",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names,
        )

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            current_raw_state,
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

    def test_map_no_derivatives(
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

        empty_asset_positions_list: list[dict[str, Any]] = []
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

    def test_margin_summary_with_high_precision_values(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test margin summary with high precision decimal values."""
        high_precision_margin_summary = HyperliquidRawMarginSummary(
            accountValue="12345.123456789012345",
            totalRawUsd="12500.987654321098765",
            totalMarginUsed="200.111111111111111",
            totalNtlPos="2000.999999999999999",
        )

        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)
        updated_data_python_names = {
            **base_dump_python_names,
            "margin_summary": high_precision_margin_summary.model_dump(by_alias=False),
            "cross_maintenance_margin_used": "50.123456789012345",
            "isolated_maintenance_margin_used": "25.987654321098765",
            "withdrawable": "9800.555555555555555",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names,
        )

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            current_raw_state,
        )

        # Verify precision is maintained
        assert summary.total_equity == Decimal("12345.123456789012345")
        assert summary.total_initial_margin_required == Decimal("200.111111111111111")
        # Adjust expected value to match actual calculation precision
        assert summary.total_maintenance_margin_required == Decimal("76.111111110111110")
        assert summary.available_equity == Decimal("9800.555555555555555")
        assert summary.total_position_notional == Decimal("2000.999999999999999")
        assert summary.total_unrealized_pnl == Decimal("0")

        # Verify HL-specific details
        assert summary.hl_details is not None
        assert summary.hl_details.cross_maintenance_margin_used == Decimal("50.123456789012345")
        assert summary.hl_details.isolated_maintenance_margin_used == Decimal("25.987654321098765")

    def test_zero_values_in_margin_summary(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test margin summary transformation with zero values."""
        zero_margin_summary = HyperliquidRawMarginSummary(
            accountValue="0.0",
            totalRawUsd="0.0",
            totalMarginUsed="0.0",
            totalNtlPos="0.0",
        )

        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)
        updated_data_python_names = {
            **base_dump_python_names,
            "margin_summary": zero_margin_summary.model_dump(by_alias=False),
            "cross_maintenance_margin_used": "0.0",
            "isolated_maintenance_margin_used": "0.0",
            "withdrawable": "0.0",
        }
        current_raw_state = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names,
        )

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            current_raw_state,
        )

        assert summary.total_equity == Decimal("0.0")
        assert summary.total_unrealized_pnl == Decimal("0")
        assert summary.total_initial_margin_required == Decimal("0.0")
        assert summary.total_maintenance_margin_required == Decimal("0.0")
        assert summary.available_equity == Decimal("0.0")

    def test_margin_summary_timestamp_generation(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that margin summary generates appropriate timestamps."""
        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_clearinghouse_state_base_fixture,
        )

        assert summary.timestamp is not None
        assert isinstance(summary.timestamp, datetime)

        # Timestamp should be very recent (within last 5 seconds)
        current_time = datetime.now(UTC)
        time_diff = current_time - summary.timestamp
        assert time_diff.total_seconds() < 5


# --- Tests for spot balance transformations ---


class TestMapRawClearinghouseStateToSpotBalances:
    """Tests for transform_raw_clearinghouse_state_to_spot_balances method."""

    def test_empty_spot_balances(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
        raw_user_state_empty_positions_no_balances: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test mapping when raw state has no spot balances."""
        spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_user_state_empty_positions_no_balances,
        )
        assert isinstance(spot_balances, dict)
        assert not spot_balances

    def test_usdc_balance_from_account_value(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
    ) -> None:
        """Test that USDC balance is created from marginSummary.accountValue."""
        margin_summary_with_usdc_value = HyperliquidRawMarginSummary(
            accountValue="10000.0",
            totalRawUsd="10000.0",
            totalMarginUsed="0",
            totalNtlPos="0",
        )
        raw_state_for_usdc_test = HyperliquidRawClearinghouseState(
            assetPositions=[],
            marginSummary=margin_summary_with_usdc_value,
            crossMaintenanceMarginUsed="0",
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
            time=1640995200000,
        )

        spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_state_for_usdc_test,
        )

        assert "USDC" in spot_balances
        usdc_balance = spot_balances["USDC"]

        assert isinstance(usdc_balance, SpotBalance)
        assert usdc_balance.exchange == ExchangeName.HYPERLIQUID.value
        assert usdc_balance.asset == "USDC"
        assert usdc_balance.total_quantity == Decimal("10000.0")
        assert usdc_balance.available_quantity == Decimal("0")
        assert isinstance(usdc_balance.timestamp, datetime)
        assert usdc_balance.hl_details is not None
        assert isinstance(usdc_balance.hl_details, HyperliquidSpotBalanceDetails)
        assert usdc_balance.bp_details is None

    def test_spot_balances_with_high_precision(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
    ) -> None:
        """Test spot balance transformation with high precision values."""
        margin_summary_high_precision = HyperliquidRawMarginSummary(
            accountValue="10000.123456789012345",
            totalRawUsd="10000.0",
            totalMarginUsed="0",
            totalNtlPos="0",
        )
        raw_state_high_precision = HyperliquidRawClearinghouseState(
            assetPositions=[],
            marginSummary=margin_summary_high_precision,
            crossMaintenanceMarginUsed="0",
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
            withdrawable="9500.987654321098765",
            time=1640995200000,
        )

        spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_state_high_precision,
        )

        usdc_balance = spot_balances["USDC"]
        assert usdc_balance.total_quantity == Decimal("10000.123456789012345")
        assert usdc_balance.available_quantity == Decimal("9500.987654321098765")

    def test_spot_balances_with_multiple_assets(self) -> None:
        """Test mapping when raw state contains multiple spot assets."""
        raw_state_with_multiple_spot = HyperliquidRawClearinghouseState(
            assetPositions=[
                HyperliquidRawAssetPosition(
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
                HyperliquidRawAssetPosition(
                    asset="SPOT-ASSET",
                    position=HyperliquidRawPositionInfo(
                        coin="SPOT-ASSET",
                        szi="10.0",
                        entryPx="0",
                        leverage=HyperliquidRawLeverage(type="isolated", value=0),
                        liquidationPx=None,
                        marginUsed="0",
                        maxLeverage=0,
                        positionValue="500.0",
                        returnOnEquity="0",
                        unrealizedPnl="0",
                    ),
                ),
            ],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="10700.0",
                totalRawUsd="500.0",
                totalNtlPos="3000.0",
                totalMarginUsed="300.0",
            ),
            crossMaintenanceMarginUsed="150.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="10200.0",
                totalRawUsd="0",
                totalNtlPos="3000.0",
                totalMarginUsed="300.0",
            ),
            isolatedMaintenanceMarginUsed="0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0",
                totalRawUsd="0",
                totalNtlPos="0",
                totalMarginUsed="0",
            ),
            withdrawable="9900.0",
            time=1640995200000,
        )

        spot_balances = (
            HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_spot_balances(
                raw_state_with_multiple_spot,
            )
        )

        assert isinstance(spot_balances, dict)
        assert "USDC" in spot_balances
        assert "SPOT-ASSET" in spot_balances

        usdc_balance = spot_balances["USDC"]
        assert usdc_balance.total_quantity == Decimal("10700.0")
        assert usdc_balance.available_quantity == Decimal("9900.0")

        spot_asset_balance = spot_balances["SPOT-ASSET"]
        assert spot_asset_balance.asset == "SPOT-ASSET"
        assert spot_asset_balance.total_quantity == Decimal("10.0")
        assert spot_asset_balance.available_quantity == Decimal("10.0")
        assert spot_asset_balance.hl_details is not None

    def test_spot_balances_timestamp_generation(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
        raw_user_state_empty_positions_no_balances: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that spot balance transformations generate appropriate timestamps."""
        # Create a state with a USDC balance for testing
        margin_summary_with_balance = HyperliquidRawMarginSummary(
            accountValue="1000.0",
            totalRawUsd="0",
            totalMarginUsed="0",
            totalNtlPos="0",
        )
        raw_state_with_balance = raw_user_state_empty_positions_no_balances.model_copy(
            update={"marginSummary": margin_summary_with_balance},
        )

        spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_state_with_balance,
        )

        if spot_balances:
            for balance in spot_balances.values():
                assert balance.timestamp is not None
                assert isinstance(balance.timestamp, datetime)

                # Timestamp should be very recent
                current_time = datetime.now(UTC)
                time_diff = current_time - balance.timestamp
                assert time_diff.total_seconds() < 5


# --- Tests for core business logic validation ---


class TestCoreBusinessLogicValidation:
    """Tests for core business logic validation in transformations."""

    def test_unrealized_pnl_calculation_accuracy(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that unrealized PnL calculations are accurate across positions."""
        # Create positions with known PnL values
        eth_position = HyperliquidRawAssetPosition(
            asset="ETH",
            position=HyperliquidRawPositionInfo(
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
            ),
        )
        btc_position = HyperliquidRawAssetPosition(
            asset="BTC",
            position=HyperliquidRawPositionInfo(
                coin="BTC",
                szi="-0.1",
                entryPx="30000.0",
                unrealizedPnl="-75.50",
                maxLeverage=10,
                positionValue="3000.0",
                liquidationPx="25000.0",
                marginUsed="300.0",
                leverage=HyperliquidRawLeverage(type="isolated", value=5),
                returnOnEquity="-0.25",
            ),
        )

        base_dump = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)
        updated_data = {
            **base_dump,
            "asset_positions": [eth_position, btc_position],
        }
        raw_state = HyperliquidRawClearinghouseState.model_validate(updated_data)

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_state,
        )

        # Expected total unrealized PnL: 150.25 + (-75.50) = 74.75
        expected_total_pnl = Decimal("150.25") + Decimal("-75.50")
        assert summary.total_unrealized_pnl == expected_total_pnl

    def test_margin_requirement_calculations(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that margin requirement calculations are correct."""
        base_dump = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)

        # Set specific margin values for testing
        updated_data = {
            **base_dump,
            "cross_maintenance_margin_used": "100.0",
            "isolated_maintenance_margin_used": "50.0",
        }
        raw_state = HyperliquidRawClearinghouseState.model_validate(updated_data)

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_state,
        )

        # Total maintenance margin should be cross + isolated
        expected_total_maintenance = Decimal("100.0") + Decimal("50.0")
        assert summary.total_maintenance_margin_required == expected_total_maintenance

    def test_available_equity_calculation(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that available equity calculation is correct."""
        # Set a specific withdrawable amount
        test_withdrawable = "5000.0"
        raw_state = raw_clearinghouse_state_base_fixture.model_copy(
            update={"withdrawable": test_withdrawable},
        )

        summary = HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_state,
        )

        # Available equity should equal withdrawable amount
        assert summary.available_equity == Decimal(test_withdrawable)

    def test_exchange_assignment_consistency(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that exchange assignments are consistent across all transformations."""
        # Test margin summary
        margin_summary = (
            HyperliquidAccountDataMapper.transform_raw_clearinghouse_state_to_margin_summary(
                raw_clearinghouse_state_base_fixture,
            )
        )
        assert margin_summary.exchange == ExchangeName.HYPERLIQUID.value

        # Test spot balances with USDC
        margin_summary_with_usdc = HyperliquidRawMarginSummary(
            accountValue="1000.0",
            totalRawUsd="0",
            totalMarginUsed="0",
            totalNtlPos="0",
        )
        raw_state_with_usdc = raw_clearinghouse_state_base_fixture.model_copy(
            update={"marginSummary": margin_summary_with_usdc},
        )

        spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_state_with_usdc,
        )

        for balance in spot_balances.values():
            assert balance.exchange == ExchangeName.HYPERLIQUID.value
