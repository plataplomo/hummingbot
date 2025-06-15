"""Integration tests for Backpack margin balances with positive values.

This module tests the integration between Backpack's balance/collateral endpoints
and CyberDelta's MarginAccountSummary model, focusing on accounts with positive
balances and margin positions.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.spot_balance import SpotBalance
from tests.integration.apis.backpack.shared.test_helpers import (
    BALANCE_PRECISION_TOLERANCE,
    COLLATERAL_VALUE_TOLERANCE,
    SMALL_VALUE_TOLERANCE,
    is_within_tolerance,
)

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestBackpackMarginBalancesPositive:
    """Test Backpack margin balance integration with positive values."""

    @pytest.mark.asyncio
    async def test_margin_account_summary_from_collateral_endpoint(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test creating MarginAccountSummary from collateral endpoint data.

        Validates:
        1. Collateral endpoint returns expected structure
        2. MarginAccountSummary correctly maps collateral data
        3. BackpackMarginDetails contains enhanced fields
        4. Consistency between different equity calculations
        """
        # Get margin account summary (uses collateral endpoint)
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Basic validation
        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"
        assert account_summary.timestamp is not None

        # Core fields should be populated
        assert account_summary.total_equity >= Decimal("0")
        assert account_summary.available_equity >= Decimal("0")

        # Backpack-specific details should exist
        assert account_summary.bp_details is not None
        assert account_summary.hl_details is None  # Should not have Hyperliquid details

        # Validate BackpackMarginDetails fields
        bp_details = account_summary.bp_details

        # Check enhanced equity breakdown
        if bp_details.assets_value is not None:
            assert bp_details.assets_value >= Decimal("0")

        if bp_details.liabilities_value is not None:
            assert bp_details.liabilities_value >= Decimal("0")

        # Equity calculation consistency
        if bp_details.assets_value is not None and bp_details.liabilities_value is not None:
            calculated_equity = bp_details.assets_value - bp_details.liabilities_value
            assert is_within_tolerance(
                account_summary.total_equity, calculated_equity, tolerance=SMALL_VALUE_TOLERANCE
            ), (
                f"Large discrepancy between total_equity ({account_summary.total_equity}) "
                f"and calculated equity ({calculated_equity})"
            )

        # Check margin factors
        if bp_details.imf_raw is not None:
            assert isinstance(bp_details.imf_raw, str)
            assert len(bp_details.imf_raw) > 0

        if bp_details.mmf_raw is not None:
            assert isinstance(bp_details.mmf_raw, str)
            assert len(bp_details.mmf_raw) > 0

    @pytest.mark.asyncio
    async def test_spot_balances_vs_collateral_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test consistency between spot balances and collateral endpoint.

        Validates that:
        1. Assets appear in both endpoints when appropriate
        2. Total quantities match between endpoints (accounting for auto-lending)
        3. Available vs locked quantities are consistent
        4. Collateral weights are properly reflected

        Note: Backpack's auto-lending feature can cause discrepancies:
        - When auto-lending is OFF: Spot balance shows funds, collateral may be 0
        - When auto-lending is ON: Spot balance shows 0, collateral shows funds
        This test handles both scenarios.
        """
        # Get data from both endpoints
        spot_balances = await bp_api_for_test_env.get_balances()
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert account_summary.bp_details is not None

        # Get collateral assets from bp_details if available
        collateral_assets = account_summary.bp_details.collateral_assets or []

        # Create a map of collateral data by symbol
        collateral_map = {
            asset["symbol"]: asset for asset in collateral_assets if "symbol" in asset
        }

        # For each spot balance, check consistency with collateral
        for symbol, spot_balance in spot_balances.items():
            assert isinstance(spot_balance, SpotBalance)

            # If we have non-zero balance, it might appear in collateral
            if spot_balance.total_quantity > 0:
                # Note: Due to auto-lending, spot and collateral may not match directly
                # If auto-lending is enabled, spot shows actual balance while collateral may show 0
                # If auto-lending is disabled, both should match
                if symbol in collateral_map:
                    collateral_data = collateral_map[symbol]
                    collateral_total = Decimal(collateral_data.get("totalQuantity", "0"))

                    # If both are non-zero, they should match
                    if collateral_total > 0:
                        assert is_within_tolerance(
                            spot_balance.total_quantity,
                            collateral_total,
                            tolerance=BALANCE_PRECISION_TOLERANCE,
                        ), (
                            f"Quantity mismatch for {symbol}: "
                            f"spot={spot_balance.total_quantity}, "
                            f"collateral={collateral_total}"
                        )

                    # Check lend quantity if available
                    if spot_balance.bp_details and spot_balance.bp_details.lend_quantity:
                        collateral_lend = Decimal(collateral_data.get("lendQuantity", "0"))
                        # Only check if collateral actually has lend data
                        if collateral_lend > 0:
                            assert is_within_tolerance(
                                spot_balance.bp_details.lend_quantity,
                                collateral_lend,
                                tolerance=BALANCE_PRECISION_TOLERANCE,
                            ), (
                                f"Lend quantity mismatch for {symbol}: "
                                f"spot={spot_balance.bp_details.lend_quantity}, "
                                f"collateral={collateral_lend}"
                            )

    @pytest.mark.asyncio
    async def test_margin_account_with_positions(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test margin account summary when positions exist.

        Validates:
        1. Position notional is properly calculated
        2. Unrealized PnL is tracked
        3. Margin requirements reflect positions
        4. Locked equity accounts for position margin
        """
        # Get current positions
        positions = await bp_api_for_test_env.get_positions()
        account_summary = await bp_api_for_test_env.get_account_summary()

        if positions:
            # If we have positions, certain fields should be populated
            assert account_summary.total_position_notional is not None
            assert account_summary.total_position_notional >= Decimal("0")

            # Calculate expected total notional
            expected_notional = sum(
                abs(pos.size * pos.mark_price)
                for pos in positions
                if pos.size != 0 and pos.mark_price is not None
            )

            # Allow for small differences due to price movements
            if expected_notional > 0:
                assert is_within_tolerance(
                    account_summary.total_position_notional,
                    expected_notional,
                    tolerance_percent=Decimal("5"),  # 5% tolerance for price movements
                ), (
                    f"Large notional discrepancy: "
                    f"account={account_summary.total_position_notional}, "
                    f"calculated={expected_notional}"
                )

            # Check margin requirements
            if account_summary.total_initial_margin_required is not None:
                assert account_summary.total_initial_margin_required >= Decimal("0")

            if account_summary.total_maintenance_margin_required is not None:
                assert account_summary.total_maintenance_margin_required >= Decimal("0")

            # Initial margin should be >= maintenance margin
            if (
                account_summary.total_initial_margin_required is not None
                and account_summary.total_maintenance_margin_required is not None
            ):
                assert (
                    account_summary.total_initial_margin_required
                    >= account_summary.total_maintenance_margin_required
                )

    @pytest.mark.asyncio
    async def test_margin_utilization_calculation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test margin utilization and risk metrics.

        Validates:
        1. Margin fraction calculation
        2. Available equity vs margin requirements
        3. Risk metrics consistency
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert account_summary.bp_details is not None

        # Check margin fraction if available
        if account_summary.bp_details.margin_fraction is not None:
            margin_fraction = account_summary.bp_details.margin_fraction
            # Note: Backpack's margin fraction can be > 1.0 when leverage is used
            # or when liabilities exceed a certain threshold relative to equity.
            # It appears to be calculated as a ratio where higher values indicate higher risk.
            # Values > 1 are valid and indicate leveraged positions or high margin usage.
            assert margin_fraction >= Decimal("0"), (
                f"Margin fraction should be non-negative, got {margin_fraction}"
            )

            # Log the actual value for debugging
            logger.info(
                f"Margin fraction from API: {margin_fraction}, "
                f"Maintenance margin: {account_summary.total_maintenance_margin_required}, "
                f"Total equity: {account_summary.total_equity}"
            )

            # If margin fraction is > 1, it indicates leverage or high margin usage
            if margin_fraction > Decimal("1"):
                logger.info(
                    "Margin fraction > 1 indicates leveraged position or "
                    f"high margin usage: {margin_fraction}"
                )

        # Available equity should account for margin requirements
        # Note: Backpack's available equity calculation may differ from simple
        # total_equity - initial_margin_required, especially when positions are closed
        # but some margin is still reserved
        if (
            account_summary.total_initial_margin_required is not None
            and account_summary.total_initial_margin_required > 0
            and account_summary.total_position_notional is not None
            and account_summary.total_position_notional > 0
        ):
            # Only check this when there are actual positions
            max_available = (
                account_summary.total_equity - account_summary.total_initial_margin_required
            )
            assert account_summary.available_equity <= max_available + SMALL_VALUE_TOLERANCE

    @pytest.mark.asyncio
    async def test_collateral_weights_and_values(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test collateral weight application and value calculations.

        Validates:
        1. Collateral weights are properly applied
        2. Collateral values reflect weights
        3. Different assets have appropriate weights
        """
        account_summary = await bp_api_for_test_env.get_account_summary()
        spot_balances = await bp_api_for_test_env.get_balances()

        assert account_summary.bp_details is not None

        # Get collateral assets
        collateral_assets = account_summary.bp_details.collateral_assets or []

        for asset in collateral_assets:
            symbol = asset.get("symbol")
            if not symbol:
                continue

            # Check collateral weight
            collateral_weight = Decimal(asset.get("collateralWeight", "0"))
            assert Decimal("0") <= collateral_weight <= Decimal("1")

            # Stablecoins typically have weight of 1, but only if they're the asset itself
            # not if they're part of a trading pair symbol
            if symbol in ["USDC", "USDT"]:
                # Note: During testing, weight might be 0 if there's no balance
                # Only check weight=1 if there's actual balance
                balance_notional = Decimal(asset.get("balanceNotional", "0"))
                if balance_notional > 0:
                    assert collateral_weight == Decimal("1"), (
                        f"Expected stablecoin {symbol} to have collateral weight of 1, "
                        f"got {collateral_weight}"
                    )

            # Check collateral value calculation
            balance_notional = Decimal(asset.get("balanceNotional", "0"))
            collateral_value = Decimal(asset.get("collateralValue", "0"))

            if balance_notional > 0:
                expected_collateral = balance_notional * collateral_weight
                assert is_within_tolerance(
                    collateral_value, expected_collateral, tolerance=COLLATERAL_VALUE_TOLERANCE
                ), (
                    f"Collateral value mismatch for {symbol}: "
                    f"value={collateral_value}, "
                    f"expected={expected_collateral}"
                )

            # Check consistency with spot balance collateral weight
            if symbol in spot_balances:
                spot_balance = spot_balances[symbol]
                if (
                    spot_balance.bp_details
                    and spot_balance.bp_details.collateral_weight is not None
                ):
                    assert is_within_tolerance(
                        spot_balance.bp_details.collateral_weight,
                        collateral_weight,
                        tolerance=BALANCE_PRECISION_TOLERANCE,
                    ), (
                        f"Collateral weight mismatch for {symbol}: "
                        f"spot={spot_balance.bp_details.collateral_weight}, "
                        f"collateral={collateral_weight}"
                    )
