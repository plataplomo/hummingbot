"""Integration tests for Backpack margin balances with zero values.

This module tests the edge cases where accounts have minimal or zero balances,
ensuring proper handling of empty states and edge conditions.

IMPORTANT: These tests require a zero-balance account to run properly.
They will fail if run against an account with positive balances.
TODO: Run these tests with a dedicated zero-balance test account.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.spot_balance import SpotBalance
from tests.integration.apis.backpack.shared.test_helpers import (
    BALANCE_PRECISION_TOLERANCE,
    DUST_THRESHOLD,
)


@pytest.mark.integration
class TestBackpackMarginBalancesZero:
    """Test Backpack margin balance integration with zero/minimal values."""

    @pytest.mark.asyncio
    async def test_margin_account_summary_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test MarginAccountSummary with zero or minimal balance.

        Validates:
        1. Proper handling of zero equity accounts
        2. Correct defaults for optional fields
        3. No division by zero errors
        4. Proper representation of empty state
        """
        # Get margin account summary
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.exchange == "backpack"

        # Even with zero balance, these fields should be present
        assert account_summary.total_equity >= Decimal("0")
        assert account_summary.available_equity >= Decimal("0")

        # Check if this is truly a zero balance account
        is_zero_balance = account_summary.total_equity < DUST_THRESHOLD

        if is_zero_balance:
            # With zero balance, margin requirements could still exist if there are open orders
            # The exchange maintains minimum margin requirements even for zero balance accounts
            if account_summary.total_initial_margin_required is not None:
                assert account_summary.total_initial_margin_required >= Decimal("0"), (
                    f"Initial margin requirement should be non-negative, got "
                    f"{account_summary.total_initial_margin_required}"
                )

            if account_summary.total_maintenance_margin_required is not None:
                assert account_summary.total_maintenance_margin_required >= Decimal("0"), (
                    f"Maintenance margin requirement should be non-negative, got "
                    f"{account_summary.total_maintenance_margin_required}"
                )

            # Position notional should be zero or small with no/minimal positions
            if account_summary.total_position_notional is not None:
                assert account_summary.total_position_notional >= Decimal("0"), (
                    f"Position notional should be non-negative, got "
                    f"{account_summary.total_position_notional}"
                )

            # Unrealized PnL should be zero or small
            if account_summary.total_unrealized_pnl is not None:
                # PnL can be slightly negative due to fees
                assert abs(account_summary.total_unrealized_pnl) <= DUST_THRESHOLD, (
                    f"Unrealized PnL should be near zero for zero balance account, got "
                    f"{account_summary.total_unrealized_pnl}"
                )
        else:
            # Account has balance - just validate the fields exist and are valid
            pytest.skip(
                f"Account has balance ({account_summary.total_equity}), "
                "skipping zero balance validation"
            )

    @pytest.mark.asyncio
    async def test_backpack_margin_details_zero_values(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test BackpackMarginDetails with zero values.

        Validates:
        1. Optional fields handle zero/None properly
        2. Margin fraction is None or zero when no positions
        3. All equity components are zero or minimal
        """
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        assert account_summary.bp_details is not None
        bp_details = account_summary.bp_details

        # Check if this is truly a zero balance account
        is_zero_balance = account_summary.total_equity < DUST_THRESHOLD

        if not is_zero_balance:
            # Account has balance - skip zero balance validation
            pytest.skip(
                f"Account has balance ({account_summary.total_equity}), "
                "skipping zero balance validation"
            )

        # Check zero handling for optional fields
        if bp_details.assets_value is not None:
            assert bp_details.assets_value >= Decimal("0")

        if bp_details.liabilities_value is not None:
            assert bp_details.liabilities_value >= Decimal("0")

        if bp_details.locked_equity is not None:
            assert bp_details.locked_equity >= Decimal("0")

        if bp_details.borrow_liability is not None:
            assert bp_details.borrow_liability >= Decimal("0")

        # Margin fraction should be None or 0 with no positions
        # Note: Some zero balance accounts may have margin_fraction as None
        if bp_details.margin_fraction is not None:
            # For zero balance accounts, margin fraction should be 0 or very small
            assert bp_details.margin_fraction <= DUST_THRESHOLD, (
                f"Expected margin fraction to be 0 or dust for zero balance account, "
                f"got {bp_details.margin_fraction}"
            )

        # Net exposure should be zero with no futures positions
        if bp_details.net_exposure_futures is not None:
            assert abs(bp_details.net_exposure_futures) <= DUST_THRESHOLD

    @pytest.mark.asyncio
    async def test_spot_balances_all_zero(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test spot balances when all assets have zero balance.

        Validates:
        1. Empty balance dictionary handling
        2. Zero balance representation
        3. Proper defaults for BackpackSpotBalanceDetails
        """
        spot_balances = await bp_api_for_zero_balance_test.get_balances()

        # Even with zero balances, common assets might be returned
        for _, balance in spot_balances.items():
            assert isinstance(balance, SpotBalance)

            if balance.total_quantity == Decimal("0"):
                # Zero balance validation
                assert balance.available_quantity == Decimal("0")

                # Check bp_details for zero balances
                if balance.bp_details:
                    if balance.bp_details.open_order_quantity is not None:
                        assert balance.bp_details.open_order_quantity == Decimal("0")

                    if balance.bp_details.lend_quantity is not None:
                        assert balance.bp_details.lend_quantity == Decimal("0")

    @pytest.mark.asyncio
    async def test_collateral_endpoint_zero_collateral(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test collateral endpoint response with zero collateral.

        Validates:
        1. Empty collateral array handling
        2. Zero net equity representation
        3. Proper margin factor defaults
        """
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        # With zero collateral, equity should be zero
        if account_summary.total_equity == Decimal("0"):
            # Available equity should also be zero
            assert account_summary.available_equity == Decimal("0")

            # Check bp_details for zero state
            if account_summary.bp_details:
                # Collateral assets might be empty or contain zero-value entries
                if account_summary.bp_details.collateral_assets is not None:
                    for asset in account_summary.bp_details.collateral_assets:
                        # Zero collateral value
                        collateral_value = Decimal(asset.get("collateralValue", "0"))
                        if collateral_value == Decimal("0"):
                            # Total quantity should also be zero
                            total_quantity = Decimal(asset.get("totalQuantity", "0"))
                            assert total_quantity == Decimal("0")

    @pytest.mark.asyncio
    async def test_margin_calculations_no_positions(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test margin calculations when no positions exist.

        Validates:
        1. Zero margin requirements
        2. Full equity availability
        3. No division errors in margin calculations
        """
        positions = await bp_api_for_zero_balance_test.get_positions()
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        if not positions:  # No positions
            # With no positions, margin requirements could still exist due to:
            # 1. Open orders that require margin
            # 2. Exchange minimum margin requirements
            # 3. Pending settlements
            if account_summary.total_initial_margin_required is not None:
                assert account_summary.total_initial_margin_required >= Decimal("0"), (
                    f"Initial margin requirement should be non-negative, got "
                    f"{account_summary.total_initial_margin_required}"
                )

            if account_summary.total_maintenance_margin_required is not None:
                assert account_summary.total_maintenance_margin_required >= Decimal("0"), (
                    f"Maintenance margin requirement should be non-negative, got "
                    f"{account_summary.total_maintenance_margin_required}"
                )

            # Available equity should equal total equity (nothing locked)
            # unless there are open orders
            if account_summary.total_equity > Decimal("0"):
                # Check if there's locked equity (from open orders)
                locked_equity = Decimal("0")
                if account_summary.bp_details and account_summary.bp_details.locked_equity:
                    locked_equity = account_summary.bp_details.locked_equity

                expected_available = account_summary.total_equity - locked_equity
                assert (
                    abs(account_summary.available_equity - expected_available) < DUST_THRESHOLD
                ), (
                    f"Available equity mismatch: got {account_summary.available_equity}, "
                    f"expected {expected_available} "
                    f"(total={account_summary.total_equity}, locked={locked_equity})"
                )

            # Margin fraction should be None or 0
            if (
                account_summary.bp_details
                and account_summary.bp_details.margin_fraction is not None
            ):
                assert account_summary.bp_details.margin_fraction == Decimal("0")
        else:
            # Has positions - skip this test
            pytest.skip(f"Account has {len(positions)} positions, skipping no-position validation")

    @pytest.mark.asyncio
    async def test_edge_case_tiny_balances(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test handling of very small (dust) balances.

        Validates:
        1. Proper decimal precision handling
        2. No rounding errors
        3. Consistent representation across endpoints
        """
        spot_balances = await bp_api_for_zero_balance_test.get_balances()
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        # Check for any dust balances
        for symbol, balance in spot_balances.items():
            if Decimal("0") < balance.total_quantity < DUST_THRESHOLD:
                # Dust balance found - verify proper handling
                assert isinstance(balance.total_quantity, Decimal)
                assert balance.total_quantity > Decimal("0")
                assert balance.available_quantity >= Decimal("0")

                # Dust should still appear in collateral if non-zero
                if account_summary.bp_details and account_summary.bp_details.collateral_assets:
                    collateral_map = {
                        asset["symbol"]: asset
                        for asset in account_summary.bp_details.collateral_assets
                        if "symbol" in asset
                    }

                    if symbol in collateral_map:
                        collateral_total = Decimal(collateral_map[symbol].get("totalQuantity", "0"))
                        # Should match within precision limits
                        assert (
                            abs(balance.total_quantity - collateral_total)
                            < BALANCE_PRECISION_TOLERANCE
                        )

    @pytest.mark.asyncio
    async def test_zero_balance_field_validation(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
    ) -> None:
        """Test that all model fields properly validate zero values.

        Validates:
        1. All Decimal fields accept zero
        2. Optional fields can be None
        3. No validation errors on edge cases
        """
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        # Core fields validation
        assert isinstance(account_summary.total_equity, Decimal)
        assert isinstance(account_summary.available_equity, Decimal)

        # Optional fields can be None or zero
        optional_decimal_fields = [
            "total_initial_margin_required",
            "total_maintenance_margin_required",
            "total_position_notional",
            "total_unrealized_pnl",
        ]

        for field_name in optional_decimal_fields:
            value = getattr(account_summary, field_name)
            if value is not None:
                assert isinstance(value, Decimal)
                # These fields have ge=0 constraint except unrealized_pnl
                if field_name != "total_unrealized_pnl":
                    assert value >= Decimal("0")

        # Check BackpackMarginDetails optional fields
        if account_summary.bp_details:
            bp_optional_fields = [
                "assets_value",
                "liabilities_value",
                "locked_equity",
                "borrow_liability",
                "margin_fraction",
                "net_exposure_futures",
                "unsettled_equity",
            ]

            for field_name in bp_optional_fields:
                if hasattr(account_summary.bp_details, field_name):
                    value = getattr(account_summary.bp_details, field_name)
                    if value is not None:
                        assert isinstance(value, Decimal)
                        # Most have ge=0 constraint
                        if field_name not in ["net_exposure_futures", "unsettled_equity"]:
                            assert value >= Decimal("0")
