"""Integration tests for Backpack margin account data flow.

Tests the integration between spot balances, collateral data, and margin account summary.
Validates data consistency and calculations across different API endpoints.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.spot_balance import SpotBalance
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    BALANCE_PRECISION_TOLERANCE,
    COLLATERAL_VALUE_TOLERANCE,
    DUST_THRESHOLD,
    LARGE_VALUE_TOLERANCE,
    SMALL_VALUE_TOLERANCE,
    is_stablecoin,
    is_within_tolerance,
)


@pytest.mark.integration
class TestBackpackMarginIntegrationFlow:
    """Test margin account data integration across endpoints."""

    @pytest.mark.asyncio
    async def test_spot_balances_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that spot balances are properly structured and contain required fields."""
        spot_balances = await bp_api_for_test_env.get_balances()

        assert isinstance(spot_balances, dict), "Balances should be a dict"
        assert len(spot_balances) > 0, "Should have at least one balance"

        for symbol, balance in spot_balances.items():
            assert isinstance(balance, SpotBalance), f"{symbol} balance should be SpotBalance"
            assert balance.exchange == "backpack", f"{symbol} should be from backpack exchange"
            assert balance.asset == symbol, f"{symbol} asset field mismatch"
            assert balance.total_quantity >= Decimal(0), f"{symbol} total should be non-negative"
            assert balance.available_quantity >= Decimal(0), (
                f"{symbol} available should be non-negative"
            )
            assert balance.available_quantity <= balance.total_quantity, (
                f"{symbol} available should not exceed total"
            )

            # Validate bp_details if present
            if balance.bp_details and balance.total_quantity > 0:
                if balance.bp_details.lend_quantity is not None:
                    assert balance.bp_details.lend_quantity >= Decimal(0), (
                        f"{symbol} lend_quantity should be non-negative"
                    )
                    assert balance.bp_details.lend_quantity <= balance.total_quantity, (
                        f"{symbol} lend_quantity should not exceed total"
                    )

                if balance.bp_details.open_order_quantity is not None:
                    assert balance.bp_details.open_order_quantity >= Decimal(0), (
                        f"{symbol} open_order_quantity should be non-negative"
                    )

                if balance.bp_details.collateral_weight is not None:
                    assert Decimal(0) <= balance.bp_details.collateral_weight <= Decimal(1), (
                        f"{symbol} collateral_weight should be between 0 and 1"
                    )

    @pytest.mark.asyncio
    async def test_margin_account_summary_structure(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that margin account summary contains all required fields and calculations."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Basic structure validation
        assert isinstance(account_summary, MarginAccountSummary), "Should be MarginAccountSummary"
        assert account_summary.exchange == "backpack", "Should be from backpack exchange"
        assert account_summary.timestamp is not None, "Should have timestamp"

        # Core financial fields
        assert account_summary.total_equity >= Decimal(0), "Total equity should be non-negative"
        assert account_summary.available_equity >= Decimal(0), (
            "Available equity should be non-negative"
        )
        assert account_summary.available_equity <= account_summary.total_equity, (
            "Available equity should not exceed total equity"
        )

        # Backpack-specific details must exist
        assert account_summary.bp_details is not None, "Should have bp_details"
        assert account_summary.hl_details is None, "Should not have hl_details"

        bp_details = account_summary.bp_details

        # Validate equity breakdown if available
        if bp_details.assets_value is not None and bp_details.liabilities_value is not None:
            assert bp_details.assets_value >= Decimal(0), "Assets value should be non-negative"
            assert bp_details.liabilities_value >= Decimal(0), (
                "Liabilities value should be non-negative"
            )

            calculated_equity = bp_details.assets_value - bp_details.liabilities_value
            assert is_within_tolerance(
                calculated_equity,
                account_summary.total_equity,
                tolerance=SMALL_VALUE_TOLERANCE,
            ), f"Equity calculation mismatch: {calculated_equity} != {account_summary.total_equity}"

        # Validate margin fraction
        if bp_details.margin_fraction is not None:
            assert bp_details.margin_fraction >= Decimal(0), (
                "Margin fraction should be non-negative"
            )

    @pytest.mark.asyncio
    async def test_spot_vs_collateral_data_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test consistency between spot balance and collateral endpoints."""
        spot_balances = await bp_api_for_test_env.get_balances()
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert account_summary.bp_details is not None
        collateral_assets = account_summary.bp_details.collateral_assets or []

        # Create collateral map for easy lookup
        collateral_map = {
            asset["symbol"]: asset for asset in collateral_assets if "symbol" in asset
        }

        # Test each spot balance against collateral data
        for symbol, spot_balance in spot_balances.items():
            if spot_balance.total_quantity == 0:
                continue  # Skip zero balances

            if symbol in collateral_map:
                collateral_data = collateral_map[symbol]
                collateral_total = Decimal(collateral_data.get("totalQuantity", "0"))

                # Only validate when both have non-zero values
                if collateral_total > 0:
                    assert is_within_tolerance(
                        spot_balance.total_quantity,
                        collateral_total,
                        tolerance=BALANCE_PRECISION_TOLERANCE,
                    ), (
                        f"{symbol} quantity mismatch: "
                        f"spot={spot_balance.total_quantity}, collateral={collateral_total}"
                    )

                # Validate collateral value calculation
                collateral_weight = Decimal(collateral_data.get("collateralWeight", "0"))
                collateral_value = Decimal(collateral_data.get("collateralValue", "0"))
                mark_price = Decimal(collateral_data.get("assetMarkPrice", "0"))

                if mark_price > 0 and collateral_total > 0:
                    expected_value = collateral_total * mark_price * collateral_weight
                    assert abs(collateral_value - expected_value) < COLLATERAL_VALUE_TOLERANCE, (
                        f"{symbol} collateral value mismatch: "
                        f"expected={expected_value}, actual={collateral_value}"
                    )

    @pytest.mark.asyncio
    async def test_margin_requirements_with_positions(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test margin requirements calculation when positions exist."""
        positions = await bp_api_for_test_env.get_positions()
        account_summary = await bp_api_for_test_env.get_account_summary()

        if positions:
            # With positions, should have position notional
            assert account_summary.total_position_notional is not None
            assert account_summary.total_position_notional >= Decimal(0)

            # Calculate expected notional from positions
            expected_notional = Decimal(
                sum(
                    abs(pos.size * pos.mark_price)
                    for pos in positions
                    if pos.size != 0 and pos.mark_price is not None
                ),
            )

            if expected_notional > 0:
                # Use a more generous tolerance for position notional since mark prices can differ
                # between the position and account summary endpoints due to timing differences
                tolerance_percent = Decimal("0.1")  # 0.1% tolerance for notional calculations
                assert is_within_tolerance(
                    account_summary.total_position_notional,
                    expected_notional,
                    tolerance_percent=tolerance_percent,
                ), (
                    f"Position notional mismatch: "
                    f"account={account_summary.total_position_notional}, "
                    f"calculated={expected_notional}"
                )

            # Validate margin requirements
            if account_summary.total_initial_margin_required is not None:
                assert account_summary.total_initial_margin_required >= Decimal(0)

            if account_summary.total_maintenance_margin_required is not None:
                assert account_summary.total_maintenance_margin_required >= Decimal(0)

            # Initial margin >= maintenance margin
            if (
                account_summary.total_initial_margin_required is not None
                and account_summary.total_maintenance_margin_required is not None
            ):
                assert (
                    account_summary.total_initial_margin_required
                    >= account_summary.total_maintenance_margin_required
                ), "Initial margin should be >= maintenance margin"

    @pytest.mark.asyncio
    async def test_available_equity_calculation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test available equity calculation with locked equity."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert account_summary.bp_details is not None
        bp_details = account_summary.bp_details

        # If locked equity is provided, validate available equity calculation
        if bp_details.locked_equity is not None and bp_details.locked_equity > 0:
            expected_available = account_summary.total_equity - bp_details.locked_equity
            assert is_within_tolerance(
                expected_available,
                account_summary.available_equity,
                tolerance=LARGE_VALUE_TOLERANCE,
            ), (
                f"Available equity mismatch: "
                f"expected={expected_available} "
                f"(total={account_summary.total_equity} - locked={bp_details.locked_equity}), "
                f"actual={account_summary.available_equity}"
            )

    @pytest.mark.asyncio
    async def test_collateral_weight_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that collateral weights are properly applied across assets."""
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert account_summary.bp_details is not None
        collateral_assets = account_summary.bp_details.collateral_assets or []

        # Track what we find for validation
        weights_validated = False

        for asset in collateral_assets:
            symbol = asset.get("symbol")
            if not symbol:
                continue

            # Get values with proper defaults (handle both camelCase and snake_case)
            collateral_weight_str = asset.get(
                "collateralWeight",
                asset.get("collateral_weight", "0"),
            )
            balance_notional_str = asset.get("balanceNotional", asset.get("balance_notional", "0"))

            # Skip if no collateral weight info
            if (
                collateral_weight_str == "0"
                and "collateralWeight" not in asset
                and "collateral_weight" not in asset
            ):
                continue

            collateral_weight = Decimal(collateral_weight_str)
            balance_notional = Decimal(balance_notional_str)

            # Validate weight range
            assert Decimal(0) <= collateral_weight <= Decimal(1), (
                f"{symbol} collateral weight out of range: {collateral_weight}"
            )
            weights_validated = True

            # Check stablecoins have weight of 1 when they have substantial balance
            if is_stablecoin(symbol) and balance_notional > DUST_THRESHOLD:
                assert collateral_weight == Decimal(1), (
                    f"Stablecoin {symbol} with balance {balance_notional} should have "
                    f"collateral weight of 1, got {collateral_weight}"
                )

        # More flexible validation - we should have validated at least some weights
        if len(collateral_assets) > 0:
            assert weights_validated, (
                f"No collateral weights found to validate in assets: {collateral_assets}"
            )
