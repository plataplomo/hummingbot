"""End-to-end integration test for Backpack margin account flow.

This module demonstrates the complete integration flow between:
- Spot balances from /api/v1/capital
- Collateral data from /api/v1/capital/collateral
- MarginAccountSummary model integration
- Consistency validation across endpoints
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.spot_balance import SpotBalance


@pytest.mark.integration
class TestBackpackMarginIntegrationFlow:
    """Test complete margin account integration flow."""

    async def test_complete_margin_account_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test the complete flow of margin account data integration.
        
        This test demonstrates how the Backpack API integrates:
        1. Basic spot balance data
        2. Enhanced collateral information
        3. Margin account summary with risk metrics
        4. Position-aware margin calculations
        
        The test validates the consistency and correctness of data
        across all these components.
        """
        # Step 1: Get spot balances (basic endpoint)
        print("\n=== Step 1: Getting Spot Balances ===")
        spot_balances = await bp_api_for_test_env.get_balances()
        
        # Validate spot balances
        assert isinstance(spot_balances, dict)
        for symbol, balance in spot_balances.items():
            assert isinstance(balance, SpotBalance)
            assert balance.exchange == "backpack"
            assert balance.asset == symbol
            print(f"{symbol}: total={balance.total_quantity}, available={balance.available_quantity}")
            
            # Check Backpack-specific details
            if balance.bp_details:
                if balance.bp_details.lend_quantity is not None:
                    print(f"  - Lending: {balance.bp_details.lend_quantity}")
                if balance.bp_details.open_order_quantity is not None:
                    print(f"  - In Orders: {balance.bp_details.open_order_quantity}")
                if balance.bp_details.collateral_weight is not None:
                    print(f"  - Collateral Weight: {balance.bp_details.collateral_weight}")
        
        # Step 2: Get margin account summary (uses collateral endpoint)
        print("\n=== Step 2: Getting Margin Account Summary ===")
        account_summary = await bp_api_for_test_env.get_account_summary()
        
        assert isinstance(account_summary, MarginAccountSummary)
        print(f"Total Equity: ${account_summary.total_equity}")
        print(f"Available Equity: ${account_summary.available_equity}")
        
        # Step 3: Validate enhanced Backpack details
        print("\n=== Step 3: Enhanced Backpack Details ===")
        assert account_summary.bp_details is not None
        bp_details = account_summary.bp_details
        
        if bp_details.assets_value is not None:
            print(f"Assets Value: ${bp_details.assets_value}")
        if bp_details.liabilities_value is not None:
            print(f"Liabilities Value: ${bp_details.liabilities_value}")
        if bp_details.locked_equity is not None:
            print(f"Locked Equity: ${bp_details.locked_equity}")
        if bp_details.margin_fraction is not None:
            print(f"Margin Utilization: {bp_details.margin_fraction * 100:.2f}%")
        
        # Step 4: Cross-validate spot vs collateral data
        print("\n=== Step 4: Cross-Validation ===")
        if bp_details.collateral_assets:
            collateral_map = {
                asset["symbol"]: asset
                for asset in bp_details.collateral_assets
                if "symbol" in asset
            }
            
            # Validate each spot balance against collateral
            for symbol, spot_balance in spot_balances.items():
                if symbol in collateral_map and spot_balance.total_quantity > 0:
                    collateral_data = collateral_map[symbol]
                    
                    # Validate total quantity matches
                    spot_total = spot_balance.total_quantity
                    collateral_total = Decimal(collateral_data.get("totalQuantity", "0"))
                    
                    print(f"\n{symbol} Validation:")
                    print(f"  Spot Total: {spot_total}")
                    print(f"  Collateral Total: {collateral_total}")
                    
                    # They should match closely
                    quantity_diff = abs(spot_total - collateral_total)
                    assert quantity_diff < Decimal("0.0001"), (
                        f"Quantity mismatch for {symbol}: "
                        f"spot={spot_total}, collateral={collateral_total}"
                    )
                    
                    # Validate collateral value calculation
                    collateral_weight = Decimal(collateral_data.get("collateralWeight", "0"))
                    collateral_value = Decimal(collateral_data.get("collateralValue", "0"))
                    mark_price = Decimal(collateral_data.get("assetMarkPrice", "0"))
                    
                    if mark_price > 0:
                        expected_value = collateral_total * mark_price * collateral_weight
                        value_diff = abs(collateral_value - expected_value)
                        print(f"  Collateral Value: ${collateral_value} "
                              f"(weight={collateral_weight})")
                        
                        # Allow small rounding differences
                        assert value_diff < Decimal("0.01"), (
                            f"Collateral value mismatch for {symbol}"
                        )
        
        # Step 5: Check positions and margin requirements
        print("\n=== Step 5: Positions and Margin ===")
        positions = await bp_api_for_test_env.get_positions()
        
        if positions:
            print(f"Active Positions: {len(positions)}")
            
            # With positions, we should have margin requirements
            if account_summary.total_initial_margin_required is not None:
                print(f"Initial Margin Required: ${account_summary.total_initial_margin_required}")
            if account_summary.total_maintenance_margin_required is not None:
                print(f"Maintenance Margin Required: ${account_summary.total_maintenance_margin_required}")
            
            # Calculate margin ratio
            if (
                account_summary.total_maintenance_margin_required is not None and
                account_summary.total_maintenance_margin_required > 0 and
                account_summary.total_equity > 0
            ):
                margin_ratio = (
                    account_summary.total_maintenance_margin_required / 
                    account_summary.total_equity
                )
                print(f"Margin Ratio: {margin_ratio * 100:.2f}%")
                
                # Validate against reported margin fraction if available
                if bp_details.margin_fraction is not None:
                    fraction_diff = abs(margin_ratio - bp_details.margin_fraction)
                    assert fraction_diff < Decimal("0.1"), (
                        "Large discrepancy between calculated and reported margin fraction"
                    )
        else:
            print("No active positions")
            
            # Without positions, margin requirements should be zero or None
            if account_summary.total_initial_margin_required is not None:
                assert account_summary.total_initial_margin_required == Decimal("0")
            if account_summary.total_maintenance_margin_required is not None:
                assert account_summary.total_maintenance_margin_required == Decimal("0")
        
        # Step 6: Final consistency checks
        print("\n=== Step 6: Final Consistency Checks ===")
        
        # Assets - Liabilities = Net Equity
        if (
            bp_details.assets_value is not None and
            bp_details.liabilities_value is not None
        ):
            calculated_equity = bp_details.assets_value - bp_details.liabilities_value
            equity_diff = abs(calculated_equity - account_summary.total_equity)
            print(f"Calculated Equity: ${calculated_equity}")
            print(f"Reported Equity: ${account_summary.total_equity}")
            assert equity_diff < Decimal("0.01"), "Equity calculation mismatch"
        
        # Available equity consistency
        if bp_details.locked_equity is not None and bp_details.locked_equity > 0:
            expected_available = account_summary.total_equity - bp_details.locked_equity
            available_diff = abs(expected_available - account_summary.available_equity)
            print(f"Expected Available: ${expected_available}")
            print(f"Reported Available: ${account_summary.available_equity}")
            assert available_diff < Decimal("1.0"), "Available equity mismatch"
        
        print("\n✅ All integration checks passed!")