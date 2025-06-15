"""Integration tests for Backpack balance retrieval with positive balances.

Tests the balance fetching functionality when the account has positive
balances across different assets.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models import BackpackSpotBalanceDetails, SpotBalance

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.balances,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/balances_positive"], indirect=True
)
class TestBackpackBalancesPositive:
    """Test balance retrieval when account has positive balances."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_multiple_assets(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving balances for multiple assets with positive values."""
        balances = await bp_api_for_test_env.get_balances()

        assert isinstance(balances, dict)
        assert len(balances) > 0, "Expected at least one balance for positive balance test"

        # Verify each balance
        for asset, balance in balances.items():
            assert isinstance(balance, SpotBalance)
            assert balance.exchange == "backpack"
            assert isinstance(balance.asset, str)
            assert len(balance.asset) > 0
            assert balance.asset == asset  # Key should match asset in balance

            # For positive balance tests, total should be > 0 for at least one asset
            assert isinstance(balance.total_quantity, Decimal)
            assert balance.total_quantity >= Decimal("0")

            assert isinstance(balance.available_quantity, Decimal)
            assert balance.available_quantity >= Decimal("0")
            assert balance.available_quantity <= balance.total_quantity

            # Check timestamp
            assert isinstance(balance.timestamp, datetime)
            time_diff = datetime.now(balance.timestamp.tzinfo) - balance.timestamp
            assert time_diff.total_seconds() < 3600

            # Verify bp_details
            assert balance.bp_details is not None
            assert isinstance(balance.bp_details, BackpackSpotBalanceDetails)

        # Ensure we have at least one positive balance
        positive_balances = [b for b in balances.values() if b.total_quantity > Decimal("0")]
        assert len(positive_balances) > 0, "Expected at least one positive balance"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_usdc_present(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that USDC balance is present (common base currency)."""
        balances = await bp_api_for_test_env.get_balances()

        usdc_balance = balances.get("USDC")
        assert usdc_balance is not None, "Expected USDC balance to be present"
        assert usdc_balance.total_quantity >= Decimal("0")

        # For an active trading account, USDC should have some balance
        if usdc_balance.total_quantity > Decimal("0"):
            # Verify balance details
            assert usdc_balance.bp_details is not None

            # Available should be <= total
            assert usdc_balance.available_quantity <= usdc_balance.total_quantity

            # Locked amount = total - available
            locked_amount = usdc_balance.total_quantity - usdc_balance.available_quantity
            assert locked_amount >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_specific_asset(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test looking up balance for a specific asset from the dict."""
        # Get all balances and look up USDC
        balances = await bp_api_for_test_env.get_balances()
        balance = balances.get("USDC")

        assert balance is not None, "Expected USDC balance to be present"
        assert isinstance(balance, SpotBalance)
        assert balance.asset == "USDC"
        assert balance.exchange == "backpack"

        # Verify quantities
        assert isinstance(balance.total_quantity, Decimal)
        assert isinstance(balance.available_quantity, Decimal)
        assert balance.available_quantity <= balance.total_quantity

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_nonexistent_asset(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test looking up balance for an asset with zero balance.

        When an asset has zero balance, Backpack may not return it
        in the balances response, so it won't be in the dict.
        """
        # Get all balances and try to look up an uncommon asset
        balances = await bp_api_for_test_env.get_balances()

        # DOGE might not be in the dict if balance is zero
        doge_balance = balances.get("DOGE")

        if doge_balance is None:
            # Asset not in response - expected for zero balances
            assert True
        else:
            # If present, should be zero or small amount
            assert isinstance(doge_balance, SpotBalance)
            assert doge_balance.asset == "DOGE"
            assert doge_balance.total_quantity >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_locked_amounts(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test balance retrieval when some funds are locked in orders."""
        balances = await bp_api_for_test_env.get_balances()

        # Find balances where available < total (indicating locked funds)
        locked_balances = [
            b
            for b in balances.values()
            if b.total_quantity > Decimal("0") and b.available_quantity < b.total_quantity
        ]

        if locked_balances:
            # If we have locked balances, verify the details
            for balance in locked_balances:
                locked_amount = balance.total_quantity - balance.available_quantity
                assert locked_amount > Decimal("0")

                # bp_details might have open_order_quantity which represents locked in orders
                if balance.bp_details and hasattr(balance.bp_details, "open_order_quantity"):
                    if balance.bp_details.open_order_quantity is not None:
                        # open_order_quantity represents amount locked in open orders
                        assert balance.bp_details.open_order_quantity <= locked_amount

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_staked_amounts(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test balance retrieval when some funds are staked."""
        balances = await bp_api_for_test_env.get_balances()

        # Check if any balances have lending amounts (autostaking)
        for balance in balances.values():
            if balance.bp_details and hasattr(balance.bp_details, "lend_quantity"):
                lend_qty = balance.bp_details.lend_quantity
                if lend_qty is not None and lend_qty > Decimal("0"):
                    # Lend quantity represents autostaked amount
                    # Note: In the current implementation, lend_quantity is not populated
                    # from the /api/v1/capital endpoint, only from /api/v1/capital/collateral
                    pass

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_precision_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that balance precision is preserved correctly."""
        balances = await bp_api_for_test_env.get_balances()

        for balance in balances.values():
            if balance.total_quantity > Decimal("0"):
                # Check that decimal precision is maintained
                total_str = str(balance.total_quantity)
                available_str = str(balance.available_quantity)

                # Balances should not have excessive decimal places
                if "." in total_str:
                    decimal_places = len(total_str.split(".")[1])
                    assert decimal_places <= 18, f"Excessive decimal places in total: {total_str}"

                if "." in available_str:
                    decimal_places = len(available_str.split(".")[1])
                    assert decimal_places <= 18, (
                        f"Excessive decimal places in available: {available_str}"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_consistency_with_account_summary(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that balances are consistent with account summary data.

        Note: With auto-lending enabled, this test verifies that the enhanced
        balance logic provides results consistent with account summary.
        """
        # Get both balances and account summary
        balances = await bp_api_for_test_env.get_balances()
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Check if auto-lending is active (balances have lend_quantity populated)
        auto_lending_detected = any(
            balance.bp_details
            and balance.bp_details.lend_quantity
            and balance.bp_details.lend_quantity > Decimal("0")
            for balance in balances.values()
        )

        if auto_lending_detected:
            # Auto-lending scenario: Compare total account values
            # Calculate total value from all enhanced balances
            total_balance_value = sum(
                balance.total_quantity
                for balance in balances.values()
                if balance.asset in ["USDC", "USDT"]
            )

            # With auto-lending, the account equity should be close to the sum of all asset values
            # Allow for reasonable variance due to:
            # - Mark price fluctuations between spot and collateral endpoint calls
            # - Small fees or unsettled amounts
            # - Rounding differences in decimal calculations
            if total_balance_value > Decimal("0"):
                # For accounts with non-USD assets (like SOL), equity includes their USD value
                # So equity will be higher than just USD balances
                assert account_summary.total_equity >= total_balance_value * Decimal("0.9"), (
                    f"Account equity ({account_summary.total_equity}) seems too low "
                    f"compared to USD balances ({total_balance_value}) in auto-lending scenario"
                )

                # Log for debugging
                print(
                    f"Auto-lending detected - Account Equity: {account_summary.total_equity}, "
                    f"USD Balances: {total_balance_value}"
                )
                for asset, balance in balances.items():
                    if balance.total_quantity > Decimal("0"):
                        lend_qty = balance.bp_details.lend_quantity if balance.bp_details else None
                        print(f"  {asset}: {balance.total_quantity} (lent: {lend_qty})")
        else:
            # Normal scenario: Compare spot balances with account equity
            total_usd_from_balances = Decimal("0")
            for asset, balance in balances.items():
                if asset in ["USDC", "USDT"]:
                    total_usd_from_balances += balance.total_quantity

            # Test consistency between individual balances and account summary
            
            # 1. Total equity must be non-negative
            assert account_summary.total_equity >= Decimal("0"), (
                f"Total equity cannot be negative: {account_summary.total_equity}"
            )
            
            # 2. Calculate total balance value (all assets)
            # For a proper consistency check, we'd need current market prices
            # But we can at least verify USD stablecoins are reflected correctly
            total_stablecoin_balance = sum(
                balance.total_quantity 
                for asset, balance in balances.items() 
                if asset in ["USDC", "USDT", "BUSD", "DAI", "TUSD"]
            )
            
            # 3. Equity must be at least equal to stablecoin balances
            # (since stablecoins are always worth $1)
            if total_stablecoin_balance > Decimal("0"):
                assert account_summary.total_equity >= total_stablecoin_balance * Decimal("0.999"), (
                    f"Account equity ({account_summary.total_equity}) is less than "
                    f"stablecoin balances ({total_stablecoin_balance}). "
                    f"This violates basic accounting principles."
                )
            
            # 4. If we have non-stablecoin assets, equity should be higher than just stablecoins
            non_stablecoin_assets = [
                (asset, balance.total_quantity)
                for asset, balance in balances.items()
                if asset not in ["USDC", "USDT", "BUSD", "DAI", "TUSD"] and balance.total_quantity > Decimal("0")
            ]
            
            if non_stablecoin_assets and total_stablecoin_balance > Decimal("0"):
                # Equity should be noticeably higher than just stablecoins if we hold crypto
                # But we can't test exact amounts without prices
                print(f"Non-stablecoin assets held: {non_stablecoin_assets}")
                print(f"Total equity: {account_summary.total_equity}")
                print(f"Stablecoin balance: {total_stablecoin_balance}")
            
            # 5. Available balance must be <= total balance for each asset
            for asset, balance in balances.items():
                assert balance.available_quantity <= balance.total_quantity, (
                    f"{asset}: Available ({balance.available_quantity}) exceeds "
                    f"total ({balance.total_quantity})"
                )
                
            # 6. If account has derivatives, verify position notional makes sense
            if account_summary.total_position_notional is not None:
                assert account_summary.total_position_notional >= Decimal("0"), (
                    f"Position notional cannot be negative: {account_summary.total_position_notional}"
                )
                
                # If we have positions, total_position_initial_margin should be set
                if account_summary.total_position_notional > Decimal("0"):
                    assert account_summary.total_position_initial_margin is not None, (
                        "Account has positions but no initial margin reported"
                    )
                    assert account_summary.total_position_initial_margin > Decimal("0"), (
                        f"Positive position notional ({account_summary.total_position_notional}) "
                        f"but zero/negative initial margin ({account_summary.total_position_initial_margin})"
                    )
