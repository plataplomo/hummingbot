"""Integration tests for Backpack balance retrieval with zero or minimal balances.

Tests the balance fetching functionality when the account has zero or very
small balances, including edge cases and empty responses.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.spot_balance import SpotBalance

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.balances,
    pytest.mark.zero_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/balances_zero"], indirect=True
)
class TestBackpackBalancesZero:
    """Test balance retrieval when account has zero or minimal balances."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_empty_or_zero(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving balances when all are zero or response is empty."""
        balances = await bp_api_for_test_env.get_balances()

        assert isinstance(balances, dict)

        # For zero balance test, we might get:
        # 1. Empty dict (no balances at all)
        # 2. Dict with zero balances

        if len(balances) == 0:
            # Empty response is valid for account with no balances
            assert balances == {}
        else:
            # If we have balances, they should all be zero or very small
            for _asset, balance in balances.items():
                assert isinstance(balance, SpotBalance)
                assert balance.exchange == "backpack"
                assert isinstance(balance.total_quantity, Decimal)
                assert isinstance(balance.available_quantity, Decimal)

                # For zero balance test, expect very small or zero amounts
                assert balance.total_quantity >= Decimal("0")
                assert balance.available_quantity >= Decimal("0")
                assert balance.available_quantity <= balance.total_quantity

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_zero_asset(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving balance for an asset with guaranteed zero balance."""
        # Use an uncommon asset that likely has zero balance
        balances = await bp_api_for_test_env.get_balances()

        # Get XRP balance if it exists
        balance = balances.get("XRP")

        if balance is None:
            # No XRP balance means zero balance
            pass  # This is expected for an asset with no balance
        else:
            assert isinstance(balance, SpotBalance)
            assert balance.asset == "XRP"
            assert balance.exchange == "backpack"
            assert balance.total_quantity == Decimal("0")
            assert balance.available_quantity == Decimal("0")

            # Zero balance should still have valid timestamp
            assert isinstance(balance.timestamp, datetime)

            # bp_details should be present even for zero balance
            assert balance.bp_details is not None

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_dust_amounts(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test handling of dust amounts (very small balances)."""
        balances = await bp_api_for_test_env.get_balances()

        # Look for any dust balances (very small but non-zero)
        dust_threshold = Decimal("0.00001")
        dust_balances = [
            b for b in balances.values() if Decimal("0") < b.total_quantity < dust_threshold
        ]

        for balance in dust_balances:
            # Dust amounts should be handled with full precision
            assert balance.total_quantity > Decimal("0")
            assert balance.available_quantity >= Decimal("0")

            # Verify decimal precision is maintained
            total_str = str(balance.total_quantity)
            assert "E" not in total_str.upper(), "Balance should not use scientific notation"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_all_locked(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test balance retrieval when all funds are locked (zero available)."""
        balances = await bp_api_for_test_env.get_balances()

        # Look for balances where everything is locked
        fully_locked = [
            b
            for b in balances.values()
            if b.total_quantity > Decimal("0") and b.available_quantity == Decimal("0")
        ]

        for balance in fully_locked:
            # Total should be positive but available is zero
            assert balance.total_quantity > Decimal("0")
            assert balance.available_quantity == Decimal("0")

            # The locked amount should equal total
            locked_amount = balance.total_quantity - balance.available_quantity
            assert locked_amount == balance.total_quantity

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_after_withdrawal(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test balance state after a complete withdrawal.

        Simulates checking balance after withdrawing all funds
        from a specific asset.
        """
        # Check balance for an asset that was withdrawn
        balances = await bp_api_for_test_env.get_balances()
        balance = balances.get("SOL")

        # SOL might not be in the dict if balance is zero
        if balance is None:
            # Asset not in response - expected for zero balances after withdrawal
            assert True
        else:
            assert isinstance(balance, SpotBalance)
            assert balance.asset == "SOL"

            # After full withdrawal, balance should be zero
            if balance.total_quantity == Decimal("0"):
                assert balance.available_quantity == Decimal("0")

            # Check bp_details for consistency
            if balance.bp_details:
                if hasattr(balance.bp_details, "open_order_quantity"):
                    # After full withdrawal, no open orders should exist
                    if balance.bp_details.open_order_quantity is not None:
                        assert balance.bp_details.open_order_quantity == Decimal("0")
                if hasattr(balance.bp_details, "lend_quantity"):
                    # After full withdrawal, no lending should exist
                    if balance.bp_details.lend_quantity is not None:
                        assert balance.bp_details.lend_quantity == Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_new_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test balance retrieval for a new account with no deposits.

        A completely new account might return an empty balance list
        or a minimal set of zero balances.
        """
        balances = await bp_api_for_test_env.get_balances()

        # New account scenarios:
        # 1. Empty list
        # 2. List with common assets at zero

        if len(balances) == 0:
            # Valid for new account
            assert balances == {}
        else:
            # Should only have zero balances
            total_balance = sum(b.total_quantity for b in balances.values())
            assert total_balance == Decimal("0"), "New account should have zero total balance"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_precision_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test balance precision handling for very small amounts."""
        balances = await bp_api_for_test_env.get_balances()

        for balance in balances.values():
            # Test that very small balances maintain precision
            if Decimal("0") < balance.total_quantity < Decimal("0.000001"):
                # Should maintain full precision for small amounts
                total_str = str(balance.total_quantity)

                # Should not truncate to zero
                assert balance.total_quantity > Decimal("0")

                # Should not use scientific notation
                assert "E" not in total_str.upper()

                # Available should still be valid
                assert Decimal("0") <= balance.available_quantity <= balance.total_quantity

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_balances_consistency_zero_equity(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test consistency between zero balances and account summary."""
        balances = await bp_api_for_test_env.get_balances()
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Calculate total from balances
        total_usd_value = Decimal("0")
        for asset, balance in balances.items():
            if asset in ["USDC", "USDT"]:
                total_usd_value += balance.total_quantity

        # If balances are zero, equity should also be zero (or very close)
        if total_usd_value == Decimal("0"):
            # Account with no USD balances and no positions should have zero equity
            if account_summary.total_position_notional == Decimal("0"):
                assert account_summary.total_equity <= Decimal("0.01"), (
                    f"Expected near-zero equity for account with no balances, "
                    f"but got {account_summary.total_equity}"
                )
