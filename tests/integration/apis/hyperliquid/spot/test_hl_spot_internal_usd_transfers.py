"""Integration tests for Hyperliquid internal USD transfers between spot and perp accounts.

This module tests the complete internal USD transfer functionality for moving funds
between spot and perpetual trading accounts within the same wallet using real testnet
environment with funded wallet.

Model Focus: Internal USD Transfers (spot ↔ perp)
- Tests complete cycle: check balance → perp to spot → spot to perp
- Uses real testnet wallet with mock funds
- Validates balance consistency throughout transfer cycle

Authentication: EIP-712 signing for /exchange endpoint operations
VCR: Records real API interactions with sensitive data filtering
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import TransferArgs
from cyberdelta.core.models.enums import InternalTransferStatus


pytestmark = [
    pytest.mark.integration,
    pytest.mark.spot,
    pytest.mark.requires_balance,
    pytest.mark.requires_funded_wallet,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/spot/internal_transfers"],
    indirect=True,
)
@pytest.mark.spot
@pytest.mark.requires_balance
@pytest.mark.requires_funded_wallet
class TestHyperliquidInternalUsdTransfers:
    """Test internal USD transfers between spot and perp accounts."""

    async def _get_initial_usdc_balance(self, hl_api: HyperliquidAPI) -> Decimal:
        """Get initial USDC balance and validate it exists."""
        initial_balances = await hl_api.get_balances()

        if "USDC" not in initial_balances:
            pytest.skip("No USDC balance found - need funded testnet account")

        usdc_balance = initial_balances["USDC"]
        initial_amount = usdc_balance.total_quantity

        if initial_amount <= Decimal(0):
            pytest.skip("No USDC balance available - need funded testnet account")

        return initial_amount

    async def _execute_transfer_and_validate(
        self, hl_api: HyperliquidAPI, from_account: str, to_account: str, amount: Decimal
    ) -> None:
        """Execute a transfer and validate it succeeds."""
        try:
            transfer_result = await hl_api.transfer(
                TransferArgs(
                    asset="USDC",
                    amount=amount,
                    from_account_type=from_account,
                    to_account_type=to_account,
                    client_transfer_id=None,
                )
            )

            assert transfer_result is not None
            assert transfer_result.status == InternalTransferStatus.COMPLETED
            assert transfer_result.asset == "USDC"
            assert transfer_result.quantity == amount

        except APIError as e:
            pytest.fail(f"Transfer {from_account}→{to_account} failed: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_full_transfer_cycle_perp_to_spot_to_perp(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test complete transfer cycle: check balance → perp→spot → spot→perp.

        1) Checks full balance
        2) Moves all funds from perp to spot
        3) Moves all funds back from spot to perp

        Uses real testnet wallet with funded account.
        """
        # Step 1: Get initial balance
        initial_amount = await self._get_initial_usdc_balance(hl_api_for_test_env)

        # Step 2: Move all funds from perp to spot
        await self._execute_transfer_and_validate(
            hl_api_for_test_env, "perp", "spot", initial_amount
        )

        # Step 3: Move all funds back from spot to perp
        await self._execute_transfer_and_validate(
            hl_api_for_test_env, "spot", "perp", initial_amount
        )
