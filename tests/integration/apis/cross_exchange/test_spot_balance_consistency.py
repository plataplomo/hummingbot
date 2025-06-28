"""Cross-exchange spot balance consistency tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.cross_exchange, pytest.mark.spot]


class TestCrossExchangeSpotBalanceConsistency:
    """Cross-exchange spot balance consistency tests."""

    @pytest.mark.skip(reason="Hyperliquid spot business logic isn't ready yet")
    @pytest.mark.parametrize(
        ("symbol", "expected_precision"),
        [
            ("SOL", 8),
            ("BTC", 8),
            ("ETH", 8),
            ("USDC", 6),
        ],
    )
    @pytest.mark.asyncio
    async def test_spot_balance_precision(
        self,
        exchange_client: BackpackAPI | HyperliquidAPI,
        symbol: str,
        expected_precision: int,
    ) -> None:
        """Test spot balance precision across exchanges and symbols."""
        try:
            balances = await exchange_client.get_balances()

            if symbol in balances:
                balance = balances[symbol]
                assert_valid_spot_balance(balance)

                # Verify precision handling
                total_str = str(balance.total_quantity)
                if "." in total_str:
                    decimal_places = len(total_str.split(".")[1].rstrip("0"))
                    assert decimal_places <= expected_precision

                available_str = str(balance.available_quantity)
                if "." in available_str:
                    decimal_places = len(available_str.split(".")[1].rstrip("0"))
                    assert decimal_places <= expected_precision
            else:
                pytest.skip(f"Symbol {symbol} not available on {exchange_client.exchange_name}")

        except NotImplementedError:
            pytest.skip(f"get_balances not implemented for {exchange_client.exchange_name}")

    @pytest.mark.skip(reason="Hyperliquid spot business logic isn't ready yet")
    @pytest.mark.parametrize(
        "test_amount",
        [
            pytest.param(Decimal("0.00000001"), id="dust"),
            pytest.param(Decimal("0.1"), id="small"),
            pytest.param(Decimal(100), id="normal"),
            pytest.param(Decimal("999999.99"), id="large"),
        ],
    )
    @pytest.mark.asyncio
    async def test_balance_amount_handling(
        self,
        exchange_client: BackpackAPI | HyperliquidAPI,
        test_amount: Decimal,
    ) -> None:
        """Test balance amount handling across different scales."""
        try:
            balances = await exchange_client.get_balances()

            for balance in balances.values():
                assert_valid_spot_balance(balance)

                # Test arithmetic operations with test amounts
                test_total = balance.total_quantity + test_amount
                assert test_total.is_finite()
                assert test_total >= balance.total_quantity

                # Test precision preservation
                if balance.total_quantity > Decimal(0):
                    ratio = test_amount / balance.total_quantity
                    assert ratio.is_finite()

        except NotImplementedError:
            pytest.skip(f"get_balances not implemented for {exchange_client.exchange_name}")
