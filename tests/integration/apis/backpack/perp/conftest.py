"""Perpetual trading specific fixtures for Backpack integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI


@pytest.fixture
async def bp_perp_test_config(
    bp_api_for_test_env: BackpackAPI,
) -> dict[str, list[str] | list[Decimal] | Decimal | int]:
    """Backpack perp test configuration based on real market data.

    This fixture dynamically fetches actual market constraints instead of using
    hardcoded values to ensure tests align with real exchange behavior.
    """
    # Import here to avoid circular imports
    from tests.integration.apis.backpack.shared.bp_test_helpers import get_market_constraints

    # Get available perp markets dynamically
    try:
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await bp_api_for_test_env.get_markets(args=GetMarketsArgs())
        perp_symbols = [m.symbol for m in markets if "_PERP" in m.symbol.upper()][:3]

        if not perp_symbols:
            # Fallback to common symbols if no markets available
            perp_symbols = ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"]

        # Get actual constraints from first available perp market
        constraints = await get_market_constraints(bp_api_for_test_env, perp_symbols[0])
        min_quantity = constraints.get("min_quantity", constraints["step_size"])
        max_leverage = constraints.get("max_leverage", Decimal(100))

        return {
            "symbols": perp_symbols,
            "min_position_size": min_quantity,
            "test_sizes": [
                min_quantity,  # Minimum size
                min_quantity * Decimal(10),  # 10x minimum
                min_quantity * Decimal(100),  # 100x minimum
            ],
            "max_leverage": int(max_leverage),
        }

    except Exception:
        # If dynamic fetching fails, skip tests that depend on this fixture
        pytest.skip("Unable to fetch real market constraints for perp test configuration")
