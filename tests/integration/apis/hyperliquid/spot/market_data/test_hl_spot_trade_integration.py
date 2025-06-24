"""Integration tests for Hyperliquid Spot Trade model pipeline.

NOTE: Hyperliquid currently only supports perpetual futures trading and does not
offer spot trading markets. These tests are placeholder implementations that will
be activated when/if Hyperliquid adds spot market support.
"""

from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI


pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/spot/market_data/trade"], indirect=True
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_spot_trades_not_supported_placeholder(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Placeholder test noting that Hyperliquid does not currently support spot markets."""
    assert True, "Hyperliquid does not currently support spot markets - placeholder test"
