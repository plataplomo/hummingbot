"""Integration tests for Hyperliquid Spot Market model pipeline.

NOTE: Hyperliquid currently only supports perpetual futures trading and does not
offer spot trading markets. These tests are placeholder implementations that will
be activated when/if Hyperliquid adds spot market support.

Tests would cover:
- Successful spot market metadata retrieval for valid symbols
- Multiple spot markets retrieval and validation
- Edge cases and error handling (non-existent symbols)
- Data type validation and business logic validation
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI


pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/spot/market_data/market"], indirect=True
)
@pytest.mark.spot
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_hl_spot_markets_not_supported_placeholder(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Placeholder test noting that Hyperliquid does not currently support spot markets."""
    # Hyperliquid currently only supports perpetual futures
    # This test serves as a placeholder for future spot market support
    assert True, "Hyperliquid does not currently support spot markets - placeholder test"
