"""Integration tests for Backpack public endpoints using pytest-recording (VCR).

These tests make real HTTP requests to Backpack's public API endpoints and use
cassette-based recording to avoid repeated network calls while maintaining test reliability.
"""

from typing import Any

import aiohttp
import pytest

from cyberdelta.config.config_models import ExchangeSpecificConfig

# Using standardized fixtures from conftest.py:
# - active_bp_config: ExchangeSpecificConfig for Backpack


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_markets_endpoint(
    active_bp_config: ExchangeSpecificConfig,
) -> None:
    """Test Backpack's public markets endpoint.

    This test demonstrates VCR usage with a different exchange (Backpack)
    to show cross-exchange compatibility and different API patterns.
    """
    # Use active configuration to get the correct API base URL
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/markets"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify markets response structure
            assert isinstance(data, list), "Response should be a list of markets"
            assert len(data) > 0, "Should have at least one market"

            # Verify market structure
            for market in data:
                assert isinstance(market, dict), "Each market should be a dict"
                assert "symbol" in market, "Each market should have a 'symbol' field"
                assert "orderBookState" in market, (
                    "Each market should have a 'orderBookState' field"
                )
                assert "baseSymbol" in market, "Each market should have a 'baseSymbol' field"
                assert "quoteSymbol" in market, "Each market should have a 'quoteSymbol' field"

            # Verify we have common trading pairs
            symbols = [market["symbol"] for market in data]
            # Common pairs that should exist on Backpack
            expected_pairs = ["SOL_USDC", "BTC_USDC", "ETH_USDC"]
            found_pairs = [pair for pair in expected_pairs if pair in symbols]
            assert len(found_pairs) > 0, (
                f"Should have at least one common pair from {expected_pairs}"
            )
