"""Integration tests for Hyperliquid public endpoints using pytest-recording (VCR).

These tests make real HTTP requests to Hyperliquid's public API endpoints and use
cassette-based recording to avoid repeated network calls while maintaining test reliability.
"""

from typing import Any, cast

import aiohttp
import pytest

from cyberdelta.config.config_models import ExchangeSpecificConfig

# Now using standardized fixtures from conftest.py:
# - active_hl_config: Environment-aware ExchangeSpecificConfig for Hyperliquid


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_meta_and_asset_ctxs_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with metaAndAssetCtxs type.

    This test:
    1. Makes a real HTTP request to Hyperliquid's /info endpoint
    2. Sends a POST request with {"type": "metaAndAssetCtxs"} payload
    3. Verifies the response contains valid asset metadata and contexts
    4. Uses VCR to record the HTTP interaction for future test runs

    This is a good candidate for VCR conversion because:
    - It's a public endpoint (no authentication required)
    - It's a simple POST request with a small, stable payload
    - The response structure is relatively stable
    - It demonstrates the basic API functionality
    
    Cassettes are organized in tests/cassettes/apis/hyperliquid/public/.
    """
    # Use configuration system to get the correct API base URL
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "metaAndAssetCtxs"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            # Verify successful response
            assert response.status == 200, f"Expected status 200, got {response.status}"

            # Parse JSON response
            data: list[Any] = await response.json()

            # Verify response structure - Hyperliquid returns a 2-element array
            assert isinstance(data, list), "Response should be a list"
            assert len(data) == 2, "Response should have exactly 2 elements"

            # First element is meta (universe data)
            meta: dict[str, Any] = data[0]
            assert isinstance(meta, dict), "Meta should be a dict"
            assert "universe" in meta, "Meta should have 'universe' key"
            assert isinstance(meta["universe"], list), "Universe should be a list"

            # Second element is asset contexts
            asset_ctxs: list[Any] = data[1]
            assert isinstance(asset_ctxs, list), "Asset contexts should be a list"

            # For a real API call, we expect some assets to be available
            # Extract universe list and validate structure
            assert "universe" in meta, "Meta should have universe key"
            raw_universe_data = cast(list[dict[str, Any]], meta["universe"])
            assert isinstance(raw_universe_data, list), "Universe should be a list"

            # Cast to proper types for type checker compliance
            raw_universe: list[dict[str, Any]] = []
            for universe_item in raw_universe_data:
                assert isinstance(universe_item, dict), "Each universe item should be a dict"
                assert "name" in universe_item, "Each universe item should have a 'name' field"
                raw_universe.append(universe_item)

            # Now we can safely work with the validated data
            assert len(raw_universe) > 0, "Should have at least one asset in universe"
            assert len(asset_ctxs) > 0, "Should have at least one asset context"

            # Verify that the number of universe items matches asset contexts
            assert len(raw_universe) == len(asset_ctxs), (
                "Universe and asset contexts should have matching lengths"
            )

            # Verify structure of asset context items
            # Note: Hyperliquid asset contexts don't have "name" field,
            # they are ordered to match the universe items by index
            for ctx in asset_ctxs:
                ctx_dict: dict[str, Any] = ctx
                assert isinstance(ctx_dict, dict), "Each asset context should be a dict"
                # Check for common fields in asset contexts
                assert "markPx" in ctx_dict, "Each asset context should have a 'markPx' field"
                assert "funding" in ctx_dict, "Each asset context should have a 'funding' field"

            # Verify that common assets like BTC or ETH are present
            asset_names: list[str] = []
            for item in raw_universe:
                asset_name = item.get("name", "")
                if isinstance(asset_name, str):
                    asset_names.append(asset_name)
            assert any(name in ["BTC", "ETH", "SOL"] for name in asset_names), (
                "Should have at least one common asset like BTC, ETH, or SOL"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/demo/filtering"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_vcr_sensitive_data_filtering_demo(custom_vcr_config: dict[str, Any]) -> None:
    """Demonstration test for VCR sensitive data filtering capabilities.

    This test shows how VCR filters sensitive headers and query parameters
    while preserving functional test data. It makes a request to httpbin.org
    which echoes back the request headers, allowing us to verify filtering works.
    
    Cassettes are organized in tests/cassettes/apis/demo/filtering/.
    """
    # Test URL that echoes back request data
    url = "https://httpbin.org/anything"

    # Simulate sensitive headers that should be filtered
    sensitive_headers = {
        "X-API-Key": "secret_api_key_12345",
        "X-Signature": "hmac_signature_abcdef",
        "X-Timestamp": "1640995200",
        "Authorization": "Bearer secret_token_xyz",
        "X-BP-API-Key": "backpack_secret_key",
        "User-Agent": "Custom-Agent/1.0",  # Should be normalized
    }

    # Simulate sensitive query parameters
    sensitive_params = {
        "api_key": "query_secret_key",
        "signature": "query_signature_123",
        "timestamp": "1640995200",
        "user_id": "sensitive_user_123",
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=sensitive_headers, params=sensitive_params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify the service received our request
            assert "headers" in data, "Response should contain headers"
            assert "args" in data, "Response should contain query args"

            # The actual filtering verification will be done by examining the
            # generated cassette file, but the test itself should pass normally
            assert data["url"] is not None, "URL should be present"

            # Note: The sensitive data filtering happens at the VCR level
            # when recording cassettes, not in the actual HTTP response.
            # The filtering protects against leaking credentials in test files.


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_l2_book_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with l2Book type for order book data.

    This test demonstrates VCR usage with a different endpoint that returns
    order book data. Shows how VCR works with various API response structures.
    
    Cassettes are organized in tests/cassettes/apis/hyperliquid/public/.
    """
    # Use configuration system to get the correct API base URL
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "l2Book", "coin": "BTC"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify l2Book response structure
            assert isinstance(data, dict), "Response should be a dict"
            assert "levels" in data, "Response should have 'levels' field"
            assert "time" in data, "Response should have 'time' field"

            # Verify levels structure (bids and asks)
            levels_list: list[Any] = data["levels"]
            assert isinstance(levels_list, list), "Levels should be a list"
            assert len(levels_list) == 2, "Should have 2 levels (bids and asks)"

            # Verify bids and asks are lists
            bids: list[Any] = data["levels"][0]
            asks: list[Any] = data["levels"][1]
            assert isinstance(bids, list), "Bids should be a list"
            assert isinstance(asks, list), "Asks should be a list"

            # For a liquid market like BTC, we expect some orders
            assert len(bids) > 0, "Should have at least one bid"
            assert len(asks) > 0, "Should have at least one ask"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_all_mids_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with allMids type for mid prices.

    This test demonstrates VCR with yet another endpoint format,
    showing how the same infrastructure handles different data types.
    
    Cassettes are organized in tests/cassettes/apis/hyperliquid/public/.
    """
    # Use configuration system to get the correct API base URL
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "allMids"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify allMids response structure
            # Note: Hyperliquid's allMids endpoint returns a dict where keys are asset indices
            # (@1, @2, etc.) and symbol names (BTC, ETH, etc.), and values are price strings
            assert isinstance(data, dict), "Response should be a dict"

            # Verify we have mid prices for assets
            assert len(data) > 0, "Should have mid prices for at least one asset"

            # Verify mid price format (should be string representations of numbers)
            for symbol, price in data.items():
                assert isinstance(symbol, str), f"Symbol {symbol} should be a string"
                assert isinstance(price, str), f"Price for {symbol} should be a string"
                # Verify it's a valid number
                float(price)  # Should not raise an exception

            # Verify some common assets are present
            common_assets = ["BTC", "ETH", "SOL"]
            found_assets = [asset for asset in common_assets if asset in data]
            assert len(found_assets) > 0, (
                f"Should have at least one common asset from {common_assets}"
            )
