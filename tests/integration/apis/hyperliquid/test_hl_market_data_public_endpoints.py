"""Integration tests for Hyperliquid public endpoints using pytest-recording (VCR).

These tests make real HTTP requests to Hyperliquid's public API endpoints and use
cassette-based recording to avoid repeated network calls while maintaining test reliability.
"""

from typing import Any, TypeGuard, cast

import aiohttp
import pytest

from cyberdelta.config.config_models import ExchangeSpecificConfig

# Now using standardized fixtures from conftest.py:
# - active_hl_config: Environment-aware ExchangeSpecificConfig for Hyperliquid


def _validate_universe_data(obj: object) -> TypeGuard[list[dict[str, Any]]]:
    """Type guard to verify object is a valid universe list."""
    try:
        if not isinstance(obj, list):
            return False
        # Use explicit type checks that pyright accepts
        for item in cast(list[Any], obj):  # type: ignore [redundant-cast]
            if not isinstance(item, dict):
                return False
            if "name" not in cast(dict[str, Any], item):
                return False
        return True
    except (TypeError, AttributeError):
        return False


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

            # Extract universe list and validate structure with TypeGuard
            assert "universe" in meta, "Meta should have universe key"

            # Cast to Any first to avoid Unknown type issues with pyright
            universe_obj = cast(Any, meta["universe"])

            # Direct validation with TypeGuard
            if not _validate_universe_data(universe_obj):
                raise AssertionError("Universe should be a list of dicts with 'name' field")

            # TypeGuard has already validated the structure
            raw_universe = universe_obj

            # Validate data integrity
            assert len(raw_universe) > 0, "Should have at least one asset in universe"
            assert len(asset_ctxs) > 0, "Should have at least one asset context"
            assert len(raw_universe) == len(asset_ctxs), (
                "Universe and asset contexts should have matching lengths"
            )

            # Verify structure of asset context items
            for ctx in asset_ctxs:
                ctx_dict: dict[str, Any] = ctx
                assert isinstance(ctx_dict, dict), "Each asset context should be a dict"
                assert "markPx" in ctx_dict, "Each asset context should have a 'markPx' field"
                assert "funding" in ctx_dict, "Each asset context should have a 'funding' field"

            # Verify common assets are present
            asset_names = [
                item.get("name", "") for item in raw_universe if isinstance(item.get("name"), str)
            ]
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_meta_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with meta type for asset definitions."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "meta"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify meta response structure
            assert isinstance(data, list), "Response should be a list"
            assert len(data) > 0, "Should have at least one asset"

            # Verify asset structure
            for asset in data:
                assert isinstance(asset, dict), "Each asset should be a dict"
                assert "name" in asset, "Each asset should have a 'name' field"
                assert "szDecimals" in asset, "Each asset should have a 'szDecimals' field"

            # Verify common assets are present
            asset_names = [asset["name"] for asset in data]
            common_assets = ["BTC", "ETH", "SOL"]
            found_assets = [asset for asset in common_assets if asset in asset_names]
            assert len(found_assets) > 0, (
                f"Should have at least one common asset from {common_assets}"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_recent_trades_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with recentTrades type."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "recentTrades", "coin": "BTC"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify recentTrades response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are trades, verify their structure
            if len(data) > 0:
                for trade in data:
                    assert isinstance(trade, dict), "Each trade should be a dict"
                    assert "px" in trade, "Each trade should have a 'px' field"
                    assert "sz" in trade, "Each trade should have a 'sz' field"
                    assert "time" in trade, "Each trade should have a 'time' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_candle_snapshot_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with candleSnapshot type for historical data."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Get data for last 24 hours
    end_time = 1640995200  # Fixed timestamp for VCR consistency
    start_time = end_time - 86400  # 24 hours earlier

    payload = {
        "type": "candleSnapshot",
        "coin": "BTC",
        "interval": "1h",
        "startTime": start_time,
        "endTime": end_time,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify candleSnapshot response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are candles, verify their structure
            if len(data) > 0:
                for candle in data:
                    assert isinstance(candle, dict), "Each candle should be a dict"
                    assert "T" in candle, "Each candle should have a 'T' (time) field"
                    assert "o" in candle, "Each candle should have a 'o' (open) field"
                    assert "h" in candle, "Each candle should have a 'h' (high) field"
                    assert "l" in candle, "Each candle should have a 'l' (low) field"
                    assert "c" in candle, "Each candle should have a 'c' (close) field"
                    assert "v" in candle, "Each candle should have a 'v' (volume) field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_funding_history_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with fundingHistory type."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Get funding history for last 24 hours
    end_time = 1640995200  # Fixed timestamp for VCR consistency
    start_time = end_time - 86400  # 24 hours earlier

    payload = {
        "type": "fundingHistory",
        "coin": "BTC",
        "startTime": start_time,
        "endTime": end_time,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify fundingHistory response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are funding records, verify their structure
            if len(data) > 0:
                for funding in data:
                    assert isinstance(funding, dict), "Each funding record should be a dict"
                    assert "coin" in funding, "Each funding record should have a 'coin' field"
                    assert "fundingRate" in funding, (
                        "Each funding record should have a 'fundingRate' field"
                    )
                    assert "time" in funding, "Each funding record should have a 'time' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_spot_meta_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with spotMeta type for spot trading assets."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "spotMeta"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify spotMeta response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are spot assets, verify their structure
            if len(data) > 0:
                for asset in data:
                    assert isinstance(asset, dict), "Each spot asset should be a dict"
                    # Note: Structure may vary, just verify it's a valid dict


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_spot_meta_and_asset_ctxs_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with spotMetaAndAssetCtxs type."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "spotMetaAndAssetCtxs"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[Any] = await response.json()

            # Verify spotMetaAndAssetCtxs response structure
            assert isinstance(data, list), "Response should be a list"
            # Note: Structure may vary between spot meta and contexts


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_clearinghouse_state_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with clearinghouseState type.

    Uses a public address for testing.
    """
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Use a known public address (null address for testing)
    payload = {"type": "clearinghouseState", "user": "0x0000000000000000000000000000000000000000"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify clearinghouseState response structure
            assert isinstance(data, dict), "Response should be a dict"
            # Common fields in clearinghouse state
            if "assetPositions" in data:
                assert isinstance(data["assetPositions"], list), "Asset positions should be a list"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_spot_clearinghouse_state_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with spotClearinghouseState type."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Use a known public address (null address for testing)
    payload = {
        "type": "spotClearinghouseState",
        "user": "0x0000000000000000000000000000000000000000",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify spotClearinghouseState response structure
            assert isinstance(data, dict), "Response should be a dict"
            # Note: May be empty for null address, but should still be valid JSON


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_open_orders_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with openOrders type using a public address."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Use a known public address (null address for testing)
    payload = {"type": "openOrders", "user": "0x0000000000000000000000000000000000000000"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify openOrders response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are orders, verify their structure
            if len(data) > 0:
                for order in data:
                    assert isinstance(order, dict), "Each order should be a dict"
                    assert "coin" in order, "Each order should have a 'coin' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_user_fills_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with userFills type using a public address."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Use a known public address (null address for testing)
    payload = {"type": "userFills", "user": "0x0000000000000000000000000000000000000000"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify userFills response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are fills, verify their structure
            if len(data) > 0:
                for fill in data:
                    assert isinstance(fill, dict), "Each fill should be a dict"
                    assert "coin" in fill, "Each fill should have a 'coin' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_user_funding_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with userFunding type using a public address."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"

    # Use a known public address (null address for testing)
    payload = {"type": "userFunding", "user": "0x0000000000000000000000000000000000000000"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify userFunding response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are funding records, verify their structure
            if len(data) > 0:
                for funding in data:
                    assert isinstance(funding, dict), "Each funding record should be a dict"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hyperliquid_info_perp_dexs_public_endpoint(
    active_hl_config: ExchangeSpecificConfig,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test Hyperliquid's public /info endpoint with perpDexs type for perpetual DEX info."""
    base_url = str(active_hl_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/info"
    payload = {"type": "perpDexs"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify perpDexs response structure
            assert isinstance(data, list), "Response should be a list"

            # If there are DEX entries, verify their structure
            if len(data) > 0:
                for dex in data:
                    assert isinstance(dex, dict), "Each DEX entry should be a dict"
                    # Basic structure validation - actual fields may vary
