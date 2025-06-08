"""Integration tests for Backpack public endpoints using pytest-recording (VCR).

These tests make real HTTP requests to Backpack's public API endpoints and use
cassette-based recording to avoid repeated network calls while maintaining test reliability.
"""

from typing import Any, TypeGuard, cast

import aiohttp
import pytest

from cyberdelta.config.config_models import ExchangeSpecificConfig

# Using standardized fixtures from conftest.py:
# - active_bp_config: ExchangeSpecificConfig for Backpack


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_markets_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,  # Accept the parametrized fixture
) -> None:
    """Test Backpack's public markets endpoint.

    This test demonstrates VCR usage with a different exchange (Backpack)
    to show cross-exchange compatibility and different API patterns.
    Cassettes are organized in tests/cassettes/apis/backpack/public/.
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


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_ping_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public ping endpoint for health check."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/ping"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            # Ping endpoint returns plain text "pong"
            text_data = await response.text()
            assert text_data.strip().lower() == "pong", "Ping should return 'pong'"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_time_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public time endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/time"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            # Time endpoint returns a timestamp string
            text_data = await response.text()
            assert text_data.strip(), "Time endpoint should return non-empty timestamp"
            # Verify it's a valid integer timestamp
            int(text_data.strip())  # Should not raise an exception


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_status_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public status endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/status"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify status response structure
            assert isinstance(data, dict), "Response should be a dict"
            # Status endpoint typically has status and message fields
            assert "status" in data, "Status response should have a 'status' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_assets_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public assets endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/assets"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify assets response structure
            assert isinstance(data, list), "Response should be a list of assets"
            assert len(data) > 0, "Should have at least one asset"

            # Verify asset structure
            for asset in data:
                assert isinstance(asset, dict), "Each asset should be a dict"
                assert "symbol" in asset, "Each asset should have a 'symbol' field"

            # Verify common assets are present
            symbols = [asset["symbol"] for asset in data]
            common_assets = ["USDC", "BTC", "ETH", "SOL"]
            found_assets = [asset for asset in common_assets if asset in symbols]
            assert len(found_assets) > 0, (
                f"Should have at least one common asset from {common_assets}"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_collateral_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public collateral endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/collateral"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify collateral response structure
            assert isinstance(data, list), "Response should be a list of collateral info"

            # If there are collateral assets, verify their structure
            if len(data) > 0:
                for collateral in data:
                    assert isinstance(collateral, dict), "Each collateral should be a dict"
                    assert "symbol" in collateral, "Each collateral should have a 'symbol' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_ticker_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public ticker endpoint for a specific symbol."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/ticker"
    params = {"symbol": "SOL_USDC"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify ticker response structure
            assert isinstance(data, dict), "Response should be a dict"
            assert "symbol" in data, "Ticker should have a 'symbol' field"
            assert "lastPrice" in data, "Ticker should have a 'lastPrice' field"
            assert "volume" in data, "Ticker should have a 'volume' field"
            assert data["symbol"] == "SOL_USDC", "Symbol should match the requested symbol"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_tickers_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public tickers endpoint for all symbols."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/tickers"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify tickers response structure
            assert isinstance(data, list), "Response should be a list of tickers"
            assert len(data) > 0, "Should have at least one ticker"

            # Verify ticker structure
            for ticker in data:
                assert isinstance(ticker, dict), "Each ticker should be a dict"
                assert "symbol" in ticker, "Each ticker should have a 'symbol' field"
                assert "lastPrice" in ticker, "Each ticker should have a 'lastPrice' field"

            # Verify common symbols are present
            symbols = [ticker["symbol"] for ticker in data]
            common_symbols = ["SOL_USDC", "BTC_USDC", "ETH_USDC"]
            found_symbols = [symbol for symbol in common_symbols if symbol in symbols]
            assert len(found_symbols) > 0, (
                f"Should have at least one common symbol from {common_symbols}"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_depth_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public depth (order book) endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/depth"
    params = {"symbol": "SOL_USDC"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify depth response structure
            assert isinstance(data, dict), "Response should be a dict"
            assert "bids" in data, "Depth should have a 'bids' field"
            assert "asks" in data, "Depth should have an 'asks' field"
            assert "timestamp" in data, "Depth should have a 'timestamp' field"

            # Type guard for order book data - explicit Any parameters to avoid Unknown
            def is_valid_order_book_side(obj: object) -> TypeGuard[list[list[str]]]:
                """Type guard to verify object is a valid order book side."""
                try:
                    if not isinstance(obj, list):
                        return False
                    # Use explicit type checks that pyright accepts
                    for item in cast(list[Any], obj):  # type: ignore [redundant-cast]
                        if not isinstance(item, list):
                            return False
                        if len(cast(list[Any], item)) != 2:  # type: ignore [redundant-cast]
                            return False
                    return True
                except (TypeError, AttributeError):
                    return False

            # Verify bids and asks structure with TypeGuard + cast pattern
            bids_data = data.get("bids", [])
            asks_data = data.get("asks", [])

            # Use TypeGuard to validate - after validation, safe to use
            if not is_valid_order_book_side(bids_data):
                raise AssertionError("Bids should be a list of [price, quantity] pairs")

            if not is_valid_order_book_side(asks_data):
                raise AssertionError("Asks should be a list of [price, quantity] pairs")

            # After TypeGuard validation, we know the structure is correct


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_trades_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public trades endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/trades"
    params: dict[str, str | int] = {"symbol": "SOL_USDC", "limit": 50}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify trades response structure
            assert isinstance(data, list), "Response should be a list of trades"

            # If there are trades, verify their structure
            if len(data) > 0:
                for trade in data:
                    assert isinstance(trade, dict), "Each trade should be a dict"
                    assert "id" in trade, "Each trade should have an 'id' field"
                    assert "price" in trade, "Each trade should have a 'price' field"
                    assert "quantity" in trade, "Each trade should have a 'quantity' field"
                    assert "timestamp" in trade, "Each trade should have a 'timestamp' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_klines_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public klines (candlestick) endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/klines"

    # Use fixed timestamps for VCR consistency
    start_time = 1640995200  # Fixed timestamp
    end_time = start_time + 3600  # 1 hour later

    params: dict[str, str | int] = {
        "symbol": "SOL_USDC",
        "interval": "1h",
        "startTime": start_time,
        "endTime": end_time,
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[list[Any]] = await response.json()

            # Verify klines response structure
            assert isinstance(data, list), "Response should be a list of klines"

            # If there are klines, verify their structure
            if len(data) > 0:
                for kline in data:
                    assert isinstance(kline, list), "Each kline should be a list"
                    assert len(kline) >= 6, (
                        "Each kline should have at least 6 elements "
                        "[timestamp, open, high, low, close, volume]"
                    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_funding_rates_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public funding rates endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/fundingRates"
    params: dict[str, str | int] = {"symbol": "SOL_USDC_PERP", "limit": 10}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify funding rates response structure
            assert isinstance(data, list), "Response should be a list of funding rates"

            # If there are funding rates, verify their structure
            if len(data) > 0:
                for rate in data:
                    assert isinstance(rate, dict), "Each funding rate should be a dict"
                    assert "symbol" in rate, "Each funding rate should have a 'symbol' field"
                    assert "fundingRate" in rate, (
                        "Each funding rate should have a 'fundingRate' field"
                    )
                    assert "timestamp" in rate, "Each funding rate should have a 'timestamp' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_mark_prices_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public mark prices endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/markPrices"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify mark prices response structure
            assert isinstance(data, list), "Response should be a list of mark prices"

            # If there are mark prices, verify their structure
            if len(data) > 0:
                for price in data:
                    assert isinstance(price, dict), "Each mark price should be a dict"
                    assert "symbol" in price, "Each mark price should have a 'symbol' field"
                    assert "markPrice" in price, "Each mark price should have a 'markPrice' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_open_interest_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public open interest endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/openInterest"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify open interest response structure
            assert isinstance(data, list), "Response should be a list of open interest data"

            # If there are open interest records, verify their structure
            if len(data) > 0:
                for oi in data:
                    assert isinstance(oi, dict), "Each open interest record should be a dict"
                    assert "symbol" in oi, "Each open interest record should have a 'symbol' field"
                    assert "openInterest" in oi, (
                        "Each open interest record should have an 'openInterest' field"
                    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_market_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public market endpoint for a specific symbol."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/market"
    params = {"symbol": "SOL_USDC"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: dict[str, Any] = await response.json()

            # Verify market response structure
            assert isinstance(data, dict), "Response should be a dict"
            assert "symbol" in data, "Market should have a 'symbol' field"
            assert "baseSymbol" in data, "Market should have a 'baseSymbol' field"
            assert "quoteSymbol" in data, "Market should have a 'quoteSymbol' field"
            assert data["symbol"] == "SOL_USDC", "Symbol should match the requested symbol"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_borrow_lend_markets_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public borrow lend markets endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/borrowLend/markets"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify borrow lend markets response structure
            assert isinstance(data, list), "Response should be a list of borrow lend markets"

            # If there are markets, verify their structure
            if len(data) > 0:
                for market in data:
                    assert isinstance(market, dict), "Each market should be a dict"
                    assert "symbol" in market, "Each market should have a 'symbol' field"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_borrow_lend_markets_history_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public borrow lend markets history endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/borrowLend/markets/history"
    params = {"interval": "1d"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify borrow lend markets history response structure
            assert isinstance(data, list), "Response should be a list of historical data"

            # If there are records, verify their structure
            if len(data) > 0:
                for record in data:
                    assert isinstance(record, dict), "Each record should be a dict"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_backpack_public_trades_history_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,
) -> None:
    """Test Backpack's public trades history endpoint."""
    base_url = str(active_bp_config.active_api_base_url).rstrip("/")
    url = f"{base_url}/api/v1/trades/history"
    params: dict[str, str | int] = {"symbol": "SOL_USDC", "limit": 10}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200, f"Expected status 200, got {response.status}"

            data: list[dict[str, Any]] = await response.json()

            # Verify trades history response structure
            assert isinstance(data, list), "Response should be a list of historical trades"

            # If there are trades, verify their structure
            if len(data) > 0:
                for trade in data:
                    assert isinstance(trade, dict), "Each trade should be a dict"
                    assert "id" in trade, "Each trade should have an 'id' field"
                    assert "price" in trade, "Each trade should have a 'price' field"
                    assert "quantity" in trade, "Each trade should have a 'quantity' field"
