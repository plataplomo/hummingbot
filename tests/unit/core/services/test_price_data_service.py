"""Tests for the PriceDataService class."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.models import Ticker
from cyberdelta.core.services.price_data_service import PriceDataService
from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL


class TestPriceDataService:
    """Test cases for PriceDataService."""

    @pytest.fixture
    def app_settings(self) -> MagicMock:
        """Create mock app settings.

        Returns:
            MagicMock: Mock AppSettings instance for testing.
        """
        settings = MagicMock(spec=AppSettings)
        settings.get = MagicMock(return_value={})
        return settings

    @pytest.fixture
    def mock_api_clients(self) -> dict[str, AsyncMock]:
        """Create mock API clients.

        Returns:
            dict[str, AsyncMock]: Dictionary of mock API clients for testing.
        """
        hyperliquid_client = AsyncMock(spec=ExchangeAPI)
        backpack_client = AsyncMock(spec=ExchangeAPI)

        # Setup default return values
        hyperliquid_client.get_ticker.return_value = None
        backpack_client.get_ticker.return_value = None

        return {
            "hyperliquid": hyperliquid_client,
            "backpack": backpack_client,
        }

    @pytest.fixture
    def price_service(
        self,
        app_settings: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> PriceDataService:
        """Create PriceDataService instance.

        Returns:
            PriceDataService: Configured price data service instance for testing.
        """
        # Cast to the expected type for mypy
        api_clients = cast("dict[str, ExchangeAPI]", mock_api_clients)
        return PriceDataService(
            app_settings=app_settings,
            api_clients=api_clients,
            cache_expiry_seconds=30,
        )

    def test_init(
        self,
        price_service: PriceDataService,
        app_settings: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test service initialization."""
        assert price_service.app_settings == app_settings
        assert price_service.api_clients == mock_api_clients
        assert price_service.cache_expiry_seconds == 30
        # Use public API to verify empty cache
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 0

    def test_register_api_client(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test registering an API client."""
        new_client = AsyncMock(spec=ExchangeAPI)
        price_service.register_api_client("new_exchange", new_client)

        assert "new_exchange" in price_service.api_clients
        assert price_service.api_clients["new_exchange"] == new_client

    @pytest.mark.asyncio
    async def test_get_ticker_cache_miss_success(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting ticker with cache miss and successful API call."""
        # Setup mock ticker
        btc_symbol = BTC_HL
        mock_ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = mock_ticker

        # Execute
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Verify
        assert result == mock_ticker
        mock_api_clients["hyperliquid"].get_ticker.assert_awaited_once_with(btc_symbol.value)
        # Check it was cached using public API
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 1

    @pytest.mark.asyncio
    async def test_get_ticker_cache_hit(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting ticker with cache hit."""
        # Pre-populate cache using public API
        btc_symbol = BTC_HL
        mock_ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        price_service.cache_ticker("hyperliquid", btc_symbol.value, mock_ticker)

        # Execute
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Verify
        assert result == mock_ticker
        # API should not be called due to cache hit
        mock_api_clients["hyperliquid"].get_ticker.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_get_ticker_cache_expired(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting ticker with expired cache entry."""
        # Pre-populate cache with entry that will expire during test
        btc_symbol = BTC_HL
        old_ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            bid=Decimal(49000),
            ask=Decimal(49100),
            timestamp=datetime.now(UTC),
        )
        # Use a very short cache expiry for this test
        price_service.cache_expiry_seconds = 1
        price_service.cache_ticker("hyperliquid", btc_symbol.value, old_ticker)

        # Wait for cache to expire
        await asyncio.sleep(1.1)

        # Setup new ticker from API
        new_ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = new_ticker

        # Execute
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Verify
        assert result == new_ticker
        mock_api_clients["hyperliquid"].get_ticker.assert_awaited_once_with(btc_symbol.value)

        # Reset cache expiry for other tests
        price_service.cache_expiry_seconds = 30

    @pytest.mark.asyncio
    async def test_get_ticker_no_client(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting ticker with no API client."""
        btc_symbol = BTC_HL
        result = await price_service.get_ticker("unknown_exchange", btc_symbol.value)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_ticker_api_error(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting ticker with API error."""
        # Current business logic doesn't catch generic Exception, so it propagates
        mock_api_clients["hyperliquid"].get_ticker.side_effect = Exception("API Error")

        # Act & Assert - Current business logic lets generic Exception propagate
        # This is the current behavior and source of truth
        btc_symbol = BTC_HL
        with pytest.raises(Exception) as exc_info:
            await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Verify the exception details
        assert "API Error" in str(exc_info.value)
        mock_api_clients["hyperliquid"].get_ticker.assert_awaited_once_with(btc_symbol.value)

    @pytest.mark.asyncio
    async def test_get_ticker_api_returns_none(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting ticker when API returns None."""
        mock_api_clients["hyperliquid"].get_ticker.return_value = None

        btc_symbol = BTC_HL
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        assert result is None
        mock_api_clients["hyperliquid"].get_ticker.assert_awaited_once_with(btc_symbol.value)

    def test_cache_ticker(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test caching ticker data."""
        eth_symbol = ETH_HL
        ticker = Ticker(
            symbol=eth_symbol,
            exchange="test_exchange",
            bid=Decimal(3000),
            ask=Decimal(3010),
            timestamp=datetime.now(UTC),
        )

        price_service.cache_ticker("backpack", eth_symbol.value, ticker)

        # Verify using public API
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["exchanges"] == 1

    @pytest.mark.asyncio
    async def test_get_cached_ticker_behavior(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test caching behavior by observing get_ticker results."""
        # Test cache miss behavior
        btc_symbol = BTC_HL
        mock_api_clients["hyperliquid"].get_ticker.return_value = None
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)
        assert result is None

        # Verify cache is still empty
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 0

    @pytest.mark.timing
    def test_cache_cleanup_behavior(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test cache cleanup functionality using public APIs."""
        # Add ticker that will expire
        btc_symbol = BTC_HL
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        # Use very short expiry for test
        price_service.cache_expiry_seconds = 1
        price_service.cache_ticker("hyperliquid", btc_symbol.value, ticker)

        # Verify entry exists
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 1

        # Wait for expiry
        time.sleep(1.1)

        # Cleanup expired entries
        removed_count = price_service.cleanup_expired_entries()
        assert removed_count == 1

        # Verify cache is now empty
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 0

        # Reset for other tests
        price_service.cache_expiry_seconds = 30

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_same_asset(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting price when asset is same as base currency."""
        result = await price_service.get_price_in_base_currency("hyperliquid", "USDC", "USDC")
        assert result == Decimal("1.0")

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_with_mid_price(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price using mid price."""
        btc_usdc_symbol = BTC_HL  # Note: This creates BTC-PERP, but for string API we'll use proper format
        ticker = Ticker(
            exchange="hyperliquid",
            symbol=btc_usdc_symbol,
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        # mid_price will be computed as (50000 + 50100) / 2 = 50050
        mock_api_clients["hyperliquid"].get_ticker.return_value = ticker

        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        assert result == Decimal(50050)

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_with_bid_ask(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price using bid/ask average."""
        btc_usdc_symbol = BTC_HL  # Note: This creates BTC-PERP, but for string API we'll use proper format
        ticker = Ticker(
            exchange="hyperliquid",
            symbol=btc_usdc_symbol,
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = ticker

        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        assert result == Decimal(50050)  # (50000 + 50100) / 2

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_bid_only(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price using only bid."""
        btc_usdc_symbol = BTC_HL  # Note: This creates BTC-PERP, but for string API we'll use proper format
        ticker = Ticker(
            exchange="hyperliquid",
            symbol=btc_usdc_symbol,
            bid=Decimal(50000),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = ticker

        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        assert result == Decimal(50000)

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_ask_only(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price using only ask."""
        btc_usdc_symbol = BTC_HL  # Note: This creates BTC-PERP, but for string API we'll use proper format
        ticker = Ticker(
            exchange="hyperliquid",
            symbol=btc_usdc_symbol,
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = ticker

        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        assert result == Decimal(50100)

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_try_multiple_formats(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price tries multiple symbol formats."""

        # Only return ticker for one specific format
        # This test is specifically for checking that the service tries multiple symbol formats
        # We need to return a ticker only for the "BTC_USDC" format (Backpack spot format)
        def get_ticker_side_effect(symbol: str) -> Ticker | None:
            if symbol == "BTC_USDC":
                # Create a symbol object for the ticker response
                from cyberdelta.core.symbols.api import symbol
                btc_usdc_symbol = symbol("BTC_USDC", "backpack")
                return Ticker(
                    symbol=btc_usdc_symbol,
                    exchange="backpack",
                    bid=Decimal(50000),
                    ask=Decimal(50100),
                    timestamp=datetime.now(UTC),
                )
            return None

        mock_api_clients["backpack"].get_ticker.side_effect = get_ticker_side_effect

        result = await price_service.get_price_in_base_currency("backpack", "BTC", "USDC")

        assert result == Decimal(50050)
        # Should have been called with multiple formats
        assert mock_api_clients["backpack"].get_ticker.await_count >= 3

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_not_found(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price when no ticker found."""
        mock_api_clients["hyperliquid"].get_ticker.return_value = None

        result = await price_service.get_price_in_base_currency("hyperliquid", "XYZ", "USDC")

        assert result is None

    def test_clear_cache_specific_exchange(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test clearing cache for specific exchange."""
        # Add some cached data using public API
        btc_symbol = BTC_HL
        ticker1 = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        eth_symbol = ETH_HL
        ticker2 = Ticker(
            exchange="test_exchange",
            symbol=eth_symbol,
            bid=Decimal(3000),
            ask=Decimal(3010),
            timestamp=datetime.now(UTC),
        )
        price_service.cache_ticker("hyperliquid", btc_symbol.value, ticker1)
        price_service.cache_ticker("backpack", eth_symbol.value, ticker2)

        # Verify both are cached
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 2
        assert stats["exchanges"] == 2

        # Clear only hyperliquid
        price_service.clear_cache("hyperliquid")

        # Verify only backpack remains
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["exchanges"] == 1

    def test_clear_cache_all(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test clearing all cache."""
        # Add some cached data using public API
        btc_symbol = BTC_HL
        ticker1 = Ticker(
            exchange="test_exchange",
            symbol=btc_symbol,
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        eth_symbol = ETH_HL
        ticker2 = Ticker(
            exchange="test_exchange",
            symbol=eth_symbol,
            bid=Decimal(3000),
            ask=Decimal(3010),
            timestamp=datetime.now(UTC),
        )
        price_service.cache_ticker("hyperliquid", btc_symbol.value, ticker1)
        price_service.cache_ticker("backpack", eth_symbol.value, ticker2)

        # Verify cache has data
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 2

        # Clear all
        price_service.clear_cache()

        # Verify cache is empty
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 0

    def test_get_cache_stats(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting cache statistics."""
        # Add some cached data using public API
        btc_symbol = BTC_HL
        ticker1 = Ticker(
            exchange="test_exchange",
            symbol=btc_symbol,
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        eth_symbol = ETH_HL
        ticker2 = Ticker(
            exchange="test_exchange",
            symbol=eth_symbol,
            bid=Decimal(3000),
            ask=Decimal(3010),
            timestamp=datetime.now(UTC),
        )
        sol_symbol = SOL_HL
        ticker3 = Ticker(
            exchange="test_exchange",
            symbol=sol_symbol,
            bid=Decimal(100),
            ask=Decimal(101),
            timestamp=datetime.now(UTC),
        )
        price_service.cache_ticker("hyperliquid", btc_symbol.value, ticker1)
        price_service.cache_ticker("hyperliquid", eth_symbol.value, ticker2)
        price_service.cache_ticker("backpack", sol_symbol.value, ticker3)

        stats = price_service.get_cache_stats()

        assert stats == {
            "exchanges": 2,
            "total_entries": 3,
            "cache_expiry_seconds": 30,
        }

    @pytest.mark.timing
    def test_cleanup_expired_entries(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test cleanup of expired entries using public APIs."""
        # Add some tickers with different ages
        btc_symbol = BTC_HL
        ticker1 = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        eth_symbol = ETH_HL
        ticker2 = Ticker(
            exchange="test_exchange",
            symbol=eth_symbol,
            bid=Decimal(3000),
            ask=Decimal(3010),
            timestamp=datetime.now(UTC),
        )
        sol_symbol = SOL_HL
        ticker3 = Ticker(
            symbol=sol_symbol,
            exchange="test_exchange",
            bid=Decimal(100),
            ask=Decimal(101),
            timestamp=datetime.now(UTC),
        )

        # Use very short expiry for test
        original_expiry = price_service.cache_expiry_seconds
        price_service.cache_expiry_seconds = 1

        # Add tickers
        price_service.cache_ticker("hyperliquid", btc_symbol.value, ticker1)
        price_service.cache_ticker("hyperliquid", eth_symbol.value, ticker2)
        price_service.cache_ticker("backpack", sol_symbol.value, ticker3)

        # Verify all cached
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 3

        # Wait for expiry
        time.sleep(1.1)

        # Cleanup expired entries
        removed_count = price_service.cleanup_expired_entries()
        assert removed_count == 3

        # Verify cache is empty
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 0

        # Reset for other tests
        price_service.cache_expiry_seconds = original_expiry

    def test_cleanup_expired_entries_empty_cache(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test cleanup when cache is empty."""
        # Ensure cache is empty
        price_service.clear_cache()

        # Cleanup should return 0
        removed_count = price_service.cleanup_expired_entries()
        assert removed_count == 0

        # Cache should still be empty
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] == 0

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_concurrent_ticker_requests(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test concurrent ticker requests for same symbol."""
        # Setup mock to simulate slow API call
        btc_symbol = BTC_HL
        call_count = 0

        async def slow_get_ticker(symbol: str) -> Ticker:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)  # Simulate network delay
            return Ticker(
                symbol=btc_symbol,
                exchange="hyperliquid",
                bid=Decimal(50000),
                ask=Decimal(50100),
                timestamp=datetime.now(UTC),
            )

        mock_api_clients["hyperliquid"].get_ticker.side_effect = slow_get_ticker

        # Make concurrent requests
        tasks = [
            price_service.get_ticker("hyperliquid", btc_symbol.value),
            price_service.get_ticker("hyperliquid", btc_symbol.value),
            price_service.get_ticker("hyperliquid", btc_symbol.value),
        ]

        results = await asyncio.gather(*tasks)

        # All should get the same ticker
        assert all(r is not None for r in results)
        assert all(r.symbol == btc_symbol for r in results if r)

        # API should be called multiple times (no deduplication implemented)
        assert call_count == 3
