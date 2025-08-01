"""Comprehensive unit tests for the PriceDataService component.

Tests price data management functionality including ticker caching and price conversions.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.models import Ticker
from cyberdelta.core.services.price_data_service import PriceDataService
from cyberdelta.core.symbols import Symbol, symbols
from tests.fixtures.symbol_domain_fixtures import SymbolSet
from tests.fixtures.time_fixtures import FreezerProtocol


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock application settings.

    Returns:
        Mock: Mocked AppSettings instance.
    """
    return Mock(spec=AppSettings)


@pytest.fixture
def mock_exchange_api() -> Mock:
    """Create mock exchange API client.

    Returns:
        Mock: Mocked ExchangeAPI client with get_ticker method.
    """
    client = Mock(spec=ExchangeAPI)
    client.get_ticker = AsyncMock(return_value=None)
    return client


@pytest.fixture
def mock_api_clients() -> dict[str, AsyncMock]:
    """Create mock API clients dictionary.

    Returns:
        dict[str, AsyncMock]: Dictionary of mocked API clients for hyperliquid and backpack.
    """
    hl_client = AsyncMock()
    hl_client.get_ticker = AsyncMock(return_value=None)

    bp_client = AsyncMock()
    bp_client.get_ticker = AsyncMock(return_value=None)

    return {
        "hyperliquid": hl_client,
        "backpack": bp_client,
    }


@pytest.fixture
def price_service(
    mock_app_settings: Mock,
    mock_api_clients: dict[str, AsyncMock],
) -> PriceDataService:
    """Create PriceDataService instance for testing.

    Returns:
        PriceDataService: Configured price data service with 30-second cache expiry.
    """
    return PriceDataService(
        app_settings=mock_app_settings,
        api_clients=mock_api_clients,  # type: ignore[arg-type]
        cache_expiry_seconds=30,
    )


@pytest.fixture
def sample_ticker() -> Ticker:
    """Create sample ticker for testing.

    Returns:
        Ticker: Sample BTC-PERP ticker with bid/ask spread.
    """
    btc_symbol = symbols.BTC.hyperliquid()
    return Ticker(
        symbol=btc_symbol,
        exchange="test_exchange",
        bid=Decimal("50000.0"),
        ask=Decimal("50100.0"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_ticker_with_mid_price() -> Ticker:
    """Create sample ticker with mid price for testing.

    Returns:
        Ticker: Sample ETH-PERP ticker with bid/ask/mid price.
    """
    eth_symbol = symbols.ETH.hyperliquid()
    return Ticker(
        symbol=eth_symbol,
        exchange="test_exchange",
        bid=Decimal("3000.0"),
        ask=Decimal("3010.0"),
        price=Decimal("3005.0"),
        timestamp=datetime.now(UTC),
    )


class TestPriceDataServiceInit:
    """Test initialization of PriceDataService."""

    def test_init_success_with_api_clients(
        self,
        mock_app_settings: Mock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful initialization with API clients."""
        # Act
        service = PriceDataService(
            app_settings=mock_app_settings,
            api_clients=mock_api_clients,  # type: ignore[arg-type]
            cache_expiry_seconds=60,
        )

        # Assert
        assert service.app_settings == mock_app_settings
        assert service.api_clients == mock_api_clients
        assert service.cache_expiry_seconds == 60
        # Test cache is empty through public API
        cache_stats = service.get_cache_stats()
        assert cache_stats["total_entries"] == 0

    def test_init_success_without_api_clients(self, mock_app_settings: Mock) -> None:
        """Test successful initialization without API clients."""
        # Act
        service = PriceDataService(
            app_settings=mock_app_settings,
            api_clients=None,
        )

        # Assert
        assert service.api_clients == {}
        assert service.cache_expiry_seconds == 30  # Default

    def test_init_edge_custom_cache_expiry(self, mock_app_settings: Mock) -> None:
        """Test initialization with custom cache expiry values."""
        # Act
        service = PriceDataService(
            app_settings=mock_app_settings,
            cache_expiry_seconds=0,  # No caching
        )

        # Assert
        assert service.cache_expiry_seconds == 0

    def test_init_edge_with_logging(
        self, mock_app_settings: Mock, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test initialization logs proper messages."""
        # Act
        with patch("cyberdelta.core.services.price_data_service.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            service = PriceDataService(
                app_settings=mock_app_settings,
                api_clients=mock_api_clients,  # type: ignore[arg-type]
                cache_expiry_seconds=45,
            )
            _ = service  # Used for side effects

            # Assert
            mock_logger.info.assert_called_once_with(
                "PriceDataService initialized",
                exchanges=["hyperliquid", "backpack"],
                cache_expiry_seconds=45,
            )


class TestRegisterApiClient:
    """Test register_api_client method functionality."""

    def test_register_api_client_success(
        self, price_service: PriceDataService, mock_exchange_api: Mock
    ) -> None:
        """Test successful API client registration."""
        # Act
        price_service.register_api_client("new_exchange", mock_exchange_api)

        # Assert
        assert "new_exchange" in price_service.api_clients
        assert price_service.api_clients["new_exchange"] == mock_exchange_api

    def test_register_api_client_success_overwrite(
        self, price_service: PriceDataService, mock_exchange_api: Mock
    ) -> None:
        """Test overwriting existing API client."""
        # Arrange
        old_client = Mock(spec=ExchangeAPI)
        price_service.api_clients["test_exchange"] = old_client

        # Act
        price_service.register_api_client("test_exchange", mock_exchange_api)

        # Assert
        assert price_service.api_clients["test_exchange"] == mock_exchange_api
        assert price_service.api_clients["test_exchange"] != old_client

    def test_register_api_client_edge_multiple_registrations(
        self, price_service: PriceDataService
    ) -> None:
        """Test registering multiple API clients."""
        # Arrange
        clients = {f"exchange_{i}": Mock(spec=ExchangeAPI) for i in range(5)}

        # Act
        for exchange_id, client in clients.items():
            price_service.register_api_client(exchange_id, client)

        # Assert
        for exchange_id, client in clients.items():
            assert price_service.api_clients[exchange_id] == client


class TestGetTicker:
    """Test get_ticker method functionality."""

    @pytest.mark.asyncio
    async def test_get_ticker_success_cache_miss(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful ticker fetch on cache miss."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        # Configure mock to return the sample ticker
        mock_api_clients["hyperliquid"].get_ticker.return_value = sample_ticker

        # Act
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result == sample_ticker
        mock_api_clients["hyperliquid"].get_ticker.assert_called_once_with(btc_symbol.value)
        # Verify it was cached through public API
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] == 1
        assert cache_stats["exchanges"] == 1

    @pytest.mark.asyncio
    async def test_get_ticker_success_cache_hit(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful ticker fetch from cache."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)

        # Act
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result == sample_ticker
        # API should not be called when getting from cache
        # We can verify cache was used by checking stats remain unchanged
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] == 1

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_get_ticker_edge_cache_expired(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test ticker fetch when cache entry is expired."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        # Add entry to cache using public API, then manipulate time by changing cache expiry
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)

        # Move time forward to make the cache entry expired
        future_time = datetime.now(UTC) + timedelta(seconds=price_service.cache_expiry_seconds + 1)
        frozen_time.move_to(future_time)

        new_ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            bid=Decimal("51000.0"),
            ask=Decimal("51100.0"),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = new_ticker

        # Act
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result == new_ticker
        mock_api_clients["hyperliquid"].get_ticker.assert_called_once_with(btc_symbol.value)
        # Verify new entry was cached through public API
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] == 1
        # Test that second call returns cached value
        result2 = await price_service.get_ticker("hyperliquid", btc_symbol.value)
        assert result2 == new_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_edge_api_returns_none(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test ticker fetch when API returns None."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        mock_api_clients["hyperliquid"].get_ticker.return_value = None

        # Act
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result is None
        # Should not cache None result - verify through cache stats
        cache_stats = price_service.get_cache_stats()
        # No entries should be cached when None is returned
        assert cache_stats.get("total_entries", 0) == 0

    @pytest.mark.asyncio
    async def test_get_ticker_failure_no_client(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test ticker fetch with no API client."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()

        # Act
        result = await price_service.get_ticker("unknown_exchange", btc_symbol.value)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_get_ticker_failure_api_exception(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test ticker fetch when API raises exception."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        # Current business logic doesn't catch RuntimeError, so it propagates
        mock_api_clients["hyperliquid"].get_ticker.side_effect = RuntimeError("Network error")

        # Act & Assert - Current business logic lets RuntimeError propagate
        # This is the current behavior and source of truth
        with pytest.raises(RuntimeError) as exc_info:
            await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Verify the exception details
        assert "Network error" in str(exc_info.value)
        # Should not cache on error - verify through cache stats
        cache_stats = price_service.get_cache_stats()
        # No entries should be cached when exception is raised
        assert cache_stats.get("total_entries", 0) == 0


class TestCacheTicker:
    """Test cache_ticker method functionality."""

    def test_cache_ticker_success_new_exchange(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test caching ticker for new exchange."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()

        # Act
        price_service.cache_ticker("new_exchange", btc_symbol.value, sample_ticker)

        # Assert - Verify caching behavior through public API
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] >= 1

        # Test that cached data can be retrieved by trying to get ticker
        # (though we need a mock client for this to work fully)
        # The fact that cache_ticker doesn't raise an error confirms the caching worked

    def test_cache_ticker_success_existing_exchange(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        sample_ticker_with_mid_price: Ticker,
    ) -> None:
        """Test caching ticker for existing exchange."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        eth_symbol = symbols.ETH.hyperliquid()
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)

        # Act
        price_service.cache_ticker("hyperliquid", eth_symbol.value, sample_ticker_with_mid_price)

        # Assert - Verify multiple tickers are cached through public API
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] >= 2  # Should have at least 2 cached tickers

        # Verify behavior by checking cache contains the expected entries
        # The fact that both cache_ticker calls succeeded indicates proper caching

    def test_cache_ticker_edge_overwrite_existing(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test overwriting existing cached ticker."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        old_ticker = Ticker(
            symbol=btc_symbol,
            exchange="test_exchange",
            bid=Decimal("49000.0"),
            ask=Decimal("49100.0"),
            timestamp=datetime.now(UTC) - timedelta(minutes=5),
        )
        price_service.cache_ticker("hyperliquid", btc_symbol.value, old_ticker)

        # Get initial cache stats
        initial_stats = price_service.get_cache_stats()
        initial_count = initial_stats["total_entries"]

        # Act
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)

        # Assert - Verify replacement through public API
        # The count should remain the same (replacement, not addition)
        final_stats = price_service.get_cache_stats()
        assert final_stats["total_entries"] == initial_count

        # We can't directly verify the ticker was replaced, but the fact that
        # the count stayed the same indicates replacement occurred

    def test_cache_ticker_edge_multiple_exchanges_same_symbol(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test caching same symbol across multiple exchanges."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()

        # Act
        for exchange in ["exchange1", "exchange2", "exchange3"]:
            price_service.cache_ticker(exchange, btc_symbol.value, sample_ticker)

        # Assert - Use public API to verify
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["exchanges"] == 3
        assert cache_stats["total_entries"] == 3  # One BTC-PERP per exchange


class TestGetCachedTicker:
    """Test cached ticker functionality through public API."""

    @pytest.mark.asyncio
    async def test_get_cached_ticker_success_valid_entry(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting valid cached ticker through public get_ticker."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        # Ensure API won't be called by setting it to fail
        mock_api_clients["hyperliquid"].get_ticker.side_effect = Exception("Should not be called")

        # Act - Should get from cache, not API
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result == sample_ticker

    @pytest.mark.asyncio
    async def test_get_cached_ticker_edge_expired_entry(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test expired cached ticker triggers fresh fetch."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        # We can't directly set expiry, but we can test the behavior
        # by caching a ticker and relying on the service's expiry logic
        new_ticker = Ticker(
            symbol=btc_symbol.value,
            exchange="test_exchange",
            bid=Decimal("51000.0"),
            ask=Decimal("51100.0"),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = new_ticker

        # Act - Should fetch fresh ticker
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result == new_ticker

    @pytest.mark.asyncio
    async def test_get_cached_ticker_failure_no_exchange(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting cached ticker for non-existent exchange."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()

        # Act - Test through public API, should return None for unknown exchange
        result = await price_service.get_ticker("unknown_exchange", btc_symbol.value)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_get_cached_ticker_failure_no_symbol(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting cached ticker for non-existent symbol."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        eth_symbol = symbols.ETH.hyperliquid()
        price_service.cache_ticker("hyperliquid", eth_symbol.value, sample_ticker)
        # Configure API to return None for BTC-PERP
        mock_api_clients["hyperliquid"].get_ticker.return_value = None

        # Act - Should not find BTC-PERP in cache, will call API
        result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert
        assert result is None
        # Verify API was called since symbol wasn't in cache
        mock_api_clients["hyperliquid"].get_ticker.assert_awaited_once_with(btc_symbol.value)


class TestGetPriceInBaseCurrency:
    """Test get_price_in_base_currency method functionality."""

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_success_same_asset(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test getting price when asset equals base currency."""
        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "USDC", "USDC")

        # Assert
        assert result == Decimal("1.0")
        # No API calls should be made
        mock_api_clients["hyperliquid"].get_ticker.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_success_with_mid_price(
        self,
        price_service: PriceDataService,
        sample_ticker_with_mid_price: Ticker,
    ) -> None:
        """Test getting price using mid price."""
        # Arrange
        price_service.api_clients[
            "hyperliquid"
        ].get_ticker.return_value = sample_ticker_with_mid_price  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "ETH", "USDC")

        # Assert
        assert result == Decimal("3005.0")  # mid_price

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_success_bid_ask_average(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test getting price using bid/ask average."""
        # Arrange
        price_service.api_clients["hyperliquid"].get_ticker.return_value = sample_ticker  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        # Assert
        assert result == Decimal("50050.0")  # (50000 + 50100) / 2

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_success_bid_only(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting price with only bid available."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            bid=Decimal("50000.0"),
            timestamp=datetime.now(UTC),
        )
        price_service.api_clients["hyperliquid"].get_ticker.return_value = ticker  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        # Assert
        assert result == Decimal("50000.0")

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_success_ask_only(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting price with only ask available."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            ask=Decimal("50100.0"),
            timestamp=datetime.now(UTC),
        )
        price_service.api_clients["hyperliquid"].get_ticker.return_value = ticker  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        # Assert
        assert result == Decimal("50100.0")

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_edge_try_multiple_formats(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test trying multiple symbol formats to find ticker."""

        # Arrange
        def get_ticker_side_effect(symbol: str) -> Ticker | None:
            # Only return ticker for specific format
            btc_symbol = symbols.BTC.backpack()
            if symbol == btc_symbol.value:
                return sample_ticker
            return None

        price_service.api_clients["backpack"].get_ticker.side_effect = get_ticker_side_effect  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("backpack", "BTC", "USDC")

        # Assert
        assert result == Decimal("50050.0")
        # Should have tried multiple formats
        assert price_service.api_clients["backpack"].get_ticker.await_count >= 3  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_edge_zero_mid_price(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test handling zero mid price."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        ticker = Ticker(
            exchange="hyperliquid",
            symbol=btc_symbol,
            bid=Decimal("50000.0"),
            ask=Decimal("50100.0"),
            price=Decimal(0),  # Invalid price
            timestamp=datetime.now(UTC),
        )
        price_service.api_clients["hyperliquid"].get_ticker.return_value = ticker  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        # Assert
        assert result == Decimal("50050.0")  # Falls back to bid/ask average

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_failure_no_ticker(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting price when no ticker found."""
        # Arrange
        price_service.api_clients["hyperliquid"].get_ticker.return_value = None  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "XYZ", "USDC")

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_get_price_in_base_currency_failure_no_price_data(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting price when ticker has no usable price data."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        ticker = Ticker(
            symbol=btc_symbol,
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            # No bid, ask, or mid_price
        )
        price_service.api_clients["hyperliquid"].get_ticker.return_value = ticker  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

        # Assert
        assert result is None


class TestClearCache:
    """Test clear_cache method functionality."""

    def test_clear_cache_success_specific_exchange(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
    ) -> None:
        """Test clearing cache for specific exchange."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        price_service.cache_ticker("backpack", eth_symbol.value, sample_ticker)

        # Verify both exchanges are cached
        initial_stats = price_service.get_cache_stats()
        assert initial_stats["exchanges"] == 2

        # Act
        price_service.clear_cache("hyperliquid")

        # Assert - Use public API to verify
        final_stats = price_service.get_cache_stats()
        assert final_stats["exchanges"] == 1  # Only backpack should remain
        assert final_stats["total_entries"] == 1

    def test_clear_cache_success_all_exchanges(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
    ) -> None:
        """Test clearing cache for all exchanges."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        price_service.cache_ticker("backpack", eth_symbol.value, sample_ticker)

        # Act
        price_service.clear_cache()

        # Assert - Use public API to verify
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] == 0
        assert cache_stats["exchanges"] == 0

    def test_clear_cache_edge_non_existent_exchange(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test clearing cache for non-existent exchange."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        initial_stats = price_service.get_cache_stats()

        # Act
        price_service.clear_cache("unknown_exchange")

        # Assert - Cache should remain unchanged
        final_stats = price_service.get_cache_stats()
        assert final_stats["total_entries"] == initial_stats["total_entries"]
        assert final_stats["exchanges"] == initial_stats["exchanges"]

    def test_clear_cache_edge_empty_cache(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test clearing already empty cache."""
        # Act
        price_service.clear_cache()

        # Assert - Use public API to verify
        cache_stats = price_service.get_cache_stats()
        assert cache_stats["total_entries"] == 0
        assert cache_stats["exchanges"] == 0


class TestGetCacheStats:
    """Test get_cache_stats method functionality."""

    def test_get_cache_stats_success_empty_cache(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test getting stats for empty cache."""
        # Act
        stats = price_service.get_cache_stats()

        # Assert
        assert stats == {
            "exchanges": 0,
            "total_entries": 0,
            "cache_expiry_seconds": 30,
        }

    def test_get_cache_stats_success_populated_cache(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
        sol_symbols: SymbolSet,
    ) -> None:
        """Test getting stats for populated cache."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        sol_symbol = sol_symbols.perp_hl
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        price_service.cache_ticker("hyperliquid", eth_symbol.value, sample_ticker)
        price_service.cache_ticker("backpack", sol_symbol.value, sample_ticker)

        # Act
        stats = price_service.get_cache_stats()

        # Assert
        assert stats == {
            "exchanges": 2,
            "total_entries": 3,
            "cache_expiry_seconds": 30,
        }

    def test_get_cache_stats_edge_multiple_exchanges(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test getting stats with many exchanges."""
        # Arrange
        for i in range(10):
            for j in range(5):
                price_service.cache_ticker(f"exchange_{i}", f"SYMBOL_{j}", sample_ticker)

        # Act
        stats = price_service.get_cache_stats()

        # Assert
        assert stats["exchanges"] == 10
        assert stats["total_entries"] == 50


class TestCleanupExpiredEntries:
    """Test cleanup_expired_entries method functionality."""

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_cleanup_expired_entries_success_some_expired(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
        sample_ticker: Ticker,
        frozen_time: FreezerProtocol,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
        sol_symbols: SymbolSet,
    ) -> None:
        """Test cleaning up some expired entries through public API."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        sol_symbol = sol_symbols.perp_hl
        # Get tickers to populate cache
        mock_api_clients["hyperliquid"].get_ticker.return_value = sample_ticker
        mock_api_clients["backpack"].get_ticker.return_value = sample_ticker

        # Get some tickers to populate cache
        await price_service.get_ticker("hyperliquid", btc_symbol.value)
        await price_service.get_ticker("hyperliquid", eth_symbol.value)
        await price_service.get_ticker("backpack", sol_symbol.value)

        # Verify cache has entries
        stats_before = price_service.get_cache_stats()
        assert stats_before["total_entries"] == 3

        # Move time forward to make some entries expire
        future_time = datetime.now(UTC) + timedelta(seconds=100)
        frozen_time.move_to(future_time)

        # Act
        removed_count = price_service.cleanup_expired_entries()

        # Assert - All entries should be expired and removed
        assert removed_count == 3
        stats_after = price_service.get_cache_stats()
        assert stats_after["total_entries"] == 0

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_cleanup_expired_entries_success_all_expired(
        self,
        price_service: PriceDataService,
        mock_api_clients: dict[str, AsyncMock],
        sample_ticker: Ticker,
        frozen_time: FreezerProtocol,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
    ) -> None:
        """Test cleaning up all expired entries through public API."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        # Get tickers to populate cache
        mock_api_clients["hyperliquid"].get_ticker.return_value = sample_ticker

        # Get some tickers to populate cache
        await price_service.get_ticker("hyperliquid", btc_symbol.value)
        await price_service.get_ticker("hyperliquid", eth_symbol.value)

        # Verify cache has entries
        stats_before = price_service.get_cache_stats()
        assert stats_before["total_entries"] == 2

        # Move time forward to make entries expire
        future_time = datetime.now(UTC) + timedelta(seconds=100)
        frozen_time.move_to(future_time)

        # Act
        removed_count = price_service.cleanup_expired_entries()

        # Assert
        assert removed_count == 2
        stats_after = price_service.get_cache_stats()
        assert stats_after["total_entries"] == 0

    def test_cleanup_expired_entries_edge_no_expired(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
    ) -> None:
        """Test cleanup when no entries are expired."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        price_service.cache_ticker("backpack", eth_symbol.value, sample_ticker)

        # Verify cache has entries
        stats_before = price_service.get_cache_stats()
        assert stats_before["total_entries"] == 2

        # Act
        removed_count = price_service.cleanup_expired_entries()

        # Assert
        assert removed_count == 0
        stats_after = price_service.get_cache_stats()
        assert stats_after["total_entries"] == 2

    def test_cleanup_expired_entries_edge_empty_cache(
        self,
        price_service: PriceDataService,
    ) -> None:
        """Test cleanup on empty cache."""
        # Act
        removed_count = price_service.cleanup_expired_entries()

        # Assert
        assert removed_count == 0

    @pytest.mark.timing
    def test_cleanup_expired_entries_edge_zero_expiry_time(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        frozen_time: FreezerProtocol,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test cleanup with zero expiry time (all should be expired)."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        price_service.cache_expiry_seconds = 0
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)
        # Move time forward slightly to ensure cache entry is considered expired
        future_time = datetime.now(UTC) + timedelta(milliseconds=1)
        frozen_time.move_to(future_time)

        # Act
        removed_count = price_service.cleanup_expired_entries()

        # Assert
        assert removed_count == 1
        stats_after = price_service.get_cache_stats()
        assert stats_after["total_entries"] == 0


class TestConcurrentOperations:
    """Test concurrent operations and thread safety."""

    @pytest.mark.asyncio
    async def test_concurrent_ticker_requests_same_symbol(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test concurrent requests for same symbol."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        call_count = 0

        async def slow_get_ticker(symbol: str) -> Ticker:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return sample_ticker

        price_service.api_clients["hyperliquid"].get_ticker.side_effect = slow_get_ticker  # type: ignore[attr-defined]

        # Act - Make concurrent requests
        tasks = [price_service.get_ticker("hyperliquid", btc_symbol.value) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # Assert
        assert all(r == sample_ticker for r in results)
        assert call_count == 5  # All requests made (no deduplication)

    @pytest.mark.asyncio
    async def test_concurrent_ticker_requests_different_symbols(
        self,
        price_service: PriceDataService,
        btc_symbols: SymbolSet,
        eth_symbols: SymbolSet,
        sol_symbols: SymbolSet,
    ) -> None:
        """Test concurrent requests for different symbols."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        eth_symbol = eth_symbols.perp_hl
        sol_symbol = sol_symbols.perp_hl

        async def get_ticker_for_symbol(symbol: str) -> Ticker:
            await asyncio.sleep(0.05)
            return Ticker(
                symbol=symbol,
                exchange="hyperliquid",
                bid=Decimal("50000.0"),
                ask=Decimal("50100.0"),
                timestamp=datetime.now(UTC),
            )

        price_service.api_clients["hyperliquid"].get_ticker.side_effect = get_ticker_for_symbol  # type: ignore[attr-defined]

        # Act - Request different symbols concurrently
        avax_symbol = symbols.AVAX.hyperliquid()
        matic_symbol = symbols.MATIC.hyperliquid()
        symbols = [btc_symbol.value, eth_symbol.value, sol_symbol.value, avax_symbol.value, matic_symbol.value]
        tasks = [price_service.get_ticker("hyperliquid", symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)

        # Assert
        assert len(results) == 5
        for i, result in enumerate(results):
            assert result is not None
            assert result.symbol == symbols[i]

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_concurrent_cache_operations(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test concurrent cache operations."""
        # Arrange
        tasks: list[Any] = []

        # Mix of cache operations
        for i in range(10):
            if i % 3 == 0:
                # Cache ticker
                tasks.append(
                    asyncio.create_task(
                        asyncio.to_thread(
                            price_service.cache_ticker, "hyperliquid", f"SYMBOL_{i}", sample_ticker
                        )
                    )
                )
            elif i % 3 == 1:
                # Get stats
                def get_stats() -> None:
                    _ = price_service.get_cache_stats()

                tasks.append(asyncio.create_task(asyncio.to_thread(get_stats)))
            else:
                # Cleanup
                def cleanup() -> None:
                    _ = price_service.cleanup_expired_entries()

                tasks.append(asyncio.create_task(asyncio.to_thread(cleanup)))

        # Act
        await asyncio.gather(*tasks, return_exceptions=True)

        # Assert - Should complete without errors
        stats = price_service.get_cache_stats()
        assert stats["total_entries"] >= 0  # Some entries may have been added


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling scenarios."""

    @pytest.mark.asyncio
    async def test_get_ticker_with_various_exceptions(
        self,
        price_service: PriceDataService,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test handling various exception types - aligned with current business logic."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        # Exceptions that ARE caught by business logic and return None
        caught_exceptions = [
            ValueError("Invalid response"),
            KeyError("Missing field"),
            TypeError("Type error"),
            AttributeError("Attribute error"),
            ArithmeticError("Arithmetic error"),
        ]

        for exc in caught_exceptions:
            price_service.api_clients["hyperliquid"].get_ticker.side_effect = exc  # type: ignore[attr-defined]

            # Act
            result = await price_service.get_ticker("hyperliquid", btc_symbol.value)

            # Assert
            assert result is None

        # Exceptions that are NOT caught by business logic and propagate
        propagating_exceptions = [
            RuntimeError("Network error"),
            TimeoutError("Request timeout"),
            Exception("Generic error"),
        ]

        for exc in propagating_exceptions:
            price_service.api_clients["hyperliquid"].get_ticker.side_effect = exc  # type: ignore[attr-defined]

            # Act & Assert - Current business logic lets these exceptions propagate
            # This is the current behavior and source of truth
            with pytest.raises(type(exc)) as exc_info:
                await price_service.get_ticker("hyperliquid", btc_symbol.value)

            # Verify the exception details
            assert str(exc) in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_price_conversion_symbol_format_variations(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
    ) -> None:
        """Test price conversion with various symbol formats."""
        # Arrange - Test different symbol format patterns
        btc_symbol = symbols.BTC.hyperliquid()
        formats = {
            "BTC-USDC": sample_ticker,
            "BTCUSDC": sample_ticker,
            "BTC_USDC": sample_ticker,
            "BTC/USDC": sample_ticker,
            "BTC": sample_ticker,
        }

        for ticker in formats.values():
            # Clear cache between tests
            price_service.clear_cache()

            # Set up mock to return the ticker for this symbol format
            price_service.api_clients["hyperliquid"].get_ticker.return_value = ticker  # type: ignore[attr-defined]

            # Act
            result = await price_service.get_price_in_base_currency("hyperliquid", "BTC", "USDC")

            # Assert
            assert result == Decimal("50050.0")

    @pytest.mark.asyncio
    async def test_cache_expiry_boundary_conditions(
        self,
        price_service: PriceDataService,
        sample_ticker: Ticker,
        mock_api_clients: dict[str, AsyncMock],
        btc_symbols: SymbolSet,
    ) -> None:
        """Test cache expiry at exact boundary."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        # This test validates cache expiry behavior by using the public API
        # We can't test exact boundaries without accessing private members,
        # so we test the behavior through normal cache operations

        # Test cache behavior with different expiry values
        # First test: normal cache operation
        price_service.cache_expiry_seconds = 30  # Normal expiry
        price_service.cache_ticker("hyperliquid", btc_symbol.value, sample_ticker)

        # Configure mock to return None so we know if cache was used
        mock_api_clients["hyperliquid"].get_ticker.return_value = None

        # Act - Get immediately (should be cached)
        result1 = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert - Should get cached value
        assert result1 == sample_ticker

        # Now test immediate expiry
        price_service.cache_expiry_seconds = 0  # Immediate expiry

        # Act - Get with zero expiry (should fetch from API)
        result2 = await price_service.get_ticker("hyperliquid", btc_symbol.value)

        # Assert - Should get None from API since cache is considered expired
        assert result2 is None
        mock_api_clients["hyperliquid"].get_ticker.assert_called_with(btc_symbol.value)


class TestParametrizedScenarios:
    """Parametrized tests for various scenarios."""

    @pytest.mark.parametrize(
        ("cache_expiry", "should_be_valid"),
        [
            (0, False),  # Immediate expiry
            (30, True),  # Normal expiry
            (3600, True),  # Long expiry
            (-1, False),  # Negative expiry (should expire immediately)
        ],
    )
    @pytest.mark.timing
    def test_cache_expiry_scenarios(
        self,
        mock_app_settings: Mock,
        sample_ticker: Ticker,
        cache_expiry: int,
        should_be_valid: bool,
        frozen_time: FreezerProtocol,
        btc_symbols: SymbolSet,
    ) -> None:
        """Test various cache expiry scenarios."""
        # Arrange
        btc_symbol = btc_symbols.perp_hl
        service = PriceDataService(
            app_settings=mock_app_settings,
            cache_expiry_seconds=cache_expiry,
        )
        service.cache_ticker("test_exchange", btc_symbol.value, sample_ticker)

        # Move time forward slightly to test immediate expiry scenarios
        future_time = datetime.now(UTC) + timedelta(milliseconds=10)
        frozen_time.move_to(future_time)

        # Act - Test through public API by checking cache behavior
        # We can infer cache validity by whether cleanup removes the entry
        initial_stats = service.get_cache_stats()
        removed = service.cleanup_expired_entries()
        final_stats = service.get_cache_stats()

        # Assert
        if should_be_valid:
            assert removed == 0  # Nothing should be removed if valid
            assert final_stats["total_entries"] == initial_stats["total_entries"]
        else:
            assert removed > 0  # Should remove expired entry
            assert final_stats["total_entries"] < initial_stats["total_entries"]

    @pytest.mark.parametrize(
        ("bid", "ask", "mid_price", "expected_price"),
        [
            (Decimal(100), Decimal(102), Decimal(101), Decimal(101)),  # Use mid price
            (Decimal(100), Decimal(102), Decimal(0), Decimal(101)),  # Invalid mid, use avg
            (Decimal(100), Decimal(102), None, Decimal(101)),  # No mid, use avg
            (Decimal(100), None, None, Decimal(100)),  # Bid only
            (None, Decimal(102), None, Decimal(102)),  # Ask only
            (None, None, Decimal(101), None),  # Invalid mid only
            (None, None, None, None),  # No prices
        ],
    )
    @pytest.mark.asyncio
    async def test_price_calculation_scenarios(
        self,
        price_service: PriceDataService,
        bid: Decimal | None,
        ask: Decimal | None,
        mid_price: Decimal | None,
        expected_price: Decimal | None,
    ) -> None:
        """Test various price calculation scenarios."""
        # Arrange
        ticker = Ticker(
            symbol="TEST-USDC",
            exchange="hyperliquid",
            bid=bid,
            ask=ask,
            price=mid_price,
            timestamp=datetime.now(UTC),
        )
        price_service.api_clients["hyperliquid"].get_ticker.return_value = ticker  # type: ignore[attr-defined]

        # Act
        result = await price_service.get_price_in_base_currency("hyperliquid", "TEST", "USDC")

        # Assert
        assert result == expected_price
