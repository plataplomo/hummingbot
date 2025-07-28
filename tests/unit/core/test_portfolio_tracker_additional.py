"""Additional comprehensive unit tests for PortfolioTracker.

Tests additional public methods that need better coverage,
focusing on watchlist management, active symbols, performance stats,
and state management.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings, PortfolioTrackerConfig
from cyberdelta.core.models import (
    DerivativePosition,
    Order,
    OrderSide,
    SpotBalance,
    Ticker,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker


# Import shared fixtures from conftest.py - they will be automatically available
# The following fixtures are imported:
# - mock_app_settings (but we need to override it to add exchange config)
# - pt_config (as mock_pt_config)
# - sample_spot_balance
# - sample_derivative_position
# - sample_order


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings for testing with exchange configuration.
    
    Returns:
        Mock: Mock instance of AppSettings with configured exchanges.
    """
    settings = Mock(spec=AppSettings)

    # Configure exchanges as a dict with ExchangeSpecificConfig objects
    mock_exchange_config = Mock()
    mock_exchange_config.enabled = True
    settings.exchanges = {
        "hyperliquid": mock_exchange_config,
        "backpack": mock_exchange_config,
    }

    return settings


@pytest.fixture
def mock_pt_config(pt_config: PortfolioTrackerConfig) -> Mock:
    """Extend the shared pt_config with additional mock settings.
    
    Returns:
        Mock: Mock instance of PortfolioTrackerConfig with extended settings.
    """
    config = Mock(spec=PortfolioTrackerConfig)
    config.high_watermark_file = ""
    config.state_file = ""
    config.memory_settings = Mock()
    config.memory_settings.max_ticker_entries = 1000
    config.memory_settings.data_retention_hours = 24
    config.memory_settings.cleanup_interval_minutes = 60
    # Copy data_freshness_seconds from shared config
    config.data_freshness_seconds = pt_config.data_freshness_seconds
    return config


@pytest.fixture
def portfolio_tracker(mock_app_settings: Mock, mock_pt_config: Mock) -> PortfolioTracker:
    """Create a PortfolioTracker instance for testing.
    
    Returns:
        PortfolioTracker: Configured portfolio tracker instance for testing.
    """
    return PortfolioTracker(mock_app_settings, mock_pt_config)


# The sample_ticker fixture is already available from conftest.py
# We don't need to redefine it here


class TestPortfolioTrackerWatchlistManagement:
    """Test suite for watchlist management functionality."""

    # ==================== SUCCESS CASES ====================

    def test_add_symbol_to_watchlist_success_new_symbol(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test adding a new symbol to watchlist."""
        # Arrange
        symbol = "BTC-PERP"
        assert symbol not in portfolio_tracker.watchlist

        # Act
        with patch("cyberdelta.core.portfolio_tracker.logger") as mock_logger:
            portfolio_tracker.add_symbol_to_watchlist(symbol)

        # Assert
        assert symbol in portfolio_tracker.watchlist
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "symbol_added_to_watchlist" in call_args[0]
        assert call_args[1]["symbol"] == symbol

    def test_remove_symbol_from_watchlist_success_existing_symbol(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test removing an existing symbol from watchlist."""
        # Arrange
        symbol = "ETH-PERP"
        portfolio_tracker.watchlist.add(symbol)
        assert symbol in portfolio_tracker.watchlist

        # Act
        with patch("cyberdelta.core.portfolio_tracker.logger") as mock_logger:
            portfolio_tracker.remove_symbol_from_watchlist(symbol)

        # Assert
        assert symbol not in portfolio_tracker.watchlist
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "symbol_removed_from_watchlist" in call_args[0]
        assert call_args[1]["symbol"] == symbol

    def test_get_watchlist_success_returns_copy(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting watchlist returns a copy."""
        # Arrange
        symbols = {"BTC-PERP", "ETH-PERP", "SOL-PERP"}
        portfolio_tracker.watchlist.update(symbols)

        # Act
        returned_watchlist = portfolio_tracker.get_watchlist()

        # Assert
        assert returned_watchlist == symbols
        assert returned_watchlist is not portfolio_tracker.watchlist  # Should be a copy

        # Modify returned copy should not affect original
        returned_watchlist.add("DOGE-PERP")
        assert "DOGE-PERP" not in portfolio_tracker.watchlist

    def test_get_watchlist_success_empty_watchlist(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test getting empty watchlist."""
        # Act
        watchlist = portfolio_tracker.get_watchlist()

        # Assert
        assert watchlist == set()
        assert isinstance(watchlist, set)

    # ==================== EDGE CASES ====================

    def test_add_symbol_to_watchlist_edge_duplicate_symbol(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test adding symbol that's already in watchlist."""
        # Arrange
        symbol = "BTC-PERP"
        portfolio_tracker.watchlist.add(symbol)

        # Act
        with patch("cyberdelta.core.portfolio_tracker.logger") as mock_logger:
            portfolio_tracker.add_symbol_to_watchlist(symbol)

        # Assert
        assert symbol in portfolio_tracker.watchlist
        # Should not log when symbol already exists
        mock_logger.info.assert_not_called()

    def test_remove_symbol_from_watchlist_edge_nonexistent_symbol(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test removing symbol that's not in watchlist."""
        # Arrange
        symbol = "NONEXISTENT-PERP"
        assert symbol not in portfolio_tracker.watchlist

        # Act
        with patch("cyberdelta.core.portfolio_tracker.logger") as mock_logger:
            portfolio_tracker.remove_symbol_from_watchlist(symbol)

        # Assert
        assert symbol not in portfolio_tracker.watchlist
        # Should not log when symbol doesn't exist
        mock_logger.info.assert_not_called()

    def test_add_symbol_to_watchlist_edge_empty_string(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test adding empty string to watchlist."""
        # Act
        portfolio_tracker.add_symbol_to_watchlist("")

        # Assert
        assert "" in portfolio_tracker.watchlist

    def test_add_symbol_to_watchlist_edge_special_characters(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test adding symbol with special characters."""
        # Arrange
        symbol = "BTC/USD-PERP_Q24"

        # Act
        portfolio_tracker.add_symbol_to_watchlist(symbol)

        # Assert
        assert symbol in portfolio_tracker.watchlist


class TestPortfolioTrackerActiveSymbols:
    """Test suite for active symbols management."""

    # ==================== SUCCESS CASES ====================

    def test_get_active_symbols_success_with_positions(
        self, portfolio_tracker: PortfolioTracker, sample_derivative_position: DerivativePosition
    ) -> None:
        """Test getting active symbols when positions exist."""
        # Arrange
        exchange_id = "hyperliquid"
        portfolio_tracker.positions[exchange_id]["BTC-PERP"] = sample_derivative_position

        # Act
        active_symbols = portfolio_tracker.get_active_symbols()

        # Assert
        assert "BTC-PERP" in active_symbols
        assert isinstance(active_symbols, set)

    def test_get_active_symbols_success_with_orders(
        self, portfolio_tracker: PortfolioTracker, sample_order: Order
    ) -> None:
        """Test getting active symbols when orders exist."""
        # Arrange
        exchange_id = "hyperliquid"
        portfolio_tracker.orders[exchange_id]["test_order_1"] = sample_order

        # Act
        active_symbols = portfolio_tracker.get_active_symbols()

        # Assert
        assert "BTC-PERP" in active_symbols

    def test_get_active_symbols_success_returns_copy(
        self, portfolio_tracker: PortfolioTracker, sample_derivative_position: DerivativePosition
    ) -> None:
        """Test that get_active_symbols returns a copy."""
        # Arrange
        exchange_id = "hyperliquid"
        portfolio_tracker.positions[exchange_id]["BTC-PERP"] = sample_derivative_position
        portfolio_tracker.update_active_symbols()

        # Act
        returned_symbols = portfolio_tracker.get_active_symbols()

        # Assert
        assert returned_symbols is not portfolio_tracker.active_symbols  # Should be a copy

        # Modify returned copy should not affect original
        returned_symbols.add("FAKE-SYMBOL")
        assert "FAKE-SYMBOL" not in portfolio_tracker.active_symbols

    def test_get_active_symbols_success_empty_when_no_data(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test getting active symbols when no positions or orders exist."""
        # Act
        active_symbols = portfolio_tracker.get_active_symbols()

        # Assert
        assert active_symbols == set()

    # ==================== EDGE CASES ====================

    def test_get_active_symbols_edge_with_zero_size_position(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test active symbols with zero-size position."""
        # Arrange
        zero_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="ETH-PERP",
            side=OrderSide.BUY,
            size=Decimal("0.0"),  # Zero size
            entry_price=None,
            timestamp=datetime.now(UTC),
        )
        portfolio_tracker.positions["hyperliquid"]["ETH-PERP"] = zero_position

        # Act
        active_symbols = portfolio_tracker.get_active_symbols()

        # Assert
        # Zero-size positions should not be considered active
        assert "ETH-PERP" not in active_symbols


class TestPortfolioTrackerPerformanceStats:
    """Test suite for performance statistics functionality."""

    # ==================== SUCCESS CASES ====================

    def test_get_performance_stats_success_empty_tracker(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test getting performance stats from empty tracker."""
        # Act
        stats = portfolio_tracker.get_performance_stats()

        # Assert
        assert isinstance(stats, dict)
        assert stats["background_tasks"] == 0
        assert stats["exchanges_tracked"] == 0  # Should be 0 for empty tracker
        assert stats["total_positions"] == 0
        assert stats["total_orders"] == 0
        assert stats["total_balances"] == 0
        assert stats["ticker_cache_size"] == 0
        assert "memory_limits" in stats
        assert isinstance(stats["memory_limits"], dict)

    def test_get_performance_stats_success_with_data(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_spot_balance: SpotBalance,
        sample_derivative_position: DerivativePosition,
        sample_order: Order,
        sample_ticker: Ticker,
    ) -> None:
        """Test getting performance stats with data present."""
        # Arrange
        exchange_id = "hyperliquid"

        # Add some data
        portfolio_tracker.balances[exchange_id]["USDC"] = sample_spot_balance
        portfolio_tracker.positions[exchange_id]["BTC-PERP"] = sample_derivative_position
        portfolio_tracker.orders[exchange_id]["test_order_1"] = sample_order
        portfolio_tracker.tickers["BTC-PERP"] = sample_ticker

        # Act
        stats = portfolio_tracker.get_performance_stats()

        # Assert
        assert stats["exchanges_tracked"] == 1
        assert stats["total_positions"] == 1
        assert stats["total_orders"] == 1
        assert stats["total_balances"] == 1
        assert stats["ticker_cache_size"] == 1

    def test_get_performance_stats_success_memory_limits_structure(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test that memory limits are properly structured."""
        # Act
        stats = portfolio_tracker.get_performance_stats()

        # Assert
        memory_limits = stats["memory_limits"]
        assert "max_ticker_entries" in memory_limits
        assert "data_retention_hours" in memory_limits
        assert "cleanup_interval_minutes" in memory_limits
        assert isinstance(memory_limits["max_ticker_entries"], int)
        assert isinstance(memory_limits["data_retention_hours"], int)
        assert isinstance(memory_limits["cleanup_interval_minutes"], int)

    def test_get_performance_stats_success_multiple_exchanges(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_derivative_position: DerivativePosition,
    ) -> None:
        """Test performance stats with multiple exchanges."""
        # Arrange
        # Create balances for multiple exchanges
        balance_hl = SpotBalance(
            exchange="hyperliquid",
            asset="USDC",
            total_quantity=Decimal(1000),
            available_quantity=Decimal(900),
            timestamp=datetime.now(UTC),
        )
        balance_bp = SpotBalance(
            exchange="backpack",
            asset="USDC",
            total_quantity=Decimal(500),
            available_quantity=Decimal(450),
            timestamp=datetime.now(UTC),
        )

        portfolio_tracker.balances["hyperliquid"]["USDC"] = balance_hl
        portfolio_tracker.balances["backpack"]["USDC"] = balance_bp
        portfolio_tracker.positions["hyperliquid"]["BTC-PERP"] = sample_derivative_position

        # Act
        stats = portfolio_tracker.get_performance_stats()

        # Assert
        assert stats["exchanges_tracked"] == 2
        assert stats["total_balances"] == 2
        assert stats["total_positions"] == 1

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_performance_stats_edge_exchange_locks_tracking(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test that exchange locks are properly tracked in performance stats."""
        # Arrange
        # Trigger creation of exchange locks indirectly through concurrent operations
        balance1 = SpotBalance(
            exchange="hyperliquid",
            asset="USDC",
            total_quantity=Decimal(1000),
            available_quantity=Decimal(900),
            timestamp=datetime.now(UTC),
        )
        balance2 = SpotBalance(
            exchange="backpack",
            asset="USDC",
            total_quantity=Decimal(500),
            available_quantity=Decimal(450),
            timestamp=datetime.now(UTC),
        )

        # Update balances which will create exchange locks internally
        await portfolio_tracker.update_balances("hyperliquid", {"USDC": balance1})
        await portfolio_tracker.update_balances("backpack", {"USDC": balance2})

        # Act
        stats = portfolio_tracker.get_performance_stats()

        # Assert
        # Exchange locks should be tracked after updating data from multiple exchanges
        assert "exchange_locks" in stats
        assert stats["exchange_locks"] >= 2


class TestPortfolioTrackerReset:
    """Test suite for reset functionality."""

    # ==================== SUCCESS CASES ====================

    def test_reset_success_clears_all_data(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_spot_balance: SpotBalance,
        sample_derivative_position: DerivativePosition,
        sample_order: Order,
        sample_ticker: Ticker,
    ) -> None:
        """Test that reset clears all portfolio data."""
        # Arrange
        exchange_id = "hyperliquid"

        # Add various data
        portfolio_tracker.balances[exchange_id]["USDC"] = sample_spot_balance
        portfolio_tracker.positions[exchange_id]["BTC-PERP"] = sample_derivative_position
        portfolio_tracker.orders[exchange_id]["test_order_1"] = sample_order
        portfolio_tracker.tickers["BTC-PERP"] = sample_ticker
        portfolio_tracker.watchlist.add("BTC-PERP")
        portfolio_tracker.active_symbols.add("BTC-PERP")
        portfolio_tracker.high_watermark = Decimal("10000.0")
        portfolio_tracker.realized_pnl = Decimal("500.0")
        portfolio_tracker.last_update_time[exchange_id] = datetime.now(UTC)

        # Act
        with patch("cyberdelta.core.portfolio_tracker.logger") as mock_logger:
            portfolio_tracker.reset()

        # Assert
        assert len(portfolio_tracker.balances) == 0
        assert len(portfolio_tracker.positions) == 0
        assert len(portfolio_tracker.orders) == 0
        assert len(portfolio_tracker.tickers) == 0
        assert len(portfolio_tracker.watchlist) == 0
        assert len(portfolio_tracker.active_symbols) == 0
        assert portfolio_tracker.high_watermark == Decimal("0.0")
        assert portfolio_tracker.realized_pnl == Decimal("0.0")
        # last_update_time will be reinitialized with default entries for enabled exchanges
        # Check that it has the default minimal timestamp values
        for exchange_id in portfolio_tracker.last_update_time:
            min_time_utc = datetime.min.replace(tzinfo=UTC)
            assert portfolio_tracker.last_update_time[exchange_id] == min_time_utc

        # Should log reset event
        mock_logger.info.assert_called_once_with("PortfolioTracker state has been reset.")

    def test_reset_success_reinitializes_data_structures(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test that reset reinitializes data structures."""
        # Act
        portfolio_tracker.reset()

        # Assert
        # After reset, data structures should be reinitialized
        # The _initialize_data_structures method should create default entries
        # for enabled exchanges
        assert isinstance(portfolio_tracker.balances, dict)
        assert isinstance(portfolio_tracker.positions, dict)
        assert isinstance(portfolio_tracker.orders, dict)

    def test_reset_success_empty_tracker(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test reset on already empty tracker."""
        # Act
        with patch("cyberdelta.core.portfolio_tracker.logger") as mock_logger:
            portfolio_tracker.reset()

        # Assert
        # Should still work and log the reset
        mock_logger.info.assert_called_once_with("PortfolioTracker state has been reset.")

    # ==================== EDGE CASES ====================

    def test_reset_edge_preserves_configuration(
        self, portfolio_tracker: PortfolioTracker, sample_ticker: Ticker
    ) -> None:
        """Test that reset preserves configuration settings through behavior."""
        # Arrange
        # Add many tickers to test memory limits are preserved
        for i in range(5):
            ticker = Ticker(
                symbol=f"TEST-{i}",
                exchange="test_exchange",
                bid=Decimal("100.0"),
                ask=Decimal("101.0"),
                price=Decimal("100.5"),
                timestamp=datetime.now(UTC),
            )
            portfolio_tracker.tickers[f"TEST-{i}"] = ticker

        # Act
        portfolio_tracker.reset()

        # Assert
        # Configuration should be preserved - test through behavior
        # Add new ticker after reset to verify memory limits still work
        portfolio_tracker.tickers["NEW-TICKER"] = sample_ticker
        assert "NEW-TICKER" in portfolio_tracker.tickers

        # Check that performance stats still include memory limits
        stats = portfolio_tracker.get_performance_stats()
        assert "memory_limits" in stats
        assert "max_ticker_entries" in stats["memory_limits"]

    def test_reset_edge_preserves_app_settings(
        self, portfolio_tracker: PortfolioTracker, mock_app_settings: Mock
    ) -> None:
        """Test that reset preserves app settings reference."""
        # Act
        portfolio_tracker.reset()

        # Assert
        assert portfolio_tracker.app_settings is mock_app_settings


class TestPortfolioTrackerUtilityMethods:
    """Test suite for utility and helper methods."""

    def test_get_relevant_symbols_success_combines_active_and_watchlist(
        self, portfolio_tracker: PortfolioTracker, sample_derivative_position: DerivativePosition
    ) -> None:
        """Test that get_relevant_symbols combines active symbols and watchlist."""
        # Arrange
        portfolio_tracker.positions["hyperliquid"]["BTC-PERP"] = sample_derivative_position
        portfolio_tracker.watchlist.add("ETH-PERP")
        portfolio_tracker.watchlist.add("SOL-PERP")
        portfolio_tracker.update_active_symbols()

        # Act
        relevant_symbols = portfolio_tracker.get_relevant_symbols()

        # Assert
        expected_symbols = {"BTC-PERP", "ETH-PERP", "SOL-PERP"}
        assert relevant_symbols == expected_symbols

    def test_get_relevant_symbols_success_deduplicates(
        self, portfolio_tracker: PortfolioTracker, sample_derivative_position: DerivativePosition
    ) -> None:
        """Test that get_relevant_symbols handles duplicates correctly."""
        # Arrange
        portfolio_tracker.positions["hyperliquid"]["BTC-PERP"] = sample_derivative_position
        portfolio_tracker.watchlist.add("BTC-PERP")  # Same symbol in both
        portfolio_tracker.update_active_symbols()

        # Act
        relevant_symbols = portfolio_tracker.get_relevant_symbols()

        # Assert
        assert "BTC-PERP" in relevant_symbols
        assert len(relevant_symbols) == 1  # No duplicates

    def test_update_active_symbols_success_identifies_positions(
        self, portfolio_tracker: PortfolioTracker, sample_derivative_position: DerivativePosition
    ) -> None:
        """Test that update_active_symbols identifies symbols with positions."""
        # Arrange
        portfolio_tracker.positions["hyperliquid"]["BTC-PERP"] = sample_derivative_position

        # Act
        portfolio_tracker.update_active_symbols()

        # Assert
        assert "BTC-PERP" in portfolio_tracker.active_symbols

    def test_update_active_symbols_success_identifies_orders(
        self, portfolio_tracker: PortfolioTracker, sample_order: Order
    ) -> None:
        """Test that update_active_symbols identifies symbols with orders."""
        # Arrange
        portfolio_tracker.orders["hyperliquid"]["test_order_1"] = sample_order

        # Act
        portfolio_tracker.update_active_symbols()

        # Assert
        assert "BTC-PERP" in portfolio_tracker.active_symbols

    def test_update_active_symbols_success_excludes_zero_positions(
        self, portfolio_tracker: PortfolioTracker
    ) -> None:
        """Test that update_active_symbols excludes zero-size positions."""
        # Arrange
        zero_position = DerivativePosition(
            exchange="hyperliquid",
            symbol="ETH-PERP",
            side=OrderSide.BUY,
            size=Decimal("0.0"),
            entry_price=None,
            timestamp=datetime.now(UTC),
        )
        portfolio_tracker.positions["hyperliquid"]["ETH-PERP"] = zero_position

        # Act
        portfolio_tracker.update_active_symbols()

        # Assert
        assert "ETH-PERP" not in portfolio_tracker.active_symbols
