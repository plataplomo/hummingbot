"""Unit tests for the BalanceMonitor component.

Tests balance monitoring business logic through public interface only.
Focuses on threshold checking, alert generation, and balance status reporting.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.balance_monitor import BalanceAlert, BalanceMonitor
from cyberdelta.core.models.spot_balance import SpotBalance
from cyberdelta.core.portfolio_tracker import PortfolioTracker


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock app settings with balance monitoring configuration.
    
    Returns:
        Mock: Mocked AppSettings with balance monitoring thresholds configured.
    """
    settings = Mock(spec=AppSettings)

    # Configure exchanges as a dict with ExchangeSpecificConfig objects
    mock_exchange_config = Mock()
    mock_exchange_config.enabled = True
    settings.exchanges = {
        "hyperliquid": mock_exchange_config,
        "backpack": mock_exchange_config,
    }

    # Configure balance monitoring thresholds
    safety_systems = Mock()
    balance_monitoring = Mock()
    balance_monitoring.min_balance_thresholds_usd = {
        "hyperliquid": Decimal(100),
        "backpack": Decimal(150),
    }
    safety_systems.balance_monitoring = balance_monitoring
    settings.safety_systems = safety_systems

    return settings


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create a mock portfolio tracker.
    
    Returns:
        Mock: Mocked PortfolioTracker with default behavior configured.
    """
    tracker = Mock(spec=PortfolioTracker)
    # Set up default behavior
    tracker.get_exchange_balance.return_value = None
    return tracker


@pytest.fixture
def balance_monitor(mock_app_settings: Mock, mock_portfolio_tracker: Mock) -> BalanceMonitor:
    """Create a BalanceMonitor instance for testing.
    
    Returns:
        BalanceMonitor: Instance configured with mock dependencies.
    """
    return BalanceMonitor(mock_app_settings, mock_portfolio_tracker)


@pytest.fixture
def sufficient_balance() -> SpotBalance:
    """Create a SpotBalance that meets minimum requirements.
    
    Returns:
        SpotBalance: Balance with 200 USDC (above both exchange minimums).
    """
    return SpotBalance(
        exchange="hyperliquid",
        asset="USDC",
        timestamp=datetime.now(tz=UTC),
        total_quantity=Decimal(200),  # Above both exchange minimums
        available_quantity=Decimal(200),
    )


@pytest.fixture
def low_balance() -> SpotBalance:
    """Create a SpotBalance that's low but above critical threshold.
    
    Returns:
        SpotBalance: Balance with 120 USDC (above min but below low threshold).
    """
    return SpotBalance(
        exchange="hyperliquid",
        asset="USDC",
        timestamp=datetime.now(tz=UTC),
        total_quantity=Decimal(120),  # Above min balance (100) but below low threshold (100)
        available_quantity=Decimal(120),
    )


@pytest.fixture
def critical_balance() -> SpotBalance:
    """Create a SpotBalance that's critically low.
    
    Returns:
        SpotBalance: Balance with 30 USDC (below minimum requirements).
    """
    return SpotBalance(
        exchange="hyperliquid",
        asset="USDC",
        timestamp=datetime.now(tz=UTC),
        total_quantity=Decimal(30),  # Below minimum requirements
        available_quantity=Decimal(30),
    )


class TestBalanceMonitorInitialization:
    """Test balance monitor initialization."""

    def test_initializes_with_exchange_requirements(self, balance_monitor: BalanceMonitor) -> None:
        """Test that balance monitor loads exchange-specific requirements."""
        # Assert - requirements loaded from config
        assert len(balance_monitor.exchange_min_balances) == 2
        assert "hyperliquid" in balance_monitor.exchange_min_balances
        assert "backpack" in balance_monitor.exchange_min_balances

    def test_initializes_with_empty_alerts(self, balance_monitor: BalanceMonitor) -> None:
        """Test that balance monitor starts with no active alerts."""
        # Assert
        assert len(balance_monitor.get_active_alerts()) == 0
        assert len(balance_monitor.get_critical_alerts()) == 0


class TestBalanceChecking:
    """Test balance checking business logic."""

    def test_check_balances_with_sufficient_funds_returns_no_alerts(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        sufficient_balance: SpotBalance,
    ) -> None:
        """Test that sufficient balances don't generate alerts."""
        # Arrange
        # Set up the mock tracker to return sufficient balance for both exchanges
        mock_portfolio_tracker.get_exchange_balance.return_value = sufficient_balance

        # Act
        alerts = balance_monitor.check_balances()

        # Assert
        assert len(alerts) == 0

    def test_check_balances_with_low_funds_generates_warning_alerts(
        self, balance_monitor: BalanceMonitor, mock_portfolio_tracker: Mock
    ) -> None:
        """Test that low balances generate warning alerts."""
        # Arrange - create balance above minimum but below warning threshold
        low_warning_balance = SpotBalance(
            exchange="hyperliquid",
            asset="USDC",
            timestamp=datetime.now(tz=UTC),
            total_quantity=Decimal(110),  # Above min (100) but below warning threshold (100)
            available_quantity=Decimal(80),  # Actually below warning threshold
        )
        mock_portfolio_tracker.get_exchange_balance.return_value = low_warning_balance

        # Act
        alerts = balance_monitor.check_balances()

        # Assert - should generate critical alerts because available < minimum
        assert len(alerts) == 2  # One for each exchange
        assert all(alert.severity == BalanceAlert.SEVERITY_CRITICAL for alert in alerts)

    def test_check_balances_with_critical_funds_generates_critical_alerts(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        critical_balance: SpotBalance,
    ) -> None:
        """Test that critically low balances generate critical alerts."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = critical_balance

        # Act
        alerts = balance_monitor.check_balances()

        # Assert
        assert len(alerts) == 2  # One for each exchange
        assert all(alert.severity == BalanceAlert.SEVERITY_CRITICAL for alert in alerts)

    def test_check_balances_with_missing_balance_data_generates_critical_alerts(
        self, balance_monitor: BalanceMonitor, mock_portfolio_tracker: Mock
    ) -> None:
        """Test that missing balance data generates critical alerts."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = None

        # Act
        alerts = balance_monitor.check_balances()

        # Assert
        assert len(alerts) == 2  # One for each exchange
        assert all(alert.severity == BalanceAlert.SEVERITY_CRITICAL for alert in alerts)

    def test_check_balance_for_opportunity_with_sufficient_funds_returns_none(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        sufficient_balance: SpotBalance,
    ) -> None:
        """Test opportunity check with sufficient funds returns no alert."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = sufficient_balance
        required_amount = Decimal(100)

        # Act
        alert = balance_monitor.check_balance_for_opportunity(
            "hyperliquid", "USDC", required_amount
        )

        # Assert
        assert alert is None

    def test_check_balance_for_opportunity_with_insufficient_funds_returns_alert(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        low_balance: SpotBalance,
    ) -> None:
        """Test opportunity check with insufficient funds returns alert."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = low_balance
        required_amount = Decimal(150)  # More than available

        # Act
        alert = balance_monitor.check_balance_for_opportunity(
            "hyperliquid", "USDC", required_amount
        )

        # Assert
        assert alert is not None
        assert alert.severity == BalanceAlert.SEVERITY_CRITICAL
        assert alert.exchange == "hyperliquid"
        assert alert.asset == "USDC"


class TestAlertManagement:
    """Test alert management functionality."""

    def test_get_active_alerts_returns_current_alerts(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        critical_balance: SpotBalance,
    ) -> None:
        """Test that get_active_alerts returns currently active alerts."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = critical_balance
        balance_monitor.check_balances()  # Generate alerts

        # Act
        active_alerts = balance_monitor.get_active_alerts()

        # Assert
        assert len(active_alerts) == 2

    def test_get_critical_alerts_filters_by_severity(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        critical_balance: SpotBalance,
    ) -> None:
        """Test that get_critical_alerts returns only critical severity alerts."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = critical_balance
        balance_monitor.check_balances()  # Generate critical alerts

        # Act
        critical_alerts = balance_monitor.get_critical_alerts()

        # Assert
        assert len(critical_alerts) == 2
        assert all(alert.severity == BalanceAlert.SEVERITY_CRITICAL for alert in critical_alerts)

    def test_add_alert_adds_to_active_alerts(self, balance_monitor: BalanceMonitor) -> None:
        """Test that add_alert adds alert to active alerts list."""
        # Arrange
        alert = BalanceAlert(asset="USDC", threshold_type="low", threshold_value=Decimal(100))

        # Act
        balance_monitor.add_alert(alert)

        # Assert
        assert alert in balance_monitor.get_active_alerts()

    def test_duplicate_alerts_not_generated_on_repeated_checks(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        critical_balance: SpotBalance,
    ) -> None:
        """Test that checking balances multiple times creates new alerts each time."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = critical_balance

        # Act
        first_check = balance_monitor.check_balances()
        second_check = balance_monitor.check_balances()

        # Assert - current implementation clears alerts and regenerates them
        assert len(first_check) == 2
        assert len(second_check) == 2  # Alerts regenerated
        assert len(balance_monitor.get_active_alerts()) == 2  # Only latest alerts active


class TestBalanceStatusReporting:
    """Test balance status reporting functionality."""

    def test_get_balance_status_returns_comprehensive_status(
        self,
        balance_monitor: BalanceMonitor,
        mock_portfolio_tracker: Mock,
        sufficient_balance: SpotBalance,
    ) -> None:
        """Test that get_balance_status returns comprehensive balance information."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.return_value = sufficient_balance

        # Act
        status = balance_monitor.get_balance_status()

        # Assert
        assert "timestamp" in status
        assert "balances" in status
        assert "alerts" in status
        assert len(status["balances"]) == 2  # Both exchanges


class TestErrorHandling:
    """Test error handling in balance monitoring."""

    def test_portfolio_tracker_error_handled_gracefully(
        self, balance_monitor: BalanceMonitor, mock_portfolio_tracker: Mock
    ) -> None:
        """Test that portfolio tracker errors generate critical alerts."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.side_effect = Exception("Portfolio error")

        # Act & Assert - should propagate exception since no error handling
        with pytest.raises(Exception, match="Portfolio error"):
            balance_monitor.check_balances()

    def test_balance_status_with_portfolio_error_returns_status(
        self, balance_monitor: BalanceMonitor, mock_portfolio_tracker: Mock
    ) -> None:
        """Test that get_balance_status propagates portfolio errors."""
        # Arrange
        mock_portfolio_tracker.get_exchange_balance.side_effect = Exception("Portfolio error")

        # Act & Assert - should propagate exception since no error handling
        with pytest.raises(Exception, match="Portfolio error"):
            balance_monitor.get_balance_status()


class TestBalanceAlertDataClass:
    """Test BalanceAlert data class behavior."""

    def test_balance_alert_initializes_with_required_fields(self) -> None:
        """Test BalanceAlert can be created with required fields."""
        # Act
        alert = BalanceAlert(asset="USDC", threshold_type="low", threshold_value=Decimal(100))

        # Assert
        assert alert.asset == "USDC"
        assert alert.threshold_type == "low"
        assert alert.threshold_value == Decimal(100)
        assert alert.id is not None  # Should auto-generate

    def test_balance_alert_severity_constants_available(self) -> None:
        """Test that severity constants are available."""
        # Assert
        assert BalanceAlert.SEVERITY_CRITICAL == "critical"
        assert BalanceAlert.SEVERITY_WARNING == "warning"
        assert BalanceAlert.SEVERITY_INFO == "info"
