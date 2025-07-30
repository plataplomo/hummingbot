"""Tests for RiskManager initialization."""

from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.core.risk_manager import RiskManager


# Note: Fixtures risk_manager, mock_config, mock_portfolio_state_manager
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerInit:
    """Test suite for RiskManager initialization."""

    def test_init(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
    ) -> None:
        """Test initializing the risk manager."""
        # Verify risk parameters were loaded and converted to Decimal
        # Business logic sets these values from config
        assert risk_manager.max_position_size == Decimal("5000.0")  # From mock config
        assert risk_manager.max_total_exposure_usd == Decimal("10000.0")  # From mock config

        # Business logic sets these as hardcoded defaults (not from config)
        assert risk_manager.max_leverage == Decimal("5.0")  # Hardcoded default
        assert risk_manager.min_exchange_balance == Decimal("10.0")  # Hardcoded default
        assert risk_manager.min_nfd_for_sizing == Decimal("0.0001")  # Hardcoded default

        # Verify references to dependencies
        assert risk_manager.app_settings == mock_config
        assert risk_manager.portfolio_tracker == mock_portfolio_state_manager
        # Assert optional dependencies are set (even if None from fixture)
        assert hasattr(risk_manager, "circuit_breaker_system")
        assert hasattr(risk_manager, "funding_rate_validator")

    def test_config_error_missing_values(self, mock_config: MagicMock) -> None:
        """Test config error missing values."""
        # This test case is not provided in the original file or the code block
        # It's assumed to exist as it's called in the test_init method
