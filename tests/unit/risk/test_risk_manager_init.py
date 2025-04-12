#!/usr/bin/env python
"""Tests for RiskManager initialization."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.risk_manager import RiskManager

# Note: Fixtures risk_manager, mock_config, mock_portfolio_tracker
#       are provided by tests/unit/risk/conftest.py

class TestRiskManagerInit:
    """Test suite for RiskManager initialization."""

    def test_init(
        self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test initializing the risk manager."""
        # Verify risk parameters were loaded and converted to Decimal
        # Using known values from mock_config_values fixture
        assert risk_manager.max_position_size == Decimal("1000.0")
        assert risk_manager.max_total_exposure == Decimal("20000.0")
        assert risk_manager.max_leverage == Decimal("3.0")
        assert risk_manager.min_exchange_balance == Decimal("10.0")
        assert risk_manager.min_net_funding_differential == Decimal("0.0001")
        # Verify references to dependencies
        assert risk_manager.config == mock_config
        assert risk_manager.portfolio_tracker == mock_portfolio_tracker
        # Assert optional dependencies are set (even if None from fixture)
        assert hasattr(risk_manager, 'circuit_breaker_system')
        assert hasattr(risk_manager, 'funding_rate_validator') 