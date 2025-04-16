#!/usr/bin/env python
"""Tests for RiskManager portfolio constraint checking logic."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager

# Note: Fixtures risk_manager, mock_portfolio_tracker
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerConstraints:
    """Test suite for RiskManager constraint checking (_check_portfolio_constraints)."""

    def test_check_portfolio_constraints(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test portfolio constraint checking logic (passing case)."""
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("10000.0")
        # Create a minimal valid ArbitrageOpportunity
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price="30000",
            short_price="29900",
            long_funding_rate="0.01",
            short_funding_rate="0.005",
            net_funding_differential="0.005",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # Direct access to protected method is justified here for white-box testing;
        # no public interface exposes this logic.
        is_valid, _ = risk_manager._check_portfolio_constraints(Decimal("1000.0"), opportunity)
        assert is_valid, "Expected constraints to pass with default mocks"

    def test_check_portfolio_constraints_fail_total_exposure(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test failure due to exceeding total exposure."""
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("20000.0")
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("19500.0")
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price="30000",
            short_price="29900",
            long_funding_rate="0.01",
            short_funding_rate="0.005",
            net_funding_differential="0.005",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # Direct access to protected method is justified here for white-box testing;
        # no public interface exposes this logic.
        is_valid, _ = risk_manager._check_portfolio_constraints(Decimal("1000.0"), opportunity)
        assert not is_valid, "Expected failure due to total exposure limit"

    def test_check_portfolio_constraints_fail_leverage(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test failure due to exceeding leverage."""
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("1000.0")
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("10000.0")
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price="30000",
            short_price="29900",
            long_funding_rate="0.01",
            short_funding_rate="0.005",
            net_funding_differential="0.005",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # Direct access to protected method is justified here for white-box testing;
        # no public interface exposes this logic.
        is_valid, _ = risk_manager._check_portfolio_constraints(Decimal("1000.0"), opportunity)
        assert not is_valid, "Expected failure due to leverage limit"
