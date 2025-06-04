#!/usr/bin/env python
"""Tests for RiskManager portfolio constraint checking logic through public interface."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Note: Fixtures risk_manager, mock_portfolio_tracker
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerConstraints:
    """Test suite for RiskManager constraint checking through public interface."""

    @pytest.mark.asyncio
    async def test_portfolio_constraints_pass_through_size_opportunity(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock,
    ) -> None:
        """Test portfolio constraint checking logic (passing case) through public interface."""
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("3000.0")
        # Create a minimal valid ArbitrageOpportunity
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("30000"),
            short_price=Decimal("29900"),
            long_funding_rate=Decimal("0.01"),
            short_funding_rate=Decimal("0.005"),
            net_funding_differential=Decimal("0.005"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # Test through public interface - size_opportunity should succeed
        result = await risk_manager.size_opportunity(opportunity)
        assert result is not None, (
            "Expected opportunity to be sized successfully with valid constraints"
        )

    @pytest.mark.asyncio
    async def test_portfolio_constraints_fail_through_size_opportunity(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock,
    ) -> None:
        """Test failure due to exceeding constraints through public interface."""
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("1000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal(
            "10000.0",
        )  # High leverage
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("30000"),
            short_price=Decimal("29900"),
            long_funding_rate=Decimal("0.01"),
            short_funding_rate=Decimal("0.005"),
            net_funding_differential=Decimal("0.005"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # Test through public interface - size_opportunity should fail due to constraints
        result = await risk_manager.size_opportunity(opportunity)
        assert result is None, "Expected opportunity to be rejected due to constraint violations"
