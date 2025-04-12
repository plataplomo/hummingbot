#!/usr/bin/env python
"""Tests for RiskManager portfolio constraint checking logic."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.risk_manager import RiskManager

# Note: Fixtures risk_manager, mock_portfolio_tracker
#       are provided by tests/unit/risk/conftest.py

class TestRiskManagerConstraints:
    """Test suite for RiskManager constraint checking (_check_portfolio_constraints)."""

    def test_check_portfolio_constraints(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test portfolio constraint checking logic (passing case)."""
        # --- Arrange (Passing Case) ---
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("10000.0")
        mock_portfolio_tracker.get_exchange_exposure.return_value = Decimal("5000.0")
        mock_portfolio_tracker.get_exchange_collateral_balance.return_value = Decimal("50000.0")

        # --- Act & Assert (Passing Case) ---
        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", Decimal("1000.0"), Decimal("1000.0")
            )
            is True
        ), "Expected constraints to pass with default mocks"

    def test_check_portfolio_constraints_fail_total_exposure(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test failure due to exceeding total exposure."""
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("19500.0")
        mock_portfolio_tracker.get_exchange_collateral_balance.return_value = Decimal("50000.0")

        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", Decimal("1000.0"), Decimal("1000.0")
            )
            is False
        ), "Expected failure due to total exposure limit"

    def test_check_portfolio_constraints_fail_leverage(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test failure due to exceeding leverage."""
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("10000.0")
        mock_portfolio_tracker.get_exchange_collateral_balance.return_value = Decimal("100.0")

        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", Decimal("1000.0"), Decimal("1000.0")
            )
            is False
        ), "Expected failure due to leverage limit" 