"""Tests for RiskManager portfolio constraint checking logic through public interface."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.risk_manager import RiskManager
from tests.common_symbols import BTC_HL
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Note: Fixtures risk_manager, mock_portfolio_state_manager
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerConstraints:
    """Test suite for RiskManager constraint checking through public interface."""

    @pytest.mark.asyncio
    async def test_portfolio_constraints_pass_through_size_opportunity(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
    ) -> None:
        """Test portfolio constraint checking logic (passing case) through public interface."""
        # Use simple sizing path for easier testing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal("1000.0")

        # Create risk manager with simple path
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        mock_portfolio_state_manager.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal("3000.0")
        mock_portfolio_state_manager.get_exchange_balance.return_value = MagicMock(
            total_quantity=Decimal(50000),
            available_quantity=Decimal(50000),
        )
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
        mock_circuit_breaker_system.can_execute.return_value = (True, None)

        # Create a minimal valid ArbitrageOpportunity
        opportunity = ArbitrageOpportunity(
            symbol=BTC_HL.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal(30000),
            short_price=Decimal(29900),
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
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
    ) -> None:
        """Test failure due to exceeding constraints through public interface."""
        # Use simple sizing path with low max exposure to trigger constraint failure
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal("1000.0")
        mock_config.risk.global_risk.max_total_exposure_usd = Decimal("5000.0")  # Low limit

        # Create risk manager with simple path
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        mock_portfolio_state_manager.get_total_capital.return_value = Decimal("1000.0")
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(
            "4500.0",
        )  # High existing exposure; adding 1000 would exceed 5000 limit
        mock_portfolio_state_manager.get_exchange_balance.return_value = MagicMock(
            total_quantity=Decimal(1000),
            available_quantity=Decimal(1000),
        )
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
        mock_circuit_breaker_system.can_execute.return_value = (True, None)

        opportunity = ArbitrageOpportunity(
            symbol=BTC_HL.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal(30000),
            short_price=Decimal(29900),
            long_funding_rate=Decimal("0.01"),
            short_funding_rate=Decimal("0.005"),
            net_funding_differential=Decimal("0.005"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        # Test through public interface - size_opportunity should fail due to constraints
        result = await risk_manager.size_opportunity(opportunity)
        assert result is None, "Expected opportunity to be rejected due to constraint violations"
