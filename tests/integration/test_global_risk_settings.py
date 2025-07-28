"""Integration tests for GlobalRiskSettings integration and constraints.

This module tests the GlobalRiskSettings integration with the enhanced risk management system:
- Position size limits enforcement
- Total exposure constraints
- Portfolio-level risk management
- Cross-checker integration with GlobalRiskSettings
- Constraint validation integration
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal

# Mock types for testing
from typing import Protocol

import pytest

from cyberdelta.config import AppSettings
from cyberdelta.config.models.config_models import (
    GlobalRiskSettings,
)
from cyberdelta.core.models.spot_balance import SpotBalance
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import ConstraintContext
from cyberdelta.core.risk.constraints.orchestrator.constraint_validator import ConstraintValidator
from cyberdelta.core.risk.orchestrator.risk_manager_factory import RiskManagerFactory
from cyberdelta.core.risk.orchestrator.risk_manager_orchestrator import (
    ProcessingStatus,
)
from cyberdelta.core.risk.sizing.models.sizing_result import (
    SizedOpportunity,
    SizingResult,
    SizingStatus,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class Position(Protocol):
    """Protocol for position data."""

    symbol: str


class MockPortfolioTracker:
    """Mock portfolio tracker for testing."""

    def __init__(self, balance_ratio: float = 0.8) -> None:
        """Initialize the mock portfolio tracker."""
        self.balance_ratio = balance_ratio

    async def get_exchange_balance_ratio(self, exchange: str) -> float:
        """Return mock balance ratio."""
        return self.balance_ratio

    def get_total_capital(self) -> Decimal:
        """Get total available capital.
        
        Returns:
            Total capital amount of 100000.
        """
        return Decimal(100000)

    def get_exchange_balance(self, exchange: str, asset: str) -> Decimal:
        """Get balance for specific exchange and asset.
        
        Args:
            exchange: The exchange name.
            asset: The asset symbol.
            
        Returns:
            Mock balance of 1000 for any exchange/asset combination.
        """
        return Decimal(1000)

    def get_all_positions(self) -> Sequence[tuple[str, Position]]:
        """Get all current positions.
        
        Returns:
            Empty list of positions for testing.
        """
        return []

    async def get_current_drawdown(self) -> Decimal | None:
        """Get current portfolio drawdown.
        
        Returns:
            Fixed drawdown of 5% for testing.
        """
        return Decimal("0.05")  # 5% drawdown

    async def get_total_exposure_usd(self) -> Decimal:
        """Get total portfolio exposure in USD.
        
        Returns:
            Total exposure of 5000 USD.
        """
        return Decimal(5000)

    def get_exchange_balances(self, exchange: str) -> list[SpotBalance]:
        """Get all exchange balances.
        
        Args:
            exchange: The exchange name.
            
        Returns:
            List containing a single USDC balance with 1000 total and 800 available.
        """
        return [
            SpotBalance(
                exchange=exchange,
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal(1000),
                available_quantity=Decimal(800),
            )
        ]

    def get_total_portfolio_value(self) -> Decimal:
        """Get total portfolio value.
        
        Returns:
            Total portfolio value of 50000.
        """
        return Decimal(50000)


@pytest.fixture
def global_risk_settings() -> GlobalRiskSettings:
    """Create GlobalRiskSettings for testing.
    
    Returns:
        GlobalRiskSettings with constrained position and exposure limits.
    """
    return GlobalRiskSettings(
        max_position_usd=Decimal(5000),  # Lower limit for testing
        max_total_exposure_usd=Decimal(15000),  # Lower limit for testing
    )


@pytest.fixture
def constrained_app_settings(global_risk_settings: GlobalRiskSettings) -> AppSettings:
    """Create AppSettings with constrained GlobalRiskSettings.
    
    Args:
        global_risk_settings: The global risk settings to use.
        
    Returns:
        AppSettings configured with the provided global risk limits.
    """
    return AppSettings.model_validate({
        "general": {"version": "1.0.0", "environment": "test", "debug": True},
        "exchanges": {},
        "strategies": {"strategies_list": []},
        "risk": {
            "enabled": True,
            "global": {
                "max_position_usd": str(global_risk_settings.max_position_usd),
                "max_total_exposure_usd": str(global_risk_settings.max_total_exposure_usd),
            },
            "checkers": {},
            "sizing": {},
        },
        "execution": {"retry_attempts": 3, "timeout_seconds": 30},
        "safety_systems": {"max_portfolio_value_usd": "100000"},
        "monitoring": {"log_level": "INFO"},
        "portfolio_tracker": {"update_interval_seconds": 60},
    })


@pytest.fixture
def mock_portfolio_tracker() -> MockPortfolioTracker:
    """Create mock portfolio tracker.
    
    Returns:
        MockPortfolioTracker instance for testing.
    """
    return MockPortfolioTracker()


@pytest.fixture
def high_value_opportunity() -> ArbitrageOpportunity:
    """Create a high-value arbitrage opportunity for testing constraints.
    
    Returns:
        ArbitrageOpportunity with large price spread for constraint testing.
    """
    return ArbitrageOpportunity(
        symbol="BTC-USD",
        long_exchange="Hyperliquid",
        short_exchange="Backpack",
        long_price=Decimal(50000),
        short_price=Decimal(51000),  # Large spread for high value
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("0.0002"),
        timestamp=datetime.fromtimestamp(1234567890, tz=UTC),
        volatility=Decimal("0.1"),
        confidence_score=0.9,
    )


class TestGlobalRiskSettingsIntegration:
    """Tests for GlobalRiskSettings integration with the risk management system."""

    async def test_position_size_limit_enforcement(
        self,
        constrained_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        high_value_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test that position sizes are limited by GlobalRiskSettings.max_position_usd."""
        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=constrained_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        result = await orchestrator.process_opportunity(high_value_opportunity)

        if result.status == ProcessingStatus.APPROVED:
            # Position size should not exceed global limit
            assert (
                result.position_size_usd
                <= constrained_app_settings.risk.global_risk.max_position_usd
            )
            assert result.sized_opportunity is not None
            assert (
                result.sized_opportunity.total_size_usd
                <= constrained_app_settings.risk.global_risk.max_position_usd
            )

    async def test_total_exposure_constraint_enforcement(
        self,
        constrained_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        high_value_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test total exposure constraints with multiple positions."""
        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=constrained_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        # Process multiple opportunities to build up exposure
        opportunities: list[ArbitrageOpportunity] = []
        for i in range(5):
            opp = high_value_opportunity.model_copy(deep=True)
            opp.symbol = f"SYMBOL{i}"
            opp.long_price = Decimal(10000) + (i * 1000)
            opp.short_price = opp.long_price + Decimal(200)  # 2% spread
            opportunities.append(opp)

        approved_positions: list[SizedOpportunity] = []
        total_exposure = Decimal(0)

        for opportunity in opportunities:
            result = await orchestrator.process_opportunity(opportunity)

            if result.status == ProcessingStatus.APPROVED and result.sized_opportunity:
                # Add to portfolio
                orchestrator.add_position(result.sized_opportunity)
                approved_positions.append(result.sized_opportunity)
                total_exposure += result.position_size_usd

                # Check that total exposure doesn't exceed global limit
                portfolio_metrics = orchestrator.get_portfolio_metrics()
                assert (
                    portfolio_metrics["total_exposure"]
                    <= constrained_app_settings.risk.global_risk.max_total_exposure_usd
                )

    async def test_constraint_validator_with_global_limits(
        self,
        constrained_app_settings: AppSettings,
    ) -> None:
        """Test ConstraintValidator respects GlobalRiskSettings limits."""
        constraint_validator = ConstraintValidator(app_settings=constrained_app_settings)

        # Create a sized opportunity that exceeds global position limit
        sizing_result = SizingResult(
            status=SizingStatus.SUCCESS,
            position_size_usd=Decimal(10000),
            allocation_percentage=Decimal("0.1"),
        )
        oversized_opportunity = SizedOpportunity(
            opportunity=high_value_opportunity(),
            sizing_result=sizing_result,
            long_size_usd=Decimal(5000),
            short_size_usd=Decimal(5000),
            sizing_method="test",
        )

        # Create constraint context
        context = ConstraintContext(
            total_capital=Decimal(100000),
            available_capital=Decimal(100000),
            reserved_capital=Decimal(0),
            current_positions=[],
            current_allocations={},
            current_exchange_allocations={},
            current_leverage=Decimal("1.0"),
        )

        # Validate constraints
        result = await constraint_validator.validate_opportunity(oversized_opportunity, context)

        # Should fail due to position size exceeding global limit
        assert result.failed or result.has_warnings
        if result.violations:
            # Check for position size violation
            position_violations = [v for v in result.violations if "position" in v.message.lower()]
            assert len(position_violations) > 0

    async def test_global_risk_settings_in_different_configurations(
        self,
        mock_portfolio_tracker: MockPortfolioTracker,
        high_value_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test GlobalRiskSettings behavior with different sizing configurations."""
        # Test with Kelly sizing
        kelly_settings = AppSettings.model_validate({
            "general": {"version": "1.0.0", "environment": "test", "debug": True},
            "exchanges": {},
            "strategies": {"strategies_list": []},
            "risk": {
                "enabled": True,
                "global": {"max_position_usd": "3000", "max_total_exposure_usd": "10000"},
                "checkers": {
                    "enable_required_fields": True,
                    "enable_profitability": True,
                    "enable_price_sanity": True,
                    "enable_volatility": True,
                    "enable_funding_rate": False,
                    "enable_circuit_breaker": False,
                    "enable_balance": True,
                    "thresholds": {
                        "min_profitability": "0.001",
                        "max_volatility": "1.0",
                        "min_balance_ratio": "0.1",
                    },
                },
                "sizing": {
                    "method": "kelly",
                    "kelly_multiplier": "1.0",
                    "kelly_max_allocation": "1.0",
                    "kelly_min_allocation": "0.01",
                    "kelly_risk_free_rate": 0.02,
                    "min_position_size": "100",
                    "max_position_size": "50000",
                    "max_leverage": "5.0",
                    "max_portfolio_allocation": "1.0",
                    "total_capital": "50000",
                    "min_volatility": "0.001",
                    "max_volatility_bound": "2.0",
                    "volatility_lookback_hours": 24,
                    "enable_validation_factors": False,
                },
            },
            "execution": {"retry_attempts": 3, "timeout_seconds": 30},
            "safety_systems": {"max_portfolio_value_usd": "100000"},
            "monitoring": {"log_level": "INFO"},
            "portfolio_tracker": {"update_interval_seconds": 60},
        })

        kelly_orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=kelly_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        result = await kelly_orchestrator.process_opportunity(high_value_opportunity)

        if result.status == ProcessingStatus.APPROVED:
            # Even with Kelly sizing, should respect global limits
            assert result.position_size_usd <= kelly_settings.risk.global_risk.max_position_usd

    async def test_global_settings_validation_during_initialization(self) -> None:
        """Test that GlobalRiskSettings are properly validated during AppSettings creation."""
        # Test with invalid global settings (negative values)
        with pytest.raises(ValueError):  # Should raise validation error
            AppSettings.model_validate({
                "general": {"version": "1.0.0", "environment": "test", "debug": True},
                "exchanges": {},
                "strategies": {"strategies_list": []},
                "risk": {
                    "enabled": True,
                    "global": {
                        "max_position_usd": "-1000",  # Invalid negative value
                        "max_total_exposure_usd": "10000",
                    },
                    "checkers": {},
                    "sizing": {},
                },
                "execution": {"retry_attempts": 3, "timeout_seconds": 30},
                "safety_systems": {"max_portfolio_value_usd": "100000"},
                "monitoring": {"log_level": "INFO"},
                "portfolio_tracker": {"update_interval_seconds": 60},
            })

        # Test with valid global settings
        valid_settings = AppSettings.model_validate({
            "general": {"version": "1.0.0", "environment": "test", "debug": True},
            "exchanges": {},
            "strategies": {"strategies_list": []},
            "risk": {
                "enabled": True,
                "global": {"max_position_usd": "5000", "max_total_exposure_usd": "25000"},
                "checkers": {},
                "sizing": {},
            },
            "execution": {"retry_attempts": 3, "timeout_seconds": 30},
            "safety_systems": {"max_portfolio_value_usd": "100000"},
            "monitoring": {"log_level": "INFO"},
            "portfolio_tracker": {"update_interval_seconds": 60},
        })

        # Should create successfully
        assert valid_settings.risk.global_risk.max_position_usd == Decimal(5000)
        assert valid_settings.risk.global_risk.max_total_exposure_usd == Decimal(25000)

    async def test_global_settings_with_preset_configurations(
        self,
        mock_portfolio_tracker: MockPortfolioTracker,
        high_value_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test GlobalRiskSettings behavior with different preset configurations."""
        base_settings = AppSettings.model_validate({
            "general": {"version": "1.0.0", "environment": "test", "debug": True},
            "exchanges": {},
            "strategies": {"strategies_list": []},
            "risk": {
                "enabled": True,
                "global": {"max_position_usd": "2000", "max_total_exposure_usd": "8000"},
                "checkers": {},
                "sizing": {"total_capital": "50000"},
            },
            "execution": {"retry_attempts": 3, "timeout_seconds": 30},
            "safety_systems": {"max_portfolio_value_usd": "100000"},
            "monitoring": {"log_level": "INFO"},
            "portfolio_tracker": {"update_interval_seconds": 60},
        })

        # Test conservative preset
        conservative_orchestrator = RiskManagerFactory.create_from_preset(
            preset_name="conservative",
            base_app_settings=base_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        conservative_result = await conservative_orchestrator.process_opportunity(
            high_value_opportunity
        )

        # Test moderate preset
        moderate_orchestrator = RiskManagerFactory.create_from_preset(
            preset_name="moderate",
            base_app_settings=base_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        moderate_result = await moderate_orchestrator.process_opportunity(high_value_opportunity)

        # Test aggressive preset
        aggressive_orchestrator = RiskManagerFactory.create_from_preset(
            preset_name="aggressive",
            base_app_settings=base_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        aggressive_result = await aggressive_orchestrator.process_opportunity(
            high_value_opportunity
        )

        # All should respect global limits regardless of preset
        for result in [conservative_result, moderate_result, aggressive_result]:
            if result.status == ProcessingStatus.APPROVED:
                assert result.position_size_usd <= base_settings.risk.global_risk.max_position_usd

    async def test_portfolio_metrics_with_global_constraints(
        self,
        constrained_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        high_value_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test portfolio metrics calculation with global constraints."""
        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=constrained_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        # Process and add multiple positions
        for i in range(3):
            opportunity = high_value_opportunity.model_copy(deep=True)
            opportunity.symbol = f"TEST{i}"

            result = await orchestrator.process_opportunity(opportunity)

            if result.status == ProcessingStatus.APPROVED and result.sized_opportunity:
                orchestrator.add_position(result.sized_opportunity)

        # Get portfolio metrics
        metrics = orchestrator.get_portfolio_metrics()

        # Verify metrics respect global constraints
        assert (
            metrics["total_exposure"]
            <= constrained_app_settings.risk.global_risk.max_total_exposure_usd
        )

        # Individual positions should respect max position size
        for position in metrics["positions"]:
            assert position["size"] <= constrained_app_settings.risk.global_risk.max_position_usd

    async def test_capital_management_with_global_limits(
        self,
        constrained_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        high_value_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test capital management integration with global risk limits."""
        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=constrained_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        # Set total capital
        total_capital = Decimal(20000)
        orchestrator.set_capital(total_capital)

        result = await orchestrator.process_opportunity(high_value_opportunity)

        if result.status == ProcessingStatus.APPROVED and result.sized_opportunity:
            # Reserve capital for the position
            orchestrator.add_position(result.sized_opportunity)

            # Check available capital is reduced
            available_capital = orchestrator.available_capital
            assert available_capital < total_capital

            # Position size should still respect global limits
            assert (
                result.position_size_usd
                <= constrained_app_settings.risk.global_risk.max_position_usd
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
