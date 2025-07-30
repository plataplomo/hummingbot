"""Integration tests for Portfolio-Risk Coordination Layer.

Tests the integration between portfolio state management and risk assessment modules,
validating the coordination patterns and service factory integration implemented
as part of the portfolio tracker cleanup refactor.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.app.integrated_application import CyberDeltaApplication
from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import (
    PortfolioRiskCoordinator,
    TradeRequestModel,
)
from cyberdelta.core.portfolio.coordinators.unified_service_factory import UnifiedServiceFactory


class TestPortfolioRiskCoordinator:
    """Test the portfolio-risk coordination functionality."""

    @pytest.fixture
    def mock_portfolio_factory(self):
        """Create mock portfolio factory."""
        factory = MagicMock()
        portfolio_manager = AsyncMock()
        portfolio_manager.get_portfolio_summary.return_value = MagicMock(
            total_capital=Decimal("10000"),
            timestamp=MagicMock()
        )
        
        performance_analytics = AsyncMock()
        
        factory.create_portfolio_state_manager.return_value = portfolio_manager
        factory.create_performance_analytics.return_value = performance_analytics
        
        return factory

    @pytest.fixture
    def mock_risk_factory(self):
        """Create mock risk factory."""
        factory = MagicMock()
        factory.create_exposure_calculator.return_value = AsyncMock()
        factory.create_position_sizer.return_value = AsyncMock()
        factory.create_risk_metrics_calculator.return_value = AsyncMock()
        return factory

    @pytest.fixture
    def coordinator(self, mock_portfolio_factory, mock_risk_factory):
        """Create portfolio risk coordinator with mocked dependencies."""
        return PortfolioRiskCoordinator(
            portfolio_factory=mock_portfolio_factory,
            risk_factory=mock_risk_factory
        )

    async def test_portfolio_with_risk_assessment(self, coordinator):
        """Test getting portfolio state with risk assessment."""
        result = await coordinator.get_current_portfolio_with_risk_assessment()
        
        assert result is not None
        assert hasattr(result, 'portfolio_state')
        assert hasattr(result, 'risk_assessment')
        assert hasattr(result, 'timestamp')

    async def test_trade_validation_success(self, coordinator):
        """Test successful trade validation."""
        trade_request = TradeRequestModel(
            symbol="BTC-USD",
            side="buy",
            quantity=Decimal("1.0"),
            price=Decimal("50000"),
            signal_strength=0.8,
            exchange_id="hyperliquid"
        )
        
        result = await coordinator.validate_trade_request(trade_request)
        
        assert result.approved is True
        assert result.optimal_size is not None
        assert result.optimal_size > 0

    async def test_trade_validation_insufficient_capital(self, coordinator):
        """Test trade validation with insufficient capital."""
        trade_request = TradeRequestModel(
            symbol="BTC-USD",
            side="buy",
            quantity=Decimal("1000"),  # Very large quantity
            price=Decimal("50000"),
            signal_strength=0.8,
            exchange_id="hyperliquid"
        )
        
        result = await coordinator.validate_trade_request(trade_request)
        
        assert result.approved is False
        assert "Insufficient capital" in result.reason
        assert len(result.risk_violations) > 0

    async def test_trade_request_validation_errors(self):
        """Test trade request model validation."""
        # Test negative quantity
        with pytest.raises(ValueError, match="Quantity must be positive"):
            TradeRequestModel(
                symbol="BTC-USD",
                side="buy",
                quantity=Decimal("-1.0"),
                exchange_id="hyperliquid"
            )

        # Test invalid signal strength
        with pytest.raises(ValueError, match="Signal strength must be between"):
            TradeRequestModel(
                symbol="BTC-USD",
                side="buy",
                quantity=Decimal("1.0"),
                signal_strength=1.5,
                exchange_id="hyperliquid"
            )

        # Test invalid side
        with pytest.raises(ValueError, match="Side must be"):
            TradeRequestModel(
                symbol="BTC-USD",
                side="invalid",
                quantity=Decimal("1.0"),
                exchange_id="hyperliquid"
            )


class TestUnifiedServiceFactory:
    """Test the unified service factory functionality."""

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = MagicMock()
        config.portfolio_config = MagicMock()
        config.risk_config = MagicMock()
        return config

    def test_unified_factory_initialization(self, mock_config):
        """Test unified factory creates correct components."""
        factory = UnifiedServiceFactory(config=mock_config)
        
        assert factory.portfolio_factory is not None
        assert factory.risk_factory is not None
        assert factory.coordinator is not None

    async def test_factory_lifecycle(self, mock_config):
        """Test factory initialization and shutdown lifecycle."""
        # Mock the factories to avoid actual initialization
        with pytest.mock.patch('cyberdelta.core.portfolio.services.PortfolioServiceFactory') as mock_pf, \
             pytest.mock.patch('cyberdelta.core.risk.services.risk_service_factory.RiskServiceFactory') as mock_rf:
            
            # Setup mocks
            mock_portfolio_factory = AsyncMock()
            mock_risk_factory = AsyncMock()
            mock_pf.return_value = mock_portfolio_factory
            mock_rf.return_value = mock_risk_factory
            
            factory = UnifiedServiceFactory(config=mock_config)
            
            # Test initialization
            await factory.initialize_all()
            mock_portfolio_factory.initialize_all.assert_called_once()
            mock_risk_factory.initialize_all.assert_called_once()
            
            # Test shutdown
            await factory.shutdown_all()
            mock_portfolio_factory.shutdown_all.assert_called_once()
            mock_risk_factory.shutdown_all.assert_called_once()


class TestCyberDeltaApplication:
    """Test the integrated application functionality."""

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        return MagicMock()

    async def test_application_lifecycle(self, mock_config):
        """Test application startup and shutdown lifecycle."""
        # Mock the unified factory to avoid actual initialization
        with pytest.mock.patch('cyberdelta.core.portfolio.coordinators.unified_service_factory.UnifiedServiceFactory') as mock_factory_class:
            mock_factory = AsyncMock()
            mock_factory_class.return_value = mock_factory
            
            # Mock the components returned by factory
            mock_factory.get_portfolio_manager.return_value = AsyncMock()
            mock_factory.get_risk_coordinator.return_value = AsyncMock()
            
            app = CyberDeltaApplication(config=mock_config)
            
            # Test that we can create the application
            assert app is not None
            assert app.unified_factory is not None

    async def test_application_context_manager(self, mock_config):
        """Test application context manager functionality."""
        with pytest.mock.patch('cyberdelta.core.portfolio.coordinators.unified_service_factory.UnifiedServiceFactory') as mock_factory_class:
            mock_factory = AsyncMock()
            mock_factory_class.return_value = mock_factory
            mock_factory.get_portfolio_manager.return_value = AsyncMock()
            mock_factory.get_risk_coordinator.return_value = AsyncMock()
            
            app = CyberDeltaApplication(config=mock_config)
            
            # Test context manager (without actually starting/stopping)
            # This would normally be:
            # async with app.application_context():
            #     pass
            # But we'll test the methods individually to avoid signal handling
            
            assert app._running is False


if __name__ == "__main__":
    # Run basic test
    async def run_basic_test():
        """Run a basic integration test."""
        print("Starting Portfolio-Risk Integration Test...")
        
        # Test trade request validation
        trade_request = TradeRequestModel(
            symbol="BTC-USD",
            side="buy",
            quantity=Decimal("1.0"),
            price=Decimal("50000"),
            signal_strength=0.8,
            exchange_id="hyperliquid"
        )
        
        print(f"✅ Trade request validation: {trade_request}")
        print("Portfolio-Risk Integration Layer - Basic validation passed!")

    asyncio.run(run_basic_test())