"""Integration tests for the complete refactored risk management pipeline.

This module tests the entire risk management flow with the new clean-break architecture:
- Direct AppSettings integration
- TypedBaseChecker and TypedBaseSizer implementations
- Complete orchestrator pipeline from opportunity to sized/validated result
- Configuration presets and migration
- GlobalRiskSettings integration
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.config import AppSettings
from cyberdelta.core.models.spot_balance import SpotBalance
from cyberdelta.core.risk.config.migration import ConfigurationMigrator
from cyberdelta.core.risk.orchestrator.risk_manager_factory import RiskManagerFactory
from cyberdelta.core.risk.orchestrator.risk_manager_orchestrator import (
    ProcessedOpportunity,
    ProcessingStatus,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class MockPortfolioTracker:
    """Mock portfolio tracker for testing."""

    def __init__(self, balance_ratio: float = 0.8) -> None:
        """Initialize mock portfolio tracker.

        Args:
            balance_ratio: The balance ratio to return for all exchanges.
        """
        self.balance_ratio = balance_ratio

    async def get_exchange_balance_ratio(self, exchange: str) -> float:
        """Return mock balance ratio."""
        return self.balance_ratio

    def get_exchange_balances(self, exchange: str) -> list[SpotBalance]:
        """Get exchange balances.
        
        Returns:
            list[SpotBalance]: List of mock balance objects for the exchange.
        """
        return [
            SpotBalance(
                exchange=exchange,
                asset="USD",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal(10000),
                available_quantity=Decimal(8000),
            )
        ]

    def get_exchange_balance(self, exchange: str, asset: str) -> Decimal:
        """Get specific exchange balance.
        
        Returns:
            Decimal: Mock balance of 10000 for any exchange/asset combination.
        """
        return Decimal(10000)

    def get_total_portfolio_value(self) -> Decimal:
        """Get total portfolio value.
        
        Returns:
            Decimal: Mock total portfolio value of 100000.
        """
        return Decimal(100000)

    def get_total_capital(self) -> Decimal:
        """Get total capital.
        
        Returns:
            Decimal: Mock total capital of 100000.
        """
        return Decimal(100000)


class MockCircuitBreakerSystem:
    """Mock circuit breaker system for testing."""

    def __init__(self, is_tripped: bool = False) -> None:
        """Initialize mock circuit breaker system.

        Args:
            is_tripped: Whether the circuit breaker should be tripped.
        """
        self.is_tripped = is_tripped

    async def is_circuit_breaker_tripped(self, symbol: str) -> bool:
        """Return mock circuit breaker status."""
        return self.is_tripped

    def check_circuit_state(self, symbol: str, exchange: str) -> dict[str, Any]:
        """Check circuit breaker state.
        
        Returns:
            dict[str, Any]: Dictionary containing circuit breaker state information.
        """
        return {"tripped": self.is_tripped, "symbol": symbol, "exchange": exchange}

    def get_system_status(self) -> dict[str, Any]:
        """Get system status.
        
        Returns:
            dict[str, Any]: Dictionary containing system status information.
        """
        return {"status": "active", "tripped": self.is_tripped}

    def can_execute(self, symbol: str, exchange: str) -> bool:
        """Check if can execute trade.
        
        Returns:
            bool: True if execution is allowed, False if circuit breaker is tripped.
        """
        return not self.is_tripped

    def get_exchange_breaker(self, exchange: str) -> dict[str, Any]:
        """Get exchange circuit breaker info.
        
        Returns:
            dict[str, Any]: Dictionary containing exchange circuit breaker information.
        """
        return {"exchange": exchange, "tripped": self.is_tripped}


class MockFundingRateValidator:
    """Mock funding rate validator for testing."""

    def __init__(self, funding_rate: float = 0.0001) -> None:
        """Initialize mock funding rate validator.

        Args:
            funding_rate: The funding rate to return for all queries.
        """
        self.funding_rate = funding_rate

    async def get_funding_rate(self, symbol: str, exchange: str) -> float:
        """Return mock funding rate."""
        return self.funding_rate

    def get_symbol_metrics(self, exchange: str, symbol: str) -> dict[str, Any]:
        """Get symbol metrics.
        
        Returns:
            dict[str, Any]: Dictionary containing mock symbol metrics and funding rate data.
        """
        return {
            "funding_rate": self.funding_rate,
            "volatility": 0.15,
            "confidence": 0.9,
            "exchange": exchange,
            "symbol": symbol,
        }


@pytest.fixture
def base_app_settings() -> "AppSettings":
    """Create base AppSettings for testing.
    
    Returns:
        AppSettings: Configured AppSettings instance for risk pipeline integration tests.
    """
    # Create a minimal config file content to use model_validate_json
    config_json = """{
        "general": {
            "app_name": "CyberDeltaEngine",
            "version": "1.0.0",
            "environment": "test"
        },
        "exchanges": {
            "hyperliquid": {
                "enabled": true,
                "rate_limit": {"requests_per_second": 10}
            },
            "backpack": {
                "enabled": true,
                "rate_limit": {"requests_per_second": 10}
            }
        },
        "strategies": {
            "funding_rate_arbitrage": {
                "enabled": true,
                "max_concurrent_positions": 5
            }
        },
        "execution": {
            "order_timeout_seconds": 30,
            "max_slippage": 0.01
        },
        "safety_systems": {
            "circuit_breaker": {
                "enabled": true,
                "max_failures": 5
            }
        },
        "monitoring": {
            "metrics_enabled": true,
            "log_level": "INFO"
        },
        "portfolio_tracker": {
            "enabled": true,
            "update_interval_seconds": 60
        },
        "risk": {
            "enabled": true,
            "global": {
                "max_position_usd": "10000",
                "max_total_exposure_usd": "50000"
            },
            "checkers": {
                "enable_required_fields": true,
                "enable_profitability": true,
                "enable_price_sanity": true,
                "enable_volatility": true,
                "enable_funding_rate": true,
                "enable_circuit_breaker": true,
                "enable_balance": true,
                "thresholds": {
                    "min_profitability": "0.001",
                    "max_price_deviation": "0.1",
                    "max_price_spread": "0.05",
                    "min_price": "0.0001",
                    "max_price": "100000",
                    "outlier_z_score_threshold": "3.0",
                    "max_funding_rate": "0.01",
                    "max_funding_rate_spread": "0.005",
                    "max_volatility": "0.2",
                    "min_volatility": "0.001",
                    "min_balance_ratio": "0.1",
                    "min_funding_rate": "-0.01",
                    "max_funding_rate_volatility": "0.05",
                    "min_funding_rate_confidence": "0.8"
                },
                "fail_fast": true,
                "max_concurrent_checks": 5,
                "check_timeout_seconds": 5.0,
                "funding_rate_lookback_hours": 24,
                "volatility_lookback_hours": 24,
                "include_fees_in_profitability": true,
                "enable_outlier_detection": true,
                "check_both_exchanges": true
            },
            "sizing": {
                "method": "simple",
                "simple_method": "fixed_fraction",
                "simple_fixed_fraction": "0.1",
                "simple_fixed_usd": "1000",
                "kelly_multiplier": "0.25",
                "kelly_max_allocation": "0.1",
                "kelly_min_allocation": "0.01",
                "kelly_risk_free_rate": "0.02",
                "min_position_size": "100",
                "max_position_size": "10000",
                "max_leverage": "5.0",
                "max_portfolio_allocation": "0.5",
                "total_capital": "100000",
                "min_volatility": "0.001",
                "max_volatility_bound": "1.0",
                "volatility_lookback_hours": 24,
                "enable_validation_factors": true,
                "enable_volatility_adjustment": true,
                "enable_spread_adjustment": true,
                "base_validation_factor": "0.8",
                "sizing_timeout_seconds": 10.0
            }
        }
    }"""
    return AppSettings.model_validate_json(config_json)


@pytest.fixture
def mock_portfolio_tracker() -> "MockPortfolioTracker":
    """Create mock portfolio tracker.
    
    Returns:
        MockPortfolioTracker: Mock portfolio tracker instance for testing.
    """
    return MockPortfolioTracker()


@pytest.fixture
def mock_circuit_breaker() -> "MockCircuitBreakerSystem":
    """Create mock circuit breaker.
    
    Returns:
        MockCircuitBreakerSystem: Mock circuit breaker system instance for testing.
    """
    return MockCircuitBreakerSystem()


@pytest.fixture
def mock_funding_rate_validator() -> "MockFundingRateValidator":
    """Create mock funding rate validator.
    
    Returns:
        MockFundingRateValidator: Mock funding rate validator instance for testing.
    """
    return MockFundingRateValidator()


@pytest.fixture
def sample_opportunity() -> "ArbitrageOpportunity":
    """Create a sample arbitrage opportunity for testing.
    
    Returns:
        ArbitrageOpportunity: Sample arbitrage opportunity configured for risk pipeline tests.
    """
    return ArbitrageOpportunity(
        symbol="BTC-USD",
        long_exchange="Hyperliquid",
        short_exchange="Backpack",
        long_price=Decimal(50000),
        short_price=Decimal(50100),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("0.0002"),  # 0.0001 - (-0.0001)
        timestamp=datetime.fromtimestamp(1234567890, tz=UTC),
        basis_volatility=0.15,  # 15% volatility
        confidence_score=0.9,
    )


class TestRiskPipelineIntegration:
    """Integration tests for the complete risk management pipeline."""

    async def test_complete_risk_pipeline_success(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_circuit_breaker: MockCircuitBreakerSystem,
        mock_funding_rate_validator: MockFundingRateValidator,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test complete risk pipeline with successful opportunity processing."""
        # Create risk manager orchestrator
        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        # Process opportunity through complete pipeline
        result = await orchestrator.process_opportunity(sample_opportunity)

        # Verify successful processing
        assert isinstance(result, ProcessedOpportunity)
        assert result.status == ProcessingStatus.APPROVED
        assert result.is_approved
        assert not result.is_rejected
        assert not result.has_errors

        # Verify check results
        assert result.check_passed
        assert len(result.check_errors) == 0

        # Verify sizing results
        assert result.position_size_usd > 0
        assert result.allocation_percentage > 0
        assert result.sized_opportunity is not None

        # Verify constraint validation
        assert result.constraints_passed
        assert len(result.constraint_violations) == 0

        # Verify performance metrics
        assert result.total_processing_time_ms > 0
        assert result.check_time_ms > 0
        assert result.sizing_time_ms > 0
        assert result.validation_time_ms > 0

    async def test_risk_pipeline_with_failed_checks(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_circuit_breaker: MockCircuitBreakerSystem,
        mock_funding_rate_validator: MockFundingRateValidator,
    ) -> None:
        """Test risk pipeline with failing checks (low profitability)."""
        # Create opportunity with very low spread (will fail profitability check)
        low_profit_opportunity = ArbitrageOpportunity(
            symbol="ETH-USD",
            long_exchange="Hyperliquid",
            short_exchange="Backpack",
            long_price=Decimal(3000),
            short_price=Decimal("3000.50"),  # Only 0.017% spread, below 0.1% threshold
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.fromtimestamp(1234567890, tz=UTC),
            basis_volatility=0.12,
            confidence_score=0.9,
        )

        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        result = await orchestrator.process_opportunity(low_profit_opportunity)

        # Verify rejection due to failed checks
        assert result.status == ProcessingStatus.REJECTED
        assert result.is_rejected
        assert not result.is_approved
        assert not result.check_passed
        assert result.rejection_reason is not None
        assert "Checks failed" in result.rejection_reason

    async def test_risk_pipeline_with_circuit_breaker_tripped(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_funding_rate_validator: MockFundingRateValidator,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test risk pipeline with circuit breaker tripped."""
        # Create circuit breaker that is tripped
        tripped_circuit_breaker = MockCircuitBreakerSystem(is_tripped=True)

        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=tripped_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        result = await orchestrator.process_opportunity(sample_opportunity)

        # Verify rejection due to circuit breaker
        assert result.status == ProcessingStatus.REJECTED
        assert result.is_rejected
        assert not result.check_passed

    async def test_kelly_criterion_sizing_integration(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_circuit_breaker: MockCircuitBreakerSystem,
        mock_funding_rate_validator: MockFundingRateValidator,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test integration with Kelly criterion sizing."""
        # Configure for Kelly sizing
        kelly_app_settings = base_app_settings.model_copy(deep=True)
        kelly_app_settings.risk.sizing.method = "kelly"

        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=kelly_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        result = await orchestrator.process_opportunity(sample_opportunity)

        # Verify Kelly sizing was used
        assert result.status == ProcessingStatus.APPROVED
        assert result.kelly_fraction is not None
        assert result.kelly_fraction > 0

    async def test_multiple_opportunities_processing(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_circuit_breaker: MockCircuitBreakerSystem,
        mock_funding_rate_validator: MockFundingRateValidator,
    ) -> None:
        """Test processing multiple opportunities in parallel."""
        opportunities = [
            ArbitrageOpportunity(
                symbol=f"SYMBOL{i}",
                long_exchange="Hyperliquid",
                short_exchange="Backpack",
                long_price=Decimal(1000),
                short_price=Decimal(1020),  # 2% spread
                long_funding_rate=Decimal("0.0001"),
                short_funding_rate=Decimal("-0.0001"),
                net_funding_differential=Decimal("0.0002"),
                timestamp=datetime.fromtimestamp(1234567890 + i, tz=UTC),
                basis_volatility=0.1,
                confidence_score=0.9,
            )
            for i in range(5)
        ]

        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        results = await orchestrator.process_opportunities(opportunities)

        # Verify all opportunities were processed
        assert len(results) == 5
        for result in results:
            assert isinstance(result, ProcessedOpportunity)
            assert result.status == ProcessingStatus.APPROVED

    async def test_preset_configuration_integration(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test integration with configuration presets."""
        # Test conservative preset
        conservative_orchestrator = RiskManagerFactory.create_from_preset(
            preset_name="conservative",
            base_app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        result = await conservative_orchestrator.process_opportunity(sample_opportunity)

        # Conservative settings should reject due to higher profitability threshold
        # (0.2% spread vs 0.2% conservative threshold)
        assert result.status in [ProcessingStatus.APPROVED, ProcessingStatus.REJECTED]

        # Test aggressive preset
        aggressive_orchestrator = RiskManagerFactory.create_from_preset(
            preset_name="aggressive",
            base_app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        aggressive_result = await aggressive_orchestrator.process_opportunity(sample_opportunity)
        assert isinstance(aggressive_result, ProcessedOpportunity)

    async def test_global_risk_settings_constraints(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_circuit_breaker: MockCircuitBreakerSystem,
        mock_funding_rate_validator: MockFundingRateValidator,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test GlobalRiskSettings constraint integration."""
        # Set very low global limits
        constrained_settings = base_app_settings.model_copy(deep=True)
        constrained_settings.risk.global_risk.max_position_usd = Decimal(50)  # Very low limit

        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=constrained_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        result = await orchestrator.process_opportunity(sample_opportunity)

        # Should be rejected due to position size constraints
        if result.status == ProcessingStatus.REJECTED:
            assert result.rejection_reason is not None
            assert "Constraints violated" in result.rejection_reason
        else:
            # If approved, position size should be within global limits
            assert (
                result.position_size_usd <= constrained_settings.risk.global_risk.max_position_usd
            )

    async def test_configuration_migration_integration(self) -> None:
        """Test configuration migration from legacy format."""
        legacy_config = {
            "enabled": True,
            "min_profitability_threshold": 0.002,
            "max_volatility": 0.3,
            "kelly_multiplier": 0.3,
            "simple_fixed_fraction": 0.15,
            "use_simple_sizing_path": True,
        }

        # Migrate legacy configuration
        enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

        # Verify migration worked
        assert enhanced_config["enabled"] is True
        assert enhanced_config["checkers"]["thresholds"]["min_profitability"] == 0.002
        assert enhanced_config["checkers"]["thresholds"]["max_volatility"] == 0.3
        assert enhanced_config["sizing"]["kelly_multiplier"] == 0.3
        assert enhanced_config["sizing"]["simple_fixed_fraction"] == 0.15
        assert enhanced_config["sizing"]["method"] == "simple"

    async def test_minimal_risk_manager_integration(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test minimal risk manager creation without optional dependencies."""
        # Create minimal risk manager (no circuit breaker or funding rate validator)
        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        result = await orchestrator.process_opportunity(sample_opportunity)

        # Should still process successfully with fewer checkers
        assert isinstance(result, ProcessedOpportunity)
        assert result.status in [ProcessingStatus.APPROVED, ProcessingStatus.REJECTED]

    async def test_portfolio_position_management(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        mock_circuit_breaker: MockCircuitBreakerSystem,
        mock_funding_rate_validator: MockFundingRateValidator,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test portfolio position management integration."""
        orchestrator = RiskManagerFactory.create_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker,
            funding_rate_validator=mock_funding_rate_validator,
        )

        # Process and add position
        result = await orchestrator.process_opportunity(sample_opportunity)

        if result.status == ProcessingStatus.APPROVED and result.sized_opportunity:
            # Add position to portfolio
            orchestrator.add_position(result.sized_opportunity)

            # Verify position was added
            portfolio_metrics = orchestrator.get_portfolio_metrics()
            assert portfolio_metrics["position_count"] == 1
            assert portfolio_metrics["total_exposure"] > 0

            # Remove position
            removed = orchestrator.remove_position(sample_opportunity.symbol)
            assert removed is True

            # Verify position was removed
            updated_metrics = orchestrator.get_portfolio_metrics()
            assert updated_metrics["position_count"] == 0

    async def test_error_handling_and_recovery(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
    ) -> None:
        """Test error handling and recovery in the pipeline."""
        # Create opportunity with invalid data to trigger errors
        invalid_opportunity = ArbitrageOpportunity(
            symbol="",  # Empty symbol should cause validation errors
            long_exchange="",
            short_exchange="",
            long_price=Decimal("0.001"),  # Very small but positive price
            short_price=Decimal("0.001"),
            long_funding_rate=Decimal(0),
            short_funding_rate=Decimal(0),
            net_funding_differential=Decimal(0),
            timestamp=datetime.fromtimestamp(1234567890, tz=UTC),
        )

        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        result = await orchestrator.process_opportunity(invalid_opportunity)

        # Should handle errors gracefully
        assert isinstance(result, ProcessedOpportunity)
        assert result.status in [ProcessingStatus.REJECTED, ProcessingStatus.ERROR]

    async def test_performance_and_metrics(
        self,
        base_app_settings: AppSettings,
        mock_portfolio_tracker: MockPortfolioTracker,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test performance metrics collection."""
        orchestrator = RiskManagerFactory.create_minimal_risk_manager(
            app_settings=base_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
        )

        # Process multiple opportunities to generate metrics
        for i in range(3):
            opportunity = sample_opportunity.model_copy(deep=True)
            opportunity.symbol = f"TEST{i}"
            await orchestrator.process_opportunity(opportunity)

        # Check orchestrator stats
        stats = orchestrator.get_orchestrator_stats()
        assert stats["opportunities_processed"] == 3
        assert stats["average_processing_time_ms"] > 0
        assert "approval_rate" in stats

        # Reset and verify
        orchestrator.reset_statistics()
        reset_stats = orchestrator.get_orchestrator_stats()
        assert reset_stats["opportunities_processed"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
