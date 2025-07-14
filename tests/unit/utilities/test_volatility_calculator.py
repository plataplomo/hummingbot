"""Tests for VolatilityCalculator utility."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock

from cyberdelta.core.risk.utils.volatility_calculator import (
    PriceData,
    VolatilityCalculator,
    VolatilityResult,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class TestVolatilityCalculator:
    """Test cases for VolatilityCalculator."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.calculator = VolatilityCalculator()

    def test_initialization(self) -> None:
        """Test calculator initialization."""
        assert self.calculator is not None
        assert hasattr(self.calculator, "calculate_volatility")

    def test_calculate_volatility_with_price_data(self) -> None:
        """Test volatility calculation with price data."""
        # Create price data
        price_data = [
            PriceData(
                timestamp=datetime.now(tz=UTC) - timedelta(hours=i),
                open=Decimal(100) + Decimal(str(i * 0.5)),
                high=Decimal(101) + Decimal(str(i * 0.5)),
                low=Decimal(99) + Decimal(str(i * 0.5)),
                close=Decimal(100) + Decimal(str(i * 0.5)),
                volume=Decimal(1000),
            )
            for i in range(20)
        ]

        result = self.calculator.calculate_volatility(price_data)

        assert isinstance(result, VolatilityResult)
        assert result.volatility > 0
        assert result.annualized_volatility > 0
        assert result.data_points > 0

    def test_calculate_volatility_for_opportunity(self) -> None:
        """Test volatility calculation for arbitrage opportunity."""
        # Mock opportunity
        opportunity = Mock(spec=ArbitrageOpportunity)
        opportunity.symbol = "BTC-USDT"
        opportunity.long_price = Decimal(50000)
        opportunity.short_price = Decimal(50100)
        opportunity.volatility = Decimal("0.02")

        result = self.calculator.calculate_volatility_for_opportunity(opportunity)

        assert isinstance(result, VolatilityResult)
        assert result.volatility > 0

    def test_insufficient_data(self) -> None:
        """Test handling of insufficient data."""
        # Less than minimum required data points
        price_data = [
            PriceData(
                timestamp=datetime.now(tz=UTC),
                open=Decimal(100),
                high=Decimal(101),
                low=Decimal(99),
                close=Decimal(100),
                volume=Decimal(1000),
            )
        ]

        result = self.calculator.calculate_volatility(price_data)

        assert result.volatility > 0  # Should return a default value
        assert result.warnings is not None  # Should have warnings
        assert len(result.warnings) > 0

    def test_volatility_with_empty_data(self) -> None:
        """Test volatility calculation with empty data."""
        result = self.calculator.calculate_volatility([])

        assert result.volatility > 0  # Should return default
        assert result.warnings is not None
        assert len(result.warnings) > 0

    def test_set_ewma_lambda(self) -> None:
        """Test setting EWMA lambda parameter."""
        self.calculator.set_ewma_lambda(Decimal("0.94"))
        assert self.calculator.ewma_lambda == Decimal("0.94")

    def test_set_garch_parameters(self) -> None:
        """Test setting GARCH parameters."""
        self.calculator.set_garch_parameters(
            omega=Decimal("0.00001"), alpha=Decimal("0.1"), beta=Decimal("0.85")
        )
        assert self.calculator.garch_omega == Decimal("0.00001")
        assert self.calculator.garch_alpha == Decimal("0.1")
        assert self.calculator.garch_beta == Decimal("0.85")

    def test_get_calculator_stats(self) -> None:
        """Test getting calculator statistics."""
        stats = self.calculator.get_calculator_stats()

        assert isinstance(stats, dict)
        assert "default_method" in stats
        assert "ewma_lambda" in stats
        assert "cache_size" in stats

    def test_clear_cache(self) -> None:
        """Test cache clearing."""
        # Add some data to cache
        price_data = [
            PriceData(
                timestamp=datetime.now(tz=UTC) - timedelta(hours=i),
                open=Decimal(100),
                high=Decimal(101),
                low=Decimal(99),
                close=Decimal(100),
                volume=Decimal(1000),
            )
            for i in range(20)
        ]

        self.calculator.calculate_volatility(price_data)

        # Clear cache
        self.calculator.clear_cache()

        stats = self.calculator.get_calculator_stats()
        assert stats["cache_size"] == 0
