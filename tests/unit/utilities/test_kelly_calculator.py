"""Tests for KellyCalculator utility."""

from decimal import Decimal
from unittest.mock import Mock

import pytest

from cyberdelta.core.risk.utils.kelly_calculator import (
    KellyCalculator,
    KellyInput,
    KellyResult,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class TestKellyCalculator:
    """Test cases for KellyCalculator."""
    
    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.calculator = KellyCalculator()
    
    def test_initialization(self) -> None:
        """Test calculator initialization."""
        assert self.calculator is not None
        assert self.calculator.default_multiplier == Decimal("0.25")
        assert hasattr(self.calculator, "calculate_kelly")
    
    def test_continuous_kelly_calculation(self) -> None:
        """Test continuous Kelly calculation."""
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),  # 10% expected return
            volatility=Decimal("0.2"),  # 20% volatility
            risk_free_rate=Decimal("0.02")  # 2% risk-free rate
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        assert isinstance(result, KellyResult)
        assert result.kelly_fraction > 0
        assert result.recommended_fraction > 0
        assert result.recommended_fraction < result.kelly_fraction  # Due to multiplier
        assert result.expected_growth_rate > 0
    
    def test_binary_kelly_calculation(self) -> None:
        """Test binary Kelly calculation."""
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),  # Required parameter
            volatility=Decimal("0.2"),  # Required parameter
            win_probability=Decimal("0.6"),  # 60% win probability
            win_amount=Decimal("1.0"),  # Win $1
            loss_amount=Decimal("1.0")  # Lose $1
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        assert isinstance(result, KellyResult)
        assert result.kelly_fraction > 0
        # For 60% win rate with 1:1 payout, Kelly = 0.6 - 0.4 = 0.2
        assert abs(result.kelly_fraction - Decimal("0.2")) < Decimal("0.01")
    
    def test_kelly_bounds(self) -> None:
        """Test Kelly fraction bounds."""
        # Set tight bounds
        self.calculator.set_kelly_bounds(
            min_fraction=Decimal("0.01"),
            max_fraction=Decimal("0.1")
        )
        
        # Test with high expected return that would normally give high Kelly
        kelly_input = KellyInput(
            expected_return=Decimal("0.5"),  # 50% expected return
            volatility=Decimal("0.1")  # 10% volatility
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        assert result.adjusted_kelly <= Decimal("0.1")  # Respects max bound
    
    def test_kelly_for_opportunity(self) -> None:
        """Test Kelly calculation for arbitrage opportunity."""
        # Mock opportunity
        opportunity = Mock(spec=ArbitrageOpportunity)
        opportunity.spread_percentage = Decimal("0.02")  # 2% spread
        opportunity.volatility = Decimal("0.01")  # 1% volatility
        opportunity.symbol = "BTC-USDT"
        
        result = self.calculator.calculate_kelly_for_opportunity(opportunity)
        
        assert isinstance(result, KellyResult)
        assert result.kelly_fraction > 0
        assert result.recommended_fraction > 0
    
    def test_kelly_adjustments(self) -> None:
        """Test Kelly adjustments for risk factors."""
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),
            volatility=Decimal("0.2"),
            
            sharpe_ratio=Decimal("0.5"),  # Low Sharpe ratio
            max_drawdown=Decimal("0.2"),  # 20% max drawdown
            
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        assert result.calculation_details is not None
        assert "sharpe_adjustment" in result.calculation_details
        assert "drawdown_adjustment" in result.calculation_details
        assert "correlation_adjustment" in result.calculation_details
    
    def test_expected_growth_rate(self) -> None:
        """Test expected growth rate calculation."""
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),
            volatility=Decimal("0.2")
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        assert result.expected_growth_rate > 0
        assert result.expected_growth_rate < kelly_input.expected_return  # Due to volatility drag
    
    def test_time_to_double(self) -> None:
        """Test time to double calculation."""
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),
            volatility=Decimal("0.2")
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        assert result.time_to_double is not None
        assert result.time_to_double > 0
    
    def test_set_default_multiplier(self) -> None:
        """Test setting default Kelly multiplier."""
        self.calculator.set_default_multiplier(Decimal("0.5"))
        
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),
            volatility=Decimal("0.2")
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        # Recommended should be 50% of Kelly
        assert result.recommended_fraction == result.adjusted_kelly * Decimal("0.5")
    
    def test_enable_adjustments(self) -> None:
        """Test enabling/disabling adjustments."""
        # Disable Sharpe adjustment
        self.calculator.enable_adjustment("sharpe", False)
        
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),
            volatility=Decimal("0.2"),
            
            sharpe_ratio=Decimal("0.5")  # Low Sharpe ratio
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        # Sharpe adjustment should not be applied
        if result.calculation_details:
            assert result.calculation_details.get("sharpe_adjustment", 1.0) == 1.0
    
    def test_get_calculator_stats(self) -> None:
        """Test getting calculator statistics."""
        stats = self.calculator.get_calculator_stats()
        
        assert isinstance(stats, dict)
        assert "default_multiplier" in stats
        assert "min_kelly_fraction" in stats
        assert "max_kelly_fraction" in stats
        assert "adjustments_enabled" in stats
    
    def test_invalid_kelly_input(self) -> None:
        """Test validation of invalid Kelly input."""
        # Negative expected return should be allowed (for shorting)
        # But negative volatility should raise error
        with pytest.raises(ValueError):
            kelly_input = KellyInput(
                expected_return=Decimal("0.1"),
                volatility=Decimal("-0.2")  # Invalid negative volatility
            )
            kelly_input.validate()
    
    def test_edge_cases(self) -> None:
        """Test edge cases in Kelly calculation."""
        # Zero volatility
        kelly_input = KellyInput(
            expected_return=Decimal("0.1"),
            volatility=Decimal("0.0001")  # Very small volatility
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        # Should hit max bound with near-zero volatility
        assert result.adjusted_kelly == self.calculator.max_kelly_fraction
        
        # Zero expected return
        kelly_input = KellyInput(
            expected_return=Decimal(0),
            volatility=Decimal("0.2")
        )
        
        result = self.calculator.calculate_kelly(kelly_input)
        
        # Should recommend no allocation
        assert result.kelly_fraction <= 0