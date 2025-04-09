import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity

class TestRiskManager:
    """Test suite for RiskManager component."""

    @pytest.fixture
    def risk_manager(self, mock_config, mock_portfolio_tracker):
        """Create a RiskManager instance with mocked dependencies."""
        return RiskManager(mock_config, mock_portfolio_tracker)

    def test_calculate_kelly_size(self, risk_manager, mock_arbitrage_opportunity):
        """Test Kelly criterion position sizing calculation."""
        # Calculate Kelly size
        size = risk_manager._calculate_kelly_size(mock_arbitrage_opportunity, 10000.0)
        
        # Since we're using mocked data, we can't predict the exact output
        # But we can verify that the output is a reasonable number
        assert size > 0
        assert size < 10000.0  # Should not exceed total capital
        
        # Test with zero volatility (should use minimum value)
        mock_arbitrage_opportunity.basis_volatility = 0.0
        size = risk_manager._calculate_kelly_size(mock_arbitrage_opportunity, 10000.0)
        assert size > 0
        
        # Test with negative net funding (should return zero)
        mock_arbitrage_opportunity.net_funding_differential = -0.01
        mock_arbitrage_opportunity.basis_volatility = 0.01
        size = risk_manager._calculate_kelly_size(mock_arbitrage_opportunity, 10000.0)
        assert size == 0
        
        # Reset the opportunity for other tests
        mock_arbitrage_opportunity.net_funding_differential = 0.05
        mock_arbitrage_opportunity.basis_volatility = 0.01

    def test_check_portfolio_constraints(self, risk_manager, mock_portfolio_tracker):
        """Test portfolio constraint checking."""
        # Test with acceptable position sizes
        result = risk_manager._check_portfolio_constraints(
            "hyperliquid", "backpack", 1000.0, 1000.0
        )
        
        # Should pass constraints
        assert result is True
        
        # Test with position size exceeding max exposure
        # Mock total exposure to be close to the limit
        mock_portfolio_tracker.get_total_exposure.return_value = 4500.0
        
        result = risk_manager._check_portfolio_constraints(
            "hyperliquid", "backpack", 1000.0, 1000.0
        )
        
        # Should fail constraints (4500 + 1000 + 1000 > 5000)
        assert result is False
        
        # Reset the exposure
        mock_portfolio_tracker.get_total_exposure.return_value = 1000.0
        
        # Test with per-exchange exposure exceeding limit
        # Mock exchange exposure to be close to the limit
        mock_portfolio_tracker.get_exchange_exposure.return_value = 9000.0
        mock_portfolio_tracker.get_total_capital.return_value = 10000.0
        
        result = risk_manager._check_portfolio_constraints(
            "hyperliquid", "backpack", 1000.0, 1000.0
        )
        
        # Should fail constraints (9000 + 1000 > 10000 * 0.8)
        assert result is False
        
        # Reset the exchange exposure
        mock_portfolio_tracker.get_exchange_exposure.return_value = 1000.0
        
        # Test with leverage exceeding max leverage
        # Mock exchange balance to be small
        mock_portfolio_tracker.get_exchange_balance.return_value = 100.0
        
        result = risk_manager._check_portfolio_constraints(
            "hyperliquid", "backpack", 1000.0, 1000.0
        )
        
        # Should fail constraints (1000 / 100 = 10x leverage > 5x max)
        assert result is False
        
        # Reset the exchange balance
        mock_portfolio_tracker.get_exchange_balance.return_value = 1000.0
        
        # Test with zero exchange balance
        mock_portfolio_tracker.get_exchange_balance.return_value = 0.0
        
        result = risk_manager._check_portfolio_constraints(
            "hyperliquid", "backpack", 1000.0, 1000.0
        )
        
        # Should fail constraints (can't calculate leverage with zero balance)
        assert result is False

    def test_size_opportunity(self, risk_manager, mock_arbitrage_opportunity, mock_portfolio_tracker):
        """Test opportunity sizing."""
        # Set up reasonable mocks
        mock_portfolio_tracker.get_total_capital.return_value = 10000.0
        mock_portfolio_tracker.get_exchange_balance.return_value = 5000.0
        mock_portfolio_tracker.get_exchange_exposure.return_value = 1000.0
        mock_portfolio_tracker.get_total_exposure.return_value = 2000.0
        
        # Size the opportunity
        sized_opportunity = risk_manager.size_opportunity(mock_arbitrage_opportunity)
        
        # Verify the result
        assert sized_opportunity is not None
        assert isinstance(sized_opportunity, SizedOpportunity)
        assert sized_opportunity.opportunity == mock_arbitrage_opportunity
        assert 0 < sized_opportunity.long_size <= risk_manager.max_position_size
        assert 0 < sized_opportunity.short_size <= risk_manager.max_position_size
        assert sized_opportunity.long_size == sized_opportunity.short_size  # Equal sizes for delta neutrality
        assert sized_opportunity.allocation_percentage > 0
        assert sized_opportunity.expected_profit > 0
        assert sized_opportunity.expected_return > 0
        assert sized_opportunity.risk_adjusted_return > 0
        
        # Test with zero total capital
        mock_portfolio_tracker.get_total_capital.return_value = 0.0
        sized_opportunity = risk_manager.size_opportunity(mock_arbitrage_opportunity)
        assert sized_opportunity is None
        
        # Reset total capital
        mock_portfolio_tracker.get_total_capital.return_value = 10000.0
        
        # Test with failed portfolio constraints
        # Make a constraint fail by setting balance to zero
        mock_portfolio_tracker.get_exchange_balance.return_value = 0.0
        sized_opportunity = risk_manager.size_opportunity(mock_arbitrage_opportunity)
        assert sized_opportunity is None

    def test_validate_opportunities(self, risk_manager, mock_arbitrage_opportunity, mock_portfolio_tracker):
        """Test opportunity validation."""
        # Set up reasonable mocks
        mock_portfolio_tracker.get_total_capital.return_value = 10000.0
        mock_portfolio_tracker.get_exchange_balance.return_value = 5000.0
        mock_portfolio_tracker.get_exchange_exposure.return_value = 1000.0
        mock_portfolio_tracker.get_total_exposure.return_value = 2000.0
        
        # Create list of opportunities
        opportunities = [mock_arbitrage_opportunity]
        
        # Validate opportunities
        sized_opportunities = risk_manager.validate_opportunities(opportunities)
        
        # Verify the results
        assert len(sized_opportunities) == 1
        assert isinstance(sized_opportunities[0], SizedOpportunity)
        assert sized_opportunities[0].opportunity == mock_arbitrage_opportunity
        
        # Test with empty list
        sized_opportunities = risk_manager.validate_opportunities([])
        assert len(sized_opportunities) == 0
        
        # Test with failing opportunity
        # Make a constraint fail by setting balance to zero
        mock_portfolio_tracker.get_exchange_balance.return_value = 0.0
        sized_opportunities = risk_manager.validate_opportunities([mock_arbitrage_opportunity])
        assert len(sized_opportunities) == 0
        
        # Reset balance
        mock_portfolio_tracker.get_exchange_balance.return_value = 5000.0
        
        # Test with multiple opportunities
        # Create a second opportunity
        second_opportunity = MagicMock()
        second_opportunity.symbol = "ETH"
        second_opportunity.long_exchange = "hyperliquid"
        second_opportunity.short_exchange = "backpack"
        second_opportunity.net_funding_differential = 0.03
        second_opportunity.basis_volatility = 0.02
        second_opportunity.expected_profit = 5.0
        second_opportunity.confidence = 0.7
        second_opportunity.timestamp = datetime.now()
        
        # Validate multiple opportunities
        sized_opportunities = risk_manager.validate_opportunities([mock_arbitrage_opportunity, second_opportunity])
        
        # Verify the results
        assert len(sized_opportunities) == 2
        
        # Test sorting by risk-adjusted return
        # The first opportunity should have a higher risk-adjusted return
        assert sized_opportunities[0].risk_adjusted_return >= sized_opportunities[1].risk_adjusted_return

    def test_exchange_risk_modifiers(self, risk_manager, mock_arbitrage_opportunity, mock_portfolio_tracker):
        """Test exchange-specific risk modifiers."""
        # Set up reasonable mocks
        mock_portfolio_tracker.get_total_capital.return_value = 10000.0
        mock_portfolio_tracker.get_exchange_balance.return_value = 5000.0
        mock_portfolio_tracker.get_exchange_exposure.return_value = 1000.0
        mock_portfolio_tracker.get_total_exposure.return_value = 2000.0
        
        # Create a clone of the opportunity to use backpack as the long exchange
        from copy import deepcopy
        swapped_opportunity = deepcopy(mock_arbitrage_opportunity)
        swapped_opportunity.long_exchange = "backpack"
        swapped_opportunity.short_exchange = "hyperliquid"
        
        # Size both opportunities
        original_opportunity = risk_manager.size_opportunity(mock_arbitrage_opportunity)
        swapped_opportunity = risk_manager.size_opportunity(swapped_opportunity)
        
        # The hyperliquid exchange has a risk modifier of 0.9 while backpack is 1.0
        # So the original opportunity (long on hyperliquid) should have a smaller size
        assert original_opportunity.long_size <= swapped_opportunity.long_size

    def test_sized_opportunity_str(self, risk_manager, mock_arbitrage_opportunity, mock_portfolio_tracker):
        """Test the string representation of a SizedOpportunity."""
        # Set up reasonable mocks
        mock_portfolio_tracker.get_total_capital.return_value = 10000.0
        mock_portfolio_tracker.get_exchange_balance.return_value = 5000.0
        mock_portfolio_tracker.get_exchange_exposure.return_value = 1000.0
        mock_portfolio_tracker.get_total_exposure.return_value = 2000.0
        
        # Size the opportunity
        sized_opportunity = risk_manager.size_opportunity(mock_arbitrage_opportunity)
        
        # Get the string representation
        string_rep = str(sized_opportunity)
        
        # Verify the string contains key information
        assert "SizedOpportunity" in string_rep
        assert mock_arbitrage_opportunity.symbol in string_rep
        assert mock_arbitrage_opportunity.long_exchange in string_rep
        assert mock_arbitrage_opportunity.short_exchange in string_rep
        assert str(round(sized_opportunity.long_size, 2)) in string_rep
        assert str(round(sized_opportunity.expected_profit, 2)) in string_rep 