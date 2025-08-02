"""Example of migrating tests from SizedOpportunity to RiskAnalysis."""

from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from cyberdelta.core.risk_manager import RiskAnalysis
from cyberdelta.core.risk.sizing.models.sizing_result import SizingResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# OLD TEST PATTERN
def old_test_pattern():
    """Old pattern using SizedOpportunity."""
    # from cyberdelta.core.risk_types import SizedOpportunity
    
    # mock_sized_opp = SizedOpportunity(
    #     opportunity=opportunity,
    #     long_size=Decimal("1000.0"),
    #     short_size=Decimal("1000.0"),
    #     allocation_percentage=Decimal("10.0"),
    #     expected_profit=Decimal("100.0"),
    #     expected_return=Decimal("10.0"),
    #     risk_adjusted_return=Decimal("5.0"),
    # )
    # 
    # mock_risk_manager.size_opportunity.return_value = mock_sized_opp
    pass

# NEW TEST PATTERN
def new_test_pattern():
    """New pattern using RiskAnalysis."""
    opportunity = Mock(spec=ArbitrageOpportunity)
    
    # Create sizing result
    mock_sizing_result = SizingResult(
        success=True,
        position_size=Decimal("1000.0"),
        message="Sized successfully",
        risk_metrics={
            "sharpe_ratio": 5.0,
            "expected_return": 10.0
        },
        expected_profit_usd=Decimal("100.0"),
        kelly_fraction=Decimal("0.1")  # 10% allocation
    )
    
    # Create risk analysis
    mock_risk_analysis = RiskAnalysis(
        opportunity=opportunity,
        approved=True,
        sizing=mock_sizing_result,
        checks={"all": "passed"},
        constraints={"all": "satisfied"},
        rejection_reason=None
    )
    
    # Mock the new method
    mock_risk_manager = Mock()
    mock_risk_manager.analyze_opportunity = AsyncMock(return_value=mock_risk_analysis)
    
    return mock_risk_manager, mock_risk_analysis

# USAGE IN TESTS
async def test_strategy_with_new_api():
    """Example test using new API."""
    mock_rm, mock_analysis = new_test_pattern()
    
    # In production code:
    # analysis = await risk_manager.analyze_opportunity(opportunity)
    # if analysis.approved:
    #     position_size = analysis.sizing.position_size
    
    # In test assertions:
    assert mock_analysis.approved
    assert mock_analysis.sizing.position_size == Decimal("1000.0")
    assert mock_analysis.sizing.expected_profit_usd == Decimal("100.0")
    
    # Access risk metrics
    sharpe = mock_analysis.sizing.risk_metrics.get("sharpe_ratio")
    assert sharpe == 5.0