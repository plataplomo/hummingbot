# Breaking Change Plan: Risk Management System (Clean Break)

## Executive Summary

Direct refactor of the risk management system with no backwards compatibility. System is in development, not production.

## Current State

- **Monolithic RiskManager**: 2,599 lines mixing validation, sizing, and constraints
- **Legacy types**: `SizedOpportunity` couples opportunity with sizing
- **Boolean flags**: `use_simple_sizing_path` controls algorithm selection
- **Duplicate code**: Sizing logic exists in both RiskManager and modular risk/ directory

## Target State

- **Clean RiskManager**: ~350 lines, pure orchestration
- **Modern types**: `RiskAnalysis` with structured data
- **Strategy pattern**: Explicit sizing method configuration
- **Single source**: All logic in modular risk/ components

## Implementation Plan

### Step 1: Delete Old Code

```bash
# Remove old implementation
rm cyberdelta/core/risk_manager_original.py
rm cyberdelta/core/risk_types.py

# The current refactored risk_manager.py stays
```

### Step 2: Create New Risk Manager

```python
# cyberdelta/core/risk_manager.py
"""Modern risk manager with clean API."""

from decimal import Decimal
from typing import Optional

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.core.risk.sizing.models.sizing_result import SizingResult
from cyberdelta.core.risk.checks.pipeline import CheckPipeline
from cyberdelta.core.risk.constraints.orchestrator import ConstraintValidator
from cyberdelta.validation.funding_data import ArbitrageOpportunity

logger = get_logger(__name__)


class RiskAnalysis:
    """Complete risk analysis result."""
    
    def __init__(
        self,
        opportunity: ArbitrageOpportunity,
        approved: bool,
        sizing: Optional[SizingResult] = None,
        checks: Optional[dict] = None,
        constraints: Optional[dict] = None,
        rejection_reason: Optional[str] = None
    ):
        self.opportunity = opportunity
        self.approved = approved
        self.sizing = sizing
        self.checks = checks or {}
        self.constraints = constraints or {}
        self.rejection_reason = rejection_reason


class RiskManager:
    """Risk manager using modular architecture."""

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_state_manager: PortfolioStateManager,
        risk_factory: RiskServiceFactory,
    ) -> None:
        """Initialize with required dependencies only."""
        self.app_settings = app_settings
        self.portfolio_state_manager = portfolio_state_manager
        self.risk_factory = risk_factory
        
        # Create services
        self.position_sizer = risk_factory.create_position_sizer(
            app_settings.risk.sizing.method
        )
        self.check_pipeline = self._create_check_pipeline()
        self.constraint_validator = self._create_constraint_validator()
        
        logger.info(
            "risk_manager_initialized",
            sizing_method=app_settings.risk.sizing.method
        )

    def _create_check_pipeline(self) -> CheckPipeline:
        """Create check pipeline from configuration."""
        # In full implementation, this would use risk_factory
        # For now, return a simple pipeline
        from cyberdelta.core.risk.checks.pipeline import CheckPipeline
        return CheckPipeline(self.app_settings)

    def _create_constraint_validator(self) -> ConstraintValidator:
        """Create constraint validator from configuration."""
        from cyberdelta.core.risk.constraints.orchestrator import ConstraintValidator
        return ConstraintValidator(self.app_settings)

    async def analyze_opportunity(
        self, 
        opportunity: ArbitrageOpportunity
    ) -> RiskAnalysis:
        """Analyze opportunity through complete risk pipeline.
        
        Args:
            opportunity: The arbitrage opportunity to analyze
            
        Returns:
            RiskAnalysis with complete risk assessment
        """
        logger.info(
            "analyzing_opportunity",
            symbol=opportunity.symbol,
            exchanges=f"{opportunity.long_exchange}/{opportunity.short_exchange}"
        )
        
        # Run checks
        check_results = await self.check_pipeline.run_checks(opportunity)
        if not check_results.passed:
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                checks=check_results.to_dict(),
                rejection_reason=check_results.get_failure_reason()
            )
        
        # Get available capital
        portfolio_state = await self.portfolio_state_manager.get_portfolio_summary()
        available_capital = portfolio_state.total_capital
        
        if available_capital <= Decimal(0):
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                rejection_reason="Insufficient capital"
            )
        
        # Size the opportunity
        sizing_result = await self.position_sizer.size_opportunity(
            opportunity,
            available_capital
        )
        
        if not sizing_result.success:
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                sizing=sizing_result,
                rejection_reason=sizing_result.message
            )
        
        # Validate constraints
        constraint_results = await self.constraint_validator.validate(
            opportunity,
            sizing_result,
            portfolio_state
        )
        
        if not constraint_results.passed:
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                sizing=sizing_result,
                constraints=constraint_results.to_dict(),
                rejection_reason=constraint_results.get_failure_reason()
            )
        
        # All checks passed
        return RiskAnalysis(
            opportunity=opportunity,
            approved=True,
            sizing=sizing_result,
            checks=check_results.to_dict(),
            constraints=constraint_results.to_dict()
        )

    async def get_risk_metrics(self) -> dict:
        """Get current risk metrics."""
        portfolio_state = await self.portfolio_state_manager.get_portfolio_summary()
        
        return {
            "portfolio_exposure": float(portfolio_state.total_exposure),
            "available_capital": float(portfolio_state.total_capital),
            "sizing_method": self.app_settings.risk.sizing.method,
            "position_sizer_stats": self.position_sizer.get_performance_stats(),
        }
```

### Step 3: Update Configuration

```yaml
# OLD config.yaml - DELETE THESE FIELDS
risk:
  use_simple_sizing_path: true  # DELETE
  simple_sizing_method: "fixed_fraction"  # DELETE
  simple_fixed_fraction: 0.1  # DELETE
  simple_fixed_usd_size: 10.0  # DELETE

# NEW config.yaml - USE THESE FIELDS
risk:
  sizing:
    method: "simple"  # or "kelly" or "production_kelly"
    parameters:
      mode: "fixed_fraction"
      fraction: 0.1
```

### Step 4: Update Strategy

```python
# cyberdelta/strategies/funding_rate_arbitrage.py

# DELETE old imports and types
from cyberdelta.core.risk_types import SizedOpportunity  # DELETE

# ADD new imports
from cyberdelta.core.risk_manager import RiskManager, RiskAnalysis

class FundingRateArbitrageStrategy:
    def __init__(self):
        # DELETE old storage
        # self.sized_opportunities: dict[Symbol, SizedOpportunity] = {}
        
        # ADD new storage
        self.risk_analyses: dict[Symbol, RiskAnalysis] = {}
    
    async def generate_signals(self):
        # DELETE old code
        # sized_opportunity = await self.risk_manager.size_opportunity(opportunity)
        # if (sized_opportunity 
        #     and sized_opportunity.long_size > Decimal(0)
        #     and sized_opportunity.short_size > Decimal(0)):
        
        # ADD new code
        analysis = await self.risk_manager.analyze_opportunity(opportunity)
        if analysis.approved:
            self.risk_analyses[opportunity.symbol] = analysis
            entry_signals = self._generate_entry_signal(
                opportunity, analysis, perp_ticker, spot_ticker
            )
    
    def _generate_entry_signal(self, opportunity, analysis: RiskAnalysis):
        # DELETE old code
        # perp_size = (sized_opportunity.long_size 
        #             if perp_side == OrderSide.BUY
        #             else sized_opportunity.short_size)
        
        # ADD new code - delta neutral = same size both sides
        position_size = analysis.sizing.position_size
        perp_size = position_size
        spot_size = position_size
```

### Step 5: Update Main.py

```python
# main.py
def _initialize_core_components(config: AppSettings) -> dict[str, Any]:
    # ... existing code ...
    
    # DELETE old initialization
    # risk_manager = RiskManager(
    #     config,
    #     portfolio_state_manager,
    #     circuit_breaker,  # REMOVED
    #     None,  # REMOVED
    #     risk_factory  # Was optional
    # )
    
    # ADD new initialization - risk_factory is REQUIRED
    risk_manager = RiskManager(
        config,
        portfolio_state_manager,
        risk_factory
    )
```

### Step 6: Update All Tests

```python
# DELETE all old test patterns
# mock_risk_manager.size_opportunity.return_value = SizedOpportunity(...)

# ADD new test patterns
mock_risk_manager.analyze_opportunity.return_value = RiskAnalysis(
    opportunity=opportunity,
    approved=True,
    sizing=SizingResult(
        success=True,
        position_size=Decimal("100"),
        risk_metrics={"sharpe_ratio": 0.5}
    )
)
```

### Step 7: Delete Legacy Code

```bash
# Files to delete
rm cyberdelta/core/risk_manager_original.py
rm cyberdelta/core/risk_types.py
rm cyberdelta/core/risk_manager_refactored.py  # If still exists
rm cyberdelta/core/risk_manager_breaking.py  # Example file

# Update imports globally
find . -name "*.py" -exec sed -i 's/from cyberdelta.core.risk_types import/from cyberdelta.core.risk_manager import/g' {} \;
```

## Summary of Changes

### What Gets Deleted
- `SizedOpportunity` class - gone forever
- `use_simple_sizing_path` config - gone
- All legacy config fields - gone
- 2,250 lines of duplicate code - gone

### What Gets Added
- `RiskAnalysis` class - clean, structured result
- `analyze_opportunity()` method - single entry point
- Modern config structure - explicit and clear

### API Changes
```python
# OLD API
sized_opp = await risk_manager.size_opportunity(opportunity)
if sized_opp:
    size = sized_opp.long_size

# NEW API
analysis = await risk_manager.analyze_opportunity(opportunity)
if analysis.approved:
    size = analysis.sizing.position_size
```

## Benefits

1. **87% less code** (2,599 → 350 lines)
2. **Clear separation** - validation, sizing, constraints
3. **Better types** - RiskAnalysis is self-documenting
4. **No legacy** - clean slate for future development
5. **Modular** - easy to extend and modify

## No Migration Needed

Since this is a development system:
- Just update the code
- Fix the tests
- Update the config
- Done

No backwards compatibility, no gradual rollout, no shadow testing. Just a clean refactor.