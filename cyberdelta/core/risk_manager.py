"""Modern risk manager with clean API."""

from decimal import Decimal
from typing import Any

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.risk.checks.pipeline import CheckPipeline
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import ConstraintContext
from cyberdelta.core.risk.constraints.orchestrator import ConstraintValidator
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity, SizingResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


logger = get_logger(__name__)


class RiskAnalysis:
    """Complete risk analysis result."""

    def __init__(
        self,
        opportunity: ArbitrageOpportunity,
        approved: bool,
        sizing: SizingResult | None = None,
        checks: dict[str, Any] | None = None,
        constraints: dict[str, Any] | None = None,
        rejection_reason: str | None = None,
    ) -> None:
        """Initialize risk analysis result.

        Args:
            opportunity: The arbitrage opportunity analyzed
            approved: Whether the opportunity was approved for trading
            sizing: Sizing result if approved
            checks: Check pipeline results
            constraints: Constraint validation results
            rejection_reason: Reason for rejection if not approved
        """
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
        self.position_sizer = risk_factory.create_position_sizer(app_settings.risk.sizing.method)
        self.check_pipeline = self._create_check_pipeline()
        self.constraint_validator = self._create_constraint_validator()

        logger.info("risk_manager_initialized", sizing_method=app_settings.risk.sizing.method)

    def _create_check_pipeline(self) -> CheckPipeline:
        """Create check pipeline from configuration.

        Returns:
            CheckPipeline configured with empty checkers list
        """
        return CheckPipeline(checkers=[])

    def _create_constraint_validator(self) -> ConstraintValidator:
        """Create constraint validator from configuration.

        Returns:
            ConstraintValidator configured with app settings
        """
        return ConstraintValidator(self.app_settings)

    async def analyze_opportunity(self, opportunity: ArbitrageOpportunity) -> RiskAnalysis:
        """Analyze opportunity through complete risk pipeline.

        Args:
            opportunity: The arbitrage opportunity to analyze

        Returns:
            RiskAnalysis with complete risk assessment
        """
        logger.info(
            "analyzing_opportunity",
            symbol=opportunity.symbol,
            exchanges=f"{opportunity.long_exchange}/{opportunity.short_exchange}",
        )

        # Run checks
        check_results = await self.check_pipeline.run_pipeline(opportunity)
        if not check_results.passed:
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                checks=check_results.to_dict(),
                rejection_reason=check_results.get_failure_reason(),
            )

        # Get available capital
        portfolio_state = await self.portfolio_state_manager.get_portfolio_summary()
        available_capital = portfolio_state.total_account_value

        if available_capital <= Decimal(0):
            return RiskAnalysis(
                opportunity=opportunity, approved=False, rejection_reason="Insufficient capital"
            )

        # Size the opportunity
        sizing_result = await self.position_sizer.size_opportunity(opportunity, available_capital)

        if not sizing_result.success:
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                sizing=sizing_result,
                rejection_reason=sizing_result.message,
            )

        # Validate constraints
        constraint_context = ConstraintContext(
            total_capital=portfolio_state.total_account_value,
            available_capital=portfolio_state.free_collateral,
            reserved_capital=portfolio_state.total_account_value - portfolio_state.free_collateral,
            current_positions=[],
            current_allocations={},
            current_exchange_allocations={},
            current_leverage=portfolio_state.leverage,
            current_risk_metrics=None,
        )

        sized_opportunity = SizedOpportunity(
            opportunity=opportunity,
            sizing_result=sizing_result,
            long_size_usd=sizing_result.position_size_usd,
            short_size_usd=sizing_result.position_size_usd,
        )

        constraint_results = await self.constraint_validator.validate_opportunity(
            sized_opportunity, constraint_context
        )

        if not constraint_results.passed:
            return RiskAnalysis(
                opportunity=opportunity,
                approved=False,
                sizing=sizing_result,
                constraints=constraint_results.to_dict(),
                rejection_reason=constraint_results.get_failure_reason(),
            )

        # All checks passed
        return RiskAnalysis(
            opportunity=opportunity,
            approved=True,
            sizing=sizing_result,
            checks=check_results.to_dict(),
            constraints=constraint_results.to_dict(),
        )

    async def get_risk_metrics(self) -> dict[str, Any]:
        """Get current risk metrics.

        Returns:
            Dictionary containing current risk metrics
        """
        portfolio_state = await self.portfolio_state_manager.get_portfolio_summary()

        return {
            "portfolio_exposure": float(portfolio_state.gross_exposure),
            "available_capital": float(portfolio_state.total_account_value),
            "sizing_method": self.app_settings.risk.sizing.method,
            "position_sizer_stats": self.position_sizer.get_performance_stats(),
        }
