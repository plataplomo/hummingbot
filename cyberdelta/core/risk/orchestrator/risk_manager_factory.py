"""Factory for creating configured RiskManagerOrchestrator instances with clean architecture."""

from typing import Literal, cast

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger

# Import the actual protocol types
from cyberdelta.core.risk.checks.checkers.circuit_breaker_checker import (
    CircuitBreakerChecker,
    CircuitBreakerSystemProtocol,
)
from cyberdelta.core.risk.checks.checkers.exchange_balance_checker import (
    ExchangeBalanceChecker,
    PortfolioTrackerProtocol,
)
from cyberdelta.core.risk.checks.checkers.funding_rate_checker import (
    FundingRateChecker,
    FundingRateValidatorProtocol,
)
from cyberdelta.core.risk.checks.checkers.price_sanity_checker import PriceSanityChecker
from cyberdelta.core.risk.checks.checkers.profitability_checker import ProfitabilityChecker
from cyberdelta.core.risk.checks.checkers.required_fields_checker import RequiredFieldsChecker
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.checkers.volatility_checker import VolatilityChecker
from cyberdelta.core.risk.checks.models.check_result import CheckResult
from cyberdelta.core.risk.checks.pipeline.check_pipeline import CheckPipeline, PipelineChecker
from cyberdelta.core.risk.config.migration import ConfigurationMigrator
from cyberdelta.core.risk.constraints.orchestrator.constraint_validator import ConstraintValidator
from cyberdelta.core.risk.exceptions.base_exceptions import RiskConfigError
from cyberdelta.core.risk.orchestrator.risk_manager_orchestrator import RiskManagerOrchestrator
from cyberdelta.core.risk.sizing.orchestrator.position_sizer import PositionSizer
from cyberdelta.core.risk.sizing.strategies.kelly_criterion_sizer import KellyCriterionSizer
from cyberdelta.core.risk.sizing.strategies.simple_sizer import SimpleSizer
from cyberdelta.core.risk.sizing.strategies.typed_base_sizer import TypedBaseSizer


class RiskManagerFactory:
    """Factory for creating configured RiskManagerOrchestrator instances."""

    @staticmethod
    def create_risk_manager(
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTrackerProtocol,
        circuit_breaker_system: CircuitBreakerSystemProtocol | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
    ) -> RiskManagerOrchestrator:
        """Create a complete risk manager orchestrator with all components.

        Args:
            app_settings: Application settings with enhanced risk configuration
            portfolio_tracker: Portfolio tracker protocol
            circuit_breaker_system: Optional circuit breaker system
            funding_rate_validator: Optional funding rate validator

        Returns:
            Configured RiskManagerOrchestrator instance
        """
        logger = get_logger("RiskManagerFactory")
        logger.info("Creating risk manager orchestrator")

        # Create check pipeline
        checkers = RiskManagerFactory._create_checkers(
            app_settings=app_settings,
            circuit_breaker_system=circuit_breaker_system,
            funding_rate_validator=funding_rate_validator,
            portfolio_tracker=portfolio_tracker,
        )

        # Cast to PipelineChecker list for mypy compatibility
        pipeline_checkers = cast(list[PipelineChecker], checkers)
        check_pipeline = CheckPipeline(checkers=pipeline_checkers)

        # Configure pipeline settings from AppSettings
        check_pipeline.stop_on_first_failure = app_settings.risk.checkers.fail_fast
        check_pipeline.max_concurrent_checks = app_settings.risk.checkers.max_concurrent_checks
        check_pipeline.timeout_seconds = app_settings.risk.checkers.check_timeout_seconds

        # Create position sizer
        position_sizer = RiskManagerFactory._create_position_sizer(app_settings)

        # Create constraint validator
        constraint_validator = RiskManagerFactory._create_constraint_validator(app_settings)

        # Create orchestrator
        orchestrator = RiskManagerOrchestrator(
            app_settings=app_settings,
            check_pipeline=check_pipeline,
            position_sizer=position_sizer,
            constraint_validator=constraint_validator,
        )

        logger.info("Created risk manager orchestrator", checker_count=len(checkers))
        return orchestrator

    @staticmethod
    def create_minimal_risk_manager(
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTrackerProtocol,
    ) -> RiskManagerOrchestrator:
        """Create a minimal risk manager for testing or simple use cases.

        Args:
            app_settings: Application settings
            portfolio_tracker: Portfolio tracker

        Returns:
            Minimal RiskManagerOrchestrator instance
        """
        return RiskManagerFactory.create_risk_manager(
            app_settings=app_settings,
            portfolio_tracker=portfolio_tracker,
        )

    @staticmethod
    def create_from_preset(
        preset_name: Literal["conservative", "moderate", "aggressive"],
        base_app_settings: AppSettings,
        portfolio_tracker: PortfolioTrackerProtocol,
        circuit_breaker_system: CircuitBreakerSystemProtocol | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
    ) -> RiskManagerOrchestrator:
        """Create risk manager from a configuration preset.

        Args:
            preset_name: Preset name (conservative, moderate, aggressive)
            base_app_settings: Base application settings to modify
            portfolio_tracker: Portfolio tracker
            circuit_breaker_system: Optional circuit breaker system
            funding_rate_validator: Optional funding rate validator

        Returns:
            RiskManagerOrchestrator configured with preset

        Raises:
            RiskConfigError: If preset name is invalid
        """
        if preset_name not in {"conservative", "moderate", "aggressive"}:
            raise RiskConfigError(
                RiskConfigError.UNKNOWN_PRESET_CONFIGURATION,
                config_field="preset_name",
                config_value=preset_name,
            )

        # Apply preset configuration to AppSettings
        preset_config = ConfigurationMigrator.apply_preset(
            base_config=base_app_settings.model_dump(),
            preset_name=preset_name,
        )

        # Create new AppSettings with preset configuration
        app_settings = AppSettings.model_validate(preset_config)

        return RiskManagerFactory.create_risk_manager(
            app_settings=app_settings,
            portfolio_tracker=portfolio_tracker,
            circuit_breaker_system=circuit_breaker_system,
            funding_rate_validator=funding_rate_validator,
        )

    @staticmethod
    def _create_checkers(
        app_settings: AppSettings,
        circuit_breaker_system: CircuitBreakerSystemProtocol | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
        portfolio_tracker: PortfolioTrackerProtocol | None = None,
    ) -> list[TypedBaseChecker[CheckResult]]:
        """Create checkers based on AppSettings configuration.

        Args:
            app_settings: Application settings with enhanced risk configuration
            circuit_breaker_system: Optional circuit breaker system
            funding_rate_validator: Optional funding rate validator
            portfolio_tracker: Optional portfolio tracker

        Returns:
            List of configured checkers
        """
        checkers: list[TypedBaseChecker[CheckResult]] = []

        # Required fields checker
        if app_settings.risk.checkers.enable_required_fields:
            required_fields_checker = RequiredFieldsChecker(app_settings)
            checkers.append(required_fields_checker)

        # Profitability checker
        if app_settings.risk.checkers.enable_profitability:
            profitability_checker = ProfitabilityChecker(app_settings)
            checkers.append(profitability_checker)

        # Circuit breaker checker
        if app_settings.risk.checkers.enable_circuit_breaker and circuit_breaker_system:
            circuit_breaker_checker = CircuitBreakerChecker(
                app_settings,
                circuit_breaker_system,
            )
            checkers.append(circuit_breaker_checker)

        # Price sanity checker
        if app_settings.risk.checkers.enable_price_sanity:
            price_sanity_checker = PriceSanityChecker(app_settings)
            checkers.append(price_sanity_checker)

        # Funding rate checker
        if app_settings.risk.checkers.enable_funding_rate and funding_rate_validator:
            funding_rate_checker = FundingRateChecker(
                app_settings,
                funding_rate_validator,
            )
            checkers.append(funding_rate_checker)

        # Volatility checker
        if app_settings.risk.checkers.enable_volatility:
            volatility_checker = VolatilityChecker(app_settings)
            checkers.append(volatility_checker)

        # Exchange balance checker
        if app_settings.risk.checkers.enable_balance and portfolio_tracker:
            exchange_balance_checker = ExchangeBalanceChecker(
                app_settings,
                portfolio_tracker,
            )
            checkers.append(exchange_balance_checker)

        return checkers

    @staticmethod
    def _create_position_sizer(app_settings: AppSettings) -> PositionSizer:
        """Create position sizer based on AppSettings configuration.

        Args:
            app_settings: Application settings with enhanced risk configuration

        Returns:
            Configured PositionSizer
        """
        # Create sizing strategy based on method
        if app_settings.risk.sizing.method == "kelly":
            sizer: TypedBaseSizer = KellyCriterionSizer(app_settings)
        else:  # simple
            sizer = SimpleSizer(app_settings)

        # Create position sizer orchestrator
        return PositionSizer(
            sizer=sizer,
            app_settings=app_settings,
        )

    @staticmethod
    def _create_constraint_validator(app_settings: AppSettings) -> ConstraintValidator:
        """Create constraint validator based on AppSettings configuration.

        Args:
            app_settings: Application settings with enhanced risk configuration

        Returns:
            Configured ConstraintValidator
        """
        return ConstraintValidator(app_settings=app_settings)
