"""Risk manager orchestrator that coordinates all risk management components with AppSettings."""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.checks.pipeline.check_pipeline import CheckPipeline
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import ConstraintContext
from cyberdelta.core.risk.constraints.orchestrator.constraint_validator import ConstraintValidator
from cyberdelta.core.risk.exceptions.base_exceptions import RiskError
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity
from cyberdelta.core.risk.sizing.orchestrator.position_sizer import PositionSizer
from cyberdelta.core.symbols import Symbol
from cyberdelta.core.risk.utils.risk_metrics_calculator import (
    PortfolioSnapshot,
    RiskMetricsCalculator,
)
from cyberdelta.core.risk.utils.validation_factor_applier import ValidationFactorApplier
from cyberdelta.core.risk.utils.volatility_calculator import (
    PriceData,
    VolatilityCalculator,
    VolatilityResult,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from cyberdelta.core.risk.config.risk_module_config import RiskModuleConfig, load_risk_config_from_settings


def _create_str_list() -> list[str]:
    """Create typed string list for dataclass fields.

    Returns:
        Empty list of strings for dataclass default factory.
    """
    return []


def _create_str_any_dict() -> dict[str, Any]:
    """Create typed dict for dataclass fields.

    Returns:
        Empty dictionary with string keys and any values for dataclass default factory.
    """
    return {}


# Constants
MIN_SNAPSHOTS_FOR_RISK_METRICS = 30  # Minimum snapshots needed for comprehensive risk metrics


class ProcessingStatus(Enum):
    """Status of opportunity processing."""

    PENDING = "pending"
    CHECKING = "checking"
    SIZING = "sizing"
    VALIDATING = "validating"
    APPROVED = "approved"
    REJECTED = "rejected"
    ERROR = "error"


@dataclass
class ProcessedOpportunity:
    """Result of processing an arbitrage opportunity through risk management."""

    # Core data
    opportunity: ArbitrageOpportunity
    sized_opportunity: SizedOpportunity | None = None

    # Processing status
    status: ProcessingStatus = ProcessingStatus.PENDING
    approval_timestamp: datetime | None = None

    # Check results
    check_passed: bool = False
    check_warnings: list[str] = field(default_factory=_create_str_list)
    check_errors: list[str] = field(default_factory=_create_str_list)

    # Sizing results
    position_size_usd: Decimal = Decimal(0)
    allocation_percentage: Decimal = Decimal(0)
    kelly_fraction: Decimal | None = None

    # Constraint results
    constraints_passed: bool = False
    constraint_violations: list[str] = field(default_factory=_create_str_list)

    # Risk metrics
    expected_return: Decimal | None = None
    volatility: Decimal | None = None
    risk_adjusted_size: Decimal | None = None
    validation_factor: Decimal = Decimal("1.0")

    # Performance metrics
    total_processing_time_ms: float = 0
    check_time_ms: float = 0
    sizing_time_ms: float = 0
    validation_time_ms: float = 0

    # Metadata
    rejection_reason: str | None = None
    processing_metadata: dict[str, Any] = field(default_factory=_create_str_any_dict)

    @property
    def is_approved(self) -> bool:
        """Check if opportunity is approved."""
        return self.status == ProcessingStatus.APPROVED

    @property
    def is_rejected(self) -> bool:
        """Check if opportunity is rejected."""
        return self.status == ProcessingStatus.REJECTED

    @property
    def has_errors(self) -> bool:
        """Check if processing had errors."""
        return self.status == ProcessingStatus.ERROR or bool(self.check_errors)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of the processed opportunity with all metrics.
        """
        return {
            "opportunity": {
                "symbol": getattr(self.opportunity, "symbol", "unknown"),
                "long_exchange": getattr(self.opportunity, "long_exchange", "unknown"),
                "short_exchange": getattr(self.opportunity, "short_exchange", "unknown"),
                "spread_percentage": float(getattr(self.opportunity, "spread_percentage", 0)),
            },
            "status": self.status.value,
            "approval_timestamp": (
                self.approval_timestamp.isoformat() if self.approval_timestamp else None
            ),
            "check_results": {
                "passed": self.check_passed,
                "warnings": self.check_warnings,
                "errors": self.check_errors,
            },
            "sizing_results": {
                "position_size_usd": float(self.position_size_usd),
                "allocation_percentage": float(self.allocation_percentage),
                "kelly_fraction": float(self.kelly_fraction) if self.kelly_fraction else None,
            },
            "constraint_results": {
                "passed": self.constraints_passed,
                "violations": self.constraint_violations,
            },
            "risk_metrics": {
                "expected_return": float(self.expected_return) if self.expected_return else None,
                "volatility": float(self.volatility) if self.volatility else None,
                "risk_adjusted_size": (
                    float(self.risk_adjusted_size) if self.risk_adjusted_size else None
                ),
                "validation_factor": float(self.validation_factor),
            },
            "performance": {
                "total_processing_time_ms": self.total_processing_time_ms,
                "check_time_ms": self.check_time_ms,
                "sizing_time_ms": self.sizing_time_ms,
                "validation_time_ms": self.validation_time_ms,
            },
            "rejection_reason": self.rejection_reason,
            "is_approved": self.is_approved,
            "is_rejected": self.is_rejected,
            "has_errors": self.has_errors,
        }


class RiskManagerOrchestrator:
    """Main orchestrator for the modular risk management system with direct AppSettings access."""

    def __init__(
        self,
        app_settings: AppSettings,
        check_pipeline: CheckPipeline,
        position_sizer: PositionSizer,
        constraint_validator: ConstraintValidator,
    ) -> None:
        """Initialize the risk manager orchestrator with direct AppSettings access.

        Args:
            app_settings: Application settings with enhanced risk configuration
            check_pipeline: Pipeline for running checks
            position_sizer: Position sizing orchestrator
            constraint_validator: Constraint validation orchestrator
        """
        self.app_settings = app_settings
        self.risk_settings = app_settings.risk
        self.global_risk = app_settings.risk.global_risk
        self.sizing_settings = app_settings.risk.sizing
        self.check_pipeline = check_pipeline
        self.position_sizer = position_sizer
        self.constraint_validator = constraint_validator
        self.logger = get_logger(self.__class__.__name__)

        # Initialize utilities with AppSettings-derived configs
        self.volatility_calculator = VolatilityCalculator(self._create_volatility_config())
        self.validation_factor_applier = ValidationFactorApplier(
            self._create_validation_factor_config()
        )
        self.risk_metrics_calculator = RiskMetricsCalculator({})  # Keep default for now

        # Portfolio state
        self.current_positions: list[SizedOpportunity] = []
        self.portfolio_snapshots: list[PortfolioSnapshot] = []

        # Capital management from AppSettings
        self.total_account_value = self.sizing_settings.total_capital or Decimal("10000.0")
        self.available_capital = self.sizing_settings.total_capital or Decimal("10000.0")

        # Performance tracking
        self.opportunities_processed = 0
        self.opportunities_approved = 0
        self.opportunities_rejected = 0
        self.total_processing_time = 0.0

        # Load risk module configuration
        self.risk_config = load_risk_config_from_settings(app_settings)
        
        # Configuration from risk module config
        self.max_concurrent_processing = self.risk_config.orchestrator.max_concurrent_processing
        self.processing_timeout = self.risk_config.orchestrator.processing_timeout
        self.enable_parallel_processing = self.risk_config.orchestrator.enable_parallel_processing

    def _create_volatility_config(self) -> dict[str, Any]:
        """Create volatility calculator configuration from AppSettings.

        Returns:
            Configuration dictionary for volatility calculator.
        """
        return {
            "lookback_hours": self.risk_settings.checkers.volatility_lookback_hours,
            "min_volatility": float(self.sizing_settings.min_volatility),
            "max_volatility": float(self.sizing_settings.max_volatility_bound),
            "use_garch": self.risk_config.orchestrator.use_garch,
            "confidence_level": self.risk_config.orchestrator.confidence_level,
        }

    def _create_validation_factor_config(self) -> dict[str, Any]:
        """Create validation factor applier configuration from AppSettings.

        Returns:
            Configuration dictionary for validation factor applier.
        """
        return {
            "enable_validation_factors": self.sizing_settings.enable_validation_factors,
            "enable_volatility_adjustment": self.sizing_settings.enable_volatility_adjustment,
            "enable_spread_adjustment": self.sizing_settings.enable_spread_adjustment,
            "base_validation_factor": float(self.sizing_settings.base_validation_factor),
            "volatility_threshold": float(self.risk_settings.checkers.thresholds.max_volatility),
            "spread_threshold": float(self.risk_settings.checkers.thresholds.max_price_spread),
        }

    @classmethod
    def from_legacy_config(
        cls,
        legacy_config: dict[str, Any],
        app_settings: AppSettings,
        check_pipeline: CheckPipeline,
        position_sizer: PositionSizer,
        constraint_validator: ConstraintValidator,
    ) -> "RiskManagerOrchestrator":
        """Create RiskManagerOrchestrator from legacy config and AppSettings.

        Args:
            legacy_config: Legacy configuration dictionary (ignored in favor of AppSettings)
            app_settings: Application settings with enhanced risk configuration
            check_pipeline: Pipeline for running checks
            position_sizer: Position sizing orchestrator
            constraint_validator: Constraint validation orchestrator

        Returns:
            RiskManagerOrchestrator instance configured from AppSettings
        """
        # Use AppSettings and ignore legacy config - clean break refactoring
        return cls(app_settings, check_pipeline, position_sizer, constraint_validator)

    async def process_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        historical_prices: list[PriceData] | None = None,
    ) -> ProcessedOpportunity:
        """Process a single arbitrage opportunity through the risk management pipeline.

        Args:
            opportunity: The arbitrage opportunity to process
            historical_prices: Optional historical price data

        Returns:
            ProcessedOpportunity with complete processing results
        """
        start_time = time.time()

        result = ProcessedOpportunity(opportunity=opportunity)

        try:
            # Phase 1: Run checks
            result.status = ProcessingStatus.CHECKING
            check_start = time.time()

            check_result = await self.check_pipeline.run_checks(opportunity)
            result.check_time_ms = (time.time() - check_start) * 1000

            result.check_passed = check_result.passed
            # Extract warnings and errors from details if available
            if check_result.details:
                result.check_warnings = check_result.details.get("warnings", [])
                result.check_errors = check_result.details.get("errors", [])
            else:
                result.check_warnings = []
                result.check_errors = []
                if not check_result.passed and check_result.message:
                    result.check_errors = [check_result.message]

            if not check_result.passed:
                result.status = ProcessingStatus.REJECTED
                result.rejection_reason = f"Checks failed: {', '.join(result.check_errors)}"
                return result

            # Phase 2: Calculate volatility
            volatility_result = await self._calculate_volatility(opportunity, historical_prices)
            result.volatility = volatility_result.volatility

            # Phase 3: Size position
            result.status = ProcessingStatus.SIZING
            sizing_start = time.time()

            sizing_result = await self.position_sizer.size_opportunity(
                opportunity,
                self.available_capital,
            )
            result.sizing_time_ms = (time.time() - sizing_start) * 1000

            if not sizing_result.success:
                result.status = ProcessingStatus.REJECTED
                result.rejection_reason = f"Sizing failed: {sizing_result.message}"
                return result

            result.position_size_usd = sizing_result.position_size_usd
            result.allocation_percentage = sizing_result.allocation_percentage
            result.kelly_fraction = sizing_result.kelly_fraction
            result.risk_adjusted_size = sizing_result.risk_adjusted_size

            # Create sized opportunity
            sized_opportunity = self.position_sizer.create_sized_opportunity(
                opportunity,
                sizing_result,
            )
            result.sized_opportunity = sized_opportunity

            # Phase 4: Validate constraints
            result.status = ProcessingStatus.VALIDATING
            validation_start = time.time()

            constraint_context = self._create_constraint_context()
            constraint_result = await self.constraint_validator.validate_opportunity(
                sized_opportunity,
                constraint_context,
            )
            result.validation_time_ms = (time.time() - validation_start) * 1000

            result.constraints_passed = constraint_result.passed
            result.constraint_violations = [
                v.message for v in constraint_result.violations if v.is_blocking
            ]

            if not constraint_result.passed:
                result.status = ProcessingStatus.REJECTED
                result.rejection_reason = (
                    f"Constraints violated: {', '.join(result.constraint_violations)}"
                )
                return result

            # Phase 5: Apply validation factors
            validation_factor_result = self.validation_factor_applier.apply_validation_factors(
                opportunity,
                [check_result],
            )
            result.validation_factor = validation_factor_result.final_factor

            # Final approval
            result.status = ProcessingStatus.APPROVED
            result.approval_timestamp = datetime.now(tz=UTC)

            # Update metrics
            result.expected_return = sizing_result.expected_return

        except TimeoutError:
            result.status = ProcessingStatus.ERROR
            result.rejection_reason = "Processing timeout"
            self.logger.exception(
                "Processing timeout", symbol=getattr(opportunity, "symbol", "unknown")
            )

        except Exception as e:
            result.status = ProcessingStatus.ERROR
            result.rejection_reason = f"Processing error: {e!s}"
            self.logger.exception("Processing error")

        finally:
            # Calculate total processing time
            result.total_processing_time_ms = (time.time() - start_time) * 1000

            # Update statistics
            self._update_statistics(result)

            self.logger.debug(
                "Processed opportunity",
                status=result.status.value,
                processing_time_ms=result.total_processing_time_ms,
            )

        return result

    async def process_opportunities(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[ProcessedOpportunity]:
        """Process multiple opportunities concurrently.

        Args:
            opportunities: List of arbitrage opportunities

        Returns:
            List of ProcessedOpportunity results
        """
        if not opportunities:
            return []

        self.logger.info("Processing opportunities", count=len(opportunities))

        if self.enable_parallel_processing:
            return await self._process_opportunities_parallel(opportunities)
        return await self._process_opportunities_sequential(opportunities)

    async def _process_opportunities_parallel(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[ProcessedOpportunity]:
        """Process opportunities in parallel with concurrency control.

        Returns:
            List of processed opportunities with results from parallel processing.
        """
        results: list[ProcessedOpportunity] = []

        # Process in batches to control concurrency
        for i in range(0, len(opportunities), self.max_concurrent_processing):
            batch = opportunities[i : i + self.max_concurrent_processing]

            # Create tasks for batch
            tasks = [
                asyncio.create_task(
                    asyncio.wait_for(
                        self.process_opportunity(opp),
                        timeout=self.processing_timeout,
                    ),
                )
                for opp in batch
            ]

            # Wait for batch to complete
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process batch results
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    # Create error result
                    error_result = ProcessedOpportunity(
                        opportunity=batch[j],
                        status=ProcessingStatus.ERROR,
                        rejection_reason=f"Processing error: {result!s}",
                    )
                    results.append(error_result)
                elif isinstance(result, ProcessedOpportunity):
                    results.append(result)

        return results

    async def _process_opportunities_sequential(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[ProcessedOpportunity]:
        """Process opportunities sequentially.

        Returns:
            List of processed opportunities with results from sequential processing.
        """
        results: list[ProcessedOpportunity] = []

        for opportunity in opportunities:
            try:
                result = await asyncio.wait_for(
                    self.process_opportunity(opportunity),
                    timeout=self.processing_timeout,
                )
                results.append(result)
            except TimeoutError:
                error_result = ProcessedOpportunity(
                    opportunity=opportunity,
                    status=ProcessingStatus.ERROR,
                    rejection_reason="Processing timeout",
                )
                results.append(error_result)
            except RiskError as e:
                error_result = ProcessedOpportunity(
                    opportunity=opportunity,
                    status=ProcessingStatus.ERROR,
                    rejection_reason=f"Risk error: {e!s}",
                )
                results.append(error_result)
            except (ValueError, TypeError, ArithmeticError, KeyError, AttributeError) as e:
                # Create error result for known processing exceptions
                error_result = ProcessedOpportunity(
                    opportunity=opportunity,
                    status=ProcessingStatus.ERROR,
                    rejection_reason=f"Processing error: {e!s}",
                )
                results.append(error_result)

        return results

    async def _calculate_volatility(
        self,
        opportunity: ArbitrageOpportunity,
        historical_prices: list[PriceData] | None,
    ) -> VolatilityResult:
        """Calculate volatility for opportunity.

        Returns:
            Volatility calculation result with computed volatility metrics.
        """
        return self.volatility_calculator.calculate_volatility_for_opportunity(
            opportunity,
            historical_prices,
        )

    def _create_constraint_context(self) -> ConstraintContext:
        """Create constraint validation context.

        Returns:
            Constraint context with current portfolio state for validation.
        """
        # Calculate current allocations
        current_allocations: dict[str, Decimal] = {}
        current_exchange_allocations: dict[str, Decimal] = {}

        for position in self.current_positions:
            symbol = position.symbol
            long_exchange = position.long_exchange
            short_exchange = position.short_exchange

            # Symbol allocation - use symbol.value as key
            if symbol.value not in current_allocations:
                current_allocations[symbol.value] = Decimal(0)
            current_allocations[symbol.value] += position.allocation_percentage

            # Exchange allocations
            for exchange in [long_exchange, short_exchange]:
                if exchange not in current_exchange_allocations:
                    current_exchange_allocations[exchange] = Decimal(0)
                current_exchange_allocations[exchange] += position.allocation_percentage / 2

        # Calculate current leverage
        total_position_value = sum(pos.total_size_usd for pos in self.current_positions)
        current_leverage = (
            total_position_value / self.total_account_value if self.total_account_value > 0 else Decimal(0)
        )

        return ConstraintContext(
            total_capital=self.total_account_value,
            available_capital=self.available_capital,
            reserved_capital=self.total_account_value - self.available_capital,
            current_positions=self.current_positions,
            current_allocations=current_allocations,
            current_exchange_allocations=current_exchange_allocations,
            current_leverage=current_leverage,
        )

    def _update_statistics(self, result: ProcessedOpportunity) -> None:
        """Update orchestrator statistics."""
        self.opportunities_processed += 1
        self.total_processing_time += result.total_processing_time_ms

        if result.is_approved:
            self.opportunities_approved += 1
        elif result.is_rejected:
            self.opportunities_rejected += 1

    def add_position(self, sized_opportunity: SizedOpportunity) -> None:
        """Add an approved position to the portfolio."""
        self.current_positions.append(sized_opportunity)

        # Update available capital
        self.position_sizer.reserve_capital(sized_opportunity.total_size_usd)
        self.available_capital = self.position_sizer.get_available_capital(self.total_account_value)

        # Create portfolio snapshot
        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(tz=UTC),
            total_value=self.total_account_value,
            positions=self.current_positions.copy(),
            cash_balance=self.available_capital,
            total_exposure=sum((pos.total_size_usd for pos in self.current_positions), Decimal(0)),
            # Simplified
            net_exposure=(
                sum((pos.total_size_usd for pos in self.current_positions), Decimal(0))
                * Decimal("0.1")
            ),
            gross_leverage=(
                sum((pos.total_size_usd for pos in self.current_positions), Decimal(0))
                / self.total_account_value
                if self.total_account_value > 0
                else Decimal(0)
            ),
        )
        self.portfolio_snapshots.append(snapshot)

        self.logger.info(
            "Added position",
            symbol=sized_opportunity.symbol,
            size_usd=float(sized_opportunity.total_size_usd),
        )

    def remove_position(self, symbol: Symbol) -> bool:
        """Remove a position from the portfolio.

        Returns:
            True if position was found and removed, False otherwise.
        """
        for i, position in enumerate(self.current_positions):
            if position.symbol == symbol:
                # Release capital
                self.position_sizer.release_capital(position.total_size_usd)
                self.available_capital = self.position_sizer.get_available_capital(
                    self.total_account_value
                )

                # Remove position
                self.current_positions.pop(i)

                self.logger.info("Removed position", symbol=symbol)
                return True

        return False

    def get_portfolio_metrics(self) -> dict[str, Any]:
        """Get current portfolio metrics.

        Returns:
            Dictionary with portfolio metrics including value, positions, and risk data.
        """
        if len(self.portfolio_snapshots) >= MIN_SNAPSHOTS_FOR_RISK_METRICS:
            # Calculate comprehensive risk metrics
            risk_metrics = self.risk_metrics_calculator.calculate_risk_metrics(
                self.portfolio_snapshots[-MIN_SNAPSHOTS_FOR_RISK_METRICS:],  # Last 30 snapshots
            )
            risk_dict = risk_metrics.to_dict()
        else:
            risk_dict = None

        return {
            "portfolio_value": float(self.total_account_value),
            "available_capital": float(self.available_capital),
            "position_count": len(self.current_positions),
            "total_exposure": float(sum(pos.total_size_usd for pos in self.current_positions)),
            "positions": [
                {
                    "symbol": pos.symbol,
                    "size": float(pos.total_size_usd),
                    "allocation": float(pos.allocation_percentage),
                }
                for pos in self.current_positions
            ],
            "risk_metrics": risk_dict,
        }

    def get_orchestrator_stats(self) -> dict[str, Any]:
        """Get orchestrator statistics.

        Returns:
            Dictionary with processing statistics and performance metrics.
        """
        avg_processing_time = (
            self.total_processing_time / self.opportunities_processed
            if self.opportunities_processed > 0
            else 0
        )

        approval_rate = (
            self.opportunities_approved / self.opportunities_processed
            if self.opportunities_processed > 0
            else 0
        )

        return {
            "opportunities_processed": self.opportunities_processed,
            "opportunities_approved": self.opportunities_approved,
            "opportunities_rejected": self.opportunities_rejected,
            "approval_rate": approval_rate,
            "average_processing_time_ms": avg_processing_time,
            "total_processing_time_ms": self.total_processing_time,
            "current_positions": len(self.current_positions),
            "available_capital": float(self.available_capital),
            "portfolio_snapshots": len(self.portfolio_snapshots),
        }

    def reset_statistics(self) -> None:
        """Reset orchestrator statistics."""
        self.opportunities_processed = 0
        self.opportunities_approved = 0
        self.opportunities_rejected = 0
        self.total_processing_time = 0.0
        self.logger.info("Reset orchestrator statistics")

    def set_capital(self, total_capital: Decimal) -> None:
        """Update total capital."""
        self.total_account_value = total_capital
        self.available_capital = self.position_sizer.get_available_capital(total_capital)
        self.logger.info("Set total capital", total_capital_usd=float(total_capital))

    def __str__(self) -> str:
        """String representation.

        Returns:
            Human-readable string with basic orchestrator state.
        """
        return (
            f"RiskManagerOrchestrator(positions={len(self.current_positions)}, "
            f"available=${self.available_capital:.2f})"
        )

    def __repr__(self) -> str:
        """Detailed representation.

        Returns:
            Detailed string representation for debugging with all key metrics.
        """
        return (
            f"RiskManagerOrchestrator(positions={len(self.current_positions)}, "
            f"total_capital=${self.total_account_value:.2f}, "
            f"available_capital=${self.available_capital:.2f}, "
            f"processed={self.opportunities_processed})"
        )
