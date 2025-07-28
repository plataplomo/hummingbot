"""Constraint validator orchestrator with direct AppSettings access."""

import asyncio
import decimal
import time
from typing import Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.constraints.checkers.exchange_constraint_checker import (
    ExchangeConstraintChecker,
)
from cyberdelta.core.risk.constraints.checkers.leverage_constraint_checker import (
    LeverageConstraintChecker,
)
from cyberdelta.core.risk.constraints.checkers.portfolio_constraint_checker import (
    PortfolioConstraintChecker,
)
from cyberdelta.core.risk.constraints.checkers.position_constraint_checker import (
    PositionConstraintChecker,
)
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import (
    ConstraintContext,
    ConstraintInterface,
    ConstraintResult,
    ConstraintResultStatus,
)
from cyberdelta.core.risk.constraints.models.constraint_models import ConstraintViolation
from cyberdelta.core.risk.exceptions.base_exceptions import RiskError
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity


class ConstraintValidator:
    """Orchestrates constraint validation across multiple validators with AppSettings access."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the constraint validator with direct AppSettings access.

        Args:
            app_settings: Application settings with enhanced risk configuration
        """
        self.app_settings = app_settings
        self.risk_settings = app_settings.risk
        self.global_risk = app_settings.risk.global_risk
        self.logger = get_logger(self.__class__.__name__)

        # Initialize constraint checkers
        self.validators: list[ConstraintInterface] = []
        self._initialize_validators()

        # Configuration from AppSettings
        self.fail_fast = self.risk_settings.checkers.fail_fast
        self.max_concurrent_validators = 4  # Hardcoded default as not in enhanced config
        self.validation_timeout = 30.0  # Hardcoded default as not in enhanced config

        # Performance tracking
        self.validation_count = 0
        self.total_validation_time = 0.0
        self.validation_success_rate = 0.0

    def _initialize_validators(self) -> None:
        """Initialize constraint validators using AppSettings configuration."""
        # Position constraint checker - always enabled by default
        position_checker = PositionConstraintChecker(
            config=self._create_position_config(),
        )
        self.validators.append(position_checker)

        # Portfolio constraint checker - always enabled by default
        portfolio_checker = PortfolioConstraintChecker(
            config=self._create_portfolio_config(),
        )
        self.validators.append(portfolio_checker)

        # Exchange constraint checker - always enabled by default
        exchange_checker = ExchangeConstraintChecker(
            config=self._create_exchange_config(),
        )
        self.validators.append(exchange_checker)

        # Leverage constraint checker - always enabled by default
        leverage_checker = LeverageConstraintChecker(
            config=self._create_leverage_config(),
        )
        self.validators.append(leverage_checker)

        self.logger.info(
            "Initialized constraint validators from AppSettings", count=len(self.validators)
        )

    def _create_position_config(self) -> dict[str, Any]:
        """Create position constraint configuration from AppSettings.
        
        Returns:
            Dictionary containing position constraint configuration settings
        """
        return {
            "enabled": True,
            "min_position_size": float(self.risk_settings.sizing.min_position_size),
            "max_position_size": float(self.risk_settings.sizing.max_position_size),
            "min_allocation_percentage": float(self.risk_settings.sizing.kelly_min_allocation),
            "max_allocation_percentage": float(self.risk_settings.sizing.max_portfolio_allocation),
            "max_leverage": float(self.risk_settings.sizing.max_leverage),
            "max_positions_per_symbol": 1,  # Hardcoded default
            "max_positions_per_exchange": 10,  # Hardcoded default
            "max_risk_per_position": float(self.global_risk.max_position_usd),
            "max_volatility_per_position": float(
                self.risk_settings.checkers.thresholds.max_volatility
            ),
        }

    def _create_portfolio_config(self) -> dict[str, Any]:
        """Create portfolio constraint configuration from AppSettings.
        
        Returns:
            Dictionary containing portfolio constraint configuration settings
        """
        return {
            "enabled": True,
            "max_total_exposure": float(self.global_risk.max_total_exposure_usd),
            "max_portfolio_allocation": float(self.risk_settings.sizing.max_portfolio_allocation),
            "max_correlation_exposure": 0.8,  # Hardcoded default
            "max_sector_exposure": 0.5,  # Hardcoded default
        }

    def _create_exchange_config(self) -> dict[str, Any]:
        """Create exchange constraint configuration from AppSettings.
        
        Returns:
            Dictionary containing exchange constraint configuration settings
        """
        return {
            "enabled": True,
            "max_exchange_allocation": 0.6,  # Hardcoded default - 60% max per exchange
            "max_exchange_leverage": float(self.risk_settings.sizing.max_leverage),
            "min_exchange_balance_ratio": float(
                self.risk_settings.checkers.thresholds.min_balance_ratio
            ),
        }

    def _create_leverage_config(self) -> dict[str, Any]:
        """Create leverage constraint configuration from AppSettings.
        
        Returns:
            Dictionary containing leverage constraint configuration settings
        """
        return {
            "enabled": True,
            "max_leverage": float(self.risk_settings.sizing.max_leverage),
            "max_portfolio_leverage": float(self.risk_settings.sizing.max_leverage) * 0.8,
            "leverage_buffer": 0.1,  # 10% buffer
        }

    async def validate_opportunity(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> ConstraintResult:
        """Validate constraints for a sized opportunity.

        Args:
            opportunity: The sized opportunity to validate
            context: Context information for validation

        Returns:
            ConstraintResult with validation results
        """
        if not self.validators:
            return ConstraintResult.passed_result(
                message="No constraint validators configured",
                details={"validators_count": 0},
            )

        start_time = time.time()

        try:
            self.logger.debug("Starting constraint validation", symbol=opportunity.symbol)

            # Run validators
            if self.fail_fast:
                results = await self._validate_fail_fast(opportunity, context)
            else:
                results = await self._validate_all(opportunity, context)

            # Aggregate results
            aggregated_result = self._aggregate_results(results)

            # Update performance metrics
            execution_time_ms = (time.time() - start_time) * 1000
            aggregated_result.execution_time_ms = execution_time_ms
            aggregated_result.constraints_checked = len(results)

            self._update_performance_metrics(aggregated_result)

            self.logger.debug(
                "Constraint validation completed", status=aggregated_result.status.value
            )

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            self.logger.exception("Constraint validation error")
            return ConstraintResult.failed_result(
                violations=[],
                message=f"Validation error: {e!s}",
                details={"error": str(e)},
                execution_time_ms=execution_time_ms,
            )
        else:
            return aggregated_result

    async def _validate_fail_fast(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintResult]:
        """Validate with fail-fast strategy.
        
        Returns:
            List of constraint validation results, stops at first failure
        """
        results: list[ConstraintResult] = []

        for validator in self.validators:
            if not validator.is_enabled():
                continue

            try:
                result = await asyncio.wait_for(
                    validator.validate(opportunity, context),
                    timeout=self.validation_timeout,
                )
                results.append(result)

                # Stop on first failure
                if result.failed:
                    self.logger.debug(
                        "Fail-fast: stopped at validator", validator_name=validator.name
                    )
                    break

            except TimeoutError:
                timeout_result = ConstraintResult.failed_result(
                    violations=[],
                    message=f"Validation timeout for {validator.name}",
                    details={"validator": validator.name, "timeout": self.validation_timeout},
                )
                results.append(timeout_result)
                break

            except (RiskError, ValueError, TypeError) as e:
                error_result = ConstraintResult.failed_result(
                    violations=[],
                    message=f"Validation error in {validator.name}: {e!s}",
                    details={
                        "validator": validator.name,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                results.append(error_result)
                break
            except (
                KeyError,
                AttributeError,
                decimal.InvalidOperation,
                decimal.DivisionByZero,
            ) as e:
                error_result = ConstraintResult.failed_result(
                    violations=[],
                    message=f"Validation error in {validator.name}: {e!s}",
                    details={
                        "validator": validator.name,
                        "error": str(e),
                        "error_type": "validation_error",
                    },
                )
                results.append(error_result)
                break

        return results

    async def _validate_all(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintResult]:
        """Validate with all validators.
        
        Returns:
            List of constraint validation results from all enabled validators
        """
        results: list[ConstraintResult] = []

        # Create validation tasks
        tasks: list[tuple[ConstraintInterface, asyncio.Task[ConstraintResult]]] = []
        for validator in self.validators:
            if validator.is_enabled():
                task = asyncio.create_task(
                    asyncio.wait_for(
                        validator.validate(opportunity, context),
                        timeout=self.validation_timeout,
                    ),
                )
                tasks.append((validator, task))

        # Execute in batches to control concurrency
        for i in range(0, len(tasks), self.max_concurrent_validators):
            batch = tasks[i : i + self.max_concurrent_validators]

            # Wait for batch to complete
            batch_results = await asyncio.gather(
                *[task for _, task in batch],
                return_exceptions=True,
            )

            # Process batch results
            for j, result in enumerate(batch_results):
                validator, _ = batch[j]

                if isinstance(result, asyncio.TimeoutError):
                    timeout_result = ConstraintResult.failed_result(
                        violations=[],
                        message=f"Validation timeout for {validator.name}",
                        details={"validator": validator.name, "timeout": self.validation_timeout},
                    )
                    results.append(timeout_result)

                elif isinstance(result, Exception):
                    error_result = ConstraintResult.failed_result(
                        violations=[],
                        message=f"Validation error in {validator.name}: {result!s}",
                        details={"validator": validator.name, "error": str(result)},
                    )
                    results.append(error_result)

                elif isinstance(result, ConstraintResult):
                    results.append(result)

        return results

    def _aggregate_results(self, results: list[ConstraintResult]) -> ConstraintResult:
        """Aggregate validation results.
        
        Returns:
            Single aggregated constraint result with combined status and violations
        """
        if not results:
            return ConstraintResult.passed_result(
                message="No validation results to aggregate",
                details={"results_count": 0},
            )

        # Collect all violations
        all_violations: list[ConstraintViolation] = []
        for result in results:
            all_violations.extend(result.violations)

        # Categorize violations
        blocking_violations = [v for v in all_violations if v.is_blocking]
        warning_violations = [v for v in all_violations if not v.is_blocking]

        # Determine overall status
        if blocking_violations:
            status = ConstraintResultStatus.FAILED
            message = (
                f"Constraint validation failed: {len(blocking_violations)} blocking violations"
            )
        elif warning_violations:
            status = ConstraintResultStatus.WARNING
            message = (
                f"Constraint validation passed with warnings: {len(warning_violations)} warnings"
            )
        else:
            status = ConstraintResultStatus.PASSED
            message = "All constraint validations passed"

        # Calculate aggregate metrics
        total_execution_time = sum(
            r.execution_time_ms for r in results if r.execution_time_ms is not None
        )
        total_constraints_checked = sum(r.constraints_checked for r in results)

        # Create aggregated result
        return ConstraintResult(
            status=status,
            violations=all_violations,
            message=message,
            details={
                "total_validators": len(results),
                "passed_validators": len([r for r in results if r.passed]),
                "failed_validators": len([r for r in results if r.failed]),
                "warning_validators": len([r for r in results if r.has_warnings]),
                "blocking_violations": len(blocking_violations),
                "warning_violations": len(warning_violations),
                "validator_results": [
                    {
                        "validator": getattr(r, "validator_name", "unknown"),
                        "status": r.status.value,
                        "violations": len(r.violations),
                        "execution_time_ms": r.execution_time_ms,
                    }
                    for r in results
                ],
            },
            execution_time_ms=total_execution_time,
            constraints_checked=total_constraints_checked,
        )

    def _update_performance_metrics(self, result: ConstraintResult) -> None:
        """Update performance metrics."""
        self.validation_count += 1

        if result.execution_time_ms:
            self.total_validation_time += result.execution_time_ms

        # Update success rate
        if result.passed:
            self.validation_success_rate = (
                self.validation_success_rate * (self.validation_count - 1) + 1
            ) / self.validation_count
        else:
            self.validation_success_rate = (
                self.validation_success_rate * (self.validation_count - 1)
            ) / self.validation_count

    def add_validator(self, validator: ConstraintInterface) -> None:
        """Add a custom validator."""
        self.validators.append(validator)
        self.logger.info("Added validator", validator_name=validator.name)

    def remove_validator(self, validator_name: str) -> None:
        """Remove a validator by name."""
        self.validators = [v for v in self.validators if v.name != validator_name]
        self.logger.info("Removed validator", validator_name=validator_name)

    def get_validator(self, validator_name: str) -> ConstraintInterface | None:
        """Get a validator by name.
        
        Returns:
            Validator instance if found, None otherwise
        """
        for validator in self.validators:
            if validator.name == validator_name:
                return validator
        return None

    def enable_validator(self, validator_name: str) -> None:
        """Enable a validator."""
        validator = self.get_validator(validator_name)
        if validator:
            validator.enable()
            self.logger.info("Enabled validator", validator_name=validator_name)

    def disable_validator(self, validator_name: str) -> None:
        """Disable a validator."""
        validator = self.get_validator(validator_name)
        if validator:
            validator.disable()
            self.logger.info("Disabled validator", validator_name=validator_name)

    def set_fail_fast(self, enabled: bool) -> None:
        """Set fail-fast mode."""
        self.fail_fast = enabled
        self.logger.info("Set fail-fast mode", enabled=enabled)

    def set_concurrency_limits(self, max_concurrent: int, timeout: float) -> None:
        """Set concurrency limits."""
        self.max_concurrent_validators = max_concurrent
        self.validation_timeout = timeout
        self.logger.info(
            "Set concurrency limits", max_concurrent=max_concurrent, timeout_seconds=timeout
        )

    def get_performance_stats(self) -> dict[str, Any]:
        """Get performance statistics.
        
        Returns:
            Dictionary containing validation performance statistics
        """
        avg_validation_time = (
            self.total_validation_time / self.validation_count if self.validation_count > 0 else 0
        )

        return {
            "validation_count": self.validation_count,
            "average_validation_time_ms": avg_validation_time,
            "success_rate": self.validation_success_rate,
            "total_validators": len(self.validators),
            "enabled_validators": len([v for v in self.validators if v.is_enabled()]),
            "disabled_validators": len([v for v in self.validators if not v.is_enabled()]),
            "fail_fast_enabled": self.fail_fast,
            "max_concurrent_validators": self.max_concurrent_validators,
            "validation_timeout": self.validation_timeout,
        }

    def get_validator_stats(self) -> dict[str, Any]:
        """Get validator statistics.
        
        Returns:
            Dictionary containing statistics for each validator
        """
        return {
            validator.name: {
                "type": validator.constraint_type,
                "enabled": validator.is_enabled(),
                "config": getattr(validator, "config", {}),
            }
            for validator in self.validators
        }

    def reset_performance_metrics(self) -> None:
        """Reset performance metrics."""
        self.validation_count = 0
        self.total_validation_time = 0.0
        self.validation_success_rate = 0.0
        self.logger.info("Reset performance metrics")

    def get_config(self) -> dict[str, Any]:
        """Get configuration as dictionary for backward compatibility.
        
        Returns:
            Dictionary containing complete validator configuration
        """
        return {
            "enabled": True,
            "fail_fast": self.fail_fast,
            "max_concurrent_validators": self.max_concurrent_validators,
            "validation_timeout": self.validation_timeout,
            "validators": {
                "position": self._create_position_config(),
                "portfolio": self._create_portfolio_config(),
                "exchange": self._create_exchange_config(),
                "leverage": self._create_leverage_config(),
            },
        }

    @classmethod
    def from_config(
        cls, config: dict[str, Any], app_settings: AppSettings
    ) -> "ConstraintValidator":
        """Create ConstraintValidator from legacy config and AppSettings.

        Args:
            config: Legacy configuration dictionary (ignored in favor of AppSettings)
            app_settings: Application settings with enhanced risk configuration

        Returns:
            ConstraintValidator instance configured from AppSettings
        """
        # Use AppSettings and ignore legacy config - clean break refactoring
        return cls(app_settings)

    def __str__(self) -> str:
        """String representation.
        
        Returns:
            Human-readable string representation of the validator
        """
        return f"ConstraintValidator(validators={len(self.validators)}, fail_fast={self.fail_fast})"

    def __repr__(self) -> str:
        """Detailed representation.
        
        Returns:
            Detailed string representation suitable for debugging
        """
        return (
            f"ConstraintValidator(validators={len(self.validators)}, "
            f"fail_fast={self.fail_fast}, app_settings={self.app_settings})"
        )
