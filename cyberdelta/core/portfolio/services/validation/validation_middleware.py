"""Validation middleware for portfolio components."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeGuard, TypeVar

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition, SpotBalance, Trade
from cyberdelta.core.portfolio.exceptions.integrity import PortfolioIntegrityError
from cyberdelta.core.portfolio.portfolio_types.validation_types import (
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
)
from cyberdelta.core.portfolio.services.validation.portfolio_validation_service import (
    PortfolioValidationService,
)


logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T", bound=object)


class ValidationMiddleware:
    """Middleware for adding validation capabilities to portfolio components."""

    def __init__(self, validation_service: PortfolioValidationService) -> None:
        """Initialize validation middleware.

        Args:
            validation_service: The portfolio validation service to use
        """
        self.validation_service = validation_service
        self.validation_enabled = True
        self.strict_mode = False  # If True, validation errors will raise exceptions

    def enable_validation(self) -> None:
        """Enable validation."""
        self.validation_enabled = True
        logger.info("validation_middleware_enabled")

    def disable_validation(self) -> None:
        """Disable validation."""
        self.validation_enabled = False
        logger.info("validation_middleware_disabled")

    def set_strict_mode(self, strict: bool) -> None:
        """Set strict mode."""
        self.strict_mode = strict
        logger.info("validation_middleware_strict_mode_set", strict=strict)

    def validate_trade(
        self, fail_on_error: bool | None = None
    ) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
        """Decorator to validate trades.
        
        Returns:
            Decorator function that adds trade validation to async methods.
        """

        def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                # Extract trade from arguments
                trade = self._extract_trade_from_args(args, kwargs)

                if trade and self.validation_enabled:
                    validation_result = await self.validation_service.validate_trade(trade)

                    should_fail = fail_on_error if fail_on_error is not None else self.strict_mode

                    if not validation_result.is_valid and should_fail:
                        error_issues = validation_result.get_errors()
                        if error_issues:
                            raise PortfolioIntegrityError("Trade")

                    # Log validation issues
                    self._log_validation_issues(validation_result, "trade")

                return await func(*args, **kwargs)

            return wrapper

        return decorator

    def validate_balance(
        self, fail_on_error: bool | None = None
    ) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
        """Decorator to validate balances.
        
        Returns:
            Decorator function that adds balance validation to async methods.
        """

        def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                # Extract balance from arguments
                balance = self._extract_balance_from_args(args, kwargs)

                if balance and self.validation_enabled:
                    validation_result = await self.validation_service.validate_balance(balance)

                    should_fail = fail_on_error if fail_on_error is not None else self.strict_mode

                    if not validation_result.is_valid and should_fail:
                        error_issues = validation_result.get_errors()
                        if error_issues:
                            raise PortfolioIntegrityError("Balance")

                    # Log validation issues
                    self._log_validation_issues(validation_result, "balance")

                return await func(*args, **kwargs)

            return wrapper

        return decorator

    def validate_position(
        self, fail_on_error: bool | None = None
    ) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
        """Decorator to validate positions.
        
        Returns:
            Decorator function that adds position validation to async methods.
        """

        def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                # Extract position from arguments
                position = self._extract_position_from_args(args, kwargs)

                if position and self.validation_enabled:
                    validation_result = await self.validation_service.validate_position(position)

                    should_fail = fail_on_error if fail_on_error is not None else self.strict_mode

                    if not validation_result.is_valid and should_fail:
                        error_issues = validation_result.get_errors()
                        if error_issues:
                            raise PortfolioIntegrityError("Position")

                    # Log validation issues
                    self._log_validation_issues(validation_result, "position")

                return await func(*args, **kwargs)

            return wrapper

        return decorator

    def validate_batch_trades(
        self, fail_on_error: bool | None = None
    ) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
        """Decorator to validate batch of trades.
        
        Returns:
            Decorator function that adds batch trade validation to async methods.
        """

        def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
            @functools.wraps(func)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                # Extract trades from arguments
                trades = self._extract_trades_from_args(args, kwargs)

                if trades and self.validation_enabled:
                    validation_result = await self.validation_service.validate_batch_trades(trades)

                    should_fail = fail_on_error if fail_on_error is not None else self.strict_mode

                    if not validation_result.is_valid and should_fail:
                        error_issues = validation_result.get_errors()
                        if error_issues:
                            raise PortfolioIntegrityError("Batch")

                    # Log validation issues
                    self._log_validation_issues(validation_result, "batch_trades")

                return await func(*args, **kwargs)

            return wrapper

        return decorator

    def _extract_trade_from_args(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Trade | None:
        """Extract trade from function arguments.
        
        Returns:
            Trade object if found in arguments, None otherwise.
        """
        # Try to find trade in positional arguments
        for arg in args:
            if isinstance(arg, Trade):
                return arg

        # Try to find trade in keyword arguments
        for key, value in kwargs.items():
            if key in {"trade", "trade_data"} and isinstance(value, Trade):
                return value

        return None

    def _extract_balance_from_args(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> SpotBalance | None:
        """Extract balance from function arguments.
        
        Returns:
            SpotBalance object if found in arguments, None otherwise.
        """
        # Try to find balance in positional arguments
        for arg in args:
            if isinstance(arg, SpotBalance):
                return arg

        # Try to find balance in keyword arguments
        for key, value in kwargs.items():
            if key in {"balance", "balance_data"} and isinstance(value, SpotBalance):
                return value

        return None

    def _extract_position_from_args(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> DerivativePosition | None:
        """Extract position from function arguments.
        
        Returns:
            DerivativePosition object if found in arguments, None otherwise.
        """
        # Try to find position in positional arguments
        for arg in args:
            if isinstance(arg, DerivativePosition):
                return arg

        # Try to find position in keyword arguments
        for key, value in kwargs.items():
            if key in {"position", "position_data"} and isinstance(value, DerivativePosition):
                return value

        return None

    def _is_trade_list(self, obj: object) -> TypeGuard[list[Trade]]:
        """Type guard for list of trades.
        
        Returns:
            True if obj is a non-empty list of Trade objects, False otherwise.
        """
        if not isinstance(obj, list):
            return False
        # After isinstance check, obj is known to be list
        # Type annotation for pyright type inference with TypeGuard
        typed_list: list[Any] = obj  # pyright: ignore[reportUnknownVariableType]
        if len(typed_list) == 0:
            return False
        # Check first item to avoid iterating Unknown types
        return isinstance(typed_list[0], Trade)

    def _extract_trades_from_args(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> list[Trade] | None:
        """Extract list of trades from function arguments.
        
        Returns:
            List of Trade objects if found in arguments, None otherwise.
        """
        # Try to find trades in positional arguments
        for arg in args:
            if self._is_trade_list(arg):
                return arg

        # Try to find trades in keyword arguments
        for key, value in kwargs.items():
            if key in {"trades", "trade_list"} and self._is_trade_list(value):
                return value

        return None

    def _log_validation_issues(
        self, validation_result: ValidationResult[Any], data_type: str
    ) -> None:
        """Log validation issues."""
        if not validation_result.is_valid:
            error_issues = validation_result.get_errors()
            warning_issues = validation_result.get_warnings()

            if error_issues:
                logger.error(
                    "validation_error_issues",
                    data_type=data_type,
                    issue_count=len(error_issues),
                    issues=[issue.message for issue in error_issues],
                )

            if warning_issues:
                logger.warning(
                    "validation_warning_issues",
                    data_type=data_type,
                    issue_count=len(warning_issues),
                    issues=[issue.message for issue in warning_issues],
                )


def create_validation_mixin(validation_service: PortfolioValidationService) -> type[Any]:
    """Create a mixin class for adding validation capabilities.
    
    Returns:
        ValidationMixin class configured with the provided validation service.
    """

    class ValidationMixin:
        """Mixin for adding validation capabilities to portfolio components."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)
            self.validation_service = validation_service
            self.validation_middleware = ValidationMiddleware(validation_service)

        def enable_validation(self) -> None:
            """Enable validation for this component."""
            self.validation_middleware.enable_validation()

        def disable_validation(self) -> None:
            """Disable validation for this component."""
            self.validation_middleware.disable_validation()

        def set_strict_validation(self, strict: bool) -> None:
            """Set strict validation mode."""
            self.validation_middleware.set_strict_mode(strict)

        async def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
            """Validate a trade.
            
            Returns:
                Validation result containing the validated trade and any issues found.
            """
            return await self.validation_service.validate_trade(trade)

        async def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
            """Validate a balance.
            
            Returns:
                Validation result containing the validated balance and any issues found.
            """
            return await self.validation_service.validate_balance(balance)

        async def validate_position(
            self, position: DerivativePosition
        ) -> ValidationResult[DerivativePosition]:
            """Validate a position.
            
            Returns:
                Validation result containing the validated position and any issues found.
            """
            return await self.validation_service.validate_position(position)

        def get_validation_statistics(self) -> dict[str, Any]:
            """Get validation statistics.
            
            Returns:
                Dictionary containing validation statistics from the service.
            """
            return self.validation_service.get_validation_stats().model_dump()

    return ValidationMixin


# Common validation decorators
def validate_trade_input(
    validation_service: PortfolioValidationService, fail_on_error: bool = False
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator for validating trade inputs.
    
    Returns:
        Decorator function that validates trade inputs before method execution.
    """
    middleware = ValidationMiddleware(validation_service)
    return middleware.validate_trade(fail_on_error=fail_on_error)


def validate_balance_input(
    validation_service: PortfolioValidationService, fail_on_error: bool = False
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator for validating balance inputs.
    
    Returns:
        Decorator function that validates balance inputs before method execution.
    """
    middleware = ValidationMiddleware(validation_service)
    return middleware.validate_balance(fail_on_error=fail_on_error)


def validate_position_input(
    validation_service: PortfolioValidationService, fail_on_error: bool = False
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator for validating position inputs.
    
    Returns:
        Decorator function that validates position inputs before method execution.
    """
    middleware = ValidationMiddleware(validation_service)
    return middleware.validate_position(fail_on_error=fail_on_error)


# Utility functions
async def validate_data_integrity(
    validation_service: PortfolioValidationService,
    trades: list[Trade] | None = None,
    balances: list[SpotBalance] | None = None,
    positions: list[DerivativePosition] | None = None,
) -> dict[str, ValidationResult[Any]]:
    """Validate data integrity for multiple data types.
    
    Returns:
        Dictionary mapping data type names to their validation results.
    """
    results: dict[str, ValidationResult[Any]] = {}

    if trades:
        results["trades"] = await validation_service.validate_batch_trades(trades)

    if balances:
        balance_results = [
            await validation_service.validate_balance(balance) for balance in balances
        ]

        # Combine results
        all_issues: list[ValidationIssue] = []
        for balance_result in balance_results:
            all_issues.extend(balance_result.issues)

        balance_overall_result = ValidationResult[list[SpotBalance]].from_issues(
            balances, all_issues
        )
        results["balances"] = balance_overall_result

    if positions:
        position_results: list[ValidationResult[DerivativePosition]] = [
            await validation_service.validate_position(position) for position in positions
        ]

        # Combine results
        position_issues: list[ValidationIssue] = []
        for position_result in position_results:
            position_issues.extend(position_result.issues)

        position_overall_result = ValidationResult[list[DerivativePosition]].from_issues(
            positions, position_issues
        )
        results["positions"] = position_overall_result

    return results


def create_validation_report(
    validation_results: dict[str, ValidationResult[Any]],
) -> dict[str, Any]:
    """Create a comprehensive validation report.
    
    Returns:
        Dictionary containing timestamp, overall validity status, data type summaries,
        and issue counts by severity.
    """
    report: dict[str, Any] = {
        "timestamp": __import__("time").time(),
        "overall_valid": all(result.is_valid for result in validation_results.values()),
        "data_types": {},
        "summary": {
            "total_issues": 0,
            "error_issues": 0,
            "warning_issues": 0,
            "info_issues": 0,
        },
    }

    for data_type, result in validation_results.items():
        error_count = len(result.get_errors())
        warning_count = len(result.get_warnings())
        info_count = len([i for i in result.issues if i.severity == ValidationSeverity.INFO])

        report["data_types"][data_type] = {
            "is_valid": result.is_valid,
            "issue_count": len(result.issues),
            "issues_by_severity": {
                "error": error_count,
                "warning": warning_count,
                "info": info_count,
            },
        }

        # Update summary
        report["summary"]["total_issues"] += len(result.issues)
        report["summary"]["error_issues"] += error_count
        report["summary"]["warning_issues"] += warning_count
        report["summary"]["info_issues"] += info_count

    return report
