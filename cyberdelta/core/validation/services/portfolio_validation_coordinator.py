"""Portfolio validation coordination service."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    ValidationIssue,
    ValidationResult,
)
from cyberdelta.core.portfolio.portfolio_types.models import ValidationStatistics
from cyberdelta.core.portfolio.services.validation.balance_validation_service import BalanceValidationService
from cyberdelta.core.portfolio.services.validation.position_validation_service import PositionValidationService
from cyberdelta.core.portfolio.services.validation.trade_validation_service import TradeValidationService

if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, SpotBalance, Trade
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol

logger = get_logger(__name__)


class PortfolioValidationCoordinator:
    """Main portfolio validation service with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Protocol-based dependencies
    - No inheritance from base services
    - Configuration from AppSettings
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol,
    ) -> None:
        """Initialize portfolio validation service.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for accessing portfolio data
        """
        self.app_settings = app_settings
        self.validation_config = app_settings.validation
        self.state_container = state_container

        # Initialize individual validation services
        self.trade_validator = TradeValidationService(app_settings)
        self.balance_validator = BalanceValidationService(app_settings)
        self.position_validator = PositionValidationService(app_settings)

        # Validation statistics
        self.validation_stats: dict[str, dict[str, int]] = defaultdict(
            lambda: {"total": 0, "passed": 0, "failed": 0}
        )
        self.recent_issues: list[ValidationIssue] = []
        self.max_recent_issues = self.validation_config.max_recent_issues

        logger.info(
            "portfolio_validation_coordinator_initialized",
            validation_config_summary={
                "max_recent_issues": self.max_recent_issues,
                "min_price": self.validation_config.min_price,
                "max_price": self.validation_config.max_price,
            },
        )

    async def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
        """Validate a trade.
        
        Args:
            trade: The trade to validate.
            
        Returns:
            ValidationResult containing the trade and any validation issues found.
        """
        result = self.trade_validator.validate_trade(trade)
        self._record_validation("trade", result)
        return result

    async def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
        """Validate a balance.
        
        Args:
            balance: The spot balance to validate.
            
        Returns:
            ValidationResult containing the balance and any validation issues found.
        """
        result = self.balance_validator.validate_balance(balance)
        self._record_validation("balance", result)
        return result

    async def validate_position(
        self, position: DerivativePosition
    ) -> ValidationResult[DerivativePosition]:
        """Validate a position.
        
        Args:
            position: The derivative position to validate.
            
        Returns:
            ValidationResult containing the position and any validation issues found.
        """
        result = self.position_validator.validate_position(position)
        self._record_validation("position", result)
        return result

    async def validate_batch_trades(self, trades: list[Trade]) -> ValidationResult[list[Trade]]:
        """Validate a batch of trades.
        
        Args:
            trades: List of trades to validate.
            
        Returns:
            ValidationResult containing all trades and accumulated validation issues.
        """
        all_issues: list[ValidationIssue] = []

        for trade in trades:
            result = await self.validate_trade(trade)
            all_issues.extend(result.issues)

        return ValidationResult[list[Trade]](
            is_valid=len(all_issues) == 0,
            validated_data=trades,
            issues=all_issues
        )

    def _record_validation(self, validation_type: str, result: ValidationResult[Any]) -> None:
        """Record validation statistics and recent issues.
        
        Args:
            validation_type: Type of validation (trade, balance, position)
            result: Validation result to record
        """
        stats = self.validation_stats[validation_type]
        stats["total"] += 1
        
        if result.is_valid:
            stats["passed"] += 1
        else:
            stats["failed"] += 1
            # Add issues to recent issues list
            self.recent_issues.extend(result.issues)
            
            # Trim recent issues if necessary
            if len(self.recent_issues) > self.max_recent_issues:
                self.recent_issues = self.recent_issues[-self.max_recent_issues:]

    def get_validation_statistics(self) -> ValidationStatistics:
        """Get comprehensive validation statistics.
        
        Returns:
            ValidationStatistics object with current statistics
        """
        total_validations = sum(stats["total"] for stats in self.validation_stats.values())
        successful_validations = sum(stats["passed"] for stats in self.validation_stats.values())
        failed_validations = sum(stats["failed"] for stats in self.validation_stats.values())

        return ValidationStatistics(
            total_validations=total_validations,
            successful_validations=successful_validations,
            failed_validations=failed_validations,
            error_count=len(self.recent_issues),
        )

    def get_recent_issues(self, limit: int | None = None) -> list[ValidationIssue]:
        """Get recent validation issues.
        
        Args:
            limit: Maximum number of issues to return
            
        Returns:
            List of recent validation issues
        """
        if limit is None:
            return self.recent_issues.copy()
        return self.recent_issues[-limit:] if limit > 0 else []

    def clear_statistics(self) -> None:
        """Clear validation statistics and recent issues."""
        self.validation_stats.clear()
        self.recent_issues.clear()
        logger.info("validation_statistics_cleared")

    def get_validator_configs(self) -> dict[str, dict[str, Any]]:
        """Get configuration from all validators.
        
        Returns:
            Dictionary containing configuration from all validation services
        """
        return {
            "trade": self.trade_validator.get_validation_config(),
            "balance": self.balance_validator.get_validation_config(),
            "position": self.position_validator.get_validation_config(),
        }