"""Balance validation service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    ValidationCategory,
    ValidationChain,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
)

if TYPE_CHECKING:
    from cyberdelta.core.models import SpotBalance

logger = get_logger(__name__)


class BalanceValidationService:
    """Validates balance data and consistency with direct AppSettings access."""

    # Balance validation constants
    BALANCE_DEVIATION_THRESHOLD = 0.1  # 10% deviation threshold

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the balance validator.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        self.validation_config = app_settings.portfolio_tracker.validation

        self.min_balance_threshold = self.validation_config.min_balance_threshold
        self.allow_negative_balances = self.validation_config.allow_negative_balances

        logger.info(
            "balance_validation_service_initialized",
            min_balance_threshold=self.min_balance_threshold,
            allow_negative_balances=self.allow_negative_balances,
        )

    def validate_balance(self, balance: SpotBalance) -> ValidationResult[SpotBalance]:
        """Validate a single balance.
        
        Args:
            balance: The spot balance to validate.
            
        Returns:
            ValidationResult containing the balance and any validation issues found.
        """
        # Use validation chain for cleaner validation
        chain = ValidationChain(balance)

        # Check negative balances
        if not self.allow_negative_balances:
            chain.check(
                lambda b: b.available_quantity >= 0,
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_VALUE,
                    field="available_quantity",
                    message=f"Negative available balance: {balance.available_quantity}",
                    code="BALANCE_NEGATIVE_AVAILABLE",
                    context={"available": balance.available_quantity, "asset": balance.asset},
                ),
            )

        # Check minimum threshold
        chain.check(
            lambda b: b.total_quantity >= self.min_balance_threshold,
            ValidationIssue(
                severity=ValidationSeverity.INFO,
                category=ValidationCategory.BUSINESS_RULE,
                field="total_quantity",
                message=f"Total balance below threshold: {balance.total_quantity}",
                code="BALANCE_BELOW_THRESHOLD",
                context={
                    "total": balance.total_quantity,
                    "threshold": self.min_balance_threshold,
                    "asset": balance.asset,
                },
            ),
        )

        # Check consistency between available and total
        chain.check(
            lambda b: b.available_quantity <= b.total_quantity,
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                category=ValidationCategory.CONSISTENCY,
                field=None,
                message="Available balance exceeds total balance",
                code="BALANCE_CONSISTENCY_ERROR",
                context={
                    "available": balance.available_quantity,
                    "total": balance.total_quantity,
                    "asset": balance.asset,
                },
            ),
        )

        return chain.result()

    def update_validation_settings(
        self,
        min_balance_threshold: float | None = None,
        allow_negative_balances: bool | None = None,
    ) -> None:
        """Update balance validation settings at runtime.
        
        Args:
            min_balance_threshold: New minimum balance threshold
            allow_negative_balances: Whether to allow negative balances
        """
        if min_balance_threshold is not None:
            self.min_balance_threshold = min_balance_threshold
        if allow_negative_balances is not None:
            self.allow_negative_balances = allow_negative_balances

        logger.info(
            "balance_validation_settings_updated",
            min_balance_threshold=self.min_balance_threshold,
            allow_negative_balances=self.allow_negative_balances,
        )

    def get_validation_config(self) -> dict[str, any]:
        """Get current validation configuration.
        
        Returns:
            Dictionary containing validation settings
        """
        return {
            "min_balance_threshold": self.min_balance_threshold,
            "allow_negative_balances": self.allow_negative_balances,
            "balance_deviation_threshold": self.BALANCE_DEVIATION_THRESHOLD,
        }