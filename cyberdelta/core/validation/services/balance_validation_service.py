"""Balance validation service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from decimal import Decimal

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    ValidationCategory,
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
        
        # Get balance validation settings from safety systems configuration
        # Since we don't have per-asset thresholds in this context, we'll use the minimum
        # threshold across all configured assets as a general baseline
        balance_thresholds = app_settings.safety_systems.balance_monitoring.min_balance_thresholds_usd
        if balance_thresholds:
            # Use the minimum threshold as a conservative baseline
            self.min_balance_threshold = min(balance_thresholds.values())
        else:
            # Fallback if no thresholds are configured
            self.min_balance_threshold = Decimal("0.00001")
        
        # No direct config for this, but requiring non-negative is a safe default
        self.allow_negative_balances = False

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
        issues: list[ValidationIssue] = []

        # Check negative balances
        if not self.allow_negative_balances and balance.available_quantity < 0:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_VALUE,
                    field="available_quantity",
                    message=f"Negative available balance: {balance.available_quantity}",
                    code="BALANCE_NEGATIVE_AVAILABLE",
                    details={"available": float(balance.available_quantity), "asset": balance.asset},
                )
            )

        # Check minimum threshold
        if balance.total_quantity < self.min_balance_threshold:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    category=ValidationCategory.BUSINESS_RULE,
                    field="total_quantity",
                    message=f"Total balance below threshold: {balance.total_quantity}",
                    code="BALANCE_BELOW_THRESHOLD",
                    details={
                        "total": float(balance.total_quantity),
                        "threshold": float(self.min_balance_threshold),
                        "asset": balance.asset,
                    },
                )
            )

        # Check consistency between available and total
        if balance.available_quantity > balance.total_quantity:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.CONSISTENCY,
                    field=None,
                    message="Available balance exceeds total balance",
                    code="BALANCE_CONSISTENCY_ERROR",
                    details={
                        "available": float(balance.available_quantity),
                        "total": float(balance.total_quantity),
                        "asset": balance.asset,
                    },
                )
            )

        return ValidationResult[SpotBalance](
            is_valid=len([i for i in issues if i.is_error()]) == 0,
            validated_data=balance if len([i for i in issues if i.is_error()]) == 0 else None,
            issues=issues,
            error_count=len([i for i in issues if i.is_error()]),
            warning_count=len([i for i in issues if i.is_warning()]),
            validator_name="BalanceValidationService"
        )

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
            self.min_balance_threshold = Decimal(str(min_balance_threshold))
        if allow_negative_balances is not None:
            self.allow_negative_balances = allow_negative_balances

        logger.info(
            "balance_validation_settings_updated",
            min_balance_threshold=self.min_balance_threshold,
            allow_negative_balances=self.allow_negative_balances,
        )

    def get_validation_config(self) -> dict[str, Any]:
        """Get current validation configuration.
        
        Returns:
            Dictionary containing validation settings
        """
        return {
            "min_balance_threshold": self.min_balance_threshold,
            "allow_negative_balances": self.allow_negative_balances,
            "balance_deviation_threshold": self.BALANCE_DEVIATION_THRESHOLD,
        }