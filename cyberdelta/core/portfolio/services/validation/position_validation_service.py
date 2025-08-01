"""Position validation service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from decimal import Decimal

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    ValidationIssue,
    ValidationResult,
    create_business_rule_violation,
)

if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition

logger = get_logger(__name__)


class PositionValidationService:
    """Validates position data and risk limits with direct AppSettings access."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the position validator.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        
        # Use risk settings for position validation limits
        global_risk = app_settings.risk.global_risk
        sizing = app_settings.risk.sizing
        
        self.max_position_size = sizing.max_position_size
        self.max_leverage = sizing.max_leverage
        self.max_position_value = global_risk.max_position_usd

        logger.info(
            "position_validation_service_initialized",
            max_position_size=self.max_position_size,
            max_leverage=self.max_leverage,
            max_position_value=self.max_position_value,
        )

    def validate_position(
        self, position: DerivativePosition
    ) -> ValidationResult[DerivativePosition]:
        """Validate a single position.
        
        Args:
            position: The derivative position to validate.
            
        Returns:
            ValidationResult containing the position and any validation issues found.
        """
        issues: list[ValidationIssue] = []

        # Validate position size
        if abs(position.size) > self.max_position_size:
            issues.append(
                create_business_rule_violation(
                    "MAX_POSITION_SIZE",
                    f"Position size {abs(position.size)} exceeds maximum {self.max_position_size}",
                    "ERROR",
                    {"position_size": float(abs(position.size)), "max_size": float(self.max_position_size)},
                )
            )

        # Validate leverage using generic extraction
        leverage = self._extract_position_leverage(position)
        if leverage is not None and leverage > self.max_leverage:
            issues.append(
                create_business_rule_violation(
                    "MAX_LEVERAGE",
                    f"Leverage {leverage}x exceeds maximum {self.max_leverage}x",
                    "ERROR",
                    {"leverage": float(leverage), "max_leverage": float(self.max_leverage)},
                )
            )

        # Validate position value
        if position.mark_price and position.size:
            position_value = abs(position.mark_price * position.size)
            if position_value > self.max_position_value:
                issues.append(
                    create_business_rule_violation(
                        "MAX_POSITION_VALUE",
                        f"Position value {position_value} exceeds maximum {self.max_position_value}",
                        "ERROR",
                        {"position_value": float(position_value), "max_value": float(self.max_position_value)},
                    )
                )

        return ValidationResult[DerivativePosition](
            is_valid=len([i for i in issues if i.is_error()]) == 0,
            validated_data=position if len([i for i in issues if i.is_error()]) == 0 else None,
            issues=issues,
            error_count=len([i for i in issues if i.is_error()]),
            warning_count=len([i for i in issues if i.is_warning()]),
            validator_name="PositionValidationService"
        )

    def update_validation_limits(
        self,
        max_position_size: float | None = None,
        max_leverage: float | None = None,
        max_position_value: float | None = None,
    ) -> None:
        """Update position validation limits at runtime.
        
        Args:
            max_position_size: New maximum position size limit
            max_leverage: New maximum leverage limit
            max_position_value: New maximum position value limit
        """
        if max_position_size is not None:
            self.max_position_size = Decimal(str(max_position_size))
        if max_leverage is not None:
            self.max_leverage = Decimal(str(max_leverage))
        if max_position_value is not None:
            self.max_position_value = Decimal(str(max_position_value))

        logger.info(
            "position_validation_limits_updated",
            max_position_size=self.max_position_size,
            max_leverage=self.max_leverage,
            max_position_value=self.max_position_value,
        )

    def get_validation_config(self) -> dict[str, Any]:
        """Get current validation configuration.
        
        Returns:
            Dictionary containing validation limits and settings
        """
        return {
            "max_position_size": self.max_position_size,
            "max_leverage": self.max_leverage,
            "max_position_value": self.max_position_value,
        }

    def _extract_position_leverage(self, position: DerivativePosition) -> float | None:
        """Extract leverage from position using generic approach.
        
        Args:
            position: Position to extract leverage from
            
        Returns:
            Leverage value if available, None otherwise
        """
        # Try common position fields first
        if hasattr(position, 'leverage') and position.leverage is not None:
            return float(position.leverage)
        
        # Try generic details field
        if hasattr(position, 'details') and position.details:
            leverage = getattr(position.details, 'leverage', None)
            if leverage is not None:
                return float(leverage)
            
            # Try alternate field names
            leverage_value = getattr(position.details, 'leverage_value', None)
            if leverage_value is not None:
                return float(leverage_value)
        
        # Fallback: try to calculate from position size and margin
        if (hasattr(position, 'size') and hasattr(position, 'margin_used') and 
            position.size and position.margin_used and 
            hasattr(position, 'mark_price') and position.mark_price):
            
            try:
                position_value = abs(position.size * position.mark_price)
                margin_used = abs(position.margin_used)
                if margin_used > 0:
                    return float(position_value / margin_used)
            except (AttributeError, ZeroDivisionError, TypeError):
                pass
        
        return None