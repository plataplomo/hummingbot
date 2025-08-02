"""Trade validation service."""

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
    create_business_rule_violation,
    create_range_violation_issue,
)

if TYPE_CHECKING:
    from cyberdelta.core.models import Trade

logger = get_logger(__name__)


class TradeValidationService:
    """Validates trade data and business rules with direct AppSettings access."""

    # Symbol validation constants
    MIN_SYMBOL_LENGTH = 2  # Minimum length for a valid symbol

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the trade validator.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        
        # Use risk configuration for validation settings
        risk_checkers = app_settings.risk.checkers
        sizing = app_settings.risk.sizing
        global_risk = app_settings.risk.global_risk
        
        # Price limits from checker thresholds
        self.min_price = risk_checkers.thresholds.min_price
        self.max_price = risk_checkers.thresholds.max_price
        
        # Quantity limits - derive from position sizing limits
        # Min quantity: use a very small value to allow for fractional trading
        self.min_quantity = Decimal("0.00000001")
        # Max quantity: derive from max position size / min price (conservative)
        self.max_quantity = sizing.max_position_size / self.min_price
        
        # Trade value limits - derive from position limits
        # Min trade value: use minimum position size as a proxy
        self.min_trade_value = sizing.min_position_size / Decimal("100")  # Allow smaller trades
        # Max trade value: use max position USD from global risk
        self.max_trade_value = global_risk.max_position_usd

        logger.info(
            "trade_validation_service_initialized",
            min_price=self.min_price,
            max_price=self.max_price,
            min_quantity=self.min_quantity,
            max_quantity=self.max_quantity,
        )

    def validate_trade(self, trade: Trade) -> ValidationResult[Trade]:
        """Validate a single trade.
        
        Args:
            trade: The trade to validate.
            
        Returns:
            ValidationResult containing the trade and any validation issues found.
        """
        issues: list[ValidationIssue] = []

        # Validate price bounds
        if trade.price < self.min_price or trade.price > self.max_price:
            issues.append(
                create_range_violation_issue(
                    "price",
                    float(trade.price),
                    float(self.min_price),
                    float(self.max_price),
                )
            )

        # Validate quantity bounds
        if trade.quantity < self.min_quantity or trade.quantity > self.max_quantity:
            issues.append(
                create_range_violation_issue(
                    "quantity",
                    float(trade.quantity),
                    float(self.min_quantity),
                    float(self.max_quantity),
                )
            )

        # Validate symbol format
        if len(trade.symbol.value) < self.MIN_SYMBOL_LENGTH:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_FORMAT,
                    field="symbol",
                    message=f"Invalid symbol format: {trade.symbol}",
                    code="TRADE_INVALID_SYMBOL",
                    details={"symbol": str(trade.symbol), "trade_id": str(trade.id)},
                )
            )

        # Validate business rules
        trade_value = trade.price * trade.quantity
        if trade_value < self.min_trade_value:
            issues.append(
                create_business_rule_violation(
                    "MIN_TRADE_VALUE",
                    f"Trade value {trade_value} below minimum {self.min_trade_value}",
                    "WARNING",
                    {"trade_value": float(trade_value), "min_value": float(self.min_trade_value)},
                )
            )
        elif trade_value > self.max_trade_value:
            issues.append(
                create_business_rule_violation(
                    "MAX_TRADE_VALUE",
                    f"Trade value {trade_value} above maximum {self.max_trade_value}",
                    "WARNING",
                    {"trade_value": float(trade_value), "max_value": float(self.max_trade_value)},
                )
            )

        return ValidationResult[Trade](
            is_valid=len([i for i in issues if i.is_error()]) == 0,
            validated_data=trade if len([i for i in issues if i.is_error()]) == 0 else None,
            issues=issues,
            error_count=len([i for i in issues if i.is_error()]),
            warning_count=len([i for i in issues if i.is_warning()]),
            validator_name="TradeValidationService"
        )

    def get_validation_config(self) -> dict[str, Any]:
        """Get current validation configuration.
        
        Returns:
            Dictionary containing validation limits and settings
        """
        return {
            "min_price": self.min_price,
            "max_price": self.max_price,
            "min_quantity": self.min_quantity,
            "max_quantity": self.max_quantity,
            "min_trade_value": self.min_trade_value,
            "max_trade_value": self.max_trade_value,
            "min_symbol_length": self.MIN_SYMBOL_LENGTH,
        }

    def update_validation_limits(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_trade_value: float | None = None,
        max_trade_value: float | None = None,
    ) -> None:
        """Update validation limits at runtime.
        
        Args:
            min_price: New minimum price limit
            max_price: New maximum price limit
            min_quantity: New minimum quantity limit
            max_quantity: New maximum quantity limit
            min_trade_value: New minimum trade value limit
            max_trade_value: New maximum trade value limit
        """
        if min_price is not None:
            self.min_price = Decimal(str(min_price))
        if max_price is not None:
            self.max_price = Decimal(str(max_price))
        if min_quantity is not None:
            self.min_quantity = Decimal(str(min_quantity))
        if max_quantity is not None:
            self.max_quantity = Decimal(str(max_quantity))
        if min_trade_value is not None:
            self.min_trade_value = Decimal(str(min_trade_value))
        if max_trade_value is not None:
            self.max_trade_value = Decimal(str(max_trade_value))

        logger.info(
            "trade_validation_limits_updated",
            min_price=self.min_price,
            max_price=self.max_price,
            min_quantity=self.min_quantity,
            max_quantity=self.max_quantity,
            min_trade_value=self.min_trade_value,
            max_trade_value=self.max_trade_value,
        )