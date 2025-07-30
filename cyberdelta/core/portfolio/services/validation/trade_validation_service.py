"""Trade validation service."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
        self.validation_config = app_settings.portfolio_tracker.validation

        self.min_price = self.validation_config.min_price
        self.max_price = self.validation_config.max_price
        self.min_quantity = self.validation_config.min_quantity
        self.max_quantity = self.validation_config.max_quantity
        self.min_trade_value = self.validation_config.min_trade_value
        self.max_trade_value = self.validation_config.max_trade_value

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
                    trade.price,
                    self.min_price,
                    self.max_price,
                )
            )

        # Validate quantity bounds
        if trade.quantity < self.min_quantity or trade.quantity > self.max_quantity:
            issues.append(
                create_range_violation_issue(
                    "quantity",
                    trade.quantity,
                    self.min_quantity,
                    self.max_quantity,
                )
            )

        # Validate symbol format
        if len(trade.symbol) < self.MIN_SYMBOL_LENGTH:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category=ValidationCategory.INVALID_FORMAT,
                    field="symbol",
                    message=f"Invalid symbol format: {trade.symbol}",
                    code="TRADE_INVALID_SYMBOL",
                    context={"symbol": trade.symbol, "trade_id": trade.id},
                )
            )

        # Validate business rules
        trade_value = trade.price * trade.quantity
        if trade_value < self.min_trade_value:
            issues.append(
                create_business_rule_violation(
                    "MIN_TRADE_VALUE",
                    f"Trade value must be at least {self.min_trade_value}",
                    "trade_value",
                    f"Trade value {trade_value} below minimum {self.min_trade_value}",
                    ValidationSeverity.WARNING,
                )
            )
        elif trade_value > self.max_trade_value:
            issues.append(
                create_business_rule_violation(
                    "MAX_TRADE_VALUE",
                    f"Trade value must not exceed {self.max_trade_value}",
                    "trade_value",
                    f"Trade value {trade_value} above maximum {self.max_trade_value}",
                    ValidationSeverity.WARNING,
                )
            )

        return ValidationResult[Trade].from_issues(trade, issues)

    def get_validation_config(self) -> dict[str, any]:
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
            self.min_price = min_price
        if max_price is not None:
            self.max_price = max_price
        if min_quantity is not None:
            self.min_quantity = min_quantity
        if max_quantity is not None:
            self.max_quantity = max_quantity
        if min_trade_value is not None:
            self.min_trade_value = min_trade_value
        if max_trade_value is not None:
            self.max_trade_value = max_trade_value

        logger.info(
            "trade_validation_limits_updated",
            min_price=self.min_price,
            max_price=self.max_price,
            min_quantity=self.min_quantity,
            max_quantity=self.max_quantity,
            min_trade_value=self.min_trade_value,
            max_trade_value=self.max_trade_value,
        )