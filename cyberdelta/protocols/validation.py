"""Validation protocol definitions for validation framework.

This module defines the protocols and enums for the validation system
that consolidates all order validation logic into a single, extensible framework.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from cyberdelta.enums import ValidationCategory
from cyberdelta.models.validation import ValidationResult


if TYPE_CHECKING:
    from cyberdelta.infrastructure.validation.validation_context import ValidationContext
    from cyberdelta.models.market.order import Order


@runtime_checkable
class ValidationRule(Protocol):
    """Protocol for all validation rules in the validation framework.

    Each rule implements specific validation logic for a particular aspect
    of order validation. Rules are registered with the ValidationRegistry
    and executed by the ValidationService.

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL validation parameters from AppSettings
    - NO hardcoded values or assumptions
    - Uses typed models (Order, ValidationContext)
    - Returns explicit ValidationResult
    """

    @property
    def name(self) -> str:
        """Unique name for this validation rule.

        Used for logging, metrics, and rule identification.

        Returns:
            Rule name (e.g., "price_precision", "balance_check")
        """
        ...

    @property
    def category(self) -> ValidationCategory:
        """Category this rule belongs to.

        Categories determine execution order and grouping.

        Returns:
            ValidationCategory enum value
        """
        ...

    @property
    def enabled(self) -> bool:
        """Whether this rule is currently enabled.

        Can be used to dynamically enable/disable rules based on
        configuration or runtime conditions.

        Returns:
            True if rule should be executed, False to skip
        """
        ...

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Whether to skip this check for reduce-only orders.

        Some validation rules (like balance checks) may not apply
        to orders that are only reducing existing positions.

        Returns:
            True to skip for reduce-only orders, False to always check
        """
        ...

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Execute validation rule against the order.

        Performs the specific validation logic for this rule using the
        provided order and context information.

        Args:
            order: Order to validate
            context: Validation context with market data, portfolio state, etc.

        Returns:
            ValidationResult with any violations found

        Note:
            - Must be async to support I/O operations
            - Should handle exceptions and return violations
            - Must use configuration from context, not hardcoded values
        """
        ...


@runtime_checkable
class ValidationRegistry(Protocol):
    """Protocol for validation rule registry.

    Manages registration and retrieval of validation rules by category.
    """

    def register(self, rule: ValidationRule) -> None:
        """Register a validation rule.

        Args:
            rule: ValidationRule to register
        """
        ...

    def get_rules(self, category: ValidationCategory | None = None) -> list[ValidationRule]:
        """Get validation rules by category.

        Args:
            category: Optional category to filter by

        Returns:
            List of validation rules
        """
        ...

    def get_enabled_rules(self, category: ValidationCategory | None = None) -> list[ValidationRule]:
        """Get only enabled validation rules.

        Args:
            category: Optional category to filter by

        Returns:
            List of enabled validation rules
        """
        ...
