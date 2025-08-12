"""Validation result models for unified validation framework.

This module defines the data models used by the validation system
to return validation results and denied orders.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from cyberdelta.enums import ValidationCategory

from pydantic import BaseModel, Field

from cyberdelta.enums import ValidationCategory


class ValidationResult(BaseModel):
    """Result of validation rule execution.

    Contains any violations found during validation and metadata
    about the validation process.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses Pydantic BaseModel for type safety
    - NO dict[str, Any] usage
    - Explicit field types and validation
    """

    violations: list[str] = Field(
        default_factory=list,
        description="List of validation violations found",
    )

    is_valid: bool = Field(
        description="Whether validation passed (computed property)",
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When validation was performed",
    )

    validation_type: str = Field(
        default="STANDARD",
        description="Type of validation performed (e.g., PRE_TRADE_RISK_CHECK)",
    )

    category: ValidationCategory | None = Field(
        default=None,
        description="Category of validation rule that generated this result",
    )

    rule_name: str | None = Field(
        default=None,
        description="Name of the specific rule that generated this result",
    )

    def __init__(
        self,
        *,
        violations: list[str] | None = None,
        is_valid: bool | None = None,
        timestamp: datetime | None = None,
        validation_type: str = "STANDARD",
        category: ValidationCategory | None = None,
        rule_name: str | None = None,
    ) -> None:
        """Initialize validation result.

        Automatically computes is_valid based on violations if not provided.
        """
        if violations is None:
            violations = []

        if is_valid is None:
            is_valid = len(violations) == 0

        if timestamp is None:
            timestamp = datetime.now(UTC)

        super().__init__(
            violations=violations,
            is_valid=is_valid,
            timestamp=timestamp,
            validation_type=validation_type,
            category=category,
            rule_name=rule_name,
        )

    def add_violation(self, violation: str) -> None:
        """Add a violation to the result.

        Args:
            violation: Violation message to add

        Note:
            - Automatically updates is_valid to False
            - Maintains immutability by using Pydantic field assignment
        """
        self.violations.append(violation)
        self.is_valid = False

    def merge(self, other: ValidationResult) -> ValidationResult:
        """Merge this result with another validation result.

        Args:
            other: Other validation result to merge

        Returns:
            New ValidationResult containing violations from both

        Note:
            - Creates new instance, does not modify existing
            - Uses latest timestamp
            - Combines all violations
        """
        all_violations = self.violations + other.violations
        latest_timestamp = max(self.timestamp, other.timestamp)

        return ValidationResult(
            violations=all_violations,
            timestamp=latest_timestamp,
            validation_type=f"{self.validation_type}+{other.validation_type}",
        )

    @classmethod
    def valid(
        cls,
        validation_type: str = "STANDARD",
        category: ValidationCategory | None = None,
        rule_name: str | None = None,
    ) -> ValidationResult:
        """Create a valid (no violations) result.

        Args:
            validation_type: Type of validation
            category: Category of validation
            rule_name: Name of the rule

        Returns:
            ValidationResult with no violations
        """
        return cls(
            violations=[],
            validation_type=validation_type,
            category=category,
            rule_name=rule_name,
        )

    @classmethod
    def invalid(
        cls,
        violations: list[str],
        validation_type: str = "STANDARD",
        category: ValidationCategory | None = None,
        rule_name: str | None = None,
    ) -> ValidationResult:
        """Create an invalid result with violations.

        Args:
            violations: List of violation messages
            validation_type: Type of validation
            category: Category of validation
            rule_name: Name of the rule

        Returns:
            ValidationResult with violations
        """
        return cls(
            violations=violations,
            validation_type=validation_type,
            category=category,
            rule_name=rule_name,
        )


class OrderDenied(BaseModel):
    """Event representing an order denial due to validation failure.

    Inspired by Nautilus Trader's OrderDenied event pattern.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses Pydantic BaseModel for type safety
    - Contains human-readable reason for denial
    - Includes detailed context for debugging
    """

    order_id: str = Field(
        description="Order ID that was denied",
    )

    reason: str = Field(
        description="Human-readable reason for denial",
    )

    validation_category: ValidationCategory = Field(
        description="Category of validation that caused the denial",
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When the denial occurred",
    )

    details: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description="Additional context for debugging",
    )

    trading_state: str | None = Field(
        default=None,
        description="Trading state when denial occurred",
    )

    def __str__(self) -> str:
        """String representation for logging.

        Returns:
            Formatted string with order ID and reason
        """
        return f"OrderDenied(order_id={self.order_id}, reason={self.reason})"

    def __repr__(self) -> str:
        """Detailed representation for debugging.

        Returns:
            Detailed formatted string with all fields
        """
        return (
            f"OrderDenied(order_id={self.order_id}, "
            f"category={self.validation_category.value}, "
            f"reason={self.reason}, "
            f"timestamp={self.timestamp.isoformat()})"
        )
