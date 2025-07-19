"""Type-safe validation types and result classes."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field


if TYPE_CHECKING:
    from collections.abc import Callable


T = TypeVar("T", bound=object)  # Bounded to object for validation flexibility


def make_validation_issue_list() -> list[ValidationIssue]:
    """Factory function for validation issues list."""
    return []


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ValidationCategory(Enum):
    """Categories of validation issues."""

    # Data validation
    MISSING_REQUIRED = "missing_required"
    INVALID_TYPE = "invalid_type"
    INVALID_FORMAT = "invalid_format"
    OUT_OF_RANGE = "out_of_range"
    INVALID_VALUE = "invalid_value"

    # Business logic validation
    BUSINESS_RULE = "business_rule"
    CONSISTENCY = "consistency"
    CONSTRAINT_VIOLATION = "constraint_violation"

    # State validation
    INVALID_STATE = "invalid_state"
    STATE_TRANSITION = "state_transition"

    # Security validation
    AUTHORIZATION = "authorization"
    SECURITY_RISK = "security_risk"


class ValidationIssue(BaseModel):
    """Base class for validation issues."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    severity: ValidationSeverity
    category: ValidationCategory
    field: str | None
    message: str
    code: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)

    def is_error(self) -> bool:
        """Check if this is an error-level issue."""
        return self.severity == ValidationSeverity.ERROR

    def is_warning(self) -> bool:
        """Check if this is a warning-level issue."""
        return self.severity == ValidationSeverity.WARNING


def create_missing_field_issue(field_name: str, message: str | None = None) -> ValidationIssue:
    """Create a missing field validation issue."""
    return ValidationIssue(
        severity=ValidationSeverity.ERROR,
        category=ValidationCategory.MISSING_REQUIRED,
        field=field_name,
        message=message or f"Required field '{field_name}' is missing",
        code="MISSING_FIELD",
        context={"field": field_name},
    )


def create_type_mismatch_issue(
    field_name: str,
    expected_type: type,
    actual_type: type,
    message: str | None = None,
) -> ValidationIssue:
    """Create a type mismatch validation issue."""
    return ValidationIssue(
        severity=ValidationSeverity.ERROR,
        category=ValidationCategory.INVALID_TYPE,
        field=field_name,
        message=message
        or f"Field '{field_name}' expected {expected_type.__name__}, got {actual_type.__name__}",
        code="TYPE_MISMATCH",
        context={
            "field": field_name,
            "expected_type": expected_type.__name__,
            "actual_type": actual_type.__name__,
        },
    )


def create_range_violation_issue(
    field_name: str,
    actual_value: object,
    min_value: object | None = None,
    max_value: object | None = None,
    message: str | None = None,
) -> ValidationIssue:
    """Create a range violation validation issue."""
    if not message:
        if min_value is not None and max_value is not None:
            message = (
                f"Field '{field_name}' value {actual_value} is outside range "
                f"[{min_value}, {max_value}]"
            )
        elif min_value is not None:
            message = f"Field '{field_name}' value {actual_value} is below minimum {min_value}"
        elif max_value is not None:
            message = f"Field '{field_name}' value {actual_value} is above maximum {max_value}"
        else:
            message = f"Field '{field_name}' value {actual_value} is out of range"

    return ValidationIssue(
        severity=ValidationSeverity.ERROR,
        category=ValidationCategory.OUT_OF_RANGE,
        field=field_name,
        message=message,
        code="RANGE_VIOLATION",
        context={
            "field": field_name,
            "actual_value": actual_value,
            "min_value": min_value,
            "max_value": max_value,
        },
    )


def create_business_rule_violation(
    rule_name: str,
    rule_description: str,
    field_name: str | None = None,
    message: str | None = None,
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> ValidationIssue:
    """Create a business rule violation validation issue."""
    return ValidationIssue(
        severity=severity,
        category=ValidationCategory.BUSINESS_RULE,
        field=field_name,
        message=message or f"Business rule '{rule_name}' violated: {rule_description}",
        code="BUSINESS_RULE_VIOLATION",
        context={
            "rule_name": rule_name,
            "rule_description": rule_description,
            "field": field_name,
        },
    )


class ValidationResult[T](BaseModel):
    """Type-safe validation result container."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: T
    is_valid: bool
    issues: list[ValidationIssue] = Field(default_factory=make_validation_issue_list)

    @classmethod
    def success(cls, value: T) -> ValidationResult[T]:
        """Create a successful validation result."""
        return cls(value=value, is_valid=True, issues=[])

    @classmethod
    def failure(cls, value: T, issues: list[ValidationIssue]) -> ValidationResult[T]:
        """Create a failed validation result."""
        return cls(value=value, is_valid=False, issues=issues)

    @classmethod
    def from_issues(cls, value: T, issues: list[ValidationIssue]) -> ValidationResult[T]:
        """Create result based on issues (fails if any errors)."""
        has_errors = any(issue.is_error() for issue in issues)
        return cls(value=value, is_valid=not has_errors, issues=issues)

    def add_issue(self, issue: ValidationIssue) -> ValidationResult[T]:
        """Add an issue and return new result."""
        new_issues = [*self.issues, issue]
        has_errors = any(issue.is_error() for issue in new_issues)
        return ValidationResult(
            value=self.value,
            is_valid=not has_errors,
            issues=new_issues,
        )

    def merge(self, other: ValidationResult[Any]) -> ValidationResult[T]:
        """Merge with another validation result."""
        merged_issues = self.issues + other.issues
        has_errors = any(issue.is_error() for issue in merged_issues)
        return ValidationResult(
            value=self.value,
            is_valid=not has_errors,
            issues=merged_issues,
        )

    def get_errors(self) -> list[ValidationIssue]:
        """Get only error-level issues."""
        return [issue for issue in self.issues if issue.is_error()]

    def get_warnings(self) -> list[ValidationIssue]:
        """Get only warning-level issues."""
        return [issue for issue in self.issues if issue.is_warning()]

    def get_issues_by_field(self, field: str) -> list[ValidationIssue]:
        """Get issues for a specific field."""
        return [issue for issue in self.issues if issue.field == field]

    def has_errors(self) -> bool:
        """Check if there are any error-level issues."""
        return any(issue.is_error() for issue in self.issues)

    def has_warnings(self) -> bool:
        """Check if there are any warning-level issues."""
        return any(issue.is_warning() for issue in self.issues)

    def map(self, func: Callable[[T], U], default_value: U) -> ValidationResult[U]:
        """Transform the value while preserving validation state.

        Args:
            func: Function to transform the value
            default_value: Value to use if transformation fails or current result is invalid
        """
        if not self.is_valid:
            # If current result is invalid, cannot map - return invalid result with default
            return ValidationResult(
                value=default_value,
                is_valid=False,
                issues=self.issues,
            )

        try:
            new_value = func(self.value)
            return ValidationResult(
                value=new_value,
                is_valid=True,
                issues=self.issues,
            )
        except (ValueError, TypeError, AttributeError, RuntimeError) as e:
            # If mapping fails, add an error
            issue = ValidationIssue(
                severity=ValidationSeverity.ERROR,
                category=ValidationCategory.INVALID_VALUE,
                field=None,
                message=f"Failed to transform value: {e!s}",
                code="TRANSFORMATION_ERROR",
            )
            return ValidationResult(
                value=default_value,
                is_valid=False,
                issues=[*self.issues, issue],
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "is_valid": self.is_valid,
            "errors": [
                {
                    "field": issue.field,
                    "message": issue.message,
                    "code": issue.code,
                    "severity": issue.severity.value,
                    "category": issue.category.value,
                }
                for issue in self.issues
            ],
            "error_count": len(self.get_errors()),
            "warning_count": len(self.get_warnings()),
        }


U = TypeVar("U", bound=object)  # Bounded to object for transformation flexibility


class ValidationChain[T](BaseModel):
    """Fluent interface for chaining validations."""

    model_config = ConfigDict(extra="forbid")

    _result: ValidationResult[T] = Field(exclude=True)

    def __init__(self, value: T, **data: object) -> None:
        """Initialize validation chain."""
        super().__init__(**data)
        self._result = ValidationResult(value=value, is_valid=True, issues=[])

    def apply_validator(self, validator: Callable[[T], ValidationResult[T]]) -> ValidationChain[T]:
        """Apply a validator function."""
        if self._result.is_valid:
            validation_result = validator(self._result.value)
            self._result = self._result.merge(validation_result)
        return self

    def check(
        self,
        condition: Callable[[T], bool],
        issue: ValidationIssue,
    ) -> ValidationChain[T]:
        """Check a condition and add issue if false."""
        if self._result.is_valid and not condition(self._result.value):
            self._result = self._result.add_issue(issue)
        return self

    def transform(self, func: Callable[[T], U], default_value: U) -> ValidationChain[U]:
        """Transform the value being validated."""
        new_result = self._result.map(func, default_value)
        new_chain = ValidationChain(value=new_result.value)
        new_chain._result = new_result
        return new_chain

    def result(self) -> ValidationResult[T]:
        """Get the final validation result."""
        return self._result
