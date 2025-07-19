"""Factory classes for creating validation contexts.

This module provides factory classes for creating common validation contexts
for various field types.
"""

from decimal import Decimal

from cyberdelta.apis.base.validation_contexts import (
    NumericValidationContext,
    StringValidationContext,
    ValidationContext,
)
from cyberdelta.apis.base.validation_policies import (
    NullPolicy,
    PrecisionPolicy,
    RangePolicy,
    StringPolicy,
)


class FieldValidationFactory:
    """Factory for creating validation contexts for common field types."""

    @staticmethod
    def for_price_field(field_name: str) -> NumericValidationContext:
        """Create validation context for price fields."""
        return NumericValidationContext(
            field_name=field_name,
            context_description=f"Price field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            range_policy=RangePolicy.FINANCIAL_POSITIVE,
            precision_policy=PrecisionPolicy.PRICE_4,
            min_value=Decimal("0.0001"),
        )

    @staticmethod
    def for_quantity_field(field_name: str) -> NumericValidationContext:
        """Create validation context for quantity fields."""
        return NumericValidationContext(
            field_name=field_name,
            context_description=f"Quantity field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            range_policy=RangePolicy.POSITIVE,
            precision_policy=PrecisionPolicy.QUANTITY_6,
            min_value=Decimal("0.000001"),
        )

    @staticmethod
    def for_symbol_field(field_name: str) -> StringValidationContext:
        """Create validation context for symbol fields."""
        return StringValidationContext(
            field_name=field_name,
            context_description=f"Symbol field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            string_policy=StringPolicy.TRIM_WHITESPACE,
            min_length=3,
            max_length=20,
            pattern=r"^[A-Z0-9_-]+$",
        )

    @staticmethod
    def for_percentage_field(field_name: str) -> NumericValidationContext:
        """Create validation context for percentage fields."""
        return NumericValidationContext(
            field_name=field_name,
            context_description=f"Percentage field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            range_policy=RangePolicy.PERCENTAGE,
            precision_policy=PrecisionPolicy.PERCENTAGE_2,
            min_value=Decimal(0),
            max_value=Decimal(100),
        )

    @staticmethod
    def for_optional_field(field_name: str, base_context: ValidationContext) -> ValidationContext:
        """Create validation context for optional fields."""
        return ValidationContext(
            field_name=field_name,
            context_description=f"Optional field validation for {field_name}",
            null_policy=NullPolicy.ALLOW,
            range_policy=base_context.range_policy,
            precision_policy=base_context.precision_policy,
            string_policy=base_context.string_policy,
            timestamp_policy=base_context.timestamp_policy,
        )


__all__ = [
    "FieldValidationFactory",
]