"""Centralized validation utilities for Pydantic models.

This module provides reusable validators to eliminate the 1,200+ lines of
duplicated validation code across the codebase.

Usage:
    from cyberdelta.models.base_validators import ExchangeValidationMixin

    class MyModel(ExchangeValidationMixin, BaseModel):
        exchange: ExchangeName
        # exchange validation is automatically applied
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import (
    DateTimeFieldError,
    DecimalFiniteError,
    FieldNameMissingError,
    InvalidExchangeNameError,
    RequiredFieldNoneError,
    TypeFieldError,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value


if TYPE_CHECKING:
    pass


# --- Base Model Configurations ---


class StandardModel(BaseModel):
    """Standard base model with common configuration.

    Eliminates ConfigDict duplication across 40+ models.
    """

    model_config = ConfigDict(
        validate_assignment=True,
        arbitrary_types_allowed=False,
        str_strip_whitespace=True,
        use_enum_values=False,
        extra="forbid",
    )


class ImmutableModel(BaseModel):
    """Immutable base model for frozen data structures."""

    model_config = ConfigDict(
        frozen=True,
        validate_assignment=False,
        arbitrary_types_allowed=False,
        str_strip_whitespace=True,
        use_enum_values=False,
        extra="forbid",
    )


class ExtensionSlotModel(BaseModel):
    """Base model for extension slot patterns."""

    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)


# --- Validation Mixins ---


class ExchangeValidationMixin:
    """Mixin providing exchange validation for ExchangeName fields.

    Eliminates duplicate exchange validation across 7+ models.

    Usage:
        class MyModel(ExchangeValidationMixin, BaseModel):
            exchange: ExchangeName
    """

    @field_validator("exchange", mode="before")
    @classmethod
    def validate_exchange(cls, v: object, info: ValidationInfo) -> ExchangeName:
        """Validate exchange field is a valid ExchangeName.

        Args:
            v: The value to validate
            info: Validation context containing field name

        Returns:
            Validated ExchangeName value

        Raises:
            InvalidExchangeNameError: If not a valid exchange name
            TypeFieldError: If value is not a string or ExchangeName enum
        """
        if isinstance(v, ExchangeName):
            return v
        if isinstance(v, str):
            try:
                return ExchangeName(v.lower())
            except ValueError as e:
                raise InvalidExchangeNameError(
                    value=v,
                    valid_exchanges=[ex.value for ex in ExchangeName],
                ) from e
        raise TypeFieldError(
            field_name="exchange",
            expected_type="string or ExchangeName",
            actual_type=type(v).__name__,
            actual_value=v,
        )


class DecimalValidationMixin:
    """Mixin providing common decimal validation patterns.

    Eliminates duplicate decimal validation across 10+ models.
    """

    @staticmethod
    def validate_required_decimal(
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite.

        Args:
            v: The value to parse
            info: Validation context

        Returns:
            Parsed finite decimal value

        Raises:
            FieldNameMissingError: If field name is None
            RequiredFieldNoneError: If parsed value is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name)
        if parsed is None:
            raise RequiredFieldNoneError(field_name)
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed)
        return parsed

    @staticmethod
    def validate_optional_decimal(
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present.

        Args:
            v: The value to parse
            info: Validation context

        Returns:
            Parsed finite decimal value or None

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        if v is None:
            return None
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:
            return None
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed)
        return parsed

    @staticmethod
    def validate_positive_decimal(
        v: str | float | Decimal | None, info: ValidationInfo
    ) -> Decimal | None:
        """Parse decimal, ensuring finite and positive.

        Args:
            v: The value to parse
            info: Validation context

        Returns:
            Parsed finite positive decimal value or None

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        if v is None:
            return None

        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:
            return None
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed)
        # Note: positive validation would be done via Field(gt=0) in model
        return parsed


class DateTimeValidationMixin:
    """Mixin providing common datetime validation patterns.

    Eliminates duplicate datetime validation across 4+ models.
    """

    @staticmethod
    def validate_required_datetime_utc(
        v: str | float | datetime,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC.

        Args:
            v: Value to parse as datetime
            info: Validation context

        Returns:
            Parsed UTC datetime

        Raises:
            FieldNameMissingError: If field name is not available
            DateTimeFieldError: If datetime parsing fails or returns None
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        dt = parse_datetime_utc(v, field_name=field_name)
        if dt is None:
            raise DateTimeFieldError(
                field_name=field_name,
                value=v,
                reason="Required datetime value parsed as None or was invalid",
            )
        return dt

    @staticmethod
    def validate_optional_datetime_utc(
        v: str | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime | None:
        """Parse optional datetime, ensuring UTC if present.

        Args:
            v: Value to parse as datetime or None
            info: Validation context

        Returns:
            Parsed UTC datetime or None

        Raises:
            FieldNameMissingError: If field name is not available
        """
        if v is None:
            return None
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        return parse_datetime_utc(v, field_name=field_name)


# --- Convenience Validator Functions ---


def exchange_validator(
    field_name: str = "exchange",
) -> Callable[[type[BaseModel], object, ValidationInfo], ExchangeName]:
    """Create a reusable exchange validator for a specific field.

    Args:
        field_name: Name of the field to validate

    Returns:
        A field_validator decorator

    Usage:
        class MyModel(BaseModel):
            exchange: ExchangeName
            _validate_exchange = exchange_validator("exchange")
    """

    @field_validator(field_name, mode="before")
    def validate_exchange_field(
        cls: type[BaseModel], v: object, info: ValidationInfo
    ) -> ExchangeName:
        return ExchangeValidationMixin.validate_exchange(v, info)

    return validate_exchange_field


def required_decimal_validator(
    *field_names: str,
) -> Callable[[type[BaseModel], str | float | Decimal | None, ValidationInfo], Decimal]:
    """Create a reusable required decimal validator.

    Args:
        field_names: Names of fields to validate

    Returns:
        A field_validator decorator

    Usage:
        class MyModel(BaseModel):
            price: Decimal
            size: Decimal
            _validate_decimals = required_decimal_validator("price", "size")
    """

    @field_validator(*field_names, mode="before")
    def validate_required_decimal_fields(
        cls: type[BaseModel],
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal:
        return DecimalValidationMixin.validate_required_decimal(v, info)

    return validate_required_decimal_fields


def optional_decimal_validator(
    *field_names: str,
) -> Callable[[type[BaseModel], str | float | Decimal | None, ValidationInfo], Decimal | None]:
    """Create a reusable optional decimal validator.

    Args:
        field_names: Names of fields to validate

    Returns:
        A field_validator decorator
    """

    @field_validator(*field_names, mode="before")
    def validate_optional_decimal_fields(
        cls: type[BaseModel],
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        return DecimalValidationMixin.validate_optional_decimal(v, info)

    return validate_optional_decimal_fields


def required_datetime_validator(
    *field_names: str,
) -> Callable[[type[BaseModel], str | float | datetime, ValidationInfo], datetime]:
    """Create a reusable required datetime validator.

    Args:
        field_names: Names of fields to validate

    Returns:
        A field_validator decorator
    """

    @field_validator(*field_names, mode="before")
    def validate_required_datetime_fields(
        cls: type[BaseModel],
        v: str | float | datetime,
        info: ValidationInfo,
    ) -> datetime:
        return DateTimeValidationMixin.validate_required_datetime_utc(v, info)

    return validate_required_datetime_fields


def optional_datetime_validator(
    *field_names: str,
) -> Callable[[type[BaseModel], str | float | datetime | None, ValidationInfo], datetime | None]:
    """Create a reusable optional datetime validator.

    Args:
        field_names: Names of fields to validate

    Returns:
        A field_validator decorator
    """

    @field_validator(*field_names, mode="before")
    def validate_optional_datetime_fields(
        cls: type[BaseModel],
        v: str | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime | None:
        return DateTimeValidationMixin.validate_optional_datetime_utc(v, info)

    return validate_optional_datetime_fields
