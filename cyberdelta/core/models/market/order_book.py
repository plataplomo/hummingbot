"""Order book data model for Level 2 market depth representation.

This module provides the OrderBook model for representing immutable snapshots
of Level 2 order book data with strict validation and financial precision.

The OrderBook model ensures data integrity through:
- Strict validation of bid/ask price and quantity tuples
- Decimal precision for all financial values
- Immutable design to prevent accidental modification
- Comprehensive type checking and parsing from various input formats
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import TypeGuard, cast

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.exceptions.field_validation import (
    DecimalFieldError,
    DecimalFiniteError,
    ListFieldError,
    RangeFieldError,
    RequiredFieldNoneError,
    TypeFieldError,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


# Order book structure constants
LEVEL_PAIR_LENGTH = 2  # Expected length for price/quantity pairs in order book levels


class OrderBook(BaseModel):
    """Represents an immutable, validated snapshot of the L2 order book for a specific symbol.

    This model enforces strict validation for structure and data types during initialization,
    ensuring consistency and safety for internal use within the CyberDeltaEngine. It uses
    `mode='before'` validators to handle parsing and validation of potentially diverse input types
    (e.g., str, int, float) directly into the required internal types (Decimal, datetime).

    Attributes:
        symbol: Trading symbol (validated: required, non-empty, max 64 chars, UTF-8).
        timestamp: UTC timestamp of the snapshot (validated: required).
        bids: List of (price, quantity) tuples for bids, validated & parsed to Decimal.
              Validated: price is finite, quantity is finite & non-negative.
              Input can be list[tuple[Decimal | str | int | float,
                                      Decimal | str | int | float]].
        asks: List of (price, quantity) tuples for asks, validated & parsed to Decimal.
              Validated: price is finite, quantity is finite & non-negative.
              Input can be list[tuple[Decimal | str | int | float,
                                      Decimal | str | int | float]].

    Configuration:
        - `frozen=True`: Guarantees immutability after creation.
        - `extra='forbid'`: Prevents unexpected fields during initialization.
        - `validate_assignment=True`: Ensures validation runs even if
                    attributes are somehow modified post-init (though frozen should prevent this).

    """

    symbol: str
    timestamp: datetime
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object) -> str:
        """Validate the 'symbol' field.

        Ensures the symbol is a non-empty string with a maximum length of 64 characters.

        Args:
            v: The raw input value for the symbol.

        Returns:
            The validated symbol string.
        """
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: datetime | float | str | None) -> datetime:
        """Validate and parse the 'timestamp' field to a required UTC datetime object.

        Handles various input types (datetime, int/float ms epoch, ISO string)
            via `parse_datetime_utc`.

        Args:
            v: The raw input value for the timestamp.

        Returns:
            The validated, timezone-aware (UTC) datetime object.

        Raises:
            RequiredFieldNoneError: If the input is None after parsing.
        """
        dt = parse_datetime_utc(v, field_name="timestamp")
        if dt is None:
            # This path should ideally not be hit if the field is required by Pydantic's schema
            # validation for non-optional fields, but this check provides explicit runtime safety.
            raise RequiredFieldNoneError("timestamp")
        return dt

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def validate_and_parse_levels(
        cls,
        v: object,
        info: ValidationInfo,
    ) -> list[tuple[Decimal, Decimal]]:
        """Validate structure, parse types, and validate content for 'bids' and 'asks' fields.

        This comprehensive `mode='before'` validator handles
                the entire process for order book levels:
        1.  Ensures the input `v` is a list.
        2.  Iterates through each `level_raw` item in the list.
        3.  Validates `level_raw` structure: Must be a list or tuple of exactly length 2.
        4.  Parses `price_raw` (level_raw[0]) and `quantity_raw` (level_raw[1]) from potentially
            mixed input types (Decimal, str, int, float) into Decimal using `parse_decimal_value`.
        5.  Validates parsed `price`: Must be finite (not NaN or Infinity).
        6.  Validates parsed `quantity`: Must be finite and non-negative (>= 0).
        7.  Appends the validated `(Decimal, Decimal)` tuple to the result list.

        Args:
            v: The raw input value for the bids/asks list. Expected to be a list of lists/tuples.
            info: Pydantic validation context, used to get the field name ("bids" or "asks").

        Returns:
            A list of validated (Decimal price, Decimal quantity) tuples.

        Raises:
            ListFieldError: If the input `v` is not a list or if items within `v` are not
                lists/tuples.
        """
        field_name = info.field_name or "unknown_field"
        if not isinstance(v, list):
            raise ListFieldError(
                field_name=field_name,
                actual_type=type(v).__name__,
            )

        validated_levels: list[tuple[Decimal, Decimal]] = []

        # Process each item in the list - we know v is a list from the isinstance check above
        # Cast to help pyright understand the type after isinstance check
        v_list = cast("list[object]", v)
        list_length = len(v_list)
        for index in range(list_length):
            # Access items by index - pyright understands this pattern better
            level_raw = v_list[index]
            validated_level = cls._validate_single_level(level_raw, field_name, index)
            validated_levels.append(validated_level)

        return validated_levels

    @classmethod
    def _validate_single_level(
        cls,
        level_raw: object,
        field_name: str,
        index: int,
    ) -> tuple[Decimal, Decimal]:
        """Validate and parse a single order book level.
        
        Args:
            level_raw: Raw level data (should be list/tuple with 2 elements)
            field_name: Name of the field being validated
            index: Index of the level in the list
            
        Returns:
            Validated tuple of (price, quantity) as Decimals
            
        Raises:
            TypeFieldError: If level structure or data types are invalid
        """
        # 1. Validate Structure (Runtime check)
        cls._validate_level_structure(level_raw, field_name, index)

        # Extract raw price/quantity. Runtime checks follow.
        # Type narrowing after validation - we know level_raw is list|tuple with length 2
        # Use type guard for safe access
        if not cls._is_valid_level_sequence(level_raw):
            # This should never happen due to _validate_level_structure
            raise TypeFieldError(
                field_name=f"{field_name}[{index}]",
                expected_type="list or tuple",
                actual_type=type(level_raw).__name__,
            )

        price_raw = level_raw[0]
        quantity_raw = level_raw[1]

        # 2. Parse and validate price and quantity
        price = cls._parse_and_validate_price(price_raw, field_name, index)
        quantity = cls._parse_and_validate_quantity(quantity_raw, field_name, index)

        return (price, quantity)

    @classmethod
    def _is_valid_level_sequence(cls, level_raw: object) -> TypeGuard[Sequence[object]]:
        """Type guard to check if level_raw is a valid sequence.
        
        Args:
            level_raw: Object to check
            
        Returns:
            True if level_raw is a list or tuple, False otherwise
        """
        return isinstance(level_raw, list | tuple)

    @classmethod
    def _validate_level_structure(cls, level_raw: object, field_name: str, index: int) -> None:
        """Validate the structure of a single level.
        
        Args:
            level_raw: Raw level data to validate
            field_name: Name of the field being validated
            index: Index of the level in the list
            
        Raises:
            ListFieldError: If level is not a list/tuple or has incorrect length
            RangeFieldError: If level length is not exactly 2
        """
        if not cls._is_valid_level_sequence(level_raw):
            # Get type name without type-checking issues
            type_name = type(level_raw).__name__ if level_raw is not None else "None"
            raise ListFieldError(
                field_name=field_name,
                actual_type=type_name,
                item_index=index,
                expected_item_type="list or tuple",
            )
        # DEFENSIVE CHECK: Runtime length check.
        # After type guard check, we know level_raw is a Sequence
        level_len = len(level_raw)
        if level_len != LEVEL_PAIR_LENGTH:
            raise RangeFieldError(
                field_name=f"{field_name}[{index}]",
                value=level_len,
                constraint=f"Level item must have exactly {LEVEL_PAIR_LENGTH} elements",
            )

    @classmethod
    def _parse_and_validate_price(cls, price_raw: object, field_name: str, index: int) -> Decimal:
        """Parse and validate price value.
        
        Args:
            price_raw: Raw price value to parse and validate
            field_name: Name of the field being validated
            index: Index of the level in the list
            
        Returns:
            Validated price as a finite Decimal
            
        Raises:
            TypeFieldError: If price type is not supported
            DecimalFieldError: If price cannot be parsed to Decimal
            RequiredFieldNoneError: If parsed price is None
            DecimalFiniteError: If price is not finite
        """
        # 2. Validate and Parse Price (Runtime check + parse attempt)
        if not isinstance(price_raw, Decimal | str | int | float):
            # Get type name without type-checking issues
            type_name = type(price_raw).__name__ if price_raw is not None else "None"
            raise TypeFieldError(
                field_name=f"{field_name}[{index}].price",
                expected_type="Decimal, str, int, or float",
                actual_type=type_name,
                actual_value=price_raw,
            )
        try:
            price = parse_decimal_value(price_raw)
        except ValueError as e:
            raise DecimalFieldError(
                field_name=f"{field_name}[{index}].price",
                value=price_raw,
                reason=str(e),
            ) from e

        # DEFENSIVE CHECK: Runtime check post-parsing.
        if price is None:
            raise RequiredFieldNoneError(
                field_name=f"{field_name}[{index}].price",
                reason="Price unexpectedly None after parsing",
            )

        # 4. Post-parse Validation (Finite)
        if not price.is_finite():
            raise DecimalFiniteError(
                field_name=f"{field_name}[{index}].price",
                value=price,
                context=f"(got {price})",
            )

        return price

    @classmethod
    def _parse_and_validate_quantity(
        cls,
        quantity_raw: object,
        field_name: str,
        index: int,
    ) -> Decimal:
        """Parse and validate quantity value.
        
        Args:
            quantity_raw: Raw quantity value to parse and validate
            field_name: Name of the field being validated
            index: Index of the level in the list
            
        Returns:
            Validated quantity as a finite, non-negative Decimal
            
        Raises:
            TypeFieldError: If quantity type is not supported
            DecimalFieldError: If quantity cannot be parsed to Decimal
            RequiredFieldNoneError: If parsed quantity is None
            DecimalFiniteError: If quantity is not finite
            RangeFieldError: If quantity is negative
        """
        # 3. Validate and Parse Quantity (Runtime check + parse attempt)
        if not isinstance(quantity_raw, Decimal | str | int | float):
            # Get type name without type-checking issues
            type_name = type(quantity_raw).__name__ if quantity_raw is not None else "None"
            raise TypeFieldError(
                field_name=f"{field_name}[{index}].quantity",
                expected_type="Decimal, str, int, or float",
                actual_type=type_name,
                actual_value=quantity_raw,
            )
        try:
            quantity = parse_decimal_value(quantity_raw)
        except ValueError as e:
            raise DecimalFieldError(
                field_name=f"{field_name}[{index}].quantity",
                value=quantity_raw,
                reason=str(e),
            ) from e

        # DEFENSIVE CHECK: Runtime check post-parsing.
        if quantity is None:
            raise RequiredFieldNoneError(
                field_name=f"{field_name}[{index}].quantity",
                reason="Quantity unexpectedly None after parsing",
            )

        # 4. Post-parse Validation (Finite, Non-negative Quantity)
        if not quantity.is_finite():
            raise DecimalFiniteError(
                field_name=f"{field_name}[{index}].quantity",
                value=quantity,
                context=f"(got {quantity})",
            )
        if quantity < Decimal(0):
            raise RangeFieldError(
                field_name=f"{field_name}[{index}].quantity",
                value=quantity,
                min_value=0.0,
                constraint="Quantity must be non-negative",
            )

        return quantity
