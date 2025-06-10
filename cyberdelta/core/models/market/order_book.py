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

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


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

        Raises:
            ValueError: If validation fails (e.g., empty, too long).

        """
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: datetime | int | float | str | None) -> datetime:
        """Validate and parse the 'timestamp' field to a required UTC datetime object.

        Handles various input types (datetime, int/float ms epoch, ISO string)
            via `parse_datetime_utc`.

        Args:
            v: The raw input value for the timestamp.

        Returns:
            The validated, timezone-aware (UTC) datetime object.

        Raises:
            ValueError: If the input is None or cannot be parsed into a valid datetime.

        """
        dt = parse_datetime_utc(v, field_name="timestamp")
        if dt is None:
            # This path should ideally not be hit if the field is required by Pydantic's schema
            # validation for non-optional fields, but this check provides explicit runtime safety.
            raise ValueError("timestamp must not be None")
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
            TypeError: If the input `v` is not a list, or if items within `v` are not lists/tuples,
                       or if price/quantity elements have
                        fundamentally incompatible types (e.g., None, dict).
            ValueError: If items within `v` do not have length 2,
                        or if price/quantity strings/numbers
                        cannot be parsed to Decimal,
                        or if parsed values are non-finite or quantity is negative.

        """
        field_name = info.field_name or "unknown_field"
        if not isinstance(v, list):
            raise TypeError(f"{field_name} must be a list, got {type(v).__name__}")

        validated_levels: list[tuple[Decimal, Decimal]] = []

        # Pyright struggles to track types precisely when iterating over 'v: object'.
        # Runtime checks below ensure safety, but ignores are needed for static analysis.
        for index, level_raw in enumerate(v):
            validated_level = cls._validate_single_level(level_raw, field_name, index)
            validated_levels.append(validated_level)

        return validated_levels

    @classmethod
    def _validate_single_level(
        cls, level_raw: object, field_name: str, index: int
    ) -> tuple[Decimal, Decimal]:
        """Validate and parse a single order book level."""
        # 1. Validate Structure (Runtime check)
        cls._validate_level_structure(level_raw, field_name, index)

        # Extract raw price/quantity. Runtime checks follow.
        # Type narrowing after validation - we know level_raw is list|tuple with length 2
        # Cast to the appropriate type after validation
        from typing import cast

        level_sequence = cast(list[object] | tuple[object, ...], level_raw)
        price_raw = level_sequence[0]
        quantity_raw = level_sequence[1]

        # 2. Parse and validate price and quantity
        price = cls._parse_and_validate_price(price_raw, field_name, index)
        quantity = cls._parse_and_validate_quantity(quantity_raw, field_name, index)

        return (price, quantity)

    @classmethod
    def _validate_level_structure(cls, level_raw: object, field_name: str, index: int) -> None:
        """Validate the structure of a single level."""
        if not isinstance(level_raw, list | tuple):
            # Ignore necessary because Pyright
            # doesn't know 'level_raw' type after initial check.
            raise TypeError(
                f"Level item in {field_name} at index {index} must be a list or tuple, "
                f"got {type(level_raw).__name__}",  # pyright: ignore[reportUnknownArgumentType]
            )
        # DEFENSIVE CHECK: Runtime length check.
        # Ignore necessary because Pyright doesn't know 'level_raw' type reliably here.
        if len(level_raw) != 2:  # pyright: ignore[reportUnknownArgumentType]
            # Ignore necessary because Pyright doesn't know 'level_raw' type reliably here.
            raise ValueError(
                f"Level item in {field_name} at index {index} must have length 2, "
                f"got length {len(level_raw)}",  # pyright: ignore[reportUnknownArgumentType]
            )

    @classmethod
    def _parse_and_validate_price(cls, price_raw: object, field_name: str, index: int) -> Decimal:
        """Parse and validate price value."""
        # 2. Validate and Parse Price (Runtime check + parse attempt)
        if not isinstance(price_raw, Decimal | str | int | float):
            # Ignore necessary because Pyright doesn't know 'price_raw' type reliably here.
            raise TypeError(
                f"Invalid price type in {field_name} at index {index}: "
                f"Expected Decimal, str, int, or float, got {type(price_raw).__name__}",  # pyright: ignore[reportUnknownArgumentType]
            )
        try:
            price = parse_decimal_value(price_raw)
        except ValueError as e:
            raise ValueError(
                f"Invalid price value in {field_name} at index {index}: {e}",
            ) from e

        # DEFENSIVE CHECK: Runtime check post-parsing.
        if price is None:
            raise ValueError(f"Price unexpectedly None after parsing at {field_name}[{index}]")

        # 4. Post-parse Validation (Finite)
        if not price.is_finite():
            raise ValueError(
                f"Invalid price value in {field_name} at index {index}: "
                f"Expected finite Decimal, got {price}",
            )

        return price

    @classmethod
    def _parse_and_validate_quantity(
        cls, quantity_raw: object, field_name: str, index: int
    ) -> Decimal:
        """Parse and validate quantity value."""
        # 3. Validate and Parse Quantity (Runtime check + parse attempt)
        if not isinstance(quantity_raw, Decimal | str | int | float):
            # Ignore necessary because Pyright doesn't know 'quantity_raw' type reliably here.
            raise TypeError(
                f"Invalid quantity type in {field_name} at index {index}: "
                f"Expected Decimal, str, int, or float, got {type(quantity_raw).__name__}",  # pyright: ignore[reportUnknownArgumentType]
            )
        try:
            quantity = parse_decimal_value(quantity_raw)
        except ValueError as e:
            raise ValueError(
                f"Invalid quantity value in {field_name} at index {index}: {e}",
            ) from e

        # DEFENSIVE CHECK: Runtime check post-parsing.
        if quantity is None:
            raise ValueError(
                f"Quantity unexpectedly None after parsing at {field_name}[{index}]",
            )

        # 4. Post-parse Validation (Finite, Non-negative Quantity)
        if not quantity.is_finite():
            raise ValueError(
                f"Invalid quantity value in {field_name} at index {index}: "
                f"Expected finite Decimal, got {quantity}",
            )
        if quantity < Decimal(0):
            raise ValueError(
                f"Invalid quantity value in {field_name} at index {index}: "
                f"Must be non-negative, got {quantity}",
            )

        return quantity
