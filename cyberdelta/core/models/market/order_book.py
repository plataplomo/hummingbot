from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


class OrderBook(BaseModel):
    """
    Represents an immutable snapshot of the order book for a specific symbol at a given time.

    This model enforces strict validation for structure and data types, ensuring consistency
    for internal use.

    Fields:
        symbol (str): Trading symbol (validated: non-empty, max 64 chars, UTF-8).
        timestamp (datetime): UTC timestamp of the snapshot (required, validated).
        bids (list[tuple[Decimal, Decimal]]): List of (price, quantity) tuples for bids.
                                            Validated: price is finite, quantity is finite & >= 0.
        asks (list[tuple[Decimal, Decimal]]): List of (price, quantity) tuples for asks.
                                            Validated: price is finite, quantity is finite & >= 0.

    Notes:
        - Prices/quantities use Decimal for precision.
        - Bids/Asks are expected to be sorted by price appropriately by the caller.
        - Immutability (frozen=True) guarantees snapshot integrity.
        - `extra='forbid'` prevents unexpected fields.
    """

    symbol: str
    timestamp: datetime
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object) -> str:
        """Validate symbol field."""
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: datetime | int | float | str | None) -> datetime:
        """Validate and parse timestamp field, ensuring it is a required UTC datetime."""
        dt = parse_datetime_utc(v, field_name="timestamp")
        if dt is None:
            # This path should ideally not be hit if the field is required by schema,
            # but validator ensures non-None result.
            raise ValueError("timestamp must not be None")
        return dt

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def validate_and_parse_levels(
        cls, v: object, info: ValidationInfo
    ) -> list[tuple[Decimal, Decimal]]:
        """
        Validate structure, parse, and validate content of bids/asks levels.

        Expects a list where each item is a list/tuple of length 2.
        Parses price and quantity to Decimal, ensuring price is finite and
        quantity is finite and non-negative.
        Returns the fully validated list of (Decimal, Decimal) tuples.
        """
        field_name = info.field_name
        if not isinstance(v, list):
            raise TypeError(f"{field_name} must be a list, got {type(v).__name__}")

        validated_levels: list[tuple[Decimal, Decimal]] = []
        for index, level_raw in enumerate(v):
            # 1. Validate Structure
            if not isinstance(level_raw, list | tuple):
                raise TypeError(
                    f"Level item in {field_name} at index {index} must be a list or tuple, "
                    f"got {type(level_raw).__name__}"
                )
            # DEFENSIVE CHECK: Runtime length check. Pyright=[reportUnknownArgumentType]
            if len(level_raw) != 2:
                raise ValueError(
                    f"Level item in {field_name} at index {index} must have length 2, "
                    f"got length {len(level_raw)}"
                )

            # Type checkers might see level_raw[0]/[1] as Any/Unknown here.
            # Runtime isinstance checks below handle validation.
            price_raw = level_raw[0]
            quantity_raw = level_raw[1]

            # 2. Validate and Parse Price
            if not isinstance(price_raw, Decimal | str | int | float):
                raise TypeError(
                    f"Invalid price type in {field_name} at index {index}: "
                    f"Expected Decimal, str, int, or float, got {type(price_raw).__name__}"
                )
            try:
                # parse_decimal_value handles str, int, float, Decimal inputs
                # It raises ValueError on failure (incl. None), which we catch below.
                price = parse_decimal_value(price_raw)
            except ValueError as e:
                # Re-raise with more context
                raise ValueError(
                    f"Invalid price value in {field_name} at index {index}: {e}"
                ) from e

            # 3. Validate and Parse Quantity
            if not isinstance(quantity_raw, Decimal | str | int | float):
                raise TypeError(
                    f"Invalid quantity type in {field_name} at index {index}: "
                    f"Expected Decimal, str, int, or float, got {type(quantity_raw).__name__}"
                )
            try:
                quantity = parse_decimal_value(quantity_raw)
            except ValueError as e:
                # Re-raise with more context
                raise ValueError(
                    f"Invalid quantity value in {field_name} at index {index}: {e}"
                ) from e

            # DEFENSIVE CHECK: Ensure price/quantity are not None before further checks & append.
            assert price is not None, (
                f"Price unexpectedly None after parsing at {field_name}[{index}]"
            )
            assert quantity is not None, (
                f"Quantity unexpectedly None after parsing at {field_name}[{index}]"
            )

            # 4. Post-parse Validation (Now that types are confirmed Decimal)
            if not price.is_finite():
                raise ValueError(
                    f"Invalid price value in {field_name} at index {index}: "
                    f"Expected finite Decimal, got {price}"
                )
            if not quantity.is_finite():
                raise ValueError(
                    f"Invalid quantity value in {field_name} at index {index}: "
                    f"Expected finite Decimal, got {quantity}"
                )
            if quantity < Decimal(0):
                raise ValueError(
                    f"Invalid quantity value in {field_name} at index {index}: "
                    f"Must be non-negative, got {quantity}"
                )

            validated_levels.append((price, quantity))

        return validated_levels
