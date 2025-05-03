from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.utils.parsing import parse_datetime_utc, validate_str_field


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
    def validate_symbol(cls, v: Any) -> str:
        """Validate symbol field."""
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: Any) -> datetime:
        """Validate and parse timestamp field, ensuring it is a required UTC datetime."""
        dt = parse_datetime_utc(v, field_name="timestamp")
        if dt is None:
            raise ValueError("timestamp must not be None")
        return dt

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def validate_level_structure(cls, v: Any, info: ValidationInfo) -> Any:
        """
        Validate the basic structure of bids/asks before Pydantic coercion.
        Ensures input is a list where each item is a list/tuple of length 2.
        Returns the original input `v` if the structure is valid, allowing Pydantic
        to perform type coercion.
        """
        field_name = info.field_name
        if not isinstance(v, list):
            raise TypeError(f"{field_name} must be a list, got {type(v).__name__}")

        for index, level_raw in enumerate(v):
            if not isinstance(level_raw, list | tuple):
                raise TypeError(
                    f"Level item in {field_name} at index {index} must be a list or tuple, "
                    f"got {type(level_raw).__name__}"
                )
            if len(level_raw) != 2:
                raise ValueError(
                    f"Level item in {field_name} at index {index} must have length 2, "
                    f"got length {len(level_raw)}"
                )
        # Return original value for Pydantic to coerce
        return v

    @field_validator("bids", "asks", mode="after")
    @classmethod
    def validate_level_content(
        cls, v: list[tuple[Decimal, Decimal]], info: ValidationInfo
    ) -> list[tuple[Decimal, Decimal]]:
        """
        Validate the content of bids/asks after Pydantic type coercion.
        Ensures quantities are non-negative. Finite checks are handled by Pydantic.
        Assumes input `v` has been successfully coerced to list[tuple[Decimal, Decimal]].
        """
        field_name = info.field_name
        for index, level in enumerate(v):
            # Input 'level' is guaranteed by Pydantic (due to mode='after' and type hint)
            # to be a tuple[Decimal, Decimal] if validation reaches this point
            # without coercion error.
            price, quantity = level

            # Finite checks are handled by Pydantic's Decimal validation
            # if not price.is_finite():
            #     raise ValueError(
            #         f"Invalid price in {field_name} at index {index}: "
            #         f"Expected finite Decimal, got {price}"
            #     )
            #
            # if not quantity.is_finite():
            #     raise ValueError(
            #         f"Invalid quantity in {field_name} at index {index}: "
            #         f"Expected finite Decimal, got {quantity}"
            #     )

            # Only check non-negative quantity here
            if quantity < Decimal(0):
                raise ValueError(
                    f"Invalid quantity in {field_name} at index {index}: "
                    f"Must be non-negative, got {quantity}"
                )

        return v
