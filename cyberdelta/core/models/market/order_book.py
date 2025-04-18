from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator

from cyberdelta.utils.parsing import parse_decimal_value


class OrderBook(BaseModel):
    """
    OrderBook represents the current state of the order book for a symbol, including all bid and ask
    levels. It is immutable (frozen=True) to ensure that order book snapshots are reliable and
    auditable.

    Fields:
        symbol (str): Trading symbol.
        bids (list[tuple[Decimal, Decimal]]): List of (price, quantity) tuples for bids.
        asks (list[tuple[Decimal, Decimal]]): List of (price, quantity) tuples for asks.
        timestamp (int | None): Optional timestamp.

    Notes:
        - All price/quantity fields use Decimal for accuracy.
        - This model is not intended for mutation after creation.
    """

    symbol: str
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def parse_levels(
        cls,
        raw_levels: list[tuple[str | int | float | Decimal, str | int | float | Decimal]],
        info: object,
    ) -> list[tuple[Decimal, Decimal]]:
        result: list[tuple[Decimal, Decimal]] = []
        for field_index, level in enumerate(raw_levels):
            if len(level) != 2:
                raise ValueError(
                    f"Invalid item in '{getattr(info, 'field_name', '<unknown>')}' "
                    f"at index {field_index}: {level}"
                )
            price = parse_decimal_value(level[0])
            quantity = parse_decimal_value(level[1])
            if price is None or quantity is None:
                raise ValueError(
                    f"Invalid price/quantity in '{getattr(info, 'field_name', '<unknown>')}' "
                    f"at index {field_index}: {level}"
                )
            result.append((price, quantity))
        return result
