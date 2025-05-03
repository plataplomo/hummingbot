from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from cyberdelta.utils.parsing import parse_decimal_value


class Ticker(BaseModel):
    """
    Ticker provides a lightweight snapshot of the best bid/ask, last price, and volume for a symbol.
    It is immutable (frozen=True) to ensure that ticker data reflects the exact state at the time of
    retrieval.

    Fields:
        symbol (str): Trading symbol.
        price (Decimal | None): Last traded price.
        bid (Decimal | None): Best bid price.
        ask (Decimal | None): Best ask price.
        volume (Decimal | None): Trading volume.
        timestamp (int | None): Optional timestamp.

    Notes:
        - All price/volume fields use Decimal for precision.
        - This model is not intended for mutation after creation.
    """

    symbol: str
    price: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    volume: Decimal | None = None
    timestamp: int | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def parse_decimal(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(raw_value)

    def to_dict(self) -> dict[str, Any]:
        """Subject to deprecation: Prefer model_dump(mode='json') for future serialization."""
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
        return data
