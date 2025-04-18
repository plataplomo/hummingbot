from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from cyberdelta.utils.parsing import parse_decimal_value


class FundingRate(BaseModel):
    """
    FundingRate models the funding rate and related data for a perpetual contract. It is immutable
    (frozen=True) to ensure that funding data is not altered after retrieval.

    Fields:
        symbol (str): Trading symbol.
        funding_rate (Decimal | None): Current funding rate.
        predicted_rate (Decimal | None): Predicted next funding rate.
        mark_price (Decimal | None): Current mark price.
        index_price (Decimal | None): Current index price.
        next_funding_time (int | None): Timestamp of next funding event.
        timestamp (int | None): Data snapshot timestamp.
        historical_rates (list[dict[str, Any]] | None): Optional historical funding data.

    Notes:
        - All rate/price fields use Decimal for precision.
        - This model is not intended for mutation after creation.
    """

    symbol: str
    funding_rate: Decimal | None = None
    predicted_rate: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    next_funding_time: int | None = None
    timestamp: int | None = None
    historical_rates: list[dict[str, Any]] | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("funding_rate", "predicted_rate", "mark_price", "index_price", mode="before")
    @classmethod
    def parse_decimal(
        cls, raw_value: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(raw_value)
