from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class MarketData(BaseModel):
    """
    DEPRECATED: This class is deprecated and replaced by Candle. Remove all usage and delete this file when migration is complete.
    """

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0.0")
    ticker_data: dict[str, dict[str, Ticker]] | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def parse_decimal(cls, raw_value: str | int | float | Decimal | None, info: object) -> Decimal:
        decimal_value = parse_decimal_value(raw_value)
        if decimal_value is None:
            raise ValueError(
                f"Field '{getattr(info, 'field_name', '<unknown>')}' cannot be None or invalid."
            )
        return decimal_value

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_datetime(
        cls, raw_value: str | int | float | datetime | None, info: object
    ) -> datetime:
        datetime_value = parse_datetime_utc(raw_value)
        if datetime_value is None:
            raise ValueError(
                f"Field '{getattr(info, 'field_name', '<unknown>')}' cannot be None or invalid."
            )
        return datetime_value

    def to_dict(self) -> dict[str, Any]:
        """Subject to deprecation: Prefer model_dump(mode='json') for future serialization."""
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
