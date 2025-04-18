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
    MarketData represents a snapshot of market information for a specific trading symbol,
    including OHLCV (Open, High, Low, Close, Volume) and optional ticker data. This model is
    immutable (frozen=True) to ensure that once market data is captured from an exchange or data
    provider, it cannot be altered, preserving auditability and data integrity.

    Fields:
        symbol (str): The trading symbol (e.g., 'BTC-PERP').
        timestamp (datetime): The UTC timestamp of the data snapshot.
        open (Decimal): Opening price for the period.
        high (Decimal): Highest price for the period.
        low (Decimal): Lowest price for the period.
        close (Decimal): Closing price for the period.
        volume (Decimal): Trading volume for the period (default 0.0).
        ticker_data (dict[str, dict[str, Ticker]] | None): Optional nested ticker data for advanced
            analytics or multi-venue aggregation.

    Notes:
        - All price and volume fields use Decimal for precision (see Decimal usage rule).
        - This model is not intended for mutation after creation; use a new instance for new data.
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
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
