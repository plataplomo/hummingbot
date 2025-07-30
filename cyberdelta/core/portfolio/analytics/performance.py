"""Performance tracking models and enums for portfolio analytics."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AnalyticsState(str, Enum):
    """Analytics operational states."""
    INACTIVE = "inactive"
    ACTIVE = "active"
    CALCULATING = "calculating"
    UPDATING = "updating"
    ERROR = "error"


class ReportFrequency(str, Enum):
    """Report generation frequencies."""
    REALTIME = "realtime"
    MINUTE = "minute"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"


class PerformanceSnapshot(BaseModel):
    """Point-in-time portfolio performance snapshot."""
    timestamp: datetime
    total_value: Decimal
    daily_pnl: Decimal
    cumulative_pnl: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    win_rate: Decimal
    sharpe_ratio: Decimal
    max_drawdown: Decimal
    positions_count: int

    model_config = ConfigDict(validate_assignment=True)

    @field_validator("win_rate", mode="before")
    @classmethod
    def validate_win_rate(cls, v: Any) -> Decimal:
        """Validate win rate is between 0 and 1."""
        value = Decimal(str(v))
        if not (Decimal(0) <= value <= Decimal(1)):
            raise ValueError("Win rate must be between 0 and 1")
        return value

    @field_validator("max_drawdown", mode="before")
    @classmethod
    def validate_max_drawdown(cls, v: Any) -> Decimal:
        """Validate max drawdown is between 0 and 1."""
        value = Decimal(str(v))
        if not (Decimal(0) <= value <= Decimal(1)):
            raise ValueError("Max drawdown must be between 0 and 1")
        return value


class AttributionResult(BaseModel):
    """Performance attribution analysis result."""
    period: timedelta
    total_pnl: Decimal
    by_exchange: dict[str, Decimal]
    by_symbol: dict[str, Decimal]
    by_strategy: dict[str, Decimal]  # Attribution to strategies, not implementation
    by_time_bucket: dict[str, Decimal]
    top_winners: list[tuple[str, Decimal]]
    top_losers: list[tuple[str, Decimal]]

    model_config = ConfigDict(validate_assignment=True)

    def get_strategy_contribution(self, strategy_id: str) -> Decimal:
        """Get P&L contribution from a specific strategy."""
        return self.by_strategy.get(strategy_id, Decimal(0))
    
    def get_exchange_contribution(self, exchange: str) -> Decimal:
        """Get P&L contribution from a specific exchange."""
        return self.by_exchange.get(exchange, Decimal(0))
    
    def get_symbol_contribution(self, symbol: str) -> Decimal:
        """Get P&L contribution from a specific symbol."""
        return self.by_symbol.get(symbol, Decimal(0))