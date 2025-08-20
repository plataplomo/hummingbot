"""Performance metrics data models.

This module contains the data models for performance tracking metrics.
Separated from the main tracker to follow the 600-line file limit.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class PerformanceMetrics(BaseModel):
    """Comprehensive performance metrics report.

    All metrics are calculated based on configuration settings and include
    only the metrics explicitly enabled in config.calculation.performance_metrics.
    """

    # Basic metrics
    total_pnl: Decimal
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    total_return_pct: Decimal

    # Period metrics (if enabled)
    daily_return_pct: Decimal | None = None
    weekly_return_pct: Decimal | None = None
    monthly_return_pct: Decimal | None = None
    yearly_return_pct: Decimal | None = None

    # Risk metrics (if enabled)
    sharpe_ratio: Decimal | None = None
    sortino_ratio: Decimal | None = None
    max_drawdown_pct: Decimal | None = None
    max_drawdown_duration_days: int | None = None
    current_drawdown_pct: Decimal | None = None

    # Trading metrics (if enabled)
    total_trades: int | None = None
    winning_trades: int | None = None
    losing_trades: int | None = None
    win_rate_pct: Decimal | None = None
    average_win: Decimal | None = None
    average_loss: Decimal | None = None
    profit_factor: Decimal | None = None

    # Statistical metrics (if enabled)
    volatility_pct: Decimal | None = None
    beta: Decimal | None = None
    alpha: Decimal | None = None

    # Metadata
    calculation_timestamp: datetime
    period_start: datetime
    period_end: datetime
    base_currency: str
