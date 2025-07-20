"""Type definitions for portfolio calculations."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class CalculationMetadata(BaseModel):
    """Metadata for calculation results."""

    model_config = ConfigDict(frozen=True)

    calculation_timestamp: float = 0.0
    calculation_method: str = ""
    data_source: str = ""
    notes: str = ""


class RealizedPnLResult(BaseModel):
    """Result of realized P&L calculation."""

    model_config = ConfigDict(frozen=True)

    pnl: Decimal
    position_size_change: Decimal
    average_entry_price: Decimal | None
    calculation_method: str
    metadata: CalculationMetadata | None = None


class UnrealizedPnLResult(BaseModel):
    """Result of unrealized P&L calculation."""

    model_config = ConfigDict(frozen=True)

    pnl: Decimal
    current_price: Decimal
    entry_price: Decimal
    position_size: Decimal
    currency: str
    metadata: CalculationMetadata | None = None


class PortfolioUnrealizedPnLResult(BaseModel):
    """Result of portfolio-wide unrealized P&L calculation."""

    model_config = ConfigDict(frozen=True)

    total_pnl: Decimal
    currency: str
    position_results: list[UnrealizedPnLResult]
    metadata: CalculationMetadata | None = None


class ExposureResult(BaseModel):
    """Result of exposure calculation."""

    model_config = ConfigDict(frozen=True)

    long_exposure: Decimal
    short_exposure: Decimal
    net_exposure: Decimal
    gross_exposure: Decimal
    currency: str
    metadata: CalculationMetadata | None = None


class PortfolioExposureResult(BaseModel):
    """Result of portfolio-wide exposure calculation."""

    model_config = ConfigDict(frozen=True)

    total_long_exposure: Decimal
    total_short_exposure: Decimal
    total_net_exposure: Decimal
    total_gross_exposure: Decimal
    currency: str
    by_symbol: dict[str, ExposureResult]
    by_exchange: dict[str, ExposureResult]
    metadata: CalculationMetadata | None = None


class PerformanceMetrics(BaseModel):
    """Portfolio performance metrics."""

    model_config = ConfigDict(frozen=True)

    total_pnl: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_return_pct: Decimal
    sharpe_ratio: Decimal | None
    max_drawdown: Decimal | None
    currency: str
    calculation_timestamp: float
    metadata: CalculationMetadata | None = None


class DrawdownMetrics(BaseModel):
    """Drawdown calculation metrics."""

    model_config = ConfigDict(frozen=True)

    current_drawdown: Decimal
    max_drawdown: Decimal
    max_drawdown_duration_seconds: float | None
    recovery_factor: Decimal | None
    metadata: CalculationMetadata | None = None
