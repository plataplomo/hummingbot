"""Update models for portfolio state management."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade


class BalanceUpdate(BaseModel):
    """Update model for balance changes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    exchange_id: str
    balances: dict[str, SpotBalance]
    timestamp: float
    reason: str | None = None


class PositionUpdate(BaseModel):
    """Update model for position changes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    exchange_id: str
    positions: list[DerivativePosition]
    timestamp: float
    reason: str | None = None


class OrderUpdate(BaseModel):
    """Update model for order changes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    exchange_id: str
    orders: list[Order]
    timestamp: float
    reason: str | None = None


class TradeUpdate(BaseModel):
    """Update model for trade execution."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    trade: Trade
    timestamp: float
    impact: TradeImpact | None = None


class TradeImpact(BaseModel):
    """Impact of a trade on portfolio state."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    balance_changes: dict[str, Decimal] = Field(default_factory=dict)
    position_changes: dict[str, Decimal] = Field(default_factory=dict)
    realized_pnl: Decimal = Decimal(0)
    fees_paid: Decimal = Decimal(0)


class CapitalSummary(BaseModel):
    """Summary of total capital across exchanges."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total_capital: Decimal
    free_capital: Decimal
    used_capital: Decimal
    capital_by_exchange: dict[str, Decimal]
    capital_by_currency: dict[str, Decimal]
    timestamp: float


class PnLSummary(BaseModel):
    """Summary of P&L metrics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total_realized_pnl: Decimal
    total_unrealized_pnl: Decimal
    daily_realized_pnl: Decimal
    daily_unrealized_pnl: Decimal
    pnl_by_exchange: dict[str, Decimal]
    pnl_by_symbol: dict[str, Decimal]
    timestamp: float


class ExposureMetrics(BaseModel):
    """Portfolio exposure metrics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    gross_exposure: Decimal
    net_exposure: Decimal
    long_exposure: Decimal
    short_exposure: Decimal
    exposure_by_symbol: dict[str, Decimal]
    exposure_by_exchange: dict[str, Decimal]
    leverage: Decimal
    timestamp: float


class PortfolioSummary(BaseModel):
    """Comprehensive portfolio summary."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total_value: Decimal
    capital_summary: CapitalSummary
    pnl_summary: PnLSummary
    exposure_metrics: ExposureMetrics
    position_count: int
    order_count: int
    trade_count: int
    health_status: str
    timestamp: float


class ManagerStats(BaseModel):
    """Statistics for portfolio managers."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    manager_name: str
    processed_items: int
    error_count: int
    last_update: float
    performance_metrics: dict[str, float]
    metadata: dict[str, Any] = Field(default_factory=dict)


class StateValidationResult(BaseModel):
    """Result of state validation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    validation_time: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class StateBackup(BaseModel):
    """State backup data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    backup_id: str
    timestamp: float
    state_data: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)
    compressed: bool = False
    checksum: str | None = None
