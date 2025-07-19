"""Specific Pydantic models for portfolio data to replace dict[str, Any] usage."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.models import DerivativePosition, Order, SpotBalance


# Type alias for issue dictionary
IssueDict = dict[str, str | int | float | bool]


# Typed factory functions to avoid Unknown type inference
def _issue_list_factory() -> list[IssueDict]:
    """Factory function for IssueDict list."""
    return []


class PortfolioMetrics(BaseModel):
    """Portfolio metrics data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Basic metrics
    total_value_usd: Decimal = Field(default=Decimal(0))
    unrealized_pnl: Decimal = Field(default=Decimal(0))
    realized_pnl: Decimal = Field(default=Decimal(0))

    # Exposure metrics
    long_exposure: Decimal = Field(default=Decimal(0))
    short_exposure: Decimal = Field(default=Decimal(0))
    net_exposure: Decimal = Field(default=Decimal(0))

    # Risk metrics
    leverage: Decimal = Field(default=Decimal(1))
    margin_ratio: Decimal = Field(default=Decimal(0))

    # Statistics
    active_positions: int = Field(default=0)
    total_orders: int = Field(default=0)
    last_update_timestamp: float = Field(default=0.0)


class ExchangeBalances(BaseModel):
    """Exchange-specific balance data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_name: str
    balances: dict[str, SpotBalance] = Field(default_factory=dict)
    last_update: float = Field(default=0.0)
    is_stale: bool = Field(default=False)


class ExchangePositions(BaseModel):
    """Exchange-specific position data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_name: str
    positions: dict[str, DerivativePosition] = Field(default_factory=dict)
    last_update: float = Field(default=0.0)
    is_stale: bool = Field(default=False)


class ExchangeOrders(BaseModel):
    """Exchange-specific order data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_name: str
    orders: dict[str, Order] = Field(default_factory=dict)
    last_update: float = Field(default=0.0)
    is_stale: bool = Field(default=False)


class PortfolioState(BaseModel):
    """Complete portfolio state data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Exchange data
    balances: dict[str, ExchangeBalances] = Field(default_factory=dict)
    positions: dict[str, ExchangePositions] = Field(default_factory=dict)
    orders: dict[str, ExchangeOrders] = Field(default_factory=dict)

    # Aggregated metrics
    metrics: PortfolioMetrics = Field(default_factory=PortfolioMetrics)

    # State metadata
    version: int = Field(default=0)
    last_update: float = Field(default=0.0)
    is_consistent: bool = Field(default=True)


class ValidationStatistics(BaseModel):
    """Validation statistics data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    stats: dict[str, dict[str, int]] = Field(default_factory=dict)
    recent_issues: list[IssueDict] = Field(default_factory=_issue_list_factory)
    timestamp: float = Field(default=0.0)


class CacheStatistics(BaseModel):
    """Cache statistics data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Basic stats
    hits: int = Field(default=0)
    misses: int = Field(default=0)
    evictions: int = Field(default=0)
    expirations: int = Field(default=0)
    sets: int = Field(default=0)
    deletes: int = Field(default=0)

    # Derived metrics
    cache_size: int = Field(default=0)
    max_size: int = Field(default=0)
    hit_rate: float = Field(default=0.0)
    fill_ratio: float = Field(default=0.0)
    memory_usage_estimate: int = Field(default=0)


class BalanceUpdateRequest(BaseModel):
    """Balance update request data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_name: str
    balances: dict[str, SpotBalance]
    force_update: bool = Field(default=False)
    update_source: str = Field(default="unknown")


class PositionUpdateRequest(BaseModel):
    """Position update request data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_name: str
    positions: list[DerivativePosition]
    force_update: bool = Field(default=False)
    update_source: str = Field(default="unknown")


class OrderUpdateRequest(BaseModel):
    """Order update request data model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_name: str
    orders: list[Order]
    force_update: bool = Field(default=False)
    update_source: str = Field(default="unknown")
