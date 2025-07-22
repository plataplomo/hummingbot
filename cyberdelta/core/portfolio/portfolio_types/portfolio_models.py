"""Portfolio data models and type definitions."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade
from cyberdelta.core.portfolio.portfolio_types.domain_models import ErrorContext


def make_trade_list() -> list[Trade]:
    """Factory function for trades list."""
    return []


def make_position_list() -> list[DerivativePosition]:
    """Factory function for positions list."""
    return []


def make_order_list() -> list[Order]:
    """Factory function for orders list."""
    return []


def make_error_list() -> list[ErrorContext]:
    """Factory function for errors list."""
    return []


def make_balance_dict() -> dict[str, SpotBalance]:
    """Factory function for balances dict."""
    return {}


class PortfolioComponentType(Enum):
    """Types of portfolio components."""

    BALANCE_MANAGER = "balance_manager"
    POSITION_MANAGER = "position_manager"
    ORDER_MANAGER = "order_manager"
    PNL_CALCULATOR = "pnl_calculator"
    EXPOSURE_CALCULATOR = "exposure_calculator"
    MARGIN_MANAGER = "margin_manager"
    RISK_MANAGER = "risk_manager"
    PERSISTENCE_SERVICE = "persistence_service"
    EVENT_DISPATCHER = "event_dispatcher"


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class ComponentHealth(BaseModel):
    """Health status of a portfolio component."""

    model_config = ConfigDict(extra="forbid")

    component_type: PortfolioComponentType
    component_name: str
    status: HealthStatus
    last_update: float
    metrics: dict[str, object] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class PortfolioSnapshot(BaseModel):
    """Complete snapshot of portfolio state at a point in time."""

    model_config = ConfigDict(extra="forbid")

    snapshot_id: UUID = Field(default_factory=uuid4)
    timestamp: float = Field(default=0.0)

    # Account values
    total_account_value: Decimal = Decimal(0)
    total_collateral: Decimal = Decimal(0)
    free_collateral: Decimal = Decimal(0)

    # P&L metrics
    total_realized_pnl: Decimal = Decimal(0)
    total_unrealized_pnl: Decimal = Decimal(0)
    daily_pnl: Decimal = Decimal(0)

    # Exposure metrics
    gross_exposure: Decimal = Decimal(0)
    net_exposure: Decimal = Decimal(0)
    leverage: Decimal = Decimal(0)

    # Risk metrics
    portfolio_var_95: Decimal = Decimal(0)
    max_drawdown: Decimal = Decimal(0)
    sharpe_ratio: Decimal | None = None

    # Counts
    active_positions: int = 0
    open_orders: int = 0
    total_trades: int = 0

    # Breakdown by exchange
    exchange_summaries: dict[str, ExchangeSummary] = Field(default_factory=dict)

    # Currency exposures
    currency_exposures: dict[str, Decimal] = Field(default_factory=dict)

    # Component health
    component_health: dict[str, ComponentHealth] = Field(default_factory=dict)

    # Metadata
    metadata: dict[str, object] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "snapshot_id": str(self.snapshot_id),
            "timestamp": self.timestamp,
            "total_account_value": str(self.total_account_value),
            "total_collateral": str(self.total_collateral),
            "free_collateral": str(self.free_collateral),
            "total_realized_pnl": str(self.total_realized_pnl),
            "total_unrealized_pnl": str(self.total_unrealized_pnl),
            "daily_pnl": str(self.daily_pnl),
            "gross_exposure": str(self.gross_exposure),
            "net_exposure": str(self.net_exposure),
            "leverage": str(self.leverage),
            "portfolio_var_95": str(self.portfolio_var_95),
            "max_drawdown": str(self.max_drawdown),
            "sharpe_ratio": str(self.sharpe_ratio) if self.sharpe_ratio else None,
            "active_positions": self.active_positions,
            "open_orders": self.open_orders,
            "total_trades": self.total_trades,
            "exchange_summaries": {
                ex: summary.to_dict() for ex, summary in self.exchange_summaries.items()
            },
            "currency_exposures": {ccy: str(exp) for ccy, exp in self.currency_exposures.items()},
            "component_health": {
                name: {
                    "component_type": health.component_type.value,
                    "component_name": health.component_name,
                    "status": health.status.value,
                    "last_update": health.last_update,
                    "metrics": health.metrics,
                    "errors": health.errors,
                    "warnings": health.warnings,
                }
                for name, health in self.component_health.items()
            },
            "metadata": self.metadata,
        }


class ExchangeSummary(BaseModel):
    """Summary of portfolio state for a single exchange."""

    model_config = ConfigDict(extra="forbid")

    exchange_id: str
    account_value: Decimal = Decimal(0)
    collateral: Decimal = Decimal(0)
    margin_used: Decimal = Decimal(0)

    # P&L
    realized_pnl: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)

    # Positions
    position_count: int = 0
    long_positions: int = 0
    short_positions: int = 0

    # Orders
    open_orders: int = 0
    buy_orders: int = 0
    sell_orders: int = 0

    # Risk
    exposure: Decimal = Decimal(0)
    leverage: Decimal = Decimal(0)
    margin_ratio: Decimal = Decimal(0)

    # Balances by asset
    spot_balances: dict[str, Decimal] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "exchange_id": self.exchange_id,
            "account_value": str(self.account_value),
            "collateral": str(self.collateral),
            "margin_used": str(self.margin_used),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "position_count": self.position_count,
            "long_positions": self.long_positions,
            "short_positions": self.short_positions,
            "open_orders": self.open_orders,
            "buy_orders": self.buy_orders,
            "sell_orders": self.sell_orders,
            "exposure": str(self.exposure),
            "leverage": str(self.leverage),
            "margin_ratio": str(self.margin_ratio),
            "spot_balances": {asset: str(bal) for asset, bal in self.spot_balances.items()},
        }


class TradingSession(BaseModel):
    """Represents a trading session with performance metrics."""

    model_config = ConfigDict(extra="forbid")

    session_id: UUID = Field(default_factory=uuid4)
    start_time: float = Field(default=0.0)
    end_time: float | None = None

    # Starting values
    starting_account_value: Decimal = Decimal(0)
    starting_positions: int = 0

    # Current values
    current_account_value: Decimal = Decimal(0)
    current_positions: int = 0

    # Session metrics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0

    # P&L
    session_realized_pnl: Decimal = Decimal(0)
    session_unrealized_pnl: Decimal = Decimal(0)
    max_profit: Decimal = Decimal(0)
    max_loss: Decimal = Decimal(0)

    # Risk metrics
    max_exposure: Decimal = Decimal(0)
    max_leverage: Decimal = Decimal(0)
    max_drawdown: Decimal = Decimal(0)

    # Fee tracking
    total_fees_paid: Decimal = Decimal(0)
    fees_by_exchange: dict[str, Decimal] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "session_id": str(self.session_id),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "starting_account_value": str(self.starting_account_value),
            "starting_positions": self.starting_positions,
            "current_account_value": str(self.current_account_value),
            "current_positions": self.current_positions,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "session_realized_pnl": str(self.session_realized_pnl),
            "session_unrealized_pnl": str(self.session_unrealized_pnl),
            "max_profit": str(self.max_profit),
            "max_loss": str(self.max_loss),
            "max_exposure": str(self.max_exposure),
            "max_leverage": str(self.max_leverage),
            "max_drawdown": str(self.max_drawdown),
            "total_fees_paid": str(self.total_fees_paid),
            "fees_by_exchange": {ex: str(fee) for ex, fee in self.fees_by_exchange.items()},
        }

    @property
    def win_rate(self) -> Decimal:
        """Calculate win rate."""
        if self.total_trades == 0:
            return Decimal(0)
        return Decimal(self.winning_trades) / Decimal(self.total_trades)

    @property
    def session_duration(self) -> float:
        """Get session duration in seconds."""
        if self.end_time is None:
            return 0.0
        return self.end_time - self.start_time

    @property
    def net_pnl(self) -> Decimal:
        """Calculate net P&L after fees."""
        return self.session_realized_pnl - self.total_fees_paid


class PortfolioUpdate(BaseModel):
    """Update message for portfolio state changes."""

    model_config = ConfigDict(extra="forbid")

    update_id: UUID = Field(default_factory=uuid4)
    timestamp: float = Field(default=0.0)
    update_type: str = ""
    exchange_id: str | None = None

    # Update data
    trades: list[Trade] = Field(default_factory=make_trade_list)
    balances: dict[str, SpotBalance] = Field(default_factory=make_balance_dict)
    positions: list[DerivativePosition] = Field(default_factory=make_position_list)
    orders: list[Order] = Field(default_factory=make_order_list)

    # Metrics update
    metrics: dict[str, float | int | str] = Field(default_factory=dict)

    # Error information
    errors: list[ErrorContext] = Field(default_factory=make_error_list)

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "update_id": str(self.update_id),
            "timestamp": self.timestamp,
            "update_type": self.update_type,
            "exchange_id": self.exchange_id,
            "trades": [t.model_dump() for t in self.trades],
            "balances": {
                k: v.model_dump()
                for k, v in self.balances.items()
            },
            "positions": [
                p.model_dump() for p in self.positions
            ],
            "orders": [o.model_dump() for o in self.orders],
            "metrics": self.metrics,
            "errors": self.errors,
        }


class RiskParameters(BaseModel):
    """Risk management parameters."""

    model_config = ConfigDict(extra="forbid")

    # Position limits
    max_position_size: Decimal | None = None
    max_position_count: int | None = None
    max_concentration_single: Decimal = Decimal("0.3")  # 30%

    # Exposure limits
    max_gross_exposure: Decimal | None = None
    max_net_exposure: Decimal | None = None
    max_leverage: Decimal = Decimal(10)

    # Loss limits
    max_daily_loss: Decimal | None = None
    max_drawdown: Decimal = Decimal("0.2")  # 20%
    stop_loss_percent: Decimal = Decimal("0.05")  # 5%

    # Risk metrics
    max_var_95: Decimal | None = None
    max_margin_usage: Decimal = Decimal("0.8")  # 80%

    # Currency limits
    max_currency_exposure: Decimal = Decimal("0.5")  # 50% in any single currency

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "max_position_size": str(self.max_position_size) if self.max_position_size else None,
            "max_position_count": self.max_position_count,
            "max_concentration_single": str(self.max_concentration_single),
            "max_gross_exposure": str(self.max_gross_exposure) if self.max_gross_exposure else None,
            "max_net_exposure": str(self.max_net_exposure) if self.max_net_exposure else None,
            "max_leverage": str(self.max_leverage),
            "max_daily_loss": str(self.max_daily_loss) if self.max_daily_loss else None,
            "max_drawdown": str(self.max_drawdown),
            "stop_loss_percent": str(self.stop_loss_percent),
            "max_var_95": str(self.max_var_95) if self.max_var_95 else None,
            "max_margin_usage": str(self.max_margin_usage),
            "max_currency_exposure": str(self.max_currency_exposure),
        }


class PerformanceMetrics(BaseModel):
    """Portfolio performance metrics."""

    model_config = ConfigDict(extra="forbid")

    # Returns
    total_return: Decimal = Decimal(0)
    daily_return: Decimal = Decimal(0)
    monthly_return: Decimal = Decimal(0)
    annual_return: Decimal = Decimal(0)

    # Volatility
    daily_volatility: Decimal = Decimal(0)
    monthly_volatility: Decimal = Decimal(0)
    annual_volatility: Decimal = Decimal(0)

    # Risk-adjusted returns
    sharpe_ratio: Decimal = Decimal(0)
    sortino_ratio: Decimal = Decimal(0)
    calmar_ratio: Decimal = Decimal(0)

    # Drawdown
    max_drawdown: Decimal = Decimal(0)
    current_drawdown: Decimal = Decimal(0)
    drawdown_duration: float = 0.0  # seconds

    # Other metrics
    win_rate: Decimal = Decimal(0)
    profit_factor: Decimal = Decimal(0)
    average_win: Decimal = Decimal(0)
    average_loss: Decimal = Decimal(0)

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary."""
        return {
            "total_return": str(self.total_return),
            "daily_return": str(self.daily_return),
            "monthly_return": str(self.monthly_return),
            "annual_return": str(self.annual_return),
            "daily_volatility": str(self.daily_volatility),
            "monthly_volatility": str(self.monthly_volatility),
            "annual_volatility": str(self.annual_volatility),
            "sharpe_ratio": str(self.sharpe_ratio),
            "sortino_ratio": str(self.sortino_ratio),
            "calmar_ratio": str(self.calmar_ratio),
            "max_drawdown": str(self.max_drawdown),
            "current_drawdown": str(self.current_drawdown),
            "drawdown_duration": self.drawdown_duration,
            "win_rate": str(self.win_rate),
            "profit_factor": str(self.profit_factor),
            "average_win": str(self.average_win),
            "average_loss": str(self.average_loss),
        }
