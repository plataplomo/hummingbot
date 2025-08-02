"""Type definitions for the clean trading engine."""

from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional, Any
from dataclasses import dataclass, field
from pydantic import BaseModel, ConfigDict, Field

# Import TradingSignal from clean_trading_engine since it's defined there
# Import PortfolioState from portfolio_types.models
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState


@dataclass
class SignalProcessingResult:
    """Result of processing a trading signal."""
    success: bool
    signal: Any  # TradingSignal type is defined in clean_trading_engine.py
    position_size: Optional[Decimal] = None
    risk_check_passed: bool = False
    execution_id: Optional[str] = None
    reason: Optional[str] = None
    risk_score: float = 0.0
    portfolio_impact: Optional[dict[str, Decimal]] = None


@dataclass
class ExecutionRequest:
    """Request to execute a trade."""
    signal: Any  # TradingSignal type
    position_size: Decimal
    risk_parameters: dict[str, Decimal]
    portfolio_context: "PortfolioContext"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PortfolioContext:
    """Portfolio context for decision making."""
    portfolio_state: PortfolioState
    total_capital: Decimal
    available_capital: Decimal
    total_exposure: Decimal
    risk_assessment: dict[str, Decimal]
    timestamp: datetime


class EngineStatus(BaseModel):
    """Engine operational status."""
    running: bool
    start_time: Optional[datetime]
    signals_processed: int
    successful_trades: int
    failed_trades: int
    active_orders: int
    total_volume: Decimal
    total_fees: Decimal
    avg_execution_time_ms: float
    risk_violations_count: int
    last_signal_time: Optional[datetime]
    components_status: dict[str, str]
    
    model_config = ConfigDict(extra="forbid")


class RiskCheckSummary(BaseModel):
    """Summary of risk checks performed."""
    total_checks: int = Field(ge=0)
    violations: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list) 
    highest_risk_score: float = Field(ge=0, le=100)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    model_config = ConfigDict(extra="forbid")