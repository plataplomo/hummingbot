"""Pydantic models for portfolio state data."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from cyberdelta.core.models import Order, SpotBalance, Trade

from .base import BaseStateModel, ValidationResult


# Typed factory functions to avoid Unknown type inference
def _trades_factory() -> list[Trade]:
    """Factory function for list[Trade].
    
    Returns:
        list[Trade]: Empty list of trades.
    """
    return []


def _orders_factory() -> list[Order]:
    """Factory function for list[Order].
    
    Returns:
        list[Order]: Empty list of orders.
    """
    return []


class ComponentHealthData(BaseModel):
    """Pydantic model for component health data."""

    component_type: str = Field(..., description="Type of portfolio component")
    component_name: str = Field(..., description="Name of the component")
    status: str = Field(..., description="Health status")
    last_update: float = Field(..., description="Timestamp of last health update")
    metrics: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Component metrics"
    )
    errors: list[str] = Field(default_factory=list, description="List of component errors")
    warnings: list[str] = Field(default_factory=list, description="List of component warnings")


class ExchangeSummaryData(BaseModel):
    """Pydantic model for exchange summary data."""

    exchange_id: str = Field(..., description="Exchange identifier")
    account_value: Decimal = Field(
        default=Decimal(0), description="Total account value on exchange"
    )
    collateral: Decimal = Field(default=Decimal(0), description="Available collateral")
    margin_used: Decimal = Field(default=Decimal(0), description="Margin currently in use")

    # P&L
    realized_pnl: Decimal = Field(default=Decimal(0), description="Realized profit and loss")
    unrealized_pnl: Decimal = Field(default=Decimal(0), description="Unrealized profit and loss")

    # Positions
    position_count: int = Field(default=0, description="Total number of positions")
    long_positions: int = Field(default=0, description="Number of long positions")
    short_positions: int = Field(default=0, description="Number of short positions")

    # Orders
    open_orders: int = Field(default=0, description="Number of open orders")
    buy_orders: int = Field(default=0, description="Number of buy orders")
    sell_orders: int = Field(default=0, description="Number of sell orders")

    # Risk
    exposure: Decimal = Field(default=Decimal(0), description="Total exposure")
    leverage: Decimal = Field(default=Decimal(0), description="Current leverage")
    margin_ratio: Decimal = Field(default=Decimal(0), description="Margin utilization ratio")

    # Balances by asset
    spot_balances: dict[str, Decimal] = Field(
        default_factory=dict, description="Spot balances by asset"
    )


class PortfolioStateData(BaseStateModel):
    """Pydantic model for complete portfolio state data.

    This replaces all dict[str, Any] and dict[str, object] patterns
    with strongly typed, validated Pydantic models.
    """

    # Portfolio identification
    portfolio_id: str = Field(..., description="Unique portfolio identifier")

    # Account values
    total_account_value: Decimal = Field(
        default=Decimal(0), description="Total account value across all exchanges"
    )
    total_collateral: Decimal = Field(default=Decimal(0), description="Total available collateral")
    free_collateral: Decimal = Field(
        default=Decimal(0), description="Free collateral available for trading"
    )

    # P&L metrics
    total_realized_pnl: Decimal = Field(
        default=Decimal(0), description="Total realized profit and loss"
    )
    total_unrealized_pnl: Decimal = Field(
        default=Decimal(0), description="Total unrealized profit and loss"
    )
    daily_pnl: Decimal = Field(default=Decimal(0), description="Daily profit and loss")

    # Exposure metrics
    gross_exposure: Decimal = Field(
        default=Decimal(0), description="Gross exposure across all positions"
    )
    net_exposure: Decimal = Field(
        default=Decimal(0), description="Net exposure across all positions"
    )
    leverage: Decimal = Field(default=Decimal(0), description="Portfolio leverage")

    # Risk metrics
    portfolio_var_95: Decimal = Field(default=Decimal(0), description="95% Value at Risk")
    max_drawdown: Decimal = Field(default=Decimal(0), description="Maximum drawdown")
    sharpe_ratio: Decimal | None = Field(default=None, description="Sharpe ratio")

    # Counts
    active_positions: int = Field(default=0, description="Number of active positions")
    open_orders: int = Field(default=0, description="Number of open orders")
    total_trades: int = Field(default=0, description="Total number of trades")

    # Breakdown by exchange
    exchange_summaries: dict[str, ExchangeSummaryData] = Field(
        default_factory=dict, description="Summary data by exchange"
    )

    # Currency exposures
    currency_exposures: dict[str, Decimal] = Field(
        default_factory=dict, description="Exposure by currency"
    )

    # Component health
    component_health: dict[str, ComponentHealthData] = Field(
        default_factory=dict, description="Health status of portfolio components"
    )

    # Strongly typed metadata (no dict[str, Any])
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Portfolio metadata"
    )

    # Historical data
    balances: dict[str, SpotBalance] = Field(
        default_factory=dict, description="Current spot balances"
    )
    trades: list[Trade] = Field(default_factory=_trades_factory, description="Recent trades")
    orders: list[Order] = Field(default_factory=_orders_factory, description="Current orders")


class TradingSessionData(BaseStateModel):
    """Pydantic model for trading session data."""

    session_id: UUID = Field(default_factory=uuid4, description="Unique session identifier")
    start_time: float = Field(..., description="Session start timestamp")
    end_time: float | None = Field(default=None, description="Session end timestamp")

    # Starting values
    starting_account_value: Decimal = Field(
        default=Decimal(0), description="Account value at session start"
    )
    starting_positions: int = Field(default=0, description="Position count at session start")

    # Current values
    current_account_value: Decimal = Field(default=Decimal(0), description="Current account value")
    current_positions: int = Field(default=0, description="Current position count")

    # Session metrics
    total_trades: int = Field(default=0, description="Total trades in session")
    winning_trades: int = Field(default=0, description="Number of winning trades")
    losing_trades: int = Field(default=0, description="Number of losing trades")

    # P&L
    session_realized_pnl: Decimal = Field(
        default=Decimal(0), description="Realized P&L for session"
    )
    session_unrealized_pnl: Decimal = Field(
        default=Decimal(0), description="Unrealized P&L for session"
    )
    max_profit: Decimal = Field(default=Decimal(0), description="Maximum profit reached")
    max_loss: Decimal = Field(default=Decimal(0), description="Maximum loss reached")

    # Risk metrics
    max_exposure: Decimal = Field(default=Decimal(0), description="Maximum exposure reached")
    max_leverage: Decimal = Field(default=Decimal(0), description="Maximum leverage used")
    max_drawdown: Decimal = Field(default=Decimal(0), description="Maximum drawdown in session")

    # Fee tracking
    total_fees_paid: Decimal = Field(default=Decimal(0), description="Total fees paid in session")
    fees_by_exchange: dict[str, Decimal] = Field(
        default_factory=dict, description="Fees paid by exchange"
    )

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


class PortfolioSummary(BaseModel):
    """Type-safe portfolio summary data.

    Clean break: No dict[str, Any] - explicit Pydantic model.
    """

    portfolio_id: str = Field(..., description="Portfolio identifier")
    total_value: Decimal = Field(..., description="Total portfolio value")
    net_pnl: Decimal = Field(..., description="Net profit and loss")
    leverage: Decimal = Field(..., description="Current leverage")
    active_positions: int = Field(..., description="Number of active positions")
    open_orders: int = Field(..., description="Number of open orders")
    is_healthy: bool = Field(..., description="Portfolio health status")
    exchange_count: int = Field(..., description="Number of connected exchanges")
    last_updated: datetime = Field(..., description="Last update timestamp")


class PortfolioState(PortfolioStateData):
    """Complete portfolio state combining data validation and behavioral protocols.

    This class combines:
    - PortfolioStateData: Pydantic model for data validation and serialization
    - StateStorable: Protocol for state persistence capabilities
    - Validatable: Protocol for comprehensive state validation

    Features:
    - Full type safety with Pydantic validation
    - Protocol compliance for behavioral contracts
    - Rich state management capabilities
    - Comprehensive validation logic
    """

    @property
    def state_key(self) -> str:
        """Generate unique state key for this portfolio state.

        Returns:
            Unique identifier for state storage
        """
        return f"portfolio_state_{self.portfolio_id}_{self.state_id}"

    async def validate_state(self) -> ValidationResult:
        """Comprehensive validation of portfolio state.

        Returns:
            ValidationResult with detailed validation status
        """
        result = ValidationResult(valid=True)

        # Run all validation checks
        validation_checks = [
            self._validate_basic_fields(),
            self._validate_financial_metrics(),
            self._validate_counts(),
            self._validate_risk_metrics(),
            self._validate_exchange_summaries(),
            self._validate_component_health(),
            self._validate_cross_consistency(),
        ]

        # Merge results
        for check_result in validation_checks:
            if not check_result.valid:
                result.valid = False
            result.errors.extend(check_result.errors)
            result.warnings.extend(check_result.warnings)

        return result

    def _validate_basic_fields(self) -> ValidationResult:
        """Validate basic required fields.
        
        Returns:
            ValidationResult: Result indicating if basic fields are valid.
        """
        result = ValidationResult(valid=True)

        if not self.portfolio_id:
            result.add_error("Portfolio ID cannot be empty")

        if not self.state_id:
            result.add_error("State ID cannot be empty")

        return result

    def _validate_financial_metrics(self) -> ValidationResult:
        """Validate financial consistency.
        
        Returns:
            ValidationResult: Result indicating if financial metrics are consistent.
        """
        result = ValidationResult(valid=True)

        if self.total_account_value < Decimal(0):
            result.add_error("Total account value cannot be negative")

        if self.free_collateral > self.total_collateral:
            result.add_error("Free collateral cannot exceed total collateral")

        return result

    def _validate_counts(self) -> ValidationResult:
        """Validate position and order counts.
        
        Returns:
            ValidationResult: Result indicating if counts are valid.
        """
        result = ValidationResult(valid=True)

        if self.active_positions < 0:
            result.add_error("Active positions cannot be negative")

        if self.open_orders < 0:
            result.add_error("Open orders cannot be negative")

        if self.total_trades < 0:
            result.add_error("Total trades cannot be negative")

        return result

    def _validate_risk_metrics(self) -> ValidationResult:
        """Validate risk metrics.
        
        Returns:
            ValidationResult: Result indicating if risk metrics are within bounds.
        """
        result = ValidationResult(valid=True)

        if self.leverage < Decimal(0):
            result.add_error("Leverage cannot be negative")

        if self.leverage > Decimal(100):
            result.add_warning("Extremely high leverage detected")

        return result

    def _validate_exchange_summaries(self) -> ValidationResult:
        """Validate exchange summary data.
        
        Returns:
            ValidationResult: Result indicating if exchange summaries are valid.
        """
        result = ValidationResult(valid=True)

        if not self.exchange_summaries:
            result.add_warning("No exchange summaries available")
            return result

        total_balance = Decimal(0)
        total_pnl = Decimal(0)

        for exchange_id, summary in self.exchange_summaries.items():
            # Validate individual exchange
            if not exchange_id:
                result.add_error("Exchange ID cannot be empty")

            if summary.account_value < Decimal(0):
                result.add_warning(f"Negative account value on {exchange_id}")

            if summary.unrealized_pnl < Decimal(-100000):
                result.add_warning(f"Large unrealized loss on {exchange_id}")

            total_balance += summary.account_value
            total_pnl += summary.unrealized_pnl

        # Cross-exchange validation
        if abs(total_pnl) > total_balance * Decimal("0.5"):
            result.add_warning("PnL is more than 50% of total balance")

        return result

    def _validate_component_health(self) -> ValidationResult:
        """Validate component health data.
        
        Returns:
            ValidationResult: Result indicating if component health is acceptable.
        """
        result = ValidationResult(valid=True)

        if not self.component_health:
            result.add_warning("No component health data available")
            return result

        unhealthy_components: list[str] = []
        error_components: list[str] = []

        for component, health in self.component_health.items():
            if health.status not in {"healthy", "warning", "error"}:
                result.add_error(f"Invalid health status for {component}: {health.status}")

            if health.status == "error":
                unhealthy_components.append(component)

            if len(health.errors) > 0:
                error_components.append(component)

        if unhealthy_components:
            result.add_error(f"Components in error state: {', '.join(unhealthy_components)}")

        if error_components:
            result.add_warning(f"Components with errors: {', '.join(error_components)}")

        return result

    def _validate_cross_consistency(self) -> ValidationResult:
        """Validate cross-data consistency.
        
        Returns:
            ValidationResult: Result indicating if data is internally consistent.
        """
        result = ValidationResult(valid=True)

        # Check if portfolio totals match exchange summaries
        if self.exchange_summaries:
            exchange_total_value = sum(
                summary.account_value for summary in self.exchange_summaries.values()
            )

            value_diff = abs(exchange_total_value - self.total_account_value)
            if value_diff > Decimal(1000):  # $1000 tolerance
                result.add_warning("Mismatch between portfolio total and exchange summaries")

            exchange_total_positions = sum(
                summary.position_count for summary in self.exchange_summaries.values()
            )

            if exchange_total_positions != self.active_positions:
                result.add_warning("Mismatch between portfolio and exchange position counts")

        return result

    # Convenience methods for state management

    def get_exchange_summary(self, exchange_id: str) -> ExchangeSummaryData | None:
        """Get summary for a specific exchange.

        Args:
            exchange_id: Exchange identifier

        Returns:
            Exchange summary or None if not found
        """
        return self.exchange_summaries.get(exchange_id)

    def get_component_health(self, component: str) -> ComponentHealthData | None:
        """Get health data for a specific component.

        Args:
            component: Component name

        Returns:
            Component health data or None if not found
        """
        return self.component_health.get(component)

    async def is_healthy(self) -> bool:
        """Check if the entire portfolio state is healthy.

        Returns:
            True if all components are healthy and validation passes
        """
        # Check component health
        if not all(health.status == "healthy" for health in self.component_health.values()):
            return False

        # Check validation
        validation_result = await self.validate_state()
        return validation_result.valid

    def get_unhealthy_components(self) -> list[str]:
        """Get list of unhealthy components.

        Returns:
            List of component names that are not healthy
        """
        return [
            component
            for component, health in self.component_health.items()
            if health.status != "healthy"
        ]

    def get_net_pnl(self) -> Decimal:
        """Calculate net P&L (realized + unrealized).

        Returns:
            Net profit and loss
        """
        return self.total_realized_pnl + self.total_unrealized_pnl

    async def get_portfolio_summary(self) -> PortfolioSummary:
        """Get high-level portfolio summary.

        Returns:
            Type-safe PortfolioSummary model
        """
        return PortfolioSummary(
            portfolio_id=self.portfolio_id,
            total_value=self.total_account_value,
            net_pnl=self.get_net_pnl(),
            leverage=self.leverage,
            active_positions=self.active_positions,
            open_orders=self.open_orders,
            is_healthy=await self.is_healthy(),
            exchange_count=len(self.exchange_summaries),
            last_updated=self.updated_at,
        )


# Factory function for creating portfolio states
def create_portfolio_state(
    portfolio_id: str, initial_value: Decimal | None = None
) -> PortfolioState:
    """Create a new portfolio state with default values.

    Args:
        portfolio_id: Portfolio identifier
        initial_value: Optional initial portfolio value

    Returns:
        New PortfolioState instance with default data
    """
    value = initial_value or Decimal(0)

    return PortfolioState(
        state_id=f"portfolio_{portfolio_id}",
        portfolio_id=portfolio_id,
        total_account_value=value,
        total_collateral=value,
        free_collateral=value,
        exchange_summaries={},
        component_health={},
        metadata={},
    )
