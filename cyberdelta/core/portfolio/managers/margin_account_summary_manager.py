"""Manager for margin account summaries across exchanges."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.events import (
    ErrorData,
    ErrorOccurredEvent,
    EventPriority,
    EventType,
)
from cyberdelta.core.portfolio.events.base import BasePortfolioEvent, EventMetadata
from cyberdelta.core.portfolio.events.error_events import ErrorContext


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.events import EventDispatcher
    from cyberdelta.core.portfolio.services.cache.cache_service import MemoryCacheService

logger = get_logger(__name__)


# Generic interfaces for account data (exchange-agnostic)
class StandardAccountData(BaseModel):
    """Standardized account data format for all exchanges."""

    account_value: float = Field(default=0, description="Account value in USD")
    total_margin_used: float = Field(default=0, description="Total margin used")
    position_margin: float = Field(default=0, description="Position margin")
    order_margin: float = Field(default=0, description="Order margin")
    initial_margin_required: float = Field(default=0, description="Initial margin required")
    maintenance_margin_required: float = Field(default=0, description="Maintenance margin required")
    unrealized_pnl: float = Field(default=0, description="Unrealized PnL")
    realized_pnl: float = Field(default=0, description="Realized PnL")
    equity: float = Field(default=0, description="Account equity")
    margin_available: float = Field(default=0, description="Available margin")


class GenericExchangeAccountData(StandardAccountData):
    """Typed interface for generic exchange account data."""

    account_value: float = Field(default=0, description="Account value")
    used_margin: float = Field(default=0, description="Used margin")
    free_margin: float = Field(default=0, description="Free margin")
    unrealized_pnl: float = Field(default=0, description="Unrealized PnL")
    realized_pnl: float = Field(default=0, description="Realized PnL")


class MarginRequirement(BaseModel):
    """Margin requirement details."""

    initial_margin: Decimal
    maintenance_margin: Decimal
    available_margin: Decimal
    margin_ratio: Decimal
    liquidation_price: Decimal | None = None


class AccountSummary(BaseModel):
    """Complete margin account summary."""

    exchange_id: str
    account_value: Decimal
    total_collateral: Decimal
    free_collateral: Decimal
    used_margin: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    margin_requirement: MarginRequirement
    timestamp: float = Field(default_factory=time.time)

    # Additional fields for detailed tracking
    total_position_value: Decimal | None = None
    total_order_margin: Decimal | None = None
    leverage: Decimal | None = None
    health_score: Decimal | None = None  # 0-100 score

    def to_dict(self) -> dict[str, str | float | dict[str, str | None] | None]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of the account summary with string values.
        """
        return {
            "exchange_id": self.exchange_id,
            "account_value": str(self.account_value),
            "total_collateral": str(self.total_collateral),
            "free_collateral": str(self.free_collateral),
            "used_margin": str(self.used_margin),
            "unrealized_pnl": str(self.unrealized_pnl),
            "realized_pnl": str(self.realized_pnl),
            "margin_requirement": {
                "initial_margin": str(self.margin_requirement.initial_margin),
                "maintenance_margin": str(self.margin_requirement.maintenance_margin),
                "available_margin": str(self.margin_requirement.available_margin),
                "margin_ratio": str(self.margin_requirement.margin_ratio),
                "liquidation_price": str(self.margin_requirement.liquidation_price)
                if self.margin_requirement.liquidation_price
                else None,
            },
            "timestamp": self.timestamp,
            "total_position_value": str(self.total_position_value)
            if self.total_position_value
            else None,
            "total_order_margin": str(self.total_order_margin) if self.total_order_margin else None,
            "leverage": str(self.leverage) if self.leverage else None,
            "health_score": str(self.health_score) if self.health_score else None,
        }

    @property
    def margin_usage_percent(self) -> Decimal:
        """Calculate margin usage percentage."""
        if self.total_collateral == 0:
            return Decimal(0)
        return (self.used_margin / self.total_collateral) * 100

    @property
    def is_at_risk(self) -> bool:
        """Check if account is at risk of liquidation."""
        # Consider at risk if margin ratio > 80% or health score < 20
        if self.margin_requirement.margin_ratio > Decimal("0.8"):
            return True
        return bool(self.health_score is not None and self.health_score < Decimal(20))


class MarginAccountSummaryManager:
    """Manages margin account summaries across exchanges."""

    def __init__(
        self,
        event_dispatcher: EventDispatcher | None = None,
        cache_service: MemoryCacheService[str, AccountSummary] | None = None,
        health_check_interval: float = 60.0,  # seconds
        risk_threshold_margin_ratio: float = 0.7,
        critical_threshold_margin_ratio: float = 0.85,
    ) -> None:
        """Initialize margin account summary manager.

        Args:
            event_dispatcher: Optional event dispatcher
            cache_service: Optional cache service
            health_check_interval: Interval for health checks
            risk_threshold_margin_ratio: Ratio above which to trigger warnings
            critical_threshold_margin_ratio: Ratio above which to trigger critical alerts
        """
        self.event_dispatcher = event_dispatcher
        self.cache_service = cache_service
        self.health_check_interval = health_check_interval
        self.risk_threshold_margin_ratio = Decimal(str(risk_threshold_margin_ratio))
        self.critical_threshold_margin_ratio = Decimal(str(critical_threshold_margin_ratio))

        # Current summaries by exchange
        self._summaries: dict[str, AccountSummary] = {}

        # Historical summaries for trend analysis
        self._summary_history: dict[str, list[AccountSummary]] = {}
        self._max_history_size = 100

        # Last health check times
        self._last_health_check: dict[str, float] = {}

        logger.info(
            "margin_account_summary_manager_initialized",
            risk_threshold=float(self.risk_threshold_margin_ratio),
            critical_threshold=float(self.critical_threshold_margin_ratio),
        )

    async def update_account_summary(
        self, exchange_id: str, account_data: dict[str, object]
    ) -> AccountSummary:
        """Update account summary from exchange data.

        Args:
            exchange_id: Exchange identifier
            account_data: Raw account data from exchange

        Returns:
            Updated account summary

        Raises:
            ValueError: If account data format is invalid
            TypeError: If account data types are incorrect
            KeyError: If required account data fields are missing
            AttributeError: If account data structure is malformed
            ArithmeticError: If calculations fail due to invalid numeric values
        """
        try:
            # Convert exchange-specific data to standardized format
            standard_data = self._normalize_account_data(account_data, exchange_id)
            
            # Parse standardized data
            summary = self._parse_standard_data(standard_data, exchange_id)

            # Store current summary
            old_summary = self._summaries.get(exchange_id)
            self._summaries[exchange_id] = summary

            # Add to history
            if exchange_id not in self._summary_history:
                self._summary_history[exchange_id] = []

            self._summary_history[exchange_id].append(summary)
            if len(self._summary_history[exchange_id]) > self._max_history_size:
                self._summary_history[exchange_id].pop(0)

            # Cache if service available
            if self.cache_service:
                cache_key = f"margin_summary:{exchange_id}"
                await self.cache_service.set(cache_key, summary, ttl=300)

            # Check for risk conditions
            await self._check_risk_conditions(summary, old_summary)

            # Perform periodic health check
            await self._perform_health_check(exchange_id, summary)

            logger.info(
                "account_summary_updated",
                exchange_id=exchange_id,
                account_value=float(summary.account_value),
                margin_ratio=float(summary.margin_requirement.margin_ratio),
                is_at_risk=summary.is_at_risk,
            )
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception(
                "account_summary_update_failed",
                exchange_id=exchange_id,
                error_type=type(e).__name__,
            )
            raise
        else:
            return summary

    def _normalize_account_data(self, account_data: dict[str, Any], exchange_id: str) -> StandardAccountData:
        """Normalize exchange-specific account data to standard format.
        
        Args:
            account_data: Raw account data from exchange
            exchange_id: Exchange identifier
            
        Returns:
            Standardized account data
        """
        # Extract common fields with fallback logic
        try:
            # Handle nested data structures generically
            if "marginSummary" in account_data:
                # Nested margin summary structure
                margin_data = account_data["marginSummary"]
                return StandardAccountData(
                    account_value=margin_data.get("accountValue", 0),
                    total_margin_used=margin_data.get("totalMarginUsed", 0),
                    position_margin=margin_data.get("totalPositionMargin", 0),
                    order_margin=margin_data.get("totalOrderMargin", 0),
                    initial_margin_required=margin_data.get("totalInitialMarginRequired", 0),
                    maintenance_margin_required=margin_data.get("totalMaintenanceMarginRequired", 0),
                    unrealized_pnl=margin_data.get("totalUnrealizedPnl", 0),
                    realized_pnl=0,  # Not provided in margin summary
                    equity=margin_data.get("accountValue", 0),
                    margin_available=margin_data.get("accountValue", 0) - margin_data.get("totalMarginUsed", 0)
                )
            else:
                # Flat structure (generic format)
                return StandardAccountData(
                    account_value=account_data.get("accountValue", account_data.get("equity", 0)),
                    total_margin_used=account_data.get("margin_used", account_data.get("totalMarginUsed", 0)),
                    position_margin=account_data.get("position_margin", 0),
                    order_margin=account_data.get("order_margin", 0),
                    initial_margin_required=account_data.get("initial_margin", 0),
                    maintenance_margin_required=account_data.get("maintenance_margin", 0),
                    unrealized_pnl=account_data.get("unrealized_pnl", 0),
                    realized_pnl=account_data.get("realized_pnl", 0),
                    equity=account_data.get("equity", account_data.get("accountValue", 0)),
                    margin_available=account_data.get("margin_available", 0)
                )
        except Exception as e:
            self.logger.warning(f"Failed to normalize account data for {exchange_id}: {e}")
            return StandardAccountData()  # Return defaults
    
    def _parse_standard_data(self, data: StandardAccountData, exchange_id: str) -> AccountSummary:
        """Parse standardized account data format.

        Args:
            data: Standardized account data
            exchange_id: Exchange identifier for logging

        Returns:
            Parsed account summary
        """
        account_value = Decimal(data.account_value)
        total_collateral = Decimal(data.total_margin_used)

        # Calculate various components
        total_position_margin = Decimal(data.position_margin)
        total_order_margin = Decimal(data.order_margin)

        free_collateral = account_value - total_collateral

        # Build margin requirement
        margin_req = MarginRequirement(
            initial_margin=Decimal(data.initial_margin_required),
            maintenance_margin=Decimal(data.maintenance_margin_required),
            available_margin=free_collateral,
            margin_ratio=total_collateral / account_value if account_value > 0 else Decimal(0),
        )

        return AccountSummary(
            exchange_id=exchange_id,
            account_value=account_value,
            total_collateral=total_collateral,
            free_collateral=free_collateral,
            used_margin=total_position_margin + total_order_margin,
            unrealized_pnl=Decimal(data.unrealized_pnl),
            realized_pnl=Decimal(data.realized_pnl),
            margin_requirement=margin_req,
            total_position_value=None,  # Not provided in standardized interface
            total_order_margin=total_order_margin,
            leverage=None,  # Not provided in standardized interface
        )


    def _parse_generic_data(
        self, exchange_id: str, data: GenericExchangeAccountData
    ) -> AccountSummary:
        """Parse generic account data format.

        Args:
            exchange_id: Exchange identifier
            data: Validated generic exchange account data

        Returns:
            Parsed account summary for generic exchange
        """
        # Generic parsing for unknown exchanges with type safety
        account_value = Decimal(data.account_value)
        used_margin = Decimal(data.used_margin)
        free_margin = Decimal(data.free_margin)

        margin_req = MarginRequirement(
            initial_margin=used_margin,  # Use used margin as initial
            maintenance_margin=used_margin,  # Use used margin as maintenance
            available_margin=free_margin,
            margin_ratio=used_margin / account_value if account_value > 0 else Decimal(0),
        )

        return AccountSummary(
            exchange_id=exchange_id,
            account_value=account_value,
            total_collateral=account_value,
            free_collateral=free_margin,
            used_margin=used_margin,
            unrealized_pnl=Decimal(data.unrealized_pnl),
            realized_pnl=Decimal(data.realized_pnl),
            margin_requirement=margin_req,
        )

    async def _check_risk_conditions(
        self, current: AccountSummary, previous: AccountSummary | None
    ) -> None:
        """Check for risk conditions and emit events."""
        if not self.event_dispatcher:
            return

        margin_ratio = current.margin_requirement.margin_ratio

        # Check critical threshold
        if margin_ratio >= self.critical_threshold_margin_ratio:
            error_data = ErrorData(
                component="MarginAccountSummaryManager",
                error_type="CRITICAL_MARGIN_LEVEL",
                error_message=f"Critical margin level reached: {float(margin_ratio * 100):.2f}%",
                error_code="MARGIN_CRITICAL",
                context=ErrorContext(
                    operation="check_risk_conditions",
                    additional_info={
                        "exchange_id": current.exchange_id,
                        "margin_ratio": str(margin_ratio),
                        "account_value": str(current.account_value),
                        "used_margin": str(current.used_margin),
                    },
                ),
                recoverable=True,
            )

            event = ErrorOccurredEvent.create(
                error=error_data, severity=EventPriority.CRITICAL, exchange_id=current.exchange_id
            )

            await self.event_dispatcher.dispatch(event)

        # Check warning threshold
        elif margin_ratio >= self.risk_threshold_margin_ratio:
            # Create a risk warning event
            class RiskWarningEvent(BasePortfolioEvent[dict[str, str | float]]):
                def _serialize_data(self) -> dict[str, str | float]:
                    return self.data

            warning_event = RiskWarningEvent(
                event_type=EventType.RISK_LIMIT_WARNING,
                data={
                    "exchange_id": current.exchange_id,
                    "margin_ratio": float(margin_ratio),
                    "threshold": float(self.risk_threshold_margin_ratio),
                    "account_value": float(current.account_value),
                },
                metadata=EventMetadata(
                    exchange_id=current.exchange_id, priority=EventPriority.HIGH
                ),
            )

            await self.event_dispatcher.dispatch(warning_event)

        # Check for rapid deterioration
        if previous and previous.margin_requirement.margin_ratio > 0:
            ratio_change = margin_ratio - previous.margin_requirement.margin_ratio
            if ratio_change > Decimal("0.1"):  # 10% rapid increase
                logger.warning(
                    "rapid_margin_deterioration",
                    exchange_id=current.exchange_id,
                    previous_ratio=float(previous.margin_requirement.margin_ratio),
                    current_ratio=float(margin_ratio),
                    change=float(ratio_change),
                )

    async def _perform_health_check(self, exchange_id: str, summary: AccountSummary) -> None:
        """Perform periodic health check."""
        now = time.time()
        last_check = self._last_health_check.get(exchange_id, 0)

        if now - last_check < self.health_check_interval:
            return

        self._last_health_check[exchange_id] = now

        # Calculate health score (0-100)
        health_score = self._calculate_health_score(summary)
        summary.health_score = health_score

        logger.info(
            "account_health_check",
            exchange_id=exchange_id,
            health_score=float(health_score),
            margin_ratio=float(summary.margin_requirement.margin_ratio),
            is_at_risk=summary.is_at_risk,
        )

    def _calculate_health_score(self, summary: AccountSummary) -> Decimal:
        """Calculate account health score (0-100).

        Args:
            summary: Account summary to evaluate

        Returns:
            Health score between 0 and 100, where 100 is healthiest
        """
        score = Decimal(100)

        # Deduct based on margin ratio
        margin_ratio = summary.margin_requirement.margin_ratio
        if margin_ratio > Decimal("0.5"):
            score -= (margin_ratio - Decimal("0.5")) * 100

        # Deduct for negative unrealized PnL
        if summary.unrealized_pnl < 0 and summary.account_value > 0:
            pnl_impact = abs(summary.unrealized_pnl) / summary.account_value * 50
            score -= min(pnl_impact, Decimal(30))

        # Deduct for low free collateral
        if summary.total_collateral > 0:
            free_ratio = summary.free_collateral / summary.total_collateral
            if free_ratio < Decimal("0.2"):
                score -= (Decimal("0.2") - free_ratio) * 100

        return max(score, Decimal(0))

    def get_summary(self, exchange_id: str) -> AccountSummary | None:
        """Get current account summary for exchange.

        Args:
            exchange_id: Exchange identifier

        Returns:
            Current account summary for the exchange, or None if not found
        """
        return self._summaries.get(exchange_id)

    def get_all_summaries(self) -> dict[str, AccountSummary]:
        """Get all current account summaries.

        Returns:
            Dictionary mapping exchange IDs to their current account summaries
        """
        return self._summaries.copy()

    def get_summary_history(
        self, exchange_id: str, limit: int | None = None
    ) -> list[AccountSummary]:
        """Get historical summaries for exchange.

        Args:
            exchange_id: Exchange identifier
            limit: Maximum number of historical summaries to return

        Returns:
            List of historical account summaries, most recent last
        """
        history = self._summary_history.get(exchange_id, [])
        if limit:
            return history[-limit:]
        return history.copy()

    def get_aggregate_metrics(self) -> dict[str, float | int]:
        """Get aggregate metrics across all exchanges.

        Returns:
            Dictionary containing aggregated metrics across all exchanges
        """
        if not self._summaries:
            return {
                "total_account_value": 0,
                "total_used_margin": 0,
                "total_unrealized_pnl": 0,
                "average_margin_ratio": 0,
                "exchanges_at_risk": 0,
            }

        total_value = sum(s.account_value for s in self._summaries.values())
        total_margin = sum(s.used_margin for s in self._summaries.values())
        total_upnl = sum(s.unrealized_pnl for s in self._summaries.values())

        margin_ratios = [s.margin_requirement.margin_ratio for s in self._summaries.values()]
        avg_margin_ratio = sum(margin_ratios) / len(margin_ratios) if margin_ratios else Decimal(0)

        at_risk_count = sum(1 for s in self._summaries.values() if s.is_at_risk)

        return {
            "total_account_value": float(total_value),
            "total_used_margin": float(total_margin),
            "total_unrealized_pnl": float(total_upnl),
            "average_margin_ratio": float(avg_margin_ratio),
            "exchanges_at_risk": at_risk_count,
            "exchange_count": len(self._summaries),
        }

    async def close(self) -> None:
        """Clean up resources."""
        logger.info("margin_account_summary_manager_closing")
        self._summaries.clear()
        self._summary_history.clear()
        self._last_health_check.clear()
