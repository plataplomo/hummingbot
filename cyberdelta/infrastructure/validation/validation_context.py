"""Validation context for unified validation framework.

This module provides the context object that carries all necessary data
for validation rules to make decisions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import TradingState


logger = get_logger(__name__)


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings
    from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
    from cyberdelta.models.market.market_snapshot import MarketSnapshot
    from cyberdelta.models.portfolio.state import PortfolioState


@dataclass
class ValidationContext:
    """Context object containing all data needed for validation rules.

    This context is built once per validation request and passed to all
    validation rules, providing a consistent view of the system state.

    Inspired by Nautilus Trader's validation context pattern, this ensures
    all rules work with the same snapshot of data.

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings
    - Uses typed models for all data
    - NO hardcoded values or assumptions
    - Immutable once created (dataclass frozen could be considered)
    """

    config: AppSettings
    """Application configuration containing all validation parameters."""

    exchange_config: ExchangeSpecificConfig | None
    """Exchange-specific configuration for the order's exchange."""

    market_snapshot: MarketSnapshot | None
    """Current market data snapshot for price/liquidity validation."""

    portfolio_state: PortfolioState | None
    """Current portfolio state for balance/position validation."""

    trading_state: TradingState
    """Current trading system state (ACTIVE, HALTED, REDUCING, RECONCILING)."""

    timestamp: datetime
    """Timestamp when this context was created for time-based validation."""

    is_reconciling: bool = False
    """Whether system is in reconciliation mode (may relax some checks)."""

    is_reduce_only: bool = False
    """Whether the order is reduce-only (closing positions only)."""

    # Note: Future enhancement fields could include:
    # - historical_fills: Recent fills for pattern detection
    # - pending_orders: Current pending orders for exposure calculation
    # - circuit_breaker_states: Circuit breaker states for safety validation

    def __post_init__(self) -> None:
        """Validate context after initialization.

        Ensures required fields are properly set based on trading state.
        """
        # In ACTIVE or REDUCING states, we should have market data
        if (
            self.trading_state in {TradingState.ACTIVE, TradingState.REDUCING}
            and self.market_snapshot is None
        ):
            # Log warning but don't fail - some rules may not need market data
            logger.warning(
                "validation_context_missing_market_data",
                trading_state=self.trading_state.value,
                timestamp=self.timestamp.isoformat(),
            )

        # Exchange config should always be available for the order's exchange
        if self.exchange_config is None:
            logger.warning(
                "validation_context_missing_exchange_config",
                trading_state=self.trading_state.value,
                timestamp=self.timestamp.isoformat(),
            )

    def has_market_data(self) -> bool:
        """Check if market data is available in context.

        Returns:
            True if market snapshot is available
        """
        return self.market_snapshot is not None

    def has_portfolio_state(self) -> bool:
        """Check if portfolio state is available in context.

        Returns:
            True if portfolio state is available
        """
        return self.portfolio_state is not None

    def has_exchange_config(self) -> bool:
        """Check if exchange configuration is available.

        Returns:
            True if exchange config is available
        """
        return self.exchange_config is not None

    def should_skip_balance_checks(self) -> bool:
        """Determine if balance checks should be skipped.

        Balance checks may be skipped during:
        - Reconciliation mode
        - Reduce-only orders (depending on configuration)

        Returns:
            True if balance checks should be skipped
        """
        if self.is_reconciling:
            return True

        # Reduce-only orders may skip balance checks if configured
        return self.is_reduce_only and self.config.validation.skip_balance_for_reduce_only

    def should_skip_risk_checks(self) -> bool:
        """Determine if risk checks should be skipped.

        Risk checks may be skipped during:
        - Reconciliation mode
        - System in HALTED state (no new risk)

        Returns:
            True if risk checks should be skipped
        """
        return self.is_reconciling or self.trading_state == TradingState.HALTED

    def get_max_price_deviation(self) -> float | None:
        """Get maximum allowed price deviation percentage.

        Returns:
            Max deviation percentage or None if not configured
        """
        if self.exchange_config:
            return self.exchange_config.max_price_deviation_pct
        return None

    def get_tick_size(self) -> float | None:
        """Get tick size for price precision validation.

        Returns:
            Tick size or None if not configured
        """
        if self.exchange_config:
            return self.exchange_config.tick_size
        return None

    def get_lot_size(self) -> float | None:
        """Get lot size for quantity precision validation.

        Returns:
            Lot size or None if not configured
        """
        if self.exchange_config:
            return self.exchange_config.lot_size
        return None
