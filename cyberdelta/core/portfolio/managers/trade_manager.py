"""Trade manager with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Trade
from cyberdelta.core.portfolio.base import StateUpdate, TypedStateManager
from cyberdelta.core.portfolio.base.typed_state_manager import StateManagerResult
from cyberdelta.core.portfolio.portfolio_types.state_types import StateValidationResult
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.protocols import (
        MetricsCollectorProtocol,
        StateContainerProtocol,
    )
    from cyberdelta.core.portfolio.protocols.validation import ValidationServiceProtocol

# Trade state type - list of trades
TradeState = list[Trade]

logger = get_logger(__name__)


class TradeManager(TypedStateManager[TradeState]):
    """Trade manager with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Inherits from TypedStateManager
    - Protocol-based dependencies
    - Strong typing with result types
    - Comprehensive trade history tracking
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[Any],
        validation_service: ValidationServiceProtocol | None = None,
        metrics_collector: MetricsCollectorProtocol | None = None,
    ) -> None:
        """Initialize the trade manager.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for trade data
            validation_service: Optional validation service
            metrics_collector: Optional metrics collector
        """
        super().__init__(app_settings, state_container, "TradeManager")

        self.validation_service = validation_service
        self.metrics_collector = metrics_collector

        # Configuration from AppSettings
        self.max_trade_history = self.portfolio_config.state.max_trade_history_size
        self.strict_validation = self.portfolio_config.validation.strict_mode

        self.logger.info(
            "trade_manager_created",
            max_trade_history=self.max_trade_history,
            strict_validation=self.strict_validation,
        )

    async def get_state(self) -> TradeState:
        """Get current trade state.

        Returns:
            Current trade state
        """
        # Trade state is not directly available from state container
        # Return empty list for now as trade history is not persisted in state container
        return []

    async def update_state(self, update: StateUpdate[TradeState]) -> StateManagerResult:
        """Update trade state.

        Args:
            update: State update with trade data

        Returns:
            Update result
        """
        try:
            # For each trade in the update, add it via the state container
            for trade in update.data:
                # Get exchange from trade object
                exchange_str = getattr(trade, "exchange_id", getattr(trade, "exchange", "unknown"))

                # Convert string to ExchangeName enum
                try:
                    exchange = (
                        ExchangeName(exchange_str)
                        if isinstance(exchange_str, str)
                        else exchange_str
                    )
                except (ValueError, AttributeError):
                    # Skip trade if we can't determine exchange
                    logger.warning(
                        "trade_exchange_conversion_failed",
                        trade_exchange=getattr(trade, "exchange", "unknown"),
                        trade_id=getattr(trade, "id", "unknown"),
                    )
                    continue

                self.state_container.add_trade(exchange, trade)

                # Record metrics if available
                if self.metrics_collector:
                    # MetricsCollectorProtocol guarantees record_state_update method
                    self.metrics_collector.record_state_update(
                        "trade_history_update", exchange, "completed"
                    )

            return StateManagerResult.success_result(
                message="Trade state updated",
                data=update.data,
            )

        except (ValueError, TypeError, AttributeError, KeyError) as e:
            return StateManagerResult.failure_result(
                message=f"Trade update failed: {e}",
                errors=[str(e)],
            )

    async def validate_state(self, state: TradeState) -> StateValidationResult:
        """Validate trade state.

        Args:
            state: Trade state to validate

        Returns:
            Validation result
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Basic validation
        self._validate_trade_history_size(state, warnings)

        # Validate individual trades
        self._validate_individual_trades(state, errors)

        # Use validation service if available
        await self._validate_with_service(state, errors, warnings)

        return StateValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            metadata={"trade_count": len(state)},
        )

    def _validate_trade_history_size(self, state: TradeState, warnings: list[str]) -> None:
        """Validate trade history size."""
        if len(state) > self.max_trade_history:
            warnings.append(
                f"Trade history size ({len(state)}) exceeds maximum ({self.max_trade_history})"
            )

    def _validate_individual_trades(self, state: TradeState, errors: list[str]) -> None:
        """Validate individual trade data."""
        for trade in state:
            if trade.price <= 0:
                errors.append(f"Invalid trade price: {trade.price} for trade {trade.id}")

            if trade.quantity <= 0:
                errors.append(f"Invalid trade quantity: {trade.quantity} for trade {trade.id}")

    async def _validate_with_service(
        self, state: TradeState, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate trades using external validation service if available."""
        if not self.validation_service:
            # ValidationServiceProtocol guarantees validate_trade method
            return

        try:
            # Validate each trade individually
            for trade in state:
                trade_result = await self.validation_service.validate_trade(trade)
                # ValidationServiceProtocol guarantees ValidationResult return type
                if not trade_result.is_valid:
                    # ValidationResult has typed errors and warnings lists
                    errors.extend(trade_result.errors)
                    warnings.extend(trade_result.warnings)
        except (ValueError, TypeError, AttributeError, KeyError) as e:
            if self.strict_validation:
                errors.append(f"Validation service error: {e}")
            else:
                warnings.append(f"Validation service error: {e}")

    async def add_trade(self, trade: Trade) -> StateManagerResult:
        """Add a new trade to the history.

        Args:
            trade: Trade to add

        Returns:
            Update result
        """
        try:
            # Get current trade history
            current_trades = await self.get_state()

            # Add new trade
            updated_trades = [*current_trades, trade]

            # Trim if exceeds maximum
            if len(updated_trades) > self.max_trade_history:
                updated_trades = updated_trades[-self.max_trade_history :]

            # Create state update
            update = StateUpdate(
                data=updated_trades,
                timestamp=datetime.now(UTC),
                source=f"add_trade_{trade.id}",
                metadata={
                    "operation": "add_trade",
                    "trade_id": trade.id,
                    "exchange": trade.exchange,
                    "symbol": trade.symbol,
                },
            )

            # Apply update with validation
            return await self.update_with_validation(update)

        except (ValueError, TypeError, AttributeError, KeyError) as e:
            self.logger.exception("add_trade_failed", trade_id=trade.id)
            return StateManagerResult.failure_result(
                message=f"Add trade failed: {e}",
                errors=[str(e)],
            )

    async def get_trades_by_exchange(self, exchange: ExchangeName) -> list[Trade]:
        """Get trades for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            List of trades for the exchange
        """
        try:
            all_trades = await self.get_state()
            return [trade for trade in all_trades if trade.exchange == exchange]
        except Exception:
            self.logger.exception("get_trades_by_exchange_failed", exchange=exchange)
            return []

    async def get_trades_by_symbol(self, symbol: str) -> list[Trade]:
        """Get trades for a specific symbol.

        Args:
            symbol: Trading symbol

        Returns:
            List of trades for the symbol
        """
        try:
            all_trades = await self.get_state()
            return [trade for trade in all_trades if trade.symbol == symbol]
        except Exception:
            self.logger.exception("get_trades_by_symbol_failed", symbol=symbol)
            return []

    async def get_recent_trades(self, limit: int = 100) -> list[Trade]:
        """Get most recent trades.

        Args:
            limit: Maximum number of trades to return

        Returns:
            List of recent trades
        """
        try:
            all_trades = await self.get_state()
            # Sort by executed_at (newest first) and limit
            sorted_trades = sorted(all_trades, key=lambda t: t.executed_at, reverse=True)
            return sorted_trades[:limit]
        except Exception:
            self.logger.exception("get_recent_trades_failed", limit=limit)
            return []

    async def get_trade_summary(self) -> dict[str, Any]:
        """Get trade history summary.

        Returns:
            Dictionary with trade history statistics
        """
        try:
            all_trades = await self.get_state()

            if not all_trades:
                return {
                    "total_trades": 0,
                    "exchanges": [],
                    "symbols": [],
                    "earliest_trade": None,
                    "latest_trade": None,
                }

            exchanges = {trade.exchange for trade in all_trades}
            symbols = {trade.symbol for trade in all_trades}
            timestamps = [trade.executed_at for trade in all_trades]

            return {
                "total_trades": len(all_trades),
                "exchanges": list(exchanges),
                "symbols": list(symbols),
                "exchange_count": len(exchanges),
                "symbol_count": len(symbols),
                "earliest_trade": min(timestamps).isoformat() if timestamps else None,
                "latest_trade": max(timestamps).isoformat() if timestamps else None,
            }

        except (ValueError, TypeError, AttributeError, KeyError) as e:
            self.logger.exception("get_trade_summary_failed")
            return {
                "total_trades": 0,
                "exchanges": [],
                "symbols": [],
                "error": str(e),
            }

    async def clear_old_trades(self, keep_count: int | None = None) -> StateManagerResult:
        """Clear old trades keeping only the most recent ones.

        Args:
            keep_count: Number of trades to keep (uses config default if None)

        Returns:
            Update result
        """
        keep_count = keep_count or self.max_trade_history

        try:
            all_trades = await self.get_state()

            if len(all_trades) <= keep_count:
                return StateManagerResult.success_result(
                    message=f"No trades to clear, current count: {len(all_trades)}"
                )

            # Keep only the most recent trades
            sorted_trades = sorted(all_trades, key=lambda t: t.executed_at, reverse=True)
            trimmed_trades = sorted_trades[:keep_count]

            # Create state update
            update = StateUpdate(
                data=trimmed_trades,
                timestamp=datetime.now(UTC),
                source="clear_old_trades",
                metadata={
                    "operation": "clear_old_trades",
                    "original_count": len(all_trades),
                    "new_count": len(trimmed_trades),
                    "removed_count": len(all_trades) - len(trimmed_trades),
                },
            )

            return await self.update_with_validation(update)

        except (ValueError, TypeError, AttributeError, KeyError) as e:
            self.logger.exception("clear_old_trades_failed")
            return StateManagerResult.failure_result(
                message=f"Clear old trades failed: {e}",
                errors=[str(e)],
            )
