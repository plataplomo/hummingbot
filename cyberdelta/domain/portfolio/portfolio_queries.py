"""Portfolio query operations.

This module contains all query/read operations extracted from PortfolioService
to maintain file size under 600 lines while keeping the same business logic.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.portfolio import NoBalancesAvailableError
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.symbols.models import Symbol


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings
    from cyberdelta.domain.portfolio.balance_manager import BalanceManager
    from cyberdelta.domain.portfolio.position_manager import PositionManager
    from cyberdelta.domain.portfolio.state_manager import PortfolioStateManager

logger = get_logger(__name__)


class PortfolioQueries:
    """Query operations for portfolio service.

    This class contains all read/query operations extracted from PortfolioService
    to maintain file size limits while preserving exact business logic.
    """

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManager,
        balance_manager: BalanceManager,
        position_manager: PositionManager,
    ) -> None:
        """Initialize query operations.

        Args:
            config: Application settings
            state_manager: State management module
            balance_manager: Balance management module
            position_manager: Position management module
        """
        self.config = config
        self._state_manager = state_manager
        self._balance_manager = balance_manager
        self._position_manager = position_manager

    async def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Returns:
            Current portfolio state
        """
        return await self._state_manager.get_state()

    async def get_total_equity_usd(self) -> Decimal:
        """Get total portfolio equity in USD.

        Returns:
            Total equity value in USD

        Raises:
            NoBalancesAvailableError: If no balances available for calculation
        """
        state = await self.get_state()
        # TODO: Implement equity calculation with market prices
        balances = list(state.balances.values())
        if not balances:
            raise NoBalancesAvailableError
        total = sum(b.total_quantity for b in balances)
        return Decimal(total)

    async def list_snapshots(self) -> list[str]:
        """List available portfolio snapshots.

        Returns:
            List of snapshot names/timestamps
        """
        return await self._state_manager.list_snapshots()

    async def get_balance(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
        balance_type: str = "available",
    ) -> Decimal:
        """Get balance for specific asset.

        Args:
            symbol: Asset symbol
            exchange: Exchange to filter by
            balance_type: Type of balance ('available', 'total', 'locked')

        Returns:
            Balance amount as Decimal
        """
        # Get balance from specific exchange
        balance = await self._balance_manager.get_balance(asset=symbol, exchange=exchange)
        if balance is None:
            return Decimal(0)

        if balance_type == "available":
            return balance.available_quantity
        return balance.total_quantity

    async def get_exchange_balances(
        self,
        exchange: ExchangeName,
        non_zero_only: bool = True,
    ) -> list[SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange to get balances for
            non_zero_only: If True, only return non-zero balances

        Returns:
            List of SpotBalance objects
        """
        state = await self.get_state()
        balances: list[SpotBalance] = []

        for balance in state.balances.values():
            if balance.exchange == exchange:
                if (
                    non_zero_only
                    and balance.available_quantity == Decimal(0)
                    and balance.total_quantity == Decimal(0)
                ):
                    continue
                balances.append(balance)

        logger.debug(
            "exchange_balances_retrieved",
            exchange=exchange.value,
            balance_count=len(balances),
            non_zero_only=non_zero_only,
        )

        return balances

    async def get_position(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> DerivativePosition | None:
        """Get position for specific symbol and exchange.

        Args:
            symbol: Symbol to get position for
            exchange: Exchange to get position from

        Returns:
            Position if exists, None otherwise
        """
        return await self._position_manager.get_position(symbol, exchange)

    async def get_exchange_positions(self, exchange: ExchangeName) -> list[DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange to get positions for

        Returns:
            List of positions
        """
        state = await self.get_state()
        return [pos for pos in state.positions.values() if pos.exchange == exchange]

    async def get_performance_metrics(
        self,
        period_days: int | None = None,
        include_positions: bool = True,
        include_balances: bool = True,
    ) -> dict[str, Any]:
        """Get portfolio performance metrics.

        Args:
            period_days: Number of days to calculate metrics for
            include_positions: Include position details
            include_balances: Include balance details

        Returns:
            Dictionary of performance metrics

        Note:
            Performance tracking should be done by a separate service
        """
        logger.warning("performance_metrics_not_available_at_portfolio_level")
        result: dict[str, Any] = {
            "metrics_available": False,
            "reason": "Performance tracking should be done by a separate service",
        }

        # Include current positions if requested
        if include_positions:
            state = await self.get_state()
            result["positions"] = [
                {
                    "symbol": pos.symbol.value,
                    "exchange": pos.exchange.value,
                    "size": float(pos.size),
                    "side": pos.side.value,
                    "entry_price": float(pos.entry_price) if pos.entry_price else None,
                    "unrealized_pnl": float(pos.unrealized_pnl) if pos.unrealized_pnl else None,
                }
                for pos in state.positions.values()
            ]

        # Include current balances if requested
        if include_balances:
            state = await self.get_state()
            result["balances"] = [
                {
                    "asset": bal.asset,
                    "exchange": bal.exchange.value,
                    "available": float(bal.available_quantity),
                    "total": float(bal.total_quantity),
                }
                for bal in state.balances.values()
                if bal.total_quantity > Decimal(0)
            ]

        return result
