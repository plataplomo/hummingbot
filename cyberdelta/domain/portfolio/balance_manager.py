"""Balance management operations for portfolio service.

This module handles all balance-related operations including:
- Balance tracking and updates
- Balance validation
- Exchange-specific balance management
"""

from datetime import UTC, datetime
from decimal import Decimal

import structlog

from cyberdelta.config import AppSettings
from cyberdelta.enums import ExchangeName
from cyberdelta.models import SpotBalance
from cyberdelta.models.market.trade import Trade
from cyberdelta.models.portfolio.pnl_report import ReconciliationReport
from cyberdelta.protocols.domain.portfolio import (
    BalanceManagerProtocol,
    PortfolioStateManagerProtocol,
)
from cyberdelta.symbols.global_service import get_symbol_service
from cyberdelta.symbols.models import Symbol


logger = structlog.get_logger(__name__)

# Module-level symbol service initialization
# Following the pattern from momentum_strategy.py - initialize once at module level
# This avoids repeated calls to get_symbol_service() in methods
_symbol_service = get_symbol_service()


class BalanceManager(BalanceManagerProtocol):
    """Manages balance operations for the portfolio service.

    This class handles:
    - Balance tracking and updates from trades
    - Balance validation against exchange data
    - Cross-exchange balance aggregation
    """

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManagerProtocol,
    ) -> None:
        """Initialize balance manager.

        Args:
            config: Application settings
            state_manager: Portfolio state manager
        """
        self.config = config
        self._state_manager = state_manager

        # Cache frequently used settings
        self._balance_tolerance = config.validation.balance_tolerance
        self._min_balance_threshold = config.validation.min_balance_threshold

    async def get_balance(
        self,
        asset: Symbol,
        exchange: ExchangeName,
    ) -> SpotBalance | None:
        """Get balance for specific asset on exchange.

        Args:
            asset: Asset symbol
            exchange: Exchange name

        Returns:
            Balance if found, None otherwise
        """
        state = await self._state_manager.get_state()
        if not state:
            return None

        key = f"{exchange.value}:{asset.value}"
        return state.balances.get(key)

    async def get_total_balance_usd(self, exchange: ExchangeName | None = None) -> Decimal:
        """Get total balance in USD.

        Args:
            exchange: Optional exchange to filter by

        Returns:
            Total balance in USD
        """
        state = await self._state_manager.get_state()
        if not state:
            return Decimal(0)

        total = Decimal(0)

        for key, balance in state.balances.items():
            # Filter by exchange if specified
            if exchange and not key.startswith(f"{exchange.value}:"):
                continue

            # For now, assume USDC = USD (would need price feed for other assets)
            if balance.asset.value in {"USDC", "USD"}:
                total += balance.total_quantity

        return total

    async def update_balance_from_trade(self, trade: Trade) -> None:
        """Update balance based on trade execution.

        Args:
            trade: Executed trade
        """
        # Get quote asset from trade symbol
        quote_asset = self._get_quote_asset(trade.symbol)
        if not quote_asset:
            logger.warning(
                "Could not determine quote asset",
                symbol=trade.symbol.value,
                exchange=trade.exchange,
            )
            return

        # Calculate cost impact
        cost = trade.quantity * trade.price
        if trade.side.value == "BUY":
            cost = -cost  # Buying costs money

        # Update balance
        # Convert trade.exchange string to ExchangeName
        exchange_enum = ExchangeName(trade.exchange)
        await self._update_balance(
            exchange=exchange_enum,
            asset=quote_asset,
            delta=cost - trade.fee,
        )

    async def _update_balance(
        self,
        exchange: ExchangeName,
        asset: Symbol,
        delta: Decimal,
    ) -> None:
        """Update balance by delta amount.

        Args:
            exchange: Exchange name
            asset: Asset symbol
            delta: Amount to add (positive) or subtract (negative)
        """
        state = await self._state_manager.get_state()
        if not state:
            return

        key = f"{exchange.value}:{asset.value}"
        current = state.balances.get(key)

        if current:
            # Update existing balance
            new_total = current.total_quantity + delta
            new_available = current.available_quantity + delta

            # Create new balance instance (immutable)
            new_balance = SpotBalance(
                exchange=exchange,
                asset=asset,
                timestamp=datetime.now(UTC),
                total_quantity=new_total,
                available_quantity=new_available,
            )
        else:
            # Create new balance entry
            new_balance = SpotBalance(
                exchange=exchange,
                asset=asset,
                timestamp=datetime.now(UTC),
                total_quantity=delta,
                available_quantity=delta,
            )

        # Update state
        state.balances[key] = new_balance
        state.timestamp = datetime.now(UTC)

        # Save state
        await self._state_manager.save_state()

        logger.info(
            "Balance updated",
            exchange=exchange.value,
            asset=asset.value,
            delta=delta,
            new_total=new_balance.total_quantity,
        )

    async def validate_balance(
        self,
        exchange: ExchangeName,
        asset: Symbol,
        expected: Decimal,
    ) -> bool:
        """Validate balance against expected value.

        Args:
            exchange: Exchange name
            asset: Asset symbol
            expected: Expected balance

        Returns:
            True if balance is within tolerance
        """
        actual = await self.get_balance(asset, exchange)
        if not actual:
            return abs(expected) < self._balance_tolerance

        diff = abs(actual.total_quantity - expected)
        return diff <= self._balance_tolerance

    async def reconcile_balances(
        self,
        exchange_balances: list[SpotBalance],
        exchange: ExchangeName,
    ) -> ReconciliationReport:
        """Reconcile local balances with exchange data.

        Args:
            exchange_balances: Typed balances from exchange
            exchange: Exchange name

        Returns:
            Typed reconciliation results
        """
        balance_discrepancies: list[str] = []
        updated = 0

        state = await self._state_manager.get_state()
        if not state:
            return ReconciliationReport(
                reconciliation_timestamp=datetime.now(UTC),
                reconciliation_successful=False,
                total_discrepancies=0,
                exchange_results={exchange.value: False},
                balance_discrepancies=[],
                position_discrepancies=[],
                error_messages=["No portfolio state available"],
            )

        # Process each exchange balance
        for exchange_balance in exchange_balances:
            asset = exchange_balance.asset
            key = f"{exchange.value}:{asset.value}"

            # Get exchange balance data
            exchange_total = exchange_balance.total_quantity

            # Get local balance
            local_balance = state.balances.get(key)

            if local_balance:
                # Check for discrepancy
                diff = abs(local_balance.total_quantity - exchange_total)
                if diff > self._balance_tolerance:
                    balance_discrepancies.append(
                        f"{asset.value}: local={local_balance.total_quantity}, "
                        f"exchange={exchange_total}, diff={diff}"
                    )

                    # Update to match exchange
                    if self.config.state.reconciliation_enabled:
                        # Use the exchange balance directly
                        state.balances[key] = exchange_balance
                        updated += 1
            elif exchange_total > 0:
                # New balance from exchange - use it directly
                state.balances[key] = exchange_balance
                updated += 1

        # Save if updated
        if updated > 0:
            state.timestamp = datetime.now(UTC)
            await self._state_manager.save_state()

        return ReconciliationReport(
            reconciliation_timestamp=datetime.now(UTC),
            reconciliation_successful=len(balance_discrepancies) == 0,
            total_discrepancies=len(balance_discrepancies),
            exchange_results={exchange.value: len(balance_discrepancies) == 0},
            balance_discrepancies=balance_discrepancies,
            position_discrepancies=[],
            error_messages=None,
        )

    def _get_quote_asset(self, symbol: Symbol) -> Symbol | None:
        """Get quote asset from trading symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Quote asset symbol or None
        """
        # Simple parsing - would be better to use symbol service
        symbol_str = symbol.value

        # Common quote assets
        quote_assets = ["USDC", "USD", "USDT", "BTC", "ETH"]

        for quote in quote_assets:
            if symbol_str.endswith(quote):
                # Create quote asset symbol using the same exchange as the trading symbol
                # This is a simplification - ideally would have explicit exchange info
                return _symbol_service.create_symbol(quote, ExchangeName.BACKPACK)

        # Try splitting by underscore or dash
        for separator in ["_", "-"]:
            if separator in symbol_str:
                parts = symbol_str.split(separator)
                parts_count = 2
                if len(parts) == parts_count:
                    return _symbol_service.create_symbol(parts[1], ExchangeName.BACKPACK)

        return None

    async def get_exchange_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of asset -> SpotBalance for the exchange
        """
        state = await self._state_manager.get_state()
        if not state:
            return {}

        result: dict[str, SpotBalance] = {}
        prefix = f"{exchange.value}:"

        for key, balance in state.balances.items():
            if key.startswith(prefix):
                asset_str = key[len(prefix) :]
                result[asset_str] = balance

        return result

    async def update_balance_directly(
        self, asset: Symbol, exchange: ExchangeName, new_balance: SpotBalance
    ) -> None:
        """Update balance directly (for reconciliation).

        Args:
            asset: Asset symbol
            exchange: Exchange name
            new_balance: New balance to set
        """
        state = await self._state_manager.get_state()
        if not state:
            return

        key = f"{exchange.value}:{asset.value}"
        state.balances[key] = new_balance
        state.timestamp = datetime.now(UTC)

        await self._state_manager.save_state()

        logger.info(
            "balance_updated_directly",
            exchange=exchange.value,
            asset=asset.value,
            total=new_balance.total_quantity,
            available=new_balance.available_quantity,
        )
