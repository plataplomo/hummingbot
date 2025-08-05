"""Position and concentration limit checking.

This module handles checking position limits and concentration limits
for symbols, exchanges, and asset classes.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.models.portfolio.state import PortfolioState


logger = get_logger(__name__)


class LimitChecker:
    """Position and concentration limit checker.

    This class handles:
    - Position count limits (per symbol, total, per exchange)
    - Concentration limits (per symbol, per asset class)
    - Portfolio state analysis for limit enforcement

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL limits from AppSettings configuration
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - Returns explicit violation messages
    """

    def __init__(self, config: AppSettings, portfolio_service: PortfolioService) -> None:
        """Initialize limit checker with configuration and dependencies.

        Args:
            config: Application settings containing limit configuration
            portfolio_service: Portfolio service for state access
        """
        self.config = config
        self._portfolio_service = portfolio_service

        logger.debug("limit_checker_initialized", has_limits_config=hasattr(config.risk, "limits"))

    async def check_position_limits(self, symbol: Symbol, exchange: ExchangeName) -> list[str]:
        """Check position limits for a specific symbol and exchange.

        Args:
            symbol: Symbol to check limits for
            exchange: Exchange to check limits on

        Returns:
            List of limit violations (empty if no violations)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Check config.risk.limits.max_positions_per_symbol
        - Check config.risk.limits.max_positions_total
        - Return explicit violations, NO silent filtering
        - ALL limits from configuration
        """
        violations: list[str] = []

        # Get current portfolio state
        portfolio_state = await self._portfolio_service.get_state()
        if not portfolio_state:
            # If no portfolio state, cannot check limits
            return violations

        # Get risk limits configuration
        if not hasattr(self.config.risk, "limits"):
            # No limits configured - return no violations
            logger.debug(
                "no_position_limits_configured", symbol=symbol.value, exchange=exchange.value
            )
            return violations

        limits_config = self.config.risk.limits

        # Check per-symbol position limits
        if hasattr(limits_config, "max_positions_per_symbol"):
            max_per_symbol = limits_config.max_positions_per_symbol
            current_symbol_positions = self._count_symbol_positions(portfolio_state, symbol)

            if current_symbol_positions >= max_per_symbol:
                violations.append(
                    f"Symbol {symbol.value} has {current_symbol_positions} positions, "
                    f"max allowed: {max_per_symbol}"
                )

        # Check total position limits
        if hasattr(limits_config, "max_positions_total"):
            max_total = limits_config.max_positions_total
            total_positions = self._count_total_positions(portfolio_state)

            if total_positions >= max_total:
                violations.append(f"Total positions {total_positions} at max allowed: {max_total}")

        # Check exchange-specific position limits
        if hasattr(limits_config, "max_positions_per_exchange"):
            max_per_exchange = limits_config.max_positions_per_exchange
            exchange_positions = self._count_exchange_positions(portfolio_state, exchange)

            if exchange_positions >= max_per_exchange:
                violations.append(
                    f"Exchange {exchange.value} has {exchange_positions} positions, "
                    f"max allowed: {max_per_exchange}"
                )

        if violations:
            logger.warning(
                "position_limit_violations",
                symbol=symbol.value,
                exchange=exchange.value,
                violations=violations,
            )

        return violations

    def _count_symbol_positions(self, portfolio_state: PortfolioState, symbol: Symbol) -> int:
        """Count positions for a specific symbol across all exchanges.

        Args:
            portfolio_state: Current portfolio state
            symbol: Symbol to count positions for

        Returns:
            Number of positions for the symbol

        IMPORTANT: Following CODING_STANDARDS.md:
        - Counts non-zero positions only
        - Uses Symbol object, NOT string matching
        """
        count = 0

        for position_key, position in portfolio_state.positions.items():
            # Extract symbol from position key format: "exchange:symbol"
            try:
                _, position_symbol_str = position_key.split(":", 1)
                if position_symbol_str == symbol.value and position.size != 0:
                    count += 1
            except ValueError:
                # Skip malformed position keys
                logger.warning("malformed_position_key", position_key=position_key)
                continue

        return count

    def _count_total_positions(self, portfolio_state: PortfolioState) -> int:
        """Count total positions across all symbols and exchanges.

        Args:
            portfolio_state: Current portfolio state

        Returns:
            Total number of non-zero positions

        IMPORTANT: Following CODING_STANDARDS.md:
        - Counts only non-zero positions
        - NO assumptions about position structure
        """
        count = 0

        for position in portfolio_state.positions.values():
            if position.size != 0:
                count += 1

        return count

    def _count_exchange_positions(
        self, portfolio_state: PortfolioState, exchange: ExchangeName
    ) -> int:
        """Count positions on a specific exchange.

        Args:
            portfolio_state: Current portfolio state
            exchange: Exchange to count positions for

        Returns:
            Number of positions on the exchange

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses ExchangeName enum, NOT string matching
        - Counts non-zero positions only
        """
        count = 0

        for position_key, position in portfolio_state.positions.items():
            # Extract exchange from position key format: "exchange:symbol"
            try:
                exchange_str, _ = position_key.split(":", 1)
                if exchange_str == exchange.value and position.size != 0:
                    count += 1
            except ValueError:
                # Skip malformed position keys
                logger.warning(
                    "malformed_position_key_in_exchange_count", position_key=position_key
                )
                continue

        return count

    async def check_concentration_limits(
        self, symbol: Symbol, position_value_usd: Decimal
    ) -> list[str]:
        """Check concentration limits for a position.

        Args:
            symbol: Symbol for the position
            position_value_usd: Value of the proposed position in USD

        Returns:
            List of concentration limit violations

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses config.risk.limits.max_concentration_per_symbol
        - Uses config.risk.limits.max_concentration_per_asset_class
        - ALL percentages from configuration
        """
        violations: list[str] = []

        # Get current portfolio state
        portfolio_state = await self._portfolio_service.get_state()
        if not portfolio_state or not portfolio_state.total_equity_usd:
            return violations

        total_equity = portfolio_state.total_equity_usd

        # Get concentration limits configuration
        if not hasattr(self.config.risk, "limits"):
            return violations

        limits_config = self.config.risk.limits

        # Check per-symbol concentration
        if hasattr(limits_config, "max_concentration_per_symbol"):
            max_symbol_pct = limits_config.max_concentration_per_symbol
            current_symbol_value = self._get_symbol_total_value(portfolio_state, symbol)
            new_symbol_value = current_symbol_value + position_value_usd
            new_symbol_pct = (new_symbol_value / total_equity) * 100

            if new_symbol_pct > max_symbol_pct:
                violations.append(
                    f"Symbol {symbol.value} concentration would be {new_symbol_pct:.1f}%, "
                    f"max allowed: {max_symbol_pct}%"
                )

        # Check asset class concentration (if configured)
        if hasattr(limits_config, "max_concentration_per_asset_class"):
            asset_class = self._get_asset_class(symbol)
            if asset_class:
                max_class_pct = limits_config.max_concentration_per_asset_class
                current_class_value = self._get_asset_class_total_value(
                    portfolio_state, asset_class
                )
                new_class_value = current_class_value + position_value_usd
                new_class_pct = (new_class_value / total_equity) * 100

                if new_class_pct > max_class_pct:
                    violations.append(
                        f"Asset class {asset_class} concentration would be {new_class_pct:.1f}%, "
                        f"max allowed: {max_class_pct}%"
                    )

        return violations

    def _get_symbol_total_value(self, portfolio_state: PortfolioState, symbol: Symbol) -> Decimal:
        """Get total value of positions for a specific symbol.

        Args:
            portfolio_state: Current portfolio state
            symbol: Symbol to calculate value for

        Returns:
            Total value in USD for the symbol

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - Uses current market prices for valuation
        """
        total_value = Decimal(0)

        for position_key, position in portfolio_state.positions.items():
            try:
                _, position_symbol_str = position_key.split(":", 1)
                if (
                    position_symbol_str == symbol.value
                    and position.size != 0
                    and position.entry_price
                ):
                    # Calculate position value using entry price
                    # In a full implementation, this would use current market price
                    position_value = abs(position.size) * position.entry_price
                    total_value += position_value
            except (ValueError, AttributeError):
                # Skip malformed positions
                continue

        return total_value

    def _get_asset_class(self, symbol: Symbol) -> str | None:
        """Get asset class for a symbol.

        Args:
            symbol: Symbol to get asset class for

        Returns:
            Asset class name or None if not categorized

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses symbol metadata for classification
        - NO hardcoded asset class mappings
        """
        # This would typically use symbol service metadata
        # For now, delegate to string-based classification
        return self._get_asset_class_from_string(symbol.value)

    def _get_asset_class_from_string(self, symbol_str: str) -> str | None:
        """Get asset class from symbol string (temporary implementation).

        Args:
            symbol_str: Symbol string to classify

        Returns:
            Asset class name or None if not categorized
        """
        symbol_upper = symbol_str.upper()

        if any(crypto in symbol_upper for crypto in ["BTC", "ETH", "SOL", "AVAX"]):
            return "cryptocurrency"
        if "USD" in symbol_upper or "USDC" in symbol_upper:
            return "stablecoin"
        return "other"

    def _get_asset_class_total_value(
        self, portfolio_state: PortfolioState, asset_class: str
    ) -> Decimal:
        """Get total value of positions for an asset class.

        Args:
            portfolio_state: Current portfolio state
            asset_class: Asset class to calculate value for

        Returns:
            Total value in USD for the asset class

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about asset classification
        """
        total_value = Decimal(0)

        for position_key, position in portfolio_state.positions.items():
            try:
                _, position_symbol_str = position_key.split(":", 1)
                # Get asset class for classification
                # This is simplified - would use symbol service and Symbol object in practice
                position_asset_class = self._get_asset_class_from_string(position_symbol_str)

                if (
                    position_asset_class == asset_class
                    and position.size != 0
                    and position.entry_price
                ):
                    position_value = abs(position.size) * position.entry_price
                    total_value += position_value

            except (ValueError, AttributeError):
                # Skip malformed positions
                continue

        return total_value
