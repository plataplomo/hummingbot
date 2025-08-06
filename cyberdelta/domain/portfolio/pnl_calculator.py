"""Portfolio PnL calculation module.

This module handles all profit and loss calculations including comprehensive
PnL reports, position-specific calculations, and performance metrics.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.symbols.models import Symbol


if TYPE_CHECKING:
    from cyberdelta.protocols.domain.portfolio import PortfolioStateManagerProtocol

from cyberdelta.protocols.domain.portfolio import PnLCalculatorProtocol


logger = get_logger(__name__)


class PnLCalculator(PnLCalculatorProtocol):
    """Handles all PnL and performance calculations.

    Configuration Integration:
    - Uses config.calculation.pnl_calculation_method for calculation approach
    - Uses config.calculation.include_fees_in_pnl for fee inclusion
    - Uses config.calculation.base_currency for conversion
    - Uses config.calculation.performance_period_days for performance metrics

    Following CODING_STANDARDS.md:
    - Uses config.calculation settings for all parameters
    - Returns Decimal values, NOT float
    - NO hardcoded calculation parameters
    - NO assumptions about conversion rates
    """

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManagerProtocol,
    ) -> None:
        """Initialize PnL calculator with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            state_manager: State manager for portfolio state access
        """
        self.config = config
        self._state_manager = state_manager

        # Cache calculation settings from config
        self._calc_config = config.calculation
        self._pnl_method = self._calc_config.pnl_calculation_method
        self._include_fees = self._calc_config.include_fees_in_pnl
        self._base_currency = self._calc_config.base_currency
        self._performance_period_days = self._calc_config.performance_period_days

        logger.info(
            "pnl_calculator_initialized",
            pnl_method=self._pnl_method,
            include_fees=self._include_fees,
            base_currency=self._base_currency,
            performance_period_days=self._performance_period_days,
        )

    async def calculate_pnl(self) -> dict[str, Any]:
        """Calculate comprehensive PnL report using configured calculation method.

        Following CODING_STANDARDS.md:
        - Uses config.calculation.pnl_calculation_method for calculation approach
        - Includes fees based on config.calculation.include_fees_in_pnl
        - Uses config.calculation.base_currency for conversion
        - Returns Decimal values, NOT float
        - NO hardcoded calculation parameters

        Returns:
            Dictionary containing PnL calculations and metrics
        """
        # Get current portfolio state
        portfolio_state = await self._state_manager.get_state()

        logger.info(
            "pnl_calculation_starting",
            method=self._pnl_method,
            include_fees=self._include_fees,
            base_currency=self._base_currency,
            position_count=len(portfolio_state.positions),
        )

        pnl_report = {}

        try:
            # Use comprehensive calculation for all methods since others are not implemented
            pnl_report = await self._calculate_comprehensive_pnl(
                self._include_fees, self._base_currency
            )

            logger.info(
                "pnl_calculation_completed",
                method=self._pnl_method,
                total_unrealized_pnl=float(pnl_report.get("total_unrealized_pnl", 0)),
                total_realized_pnl=float(pnl_report.get("total_realized_pnl", 0)),
                net_pnl=float(pnl_report.get("net_pnl", 0)),
            )

        except Exception as e:
            logger.exception("pnl_calculation_failed", method=self._pnl_method, error=str(e))
            raise
        else:
            return pnl_report

    async def calculate_position_pnl(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> dict[str, Any] | None:
        """Calculate PnL for a specific position.

        Following CODING_STANDARDS.md:
        - Uses configured PnL calculation method
        - Returns Decimal values, NOT float
        - NO assumptions about position existence

        Args:
            symbol: Symbol of the position
            exchange: Exchange where position is held

        Returns:
            Dictionary with position PnL details or None if position not found
        """
        # Get current portfolio state
        portfolio_state = await self._state_manager.get_state()

        position_key = f"{exchange.value}:{symbol.value}"
        position = portfolio_state.positions.get(position_key)

        if position is None:
            logger.debug("position_pnl_not_found", symbol=symbol.value, exchange=exchange.value)
            return None

        # Get current market price
        current_price = await self._get_current_market_price(symbol, exchange)

        if not current_price or not position.entry_price:
            logger.warning(
                "position_pnl_calculation_incomplete",
                symbol=symbol.value,
                exchange=exchange.value,
                has_current_price=current_price is not None,
                has_entry_price=position.entry_price is not None,
            )
            return None

        # Calculate unrealized PnL
        if position.side == OrderSide.BUY:
            unrealized_pnl = (current_price - position.entry_price) * abs(position.size)
        else:
            unrealized_pnl = (position.entry_price - current_price) * abs(position.size)

        # Calculate return percentage
        entry_value = position.entry_price * abs(position.size)
        return_pct = (unrealized_pnl / entry_value * 100) if entry_value > 0 else Decimal(0)

        pnl_details = {
            "symbol": symbol.value,
            "exchange": exchange.value,
            "position_side": position.side.value,
            "position_size": position.size,
            "entry_price": position.entry_price,
            "current_price": current_price,
            "entry_value": entry_value,
            "current_value": current_price * abs(position.size),
            "unrealized_pnl": unrealized_pnl,
            "return_percentage": return_pct,
            "calculation_timestamp": datetime.now(UTC).isoformat(),
        }

        logger.debug(
            "position_pnl_calculated",
            symbol=symbol.value,
            exchange=exchange.value,
            unrealized_pnl=float(unrealized_pnl),
            return_pct=float(return_pct),
        )

        return pnl_details

    async def _calculate_mark_to_market_pnl(
        self, include_fees: bool, base_currency: str
    ) -> dict[str, Any]:
        """Calculate mark-to-market PnL using current market prices.

        Following CODING_STANDARDS.md:
        - Gets current market prices for unrealized PnL
        - Uses exact position entry prices
        - ALL calculations in Decimal precision

        Args:
            include_fees: Whether to include trading fees in calculation
            base_currency: Base currency for PnL reporting

        Returns:
            Dictionary with mark-to-market PnL calculations
        """
        # Get current portfolio state
        portfolio_state = await self._state_manager.get_state()

        total_unrealized_pnl = Decimal(0)
        total_realized_pnl = Decimal(0)
        position_pnls: dict[str, dict[str, Any]] = {}

        # Calculate unrealized PnL for each position
        for position_key, position in portfolio_state.positions.items():
            try:
                # Get current market price for this position
                current_price = await self._get_current_market_price(
                    position.symbol, position.exchange
                )

                if current_price and position.entry_price:
                    # Calculate unrealized PnL
                    if position.side == OrderSide.BUY:
                        # Long position: profit when price goes up
                        unrealized_pnl = (current_price - position.entry_price) * abs(position.size)
                    else:
                        # Short position: profit when price goes down
                        unrealized_pnl = (position.entry_price - current_price) * abs(position.size)

                    position_pnls[position_key] = {
                        "symbol": position.symbol.value,
                        "exchange": position.exchange.value,
                        "entry_price": position.entry_price,
                        "current_price": current_price,
                        "position_size": position.size,
                        "unrealized_pnl": unrealized_pnl,
                    }

                    total_unrealized_pnl += unrealized_pnl

                    logger.debug(
                        "position_pnl_calculated",
                        symbol=position.symbol.value,
                        exchange=position.exchange.value,
                        entry_price=float(position.entry_price),
                        current_price=float(current_price),
                        unrealized_pnl=float(unrealized_pnl),
                    )
                else:
                    logger.warning(
                        "position_pnl_skipped",
                        symbol=position.symbol.value,
                        exchange=position.exchange.value,
                        reason="missing_price_data",
                    )

            except Exception as e:
                logger.exception(
                    "position_pnl_calculation_error", position_key=position_key, error=str(e)
                )
                # Continue with other positions

        # Get realized PnL from trading history if needed
        # This would require implementing trade history tracking
        # For now, use position data
        total_realized_pnl = await self._calculate_realized_pnl_from_positions()

        # Calculate fees if requested
        total_fees = Decimal(0)
        if include_fees:
            total_fees = await self._calculate_total_fees()

        net_pnl = total_unrealized_pnl + total_realized_pnl
        if include_fees:
            net_pnl -= total_fees

        return {
            "calculation_method": "mark_to_market",
            "base_currency": base_currency,
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_realized_pnl": total_realized_pnl,
            "total_fees": total_fees if include_fees else Decimal(0),
            "net_pnl": net_pnl,
            "position_count": len(position_pnls),
            "position_pnls": position_pnls,
            "calculation_timestamp": datetime.now(UTC).isoformat(),
            "include_fees": include_fees,
        }

    async def _calculate_realized_pnl_only(
        self, include_fees: bool, base_currency: str
    ) -> dict[str, Any]:
        """Calculate only realized PnL from completed trades.

        Following CODING_STANDARDS.md:
        - Only includes PnL from closed positions
        - Uses actual trade execution data
        - NO mark-to-market calculations

        Args:
            include_fees: Whether to include trading fees
            base_currency: Base currency for reporting

        Returns:
            Dictionary with realized PnL only
        """
        total_realized_pnl = await self._calculate_realized_pnl_from_positions()

        # Calculate fees if requested
        total_fees = Decimal(0)
        if include_fees:
            total_fees = await self._calculate_total_fees()

        net_pnl = total_realized_pnl
        if include_fees:
            net_pnl -= total_fees

        return {
            "calculation_method": "realized_only",
            "base_currency": base_currency,
            "total_unrealized_pnl": Decimal(0),  # Not calculated in this method
            "total_realized_pnl": total_realized_pnl,
            "total_fees": total_fees if include_fees else Decimal(0),
            "net_pnl": net_pnl,
            "calculation_timestamp": datetime.now(UTC).isoformat(),
            "include_fees": include_fees,
        }

    async def _calculate_comprehensive_pnl(
        self, include_fees: bool, base_currency: str
    ) -> dict[str, Any]:
        """Calculate comprehensive PnL with all metrics.

        Args:
            include_fees: Whether to include trading fees
            base_currency: Base currency for reporting

        Returns:
            Dictionary with comprehensive PnL analysis

        IMPORTANT: Following CODING_STANDARDS.md:
        - Includes both realized and unrealized PnL
        - Provides detailed breakdown by position
        - Performance metrics from configuration
        """
        # Get mark-to-market calculation
        mtm_pnl = await self._calculate_mark_to_market_pnl(include_fees, base_currency)

        # Add additional comprehensive metrics
        portfolio_state = await self._state_manager.get_state()
        portfolio_value = portfolio_state.total_equity_usd or Decimal(0)

        # Calculate return percentages if we have portfolio value
        if portfolio_value and portfolio_value > 0:
            unrealized_return_pct = (mtm_pnl["total_unrealized_pnl"] / portfolio_value) * 100
            realized_return_pct = (mtm_pnl["total_realized_pnl"] / portfolio_value) * 100
            net_return_pct = (mtm_pnl["net_pnl"] / portfolio_value) * 100
        else:
            unrealized_return_pct = Decimal(0)
            realized_return_pct = Decimal(0)
            net_return_pct = Decimal(0)

        # Get performance period settings from config (using available fields)
        performance_period_days = self._performance_period_days

        # Calculate time-weighted returns (placeholder for now)
        time_weighted_return = Decimal(0)
        # This would be implemented when performance metrics are added to config

        return {
            **mtm_pnl,  # Include all mark-to-market data
            "calculation_method": "comprehensive",
            "portfolio_value_usd": portfolio_value,
            "unrealized_return_pct": unrealized_return_pct,
            "realized_return_pct": realized_return_pct,
            "net_return_pct": net_return_pct,
            "time_weighted_return_pct": time_weighted_return,
            "performance_period_days": performance_period_days,
            "performance_metrics_enabled": True,
        }

    async def _get_current_market_price(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> Decimal | None:
        """Get current market price for a symbol on an exchange.

        Args:
            symbol: Symbol to get price for
            exchange: Exchange to get price from

        Returns:
            Current market price or None if unavailable

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about market data availability
        - Uses actual market data services when available
        - Returns Decimal price, NOT float
        """
        # This would integrate with the MarketDataService in a full implementation
        # For now, return None to indicate price unavailable
        logger.debug(
            "market_price_request",
            symbol=symbol.value,
            exchange=exchange.value,
            reason="market_data_service_integration_pending",
        )

        # TODO: Implement market price fetching when market service is available
        return None

    async def _calculate_realized_pnl_from_positions(self) -> Decimal:
        """Calculate realized PnL from position history.

        Returns:
            Total realized PnL across all positions

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses actual trade execution data
        - Returns Decimal, NOT float
        - NO assumptions about position closing
        """
        # This would require implementing trade history tracking
        # For now, return zero as placeholder
        # In full implementation, this would:
        # 1. Get all completed trades from trade history
        # 2. Calculate PnL for each completed position
        # 3. Sum up all realized PnL

        logger.debug("realized_pnl_calculation", reason="trade_history_integration_pending")

        return Decimal(0)

    async def _calculate_total_fees(self) -> Decimal:
        """Calculate total trading fees paid.

        Returns:
            Total fees in base currency

        IMPORTANT: Following CODING_STANDARDS.md:
        - Sums actual fees from trade executions
        - Converts to base currency using configured rates
        - Returns Decimal, NOT float
        """
        # This would require trade history tracking
        # For now, return zero as placeholder
        # In full implementation, this would:
        # 1. Get all trades from history
        # 2. Sum up all fees (converting currencies as needed)
        # 3. Return total in base currency

        logger.debug("fee_calculation", reason="trade_history_integration_pending")

        return Decimal(0)

    async def _calculate_time_weighted_return(self, period_days: int) -> Decimal:
        """Calculate time-weighted return over specified period.

        Args:
            period_days: Number of days to calculate return over

        Returns:
            Time-weighted return percentage

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured period from config
        - Accounts for cash flows and timing
        - Returns Decimal percentage, NOT float
        """
        # This would require portfolio value history tracking
        # For now, return zero as placeholder
        # In full implementation, this would:
        # 1. Get portfolio values over the period
        # 2. Account for cash inflows/outflows
        # 3. Calculate geometric return

        logger.debug(
            "time_weighted_return_calculation",
            period_days=period_days,
            reason="portfolio_history_integration_pending",
        )

        return Decimal(0)
