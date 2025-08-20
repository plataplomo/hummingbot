"""Portfolio calculation operations.

This module contains all calculation operations (PnL, reconciliation, health checks)
extracted from PortfolioService to maintain file size under 600 lines.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import MarkToMarketCalculator
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import ServiceType
from cyberdelta.enums.trading import OrderSide
from cyberdelta.exceptions.portfolio import NoPriceAvailableError, ReconciliationError
from cyberdelta.models import DerivativePosition
from cyberdelta.models.portfolio.pnl_report import (
    PnLReport,
    PositionPnLDetail,
    ReconciliationReport,
)
from cyberdelta.protocols.domain.market_data import MarketDataServiceProtocol
from cyberdelta.symbols.models import Symbol


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.config.models import AppSettings
    from cyberdelta.domain.portfolio.reconciliation_engine import ReconciliationEngine
    from cyberdelta.domain.portfolio.state_manager import PortfolioStateManager

logger = get_logger(__name__)


class PortfolioCalculations:
    """Calculation operations for portfolio service.

    This class contains all calculation operations (PnL, reconciliation, health)
    extracted from PortfolioService to maintain file size limits.
    """

    def __init__(
        self,
        config: AppSettings,
        state_manager: PortfolioStateManager,
        pnl_calculator: MarkToMarketCalculator,
        reconciliation_engine: ReconciliationEngine,
        api_clients: dict[str, ExchangeAPI],
        market_data_service: MarketDataServiceProtocol,
    ) -> None:
        """Initialize calculation operations.

        Args:
            config: Application settings
            state_manager: State management module
            pnl_calculator: PnL calculator instance
            reconciliation_engine: Optional reconciliation engine
            api_clients: Optional exchange API clients
            market_data_service: Optional market data service
        """
        self.config = config
        self._state_manager = state_manager
        self._pnl_calculator = pnl_calculator
        self._reconciliation_engine = reconciliation_engine
        self._api_clients = api_clients or {}
        self._market_data_service = market_data_service

        # Extract fee configuration from AppSettings
        self._include_fees_default = config.financial.pnl.include_fees_in_pnl

    async def calculate_pnl(self) -> PnLReport:
        """Calculate comprehensive PnL report.

        Returns:
            PnL report with position details and totals
        """
        state = await self._state_manager.get_state()

        position_details: list[PositionPnLDetail] = []
        total_realized = Decimal(0)
        total_unrealized = Decimal(0)

        # Calculate PnL for each position
        for position in state.positions.values():
            pnl_detail = await self.calculate_position_pnl(position)
            position_details.append(pnl_detail)

            total_realized += pnl_detail.realized_pnl_usd
            total_unrealized += pnl_detail.unrealized_pnl_usd

        return PnLReport(
            calculation_timestamp=datetime.now(UTC),
            total_realized_pnl_usd=total_realized,
            total_unrealized_pnl_usd=total_unrealized,
            net_pnl_usd=total_realized + total_unrealized,
            total_equity_usd=total_realized + total_unrealized,
            total_exposure_usd=Decimal(0),  # TODO: Implement proper exposure calculation
            calculation_method=self.config.financial.pnl.calculation_method,
            fees_included=self.config.financial.pnl.include_fees_in_pnl,
            base_currency=self.config.calculation.base_currency,
            position_pnls={
                f"{pos.exchange.value}:{pos.symbol.value}": detail
                for pos, detail in zip(state.positions.values(), position_details, strict=False)
            },
        )

    async def calculate_position_pnl(
        self,
        position: DerivativePosition,
        mark_price: Decimal | None = None,
    ) -> PositionPnLDetail:
        """Calculate PnL for a single position.

        Args:
            position: Position to calculate PnL for
            mark_price: Optional mark price (will fetch if not provided)

        Returns:
            Position PnL details
        """
        # Get mark price if not provided
        if mark_price is None and self._market_data_service:
            mark_price = await self._get_current_market_price(position.symbol, position.exchange)

        # Calculate unrealized PnL using centralized calculator
        unrealized_pnl = None
        if mark_price and position.entry_price:
            pnl_result = self._pnl_calculator.calculate_unrealized_pnl(
                position=position,
                mark_price=mark_price,
                include_fees=self._include_fees_default,  # Use config value
            )
            unrealized_pnl = pnl_result.amount

        # Get realized PnL from position
        realized_pnl = position.realized_pnl

        # Calculate final PnL values
        final_unrealized = (
            unrealized_pnl
            if unrealized_pnl is not None
            else (position.unrealized_pnl if position.unrealized_pnl is not None else Decimal(0))
        )
        final_realized = (
            realized_pnl
            if realized_pnl is not None
            else (position.realized_pnl if position.realized_pnl is not None else Decimal(0))
        )
        final_total_pnl = final_unrealized + final_realized
        final_entry_price = position.entry_price or position.mark_price or Decimal(0)
        final_current_price = mark_price or position.mark_price or Decimal(0)

        return PositionPnLDetail(
            symbol=position.symbol,
            exchange=position.exchange,
            unrealized_pnl_usd=final_unrealized,
            realized_pnl_usd=final_realized,
            total_pnl_usd=final_total_pnl,
            entry_price=final_entry_price,
            current_price=final_current_price,
            quantity=position.size,
            market_value_usd=final_current_price * position.size,
        )

    async def reconcile_with_exchanges(self) -> ReconciliationReport:
        """Reconcile portfolio state with exchanges.

        Returns:
            Reconciliation report with discrepancies

        Raises:
            ReconciliationError: If reconciliation fails
        """
        if not self._reconciliation_engine:
            raise ReconciliationError(0)

        try:
            report = await self._reconciliation_engine.reconcile_with_exchanges(self._api_clients)

            # Log any discrepancies
            if report.total_discrepancies > 0:
                logger.warning(
                    "reconciliation_discrepancies_found",
                    position_discrepancies=len(report.position_discrepancies),
                    balance_discrepancies=len(report.balance_discrepancies),
                )
            else:
                logger.info("reconciliation_successful", exchanges=len(report.exchange_results))

        except Exception as e:
            logger.exception("reconciliation_failed", error=str(e))
            raise ReconciliationError(len(self._api_clients)) from e
        else:
            return report

    # Health check removed - it doesn't belong in portfolio calculations
    # Health checks requiring execution statistics should be at a higher level

    def get_service_type(self) -> ServiceType:
        """Get service type for monitoring.

        Returns:
            Service type identifier
        """
        return ServiceType.PORTFOLIO

    async def _get_current_market_price(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> Decimal | None:
        """Get current market price for a symbol.

        Args:
            symbol: Symbol to get price for
            exchange: Exchange to get price from

        Returns:
            Current market price or None if unavailable
        """
        if not self._market_data_service:
            logger.warning(
                "market_data_service_not_available",
                symbol=symbol.value,
                exchange=exchange.value,
            )
            return None

        try:
            ticker = await self._market_data_service.get_ticker(symbol=symbol)

        except Exception as e:
            logger.exception(
                "failed_to_get_market_price",
                symbol=symbol.value,
                exchange=exchange.value,
                error=str(e),
            )
            return None
        else:
            if ticker and ticker.price:
                return ticker.price

            # No fallback to midpoint - fail fast if price not available
            self._raise_no_price_error(symbol, exchange)
            return None  # Never reached but makes type checker happy

    def _raise_no_price_error(self, symbol: Symbol, exchange: ExchangeName) -> None:
        """Raise error when no price is available.

        Args:
            symbol: Symbol with no price
            exchange: Exchange checked

        Raises:
            NoPriceAvailableError: Always raised
        """
        raise NoPriceAvailableError(symbol.value, exchange.value)

    def _calculate_average_price(
        self,
        current_price: Decimal | None,
        current_size: Decimal,
        new_price: Decimal,
        new_size: Decimal,
    ) -> Decimal:
        """Calculate weighted average price.

        Args:
            current_price: Current average price
            current_size: Current position size
            new_price: New fill price
            new_size: New fill size

        Returns:
            New weighted average price
        """
        if current_price is None or not current_size:
            return new_price

        total_value = (current_price * current_size) + (new_price * new_size)
        total_size = current_size + new_size

        if not total_size:
            return new_price

        return total_value / total_size

    def _calculate_realized_pnl(
        self,
        position_side: OrderSide,
        position_avg_price: Decimal,
        fill_price: Decimal,
        fill_quantity: Decimal,
    ) -> Decimal:
        """Calculate realized PnL from a reducing fill.

        Args:
            position_side: Current position side
            position_avg_price: Position average entry price
            fill_price: Fill execution price
            fill_quantity: Fill quantity (absolute)

        Returns:
            Realized PnL amount
        """
        if position_side == OrderSide.BUY:
            # Long position, selling to reduce
            pnl = (fill_price - position_avg_price) * fill_quantity
        else:
            # Short position, buying to reduce
            pnl = (position_avg_price - fill_price) * fill_quantity

        return pnl
