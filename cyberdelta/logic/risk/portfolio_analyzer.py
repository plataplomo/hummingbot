"""Portfolio analysis utilities for risk management.

This module provides utilities for analyzing portfolio state,
calculating exposures, and extracting portfolio data.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.models import TradeSignal


logger = get_logger(__name__)


class PortfolioAnalyzer:
    """Portfolio analysis utilities for risk assessment.

    This class handles:
    - Portfolio data extraction
    - Exposure calculations
    - Asset class analysis

    IMPORTANT: Following CODING_STANDARDS.md:
    - Returns Decimal values, NOT float
    - NO assumptions about position structure
    - Uses Symbol objects, NOT strings
    """

    def __init__(self, portfolio_service: PortfolioService) -> None:
        """Initialize portfolio analyzer with dependencies.

        Args:
            portfolio_service: Portfolio service for state access
        """
        self._portfolio_service = portfolio_service

        logger.debug("portfolio_analyzer_initialized")

    async def get_portfolio_data(
        self, signal: TradeSignal
    ) -> tuple[ExchangeName, object, Decimal, Decimal]:
        """Get portfolio data needed for risk assessment.

        Args:
            signal: Trading signal to get data for

        Returns:
            Tuple of (exchange_name, position, total_equity, current_exposure)
        """
        # Get current portfolio state - handle single exchange or list
        exchange_name = signal.exchange[0] if isinstance(signal.exchange, list) else signal.exchange

        position = await self._portfolio_service.get_position(signal.symbol, exchange_name)
        total_equity = await self._portfolio_service.get_total_equity_usd()
        current_exposure = self.calculate_exposure(position, signal.price)

        return exchange_name, position, total_equity, current_exposure

    def calculate_exposure(self, position: object | None, signal_price: Decimal | None) -> Decimal:
        """Calculate current exposure for a position.

        Args:
            position: Current position object (if any)
            signal_price: Current market price

        Returns:
            Exposure value in USD

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about position structure
        """
        if not position or not signal_price:
            return Decimal(0)

        # Get position size if available
        position_size = getattr(position, "size", None)
        if position_size is None or position_size == 0:
            return Decimal(0)

        # Calculate exposure as absolute value * current price
        return abs(Decimal(str(position_size))) * signal_price
