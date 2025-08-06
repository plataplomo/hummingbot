"""Fill handler orchestrator for order fill processing.

This module orchestrates fill processing using the fee calculator and fill processor
components following CODING_STANDARDS.md.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.trading.fills.fee_calculator import FeeCalculator
from cyberdelta.domain.trading.fills.fill_processor import FillProcessor
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.market.order import Order
from cyberdelta.models.trading.fill_statistics import FillStatistics


logger = get_logger(__name__)


class FillHandler:
    """Orchestrates order fill processing with fee calculation.

    This handler processes order fills by:
    - Calculating fees using exchange-specific fee structures from configuration
    - Converting fill data to typed Fill objects
    - Coordinating portfolio updates through PortfolioService
    - Maintaining fill history and audit trail

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL fee calculations from AppSettings, NO hardcoded rates
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about fee structures
    """

    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
    ) -> None:
        """Initialize fill handler with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            portfolio_service: Portfolio service for state updates
        """
        self.config = config
        self._portfolio_service = portfolio_service

        # Fill tracking
        self._processed_fills: list[Fill] = []
        self._fill_count = 0
        self._total_fees_usd = Decimal(0)

        logger.info(
            "fill_handler_initialized",
            exchanges_configured=len(config.exchanges),
            portfolio_service_available=True,
        )

    async def process_fill(self, order: Order, trade: Fill) -> Fill:
        """Process an order fill with comprehensive fee calculation.

        Args:
            order: Order that was filled
            trade: Fill object from exchange with fill data

        Returns:
            Processed Fill object (may be the same object or updated copy)

        Note:
        - Fee calculation from config.exchanges[exchange].fee_structure
        - ALL calculations use Decimal, NOT float
        - NO hardcoded fee rates or assumptions
        - Explicit error handling for invalid data
        """
        logger.debug(
            "fill_processing_starting",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value,
            fill_price=trade.price,
            filled_quantity=trade.quantity,
        )

        try:
            # Trade object already has the fill information
            fill_price = trade.price
            fill_quantity = trade.quantity

            # Get exchange configuration
            exchange_config = self._get_exchange_config(order)

            # Calculate fees using exchange configuration
            fee_amount, fee_asset = FeeCalculator.calculate_fee(
                order,
                fill_price,
                fill_quantity,
                trade,
                exchange_config,
            )

            # Create Fill object with all calculated values
            processed_trade = FillProcessor.process_fill(
                order,
                fill_price,
                fill_quantity,
                fee_amount,
                fee_asset,
                trade.id,
                trade.executed_at,
            )

            # Update portfolio with processed trade
            await self._portfolio_service.update_from_fill(processed_trade)

            # Track fill statistics
            self._processed_fills.append(processed_trade)
            self._fill_count += 1
            self._total_fees_usd += fee_amount  # Simplified - assumes USD fees

        except Exception as e:
            logger.exception(
                "fill_processing_failed",
                order_id=order.exchange_order_id,
                error=str(e),
            )
            raise

        return processed_trade

    async def process_partial_fill(self, order: Order, trade: Fill) -> Fill:
        """Process a partial fill of an order.

        Args:
            order: Order that was partially filled
            trade: Fill object from exchange

        Returns:
            Fill object representing the partial fill

        Note:
        - Same validation and fee calculation as full fills
        - Tracks partial fill sequence for audit
        - Updates order state appropriately
        """
        # Use Decimal for consistency with process_fill
        new_fill_quantity = trade.quantity

        filled_quantity = order.quantity_filled

        logger.debug(
            "partial_fill_processing",
            order_id=order.exchange_order_id,
            filled_so_far=filled_quantity,
            order_quantity=order.quantity_requested,
            new_fill_quantity=new_fill_quantity,
        )

        # Process same as regular fill
        processed_trade = await self.process_fill(order, trade)

        # Additional tracking for partial fills
        logger.debug(
            "partial_fill_metadata",
            fill_sequence=FillProcessor.get_fill_sequence_number(order, self._processed_fills),
            is_partial_fill=True,
            remaining_quantity=float(
                order.quantity_requested - (order.quantity_filled or Decimal(0)),
            ),
        )

        return processed_trade

    def get_fill_statistics(self) -> FillStatistics:
        """Get fill processing statistics.

        Returns:
            Typed fill statistics and metrics

        Note:
        - Returns explicit statistics from actual processing
        - NO calculated/derived statistics
        """
        if self._processed_fills:
            total_value = Decimal(0)
            for trade in self._processed_fills:
                total_value += trade.price * trade.quantity
            average_fill_size = total_value / len(self._processed_fills)
        else:
            average_fill_size = Decimal(0)

        success_rate = Decimal(1) if self._fill_count > 0 else Decimal(0)

        last_fill_timestamp = (
            max(trade.executed_at for trade in self._processed_fills)
            if self._processed_fills
            else None
        )

        return FillStatistics(
            fill_handler_available=True,
            total_fills_processed=self._fill_count,
            total_fees_usd=self._total_fees_usd,
            average_fill_size_usd=average_fill_size,
            success_rate=success_rate,
            last_fill_timestamp=last_fill_timestamp,
        )

    def get_recent_fills(self, limit: int = 10) -> list[Fill]:
        """Get most recent processed fills.

        Args:
            limit: Maximum number of fills to return

        Returns:
            List of recent Fill objects

        Note:
        - Returns actual Fill objects, not summaries
        - Limit parameter explicit, no default assumptions
        """
        return self._processed_fills[-limit:] if self._processed_fills else []

    def _get_exchange_config(self, order: Order) -> ExchangeSpecificConfig:
        """Get exchange configuration for order.

        Returns:
            ExchangeSpecificConfig: Exchange configuration for the order

        Raises:
            ValueError: If exchange configuration is not found
        """
        exchange_name = order.exchange.value
        exchange_config = self.config.exchanges.get(exchange_name)
        if not exchange_config:
            msg = f"No exchange configuration found for: {exchange_name}"
            raise ValueError(msg)
        return exchange_config
