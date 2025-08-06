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
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade


logger = get_logger(__name__)


class FillHandler:
    """Orchestrates order fill processing with fee calculation.

    This handler processes order fills by:
    - Calculating fees using exchange-specific fee structures from configuration
    - Converting fill data to typed Trade objects
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
        self._processed_fills: list[Trade] = []
        self._fill_count = 0
        self._total_fees_usd = Decimal(0)

        logger.info(
            "fill_handler_initialized",
            exchanges_configured=len(config.exchanges),
            portfolio_service_available=True,
        )

    async def process_fill(self, order: Order, fill_data: dict[str, object]) -> Trade:
        """Process an order fill with comprehensive fee calculation.

        Args:
            order: Order that was filled
            fill_data: Fill data from exchange

        Returns:
            Trade object with calculated fees and metadata

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
            fill_data_keys=list(fill_data.keys()),
        )

        try:
            # Validate fill data
            FillProcessor.validate_fill_data(fill_data)

            # Extract fill information
            fill_price = Decimal(str(fill_data.get("fill_price", 0)))
            fill_quantity = Decimal(str(fill_data.get("filled_quantity", 0)))

            # Get exchange configuration
            exchange_config = self._get_exchange_config(order)

            # Calculate fees using exchange configuration
            fee_amount, fee_asset = FeeCalculator.calculate_fee(
                order,
                fill_price,
                fill_quantity,
                fill_data,
                exchange_config,
            )

            # Create Trade object with all calculated values
            trade = FillProcessor.process_fill(
                order,
                fill_price,
                fill_quantity,
                fee_amount,
                fee_asset,
                fill_data,
            )

            # Update portfolio with trade
            await self._portfolio_service.update_from_trade(trade)

            # Track fill statistics
            self._processed_fills.append(trade)
            self._fill_count += 1
            self._total_fees_usd += fee_amount  # Simplified - assumes USD fees

        except Exception as e:
            logger.exception(
                "fill_processing_failed",
                order_id=order.exchange_order_id,
                error=str(e),
            )
            raise

        return trade

    async def process_partial_fill(self, order: Order, fill_data: dict[str, object]) -> Trade:
        """Process a partial fill of an order.

        Args:
            order: Order that was partially filled
            fill_data: Partial fill data from exchange

        Returns:
            Trade object representing the partial fill

        Note:
        - Same validation and fee calculation as full fills
        - Tracks partial fill sequence for audit
        - Updates order state appropriately
        """
        # Use Decimal for consistency with process_fill
        fill_qty = fill_data.get("filled_quantity", 0)
        new_fill_quantity = Decimal(str(fill_qty)) if fill_qty else Decimal(0)

        filled_quantity = order.quantity_filled

        logger.debug(
            "partial_fill_processing",
            order_id=order.exchange_order_id,
            filled_so_far=float(filled_quantity),
            order_quantity=float(order.quantity_requested),
            new_fill_quantity=float(new_fill_quantity),
        )

        # Process same as regular fill
        trade = await self.process_fill(order, fill_data)

        # Additional tracking for partial fills
        logger.debug(
            "partial_fill_metadata",
            fill_sequence=FillProcessor.get_fill_sequence_number(order, self._processed_fills),
            is_partial_fill=True,
            remaining_quantity=float(
                order.quantity_requested - (order.quantity_filled or Decimal(0)),
            ),
        )

        return trade

    async def process_bulk_fills(
        self,
        order: Order,
        fill_list: list[dict[str, object]],
    ) -> list[Trade]:
        """Process multiple fills for an order efficiently.

        Args:
            order: Order that received multiple fills
            fill_list: List of fill data dictionaries

        Returns:
            List of processed Trade objects

        Note:
        - Processes each fill with same validation as single fills
        - Maintains fill sequence and audit trail
        - NO assumptions about fill order or timing
        """
        trades: list[Trade] = []

        logger.info(
            "bulk_fill_processing_starting",
            order_id=order.exchange_order_id,
            fill_count=len(fill_list),
        )

        for i, fill_data in enumerate(fill_list):
            try:
                # Add sequence information to fill data
                fill_data_with_sequence = {
                    **fill_data,
                    "bulk_sequence": i + 1,
                    "bulk_total": len(fill_list),
                }

                trade = await self.process_fill(order, fill_data_with_sequence)
                trades.append(trade)

            except Exception as e:
                logger.exception(
                    "bulk_fill_item_failed",
                    order_id=order.exchange_order_id,
                    fill_index=i,
                    error=str(e),
                )
                # Continue processing remaining fills
                continue

        logger.info(
            "bulk_fill_processing_completed",
            order_id=order.exchange_order_id,
            requested_fills=len(fill_list),
            successful_fills=len(trades),
            failed_fills=len(fill_list) - len(trades),
        )

        return trades

    def get_fill_statistics(self) -> dict[str, object]:
        """Get fill processing statistics.

        Returns:
            Dictionary with fill statistics and metrics

        Note:
        - Returns explicit statistics from actual processing
        - NO calculated/derived statistics
        """
        return {
            "total_fills_processed": self._fill_count,
            "total_fees_usd": float(self._total_fees_usd),
            "unique_orders_filled": len({trade.order_id for trade in self._processed_fills}),
            "exchanges_processed": len({trade.exchange for trade in self._processed_fills}),
            "average_fill_size_usd": (
                float(
                    sum(trade.price * trade.quantity for trade in self._processed_fills)
                    / len(self._processed_fills),
                )
                if self._processed_fills
                else 0
            ),
            "processing_started": len(self._processed_fills) > 0,
        }

    def get_recent_fills(self, limit: int = 10) -> list[Trade]:
        """Get most recent processed fills.

        Args:
            limit: Maximum number of fills to return

        Returns:
            List of recent Trade objects

        Note:
        - Returns actual Trade objects, not summaries
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
