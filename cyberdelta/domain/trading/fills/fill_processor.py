"""Fill processing and trade creation.

This module processes order fills and creates Trade objects with proper
validation following CODING_STANDARDS.md.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class FillProcessor:
    """Processes order fills and creates Fill objects.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses typed Fill model
    - All monetary values as Decimal
    - NO assumptions about fill data structure
    - Explicit validation with clear error messages
    """

    @staticmethod
    def process_fill(
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee: Decimal,
        fee_asset: str,
        trade_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> Fill:
        """Process order fill and create Fill object.

        Args:
            order: Order that was filled
            fill_price: Price at which order was filled
            fill_quantity: Quantity that was filled
            fee: Calculated fee amount
            fee_asset: Asset used for fee payment
            trade_id: Optional trade ID, will generate if not provided
            timestamp: Optional timestamp, will use current time if not provided

        Returns:
            Fill object representing the fill

        Raises:
            ValueError: If order missing exchange_order_id

        Note:
        - Uses typed Fill model
        - All monetary values as Decimal
        - NO assumptions about fill data structure
        """
        # Validate order has exchange_order_id first
        if order.exchange_order_id is None:
            msg = f"Cannot process fill without exchange_order_id: {order.client_order_id}"
            raise ValueError(msg)

        # Use provided trade ID or generate one
        if trade_id is None:
            trade_id = f"trade_{uuid.uuid4().hex[:8]}"

        # Use provided timestamp or current time
        fill_timestamp = timestamp or datetime.now(UTC)

        trade = Fill(
            id=trade_id,
            symbol=order.symbol,
            executed_at=fill_timestamp,
            side=order.side,
            order_id=order.exchange_order_id,
            exchange=order.exchange,
            price=fill_price,
            quantity=fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
            client_order_id=order.client_order_id,
            is_maker=False,  # Default to taker, can be enhanced later
        )

        logger.info(
            "fill_processed_successfully",
            trade_id=trade.id,
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=trade.exchange,
            side=order.side.value if order.side else "unknown",
            price=fill_price,
            quantity=fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
        )

        return trade

    @staticmethod
    def get_fill_sequence_number(order: Order, processed_fills: list[Fill]) -> int:
        """Get sequence number for this fill within the order.

        Args:
            order: Order being filled
            processed_fills: List of previously processed fills

        Returns:
            Sequence number for this fill

        Note:
        - Tracks fill sequence for audit purposes
        - NO assumptions about fill ordering
        """
        # Count existing fills for this order
        order_fills = [
            trade for trade in processed_fills if trade.order_id == order.exchange_order_id
        ]
        return len(order_fills) + 1
