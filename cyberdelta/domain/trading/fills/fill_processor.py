"""Fill processing and trade creation.

This module processes order fills and creates Trade objects with proper
validation following CODING_STANDARDS.md.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade


logger = get_logger(__name__)


class FillProcessor:
    """Processes order fills and creates Trade objects.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses typed Trade model
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
        fill_data: dict[str, object],
    ) -> Trade:
        """Process order fill and create Trade object.

        Args:
            order: Order that was filled
            fill_price: Price at which order was filled
            fill_quantity: Quantity that was filled
            fee: Calculated fee amount
            fee_asset: Asset used for fee payment
            fill_data: Additional fill data from exchange

        Returns:
            Trade object representing the fill

        Raises:
            ValueError: If order missing exchange_order_id

        Note:
        - Uses typed Trade model
        - All monetary values as Decimal
        - NO assumptions about fill data structure
        """
        # Extract trade ID from fill data
        trade_id = fill_data.get("trade_id")
        if not isinstance(trade_id, str):
            trade_id = f"fill_{order.exchange_order_id}_{uuid.uuid4().hex[:8]}"

        # Extract timestamp from fill data
        fill_timestamp = fill_data.get("timestamp", datetime.now(UTC))
        if not isinstance(fill_timestamp, datetime):
            fill_timestamp = datetime.now(UTC)

        # Create Trade object with all calculated values
        if order.exchange_order_id is None:
            msg = f"Cannot process fill without exchange_order_id: {order.client_order_id}"
            raise ValueError(msg)

        trade = Trade(
            id=trade_id,
            symbol=order.symbol,
            executed_at=fill_timestamp,
            side=order.side,
            order_id=order.exchange_order_id,
            exchange=order.exchange.value,
            price=fill_price,
            quantity=fill_quantity,
            fee=fee,
            fee_asset=fee_asset,
            client_order_id=order.client_order_id,
            is_maker=fill_data.get("liquidity", "taker") == "maker",
        )

        logger.info(
            "fill_processed_successfully",
            trade_id=trade.id,
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=trade.exchange,
            side=order.side.value if order.side else "unknown",
            price=float(fill_price),
            quantity=float(fill_quantity),
            fee=float(fee),
            fee_asset=fee_asset,
        )

        return trade

    @staticmethod
    def validate_fill_data(fill_data: dict[str, object]) -> None:
        """Validate fill data structure and required fields.

        Args:
            fill_data: Fill data to validate

        Raises:
            ValueError: If fill data is invalid

        Note:
        - NO assumptions about fill data structure
        - Explicit validation with clear error messages
        """
        required_fields = ["fill_price", "filled_quantity"]

        for field in required_fields:
            if field not in fill_data:
                msg = f"Missing required fill data field: {field}"
                raise ValueError(msg)

            value = fill_data[field]
            if value is None:
                msg = f"Fill data field {field} cannot be None"
                raise ValueError(msg)

        # Validate numeric fields
        try:
            price = Decimal(str(fill_data["fill_price"]))
            quantity = Decimal(str(fill_data["filled_quantity"]))

            if price <= 0:
                FillProcessor._raise_invalid_price_error(price)
            if quantity <= 0:
                FillProcessor._raise_invalid_quantity_error(quantity)

        except (ValueError, TypeError) as e:
            msg = f"Invalid numeric values in fill data: {e}"
            raise ValueError(msg) from e

    @staticmethod
    def _raise_invalid_price_error(price: Decimal) -> None:
        """Raise error for invalid fill price.

        Raises:
            ValueError: Price is not positive
        """
        msg = f"Fill price must be positive: {price}"
        raise ValueError(msg)

    @staticmethod
    def _raise_invalid_quantity_error(quantity: Decimal) -> None:
        """Raise error for invalid fill quantity.

        Raises:
            ValueError: Quantity is not positive
        """
        msg = f"Fill quantity must be positive: {quantity}"
        raise ValueError(msg)

    @staticmethod
    def get_fill_sequence_number(order: Order, processed_fills: list[Trade]) -> int:
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
