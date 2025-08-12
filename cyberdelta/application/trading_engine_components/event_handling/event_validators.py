"""Event validators for ensuring event data integrity in the trading engine."""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide
from cyberdelta.enums.signals import SignalType
from cyberdelta.enums.trading import TradingAction
from cyberdelta.models.events import OrderEvent, SignalEvent


logger = get_logger(__name__)


class EventValidator:
    """Validates and extracts data from events with type safety."""

    def validate_event_required_fields(self, event: SignalEvent) -> bool:
        """Validate required fields are present in the event.

        Args:
            event: Signal event to validate

        Returns:
            True if all required fields are present, False otherwise
        """
        if not event.symbol:
            logger.error(
                "strategy_signal_event_missing_symbol",
                signal_id=event.signal_id,
                message="Signal events must include valid symbol",
            )
            return False

        if not event.exchange:
            logger.error(
                "strategy_signal_event_missing_exchange",
                signal_id=event.signal_id,
                message="Signal events must include valid exchange",
            )
            return False

        return True

    def extract_and_validate_price(self, event: SignalEvent) -> Decimal | None:
        """Extract and validate price from event.

        Args:
            event: Signal event containing price data

        Returns:
            Validated Decimal price, or None if invalid
        """
        price = event.target_price
        if price is None:
            logger.error(
                "strategy_signal_event_missing_price",
                signal_id=event.signal_id,
                message="Signal events must include valid price data",
            )
            return None

        if price <= Decimal(0):
            logger.error(
                "strategy_signal_event_invalid_price",
                signal_id=event.signal_id,
                price=str(price),
                message="Price must be positive",
            )
            return None

        return price

    def extract_signal_side(self, event: SignalEvent) -> OrderSide:
        """Extract order side from event with type safety.

        Args:
            event: Signal event containing side data

        Returns:
            Validated OrderSide enum value

        Raises:
            ValueError: If side cannot be determined from action
        """
        action = event.action
        # Map TradingAction to OrderSide - only unambiguous mappings
        action_to_side = {
            TradingAction.BUY: OrderSide.BUY,
            TradingAction.SELL: OrderSide.SELL,
        }

        mapped_side = action_to_side.get(action)
        if mapped_side is None:
            logger.error(
                "strategy_signal_event_unmappable_action",
                signal_id=event.signal_id,
                action=action.value if action else "unknown",
                message="SignalEvent action cannot be mapped to OrderSide",
            )
            msg = f"SignalEvent {event.signal_id} action {action} cannot be mapped to OrderSide"
            raise ValueError(msg)

        return mapped_side

    def extract_signal_type(self, event: SignalEvent) -> SignalType:
        """Extract signal type from event with type safety.

        Args:
            event: Signal event containing signal type data

        Returns:
            Validated SignalType enum value

        Raises:
            ValueError: If action cannot be mapped to signal type
        """
        action_enum = event.action
        # Map action to SignalType - ONLY unambiguous mappings
        action_to_signal_type = {
            TradingAction.BUY: SignalType.ENTER_LONG,
            TradingAction.SELL: SignalType.ENTER_SHORT,
            # TradingAction.HOLD: ambiguous - could be hold long or hold short
            # TradingAction.CLOSE: ambiguous - could be exit long or exit short
        }

        mapped_signal_type = action_to_signal_type.get(action_enum)
        if mapped_signal_type is None:
            logger.error(
                "strategy_signal_event_ambiguous_action",
                signal_id=event.signal_id,
                action=action_enum.value,
                message="SignalEvent action is ambiguous and cannot be safely mapped to SignalType",
            )
            msg = (
                f"SignalEvent {event.signal_id} action {action_enum.value} "
                f"is ambiguous and cannot be safely mapped to SignalType"
            )
            raise ValueError(msg)

        return mapped_signal_type

    def validate_fill_event_data(self, event: OrderEvent) -> bool:
        """Validate required fields for fill event processing.

        Args:
            event: Order event to validate

        Returns:
            True if all required fields are present, False otherwise
        """
        if not event.symbol:
            logger.error(
                "order_filled_event_missing_symbol",
                order_id=event.order_id,
                message="Fill events must include valid symbol",
            )
            return False

        if not event.exchange:
            logger.error(
                "order_filled_event_missing_exchange",
                order_id=event.order_id,
                message="Fill events must include valid exchange",
            )
            return False

        return True

    def extract_fill_price(self, event: OrderEvent) -> Decimal | None:
        """Extract and validate fill price from event.

        Args:
            event: Order event containing fill price data

        Returns:
            Validated Decimal price, or None if invalid
        """
        # Require 'fill_price' - fail fast if missing
        price = event.fill_price
        if price is None:
            logger.error(
                "order_filled_event_missing_price",
                order_id=event.order_id,
                message="Fill events must include valid fill_price",
            )
            return None

        if price <= Decimal(0):
            logger.error(
                "order_filled_event_invalid_price",
                order_id=event.order_id,
                price=str(price),
                message="Fill price must be positive",
            )
            return None

        return price

    def extract_fill_quantity(self, event: OrderEvent) -> Decimal | None:
        """Extract and validate fill quantity from event.

        Args:
            event: Order event containing fill quantity data

        Returns:
            Validated Decimal quantity, or None if invalid
        """
        quantity = event.fill_quantity
        if quantity is None:
            logger.error(
                "order_filled_event_missing_quantity",
                order_id=event.order_id,
                message="Fill events must include valid fill_quantity",
            )
            return None

        if quantity <= Decimal(0):
            logger.error(
                "order_filled_event_invalid_quantity",
                order_id=event.order_id,
                quantity=str(quantity),
                message="Fill quantity must be positive",
            )
            return None

        return quantity
