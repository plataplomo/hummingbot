"""Backpack-specific enum mapping utilities.

This module contains common enum mapping functions specific to Backpack exchange
that are shared across multiple mappers to avoid code duplication.
"""

from cyberdelta.apis.exceptions.data_transformation import UnknownEnumError
from cyberdelta.apis.exceptions.trading_transformation import UnknownOrderSideError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName, OrderSide


logger = get_logger(__name__)


class BackpackEnumMappers:
    """Common enum mapping utilities for Backpack exchange."""

    @staticmethod
    def map_side_to_internal(bp_side: str, error_type: str = "standard") -> OrderSide:
        """Map a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell", "Bid", "Ask")
            error_type: Type of error to raise ("standard", "order", "position")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            UnknownEnumError: If side cannot be mapped (error_type="standard")
            UnknownOrderSideError: If side cannot be mapped (error_type="order")
            Returns OrderSide.BUY with warning (error_type="position")
        """
        side_lower = bp_side.lower() if bp_side else ""

        # Position mapper supports additional values
        if error_type == "position":
            if side_lower in {"buy", "bid", "long"}:
                return OrderSide.BUY
            if side_lower in {"sell", "ask", "short"}:
                return OrderSide.SELL

            logger.warning(
                "unknown_position_side",
                bp_side=bp_side,
                mapped_to="BUY",
                message="Unknown Backpack position side encountered, defaulting to BUY",
            )
            return OrderSide.BUY

        # Standard mapping for orders and transactions
        if side_lower in {"buy", "bid"}:
            return OrderSide.BUY
        if side_lower in {"sell", "ask"}:
            return OrderSide.SELL

        # Different error types based on context
        if error_type == "order":
            raise UnknownOrderSideError(bp_side, exchange=ExchangeName.BACKPACK)

        raise UnknownEnumError(
            enum_type="Backpack order side",
            value=bp_side,
            valid_values=["buy", "sell", "bid", "ask", "Buy", "Sell", "Bid", "Ask"],
        )
