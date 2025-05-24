"""
CyberDeltaEngine: Backpack Account Data Mapper
---------------------------------------------

This module provides the BackpackAccountDataMapper class for transforming
Backpack Raw Account Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Balances to Internal SpotBalance models
- Transform Raw Positions to Internal DerivativePosition models
- Transform Raw Account Summaries to Internal MarginAccountSummary models
- Transform Raw User Fills to Internal Trade models
- Transform WebSocket Account Data events to Internal models

All transformation methods follow the standard pattern:
- Take a validated Raw Pydantic Model as primary input
- Return fully populated Internal Domain Model with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.core.models import (
    BackpackSpotBalanceDetails,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market.trade import BackpackTradeDetails
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class TransformationError(ValueError):
    """Raised when a validated Raw model cannot be transformed to Internal model."""

    pass


class BackpackAccountDataMapper:
    """
    Domain-focused mapper for Backpack account data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to account data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """
        Maps a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell", "Bid", "Ask")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped
        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL

        raise TransformationError(f"Unknown Backpack order side: '{bp_side}'")

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: BackpackRawFill) -> Trade:
        """
        Transforms a BackpackRawFill to an Internal Trade model.

        Args:
            raw_fill: Validated raw fill from Backpack

        Returns:
            Trade: Internal domain model with BP details populated

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Map side
            side = BackpackAccountDataMapper._map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_fill.price, allow_none=False, field_name="price")
            quantity = parse_decimal_value(
                raw_fill.quantity, allow_none=False, field_name="quantity"
            )

            if price is None or quantity is None:
                raise TransformationError("Price and quantity are required for trade")

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_fill.timestamp, field_name="timestamp")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(raw_fill.fee, allow_none=True, field_name="fee") or Decimal(
                "0"
            )

            # Create BP-specific details
            details = BackpackTradeDetails(
                system_order_type=None  # Not available in fill data
            )

            return Trade(
                id=str(raw_fill.trade_id),
                symbol=raw_fill.symbol,
                executed_at=executed_at,
                side=side,
                order_id=raw_fill.order_id,
                exchange=ExchangeName.BACKPACK.value,
                client_order_id=raw_fill.client_id,
                price=price,
                quantity=quantity,
                fee=fee,
                fee_asset=raw_fill.fee_symbol,
                is_maker=raw_fill.is_maker,
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(f"Failed to transform BackpackRawFill to Trade: {e}") from e

    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str, total_balance: str, available_balance: str
    ) -> SpotBalance:
        """
        Transforms balance data to an Internal SpotBalance model.

        Args:
            asset: Asset symbol
            total_balance: Total balance as string
            available_balance: Available balance as string

        Returns:
            SpotBalance: Internal domain model with BP details populated

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse balances
            total = parse_decimal_value(total_balance, allow_none=False, field_name="total_balance")
            available = parse_decimal_value(
                available_balance, allow_none=False, field_name="available_balance"
            )

            if total is None or available is None:
                raise TransformationError("Total and available balances are required")

            # Create BP-specific details
            details = BackpackSpotBalanceDetails()

            return SpotBalance(
                asset=asset,
                exchange=ExchangeName.BACKPACK.value,
                total_quantity=total,
                available_quantity=available,
                timestamp=datetime.now(UTC),
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform balance data to SpotBalance: {e}"
            ) from e
