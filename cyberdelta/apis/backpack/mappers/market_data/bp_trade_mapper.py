"""Backpack Fill Mapper.

This mapper handles transformations for trade-related data from the Backpack exchange.

Focused on:
- Public trade data transformations
- Recent trade data transformations
- WebSocket trade event transformations
- Fill-specific data validation and error handling
"""

from datetime import UTC, datetime
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import FillMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin, ValidationMixin
from cyberdelta.apis.exceptions import FillTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Fill
from cyberdelta.models.market.fill import BackpackFillDetails
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackFillMapper(CommonDataParserMixin, ValidationMixin, FillMapperProtocol):
    """Focused mapper for Backpack trade data transformations.

    This class contains static methods for transforming validated Backpack Raw trade models
    into CyberDeltaEngine Internal Fill Domain Models.
    """

    def _validate_trade_data(
        self,
        price: object,
        quantity: object,
        context: str,
    ) -> tuple[object, object]:
        """Validate trade price and quantity data.

        Args:
            price: Raw price value
            quantity: Raw quantity value
            context: Context for error messages

        Returns:
            tuple[object, object]: Validated price and quantity

        Raises:
            FillTransformationError: If price or quantity validation fails
        """
        # Type assertion: ensure price is compatible with parse_decimal_value
        if not isinstance(price, (str, float, int, type(None))):
            # Convert to string for parsing
            price = str(price) if price is not None else None
        parsed_price = self.parse_decimal_safely(price)
        if parsed_price is None:
            raise FillTransformationError(
                fill_source=context, reason=f"Invalid price value: {price}"
            )

        # Type assertion: ensure quantity is compatible with parse_decimal_value
        if not isinstance(quantity, (str, float, int, type(None))):
            # Convert to string for parsing
            quantity = str(quantity) if quantity is not None else None
        parsed_quantity = self.parse_decimal_safely(quantity)
        if parsed_quantity is None:
            raise FillTransformationError(
                fill_source=context, reason=f"Invalid quantity value: {quantity}"
            )

        return parsed_price, parsed_quantity

    def transform_raw_fill_to_internal(self, raw_fill: BackpackRawPublicTrade) -> Fill:
        """Transform a BackpackRawPublicTrade to an Internal Fill model.

        Args:
            raw_fill: Validated raw trade data from Backpack

        Returns:
            Fill: Internal domain model with populated fields and BP details

        Raises:
            FillTransformationError: If transformation fails

        """
        try:
            # Validate trade fields
            price, quantity = self._validate_trade_data(
                raw_fill.price,
                raw_fill.quantity,
                "BackpackRawPublicTrade",
            )

            # Parse timestamp
            executed_at = self.parse_timestamp(raw_fill.time)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_fill.symbol,
            )

            # Create BP-specific details
            details = BackpackFillDetails()

            # Use secure_transform for type-safe model creation
            trade_data: dict[str, Any] = {
                "id": raw_fill.id,
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                # BackpackRawPublicFill doesn't have side, default to BUY
                "side": OrderSide.BUY.value,
                "order_id": raw_fill.order_id,
                "exchange": ExchangeName.BACKPACK.value,
                "price": str(price),
                "quantity": str(quantity),
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Fill,
                context="backpack_public_trade_transform",
                source_exchange=ExchangeName.BACKPACK.value,
            )

        except Exception as e:
            raise FillTransformationError(
                fill_source="BackpackRawPublicTrade",
                reason=str(e),
                symbol=raw_fill.symbol,
                fill_id=raw_fill.id,
                original_error=e,
            ) from e

    def transform_raw_recent_fill_to_internal(
        self,
        raw_fill: BackpackRawRecentPublicTrade,
        symbol: Symbol,
    ) -> Fill:
        """Transform a BackpackRawRecentPublicTrade to an Internal Fill model.

        Args:
            raw_fill: Validated raw recent trade data from Backpack
            symbol: Symbol for the fill (not included in recent fill response)

        Returns:
            Fill: Internal domain model with populated fields and BP details

        Raises:
            FillTransformationError: If transformation fails

        """
        try:
            # Validate trade fields
            price, quantity = self._validate_trade_data(
                raw_fill.price,
                raw_fill.quantity,
                "BackpackRawRecentPublicTrade",
            )

            # Parse timestamp
            executed_at = self.parse_timestamp(raw_fill.timestamp)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Determine side from is_buyer_maker: if buyer is maker, then this fill is a sell
            # (taker sold to maker)
            # If buyer is not maker, then this fill is a buy (taker bought from maker)
            side = OrderSide.SELL if raw_fill.is_buyer_maker else OrderSide.BUY

            # Use the Symbol object directly (already a domain object)
            exchange_symbol = symbol

            # Create BP-specific details
            details = BackpackFillDetails()

            # Use secure_transform for type-safe model creation
            trade_data: dict[str, Any] = {
                "id": str(raw_fill.id),  # Convert int ID to string
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": "PUBLIC_TRADE",  # Not available in recent fills response
                "exchange": ExchangeName.BACKPACK.value,
                "price": str(price),
                "quantity": str(quantity),
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Fill,
                context="backpack_recent_trade_transform",
                source_exchange=ExchangeName.BACKPACK.value,
            )

        except Exception as e:
            raise FillTransformationError(
                fill_source="BackpackRawRecentPublicTrade",
                reason=str(e),
                symbol=symbol.value,
                fill_id=str(raw_fill.id),
                original_error=e,
            ) from e

    def transform_ws_fill_event_to_internal_fill(
        self, raw_fill: BackpackRawPublicTradeEvent
    ) -> Fill:
        """Transform a BackpackRawPublicTradeEvent to an Internal Fill model.

        Args:
            raw_fill: Validated raw trade event data from Backpack WebSocket

        Returns:
            Fill: Internal domain model with populated fields and BP details

        Raises:
            FillTransformationError: If transformation fails

        """
        try:
            # Validate trade fields
            price, quantity = self._validate_trade_data(
                raw_fill.price,
                raw_fill.quantity,
                "BackpackRawPublicTradeEvent",
            )

            # BackpackRawPublicFillEvent doesn't have side info, need to determine from order IDs
            # For now, default to BUY (this would need to be enhanced based on maker/taker info)
            side = OrderSide.BUY if raw_fill.is_buyer_the_maker else OrderSide.SELL

            # Parse timestamp from event_time
            executed_at = self.parse_timestamp(raw_fill.event_time)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_fill.symbol,
            )

            # Create BP-specific details
            details = BackpackFillDetails()

            # Use secure_transform for type-safe model creation
            trade_data: dict[str, Any] = {
                "id": raw_fill.trade_id,
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": raw_fill.buyer_order_id,  # Choose buyer order ID as primary
                "exchange": ExchangeName.BACKPACK.value,
                "price": str(price),
                "quantity": str(quantity),
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Fill,
                context="backpack_ws_trade_transform",
                source_exchange=ExchangeName.BACKPACK.value,
            )

        except Exception as e:
            raise FillTransformationError(
                fill_source="BackpackRawPublicTradeEvent",
                reason=str(e),
                symbol=raw_fill.symbol,
                fill_id=raw_fill.trade_id,
                original_error=e,
            ) from e
