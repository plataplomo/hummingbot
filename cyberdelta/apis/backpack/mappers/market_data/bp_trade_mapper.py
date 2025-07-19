"""Backpack Trade Mapper.

This mapper handles transformations for trade-related data from the Backpack exchange.

Focused on:
- Public trade data transformations
- Recent trade data transformations
- WebSocket trade event transformations
- Trade-specific data validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import TradeMapperProtocol
from cyberdelta.apis.exceptions import TradeTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderSide
from cyberdelta.core.models import Trade
from cyberdelta.core.models.market.trade import BackpackTradeDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackTradeMapper(TradeMapperProtocol):
    """Focused mapper for Backpack trade data transformations.

    This class contains static methods for transforming validated Backpack Raw trade models
    into CyberDeltaEngine Internal Trade Domain Models.
    """

    @staticmethod
    def _validate_trade_data(
        price: object, quantity: object, context: str
    ) -> tuple[object, object]:
        """Validate trade price and quantity data.

        Args:
            price: Raw price value
            quantity: Raw quantity value
            context: Context for error messages

        Returns:
            tuple[object, object]: Validated price and quantity

        Raises:
            MissingRequiredFieldError: If required fields are missing
        """
        # Type assertion: ensure price is compatible with parse_decimal_value
        if not isinstance(price, (str, float, int, type(None))):
            # Convert to string for parsing
            price = str(price) if price is not None else None
        parsed_price = parse_decimal_value(price, allow_none=False, field_name="price")

        # Type assertion: ensure quantity is compatible with parse_decimal_value
        if not isinstance(quantity, (str, float, int, type(None))):
            # Convert to string for parsing
            quantity = str(quantity) if quantity is not None else None
        parsed_quantity = parse_decimal_value(
            quantity,
            allow_none=False,
            field_name="quantity",
        )

        return parsed_price, parsed_quantity

    @staticmethod
    def transform_raw_trade_to_internal(raw_trade: BackpackRawPublicTrade) -> Trade:
        """Transform a BackpackRawPublicTrade to an Internal Trade model.

        Args:
            raw_trade: Validated raw trade data from Backpack

        Returns:
            Trade: Internal domain model with populated fields and BP details

        Raises:
            TradeTransformationError: If transformation fails

        """
        try:
            # Validate trade fields
            price, quantity = BackpackTradeMapper._validate_trade_data(
                raw_trade.price, raw_trade.quantity, "BackpackRawPublicTrade"
            )

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_trade.time, field_name="time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create BP-specific details
            details = BackpackTradeDetails()

            # Use secure_transform for type-safe model creation
            trade_data: dict[str, Any] = {
                "id": raw_trade.id,
                "symbol": raw_trade.symbol,
                "executed_at": executed_at.isoformat(),
                # BackpackRawPublicTrade doesn't have side, default to BUY
                "side": OrderSide.BUY.value,
                "order_id": raw_trade.order_id,
                "exchange": ExchangeName.BACKPACK.value,
                "price": str(price),
                "quantity": str(quantity),
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="backpack_public_trade_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TradeTransformationError(
                trade_source="BackpackRawPublicTrade",
                reason=str(e),
                symbol=raw_trade.symbol,
                trade_id=raw_trade.id,
                original_error=e,
            ) from e

    @staticmethod
    def transform_raw_recent_trade_to_internal(
        raw_trade: BackpackRawRecentPublicTrade,
        symbol: str,
    ) -> Trade:
        """Transform a BackpackRawRecentPublicTrade to an Internal Trade model.

        Args:
            raw_trade: Validated raw recent trade data from Backpack
            symbol: Symbol for the trade (not included in recent trade response)

        Returns:
            Trade: Internal domain model with populated fields and BP details

        Raises:
            TradeTransformationError: If transformation fails

        """
        try:
            # Validate trade fields
            price, quantity = BackpackTradeMapper._validate_trade_data(
                raw_trade.price, raw_trade.quantity, "BackpackRawRecentPublicTrade"
            )

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_trade.timestamp, field_name="timestamp")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Determine side from is_buyer_maker: if buyer is maker, then this trade is a sell
            # (taker sold to maker)
            # If buyer is not maker, then this trade is a buy (taker bought from maker)
            side = OrderSide.SELL if raw_trade.is_buyer_maker else OrderSide.BUY

            # Create BP-specific details
            details = BackpackTradeDetails()

            # Use secure_transform for type-safe model creation
            trade_data: dict[str, Any] = {
                "id": str(raw_trade.id),  # Convert int ID to string
                "symbol": symbol,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": "PUBLIC_TRADE",  # Not available in recent trades response
                "exchange": ExchangeName.BACKPACK.value,
                "price": str(price),
                "quantity": str(quantity),
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="backpack_recent_trade_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TradeTransformationError(
                trade_source="BackpackRawRecentPublicTrade",
                reason=str(e),
                symbol=symbol,
                trade_id=str(raw_trade.id),
                original_error=e,
            ) from e

    @staticmethod
    def transform_ws_trade_event_to_internal(raw_trade: BackpackRawPublicTradeEvent) -> Trade:
        """Transform a BackpackRawPublicTradeEvent to an Internal Trade model.

        Args:
            raw_trade: Validated raw trade event data from Backpack WebSocket

        Returns:
            Trade: Internal domain model with populated fields and BP details

        Raises:
            TradeTransformationError: If transformation fails

        """
        try:
            # Validate trade fields
            price, quantity = BackpackTradeMapper._validate_trade_data(
                raw_trade.price, raw_trade.quantity, "BackpackRawPublicTradeEvent"
            )

            # BackpackRawPublicTradeEvent doesn't have side info, need to determine from order IDs
            # For now, default to BUY (this would need to be enhanced based on maker/taker info)
            side = OrderSide.BUY if raw_trade.is_buyer_the_maker else OrderSide.SELL

            # Parse timestamp from event_time
            executed_at = parse_datetime_utc(raw_trade.event_time, field_name="event_time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create BP-specific details
            details = BackpackTradeDetails()

            # Use secure_transform for type-safe model creation
            trade_data: dict[str, Any] = {
                "id": raw_trade.trade_id,
                "symbol": raw_trade.symbol,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": raw_trade.buyer_order_id,  # Choose buyer order ID as primary
                "exchange": ExchangeName.BACKPACK.value,
                "price": str(price),
                "quantity": str(quantity),
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="backpack_ws_trade_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TradeTransformationError(
                trade_source="BackpackRawPublicTradeEvent",
                reason=str(e),
                symbol=raw_trade.symbol,
                trade_id=raw_trade.trade_id,
                original_error=e,
            ) from e

    # MapperProtocol methods
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Parse decimal values safely using BackpackCommonMappers."""
        return BackpackCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize symbol format using BackpackCommonMappers."""
        return BackpackCommonMappers.normalize_symbol(symbol)

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Denormalize symbol format using BackpackCommonMappers."""
        return BackpackCommonMappers.denormalize_symbol(symbol)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert timestamp to datetime using BackpackCommonMappers."""
        return BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
