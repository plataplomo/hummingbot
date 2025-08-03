"""Hyperliquid Transaction Mapper.

This mapper handles transformations for transaction-related data from the Hyperliquid exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- Trade transformations from user fills and fill data
- WebSocket fill event transformations
- Trade-specific validation and error handling
- Side mapping and trade data processing
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin
from cyberdelta.apis.exceptions import TradeTransformationError
from cyberdelta.apis.hyperliquid.mappers.utils.common_mappers import (
    map_side_to_internal,
    validate_trade_data,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsFillEvent,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import TransactionMapperProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Trade
from cyberdelta.models.market.trade import HyperliquidTradeDetails
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidTransactionMapper(CommonDataParserMixin, TransactionMapperProtocol):
    """Focused mapper for Hyperliquid transaction data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw transaction models
    into CyberDeltaEngine Internal Domain Models for trades and fills.
    """

    # Protocol-specific methods - TransactionMapperProtocol focuses on user fills

    def transform_raw_user_fill_to_internal(self, raw_fill: HyperliquidRawUserFill) -> Trade:
        """Transforms a HyperliquidRawUserFill to an Internal Trade model.

        Converts user fill data from Hyperliquid into an internal Trade domain model.

        Args:
            raw_fill: Validated raw user fill from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TradeTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_user_fill",
                symbol=raw_fill.coin,
                side=raw_fill.side,
                px=raw_fill.px,
                sz=raw_fill.sz,
                time=raw_fill.time,
                message="Transforming HyperliquidRawUserFill to Trade",
            )

            # Map side
            side = map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = self.parse_decimal_safely(raw_fill.px)
            quantity = self.parse_decimal_safely(raw_fill.sz)

            # Validate trade data
            validate_trade_data(price, quantity, "HyperliquidRawUserFill")

            # Parse timestamp
            executed_at = self.parse_timestamp(raw_fill.time)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = self.parse_decimal_safely(getattr(raw_fill, "fee", "0"), default=Decimal(0))

            # Create HL-specific details
            trade_hash = getattr(raw_fill, "hash", None)
            if trade_hash is None:
                trade_hash = f"unknown_hash_{raw_fill.time}_{raw_fill.coin}"

            details = HyperliquidTradeDetails(
                trade_hash=str(trade_hash),
                liquidation_mark_px=self.parse_decimal_safely(
                    getattr(raw_fill, "liquidationMarkPx", None), default=None
                ),
                start_position=self.parse_decimal_safely(
                    getattr(raw_fill, "startPosition", None), default=None
                ),
                dir=getattr(raw_fill, "dir", None),
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=raw_fill.coin,
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": str(getattr(raw_fill, "hash", f"fill_{raw_fill.time}_{raw_fill.coin}")),
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": str(getattr(raw_fill, "oid", "unknown")),
                "exchange": ExchangeName.HYPERLIQUID.value,
                # "client_order_id" not set - will use default UUID generation
                "price": str(price),
                "quantity": str(quantity),
                "fee": str(fee),
                "fee_asset": raw_fill.coin,  # Fee asset is the traded symbol
                "is_maker": getattr(raw_fill, "is_maker", None),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            trade = secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_user_fill_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_user_fill_transformed",
                symbol=raw_fill.coin,
                side=side.value,
                price=str(price),
                quantity=str(quantity),
                fee=str(fee),
                trade_hash=str(trade_hash),
                message="Successfully transformed HyperliquidRawUserFill to Trade",
            )

        except Exception as e:
            logger.exception(
                "raw_user_fill_transform_failed",
                symbol=getattr(raw_fill, "coin", None),
                side=getattr(raw_fill, "side", None),
                raw_fill=raw_fill.model_dump() if raw_fill else None,
                error=str(e),
                message="Failed to transform HyperliquidRawUserFill to Trade",
            )
            raise TradeTransformationError(
                trade_source="HyperliquidRawUserFill",
                reason=str(e),
                symbol=raw_fill.coin,
                original_error=e,
            ) from e
        else:
            return trade

    def transform_raw_fill_to_internal(self, raw_fill: HyperliquidRawFill) -> Trade:
        """Transforms a HyperliquidRawFill to an Internal Trade model.

        Converts fill data from Hyperliquid into an internal Trade domain model.

        Args:
            raw_fill: Validated raw fill from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TradeTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_fill",
                symbol=raw_fill.coin,
                side=raw_fill.side,
                px=raw_fill.px,
                sz=raw_fill.sz,
                tid=raw_fill.tid,
                oid=raw_fill.oid,
                message="Transforming HyperliquidRawFill to Trade",
            )

            # Map side
            side = map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = self.parse_decimal_safely(raw_fill.px)
            quantity = self.parse_decimal_safely(raw_fill.sz)

            # Validate trade data
            validate_trade_data(price, quantity, "HyperliquidRawFill")

            # Parse timestamp
            executed_at = self.parse_timestamp(raw_fill.time)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = self.parse_decimal_safely(getattr(raw_fill, "fee", "0"), default=Decimal(0))

            # Create HL-specific details
            details = HyperliquidTradeDetails(
                trade_hash=raw_fill.hash,
                liquidation_mark_px=self.parse_decimal_safely(
                    getattr(raw_fill, "liquidation_mark_px", None), default=None
                ),
                start_position=self.parse_decimal_safely(
                    getattr(raw_fill, "start_position", None), default=None
                ),
                dir=getattr(raw_fill, "dir", None),
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=raw_fill.coin,
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": str(raw_fill.tid),
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": str(raw_fill.oid),
                "exchange": ExchangeName.HYPERLIQUID.value,
                "client_order_id": raw_fill.cloid or None,
                "price": str(price),
                "quantity": str(quantity),
                "fee": str(fee),
                "fee_asset": raw_fill.coin,  # Fee asset is the traded symbol
                "is_maker": raw_fill.is_maker,
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            trade = secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_fill_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_fill_transformed",
                symbol=raw_fill.coin,
                side=side.value,
                price=str(price),
                quantity=str(quantity),
                fee=str(fee),
                trade_id=str(raw_fill.tid),
                order_id=str(raw_fill.oid),
                client_order_id=raw_fill.cloid,
                is_maker=raw_fill.is_maker,
                message="Successfully transformed HyperliquidRawFill to Trade",
            )

        except Exception as e:
            logger.exception(
                "raw_fill_transform_failed",
                symbol=getattr(raw_fill, "coin", None),
                tid=getattr(raw_fill, "tid", None),
                oid=getattr(raw_fill, "oid", None),
                raw_fill=raw_fill.model_dump() if raw_fill else None,
                error=str(e),
                message="Failed to transform HyperliquidRawFill to Trade",
            )
            raise TradeTransformationError(
                trade_source="HyperliquidRawFill",
                reason=str(e),
                symbol=raw_fill.coin,
                trade_id=str(raw_fill.tid),
                original_error=e,
            ) from e
        else:
            return trade

    def transform_ws_fill_event_to_internal(self, raw_fill: HyperliquidRawWsFillEvent) -> Trade:
        """Transforms a WebSocket fill event to an Internal Trade model.

        Converts WebSocket fill event data from Hyperliquid into an internal Trade domain model.

        Args:
            raw_fill: Validated raw WebSocket fill event from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TradeTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_ws_fill_event",
                symbol=raw_fill.coin,
                side=raw_fill.side,
                px=raw_fill.px,
                sz=raw_fill.sz,
                time=raw_fill.time,
                hash=raw_fill.hash,
                oid=raw_fill.oid,
                message="Transforming HyperliquidRawWsFillEvent to Trade",
            )

            # Map side
            side = map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = self.parse_decimal_safely(raw_fill.px)
            quantity = self.parse_decimal_safely(raw_fill.sz)

            # Validate trade data
            validate_trade_data(price, quantity, "HyperliquidRawWsFillEvent")

            # Parse timestamp (convert from milliseconds)
            executed_at = datetime.fromtimestamp(raw_fill.time / 1000, tz=UTC)

            # Create HL-specific details
            details = HyperliquidTradeDetails(
                trade_hash=raw_fill.hash,
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=raw_fill.coin,
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": raw_fill.hash,
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": str(raw_fill.oid),
                "exchange": ExchangeName.HYPERLIQUID.value,
                "client_order_id": raw_fill.cloid,
                "price": str(price),
                "quantity": str(quantity),
                "fee": "0",  # Fee not available in WS fill events
                "fee_asset": None,
                "is_maker": raw_fill.is_maker,
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            trade = secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_ws_fill_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "ws_fill_event_transformed",
                symbol=raw_fill.coin,
                side=side.value,
                price=str(price),
                quantity=str(quantity),
                trade_hash=raw_fill.hash,
                order_id=str(raw_fill.oid),
                client_order_id=raw_fill.cloid,
                is_maker=raw_fill.is_maker,
                message="Successfully transformed HyperliquidRawWsFillEvent to Trade",
            )

        except Exception as e:
            logger.exception(
                "ws_fill_event_transform_failed",
                symbol=getattr(raw_fill, "coin", None),
                hash=getattr(raw_fill, "hash", None),
                oid=getattr(raw_fill, "oid", None),
                raw_fill=raw_fill.model_dump() if raw_fill else None,
                error=str(e),
                message="Failed to transform HyperliquidRawWsFillEvent to Trade",
            )
            raise TradeTransformationError(
                trade_source="HyperliquidRawWsFillEvent",
                reason=str(e),
                symbol=raw_fill.coin,
                trade_id=raw_fill.hash,
                original_error=e,
            ) from e
        else:
            return trade
