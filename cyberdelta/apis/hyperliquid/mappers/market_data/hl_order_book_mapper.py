"""Hyperliquid Order Book Mapper.

This mapper handles transformations for order book and trade data from the Hyperliquid exchange,
extracted from the monolithic market data mapper to improve maintainability and testability.

Focused on:
- Order book transformations from L2 book data
- WebSocket book update transformations
- Public trade transformations and processing
- Trade validation and filtering
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin, ValidationMixin
from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import (
    OrderBookTransformationError,
    TradeTransformationError,
)
from cyberdelta.apis.hyperliquid.mappers.utils.common_mappers import (
    map_side_to_internal,
    validate_trade_data,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    FillMapperProtocol,
    OrderBookMapperProtocol,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Fill, OrderBook
from cyberdelta.models.market.fill import HyperliquidFillDetails
from cyberdelta.symbols import exchanges
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidOrderBookMapper(
    CommonDataParserMixin,
    ValidationMixin,
    OrderBookMapperProtocol,
    FillMapperProtocol,
):
    """Focused mapper for Hyperliquid order book and trade data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw order book
    and trade models into CyberDeltaEngine Internal Domain Models.
    """

    # Protocol-specific methods from OrderBookMapperProtocol
    def transform_raw_order_book_to_internal(
        self, raw_order_book: HyperliquidRawL2Book
    ) -> OrderBook:
        """Transform raw order book data to internal model.

        Args:
            raw_order_book: Raw order book data from API (HyperliquidRawL2Book)

        Returns:
            OrderBook domain model
        """
        # Create HyperliquidRawL2Book from dict and delegate to existing method
        return self.transform_raw_l2_book_to_internal(raw_order_book)

    # Protocol-specific methods from FillMapperProtocol
    def transform_raw_fill_to_internal(self, raw_fill: HyperliquidRawPublicTrade) -> Fill:
        """Transform raw fill data to internal model.

        Args:
            raw_fill: Raw fill data from API

        Returns:
            Fill: Fill domain model

        Raises:
            TradeTransformationError: If transformation fails
        """
        # Delegate to existing method with typed model
        result = self.transform_raw_public_trade_to_internal(raw_fill)
        if result is None:
            raise TradeTransformationError(
                trade_source="HyperliquidRawPublicTrade",
                reason="Fill transformation returned None",
                symbol=str(raw_fill.coin),
                original_error=None,
            )
        return result

    def transform_raw_l2_book_to_internal(
        self,
        raw_book: HyperliquidRawL2Book,
        depth: int | None = None,
    ) -> OrderBook:
        """Transforms a HyperliquidRawL2Book to an Internal OrderBook model.

        Args:
            raw_book: Validated raw order book data from Hyperliquid
            depth: Optional depth limit for order book levels

        Returns:
            OrderBook: Internal domain model with bid/ask levels

        Raises:
            OrderBookTransformationError: If transformation fails
            TransformationError: If secure transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_order_book_to_internal",
                symbol=str(raw_book.coin),
                time=raw_book.time,
                levels_count=len(raw_book.levels) if raw_book.levels else 0,
                depth=depth,
                message="Transforming HyperliquidRawL2Book to OrderBook",
            )

            # Parse bid and ask levels
            bids = self._parse_order_book_levels(
                raw_book,
                level_index=0,
                depth=depth,
            )
            asks = self._parse_order_book_levels(
                raw_book,
                level_index=1,
                depth=depth,
            )

            # Parse timestamp
            timestamp = self.parse_timestamp(raw_book.time)
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=str(raw_book.coin),  # Convert RawAssetString64HL to str
            )

            # Use secure_transform for type-safe model creation
            orderbook_data = {
                "symbol": exchange_symbol,  # Domain object!
                "bids": [(str(price), str(size)) for price, size in bids],
                "asks": [(str(price), str(size)) for price, size in asks],
                "timestamp": timestamp.isoformat(),
            }

            orderbook = secure_transform(
                data=orderbook_data,
                model_class=OrderBook,
                context="hyperliquid_orderbook_transform",
                source_exchange=ExchangeName.HYPERLIQUID,
            )

            logger.debug(
                "raw_order_book_to_internal_transformed",
                symbol=str(raw_book.coin),
                bids_count=len(bids),
                asks_count=len(asks),
                timestamp=timestamp.isoformat(),
                message="Successfully transformed HyperliquidRawL2Book to OrderBook",
            )
        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "order_book_transform_failed",
                symbol=str(raw_book.coin),
                raw_book=raw_book.model_dump() if raw_book else None,
                error=str(e),
                message="Failed to transform HyperliquidRawL2Book to OrderBook",
            )
            raise OrderBookTransformationError(
                source_type="HyperliquidRawL2Book",
                reason=str(e),
                symbol=str(raw_book.coin),
                original_error=e,
            ) from e
        else:
            return orderbook

    def _parse_order_book_levels(
        self,
        raw_book: HyperliquidRawL2Book,
        level_index: int,
        depth: int | None = None,
    ) -> list[tuple[Decimal, Decimal]]:
        """Parse order book levels (bids or asks) from raw data.

        Args:
            raw_book: Raw order book data
            level_index: Index for levels (0 for bids, 1 for asks)
            depth: Optional depth limit

        Returns:
            list[tuple[Decimal, Decimal]]: List of (price, size) tuples

        Raises:
            OrderBookTransformationError: If parsing fails
            TransformationError: If decimal value parsing fails
        """
        try:
            levels: list[tuple[Decimal, Decimal]] = []

            if not raw_book.levels or len(raw_book.levels) <= level_index:
                return levels

            level_data = raw_book.levels[level_index]
            for level in level_data:
                if depth is not None and len(levels) >= depth:
                    break

                try:
                    price = self.parse_decimal_safely(level.px, default=None)
                    size = self.parse_decimal_safely(level.sz, default=None)
                    if price is not None and size is not None:
                        levels.append((price, size))
                except (ValueError, TypeError):
                    continue  # Skip invalid levels

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            raise OrderBookTransformationError(
                source_type="HyperliquidRawL2Book",
                reason=f"Failed to parse order book levels: {e}",
                original_error=e,
            ) from e
        else:
            return levels

    def transform_raw_public_trade_to_internal(
        self,
        raw_trade: HyperliquidRawPublicTrade,
    ) -> Fill | None:
        """Transforms a HyperliquidRawPublicTrade to an Internal Fill model.

        Args:
            raw_trade: Validated raw public trade data from Hyperliquid

        Returns:
            Fill | None: Internal domain model with HL details populated, or None if invalid

        Raises:
            TradeTransformationError: If transformation fails
            TransformationError: If secure transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_public_trade_to_internal",
                symbol=str(raw_trade.coin),
                side=raw_trade.side,
                px=raw_trade.px,
                sz=raw_trade.sz,
                time=raw_trade.time,
                hash=raw_trade.hash,
                message="Transforming HyperliquidRawPublicTrade to Fill",
            )

            # Map side
            side = map_side_to_internal(raw_trade.side)

            # Parse price and quantity
            price = self.parse_decimal_safely(raw_trade.px)
            quantity = self.parse_decimal_safely(raw_trade.sz)

            # Validate trade data
            validate_trade_data(price, quantity, "HyperliquidRawPublicTrade")

            # Ensure trade values are not None after parsing and get validated values
            price = self.ensure_decimal_not_none(price, "price", "trade_transformation")
            quantity = self.ensure_decimal_not_none(quantity, "quantity", "trade_transformation")

            # Check for zero or negative values - return None for invalid trades
            # Also filter out extremely small quantities that are not meaningful for trading
            min_quantity_threshold = Decimal("0.000001")  # 1 micro unit minimum
            if price <= Decimal(0) or quantity <= min_quantity_threshold:
                logger.warning(
                    "invalid_trade_data_skipped",
                    symbol=str(raw_trade.coin),
                    price=str(price),
                    quantity=str(quantity),
                    message="Skipping trade with invalid price or quantity",
                )
                return None

            # Parse timestamp
            executed_at = self.parse_timestamp(raw_trade.time)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=str(raw_trade.coin),  # Convert RawAssetString64HL to str
            )

            # Create HL-specific details
            details = HyperliquidFillDetails(
                fill_hash=raw_trade.hash,
                liquidation_mark_px=None,  # Not available in public trades
                start_position=None,
                dir=None,
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": raw_trade.hash,
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": "UNKNOWN_PUBLIC_TRADE",  # Public trades don't have order IDs
                "exchange": ExchangeName.HYPERLIQUID.value,
                # "client_order_id" not set - will use default UUID generation
                "price": str(price),
                "quantity": str(quantity),
                "fee": "0",  # Fee not available in public trades
                "fee_asset": None,
                "maker_taker": None,  # Not available in public trades
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            fill = secure_transform(
                data=trade_data,
                model_class=Fill,
                context="hyperliquid_public_trade_transform",
                source_exchange=ExchangeName.HYPERLIQUID,
            )

            logger.debug(
                "raw_public_trade_to_internal_transformed",
                symbol=str(raw_trade.coin),
                side=side.value,
                price=str(price),
                quantity=str(quantity),
                trade_hash=raw_trade.hash,
                executed_at=executed_at.isoformat(),
                message="Successfully transformed HyperliquidRawPublicTrade to Fill",
            )
        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "public_trade_transform_failed",
                symbol=str(raw_trade.coin),
                trade_hash=raw_trade.hash,
                raw_trade=raw_trade.model_dump() if raw_trade else None,
                error=str(e),
                message="Failed to transform HyperliquidRawPublicTrade to Fill",
            )
            raise TradeTransformationError(
                trade_source="HyperliquidRawPublicTrade",
                reason=str(e),
                symbol=str(raw_trade.coin),
                trade_id=raw_trade.hash,
                original_error=e,
            ) from e
        else:
            return fill

    def transform_ws_trade_event_to_internal(self, raw: HyperliquidRawWsTradeEvent) -> Fill:
        """Transforms a WebSocket trade event to an Internal Fill model.

        Args:
            raw: Validated raw WebSocket trade event from Hyperliquid

        Returns:
            Fill: Internal domain model with HL details populated

        Raises:
            TradeTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_ws_trade_event_to_internal",
                symbol=str(raw.coin),
                side=raw.side,
                px=raw.px,
                sz=raw.sz,
                time=raw.time,
                hash=raw.hash,
                message="Transforming HyperliquidRawWsTradeEvent to Fill",
            )

            # Map side
            side = map_side_to_internal(raw.side)

            # Parse price and quantity
            price = self.parse_decimal_safely(raw.px)
            quantity = self.parse_decimal_safely(raw.sz)

            # Validate trade data
            validate_trade_data(price, quantity, "HyperliquidRawWsTradeEvent")

            # Parse timestamp (convert from milliseconds)
            executed_at = datetime.fromtimestamp(raw.time / 1000, tz=UTC)

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=str(raw.coin),  # Convert RawAssetString64HL to str
            )

            # Create HL-specific details
            details = HyperliquidFillDetails(
                fill_hash=raw.hash,
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": raw.hash,
                "symbol": exchange_symbol,  # Domain object!
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": "UNKNOWN_PUBLIC_TRADE",
                "exchange": ExchangeName.HYPERLIQUID.value,
                # "client_order_id" not set - will use default UUID generation
                "price": str(price),
                "quantity": str(quantity),
                "fee": "0",
                "fee_asset": None,
                "is_maker": None,
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            fill = secure_transform(
                data=trade_data,
                model_class=Fill,
                context="hyperliquid_ws_trade_transform",
                source_exchange=ExchangeName.HYPERLIQUID,
            )

            logger.debug(
                "ws_trade_event_to_internal_transformed",
                symbol=str(raw.coin),
                side=side.value,
                price=str(price),
                quantity=str(quantity),
                trade_hash=raw.hash,
                executed_at=executed_at.isoformat(),
                message="Successfully transformed HyperliquidRawWsTradeEvent to Fill",
            )
        except Exception as e:
            logger.exception(
                "ws_trade_event_transform_failed",
                symbol=str(raw.coin),
                trade_hash=raw.hash,
                raw_trade=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform HyperliquidRawWsTradeEvent to Fill",
            )
            raise TradeTransformationError(
                trade_source="HyperliquidRawWsTradeEvent",
                reason=str(e),
                symbol=str(raw.coin),
                trade_id=raw.hash,
                original_error=e,
            ) from e
        else:
            return fill

    def transform_ws_book_update_to_internal(self, raw: HyperliquidRawWsBookUpdate) -> OrderBook:
        """Transforms a WebSocket order book update to an Internal OrderBook model.

        Args:
            raw: Validated raw WebSocket book update from Hyperliquid

        Returns:
            OrderBook: Internal domain model

        Raises:
            OrderBookTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_ws_book_update_to_internal",
                symbol=str(raw.coin),
                time=raw.time,
                levels_count=len(raw.levels) if raw.levels else 0,
                message="Transforming HyperliquidRawWsBookUpdate to OrderBook",
            )

            # Parse bid levels (levels[0])
            bids: list[tuple[Decimal, Decimal]] = []
            if raw.levels and len(raw.levels) > 0:
                for level in raw.levels[0]:
                    price = self.parse_decimal_safely(level.px)
                    size = self.parse_decimal_safely(level.sz)

                    if price is not None and size is not None:
                        bids.append((price, size))

            # Parse ask levels (levels[1])
            asks: list[tuple[Decimal, Decimal]] = []
            if raw.levels and len(raw.levels) > 1:
                for level in raw.levels[1]:
                    price = self.parse_decimal_safely(level.px)
                    size = self.parse_decimal_safely(level.sz)

                    if price is not None and size is not None:
                        asks.append((price, size))

            # Parse timestamp (convert from milliseconds)
            timestamp = datetime.fromtimestamp(raw.time / 1000, tz=UTC)

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=str(raw.coin),  # Convert RawAssetString64HL to str
            )

            # Use secure_transform for type-safe model creation
            orderbook_data = {
                "symbol": exchange_symbol,  # Domain object!
                "bids": [(str(price), str(size)) for price, size in bids],
                "asks": [(str(price), str(size)) for price, size in asks],
                "timestamp": timestamp.isoformat(),
            }

            orderbook = secure_transform(
                data=orderbook_data,
                model_class=OrderBook,
                context="hyperliquid_ws_book_transform",
                source_exchange=ExchangeName.HYPERLIQUID,
            )

            logger.debug(
                "ws_book_update_to_internal_transformed",
                symbol=str(raw.coin),
                bids_count=len(bids),
                asks_count=len(asks),
                timestamp=timestamp.isoformat(),
                message="Successfully transformed HyperliquidRawWsBookUpdate to OrderBook",
            )

        except Exception as e:
            logger.exception(
                "ws_book_update_transform_failed",
                symbol=str(raw.coin),
                raw_book_update=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform HyperliquidRawWsBookUpdate to OrderBook",
            )
            raise OrderBookTransformationError(
                source_type="HyperliquidRawWsBookUpdate",
                reason=str(e),
                symbol=str(raw.coin),
                original_error=e,
            ) from e
        else:
            return orderbook

    @staticmethod
    def transform_raw_trades(
        raw_public_trades: list[HyperliquidRawPublicTrade],
        limit: int | None = None,
    ) -> list[Fill]:
        """Transforms a list of HyperliquidRawPublicTrade to Internal Fill models.

        Args:
            raw_public_trades: List of validated raw public trade data from Hyperliquid
            limit: Optional limit on number of fills to return

        Returns:
            list[Fill]: List of internal domain models

        Raises:
            TradeTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_trades",
                trades_count=len(raw_public_trades),
                limit=limit,
                message="Transforming list of HyperliquidRawPublicTrade to Fills",
            )

            fills: list[Fill] = []

            for raw_trade in raw_public_trades:
                try:
                    mapper = HyperliquidOrderBookMapper()
                    fill = mapper.transform_raw_public_trade_to_internal(raw_trade)
                    if fill is not None:
                        fills.append(fill)
                    else:
                        logger.warning(
                            "fill_transformation_skipped",
                            symbol=str(raw_trade.coin),
                            fill_hash=raw_trade.hash,
                            message="Fill transformation returned None",
                        )
                except Exception as e:
                    logger.exception(
                        "fill_transformation_failed",
                        symbol=str(raw_trade.coin),
                        fill_hash=raw_trade.hash,
                        raw_data=raw_trade.model_dump(),
                        error=str(e),
                        message="Failed to transform individual fill, continuing with others",
                    )
                    continue

            # Apply limit if specified
            if limit is not None:
                if limit <= 0:
                    return []
                fills = fills[:limit]

            logger.debug(
                "raw_trades_transformed",
                input_count=len(raw_public_trades),
                output_count=len(fills),
                limit=limit,
                message="Successfully transformed list of HyperliquidRawPublicTrade to Fills",
            )
        except Exception as e:
            logger.exception(
                "raw_trades_transform_failed",
                trades_count=len(raw_public_trades) if raw_public_trades else 0,
                limit=limit,
                error=str(e),
                message="Failed to transform list of HyperliquidRawPublicTrade to Fills",
            )
            raise TradeTransformationError(
                trade_source="list[HyperliquidRawPublicTrade]",
                reason=str(e),
                symbol="multiple",
                original_error=e,
            ) from e
        else:
            return fills
