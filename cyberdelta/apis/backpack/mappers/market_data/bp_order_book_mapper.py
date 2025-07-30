"""Backpack Order Book Mapper.

This mapper handles transformations for order book data from the Backpack exchange.

Focused on:
- REST order book data transformations
- WebSocket depth update transformations
- Order book-specific data validation and error handling
"""

import operator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawOrderBook,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import OrderBookMapperProtocol
from cyberdelta.apis.exceptions import OrderBookTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import OrderBook
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackOrderBookMapper(OrderBookMapperProtocol):
    """Focused mapper for Backpack order book data transformations.

    This class contains static methods for transforming validated Backpack Raw order book models
    into CyberDeltaEngine Internal OrderBook Domain Models.
    """

    @staticmethod
    def transform_raw_order_book_to_internal(
        symbol: Symbol,
        raw_book: BackpackRawOrderBook,
    ) -> OrderBook:
        """Transform a BackpackRawOrderBook to an Internal OrderBook model.

        Args:
            symbol: Symbol object for the order book
            raw_book: Validated raw order book data from Backpack

        Returns:
            OrderBook: Internal domain model with bid/ask levels

        Raises:
            OrderBookTransformationError: If transformation fails

        """
        try:
            # Parse bid levels
            bids: list[tuple[Decimal, Decimal]] = []
            for bid_level in raw_book.bids:
                price = parse_decimal_value(bid_level[0], allow_none=False, field_name="bid_price")
                size = parse_decimal_value(bid_level[1], allow_none=False, field_name="bid_size")
                bids.append((price, size))

            # Parse ask levels
            asks: list[tuple[Decimal, Decimal]] = []
            for ask_level in raw_book.asks:
                price = parse_decimal_value(ask_level[0], allow_none=False, field_name="ask_price")
                size = parse_decimal_value(ask_level[1], allow_none=False, field_name="ask_size")
                asks.append((price, size))

            # Sort bids in descending order (highest price first) and asks in ascending order
            # (lowest price first)
            bids.sort(key=operator.itemgetter(0), reverse=True)  # Sort by price descending
            asks.sort(key=operator.itemgetter(0), reverse=False)  # Sort by price ascending

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_book.timestamp, field_name="timestamp")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Symbol is already a domain object, use it directly
            exchange_symbol = symbol

            # Use secure_transform for type-safe model creation
            orderbook_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
                "bids": [(str(price), str(size)) for price, size in bids],
                "asks": [(str(price), str(size)) for price, size in asks],
                "timestamp": timestamp.isoformat(),
            }

            return secure_transform(
                data=orderbook_data,
                model_class=OrderBook,
                context="backpack_orderbook_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise OrderBookTransformationError(
                source_type="BackpackRawOrderBook",
                reason=str(e),
                symbol=symbol.value,  # Convert Symbol to string for error
                original_error=e,
            ) from e

    @staticmethod
    def transform_ws_depth_event_to_internal(
        symbol: Symbol,
        raw_depth: BackpackRawDepthUpdateEvent,
    ) -> OrderBook:
        """Transform a BackpackRawDepthUpdateEvent to an Internal OrderBook model.

        Args:
            symbol: Symbol domain object for the order book
            raw_depth: Validated raw depth update event data from Backpack WebSocket

        Returns:
            OrderBook: Internal domain model with bid/ask levels

        Raises:
            OrderBookTransformationError: If transformation fails

        """
        try:
            # Parse bid levels
            bids: list[tuple[Decimal, Decimal]] = []
            if raw_depth.bids is not None:
                for bid_level in raw_depth.bids:
                    price = parse_decimal_value(
                        bid_level[0],
                        allow_none=False,
                        field_name="bid_price",
                    )
                    size = parse_decimal_value(
                        bid_level[1],
                        allow_none=False,
                        field_name="bid_size",
                    )
                    bids.append((price, size))

            # Parse ask levels
            asks: list[tuple[Decimal, Decimal]] = []
            if raw_depth.asks is not None:
                for ask_level in raw_depth.asks:
                    price = parse_decimal_value(
                        ask_level[0],
                        allow_none=False,
                        field_name="ask_price",
                    )
                    size = parse_decimal_value(
                        ask_level[1],
                        allow_none=False,
                        field_name="ask_size",
                    )
                    asks.append((price, size))

            # Sort bids in descending order (highest price first) and asks in ascending order
            # (lowest price first)
            bids.sort(key=operator.itemgetter(0), reverse=True)  # Sort by price descending
            asks.sort(key=operator.itemgetter(0), reverse=False)  # Sort by price ascending

            # Parse timestamp from event_time
            timestamp = parse_datetime_utc(raw_depth.event_time, field_name="event_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Symbol is already a domain object, use it directly
            exchange_symbol = symbol

            # Use secure_transform for type-safe model creation
            orderbook_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
                "bids": [(str(price), str(size)) for price, size in bids],
                "asks": [(str(price), str(size)) for price, size in asks],
                "timestamp": timestamp.isoformat(),
            }

            return secure_transform(
                data=orderbook_data,
                model_class=OrderBook,
                context="backpack_ws_depth_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise OrderBookTransformationError(
                source_type="BackpackRawDepthUpdateEvent",
                reason=str(e),
                symbol=symbol.value,  # Convert Symbol to string for error
                original_error=e,
            ) from e

    # MapperProtocol methods
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Parse decimal values safely using BackpackCommonMappers.

        Args:
            value: Value to parse as Decimal (string, float, Decimal, or None).
            default: Default value to return if parsing fails.

        Returns:
            Parsed Decimal value or default if parsing fails.
        """
        return BackpackCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert timestamp to datetime using BackpackCommonMappers.

        Args:
            timestamp_ms: Timestamp in milliseconds (float or None).

        Returns:
            UTC datetime object if timestamp is provided, None otherwise.
        """
        return BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
