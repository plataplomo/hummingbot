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

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawOrderBook,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import OrderBookMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin
from cyberdelta.apis.exceptions import OrderBookTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import OrderBook
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackOrderBookMapper(CommonDataParserMixin, OrderBookMapperProtocol):
    """Focused mapper for Backpack order book data transformations.

    This class contains static methods for transforming validated Backpack Raw order book models
    into CyberDeltaEngine Internal OrderBook Domain Models.
    """

    def transform_raw_order_book_to_internal(
        self,
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
                price = self.parse_decimal_safely(bid_level[0])
                size = self.parse_decimal_safely(bid_level[1])
                if price is not None and size is not None:
                    bids.append((price, size))

            # Parse ask levels
            asks: list[tuple[Decimal, Decimal]] = []
            for ask_level in raw_book.asks:
                price = self.parse_decimal_safely(ask_level[0])
                size = self.parse_decimal_safely(ask_level[1])
                if price is not None and size is not None:
                    asks.append((price, size))

            # Sort bids in descending order (highest price first) and asks in ascending order
            # (lowest price first)
            bids.sort(key=operator.itemgetter(0), reverse=True)  # Sort by price descending
            asks.sort(key=operator.itemgetter(0), reverse=False)  # Sort by price ascending

            # Parse timestamp
            timestamp = self.parse_timestamp(raw_book.timestamp)
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
                source_exchange=ExchangeName.BACKPACK,
            )

        except Exception as e:
            raise OrderBookTransformationError(
                source_type="BackpackRawOrderBook",
                reason=str(e),
                symbol=symbol.value,  # Convert Symbol to string for error
                original_error=e,
            ) from e

    def transform_ws_depth_event_to_internal(
        self,
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
                    price = self.parse_decimal_safely(bid_level[0])
                    size = self.parse_decimal_safely(bid_level[1])
                    if price is not None and size is not None:
                        bids.append((price, size))

            # Parse ask levels
            asks: list[tuple[Decimal, Decimal]] = []
            if raw_depth.asks is not None:
                for ask_level in raw_depth.asks:
                    price = self.parse_decimal_safely(ask_level[0])
                    size = self.parse_decimal_safely(ask_level[1])
                    if price is not None and size is not None:
                        asks.append((price, size))

            # Sort bids in descending order (highest price first) and asks in ascending order
            # (lowest price first)
            bids.sort(key=operator.itemgetter(0), reverse=True)  # Sort by price descending
            asks.sort(key=operator.itemgetter(0), reverse=False)  # Sort by price ascending

            # Parse timestamp from event_time
            timestamp = self.parse_timestamp(raw_depth.event_time)
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
                source_exchange=ExchangeName.BACKPACK,
            )

        except Exception as e:
            raise OrderBookTransformationError(
                source_type="BackpackRawDepthUpdateEvent",
                reason=str(e),
                symbol=symbol.value,  # Convert Symbol to string for error
                original_error=e,
            ) from e
