"""CyberDeltaEngine: Backpack Market Data Mapper
---------------------------------------------

This module provides the BackpackMarketDataMapper class for transforming
Backpack Raw Market Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Tickers (BackpackRawTicker) to Internal Ticker models
- Transform Raw Order Books (BackpackRawOrderBook) to Internal OrderBook models
- Transform Raw Public Trades to Internal Trade models
- Transform Raw Funding Rates to Internal FundingRate models
- Transform Raw Candles (BackpackRawKline) to Internal Candle models
- Transform WebSocket Market Data events to Internal models

All transformation methods follow the standard pattern:
- Take a validated Raw Pydantic Model as primary input
- Return fully populated Internal Domain Model with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawOrderBook,
    BackpackRawTicker,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawTrade,
    BackpackRawTradeEvent,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.funding_rate import BackpackFundingDetails, FundingRate
from cyberdelta.core.models.market.trade import BackpackTradeDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class BackpackMarketDataMapper:
    """Domain-focused mapper for Backpack market data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to market data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """Maps a Backpack order side string to internal OrderSide enum.

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
    def transform_raw_ticker_to_internal(
        raw_ticker: BackpackRawTicker,
        symbol_override: str | None = None,
    ) -> Ticker:
        """Transforms a BackpackRawTicker to an Internal Ticker model.

        Args:
            raw_ticker: Validated raw ticker data from Backpack
            symbol_override: Optional symbol override for the ticker

        Returns:
            Ticker: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Use symbol override if provided, otherwise use raw ticker symbol
            symbol = symbol_override or raw_ticker.symbol

            # Parse core ticker fields using parsing utilities
            last_price = parse_decimal_value(raw_ticker.price, allow_none=True, field_name="price")
            bid_price = parse_decimal_value(raw_ticker.bid, allow_none=True, field_name="bid")
            ask_price = parse_decimal_value(raw_ticker.ask, allow_none=True, field_name="ask")
            volume_24h = parse_decimal_value(
                raw_ticker.volume,
                allow_none=True,
                field_name="volume",
            )

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_ticker.time, field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            return Ticker(
                symbol=symbol,
                timestamp=timestamp,
                price=last_price,
                bid=bid_price,
                ask=ask_price,
                volume=volume_24h,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawTicker to Ticker: {e}",
            ) from e

    @staticmethod
    def transform_raw_order_book_to_internal(
        symbol: str,
        raw_book: BackpackRawOrderBook,
    ) -> OrderBook:
        """Transforms a BackpackRawOrderBook to an Internal OrderBook model.

        Args:
            symbol: Symbol for the order book
            raw_book: Validated raw order book data from Backpack

        Returns:
            OrderBook: Internal domain model with bid/ask levels

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse bid levels
            bids: list[tuple[Decimal, Decimal]] = []
            for bid_level in raw_book.bids:
                price = parse_decimal_value(bid_level[0], allow_none=False, field_name="bid_price")
                size = parse_decimal_value(bid_level[1], allow_none=False, field_name="bid_size")

                if price is not None and size is not None:
                    bids.append((price, size))

            # Parse ask levels
            asks: list[tuple[Decimal, Decimal]] = []
            for ask_level in raw_book.asks:
                price = parse_decimal_value(ask_level[0], allow_none=False, field_name="ask_price")
                size = parse_decimal_value(ask_level[1], allow_none=False, field_name="ask_size")

                if price is not None and size is not None:
                    asks.append((price, size))

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_book.timestamp, field_name="timestamp")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawOrderBook to OrderBook: {e}",
            ) from e

    @staticmethod
    def transform_raw_trade_to_internal(raw_trade: BackpackRawTrade) -> Trade:
        """Transforms a BackpackRawTrade to an Internal Trade model.

        Args:
            raw_trade: Validated raw trade data from Backpack

        Returns:
            Trade: Internal domain model with populated fields and BP details

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse trade fields
            price = parse_decimal_value(raw_trade.price, allow_none=False, field_name="price")
            if price is None:
                raise TransformationError("price is required for trade")

            quantity = parse_decimal_value(
                raw_trade.quantity,
                allow_none=False,
                field_name="quantity",
            )
            if quantity is None:
                raise TransformationError("quantity is required for trade")

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_trade.time, field_name="time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create BP-specific details
            details = BackpackTradeDetails()

            return Trade(
                id=raw_trade.id,
                symbol=raw_trade.symbol,
                executed_at=executed_at,
                side=OrderSide.BUY,  # BackpackRawTrade doesn't have side, default to BUY
                order_id=raw_trade.order_id,
                exchange=ExchangeName.BACKPACK.value,
                price=price,
                quantity=quantity,
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(f"Failed to transform BackpackRawTrade to Trade: {e}") from e

    @staticmethod
    def transform_raw_funding_rate_to_internal(raw_funding: BackpackRawFundingRate) -> FundingRate:
        """Transforms a BackpackRawFundingRate to an Internal FundingRate model.

        Args:
            raw_funding: Validated raw funding rate data from Backpack

        Returns:
            FundingRate: Internal domain model with populated fields and BP details

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse funding rate
            funding_rate = parse_decimal_value(
                raw_funding.funding_rate,
                allow_none=False,
                field_name="fundingRate",
            )
            if funding_rate is None:
                raise TransformationError("funding_rate is required")

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_funding.time, field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Parse mark price and index price
            mark_price = parse_decimal_value(
                raw_funding.mark_price,
                allow_none=True,
                field_name="markPrice",
            )
            index_price = parse_decimal_value(
                raw_funding.index_price,
                allow_none=True,
                field_name="indexPrice",
            )

            # Create BP-specific details
            details = BackpackFundingDetails()

            return FundingRate(
                symbol=raw_funding.symbol,
                timestamp=timestamp,
                funding_rate=funding_rate,
                mark_price=mark_price,
                index_price=index_price,
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawFundingRate to FundingRate: {e}",
            ) from e

    @staticmethod
    def transform_raw_funding_interval_rate_to_internal(
        raw_funding: BackpackRawFundingIntervalRate,
        symbol: str,
    ) -> FundingRate:
        """Transforms a BackpackRawFundingIntervalRate to an Internal FundingRate model.

        Args:
            raw_funding: Validated raw funding interval rate data from Backpack
            symbol: Symbol for the funding rate

        Returns:
            FundingRate: Internal domain model with populated fields and BP details

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse funding rate
            funding_rate = parse_decimal_value(
                raw_funding.rate,
                allow_none=False,
                field_name="rate",
            )
            if funding_rate is None:
                raise TransformationError("funding_rate is required")

            # Parse timestamp (time is an int, convert to datetime)
            timestamp = datetime.fromtimestamp(raw_funding.time / 1000, tz=UTC)

            # Create BP-specific details
            details = BackpackFundingDetails()

            return FundingRate(
                symbol=symbol,
                timestamp=timestamp,
                funding_rate=funding_rate,
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawFundingIntervalRate to FundingRate: {e}",
            ) from e

    @staticmethod
    def transform_raw_kline_to_internal(
        symbol: str,
        interval: str,
        raw_kline: BackpackRawKline,
    ) -> Candle:
        """Transforms a BackpackRawKline to an Internal Candle model.

        Args:
            symbol: Symbol for the candle
            interval: Time interval for the candle
            raw_kline: Validated raw kline data from Backpack

        Returns:
            Candle: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse OHLCV data using correct field names
            open_price = parse_decimal_value(
                raw_kline.open_price,
                allow_none=False,
                field_name="open_price",
            )
            high_price = parse_decimal_value(
                raw_kline.high_price,
                allow_none=False,
                field_name="high_price",
            )
            low_price = parse_decimal_value(
                raw_kline.low_price,
                allow_none=False,
                field_name="low_price",
            )
            close_price = parse_decimal_value(
                raw_kline.close_price,
                allow_none=False,
                field_name="close_price",
            )
            volume = parse_decimal_value(raw_kline.volume, allow_none=False, field_name="volume")

            if any(val is None for val in [open_price, high_price, low_price, close_price, volume]):
                raise TransformationError("All OHLCV values are required for candle")

            # Parse timestamp from start_time_ms (convert milliseconds to datetime)
            open_time = datetime.fromtimestamp(raw_kline.start_time_ms / 1000, tz=UTC)

            # Type assertions since we already checked for None values above
            assert open_price is not None
            assert high_price is not None
            assert low_price is not None
            assert close_price is not None
            assert volume is not None

            return Candle(
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
            )

        except Exception as e:
            raise TransformationError(f"Failed to transform BackpackRawKline to Candle: {e}") from e

    @staticmethod
    def transform_ws_ticker_event_to_internal(raw_ticker: BackpackRawTickerEvent) -> Ticker:
        """Transforms a BackpackRawTickerEvent to an Internal Ticker model.

        Args:
            raw_ticker: Validated raw ticker event data from Backpack WebSocket

        Returns:
            Ticker: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse core ticker fields
            last_price = parse_decimal_value(
                raw_ticker.last_price,
                allow_none=True,
                field_name="lastPrice",
            )
            volume_24h = parse_decimal_value(
                raw_ticker.volume,
                allow_none=True,
                field_name="volume",
            )

            # Parse timestamp from event_time
            timestamp = parse_datetime_utc(raw_ticker.event_time, field_name="event_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            return Ticker(
                symbol=raw_ticker.symbol,
                timestamp=timestamp,
                price=last_price,
                bid=None,  # Not available in ticker event
                ask=None,  # Not available in ticker event
                volume=volume_24h,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawTickerEvent to Ticker: {e}",
            ) from e

    @staticmethod
    def transform_ws_depth_event_to_internal(
        symbol: str,
        raw_depth: BackpackRawDepthUpdateEvent,
    ) -> OrderBook:
        """Transforms a BackpackRawDepthUpdateEvent to an Internal OrderBook model.

        Args:
            symbol: Symbol for the order book
            raw_depth: Validated raw depth update event data from Backpack WebSocket

        Returns:
            OrderBook: Internal domain model with bid/ask levels

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse bid levels
            bids: list[tuple[Decimal, Decimal]] = []
            for bid_level in raw_depth.bids:
                price = parse_decimal_value(bid_level[0], allow_none=False, field_name="bid_price")
                size = parse_decimal_value(bid_level[1], allow_none=False, field_name="bid_size")

                if price is not None and size is not None:
                    bids.append((price, size))

            # Parse ask levels
            asks: list[tuple[Decimal, Decimal]] = []
            for ask_level in raw_depth.asks:
                price = parse_decimal_value(ask_level[0], allow_none=False, field_name="ask_price")
                size = parse_decimal_value(ask_level[1], allow_none=False, field_name="ask_size")

                if price is not None and size is not None:
                    asks.append((price, size))

            # Parse timestamp from event_time
            timestamp = parse_datetime_utc(raw_depth.event_time, field_name="event_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawDepthUpdateEvent to OrderBook: {e}",
            ) from e

    @staticmethod
    def transform_ws_trade_event_to_internal(raw_trade: BackpackRawTradeEvent) -> Trade:
        """Transforms a BackpackRawTradeEvent to an Internal Trade model.

        Args:
            raw_trade: Validated raw trade event data from Backpack WebSocket

        Returns:
            Trade: Internal domain model with populated fields and BP details

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse trade fields
            price = parse_decimal_value(raw_trade.price, allow_none=False, field_name="price")
            if price is None:
                raise TransformationError("price is required for trade")

            quantity = parse_decimal_value(
                raw_trade.quantity,
                allow_none=False,
                field_name="quantity",
            )
            if quantity is None:
                raise TransformationError("quantity is required for trade")

            # BackpackRawTradeEvent doesn't have side info, need to determine from order IDs
            # For now, default to BUY (this would need to be enhanced based on maker/taker info)
            side = OrderSide.BUY if raw_trade.is_buyer_the_maker else OrderSide.SELL

            # Parse timestamp from event_time
            executed_at = parse_datetime_utc(raw_trade.event_time, field_name="event_time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create BP-specific details
            details = BackpackTradeDetails()

            return Trade(
                id=raw_trade.trade_id,
                symbol=raw_trade.symbol,
                executed_at=executed_at,
                side=side,
                order_id=raw_trade.buyer_order_id,  # Choose buyer order ID as primary
                exchange=ExchangeName.BACKPACK.value,
                price=price,
                quantity=quantity,
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawTradeEvent to Trade: {e}",
            ) from e
