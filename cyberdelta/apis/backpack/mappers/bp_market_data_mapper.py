"""CyberDeltaEngine: Backpack Market Data Mapper.

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
from typing import Any, cast

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawMarket,
    BackpackRawOrderBook,
    BackpackRawTicker,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import Candle, Market
from cyberdelta.core.models.market.funding_rate import BackpackFundingDetails, FundingRate
from cyberdelta.core.models.market.market import BackpackMarketDetails
from cyberdelta.core.models.market.ticker import BackpackTickerDetails
from cyberdelta.core.models.market.trade import BackpackTradeDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform

logger = logging.getLogger(__name__)


class BackpackMarketDataMapper:
    """Domain-focused mapper for Backpack market data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to market data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """Map a Backpack order side string to internal OrderSide enum.

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
        """Transform a BackpackRawTicker to an Internal Ticker model.

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

            # Parse core ticker fields using new model structure
            last_price = parse_decimal_value(
                raw_ticker.last_price, allow_none=False, field_name="lastPrice"
            )
            volume_24h = parse_decimal_value(
                raw_ticker.volume, allow_none=False, field_name="volume"
            )

            # Parse extension fields for BackpackTickerDetails
            first_price = parse_decimal_value(
                raw_ticker.first_price, allow_none=False, field_name="firstPrice"
            )
            high_price = parse_decimal_value(raw_ticker.high, allow_none=False, field_name="high")
            low_price = parse_decimal_value(raw_ticker.low, allow_none=False, field_name="low")
            price_change = parse_decimal_value(
                raw_ticker.price_change, allow_none=False, field_name="priceChange"
            )
            price_change_percent = parse_decimal_value(
                raw_ticker.price_change_percent, allow_none=False, field_name="priceChangePercent"
            )
            quote_volume = parse_decimal_value(
                raw_ticker.quote_volume, allow_none=False, field_name="quoteVolume"
            )

            # Parse trade count
            trades_count = None
            if raw_ticker.trades:
                try:
                    trades_count = int(raw_ticker.trades)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Failed to parse trades count '{raw_ticker.trades}' for {symbol}"
                    )

            # Generate timestamp since API doesn't provide it
            timestamp = datetime.now(UTC)

            # Create Backpack-specific extension details
            bp_details = BackpackTickerDetails(
                first_price=first_price,
                high=high_price,
                low=low_price,
                price_change=price_change,
                price_change_percent=price_change_percent,
                quote_volume=quote_volume,
                trades=trades_count,
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            ticker_data: dict[str, Any] = {
                "symbol": symbol,
                "timestamp": timestamp.isoformat(),
                "price": str(last_price),  # Map lastPrice to core price field
                "bid": None,  # Not available from Backpack ticker endpoint
                "ask": None,  # Not available from Backpack ticker endpoint
                "volume": str(volume_24h) if volume_24h is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=ticker_data,
                model_class=Ticker,
                context="backpack_ticker_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawTicker to Ticker: {e}",
            ) from e

    @staticmethod
    def transform_raw_market_to_internal(raw_market: BackpackRawMarket) -> Market:
        """Transform a BackpackRawMarket to an Internal Market model.

        Args:
            raw_market: Validated raw market data from Backpack

        Returns:
            Market: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse core market fields (required, won't be None since allow_none=False)
            tick_size = cast(
                Decimal,
                parse_decimal_value(
                    raw_market.filters.price.tick_size, allow_none=False, field_name="tickSize"
                ),
            )
            step_size = cast(
                Decimal,
                parse_decimal_value(
                    raw_market.filters.quantity.step_size, allow_none=False, field_name="stepSize"
                ),
            )

            # Parse optional price limits
            min_price = parse_decimal_value(
                raw_market.filters.price.min_price, allow_none=True, field_name="minPrice"
            )
            max_price = parse_decimal_value(
                raw_market.filters.price.max_price, allow_none=True, field_name="maxPrice"
            )

            # Parse optional quantity limits
            min_quantity = parse_decimal_value(
                raw_market.filters.quantity.min_quantity, allow_none=True, field_name="minQuantity"
            )
            max_quantity = parse_decimal_value(
                raw_market.filters.quantity.max_quantity, allow_none=True, field_name="maxQuantity"
            )

            # Parse created_at timestamp
            created_at = None
            if raw_market.created_at:
                created_at = parse_datetime_utc(raw_market.created_at, field_name="createdAt")

            # Create Backpack-specific details
            bp_details = BackpackMarketDetails(
                order_book_state=raw_market.order_book_state,
                created_at_raw=raw_market.created_at,
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            market_data: dict[str, Any] = {
                "symbol": raw_market.symbol,
                "base_symbol": raw_market.base_symbol,
                "quote_symbol": raw_market.quote_symbol,
                "market_type": raw_market.market_type,
                "tick_size": str(tick_size),
                "step_size": str(step_size),
                "min_price": str(min_price) if min_price is not None else None,
                "max_price": str(max_price) if max_price is not None else None,
                "min_quantity": str(min_quantity) if min_quantity is not None else None,
                "max_quantity": str(max_quantity) if max_quantity is not None else None,
                "status": raw_market.order_book_state,
                "created_at": created_at.isoformat() if created_at is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=market_data,
                model_class=Market,
                context="backpack_market_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawMarket to Market: {e}",
            ) from e

    @staticmethod
    def transform_raw_order_book_to_internal(
        symbol: str,
        raw_book: BackpackRawOrderBook,
    ) -> OrderBook:
        """Transform a BackpackRawOrderBook to an Internal OrderBook model.

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

            # Sort bids in descending order (highest price first) and asks in ascending order
            # (lowest price first)
            bids.sort(key=lambda x: x[0], reverse=True)  # Sort by price descending
            asks.sort(key=lambda x: x[0], reverse=False)  # Sort by price ascending

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_book.timestamp, field_name="timestamp")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            orderbook_data: dict[str, Any] = {
                "symbol": symbol,
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
            raise TransformationError(
                f"Failed to transform BackpackRawOrderBook to OrderBook: {e}",
            ) from e

    @staticmethod
    def transform_raw_trade_to_internal(raw_trade: BackpackRawPublicTrade) -> Trade:
        """Transform a BackpackRawPublicTrade to an Internal Trade model.

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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
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
            raise TransformationError(
                f"Failed to transform BackpackRawPublicTrade to Trade: {e}"
            ) from e

    @staticmethod
    def transform_raw_recent_trade_to_internal(
        raw_trade: BackpackRawRecentPublicTrade, symbol: str
    ) -> Trade:
        """Transform a BackpackRawRecentPublicTrade to an Internal Trade model.

        Args:
            raw_trade: Validated raw recent trade data from Backpack
            symbol: Symbol for the trade (not included in recent trade response)

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
            executed_at = parse_datetime_utc(raw_trade.timestamp, field_name="timestamp")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Determine side from is_buyer_maker: if buyer is maker, then this trade is a sell
            # (taker sold to maker)
            # If buyer is not maker, then this trade is a buy (taker bought from maker)
            side = OrderSide.SELL if raw_trade.is_buyer_maker else OrderSide.BUY

            # Create BP-specific details
            details = BackpackTradeDetails()

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            trade_data: dict[str, Any] = {
                "id": str(raw_trade.id),  # Convert int ID to string
                "symbol": symbol,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": "",  # Not available in recent trades response
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
            raise TransformationError(
                f"Failed to transform BackpackRawRecentPublicTrade to Trade: {e}"
            ) from e

    @staticmethod
    def transform_raw_funding_rate_to_internal(raw_funding: BackpackRawFundingRate) -> FundingRate:
        """Transform a BackpackRawFundingRate to an Internal FundingRate model.

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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            funding_data: dict[str, Any] = {
                "symbol": raw_funding.symbol,
                "timestamp": timestamp.isoformat(),
                "funding_rate": str(funding_rate),
                "mark_price": str(mark_price) if mark_price is not None else None,
                "index_price": str(index_price) if index_price is not None else None,
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="backpack_funding_rate_transform",
                source_exchange="backpack",
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
        """Transform a BackpackRawFundingIntervalRate to an Internal FundingRate model.

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

            # Parse timestamp (time is now an ISO datetime string)
            timestamp = parse_datetime_utc(raw_funding.time, field_name="time")
            if timestamp is None:
                raise ValueError("Funding rate timestamp cannot be None")

            # Create BP-specific details
            details = BackpackFundingDetails()

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            funding_data: dict[str, Any] = {
                "symbol": symbol,
                "timestamp": timestamp.isoformat(),
                "funding_rate": str(funding_rate),
                "mark_price": None,
                "index_price": None,
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="backpack_funding_interval_transform",
                source_exchange="backpack",
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
        """Transform a BackpackRawKline to an Internal Candle model.

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

            # Type checks since we already validated None values above
            if open_price is None:
                raise ValueError(f"open_price is None for symbol {symbol}")
            if high_price is None:
                raise ValueError(f"high_price is None for symbol {symbol}")
            if low_price is None:
                raise ValueError(f"low_price is None for symbol {symbol}")
            if close_price is None:
                raise ValueError(f"close_price is None for symbol {symbol}")
            if volume is None:
                raise ValueError(f"volume is None for symbol {symbol}")

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            candle_data: dict[str, Any] = {
                "symbol": symbol,
                "interval": interval,
                "open_time": open_time.isoformat(),
                "open": str(open_price),
                "high": str(high_price),
                "low": str(low_price),
                "close": str(close_price),
                "volume": str(volume),
            }

            return secure_transform(
                data=candle_data,
                model_class=Candle,
                context="backpack_kline_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(f"Failed to transform BackpackRawKline to Candle: {e}") from e

    @staticmethod
    def transform_ws_ticker_event_to_internal(raw_ticker: BackpackRawTickerEvent) -> Ticker:
        """Transform a BackpackRawTickerEvent to an Internal Ticker model.

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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            ticker_data: dict[str, Any] = {
                "symbol": raw_ticker.symbol,
                "timestamp": timestamp.isoformat(),
                "price": str(last_price) if last_price is not None else None,
                "bid": None,  # Not available in ticker event
                "ask": None,  # Not available in ticker event
                "volume": str(volume_24h) if volume_24h is not None else None,
                "bp_details": None,
                "hl_details": None,
            }

            return secure_transform(
                data=ticker_data,
                model_class=Ticker,
                context="backpack_ws_ticker_transform",
                source_exchange="backpack",
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
        """Transform a BackpackRawDepthUpdateEvent to an Internal OrderBook model.

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

            # Sort bids in descending order (highest price first) and asks in ascending order
            # (lowest price first)
            bids.sort(key=lambda x: x[0], reverse=True)  # Sort by price descending
            asks.sort(key=lambda x: x[0], reverse=False)  # Sort by price ascending

            # Parse timestamp from event_time
            timestamp = parse_datetime_utc(raw_depth.event_time, field_name="event_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            orderbook_data: dict[str, Any] = {
                "symbol": symbol,
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
            raise TransformationError(
                f"Failed to transform BackpackRawDepthUpdateEvent to OrderBook: {e}",
            ) from e

    @staticmethod
    def transform_ws_trade_event_to_internal(raw_trade: BackpackRawPublicTradeEvent) -> Trade:
        """Transform a BackpackRawPublicTradeEvent to an Internal Trade model.

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

            # BackpackRawPublicTradeEvent doesn't have side info, need to determine from order IDs
            # For now, default to BUY (this would need to be enhanced based on maker/taker info)
            side = OrderSide.BUY if raw_trade.is_buyer_the_maker else OrderSide.SELL

            # Parse timestamp from event_time
            executed_at = parse_datetime_utc(raw_trade.event_time, field_name="event_time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create BP-specific details
            details = BackpackTradeDetails()

            # SECURITY FIX: Use secure_transform instead of direct instantiation
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
            raise TransformationError(
                f"Failed to transform BackpackRawPublicTradeEvent to Trade: {e}",
            ) from e
