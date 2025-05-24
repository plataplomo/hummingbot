"""
CyberDeltaEngine: Hyperliquid Market Data Mapper
------------------------------------------------

This module provides the HyperliquidMarketDataMapper class for transforming
Hyperliquid Raw Market Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Tickers (HyperliquidRawAssetCtx) to Internal Ticker models
- Transform Raw Order Books (HyperliquidRawL2Book) to Internal OrderBook models
- Transform Raw Public Trades to Internal Trade models
- Transform Raw Funding Rates to Internal FundingRate models
- Transform Raw Candles to Internal Candle models
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

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate, HyperliquidFundingDetails
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class HyperliquidMarketDataMapper:
    """
    Domain-focused mapper for Hyperliquid market data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw models
    related to market data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(hl_side: str) -> OrderSide:
        """
        Maps a Hyperliquid order side string to internal OrderSide enum.

        Args:
            hl_side: Raw side string from Hyperliquid ("B" or "A")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped
        """
        if hl_side == "B":
            return OrderSide.BUY
        elif hl_side == "A":
            return OrderSide.SELL

        raise TransformationError(f"Unknown Hyperliquid order side: '{hl_side}'")

    @staticmethod
    def transform_raw_asset_ctx_to_ticker(raw_asset_ctx: HyperliquidRawAssetCtx) -> Ticker:
        """
        Transforms a HyperliquidRawAssetCtx to an Internal Ticker model.

        Args:
            raw_asset_ctx: Validated raw asset context data from Hyperliquid

        Returns:
            Ticker: Internal domain model with populated fields and HL details

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse core ticker fields using parsing utilities
            mark_px = parse_decimal_value(
                raw_asset_ctx.mark_px, allow_none=False, field_name="markPx"
            )
            if mark_px is None:
                raise TransformationError("mark_px is required for ticker")

            # Extract volume data from day_ntl_vlm (daily notional volume)
            volume_24h = None
            if raw_asset_ctx.day_ntl_vlm:
                volume_24h = parse_decimal_value(
                    raw_asset_ctx.day_ntl_vlm, allow_none=True, field_name="dayNtlVlm"
                )

            # Get current timestamp for ticker timestamp
            timestamp = datetime.now(UTC)

            return Ticker(
                symbol=raw_asset_ctx.name,
                timestamp=timestamp,
                price=mark_px,  # Using mark_px as the last price
                bid=None,  # Not available in asset context
                ask=None,  # Not available in asset context
                volume=volume_24h,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawAssetCtx to Ticker: {e}"
            ) from e

    @staticmethod
    def transform_raw_order_book_to_internal(
        raw_book: HyperliquidRawL2Book, depth: int | None = None
    ) -> OrderBook:
        """
        Transforms a HyperliquidRawL2Book to an Internal OrderBook model.

        Args:
            raw_book: Validated raw order book data from Hyperliquid
            depth: Optional depth limit for order book levels

        Returns:
            OrderBook: Internal domain model with bid/ask levels

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse bid levels (raw_book.levels[0])
            bids: list[tuple[Decimal, Decimal]] = []
            if raw_book.levels and len(raw_book.levels) > 0:
                bid_levels = raw_book.levels[0]
                for level in bid_levels:
                    if depth is not None and len(bids) >= depth:
                        break

                    price = parse_decimal_value(level.px, allow_none=False, field_name="px")
                    size = parse_decimal_value(level.sz, allow_none=False, field_name="sz")

                    if price is not None and size is not None:
                        bids.append((price, size))

            # Parse ask levels (raw_book.levels[1])
            asks: list[tuple[Decimal, Decimal]] = []
            if raw_book.levels and len(raw_book.levels) > 1:
                ask_levels = raw_book.levels[1]
                for level in ask_levels:
                    if depth is not None and len(asks) >= depth:
                        break

                    price = parse_decimal_value(level.px, allow_none=False, field_name="px")
                    size = parse_decimal_value(level.sz, allow_none=False, field_name="sz")

                    if price is not None and size is not None:
                        asks.append((price, size))

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_book.time, field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            return OrderBook(
                symbol=raw_book.coin,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawL2Book to OrderBook: {e}"
            ) from e

    @staticmethod
    def transform_raw_public_trade_to_internal(raw_trade: HyperliquidRawPublicTrade) -> Trade:
        """
        Transforms a HyperliquidRawPublicTrade to an Internal Trade model.

        Args:
            raw_trade: Validated raw public trade data from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Map side
            side = HyperliquidMarketDataMapper._map_side_to_internal(raw_trade.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_trade.px, allow_none=False, field_name="px")
            quantity = parse_decimal_value(raw_trade.sz, allow_none=False, field_name="sz")

            if price is None or quantity is None:
                raise TransformationError("Price and quantity are required for trade")

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_trade.time, field_name="time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Create HL-specific details
            details = HyperliquidTradeDetails(
                trade_hash=raw_trade.hash,
                liquidation_mark_px=None,  # Not available in public trades
                start_position=None,
                dir=None,
            )

            return Trade(
                id=raw_trade.hash,
                symbol=raw_trade.coin,
                executed_at=executed_at,
                side=side,
                order_id="UNKNOWN_PUBLIC_TRADE",  # Public trades don't have order IDs
                exchange=ExchangeName.HYPERLIQUID.value,
                client_order_id=None,
                price=price,
                quantity=quantity,
                fee=Decimal("0"),  # Fee not available in public trades
                fee_asset=None,
                is_maker=None,  # Not available in public trades
                hl_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawPublicTrade to Trade: {e}"
            ) from e

    @staticmethod
    def transform_raw_asset_ctx_to_funding_rate(
        raw_asset_ctx: HyperliquidRawAssetCtx,
    ) -> FundingRate | None:
        """
        Transforms a HyperliquidRawAssetCtx to an Internal FundingRate model.

        Args:
            raw_asset_ctx: Validated raw asset context data from Hyperliquid

        Returns:
            FundingRate: Internal domain model with HL details, or None if no funding data

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Check if funding data is available
            if not raw_asset_ctx.funding:
                return None

            funding_rate = parse_decimal_value(
                raw_asset_ctx.funding, allow_none=False, field_name="funding"
            )

            if funding_rate is None:
                return None

            # Create timestamp - use current time since funding data doesn't include timestamp
            timestamp = datetime.now(UTC)

            # Create HL-specific details
            details = HyperliquidFundingDetails(
                # Add any HL-specific funding rate fields here
            )

            return FundingRate(
                symbol=raw_asset_ctx.name,
                timestamp=timestamp,
                funding_rate=funding_rate,
                next_funding_time=None,  # Not available in asset context
                hl_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawAssetCtx to FundingRate: {e}"
            ) from e

    @staticmethod
    def transform_raw_funding_history_item_to_internal(
        raw_item: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """
        Transforms a HyperliquidRawFundingHistoryItem to an Internal FundingRate model.

        Args:
            raw_item: Validated raw funding history item from Hyperliquid

        Returns:
            FundingRate: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse funding rate
            funding_rate = parse_decimal_value(
                raw_item.funding_rate, allow_none=False, field_name="fundingRate"
            )

            if funding_rate is None:
                raise TransformationError("Funding rate is required")

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_item.time, field_name="time")
            if timestamp is None:
                raise TransformationError("Timestamp is required for funding history")

            # Create HL-specific details
            details = HyperliquidFundingDetails(
                # Add any HL-specific funding history fields here
            )

            return FundingRate(
                symbol=raw_item.coin,
                timestamp=timestamp,
                funding_rate=funding_rate,
                next_funding_time=None,  # Not available in historical data
                hl_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawFundingHistoryItem to FundingRate: {e}"
            ) from e

    @staticmethod
    def transform_raw_candle_snapshot_to_candles(
        raw_snapshot: HyperliquidRawCandleSnapshot, symbol: str, interval: str
    ) -> list[Candle]:
        """
        Transforms a HyperliquidRawCandleSnapshot to a list of Internal Candle models.

        Args:
            raw_snapshot: Validated raw candle snapshot data from Hyperliquid
            symbol: Symbol for the candles
            interval: Time interval for the candles

        Returns:
            list[Candle]: List of internal domain models

        Raises:
            TransformationError: If transformation fails
        """
        try:
            candles: list[Candle] = []

            # Check if we have any data
            if not raw_snapshot.t:
                return candles

            # Iterate through parallel lists
            for i in range(len(raw_snapshot.t)):
                # Parse OHLCV data from parallel lists
                open_price = parse_decimal_value(
                    raw_snapshot.o[i], allow_none=False, field_name="o"
                )
                high_price = parse_decimal_value(
                    raw_snapshot.h[i], allow_none=False, field_name="h"
                )
                low_price = parse_decimal_value(raw_snapshot.l[i], allow_none=False, field_name="l")
                close_price = parse_decimal_value(
                    raw_snapshot.c[i], allow_none=False, field_name="c"
                )
                volume = parse_decimal_value(raw_snapshot.v[i], allow_none=False, field_name="v")

                if None in (open_price, high_price, low_price, close_price, volume):
                    logger.warning(f"Skipping candle at index {i} with invalid OHLCV data")
                    continue

                # Parse timestamp (convert from milliseconds)
                timestamp = datetime.fromtimestamp(raw_snapshot.t[i] / 1000, tz=UTC)

                # Type assertions since we already checked for None values above
                assert open_price is not None
                assert high_price is not None
                assert low_price is not None
                assert close_price is not None
                assert volume is not None

                candle = Candle(
                    symbol=symbol,
                    interval=interval,
                    open_time=timestamp,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )

                candles.append(candle)

            return candles

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawCandleSnapshot to Candle list: {e}"
            ) from e

    @staticmethod
    def transform_ws_trade_event_to_internal(raw: HyperliquidRawWsTradeEvent) -> Trade:
        """
        Transforms a WebSocket trade event to an Internal Trade model.

        Args:
            raw: Validated raw WebSocket trade event from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Map side
            side = HyperliquidMarketDataMapper._map_side_to_internal(raw.side)

            # Parse price and quantity
            price = parse_decimal_value(raw.px, allow_none=False, field_name="px")
            quantity = parse_decimal_value(raw.sz, allow_none=False, field_name="sz")

            if price is None or quantity is None:
                raise TransformationError("Price and quantity are required for trade")

            # Parse timestamp (convert from milliseconds)
            executed_at = datetime.fromtimestamp(raw.time / 1000, tz=UTC)

            # Create HL-specific details
            details = HyperliquidTradeDetails(
                trade_hash=raw.hash,
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            )

            return Trade(
                id=raw.hash,
                symbol=raw.coin,
                executed_at=executed_at,
                side=side,
                order_id="UNKNOWN_PUBLIC_TRADE",
                exchange=ExchangeName.HYPERLIQUID.value,
                client_order_id=None,
                price=price,
                quantity=quantity,
                fee=Decimal("0"),
                fee_asset=None,
                is_maker=None,
                hl_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawWsTradeEvent to Trade: {e}"
            ) from e

    @staticmethod
    def transform_ws_book_update_to_internal(raw: HyperliquidRawWsBookUpdate) -> OrderBook:
        """
        Transforms a WebSocket order book update to an Internal OrderBook model.

        Args:
            raw: Validated raw WebSocket book update from Hyperliquid

        Returns:
            OrderBook: Internal domain model

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse bid levels (levels[0])
            bids: list[tuple[Decimal, Decimal]] = []
            if raw.levels and len(raw.levels) > 0:
                for level in raw.levels[0]:
                    price = parse_decimal_value(level.px, allow_none=False, field_name="px")
                    size = parse_decimal_value(level.sz, allow_none=False, field_name="sz")

                    if price is not None and size is not None:
                        bids.append((price, size))

            # Parse ask levels (levels[1])
            asks: list[tuple[Decimal, Decimal]] = []
            if raw.levels and len(raw.levels) > 1:
                for level in raw.levels[1]:
                    price = parse_decimal_value(level.px, allow_none=False, field_name="px")
                    size = parse_decimal_value(level.sz, allow_none=False, field_name="sz")

                    if price is not None and size is not None:
                        asks.append((price, size))

            # Parse timestamp (convert from milliseconds)
            timestamp = datetime.fromtimestamp(raw.time / 1000, tz=UTC)

            return OrderBook(
                symbol=raw.coin,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawWsBookUpdate to OrderBook: {e}"
            ) from e
