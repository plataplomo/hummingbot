"""CyberDeltaEngine: Hyperliquid Market Data Mapper.

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

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import Candle, Market
from cyberdelta.core.models.market.funding_rate import FundingRate, HyperliquidFundingDetails
from cyberdelta.core.models.market.market import HyperliquidMarketDetails
from cyberdelta.core.models.market.mid_prices import MidPrices
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidMarketDataMapper:
    """Domain-focused mapper for Hyperliquid market data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw models
    related to market data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(hl_side: str) -> OrderSide:
        """Maps a Hyperliquid order side string to internal OrderSide enum.

        Args:
            hl_side: Raw side string from Hyperliquid ("B" or "A")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped

        """
        try:
            if hl_side == "B":
                return OrderSide.BUY
            if hl_side == "A":
                return OrderSide.SELL

            raise TransformationError(
                f"Unknown Hyperliquid order side: '{hl_side}'",
                field_name="side",
                source_value=hl_side,
            )
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            raise TransformationError(
                f"Failed to map order side: {e}",
                field_name="side",
                source_value=hl_side,
                original_exception=e,
            ) from e

    @staticmethod
    def transform_raw_asset_ctx_to_ticker(raw_asset_ctx: HyperliquidRawAssetCtx) -> Ticker:
        """Transforms a HyperliquidRawAssetCtx to an Internal Ticker model.

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
                raw_asset_ctx.mark_px,
                allow_none=False,
                field_name="markPx",
            )
            if mark_px is None:
                raise TransformationError(
                    "mark_px is required for ticker",
                    field_name="mark_px",
                    source_value=raw_asset_ctx.mark_px,
                )

            # Extract volume data from day_ntl_vlm (daily notional volume)
            volume_24h = None
            if raw_asset_ctx.day_ntl_vlm:
                volume_24h = parse_decimal_value(
                    raw_asset_ctx.day_ntl_vlm,
                    allow_none=True,
                    field_name="dayNtlVlm",
                )

            # Get current timestamp for ticker timestamp
            timestamp = datetime.now(UTC)

            # Ensure symbol is not None
            if raw_asset_ctx.name is None:
                raise TransformationError(
                    "Asset context name is required for ticker",
                    field_name="name",
                    source_value=None,
                )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            ticker_data = {
                "symbol": raw_asset_ctx.name,
                "timestamp": timestamp.isoformat(),
                "price": str(mark_px),  # Using mark_px as the last price
                "bid": None,  # Not available in asset context
                "ask": None,  # Not available in asset context
                "volume": str(volume_24h) if volume_24h is not None else None,
                "bp_details": None,
                "hl_details": None,
            }

            return secure_transform(
                data=ticker_data,
                model_class=Ticker,
                context="hyperliquid_asset_ctx_transform",
                source_exchange="hyperliquid",
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "asset_context_to_ticker_transform_failed",
                component="HyperliquidMarketDataMapper",
                action="transform_asset_context_to_ticker",
                symbol=raw_asset_ctx.name,
                error=str(e),
            )
            raise TransformationError(
                f"Failed to transform HyperliquidRawAssetCtx to Ticker: {e}",
                source_data={"symbol": raw_asset_ctx.name, "mark_px": raw_asset_ctx.mark_px},
            ) from e

    @staticmethod
    def transform_raw_order_book_to_internal(
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
            TransformationError: If transformation fails

        """
        try:
            # Parse bid and ask levels
            bids = HyperliquidMarketDataMapper._parse_order_book_levels(
                raw_book,
                level_index=0,
                depth=depth,
            )
            asks = HyperliquidMarketDataMapper._parse_order_book_levels(
                raw_book,
                level_index=1,
                depth=depth,
            )

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_book.time, field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            orderbook_data = {
                "symbol": str(raw_book.coin),  # Convert RawAssetString64HL to str
                "bids": [(str(price), str(size)) for price, size in bids],
                "asks": [(str(price), str(size)) for price, size in asks],
                "timestamp": timestamp.isoformat(),
            }

            return secure_transform(
                data=orderbook_data,
                model_class=OrderBook,
                context="hyperliquid_orderbook_transform",
                source_exchange="hyperliquid",
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "order_book_transform_failed",
                component="HyperliquidMarketDataMapper",
                action="transform_order_book",
                symbol=str(raw_book.coin),
                error=str(e),
            )
            raise TransformationError(
                f"Failed to transform HyperliquidRawL2Book to OrderBook: {e}",
                source_data={"symbol": str(raw_book.coin), "levels_count": len(raw_book.levels)},
            ) from e

    @staticmethod
    def _parse_order_book_levels(
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
            List of (price, size) tuples

        Raises:
            TransformationError: If parsing fails
        """
        try:
            levels: list[tuple[Decimal, Decimal]] = []

            if not raw_book.levels or len(raw_book.levels) <= level_index:
                return levels

            level_data = raw_book.levels[level_index]
            for level in level_data:
                if depth is not None and len(levels) >= depth:
                    break

                price = parse_decimal_value(level.px, allow_none=False, field_name="px")
                size = parse_decimal_value(level.sz, allow_none=False, field_name="sz")

                if price is not None and size is not None:
                    levels.append((price, size))

        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            raise TransformationError(
                f"Failed to parse order book levels: {e}",
                field_name="levels",
                source_value=level_index,
                original_exception=e,
            ) from e
        else:
            return levels

    @staticmethod
    def transform_raw_public_trade_to_internal(
        raw_trade: HyperliquidRawPublicTrade,
    ) -> Trade | None:
        """Transforms a HyperliquidRawPublicTrade to an Internal Trade model.

        Args:
            raw_trade: Validated raw public trade data from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated, or None if invalid

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
                raise TransformationError(
                    "Price and quantity are required for trade",
                    source_data={"px": raw_trade.px, "sz": raw_trade.sz},
                )

            # Check for zero or negative values - return None for invalid trades
            # Also filter out extremely small quantities that are not meaningful for trading
            min_quantity_threshold = Decimal("0.000001")  # 1 micro unit minimum
            if price <= Decimal(0) or quantity <= min_quantity_threshold:
                logger.warning(
                    "invalid_trade_data_skipped",
                    action="transform_public_trade",
                    price=str(price),
                    quantity=str(quantity),
                    message="Skipping trade with invalid price or quantity",
                )
                return None

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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            trade_data = {
                "id": raw_trade.hash,
                "symbol": str(raw_trade.coin),  # Convert RawAssetString64HL to str
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": "UNKNOWN_PUBLIC_TRADE",  # Public trades don't have order IDs
                "exchange": ExchangeName.HYPERLIQUID.value,
                # "client_order_id" not set - will use default UUID generation
                "price": str(price),
                "quantity": str(quantity),
                "fee": "0",  # Fee not available in public trades
                "fee_asset": None,
                "is_maker": None,  # Not available in public trades
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_public_trade_transform",
                source_exchange="hyperliquid",
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "public_trade_transform_failed",
                component="HyperliquidMarketDataMapper",
                action="transform_public_trade",
                symbol=str(raw_trade.coin),
                trade_hash=raw_trade.hash,
                error=str(e),
            )
            raise TransformationError(
                f"Failed to transform HyperliquidRawPublicTrade to Trade: {e}",
                source_data={"symbol": str(raw_trade.coin), "hash": raw_trade.hash},
            ) from e

    @staticmethod
    def transform_raw_asset_ctx_to_funding_rate(
        raw_asset_ctx: HyperliquidRawAssetCtx,
    ) -> FundingRate | None:
        """Transforms a HyperliquidRawAssetCtx to an Internal FundingRate model.

        Args:
            raw_asset_ctx: Validated raw asset context data from Hyperliquid

        Returns:
            FundingRate: Internal domain model with HL details, or None if no funding data

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse mark price first
            mark_price = parse_decimal_value(
                raw_asset_ctx.mark_px,
                allow_none=True,
                field_name="mark_px",
            )

            # Parse hourly funding rate
            hourly_funding_rate = None
            funding_rate_8hr = None

            try:
                hourly_funding_rate = parse_decimal_value(
                    raw_asset_ctx.funding,
                    allow_none=True,
                    field_name="funding",
                )

                if hourly_funding_rate is not None and hourly_funding_rate.is_finite():
                    # Convert hourly rate to 8-hour rate
                    funding_rate_8hr = hourly_funding_rate * Decimal(8)

            except ValueError:
                logger.warning(
                    "funding_rate_parse_failed",
                    action="transform_asset_context_to_funding_rate",
                    symbol=str(raw_asset_ctx.name),
                    message="Could not parse funding rate, setting to None",
                )

            # Calculate next funding time (start of next hour)
            now_utc = datetime.now(UTC)
            next_funding_time = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(
                hours=1,
            )

            # Parse additional HL-specific details
            impact_px = parse_decimal_value(
                raw_asset_ctx.impact_px,
                allow_none=True,
                field_name="impact_px",
            )

            # Create HL-specific details
            details = HyperliquidFundingDetails(
                hl_funding_hourly=hourly_funding_rate,
                hl_impact_px=impact_px,
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            funding_data = {
                "symbol": str(raw_asset_ctx.name),  # Convert RawAssetString64HL to str
                "timestamp": datetime.now(UTC).isoformat(),
                "funding_rate": str(funding_rate_8hr),  # 8-hour rate for compatibility with tests
                "predicted_rate": None,
                "mark_price": str(mark_price) if mark_price is not None else None,
                "index_price": None,
                "next_funding_time": next_funding_time.isoformat(),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="hyperliquid_asset_ctx_funding_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            logger.exception(
                "asset_context_to_funding_rate_mapping_failed",
                action="transform_asset_context_to_funding_rate",
                symbol=str(raw_asset_ctx.name),
                error=str(e),
            )
            return None

    @staticmethod
    def transform_raw_funding_history_item_to_internal(
        raw_item: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transforms a HyperliquidRawFundingHistoryItem to an Internal FundingRate model.

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
                raw_item.funding_rate,
                allow_none=False,
                field_name="fundingRate",
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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            funding_data = {
                "symbol": str(raw_item.coin),  # Convert RawAssetString64HL to str
                "timestamp": timestamp.isoformat(),
                "funding_rate": str(funding_rate),
                "predicted_rate": None,
                "mark_price": None,
                "index_price": None,
                "next_funding_time": None,  # Not available in historical data
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="hyperliquid_funding_history_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawFundingHistoryItem to FundingRate: {e}",
            ) from e

    @staticmethod
    def transform_raw_candle_snapshot_to_candles(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        symbol: str,
        interval: str,
    ) -> list[Candle]:
        """Transforms a HyperliquidRawCandleSnapshot to a list of Internal Candle models.

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
                candle = HyperliquidMarketDataMapper._parse_single_candle(
                    raw_snapshot, i, symbol, interval
                )
                if candle:
                    candles.append(candle)

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawCandleSnapshot to Candle list: {e}",
            ) from e
        else:
            return candles

    @staticmethod
    def _parse_single_candle(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        index: int,
        symbol: str,
        interval: str,
    ) -> Candle | None:
        """Parse a single candle from the raw snapshot at the given index.

        Returns None if the candle data is invalid and should be skipped.
        """
        # Parse OHLCV data from parallel lists
        ohlcv_prices = HyperliquidMarketDataMapper._parse_ohlcv_prices(raw_snapshot, index)
        if not ohlcv_prices:
            return None

        open_price, high_price, low_price, close_price, volume = ohlcv_prices

        # Parse timestamp (convert from milliseconds)
        timestamp = datetime.fromtimestamp(raw_snapshot.t[index] / 1000, tz=UTC)

        # Validate all prices are non-None
        HyperliquidMarketDataMapper._validate_candle_prices(
            open_price, high_price, low_price, close_price, volume
        )

        # Create and return candle
        return HyperliquidMarketDataMapper._create_candle_from_data(
            symbol, interval, timestamp, open_price, high_price, low_price, close_price, volume
        )

    @staticmethod
    def _parse_ohlcv_prices(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        index: int,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal] | None:
        """Parse OHLCV prices from raw snapshot at given index.

        Returns None if any price is invalid.
        """
        try:
            open_price = parse_decimal_value(
                raw_snapshot.o[index],
                allow_none=False,
                field_name="o",
            )
            high_price = parse_decimal_value(
                raw_snapshot.h[index],
                allow_none=False,
                field_name="h",
            )
            low_price = parse_decimal_value(raw_snapshot.l[index], allow_none=False, field_name="l")
            close_price = parse_decimal_value(
                raw_snapshot.c[index],
                allow_none=False,
                field_name="c",
            )
            volume = parse_decimal_value(raw_snapshot.v[index], allow_none=False, field_name="v")
        except (ValueError, TypeError, IndexError):
            logger.warning(
                "invalid_candle_data",
                action="parse_candle",
                index=index,
                message=f"Skipping candle at index {index} with invalid OHLCV data",
            )
            return None

        if None in {open_price, high_price, low_price, close_price, volume}:
            logger.warning(
                "invalid_candle_data",
                action="parse_candle",
                index=index,
                message=f"Skipping candle at index {index} with invalid OHLCV data",
            )
            return None

        # Ensure all values are non-None (type narrowing)
        if (
            open_price is not None
            and high_price is not None
            and low_price is not None
            and close_price is not None
            and volume is not None
        ):
            return open_price, high_price, low_price, close_price, volume

        # Should not reach here given the checks above
        logger.warning(
            "invalid_candle_data",
            action="parse_candle",
            index=index,
            message=f"Skipping candle at index {index} - None values after parsing",
        )
        return None

    @staticmethod
    def _validate_candle_prices(
        open_price: Decimal | None,
        high_price: Decimal | None,
        low_price: Decimal | None,
        close_price: Decimal | None,
        volume: Decimal | None,
    ) -> None:
        """Validate that all candle prices are non-None after parsing."""
        if open_price is None:
            raise ValueError("Open price unexpectedly None after validation")
        if high_price is None:
            raise ValueError("High price unexpectedly None after validation")
        if low_price is None:
            raise ValueError("Low price unexpectedly None after validation")
        if close_price is None:
            raise ValueError("Close price unexpectedly None after validation")
        if volume is None:
            raise ValueError("Volume unexpectedly None after validation")

    @staticmethod
    def _create_candle_from_data(
        symbol: str,
        interval: str,
        timestamp: datetime,
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal,
        volume: Decimal,
    ) -> Candle:
        """Create a Candle object from the provided data."""
        candle_data = {
            "symbol": symbol,
            "interval": interval,
            "open_time": timestamp.isoformat(),
            "open": str(open_price),
            "high": str(high_price),
            "low": str(low_price),
            "close": str(close_price),
            "volume": str(volume),
        }

        return secure_transform(
            data=candle_data,
            model_class=Candle,
            context="hyperliquid_candle_transform",
            source_exchange="hyperliquid",
        )

    @staticmethod
    def transform_ws_trade_event_to_internal(raw: HyperliquidRawWsTradeEvent) -> Trade:
        """Transforms a WebSocket trade event to an Internal Trade model.

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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            trade_data = {
                "id": raw.hash,
                "symbol": str(raw.coin),  # Convert RawAssetString64HL to str
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

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_ws_trade_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawWsTradeEvent to Trade: {e}",
            ) from e

    @staticmethod
    def transform_ws_book_update_to_internal(raw: HyperliquidRawWsBookUpdate) -> OrderBook:
        """Transforms a WebSocket order book update to an Internal OrderBook model.

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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            orderbook_data = {
                "symbol": str(raw.coin),  # Convert RawAssetString64HL to str
                "bids": [(str(price), str(size)) for price, size in bids],
                "asks": [(str(price), str(size)) for price, size in asks],
                "timestamp": timestamp.isoformat(),
            }

            return secure_transform(
                data=orderbook_data,
                model_class=OrderBook,
                context="hyperliquid_ws_book_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawWsBookUpdate to OrderBook: {e}",
            ) from e

    @staticmethod
    def transform_raw_trades(
        raw_public_trades: list[HyperliquidRawPublicTrade],
        limit: int | None = None,
    ) -> list[Trade]:
        """Transforms a list of HyperliquidRawPublicTrade to Internal Trade models.

        Args:
            raw_public_trades: List of validated raw public trade data from Hyperliquid
            limit: Optional limit on number of trades to return

        Returns:
            list[Trade]: List of internal domain models

        Raises:
            TransformationError: If transformation fails

        """
        trades: list[Trade] = []

        for raw_trade in raw_public_trades:
            try:
                trade = HyperliquidMarketDataMapper.transform_raw_public_trade_to_internal(
                    raw_trade,
                )
                if trade is not None:
                    trades.append(trade)
                else:
                    logger.warning(
                        "trade_transformation_skipped",
                        action="transform_raw_trades",
                        symbol=str(raw_trade.coin),
                        message="Trade transformation returned None",
                    )
            except Exception as e:
                logger.exception(
                    "trade_transformation_failed",
                    action="transform_raw_trades",
                    symbol=str(raw_trade.coin),
                    error=str(e),
                    raw_data=raw_trade.model_dump(),
                )
                continue

        # Apply limit if specified
        if limit is not None:
            if limit <= 0:
                return []
            return trades[:limit]
        return trades

    @staticmethod
    def transform_raw_meta_and_asset_ctxs_to_markets(
        raw_meta_and_asset_ctxs: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> list[Market]:
        """Transform raw meta and asset contexts to internal Market models.

        Args:
            raw_meta_and_asset_ctxs: Raw response containing asset definitions and contexts

        Returns:
            List of Market objects with metadata for all assets

        Raises:
            TransformationError: If transformation fails
        """
        try:
            markets: list[Market] = []

            # Create lookup dictionary for asset contexts by name
            asset_ctx_lookup = {ctx.name: ctx for ctx in raw_meta_and_asset_ctxs.asset_ctxs}

            # Transform each asset definition from meta
            for asset_def in raw_meta_and_asset_ctxs.meta.universe:
                try:
                    # Get corresponding asset context (optional)
                    asset_ctx = asset_ctx_lookup.get(asset_def.name)

                    market = HyperliquidMarketDataMapper._create_market_from_asset_definition(
                        asset_def,
                        asset_ctx,
                    )
                    markets.append(market)

                except (TransformationError, ValueError, TypeError, AttributeError) as e:
                    logger.warning(
                        "asset_definition_to_market_transform_failed",
                        action="transform_meta_and_asset_ctxs_to_markets",
                        asset_name=asset_def.name,
                        error=str(e),
                    )
                    continue

        except Exception as e:
            logger.exception(
                "market_transformation_failed",
                action="transform_markets",
                error=str(e),
                message=f"Failed to transform meta and asset contexts to markets: {e}",
            )
            raise TransformationError(f"Failed to transform meta and asset contexts: {e}") from e
        else:
            return markets

    @staticmethod
    def _create_market_from_asset_definition(
        asset_def: HyperliquidRawAssetDefinition,
        asset_ctx: HyperliquidRawAssetCtx | None = None,
    ) -> Market:
        """Create a Market model from Hyperliquid asset definition and context.

        Args:
            asset_def: Asset definition with trading rules
            asset_ctx: Optional asset context with current pricing data

        Returns:
            Market object with available metadata

        Raises:
            TransformationError: If market creation fails
        """
        try:
            # Calculate step_size from sz_decimals
            step_size_parsed = parse_decimal_value(f"1e-{asset_def.sz_decimals}")
            if step_size_parsed is None:
                raise TransformationError(
                    f"Failed to parse step size for {asset_def.name}",
                    field_name="sz_decimals",
                    source_value=asset_def.sz_decimals,
                )
            step_size = step_size_parsed

            # For Hyperliquid perpetuals, determine tick size from actual market prices
            # since it's not explicitly provided in their meta response
            # Note: sz_decimals refers to quantity precision, not price precision
            if asset_ctx and asset_ctx.mark_px:
                # Analyze the mark price to determine price precision
                mark_price_str = str(asset_ctx.mark_px)
                if "." in mark_price_str:
                    # Count decimal places in the actual market price
                    decimal_places = len(mark_price_str.split(".")[1].rstrip("0"))
                    tick_size = parse_decimal_value(f"1e-{decimal_places}") or Decimal("1.0")
                else:
                    # Whole number pricing
                    tick_size = Decimal("1.0")
            else:
                # Fallback for assets without market context
                # Use conservative tick size based on typical crypto price ranges
                tick_size = Decimal("1.0") if step_size <= Decimal("0.001") else step_size

            # Create Hyperliquid-specific details using proper typed model
            hl_details = HyperliquidMarketDetails(
                max_leverage=asset_def.max_leverage,
                only_isolated=asset_def.only_isolated,
                sz_decimals=asset_def.sz_decimals,
                mark_price=parse_decimal_value(asset_ctx.mark_px) if asset_ctx else None,
                funding_rate=parse_decimal_value(asset_ctx.funding) if asset_ctx else None,
            )

            # Create market with available information
            # SECURITY FIX: Use secure_transform instead of direct instantiation
            market_data = {
                "symbol": asset_def.name,
                "base_symbol": asset_def.name,  # For perps, symbol equals base
                "quote_symbol": "USD",  # Hyperliquid perps are USD-settled
                "market_type": "Perpetual",
                "tick_size": str(tick_size),
                "step_size": str(step_size),
                "min_price": None,  # Not specified in Hyperliquid meta
                "max_price": None,  # Not specified in Hyperliquid meta
                "min_quantity": str(step_size),  # Minimum is typically one step
                "max_quantity": None,  # Not specified in Hyperliquid meta
                "status": "Active",  # Assume active if in meta response
                "created_at": None,  # Not provided in meta response
                "bp_details": None,  # Not applicable
                "hl_details": hl_details.model_dump()
                if hl_details
                else None,  # Properly typed Hyperliquid details
            }

            return secure_transform(
                data=market_data,
                model_class=Market,
                context="hyperliquid_asset_def_market_transform",
                source_exchange="hyperliquid",
            )

        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            raise TransformationError(
                f"Failed to create market from asset definition: {e}",
                field_name="asset_def",
                source_value=asset_def.name if hasattr(asset_def, "name") else str(asset_def),
                original_exception=e,
            ) from e

    @staticmethod
    def transform_single_asset_to_market(
        asset_def: HyperliquidRawAssetDefinition,
        asset_ctx: HyperliquidRawAssetCtx | None = None,
    ) -> Market:
        """Transform a single asset definition to Market model.

        Convenience method for transforming individual assets.

        Args:
            asset_def: Asset definition from meta response
            asset_ctx: Optional asset context data

        Returns:
            Market object for the specified asset
        """
        return HyperliquidMarketDataMapper._create_market_from_asset_definition(
            asset_def,
            asset_ctx,
        )

    @staticmethod
    def transform_raw_all_mids_to_internal(
        raw_all_mids: HyperliquidRawAllMids,
    ) -> MidPrices:
        """Transform Hyperliquid AllMids response to internal MidPrices model.

        Args:
            raw_all_mids: Validated HyperliquidRawAllMids model containing symbol->price mapping

        Returns:
            MidPrices: Internal model containing symbol to mid price mapping

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # The raw model already has validated the structure
            # We just need to convert string prices to Decimal
            prices: dict[str, Decimal] = {}
            for symbol, price_str in raw_all_mids.root.items():
                # Use our standard decimal parsing utility
                decimal_price = parse_decimal_value(
                    price_str,
                    allow_none=False,
                    field_name=f"mid_price[{symbol}]",
                )
                if decimal_price is not None:
                    prices[symbol] = decimal_price

            # Create and return MidPrices instance
            # SECURITY FIX: Use secure_transform instead of direct instantiation
            mid_prices_data = {
                "prices": {symbol: str(price) for symbol, price in prices.items()},
                "timestamp": datetime.now(UTC).isoformat(),
                "exchange": ExchangeName.HYPERLIQUID.value,
            }

            return secure_transform(
                data=mid_prices_data,
                model_class=MidPrices,
                context="hyperliquid_all_mids_transform",
                source_exchange="hyperliquid",
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform AllMids response: {e}") from e
