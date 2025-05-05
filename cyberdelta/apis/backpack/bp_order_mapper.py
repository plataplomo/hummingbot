import logging
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawOrderBook,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawFill,
    BackpackRawTrade,
    BackpackRawTradeEvent,
)
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TimeInForce,
    TriggerType,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class BackpackOrderMapper:
    """
    Utility for transforming Backpack raw order/event models to CyberDeltaEngine internal models.

    - Maps Backpack string enums to internal enums (OrderSide, OrderStatus, etc.).
    - Handles defensive parsing and validation of all fields.
    - Used by BackpackAPI for all order-related transformations.
    """

    @staticmethod
    def map_side_to_internal(bp_side: str) -> OrderSide:
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL
        logger.warning(f"[BackpackOrderMapper] Unknown order side '{bp_side}', defaulting to BUY.")
        return OrderSide.BUY

    @staticmethod
    def map_status_to_internal(bp_status: str) -> OrderStatus:
        status_upper = (bp_status or "").upper()
        mapping = {
            "NEW": OrderStatus.NEW,
            "OPEN": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELED,
            "EXPIRED": OrderStatus.EXPIRED,
            "REJECTED": OrderStatus.REJECTED,
            "TRIGGER_PENDING": OrderStatus.TRIGGER_PENDING,
            "FAILED": OrderStatus.FAILED,
        }
        if status_upper in mapping:
            return mapping[status_upper]
        logger.warning(
            f"[BackpackOrderMapper] Unknown order status '{bp_status}', mapping to UNKNOWN."
        )
        return OrderStatus.UNKNOWN

    @staticmethod
    def map_type_to_internal(bp_type: str) -> OrderType:
        type_upper = (bp_type or "").upper()
        mapping = {
            "LIMIT": OrderType.LIMIT,
            "MARKET": OrderType.MARKET,
            "STOP_MARKET": OrderType.STOP_MARKET,
            "STOP_LIMIT": OrderType.STOP_LIMIT,
            "TAKE_PROFIT_MARKET": OrderType.TAKE_PROFIT_MARKET,
            "TAKE_PROFIT_LIMIT": OrderType.TAKE_PROFIT_LIMIT,
        }
        if type_upper in mapping:
            return mapping[type_upper]
        logger.warning(
            f"[BackpackOrderMapper] Unknown order type '{bp_type}', defaulting to LIMIT."
        )
        return OrderType.LIMIT

    @staticmethod
    def map_tif_to_internal(bp_tif: str | None) -> TimeInForce:
        if not bp_tif:
            return TimeInForce.GTC
        try:
            return TimeInForce(bp_tif.upper())
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown TIF '{bp_tif}', defaulting to GTC.")
            return TimeInForce.GTC

    @staticmethod
    def map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        if not trigger_by:
            return None
        try:
            return TriggerType(trigger_by)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown trigger_by '{trigger_by}', returning None."
            )
            return None

    @staticmethod
    def map_stp_to_internal(stp: str | None) -> SelfTradePrevention | None:
        if not stp:
            return None
        try:
            return SelfTradePrevention(stp)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown self_trade_prevention '{stp}', returning None."
            )
            return None

    @staticmethod
    def map_expiry_reason_to_internal(reason: str | None) -> OrderExpiryReason | None:
        if not reason:
            return None
        try:
            return OrderExpiryReason(reason)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown expiry_reason '{reason}', returning None."
            )
            return None

    @staticmethod
    def map_origin_to_internal(origin: str | None) -> OrderUpdateOrigin | None:
        if not origin:
            return None
        try:
            return OrderUpdateOrigin(origin)
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown origin '{origin}', returning None.")
            return None

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        # Defensive: ensure required fields are present and valid
        parsed_quantity = parse_decimal_value(raw.quantity, allow_none=False)
        if parsed_quantity is None:
            raise ValueError("quantity missing/invalid in BackpackRawOrder")
        parsed_created_at = parse_datetime_utc(raw.createdAt)
        if parsed_created_at is None:
            raise ValueError("createdAt missing/invalid in BackpackRawOrder")
        # Optional fields
        parsed_quantity_filled = parse_decimal_value(raw.executedQuantity) or Decimal("0.0")
        parsed_price = parse_decimal_value(raw.price)
        parsed_stop_price = parse_decimal_value(raw.triggerPrice)
        parsed_avg_fill_price = parse_decimal_value(raw.avgFillPrice)
        return Order(
            client_order_id=raw.clientId or "",
            exchange_order_id=raw.id,
            related_order_id=raw.relatedOrderId,
            exchange=ExchangeName.BACKPACK,
            symbol=raw.symbol,
            side=BackpackOrderMapper.map_side_to_internal(raw.side),
            order_type=BackpackOrderMapper.map_type_to_internal(raw.orderType),
            status=BackpackOrderMapper.map_status_to_internal(raw.status),
            quantity_requested=parsed_quantity,
            quantity_filled=parsed_quantity_filled,
            price=parsed_price,
            stop_price=parsed_stop_price,
            average_fill_price=parsed_avg_fill_price,
            trigger_by=BackpackOrderMapper.map_trigger_by_to_internal(raw.triggerBy),
            time_in_force=BackpackOrderMapper.map_tif_to_internal(raw.timeInForce),
            reduce_only=raw.reduceOnly or False,
            post_only=raw.postOnly or False,
            created_at=parsed_created_at,
            updated_at=parse_datetime_utc(raw.updatedAt),
            triggered_at=parse_datetime_utc(raw.triggeredAt),
            strategy_name=None,
            signal_id=None,
            trades=[],
        )

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, raw: BackpackRawBalance
    ) -> SpotBalance:
        """Transforms a raw Backpack balance object into an internal SpotBalance.

        Args:
            asset_symbol: The symbol of the asset (e.g., 'USDC').
            raw: The validated BackpackRawBalance object.

        Returns:
            The corresponding SpotBalance object.

        Raises:
            ValueError: If essential numeric fields (total, available) are missing or invalid.
        """
        # Defensive parsing of numeric strings
        parsed_total = parse_decimal_value(
            raw.total, allow_none=False, field_name=f"{asset_symbol}_total"
        )
        parsed_available = parse_decimal_value(
            raw.available, allow_none=False, field_name=f"{asset_symbol}_available"
        )

        if parsed_total is None:
            raise ValueError(
                f"Total quantity missing/invalid for {asset_symbol} in BackpackRawBalance"
            )
        if parsed_available is None:
            raise ValueError(
                f"Available quantity missing/invalid for {asset_symbol} in BackpackRawBalance"
            )

        # Assuming no specific Backpack details for SpotBalance for now
        # Need to import BackpackSpotBalanceDetails if used
        bp_details = None

        return SpotBalance(
            exchange=ExchangeName.BACKPACK,
            asset=asset_symbol.upper(),  # Ensure asset is uppercase
            timestamp=datetime.now(UTC),  # Use current time as timestamp is not in raw balance data
            total_quantity=parsed_total,
            available_quantity=parsed_available,
            bp_details=bp_details,  # Add slot if defined in SpotBalance
        )

    @staticmethod
    def transform_raw_position_to_internal(raw: BackpackRawPosition) -> DerivativePosition:
        """Transforms a raw Backpack position object into an internal DerivativePosition.

        Args:
            raw: The validated BackpackRawPosition object.

        Returns:
            The corresponding DerivativePosition object.

        Raises:
            ValueError: If essential numeric fields are missing or invalid.
        """
        # Parse core numeric fields defensively
        size_dec = parse_decimal_value(
            raw.net_quantity, allow_none=False, field_name="net_quantity"
        )
        if size_dec is None:  # Should be unreachable due to allow_none=False
            raise ValueError("net_quantity missing/invalid in BackpackRawPosition")

        entry_price_dec = parse_decimal_value(raw.entry_price)
        mark_price_dec = parse_decimal_value(raw.mark_price)
        liq_price_dec = parse_decimal_value(raw.est_liquidation_price)
        unrealized_pnl_dec = parse_decimal_value(raw.pnl_unrealized)
        realized_pnl_dec = parse_decimal_value(raw.pnl_realized)

        # Determine side
        side = OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL
        if size_dec == Decimal("0"):
            # Adjust entry price to None if size is zero, per DerivativePosition logic
            entry_price_dec = None
            # Consider how to handle side for zero position if needed, maybe None or keep last?
            # For now, stick to BUY/SELL based on last non-zero state implied by raw data.

        # Backpack API does not seem to provide a position timestamp directly
        timestamp = datetime.now(UTC)

        # Create BackpackPositionDetails (Optional - skipping for now)
        bp_details = None  # Placeholder
        # TODO: Parse raw.imf_function, raw.mmf_function, raw.cumulative_funding_payment
        #       into BackpackPositionDetails if required.

        return DerivativePosition(
            exchange=ExchangeName.BACKPACK,
            symbol=raw.symbol,
            timestamp=timestamp,
            side=side,
            size=size_dec,
            entry_price=entry_price_dec,
            mark_price=mark_price_dec,
            liquidation_price=liq_price_dec,
            unrealized_pnl=unrealized_pnl_dec,
            realized_pnl=realized_pnl_dec,
            bp_details=bp_details,
        )

    @staticmethod
    def transform_raw_trade_to_internal(raw: BackpackRawTrade) -> Trade | None:
        """Transforms a raw Backpack trade object into an internal Trade.

        Returns None if essential information (like side) cannot be determined from raw data.

        Args:
            raw: The validated BackpackRawTrade object.

        Returns:
            The corresponding Trade object, or None if side cannot be determined.

        Raises:
            ValueError: If essential fields (price, qty, time) are missing or invalid.
        """
        price_dec = parse_decimal_value(raw.price, allow_none=False, field_name="price")
        quantity_dec = parse_decimal_value(raw.quantity, allow_none=False, field_name="quantity")
        timestamp = parse_datetime_utc(raw.time, field_name="time")

        if price_dec is None:  # Should be unreachable
            raise ValueError("price missing/invalid in BackpackRawTrade")
        if quantity_dec is None:  # Should be unreachable
            raise ValueError("quantity missing/invalid in BackpackRawTrade")
        if timestamp is None:  # Should be unreachable
            raise ValueError("time missing/invalid in BackpackRawTrade")

        # Backpack REST API for recent trades doesn't provide side, fee, or maker status.
        # We might infer side if we also have the corresponding order, but not from trade alone.
        # Setting defaults for now.
        # TODO: Enhance if more info becomes available or link with order data.
        # Since side is mandatory in core Trade model, we cannot create a valid Trade object.
        # Returning None and logging a warning.
        logger.warning(
            f"[BackpackOrderMapper] Cannot determine trade side for raw trade {raw.id} "
            f"from REST API. Skipping transformation. Use WebSocket stream for complete data."
        )
        return None

    @staticmethod
    def transform_raw_funding_rate_to_internal(raw: BackpackRawFundingRate) -> FundingRate:
        """Transforms a raw Backpack funding rate object into an internal FundingRate.

        Args:
            raw: The validated BackpackRawFundingRate object.

        Returns:
            The corresponding FundingRate object.

        Raises:
            ValueError: If essential fields are missing or invalid.
        """
        funding_rate_dec = parse_decimal_value(
            raw.funding_rate, allow_none=False, field_name="funding_rate"
        )
        mark_price_dec = parse_decimal_value(raw.mark_price, field_name="mark_price")
        timestamp = parse_datetime_utc(raw.time, field_name="time")

        if funding_rate_dec is None:  # Should be unreachable
            raise ValueError("funding_rate missing/invalid in BackpackRawFundingRate")
        if timestamp is None:  # Should be unreachable
            raise ValueError("time missing/invalid in BackpackRawFundingRate")

        return FundingRate(
            symbol=raw.symbol,
            timestamp=timestamp,
            funding_rate=funding_rate_dec,
            mark_price=mark_price_dec,  # Can be None
            index_price=None,  # Not directly mapped, raw.index_price exists if needed
        )

    @staticmethod
    def transform_raw_orderbook_to_internal(symbol: str, raw: BackpackRawOrderBook) -> OrderBook:
        """Transforms a raw Backpack order book object into an internal OrderBook.

        Args:
            symbol: The market symbol this order book belongs to.
            raw: The validated BackpackRawOrderBook object.

        Returns:
            The corresponding OrderBook object.

        Raises:
            ValueError: If timestamp is missing/invalid or level parsing fails.
        """
        timestamp_dt = parse_datetime_utc(raw.timestamp)
        if timestamp_dt is None:
            raise ValueError("timestamp missing/invalid in BackpackRawOrderBook")

        bids: list[tuple[Decimal, Decimal]] = []
        asks: list[tuple[Decimal, Decimal]] = []

        try:
            for price_str, qty_str in raw.bids:
                price = parse_decimal_value(price_str, allow_none=False, field_name="bid_price")
                qty = parse_decimal_value(qty_str, allow_none=False, field_name="bid_qty")
                if (
                    price is not None and qty is not None
                ):  # Defensive, though validator should prevent
                    bids.append((price, qty))
            for price_str, qty_str in raw.asks:
                price = parse_decimal_value(price_str, allow_none=False, field_name="ask_price")
                qty = parse_decimal_value(qty_str, allow_none=False, field_name="ask_qty")
                if price is not None and qty is not None:
                    asks.append((price, qty))
        except ValueError as e:
            logger.error(f"[{ExchangeName.BACKPACK}] Error parsing order book levels: {e}")
            # Re-raise or handle as appropriate - for now, re-raise to signal failure
            raise ValueError(f"Failed to parse order book levels: {e}") from e

        # Backpack order book data is already sorted by price (highest bid first, lowest ask first)
        return OrderBook(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=timestamp_dt,
        )

    @staticmethod
    def transform_raw_fill_to_internal(raw: BackpackRawFill) -> Trade:
        """Transforms a raw Backpack fill object from history into an internal Trade.

        Args:
            raw: The validated BackpackRawFill object.

        Returns:
            The corresponding Trade object.

        Raises:
            ValueError: If essential fields (price, qty, fee, time) are missing or invalid.
        """
        price_dec_raw = parse_decimal_value(raw.price, allow_none=False, field_name="price")
        quantity_dec_raw = parse_decimal_value(
            raw.quantity, allow_none=False, field_name="quantity"
        )
        fee_dec_raw = parse_decimal_value(raw.fee, allow_none=False, field_name="fee")
        timestamp_dt = parse_datetime_utc(raw.timestamp)  # Use raw string directly

        # Add explicit None checks after parsing, even with allow_none=False
        if price_dec_raw is None:
            raise ValueError("price missing/invalid in BackpackRawFill despite allow_none=False")
        if quantity_dec_raw is None:
            raise ValueError("quantity missing/invalid in BackpackRawFill despite allow_none=False")
        if fee_dec_raw is None:
            raise ValueError("fee missing/invalid in BackpackRawFill despite allow_none=False")
        if timestamp_dt is None:
            raise ValueError("timestamp missing/invalid in BackpackRawFill")

        price_dec = price_dec_raw
        quantity_dec = quantity_dec_raw
        fee_dec = fee_dec_raw

        # Map side
        side = BackpackOrderMapper.map_side_to_internal(raw.side)

        # Create the internal Trade object
        return Trade(
            id=str(raw.trade_id),
            exchange=ExchangeName.BACKPACK,
            symbol=raw.symbol,
            order_id=raw.order_id,
            client_order_id=raw.client_id,
            side=side,
            price=price_dec,
            quantity=quantity_dec,
            fee=fee_dec,
            fee_asset=raw.fee_symbol,
            is_maker=raw.is_maker,
            executed_at=timestamp_dt,
        )

    # --- WebSocket Event Transformers ---

    @staticmethod
    def transform_ws_ticker_event_to_internal(raw: BackpackRawTickerEvent) -> Ticker:
        """Transforms a raw Backpack WS ticker event into an internal Ticker.

        Maps available fields (lastPrice -> price, volume). Bid/Ask are not
        typically in ticker events, so they are left as None.
        """
        # Assuming Ticker model can handle string inputs if validated, otherwise parse here
        # Defensive parsing for safety
        parsed_price = parse_decimal_value(raw.last_price, allow_none=False, field_name="lastPrice")
        # high = parse_decimal_value(raw.high, allow_none=False, field_name="high") # Not in Ticker
        # low = parse_decimal_value(raw.low, allow_none=False, field_name="low") # Not in Ticker
        parsed_volume = parse_decimal_value(raw.volume, allow_none=False, field_name="volume")
        # quote_volume = parse_decimal_value(raw.quote_volume) # Not in Ticker
        # price_change = parse_decimal_value(raw.price_change_percent) # Not in Ticker

        if parsed_price is None:
            raise ValueError("last_price missing/invalid")
        # if high is None:
        #     raise ValueError("high missing/invalid")
        # if low is None:
        #     raise ValueError("low missing/invalid")
        if parsed_volume is None:
            raise ValueError("volume missing/invalid")

        # Map fields from event to Ticker model
        # Bid/Ask are not available in this event, set to None.
        return Ticker(
            symbol=raw.symbol,
            timestamp=datetime.now(UTC),  # Use current time as event might not have it
            price=parsed_price,  # Map lastPrice -> price
            volume=parsed_volume,  # Map volume -> volume
            bid=None,  # Not available in event
            ask=None,  # Not available in event
        )

    @staticmethod
    def transform_ws_depth_event_to_internal(
        symbol: str, raw: BackpackRawDepthUpdateEvent
    ) -> OrderBook:
        """Transforms a raw Backpack WS depth event into an internal OrderBook."""
        # This assumes the WS event is a snapshot. If it's a diff, logic needs change.
        # Reuses the logic from the REST order book transformer.

        # Backpack WS doesn't seem to include timestamp in depth update msg
        timestamp_dt = datetime.now(UTC)

        bids: list[tuple[Decimal, Decimal]] = []
        asks: list[tuple[Decimal, Decimal]] = []

        try:
            for price_str, qty_str in raw.bids:
                price = parse_decimal_value(price_str, allow_none=False, field_name="ws_bid_price")
                qty = parse_decimal_value(qty_str, allow_none=False, field_name="ws_bid_qty")
                if price is not None and qty is not None:
                    bids.append((price, qty))
            for price_str, qty_str in raw.asks:
                price = parse_decimal_value(price_str, allow_none=False, field_name="ws_ask_price")
                qty = parse_decimal_value(qty_str, allow_none=False, field_name="ws_ask_qty")
                if price is not None and qty is not None:
                    asks.append((price, qty))
        except ValueError as e:
            logger.error(f"[{ExchangeName.BACKPACK}] Error parsing WS order book levels: {e}")
            raise ValueError(f"Failed to parse WS order book levels: {e}") from e

        # Assume data is sorted correctly from WS
        return OrderBook(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=timestamp_dt,
            # last_update_id=raw.last_update_id # Internal OrderBook might not have this
        )

    @staticmethod
    def transform_ws_trade_event_to_internal(raw: BackpackRawTradeEvent) -> Trade:
        """Transforms a raw Backpack WS trade event into an internal Trade."""
        price_dec = parse_decimal_value(raw.price, allow_none=False, field_name="price")
        quantity_dec = parse_decimal_value(raw.quantity, allow_none=False, field_name="quantity")
        # Use engine_timestamp if available, else event_time
        timestamp_raw = raw.engine_timestamp if raw.engine_timestamp is not None else raw.event_time
        timestamp_dt = parse_datetime_utc(timestamp_raw)

        if price_dec is None:
            raise ValueError("price missing/invalid")
        if quantity_dec is None:
            raise ValueError("quantity missing/invalid")
        if timestamp_dt is None:
            raise ValueError("timestamp missing/invalid")

        # Infer side: If buyer is maker, seller initiated (SELL). If seller is maker, buyer initiated (BUY).
        # This assumes 'm' maps directly to our is_maker field definition.
        side = OrderSide.BUY if not raw.is_buyer_the_maker else OrderSide.SELL

        # WS trade event doesn't provide fee info
        fee_dec = Decimal("0")
        fee_asset = "USDC"  # Assume quote asset, needs confirmation

        return Trade(
            id=raw.trade_id,  # Use trade_id as internal ID
            exchange=ExchangeName.BACKPACK,
            symbol=raw.symbol,
            order_id=raw.buyer_order_id if side == OrderSide.BUY else raw.seller_order_id,
            client_order_id=None,  # Not provided in public trade stream
            side=side,
            price=price_dec,
            quantity=quantity_dec,
            fee=fee_dec,
            fee_asset=fee_asset,
            is_maker=not raw.is_buyer_the_maker
            if side == OrderSide.BUY
            else raw.is_buyer_the_maker,
            executed_at=timestamp_dt,
        )

    @staticmethod
    def transform_raw_kline_to_internal(
        symbol: str, interval: str, raw: BackpackRawKline
    ) -> Candle:
        """Transforms a raw Backpack kline into an internal Candle model.

        Args:
            symbol: The trading symbol for the candle.
            interval: The interval string (e.g., '1m', '1h').
            raw: The validated BackpackRawKline object.

        Returns:
            A validated Candle object.

        Raises:
            ValueError: If the raw data cannot be parsed into a valid Candle.
                      (e.g., timestamp parsing failure)
        """
        try:
            # Convert start time from milliseconds to datetime object
            open_time = parse_datetime_utc(raw.start_time_ms, field_name="start_time_ms")
            if open_time is None:
                # This should not happen if raw.start_time_ms is validated as int
                raise ValueError("Parsed open_time is None unexpectedly.")

            # Create the Candle object, relying on its validators for OHLC, volume, etc.
            candle = Candle(
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                open=raw.open_price,
                high=raw.high_price,
                low=raw.low_price,
                close=raw.close_price,
                volume=raw.volume,
            )
            return candle
        except (ValidationError, ValueError) as e:
            # Log and re-raise potential validation errors during Candle creation
            logger.error(f"[{ExchangeName.BACKPACK}] Error transforming raw kline to Candle: {e}")
            raise ValueError(f"Error transforming kline data: {e}") from e
        except Exception as e:
            # Catch unexpected errors during transformation
            logger.exception(f"[{ExchangeName.BACKPACK}] Unexpected error transforming kline: {e}")
            raise ValueError(f"Unexpected error transforming kline: {e}") from e
