"""
HyperliquidOrderMapper: Maps validated Hyperliquid raw order models to
CyberDeltaEngine's internal Order model.
"""

import logging
import re
import time
from decimal import Decimal
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_api_error import (
    HYPERLIQUID_ERROR_STRINGS,
    HyperliquidAPIErrorCategory,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_api_error import HyperliquidRawApiError
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawPositionInfo
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawPositionInfo as WsPositionInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.apis.models.api import APIError, APIErrorResponse
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    OrderBook,
    OrderSide,
    Position,
    Ticker,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.order import Order
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class HyperliquidOrderMapper:
    """
    Utility for transforming Hyperliquid raw order models to CyberDeltaEngine internal Order model.
    """

    @staticmethod
    def map_side_to_internal(hl_side: str) -> OrderSide:
        if hl_side == "B":
            return OrderSide.BUY
        elif hl_side == "A":
            return OrderSide.SELL
        logger.warning(
            f"[HyperliquidOrderMapper] Unknown order side '{hl_side}', defaulting to BUY."
        )
        return OrderSide.BUY

    @staticmethod
    def map_status_to_internal(hl_status: str) -> OrderStatus:
        status_map = {
            "open": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            # Add more mappings as needed
        }
        return status_map.get(hl_status.lower(), OrderStatus.UNKNOWN)

    @staticmethod
    def map_type_to_internal(
        order_type: dict[str, Any], trigger: HyperliquidRawTriggerInfo | None
    ) -> OrderType:
        # Hyperliquid uses nested dicts for orderType,
        #   e.g. {"limit": {"tif": "Gtc"}}, {"market": {}}
        if "limit" in order_type:
            if trigger:
                if getattr(trigger, "tpsl", None) == "sl":
                    return OrderType.STOP_LIMIT
                elif getattr(trigger, "tpsl", None) == "tp":
                    return OrderType.TAKE_PROFIT_LIMIT
            return OrderType.LIMIT
        elif "market" in order_type:
            if trigger:
                if getattr(trigger, "tpsl", None) == "sl":
                    return OrderType.STOP_MARKET
                elif getattr(trigger, "tpsl", None) == "tp":
                    return OrderType.TAKE_PROFIT_MARKET
            return OrderType.MARKET
        logger.warning(
            f"[HyperliquidOrderMapper] Unknown orderType structure: {order_type}. "
            "Defaulting to LIMIT."
        )
        return OrderType.LIMIT

    @staticmethod
    def map_time_in_force(order_type: dict[str, Any]) -> TimeInForce:
        # Only limit orders have TIF in HL
        if "limit" in order_type and isinstance(order_type["limit"], dict):
            limit_dict = cast(dict[str, Any], order_type["limit"])
            tif_val = limit_dict.get("tif", "")
            tif_str = str(tif_val)
            tif = tif_str.upper()
            if tif == "GTC":
                return TimeInForce.GTC
            elif tif == "IOC":
                return TimeInForce.IOC
            elif tif == "ALO":
                return TimeInForce.ALO
        return TimeInForce.GTC

    @staticmethod
    def transform_raw_order_to_internal(
        raw: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        # Defensive parsing and mapping
        side = HyperliquidOrderMapper.map_side_to_internal(raw.side)
        order_type = HyperliquidOrderMapper.map_type_to_internal(raw.order_type, trigger)
        status = HyperliquidOrderMapper.map_status_to_internal(raw.status)
        time_in_force = HyperliquidOrderMapper.map_time_in_force(raw.order_type)

        quantity_requested = parse_decimal_value(raw.sz, allow_none=False, field_name="sz")
        if quantity_requested is None:
            raise ValueError("quantity_requested (sz) is required and could not be parsed.")
        remaining_sz = parse_decimal_value(
            str(raw.remaining_sz), allow_none=True, field_name="remainingSz"
        )
        if remaining_sz is None:
            remaining_sz = Decimal("0")
        quantity_filled = quantity_requested - remaining_sz
        price = parse_decimal_value(str(raw.limit_px), allow_none=True, field_name="limitPx")
        created_at = parse_datetime_utc(raw.timestamp, field_name="timestamp")
        if created_at is None:
            raise ValueError("created_at (timestamp) is required and could not be parsed.")
        updated_at = parse_datetime_utc(raw.status_timestamp, field_name="statusTimestamp")

        # Trigger/stop logic
        stop_price = None
        trigger_by = None
        if trigger:
            stop_price = parse_decimal_value(
                str(getattr(trigger, "trigger_px", "")), allow_none=True, field_name="triggerPx"
            )
            # Hyperliquid does not specify trigger_by (Mark/Last/Index),
            #   so leave as None or infer if possible

        return Order(
            client_order_id=raw.cloid or str(raw.oid),
            exchange_order_id=str(raw.oid),
            related_order_id=None,
            exchange="hyperliquid",
            symbol=raw.asset,
            side=side,
            order_type=order_type,
            status=status,
            quantity_requested=quantity_requested,
            quantity_filled=quantity_filled,
            executed_quote_quantity=None,
            price=price,
            stop_price=stop_price,
            average_fill_price=None,  # HL does not provide this in open order
            trigger_by=trigger_by,
            time_in_force=time_in_force,
            reduce_only=raw.reduce_only,
            post_only=(time_in_force == TimeInForce.ALO),
            self_trade_prevention=None,
            created_at=created_at,
            updated_at=updated_at,
            triggered_at=None,
            expiry_reason=None,
            origin=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
        )


class HyperliquidMapper:
    @staticmethod
    def map_raw_ctx_to_ticker(ctx: dict[str, Any]) -> Ticker:
        """
        Map a raw asset context dict from Hyperliquid to a Ticker business logic model.
        """
        mark_px = ctx.get("markPx")
        bid = parse_decimal_value(str(mark_px), allow_none=True)
        ask = parse_decimal_value(str(mark_px), allow_none=True)
        symbol = ctx.get("name", "")
        return Ticker(
            symbol=symbol,
            bid=bid,
            ask=ask,
            timestamp=int(time.time() * 1000),
        )

    @staticmethod
    def map_raw_order_book(
        symbol: str, data: dict[str, Any], depth: int | None = None
    ) -> OrderBook:
        """
        Map a raw Hyperliquid order book response to an internal OrderBook model.
        Defensive: Handles malformed or missing data gracefully.
        """
        levels_raw = data.get("levels")
        levels: list[Any] = levels_raw if isinstance(levels_raw, list) else []
        bids: list[tuple[Decimal, Decimal]] = []
        asks: list[tuple[Decimal, Decimal]] = []
        if len(levels) > 1:
            bid_levels_raw = levels[0]
            bid_levels: list[Any] = bid_levels_raw if isinstance(bid_levels_raw, list) else []
            for level in bid_levels:
                if not isinstance(level, list):
                    continue
                level_typed: list[Any] = level
                if len(level_typed) >= 2:
                    price = parse_decimal_value(
                        level_typed[0], allow_none=False, field_name="orderbook.bid.price"
                    )
                    quantity = parse_decimal_value(
                        level_typed[1], allow_none=False, field_name="orderbook.bid.qty"
                    )
                    if price is not None and quantity is not None:
                        bids.append((price, quantity))
            ask_levels_raw = levels[1]
            ask_levels: list[Any] = ask_levels_raw if isinstance(ask_levels_raw, list) else []
            for level in ask_levels:
                if not isinstance(level, list):
                    continue
                level_typed_ask: list[Any] = level
                if len(level_typed_ask) >= 2:
                    price = parse_decimal_value(
                        level_typed_ask[0], allow_none=False, field_name="orderbook.ask.price"
                    )
                    quantity = parse_decimal_value(
                        level_typed_ask[1], allow_none=False, field_name="orderbook.ask.qty"
                    )
                    if price is not None and quantity is not None:
                        asks.append((price, quantity))
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        if depth is not None and depth > 0:
            bids = bids[:depth]
            asks = asks[:depth]
        timestamp_raw = data.get("time")
        timestamp = None
        if isinstance(timestamp_raw, int | float | str):
            try:
                timestamp = int(float(timestamp_raw))
            except Exception:
                timestamp = int(time.time() * 1000)
        else:
            timestamp = int(time.time() * 1000)
        return OrderBook(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=timestamp,
        )

    @staticmethod
    def map_raw_trades(symbol: str, data: list[Any], limit: int | None = None) -> list[Trade]:
        """
        Map a list of raw Hyperliquid trade dicts to a list of internal Trade models.
        Defensive: Handles malformed or missing data gracefully.
        """
        trades: list[Trade] = []
        for trade_item in data:
            if not isinstance(trade_item, dict):
                continue
            trade_dict: dict[str, Any] = cast(dict[str, Any], trade_item)
            trade_id = str(trade_dict.get("tid", ""))
            price = parse_decimal_value(
                trade_dict.get("px", "0"), allow_none=False, field_name="trade.price"
            )
            quantity = parse_decimal_value(
                trade_dict.get("sz", "0"), allow_none=False, field_name="trade.qty"
            )
            timestamp_val = trade_dict.get("time", 0)
            executed_at = parse_datetime_utc(timestamp_val, field_name="trade.time")
            side_hl = str(trade_dict.get("side", "B"))
            if price is None or quantity is None or executed_at is None:
                continue
            side = OrderSide.BUY if side_hl == "B" else OrderSide.SELL
            trades.append(
                Trade(
                    id=trade_id,
                    symbol=symbol,
                    executed_at=executed_at,
                    side=side,
                    order_id=trade_id,
                    exchange="hyperliquid",
                    client_order_id="",
                    price=price,
                    quantity=quantity,
                    cost=price * quantity,
                    fee=Decimal("0"),
                    fee_asset="USDC",
                    is_maker=False,
                    timestamp=int(executed_at.timestamp() * 1000),
                )
            )
        if limit is not None and limit > 0:
            trades = trades[:limit]
        return trades

    @staticmethod
    def map_raw_ctx_to_funding_rate(symbol: str, ctx: dict[str, Any]) -> FundingRate | None:
        """
        Map a raw asset context dict from Hyperliquid to a FundingRate business logic model.
        Defensive: Returns None if parsing fails.
        """
        try:
            funding_rate = parse_decimal_value(
                ctx.get("funding", "0"), allow_none=False, field_name="funding_rate"
            )
            mark_price = parse_decimal_value(
                ctx.get("markPx", "0"), allow_none=False, field_name="mark_price"
            )
            now_ms = int(time.time() * 1000)
            next_funding_time_ms = (now_ms // 3600000 + 1) * 3600000
            if funding_rate is None or mark_price is None:
                return None
            return FundingRate(
                symbol=symbol,
                funding_rate=funding_rate,
                mark_price=mark_price,
                next_funding_time=next_funding_time_ms,
            )
        except (ValueError, TypeError, KeyError):
            return None

    @staticmethod
    def _regex_match(msg: str, patterns: str | list[str]) -> bool:
        """
        Helper for regex-based error message matching.
        Accepts a single pattern or a list of patterns.
        """
        if isinstance(patterns, str):
            patterns = [patterns]
        return any(re.search(p, msg, re.IGNORECASE) for p in patterns)

    @staticmethod
    def categorize_hyperliquid_error(error_message: str) -> HyperliquidAPIErrorCategory:
        """
        Map a Hyperliquid error message to a known error category, ERROR, or UNKNOWN.
        Uses canonical error substrings from hl_api_error.py for initial matching,
        then regex for variants.
        """
        if not error_message:
            return HyperliquidAPIErrorCategory.UNKNOWN
        msg = error_message.strip().lower()
        if msg == "error":
            return HyperliquidAPIErrorCategory.ERROR
        # Canonical string check (from hl_api_error.py)
        for canonical, category in HYPERLIQUID_ERROR_STRINGS.items():
            if canonical in msg:
                return category
        # Fallback to regex/robust matching for variants
        if HyperliquidMapper._regex_match(msg, r"insufficient balance"):
            return HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE
        if HyperliquidMapper._regex_match(msg, r"invalid signature"):
            return HyperliquidAPIErrorCategory.INVALID_SIGNATURE
        if HyperliquidMapper._regex_match(msg, r"invalid asset"):
            return HyperliquidAPIErrorCategory.INVALID_ASSET
        if HyperliquidMapper._regex_match(msg, r"invalid order type"):
            return HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE
        if HyperliquidMapper._regex_match(msg, r"order size too small"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL
        if HyperliquidMapper._regex_match(msg, r"order size too large"):
            return HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE
        if HyperliquidMapper._regex_match(msg, r"price out of bounds"):
            return HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS
        if HyperliquidMapper._regex_match(msg, r"rate limit exceeded"):
            return HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED
        if HyperliquidMapper._regex_match(msg, r"unauthorized"):
            return HyperliquidAPIErrorCategory.UNAUTHORIZED
        if HyperliquidMapper._regex_match(msg, r"internal server error"):
            return HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR
        if HyperliquidMapper._regex_match(msg, r"order must have minimum value"):
            return HyperliquidAPIErrorCategory.ORDER_MIN_VALUE
        if HyperliquidMapper._regex_match(
            msg, [r"order was never placed", r"already canceled", r"already filled"]
        ):
            return HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED
        if HyperliquidMapper._regex_match(msg, r"invalid twap duration"):
            return HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION
        if HyperliquidMapper._regex_match(
            msg, [r"twap was never placed", r"twap already canceled", r"twap already filled"]
        ):
            return HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED
        return HyperliquidAPIErrorCategory.UNKNOWN

    @staticmethod
    def map_category_to_api_error_code(
        category_or_message: HyperliquidAPIErrorCategory | str,
    ) -> APIErrorCode:
        """
        Map a HyperliquidAPIErrorCategory or raw error message (str) to APIErrorCode.
        If a string is provided, it is first categorized using regex logic.
        """
        if isinstance(category_or_message, str):
            category = HyperliquidMapper.categorize_hyperliquid_error(category_or_message)
        else:
            category = category_or_message
        mapping = {
            HyperliquidAPIErrorCategory.INSUFFICIENT_BALANCE: APIErrorCode.INSUFFICIENT_FUNDS,
            HyperliquidAPIErrorCategory.INVALID_SIGNATURE: APIErrorCode.AUTHENTICATION_FAILED,
            HyperliquidAPIErrorCategory.INVALID_ASSET: APIErrorCode.INVALID_SYMBOL,
            HyperliquidAPIErrorCategory.INVALID_ORDER_TYPE: APIErrorCode.INVALID_REQUEST,
            HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_SMALL: APIErrorCode.INVALID_ORDER_SIZE,
            HyperliquidAPIErrorCategory.ORDER_SIZE_TOO_LARGE: APIErrorCode.INVALID_ORDER_SIZE,
            HyperliquidAPIErrorCategory.PRICE_OUT_OF_BOUNDS: APIErrorCode.PRICE_OUT_OF_RANGE,
            HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED: APIErrorCode.RATE_LIMITED,
            HyperliquidAPIErrorCategory.UNAUTHORIZED: APIErrorCode.AUTHENTICATION_FAILED,
            HyperliquidAPIErrorCategory.INTERNAL_SERVER_ERROR: APIErrorCode.SERVER_ERROR,
            HyperliquidAPIErrorCategory.ORDER_MIN_VALUE: APIErrorCode.MIN_NOTIONAL_NOT_MET,
            HyperliquidAPIErrorCategory.ORDER_NOT_FOUND_OR_FILLED: APIErrorCode.ORDER_NOT_FOUND,
            HyperliquidAPIErrorCategory.INVALID_TWAP_DURATION: APIErrorCode.INVALID_REQUEST,
            HyperliquidAPIErrorCategory.TWAP_NOT_FOUND_OR_FILLED: APIErrorCode.ORDER_NOT_FOUND,
            HyperliquidAPIErrorCategory.UNKNOWN: APIErrorCode.EXCHANGE_SPECIFIC,
            HyperliquidAPIErrorCategory.ERROR: APIErrorCode.EXCHANGE_SPECIFIC,
        }
        return mapping.get(category, APIErrorCode.EXCHANGE_SPECIFIC)

    @staticmethod
    def map_error_response(
        response: dict[str, Any],
        http_status: int | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """
        Transform a raw Hyperliquid error response into a standardized APIError.
        This is the single entry point for mapping/categorizing/normalizing Hyperliquid errors.
        Args:
            response: The raw error response dict from Hyperliquid (should contain 'error').
            http_status: Optional HTTP status code from the response.
            original_exception: Optional original exception for chaining.
        Returns:
            APIError: The standardized internal error model for business logic.
        """
        try:
            error_obj = HyperliquidRawApiError.model_validate(response)
            category = HyperliquidMapper.categorize_hyperliquid_error(error_obj.error)
        except ValidationError:
            category = HyperliquidAPIErrorCategory.UNKNOWN
            error_obj = None

        code = HyperliquidMapper.map_category_to_api_error_code(category)
        message = getattr(error_obj, "error", str(response))
        error_response = APIErrorResponse.from_exchange_error(
            message=message,
            code=code.value,
            http_status=http_status,
            exchange_code=None,
            exchange_message=message,
            original_exception=original_exception,
        )
        return APIError(
            message=error_response.message,
            code=error_response.code,
            http_status=error_response.http_status,
            exchange_code=error_response.exchange_code,
            exchange_message=error_response.exchange_message,
            retry_after=error_response.retry_after,
            original_exception=error_response.original_exception,
        )


# --- Additional Hyperliquid Mappers ---


class HyperliquidUserFillMapper:
    """
    Maps a validated HyperliquidRawUserFill to an internal Trade model.
    Defensive: Handles malformed or missing data gracefully.
    """

    @staticmethod
    def map(raw: HyperliquidRawUserFill) -> Trade | None:
        try:
            price = parse_decimal_value(raw.px, allow_none=False, field_name="fill.price")
            quantity = parse_decimal_value(raw.sz, allow_none=False, field_name="fill.qty")
            executed_at = parse_datetime_utc(raw.time, field_name="fill.time")
            if price is None or quantity is None or executed_at is None:
                return None
            side = OrderSide.BUY if raw.side == "B" else OrderSide.SELL
            return Trade(
                id=str(raw.tid),
                symbol=raw.coin,
                executed_at=executed_at,
                side=side,
                order_id=str(raw.oid),
                exchange="hyperliquid",
                client_order_id=raw.cloid or "",
                price=price,
                quantity=quantity,
                cost=price * quantity,
                fee=parse_decimal_value(raw.fee, allow_none=True, field_name="fill.fee")
                or Decimal("0"),
                fee_asset="USDC",
                is_maker=raw.is_maker,
                timestamp=int(executed_at.timestamp() * 1000),
            )
        except Exception:
            return None


class HyperliquidPositionMapper:
    """
    Maps a validated HyperliquidRawPositionInfo to an internal Position model.
    Defensive: Handles malformed or missing data gracefully.
    """

    @staticmethod
    def map(raw: HyperliquidRawPositionInfo) -> Position | None:
        try:
            size = parse_decimal_value(raw.szi, allow_none=False, field_name="position.size")
            entry_price = parse_decimal_value(
                raw.entry_px, allow_none=False, field_name="position.entry_price"
            )
            mark_price = parse_decimal_value(
                raw.position_value, allow_none=True, field_name="position.position_value"
            )
            unrealized_pnl = parse_decimal_value(
                raw.unrealized_pnl, allow_none=True, field_name="position.unrealized_pnl"
            )
            if size is None or entry_price is None:
                return None
            side = OrderSide.BUY if size > 0 else OrderSide.SELL
            return Position(
                symbol=raw.coin,
                size=size,
                entry_price=entry_price,
                mark_price=mark_price,
                side=side,
                liquidation_price=parse_decimal_value(
                    raw.liquidation_px, allow_none=True, field_name="position.liquidation_px"
                ),
                unrealized_pnl=unrealized_pnl,
                leverage=Decimal(str(raw.leverage.value))
                if hasattr(raw, "leverage") and hasattr(raw.leverage, "value")
                else None,
            )
        except Exception:
            return None

    @staticmethod
    def map_balance(raw: dict[str, Any]) -> Balance | None:
        # Placeholder: implement if/when balance fields are available in user state
        return None


class HyperliquidCandleMapper:
    """
    Maps a validated HyperliquidRawCandleSnapshot to a list of internal Candle models.
    """

    @staticmethod
    def map(raw: HyperliquidRawCandleSnapshot, symbol: str) -> list[Candle]:
        candles: list[Candle] = []
        if raw.candles is None or raw.interval is None:
            logger.warning("Missing 'candles' or 'interval' in HyperliquidRawCandleSnapshot.")
            return candles

        interval_str = raw.interval

        for candle_data in raw.candles:
            if not candle_data:
                continue
            try:
                # The parsing logic remains similar, but instantiation uses Candle
                # Candle's validators will handle detailed checks (positivity, finite, consistency)

                # Raw data extraction (with defensive .get)
                raw_t = candle_data.get("t")
                raw_o = candle_data.get("o")
                raw_h = candle_data.get("h")
                raw_l = candle_data.get("l")
                raw_c = candle_data.get("c")
                raw_v = candle_data.get("v")

                # Attempt to create Candle instance, letting its validators handle parsing & checks
                candle = Candle(
                    symbol=symbol,
                    interval=interval_str,
                    open_time=raw_t,  # Pass raw value to Candle's validator
                    open=raw_o,  # Pass raw value to Candle's validator
                    high=raw_h,  # Pass raw value to Candle's validator
                    low=raw_l,  # Pass raw value to Candle's validator
                    close=raw_c,  # Pass raw value to Candle's validator
                    volume=raw_v,  # Pass raw value to Candle's validator
                )
                candles.append(candle)
            except (ValidationError, ValueError, TypeError) as e:
                # Catch validation errors from Candle creation or parsing issues
                logger.warning(f"Error processing or validating candle data {candle_data}: {e}")
                continue
        return candles


class HyperliquidWsEventMapper:
    """
    Maps validated Hyperliquid WebSocket event models to internal models.
    Includes mappers for fills, trades, order book updates, and positions.
    """

    @staticmethod
    def map_fill_event(raw: HyperliquidRawWsFillEvent) -> Trade | None:
        try:
            price = parse_decimal_value(raw.px, allow_none=False, field_name="ws.fill.price")
            quantity = parse_decimal_value(raw.sz, allow_none=False, field_name="ws.fill.qty")
            executed_at = parse_datetime_utc(raw.time, field_name="ws.fill.time")
            if price is None or quantity is None or executed_at is None:
                return None
            side = OrderSide.BUY if raw.side == "B" else OrderSide.SELL
            return Trade(
                id=raw.hash,
                symbol=raw.coin,
                executed_at=executed_at,
                side=side,
                order_id=str(raw.oid),
                exchange="hyperliquid",
                client_order_id=raw.cloid or "",
                price=price,
                quantity=quantity,
                cost=price * quantity,
                fee=Decimal("0"),
                fee_asset="USDC",
                is_maker=raw.is_maker,
                timestamp=int(executed_at.timestamp() * 1000),
            )
        except Exception:
            return None

    @staticmethod
    def map_trade_event(raw: HyperliquidRawWsTradeEvent) -> Trade | None:
        try:
            price = parse_decimal_value(raw.px, allow_none=False, field_name="ws.trade.price")
            quantity = parse_decimal_value(raw.sz, allow_none=False, field_name="ws.trade.qty")
            executed_at = parse_datetime_utc(raw.time, field_name="ws.trade.time")
            if price is None or quantity is None or executed_at is None:
                return None
            side = OrderSide.BUY if raw.side == "B" else OrderSide.SELL
            return Trade(
                id=raw.hash,
                symbol=raw.coin,
                executed_at=executed_at,
                side=side,
                order_id="",
                exchange="hyperliquid",
                client_order_id="",
                price=price,
                quantity=quantity,
                cost=price * quantity,
                fee=Decimal("0"),
                fee_asset="USDC",
                is_maker=False,
                timestamp=int(executed_at.timestamp() * 1000),
            )
        except Exception:
            return None

    @staticmethod
    def map_orderbook_event(raw: HyperliquidRawWsBookUpdate, symbol: str) -> OrderBook | None:
        try:
            bids: list[tuple[Decimal, Decimal]] = []
            asks: list[tuple[Decimal, Decimal]] = []
            for level in raw.levels[0]:
                price: Decimal | None = parse_decimal_value(
                    level.px, allow_none=False, field_name="ws.orderbook.bid.price"
                )
                quantity: Decimal | None = parse_decimal_value(
                    level.sz, allow_none=False, field_name="ws.orderbook.bid.qty"
                )
                if price is not None and quantity is not None:
                    bids.append((price, quantity))
            for level in raw.levels[1]:
                price_ask: Decimal | None = parse_decimal_value(
                    level.px, allow_none=False, field_name="ws.orderbook.ask.price"
                )
                quantity_ask: Decimal | None = parse_decimal_value(
                    level.sz, allow_none=False, field_name="ws.orderbook.ask.qty"
                )
                if price_ask is not None and quantity_ask is not None:
                    asks.append((price_ask, quantity_ask))
            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=raw.time,
            )
        except Exception:
            return None

    @staticmethod
    def map_position_event(raw: WsPositionInfo) -> Position | None:
        # TODO: Map fields from WsPositionInfo to Position
        #       if structure differs from HyperliquidRawPositionInfo
        # For now, return None or implement a conversion if needed
        return None


class HyperliquidApiErrorMapper:
    """
    Stub for mapping Hyperliquid API error payloads to internal error codes/exceptions.
    TODO: Implement robust error mapping as needed.
    """

    @staticmethod
    def map(raw: dict[str, Any]) -> Exception:
        # TODO: Implement error mapping logic
        return Exception("Unmapped Hyperliquid API error: " + str(raw))
