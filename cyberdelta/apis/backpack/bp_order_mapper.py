"""
CyberDeltaEngine: Backpack Order & Market Data Mapper
-----------------------------------------------------

This module provides the `BackpackOrderMapper` class, a utility responsible for
transforming raw data structures received from the Backpack Exchange API into
CyberDeltaEngine's internal, standardized domain models.

Core Responsibilities:
- Mapping Backpack-specific string enum values (e.g., for order side, status, type)
  to CyberDeltaEngine's internal Python enums (`OrderSide`, `OrderStatus`, `OrderType`, etc.).
- Parsing and validating numeric strings into `Decimal` objects for prices and quantities.
- Converting timestamp formats (e.g., ISO strings, millisecond epochs) into UTC `datetime` objects.
- Assembling validated and transformed data into internal models like `Order`, `SpotBalance`,
  `DerivativePosition`, `Trade`, `FundingRate`, `OrderBook`, and `Candle`.
- Handling potential `None` values or variations in raw data defensively to prevent errors
  during transformation.

Usage:
  The methods in this class are typically called by `BackpackAPI` after raw API responses
  have been initially validated by their respective `BackpackRaw*` Pydantic models.
  All transformation methods are static and expect validated raw Pydantic models as input.

Example:
  `internal_order = BackpackOrderMapper.transform_raw_order_to_internal(validated_raw_order)`
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawOrderBook,
    BackpackRawTicker,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawFill,
    BackpackRawTrade,
    BackpackRawTradeEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
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
from cyberdelta.core.models.margin_account import BackpackMarginDetails
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class BackpackOrderMapper:
    """
    Utility class containing static methods for transforming raw Backpack API data models
    (e.g., `BackpackRawOrder`, `BackpackRawFill`, `BackpackRawTickerEvent`) into CyberDeltaEngine's
    standardized internal domain models (e.g., `Order`, `Trade`, `Ticker`).

    Each `transform_raw_*_to_internal` method takes a validated raw Pydantic model as input
    and returns a corresponding internal domain model. These methods handle:
    - Enum mapping (e.g., "BUY" -> `OrderSide.BUY`).
    - String to `Decimal` conversion for financial figures.
    - Timestamp string/integer to `datetime` object conversion.
    - Defensive handling of optional fields and potential inconsistencies in raw data.
    """

    @staticmethod
    def map_side_to_internal(bp_side: str) -> OrderSide:
        """
        Maps a raw Backpack order side string to the internal `OrderSide` enum.

        Handles variations like "Buy", "Sell", "Bid", "Ask" (case-insensitive).

        Args:
            bp_side (str): The raw order side string from Backpack.

        Returns:
            OrderSide: The corresponding internal `OrderSide` enum value.
                       Defaults to `OrderSide.BUY` and logs a warning if mapping fails.
        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL
        logger.warning(f"[BackpackOrderMapper] Unknown order side '{bp_side}', defaulting to BUY.")
        return OrderSide.BUY

    @staticmethod
    def map_status_to_internal(bp_status: str) -> OrderStatus:
        """
        Maps a raw Backpack order status string to the internal `OrderStatus` enum.
        Performs a case-insensitive match against known Backpack status strings.

        Args:
            bp_status (str): The raw order status string from Backpack (e.g., "FILLED",
                             "CANCELLED").

        Returns:
            OrderStatus: The corresponding internal `OrderStatus` enum value.
                         Defaults to `OrderStatus.UNKNOWN` and logs a warning if mapping fails.
        """
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
        """
        Maps a raw Backpack order type string to the internal `OrderType` enum.
        Performs a case-insensitive match against known Backpack order type strings.

        Args:
            bp_type (str): The raw order type string from Backpack (e.g., "LIMIT", "MARKET").

        Returns:
            OrderType: The corresponding internal `OrderType` enum value.
                       Defaults to `OrderType.LIMIT` and logs a warning if mapping fails.
        """
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
        """
        Maps a raw Backpack TimeInForce string to the internal `TimeInForce` enum.
        Handles common TIF values like "GTC", "IOC". Case-insensitive.

        Args:
            bp_tif (str | None): The raw TimeInForce string (e.g., "GTC", "IOC"), or None.

        Returns:
            TimeInForce: The corresponding internal `TimeInForce` enum value.
                         Defaults to `TimeInForce.GTC` if input is None or mapping fails.
        """
        if not bp_tif:
            return TimeInForce.GTC
        try:
            return TimeInForce(bp_tif.upper())
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown TIF '{bp_tif}', defaulting to GTC.")
            return TimeInForce.GTC

    @staticmethod
    def map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        """
        Maps a raw Backpack trigger type string (e.g., "lastPrice", "markPrice")
        to the internal `TriggerType` enum.

        Args:
            trigger_by (str | None): The raw trigger type string, or None.

        Returns:
            TriggerType | None: The corresponding internal `TriggerType` enum value, or None.
                                Logs a warning and returns None if mapping fails.
        """
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
        """
        Maps a raw Backpack Self-Trade Prevention (STP) string to the internal
        `SelfTradePrevention` enum.

        Args:
            stp (str | None): The raw STP string (e.g., "aggressive", "passive"), or None.

        Returns:
            SelfTradePrevention | None: The corresponding internal `SelfTradePrevention`
                                        enum value, or None. Logs a warning and returns
                                        None if mapping fails.
        """
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
        """
        Maps a raw Backpack order expiry reason string to the internal `OrderExpiryReason` enum.

        Args:
            reason (str | None): The raw expiry reason string, or None.

        Returns:
            OrderExpiryReason | None: The corresponding internal `OrderExpiryReason` enum value,
                                      or None. Logs a warning and returns None if mapping fails.
        """
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
        """
        Maps a raw Backpack order update origin string to the internal `OrderUpdateOrigin` enum.

        Args:
            origin (str | None): The raw origin string (e.g., "USER_ACTION", "SYSTEM"), or None.

        Returns:
            OrderUpdateOrigin | None: The corresponding internal `OrderUpdateOrigin` enum value,
                                        or None. Logs a warning and returns None if mapping fails.
        """
        if not origin:
            return None
        try:
            return OrderUpdateOrigin(origin)
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown origin '{origin}', returning None.")
            return None

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        """
        Transforms a validated `BackpackRawOrder` object into an internal `Order` domain model.

        This method performs enum mappings, decimal conversions, and datetime parsing using
        helper methods and utility functions. It assumes `raw` has already been validated by the
        `BackpackRawOrder` Pydantic model, ensuring structural integrity and basic format checks.

        Args:
            raw (BackpackRawOrder): The validated raw order data from Backpack.

        Returns:
            Order: The corresponding internal `Order` object, populated with transformed data.

        Raises:
            ValueError: If essential fields (e.g., quantity, createdAt) from the raw model
                        are missing or cannot be parsed correctly, despite prior raw validation.
                        This indicates an unexpected issue or change in the raw model's guarantees.
        """
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
        """
        Transforms a validated `BackpackRawBalance` object for a specific asset into an
        internal `SpotBalance` domain model.

        Parses numeric strings for total and available quantities into Decimals.
        The timestamp for the balance is set to the current UTC time as Backpack's raw balance
        data does not include a timestamp.

        Args:
            asset_symbol (str): The symbol of the asset (e.g., 'USDC', 'SOL').
            raw (BackpackRawBalance): The validated raw balance data for the asset.

        Returns:
            SpotBalance: The corresponding internal `SpotBalance` object.

        Raises:
            ValueError: If essential numeric fields (`total`, `available`) are missing from the
                        raw model or cannot be parsed correctly, despite prior raw validation.
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
        """
        Transforms a validated `BackpackRawPosition` object into an internal
        `DerivativePosition` domain model.

        Handles parsing of numeric strings to Decimals, determination of position side
        (BUY/SELL) based on `net_quantity`, and sets the timestamp to current UTC time.
        Placeholder for `BackpackPositionDetails` if specific extended fields are needed.

        Args:
            raw (BackpackRawPosition): The validated raw position data from Backpack.

        Returns:
            DerivativePosition: The corresponding internal `DerivativePosition` object.

        Raises:
            ValueError: If essential numeric fields (e.g., net_quantity) are missing or cannot
                        be parsed correctly from the raw model, despite prior raw validation.
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
        """
        Transforms a validated `BackpackRawTrade` object (typically from REST API /trades endpoint)
        into an internal `Trade` domain model.

        Note: The Backpack REST API for trades (`/api/v1/trades`) typically lacks information
        like trade side, fee details, and maker status. This method attempts to map available
        fields. If critical information like `side` cannot be determined or isn't provided,
        it returns `None` and logs a warning, as a complete `Trade` object cannot be formed.
        For full trade details, WebSocket streams or fill history endpoints are usually required.

        Args:
            raw (BackpackRawTrade): The validated raw trade data from Backpack.

        Returns:
            Trade | None: The corresponding internal `Trade` object, or `None` if essential
                          information (like side) cannot be determined from the raw data.

        Raises:
            ValueError: If essential fields (price, qty, time) are missing or cannot be parsed,
                        despite prior raw validation.
        """
        price_dec = parse_decimal_value(raw.price, allow_none=False, field_name="price")
        quantity_dec = parse_decimal_value(raw.quantity, allow_none=False, field_name="quantity")
        timestamp = parse_datetime_utc(raw.time, field_name="time")

        if price_dec is None:
            raise ValueError("price missing/invalid in BackpackRawTrade")
        if quantity_dec is None:
            raise ValueError("quantity missing/invalid in BackpackRawTrade")
        if timestamp is None:
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
        """
        Transforms a validated `BackpackRawFundingRate` object into an internal
        `FundingRate` domain model.

        Parses numeric strings for funding rate and mark price to Decimals, and the timestamp
        string to a datetime object.

        Args:
            raw (BackpackRawFundingRate): The validated raw funding rate data from Backpack.

        Returns:
            FundingRate: The corresponding internal `FundingRate` object.

        Raises:
            ValueError: If essential fields (`funding_rate`, `time`) are missing or
                        cannot be parsed, despite prior raw validation.
        """
        funding_rate_dec = parse_decimal_value(
            raw.funding_rate, allow_none=False, field_name="funding_rate"
        )
        mark_price_dec = parse_decimal_value(raw.mark_price, field_name="mark_price")
        timestamp = parse_datetime_utc(raw.time, field_name="time")

        if funding_rate_dec is None:
            raise ValueError("funding_rate missing/invalid in BackpackRawFundingRate")
        if timestamp is None:
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
        """
        Transforms a validated `BackpackRawOrderBook` object (from REST API /depth endpoint)
        into an internal `OrderBook` domain model.

        Parses price and quantity strings from bids and asks lists into (Decimal, Decimal) tuples.
        Converts the raw timestamp to a datetime object. Assumes bid/ask lists are correctly
        sorted by the exchange (highest bid first, lowest ask first).

        Args:
            symbol (str): The market symbol this order book belongs to.
            raw (BackpackRawOrderBook): The validated raw order book data from Backpack.

        Returns:
            OrderBook: The corresponding internal `OrderBook` object.

        Raises:
            ValueError: If the timestamp is missing/invalid or if parsing of bid/ask levels fails.
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
        """
        Transforms a validated `BackpackRawFill` object (from REST API /history/fills endpoint)
        into an internal `Trade` domain model.

        This method handles comprehensive mapping, including parsing numeric strings
        (price, quantity, fee) to Decimals, ISO timestamp string to datetime,
        and mapping the side string to `OrderSide` enum.
        Assumes `raw` has been validated by `BackpackRawFill` Pydantic model.

        Args:
            raw (BackpackRawFill): The validated raw fill data from Backpack.

        Returns:
            Trade: The corresponding internal `Trade` object.

        Raises:
            ValueError: If essential fields (price, quantity, fee, timestamp) are missing or
                        cannot be parsed correctly from the raw model, despite prior raw validation.
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
        """
        Transforms a validated raw Backpack WebSocket ticker event (`BackpackRawTickerEvent`)
        into an internal `Ticker` domain model.

        Maps available fields (e.g., `lastPrice` to `price`, `volume`). Bid and Ask prices are
        typically not included in Backpack ticker events and are set to `None` in the
        internal model.
        The timestamp is set to the current UTC time as ticker events may not provide one.

        Args:
            raw (BackpackRawTickerEvent): The validated raw WebSocket ticker event data.

        Returns:
            Ticker: The corresponding internal `Ticker` object.

        Raises:
            ValueError: If essential numeric fields (`lastPrice`, `volume`) are missing from the
                        raw model or cannot be parsed correctly, despite prior raw validation.
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
        """
        Transforms a validated raw Backpack WebSocket depth update event
        (`BackpackRawDepthUpdateEvent`) into an internal `OrderBook` domain model.

        This method assumes the WebSocket event provides a snapshot of order book levels.
        If it were a differential update, the logic would need significant changes.
        The timestamp is set to the current UTC time as Backpack WebSocket depth events often
        do not include an explicit event timestamp in the main payload structure mapped here.

        Args:
            symbol (str): The market symbol for the order book.
            raw (BackpackRawDepthUpdateEvent): The validated raw WebSocket depth event data.

        Returns:
            OrderBook: The corresponding internal `OrderBook` object.

        Raises:
            ValueError: If parsing of bid/ask levels from the raw event fails.
        """
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
        """
        Transforms a validated raw Backpack WebSocket trade event (`BackpackRawTradeEvent`)
        into an internal `Trade` domain model.

        Infers trade side based on the `is_buyer_the_maker` flag from the event.
        Assumes fee is zero and fee asset is 'USDC' (quote asset placeholder) as these details
        are typically not provided in the public WebSocket trade stream.
        The `order_id` in the resulting `Trade` object is populated based on the inferred side
        (buyer's order ID if a BUY, seller's if a SELL).

        Args:
            raw (BackpackRawTradeEvent): The validated raw WebSocket trade event data.

        Returns:
            Trade: The corresponding internal `Trade` object.

        Raises:
            ValueError: If essential fields (price, quantity, timestamp) are missing or
                        cannot be parsed, despite prior raw validation.
        """
        price_dec = parse_decimal_value(raw.price, allow_none=False, field_name="price")
        quantity_dec = parse_decimal_value(raw.quantity, allow_none=False, field_name="quantity")
        timestamp_raw = raw.engine_timestamp if raw.engine_timestamp is not None else raw.event_time
        timestamp_dt = parse_datetime_utc(timestamp_raw)

        if price_dec is None:
            raise ValueError("price missing/invalid")
        if quantity_dec is None:
            raise ValueError("quantity missing/invalid")
        if timestamp_dt is None:
            raise ValueError("timestamp missing/invalid")

        # Infer side: If buyer is maker, seller initiated (SELL).
        # If seller is maker, buyer initiated (BUY).
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
        """
        Transforms a validated `BackpackRawKline` object into an internal `Candle` domain model.

        Converts the raw kline data, including parsing the millisecond start time to a
        datetime object and mapping prices/volume to Decimals (coercion handled by `Candle` model
        if raw kline fields are correctly validated strings/ints).

        Args:
            symbol (str): The trading symbol for the candle (e.g., "SOL_USDC").
            interval (str): The interval string for the candle (e.g., '1m', '1h').
            raw (BackpackRawKline): The validated raw kline data from Backpack.

        Returns:
            Candle: A validated `Candle` object representing the kline data.

        Raises:
            ValueError: If the raw data cannot be parsed into a valid `Candle` (e.g., timestamp
                        parsing failure, or if internal `Candle` validation fails for OHLC,
                        volume, etc., though most raw parsing is done by `BackpackRawKline`).
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

    @staticmethod
    def transform_raw_account_summary_to_internal(
        raw_settings: BackpackRawAccountSummary,
        spot_balances: dict[str, SpotBalance],
        derivative_positions: list[DerivativePosition],
    ) -> MarginAccountSummary:
        """
        Transforms raw Backpack account settings, balances, and positions into an
        internal MarginAccountSummary model.

        Note: BackpackRawAccountSummary primarily contains settings and limits.
        Calculations for total equity, available equity, etc., are approximations
        based on the provided spot balances and derivative positions, as Backpack's
        /capital endpoint (not used here) would provide more direct values.

        Args:
            raw_settings: Validated raw account summary/settings from Backpack.
            spot_balances: Dictionary of internal SpotBalance models.
            derivative_positions: List of internal DerivativePosition models.

        Returns:
            MarginAccountSummary: The populated internal margin account summary.
        """
        current_time_utc = datetime.now(UTC)

        # Calculations will use the already transformed spot_balances and derivative_positions

        # --- Calculate sums from derivative positions ---
        total_position_notional = Decimal("0.0")
        total_unrealized_pnl = Decimal("0.0")

        if derivative_positions:  # Use the passed internal list
            for pos in derivative_positions:
                if pos.entry_price is not None and pos.size != Decimal("0"):
                    total_position_notional += abs(pos.size) * pos.entry_price
                if pos.unrealized_pnl is not None:
                    total_unrealized_pnl += pos.unrealized_pnl

        # --- Approximate total equity and available equity from spot balances ---
        calculated_total_equity = Decimal("0.0")
        calculated_available_equity = Decimal("0.0")

        usdc_like_assets = ("USDC", "USD", "USDT")
        for asset_symbol, balance in spot_balances.items():  # Use passed internal dict
            if asset_symbol.upper() in usdc_like_assets:
                calculated_total_equity += balance.total_quantity
                calculated_available_equity += balance.available_quantity

        calculated_total_equity += total_unrealized_pnl

        # --- Populate BackpackMarginDetails ---
        assets_value_approx = Decimal("0.0")
        for asset_symbol, balance in spot_balances.items():  # Use passed internal dict
            if asset_symbol.upper() in usdc_like_assets:
                assets_value_approx += balance.total_quantity

        final_assets_value_for_details: Decimal | None = None
        if assets_value_approx > Decimal("0.0"):
            final_assets_value_for_details = assets_value_approx

        margin_fraction_calc = None

        bp_details = BackpackMarginDetails(
            assets_value=final_assets_value_for_details,
            borrow_liability=None,
            liabilities_value=None,
            locked_equity=None,
            margin_fraction=margin_fraction_calc,
            imf_raw=None,
            mmf_raw=None,
        )

        return MarginAccountSummary(
            exchange=ExchangeName.BACKPACK.value,
            timestamp=current_time_utc,
            total_equity=calculated_total_equity,
            available_equity=calculated_available_equity,
            total_initial_margin_required=None,
            total_maintenance_margin_required=None,
            total_position_notional=total_position_notional if derivative_positions else None,
            total_unrealized_pnl=total_unrealized_pnl if derivative_positions else None,
            hl_details=None,
            bp_details=bp_details,
        )

    @staticmethod
    def transform_raw_withdrawal_response_to_internal(
        raw: BackpackRawWithdrawalResponse,
    ) -> dict[str, Any]:
        """
        Transforms a validated raw Backpack withdrawal response (`BackpackRawWithdrawalResponse`)
        into a dictionary. Currently, this is a passthrough as no specific internal model
        for withdrawal confirmation exists beyond the raw structure.

        Args:
            raw (BackpackRawWithdrawalResponse): Validated raw withdrawal response data.

        Returns:
            dict[str, Any]: The withdrawal response data as a dictionary.
        """
        # TODO: Define a proper internal WithdrawalConfirmation model and map fully.
        # For now, return the raw model's dict representation (excluding unset fields).
        return raw.model_dump(exclude_unset=True)

    @staticmethod
    def transform_raw_ticker_to_internal(
        raw: BackpackRawTicker, symbol_override: str | None = None
    ) -> Ticker:
        """
        Transforms a validated raw Backpack REST API ticker (`BackpackRawTicker`)
        into an internal `Ticker` domain model.

        Args:
            raw (BackpackRawTicker): The validated raw REST API ticker data.
            symbol_override (str | None): Optional symbol to use if raw.symbol is not definitive.

        Returns:
            Ticker: The corresponding internal `Ticker` object.

        Raises:
            ValueError: If essential numeric fields cannot be parsed correctly or timestamp
                        is invalid.
        """
        # Defensive parsing for safety
        parsed_price = parse_decimal_value(raw.price, allow_none=True, field_name="price")
        parsed_bid = parse_decimal_value(raw.bid, allow_none=True, field_name="bid")
        parsed_ask = parse_decimal_value(raw.ask, allow_none=True, field_name="ask")
        parsed_volume = parse_decimal_value(raw.volume, allow_none=True, field_name="volume")

        timestamp_dt: datetime
        if raw.time is not None:
            try:
                parsed_ts: datetime | None
                if isinstance(raw.time, int | float):  # Assume ms if numeric
                    if raw.time > 1e11:  # Likely milliseconds
                        parsed_ts = parse_datetime_utc(raw.time / 1000, field_name="time")
                    else:  # Likely seconds
                        parsed_ts = parse_datetime_utc(raw.time, field_name="time")
                else:  # Must be str if not None and not int/float,
                    # due to BackpackRawTicker.time type hint
                    parsed_ts = parse_datetime_utc(raw.time, field_name="time")

                if parsed_ts is None:
                    raise ValueError(
                        f"Failed to parse ticker time '{raw.time}' to a valid datetime object."
                    )
                timestamp_dt = parsed_ts

            except ValueError as e_ts:
                logger.warning(
                    f"[BackpackOrderMapper] Could not parse ticker time '{raw.time}': {e_ts}, "
                    f"raising."
                )
                raise ValueError(f"Invalid ticker time '{raw.time}': {e_ts}") from e_ts
        else:
            logger.warning(
                "[BackpackOrderMapper] Ticker time is None, using current time as fallback."
            )
            # According to Ticker model, timestamp is required. Raising error if None.
            # However, BackpackRawTicker defines time as optional. If it's truly optional and
            # a Ticker *can* be created without a server-provided timestamp
            # (e.g. by using current time), this logic would change.
            # For now, assuming Ticker *requires* a valid parsed timestamp.
            # The Ticker model's @field_validator for timestamp will raise if
            # parse_datetime_utc returns None.
            # So, if raw.time is None, this will lead to an error at Ticker instantiation.
            # Let's make it explicit: Ticker requires a timestamp.
            raise ValueError(
                "Ticker time (raw.time) cannot be None for Backpack ticker transformation."
            )

        # Determine the final symbol
        symbol = symbol_override or raw.symbol

        return Ticker(
            symbol=symbol,
            timestamp=timestamp_dt,
            price=parsed_price,
            bid=parsed_bid,
            ask=parsed_ask,
            volume=parsed_volume,
        )
