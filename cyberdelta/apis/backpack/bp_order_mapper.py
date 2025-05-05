import logging
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.core.models import DerivativePosition, FundingRate, Order, SpotBalance, Trade
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
            f"[BackpackOrderMapper] Cannot determine trade side for raw trade {raw.id} from REST API. "
            f"Skipping transformation. Use WebSocket stream for complete trade data."
        )
        return None

        # --- Code below is unreachable due to return None above ---
        # side = None # Cannot determine from raw trade data
        # fee = Decimal("0")
        # fee_asset = ""
        # is_maker = None
        #
        # return Trade(
        #     id=raw.id,
        #     symbol=raw.symbol,
        #     exchange=ExchangeName.BACKPACK,
        #     order_id=raw.order_id,
        #     client_order_id="", # Not provided in raw trade
        #     side=side, # Would cause error as side cannot be None
        #     price=price_dec,
        #     quantity=quantity_dec,
        #     fee=fee,
        #     fee_asset=fee_asset,
        #     is_maker=is_maker,
        #     executed_at=timestamp,
        # )

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
