"""
CyberDeltaEngine: Hyperliquid API Data Mapper
-------------------------------------------

This module provides functions to map raw Hyperliquid API response models
(from `cyberdelta.apis.hyperliquid.models`) to the internal CyberDeltaEngine
domain models (from `cyberdelta.core.models`).

Responsibilities:
- Type Conversion: Convert raw types (strings, ints) to internal types (Decimal, datetime, Enums).
- Field Renaming: Map API field names (e.g., `totalSz`) to internal names
        (e.g., `quantity_requested`).
- Enum Mapping: Convert API status/type strings (e.g., "B") to internal enums (e.g., OrderSide.BUY).
- Data Transformation: Perform necessary calculations (e.g., calculating filled quantity).
- Error Handling: Gracefully handle potential parsing errors, logging issues, and returning
  appropriate types (e.g., None for optional fields, default values).

Usage:
These mappers are used by the Hyperliquid API client implementation after validating the raw
API response against the strict Raw Pydantic models.
"""

import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    # HyperliquidRawAssetDefinition # No longer needed here if method is removed
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.core.models import (
    DerivativePosition,
    HyperliquidMarginDetails,
    HyperliquidSpotBalanceDetails,
    MarginAccountSummary,
    Order,
    OrderBook,
    OrderSide,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderStatus,
    OrderType,
)
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate, HyperliquidFundingDetails
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
)

from .models.hl_raw_fill import HyperliquidRawFill

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
            price=price,
            stop_price=stop_price,
            average_fill_price=None,  # HL does not provide this in open order
            trigger_by=trigger_by,
            time_in_force=time_in_force,
            reduce_only=raw.reduce_only,
            post_only=(time_in_force == TimeInForce.ALO),
            created_at=created_at,
            updated_at=updated_at or created_at,  # Default updated_at to created_at if None
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
        )


class HyperliquidMapper:
    @staticmethod
    def map_raw_ctx_to_ticker(raw_asset_ctx: HyperliquidRawAssetCtx) -> Ticker:
        """
        Map a raw Hyperliquid Asset Context to an internal Ticker model.

        Args:
            raw_asset_ctx: The validated HyperliquidRawAssetCtx object.

        Returns:
            An internal Ticker object.
        """
        # mark_px is the best proxy for last_price, bid, and ask from AssetCtx
        price_val = parse_decimal_value(
            raw_asset_ctx.mark_px, allow_none=True, field_name="mark_px_for_price"
        )
        bid_val = parse_decimal_value(
            raw_asset_ctx.mark_px, allow_none=True, field_name="mark_px_for_bid"
        )
        ask_val = parse_decimal_value(
            raw_asset_ctx.mark_px, allow_none=True, field_name="mark_px_for_ask"
        )

        # day_ntl_vlm is notional volume. Ticker.volume expects base volume typically.
        # Mapping it for now, but acknowledge it's notional.
        # Alternatively, set to None if strict base volume is required.
        volume_val = parse_decimal_value(
            raw_asset_ctx.day_ntl_vlm, allow_none=True, field_name="day_ntl_vlm_for_volume"
        )

        return Ticker(
            symbol=raw_asset_ctx.name,
            timestamp=datetime.now(UTC),  # AssetCtx doesn't provide a snapshot time
            price=price_val,
            bid=bid_val,
            ask=ask_val,
            volume=volume_val,
        )

    @staticmethod
    def map_raw_order_book(raw_book: HyperliquidRawL2Book, depth: int | None = None) -> OrderBook:
        """
        Map a raw Hyperliquid L2 Book snapshot to an internal OrderBook model.

        Args:
            raw_book: The validated HyperliquidRawL2Book object.
            depth: Optional maximum number of bids/asks levels to include.

        Returns:
            An internal OrderBook object.
        """
        bids: list[tuple[Decimal, Decimal]] = []
        asks: list[tuple[Decimal, Decimal]] = []

        if raw_book.levels and len(raw_book.levels) == 2:
            raw_bids = raw_book.levels[0]
            raw_asks = raw_book.levels[1]

            for level in raw_bids:
                try:
                    price = parse_decimal_value(
                        level.px, allow_none=False, field_name=f"bid_px_{level.px}"
                    )
                    size = parse_decimal_value(
                        level.sz, allow_none=False, field_name=f"bid_sz_{level.sz}"
                    )
                    if (
                        price is not None
                        and size is not None
                        and price.is_finite()
                        and size.is_finite()
                        and size > Decimal(0)
                    ):
                        bids.append((price, size))
                    else:
                        logger.warning(
                            f"[HyperliquidMapper] Skipping invalid bid level: Px={level.px}, Sz={level.sz}"
                        )
                except (ValueError, TypeError, InvalidOperation) as e:
                    logger.warning(
                        f"[HyperliquidMapper] Error parsing bid level (Px={level.px}, Sz={level.sz}): {e}"
                    )

            for level in raw_asks:
                try:
                    price = parse_decimal_value(
                        level.px, allow_none=False, field_name=f"ask_px_{level.px}"
                    )
                    size = parse_decimal_value(
                        level.sz, allow_none=False, field_name=f"ask_sz_{level.sz}"
                    )
                    if (
                        price is not None
                        and size is not None
                        and price.is_finite()
                        and size.is_finite()
                        and size > Decimal(0)
                    ):
                        asks.append((price, size))
                    else:
                        logger.warning(
                            f"[HyperliquidMapper] Skipping invalid ask level: Px={level.px}, Sz={level.sz}"
                        )
                except (ValueError, TypeError, InvalidOperation) as e:
                    logger.warning(
                        f"[HyperliquidMapper] Error parsing ask level (Px={level.px}, Sz={level.sz}): {e}"
                    )
        else:
            logger.warning(
                f"[HyperliquidMapper] Raw book for {raw_book.coin} has invalid levels structure: {raw_book.levels}"
            )

        # Sort bids descending, asks ascending by price
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])

        if depth is not None and depth > 0:
            bids = bids[:depth]
            asks = asks[:depth]

        book_timestamp = parse_datetime_utc(raw_book.time, field_name="raw_book.time")
        if book_timestamp is None:  # Should not happen if raw_book.time is always valid int
            logger.error(
                f"[HyperliquidMapper] Failed to parse timestamp from raw_book.time: {raw_book.time}. Using current time."
            )
            book_timestamp = datetime.now(UTC)

        return OrderBook(
            symbol=raw_book.coin,
            timestamp=book_timestamp,
            bids=bids,
            asks=asks,
            # exchange field is not part of OrderBook core model as per definition checked
        )

    @staticmethod
    def transform_raw_public_trade_to_internal(raw_trade: HyperliquidRawPublicTrade) -> Trade:
        """
        Transforms a single raw public trade into an internal Trade model.
        Public trades lack fee, OID, and maker status, so these are set to None.
        """
        try:
            side = HyperliquidOrderMapper.map_side_to_internal(raw_trade.side)
            price = parse_decimal_value(raw_trade.px, allow_none=False, field_name="px")
            quantity = parse_decimal_value(raw_trade.sz, allow_none=False, field_name="sz")
            executed_at = parse_datetime_utc(raw_trade.time, field_name="time")

            if price is None or quantity is None or executed_at is None:
                # This case should ideally be caught by allow_none=False in parse_decimal_value/
                # parse_datetime_utc, which raise ValueError. Adding for defensiveness.
                raise ValueError(
                    "Essential fields (price, quantity, executed_at) could not be parsed."
                )

            details = HyperliquidTradeDetails(
                trade_hash=raw_trade.hash, liquidation_mark_px=None, start_position=None, dir=None
            )

            return Trade(
                id=raw_trade.hash,
                symbol=raw_trade.coin,
                executed_at=executed_at,
                side=side,
                order_id="UNKNOWN_PUBLIC_TRADE",  # order_id is str, not Optional[str]
                exchange=ExchangeName.HYPERLIQUID.value,
                price=price,
                quantity=quantity,
                client_order_id=None,
                fee=Decimal("0"),
                fee_asset=None,
                is_maker=None,
                hl_details=details,
            )
        except (ValueError, TypeError, InvalidOperation) as e:
            logger.error(
                f"[HyperliquidMapper] Error transforming raw public trade: {e}. Data: {raw_trade.model_dump()!r}",
                exc_info=True,
            )
            raise  # Re-raise to be caught by caller or map_raw_trades

    @staticmethod
    def map_raw_trades(
        raw_public_trades: list[HyperliquidRawPublicTrade], limit: int | None = None
    ) -> list[Trade]:
        """
        Map a list of raw Hyperliquid public trades to a list of internal Trade models.

        Args:
            raw_public_trades: A list of validated HyperliquidRawPublicTrade objects.
            limit: Optional maximum number of trades to return.

        Returns:
            A list of internal Trade objects.
        """
        trades: list[Trade] = []
        for raw_trade in raw_public_trades:
            try:
                trade = HyperliquidMapper.transform_raw_public_trade_to_internal(raw_trade)
                trades.append(trade)
            except Exception as e:  # Catch errors from transform_raw_public_trade_to_internal
                logger.warning(
                    f"[HyperliquidMapper] Skipping public trade due to transformation error: {e}. Raw: {raw_trade.model_dump()!r}"
                )
                # Continue to process other trades

        # Apply limit if specified
        if limit is not None and limit > 0:
            return trades[:limit]
        return trades

    @staticmethod
    def map_raw_ctx_to_funding_rate(raw_asset_ctx: HyperliquidRawAssetCtx) -> FundingRate | None:
        """
        Map a raw Hyperliquid Asset Context to an internal FundingRate model.

        Args:
            raw_asset_ctx: The validated HyperliquidRawAssetCtx object.

        Returns:
            An internal FundingRate object, or None if parsing fails.
        """
        try:
            mark_price_val = parse_decimal_value(
                raw_asset_ctx.mark_px, allow_none=True, field_name="mark_px"
            )

            hourly_funding_str = raw_asset_ctx.funding
            hourly_funding_val = parse_decimal_value(
                hourly_funding_str, allow_none=True, field_name="funding"
            )

            funding_rate_8hr = None
            if hourly_funding_val is not None:
                funding_rate_8hr = hourly_funding_val * Decimal("8")

            # Calculate next funding time (start of the next hour)
            now_utc = datetime.now(UTC)
            next_funding_time_val = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(
                hours=1
            )

            details = HyperliquidFundingDetails(
                hl_funding_hourly=hourly_funding_val,
                hl_prev_day_px=parse_decimal_value(
                    raw_asset_ctx.prev_day_px, allow_none=True, field_name="prev_day_px"
                ),
                hl_day_ntl_vlm=parse_decimal_value(
                    raw_asset_ctx.day_ntl_vlm, allow_none=True, field_name="day_ntl_vlm"
                ),
                hl_impact_px=parse_decimal_value(
                    raw_asset_ctx.impact_px, allow_none=True, field_name="impact_px"
                ),
            )

            return FundingRate(
                symbol=raw_asset_ctx.name,
                timestamp=now_utc,  # Snapshot time
                funding_rate=funding_rate_8hr,
                predicted_rate=None,  # Not available from AssetCtx
                mark_price=mark_price_val,
                index_price=None,  # Not available from AssetCtx
                next_funding_time=next_funding_time_val,
                hl_details=details,
            )
        except (ValueError, TypeError, InvalidOperation) as e:
            logger.error(
                f"[HyperliquidMapper] Error mapping raw asset context to FundingRate for "
                f"'{raw_asset_ctx.name}': {e}. Data: {raw_asset_ctx.model_dump()!r}",
                exc_info=True,
            )
            return None

    @staticmethod
    def map_raw_clearinghouse_state_to_derivative_positions(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> dict[str, DerivativePosition]:
        """
        Maps asset positions from the raw clearinghouse state to a dictionary of
        internal DerivativePosition models.

        Args:
            raw_state: The validated HyperliquidRawClearinghouseState object.

        Returns:
            A dictionary mapping asset symbols to DerivativePosition objects.
        """
        derivative_positions: dict[str, DerivativePosition] = {}
        if not raw_state or not raw_state.asset_positions:
            logger.warning("[HyperliquidMapper] No asset positions found in raw_state to map.")
            return derivative_positions

        for (
            raw_asset_pos
        ) in raw_state.asset_positions:  # raw_asset_pos is HyperliquidRawAssetPosition
            # The isinstance check for HyperliquidRawAssetPosition previously here was redundant.
            if not raw_asset_pos.position:
                logger.warning(
                    f"[HyperliquidMapper] Skipping invalid raw_asset_pos (missing position details): {raw_asset_pos.asset}"
                )
                continue

            # raw_asset_pos.position is HyperliquidRawPositionInfo
            try:
                mapped_pos = HyperliquidPositionMapper.map(raw_asset_pos.position)
                if mapped_pos:
                    derivative_positions[mapped_pos.symbol] = mapped_pos
                else:
                    logger.warning(
                        f"[HyperliquidMapper] Failed to map position for asset "
                        f"'{raw_asset_pos.asset}'. Raw position info: {raw_asset_pos.position.model_dump()}"
                    )
            except (ValueError, TypeError, InvalidOperation) as e:
                logger.error(
                    f"[HyperliquidMapper] Error mapping position for asset "
                    f"'{raw_asset_pos.asset}': {e}. Raw position info: {raw_asset_pos.position.model_dump()}",
                    exc_info=True,
                )
        return derivative_positions

    @staticmethod
    def map_raw_clearinghouse_state_to_margin_summary(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> MarginAccountSummary:
        """
        Maps the margin summary from the raw clearinghouse state to an internal
        MarginAccountSummary model.

        Args:
            raw_state: The validated HyperliquidRawClearinghouseState object.

        Returns:
            An internal MarginAccountSummary object.

        Raises:
            ValueError: If essential numeric fields cannot be parsed.
        """
        if not raw_state or not raw_state.margin_summary:
            logger.error("[HyperliquidMapper] Raw state or margin_summary is missing for mapping.")
            raise ValueError(
                "Raw state or margin_summary missing, cannot map MarginAccountSummary."
            )

        raw_margin_summary: HyperliquidRawMarginSummary = raw_state.margin_summary

        total_equity_val = parse_decimal_value(
            raw_margin_summary.account_value,
            allow_none=False,
            field_name="margin_summary.account_value",
        )
        total_notional_val = parse_decimal_value(
            raw_margin_summary.total_ntl_pos,
            allow_none=False,
            field_name="margin_summary.total_ntl_pos",
        )
        available_for_withdrawal_val = parse_decimal_value(
            raw_state.withdrawable,
            allow_none=False,
            field_name="withdrawable",
        )
        cross_mmr_val = parse_decimal_value(
            raw_state.cross_maintenance_margin_used,
            allow_none=False,
            field_name="cross_maintenance_margin_used",
        )
        isolated_mmr_val = parse_decimal_value(
            raw_state.isolated_maintenance_margin_used,
            allow_none=False,
            field_name="isolated_maintenance_margin_used",
        )
        total_initial_margin_val = parse_decimal_value(
            raw_margin_summary.total_margin_used,
            allow_none=False,
            field_name="margin_summary.total_margin_used",
        )

        if total_equity_val is None:  # Should be caught by allow_none=False in helper
            raise ValueError("Failed to parse total_equity (account_value).")
        if total_notional_val is None:
            raise ValueError("Failed to parse total_notional_value (total_ntl_pos).")
        if available_for_withdrawal_val is None:
            raise ValueError("Failed to parse available_for_withdrawal (withdrawable).")
        if cross_mmr_val is None:
            raise ValueError("Failed to parse cross_mmr (cross_maintenance_margin_used).")
        if isolated_mmr_val is None:
            raise ValueError("Failed to parse isolated_mmr (isolated_maintenance_margin_used).")
        if total_initial_margin_val is None:
            raise ValueError("Failed to parse total_initial_margin (total_margin_used).")

        total_maintenance_margin_val = cross_mmr_val + isolated_mmr_val

        total_unrealized_pnl_val = Decimal("0")
        if raw_state.asset_positions:
            for asset_pos in raw_state.asset_positions:
                if asset_pos.position and asset_pos.position.unrealized_pnl:
                    pnl = parse_decimal_value(
                        asset_pos.position.unrealized_pnl,
                        allow_none=True,  # PNL can be missing for an asset if no position
                        field_name=f"asset_positions.{asset_pos.asset}.unrealized_pnl",
                    )
                    if pnl is not None:
                        total_unrealized_pnl_val += pnl

        # HyperliquidMarginDetails takes cross_maintenance_margin_used and isolated_maintenance_margin_used
        hyperliquid_details = HyperliquidMarginDetails(
            cross_maintenance_margin_used=cross_mmr_val,
            isolated_maintenance_margin_used=isolated_mmr_val,
        )

        return MarginAccountSummary(
            exchange=ExchangeName.HYPERLIQUID.value,
            timestamp=datetime.now(UTC),  # Raw state doesn't provide a snapshot timestamp
            total_equity=total_equity_val,
            available_equity=available_for_withdrawal_val,  # Mapping available_for_withdrawal to available_equity
            total_initial_margin_required=total_initial_margin_val,
            total_maintenance_margin_required=total_maintenance_margin_val,
            total_position_notional=total_notional_val,
            total_unrealized_pnl=total_unrealized_pnl_val,
            hl_details=hyperliquid_details,
        )

    @staticmethod
    def map_raw_clearinghouse_state_to_spot_balances(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> dict[str, SpotBalance]:
        """
        Maps relevant fields from the raw clearinghouse state to a dictionary of
        internal SpotBalance models. For Hyperliquid, this primarily means USDC.

        Args:
            raw_state: The validated HyperliquidRawClearinghouseState object.

        Returns:
            A dictionary mapping asset symbols (e.g., "USDC") to SpotBalance objects.
        """
        spot_balances: dict[str, SpotBalance] = {}

        if not raw_state or not raw_state.margin_summary:
            logger.warning(
                "[HyperliquidMapper] Raw state or margin_summary missing, cannot map spot balances."
            )
            return spot_balances

        usdc_total_balance = parse_decimal_value(
            raw_state.margin_summary.account_value,
            allow_none=True,  # Allow parsing to fail gracefully if data is bad
            field_name="margin_summary.account_value (for spot total)",
        )
        usdc_available_balance = parse_decimal_value(
            raw_state.withdrawable,
            allow_none=True,  # Allow parsing to fail gracefully
            field_name="withdrawable (for spot available)",
        )

        if usdc_total_balance is not None:
            effective_available = (
                usdc_available_balance if usdc_available_balance is not None else Decimal("0")
            )
            # Ensure available is not more than total
            if effective_available > usdc_total_balance:
                logger.warning(
                    f"Available balance {effective_available} for USDC exceeded total {usdc_total_balance}. Clamping to total."
                )
                effective_available = usdc_total_balance
            if effective_available < Decimal("0") and usdc_total_balance >= Decimal("0"):
                logger.warning(
                    f"Available balance {effective_available} for USDC is negative while total {usdc_total_balance} is not. Setting available to 0."
                )
                effective_available = Decimal("0")

            # HyperliquidSpotBalanceDetails is currently an empty model
            details = HyperliquidSpotBalanceDetails()

            spot_balances["USDC"] = SpotBalance(
                exchange=ExchangeName.HYPERLIQUID.value,
                asset="USDC",
                timestamp=datetime.now(UTC),  # Raw state doesn't provide a snapshot timestamp
                total_quantity=usdc_total_balance,
                available_quantity=effective_available,
                hl_details=details,
            )
        else:
            logger.warning(
                "[HyperliquidMapper] Could not parse 'account_value' from margin_summary "
                "to determine USDC total spot balance."
            )

        return spot_balances

    @staticmethod
    def _map_open_orders(raw_open_orders: list[Any]) -> dict[str, list[Order]]:
        """Helper to map raw open orders list to internal models, grouped by asset."""
        open_orders: dict[str, list[Order]] = defaultdict(list)
        for item in raw_open_orders:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict open order item: {item}")
                continue
            try:
                # Validate using HyperliquidRawOrder
                from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
                    HyperliquidRawOrder,
                )

                raw_order = HyperliquidRawOrder.model_validate(item)
                # Map validated raw order to internal Order using existing mapper
                internal_order = HyperliquidOrderMapper.transform_raw_order_to_internal(raw_order)
                # Group by asset symbol
                open_orders[internal_order.symbol].append(internal_order)
            except ValidationError as e:
                logger.error(f"Validation error parsing open order: {e}. Data: {item}")
            except Exception as e:
                logger.error(f"Unexpected error mapping open order: {e}. Data: {item}")
        return dict(open_orders)  # Convert back to regular dict

    @staticmethod
    def transform_raw_fill_to_internal(raw: HyperliquidRawFill) -> Trade:
        """
        Transforms a validated HyperliquidRawFill object into an internal Trade object.

        Handles type conversions, field renaming, and populates enrichment slots.
        Raises ValueError on critical parsing errors.
        """
        executed_at = parse_datetime_utc(raw.time, field_name="executed_at")
        if executed_at is None:
            raise ValueError("executed_at (time) is required and could not be parsed.")

        price = parse_decimal_value(raw.px, allow_none=False, field_name="price")
        quantity = parse_decimal_value(raw.sz, allow_none=False, field_name="quantity")
        fee = parse_decimal_value(raw.fee, allow_none=False, field_name="fee")

        if price is None or quantity is None or fee is None:
            # Should not happen if raw model validation passed, but defensive check
            raise ValueError("Critical fill fields (price, quantity, fee) failed parsing.")

        # Map side
        side = OrderSide.BUY if raw.side == "B" else OrderSide.SELL

        # Handle optional fields for enrichment
        start_position = parse_decimal_value(
            raw.start_position, allow_none=True, field_name="start_position"
        )
        liquidation_mark_px_decimal = parse_decimal_value(
            raw.liquidation_mark_px,  # Access correct field name
            allow_none=True,
            field_name="liquidation_mark_px",
        )

        hl_details = HyperliquidTradeDetails(
            trade_hash=raw.hash,
            liquidation_mark_px=liquidation_mark_px_decimal,
            start_position=start_position,
            dir=raw.dir,
        )

        return Trade(
            id=str(raw.tid),  # Use trade ID as primary ID
            symbol=raw.coin,
            executed_at=executed_at,
            side=side,
            order_id=str(raw.oid),
            exchange=ExchangeName.HYPERLIQUID.value,
            client_order_id=raw.cloid,
            price=price,
            quantity=quantity,
            fee=fee,
            fee_asset=raw.coin,  # Assume fee is paid in quote asset (coin symbol)
            is_maker=raw.is_maker,
            hl_details=hl_details,
            bp_details=None,  # No backpack details for HL fills
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
                fee=parse_decimal_value(raw.fee, allow_none=True, field_name="fill.fee")
                or Decimal("0"),
                fee_asset="USDC",
                is_maker=raw.is_maker,
            )
        except Exception:
            return None


class HyperliquidPositionMapper:
    """
    Utility for mapping raw position data (likely from WebSocket events)
    to the internal Position model.
    """

    @staticmethod
    def map(raw: HyperliquidRawPositionInfo) -> DerivativePosition | None:
        """
        Maps a raw Hyperliquid position info object to an internal Position object.

        Returns None if any required field is missing or invalid.
        """
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
        # Extract timestamp (assuming it exists in raw.position or similar)
        # Placeholder: Use current time if timestamp is not available
        timestamp = parse_datetime_utc(getattr(raw, "timestamp", None)) or datetime.now(UTC)

        return DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,  # Add required exchange
            timestamp=timestamp,  # Add required timestamp
            symbol=raw.coin,
            size=size,
            entry_price=entry_price,
            mark_price=mark_price,
            side=side,
            liquidation_price=parse_decimal_value(
                raw.liquidation_px, allow_none=True, field_name="position.liquidation_px"
            ),
            unrealized_pnl=unrealized_pnl,
            # leverage removed
            # TODO: Add hl_details parsing if needed
        )

    @staticmethod
    def map_balance(raw: dict[str, Any]) -> SpotBalance | None:
        # Placeholder: implement if/when balance fields are available in user state
        return None


class HyperliquidCandleMapper:
    """
    Maps a validated HyperliquidRawCandleSnapshot to a list of internal Candle models.
    """

    @staticmethod
    def map(raw: HyperliquidRawCandleSnapshot, symbol: str, interval: str) -> list[Candle]:
        candles: list[Candle] = []
        n = len(raw.t)
        for i in range(n):
            try:
                # Use parse_datetime_utc for millisecond timestamp
                open_time_dt = parse_datetime_utc(raw.t[i], field_name="open_time")
                open_ = parse_decimal_value(raw.o[i], allow_none=False, field_name="open")
                high = parse_decimal_value(raw.h[i], allow_none=False, field_name="high")
                low = parse_decimal_value(raw.l[i], allow_none=False, field_name="low")
                close = parse_decimal_value(raw.c[i], allow_none=False, field_name="close")
                volume = parse_decimal_value(raw.v[i], allow_none=False, field_name="volume")

                if None in (open_time_dt, open_, high, low, close, volume):
                    logger.warning(f"Skipping candle at index {i} due to None value(s)")
                    continue

                # Ensure Non-None after check for MyPy
                assert open_time_dt is not None
                assert open_ is not None
                assert high is not None
                assert low is not None
                assert close is not None
                assert volume is not None

                # Correct argument name: use open_time
                candle = Candle(
                    symbol=symbol,
                    interval=interval,
                    open_time=open_time_dt,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
                candles.append(candle)
            except (ValidationError, ValueError, TypeError) as e:
                logger.warning(f"Error processing or validating candle data at index {i}: {e}")
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
                fee=Decimal("0"),
                fee_asset="USDC",
                is_maker=raw.is_maker,
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
                fee=Decimal("0"),
                fee_asset="USDC",
                is_maker=False,
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
            # from cyberdelta.utils.parsing import parse_datetime_utc # Removed unused import

            timestamp = parse_datetime_utc(raw.time, field_name="orderbook.time")
            if timestamp is None:
                logger.warning("OrderBook event missing or invalid timestamp, skipping.")
                return None
            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
            )
        except Exception:
            return None

    @staticmethod
    def map_position_event(raw: HyperliquidRawPositionInfo) -> DerivativePosition | None:
        # TODO: Map fields from WsPositionInfo to Position
        #       if structure differs from HyperliquidRawPositionInfo
        # For now, return None or implement a conversion if needed
        return None
