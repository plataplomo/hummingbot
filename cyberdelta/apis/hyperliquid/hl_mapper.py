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
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
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
    def map_raw_ctx_to_ticker(ctx: dict[str, Any]) -> Ticker:
        """
        Map a raw asset context dict from Hyperliquid to a Ticker business logic model.
        """
        mark_px = ctx.get("markPx")
        bid = parse_decimal_value(str(mark_px), allow_none=True)
        ask = parse_decimal_value(str(mark_px), allow_none=True)
        symbol = ctx.get("name", "")
        from datetime import datetime

        return Ticker(
            symbol=symbol,
            bid=bid,
            ask=ask,
            timestamp=datetime.now(UTC),
        )

    @staticmethod
    def map_raw_order_book(
        symbol: str, data: dict[str, Any], depth: int | None = None
    ) -> OrderBook:
        """
        Map a raw Hyperliquid order book response to an internal OrderBook model.
        Defensive: Handles malformed or missing data gracefully.
        """
        # DEFENSIVE CHECK: data is dict[str,Any], data.get() returns Any.
        # Pyright=[reportUnknownVariableType]
        levels_raw = data.get("levels")
        # DEFENSIVE CHECK: levels_raw is Any. Runtime check + list[Any] needed.
        # Pyright=[reportUnknownVariableType]
        levels: list[Any] = levels_raw if isinstance(levels_raw, list) else []
        bids: list[tuple[Decimal, Decimal]] = []
        asks: list[tuple[Decimal, Decimal]] = []

        if levels and len(levels) == 2:  # Assuming levels contains [bids_list, asks_list]
            # DEFENSIVE CHECK: levels is list[Any], so levels[0] is Any.
            # Pyright=[reportUnknownVariableType]
            bid_levels_raw = levels[0]
            # DEFENSIVE CHECK: bid_levels_raw is Any. Runtime check + list[Any] needed.
            # Pyright=[reportUnknownVariableType]
            bid_levels: list[Any] = bid_levels_raw if isinstance(bid_levels_raw, list) else []

            for level_raw in bid_levels:  # level_raw is Any from list[Any]
                # DEFENSIVE CHECK: level_raw is Any. Runtime check needed.
                # Pyright=[reportUnknownArgumentType, reportUnknownVariableType]
                if not isinstance(level_raw, list):
                    logger.warning(
                        f"[{symbol}] Skipping invalid bid level (not a list): {level_raw}"
                    )
                    continue
                # DEFENSIVE CHECK: Pyright infers level_typed as list[Unknown]
                #                  despite runtime check.
                # Pyright=[reportUnknownVariableType]
                level_typed: list[Any] = level_raw
                if len(level_typed) >= 2:
                    # DEFENSIVE CHECK: level_typed[0] and level_typed[1] are Any.
                    # parse_decimal_value handles str|Any. Pyright=[reportUnknownArgumentType]
                    price = parse_decimal_value(
                        level_typed[0], allow_none=False, field_name="orderbook.bid.price"
                    )
                    size = parse_decimal_value(
                        level_typed[1], allow_none=False, field_name="orderbook.bid.size"
                    )
                    if (
                        price is not None
                        and size is not None
                        and price.is_finite()
                        and size.is_finite()
                        and size >= Decimal(0)
                    ):
                        bids.append((price, size))
                    else:
                        logger.warning(f"[{symbol}] Invalid bid level content: {level_typed}")

            # DEFENSIVE CHECK: levels is list[Any], so levels[1] is Any.
            # Pyright=[reportUnknownVariableType]
            ask_levels_raw = levels[1]
            # DEFENSIVE CHECK: ask_levels_raw is Any. Runtime check + list[Any] needed.
            # Pyright=[reportUnknownVariableType]
            ask_levels: list[Any] = ask_levels_raw if isinstance(ask_levels_raw, list) else []

            for level_raw_ask in ask_levels:  # level_raw_ask is Any from list[Any]
                # DEFENSIVE CHECK: level_raw_ask is Any. Runtime check needed.
                # Pyright=[reportUnknownArgumentType, reportUnknownVariableType]
                if not isinstance(level_raw_ask, list):
                    logger.warning(
                        f"[{symbol}] Skipping invalid ask level (not a list): {level_raw_ask}"
                    )
                    continue
                # DEFENSIVE CHECK: Pyright infers level_typed_ask as list[Unknown]
                #                  despite runtime check.
                # Pyright=[reportUnknownVariableType]
                level_typed_ask: list[Any] = level_raw_ask
                if len(level_typed_ask) >= 2:
                    # DEFENSIVE CHECK: level_typed_ask[0] and [1] are Any.
                    # parse_decimal_value handles str|Any. Pyright=[reportUnknownArgumentType]
                    price_ask = parse_decimal_value(
                        level_typed_ask[0], allow_none=False, field_name="orderbook.ask.price"
                    )
                    size_ask = parse_decimal_value(
                        level_typed_ask[1], allow_none=False, field_name="orderbook.ask.size"
                    )
                    if (
                        price_ask is not None
                        and size_ask is not None
                        and price_ask.is_finite()
                        and size_ask.is_finite()
                        and size_ask >= Decimal(0)
                    ):
                        asks.append((price_ask, size_ask))
                    else:
                        logger.warning(f"[{symbol}] Invalid ask level content: {level_typed_ask}")

        # Sort bids descending, asks ascending
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        if depth is not None and depth > 0:
            bids = bids[:depth]
            asks = asks[:depth]
        from datetime import datetime

        timestamp = datetime.now(UTC)
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
                    fee=Decimal("0"),
                    fee_asset="USDC",
                    is_maker=False,
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
            from datetime import datetime, timedelta

            now = datetime.now(UTC)
            # Next funding time: next full hour
            next_funding_time = (now + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
            if funding_rate is None or mark_price is None:
                return None
            return FundingRate(
                symbol=symbol,
                timestamp=now,
                funding_rate=funding_rate,
                mark_price=mark_price,
                next_funding_time=next_funding_time,
            )
        except (ValueError, TypeError, KeyError):
            return None

    @staticmethod
    def transform_raw_asset_def_to_funding_rate(
        raw_asset_def: dict[str, Any],
    ) -> FundingRate | None:
        """Map a raw asset definition dict from Hyperliquid 'allMeta' to FundingRate.
        Assumes the dict contains necessary funding info similar to AssetCtx.
        """
        try:
            symbol = raw_asset_def.get("name")
            if not symbol or not isinstance(symbol, str):
                logger.warning(f"Missing or invalid symbol in raw asset def: {raw_asset_def}")
                return None

            # Attempt to parse funding rate (assuming same key as in AssetCtx)
            # NOTE: This assumes the structure is similar; needs verification.
            funding_str = raw_asset_def.get("funding")  # Check if this key exists!
            if funding_str is None:
                logger.warning(f"Missing 'funding' key for {symbol} in allMeta response.")
                return None  # Cannot create FundingRate without the rate

            funding_rate_dec = parse_decimal_value(
                str(funding_str), allow_none=False, field_name=f"{symbol}_funding"
            )
            if funding_rate_dec is None:  # Should be unreachable
                raise ValueError("Funding rate parsed to None unexpectedly.")

            # Convert funding rate from hourly basis (if needed, check HL docs)
            # funding_rate_dec = funding_rate_dec * 8 # Example if rate is hourly

            # Mark price might be available under a different key, e.g., "markPx"
            mark_price_str = raw_asset_def.get("markPx")
            mark_price_dec = parse_decimal_value(str(mark_price_str)) if mark_price_str else None

            return FundingRate(
                symbol=symbol,
                timestamp=datetime.now(UTC),  # Use current time as not provided per-asset
                funding_rate=funding_rate_dec,
                mark_price=mark_price_dec,
            )
        except (ValidationError, ValueError, TypeError, InvalidOperation) as e:
            logger.error(
                f"Error mapping raw asset definition to FundingRate: {e}. Data: {raw_asset_def}"
            )
            return None

    @staticmethod
    def _map_asset_positions(
        raw_asset_positions: list[Any],
    ) -> dict[str, DerivativePosition]:
        """Helper to map raw asset positions list to internal models."""
        positions: dict[str, DerivativePosition] = {}
        item: dict[str, Any]
        for item in raw_asset_positions:
            # Assuming item structure corresponds to HyperliquidRawAssetPosition
            # which contains 'asset' and 'position' (HyperliquidRawPositionInfo)
            asset_symbol = item.get("asset")
            position_data = item.get("position")

            if not isinstance(asset_symbol, str) or not asset_symbol:
                logger.warning(f"Skipping asset position with missing/invalid asset: {item}")
                continue
            if not isinstance(position_data, dict):
                logger.warning(
                    f"Skipping asset position with missing/invalid position data: {item}"
                )
                continue

            try:
                # Validate the inner position data using the raw model
                # Assuming HyperliquidRawPositionInfo is the correct model here
                # Need to import it if not already available
                from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
                    HyperliquidRawPositionInfo,
                )

                raw_pos_info = HyperliquidRawPositionInfo.model_validate(position_data)
                # Map validated raw model to internal DerivativePosition using existing mapper
                internal_pos = HyperliquidPositionMapper.map(
                    raw_pos_info
                )  # Pass the validated Pydantic model
                if internal_pos:
                    positions[asset_symbol] = internal_pos
                else:
                    logger.warning(
                        f"Failed to map position for asset {asset_symbol}. Data: {position_data}"
                    )
            except ValidationError as e:
                logger.error(
                    f"Validation error parsing position for asset {asset_symbol}: {e}. "
                    f"Data: {position_data}"
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error mapping position for asset {asset_symbol}: {e}. "
                    f"Data: {position_data}"
                )
        return positions

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
    def map_user_state(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> tuple[
        dict[str, DerivativePosition],
        dict[str, list[Order]],
        dict[str, MarginAccountSummary],
        dict[str, SpotBalance],
    ]:
        """Map raw user state (ClearinghouseState) to internal models."""
        positions = HyperliquidMapper._map_asset_positions(raw_state.asset_positions)
        # Assuming openOrders is available directly on raw_state or fetched separately
        # If not, this needs adjustment.
        # For now, let's assume it's accessible like asset_positions
        # Need to check HyperliquidRawClearinghouseState definition again if this fails.
        open_orders_raw = getattr(raw_state, "openOrders", [])  # Default to empty list if not found
        open_orders = HyperliquidMapper._map_open_orders(open_orders_raw)

        # Map Margin Summary
        margin_summaries: dict[str, MarginAccountSummary] = {}
        exchange_key = "hyperliquid::USER"
        raw_margin_summary = raw_state.margin_summary
        if raw_margin_summary:
            try:
                # Instantiate HL details with correct fields from raw_state, parsed to Decimal
                hl_details = HyperliquidMarginDetails(
                    cross_maintenance_margin_used=parse_decimal_value(
                        raw_state.cross_maintenance_margin_used,
                        allow_none=False,
                        field_name="cross_maintenance_margin_used",
                    )
                    or Decimal("0"),  # Default on parse failure for required field
                    isolated_maintenance_margin_used=parse_decimal_value(
                        raw_state.isolated_maintenance_margin_used,
                        allow_none=False,
                        field_name="isolated_maintenance_margin_used",
                    )
                    or Decimal("0"),  # Default on parse failure for required field
                )
                # Instantiate internal summary with correct fields mapped from raw summary, parsed
                margin_summary = MarginAccountSummary(
                    exchange="hyperliquid",
                    timestamp=datetime.now(
                        UTC
                    ),  # Use current time as raw state doesn't seem to have it
                    total_equity=parse_decimal_value(
                        raw_margin_summary.account_value,
                        allow_none=False,
                        field_name="account_value",
                    )
                    or Decimal("0"),  # Default on parse failure
                    available_equity=parse_decimal_value(
                        raw_state.withdrawable, allow_none=False, field_name="withdrawable"
                    )
                    or Decimal("0"),  # Default on parse failure
                    # Map optional core fields, parsing if available
                    total_initial_margin_required=None,
                    # Placeholder - Check raw model for equivalent
                    total_maintenance_margin_required=parse_decimal_value(  # Summing cross and iso
                        str(
                            hl_details.cross_maintenance_margin_used
                            + hl_details.isolated_maintenance_margin_used
                        ),
                        allow_none=False,
                        field_name="total_maintenance_margin_required",
                    )
                    or Decimal("0"),
                    total_position_notional=parse_decimal_value(
                        raw_margin_summary.total_ntl_pos,
                        allow_none=True,
                        field_name="total_ntl_pos",
                    ),  # Optional, no default needed if None
                    total_unrealized_pnl=None,  # Placeholder - Check raw model for equivalent
                    hl_details=hl_details,
                )
                margin_summaries[exchange_key] = margin_summary
            except (ValidationError, TypeError, InvalidOperation) as e:
                logger.error(f"Error parsing Hyperliquid margin summary: {e}")

        # Parse Spot Balances - ASSUMING accountValue IS USDC total_quantity
        spot_balances: dict[str, SpotBalance] = {}
        if raw_margin_summary:
            try:
                usdc_total_raw = raw_margin_summary.account_value
                usdc_total_dec = parse_decimal_value(
                    usdc_total_raw, allow_none=False, field_name="usdc_total (accountValue)"
                )
                usdc_available_raw = raw_state.withdrawable
                usdc_available_dec = parse_decimal_value(
                    usdc_available_raw, allow_none=False, field_name="usdc_available (withdrawable)"
                )

                if usdc_total_dec is not None and usdc_available_dec is not None:
                    hl_spot_details = HyperliquidSpotBalanceDetails()
                    spot_balance = SpotBalance(
                        exchange=ExchangeName.HYPERLIQUID,
                        asset="USDC",
                        timestamp=datetime.now(UTC),  # Use current time
                        total_quantity=usdc_total_dec,
                        available_quantity=usdc_available_dec,
                        hl_details=hl_spot_details,
                    )
                    # Use asset symbol as key
                    spot_balances["USDC"] = spot_balance
                else:
                    if usdc_total_dec is None:
                        logger.error("Failed to parse total USDC balance (accountValue)")
                    if usdc_available_dec is None:
                        logger.error("Failed to parse available USDC balance (withdrawable)")

            except (ValidationError, TypeError, InvalidOperation) as e:
                logger.error(f"Error parsing Hyperliquid USDC spot balance: {e}")
        else:
            logger.warning("Cannot parse spot balances: margin_summary missing in raw state.")

        # Change return type hint to match actual return
        # Return type is tuple[positions, open_orders, margin_summaries, spot_balances]
        # where spot_balances is dict[str, SpotBalance]
        return positions, open_orders, margin_summaries, spot_balances

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
