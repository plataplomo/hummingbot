"""Backpack Transaction Mapper.

This mapper handles transformations for transaction-related data from the Backpack exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- Order transformations from raw order data
- Trade transformations from fill and public trade data
- Transaction history processing
- Order and trade-specific validation and error handling
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.backpack.protocols.mapper_protocols import TransactionMapperProtocol
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
    MissingRequiredFieldError,
    OrderTransformationError,
    UnknownEnumError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import (
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TimeInForce,
    TriggerType,
)
from cyberdelta.core.models import BackpackOrderDetails, Order, Trade
from cyberdelta.core.models.market.trade import BackpackTradeDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackTransactionMapper(TransactionMapperProtocol):
    """Focused mapper for Backpack transaction data transformations.

    This class contains static methods for transforming validated Backpack Raw transaction models
    into CyberDeltaEngine Internal Domain Models for orders and trades.
    """

    @staticmethod
    def _ensure_trade_values_not_none(
        price: Decimal | None, quantity: Decimal | None
    ) -> tuple[Decimal, Decimal]:
        """Ensure trade price and quantity are not None after parsing.

        Args:
            price: Price value to validate
            quantity: Quantity value to validate

        Returns:
            tuple[Decimal, Decimal]: The validated non-None values for type narrowing

        Raises:
            MissingRequiredFieldError: If either value is None
        """
        if price is None or quantity is None:
            raise MissingRequiredFieldError(
                field_names=["price", "quantity"],
                context="trade",
            )
        return price, quantity

    @staticmethod
    def _ensure_public_trade_fields_not_none(
        price_dec: Decimal | None, quantity_dec: Decimal | None, timestamp: datetime | None
    ) -> None:
        """Ensure public trade required fields are not None after parsing.

        Args:
            price_dec: Price value to validate
            quantity_dec: Quantity value to validate
            timestamp: Timestamp to validate

        Raises:
            MissingRequiredFieldError: If any required field is None
        """
        if price_dec is None:
            raise MissingRequiredFieldError(
                field_names="price",
                context="BackpackRawPublicTrade",
            )
        if quantity_dec is None:
            raise MissingRequiredFieldError(
                field_names="quantity",
                context="BackpackRawPublicTrade",
            )
        if timestamp is None:
            raise MissingRequiredFieldError(
                field_names="time",
                context="BackpackRawPublicTrade",
            )

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """Map a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell", "Bid", "Ask")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            UnknownEnumError: If side cannot be mapped
        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in {"buy", "bid"}:
            return OrderSide.BUY
        if side_lower in {"sell", "ask"}:
            return OrderSide.SELL

        raise UnknownEnumError(
            enum_type="Backpack order side",
            value=bp_side,
            valid_values=["buy", "sell", "bid", "ask", "Buy", "Sell", "Bid", "Ask"],
        )

    @staticmethod
    def _map_status_to_internal(bp_status: str) -> OrderStatus:
        """Map a Backpack order status string to internal OrderStatus enum.

        Args:
            bp_status: Raw status string from Backpack

        Returns:
            OrderStatus: OrderStatus enum value corresponding to the Backpack status
        """
        status_lower = bp_status.lower() if bp_status else ""
        if status_lower == "new":
            return OrderStatus.NEW
        if status_lower in {"open", "pending"}:
            return OrderStatus.OPEN
        if status_lower in {"filled", "executed"}:
            return OrderStatus.FILLED
        if status_lower in {"cancelled", "canceled"}:
            return OrderStatus.CANCELED
        if status_lower in {"partially_filled", "partiallyfilled", "partial"}:
            return OrderStatus.PARTIALLY_FILLED
        if status_lower in {"rejected", "failed"}:
            return OrderStatus.REJECTED
        if status_lower == "expired":
            return OrderStatus.EXPIRED

        logger.warning(
            "unknown_order_status",
            bp_status=bp_status,
            mapped_to="UNKNOWN",
            message="Unknown Backpack order status encountered, defaulting to UNKNOWN",
        )
        return OrderStatus.UNKNOWN

    @staticmethod
    def _map_type_to_internal(bp_type: str) -> OrderType:
        """Map a Backpack order type string to internal OrderType enum.

        Args:
            bp_type: Raw type string from Backpack

        Returns:
            OrderType: OrderType enum value corresponding to the Backpack type
        """
        type_lower = bp_type.lower() if bp_type else ""
        if type_lower in {"limit", "limit_order"}:
            return OrderType.LIMIT
        if type_lower in {"market", "market_order"}:
            return OrderType.MARKET
        if type_lower in {"stop", "stop_loss", "stoploss", "stop_market"}:
            return OrderType.STOP_MARKET
        if type_lower in {"take_profit", "takeprofit", "take_profit_market"}:
            return OrderType.TAKE_PROFIT_MARKET
        if type_lower in {"stop_limit", "stop_loss_limit"}:
            return OrderType.STOP_LIMIT
        if type_lower in {"take_profit_limit", "takeprofit_limit"}:
            return OrderType.TAKE_PROFIT_LIMIT

        logger.warning(
            "unknown_order_type",
            bp_type=bp_type,
            mapped_to="LIMIT",
            message="Unknown Backpack order type encountered, defaulting to LIMIT",
        )
        return OrderType.LIMIT  # Default to LIMIT instead of UNKNOWN

    @staticmethod
    def _map_tif_to_internal(bp_tif: str | None) -> TimeInForce:
        """Map a Backpack time in force string to internal TimeInForce enum.

        Args:
            bp_tif: Raw time-in-force string from Backpack

        Returns:
            TimeInForce: TimeInForce enum value corresponding to the Backpack TIF
        """
        if bp_tif is None:
            return TimeInForce.GTC  # Default to GTC
        tif_lower = bp_tif.lower()
        if tif_lower == "gtc":
            return TimeInForce.GTC
        if tif_lower == "ioc":
            return TimeInForce.IOC
        if tif_lower == "fok":
            return TimeInForce.FOK

        logger.warning(
            "unknown_time_in_force",
            bp_tif=bp_tif,
            mapped_to="GTC",
            message="Unknown Backpack time-in-force encountered, defaulting to GTC",
        )
        return TimeInForce.GTC  # Default to GTC instead of UNKNOWN

    @staticmethod
    def _map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        """Map a Backpack trigger_by string to internal TriggerType enum.

        Args:
            trigger_by: Raw trigger type string from Backpack

        Returns:
            TriggerType | None: TriggerType enum value or None if trigger_by is None or unknown
        """
        if trigger_by is None:
            return None
        trigger_lower = trigger_by.lower()
        if trigger_lower in {"mark", "mark_price"}:
            return TriggerType.MARK_PRICE
        if trigger_lower in {"last", "last_price"}:
            return TriggerType.LAST_PRICE
        if trigger_lower in {"index", "index_price"}:
            return TriggerType.INDEX_PRICE

        logger.warning(
            "unknown_trigger_type",
            trigger_by=trigger_by,
            returning="None",
            message="Unknown Backpack trigger_by value encountered, returning None",
        )
        return None

    @staticmethod
    def _map_self_trade_prevention(raw_stp: str | None) -> SelfTradePrevention | None:
        """Map self trade prevention string to enum.

        Args:
            raw_stp: Raw self trade prevention string from Backpack

        Returns:
            SelfTradePrevention | None: SelfTradePrevention enum value or None if raw_stp is empty
        """
        if not raw_stp:
            return None

        stp_str = raw_stp.upper()
        stp_mapping = {
            "REJECT_TAKER": SelfTradePrevention.REJECT_TAKER,
            "REJECTTAKER": SelfTradePrevention.REJECT_TAKER,
            "REJECT_MAKER": SelfTradePrevention.REJECT_MAKER,
            "REJECTMAKER": SelfTradePrevention.REJECT_MAKER,
            "REJECT_BOTH": SelfTradePrevention.REJECT_BOTH,
            "REJECTBOTH": SelfTradePrevention.REJECT_BOTH,
            "NONE": SelfTradePrevention.NONE,
        }
        return stp_mapping.get(stp_str)

    @staticmethod
    def _map_expiry_reason(raw_expiry: str | None) -> OrderExpiryReason | None:
        """Map expiry reason string to enum.

        Args:
            raw_expiry: Raw expiry reason string from Backpack

        Returns:
            OrderExpiryReason | None: OrderExpiryReason enum value or None if raw_expiry is empty
        """
        if not raw_expiry:
            return None

        expiry_str = raw_expiry.upper()
        expiry_mapping = {
            "USER_CANCELLED": OrderExpiryReason.USER_CANCELLED,
            "CANCELLED": OrderExpiryReason.USER_CANCELLED,
            "LIQUIDATION": OrderExpiryReason.LIQUIDATION,
            "INSUFFICIENT_FUNDS": OrderExpiryReason.INSUFFICIENT_FUNDS,
            "SELF_TRADE_PREVENTION": OrderExpiryReason.SELF_TRADE_PREVENTION,
            "POST_ONLY_TAKER": OrderExpiryReason.POST_ONLY_TAKER,
            "FILL_OR_KILL": OrderExpiryReason.FILL_OR_KILL,
            "IMMEDIATE_OR_CANCEL": OrderExpiryReason.IMMEDIATE_OR_CANCEL,
        }
        return expiry_mapping.get(expiry_str, OrderExpiryReason.UNKNOWN)

    @staticmethod
    def _map_order_origin(raw_origin: str | None) -> OrderUpdateOrigin | None:
        """Map origin string to enum.

        Args:
            raw_origin: Raw order origin string from Backpack

        Returns:
            OrderUpdateOrigin | None: OrderUpdateOrigin enum value or None if raw_origin is empty
        """
        if not raw_origin:
            return None

        origin_str = raw_origin.upper()
        origin_mapping = {
            "USER": OrderUpdateOrigin.USER,
            "LIQUIDATION_AUTOCLOSE": OrderUpdateOrigin.LIQUIDATION_AUTOCLOSE,
            "ADL_AUTOCLOSE": OrderUpdateOrigin.ADL_AUTOCLOSE,
            "COLLATERAL_CONVERSION": OrderUpdateOrigin.COLLATERAL_CONVERSION,
            "SETTLEMENT_AUTOCLOSE": OrderUpdateOrigin.SETTLEMENT_AUTOCLOSE,
            "BACKSTOP_LIQUIDITY_PROVIDER": OrderUpdateOrigin.BACKSTOP_LIQUIDITY_PROVIDER,
        }
        return origin_mapping.get(origin_str, OrderUpdateOrigin.UNKNOWN)

    @staticmethod
    def _parse_required_order_fields(raw: BackpackRawOrder) -> tuple[Decimal, datetime]:
        """Parse and validate required order fields.

        Args:
            raw: Raw order data from Backpack

        Returns:
            tuple[Decimal, datetime]: Tuple of (quantity, timestamp) with parsed and
                validated values

        Raises:
            MissingRequiredFieldError: If parsing fails
        """
        parsed_quantity = parse_decimal_value(raw.quantity, allow_none=False)

        parsed_created_at = parse_datetime_utc(raw.createdAt)
        if parsed_created_at is None:
            raise MissingRequiredFieldError(
                field_names="createdAt",
                context="BackpackRawOrder",
                source_data=raw.model_dump() if raw else None,
            )

        return parsed_quantity, parsed_created_at

    @staticmethod
    def _parse_optional_order_fields(raw: BackpackRawOrder) -> dict[str, Any]:
        """Parse optional order fields.

        Args:
            raw: Raw order data from Backpack

        Returns:
            dict[str, Any]: Dictionary with parsed optional order field values
        """
        quantity_filled = parse_decimal_value(raw.executedQuantity) or Decimal("0.0")

        # Calculate average fill price if not provided but order has fills
        avg_fill_price = parse_decimal_value(raw.avgFillPrice)
        if avg_fill_price is None and quantity_filled > 0 and raw.executedQuoteQuantity:
            executed_quote = parse_decimal_value(raw.executedQuoteQuantity, allow_none=True)
            if executed_quote is not None and executed_quote > 0:
                avg_fill_price = executed_quote / quantity_filled

        return {
            "quantity_filled": quantity_filled,
            "price": parse_decimal_value(raw.price),
            "stop_price": parse_decimal_value(raw.triggerPrice),
            "avg_fill_price": avg_fill_price,
            "executed_quote_quantity": parse_decimal_value(
                raw.executedQuoteQuantity,
                allow_none=True,
            ),
        }

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: BackpackRawFill) -> Trade | None:
        """Transform a BackpackRawFill to an Internal Trade model.

        Converts fill data from Backpack order execution into an internal Trade domain model.

        Args:
            raw_fill: Validated raw fill from Backpack

        Returns:
            Trade | None: Internal domain model with BP details populated, or None if
                         price or quantity is zero

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_fill",
                trade_id=raw_fill.trade_id,
                symbol=raw_fill.symbol,
                side=raw_fill.side,
                price=raw_fill.price,
                quantity=raw_fill.quantity,
                message="Transforming BackpackRawFill to Trade",
            )

            # Map side
            side = BackpackTransactionMapper._map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_fill.price, allow_none=False, field_name="price")
            quantity = parse_decimal_value(
                raw_fill.quantity,
                allow_none=False,
                field_name="quantity",
            )

            price_typed, quantity_typed = BackpackTransactionMapper._ensure_trade_values_not_none(
                price, quantity
            )

            # Check if price or quantity is zero - Trade model requires positive values
            if price_typed <= Decimal(0) or quantity_typed <= Decimal(0):
                logger.warning(
                    "trade_zero_price_or_quantity",
                    trade_id=raw_fill.trade_id,
                    price=price_typed,
                    quantity=quantity_typed,
                    action="skipping",
                    message="Skipping trade with zero price or quantity",
                )
                return None

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_fill.timestamp, field_name="timestamp")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(raw_fill.fee, allow_none=True, field_name="fee") or Decimal(0)

            # Create BP-specific details
            details = BackpackTradeDetails(
                system_order_type=None,  # Not available in fill data
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": str(raw_fill.trade_id),
                "symbol": raw_fill.symbol,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": raw_fill.order_id,
                "exchange": ExchangeName.BACKPACK.value,
                "client_order_id": raw_fill.client_id,
                "price": str(price_typed),
                "quantity": str(quantity_typed),
                "fee": str(fee),
                "fee_asset": raw_fill.fee_symbol,
                "is_maker": raw_fill.is_maker,
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            trade = secure_transform(
                data=trade_data,
                model_class=Trade,
                context="backpack_fill_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_fill_transformed",
                trade_id=raw_fill.trade_id,
                symbol=raw_fill.symbol,
                side=side.value,
                price=str(price_typed),
                quantity=str(quantity_typed),
                fee=str(fee),
                is_maker=raw_fill.is_maker,
                message="Successfully transformed BackpackRawFill to Trade",
            )

        except Exception as e:
            logger.exception(
                "raw_fill_transform_failed",
                trade_id=getattr(raw_fill, "trade_id", None),
                symbol=getattr(raw_fill, "symbol", None),
                raw_fill=raw_fill.model_dump() if raw_fill else None,
                error=str(e),
                message="Failed to transform BackpackRawFill to Trade",
            )
            raise DataTransformationError(
                source_model="BackpackRawFill",
                target_model="Trade",
                reason=str(e),
                original_error=e,
                source_data=raw_fill.model_dump() if raw_fill else None,
            ) from e
        else:
            return trade

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        """Transform a validated BackpackRawOrder object into an internal Order domain model.

        Converts order data from Backpack into an internal Order domain model with comprehensive
        field mapping and validation.

        Args:
            raw: The validated raw order data from Backpack

        Returns:
            Order: The corresponding internal Order object

        Raises:
            OrderTransformationError: If essential fields are missing or cannot be parsed
        """
        try:
            logger.debug(
                "transforming_raw_order",
                order_id=raw.id,
                symbol=raw.symbol,
                side=raw.side,
                order_type=raw.orderType,
                status=raw.status,
                quantity=raw.quantity,
                message="Transforming BackpackRawOrder to Order",
            )

            # Parse required fields
            parsed_quantity, parsed_created_at = (
                BackpackTransactionMapper._parse_required_order_fields(raw)
            )

            # Parse optional fields
            optional_fields = BackpackTransactionMapper._parse_optional_order_fields(raw)

            # Create BackpackOrderDetails with mapped enums
            bp_details = BackpackOrderDetails(
                executed_quote_quantity=optional_fields["executed_quote_quantity"],
                self_trade_prevention=BackpackTransactionMapper._map_self_trade_prevention(
                    raw.selfTradePrevention,
                ),
                expiry_reason=BackpackTransactionMapper._map_expiry_reason(raw.expiryReason),
                origin=BackpackTransactionMapper._map_order_origin(raw.origin),
            )

            # Parse optional complex fields that can be None
            trigger_by_result = BackpackTransactionMapper._map_trigger_by_to_internal(raw.triggerBy)
            trigger_by_value = trigger_by_result.value if trigger_by_result is not None else None

            updated_dt = parse_datetime_utc(raw.updatedAt) if raw.updatedAt else None
            updated_at_value = updated_dt.isoformat() if updated_dt is not None else None

            triggered_dt = parse_datetime_utc(raw.triggeredAt) if raw.triggeredAt else None
            triggered_at_value = triggered_dt.isoformat() if triggered_dt is not None else None

            # Use secure_transform for type-safe model creation
            order_data: dict[str, Any] = {
                "client_order_id": raw.clientId or str(uuid.uuid4()),
                "exchange_order_id": raw.id,
                "related_order_id": raw.relatedOrderId,
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": raw.symbol,
                "side": BackpackTransactionMapper._map_side_to_internal(raw.side).value,
                "order_type": BackpackTransactionMapper._map_type_to_internal(raw.orderType).value,
                "status": BackpackTransactionMapper._map_status_to_internal(raw.status).value,
                "quantity_requested": str(parsed_quantity),
                "quantity_filled": str(optional_fields["quantity_filled"]),
                "price": str(optional_fields["price"])
                if optional_fields["price"] is not None
                else None,
                "stop_price": str(optional_fields["stop_price"])
                if optional_fields["stop_price"] is not None
                else None,
                "average_fill_price": str(optional_fields["avg_fill_price"])
                if optional_fields["avg_fill_price"] is not None
                else None,
                "trigger_by": trigger_by_value,
                "time_in_force": BackpackTransactionMapper._map_tif_to_internal(
                    raw.timeInForce,
                ).value,
                "reduce_only": raw.reduceOnly or False,
                "post_only": raw.postOnly or False,
                "created_at": parsed_created_at.isoformat(),
                "updated_at": updated_at_value,
                "triggered_at": triggered_at_value,
                "strategy_name": None,
                "signal_id": None,
                "trades": [],
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            order = secure_transform(
                data=order_data,
                model_class=Order,
                context="backpack_order_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_order_transformed",
                order_id=raw.id,
                client_order_id=raw.clientId,
                symbol=raw.symbol,
                side=BackpackTransactionMapper._map_side_to_internal(raw.side).value,
                order_type=BackpackTransactionMapper._map_type_to_internal(raw.orderType).value,
                status=BackpackTransactionMapper._map_status_to_internal(raw.status).value,
                quantity_requested=str(parsed_quantity),
                quantity_filled=str(optional_fields["quantity_filled"]),
                message="Successfully transformed BackpackRawOrder to Order",
            )

        except Exception as e:
            logger.exception(
                "raw_order_transform_failed",
                order_id=getattr(raw, "id", None),
                symbol=getattr(raw, "symbol", None),
                raw_order=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform BackpackRawOrder to Order",
            )
            raise OrderTransformationError(
                order_id=raw.id if raw else None,
                reason=str(e),
                order_data=raw.model_dump() if raw else None,
                original_error=e,
            ) from e
        else:
            return order

    @staticmethod
    def transform_raw_trade_to_internal(raw: BackpackRawPublicTrade) -> Trade | None:
        """Transform a validated BackpackRawPublicTrade into an internal Trade model.

        Note: Backpack REST API for trades typically lacks side information.
        Returns None if essential information cannot be determined.

        Args:
            raw: The validated raw trade data from Backpack

        Returns:
            Trade | None: The corresponding internal Trade object, or None if essential
                         information (like side) cannot be determined

        Raises:
            DataTransformationError: If essential fields are missing or cannot be parsed
        """
        try:
            logger.debug(
                "transforming_raw_public_trade",
                trade_id=raw.id,
                symbol=raw.symbol,
                price=raw.price,
                quantity=raw.quantity,
                message="Attempting to transform BackpackRawPublicTrade to Trade",
            )

            price_dec = parse_decimal_value(raw.price, allow_none=False, field_name="price")
            quantity_dec = parse_decimal_value(
                raw.quantity,
                allow_none=False,
                field_name="quantity",
            )
            timestamp = parse_datetime_utc(raw.time, field_name="time")

            BackpackTransactionMapper._ensure_public_trade_fields_not_none(
                price_dec, quantity_dec, timestamp
            )

            # Backpack REST API for recent trades doesn't provide side
            logger.warning(
                "trade_side_missing",
                trade_id=raw.id,
                symbol=raw.symbol,
                action="skipping",
                message=(
                    "Cannot determine trade side for raw trade from REST API, "
                    "skipping transformation"
                ),
            )

        except Exception as e:
            logger.exception(
                "raw_public_trade_transform_failed",
                trade_id=getattr(raw, "id", None),
                symbol=getattr(raw, "symbol", None),
                raw_trade=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform BackpackRawPublicTrade to Trade",
            )
            raise DataTransformationError(
                source_model="BackpackRawPublicTrade",
                target_model="PublicTrade",
                reason=str(e),
                original_error=e,
                source_data=raw.model_dump() if raw else None,
            ) from e
        else:
            return None

    @staticmethod
    def transform_ws_fill_event_to_internal_trade(raw_fill: BackpackRawFill) -> Trade | None:
        """Transform a WebSocket fill event (BackpackRawFill) to an Internal Trade model.

        This is an alias for transform_raw_fill_to_internal for consistency with WebSocket naming.

        Args:
            raw_fill: Validated raw fill event from Backpack WebSocket

        Returns:
            Trade | None: Internal domain model with BP details populated, or None if
                         price or quantity is zero
        """
        return BackpackTransactionMapper.transform_raw_fill_to_internal(raw_fill)

    # MapperProtocol implementation - delegate to common utilities
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Safely parse decimal values with fallback."""
        return BackpackCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Convert symbol to Backpack format (underscore-separated)."""
        return BackpackCommonMappers.normalize_symbol(symbol)

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Convert symbol from Backpack to internal format (slash-separated)."""
        return BackpackCommonMappers.denormalize_symbol(symbol)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to UTC datetime."""
        return BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
