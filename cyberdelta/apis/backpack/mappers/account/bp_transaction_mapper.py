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

from cyberdelta.apis.backpack.mappers.utils.backpack_enum_mappers import BackpackEnumMappers
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.backpack.protocols.mapper_protocols import TransactionMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin, ValidationMixin
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
    MissingRequiredFieldError,
    OrderTransformationError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import (
    OrderExpiryReason,
    OrderStatus,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TriggerType,
)
from cyberdelta.enums import (
    OrderType,
    TimeInForce,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import BackpackOrderDetails, Order, Trade
from cyberdelta.models.market.trade import BackpackTradeDetails
from cyberdelta.symbols import exchanges
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackTransactionMapper(CommonDataParserMixin, ValidationMixin, TransactionMapperProtocol):
    """Focused mapper for Backpack transaction data transformations.

    This class contains static methods for transforming validated Backpack Raw transaction models
    into CyberDeltaEngine Internal Domain Models for orders and trades.
    """

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

    def _parse_required_order_fields(
        self, raw: BackpackRawOrderResponse
    ) -> tuple[Decimal, datetime]:
        """Parse and validate required order fields.

        Args:
            raw: Raw order data from Backpack

        Returns:
            tuple[Decimal, datetime]: Tuple of (quantity, timestamp) with parsed and
                validated values

        Raises:
            MissingRequiredFieldError: If parsing fails
        """
        parsed_quantity = self.parse_decimal_safely(raw.quantity)
        if parsed_quantity is None:
            raise MissingRequiredFieldError(
                field_names="quantity",
                context="BackpackRawOrderResponse",
                source_data=raw.model_dump() if raw else None,
            )

        parsed_created_at = self.parse_timestamp(raw.createdAt)
        if parsed_created_at is None:
            raise MissingRequiredFieldError(
                field_names="createdAt",
                context="BackpackRawOrderResponse",
                source_data=raw.model_dump() if raw else None,
            )

        return parsed_quantity, parsed_created_at

    def _parse_optional_order_fields(self, raw: BackpackRawOrderResponse) -> dict[str, Any]:
        """Parse optional order fields.

        Args:
            raw: Raw order data from Backpack

        Returns:
            dict[str, Any]: Dictionary with parsed optional order field values
        """
        quantity_filled = self.parse_decimal_safely(raw.executedQuantity, default=Decimal("0.0"))

        # Calculate average fill price if not provided but order has fills
        avg_fill_price = self.parse_decimal_safely(raw.avgFillPrice, default=None)
        if (
            avg_fill_price is None
            and quantity_filled
            and quantity_filled > 0
            and raw.executedQuoteQuantity
        ):
            executed_quote = self.parse_decimal_safely(raw.executedQuoteQuantity, default=None)
            if executed_quote is not None and executed_quote > 0:
                avg_fill_price = executed_quote / quantity_filled

        return {
            "quantity_filled": quantity_filled,
            "price": self.parse_decimal_safely(raw.price, default=None),
            "stop_price": self.parse_decimal_safely(raw.triggerPrice, default=None),
            "avg_fill_price": avg_fill_price,
            "executed_quote_quantity": self.parse_decimal_safely(
                raw.executedQuoteQuantity, default=None
            ),
        }

    def transform_raw_fill_to_internal(self, raw_fill: BackpackRawFillResponse) -> Trade | None:
        """Transform a BackpackRawFillResponse to an Internal Trade model.

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
                message="Transforming BackpackRawFillResponse to Trade",
            )

            # Map side
            side = BackpackEnumMappers.map_side_to_internal(raw_fill.side, "standard")

            # Parse price and quantity
            price = self.parse_decimal_safely(raw_fill.price)
            quantity = self.parse_decimal_safely(raw_fill.quantity)

            # Ensure values are not None
            price_typed = self.ensure_decimal_not_none(price, "price", "trade")
            quantity_typed = self.ensure_decimal_not_none(quantity, "quantity", "trade")

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
            executed_at = self.parse_timestamp(raw_fill.timestamp)
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = self.parse_decimal_safely(raw_fill.fee, default=Decimal(0))

            # Create BP-specific details
            details = BackpackTradeDetails(
                system_order_type=None,  # Not available in fill data
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_fill.symbol,
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": str(raw_fill.trade_id),
                "symbol": exchange_symbol,  # Domain object!
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
                message="Successfully transformed BackpackRawFillResponse to Trade",
            )

        except Exception as e:
            logger.exception(
                "raw_fill_transform_failed",
                trade_id=getattr(raw_fill, "trade_id", None),
                symbol=getattr(raw_fill, "symbol", None),
                raw_fill=raw_fill.model_dump() if raw_fill else None,
                error=str(e),
                message="Failed to transform BackpackRawFillResponse to Trade",
            )
            raise DataTransformationError(
                source_model="BackpackRawFillResponse",
                target_model="Trade",
                reason=str(e),
                original_error=e,
                source_data=raw_fill.model_dump() if raw_fill else None,
            ) from e
        else:
            return trade

    def transform_raw_order_to_internal(self, raw: BackpackRawOrderResponse) -> Order:
        """Transform a validated BackpackRawOrderResponse into an internal Order domain model.

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
                message="Transforming BackpackRawOrderResponse to Order",
            )

            # Parse required fields
            parsed_quantity, parsed_created_at = self._parse_required_order_fields(raw)

            # Parse optional fields
            optional_fields = self._parse_optional_order_fields(raw)

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

            updated_dt = self.parse_timestamp(raw.updatedAt) if raw.updatedAt else None
            updated_at_value = updated_dt.isoformat() if updated_dt is not None else None

            triggered_dt = self.parse_timestamp(raw.triggeredAt) if raw.triggeredAt else None
            triggered_at_value = triggered_dt.isoformat() if triggered_dt is not None else None

            # Use secure_transform for type-safe model creation
            order_data: dict[str, Any] = {
                "client_order_id": raw.clientId or str(uuid.uuid4()),
                "exchange_order_id": raw.id,
                "related_order_id": raw.relatedOrderId,
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": exchanges.backpack(value=raw.symbol),  # Domain object!
                "side": BackpackEnumMappers.map_side_to_internal(raw.side, "standard").value,
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
                side=BackpackEnumMappers.map_side_to_internal(raw.side, "standard").value,
                order_type=BackpackTransactionMapper._map_type_to_internal(raw.orderType).value,
                status=BackpackTransactionMapper._map_status_to_internal(raw.status).value,
                quantity_requested=str(parsed_quantity),
                quantity_filled=str(optional_fields["quantity_filled"]),
                message="Successfully transformed BackpackRawOrderResponse to Order",
            )

        except Exception as e:
            logger.exception(
                "raw_order_transform_failed",
                order_id=getattr(raw, "id", None),
                symbol=getattr(raw, "symbol", None),
                raw_order=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform BackpackRawOrderResponse to Order",
            )
            raise OrderTransformationError(
                order_id=raw.id if raw else None,
                reason=str(e),
                order_data=raw.model_dump() if raw else None,
                original_error=e,
            ) from e
        else:
            return order

    def transform_raw_trade_to_internal(self, raw: BackpackRawPublicTrade) -> Trade | None:
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

            price_dec = self.parse_decimal_safely(raw.price)
            quantity_dec = self.parse_decimal_safely(raw.quantity)
            timestamp = self.parse_timestamp(raw.time)

            # Ensure required fields are not None
            price_dec = self.ensure_decimal_not_none(price_dec, "price", "BackpackRawPublicTrade")
            quantity_dec = self.ensure_decimal_not_none(
                quantity_dec, "quantity", "BackpackRawPublicTrade"
            )
            if timestamp is None:
                self._raise_missing_timestamp_error()

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

    def _raise_missing_timestamp_error(self) -> None:
        """Raise MissingRequiredFieldError for missing timestamp.

        Raises:
            MissingRequiredFieldError: Always raised for missing timestamp
        """
        raise MissingRequiredFieldError(
            field_names="time",
            context="BackpackRawPublicTrade",
        )

    def transform_ws_fill_event_to_internal_trade(
        self,
        raw_fill: BackpackRawFillResponse,
    ) -> Trade | None:
        """Transform a WebSocket fill event (BackpackRawFillResponse) to an Internal Trade model.

        This is an alias for transform_raw_fill_to_internal for consistency with WebSocket naming.

        Args:
            raw_fill: Validated raw fill event from Backpack WebSocket

        Returns:
            Trade | None: Internal domain model with BP details populated, or None if
                         price or quantity is zero
        """
        return self.transform_raw_fill_to_internal(raw_fill)
