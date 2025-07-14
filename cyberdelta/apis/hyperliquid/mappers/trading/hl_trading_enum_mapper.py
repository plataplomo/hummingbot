"""Hyperliquid Trading Enum Mapper.

This mapper handles enum conversions for trading-related data from the Hyperliquid exchange,
extracted from the monolithic trading data mapper to improve maintainability and testability.

Focused on:
- Order side mapping (Buy/Sell)
- Order status mapping
- Order type mapping with trigger support
- Time in force mapping
- Trigger type mapping
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import OrderTransformationError, UnknownEnumError
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawTriggerInfo
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import TradingEnumMapperProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)


logger = get_logger(__name__)


def _is_dict_str_any(value: object) -> bool:
    """Type guard to check if value is a dict[str, Any]."""
    return isinstance(value, dict)


class HyperliquidTradingEnumMapper(TradingEnumMapperProtocol):
    """Focused mapper for Hyperliquid trading enum transformations.

    Handles all enum conversions for trading operations including order side,
    status, type, time in force, and trigger mappings.

    Implements TradingEnumMapperProtocol for full protocol compliance.
    """

    # Base protocol methods - delegate to HyperliquidCommonMappers
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Parse decimal values safely with default fallback."""
        return HyperliquidCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize symbol to internal format."""
        return HyperliquidCommonMappers.normalize_symbol(symbol)

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Denormalize symbol to exchange format."""
        return HyperliquidCommonMappers.denormalize_symbol(symbol)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime."""
        return HyperliquidCommonMappers.timestamp_ms_to_datetime(timestamp_ms)

    # Protocol-compliant static methods
    @staticmethod
    def map_order_side(raw_side: str) -> OrderSide:
        """Map raw order side to internal enum.

        Args:
            raw_side: Raw order side string from API

        Returns:
            Internal order side enum
        """
        return HyperliquidTradingEnumMapper.map_side_to_internal(raw_side)

    @staticmethod
    def map_order_type(raw_type: str) -> OrderType:
        """Map raw order type to internal enum.

        Note: This is a simplified version for basic string types.
        For complex dict types, use map_type_to_internal directly.

        Args:
            raw_type: Raw order type string from API

        Returns:
            Internal order type enum
        """
        # Handle simple string cases - for dict types, use map_type_to_internal
        if raw_type.lower() == "limit":
            return OrderType.LIMIT
        if raw_type.lower() == "market":
            return OrderType.MARKET
        # Default to limit for unknown types
        logger.warning(
            "unknown_order_type_string_defaulting_to_limit",
            component="HyperliquidTradingEnumMapper",
            action="map_order_type",
            raw_type=raw_type,
            message="Unknown order type string, defaulting to LIMIT",
        )
        return OrderType.LIMIT

    @staticmethod
    def map_order_status(raw_status: str) -> OrderStatus:
        """Map raw order status to internal enum.

        Args:
            raw_status: Raw order status string from API

        Returns:
            Internal order status enum
        """
        return HyperliquidTradingEnumMapper.map_status_to_internal(raw_status)

    @staticmethod
    def validate_order_side(hl_side: str) -> None:
        """Validate order side is a known value.

        Args:
            hl_side: The side value to validate

        Raises:
            UnknownEnumError: If side is not B or A
        """
        if hl_side not in {"B", "A"}:
            raise UnknownEnumError(
                enum_type="OrderSide",
                value=hl_side,
                valid_values=["B", "A"],
            )

    @staticmethod
    def map_side_to_internal(hl_side: str) -> OrderSide:
        """Maps a Hyperliquid order side string to internal OrderSide enum.

        Args:
            hl_side: Raw side string from Hyperliquid ("B" or "A")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped

        """
        try:
            # Validate the side value
            HyperliquidTradingEnumMapper.validate_order_side(hl_side)
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to map order side: {e}",
                order_data={"hl_side": hl_side, "exception": str(e)},
                original_error=e,
            ) from e
        else:
            if hl_side == "B":
                return OrderSide.BUY
            # hl_side == "A" due to validation
            return OrderSide.SELL

    @staticmethod
    def map_status_to_internal(hl_status: str) -> OrderStatus:
        """Maps a Hyperliquid order status string to internal OrderStatus enum.

        Args:
            hl_status: Raw status string from Hyperliquid

        Returns:
            OrderStatus: Mapped internal enum value

        """
        try:
            status_map = {
                "open": OrderStatus.OPEN,
                "filled": OrderStatus.FILLED,
                "canceled": OrderStatus.CANCELED,  # Hyperliquid uses "canceled"
                "rejected": OrderStatus.REJECTED,
                # Map expired to UNKNOWN since we don't have an EXPIRED status
                "expired": OrderStatus.UNKNOWN,
            }
            mapped_status = status_map.get(hl_status.lower(), OrderStatus.UNKNOWN)

            if mapped_status == OrderStatus.UNKNOWN and hl_status.lower() not in status_map:
                logger.warning(
                    "unknown_order_status_mapped_to_unknown",
                    component="HyperliquidTradingEnumMapper",
                    action="map_status_to_internal",
                    hl_status=hl_status,
                    message="Mapping unknown order status to UNKNOWN",
                )

        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            logger.exception(
                "hl_trading_mapper_map_status_failed",
                action="map_status_to_internal",
                message="Failed to map order status",
                hl_status=hl_status,
                error=str(e),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to map order status: {e}",
                order_data={"hl_status": hl_status, "exception": str(e)},
                original_error=e,
            ) from e
        else:
            return mapped_status

    @staticmethod
    def get_trigger_type(trigger: HyperliquidRawTriggerInfo | None) -> str | None:
        """Extract trigger type from trigger info."""
        return getattr(trigger, "tpsl", None) if trigger else None

    @staticmethod
    def map_limit_order_type(trigger: HyperliquidRawTriggerInfo | None) -> OrderType:
        """Map limit order types with optional trigger."""
        trigger_type = HyperliquidTradingEnumMapper.get_trigger_type(trigger)
        if trigger_type == "sl":
            return OrderType.STOP_LIMIT
        if trigger_type == "tp":
            return OrderType.TAKE_PROFIT_LIMIT
        return OrderType.LIMIT

    @staticmethod
    def map_market_order_type(trigger: HyperliquidRawTriggerInfo | None) -> OrderType:
        """Map market order types with optional trigger."""
        trigger_type = HyperliquidTradingEnumMapper.get_trigger_type(trigger)
        if trigger_type == "sl":
            return OrderType.STOP_MARKET
        if trigger_type == "tp":
            return OrderType.TAKE_PROFIT_MARKET
        return OrderType.MARKET

    @staticmethod
    def map_type_to_internal(
        order_type: dict[str, Any],
        trigger: HyperliquidRawTriggerInfo | None,
    ) -> OrderType:
        """Maps a Hyperliquid order type dict to internal OrderType enum.

        Args:
            order_type: Raw order type dict from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            OrderType: Mapped internal enum value

        Raises:
            TransformationError: If order type structure is invalid

        """
        try:
            # Hyperliquid uses nested dicts for orderType,
            # e.g. {"limit": {"tif": "Gtc"}}, {"market": {}}
            if "limit" in order_type:
                return HyperliquidTradingEnumMapper.map_limit_order_type(trigger)
            if "market" in order_type:
                return HyperliquidTradingEnumMapper.map_market_order_type(trigger)

            logger.warning(
                "unknown_order_type_structure_defaulting_to_limit",
                component="HyperliquidTradingEnumMapper",
                action="map_type_to_internal",
                order_type=str(order_type),
                message="Unknown orderType structure, defaulting to LIMIT",
            )
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            logger.exception(
                "hl_trading_mapper_map_type_failed",
                action="map_type_to_internal",
                message="Failed to map order type",
                error=str(e),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to map order type: {e}",
                order_data={"order_type": str(order_type), "exception": str(e)},
                original_error=e,
            ) from e
        else:
            return OrderType.LIMIT

    @staticmethod
    def map_time_in_force(order_type: dict[str, Any]) -> TimeInForce:
        """Maps a Hyperliquid order type dict to internal TimeInForce enum.

        Args:
            order_type: Raw order type dict from Hyperliquid

        Returns:
            TimeInForce: Mapped internal enum value

        """
        try:
            # Only limit orders have TIF in HL
            if "limit" in order_type and _is_dict_str_any(order_type["limit"]):
                # TypeGuard confirms it's a dict[str, Any]
                limit_dict = order_type["limit"]
                tif_val: Any = limit_dict.get("tif", "")
                tif_str = str(tif_val).upper()

                if tif_str == "GTC":
                    return TimeInForce.GTC
                if tif_str == "IOC":
                    return TimeInForce.IOC
                if tif_str == "ALO":
                    return TimeInForce.ALO
                if tif_str:
                    logger.warning(
                        "unknown_tif_value_defaulting_to_gtc",
                        component="HyperliquidTradingEnumMapper",
                        action="map_time_in_force",
                        tif_str=tif_str,
                        message="Unknown TIF value, defaulting to GTC",
                    )

        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            logger.exception(
                "hl_trading_mapper_map_tif_failed",
                action="map_time_in_force",
                message="Failed to map time in force",
                error=str(e),
            )
            # Default to GTC on error rather than raising per business logic
            return TimeInForce.GTC
        else:
            return TimeInForce.GTC

    @staticmethod
    def map_trigger_type(trigger_type_str: str | None) -> TriggerType | None:
        """Map trigger type string to internal TriggerType enum.

        Args:
            trigger_type_str: Trigger type string from Hyperliquid

        Returns:
            TriggerType or None if no trigger type
        """
        if not trigger_type_str:
            return None

        if trigger_type_str.lower() == "mark":
            return TriggerType.MARK_PRICE
        if trigger_type_str.lower() == "last":
            return TriggerType.LAST_PRICE

        return None
