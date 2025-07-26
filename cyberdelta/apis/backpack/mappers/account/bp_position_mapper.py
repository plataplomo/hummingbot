"""Backpack Position Mapper.

This mapper handles transformations for position-related data from the Backpack exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- DerivativePosition transformations from raw position data
- Position updates from WebSocket data
- Position-specific data validation and type conversion
- Position-specific error handling and logging
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPositionResponse,
    BackpackRawPositionUpdate,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import PositionMapperProtocol
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import BackpackPositionDetails, DerivativePosition
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackPositionMapper(PositionMapperProtocol):
    """Focused mapper for Backpack position data transformations.

    This class contains static methods for transforming validated Backpack Raw position models
    into CyberDeltaEngine Internal DerivativePosition Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """Map a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell", "Bid", "Ask")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            ValueError: If side cannot be mapped
        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in {"buy", "bid", "long"}:
            return OrderSide.BUY
        if side_lower in {"sell", "ask", "short"}:
            return OrderSide.SELL

        logger.warning(
            "unknown_position_side",
            bp_side=bp_side,
            mapped_to="BUY",
            message="Unknown Backpack position side encountered, defaulting to BUY",
        )
        return OrderSide.BUY  # Default fallback

    @staticmethod
    def _ensure_position_size_not_none(size: Decimal | None) -> Decimal:
        """Ensure position size is not None.

        Args:
            size: Position size value

        Returns:
            Decimal: The validated size value

        Raises:
            MissingRequiredFieldError: If size is None
        """
        if size is None:
            raise MissingRequiredFieldError(
                field_names="size",
                context="position_validation",
            )
        return size

    @staticmethod
    def transform_raw_position_to_internal(raw: BackpackRawPositionResponse) -> DerivativePosition:
        """Transform a validated BackpackRawPositionResponse object into an internal model.

        Args:
            raw: The validated raw position data from Backpack

        Returns:
            DerivativePosition: The corresponding internal DerivativePosition object

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_position",
                symbol=raw.symbol,
                size=raw.net_quantity,
                message="Transforming BackpackRawPositionResponse to DerivativePosition",
            )

            # Parse and validate position size
            size_dec = parse_decimal_value(
                raw.net_quantity, allow_none=False, field_name="net_quantity"
            )
            size_typed = BackpackPositionMapper._ensure_position_size_not_none(size_dec)

            # Determine side from size (positive = long/BUY, negative = short/SELL)
            side = OrderSide.BUY if size_typed >= 0 else OrderSide.SELL

            # Parse optional price fields
            entry_price_dec = parse_decimal_value(raw.entry_price, allow_none=True)
            mark_price_dec = parse_decimal_value(raw.mark_price, allow_none=True)
            liq_price_dec = parse_decimal_value(raw.est_liquidation_price, allow_none=True)
            unrealized_pnl_dec = parse_decimal_value(raw.pnl_unrealized, allow_none=True)
            realized_pnl_dec = parse_decimal_value(raw.pnl_realized, allow_none=True)

            # Use current time as timestamp since BackpackRawPositionResponse doesn't have timestamp
            timestamp = datetime.now(UTC)

            # Parse margin and funding fields if available
            imf_base_dec = None
            imf_factor_dec = None
            mmf_base_dec = None
            mmf_factor_dec = None
            cumulative_funding_dec = None

            # imf_function, mmf_function, and cumulative_funding_payment are required fields
            if raw.imf_function:
                imf_base_dec = parse_decimal_value(raw.imf_function.base, allow_none=True)
                imf_factor_dec = parse_decimal_value(raw.imf_function.factor, allow_none=True)

            if raw.mmf_function:
                mmf_base_dec = parse_decimal_value(raw.mmf_function.base, allow_none=True)
                mmf_factor_dec = parse_decimal_value(raw.mmf_function.factor, allow_none=True)

            # cumulative_funding_payment is always present
            cumulative_funding_dec = parse_decimal_value(
                raw.cumulative_funding_payment,
                allow_none=True,
            )

            # Create BP-specific details
            bp_details = BackpackPositionDetails(
                imf_base=imf_base_dec,
                imf_factor=imf_factor_dec,
                mmf_base=mmf_base_dec,
                mmf_factor=mmf_factor_dec,
                cumulative_funding=cumulative_funding_dec,
            )

            # Use secure_transform for type-safe model creation
            position_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": raw.symbol,
                "timestamp": timestamp.isoformat(),
                "side": side.value,
                "size": str(size_typed),
                "entry_price": str(entry_price_dec) if entry_price_dec is not None else None,
                "mark_price": str(mark_price_dec) if mark_price_dec is not None else None,
                "liquidation_price": str(liq_price_dec) if liq_price_dec is not None else None,
                "unrealized_pnl": str(unrealized_pnl_dec)
                if unrealized_pnl_dec is not None
                else None,
                "realized_pnl": str(realized_pnl_dec) if realized_pnl_dec is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
            }

            position = secure_transform(
                data=position_data,
                model_class=DerivativePosition,
                context=f"backpack_position_transform_{raw.symbol}",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_position_transformed",
                symbol=raw.symbol,
                side=side.value,
                size=str(size_typed),
                entry_price=str(entry_price_dec) if entry_price_dec else None,
                mark_price=str(mark_price_dec) if mark_price_dec else None,
                message=(
                    "Successfully transformed BackpackRawPositionResponse to DerivativePosition"
                ),
            )

        except Exception as e:
            logger.exception(
                "raw_position_transform_failed",
                symbol=getattr(raw, "symbol", None),
                raw_position=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform BackpackRawPositionResponse to DerivativePosition",
            )
            raise DataTransformationError(
                source_model="BackpackRawPositionResponse",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
                source_data=raw.model_dump() if raw else None,
            ) from e
        else:
            return position

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> DerivativePosition:
        """Transform a BackpackRawPositionUpdate to an Internal DerivativePosition.

        Converts WebSocket position update data into an internal DerivativePosition domain model.

        Args:
            raw_position_update: The validated position update data from WebSocket

        Returns:
            DerivativePosition: The corresponding internal DerivativePosition object

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_ws_position_update",
                symbol=raw_position_update.symbol,
                size=raw_position_update.net_quantity,
                message="Transforming BackpackRawPositionUpdate to DerivativePosition",
            )

            # Parse and validate position size
            size_dec = parse_decimal_value(
                raw_position_update.net_quantity,
                allow_none=True,
                field_name="net_quantity",
            )
            if size_dec is None:
                size_dec = Decimal(0)
            size_typed = BackpackPositionMapper._ensure_position_size_not_none(size_dec)

            # Determine side from size (positive = long/BUY, negative = short/SELL)
            side = OrderSide.BUY if size_typed >= 0 else OrderSide.SELL

            # Parse optional price fields
            entry_price_dec = parse_decimal_value(raw_position_update.entry_price, allow_none=True)
            mark_price_dec = parse_decimal_value(raw_position_update.mark_price, allow_none=True)
            liq_price_dec = parse_decimal_value(
                raw_position_update.liquidation_price, allow_none=True
            )
            # These fields don't exist in BackpackRawPositionUpdate
            unrealized_pnl_dec = None
            realized_pnl_dec = None

            # Parse timestamp - use event_time if available
            timestamp = parse_datetime_utc(raw_position_update.event_time, field_name="event_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Parse margin fields - BackpackRawPositionUpdate uses different field names
            # than BackpackRawPositionResponse (imf_function vs initial_margin_fraction)
            imf_base_dec = None
            imf_factor_dec = None
            mmf_base_dec = None
            mmf_factor_dec = None

            # BackpackRawPositionUpdate has initial_margin_fraction and maintenance_margin_fraction
            # fields instead of imf_function/mmf_function objects
            if raw_position_update.initial_margin_fraction is not None:
                imf_base_dec = parse_decimal_value(
                    raw_position_update.initial_margin_fraction, allow_none=True
                )
                # No factor available in position update, only base value
                imf_factor_dec = None

            if raw_position_update.maintenance_margin_fraction is not None:
                mmf_base_dec = parse_decimal_value(
                    raw_position_update.maintenance_margin_fraction, allow_none=True
                )
                # No factor available in position update, only base value
                mmf_factor_dec = None

            # Create BP-specific details
            bp_details = BackpackPositionDetails(
                imf_base=imf_base_dec,
                imf_factor=imf_factor_dec,
                mmf_base=mmf_base_dec,
                mmf_factor=mmf_factor_dec,
                cumulative_funding=None,  # Not typically available in position updates
            )

            # Use secure_transform for type-safe model creation
            position_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": raw_position_update.symbol,
                "timestamp": timestamp.isoformat(),
                "side": side.value,
                "size": str(size_typed),
                "entry_price": str(entry_price_dec) if entry_price_dec is not None else None,
                "mark_price": str(mark_price_dec) if mark_price_dec is not None else None,
                "liquidation_price": str(liq_price_dec) if liq_price_dec is not None else None,
                "unrealized_pnl": str(unrealized_pnl_dec)
                if unrealized_pnl_dec is not None
                else None,
                "realized_pnl": str(realized_pnl_dec) if realized_pnl_dec is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
            }

            position = secure_transform(
                data=position_data,
                model_class=DerivativePosition,
                context=f"backpack_ws_position_update_{raw_position_update.symbol}",
                source_exchange="backpack",
            )

            logger.debug(
                "ws_position_update_transformed",
                symbol=raw_position_update.symbol,
                side=side.value,
                size=str(size_typed),
                entry_price=str(entry_price_dec) if entry_price_dec else None,
                mark_price=str(mark_price_dec) if mark_price_dec else None,
                message="Successfully transformed BackpackRawPositionUpdate to DerivativePosition",
            )

        except Exception as e:
            logger.exception(
                "ws_position_update_transform_failed",
                symbol=getattr(raw_position_update, "symbol", None),
                raw_update=raw_position_update.model_dump() if raw_position_update else None,
                error=str(e),
                message="Failed to transform BackpackRawPositionUpdate to DerivativePosition",
            )
            raise DataTransformationError(
                source_model="BackpackRawPositionUpdate",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
                source_data=raw_position_update.model_dump() if raw_position_update else None,
            ) from e
        else:
            return position

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
