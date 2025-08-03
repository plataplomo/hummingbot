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

from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPositionResponse,
    BackpackRawPositionUpdate,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import PositionMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import (
    CommonDataParserMixin,
    PositionMapperMixin,
    ValidationMixin,
)
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import BackpackPositionDetails, DerivativePosition
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackPositionMapper(
    CommonDataParserMixin,
    ValidationMixin,
    PositionMapperMixin,
    PositionMapperProtocol,
):
    """Focused mapper for Backpack position data transformations.

    This class contains static methods for transforming validated Backpack Raw position models
    into CyberDeltaEngine Internal DerivativePosition Domain Models.
    """

    def transform_raw_position_to_internal(
        self, raw: BackpackRawPositionResponse
    ) -> DerivativePosition:
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
            size_dec = self.parse_decimal_safely(raw.net_quantity)
            size_typed = self.ensure_decimal_not_none(size_dec, "size", "position_validation")

            # Determine side from size (positive = long/BUY, negative = short/SELL)
            side = OrderSide.BUY if size_typed >= 0 else OrderSide.SELL

            # Parse optional price fields
            entry_price_dec = self.parse_decimal_safely(raw.entry_price, default=None)
            mark_price_dec = self.parse_decimal_safely(raw.mark_price, default=None)
            liq_price_dec = self.parse_decimal_safely(raw.est_liquidation_price, default=None)
            unrealized_pnl_dec = self.parse_decimal_safely(raw.pnl_unrealized, default=None)
            realized_pnl_dec = self.parse_decimal_safely(raw.pnl_realized, default=None)

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
                imf_base_dec = self.parse_decimal_safely(raw.imf_function.base, default=None)
                imf_factor_dec = self.parse_decimal_safely(raw.imf_function.factor, default=None)

            if raw.mmf_function:
                mmf_base_dec = self.parse_decimal_safely(raw.mmf_function.base, default=None)
                mmf_factor_dec = self.parse_decimal_safely(raw.mmf_function.factor, default=None)

            # cumulative_funding_payment is always present
            cumulative_funding_dec = self.parse_decimal_safely(
                raw.cumulative_funding_payment, default=None
            )

            # Create BP-specific details
            bp_details = BackpackPositionDetails(
                imf_base=imf_base_dec,
                imf_factor=imf_factor_dec,
                mmf_base=mmf_base_dec,
                mmf_factor=mmf_factor_dec,
                cumulative_funding=cumulative_funding_dec,
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=raw.symbol,
            )

            # Use secure_transform for type-safe model creation
            position_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": exchange_symbol,  # Domain object!
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

    def transform_ws_position_update_to_internal_position(
        self,
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
            size_dec = self.parse_decimal_safely(raw_position_update.net_quantity, default=None)
            if size_dec is None:
                size_dec = Decimal(0)
            size_typed = self.ensure_decimal_not_none(size_dec, "size", "position_validation")

            # Determine side from size (positive = long/BUY, negative = short/SELL)
            side = OrderSide.BUY if size_typed >= 0 else OrderSide.SELL

            # Parse optional price fields
            entry_price_dec = self.parse_decimal_safely(
                raw_position_update.entry_price, default=None
            )
            mark_price_dec = self.parse_decimal_safely(raw_position_update.mark_price, default=None)
            liq_price_dec = self.parse_decimal_safely(
                raw_position_update.liquidation_price, default=None
            )
            # These fields don't exist in BackpackRawPositionUpdate
            unrealized_pnl_dec = None
            realized_pnl_dec = None

            # Parse timestamp - use event_time if available
            timestamp = self.parse_timestamp(raw_position_update.event_time)
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
                imf_base_dec = self.parse_decimal_safely(
                    raw_position_update.initial_margin_fraction, default=None
                )
                # No factor available in position update, only base value
                imf_factor_dec = None

            if raw_position_update.maintenance_margin_fraction is not None:
                mmf_base_dec = self.parse_decimal_safely(
                    raw_position_update.maintenance_margin_fraction, default=None
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

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_position_update.symbol,
            )

            # Use secure_transform for type-safe model creation
            position_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": exchange_symbol,  # Domain object!
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
