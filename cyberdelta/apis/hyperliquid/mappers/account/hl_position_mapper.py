"""Hyperliquid Position Mapper.

This mapper handles transformations for position-related data from the Hyperliquid exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- DerivativePosition transformations from clearinghouse state data
- Position data validation and processing
- WebSocket position update transformations
- Position-specific error handling and logging
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import (
    DataTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsPositionUpdateEvent,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import PositionMapperProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition, HyperliquidPositionDetails
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidPositionMapper(PositionMapperProtocol):
    """Focused mapper for Hyperliquid position data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw position models
    into CyberDeltaEngine Internal DerivativePosition Domain Models.
    """

    # Protocol method implementations (delegated to common utilities)
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

    # Protocol-specific methods
    @staticmethod
    def transform_raw_position_to_internal_static(
        raw_position: dict[str, object],
    ) -> DerivativePosition:
        """Transform raw position data to internal model (static protocol method).

        Args:
            raw_position: Raw position data from API

        Returns:
            DerivativePosition domain model
        """
        # Convert dict to validated model
        validated_position = HyperliquidRawAssetPosition.model_validate(raw_position)

        # Use static method for the actual transformation
        return HyperliquidPositionMapper.transform_raw_asset_position_to_internal(
            validated_position
        )

    @staticmethod
    def _validate_position_data(symbol: str, size: object, entry_price: object) -> None:
        """Validate position data for consistency.

        Args:
            symbol: Position symbol
            size: Position size
            entry_price: Entry price

        Raises:
            MissingRequiredFieldError: If entry price is invalid for non-zero position
            DataTransformationError: If types are invalid
        """
        # Ensure size is a Decimal
        if not isinstance(size, Decimal):
            raise DataTransformationError(
                source_model="position_data",
                target_model="position_size",
                reason=f"Expected Decimal for size, got {type(size).__name__}",
                source_data={"symbol": symbol, "size": size},
            )

        # Check if position is non-zero and validate entry price
        if size != Decimal(0) and entry_price is None:
            raise MissingRequiredFieldError(
                field_names="entry_price",
                context=f"Non-zero position {symbol}",
                source_data={"symbol": symbol, "size": str(size)},
            )

    @staticmethod
    def _validate_position_size(size: object, context: str) -> object:
        """Validate position size for valid types.

        Args:
            size: Position size to validate
            context: Context for error reporting

        Returns:
            object: The validated size value

        Raises:
            DataTransformationError: If size is not a valid type
        """
        if size is None:
            return size
        if not isinstance(size, Decimal):
            raise DataTransformationError(
                source_model="position_size",
                target_model="Decimal",
                reason=(
                    f"Invalid size type in {context}: expected Decimal, got {type(size).__name__}"
                ),
                source_data={"size": size, "context": context},
            )
        return size

    @staticmethod
    def _ensure_position_size_not_none(size: Decimal | None, position_info: object) -> Decimal:
        """Ensure position size is not None after parsing.

        Args:
            size: Size value to validate
            position_info: Position info for context

        Returns:
            Decimal: The validated non-None size value

        Raises:
            MissingRequiredFieldError: If size is None
        """
        if size is None:
            raise MissingRequiredFieldError(
                field_names="position_size",
                context="position_transformation",
                source_data=getattr(position_info, "__dict__", {}) if position_info else None,
            )
        return size

    @staticmethod
    def transform_raw_clearinghouse_state_to_derivative_positions(
        clearinghouse_data: HyperliquidRawClearinghouseState,
    ) -> dict[str, DerivativePosition]:
        """Transforms a HyperliquidRawClearinghouseState to Internal DerivativePosition models.

        Processes the clearinghouse state to extract derivative position information
        from asset positions.

        Args:
            clearinghouse_data: Validated raw clearinghouse state from Hyperliquid

        Returns:
            dict[str, DerivativePosition]: Dictionary mapping symbols to DerivativePosition models

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_clearinghouse_state_to_derivative_positions",
                has_asset_positions=hasattr(clearinghouse_data, "asset_positions"),
                asset_positions_count=(
                    len(clearinghouse_data.asset_positions)
                    if (
                        hasattr(clearinghouse_data, "asset_positions")
                        and clearinghouse_data.asset_positions
                    )
                    else 0
                ),
                message=(
                    "Transforming HyperliquidRawClearinghouseState to DerivativePosition models"
                ),
            )

            positions: dict[str, DerivativePosition] = {}

            # Extract asset positions from the raw state
            if (
                hasattr(clearinghouse_data, "asset_positions")
                and clearinghouse_data.asset_positions
            ):
                for position_data in clearinghouse_data.asset_positions:
                    HyperliquidPositionMapper._process_single_derivative_position(
                        position_data,
                        positions,
                    )

            logger.debug(
                "clearinghouse_state_to_derivative_positions_transformed",
                positions_count=len(positions),
                symbols=list(positions.keys()),
                message="Successfully transformed clearinghouse state to derivative positions",
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "clearinghouse_state_to_derivative_positions_transform_failed",
                has_asset_positions=(
                    hasattr(clearinghouse_data, "asset_positions") if clearinghouse_data else False
                ),
                error=str(e),
                message="Failed to transform clearinghouse state to derivative positions",
            )
            raise DataTransformationError(
                source_model="HyperliquidRawClearinghouseState",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
                source_data=clearinghouse_data.model_dump() if clearinghouse_data else None,
            ) from e
        else:
            return positions

    @staticmethod
    def _process_single_derivative_position(
        position_data: HyperliquidRawAssetPosition,
        positions: dict[str, DerivativePosition],
    ) -> None:
        """Process a single derivative position from asset positions.

        Extracts and validates position data, then creates a DerivativePosition
        if the position contains valid data.

        Args:
            position_data: Raw asset position data
            positions: Dictionary to populate with the processed position
        """
        if not hasattr(position_data, "position") or not position_data.position:
            return

        pos = position_data.position
        symbol = getattr(pos, "coin", None)

        if not symbol:
            return

        logger.debug(
            "processing_single_derivative_position",
            symbol=symbol,
            message="Processing single derivative position from asset position",
        )

        # Parse and validate position data
        size, entry_price = HyperliquidPositionMapper._parse_position_core_data(pos, symbol)

        if size is None:
            logger.debug(
                "single_derivative_position_skipped",
                symbol=symbol,
                reason="no_size_data",
                message="Skipping position - no size data available",
            )
            return  # Skip positions with no size data

        # Validate position data consistency
        HyperliquidPositionMapper._validate_position_data(symbol, size, entry_price)

        # For zero positions, entry price must be None per domain model rules
        if size == Decimal(0):
            entry_price = None

        # Create the derivative position
        position = HyperliquidPositionMapper._create_derivative_position(
            pos,
            symbol,
            size,
            entry_price,
        )
        positions[symbol] = position

        logger.debug(
            "single_derivative_position_processed",
            symbol=symbol,
            size=str(size),
            entry_price=str(entry_price) if entry_price else None,
            message="Successfully processed single derivative position",
        )

    @staticmethod
    def _parse_position_core_data(
        pos: HyperliquidRawPositionInfo,
        symbol: str,
    ) -> tuple[Decimal | None, Decimal | None]:
        """Parse core position data (size and entry price).

        Extracts and validates size and entry price from raw position data.

        Args:
            pos: Raw position info
            symbol: Position symbol for logging

        Returns:
            tuple[Decimal | None, Decimal | None]: Parsed size and entry price
        """
        # Parse position size
        size_str = getattr(pos, "szi", "0")
        size = parse_decimal_value(
            size_str,
            allow_none=False,
            field_name="position.szi",
        )

        # Parse entry price
        entry_price_str = pos.entry_px
        entry_price = None
        if entry_price_str and entry_price_str != "0":
            try:
                entry_price = parse_decimal_value(
                    entry_price_str,
                    allow_none=True,
                    field_name="position.entry_px",
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "entry_price_parse_failed",
                    symbol=symbol,
                    entry_price_str=entry_price_str,
                    error=str(e),
                    message="Failed to parse entry price, treating as None",
                )

        return size, entry_price

    @staticmethod
    def _create_derivative_position(
        pos: HyperliquidRawPositionInfo,
        symbol: str,
        size: Decimal,
        entry_price: Decimal | None,
    ) -> DerivativePosition:
        """Create a DerivativePosition from parsed data.

        Constructs a complete DerivativePosition model from validated position data.

        Args:
            pos: Raw position info
            symbol: Position symbol
            size: Validated position size
            entry_price: Validated entry price

        Returns:
            DerivativePosition: The constructed position model
        """
        # Parse unrealized PnL
        unrealized_pnl = parse_decimal_value(
            pos.unrealized_pnl or "0",
            allow_none=True,
            field_name="position.unrealized_pnl",
        )

        # Create HL-specific details
        details = HyperliquidPositionMapper._create_position_details(pos)

        # Parse liquidation price
        liquidation_price = parse_decimal_value(
            pos.liquidation_px,
            allow_none=True,
            field_name="position.liquidation_px",
        )

        # Determine side based on position size
        if size > Decimal(0):
            side = OrderSide.BUY
        elif size < Decimal(0):
            side = OrderSide.SELL
        else:  # size == 0, use a default (either is valid for zero positions)
            side = OrderSide.BUY

        # Use secure_transform for type-safe model creation
        position_data = {
            "exchange": ExchangeName.HYPERLIQUID.value,
            "symbol": symbol,
            "side": side.value,
            "size": str(size),
            "entry_price": str(entry_price) if entry_price is not None else None,
            "mark_price": None,  # Not available in this context
            "liquidation_price": str(liquidation_price) if liquidation_price is not None else None,
            "unrealized_pnl": str(unrealized_pnl) if unrealized_pnl is not None else None,
            "timestamp": datetime.now(UTC).isoformat(),
            "hl_details": details.model_dump() if details else None,
            "bp_details": None,
        }

        return secure_transform(
            data=position_data,
            model_class=DerivativePosition,
            context="hyperliquid_position_transform",
            source_exchange="hyperliquid",
        )

    @staticmethod
    def _create_position_details(pos: HyperliquidRawPositionInfo) -> HyperliquidPositionDetails:
        """Create HyperliquidPositionDetails from position data.

        Extracts leverage and margin information to create position details.

        Args:
            pos: Raw position info

        Returns:
            HyperliquidPositionDetails: The position details model
        """
        leverage_obj = pos.leverage
        max_leverage = pos.max_leverage or 1
        margin_used = parse_decimal_value(
            pos.margin_used,
            allow_none=True,
            field_name="position.margin_used",
        )

        # Extract leverage value from HyperliquidRawLeverage object
        leverage_value = 1  # Default
        leverage_type = "cross"  # Default
        if leverage_obj:
            leverage_value = leverage_obj.value or 1
            leverage_type = leverage_obj.type or "cross"

        return HyperliquidPositionDetails(
            leverage_type=leverage_type,
            leverage_value=int(leverage_value) if leverage_value else 1,
            max_leverage=int(max_leverage) if max_leverage else 1,
            margin_used=margin_used,
        )

    @staticmethod
    def transform_raw_asset_position_to_internal(
        raw_asset_position: HyperliquidRawAssetPosition,
    ) -> DerivativePosition:
        """Transform a HyperliquidRawAssetPosition to an Internal DerivativePosition model.

        Args:
            raw_asset_position: Raw asset position data from Hyperliquid

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            DataTransformationError: If transformation fails
        """
        if not hasattr(raw_asset_position, "position") or not raw_asset_position.position:
            raise DataTransformationError(
                source_model="HyperliquidRawAssetPosition",
                target_model="DerivativePosition",
                reason="No position data available",
                source_data=raw_asset_position.model_dump() if raw_asset_position else None,
            )

        pos = raw_asset_position.position
        symbol = getattr(pos, "coin", None)

        if not symbol:
            raise DataTransformationError(
                source_model="HyperliquidRawAssetPosition",
                target_model="DerivativePosition",
                reason="No symbol available in position data",
                source_data=raw_asset_position.model_dump() if raw_asset_position else None,
            )

        # Parse and validate position data
        size, entry_price = HyperliquidPositionMapper._parse_position_core_data(pos, symbol)

        if size is None:
            raise DataTransformationError(
                source_model="HyperliquidRawAssetPosition",
                target_model="DerivativePosition",
                reason="No position size available",
                source_data=raw_asset_position.model_dump() if raw_asset_position else None,
            )

        # Validate position data consistency
        HyperliquidPositionMapper._validate_position_data(symbol, size, entry_price)

        # For zero positions, entry price must be None per domain model rules
        if size == Decimal(0):
            entry_price = None

        # Create the derivative position
        return HyperliquidPositionMapper._create_derivative_position(
            pos,
            symbol,
            size,
            entry_price,
        )

    @staticmethod
    def _transform_raw_position_to_internal_impl(
        position_info: dict[str, Any] | object,
        symbol: str,
        timestamp: datetime,
    ) -> DerivativePosition:
        """Transforms raw position info to an Internal DerivativePosition model.

        Converts generic position information into a standardized DerivativePosition model.

        Args:
            position_info: Raw position info from Hyperliquid
            symbol: Asset symbol
            timestamp: Position timestamp

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_position_to_internal",
                symbol=symbol,
                timestamp=timestamp.isoformat(),
                message="Transforming raw position info to DerivativePosition",
            )

            # Parse position size
            size_str = getattr(position_info, "szi", "0")
            size = parse_decimal_value(size_str, allow_none=False, field_name="position.szi")

            # Validate position size
            HyperliquidPositionMapper._validate_position_size(size, "position transformation")

            # Parse entry price
            entry_price_str = getattr(position_info, "entry_px", None)
            entry_price = None
            if entry_price_str and entry_price_str != "0":
                try:
                    entry_price = parse_decimal_value(
                        entry_price_str,
                        allow_none=True,
                        field_name="position.entry_px",
                    )
                except (ValueError, TypeError) as e:
                    logger.warning(
                        "entry_price_parse_failed",
                        symbol=symbol,
                        entry_price_str=entry_price_str,
                        error=str(e),
                        message="Failed to parse entry price in raw position transformation",
                    )

            # Validate position data consistency
            HyperliquidPositionMapper._validate_position_data(symbol, size, entry_price)

            # For zero positions, entry price must be None per domain model rules
            if size == Decimal(0):
                entry_price = None

            # Parse unrealized PnL
            unrealized_pnl = parse_decimal_value(
                getattr(position_info, "unrealized_pnl", "0"),
                allow_none=True,
                field_name="position.unrealized_pnl",
            )

            # Create HL-specific details
            leverage_obj = getattr(position_info, "leverage", None)
            max_leverage = getattr(position_info, "max_leverage", 1)
            margin_used = parse_decimal_value(
                getattr(position_info, "margin_used", "0"),
                allow_none=True,
                field_name="position.margin_used",
            )

            # Extract leverage value from HyperliquidRawLeverage object
            leverage_value = 1  # Default
            leverage_type = "cross"  # Default
            if leverage_obj:
                leverage_value = getattr(leverage_obj, "value", 1) or 1
                leverage_type = getattr(leverage_obj, "type", "cross") or "cross"

            details = HyperliquidPositionDetails(
                leverage_type=leverage_type,
                leverage_value=int(leverage_value) if leverage_value else 1,
                max_leverage=int(max_leverage) if max_leverage else 1,
                margin_used=margin_used,
            )

            # Parse liquidation price
            liquidation_price = parse_decimal_value(
                getattr(position_info, "liquidation_px", None),
                allow_none=True,
                field_name="position.liquidation_px",
            )

            # Determine side based on position size
            # Ensure position size is not None after parsing and get validated value
            size = HyperliquidPositionMapper._ensure_position_size_not_none(size, position_info)

            if size > Decimal(0):
                side = OrderSide.BUY
            elif size < Decimal(0):
                side = OrderSide.SELL
            else:  # size == 0, use a default (either is valid for zero positions)
                side = OrderSide.BUY

            # Use secure_transform for type-safe model creation
            position_data = {
                "exchange": ExchangeName.HYPERLIQUID.value,
                "symbol": symbol,
                "side": side.value,
                "size": str(size),
                "entry_price": str(entry_price) if entry_price is not None else None,
                "mark_price": None,  # Not available in this context
                "liquidation_price": str(liquidation_price)
                if liquidation_price is not None
                else None,
                "unrealized_pnl": str(unrealized_pnl) if unrealized_pnl is not None else None,
                "timestamp": timestamp.isoformat(),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            position = secure_transform(
                data=position_data,
                model_class=DerivativePosition,
                context="hyperliquid_raw_position_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_position_to_internal_transformed",
                symbol=symbol,
                side=side.value,
                size=str(size),
                entry_price=str(entry_price) if entry_price else None,
                unrealized_pnl=str(unrealized_pnl) if unrealized_pnl else None,
                message="Successfully transformed raw position info to DerivativePosition",
            )

        except Exception as e:
            logger.exception(
                "raw_position_to_internal_transform_failed",
                symbol=symbol,
                timestamp=timestamp.isoformat() if timestamp else None,
                position_info=getattr(position_info, "__dict__", position_info)
                if position_info
                else None,
                error=str(e),
                message="Failed to transform raw position info to DerivativePosition",
            )
            raise DataTransformationError(
                source_model="raw position",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
                source_data=getattr(position_info, "__dict__", {}) if position_info else None,
            ) from e
        else:
            return position

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: HyperliquidRawWsPositionUpdateEvent,
    ) -> DerivativePosition:
        """Transforms a HyperliquidRawWsPositionUpdateEvent to an Internal DerivativePosition model.

        Converts WebSocket position update events into standardized DerivativePosition models.

        Args:
            raw_position_update: Validated raw position update event data from Hyperliquid WebSocket

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_ws_position_update",
                asset=raw_position_update.asset,
                time=raw_position_update.time,
                message="Transforming HyperliquidRawWsPositionUpdateEvent to DerivativePosition",
            )

            # Extract position info from the WebSocket event
            position_info = raw_position_update.position
            symbol = raw_position_update.asset
            timestamp = parse_datetime_utc(str(raw_position_update.time), field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Use the existing position transformation logic
            position = HyperliquidPositionMapper._transform_raw_position_to_internal_impl(
                position_info,
                symbol,
                timestamp,
            )

            logger.debug(
                "ws_position_update_transformed",
                asset=symbol,
                timestamp=timestamp.isoformat(),
                message="Successfully transformed WebSocket position update to DerivativePosition",
            )

        except Exception as e:
            logger.exception(
                "ws_position_update_transform_failed",
                asset=getattr(raw_position_update, "asset", None),
                time=getattr(raw_position_update, "time", None),
                raw_update=raw_position_update.model_dump() if raw_position_update else None,
                error=str(e),
                message="Failed to transform WebSocket position update to DerivativePosition",
            )
            raise DataTransformationError(
                source_model="HyperliquidRawWsPositionUpdateEvent",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
                source_data=raw_position_update.model_dump() if raw_position_update else None,
            ) from e
        else:
            return position
