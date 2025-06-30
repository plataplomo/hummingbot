"""CyberDeltaEngine: Hyperliquid Account Data Mapper.

------------------------------------------------

This module provides the HyperliquidAccountDataMapper class for transforming
Hyperliquid Raw Account Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Balances (HyperliquidRawClearinghouseState) to Internal SpotBalance models
- Transform Raw Positions (HyperliquidRawPositionInfo) to Internal DerivativePosition models
- Transform Raw Account Summaries to Internal MarginAccountSummary models
- Transform Raw User Fills to Internal Trade models
- Transform WebSocket Account Data events to Internal models

All transformation methods follow the standard pattern:
- Take a validated Raw Pydantic Model as primary input
- Return fully populated Internal Domain Model with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsPositionUpdateEvent,
)
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    AccountSettings,
    DerivativePosition,
    HyperliquidMarginDetails,
    HyperliquidPositionDetails,
    HyperliquidSpotBalanceDetails,
    MarginAccountSummary,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.account_settings import HyperliquidAccountSettingsDetails
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    DataTransformationError,
    MissingRequiredFieldError,
    TradeTransformationError,
    UnknownEnumError,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidAccountDataMapper:
    """Domain-focused mapper for Hyperliquid account data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw models
    related to account data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _validate_side_value(hl_side: str) -> None:
        """Validate order side value.

        Args:
            hl_side: Raw side string from Hyperliquid

        Raises:
            UnknownEnumError: If side cannot be mapped
        """
        if hl_side not in {"B", "A"}:
            raise UnknownEnumError(
                enum_type="Hyperliquid order side", value=hl_side, valid_values=["B", "A"]
            )

    @staticmethod
    def _map_side_to_internal(hl_side: str) -> OrderSide:
        """Maps a Hyperliquid order side string to internal OrderSide enum.

        Args:
            hl_side: Raw side string from Hyperliquid ("B" or "A")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped

        """
        try:
            # Validate side value first
            HyperliquidAccountDataMapper._validate_side_value(hl_side)

            if hl_side == "B":
                return OrderSide.BUY
            if hl_side == "A":
                return OrderSide.SELL

        except Exception as e:
            if isinstance(e, TransformationError):
                raise
            raise UnknownEnumError(
                enum_type="Hyperliquid order side", value=hl_side, valid_values=["B", "A"]
            ) from e
        else:
            # This should not be reached due to validation above, but for type safety
            return OrderSide.BUY

    @staticmethod
    def transform_raw_clearinghouse_state_to_spot_balances(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> dict[str, SpotBalance]:
        """Transforms a HyperliquidRawClearinghouseState to Internal SpotBalance models.

        Args:
            raw_state: Validated raw clearinghouse state from Hyperliquid

        Returns:
            dict[str, SpotBalance]: Dictionary mapping asset symbols to SpotBalance models

        Raises:
            TransformationError: If transformation fails

        """
        try:
            spot_balances: dict[str, SpotBalance] = {}

            # Extract USDC balance from margin summary account value
            HyperliquidAccountDataMapper._process_usdc_balance(raw_state, spot_balances)

            # Check for other spot assets in asset positions
            HyperliquidAccountDataMapper._process_other_spot_assets(raw_state, spot_balances)

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "clearinghouse_state_to_spot_balances_transform_failed",
                component="HyperliquidAccountDataMapper",
                action="transform_clearinghouse_state_to_spot_balances",
                error=str(e),
            )
            raise DataTransformationError(
                source_model="HyperliquidRawClearinghouseState",
                target_model="SpotBalance",
                reason=str(e),
                original_error=e,
            ) from e
        else:
            return spot_balances

    @staticmethod
    def _process_usdc_balance(
        raw_state: HyperliquidRawClearinghouseState,
        spot_balances: dict[str, SpotBalance],
    ) -> None:
        """Process USDC balance from margin summary."""
        if not (hasattr(raw_state, "margin_summary") and raw_state.margin_summary):
            return

        total_usdc = parse_decimal_value(
            raw_state.margin_summary.account_value,
            allow_none=False,
            field_name="margin_summary.account_value",
        )

        available_usdc = parse_decimal_value(
            raw_state.withdrawable,
            allow_none=True,
            field_name="withdrawable",
        )

        if total_usdc is not None and total_usdc >= Decimal(0):
            # Create HL-specific details
            details = HyperliquidSpotBalanceDetails()

            # Use withdrawable as available, or total if withdrawable is None/invalid
            if available_usdc is None or available_usdc < Decimal(0):
                available_usdc = Decimal(0)
            elif available_usdc > total_usdc:
                available_usdc = total_usdc

            # Hyperliquid primarily uses USDC for spot balances
            # SECURITY FIX: Use secure_transform instead of direct instantiation
            balance_data = {
                "asset": "USDC",
                "exchange": ExchangeName.HYPERLIQUID.value,
                "total_quantity": str(total_usdc),
                "available_quantity": str(available_usdc),
                "timestamp": datetime.now(UTC).isoformat(),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            spot_balance = secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context="hyperliquid_usdc_balance_transform",
                source_exchange="hyperliquid",
            )

            spot_balances["USDC"] = spot_balance

    @staticmethod
    def _process_other_spot_assets(
        raw_state: HyperliquidRawClearinghouseState,
        spot_balances: dict[str, SpotBalance],
    ) -> None:
        """Process other spot assets from asset positions."""
        if not (hasattr(raw_state, "asset_positions") and raw_state.asset_positions):
            return

        for asset_pos in raw_state.asset_positions:
            asset_name = asset_pos.asset

            # Skip USDC as it's handled above, and skip obvious perps
            if asset_name is None or asset_name == "USDC" or "-PERP" in asset_name.upper():
                continue

            HyperliquidAccountDataMapper._process_single_spot_asset(
                asset_pos,
                asset_name,
                spot_balances,
            )

    @staticmethod
    def _process_single_spot_asset(
        asset_pos: HyperliquidRawAssetPosition,
        asset_name: str,
        spot_balances: dict[str, SpotBalance],
    ) -> None:
        """Process a single spot asset position."""
        # Process potential spot assets
        if not (hasattr(asset_pos, "position") and asset_pos.position):
            return

        pos = asset_pos.position
        size_str = getattr(pos, "szi", "0")
        size = parse_decimal_value(
            size_str,
            allow_none=True,
            field_name=f"asset_positions.{asset_name}.szi",
        )

        if size is not None and size >= Decimal(0):
            # Create HL-specific details
            details = HyperliquidSpotBalanceDetails()

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            balance_data = {
                "asset": asset_name,
                "exchange": ExchangeName.HYPERLIQUID.value,
                "total_quantity": str(size),
                "available_quantity": str(size),  # Assume all available for spot
                "timestamp": datetime.now(UTC).isoformat(),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            spot_balance = secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context="hyperliquid_spot_asset_transform",
                source_exchange="hyperliquid",
            )

            spot_balances[asset_name] = spot_balance

    @staticmethod
    def transform_raw_clearinghouse_state_to_derivative_positions(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> dict[str, DerivativePosition]:
        """Transforms a HyperliquidRawClearinghouseState to Internal DerivativePosition models.

        Args:
            raw_state: Validated raw clearinghouse state from Hyperliquid

        Returns:
            dict[str, DerivativePosition]: Dictionary mapping symbols to DerivativePosition models

        Raises:
            TransformationError: If transformation fails

        """
        try:
            positions: dict[str, DerivativePosition] = {}

            # Extract asset positions from the raw state
            if hasattr(raw_state, "asset_positions") and raw_state.asset_positions:
                for position_data in raw_state.asset_positions:
                    HyperliquidAccountDataMapper._process_single_derivative_position(
                        position_data,
                        positions,
                    )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "clearinghouse_state_to_derivative_positions_transform_failed",
                component="HyperliquidAccountDataMapper",
                action="transform_clearinghouse_state_to_derivative_positions",
                error=str(e),
            )
            raise DataTransformationError(
                source_model="HyperliquidRawClearinghouseState",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
            ) from e
        else:
            return positions

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
        if size != Decimal(0):
            if entry_price is None:
                raise MissingRequiredFieldError("entry_price", f"non-zero position {symbol}")
            if not isinstance(entry_price, Decimal):
                raise DataTransformationError(
                    source_model="position_data",
                    target_model="entry_price",
                    reason=f"Expected Decimal for entry_price, got {type(entry_price).__name__}",
                    source_data={"symbol": symbol, "entry_price": entry_price},
                )
            if entry_price <= Decimal(0):
                raise MissingRequiredFieldError("entry_price", f"non-zero position {symbol}")

    @staticmethod
    def _process_single_derivative_position(
        position_data: HyperliquidRawAssetPosition,
        positions: dict[str, DerivativePosition],
    ) -> None:
        """Process a single derivative position from asset positions."""
        if not hasattr(position_data, "position") or not position_data.position:
            return

        pos = position_data.position
        symbol = getattr(pos, "coin", None)

        if not symbol:
            return

        # Parse and validate position data
        size, entry_price = HyperliquidAccountDataMapper._parse_position_core_data(pos, symbol)

        if size is None:
            return  # Skip positions with no size data

        # Validate position data consistency
        HyperliquidAccountDataMapper._validate_position_data(symbol, size, entry_price)

        # For zero positions, entry price must be None per domain model rules
        if size == Decimal(0):
            entry_price = None

        # Create the derivative position
        position = HyperliquidAccountDataMapper._create_derivative_position(
            pos,
            symbol,
            size,
            entry_price,
        )
        positions[symbol] = position

    @staticmethod
    def _parse_position_core_data(
        pos: HyperliquidRawPositionInfo,
        symbol: str,
    ) -> tuple[Decimal | None, Decimal | None]:
        """Parse core position data (size and entry price)."""
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
                )

        return size, entry_price

    @staticmethod
    def _create_derivative_position(
        pos: HyperliquidRawPositionInfo,
        symbol: str,
        size: Decimal,
        entry_price: Decimal | None,
    ) -> DerivativePosition:
        """Create a DerivativePosition from parsed data."""
        # Parse unrealized PnL
        unrealized_pnl = parse_decimal_value(
            pos.unrealized_pnl or "0",
            allow_none=True,
            field_name="position.unrealized_pnl",
        )

        # Create HL-specific details
        details = HyperliquidAccountDataMapper._create_position_details(pos)

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

        # SECURITY FIX: Use secure_transform instead of direct instantiation
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
        """Create HyperliquidPositionDetails from position data."""
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
    def _validate_margin_summary_data(
        margin_summary: object, context: str
    ) -> HyperliquidRawMarginSummary:
        """Validate margin summary data.

        Args:
            margin_summary: Margin summary object
            context: Context for error messages

        Returns:
            HyperliquidRawMarginSummary: Validated margin summary

        Raises:
            MissingRequiredFieldError: If margin summary is missing
            DataTransformationError: If margin summary is not the expected type
        """
        if not margin_summary:
            raise MissingRequiredFieldError("margin_summary", context)

        if not isinstance(margin_summary, HyperliquidRawMarginSummary):
            raise DataTransformationError(
                source_model="margin_summary",
                target_model="HyperliquidRawMarginSummary",
                reason=f"Expected HyperliquidRawMarginSummary, got {type(margin_summary).__name__}",
                source_data=margin_summary,
            )

        return margin_summary

    @staticmethod
    def _validate_required_margin_fields(
        account_value: object, total_margin_used: object, context: str
    ) -> tuple[object, object]:
        """Validate required margin fields.

        Args:
            account_value: Account value
            total_margin_used: Total margin used
            context: Context for error messages

        Returns:
            tuple[object, object]: Validated values

        Raises:
            MissingRequiredFieldError: If required fields are missing
        """
        missing_fields: list[str] = []
        if account_value is None:
            missing_fields.append("account_value")
        if total_margin_used is None:
            missing_fields.append("total_margin_used")

        if missing_fields:
            raise MissingRequiredFieldError(missing_fields, context)

        return account_value, total_margin_used

    @staticmethod
    def _validate_maintenance_margin_fields(
        cross_mmr: object, withdrawable: object, context: str
    ) -> tuple[object, object]:
        """Validate maintenance margin fields.

        Args:
            cross_mmr: Cross maintenance margin
            withdrawable: Withdrawable amount
            context: Context for error messages

        Returns:
            tuple[object, object]: Validated values

        Raises:
            MissingRequiredFieldError: If required fields are missing
        """
        missing_fields: list[str] = []
        if cross_mmr is None:
            missing_fields.append("cross_maintenance_margin_used")
        if withdrawable is None:
            missing_fields.append("withdrawable")

        if missing_fields:
            raise MissingRequiredFieldError(missing_fields, context)

        return cross_mmr, withdrawable

    @staticmethod
    def _ensure_cross_mmr_not_none(cross_mmr: object, source_data: object) -> None:
        """Ensure cross maintenance margin is not None after parsing.

        Args:
            cross_mmr: Parsed cross maintenance margin
            source_data: Source data for error context

        Raises:
            DataTransformationError: If cross_mmr is None
        """
        if cross_mmr is None:
            raise DataTransformationError(
                source_model="cross_maintenance_margin_used",
                target_model="Decimal",
                reason="cross_mmr should not be None after parsing with allow_none=False",
                source_data=source_data,
            )

    @staticmethod
    def _ensure_position_size_not_none(size: object, position_info: object) -> None:
        """Ensure position size is not None after parsing.

        Args:
            size: Parsed position size
            position_info: Source position info for error context

        Raises:
            DataTransformationError: If size is None
        """
        if size is None:
            raise DataTransformationError(
                source_model="position.szi",
                target_model="Decimal",
                reason="size should not be None after parsing with allow_none=False",
                source_data=getattr(position_info, "szi", "0"),
            )

    @staticmethod
    def transform_raw_clearinghouse_state_to_margin_summary(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> MarginAccountSummary:
        """Transforms a HyperliquidRawClearinghouseState to an Internal MarginAccountSummary model.

        Args:
            raw_state: Validated raw clearinghouse state from Hyperliquid

        Returns:
            MarginAccountSummary: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse margin summary data
            margin_summary = getattr(raw_state, "margin_summary", None)

            # Validate margin summary data
            margin_summary = HyperliquidAccountDataMapper._validate_margin_summary_data(
                margin_summary, "clearinghouse state"
            )

            # Parse core margin fields
            account_value = parse_decimal_value(
                margin_summary.account_value,
                allow_none=False,
                field_name="marginSummary.account_value",
            )

            total_margin_used = parse_decimal_value(
                margin_summary.total_margin_used,
                allow_none=False,
                field_name="marginSummary.total_margin_used",
            )

            # Parse additional fields for completeness
            total_ntl_pos = parse_decimal_value(
                margin_summary.total_ntl_pos,
                allow_none=True,
                field_name="marginSummary.total_ntl_pos",
            )

            # total_raw_usd not needed for MarginAccountSummary

            # Validate required margin fields
            HyperliquidAccountDataMapper._validate_required_margin_fields(
                account_value, total_margin_used, "margin summary"
            )

            # Parse maintenance margin fields from the clearinghouse state
            cross_mmr = parse_decimal_value(
                raw_state.cross_maintenance_margin_used,
                allow_none=False,
                field_name="cross_maintenance_margin_used",
            )
            # Parse isolated maintenance margin (optional field)
            isolated_mmr = parse_decimal_value(
                raw_state.isolated_maintenance_margin_used,
                allow_none=True,
                field_name="isolated_maintenance_margin_used",
            )

            # Skip individual cross_mmr validation here since we'll validate together

            # Calculate total maintenance margin
            # If isolated margin is not provided, use only cross margin
            # Type assertion: parse_decimal_value with allow_none=False guarantees non-None result
            HyperliquidAccountDataMapper._ensure_cross_mmr_not_none(
                cross_mmr, raw_state.cross_maintenance_margin_used
            )
            if cross_mmr is None:
                raise DataTransformationError(
                    source_model="cross_maintenance_margin_used",
                    target_model="Decimal",
                    reason="cross_mmr should not be None after parsing with allow_none=False",
                    source_data=raw_state.cross_maintenance_margin_used,
                )

            total_maintenance_margin = cross_mmr + (
                isolated_mmr if isolated_mmr is not None else Decimal(0)
            )

            # Calculate available margin (withdrawable from raw state)
            withdrawable = parse_decimal_value(
                raw_state.withdrawable,
                allow_none=False,
                field_name="withdrawable",
            )

            # Validate maintenance margin fields
            HyperliquidAccountDataMapper._validate_maintenance_margin_fields(
                cross_mmr, withdrawable, "margin summary"
            )

            # Calculate total unrealized PnL from derivative positions
            mapper = HyperliquidAccountDataMapper
            derivative_positions = mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                raw_state,
            )
            total_unrealized_pnl = Decimal(0)
            for position in derivative_positions.values():
                if position.unrealized_pnl is not None and position.unrealized_pnl.is_finite():
                    total_unrealized_pnl += position.unrealized_pnl

            # Create HL-specific details with required fields
            details = HyperliquidMarginDetails(
                cross_maintenance_margin_used=cross_mmr,
                isolated_maintenance_margin_used=isolated_mmr
                if isolated_mmr is not None
                else Decimal(0),
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            margin_data = {
                "exchange": ExchangeName.HYPERLIQUID.value,
                "timestamp": datetime.now(UTC).isoformat(),
                "total_equity": str(account_value),
                "available_equity": str(withdrawable),
                "total_initial_margin_required": str(total_margin_used),
                "total_maintenance_margin_required": str(total_maintenance_margin),
                "total_position_notional": str(total_ntl_pos)
                if total_ntl_pos is not None
                else None,
                "total_unrealized_pnl": str(total_unrealized_pnl),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=margin_data,
                model_class=MarginAccountSummary,
                context="hyperliquid_margin_summary_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise DataTransformationError(
                source_model="HyperliquidRawClearinghouseState",
                target_model="MarginAccountSummary",
                reason=str(e),
                original_error=e,
            ) from e

    @staticmethod
    def transform_raw_user_fill_to_internal(raw_fill: HyperliquidRawUserFill) -> Trade:
        """Transforms a HyperliquidRawUserFill to an Internal Trade model.

        Args:
            raw_fill: Validated raw user fill from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Map side
            side = HyperliquidAccountDataMapper._map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_fill.px, allow_none=False, field_name="px")
            quantity = parse_decimal_value(raw_fill.sz, allow_none=False, field_name="sz")

            # Validate trade data
            HyperliquidAccountDataMapper._validate_trade_data(
                price, quantity, "HyperliquidRawUserFill"
            )

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_fill.time, field_name="time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(
                getattr(raw_fill, "fee", "0"),
                allow_none=True,
                field_name="fee",
            ) or Decimal(0)

            # Create HL-specific details
            trade_hash = getattr(raw_fill, "hash", None)
            if trade_hash is None:
                trade_hash = f"unknown_hash_{raw_fill.time}_{raw_fill.coin}"

            details = HyperliquidTradeDetails(
                trade_hash=str(trade_hash),
                liquidation_mark_px=parse_decimal_value(
                    getattr(raw_fill, "liquidationMarkPx", None),
                    allow_none=True,
                    field_name="liquidationMarkPx",
                ),
                start_position=parse_decimal_value(
                    getattr(raw_fill, "startPosition", None),
                    allow_none=True,
                    field_name="startPosition",
                ),
                dir=getattr(raw_fill, "dir", None),
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            trade_data = {
                "id": str(getattr(raw_fill, "hash", f"fill_{raw_fill.time}_{raw_fill.coin}")),
                "symbol": raw_fill.coin,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": str(getattr(raw_fill, "oid", "unknown")),
                "exchange": ExchangeName.HYPERLIQUID.value,
                # "client_order_id" not set - will use default UUID generation
                "price": str(price),
                "quantity": str(quantity),
                "fee": str(fee),
                "fee_asset": raw_fill.coin,  # Fee asset is the traded symbol
                "is_maker": getattr(raw_fill, "is_maker", None),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_user_fill_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise TradeTransformationError(
                trade_source="HyperliquidRawUserFill",
                reason=str(e),
                symbol=raw_fill.coin,
                original_error=e,
            ) from e

    @staticmethod
    def _validate_trade_data(
        price: object, quantity: object, context: str
    ) -> tuple[object, object]:
        """Validate trade price and quantity data.

        Args:
            price: Raw price value
            quantity: Raw quantity value
            context: Context for error messages

        Returns:
            tuple[object, object]: Validated price and quantity

        Raises:
            MissingRequiredFieldError: If required fields are missing
        """
        missing_fields: list[str] = []
        if price is None:
            missing_fields.append("price")
        if quantity is None:
            missing_fields.append("quantity")

        if missing_fields:
            raise MissingRequiredFieldError(missing_fields, context)

        return price, quantity

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: HyperliquidRawFill) -> Trade:
        """Transforms a HyperliquidRawFill to an Internal Trade model.

        Args:
            raw_fill: Validated raw fill from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Map side
            side = HyperliquidAccountDataMapper._map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_fill.px, allow_none=False, field_name="px")
            quantity = parse_decimal_value(raw_fill.sz, allow_none=False, field_name="sz")

            # Validate trade data
            HyperliquidAccountDataMapper._validate_trade_data(price, quantity, "HyperliquidRawFill")

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_fill.time, field_name="time")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(
                getattr(raw_fill, "fee", "0"),
                allow_none=True,
                field_name="fee",
            ) or Decimal(0)

            # Create HL-specific details
            details = HyperliquidTradeDetails(
                trade_hash=raw_fill.hash,
                liquidation_mark_px=parse_decimal_value(
                    getattr(raw_fill, "liquidation_mark_px", None),
                    allow_none=True,
                    field_name="liquidationMarkPx",
                ),
                start_position=parse_decimal_value(
                    getattr(raw_fill, "start_position", None),
                    allow_none=True,
                    field_name="startPosition",
                ),
                dir=getattr(raw_fill, "dir", None),
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            trade_data = {
                "id": str(raw_fill.tid),
                "symbol": raw_fill.coin,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": str(raw_fill.oid),
                "exchange": ExchangeName.HYPERLIQUID.value,
                "client_order_id": raw_fill.cloid or None,
                "price": str(price),
                "quantity": str(quantity),
                "fee": str(fee),
                "fee_asset": raw_fill.coin,  # Fee asset is the traded symbol
                "is_maker": raw_fill.is_maker,
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_fill_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise TradeTransformationError(
                trade_source="HyperliquidRawFill",
                reason=str(e),
                symbol=raw_fill.coin,
                trade_id=str(raw_fill.tid),
                original_error=e,
            ) from e

    @staticmethod
    def transform_ws_fill_event_to_internal(raw_fill: HyperliquidRawWsFillEvent) -> Trade:
        """Transforms a WebSocket fill event to an Internal Trade model.

        Args:
            raw_fill: Validated raw WebSocket fill event from Hyperliquid

        Returns:
            Trade: Internal domain model with HL details populated

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Map side
            side = HyperliquidAccountDataMapper._map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_fill.px, allow_none=False, field_name="px")
            quantity = parse_decimal_value(raw_fill.sz, allow_none=False, field_name="sz")

            # Validate trade data
            HyperliquidAccountDataMapper._validate_trade_data(
                price, quantity, "HyperliquidRawWsFillEvent"
            )

            # Parse timestamp (convert from milliseconds)
            executed_at = datetime.fromtimestamp(raw_fill.time / 1000, tz=UTC)

            # Create HL-specific details
            details = HyperliquidTradeDetails(
                trade_hash=raw_fill.hash,
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            trade_data = {
                "id": raw_fill.hash,
                "symbol": raw_fill.coin,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": str(raw_fill.oid),
                "exchange": ExchangeName.HYPERLIQUID.value,
                "client_order_id": raw_fill.cloid,
                "price": str(price),
                "quantity": str(quantity),
                "fee": "0",  # Fee not available in WS fill events
                "fee_asset": None,
                "is_maker": raw_fill.is_maker,
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="hyperliquid_ws_fill_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise TradeTransformationError(
                trade_source="HyperliquidRawWsFillEvent",
                reason=str(e),
                symbol=raw_fill.coin,
                trade_id=raw_fill.hash,
                original_error=e,
            ) from e

    @staticmethod
    def _validate_position_size(size: object, context: str) -> object:
        """Validate position size.

        Args:
            size: Position size value
            context: Context for error messages

        Returns:
            object: Validated size

        Raises:
            MissingRequiredFieldError: If size is missing
        """
        if size is None:
            raise MissingRequiredFieldError("size", context)
        return size

    @staticmethod
    def transform_raw_position_to_internal(
        position_info: dict[str, Any] | object,
        symbol: str,
        timestamp: datetime,
    ) -> DerivativePosition:
        """Transforms raw position info to an Internal DerivativePosition model.

        Args:
            position_info: Raw position info from Hyperliquid
            symbol: Asset symbol
            timestamp: Position timestamp

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse position size
            size_str = getattr(position_info, "szi", "0")
            size = parse_decimal_value(size_str, allow_none=False, field_name="position.szi")

            # Validate position size
            HyperliquidAccountDataMapper._validate_position_size(size, "position transformation")

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
                    )

            # Validate position data consistency
            HyperliquidAccountDataMapper._validate_position_data(symbol, size, entry_price)

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
            # Type assertion: parse_decimal_value with allow_none=False guarantees non-None result
            HyperliquidAccountDataMapper._ensure_position_size_not_none(size, position_info)
            if size is None:
                raise DataTransformationError(
                    source_model="HyperliquidRawPositionInfo.szi",
                    target_model="Decimal",
                    reason="size should not be None after parsing with allow_none=False",
                    source_data=position_info,
                )

            if size > Decimal(0):
                side = OrderSide.BUY
            elif size < Decimal(0):
                side = OrderSide.SELL
            else:  # size == 0, use a default (either is valid for zero positions)
                side = OrderSide.BUY

            # SECURITY FIX: Use secure_transform instead of direct instantiation
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

            return secure_transform(
                data=position_data,
                model_class=DerivativePosition,
                context="hyperliquid_raw_position_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise DataTransformationError(
                source_model="raw position",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
            ) from e

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: HyperliquidRawWsPositionUpdateEvent,
    ) -> DerivativePosition:
        """Transforms a HyperliquidRawWsPositionUpdateEvent to an Internal DerivativePosition model.

        Args:
            raw_position_update: Validated raw position update event data from Hyperliquid WebSocket

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Extract position info from the WebSocket event
            position_info = raw_position_update.position
            symbol = raw_position_update.asset
            timestamp = parse_datetime_utc(str(raw_position_update.time), field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Use the existing position transformation logic
            return HyperliquidAccountDataMapper.transform_raw_position_to_internal(
                position_info,
                symbol,
                timestamp,
            )

        except Exception as e:
            raise DataTransformationError(
                source_model="HyperliquidRawWsPositionUpdateEvent",
                target_model="DerivativePosition",
                reason=str(e),
                original_error=e,
            ) from e

    @staticmethod
    def transform_account_settings_update_to_internal(
        args: "UpdateAccountSettingsArgs",
        exchange_name: str,
        asset_leverage_settings: dict[int, int] | None = None,
    ) -> "AccountSettings":
        """Transform account settings update args to internal AccountSettings model.

        Args:
            args: The account settings update arguments
            exchange_name: Name of the exchange
            asset_leverage_settings: Optional dict mapping asset indices to leverage values

        Returns:
            AccountSettings: Internal model representing the updated settings

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Create Hyperliquid-specific details
            hl_details = HyperliquidAccountSettingsDetails(
                asset_leverage_settings=asset_leverage_settings,
                cross_margin_enabled=True,  # Default to cross margin
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            settings_data = {
                "exchange": exchange_name,
                "timestamp": datetime.now(UTC).isoformat(),
                # This serves as the "default" for new positions
                "leverage_limit": args.leverage_limit,
                "auto_borrow_settlements": None,  # Not supported by Hyperliquid
                "auto_lend": None,  # Not supported by Hyperliquid
                "auto_realize_pnl": None,  # Not supported by Hyperliquid
                "auto_repay_borrows": None,  # Not supported by Hyperliquid
                "hl_details": hl_details.model_dump() if hl_details else None,
                "bp_details": None,
            }

            return secure_transform(
                data=settings_data,
                model_class=AccountSettings,
                context="hyperliquid_account_settings_transform",
                source_exchange="hyperliquid",
            )

        except Exception as e:
            raise DataTransformationError(
                source_model="UpdateAccountSettingsArgs",
                target_model="AccountSettings",
                reason=str(e),
                original_error=e,
            ) from e
