"""Backpack Account Summary Mapper.

This mapper handles transformations for account summary and settings data from the Backpack
exchange, extracted from the monolithic account data mapper to improve maintainability and
testability.

Focused on:
- MarginAccountSummary transformations from raw account summaries
- Enhanced account summary with collateral data
- Account settings transformations
- Account-related data validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal
from itertools import starmap

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.protocols.mapper_protocols import AccountSummaryMapperProtocol
from cyberdelta.apis.exceptions.data_transformation import DataTransformationError
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    AccountSettings,
    BackpackAccountSettingsDetails,
    BackpackMarginDetails,
    MarginAccountSummary,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackAccountSummaryMapper(AccountSummaryMapperProtocol):
    """Focused mapper for Backpack account summary and settings data transformations.

    This class contains static methods for transforming validated Backpack Raw account models
    into CyberDeltaEngine Internal Domain Models for account summaries and settings.
    """

    @staticmethod
    def transform_raw_account_summary_to_internal(
        raw_settings: BackpackRawAccountSummary,
        spot_balances_raw: dict[str, BackpackRawBalance],
        derivative_positions_raw: list[BackpackRawPosition],
    ) -> MarginAccountSummary:
        """Transform raw Backpack account data into an internal MarginAccountSummary.

        Creates a basic margin account summary using balance and position data,
        with calculated equity and notional values.

        Args:
            raw_settings: The validated BackpackRawAccountSummary Pydantic model
            spot_balances_raw: A dictionary of validated raw spot balances
            derivative_positions_raw: A list of validated raw derivative positions

        Returns:
            MarginAccountSummary: The corresponding internal MarginAccountSummary model

        Raises:
            DataTransformationError: If critical numeric fields cannot be parsed
        """
        try:
            logger.debug(
                "transforming_raw_account_summary",
                leverage_limit=raw_settings.leverage_limit,
                spot_balances_count=len(spot_balances_raw),
                positions_count=len(derivative_positions_raw),
                message="Transforming BackpackRawAccountSummary to MarginAccountSummary",
            )

            # Transform spot balances using the dedicated mapper
            internal_spot_balances = list(
                starmap(
                    BackpackBalanceMapper.transform_raw_balance_to_internal,
                    spot_balances_raw.items(),
                ),
            )

            # Transform derivative positions using the dedicated mapper
            internal_derivative_positions = [
                BackpackPositionMapper.transform_raw_position_to_internal(pos_raw)
                for pos_raw in derivative_positions_raw
            ]

            # Calculate equity values from USD-based spot balances
            calculated_total_equity = Decimal("0.0")
            calculated_available_equity = Decimal("0.0")
            calculated_total_position_notional = Decimal("0.0")
            calculated_total_unrealized_pnl = Decimal("0.0")
            calculated_assets_value_spot = Decimal("0.0")

            for sb in internal_spot_balances:
                if sb.asset.upper() in {"USD", "USDC", "USDT"}:
                    calculated_total_equity += sb.total_quantity
                    calculated_available_equity += sb.total_quantity
                    calculated_assets_value_spot += sb.total_quantity

            for dp in internal_derivative_positions:
                if dp.unrealized_pnl is not None:
                    calculated_total_unrealized_pnl += dp.unrealized_pnl
                if (dp.entry_price is not None) and (
                    dp.size.is_finite() and dp.entry_price.is_finite()
                ):
                    calculated_total_position_notional += abs(dp.size * dp.entry_price)

            calculated_total_equity += calculated_total_unrealized_pnl

            # Create BP-specific details with basic account data
            bp_details = BackpackMarginDetails(
                assets_value=calculated_assets_value_spot,
                borrow_liability=None,
                liabilities_value=None,
                locked_equity=None,
                margin_fraction=None,
                imf_raw=None,  # IMF should come from actual margin data, not leverage limit
                mmf_raw=None,
                leverage_limit=raw_settings.leverage_limit,
            )

            # Use secure_transform for type-safe model creation
            margin_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "timestamp": datetime.now(UTC).isoformat(),
                "total_equity": str(calculated_total_equity),
                "available_equity": str(calculated_available_equity),
                "total_initial_margin_required": None,
                "total_maintenance_margin_required": None,
                "total_position_notional": str(
                    calculated_total_position_notional
                    if internal_derivative_positions
                    else Decimal("0.0"),
                ),
                "total_unrealized_pnl": str(
                    calculated_total_unrealized_pnl
                    if internal_derivative_positions
                    else Decimal("0.0"),
                ),
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            summary = secure_transform(
                data=margin_data,
                model_class=MarginAccountSummary,
                context="backpack_account_summary_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_account_summary_transformed",
                total_equity=str(calculated_total_equity),
                available_equity=str(calculated_available_equity),
                position_notional=str(calculated_total_position_notional),
                unrealized_pnl=str(calculated_total_unrealized_pnl),
                message=(
                    "Successfully transformed BackpackRawAccountSummary to MarginAccountSummary"
                ),
            )

        except (ValidationError, TypeError, AttributeError, KeyError) as e:
            logger.exception(
                "raw_account_summary_transform_failed",
                leverage_limit=getattr(raw_settings, "leverage_limit", None),
                spot_balances_count=len(spot_balances_raw) if spot_balances_raw else 0,
                positions_count=len(derivative_positions_raw) if derivative_positions_raw else 0,
                error=str(e),
                message="Failed to transform BackpackRawAccountSummary to MarginAccountSummary",
            )
            raise DataTransformationError(
                source_model="BackpackRawAccountSummary",
                target_model="MarginAccountSummary",
                reason=str(e),
                original_error=e,
                source_data=raw_settings.model_dump() if raw_settings else None,
            ) from e
        else:
            return summary

    @staticmethod
    def transform_enhanced_account_data_to_margin_summary(
        raw_collateral: BackpackRawCollateralResponse,
        raw_settings: BackpackRawAccountSummary,
        raw_positions: list[BackpackRawPosition],
    ) -> MarginAccountSummary:
        """Transform enhanced collateral data into internal MarginAccountSummary.

        Uses comprehensive margin data from /api/v1/capital/collateral endpoint
        to create a detailed MarginAccountSummary with enhanced bp_details.

        Args:
            raw_collateral: Validated collateral response from Backpack API
            raw_settings: Validated account settings from Backpack API
            raw_positions: List of validated derivative positions

        Returns:
            MarginAccountSummary: Enhanced account summary with collateral data

        Raises:
            DataTransformationError: If critical data cannot be parsed
        """
        try:
            logger.debug(
                "transforming_enhanced_account_data",
                net_equity=raw_collateral.net_equity,
                net_equity_available=raw_collateral.net_equity_available,
                collateral_assets_count=(
                    len(raw_collateral.collateral) if raw_collateral.collateral else 0
                ),
                positions_count=len(raw_positions),
                message="Transforming enhanced collateral data to MarginAccountSummary",
            )

            # Parse core equity fields from collateral response
            total_equity = parse_decimal_value(
                raw_collateral.net_equity,
                allow_none=False,
                field_name="net_equity",
            )
            # total_equity is guaranteed to be Decimal (not None) due to allow_none=False

            # Use the exchange's net_equity_available which correctly accounts for locked margin
            # When auto-lending is active, the exchange correctly reports available equity
            # accounting for both lent funds and locked margin from open positions
            available_equity = parse_decimal_value(
                raw_collateral.net_equity_available,
                allow_none=False,
                field_name="net_equity_available",
            )
            # available_equity is guaranteed to be Decimal (not None) due to allow_none=False

            # Parse detailed collateral fields
            assets_value = parse_decimal_value(raw_collateral.assets_value, allow_none=True)
            liabilities_value = parse_decimal_value(
                raw_collateral.liabilities_value,
                allow_none=True,
            )
            locked_equity = parse_decimal_value(raw_collateral.net_equity_locked, allow_none=True)
            borrow_liability = parse_decimal_value(raw_collateral.borrow_liability, allow_none=True)
            unsettled_equity = parse_decimal_value(raw_collateral.unsettled_equity, allow_none=True)
            margin_fraction = parse_decimal_value(raw_collateral.margin_fraction, allow_none=True)
            net_exposure_futures = parse_decimal_value(
                raw_collateral.net_exposure_futures,
                allow_none=True,
            )

            # Parse margin factors
            imf_value = parse_decimal_value(raw_collateral.imf, allow_none=True)
            mmf_value = parse_decimal_value(raw_collateral.mmf, allow_none=True)

            # Parse unrealized PnL
            total_unrealized_pnl = parse_decimal_value(
                raw_collateral.pnl_unrealized,
                allow_none=True,
            )

            # Transform derivative positions using the dedicated mapper
            internal_derivative_positions = [
                BackpackPositionMapper.transform_raw_position_to_internal(pos_raw)
                for pos_raw in raw_positions
            ]

            # Calculate position notional from derivative positions
            calculated_total_position_notional = Decimal("0.0")
            calculated_total_unrealized_pnl_positions = Decimal("0.0")

            for dp in internal_derivative_positions:
                if dp.unrealized_pnl is not None:
                    calculated_total_unrealized_pnl_positions += dp.unrealized_pnl
                if (
                    dp.entry_price is not None
                    and dp.size.is_finite()
                    and dp.entry_price.is_finite()
                ):
                    calculated_total_position_notional += abs(dp.size * dp.entry_price)

            # Prepare collateral assets data for bp_details
            collateral_assets_data: list[dict[str, str]] = [
                {
                    "symbol": asset.symbol,
                    "total_quantity": str(asset.total_quantity),
                    "collateral_value": str(asset.collateral_value),
                    "collateral_weight": str(asset.collateral_weight),
                    "asset_mark_price": str(asset.asset_mark_price),
                }
                for asset in raw_collateral.collateral
            ]

            # Create enhanced BackpackMarginDetails
            bp_details = BackpackMarginDetails(
                assets_value=assets_value,
                liabilities_value=liabilities_value,
                locked_equity=locked_equity,
                borrow_liability=borrow_liability,
                unsettled_equity=unsettled_equity,
                margin_fraction=margin_fraction,
                net_exposure_futures=net_exposure_futures,
                imf_raw=str(raw_collateral.imf) if raw_collateral.imf else None,
                mmf_raw=str(raw_collateral.mmf) if raw_collateral.mmf else None,
                leverage_limit=raw_settings.leverage_limit,
                subaccount_id=None,  # Set if subaccount was used in request
                collateral_assets=collateral_assets_data,
                source_endpoint="collateral",
            )

            # Use secure_transform for type-safe model creation
            margin_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "timestamp": datetime.now(UTC).isoformat(),
                "total_equity": str(total_equity),
                "available_equity": str(available_equity),
                "total_initial_margin_required": str(imf_value) if imf_value is not None else None,
                "total_maintenance_margin_required": str(mmf_value)
                if mmf_value is not None
                else None,
                "total_position_notional": str(calculated_total_position_notional),
                "total_unrealized_pnl": (
                    str(total_unrealized_pnl) if total_unrealized_pnl is not None else None
                ),
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            summary = secure_transform(
                data=margin_data,
                model_class=MarginAccountSummary,
                context="backpack_enhanced_account_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "enhanced_account_data_transformed",
                total_equity=str(total_equity),
                available_equity=str(available_equity),
                assets_value=str(assets_value) if assets_value else None,
                margin_fraction=str(margin_fraction) if margin_fraction else None,
                imf=str(imf_value) if imf_value else None,
                mmf=str(mmf_value) if mmf_value else None,
                message="Successfully transformed enhanced collateral data to MarginAccountSummary",
            )

        except (ValidationError, TypeError, AttributeError, KeyError) as e:
            logger.exception(
                "enhanced_account_data_transform_failed",
                net_equity=getattr(raw_collateral, "net_equity", None),
                net_equity_available=getattr(raw_collateral, "net_equity_available", None),
                collateral_count=(
                    len(raw_collateral.collateral)
                    if raw_collateral and raw_collateral.collateral
                    else 0
                ),
                positions_count=len(raw_positions) if raw_positions else 0,
                error=str(e),
                message="Failed to transform enhanced collateral data to MarginAccountSummary",
            )
            raise DataTransformationError(
                source_model="BackpackRawCollateralResponse",
                target_model="MarginAccountSummary",
                reason=str(e),
                original_error=e,
                source_data=raw_collateral.model_dump() if raw_collateral else None,
            ) from e
        else:
            return summary

    @staticmethod
    def transform_account_settings_update_to_internal(
        args: UpdateAccountSettingsArgs,
        exchange_name: str,
    ) -> AccountSettings:
        """Transform updated account settings args to internal AccountSettings model.

        Converts account settings update arguments into an internal AccountSettings domain model.

        Args:
            args: The account settings update arguments that were applied
            exchange_name: Name of the exchange

        Returns:
            AccountSettings: Internal model representing the updated settings

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_account_settings_update",
                leverage_limit=args.leverage_limit,
                auto_borrow_settlements=args.auto_borrow_settlements,
                auto_lend=args.auto_lend,
                auto_realize_pnl=args.auto_realize_pnl,
                auto_repay_borrows=args.auto_repay_borrows,
                message="Transforming UpdateAccountSettingsArgs to AccountSettings",
            )

            # Create Backpack-specific details
            bp_details = BackpackAccountSettingsDetails(
                leverage_limit_raw=str(args.leverage_limit) if args.leverage_limit else None,
                source_endpoint="/api/v1/account",
            )

            # Use secure_transform for type-safe model creation
            settings_data = {
                "exchange": exchange_name,
                "timestamp": datetime.now(UTC).isoformat(),
                "leverage_limit": args.leverage_limit,
                "auto_borrow_settlements": args.auto_borrow_settlements,
                "auto_lend": args.auto_lend,
                "auto_realize_pnl": args.auto_realize_pnl,
                "auto_repay_borrows": args.auto_repay_borrows,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,  # Not applicable for Backpack
            }

            settings = secure_transform(
                data=settings_data,
                model_class=AccountSettings,
                context="backpack_account_settings_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "account_settings_update_transformed",
                leverage_limit=args.leverage_limit,
                auto_borrow_settlements=args.auto_borrow_settlements,
                auto_lend=args.auto_lend,
                auto_realize_pnl=args.auto_realize_pnl,
                auto_repay_borrows=args.auto_repay_borrows,
                message="Successfully transformed UpdateAccountSettingsArgs to AccountSettings",
            )

        except Exception as e:
            logger.exception(
                "account_settings_update_transform_failed",
                leverage_limit=getattr(args, "leverage_limit", None),
                error=str(e),
                message="Failed to transform UpdateAccountSettingsArgs to AccountSettings",
            )
            raise DataTransformationError(
                source_model="UpdateAccountSettingsArgs",
                target_model="AccountSettings",
                reason=str(e),
                original_error=e,
                source_data=args.model_dump() if args else None,
            ) from e
        else:
            return settings

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
