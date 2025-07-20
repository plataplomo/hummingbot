"""Hyperliquid Account Summary Mapper.

This mapper handles transformations for account summary and settings data from the
Hyperliquid exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- MarginAccountSummary transformations from clearinghouse state data
- Account settings transformations
- Margin data validation and processing
- Account-related data validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.exceptions import (
    DataTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_position_mapper import (
    HyperliquidPositionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
    HyperliquidRawMarginSummary,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
)
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    AccountSettings,
    HyperliquidMarginDetails,
    MarginAccountSummary,
)
from cyberdelta.core.models.account_settings import HyperliquidAccountSettingsDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidAccountSummaryMapper(AccountSummaryMapperProtocol):
    """Focused mapper for Hyperliquid account summary and settings data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw account models
    into CyberDeltaEngine Internal Domain Models for account summaries and settings.
    """

    # Protocol method implementations - delegate to common utilities
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

    # Protocol-specific method from AccountSummaryMapperProtocol
    @staticmethod
    def transform_raw_summary_to_internal(
        raw_summary: HyperliquidRawClearinghouseState,
    ) -> MarginAccountSummary:
        """Transform raw account summary data to internal model.

        Args:
            raw_summary: Raw clearinghouse state from API (contains margin summary)

        Returns:
            MarginAccountSummary domain model
        """
        # Delegate to existing method that matches git history business logic
        return HyperliquidAccountSummaryMapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_summary
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
    def _ensure_cross_mmr_not_none(cross_mmr: Decimal | None, source_data: object) -> Decimal:
        """Ensure cross maintenance margin is not None after parsing.

        Args:
            cross_mmr: Parsed cross maintenance margin
            source_data: Source data for error context

        Returns:
            Decimal: The validated non-None cross_mmr

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
        return cross_mmr

    @staticmethod
    def transform_raw_clearinghouse_state_to_margin_summary(
        clearinghouse_data: HyperliquidRawClearinghouseState,
    ) -> MarginAccountSummary:
        """Transforms a HyperliquidRawClearinghouseState to an Internal MarginAccountSummary model.

        Processes the clearinghouse state to extract margin summary information including
        equity, margin requirements, and position notional values.

        Args:
            clearinghouse_data: Validated raw clearinghouse state from Hyperliquid

        Returns:
            MarginAccountSummary: Internal domain model with HL details populated

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_clearinghouse_state_to_margin_summary",
                has_margin_summary=True,  # margin_summary is a required field
                has_cross_mmr=True,  # cross_maintenance_margin_used is a required field
                has_withdrawable=True,  # withdrawable is a required field
                message="Transforming HyperliquidRawClearinghouseState to MarginAccountSummary",
            )

            # Parse margin summary data
            margin_summary = getattr(clearinghouse_data, "margin_summary", None)

            # Validate margin summary data
            margin_summary = HyperliquidAccountSummaryMapper._validate_margin_summary_data(
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

            # Validate required margin fields
            HyperliquidAccountSummaryMapper._validate_required_margin_fields(
                account_value, total_margin_used, "margin summary"
            )

            # Parse maintenance margin fields from the clearinghouse state
            cross_mmr = parse_decimal_value(
                clearinghouse_data.cross_maintenance_margin_used,
                allow_none=False,
                field_name="cross_maintenance_margin_used",
            )
            # Parse isolated maintenance margin (optional field)
            isolated_mmr = parse_decimal_value(
                clearinghouse_data.isolated_maintenance_margin_used,
                allow_none=True,
                field_name="isolated_maintenance_margin_used",
            )

            # Calculate total maintenance margin
            # If isolated margin is not provided, use only cross margin
            # Ensure cross_mmr is not None after parsing and get validated value
            cross_mmr = HyperliquidAccountSummaryMapper._ensure_cross_mmr_not_none(
                cross_mmr, clearinghouse_data.cross_maintenance_margin_used
            )

            total_maintenance_margin = cross_mmr + (
                isolated_mmr if isolated_mmr is not None else Decimal(0)
            )

            # Calculate available margin (withdrawable from raw state)
            withdrawable = parse_decimal_value(
                clearinghouse_data.withdrawable,
                allow_none=False,
                field_name="withdrawable",
            )

            # Validate maintenance margin fields
            HyperliquidAccountSummaryMapper._validate_maintenance_margin_fields(
                cross_mmr, withdrawable, "margin summary"
            )

            # Calculate total unrealized PnL from derivative positions
            derivative_positions = (
                HyperliquidPositionMapper.transform_raw_clearinghouse_state_to_derivative_positions(
                    clearinghouse_data,
                )
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

            # Use secure_transform for type-safe model creation
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

            summary = secure_transform(
                data=margin_data,
                model_class=MarginAccountSummary,
                context="hyperliquid_margin_summary_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "clearinghouse_state_to_margin_summary_transformed",
                total_equity=str(account_value),
                available_equity=str(withdrawable),
                total_initial_margin_required=str(total_margin_used),
                total_maintenance_margin_required=str(total_maintenance_margin),
                total_position_notional=str(total_ntl_pos) if total_ntl_pos else None,
                total_unrealized_pnl=str(total_unrealized_pnl),
                message="Successfully transformed clearinghouse state to MarginAccountSummary",
            )

        except Exception as e:
            logger.exception(
                "clearinghouse_state_to_margin_summary_transform_failed",
                # clearinghouse_data should not be None in this context
                has_margin_summary=True,
                # clearinghouse_data should not be None in this context
                has_cross_mmr=True,
                clearinghouse_data=clearinghouse_data.model_dump() if clearinghouse_data else None,
                error=str(e),
                message="Failed to transform clearinghouse state to MarginAccountSummary",
            )
            raise DataTransformationError(
                source_model="HyperliquidRawClearinghouseState",
                target_model="MarginAccountSummary",
                reason=str(e),
                original_error=e,
                source_data=clearinghouse_data.model_dump() if clearinghouse_data else None,
            ) from e
        else:
            return summary

    @staticmethod
    def transform_account_settings_update_to_internal(
        args: UpdateAccountSettingsArgs,
        exchange_name: str,
        asset_leverage_settings: dict[int, int] | None = None,
    ) -> AccountSettings:
        """Transform account settings update args to internal AccountSettings model.

        Converts account settings update arguments into an internal AccountSettings domain model
        with Hyperliquid-specific features.

        Args:
            args: The account settings update arguments
            exchange_name: Name of the exchange
            asset_leverage_settings: Optional dict mapping asset indices to leverage values

        Returns:
            AccountSettings: Internal model representing the updated settings

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_account_settings_update",
                exchange_name=exchange_name,
                leverage_limit=args.leverage_limit,
                asset_leverage_settings_count=len(asset_leverage_settings)
                if asset_leverage_settings
                else 0,
                message="Transforming UpdateAccountSettingsArgs to AccountSettings",
            )

            # Create Hyperliquid-specific details
            hl_details = HyperliquidAccountSettingsDetails(
                asset_leverage_settings=asset_leverage_settings,
                cross_margin_enabled=True,  # Default to cross margin
            )

            # Use secure_transform for type-safe model creation
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

            settings = secure_transform(
                data=settings_data,
                model_class=AccountSettings,
                context="hyperliquid_account_settings_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "account_settings_update_transformed",
                exchange_name=exchange_name,
                leverage_limit=args.leverage_limit,
                asset_leverage_settings_count=len(asset_leverage_settings)
                if asset_leverage_settings
                else 0,
                cross_margin_enabled=True,
                message="Successfully transformed UpdateAccountSettingsArgs to AccountSettings",
            )

        except Exception as e:
            logger.exception(
                "account_settings_update_transform_failed",
                exchange_name=exchange_name,
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
