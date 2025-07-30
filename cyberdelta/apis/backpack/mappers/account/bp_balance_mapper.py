"""Backpack Balance Mapper.

This mapper handles transformations for balance-related data from the Backpack exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- SpotBalance transformations from raw balance data
- Balance data validation and type conversion
- Balance-specific error handling and logging
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralAsset
from cyberdelta.apis.backpack.protocols.mapper_protocols import BalanceMapperProtocol
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import BackpackSpotBalanceDetails, SpotBalance
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackBalanceMapper(BalanceMapperProtocol):
    """Focused mapper for Backpack balance data transformations.

    This class contains static methods for transforming validated Backpack Raw balance models
    into CyberDeltaEngine Internal SpotBalance Domain Models.
    """

    @staticmethod
    def _ensure_balance_values_not_none(total: Decimal | None, available: Decimal | None) -> None:
        """Ensure balance values are not None.

        Args:
            total: Total balance value
            available: Available balance value

        Raises:
            MissingRequiredFieldError: If any value is None
        """
        if total is None:
            raise MissingRequiredFieldError(
                field_names="total_balance",
                context="balance_validation",
            )
        if available is None:
            raise MissingRequiredFieldError(
                field_names="available_balance",
                context="balance_validation",
            )

    @staticmethod
    def _ensure_balance_field_not_none(
        field_value: Decimal | None,
        field_name: str,
    ) -> Decimal:
        """Ensure a balance field is not None and return typed value.

        Args:
            field_value: The field value to check
            field_name: The name of the field for error reporting

        Returns:
            Decimal: The validated field value

        Raises:
            MissingRequiredFieldError: If field_value is None
        """
        if field_value is None:
            raise MissingRequiredFieldError(
                field_names=field_name,
                context="balance_field_validation",
            )
        return field_value

    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str,
        total_balance: str,
        available_balance: str,
    ) -> SpotBalance:
        """Transform balance data to an Internal SpotBalance model.

        Args:
            asset: Asset symbol
            total_balance: Total balance as string
            available_balance: Available balance as string

        Returns:
            SpotBalance: Internal domain model with BP details populated

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_balance_data",
                asset=asset,
                total_balance=total_balance,
                available_balance=available_balance,
                message="Transforming balance data to SpotBalance",
            )

            # Parse balances
            total = parse_decimal_value(total_balance, allow_none=False, field_name="total_balance")
            available = parse_decimal_value(
                available_balance,
                allow_none=False,
                field_name="available_balance",
            )

            BackpackBalanceMapper._ensure_balance_values_not_none(total, available)

            # Create BP-specific details
            details = BackpackSpotBalanceDetails()

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=asset,
            )

            # Use secure_transform for type-safe model creation
            balance_data = {
                "asset": exchange_symbol,  # Domain object!
                "exchange": ExchangeName.BACKPACK.value,
                "total_quantity": str(total),
                "available_quantity": str(available),
                "timestamp": datetime.now(UTC).isoformat(),
                "bp_details": details.model_dump() if details else None,
            }

            spot_balance = secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context="backpack_balance_from_dict",
                source_exchange="backpack",
            )

            logger.debug(
                "balance_data_transformed",
                asset=asset,
                total_quantity=str(total),
                available_quantity=str(available),
                message="Successfully transformed balance data to SpotBalance",
            )

        except Exception as e:
            logger.exception(
                "balance_transform_failed",
                asset=asset,
                total_balance=total_balance,
                available_balance=available_balance,
                error=str(e),
                message="Failed to transform balance data to SpotBalance",
            )
            raise DataTransformationError(
                source_model="balance_data",
                target_model="SpotBalance",
                reason=str(e),
                original_error=e,
            ) from e
        else:
            return spot_balance

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: Symbol,
        raw: BackpackRawBalanceResponse,
    ) -> SpotBalance:
        """Transform a validated BackpackRawBalanceResponse object for a specific asset.

        Converts the raw balance data into an internal SpotBalance domain model.

        Args:
            asset_symbol: The Symbol domain object of the asset
            raw: The validated raw balance data for the asset

        Returns:
            SpotBalance: The corresponding internal SpotBalance object

        Raises:
            DataTransformationError: If essential numeric fields are missing or invalid
        """
        try:
            logger.debug(
                "transforming_raw_balance",
                asset_symbol=asset_symbol.value,
                raw_available=raw.available,
                raw_locked=raw.locked,
                raw_staked=raw.staked,
                message="Transforming BackpackRawBalanceResponse to SpotBalance",
            )

            # Defensive parsing of numeric strings
            parsed_available = parse_decimal_value(
                raw.available,
                allow_none=False,
                field_name=f"{asset_symbol.value}_available",
            )
            parsed_locked = parse_decimal_value(
                raw.locked,
                allow_none=False,
                field_name=f"{asset_symbol.value}_locked",
            )
            parsed_staked = parse_decimal_value(
                raw.staked,
                allow_none=False,
                field_name=f"{asset_symbol.value}_staked",
            )

            available_typed = BackpackBalanceMapper._ensure_balance_field_not_none(
                parsed_available,
                "available",
            )
            locked_typed = BackpackBalanceMapper._ensure_balance_field_not_none(
                parsed_locked,
                "locked",
            )
            staked_typed = BackpackBalanceMapper._ensure_balance_field_not_none(
                parsed_staked,
                "staked",
            )

            # Calculate total balance
            total_balance = available_typed + locked_typed + staked_typed

            # Create BP-specific details with available fields
            # Note: BackpackSpotBalanceDetails only supports lend_quantity,
            # open_order_quantity, and collateral_weight
            # The locked and staked quantities are tracked in the main balance amounts
            details = BackpackSpotBalanceDetails(
                lend_quantity=staked_typed if staked_typed > 0 else None,
            )

            # Use the Symbol object directly (no need to create another)
            exchange_symbol = asset_symbol

            # Use secure_transform for type-safe model creation
            balance_data = {
                "asset": exchange_symbol,  # Domain object!
                "exchange": ExchangeName.BACKPACK.value,
                "total_quantity": str(total_balance),
                "available_quantity": str(available_typed),
                "timestamp": datetime.now(UTC).isoformat(),
                "bp_details": details.model_dump() if details else None,
            }

            spot_balance = secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context="backpack_raw_balance_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_balance_transformed",
                asset_symbol=asset_symbol,
                total_balance=str(total_balance),
                available_quantity=str(available_typed),
                locked_quantity=str(locked_typed),
                staked_quantity=str(staked_typed),
                message="Successfully transformed BackpackRawBalanceResponse to SpotBalance",
            )

        except Exception as e:
            logger.exception(
                "raw_balance_transform_failed",
                asset_symbol=asset_symbol,
                raw_balance=raw.model_dump() if raw else None,
                error=str(e),
                message="Failed to transform BackpackRawBalanceResponse to SpotBalance",
            )
            raise DataTransformationError(
                source_model="BackpackRawBalanceResponse",
                target_model="SpotBalance",
                reason=str(e),
                original_error=e,
                source_data=raw.model_dump() if raw else None,
            ) from e
        else:
            return spot_balance

    @staticmethod
    def create_balance_from_collateral(
        symbol: Symbol,
        collateral_data: BackpackRawCollateralAsset,
        exchange_name: str,
    ) -> SpotBalance:
        """Create a SpotBalance from collateral data when standard balance endpoint is unavailable.

        Args:
            symbol: Asset Symbol domain object
            collateral_data: Raw collateral data for the asset
            exchange_name: Exchange name

        Returns:
            SpotBalance: Internal domain model created from collateral data

        Raises:
            DataTransformationError: If transformation fails
        """
        # Type is already imported at module level

        try:
            logger.debug(
                "creating_balance_from_collateral",
                symbol=symbol.value,
                total_quantity=collateral_data.total_quantity,
                available_quantity=collateral_data.available_quantity,
                message="Creating SpotBalance from collateral data",
            )

            # Parse quantities from collateral data
            total = parse_decimal_value(
                collateral_data.total_quantity,
                allow_none=False,
                field_name="total_quantity",
            )
            available = parse_decimal_value(
                collateral_data.available_quantity,
                allow_none=False,
                field_name="available_quantity",
            )

            BackpackBalanceMapper._ensure_balance_values_not_none(total, available)

            # Create BP-specific details from collateral data
            details = BackpackSpotBalanceDetails(
                open_order_quantity=parse_decimal_value(
                    collateral_data.open_order_quantity,
                    allow_none=True,
                    field_name="open_order_quantity",
                ),
                lend_quantity=parse_decimal_value(
                    collateral_data.lend_quantity,
                    allow_none=True,
                    field_name="lend_quantity",
                ),
                collateral_weight=parse_decimal_value(
                    collateral_data.collateral_weight,
                    allow_none=True,
                    field_name="collateral_weight",
                ),
            )

            # Use the Symbol object directly (no need to create another)
            exchange_symbol = symbol

            # Use secure_transform for type-safe model creation
            balance_data = {
                "asset": exchange_symbol,  # Domain object!
                "exchange": exchange_name,
                "total_quantity": str(total),
                "available_quantity": str(available),
                "timestamp": datetime.now(UTC).isoformat(),
                "bp_details": details.model_dump() if details else None,
            }

            spot_balance = secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context="backpack_balance_from_collateral",
                source_exchange=exchange_name,
            )

            logger.debug(
                "balance_from_collateral_created",
                symbol=symbol,
                total_quantity=str(total),
                available_quantity=str(available),
                message="Successfully created SpotBalance from collateral data",
            )

        except Exception as e:
            logger.exception(
                "collateral_balance_transform_failed",
                symbol=symbol,
                collateral_data=collateral_data.model_dump() if collateral_data else None,
                error=str(e),
                message="Failed to create SpotBalance from collateral data",
            )
            raise DataTransformationError(
                source_model="BackpackRawCollateralAsset",
                target_model="SpotBalance",
                reason=str(e),
                original_error=e,
                source_data=collateral_data.model_dump() if collateral_data else None,
            ) from e
        else:
            return spot_balance

    # MapperProtocol implementation - delegate to common utilities
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Safely parse decimal values with fallback.

        Args:
            value: Value to parse as Decimal (string, float, Decimal, or None).
            default: Default value to return if parsing fails.

        Returns:
            Parsed Decimal value or default if parsing fails.
        """
        return BackpackCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to UTC datetime.

        Args:
            timestamp_ms: Timestamp in milliseconds (float or None).

        Returns:
            UTC datetime object if timestamp is provided, None otherwise.
        """
        return BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
