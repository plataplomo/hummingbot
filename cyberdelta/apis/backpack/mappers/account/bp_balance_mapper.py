"""Backpack Balance Mapper.

This mapper handles transformations for balance-related data from the Backpack exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- SpotBalance transformations from raw balance data
- Balance data validation and type conversion
- Balance-specific error handling and logging
"""

from datetime import UTC, datetime

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralAsset
from cyberdelta.apis.backpack.protocols.mapper_protocols import BalanceMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import (
    BalanceMapperMixin,
    CommonDataParserMixin,
    ValidationMixin,
)
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import BackpackSpotBalanceDetails, SpotBalance
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackBalanceMapper(
    CommonDataParserMixin,
    ValidationMixin,
    BalanceMapperMixin,
    BalanceMapperProtocol,
):
    """Focused mapper for Backpack balance data transformations.

    This class contains static methods for transforming validated Backpack Raw balance models
    into CyberDeltaEngine Internal SpotBalance Domain Models.
    """

    def transform_balance_data_to_spot_balance(
        self,
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
            total = self.parse_decimal_safely(total_balance)
            available = self.parse_decimal_safely(available_balance)

            total = self.ensure_decimal_not_none(total, "total_balance", "balance_validation")
            available = self.ensure_decimal_not_none(
                available, "available_balance", "balance_validation"
            )

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

    def transform_raw_balance_to_internal(
        self,
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
            parsed_available = self.parse_decimal_safely(raw.available)
            parsed_locked = self.parse_decimal_safely(raw.locked)
            parsed_staked = self.parse_decimal_safely(raw.staked)

            available_typed = self.ensure_decimal_not_none(
                parsed_available,
                "available",
                "balance_field_validation",
            )
            locked_typed = self.ensure_decimal_not_none(
                parsed_locked,
                "locked",
                "balance_field_validation",
            )
            staked_typed = self.ensure_decimal_not_none(
                parsed_staked,
                "staked",
                "balance_field_validation",
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

    def create_balance_from_collateral(
        self,
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
            total = self.parse_decimal_safely(collateral_data.total_quantity)
            available = self.parse_decimal_safely(collateral_data.available_quantity)

            total = self.ensure_decimal_not_none(total, "total_balance", "balance_validation")
            available = self.ensure_decimal_not_none(
                available, "available_balance", "balance_validation"
            )

            # Create BP-specific details from collateral data
            details = BackpackSpotBalanceDetails(
                open_order_quantity=self.parse_decimal_safely(
                    collateral_data.open_order_quantity, default=None
                ),
                lend_quantity=self.parse_decimal_safely(
                    collateral_data.lend_quantity, default=None
                ),
                collateral_weight=self.parse_decimal_safely(
                    collateral_data.collateral_weight, default=None
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

