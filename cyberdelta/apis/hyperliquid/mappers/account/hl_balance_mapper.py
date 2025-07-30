"""Hyperliquid Balance Mapper.

This mapper handles transformations for balance-related data from the Hyperliquid exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- SpotBalance transformations from clearinghouse state data
- USDC balance processing from margin summary
- Other spot asset processing from asset positions
- Balance-specific validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import DataTransformationError
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import BalanceMapperProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import HyperliquidSpotBalanceDetails, SpotBalance
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidBalanceMapper(BalanceMapperProtocol):
    """Focused mapper for Hyperliquid balance data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw balance models
    into CyberDeltaEngine Internal SpotBalance Domain Models.
    """

    # Protocol method implementations (delegated to common utilities)
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Parse decimal values safely with default fallback.

        Args:
            value: Value to parse as Decimal
            default: Default value if parsing fails

        Returns:
            Decimal: Parsed decimal value or default
        """
        return HyperliquidCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: Timestamp in milliseconds

        Returns:
            datetime | None: Converted datetime or None if input is None
        """
        return HyperliquidCommonMappers.timestamp_ms_to_datetime(timestamp_ms)

    # Protocol-specific methods
    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: Symbol,
        raw_user_state: HyperliquidRawClearinghouseState,
    ) -> SpotBalance:
        """Transform raw balance data to internal model.

        Args:
            asset_symbol: The asset symbol for this balance
            raw_user_state: Raw user state data containing balance information

        Returns:
            SpotBalance domain model
        """
        # Get all balances and return the requested one
        balances = HyperliquidBalanceMapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_user_state,
        )

        asset_symbol_str = asset_symbol.value
        if asset_symbol_str not in balances:
            # Return zero balance for missing assets
            return HyperliquidBalanceMapper._create_zero_balance(asset_symbol_str)

        return balances[asset_symbol_str]

    @staticmethod
    def _create_zero_balance(asset_symbol: str) -> SpotBalance:
        """Create a zero balance for a given asset.

        Args:
            asset_symbol: The asset symbol

        Returns:
            SpotBalance with zero quantities
        """
        details = HyperliquidSpotBalanceDetails()

        # Create domain symbol at entry point
        exchange_symbol = exchanges.hyperliquid(
            value=asset_symbol,
        )

        balance_data = {
            "asset": exchange_symbol,  # Domain object!
            "exchange": ExchangeName.HYPERLIQUID.value,
            "total_quantity": "0",
            "available_quantity": "0",
            "timestamp": datetime.now(UTC).isoformat(),
            "hl_details": details.model_dump() if details else None,
            "bp_details": None,
        }

        return secure_transform(
            data=balance_data,
            model_class=SpotBalance,
            context="hyperliquid_zero_balance_transform",
            source_exchange="hyperliquid",
        )

    @staticmethod
    def transform_raw_clearinghouse_state_to_spot_balances(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> dict[str, SpotBalance]:
        """Transforms a HyperliquidRawClearinghouseState to Internal SpotBalance models.

        Processes the clearinghouse state to extract USDC balances from margin summary
        and other spot assets from asset positions.

        Args:
            raw_state: Validated raw clearinghouse state from Hyperliquid

        Returns:
            dict[str, SpotBalance]: Dictionary mapping asset symbols to SpotBalance models

        Raises:
            TransformationError: If transformation fails during secure_transform
            DataTransformationError: If any other exception occurs during transformation
        """
        try:
            logger.debug(
                "transforming_clearinghouse_state_to_spot_balances",
                has_margin_summary=True,  # margin_summary is a required field
                has_asset_positions=True,  # asset_positions is a required field
                asset_positions_count=len(raw_state.asset_positions)
                if raw_state.asset_positions
                else 0,
                message="Transforming HyperliquidRawClearinghouseState to SpotBalance models",
            )

            spot_balances: dict[str, SpotBalance] = {}

            # Extract USDC balance from margin summary account value
            HyperliquidBalanceMapper._process_usdc_balance(raw_state, spot_balances)

            # Check for other spot assets in asset positions
            HyperliquidBalanceMapper._process_other_spot_assets(raw_state, spot_balances)

            logger.debug(
                "clearinghouse_state_to_spot_balances_transformed",
                spot_balances_count=len(spot_balances),
                asset_symbols=list(spot_balances.keys()),
                message="Successfully transformed clearinghouse state to spot balances",
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "clearinghouse_state_to_spot_balances_transform_failed",
                # raw_state should not be None in this context
                has_margin_summary=True,
                # raw_state should not be None in this context
                has_asset_positions=True,
                error=str(e),
                message="Failed to transform clearinghouse state to spot balances",
            )
            raise DataTransformationError(
                source_model="HyperliquidRawClearinghouseState",
                target_model="SpotBalance",
                reason=str(e),
                original_error=e,
                source_data=raw_state.model_dump() if raw_state else None,
            ) from e
        else:
            return spot_balances

    @staticmethod
    def _process_usdc_balance(
        raw_state: HyperliquidRawClearinghouseState,
        spot_balances: dict[str, SpotBalance],
    ) -> None:
        """Process USDC balance from margin summary.

        Extracts USDC balance information from the margin summary's account value
        and withdrawable amount.

        Args:
            raw_state: Raw clearinghouse state containing margin summary
            spot_balances: Dictionary to populate with USDC balance
        """
        # margin_summary is a required field in HyperliquidRawClearinghouseState
        if not raw_state.margin_summary:
            logger.debug(
                "usdc_balance_processing_skipped",
                reason="no_margin_summary",
                message="Skipping USDC balance processing - no margin summary available",
            )
            return

        logger.debug(
            "processing_usdc_balance",
            account_value=raw_state.margin_summary.account_value,
            withdrawable=raw_state.withdrawable,
            message="Processing USDC balance from margin summary",
        )

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

        if total_usdc >= Decimal(0):
            # Create HL-specific details
            details = HyperliquidSpotBalanceDetails()

            # Use withdrawable as available, or total if withdrawable is None/invalid
            if available_usdc is None or available_usdc < Decimal(0):
                available_usdc = Decimal(0)
            elif available_usdc > total_usdc:
                available_usdc = total_usdc

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value="USDC",
            )

            # Use secure_transform for type-safe model creation
            balance_data = {
                "asset": exchange_symbol,  # Domain object!
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

            logger.debug(
                "usdc_balance_processed",
                total_quantity=str(total_usdc),
                available_quantity=str(available_usdc),
                message="Successfully processed USDC balance from margin summary",
            )

    @staticmethod
    def _process_other_spot_assets(
        raw_state: HyperliquidRawClearinghouseState,
        spot_balances: dict[str, SpotBalance],
    ) -> None:
        """Process other spot assets from asset positions.

        Iterates through asset positions to find and process non-USDC spot assets.

        Args:
            raw_state: Raw clearinghouse state containing asset positions
            spot_balances: Dictionary to populate with spot asset balances
        """
        # asset_positions is a required field in HyperliquidRawClearinghouseState
        if not raw_state.asset_positions:
            logger.debug(
                "other_spot_assets_processing_skipped",
                reason="no_asset_positions",
                message="Skipping other spot assets processing - no asset positions available",
            )
            return

        logger.debug(
            "processing_other_spot_assets",
            asset_positions_count=len(raw_state.asset_positions),
            message="Processing other spot assets from asset positions",
        )

        for asset_pos in raw_state.asset_positions:
            asset_name = asset_pos.asset

            # Skip USDC as it's handled above, and skip obvious perps
            if asset_name is None or asset_name == "USDC" or "-PERP" in asset_name.upper():
                continue

            HyperliquidBalanceMapper._process_single_spot_asset(
                asset_pos,
                asset_name,
                spot_balances,
            )

        logger.debug(
            "other_spot_assets_processed",
            processed_assets=len([k for k in spot_balances if k != "USDC"]),
            message="Successfully processed other spot assets",
        )

    @staticmethod
    def _process_single_spot_asset(
        asset_pos: HyperliquidRawAssetPosition,
        asset_name: str,
        spot_balances: dict[str, SpotBalance],
    ) -> None:
        """Process a single spot asset position.

        Extracts balance information from a single asset position and creates
        a SpotBalance model if the asset has a positive balance.

        Args:
            asset_pos: Raw asset position data
            asset_name: Name of the asset
            spot_balances: Dictionary to populate with the asset balance
        """
        logger.debug(
            "processing_single_spot_asset",
            asset_name=asset_name,
            has_position=True,  # position is a required field in HyperliquidRawAssetPosition
            message="Processing single spot asset position",
        )

        # Process potential spot assets
        # position is a required field in HyperliquidRawAssetPosition
        if not asset_pos.position:
            logger.debug(
                "single_spot_asset_skipped",
                asset_name=asset_name,
                reason="no_position_data",
                message="Skipping asset - no position data available",
            )
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

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=asset_name,
            )

            # Use secure_transform for type-safe model creation
            balance_data = {
                "asset": exchange_symbol,  # Domain object!
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

            logger.debug(
                "single_spot_asset_processed",
                asset_name=asset_name,
                total_quantity=str(size),
                available_quantity=str(size),
                message="Successfully processed single spot asset",
            )
        else:
            logger.debug(
                "single_spot_asset_skipped",
                asset_name=asset_name,
                size=str(size) if size is not None else None,
                reason="zero_or_negative_size",
                message="Skipping asset - zero or negative size",
            )
