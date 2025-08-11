"""Hyperliquid Market Metadata Mapper.

This mapper handles transformations for market metadata from the Hyperliquid exchange,
extracted from the monolithic market data mapper to improve maintainability and testability.

Focused on:
- Market transformations from asset definitions and contexts
- Asset metadata processing and validation
- Market details and trading rules extraction
- Market status and configuration data
"""

from decimal import Decimal

from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin, ValidationMixin
from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import (
    DataTransformationError,
    MarketTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    MarketMapperProtocol,
    MarketMetadataMapperProtocol,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market import Market
from cyberdelta.models.market.market import HyperliquidMarketDetails
from cyberdelta.symbols import exchanges
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidMarketMetadataMapper(
    CommonDataParserMixin,
    ValidationMixin,
    MarketMetadataMapperProtocol,
    MarketMapperProtocol,
):
    """Focused mapper for Hyperliquid market metadata transformations.

    This class contains static methods for transforming validated Hyperliquid Raw market metadata
    into CyberDeltaEngine Internal Domain Models for markets and trading rules.
    """

    # Protocol-specific method from MarketMapperProtocol
    def transform_single_asset_to_market(
        self,
        asset_def: HyperliquidRawAssetDefinition,
        asset_ctx: HyperliquidRawAssetCtx | None = None,
    ) -> Market:
        """Transform asset definition and context to market model.

        Args:
            asset_def: Asset definition with trading rules from meta response
            asset_ctx: Optional asset context with current pricing data

        Returns:
            Market domain model
        """
        # Delegate to existing business logic method from git history
        return self._create_market_from_asset_definition(
            asset_def,
            asset_ctx,
        )

    @staticmethod
    def _validate_asset_definition_data(step_size_parsed: object, asset_name: str) -> Decimal:
        """Validate asset definition data.

        Args:
            step_size_parsed: Parsed step size value
            asset_name: Asset name for context

        Returns:
            Decimal: Validated step size

        Raises:
            MissingRequiredFieldError: If step size is invalid
            DataTransformationError: If step size is not a Decimal
        """
        if step_size_parsed is None:
            raise MissingRequiredFieldError("sz_decimals", f"asset definition for {asset_name}")
        if not isinstance(step_size_parsed, Decimal):
            raise DataTransformationError(
                source_model="asset_definition",
                target_model="step_size",
                reason=f"Expected Decimal, got {type(step_size_parsed).__name__}",
                source_data={"step_size": step_size_parsed, "asset_name": asset_name},
            )
        return step_size_parsed

    def transform_raw_meta_and_asset_ctxs_to_markets(
        self,
        raw_meta_and_asset_ctxs: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> list[Market]:
        """Transform raw meta and asset contexts to internal Market models.

        Args:
            raw_meta_and_asset_ctxs: Raw response containing asset definitions and contexts

        Returns:
            List of Market objects with metadata for all assets

        Raises:
            MarketTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_meta_and_asset_ctxs_to_markets",
                universe_count=len(raw_meta_and_asset_ctxs.meta.universe)
                if raw_meta_and_asset_ctxs.meta
                else 0,
                asset_ctxs_count=len(raw_meta_and_asset_ctxs.asset_ctxs)
                if raw_meta_and_asset_ctxs.asset_ctxs
                else 0,
                message="Transforming HyperliquidRawMetaAndAssetCtxsResponse to Markets",
            )

            markets: list[Market] = []

            # Create lookup dictionary for asset contexts by name
            asset_ctx_lookup = {ctx.name: ctx for ctx in raw_meta_and_asset_ctxs.asset_ctxs}

            # Transform each asset definition from meta
            for asset_def in raw_meta_and_asset_ctxs.meta.universe:
                try:
                    # Get corresponding asset context (optional)
                    asset_ctx = asset_ctx_lookup.get(asset_def.name)

                    market = self._create_market_from_asset_definition(
                        asset_def,
                        asset_ctx,
                    )
                    markets.append(market)

                except (TransformationError, ValueError, TypeError, AttributeError) as e:
                    logger.warning(
                        "asset_definition_to_market_transform_failed",
                        asset_name=asset_def.name,
                        error=str(e),
                        message="Failed to transform individual asset definition, "
                        "continuing with others",
                    )
                    continue

            logger.debug(
                "raw_meta_and_asset_ctxs_to_markets_transformed",
                input_universe_count=len(raw_meta_and_asset_ctxs.meta.universe),
                output_markets_count=len(markets),
                message="Successfully transformed "
                "HyperliquidRawMetaAndAssetCtxsResponse to Markets",
            )

        except Exception as e:
            logger.exception(
                "market_transformation_failed",
                universe_count=len(raw_meta_and_asset_ctxs.meta.universe)
                if raw_meta_and_asset_ctxs and raw_meta_and_asset_ctxs.meta
                else 0,
                error=str(e),
                message="Failed to transform meta and asset contexts to markets",
            )
            raise MarketTransformationError(
                reason=f"Failed to transform meta and asset contexts to markets: {e}",
                original_error=e,
                source_type="HyperliquidRawMetaAndAssetCtxs",
            ) from e
        else:
            return markets

    def _create_market_from_asset_definition(
        self,
        asset_def: HyperliquidRawAssetDefinition,
        asset_ctx: HyperliquidRawAssetCtx | None = None,
    ) -> Market:
        """Create a Market model from Hyperliquid asset definition and context.

        Args:
            asset_def: Asset definition with trading rules
            asset_ctx: Optional asset context with current pricing data

        Returns:
            Market object with available metadata

        Raises:
            TransformationError: If data validation fails
            MarketTransformationError: If market creation fails
        """
        try:
            logger.debug(
                "creating_market_from_asset_definition",
                asset_name=asset_def.name,
                sz_decimals=asset_def.sz_decimals,
                max_leverage=asset_def.max_leverage,
                only_isolated=asset_def.only_isolated,
                has_asset_ctx=asset_ctx is not None,
                message="Creating Market from HyperliquidRawAssetDefinition",
            )

            # Calculate step_size from sz_decimals
            step_size_parsed = self.parse_decimal_safely(f"1e-{asset_def.sz_decimals}")
            step_size = HyperliquidMarketMetadataMapper._validate_asset_definition_data(
                step_size_parsed,
                asset_def.name,
            )

            # For Hyperliquid perpetuals, determine tick size from actual market prices
            # since it's not explicitly provided in their meta response
            # Note: sz_decimals refers to quantity precision, not price precision
            if asset_ctx and asset_ctx.mark_px:
                # Analyze the mark price to determine price precision
                mark_price_str = str(asset_ctx.mark_px)
                if "." in mark_price_str:
                    # Count decimal places in the actual market price
                    decimal_places = len(mark_price_str.split(".")[1].rstrip("0"))
                    tick_size = self.parse_decimal_safely(
                        f"1e-{decimal_places}", default=Decimal("1.0")
                    )
                else:
                    # Whole number pricing
                    tick_size = Decimal("1.0")
            else:
                # Fallback for assets without market context
                # Use conservative tick size based on typical crypto price ranges
                tick_size = Decimal("1.0") if step_size <= Decimal("0.001") else step_size

            # Create Hyperliquid-specific details using proper typed model
            hl_details = HyperliquidMarketDetails(
                max_leverage=asset_def.max_leverage,
                only_isolated=asset_def.only_isolated,
                sz_decimals=asset_def.sz_decimals,
                mark_price=self.parse_decimal_safely(asset_ctx.mark_px, default=None)
                if asset_ctx
                else None,
                funding_rate=self.parse_decimal_safely(asset_ctx.funding, default=None)
                if asset_ctx
                else None,
            )

            # Parse symbol to domain object at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=asset_def.name,  # e.g., "BTC"
                asset_index=getattr(asset_def, "asset_index", None),
            )

            # Create market with available information
            # Use secure_transform for type-safe model creation
            market_data = {
                "symbol": exchange_symbol,  # Domain object!
                "market_type": "Perpetual",
                "tick_size": str(tick_size),
                "step_size": str(step_size),
                "min_price": None,  # Not specified in Hyperliquid meta
                "max_price": None,  # Not specified in Hyperliquid meta
                "min_quantity": str(step_size),  # Minimum is typically one step
                "max_quantity": None,  # Not specified in Hyperliquid meta
                "status": "Active",  # Assume active if in meta response
                "created_at": None,  # Not provided in meta response
                "bp_details": None,  # Not applicable
                "hl_details": hl_details.model_dump()
                if hl_details
                else None,  # Properly typed Hyperliquid details
            }

            market = secure_transform(
                data=market_data,
                model_class=Market,
                context="hyperliquid_asset_def_market_transform",
                source_exchange=ExchangeName.HYPERLIQUID.value,
            )

            logger.debug(
                "market_from_asset_definition_created",
                asset_name=asset_def.name,
                tick_size=str(tick_size),
                step_size=str(step_size),
                max_leverage=asset_def.max_leverage,
                mark_price=str(hl_details.mark_price) if hl_details.mark_price else None,
                message="Successfully created Market from HyperliquidRawAssetDefinition",
            )
        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "market_from_asset_definition_creation_failed",
                asset_name=getattr(asset_def, "name", None),
                asset_def=asset_def.model_dump() if asset_def else None,
                asset_ctx=asset_ctx.model_dump() if asset_ctx else None,
                error=str(e),
                message="Failed to create Market from HyperliquidRawAssetDefinition",
            )
            raise MarketTransformationError(
                reason=str(e),
                symbol=asset_def.name,  # name is a required field in HyperliquidRawAssetDefinition
                original_error=e,
            ) from e
        else:
            return market
