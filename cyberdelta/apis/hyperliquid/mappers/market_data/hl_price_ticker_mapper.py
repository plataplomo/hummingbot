"""Hyperliquid Price Ticker Mapper.

This mapper handles transformations for price ticker and mid price data from the
Hyperliquid exchange,
extracted from the monolithic market data mapper to improve maintainability and testability.

Focused on:
- Ticker transformations from asset context data
- Mid price transformations from all mids data
- Price-related validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import (
    DataTransformationError,
    MarketTransformationError,
    MissingRequiredFieldError,
    TickerTransformationError,
)
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    PriceTickerMapperProtocol,
    TickerMapperProtocol,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Ticker
from cyberdelta.core.models.market.mid_prices import MidPrices
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidPriceTickerMapper(PriceTickerMapperProtocol, TickerMapperProtocol):
    """Focused mapper for Hyperliquid price ticker and mid price data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw price models
    into CyberDeltaEngine Internal Domain Models for tickers and mid prices.
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

    # Protocol-specific methods from PriceTickerMapperProtocol
    @staticmethod
    def transform_raw_price_ticker_to_internal(raw_ticker: HyperliquidRawAssetCtx) -> Ticker:
        """Transform raw price ticker data to internal model.

        Args:
            raw_ticker: Raw price ticker data from API

        Returns:
            Ticker domain model
        """
        # Delegate to existing method with typed model
        return HyperliquidPriceTickerMapper.transform_raw_asset_ctx_to_ticker(raw_ticker)

    # Protocol-specific methods from TickerMapperProtocol
    @staticmethod
    def transform_raw_ticker_to_internal(raw_ticker: HyperliquidRawAssetCtx) -> Ticker:
        """Transform raw ticker data to internal model.

        Args:
            raw_ticker: Raw ticker data from API

        Returns:
            Ticker domain model
        """
        # Delegate to existing method that matches git history business logic
        return HyperliquidPriceTickerMapper.transform_raw_asset_ctx_to_ticker(raw_ticker)

    @staticmethod
    def _validate_asset_ctx_data(mark_px: object, name: object, context: str) -> tuple[object, str]:
        """Validate asset context data.

        Args:
            mark_px: Raw mark price value
            name: Raw symbol name value
            context: Context for error messages

        Returns:
            tuple[object, str]: Validated mark_px and name

        Raises:
            MissingRequiredFieldError: If required fields are missing
            DataTransformationError: If data types are invalid
        """
        if mark_px is None:
            raise MissingRequiredFieldError("mark_px", context)
        if name is None:
            raise MissingRequiredFieldError("name", context)
        if not isinstance(name, str):
            raise DataTransformationError(
                source_model="asset_ctx",
                target_model="ticker",
                reason=f"Expected name to be str, got {type(name).__name__}",
                source_data={"name": name},
            )
        return mark_px, name

    @staticmethod
    def transform_raw_asset_ctx_to_ticker(raw_asset_ctx: HyperliquidRawAssetCtx) -> Ticker:
        """Transforms a HyperliquidRawAssetCtx to an Internal Ticker model.

        Converts asset context data from Hyperliquid into an internal Ticker domain model
        with mark price and volume information.

        Args:
            raw_asset_ctx: Validated raw asset context data from Hyperliquid

        Returns:
            Ticker: Internal domain model with populated fields and HL details

        Raises:
            TickerTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_asset_ctx_to_ticker",
                symbol=raw_asset_ctx.name,
                mark_px=raw_asset_ctx.mark_px,
                day_ntl_vlm=raw_asset_ctx.day_ntl_vlm,
                message="Transforming HyperliquidRawAssetCtx to Ticker",
            )

            # Validate asset context data
            HyperliquidPriceTickerMapper._validate_asset_ctx_data(
                raw_asset_ctx.mark_px, raw_asset_ctx.name, "HyperliquidRawAssetCtx"
            )

            mark_px = parse_decimal_value(
                raw_asset_ctx.mark_px,
                allow_none=False,
                field_name="markPx",
            )

            # Extract volume data from day_ntl_vlm (daily notional volume)
            volume_24h = None
            if raw_asset_ctx.day_ntl_vlm:
                volume_24h = parse_decimal_value(
                    raw_asset_ctx.day_ntl_vlm,
                    allow_none=True,
                    field_name="dayNtlVlm",
                )

            # Get current timestamp for ticker timestamp
            timestamp = datetime.now(UTC)

            # Use secure_transform for type-safe model creation
            ticker_data = {
                "symbol": raw_asset_ctx.name,
                "exchange": "hyperliquid",  # Required field for Ticker model
                "timestamp": timestamp.isoformat(),
                "price": str(mark_px),  # Using mark_px as the last price
                "bid": None,  # Not available in asset context
                "ask": None,  # Not available in asset context
                "volume": str(volume_24h) if volume_24h is not None else None,
                "bp_details": None,
                "hl_details": None,
            }

            ticker = secure_transform(
                data=ticker_data,
                model_class=Ticker,
                context="hyperliquid_asset_ctx_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_asset_ctx_to_ticker_transformed",
                symbol=raw_asset_ctx.name,
                price=str(mark_px),
                volume=str(volume_24h) if volume_24h else None,
                timestamp=timestamp.isoformat(),
                message="Successfully transformed HyperliquidRawAssetCtx to Ticker",
            )
        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "asset_context_to_ticker_transform_failed",
                symbol=getattr(raw_asset_ctx, "name", None),
                mark_px=getattr(raw_asset_ctx, "mark_px", None),
                raw_asset_ctx=raw_asset_ctx.model_dump() if raw_asset_ctx else None,
                error=str(e),
                message="Failed to transform HyperliquidRawAssetCtx to Ticker",
            )
            raise TickerTransformationError(
                ticker_source="HyperliquidRawAssetCtx",
                reason=str(e),
                symbol=raw_asset_ctx.name,
                original_error=e,
            ) from e
        else:
            return ticker

    @staticmethod
    def transform_raw_all_mids_to_internal(
        raw_all_mids: HyperliquidRawAllMids,
    ) -> MidPrices:
        """Transform Hyperliquid AllMids response to internal MidPrices model.

        Converts mid price data from Hyperliquid into an internal MidPrices domain model
        containing symbol to price mappings.

        Args:
            raw_all_mids: Validated HyperliquidRawAllMids model containing symbol->price mapping

        Returns:
            MidPrices: Internal model containing symbol to mid price mapping

        Raises:
            MarketTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_all_mids_to_internal",
                symbols_count=len(raw_all_mids.root) if raw_all_mids.root else 0,
                symbols=list(raw_all_mids.root.keys()) if raw_all_mids.root else [],
                message="Transforming HyperliquidRawAllMids to MidPrices",
            )

            # The raw model already has validated the structure
            # We just need to convert string prices to Decimal
            prices: dict[str, Decimal] = {}
            for symbol, price_str in raw_all_mids.root.items():
                # Use our standard decimal parsing utility
                decimal_price = parse_decimal_value(
                    price_str,
                    allow_none=False,
                    field_name=f"mid_price[{symbol}]",
                )
                prices[symbol] = decimal_price

            # Use secure_transform for type-safe model creation
            mid_prices_data = {
                "prices": {symbol: str(price) for symbol, price in prices.items()},
                "timestamp": datetime.now(UTC).isoformat(),
                "exchange": ExchangeName.HYPERLIQUID.value,
            }

            mid_prices = secure_transform(
                data=mid_prices_data,
                model_class=MidPrices,
                context="hyperliquid_all_mids_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_all_mids_to_internal_transformed",
                symbols_count=len(prices),
                symbols=list(prices.keys()),
                message="Successfully transformed HyperliquidRawAllMids to MidPrices",
            )
        except Exception as e:
            logger.exception(
                "raw_all_mids_to_internal_transform_failed",
                symbols_count=len(raw_all_mids.root) if raw_all_mids and raw_all_mids.root else 0,
                raw_all_mids=raw_all_mids.model_dump() if raw_all_mids else None,
                error=str(e),
                message="Failed to transform HyperliquidRawAllMids to MidPrices",
            )
            raise MarketTransformationError(
                reason=str(e),
                original_error=e,
            ) from e
        else:
            return mid_prices
