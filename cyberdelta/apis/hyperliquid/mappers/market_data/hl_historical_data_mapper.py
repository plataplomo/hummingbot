"""Hyperliquid Historical Data Mapper.

This mapper handles transformations for historical market data from the Hyperliquid exchange,
extracted from the monolithic market data mapper to improve maintainability and testability.

Focused on:
- Funding rate transformations from asset context and funding history
- Candle/OHLCV transformations from candle snapshots
- Historical data validation and processing
- Time-based data validation
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cyberdelta.apis.exceptions import (
    CandleTransformationError,
    FundingRateTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
    HyperliquidRawWsCandle,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    CandleMapperProtocol,
    FundingRateMapperProtocol,
    HistoricalDataMapperProtocol,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate, HyperliquidFundingDetails
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidHistoricalDataMapper(
    HistoricalDataMapperProtocol,
    CandleMapperProtocol,
    FundingRateMapperProtocol,
):
    """Focused mapper for Hyperliquid historical market data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw historical models
    into CyberDeltaEngine Internal Domain Models for funding rates and candles.
    """

    # Protocol method implementations - delegate to common utilities
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Parse decimal values safely with default fallback.

        Args:
            value: The value to parse as Decimal.
            default: The default value to return if parsing fails.

        Returns:
            The parsed Decimal value or the default.
        """
        return HyperliquidCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: The timestamp in milliseconds.

        Returns:
            The converted datetime object or None if input is None.
        """
        return HyperliquidCommonMappers.timestamp_ms_to_datetime(timestamp_ms)

    # Protocol-specific methods from CandleMapperProtocol
    @staticmethod
    def transform_raw_candle_to_internal(raw_candle: HyperliquidRawCandleSnapshot) -> Candle:
        """Transform raw candle data to internal model.

        Args:
            raw_candle: Raw candle snapshot data from API

        Returns:
            Candle domain model

        Raises:
            CandleTransformationError: If no candles could be transformed from the raw snapshot.
        """
        # For a single candle snapshot, transform and return first candle
        # Default symbol and interval if not available
        default_symbol = exchanges.hyperliquid(value="UNKNOWN")
        candles = HyperliquidHistoricalDataMapper.transform_raw_candle_snapshot_to_candles(
            raw_candle,
            default_symbol,
            "1h",
        )

        if not candles:
            raise CandleTransformationError(
                reason="No candles transformed from raw snapshot",
                symbol="UNKNOWN",
                interval="1h",
                original_error=None,
            )

        return candles[0]

    @staticmethod
    def transform_ws_candle_to_internal(raw_ws_candle: HyperliquidRawWsCandle) -> Candle:
        """Transform WebSocket candle to internal Candle model.

        Args:
            raw_ws_candle: Validated WebSocket candle data from Hyperliquid

        Returns:
            Candle domain model

        Raises:
            CandleTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_ws_candle_to_internal",
                symbol=raw_ws_candle.s,
                interval=raw_ws_candle.i,
                timestamp=raw_ws_candle.t,
                message="Transforming HyperliquidRawWsCandle to Candle",
            )

            # Parse OHLCV data
            open_price = parse_decimal_value(raw_ws_candle.o, allow_none=False, field_name="open")
            high_price = parse_decimal_value(raw_ws_candle.h, allow_none=False, field_name="high")
            low_price = parse_decimal_value(raw_ws_candle.l, allow_none=False, field_name="low")
            close_price = parse_decimal_value(raw_ws_candle.c, allow_none=False, field_name="close")
            volume = parse_decimal_value(raw_ws_candle.v, allow_none=False, field_name="volume")

            # Convert timestamp from milliseconds to datetime
            open_time = datetime.fromtimestamp(raw_ws_candle.t / 1000, UTC)

            # Use secure_transform for type-safe model creation
            candle_data = {
                "symbol": raw_ws_candle.s,
                "interval": raw_ws_candle.i,
                "open_time": open_time.isoformat(),
                "open": str(open_price),
                "high": str(high_price),
                "low": str(low_price),
                "close": str(close_price),
                "volume": str(volume),
            }

            candle = secure_transform(
                data=candle_data,
                model_class=Candle,
                context="hyperliquid_ws_candle_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "ws_candle_to_internal_transformed",
                symbol=raw_ws_candle.s,
                interval=raw_ws_candle.i,
                open_time=open_time.isoformat(),
                message="Successfully transformed HyperliquidRawWsCandle to Candle",
            )

        except Exception as e:
            logger.exception(
                "ws_candle_transform_failed",
                symbol=raw_ws_candle.s,
                interval=raw_ws_candle.i,
                raw_candle=raw_ws_candle.model_dump() if raw_ws_candle else None,
                error=str(e),
                message="Failed to transform HyperliquidRawWsCandle to Candle",
            )
            raise CandleTransformationError(
                reason=str(e),
                symbol=raw_ws_candle.s,
                interval=raw_ws_candle.i,
                original_error=e,
            ) from e
        else:
            return candle

    # Protocol-specific methods from HistoricalDataMapperProtocol
    @staticmethod
    def transform_raw_funding_history_to_internal(
        raw_funding: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transform raw funding history data to internal model.

        Args:
            raw_funding: Raw funding history item from API

        Returns:
            FundingRate domain model
        """
        # Delegate to existing business logic method
        return HyperliquidHistoricalDataMapper.transform_raw_funding_history_item_to_internal(
            raw_funding,
        )

    # Protocol-specific methods from FundingRateMapperProtocol
    @staticmethod
    def transform_raw_funding_rate_to_internal(
        raw_funding_rate: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transform raw funding rate data to internal model.

        Args:
            raw_funding_rate: Raw funding rate history item from API

        Returns:
            FundingRate domain model
        """
        # Delegate to existing business logic method
        return HyperliquidHistoricalDataMapper.transform_raw_funding_history_item_to_internal(
            raw_funding_rate,
        )

    @staticmethod
    def _validate_funding_data(
        funding_rate: object,
        timestamp: object,
        context: str,
    ) -> tuple[object, object]:
        """Validate funding rate data.

        Args:
            funding_rate: Raw funding rate value
            timestamp: Raw timestamp value
            context: Context for error messages

        Returns:
            tuple[object, object]: Validated funding_rate and timestamp

        Raises:
            MissingRequiredFieldError: If required fields are missing
        """
        if funding_rate is None:
            raise MissingRequiredFieldError("funding_rate", context)
        if timestamp is None:
            raise MissingRequiredFieldError("timestamp", context)
        return funding_rate, timestamp

    @staticmethod
    def _ensure_funding_timestamp_not_none(timestamp: datetime | None) -> datetime:
        """Ensure funding timestamp is not None.

        Args:
            timestamp: Parsed timestamp value

        Returns:
            datetime: Non-None timestamp value

        Raises:
            FundingRateTransformationError: If timestamp is None
        """
        if timestamp is None:
            raise FundingRateTransformationError(
                source_type="funding_history",
                reason="Timestamp is None after parsing",
                symbol="unknown",
                original_error=None,
            )
        return timestamp

    @staticmethod
    def transform_raw_asset_ctx_to_funding_rate(
        raw_asset_ctx: HyperliquidRawAssetCtx,
    ) -> FundingRate | None:
        """Transforms a HyperliquidRawAssetCtx to an Internal FundingRate model.

        Args:
            raw_asset_ctx: Validated raw asset context data from Hyperliquid

        Returns:
            FundingRate: Internal domain model with HL details, or None if no funding data
        """
        try:
            logger.debug(
                "transforming_raw_asset_ctx_to_funding_rate",
                symbol=raw_asset_ctx.name,
                mark_px=raw_asset_ctx.mark_px,
                funding=raw_asset_ctx.funding,
                impact_px=raw_asset_ctx.impact_px,
                message="Transforming HyperliquidRawAssetCtx to FundingRate",
            )

            # Parse mark price first
            mark_price = parse_decimal_value(
                raw_asset_ctx.mark_px,
                allow_none=True,
                field_name="mark_px",
            )

            # Parse hourly funding rate
            hourly_funding_rate = None
            funding_rate_8hr = None

            try:
                hourly_funding_rate = parse_decimal_value(
                    raw_asset_ctx.funding,
                    allow_none=True,
                    field_name="funding",
                )

                if hourly_funding_rate is not None and hourly_funding_rate.is_finite():
                    # Convert hourly rate to 8-hour rate
                    funding_rate_8hr = hourly_funding_rate * Decimal(8)

            except ValueError:
                logger.warning(
                    "funding_rate_parse_failed",
                    symbol=str(raw_asset_ctx.name),
                    funding=raw_asset_ctx.funding,
                    message="Could not parse funding rate, setting to None",
                )

            # Calculate next funding time (start of next hour)
            now_utc = datetime.now(UTC)
            next_funding_time = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(
                hours=1,
            )

            # Parse additional HL-specific details
            impact_px = parse_decimal_value(
                raw_asset_ctx.impact_px,
                allow_none=True,
                field_name="impact_px",
            )

            # Create HL-specific details
            details = HyperliquidFundingDetails(
                hl_funding_hourly=hourly_funding_rate,
                hl_impact_px=impact_px,
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=str(raw_asset_ctx.name),  # Convert RawAssetString64HL to str
            )

            # Use secure_transform for type-safe model creation
            funding_data = {
                "symbol": exchange_symbol,  # Domain object!
                "timestamp": datetime.now(UTC).isoformat(),
                "funding_rate": str(funding_rate_8hr)
                if funding_rate_8hr is not None
                else None,  # 8-hour rate for compatibility
                "predicted_rate": None,
                "mark_price": str(mark_price) if mark_price is not None else None,
                "index_price": None,
                "next_funding_time": next_funding_time.isoformat(),
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            funding_rate_model = secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="hyperliquid_asset_ctx_funding_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_asset_ctx_to_funding_rate_transformed",
                symbol=str(raw_asset_ctx.name),
                funding_rate_8hr=str(funding_rate_8hr) if funding_rate_8hr else None,
                mark_price=str(mark_price) if mark_price else None,
                next_funding_time=next_funding_time.isoformat(),
                message="Successfully transformed HyperliquidRawAssetCtx to FundingRate",
            )
        except Exception as e:
            logger.exception(
                "asset_context_to_funding_rate_mapping_failed",
                symbol=str(raw_asset_ctx.name),
                raw_asset_ctx=raw_asset_ctx.model_dump() if raw_asset_ctx else None,
                error=str(e),
                message="Failed to transform HyperliquidRawAssetCtx to FundingRate",
            )
            return None
        else:
            return funding_rate_model

    @staticmethod
    def transform_raw_funding_history_item_to_internal(
        raw_funding_item: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transforms a HyperliquidRawFundingHistoryItem to an Internal FundingRate model.

        Args:
            raw_funding_item: Validated raw funding history item from Hyperliquid

        Returns:
            FundingRate: Internal domain model with HL details populated

        Raises:
            FundingRateTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_funding_history_item_to_internal",
                symbol=str(raw_funding_item.coin),
                funding_rate=raw_funding_item.funding_rate,
                time=raw_funding_item.time,
                message="Transforming HyperliquidRawFundingHistoryItem to FundingRate",
            )

            # Parse funding rate
            funding_rate = parse_decimal_value(
                raw_funding_item.funding_rate,
                allow_none=False,
                field_name="fundingRate",
            )

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_funding_item.time, field_name="time")
            timestamp = HyperliquidHistoricalDataMapper._ensure_funding_timestamp_not_none(
                timestamp,
            )

            # Validate funding data
            HyperliquidHistoricalDataMapper._validate_funding_data(
                funding_rate,
                timestamp,
                "HyperliquidRawFundingHistoryItem",
            )

            # Create domain symbol at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=str(raw_funding_item.coin),  # Convert RawAssetString64HL to str
            )

            # Create HL-specific details
            details = HyperliquidFundingDetails(
                # Add any HL-specific funding history fields here
            )

            # Use secure_transform for type-safe model creation
            funding_data = {
                "symbol": exchange_symbol,  # Domain object!
                "timestamp": timestamp.isoformat(),
                "funding_rate": str(funding_rate),
                "predicted_rate": None,
                "mark_price": None,
                "index_price": None,
                "next_funding_time": None,  # Not available in historical data
                "hl_details": details.model_dump() if details else None,
                "bp_details": None,
            }

            funding_rate_model = secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="hyperliquid_funding_history_transform",
                source_exchange="hyperliquid",
            )

            logger.debug(
                "raw_funding_history_item_to_internal_transformed",
                symbol=str(raw_funding_item.coin),
                funding_rate=str(funding_rate),
                timestamp=timestamp.isoformat(),
                message="Successfully transformed HyperliquidRawFundingHistoryItem to FundingRate",
            )
        except Exception as e:
            logger.exception(
                "funding_history_item_transform_failed",
                symbol=str(raw_funding_item.coin),
                raw_funding_item=raw_funding_item.model_dump() if raw_funding_item else None,
                error=str(e),
                message="Failed to transform HyperliquidRawFundingHistoryItem to FundingRate",
            )
            raise FundingRateTransformationError(
                source_type="HyperliquidRawFundingHistoryItem",
                reason=str(e),
                symbol=str(raw_funding_item.coin),
                original_error=e,
            ) from e
        else:
            return funding_rate_model

    @staticmethod
    def transform_raw_candle_snapshot_to_candles(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        symbol: Symbol,
        interval: str,
    ) -> list[Candle]:
        """Transforms a HyperliquidRawCandleSnapshot to a list of Internal Candle models.

        Args:
            raw_snapshot: Validated raw candle snapshot data from Hyperliquid
            symbol: Symbol for the candles
            interval: Time interval for the candles

        Returns:
            list[Candle]: List of internal domain models

        Raises:
            CandleTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_candle_snapshot_to_candles",
                symbol=symbol,
                interval=interval,
                candles_count=len(raw_snapshot.t) if raw_snapshot.t else 0,
                message="Transforming HyperliquidRawCandleSnapshot to Candles",
            )

            candles: list[Candle] = []

            # Check if we have any data
            if not raw_snapshot.t:
                logger.debug(
                    "empty_candle_snapshot",
                    symbol=symbol,
                    interval=interval,
                    message="Empty candle snapshot, returning empty list",
                )
                return candles

            # Iterate through parallel lists
            for i in range(len(raw_snapshot.t)):
                candle = HyperliquidHistoricalDataMapper._parse_single_candle(
                    raw_snapshot,
                    i,
                    symbol,
                    interval,
                )
                if candle:
                    candles.append(candle)

            logger.debug(
                "raw_candle_snapshot_to_candles_transformed",
                symbol=symbol,
                interval=interval,
                input_count=len(raw_snapshot.t),
                output_count=len(candles),
                message="Successfully transformed HyperliquidRawCandleSnapshot to Candles",
            )

        except Exception as e:
            logger.exception(
                "candle_snapshot_transform_failed",
                symbol=symbol,
                interval=interval,
                candles_count=len(raw_snapshot.t) if raw_snapshot and raw_snapshot.t else 0,
                error=str(e),
                message="Failed to transform HyperliquidRawCandleSnapshot to Candles",
            )
            raise CandleTransformationError(
                reason=str(e),
                symbol=symbol.value,
                interval=interval,
                original_error=e,
            ) from e
        else:
            return candles

    @staticmethod
    def _parse_single_candle(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        index: int,
        symbol: Symbol,
        interval: str,
    ) -> Candle | None:
        """Parse a single candle from the raw snapshot at the given index.

        Args:
            raw_snapshot: The raw candle snapshot containing OHLCV data.
            index: The index of the candle to parse.
            symbol: The symbol for the candle.
            interval: The time interval for the candle.

        Returns:
            A Candle object if the data is valid, None if the candle data is invalid and
            should be skipped.
        """
        # Parse OHLCV data from parallel lists
        ohlcv_prices = HyperliquidHistoricalDataMapper._parse_ohlcv_prices(raw_snapshot, index)
        if not ohlcv_prices:
            return None

        open_price, high_price, low_price, close_price, volume = ohlcv_prices

        # Parse timestamp (convert from milliseconds)
        timestamp = datetime.fromtimestamp(raw_snapshot.t[index] / 1000, tz=UTC)

        # Validate all prices are non-None
        HyperliquidHistoricalDataMapper._validate_candle_prices(
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
        )

        # Create and return candle
        return HyperliquidHistoricalDataMapper._create_candle_from_data(
            symbol,
            interval,
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
        )

    @staticmethod
    def _parse_ohlcv_prices(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        index: int,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal] | None:
        """Parse OHLCV prices from raw snapshot at given index.

        Args:
            raw_snapshot: The raw candle snapshot containing OHLCV data.
            index: The index of the candle to parse.

        Returns:
            A tuple of (open, high, low, close, volume) prices if all are valid,
            None if any price is invalid.
        """
        try:
            open_price = parse_decimal_value(
                raw_snapshot.o[index],
                allow_none=False,
                field_name="o",
            )
            high_price = parse_decimal_value(
                raw_snapshot.h[index],
                allow_none=False,
                field_name="h",
            )
            low_price = parse_decimal_value(raw_snapshot.l[index], allow_none=False, field_name="l")
            close_price = parse_decimal_value(
                raw_snapshot.c[index],
                allow_none=False,
                field_name="c",
            )
            volume = parse_decimal_value(raw_snapshot.v[index], allow_none=False, field_name="v")
        except (ValueError, TypeError, IndexError):
            logger.warning(
                "invalid_candle_data_skipped",
                index=index,
                message=f"Skipping candle at index {index} with invalid OHLCV data",
            )
            return None

        if None in {open_price, high_price, low_price, close_price, volume}:
            logger.warning(
                "invalid_candle_data_skipped",
                index=index,
                message=f"Skipping candle at index {index} with invalid OHLCV data",
            )
            return None

        # At this point, all values are guaranteed to be non-None
        return open_price, high_price, low_price, close_price, volume

    @staticmethod
    def _validate_candle_prices(
        open_price: Decimal | None,
        high_price: Decimal | None,
        low_price: Decimal | None,
        close_price: Decimal | None,
        volume: Decimal | None,
    ) -> None:
        """Validate that all candle prices are non-None after parsing.

        Args:
            open_price: The open price of the candle.
            high_price: The high price of the candle.
            low_price: The low price of the candle.
            close_price: The close price of the candle.
            volume: The volume of the candle.

        Raises:
            MissingRequiredFieldError: If any of the required price fields are None.
        """
        missing_fields: list[str] = []
        if open_price is None:
            missing_fields.append("open_price")
        if high_price is None:
            missing_fields.append("high_price")
        if low_price is None:
            missing_fields.append("low_price")
        if close_price is None:
            missing_fields.append("close_price")
        if volume is None:
            missing_fields.append("volume")

        if missing_fields:
            raise MissingRequiredFieldError(missing_fields, "candle after validation")

    @staticmethod
    def _create_candle_from_data(
        symbol: Symbol,
        interval: str,
        timestamp: datetime,
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal,
        volume: Decimal,
    ) -> Candle:
        """Create a Candle object from the provided data.

        Returns:
            Candle: The created candle object
        """
        candle_data = {
            "symbol": symbol,  # Already a Symbol object!
            "interval": interval,
            "open_time": timestamp.isoformat(),
            "open": str(open_price),
            "high": str(high_price),
            "low": str(low_price),
            "close": str(close_price),
            "volume": str(volume),
        }

        return secure_transform(
            data=candle_data,
            model_class=Candle,
            context="hyperliquid_candle_transform",
            source_exchange="hyperliquid",
        )
