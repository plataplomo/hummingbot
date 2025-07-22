"""Backpack Ticker Mapper.

This mapper handles transformations for ticker-related data from the Backpack exchange.

Focused on:
- REST ticker data transformations
- WebSocket ticker event transformations
- Ticker-specific data validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawTicker,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import TickerMapperProtocol
from cyberdelta.apis.exceptions import TickerTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Ticker
from cyberdelta.core.models.market.ticker import BackpackTickerDetails
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackTickerMapper(TickerMapperProtocol):
    """Focused mapper for Backpack ticker data transformations.

    This class contains static methods for transforming validated Backpack Raw ticker models
    into CyberDeltaEngine Internal Ticker Domain Models.
    """

    @staticmethod
    def transform_raw_ticker_to_internal(
        raw_ticker: BackpackRawTicker,
        symbol_override: str | None = None,
    ) -> Ticker:
        """Transform a BackpackRawTicker to an Internal Ticker model.

        Args:
            raw_ticker: Validated raw ticker data from Backpack
            symbol_override: Optional symbol override for the ticker

        Returns:
            Ticker: Internal domain model with populated fields

        Raises:
            TickerTransformationError: If transformation fails

        """
        symbol = symbol_override  # Initialize for exception handling
        try:
            # Use symbol override if provided, otherwise use raw ticker symbol
            symbol = symbol_override or raw_ticker.symbol

            # Validate symbol format
            if symbol and not BackpackCommonMappers.is_valid_symbol(symbol):
                logger.warning(
                    "invalid_symbol_format",
                    symbol=symbol,
                    expected_format="BASE_QUOTE",
                    context="ticker_transform",
                    message="Invalid Backpack symbol format in ticker data",
                )

            # Parse core ticker fields using new model structure
            last_price = parse_decimal_value(
                raw_ticker.last_price,
                allow_none=False,
                field_name="lastPrice",
            )
            volume_24h = parse_decimal_value(
                raw_ticker.volume,
                allow_none=False,
                field_name="volume",
            )

            # Parse extension fields for BackpackTickerDetails
            first_price = parse_decimal_value(
                raw_ticker.first_price,
                allow_none=False,
                field_name="firstPrice",
            )
            high_price = parse_decimal_value(raw_ticker.high, allow_none=False, field_name="high")
            low_price = parse_decimal_value(raw_ticker.low, allow_none=False, field_name="low")
            price_change = parse_decimal_value(
                raw_ticker.price_change,
                allow_none=False,
                field_name="priceChange",
            )
            price_change_percent = parse_decimal_value(
                raw_ticker.price_change_percent,
                allow_none=False,
                field_name="priceChangePercent",
            )
            quote_volume = parse_decimal_value(
                raw_ticker.quote_volume,
                allow_none=False,
                field_name="quoteVolume",
            )

            # Parse trade count
            trades_count = None
            if raw_ticker.trades:
                try:
                    trades_count = int(raw_ticker.trades)
                except (ValueError, TypeError):
                    logger.warning(
                        "trades_count_parse_failed",
                        raw_trades_count=raw_ticker.trades,
                        symbol=symbol,
                        message="Failed to parse trades count for symbol",
                    )

            # Generate timestamp since API doesn't provide it
            timestamp = datetime.now(UTC)

            # Create Backpack-specific extension details
            bp_details = BackpackTickerDetails(
                first_price=first_price,
                high=high_price,
                low=low_price,
                price_change=price_change,
                price_change_percent=price_change_percent,
                quote_volume=quote_volume,
                trades=trades_count,
            )

            # Use secure_transform for type-safe model creation
            ticker_data: dict[str, Any] = {
                "symbol": symbol,
                "exchange": "backpack",  # Required field for Ticker model
                "timestamp": timestamp.isoformat(),
                "price": str(last_price),  # Map lastPrice to core price field
                "bid": None,  # Not available from Backpack ticker endpoint
                "ask": None,  # Not available from Backpack ticker endpoint
                "volume": str(volume_24h),
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=ticker_data,
                model_class=Ticker,
                context="backpack_ticker_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TickerTransformationError(
                ticker_source="BackpackRawTicker",
                reason=str(e),
                symbol=symbol,
                original_error=e,
            ) from e

    @staticmethod
    def transform_ws_ticker_event_to_internal(raw_ticker: BackpackRawTickerEvent) -> Ticker:
        """Transform a BackpackRawTickerEvent to an Internal Ticker model.

        Args:
            raw_ticker: Validated raw ticker event data from Backpack WebSocket

        Returns:
            Ticker: Internal domain model with populated fields

        Raises:
            TickerTransformationError: If transformation fails

        """
        try:
            # Validate symbol format
            if raw_ticker.symbol and not BackpackCommonMappers.is_valid_symbol(raw_ticker.symbol):
                logger.warning(
                    "invalid_symbol_format",
                    symbol=raw_ticker.symbol,
                    expected_format="BASE_QUOTE",
                    context="ws_ticker_transform",
                    message="Invalid Backpack symbol format in WebSocket ticker event",
                )

            # Parse core ticker fields
            last_price = parse_decimal_value(
                raw_ticker.last_price,
                allow_none=True,
                field_name="lastPrice",
            )
            volume_24h = parse_decimal_value(
                raw_ticker.volume,
                allow_none=True,
                field_name="volume",
            )

            # Parse timestamp from event_time
            timestamp = parse_datetime_utc(raw_ticker.event_time, field_name="event_time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Use secure_transform for type-safe model creation
            ticker_data: dict[str, Any] = {
                "symbol": raw_ticker.symbol,
                "exchange": "backpack",  # Required field for Ticker model
                "timestamp": timestamp.isoformat(),
                "price": str(last_price) if last_price is not None else None,
                "bid": None,  # Not available in ticker event
                "ask": None,  # Not available in ticker event
                "volume": str(volume_24h),
                "bp_details": None,
                "hl_details": None,
            }

            return secure_transform(
                data=ticker_data,
                model_class=Ticker,
                context="backpack_ws_ticker_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TickerTransformationError(
                ticker_source="BackpackRawTickerEvent",
                reason=str(e),
                symbol=raw_ticker.symbol,
                original_error=e,
            ) from e

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
