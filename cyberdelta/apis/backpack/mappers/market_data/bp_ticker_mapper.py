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
    BackpackRawTickerEvent,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import TickerMapperProtocol
from cyberdelta.apis.exceptions import TickerTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Ticker
from cyberdelta.core.models.market.ticker import BackpackTickerDetails
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import ExchangeName
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
        raw_ticker: BackpackRawTickerResponse,
        symbol_override: str | None = None,
    ) -> Ticker:
        """Transform a BackpackRawTickerResponse to an Internal Ticker model.

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

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(value=symbol)

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
                "symbol": exchange_symbol,  # Domain object!
                "exchange": ExchangeName.BACKPACK.value,
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
                ticker_source="BackpackRawTickerResponse",
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

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(value=raw_ticker.symbol)

            # Use secure_transform for type-safe model creation
            ticker_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
                "exchange": ExchangeName.BACKPACK.value,
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
