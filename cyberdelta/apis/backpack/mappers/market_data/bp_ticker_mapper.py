"""Backpack Ticker Mapper.

This mapper handles transformations for ticker-related data from the Backpack exchange.

Focused on:
- REST ticker data transformations
- WebSocket ticker event transformations
- Ticker-specific data validation and error handling
"""

from datetime import UTC, datetime
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawTickerEvent,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import TickerMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin
from cyberdelta.apis.exceptions import TickerTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.models import Ticker
from cyberdelta.models.market.ticker import BackpackTickerDetails
from cyberdelta.symbols import exchanges
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackTickerMapper(CommonDataParserMixin, TickerMapperProtocol):
    """Focused mapper for Backpack ticker data transformations.

    This class contains static methods for transforming validated Backpack Raw ticker models
    into CyberDeltaEngine Internal Ticker Domain Models.
    """

    def transform_raw_ticker_to_internal(
        self,
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
            last_price = self.parse_decimal_safely(raw_ticker.last_price)
            volume_24h = self.parse_decimal_safely(raw_ticker.volume)

            # Parse extension fields for BackpackTickerDetails
            first_price = self.parse_decimal_safely(raw_ticker.first_price)
            high_price = self.parse_decimal_safely(raw_ticker.high)
            low_price = self.parse_decimal_safely(raw_ticker.low)
            price_change = self.parse_decimal_safely(raw_ticker.price_change)
            price_change_percent = self.parse_decimal_safely(raw_ticker.price_change_percent)
            quote_volume = self.parse_decimal_safely(raw_ticker.quote_volume)

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
                source_exchange=ExchangeName.BACKPACK,
            )

        except Exception as e:
            raise TickerTransformationError(
                ticker_source="BackpackRawTickerResponse",
                reason=str(e),
                symbol=symbol,
                original_error=e,
            ) from e

    def transform_ws_ticker_event_to_internal(self, raw_ticker: BackpackRawTickerEvent) -> Ticker:
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
            last_price = self.parse_decimal_safely(raw_ticker.last_price, default=None)
            volume_24h = self.parse_decimal_safely(raw_ticker.volume, default=None)

            # Parse timestamp from event_time
            timestamp = self.parse_timestamp(raw_ticker.event_time)
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
                source_exchange=ExchangeName.BACKPACK,
            )

        except Exception as e:
            raise TickerTransformationError(
                ticker_source="BackpackRawTickerEvent",
                reason=str(e),
                symbol=raw_ticker.symbol,
                original_error=e,
            ) from e
