"""Backpack Candle Mapper.

This mapper handles transformations for candle/kline data from the Backpack exchange.

Focused on:
- Kline data transformations
- OHLCV data validation and processing
- Candle-specific data validation and error handling
"""

from datetime import UTC, datetime
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.backpack.protocols.mapper_protocols import CandleMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin, ValidationMixin
from cyberdelta.apis.exceptions import CandleTransformationError, MissingRequiredFieldError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market import Candle
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackCandleMapper(CommonDataParserMixin, ValidationMixin, CandleMapperProtocol):
    """Focused mapper for Backpack candle data transformations.

    This class contains static methods for transforming validated Backpack Raw kline models
    into CyberDeltaEngine Internal Candle Domain Models.
    """

    @staticmethod
    def _validate_candle_data(
        open_price: object,
        high_price: object,
        low_price: object,
        close_price: object,
        volume: object,
        symbol: str,
    ) -> None:
        """Validate candle OHLCV data.

        Args:
            open_price: Open price value
            high_price: High price value
            low_price: Low price value
            close_price: Close price value
            volume: Volume value
            symbol: Symbol for context

        Raises:
            MissingRequiredFieldError: If any OHLCV value is None
        """
        if any(val is None for val in [open_price, high_price, low_price, close_price, volume]):
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

            raise MissingRequiredFieldError(missing_fields, f"candle for {symbol}")

    def transform_raw_kline_to_internal(
        self,
        symbol: Symbol,
        interval: str,
        raw_kline: BackpackRawKlineResponse,
    ) -> Candle:
        """Transform a BackpackRawKlineResponse to an Internal Candle model.

        Args:
            symbol: Symbol domain object for the candle
            interval: Time interval for the candle
            raw_kline: Validated raw kline data from Backpack

        Returns:
            Candle: Internal domain model with populated fields

        Raises:
            CandleTransformationError: If transformation fails

        """
        try:
            # Parse OHLCV data using correct field names
            open_price = self.parse_decimal_safely(raw_kline.open_price)
            high_price = self.parse_decimal_safely(raw_kline.high_price)
            low_price = self.parse_decimal_safely(raw_kline.low_price)
            close_price = self.parse_decimal_safely(raw_kline.close_price)
            volume = self.parse_decimal_safely(raw_kline.volume)

            # Validate all OHLCV values are present
            BackpackCandleMapper._validate_candle_data(
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                symbol.value,
            )

            # Parse timestamp from start_time_ms using common mapper utility
            open_time = self.timestamp_ms_to_datetime(raw_kline.start_time_ms)
            if not open_time:
                # Fallback to current time if conversion fails
                open_time = datetime.now(UTC)
                logger.warning(
                    "timestamp_conversion_failed",
                    start_time_ms=raw_kline.start_time_ms,
                    symbol=symbol.value,
                    message="Failed to convert kline timestamp, using current time",
                )

            # Use the Symbol object directly (no need to create another)
            exchange_symbol = symbol

            # Use secure_transform for type-safe model creation
            candle_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
                "interval": interval,
                "open_time": open_time.isoformat(),
                "open": str(open_price),
                "high": str(high_price),
                "low": str(low_price),
                "close": str(close_price),
                "volume": str(volume),
            }

            return secure_transform(
                data=candle_data,
                model_class=Candle,
                context="backpack_kline_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise CandleTransformationError(
                reason=str(e),
                symbol=symbol.value,  # Convert Symbol to string for error
                interval=interval,
                original_error=e,
            ) from e

    # MapperProtocol methods
