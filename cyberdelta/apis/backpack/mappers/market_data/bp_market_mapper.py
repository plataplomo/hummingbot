"""Backpack Market Mapper.

This mapper handles transformations for market metadata from the Backpack exchange.

Focused on:
- Market configuration and metadata transformations
- Market filter and limit data processing
- Market-specific data validation and error handling
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawMarket
from cyberdelta.apis.backpack.protocols.mapper_protocols import MarketMapperProtocol
from cyberdelta.apis.exceptions import MarketTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import Market
from cyberdelta.core.models.market.market import BackpackMarketDetails
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackMarketMapper(MarketMapperProtocol):
    """Focused mapper for Backpack market metadata transformations.

    This class contains static methods for transforming validated Backpack Raw market models
    into CyberDeltaEngine Internal Market Domain Models.
    """

    @staticmethod
    def transform_raw_market_to_internal(raw_market: BackpackRawMarket) -> Market:
        """Transform a BackpackRawMarket to an Internal Market model.

        Args:
            raw_market: Validated raw market data from Backpack

        Returns:
            Market: Internal domain model with populated fields

        Raises:
            MarketTransformationError: If transformation fails

        """
        try:
            # Parse core market fields (required, won't be None since allow_none=False)
            tick_size = parse_decimal_value(
                raw_market.filters.price.tick_size,
                allow_none=False,
                field_name="tickSize",
            )
            step_size = parse_decimal_value(
                raw_market.filters.quantity.step_size,
                allow_none=False,
                field_name="stepSize",
            )

            # Parse optional price limits
            min_price = parse_decimal_value(
                raw_market.filters.price.min_price,
                allow_none=True,
                field_name="minPrice",
            )
            max_price = parse_decimal_value(
                raw_market.filters.price.max_price,
                allow_none=True,
                field_name="maxPrice",
            )

            # Parse optional quantity limits
            min_quantity = parse_decimal_value(
                raw_market.filters.quantity.min_quantity,
                allow_none=True,
                field_name="minQuantity",
            )
            max_quantity = parse_decimal_value(
                raw_market.filters.quantity.max_quantity,
                allow_none=True,
                field_name="maxQuantity",
            )

            # Parse created_at timestamp
            created_at = None
            if raw_market.created_at:
                created_at = parse_datetime_utc(raw_market.created_at, field_name="createdAt")

            # Create Backpack-specific details
            bp_details = BackpackMarketDetails(
                order_book_state=raw_market.order_book_state,
                created_at_raw=raw_market.created_at,
            )

            # Use secure_transform for type-safe model creation
            market_data: dict[str, Any] = {
                "symbol": raw_market.symbol,
                "base_symbol": raw_market.base_symbol,
                "quote_symbol": raw_market.quote_symbol,
                "market_type": raw_market.market_type,
                "tick_size": str(tick_size),
                "step_size": str(step_size),
                "min_price": str(min_price) if min_price is not None else None,
                "max_price": str(max_price) if max_price is not None else None,
                "min_quantity": str(min_quantity) if min_quantity is not None else None,
                "max_quantity": str(max_quantity) if max_quantity is not None else None,
                "status": raw_market.order_book_state,
                "created_at": created_at.isoformat() if created_at is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=market_data,
                model_class=Market,
                context="backpack_market_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise MarketTransformationError(
                reason=str(e),
                symbol=raw_market.symbol,
                original_error=e,
            ) from e

    # MapperProtocol methods
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Parse decimal values safely using BackpackCommonMappers."""
        return BackpackCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize symbol format using BackpackCommonMappers."""
        return BackpackCommonMappers.normalize_symbol(symbol)

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Denormalize symbol format using BackpackCommonMappers."""
        return BackpackCommonMappers.denormalize_symbol(symbol)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert timestamp to datetime using BackpackCommonMappers."""
        return BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
