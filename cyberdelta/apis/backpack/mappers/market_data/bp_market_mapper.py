"""Backpack Market Mapper.

This mapper handles transformations for market metadata from the Backpack exchange.

Focused on:
- Market configuration and metadata transformations
- Market filter and limit data processing
- Market-specific data validation and error handling
"""

from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawMarketResponse
from cyberdelta.apis.backpack.protocols.mapper_protocols import MarketMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin
from cyberdelta.apis.exceptions import MarketTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import exchanges
from cyberdelta.models.market import Market
from cyberdelta.models.market.market import BackpackMarketDetails
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackMarketMapper(CommonDataParserMixin, MarketMapperProtocol):
    """Focused mapper for Backpack market metadata transformations.

    This class contains static methods for transforming validated Backpack Raw market models
    into CyberDeltaEngine Internal Market Domain Models.
    """

    def transform_raw_market_to_internal(self, raw_market: BackpackRawMarketResponse) -> Market:
        """Transform a BackpackRawMarketResponse to an Internal Market model.

        Args:
            raw_market: Validated raw market data from Backpack

        Returns:
            Market: Internal domain model with populated fields

        Raises:
            MarketTransformationError: If transformation fails

        """
        try:
            # Parse core market fields (required, won't be None since allow_none=False)
            tick_size = self.parse_decimal_safely(raw_market.filters.price.tick_size)
            step_size = self.parse_decimal_safely(raw_market.filters.quantity.step_size)

            # Parse optional price limits
            min_price = self.parse_decimal_safely(raw_market.filters.price.min_price, default=None)
            max_price = self.parse_decimal_safely(raw_market.filters.price.max_price, default=None)

            # Parse optional quantity limits
            min_quantity = self.parse_decimal_safely(
                raw_market.filters.quantity.min_quantity, default=None
            )
            max_quantity = self.parse_decimal_safely(
                raw_market.filters.quantity.max_quantity, default=None
            )

            # Parse created_at timestamp
            created_at = self.parse_timestamp(raw_market.created_at)

            # Create Backpack-specific details
            bp_details = BackpackMarketDetails(
                order_book_state=raw_market.order_book_state,
                created_at_raw=raw_market.created_at,
            )

            # Parse symbol to domain object at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_market.symbol,  # e.g., "BTC_USDC"
                symbol_id=getattr(raw_market, "symbol_id", None),
            )

            # Use secure_transform for type-safe model creation
            market_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
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
