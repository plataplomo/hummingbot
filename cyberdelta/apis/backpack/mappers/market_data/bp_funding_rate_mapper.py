"""Backpack Funding Rate Mapper.

This mapper handles transformations for funding rate data from the Backpack exchange.

Focused on:
- Funding rate data transformations
- Funding interval rate transformations
- Funding rate-specific data validation and error handling
"""

from datetime import UTC, datetime
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRateResponse,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import FundingRateMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin, ValidationMixin
from cyberdelta.apis.exceptions import (
    DataTransformationError,
    FundingRateTransformationError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.funding_rate import BackpackFundingDetails, FundingRate
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackFundingRateMapper(CommonDataParserMixin, ValidationMixin, FundingRateMapperProtocol):
    """Focused mapper for Backpack funding rate data transformations.

    This class contains static methods for transforming validated Backpack Raw funding rate models
    into CyberDeltaEngine Internal FundingRate Domain Models.
    """

    def transform_raw_funding_rate_to_internal(
        self,
        raw_funding: BackpackRawFundingRateResponse,
    ) -> FundingRate:
        """Transform a BackpackRawFundingRateResponse to an Internal FundingRate model.

        Args:
            raw_funding: Validated raw funding rate data from Backpack

        Returns:
            FundingRate: Internal domain model with populated fields and BP details

        Raises:
            FundingRateTransformationError: If transformation fails

        """
        try:
            # Parse funding rate
            funding_rate = self.parse_decimal_safely(raw_funding.funding_rate)
            funding_rate = self.ensure_decimal_not_none(
                funding_rate,
                "funding_rate",
                "BackpackRawFundingRateResponse",
            )

            # Parse timestamp
            timestamp = self.parse_timestamp(raw_funding.time)
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Parse mark price and index price
            mark_price = self.parse_decimal_safely(raw_funding.mark_price)
            index_price = self.parse_decimal_safely(raw_funding.index_price)

            # Create BP-specific details
            details = BackpackFundingDetails()

            # Create domain symbol at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_funding.symbol,
            )

            # Use secure_transform for type-safe model creation
            funding_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
                "timestamp": timestamp.isoformat(),
                "funding_rate": str(funding_rate),
                "mark_price": str(mark_price) if mark_price is not None else None,
                "index_price": str(index_price) if index_price is not None else None,
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="backpack_funding_rate_transform",
                source_exchange=ExchangeName.BACKPACK,
            )

        except Exception as e:
            raise FundingRateTransformationError(
                source_type="BackpackRawFundingRateResponse",
                reason=str(e),
                symbol=raw_funding.symbol,
                original_error=e,
            ) from e

    def transform_raw_funding_interval_rate_to_internal(
        self,
        raw_funding: BackpackRawFundingIntervalRate,
        symbol: Symbol,
    ) -> FundingRate:
        """Transform a BackpackRawFundingIntervalRate to an Internal FundingRate model.

        Args:
            raw_funding: Validated raw funding interval rate data from Backpack
            symbol: Symbol domain object for the funding rate

        Returns:
            FundingRate: Internal domain model with populated fields and BP details

        Raises:
            FundingRateTransformationError: If transformation fails

        """
        try:
            # Parse funding rate
            funding_rate = self.parse_decimal_safely(raw_funding.rate)
            funding_rate = self.ensure_decimal_not_none(
                funding_rate,
                "funding_rate",
                "BackpackRawFundingIntervalRate",
            )

            # Parse timestamp (time is now an ISO datetime string)
            timestamp = self.parse_timestamp(raw_funding.time)
            if timestamp is None:
                self._raise_timestamp_validation_error()
                # This line is unreachable but helps with type narrowing
                timestamp = datetime.now(UTC)

            # Use the Symbol object directly (no need to create another)
            exchange_symbol = symbol

            # Create BP-specific details
            details = BackpackFundingDetails()

            # Use secure_transform for type-safe model creation
            funding_data: dict[str, Any] = {
                "symbol": exchange_symbol,  # Domain object!
                "timestamp": timestamp.isoformat(),
                "funding_rate": str(funding_rate),
                "mark_price": None,
                "index_price": None,
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=funding_data,
                model_class=FundingRate,
                context="backpack_funding_interval_transform",
                source_exchange=ExchangeName.BACKPACK,
            )

        except Exception as e:
            raise FundingRateTransformationError(
                source_type="BackpackRawFundingIntervalRate",
                reason=str(e),
                symbol=symbol.value,  # Convert Symbol to string for error
                original_error=e,
            ) from e

    def _raise_timestamp_validation_error(self) -> None:
        """Raise DataTransformationError for timestamp validation failure.

        Raises:
            DataTransformationError: Always raised for timestamp validation failure
        """
        raise DataTransformationError(
            source_model="BackpackRawFundingIntervalRate",
            target_model="FundingRate",
            reason="Timestamp is not a datetime object after validation",
        )
