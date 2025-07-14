"""Backpack Funding Rate Mapper.

This mapper handles transformations for funding rate data from the Backpack exchange.

Focused on:
- Funding rate data transformations
- Funding interval rate transformations
- Funding rate-specific data validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import FundingRateMapperProtocol
from cyberdelta.apis.exceptions import (
    DataTransformationError,
    FundingRateTransformationError,
    MissingRequiredFieldError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.funding_rate import BackpackFundingDetails, FundingRate
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackFundingRateMapper(FundingRateMapperProtocol):
    """Focused mapper for Backpack funding rate data transformations.

    This class contains static methods for transforming validated Backpack Raw funding rate models
    into CyberDeltaEngine Internal FundingRate Domain Models.
    """

    @staticmethod
    def _validate_funding_rate_data(funding_rate: object, context: str) -> object:
        """Validate funding rate data.

        Args:
            funding_rate: Raw funding rate value
            context: Context for error messages

        Returns:
            object: Validated funding rate

        Raises:
            MissingRequiredFieldError: If funding rate is missing
        """
        if funding_rate is None:
            raise MissingRequiredFieldError("funding_rate", context)
        return funding_rate

    @staticmethod
    def _validate_funding_timestamp(timestamp: object, context: str) -> object:
        """Validate funding rate timestamp.

        Args:
            timestamp: Raw timestamp value
            context: Context for error messages

        Returns:
            object: Validated timestamp

        Raises:
            MissingRequiredFieldError: If timestamp is None
        """
        if timestamp is None:
            raise MissingRequiredFieldError("timestamp", context)
        return timestamp

    @staticmethod
    def _ensure_timestamp_not_none(timestamp: datetime | None, source_data: object) -> datetime:
        """Ensure timestamp is not None after validation.

        Args:
            timestamp: Parsed timestamp
            source_data: Source data for error context

        Returns:
            The validated non-None timestamp

        Raises:
            DataTransformationError: If timestamp is None
        """
        if timestamp is None:
            raise DataTransformationError(
                source_model="BackpackRawFundingRate.time",
                target_model="datetime",
                reason="timestamp should not be None after validation",
                source_data=source_data,
            )
        return timestamp

    @staticmethod
    def transform_raw_funding_rate_to_internal(raw_funding: BackpackRawFundingRate) -> FundingRate:
        """Transform a BackpackRawFundingRate to an Internal FundingRate model.

        Args:
            raw_funding: Validated raw funding rate data from Backpack

        Returns:
            FundingRate: Internal domain model with populated fields and BP details

        Raises:
            FundingRateTransformationError: If transformation fails

        """
        try:
            # Parse funding rate
            funding_rate = parse_decimal_value(
                raw_funding.funding_rate,
                allow_none=False,
                field_name="fundingRate",
            )
            BackpackFundingRateMapper._validate_funding_rate_data(
                funding_rate, "BackpackRawFundingRate"
            )

            # Parse timestamp
            timestamp = parse_datetime_utc(raw_funding.time, field_name="time")
            if timestamp is None:
                timestamp = datetime.now(UTC)

            # Parse mark price and index price
            mark_price = parse_decimal_value(
                raw_funding.mark_price,
                allow_none=True,
                field_name="markPrice",
            )
            index_price = parse_decimal_value(
                raw_funding.index_price,
                allow_none=True,
                field_name="indexPrice",
            )

            # Create BP-specific details
            details = BackpackFundingDetails()

            # Use secure_transform for type-safe model creation
            funding_data: dict[str, Any] = {
                "symbol": raw_funding.symbol,
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
                source_exchange="backpack",
            )

        except Exception as e:
            raise FundingRateTransformationError(
                source_type="BackpackRawFundingRate",
                reason=str(e),
                symbol=raw_funding.symbol,
                original_error=e,
            ) from e

    @staticmethod
    def transform_raw_funding_interval_rate_to_internal(
        raw_funding: BackpackRawFundingIntervalRate,
        symbol: str,
    ) -> FundingRate:
        """Transform a BackpackRawFundingIntervalRate to an Internal FundingRate model.

        Args:
            raw_funding: Validated raw funding interval rate data from Backpack
            symbol: Symbol for the funding rate

        Returns:
            FundingRate: Internal domain model with populated fields and BP details

        Raises:
            FundingRateTransformationError: If transformation fails

        """
        try:
            # Parse funding rate
            funding_rate = parse_decimal_value(
                raw_funding.rate,
                allow_none=False,
                field_name="rate",
            )
            BackpackFundingRateMapper._validate_funding_rate_data(
                funding_rate, "BackpackRawFundingIntervalRate"
            )

            # Parse timestamp (time is now an ISO datetime string)
            timestamp = parse_datetime_utc(raw_funding.time, field_name="time")
            BackpackFundingRateMapper._validate_funding_timestamp(
                timestamp, "BackpackRawFundingIntervalRate"
            )

            # Ensure timestamp is not None after validation and get the validated value
            timestamp = BackpackFundingRateMapper._ensure_timestamp_not_none(
                timestamp, raw_funding.time
            )

            # Create BP-specific details
            details = BackpackFundingDetails()

            # Use secure_transform for type-safe model creation
            funding_data: dict[str, Any] = {
                "symbol": symbol,
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
                source_exchange="backpack",
            )

        except Exception as e:
            raise FundingRateTransformationError(
                source_type="BackpackRawFundingIntervalRate",
                reason=str(e),
                symbol=symbol,
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
