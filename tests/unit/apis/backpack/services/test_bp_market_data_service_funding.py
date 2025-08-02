"""Unit tests for BackpackMarketDataService funding rate functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args.market_data import GetHistoricalFundingRatesArgs
from cyberdelta.core.models.market import FundingRate
from cyberdelta.core.models.market.funding_rate import BackpackFundingDetails
from tests.common_symbols import SOL_BP


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_market_data"]


class TestBackpackMarketDataServiceFunding:
    """Tests for the BackpackMarketDataService funding rate functionality."""

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate successfully retrieves and processes funding rate data."""
        symbol = SOL_BP.value
        raw_time_str = "2023-10-27T10:00:00"
        expected_internal_funding_rate = FundingRate(
            symbol=symbol,
            timestamp=datetime.fromisoformat(raw_time_str),
            funding_rate=Decimal("0.0001"),
            bp_details=BackpackFundingDetails(),
        )

        # Mock the market metadata service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            # Mock async method to return a coroutine
            mock_metadata_service.get_funding_rates = AsyncMock(
                return_value=[expected_internal_funding_rate]
            )
            result_funding_rate = await backpack_market_data_service.get_funding_rate(symbol)

            # Verify the business logic calls the market metadata service with correct arguments
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]
            assert result_funding_rate == expected_internal_funding_rate

    @pytest.mark.asyncio
    async def test_get_funding_rate_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate when market metadata service returns empty list."""
        symbol = SOL_BP.value

        # Mock the market metadata service to return empty list (no funding rates)
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_funding_rates = AsyncMock(return_value=[])

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_funding_rate(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_SYMBOL.value
            assert f"No funding rate found for symbol {symbol}" in exc_info.value.message

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]

    @pytest.mark.asyncio
    async def test_get_funding_rate_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate handles validation error from market metadata service."""
        symbol = SOL_BP.value

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawFundingIntervalRate.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the market metadata service to raise a validation error
            with patch.object(
                backpack_market_data_service, "_market_metadata_service"
            ) as mock_metadata_service:
                mock_metadata_service.get_funding_rates = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_funding_rate(symbol)

                # Verify the business logic calls the market metadata service
                mock_metadata_service.get_funding_rates.assert_called_once()
                call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
                assert call_args.symbols == [symbol]

    @pytest.mark.asyncio
    async def test_get_funding_rate_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate handles unexpected exception from market metadata service."""
        symbol = SOL_BP.value

        # Mock the market metadata service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_funding_rates = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_funding_rate(symbol)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates success.

        Note: Current business logic delegates to market metadata service's get_funding_rates method
        rather than making direct historical funding rate calls.
        """
        symbol = SOL_BP.value
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000
        limit = 10

        # Create expected internal funding rates
        expected_funding_rates = [
            FundingRate(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(1678880000, tz=UTC),
                funding_rate=Decimal("0.0001"),
                bp_details=BackpackFundingDetails(),
            ),
            FundingRate(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(1678883600, tz=UTC),
                funding_rate=Decimal("0.0002"),
                bp_details=BackpackFundingDetails(),
            ),
        ]

        # Mock the market metadata service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_funding_rates = AsyncMock(return_value=expected_funding_rates)

            args = GetHistoricalFundingRatesArgs(
                symbol=symbol,
                start_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                end_time=datetime.fromtimestamp(end_time_ms / 1000, tz=UTC),
                limit=limit,
            )
            result = await backpack_market_data_service.get_historical_funding_rates(args)

            # Verify the business logic calls the market metadata service with correct arguments
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]
            assert result == expected_funding_rates

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when market metadata service returns empty list."""
        symbol = SOL_BP.value
        start_time_ms = 1678880000000
        limit = 5

        # Mock the market metadata service to return empty list (no funding rates)
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_funding_rates = AsyncMock(return_value=[])

            args = GetHistoricalFundingRatesArgs(
                symbol=symbol,
                start_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                limit=limit,
            )
            result = await backpack_market_data_service.get_historical_funding_rates(args)

            # Business logic should return empty list when no rates found
            assert result == []

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles validation error from metadata service."""
        symbol = SOL_BP.value

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawFundingIntervalRate.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the market metadata service to raise a validation error
            with patch.object(
                backpack_market_data_service, "_market_metadata_service"
            ) as mock_metadata_service:
                mock_metadata_service.get_funding_rates = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    args = GetHistoricalFundingRatesArgs(symbol=symbol)
                    await backpack_market_data_service.get_historical_funding_rates(args)

                # Verify the business logic calls the market metadata service
                mock_metadata_service.get_funding_rates.assert_called_once()
                call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
                assert call_args.symbols == [symbol]

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles unexpected exception from metadata service."""
        symbol = SOL_BP.value

        # Mock the market metadata service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_funding_rates = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            with pytest.raises(Exception) as exc_info:
                args = GetHistoricalFundingRatesArgs(symbol=symbol)
                await backpack_market_data_service.get_historical_funding_rates(args)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_with_optional_parameters(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates with only symbol parameter."""
        symbol = SOL_BP.value

        # Create expected internal funding rate
        expected_funding_rate = FundingRate(
            symbol=symbol,
            timestamp=datetime.fromisoformat("2023-10-27T10:00:00+00:00"),
            funding_rate=Decimal("0.0001"),
            bp_details=BackpackFundingDetails(),
        )

        # Mock the market metadata service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_funding_rates = AsyncMock(
                return_value=[expected_funding_rate]
            )

            args = GetHistoricalFundingRatesArgs(symbol=symbol)
            result = await backpack_market_data_service.get_historical_funding_rates(args)

            # Verify the business logic calls the market metadata service with correct arguments
            mock_metadata_service.get_funding_rates.assert_called_once()
            call_args = mock_metadata_service.get_funding_rates.call_args[0][0]
            assert call_args.symbols == [symbol]
            assert result == [expected_funding_rate]
