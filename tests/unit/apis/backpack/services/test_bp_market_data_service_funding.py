"""Unit tests for BackpackMarketDataService funding rate functionality.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs
from cyberdelta.core.models.market import FundingRate
from cyberdelta.core.models.market.funding_rate import BackpackFundingDetails

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
        symbol = "SOL-PERP"
        mock_endpoint_path = "/api/v1/funding"
        mock_params = {"symbol": symbol}
        raw_time_str = "2023-10-27T10:00:00Z"
        mock_raw_response_content = {
            "symbol": symbol,
            "rate": "0.0001",
            "markPrice": "100.0",
            "indexPrice": "99.0",
            "time": raw_time_str,
        }
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_funding_rate = BackpackRawFundingRate(
            symbol=symbol,
            rate="0.0001",
            markPrice="100.0",
            indexPrice="99.0",
            time=raw_time_str,
        )
        expected_internal_funding_rate = FundingRate(
            symbol=symbol,
            timestamp=datetime.fromisoformat(raw_time_str.replace("Z", "+00:00")),
            funding_rate=Decimal("0.0001"),
            mark_price=Decimal("100.0"),
            index_price=Decimal("99.0"),
            next_funding_time=datetime.fromisoformat(raw_time_str.replace("Z", "+00:00")),
            bp_details=BackpackFundingDetails(),
        )

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_funding_rate_response.return_value = mock_raw_funding_rate

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_funding_rate_to_internal.return_value = (
                expected_internal_funding_rate
            )
            result_funding_rate = await backpack_market_data_service.get_funding_rate(symbol)

            mock_request_builder.build_get_funding_rate_params.assert_called_once_with(
                symbol=symbol,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_funding_rate_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                mock_status_code,
                mock_headers_from_client,
            )
            mock_mapper.transform_raw_funding_rate_to_internal.assert_called_once_with(
                mock_raw_funding_rate,
            )
            assert result_funding_rate == expected_internal_funding_rate

    @pytest.mark.asyncio
    async def test_get_funding_rate_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate when HTTP client returns None content."""
        symbol = "SOL-PERP"
        mock_endpoint_path = "/api/v1/funding"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_funding_rate(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert "No data for funding_rate" in exc_info.value.message

            mock_request_builder.build_get_funding_rate_params.assert_called_once_with(
                symbol=symbol,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_funding_rate_response.assert_not_called()
            mock_mapper.transform_raw_funding_rate_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_funding_rate_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate handles validation error from response handler."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"invalid": "funding_rate_data"}

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawFundingRate.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_funding_rate_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_funding_rate(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_funding_rate_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate handles unexpected exception."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"symbol": symbol, "rate": "0.001"}

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_funding_rate_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_funding_rate(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates success."""
        symbol = "SOL-PERP"
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000
        # Service converts to seconds, not milliseconds
        expected_start_time_s = start_time_ms // 1000
        expected_end_time_s = end_time_ms // 1000
        limit = 10
        mock_endpoint_path = "/api/v1/funding/history"
        mock_params = {
            "symbol": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "limit": limit,
        }
        raw_time_1 = "2023-10-27T10:00:00Z"
        raw_time_2 = "2023-10-28T10:00:00Z"
        mock_raw_response_content_list = [
            {
                "symbol": symbol,
                "rate": "0.0001",
                "markPrice": "100.0",
                "indexPrice": "99.0",
                "time": raw_time_1,
            },
            {
                "symbol": symbol,
                "rate": "0.0002",
                "markPrice": "101.0",
                "indexPrice": "100.0",
                "time": raw_time_2,
            },
        ]
        mock_validated_funding_rates_raw = mock_raw_response_content_list
        mock_headers_from_client = MagicMock()

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content_list,
            200,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_historical_funding_rates_response.return_value = (
            mock_validated_funding_rates_raw
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_funding_rates = [MagicMock(spec=FundingRate), MagicMock(spec=FundingRate)]
            mock_mapper.transform_raw_funding_interval_rate_to_internal.side_effect = (
                mock_internal_funding_rates
            )

            args = GetHistoricalFundingRatesArgs(
                symbol=symbol,
                start_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                end_time=datetime.fromtimestamp(end_time_ms / 1000, tz=UTC),
                limit=limit,
            )
            result = await backpack_market_data_service.get_historical_funding_rates(args)

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol,
                start_time_ms=expected_start_time_s,
                end_time_ms=expected_end_time_s,
                limit=limit,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )

            mock_response_handler.handle_get_historical_funding_rates_response.assert_called_once()
            call_args_tuple = (
                mock_response_handler.handle_get_historical_funding_rates_response.call_args
            )

            # Check positional arguments
            assert call_args_tuple.args[0] == mock_raw_response_content_list
            assert call_args_tuple.args[1] == symbol
            assert call_args_tuple.args[2] == 200
            assert call_args_tuple.args[3] is mock_headers_from_client

            # Check keyword arguments
            assert not call_args_tuple.kwargs

            # Assert calls to mapper
            assert mock_mapper.transform_raw_funding_interval_rate_to_internal.call_count == len(
                mock_validated_funding_rates_raw,
            )

            assert len(result) == len(mock_internal_funding_rates)

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when HTTP client returns None content."""
        symbol = "SOL-PERP"
        start_time_ms = 1678880000000
        expected_start_time_s = start_time_ms // 1000
        limit = 5
        mock_endpoint_path = "/api/v1/funding/history"
        mock_params = {"symbol": symbol, "startTime": start_time_ms, "limit": limit}

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                args = GetHistoricalFundingRatesArgs(
                    symbol=symbol,
                    start_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                    limit=limit,
                )
                await backpack_market_data_service.get_historical_funding_rates(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg = f"No data for historical funding rates {symbol}, status: 200"
            assert exc_info.value.message == expected_msg

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol,
                start_time_ms=expected_start_time_s,
                end_time_ms=None,
                limit=limit,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_historical_funding_rates_response.assert_not_called()
            mock_mapper.transform_raw_funding_interval_rate_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles validation error from response handler."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol, "limit": 10}
        mock_raw_response = [{"invalid": "funding_rate_data"}]

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawFundingRate.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_historical_funding_rates_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            args = GetHistoricalFundingRatesArgs(symbol=symbol)
            await backpack_market_data_service.get_historical_funding_rates(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles unexpected exception."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol, "limit": 10}
        mock_raw_response = [{"symbol": symbol, "rate": "0.001"}]

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_historical_funding_rates_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            args = GetHistoricalFundingRatesArgs(symbol=symbol)
            await backpack_market_data_service.get_historical_funding_rates(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_with_optional_parameters(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates with only symbol parameter."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol}
        mock_raw_response = [
            {
                "symbol": symbol,
                "rate": "0.0001",
                "markPrice": "100.0",
                "indexPrice": "99.0",
                "time": "2023-10-27T10:00:00Z",
            },
        ]

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_historical_funding_rates_response.return_value = (
            mock_raw_response
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_funding_rates = [MagicMock(spec=FundingRate)]
            mock_mapper.transform_raw_funding_interval_rate_to_internal.side_effect = (
                mock_internal_funding_rates
            )

            args = GetHistoricalFundingRatesArgs(symbol=symbol)
            result = await backpack_market_data_service.get_historical_funding_rates(args)

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol,
                start_time_ms=None,
                end_time_ms=None,
                limit=None,
            )
            assert result == mock_internal_funding_rates
