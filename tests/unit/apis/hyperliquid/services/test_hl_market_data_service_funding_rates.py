"""
Unit tests for HyperliquidMarketDataService funding rate functionality.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.common_raw_types import RawHlCoinName
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.market import FundingRate

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_market_data"]


class TestHyperliquidMarketDataServiceFundingRates:
    """Tests for the HyperliquidMarketDataService funding rate functionality."""

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_funding_rate successfully retrieves and processes funding rate data."""
        symbol_to_find = "BTC"

        mock_raw_asset_ctx_btc = HyperliquidRawAssetCtx(
            name="BTC",
            funding="0.0001",
            markPx="50000.0",
            prevDayPx="49000.0",
            dayNtlVlm="1000",
            impactPx="50001.0",
        )
        mock_raw_asset_ctx_eth = HyperliquidRawAssetCtx(
            name="ETH",
            funding="0.0002",
            markPx="3000.0",
            prevDayPx="2900.0",
            dayNtlVlm="500",
            impactPx="3001.0",
        )
        mock_meta_response = HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="BTC", szDecimals=5, maxLeverage=100, onlyIsolated=False
                ),
                HyperliquidRawAssetDefinition(
                    name="ETH", szDecimals=5, maxLeverage=100, onlyIsolated=False
                ),
            ]
        )
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=mock_meta_response,
            asset_ctxs=[mock_raw_asset_ctx_btc, mock_raw_asset_ctx_eth],
        )

        expected_internal_funding_rate = FundingRate(
            symbol=symbol_to_find,
            funding_rate=Decimal("0.0001"),
            timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        )

        mock_hl_mapper.transform_raw_asset_ctx_to_funding_rate.return_value = (
            expected_internal_funding_rate
        )

        # Mock the get_all_asset_contexts_raw method using patch
        with patch.object(
            hyperliquid_market_data_service,
            "get_all_asset_contexts_raw",
            new=AsyncMock(return_value=mock_all_contexts_response),
        ) as mock_get_contexts:
            result_funding_rate = await hyperliquid_market_data_service.get_funding_rate(
                symbol_to_find
            )

            mock_get_contexts.assert_called_once_with()
            mock_hl_mapper.transform_raw_asset_ctx_to_funding_rate.assert_called_once_with(
                mock_raw_asset_ctx_btc
            )
            assert result_funding_rate == expected_internal_funding_rate

    @pytest.mark.asyncio
    async def test_get_funding_rate_not_found(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_funding_rate returns None when symbol is not found."""
        symbol = "UNKNOWN"
        mock_meta_response = HyperliquidRawMetaResponse(universe=[])
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=mock_meta_response, asset_ctxs=[]
        )

        with patch.object(
            hyperliquid_market_data_service,
            "get_all_asset_contexts_raw",
            new=AsyncMock(return_value=mock_all_contexts_response),
        ):
            result = await hyperliquid_market_data_service.get_funding_rate(symbol)
            assert result is None

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates successfully retrieves and processes data."""
        symbol = "BTC"
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC)
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        # Setup request payload mocking
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        # Mock raw response content
        mock_raw_response_content = [
            {
                "coin": symbol,
                "fundingRate": "0.0001",
                "premium": "0.00005",
                "time": 1672531200000,
            },
            {
                "coin": symbol,
                "fundingRate": "0.0002",
                "premium": "0.00010",
                "time": 1672617600000,
            },
        ]

        # Mock validated response from handler
        mock_validated_funding_items = [
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName(symbol),
                fundingRate="0.0001",
                premium="0.00005",
                time=1672531200000,
            ),
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName(symbol),
                fundingRate="0.0002",
                premium="0.00010",
                time=1672617600000,
            ),
        ]

        # Expected internal funding rates from mapper
        expected_internal_funding_rates = [
            FundingRate(
                symbol=symbol,
                funding_rate=Decimal("0.0001"),
                timestamp=datetime.fromtimestamp(1672531200000 / 1000, tz=UTC),
            ),
            FundingRate(
                symbol=symbol,
                funding_rate=Decimal("0.0002"),
                timestamp=datetime.fromtimestamp(1672617600000 / 1000, tz=UTC),
            ),
        ]

        # Configure mocks
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (mock_raw_response_content, 200, mock_headers)
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = (
            mock_validated_funding_items
        )
        mock_hl_mapper.transform_raw_funding_history_item_to_internal.side_effect = (
            expected_internal_funding_rates
        )

        # Call service method with correct parameter names
        result_funding_rates = await hyperliquid_market_data_service.get_historical_funding_rates(
            symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )

        # Assertions
        mock_hl_request_builder.build_historical_funding_rates_payload.assert_called_once_with(
            symbol=symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        mock_payload_model.model_dump.assert_called_once_with(by_alias=True, exclude_none=True)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_payload_dict,
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.assert_called_once_with(
            raw_response_content=mock_raw_response_content,
            status_code=200,
            headers=mock_headers,
        )
        # Verify mapper calls
        for validated_item in mock_validated_funding_items:
            mock_hl_mapper.transform_raw_funding_history_item_to_internal.assert_any_call(
                validated_item
            )
        assert result_funding_rates == expected_internal_funding_rates

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when HTTP client returns None content."""
        symbol = "ETH"
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for historical funding rates" in exc_info.value.message

        mock_hl_request_builder.build_historical_funding_rates_payload.assert_called_once_with(
            symbol=symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_payload_dict,
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.assert_not_called()
        mock_hl_mapper.transform_raw_funding_history_item_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_response_validation_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles response validation errors."""
        symbol = "BTC"
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        # Setup mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        # Mock malformed response
        mock_malformed_response = [
            {"coin": symbol, "fundingRate": "invalid", "time": "not_a_number"}
        ]
        mock_http_client_requester.return_value = (mock_malformed_response, 200, {})

        # Mock response handler to raise ValidationError
        validation_error = ValidationError.from_exception_data(
            title="HyperliquidRawFundingHistoryItem", line_errors=[]
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.side_effect = APIError(
            message="Invalid funding history response structure",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=validation_error,
        )

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid funding history response structure" in exc_info.value.message
        mock_hl_response_handler.handle_historical_funding_rates_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_mapper_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles mapper errors gracefully."""
        symbol = "BTC"
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        # Setup successful HTTP and response handler mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        mock_raw_response_content = [
            {"coin": symbol, "fundingRate": "0.0001", "premium": "0.00005", "time": 1672531200000}
        ]
        mock_http_client_requester.return_value = (mock_raw_response_content, 200, {})

        mock_validated_funding_item = HyperliquidRawFundingHistoryItem(
            coin=RawHlCoinName(symbol), fundingRate="0.0001", premium="0.00005", time=1672531200000
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = [
            mock_validated_funding_item
        ]

        # Configure mapper to raise an error
        mock_hl_mapper.transform_raw_funding_history_item_to_internal.side_effect = ValueError(
            "Mapper processing failed"
        )

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Processing historical funding rate data failed" in exc_info.value.message
        assert isinstance(exc_info.value.__cause__, ValueError)
        assert str(exc_info.value.__cause__) == "Mapper processing failed"

        mock_hl_mapper.transform_raw_funding_history_item_to_internal.assert_called_once_with(
            mock_validated_funding_item
        )

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_api_error_from_handler(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates propagates APIError from response handler."""
        symbol = "ETH"
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        # Setup mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "fundingHistory", "coin": symbol}
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        # HTTP client returns successful response
        mock_response_content = [{"coin": symbol, "fundingRate": "0.0001", "time": 1672531200000}]
        mock_http_client_requester.return_value = (mock_response_content, 200, {})

        # Response handler raises APIError
        api_error = APIError(
            message="Funding history response handler failed",
            code=APIErrorCode.SERVER_ERROR.value,
            http_status=200,
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.side_effect = api_error

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

        assert exc_info.value == api_error
        mock_hl_response_handler.handle_historical_funding_rates_response.assert_called_once_with(
            raw_response_content=mock_response_content, status_code=200, headers={}
        )

    @pytest.mark.asyncio
    async def test_get_funding_rate_full_error_chain_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_funding_rate error handling when get_all_asset_contexts_raw fails."""
        symbol = "BTC"

        # Mock get_all_asset_contexts_raw to raise APIError
        with patch.object(
            hyperliquid_market_data_service,
            "get_all_asset_contexts_raw",
            new=AsyncMock(
                side_effect=APIError(
                    message="Failed to get asset contexts",
                    code=APIErrorCode.NETWORK_ISSUE.value,
                )
            ),
        ):
            with pytest.raises(APIError) as exc_info:
                await hyperliquid_market_data_service.get_funding_rate(symbol)

            assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value
            assert "Failed to get asset contexts" in exc_info.value.message
            # Mapper should not be called when asset contexts retrieval fails
            mock_hl_mapper.transform_raw_asset_ctx_to_funding_rate.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_comprehensive_error_scenarios(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test comprehensive error scenarios for get_historical_funding_rates."""
        symbol = "BTC"
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        # Test case 1: Empty response but successful
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "fundingHistory", "coin": symbol}
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        # Empty successful response
        mock_http_client_requester.return_value = ([], 200, {})
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = []

        result_empty = await hyperliquid_market_data_service.get_historical_funding_rates(
            symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        assert result_empty == []

        # Test case 2: Partial mapper failures
        mock_funding_items = [
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName(symbol),
                fundingRate="0.0001",
                premium="0.00005",
                time=1672531200000,
            ),
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName(symbol),
                fundingRate="0.0002",
                premium="0.00010",
                time=1672617600000,
            ),
        ]
        mock_http_client_requester.return_value = ([{"some": "data"}], 200, {})
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = (
            mock_funding_items
        )

        # Configure mapper to succeed for first, fail for second
        def mapper_side_effect(item: HyperliquidRawFundingHistoryItem) -> FundingRate:
            if item.time == 1672531200000:
                return FundingRate(
                    symbol=symbol,
                    funding_rate=Decimal("0.0001"),
                    timestamp=datetime.fromtimestamp(item.time / 1000, tz=UTC),
                )
            else:
                raise ValueError("Invalid funding rate data")

        mock_hl_mapper.transform_raw_funding_history_item_to_internal.side_effect = (
            mapper_side_effect
        )

        # Should fail on the second item and raise APIError
        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Processing historical funding rate data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_connection_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when HTTP client raises connection error."""
        symbol = "BTC"
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        mock_payload_model = MagicMock()
        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_payload_model
        )

        # Mock HTTP client to raise network error
        mock_http_client_requester.side_effect = APIError(
            message="Connection failed", code=APIErrorCode.NETWORK_ISSUE.value
        )

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Connection failed" in exc_info.value.message

        mock_hl_request_builder.build_historical_funding_rates_payload.assert_called_once_with(
            symbol=symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    @pytest.mark.asyncio
    async def test_get_funding_rate_mapper_unexpected_exception(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_funding_rate handles unexpected mapper exceptions."""
        symbol = "BTC"

        mock_raw_asset_ctx_btc = HyperliquidRawAssetCtx(
            name="BTC",
            funding="0.0001",
            markPx="50000.0",
            prevDayPx="49000.0",
            dayNtlVlm="1000",
            impactPx="50001.0",
        )
        mock_meta_response = HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="BTC", szDecimals=5, maxLeverage=100, onlyIsolated=False
                )
            ]
        )
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=mock_meta_response,
            asset_ctxs=[mock_raw_asset_ctx_btc],
        )

        # Configure mapper to raise unexpected exception
        mock_hl_mapper.transform_raw_asset_ctx_to_funding_rate.side_effect = RuntimeError(
            "Unexpected mapper error"
        )

        with patch.object(
            hyperliquid_market_data_service,
            "get_all_asset_contexts_raw",
            new=AsyncMock(return_value=mock_all_contexts_response),
        ):
            with pytest.raises(APIError) as exc_info:
                await hyperliquid_market_data_service.get_funding_rate(symbol)

            assert exc_info.value.code == APIErrorCode.UNKNOWN.value
            assert "Unexpected error getting funding rate" in exc_info.value.message
            assert isinstance(exc_info.value.__cause__, RuntimeError)
            assert str(exc_info.value.__cause__) == "Unexpected mapper error"
