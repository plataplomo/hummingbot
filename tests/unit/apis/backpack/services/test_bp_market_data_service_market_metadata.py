"""Unit tests for BackpackMarketDataService market metadata functionality."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawMarket
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetMarketParams,
    BackpackRawGetMarketsParams,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs
from cyberdelta.core.models.market import Market


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_market_data"]


class TestBackpackMarketDataServiceMarketMetadata:
    """Tests for the BackpackMarketDataService market metadata functionality."""

    @pytest.mark.asyncio
    async def test_get_market_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market successfully retrieves and processes market metadata."""
        symbol = "BTC_USDC"

        # Mock raw market data response
        mock_raw_market_data = {
            "symbol": "BTC_USDC",
            "baseSymbol": "BTC",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {"minPrice": "0.01", "maxPrice": "100000.00", "tickSize": "0.01"},
                "quantity": {
                    "minQuantity": "0.001",
                    "maxQuantity": "10000.00",
                    "stepSize": "0.001",
                },
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }

        mock_validated_market_raw = BackpackRawMarket.model_validate(mock_raw_market_data)
        mock_headers_from_client = {"X-Test-Header": "value"}

        # Setup mocks
        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (
            mock_raw_market_data,
            200,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_market_response.return_value = mock_validated_market_raw

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_market = MagicMock(spec=Market)
            mock_internal_market.symbol = symbol
            mock_mapper.transform_raw_market_to_internal.return_value = mock_internal_market

            args = GetMarketArgs(symbol=symbol)
            result = await backpack_market_data_service.get_market(args)

            # Verify all dependencies were called correctly
            mock_request_builder.build_get_market_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/market",
                params={"symbol": symbol},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_response.assert_called_once_with(
                mock_raw_market_data,
                symbol,
                200,
                mock_headers_from_client,
            )
            mock_mapper.transform_raw_market_to_internal.assert_called_once_with(
                mock_validated_market_raw
            )
            assert result == mock_internal_market

    @pytest.mark.asyncio
    async def test_get_market_empty_symbol_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market validation error for empty symbol at args model level."""
        empty_symbol = ""

        # The validation error should occur when creating GetMarketArgs, not in the service
        with pytest.raises(ValidationError) as exc_info:
            GetMarketArgs(symbol=empty_symbol)

        assert "String cannot be empty" in str(exc_info.value)

        # Verify no external calls were made
        mock_request_builder.build_get_market_params.assert_not_called()
        mock_http_client_requester.assert_not_called()
        mock_response_handler.handle_get_market_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market when HTTP client returns None content."""
        symbol = "BTC_USDC"

        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            args = GetMarketArgs(symbol=symbol)
            await backpack_market_data_service.get_market(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        expected_error_msg = f"No data received for market ({symbol}), status: 200"
        assert exc_info.value.message == expected_error_msg

        mock_response_handler.handle_get_market_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_invalid_response_type(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market when HTTP client returns non-dict response."""
        symbol = "BTC_USDC"

        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )
        # Return a list instead of a dict
        mock_http_client_requester.return_value = (["invalid", "response"], 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            args = GetMarketArgs(symbol=symbol)
            await backpack_market_data_service.get_market(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        expected_error_msg = (
            f"Unexpected market ({symbol}) response format: expected dict, got list"
        )
        assert exc_info.value.message == expected_error_msg

        mock_response_handler.handle_get_market_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_response_handler_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market handles validation error from response handler."""
        symbol = "BTC_USDC"
        mock_raw_response = {"invalid": "market_data"}

        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError
        try:
            BackpackRawMarket.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_market_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            args = GetMarketArgs(symbol=symbol)
            await backpack_market_data_service.get_market(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_transformation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market handles transformation error from mapper."""
        symbol = "BTC_USDC"
        mock_raw_response = {"symbol": "BTC_USDC", "baseAsset": "BTC"}
        mock_validated_market_raw = MagicMock(spec=BackpackRawMarket)

        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_market_response.return_value = mock_validated_market_raw

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            from cyberdelta.apis.models.api_error import TransformationError

            mock_mapper.transform_raw_market_to_internal.side_effect = TransformationError(
                "Failed to transform market data"
            )

            with pytest.raises(APIError) as exc_info:
                args = GetMarketArgs(symbol=symbol)
                await backpack_market_data_service.get_market(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert "Failed to process/transform exchange data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market handles unexpected exception."""
        symbol = "BTC_USDC"
        mock_raw_response = {"symbol": "BTC_USDC"}

        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_market_response.side_effect = Exception("Unexpected error")

        with pytest.raises(APIError) as exc_info:
            args = GetMarketArgs(symbol=symbol)
            await backpack_market_data_service.get_market(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error occurred." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_markets_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets successfully retrieves and processes all market metadata."""
        # Mock raw markets data response
        mock_raw_markets_data = [
            {
                "symbol": "BTC_USDC",
                "baseSymbol": "BTC",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {"minPrice": "0.01", "maxPrice": "100000.00", "tickSize": "0.01"},
                    "quantity": {
                        "minQuantity": "0.001",
                        "maxQuantity": "10000.00",
                        "stepSize": "0.001",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            },
            {
                "symbol": "ETH_USDC",
                "baseSymbol": "ETH",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {"minPrice": "0.01", "maxPrice": "10000.00", "tickSize": "0.01"},
                    "quantity": {
                        "minQuantity": "0.001",
                        "maxQuantity": "1000.00",
                        "stepSize": "0.001",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            },
        ]

        mock_validated_markets_raw = [
            BackpackRawMarket.model_validate(market) for market in mock_raw_markets_data
        ]
        mock_headers_from_client = {"X-Rate-Limit": "100"}

        # Setup mocks
        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        mock_http_client_requester.return_value = (
            mock_raw_markets_data,
            200,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_markets_response.return_value = mock_validated_markets_raw

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_markets = [MagicMock(spec=Market), MagicMock(spec=Market)]
            mock_internal_markets[0].symbol = "BTC_USDC"
            mock_internal_markets[1].symbol = "ETH_USDC"
            mock_mapper.transform_raw_market_to_internal.side_effect = mock_internal_markets

            args = GetMarketsArgs()
            result = await backpack_market_data_service.get_markets(args)

            # Verify all dependencies were called correctly
            mock_request_builder.build_get_markets_params.assert_called_once()
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/markets",
                params={},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_markets_response.assert_called_once_with(
                mock_raw_markets_data,
                200,
            )
            assert mock_mapper.transform_raw_market_to_internal.call_count == len(
                mock_validated_markets_raw
            )
            assert result == mock_internal_markets

    @pytest.mark.asyncio
    async def test_get_markets_empty_list(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles empty markets list."""
        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        mock_http_client_requester.return_value = ([], 200, {})
        mock_response_handler.handle_get_markets_response.return_value = []

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            args = GetMarketsArgs()
            result = await backpack_market_data_service.get_markets(args)

            assert result == []
            mock_mapper.transform_raw_market_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_markets_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets when HTTP client returns None content."""
        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            args = GetMarketsArgs()
            await backpack_market_data_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        expected_error_msg = "No data received for markets data, status: 200"
        assert exc_info.value.message == expected_error_msg

        mock_response_handler.handle_get_markets_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_markets_invalid_response_type(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets when HTTP client returns non-list response."""
        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        # Return a dict instead of a list
        mock_http_client_requester.return_value = ({"invalid": "response"}, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            args = GetMarketsArgs()
            await backpack_market_data_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        expected_error_msg = "Unexpected markets data response format: expected list, got dict"
        assert exc_info.value.message == expected_error_msg

        mock_response_handler.handle_get_markets_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_markets_response_handler_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles validation error from response handler."""
        mock_raw_response = [{"invalid": "market_data"}]

        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError
        try:
            BackpackRawMarket.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_markets_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            args = GetMarketsArgs()
            await backpack_market_data_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_markets_transformation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles transformation error from mapper."""
        mock_raw_response = [{"symbol": "BTC_USDC"}]
        mock_validated_markets_raw = [MagicMock(spec=BackpackRawMarket)]
        mock_validated_markets_raw[0].symbol = "BTC_USDC"

        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_markets_response.return_value = mock_validated_markets_raw

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            from cyberdelta.apis.models.api_error import TransformationError

            mock_mapper.transform_raw_market_to_internal.side_effect = TransformationError(
                "Failed to transform markets data"
            )

            # In get_markets, individual transformation errors are caught and logged,
            # but the service continues and returns successfully with fewer markets
            args = GetMarketsArgs()
            result = await backpack_market_data_service.get_markets(args)

            # Should return empty list since the single market failed to transform
            assert result == []

    @pytest.mark.asyncio
    async def test_get_markets_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles unexpected exception."""
        mock_raw_response = [{"symbol": "BTC_USDC"}]

        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_markets_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            args = GetMarketsArgs()
            await backpack_market_data_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error occurred." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_markets_api_error_propagation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets properly propagates APIError from HTTP client."""
        mock_request_builder.build_get_markets_params.return_value = BackpackRawGetMarketsParams()

        # Create an APIError that would come from the HTTP client
        api_error = APIError("Service unavailable", APIErrorCode.SERVICE_UNAVAILABLE.value)
        mock_http_client_requester.side_effect = api_error

        with pytest.raises(APIError) as exc_info:
            args = GetMarketsArgs()
            await backpack_market_data_service.get_markets(args)

        # The original APIError should be re-raised
        assert exc_info.value == api_error
        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value

        mock_response_handler.handle_get_markets_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_api_error_propagation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market properly propagates APIError from HTTP client."""
        symbol = "BTC_USDC"

        mock_request_builder.build_get_market_params.return_value = BackpackRawGetMarketParams(
            symbol=symbol
        )

        # Create an APIError that would come from the HTTP client
        api_error = APIError("Symbol not found", APIErrorCode.SYMBOL_NOT_FOUND.value)
        mock_http_client_requester.side_effect = api_error

        with pytest.raises(APIError) as exc_info:
            args = GetMarketArgs(symbol=symbol)
            await backpack_market_data_service.get_market(args)

        # The original APIError should be re-raised
        assert exc_info.value == api_error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value

        mock_response_handler.handle_get_market_response.assert_not_called()
