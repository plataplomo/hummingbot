"""Unit tests for BackpackMarketDataService public data functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetOrderBookParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.market import OrderBook, Ticker, Trade

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_market_data"]


class TestBackpackMarketDataServicePublicData:
    """Tests for the BackpackMarketDataService public data functionality."""

    # =============================================================================
    # INPUT VALIDATION TESTS (NEW - ITERATION 2)
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_ticker_empty_symbol_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_ticker raises ValueError for empty symbol."""
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_ticker("")  # Empty symbol should be rejected

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_empty_symbol_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_order_book raises ValueError for empty symbol."""
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_order_book("")  # Empty symbol should be rejected

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_invalid_limit_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_order_book raises ValueError for invalid limit values."""
        # Test zero limit
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_order_book(
                symbol="SOL_USDC",
                limit=0,  # Invalid: zero limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

        # Test negative limit
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_order_book(
                symbol="SOL_USDC",
                limit=-5,  # Invalid: negative limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_recent_trades_empty_symbol_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_recent_trades raises ValueError for empty symbol."""
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_recent_trades(
                "",
            )  # Empty symbol should be rejected

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_recent_trades_invalid_limit_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_recent_trades raises ValueError for invalid limit values."""
        # Test zero limit
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_recent_trades(
                symbol="SOL_USDC",
                limit=0,  # Invalid: zero limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

        # Test negative limit
        with pytest.raises(ValueError) as exc_info:
            await backpack_market_data_service.get_recent_trades(
                symbol="SOL_USDC",
                limit=-10,  # Invalid: negative limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

    # =============================================================================
    # EXISTING FUNCTIONALITY TESTS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol = "SOL_USDC"
        mock_timestamp_int = 1678886400  # Example timestamp
        mock_timestamp_dt = datetime.fromtimestamp(mock_timestamp_int, tz=UTC)

        mock_endpoint_path = "/api/v1/ticker"
        mock_raw_response_content = {
            "symbol": symbol,
            "price": "100.0",
            "volume": "1000.0",
            "bid": "99.9",
            "ask": "100.1",
            "time": mock_timestamp_int,
        }
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_ticker = BackpackRawTicker(
            symbol=symbol,
            firstPrice="99.0",
            lastPrice="100.0",
            high="101.0",
            low="99.0",
            priceChange="1.0",
            priceChangePercent="1.01",
            volume="1000.0",
            quoteVolume="100000.0",
            trades="50",
        )
        mock_internal_ticker = Ticker(
            symbol=symbol,
            price=Decimal("100.0"),
            volume=Decimal("1000.0"),
            bid=Decimal("99.9"),
            ask=Decimal("100.1"),
            timestamp=mock_timestamp_dt,
        )

        mock_request_builder.build_get_ticker_params.return_value = BackpackRawGetTickerParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_ticker_response.return_value = mock_raw_ticker

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_ticker_to_internal.return_value = mock_internal_ticker

            result_ticker = await backpack_market_data_service.get_ticker(symbol)

            mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params={"symbol": symbol},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_mapper.transform_raw_ticker_to_internal.assert_called_once_with(
                mock_raw_ticker,
                symbol_override=symbol,
            )
            mock_response_handler.handle_get_ticker_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                mock_status_code,
                mock_headers_from_client,
            )
            assert result_ticker == mock_internal_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_endpoint_path = "/api/v1/ticker"

        mock_request_builder.build_get_ticker_params.return_value = BackpackRawGetTickerParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"No data received for ticker ({symbol}), status: 200"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params={"symbol": symbol},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_ticker_response.assert_not_called()
            mock_mapper.transform_raw_ticker_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_ticker_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_raw_response: dict[str, Any] = {"invalid": "ticker_data"}

        mock_request_builder.build_get_ticker_params.return_value = BackpackRawGetTickerParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawTicker.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_ticker_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_ticker_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles unexpected exception."""
        symbol = "SOL_USDC"
        mock_raw_response = {"symbol": symbol, "price": "100.0"}

        mock_request_builder.build_get_ticker_params.return_value = BackpackRawGetTickerParams(
            symbol=symbol
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_ticker_response.side_effect = Exception("Unexpected error")

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error occurred" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol = "SOL_USDC"
        depth = 10
        mock_raw_response_content = {"bids": [["2000.0", "10.0"]], "asks": [["2001.0", "5.0"]]}
        mock_headers_from_client = MagicMock()

        mock_request_builder.build_get_order_book_params.return_value = (
            BackpackRawGetOrderBookParams(symbol=symbol, limit=depth)
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers_from_client,
        )
        mock_validated_book = MagicMock(spec=BackpackRawOrderBook)
        mock_response_handler.handle_get_order_book_response.return_value = mock_validated_book

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_book = MagicMock(spec=OrderBook)
            mock_mapper.transform_raw_order_book_to_internal.return_value = mock_internal_book

            result = await backpack_market_data_service.get_order_book(symbol, limit=depth)

            mock_request_builder.build_get_order_book_params.assert_called_once_with(
                symbol=symbol,
                limit=depth,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/depth",
                params={"symbol": symbol, "limit": depth},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_order_book_response.assert_called_once()
            mock_mapper.transform_raw_order_book_to_internal.assert_called_once_with(
                symbol,
                mock_validated_book,
            )
            assert result == mock_internal_book

    @pytest.mark.asyncio
    async def test_get_order_book_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book when HTTP client returns None content."""
        symbol = "SOL_USDC"
        depth = 5

        mock_request_builder.build_get_order_book_params.return_value = (
            BackpackRawGetOrderBookParams(symbol=symbol, limit=depth)
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol=symbol, limit=depth)

        assert f"No data received for order book ({symbol}), status: 200" in exc_info.value.message

        mock_request_builder.build_get_order_book_params.assert_called_once_with(
            symbol=symbol,
            limit=depth,
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/depth",
            params={"symbol": symbol, "limit": depth},
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_response_handler.handle_get_order_book_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_book_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_raw_response: dict[str, Any] = {"invalid": "order_book_data"}

        mock_request_builder.build_get_order_book_params.return_value = (
            BackpackRawGetOrderBookParams(symbol=symbol, limit=20)
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrderBook.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_order_book_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book raises APIError when an unexpected exception occurs."""
        symbol = "SOL_USDC"
        depth = 5

        # Arrange: Configure the mocks to trigger unexpected exception
        mock_request_builder.build_get_order_book_params.return_value = (
            BackpackRawGetOrderBookParams(symbol=symbol, limit=depth)
        )
        mock_http_client_requester.return_value = ({"mock": "response"}, 200, {})
        mock_response_handler.handle_get_order_book_response.side_effect = Exception(
            "Unexpected error",
        )

        # Act & Assert: Call the service method and verify the exception
        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol=symbol, limit=depth)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error occurred." in exc_info.value.message

        mock_request_builder.build_get_order_book_params.assert_called_once_with(
            symbol=symbol,
            limit=depth,
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/depth",
            params={"symbol": symbol, "limit": depth},
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_response_handler.handle_get_order_book_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes trade data."""
        symbol = "SOL_USDC"
        limit = 50
        mock_endpoint_path = "/api/v1/trades"
        mock_raw_response_content = [
            {
                "id": 12345,
                "isBuyerMaker": False,
                "price": "2000.0",
                "quantity": "1.0",
                "quoteQuantity": "2000.0",
                "timestamp": 1678886400100,
            },
            {
                "id": 12346,
                "isBuyerMaker": True,
                "price": "2000.1",
                "quantity": "0.5",
                "quoteQuantity": "1000.05",
                "timestamp": 1678886400200,
            },
        ]
        mock_headers_from_client = {
            "Content-Type": "application/json",
            "X-RateLimit-Remaining": "100",
        }

        mock_request_builder.build_get_recent_trades_params.return_value = (
            BackpackRawGetRecentTradesParams(symbol=symbol, limit=limit)
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers_from_client,
        )
        # Mock raw trade models that the response handler would return
        from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawRecentPublicTrade

        mock_raw_trade_models = [
            BackpackRawRecentPublicTrade(
                id=12345,
                isBuyerMaker=False,
                price="2000.0",
                quantity="1.0",
                quoteQuantity="2000.0",
                timestamp=1678886400100,
            ),
            BackpackRawRecentPublicTrade(
                id=12346,
                isBuyerMaker=True,
                price="2000.1",
                quantity="0.5",
                quoteQuantity="1000.05",
                timestamp=1678886400200,
            ),
        ]
        mock_response_handler.handle_get_recent_trades_response.return_value = mock_raw_trade_models

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_trades = [MagicMock(spec=Trade), MagicMock(spec=Trade)]
            mock_mapper.transform_raw_recent_trade_to_internal.side_effect = mock_internal_trades

            result = await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
                symbol=symbol,
                limit=limit,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params={"symbol": symbol, "limit": limit},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_recent_trades_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                200,
                mock_headers_from_client,
            )
            assert mock_mapper.transform_raw_recent_trade_to_internal.call_count == len(
                mock_raw_trade_models,
            )
            assert result == mock_internal_trades

    @pytest.mark.asyncio
    async def test_get_recent_trades_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades when HTTP client returns None content."""
        symbol = "ETH_USDC"
        limit = 5
        mock_endpoint_path = "/api/v1/trades"
        mock_params = {"symbol": symbol, "limit": limit}

        mock_request_builder.build_get_recent_trades_params.return_value = (
            BackpackRawGetRecentTradesParams(symbol=symbol, limit=limit)
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert (
                f"No data received for recent trades ({symbol}), status: 200"
                in exc_info.value.message
            )

            mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
                symbol=symbol,
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
            mock_response_handler.handle_get_recent_trades_response.assert_not_called()
            mock_mapper.transform_raw_recent_trade_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_recent_trades_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_raw_response: list[dict[str, str | int]] = [{"invalid": "trade_data"}]

        mock_request_builder.build_get_recent_trades_params.return_value = (
            BackpackRawGetRecentTradesParams(symbol=symbol, limit=100)
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade

            BackpackRawPublicTrade.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_recent_trades_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades handles unexpected exception."""
        symbol = "SOL_USDC"
        mock_raw_response: list[dict[str, str | int]] = [
            {
                "id": 1,
                "isBuyerMaker": False,
                "price": "100.0",
                "quantity": "1.0",
                "quoteQuantity": "100.0",
                "timestamp": 123,
            },
        ]

        mock_request_builder.build_get_recent_trades_params.return_value = (
            BackpackRawGetRecentTradesParams(symbol=symbol, limit=100)
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_recent_trades_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error occurred" in exc_info.value.message
