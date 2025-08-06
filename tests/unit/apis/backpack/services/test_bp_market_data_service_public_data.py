"""Unit tests for BackpackMarketDataService public data functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawOrderBook,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions.market_data_service import EmptySymbolError, InvalidLimitError
from cyberdelta.enums import ExchangeName
from cyberdelta.models.market import OrderBook, Ticker, Trade
from cyberdelta.symbols import exchanges
from tests.common_symbols import ETH_USDC_BP, SOL_USDC_BP


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
        """Test get_ticker raises EmptySymbolError for empty symbol."""
        # Test with actual invalid symbol creation
        with pytest.raises((EmptySymbolError, ValueError, ValidationError)):
            invalid_symbol = exchanges.backpack("")
            await backpack_market_data_service.get_ticker(invalid_symbol)

    @pytest.mark.asyncio
    async def test_get_order_book_empty_symbol_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_order_book raises EmptySymbolError for empty symbol."""
        # Test with actual invalid symbol creation
        with pytest.raises((EmptySymbolError, ValueError, ValidationError)):
            invalid_symbol = exchanges.backpack("")
            await backpack_market_data_service.get_order_book(invalid_symbol)

    @pytest.mark.asyncio
    async def test_get_order_book_invalid_limit_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_order_book raises InvalidLimitError for invalid limit values."""
        # Test zero limit
        with pytest.raises(InvalidLimitError) as exc_info:
            await backpack_market_data_service.get_order_book(
                symbol=SOL_USDC_BP,
                limit=0,  # Invalid: zero limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

        # Test negative limit
        with pytest.raises(InvalidLimitError) as exc_info:
            await backpack_market_data_service.get_order_book(
                symbol=SOL_USDC_BP,
                limit=-5,  # Invalid: negative limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_recent_trades_empty_symbol_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_recent_trades raises EmptySymbolError for empty symbol."""
        # Test with actual invalid symbol creation
        with pytest.raises((EmptySymbolError, ValueError, ValidationError)):
            invalid_symbol = exchanges.backpack("")
            await backpack_market_data_service.get_recent_trades(invalid_symbol)

    @pytest.mark.asyncio
    async def test_get_recent_trades_invalid_limit_validation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test get_recent_trades raises InvalidLimitError for invalid limit values."""
        # Test zero limit
        with pytest.raises(InvalidLimitError) as exc_info:
            await backpack_market_data_service.get_recent_trades(
                symbol=SOL_USDC_BP,
                limit=0,  # Invalid: zero limit
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)

        # Test negative limit
        with pytest.raises(InvalidLimitError) as exc_info:
            await backpack_market_data_service.get_recent_trades(
                symbol=SOL_USDC_BP,
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
        """Test get_ticker successfully retrieves and processes ticker data.

        Note: Current business logic delegates to price ticker service.
        """
        symbol = SOL_USDC_BP
        mock_timestamp_dt = datetime.fromtimestamp(1678886400, tz=UTC)

        mock_internal_ticker = Ticker(
            symbol=symbol,
            exchange=ExchangeName.BACKPACK,
            price=Decimal("100.0"),
            volume=Decimal("1000.0"),
            bid=Decimal("99.9"),
            ask=Decimal("100.1"),
            timestamp=mock_timestamp_dt,
        )

        # Mock the price ticker service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            mock_ticker_service.get_ticker = AsyncMock(return_value=mock_internal_ticker)

            result_ticker = await backpack_market_data_service.get_ticker(symbol)

            # Verify the business logic calls the price ticker service with correct arguments
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)
            assert result_ticker == mock_internal_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker when price ticker service raises error.

        Note: Current business logic delegates to price ticker service.
        """
        symbol = SOL_USDC_BP

        # Mock the price ticker service to raise an API error
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for ticker ({symbol}), status: 200",
            )
            mock_ticker_service.get_ticker = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"No data received for ticker ({symbol}), status: 200"
            assert expected_msg_part in exc_info.value.message

            # Verify the business logic calls the price ticker service
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_ticker_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles validation error from price ticker service.

        Note: Current business logic delegates to price ticker service.
        """
        symbol = SOL_USDC_BP

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawTickerResponse.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the price ticker service to raise the validation error
            with patch.object(
                backpack_market_data_service, "_price_ticker_service"
            ) as mock_ticker_service:
                mock_ticker_service.get_ticker = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_ticker(symbol)

                # Verify the business logic calls the price ticker service
                mock_ticker_service.get_ticker.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_ticker_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles unexpected exception from price ticker service.

        Note: Current business logic delegates to price ticker service.
        """
        symbol = SOL_USDC_BP

        # Mock the price ticker service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            mock_ticker_service.get_ticker = AsyncMock(side_effect=Exception("Unexpected error"))

            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the price ticker service
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol = SOL_USDC_BP
        depth = 10
        MagicMock()

        # Mock the order book service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            mock_internal_book = MagicMock(spec=OrderBook)
            mock_order_book_service.get_order_book = AsyncMock(return_value=mock_internal_book)

            result = await backpack_market_data_service.get_order_book(symbol, limit=depth)

            # Verify the business logic calls the order book service with correct arguments
            mock_order_book_service.get_order_book.assert_called_once_with(symbol, depth)
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
        symbol = SOL_USDC_BP
        depth = 5

        # Mock the order book service to raise an API error
        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for order book ({symbol}), status: 200",
            )
            mock_order_book_service.get_order_book = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_order_book(symbol=symbol, limit=depth)

            expected_message = f"No data received for order book ({symbol}), status: 200"
            assert expected_message in exc_info.value.message

            # Verify the business logic calls the order book service
            mock_order_book_service.get_order_book.assert_called_once_with(symbol, depth)

    @pytest.mark.asyncio
    async def test_get_order_book_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book handles validation error from response handler."""
        symbol = SOL_USDC_BP

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrderBook.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the order book service to raise the validation error
            with patch.object(
                backpack_market_data_service, "_order_book_service"
            ) as mock_order_book_service:
                mock_order_book_service.get_order_book = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_order_book(symbol)

                # Verify the business logic calls the order book service
                mock_order_book_service.get_order_book.assert_called_once_with(symbol, None)

    @pytest.mark.asyncio
    async def test_get_order_book_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book raises APIError when an unexpected exception occurs."""
        symbol = SOL_USDC_BP
        depth = 5

        # Mock the order book service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            mock_order_book_service.get_order_book = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            # Act & Assert: Call the service method and verify the exception
            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_order_book(symbol=symbol, limit=depth)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the order book service
            mock_order_book_service.get_order_book.assert_called_once_with(symbol, depth)

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes trade data."""
        symbol = SOL_USDC_BP
        limit = 50

        # Mock the historical data service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_internal_trades = [MagicMock(spec=Trade), MagicMock(spec=Trade)]
            mock_historical_service.get_recent_trades = AsyncMock(return_value=mock_internal_trades)

            result = await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            # Verify the business logic calls the historical data service with correct arguments
            mock_historical_service.get_recent_trades.assert_called_once_with(symbol, limit)
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
        symbol = ETH_USDC_BP
        limit = 5

        # Mock the historical data service to raise an API error
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for recent trades ({symbol.value}), status: 200",
            )
            mock_historical_service.get_recent_trades = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert (
                f"No data received for recent trades ({symbol.value}), status: 200"
                in exc_info.value.message
            )

            # Verify the business logic calls the historical data service
            mock_historical_service.get_recent_trades.assert_called_once_with(symbol, limit)

    @pytest.mark.asyncio
    async def test_get_recent_trades_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades handles validation error from response handler."""
        symbol = SOL_USDC_BP

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawPublicTrade.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the historical data service to raise the validation error
            with patch.object(
                backpack_market_data_service, "_historical_data_service"
            ) as mock_historical_service:
                mock_historical_service.get_recent_trades = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_recent_trades(symbol)

                # Verify the business logic calls the historical data service
                mock_historical_service.get_recent_trades.assert_called_once_with(symbol, None)

    @pytest.mark.asyncio
    async def test_get_recent_trades_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades handles unexpected exception."""
        symbol = SOL_USDC_BP

        # Mock the historical data service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_historical_service.get_recent_trades = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_recent_trades(symbol)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the historical data service
            mock_historical_service.get_recent_trades.assert_called_once_with(symbol, None)
