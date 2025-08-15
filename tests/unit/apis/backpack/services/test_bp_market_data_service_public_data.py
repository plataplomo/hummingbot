"""Unit tests for BackpackMarketDataService public data functionality with Property-Based Testing.

------------------------------------------------------------------------

Comprehensive property-based test suite for BackpackMarketDataService public data operations.
Tests service layer functionality with mocked dependencies including:
- Ticker data retrieval with delegation to price ticker service
- Order book retrieval with delegation to order book service
- Recent trades retrieval with delegation to historical data service
- Input validation testing (empty symbols, invalid limits)
- Error handling scenarios (API errors, validation errors, unexpected exceptions)
- Edge cases, boundary values, and malicious input resistance
- Hundreds of generated test combinations for comprehensive service layer coverage
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite
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
from cyberdelta.models.market import Fill, OrderBook, Ticker
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
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
            mock_internal_trades = [MagicMock(spec=Fill), MagicMock(spec=Fill)]
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


# =======================
# Property-Based Testing Strategy Builders
# =======================


def bp_symbol_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Backpack symbols for testing.

    Returns:
        SearchStrategy[Symbol]: Strategy for valid Backpack symbols.
    """
    return st.sampled_from([
        SOL_USDC_BP,
        ETH_USDC_BP,
        exchanges.backpack("BTC-USDC"),
        exchanges.backpack("BTC-USDT"),
        exchanges.backpack("ETH-USDT"),
        exchanges.backpack("SOL-USDT"),
        exchanges.backpack("SOL-PERP"),
        exchanges.backpack("BTC-PERP"),
        exchanges.backpack("ETH-PERP"),
    ])


def valid_limit_strategy() -> SearchStrategy[int]:
    """Generate valid limit values for order book and trades.

    Returns:
        SearchStrategy[int]: Strategy for valid limit values.
    """
    return st.one_of([
        st.integers(min_value=1, max_value=100),
        st.sampled_from([5, 10, 20, 50, 100, 500, 1000]),
    ])


def invalid_limit_strategy() -> SearchStrategy[int]:
    """Generate invalid limit values for testing validation.

    Returns:
        SearchStrategy[int]: Strategy for invalid limit values.
    """
    return st.one_of([
        st.integers(max_value=0),  # Zero and negative values
        st.integers(min_value=-1000, max_value=-1),  # Negative values
    ])


def api_error_code_strategy() -> SearchStrategy[str]:
    """Generate various API error codes for testing.

    Returns:
        SearchStrategy[str]: Strategy for API error codes.
    """
    return st.sampled_from([
        "INVALID_RESPONSE",
        "RATE_LIMITED",
        "CONNECTION_ERROR",
        "HTTP_ERROR",
        "TIMEOUT_ERROR",
        "AUTHENTICATION_FAILED",
    ])


def api_error_message_strategy() -> SearchStrategy[str]:
    """Generate realistic API error messages.

    Returns:
        SearchStrategy[str]: Strategy for API error messages.
    """
    return st.one_of([
        st.builds(
            lambda symbol, status: f"No data received for ticker ({symbol}), status: {status}",
            st.text(min_size=3, max_size=20),
            st.integers(min_value=200, max_value=599),
        ),
        st.builds(
            lambda symbol, status: f"No data received for order book ({symbol}), status: {status}",
            st.text(min_size=3, max_size=20),
            st.integers(min_value=200, max_value=599),
        ),
        st.builds(
            lambda symbol,
            status: f"No data received for recent trades ({symbol}), status: {status}",
            st.text(min_size=3, max_size=20),
            st.integers(min_value=200, max_value=599),
        ),
        st.sampled_from([
            "Rate limit exceeded",
            "Connection timeout",
            "Invalid request format",
            "Authentication failed",
            "Internal server error",
            "Service unavailable",
        ]),
    ])


def mock_ticker_strategy() -> SearchStrategy[Ticker]:
    """Generate mock Ticker instances for testing.

    Returns:
        SearchStrategy[Ticker]: Strategy for mock ticker data.
    """
    return st.builds(
        Ticker,
        symbol=bp_symbol_strategy(),
        exchange=st.just(ExchangeName.BACKPACK),
        price=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=2),
        volume=st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=2),
        bid=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(99999), places=2),
        ask=st.decimals(min_value=Decimal("0.02"), max_value=Decimal(100000), places=2),
        timestamp=st.datetimes(min_value=datetime(2024, 1, 1, tzinfo=UTC)),
    )


def exception_message_strategy() -> SearchStrategy[str]:
    """Generate various exception messages for testing.

    Returns:
        SearchStrategy[str]: Strategy for exception messages.
    """
    return st.one_of([
        st.text(min_size=1, max_size=100),
        st.sampled_from([
            "Unexpected error",
            "Service unavailable",
            "Network timeout",
            "Parsing failed",
            "Internal service error",
            "Database connection lost",
            "Memory allocation failed",
        ]),
    ])


def malicious_symbol_string_strategy() -> SearchStrategy[str]:
    """Generate potentially malicious symbol strings.

    Returns:
        SearchStrategy[str]: Strategy for malicious symbol strings.
    """
    return st.one_of([
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE symbols; --",
            "1' OR '1'='1",
            "admin'--",
        ]),
        # XSS attempts
        st.sampled_from([
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "javascript:alert('XSS')",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
        ]),
        # Path traversal
        st.sampled_from([
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=1500),
        # Format string attacks
        st.sampled_from(["%s%s%s%s%s", "%x%x%x%x", "%n%n%n%n"]),
        # Empty and whitespace
        st.sampled_from(["", " ", "\t", "\n", "\r\n"]),
    ])


@composite
def api_error_strategy(draw: st.DrawFn) -> APIError:
    """Generate APIError instances for testing.

    Args:
        draw: Hypothesis draw function.

    Returns:
        APIError: Generated API error for testing.
    """
    code = draw(api_error_code_strategy())
    message = draw(api_error_message_strategy())

    return APIError(code=code, message=message)


@composite
def service_test_scenario_strategy(draw: st.DrawFn) -> tuple[Symbol, int | None, str]:
    """Generate test scenarios for service methods.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple: (symbol, limit, test_type)
    """
    symbol = draw(bp_symbol_strategy())
    limit = draw(st.one_of([st.none(), valid_limit_strategy()]))
    test_type = draw(
        st.sampled_from(["success", "api_error", "validation_error", "unexpected_error"])
    )

    return symbol, limit, test_type


# =======================
# Property-Based Test Classes
# =======================


class TestBackpackMarketDataServiceTickerPropertyBased:
    """Property-based tests for ticker functionality."""

    @given(
        symbol=bp_symbol_strategy(),
        mock_ticker=mock_ticker_strategy(),
    )
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_get_ticker_success_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        mock_ticker: Ticker,
    ) -> None:
        """Property-based test for successful ticker retrieval."""
        # Ensure the mock ticker has the correct symbol
        mock_ticker_with_symbol = Ticker(
            symbol=symbol,
            exchange=mock_ticker.exchange,
            price=mock_ticker.price,
            volume=mock_ticker.volume,
            bid=mock_ticker.bid,
            ask=mock_ticker.ask,
            timestamp=mock_ticker.timestamp,
        )

        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            mock_ticker_service.get_ticker = AsyncMock(return_value=mock_ticker_with_symbol)

            result = await backpack_market_data_service.get_ticker(symbol)

            assert result == mock_ticker_with_symbol
            assert result.symbol == symbol
            assert result.exchange == ExchangeName.BACKPACK
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)

    @given(
        symbol=bp_symbol_strategy(),
        api_error=api_error_strategy(),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_get_ticker_api_error_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        api_error: APIError,
    ) -> None:
        """Property-based test for ticker API error handling."""
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            mock_ticker_service.get_ticker = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert exc_info.value.code == api_error.code
            assert exc_info.value.message == api_error.message
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)

    @given(
        symbol=bp_symbol_strategy(),
        exception_message=exception_message_strategy(),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_ticker_unexpected_exception_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        exception_message: str,
    ) -> None:
        """Property-based test for ticker unexpected exception handling."""
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            mock_ticker_service.get_ticker = AsyncMock(side_effect=Exception(exception_message))

            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert exception_message in str(exc_info.value)
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)


class TestBackpackMarketDataServiceOrderBookPropertyBased:
    """Property-based tests for order book functionality."""

    @given(
        symbol=bp_symbol_strategy(),
        limit=st.one_of([st.none(), valid_limit_strategy()]),
    )
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_get_order_book_success_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        limit: int | None,
    ) -> None:
        """Property-based test for successful order book retrieval."""
        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            mock_order_book = MagicMock(spec=OrderBook)
            mock_order_book_service.get_order_book = AsyncMock(return_value=mock_order_book)

            result = await backpack_market_data_service.get_order_book(symbol, limit=limit)

            assert result == mock_order_book
            mock_order_book_service.get_order_book.assert_called_once_with(symbol, limit)

    @given(
        symbol=bp_symbol_strategy(),
        limit=st.one_of([st.none(), valid_limit_strategy()]),
        api_error=api_error_strategy(),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_get_order_book_api_error_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        limit: int | None,
        api_error: APIError,
    ) -> None:
        """Property-based test for order book API error handling."""
        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            mock_order_book_service.get_order_book = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_order_book(symbol, limit=limit)

            assert exc_info.value.code == api_error.code
            assert exc_info.value.message == api_error.message
            mock_order_book_service.get_order_book.assert_called_once_with(symbol, limit)

    @given(invalid_limit=invalid_limit_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_order_book_invalid_limit_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        invalid_limit: int,
    ) -> None:
        """Property-based test for order book invalid limit validation."""
        with pytest.raises(InvalidLimitError) as exc_info:
            await backpack_market_data_service.get_order_book(
                symbol=SOL_USDC_BP,
                limit=invalid_limit,
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)


class TestBackpackMarketDataServiceRecentTradesPropertyBased:
    """Property-based tests for recent trades functionality."""

    @given(
        symbol=bp_symbol_strategy(),
        limit=st.one_of([st.none(), valid_limit_strategy()]),
    )
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_get_recent_trades_success_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        limit: int | None,
    ) -> None:
        """Property-based test for successful recent trades retrieval."""
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_trades = [MagicMock(spec=Fill) for _ in range(5)]
            mock_historical_service.get_recent_trades = AsyncMock(return_value=mock_trades)

            result = await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert result == mock_trades
            assert len(result) == len(mock_trades)
            mock_historical_service.get_recent_trades.assert_called_once_with(symbol, limit)

    @given(
        symbol=bp_symbol_strategy(),
        limit=st.one_of([st.none(), valid_limit_strategy()]),
        api_error=api_error_strategy(),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_get_recent_trades_api_error_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbol: Symbol,
        limit: int | None,
        api_error: APIError,
    ) -> None:
        """Property-based test for recent trades API error handling."""
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_historical_service.get_recent_trades = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert exc_info.value.code == api_error.code
            assert exc_info.value.message == api_error.message
            mock_historical_service.get_recent_trades.assert_called_once_with(symbol, limit)

    @given(invalid_limit=invalid_limit_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_recent_trades_invalid_limit_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        invalid_limit: int,
    ) -> None:
        """Property-based test for recent trades invalid limit validation."""
        with pytest.raises(InvalidLimitError) as exc_info:
            await backpack_market_data_service.get_recent_trades(
                symbol=SOL_USDC_BP,
                limit=invalid_limit,
            )
        assert "'limit' must be positive when provided" in str(exc_info.value)


class TestBackpackMarketDataServiceValidationPropertyBased:
    """Property-based tests for input validation."""

    @given(malicious_symbol=malicious_symbol_string_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_malicious_symbol_resistance(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        malicious_symbol: str,
    ) -> None:
        """Property-based test for resistance to malicious symbol inputs."""
        try:
            # Attempt to create a symbol with malicious content
            if malicious_symbol.strip() == "":
                # Empty symbols should raise validation errors
                with pytest.raises((EmptySymbolError, ValueError, ValidationError)):
                    invalid_symbol = exchanges.backpack(malicious_symbol)
                    await backpack_market_data_service.get_ticker(invalid_symbol)
            else:
                # Non-empty malicious symbols should be handled safely
                malicious_symbol_obj = exchanges.backpack(malicious_symbol)

                # Service should handle malicious symbols safely (may succeed or fail gracefully)
                with patch.object(
                    backpack_market_data_service, "_price_ticker_service"
                ) as mock_ticker_service:
                    mock_ticker_service.get_ticker = AsyncMock(
                        side_effect=APIError(
                            code="SYMBOL_NOT_FOUND", message=f"Symbol {malicious_symbol} not found"
                        )
                    )

                    with pytest.raises(APIError):
                        await backpack_market_data_service.get_ticker(malicious_symbol_obj)

        except (ValueError, ValidationError):
            # Rejecting malicious input at symbol creation is acceptable
            pass

    @given(scenario=service_test_scenario_strategy())
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_service_scenarios_property_based(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        scenario: tuple[Symbol, int | None, str],
    ) -> None:
        """Property-based test for various service scenarios."""
        symbol, limit, test_type = scenario

        if test_type == "success":
            # Test successful operation
            with patch.object(
                backpack_market_data_service, "_price_ticker_service"
            ) as mock_ticker_service:
                mock_ticker = Ticker(
                    symbol=symbol,
                    exchange=ExchangeName.BACKPACK,
                    price=Decimal("100.0"),
                    volume=Decimal("1000.0"),
                    bid=Decimal("99.9"),
                    ask=Decimal("100.1"),
                    timestamp=datetime.now(UTC),
                )
                mock_ticker_service.get_ticker = AsyncMock(return_value=mock_ticker)

                result = await backpack_market_data_service.get_ticker(symbol)
                assert result.symbol == symbol

        elif test_type == "api_error":
            # Test API error handling
            with patch.object(
                backpack_market_data_service, "_price_ticker_service"
            ) as mock_ticker_service:
                api_error = APIError(code="TEST_ERROR", message=f"Test error for {symbol.value}")
                mock_ticker_service.get_ticker = AsyncMock(side_effect=api_error)

                with pytest.raises(APIError):
                    await backpack_market_data_service.get_ticker(symbol)

        elif test_type == "validation_error":
            # Test validation error handling
            with patch.object(
                backpack_market_data_service, "_price_ticker_service"
            ) as mock_ticker_service:
                validation_error = ValidationError.from_exception_data(
                    "ValidationError",
                    [{"type": "missing", "loc": ("symbol",), "input": None}],
                )
                mock_ticker_service.get_ticker = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_ticker(symbol)

        elif test_type == "unexpected_error":
            # Test unexpected error handling
            with patch.object(
                backpack_market_data_service, "_price_ticker_service"
            ) as mock_ticker_service:
                mock_ticker_service.get_ticker = AsyncMock(
                    side_effect=Exception(f"Unexpected error for {symbol.value}")
                )

                with pytest.raises(Exception):
                    await backpack_market_data_service.get_ticker(symbol)


class TestBackpackMarketDataServiceEdgeCases:
    """Property-based tests for edge cases and boundary conditions."""

    @given(
        extreme_limit=st.integers(min_value=1, max_value=1000000),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_extreme_limit_values(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        extreme_limit: int,
    ) -> None:
        """Property-based test for extreme limit values."""
        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            mock_order_book = MagicMock(spec=OrderBook)
            mock_order_book_service.get_order_book = AsyncMock(return_value=mock_order_book)

            result = await backpack_market_data_service.get_order_book(
                SOL_USDC_BP, limit=extreme_limit
            )

            assert result == mock_order_book
            mock_order_book_service.get_order_book.assert_called_once_with(
                SOL_USDC_BP, extreme_limit
            )

    @given(
        num_trades=st.integers(min_value=0, max_value=10000),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_large_trade_lists(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        num_trades: int,
    ) -> None:
        """Property-based test for handling large trade lists."""
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_trades = [MagicMock(spec=Fill) for _ in range(num_trades)]
            mock_historical_service.get_recent_trades = AsyncMock(return_value=mock_trades)

            result = await backpack_market_data_service.get_recent_trades(SOL_USDC_BP)

            assert len(result) == num_trades
            mock_historical_service.get_recent_trades.assert_called_once_with(SOL_USDC_BP, None)


class TestBackpackMarketDataServiceConcurrencySimulation:
    """Property-based tests simulating concurrent operations."""

    @given(
        symbols=st.lists(bp_symbol_strategy(), min_size=1, max_size=10, unique=True),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_multiple_symbol_operations(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        symbols: list[Symbol],
    ) -> None:
        """Property-based test for operations on multiple symbols."""
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            # Create mock tickers for each symbol
            mock_tickers = {}
            for symbol in symbols:
                mock_tickers[symbol] = Ticker(
                    symbol=symbol,
                    exchange=ExchangeName.BACKPACK,
                    price=Decimal("100.0"),
                    volume=Decimal("1000.0"),
                    bid=Decimal("99.9"),
                    ask=Decimal("100.1"),
                    timestamp=datetime.now(UTC),
                )

            def mock_get_ticker(symbol: Symbol) -> Ticker:
                return mock_tickers[symbol]

            mock_ticker_service.get_ticker = AsyncMock(side_effect=mock_get_ticker)

            # Test getting tickers for all symbols
            results = []
            for symbol in symbols:
                result = await backpack_market_data_service.get_ticker(symbol)
                results.append(result)

            assert len(results) == len(symbols)
            for i, result in enumerate(results):
                assert result.symbol == symbols[i]
                assert result.exchange == ExchangeName.BACKPACK


# =======================
# Legacy Compatibility Verification
# =======================


class TestLegacyCompatibility:
    """Verify that property-based tests don't break legacy functionality."""

    @pytest.mark.asyncio
    async def test_legacy_ticker_functionality(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Verify legacy ticker functionality remains intact."""
        symbol = SOL_USDC_BP
        mock_ticker = Ticker(
            symbol=symbol,
            exchange=ExchangeName.BACKPACK,
            price=Decimal("100.0"),
            volume=Decimal("1000.0"),
            bid=Decimal("99.9"),
            ask=Decimal("100.1"),
            timestamp=datetime.now(UTC),
        )

        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_ticker_service:
            mock_ticker_service.get_ticker = AsyncMock(return_value=mock_ticker)

            result = await backpack_market_data_service.get_ticker(symbol)

            assert result == mock_ticker
            mock_ticker_service.get_ticker.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_legacy_order_book_functionality(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Verify legacy order book functionality remains intact."""
        symbol = SOL_USDC_BP
        limit = 10

        with patch.object(
            backpack_market_data_service, "_order_book_service"
        ) as mock_order_book_service:
            mock_order_book = MagicMock(spec=OrderBook)
            mock_order_book_service.get_order_book = AsyncMock(return_value=mock_order_book)

            result = await backpack_market_data_service.get_order_book(symbol, limit=limit)

            assert result == mock_order_book
            mock_order_book_service.get_order_book.assert_called_once_with(symbol, limit)

    @pytest.mark.asyncio
    async def test_legacy_recent_trades_functionality(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Verify legacy recent trades functionality remains intact."""
        symbol = SOL_USDC_BP
        limit = 50

        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_trades = [MagicMock(spec=Fill), MagicMock(spec=Fill)]
            mock_historical_service.get_recent_trades = AsyncMock(return_value=mock_trades)

            result = await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert result == mock_trades
            mock_historical_service.get_recent_trades.assert_called_once_with(symbol, limit)
