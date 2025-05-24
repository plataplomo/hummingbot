"""
Unit tests for the BackpackAPI client implementation.
Tests use dependency injection patterns to mock collaborators and focus on public interface testing.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import DerivativePosition, MarginAccountSummary, SpotBalance, Trade
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order

# Constants for testing
TEST_API_KEY = "test_api_key_123"
TEST_API_SECRET = "test_api_secret_456"


# --- Dependency Injection Test Fixtures for BackpackAPI ---


@pytest.fixture
def mock_bp_http_client() -> MagicMock:
    """Mock HttpClient for BackpackAPI."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    return mock_client


@pytest.fixture
def mock_bp_authenticator() -> MagicMock:
    """Mock BackpackHmacAuthenticator."""
    from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator

    mock_auth = MagicMock(spec=BackpackHmacAuthenticator)
    mock_auth.prepare_request = AsyncMock()
    return mock_auth


@pytest.fixture
def mock_bp_error_mapper() -> MagicMock:
    """Mock BackpackErrorMapper."""
    mock_mapper = MagicMock()
    mock_mapper.map_exchange_error = MagicMock()
    mock_mapper.map_string_error = MagicMock()
    return mock_mapper


@pytest.fixture
def mock_bp_request_builder() -> MagicMock:
    """Mock BackpackRequestBuilder."""
    mock_builder = MagicMock()
    mock_builder.build_place_order_payload = MagicMock()
    return mock_builder


@pytest.fixture
def mock_bp_response_handler() -> MagicMock:
    """Mock BackpackResponseHandler."""
    mock_handler = MagicMock()
    mock_handler.handle_response = MagicMock()
    return mock_handler


@pytest.fixture
def mock_bp_mapper() -> MagicMock:
    """Mock BackpackMapper."""
    mock_mapper = MagicMock()
    mock_mapper.transform_raw_to_internal = MagicMock()
    return mock_mapper


@pytest.fixture
def mock_bp_order_mapper() -> MagicMock:
    """Mock BackpackOrderMapper."""
    mock_mapper = MagicMock()
    mock_mapper.transform_raw_order_to_internal = MagicMock()
    return mock_mapper


@pytest.fixture
def mock_bp_account_service() -> MagicMock:
    """Mock BackpackAccountService."""
    mock_service = MagicMock()
    mock_service.get_balances = AsyncMock()
    mock_service.get_account_info = AsyncMock()
    mock_service.get_positions = AsyncMock()
    mock_service.get_order_history = AsyncMock()
    mock_service.get_trade_history = AsyncMock()
    return mock_service


@pytest.fixture
def mock_bp_trading_service() -> MagicMock:
    """Mock BackpackTradingService."""
    mock_service = MagicMock()
    mock_service.place_order = AsyncMock()
    mock_service.cancel_order = AsyncMock()
    mock_service.get_order = AsyncMock()
    mock_service.get_order_status = AsyncMock()
    mock_service.get_open_orders = AsyncMock()
    return mock_service


@pytest.fixture
def mock_bp_market_data_service() -> MagicMock:
    """Mock BackpackMarketDataService."""
    mock_service = MagicMock()
    mock_service.get_ticker = AsyncMock()
    mock_service.get_funding_rates = AsyncMock()
    return mock_service


@pytest.fixture
def mock_bp_ws_manager() -> MagicMock:
    """Mock WebSocketManager for BackpackAPI."""
    mock_manager = MagicMock()
    mock_manager.send_json = AsyncMock()
    mock_manager.close = AsyncMock()
    return mock_manager


@pytest.fixture
def bp_api_with_di(
    backpack_config: dict[str, Any],
    backpack_secrets: dict[str, str | None],
    mock_bp_account_service: MagicMock,
    mock_bp_trading_service: MagicMock,
    mock_bp_market_data_service: MagicMock,
) -> Callable[..., BackpackAPI]:
    """
    Factory fixture to create BackpackAPI instances with all dependencies injected.
    This enables black-box testing without accessing private members.
    """
    from cyberdelta.apis.backpack.bp_api import BackpackAPI

    def _create_api(
        # Allow overriding specific dependencies if needed
        config: dict[str, Any] | None = None,
        secrets: dict[str, str | None] | None = None,
        **overrides: MagicMock,
    ) -> BackpackAPI:
        # Use provided config/secrets or defaults
        actual_config = config if config is not None else backpack_config
        actual_secrets = secrets if secrets is not None else backpack_secrets

        # Create the API instance
        api = BackpackAPI(actual_config, actual_secrets)

        # Inject service dependencies (these are public attributes)
        api.account_service = overrides.get("account_service", mock_bp_account_service)
        api.trading_service = overrides.get("trading_service", mock_bp_trading_service)
        api.market_data_service = overrides.get("market_data_service", mock_bp_market_data_service)

        return api

    return _create_api


class TestBackpackAPIInitialization:
    """Test BackpackAPI initialization with dependency injection."""

    def test_api_creation_with_di_fixture(self, bp_api_with_di: Callable[..., BackpackAPI]) -> None:
        """Test that the DI fixture creates a valid API instance."""
        api = bp_api_with_di()

        # Verify the API instance is created correctly
        assert api is not None
        assert api.exchange_name == "backpack"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

    def test_api_creation_with_custom_config(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test API creation with custom configuration."""
        custom_config = {
            "base_url": "https://custom.backpack.api",
            "ws_endpoint": "wss://custom.backpack.ws",
        }

        api = bp_api_with_di(config=custom_config)
        assert api is not None


class TestBackpackAPIAccountOperations:
    """Test account-related operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_balances_delegates_to_account_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that get_balances properly delegates to account service."""
        api = bp_api_with_di()

        # Configure mock account service
        expected_balances = {
            "USDC": SpotBalance(
                exchange="backpack",
                asset="USDC",
                total_quantity=Decimal("5000.0"),
                available_quantity=Decimal("4800.0"),
                timestamp=datetime.now(UTC),
            )
        }
        mock_bp_account_service.get_balances.return_value = expected_balances

        # Test delegation
        result = await api.get_balances()

        # Verify service was called and result returned
        mock_bp_account_service.get_balances.assert_called_once()
        assert result == expected_balances

        await api.close()

    @pytest.mark.asyncio
    async def test_get_account_summary_delegates_to_account_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that get_account_summary properly delegates to account service."""
        api = bp_api_with_di()

        # Configure mock account service
        expected_summary = MarginAccountSummary(
            exchange="backpack",
            timestamp=datetime.now(UTC),
            total_equity=Decimal("5000.0"),
            available_equity=Decimal("4200.0"),
            total_initial_margin_required=None,
            total_maintenance_margin_required=Decimal("80.0"),
            total_unrealized_pnl=Decimal("25.0"),
        )
        mock_bp_account_service.get_account_info.return_value = expected_summary

        # Test delegation
        result = await api.get_account_summary()

        # Verify service was called and result returned
        mock_bp_account_service.get_account_info.assert_called_once()
        assert result == expected_summary

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_delegates_to_account_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that get_positions properly delegates to account service."""
        api = bp_api_with_di()

        # Configure mock account service
        expected_positions: list[DerivativePosition] = []  # Empty positions list
        mock_bp_account_service.get_positions.return_value = expected_positions

        # Test delegation
        result = await api.get_positions()

        # Verify service was called and result returned
        mock_bp_account_service.get_positions.assert_called_once_with(symbol=None)
        assert result == expected_positions

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_with_symbol_delegates_to_account_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that get_positions with symbol properly delegates to account service."""
        api = bp_api_with_di()

        # Configure mock account service
        expected_positions: list[DerivativePosition] = []
        mock_bp_account_service.get_positions.return_value = expected_positions

        # Test delegation with symbol
        result = await api.get_positions(symbol="SOL")

        # Verify service was called with correct parameters
        mock_bp_account_service.get_positions.assert_called_once_with(symbol="SOL")
        assert result == expected_positions

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_history_delegates_to_account_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that get_order_history properly delegates to account service."""
        api = bp_api_with_di()

        # Configure mock account service
        expected_orders: list[Order] = []  # Empty orders list
        mock_bp_account_service.get_order_history.return_value = expected_orders

        # Test delegation
        result = await api.get_order_history(symbol="SOL")

        # Verify service was called with correct parameters
        mock_bp_account_service.get_order_history.assert_called_once_with(
            symbol="SOL",
            start_time=None,
            end_time=None,
            limit=100,
            order_id=None,
            client_order_id=None,
        )
        assert result == expected_orders

        await api.close()

    @pytest.mark.asyncio
    async def test_get_trade_history_delegates_to_account_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that get_trade_history properly delegates to account service."""
        api = bp_api_with_di()

        # Configure mock account service
        expected_trades: list[Trade] = []  # Empty trades list
        mock_bp_account_service.get_trade_history.return_value = expected_trades

        # Test delegation
        result = await api.get_trade_history(symbol="SOL", limit=50)

        # Verify service was called with correct parameters
        mock_bp_account_service.get_trade_history.assert_called_once_with(symbol="SOL", limit=50)
        assert result == expected_trades

        await api.close()


class TestBackpackAPITradingOperations:
    """Test trading-related operations with service delegation."""

    @pytest.mark.asyncio
    async def test_place_order_delegates_to_trading_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that place_order properly delegates to trading service."""
        api = bp_api_with_di()

        # Configure mock trading service
        test_time = datetime.now(UTC)
        expected_order = Order(
            client_order_id="test_order_789",
            exchange_order_id="bp_order_101",
            exchange="backpack",
            symbol="SOL",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            status=OrderStatus.NEW,
            quantity_requested=Decimal("10.0"),
            quantity_filled=Decimal("0.0"),
            price=Decimal("150.0"),
            average_fill_price=None,
            time_in_force=TimeInForce.GTC,
            created_at=test_time,
            updated_at=test_time,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            reduce_only=False,
            post_only=False,
            trades=[],
        )
        mock_bp_trading_service.place_order.return_value = expected_order

        # Test delegation
        result = await api.place_order(
            symbol="SOL",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.0"),
            price=Decimal("150.0"),
            time_in_force=TimeInForce.GTC,
        )

        # Verify service was called with correct parameters
        mock_bp_trading_service.place_order.assert_called_once_with(
            symbol="SOL",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("150.0"),
            stop_price=None,
            client_order_id=None,
            post_only=False,
        )
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_delegates_to_trading_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that cancel_order properly delegates to trading service."""
        api = bp_api_with_di()

        # Configure mock trading service
        mock_bp_trading_service.cancel_order.return_value = True

        # Test delegation
        result = await api.cancel_order("order_789", symbol="SOL")

        # Verify service was called with correct parameters
        mock_bp_trading_service.cancel_order.assert_called_once_with(
            order_id="order_789", symbol="SOL"
        )
        assert result is True

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_delegates_to_trading_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that get_order properly delegates to trading service."""
        api = bp_api_with_di()

        # Configure mock trading service
        test_order = Order(
            client_order_id="bp_test_order",
            exchange_order_id="bp_order_102",
            exchange="backpack",
            symbol="SOL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("5.0"),
            quantity_filled=Decimal("5.0"),
            price=None,  # Market order
            average_fill_price=Decimal("148.0"),
            time_in_force=TimeInForce.IOC,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            reduce_only=False,
            post_only=False,
            trades=[],
        )
        mock_bp_trading_service.get_order.return_value = test_order

        # Test delegation
        result = await api.get_order("order_102", symbol="SOL")

        # Verify service was called with correct parameters
        mock_bp_trading_service.get_order.assert_called_once_with(
            order_id="order_102", symbol="SOL", client_order_id=None
        )
        assert result == test_order

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_status_delegates_to_trading_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that get_order_status properly delegates to trading service."""
        api = bp_api_with_di()

        # Configure mock trading service
        test_order = Order(
            client_order_id="bp_status_order",
            exchange_order_id="bp_order_103",
            exchange="backpack",
            symbol="SOL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.PARTIALLY_FILLED,
            quantity_requested=Decimal("20.0"),
            quantity_filled=Decimal("12.0"),
            price=Decimal("145.0"),
            average_fill_price=Decimal("145.0"),
            time_in_force=TimeInForce.GTC,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            reduce_only=False,
            post_only=False,
            trades=[],
        )
        mock_bp_trading_service.get_order_status.return_value = test_order

        # Test delegation
        result = await api.get_order_status("order_103", symbol="SOL")

        # Verify service was called with correct parameters
        mock_bp_trading_service.get_order_status.assert_called_once_with(
            order_id="order_103", symbol="SOL", client_order_id=None
        )
        assert result == test_order

        await api.close()

    @pytest.mark.asyncio
    async def test_get_open_orders_delegates_to_trading_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that get_open_orders properly delegates to trading service."""
        api = bp_api_with_di()

        # Configure mock trading service
        expected_orders: list[Order] = []  # Empty orders list
        mock_bp_trading_service.get_open_orders.return_value = expected_orders

        # Test delegation
        result = await api.get_open_orders()

        # Verify service was called and result returned
        mock_bp_trading_service.get_open_orders.assert_called_once_with(symbol=None)
        assert result == expected_orders

        await api.close()


class TestBackpackAPIMarketDataOperations:
    """Test market data operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_ticker_delegates_to_market_data_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_market_data_service: MagicMock
    ) -> None:
        """Test that get_ticker properly delegates to market data service."""
        api = bp_api_with_di()

        # Configure mock market data service
        from cyberdelta.core.models import Ticker

        expected_ticker = Ticker(
            symbol="SOL",
            price=Decimal("150.0"),
            timestamp=datetime.now(UTC),
        )
        mock_bp_market_data_service.get_ticker.return_value = expected_ticker

        # Test delegation
        result = await api.get_ticker("SOL")

        # Verify service was called with correct parameters
        mock_bp_market_data_service.get_ticker.assert_called_once_with(symbol="SOL")
        assert result == expected_ticker

        await api.close()

    @pytest.mark.asyncio
    async def test_get_funding_rates_delegates_to_market_data_service(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_market_data_service: MagicMock
    ) -> None:
        """Test that get_funding_rates properly delegates to market data service."""
        api = bp_api_with_di()

        # Configure mock market data service
        from cyberdelta.core.models import FundingRate

        expected_rates = [
            FundingRate(
                symbol="SOL",
                funding_rate=Decimal("0.0003"),
                timestamp=datetime.now(UTC),
                next_funding_time=datetime.now(UTC),
            ),
        ]
        mock_bp_market_data_service.get_funding_rates.return_value = expected_rates

        # Test delegation
        result = await api.get_funding_rates(symbols=["SOL"])

        # Verify service was called with correct parameters
        mock_bp_market_data_service.get_funding_rates.assert_called_once_with(symbols=["SOL"])
        assert result == expected_rates

        await api.close()


class TestBackpackAPIErrorHandling:
    """Test error handling and propagation."""

    @pytest.mark.asyncio
    async def test_service_error_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that service errors are properly propagated."""
        api = bp_api_with_di()

        # Configure mock service to raise an error
        service_error = APIError(
            "Symbol not found",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_bp_trading_service.get_order.side_effect = service_error

        # Test error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_order("missing_order", symbol="SOL")

        # Verify the error is the same as from the service
        assert exc_info.value == service_error

        await api.close()

    @pytest.mark.asyncio
    async def test_authentication_error_handling(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test authentication error handling."""
        api = bp_api_with_di()

        # Configure mock service to raise authentication error
        auth_error = APIError(
            "Invalid API credentials",
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
        )
        mock_bp_trading_service.place_order.side_effect = auth_error

        # Test error propagation
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="SOL",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("150.0"),
                time_in_force=TimeInForce.GTC,
            )

        # Verify the error code
        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value

        await api.close()


class TestBackpackAPIComprehensiveErrorHandling:
    """Comprehensive edge case and failure scenario testing."""

    # =============================================================================
    # I. DATA RETRIEVAL METHOD ERROR SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_balances_service_validation_error(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_balances handles service ValidationError gracefully."""
        api = bp_api_with_di()

        # Mock service to raise APIError wrapping ValidationError
        from pydantic import ValidationError

        mock_bp_account_service.get_balances.side_effect = APIError(
            message="Invalid balance response structure",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=ValidationError.from_exception_data(
                title="BalanceModel", line_errors=[]
            ),
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid balance response structure" in exc_info.value.message
        mock_bp_account_service.get_balances.assert_called_once()

        await api.close()

    @pytest.mark.asyncio
    async def test_get_ticker_empty_successful_response(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_market_data_service: MagicMock
    ) -> None:
        """Test get_ticker handles empty but successful response correctly."""
        api = bp_api_with_di()

        # Mock service to return None (no ticker found)
        mock_bp_market_data_service.get_ticker.return_value = None

        result = await api.get_ticker("UNKNOWN_SYMBOL")

        assert result is None
        # DEFENSIVE CHECK: Mock assertion after successful test. Mypy=[unreachable]
        mock_bp_market_data_service.get_ticker.assert_called_once_with(symbol="UNKNOWN_SYMBOL")  # type: ignore[unreachable]

        # DEFENSIVE CHECK: Resource cleanup after test. Mypy=[unreachable]
        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_rate_limited_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_positions propagates RATE_LIMITED error correctly."""
        api = bp_api_with_di()

        mock_bp_account_service.get_positions.side_effect = APIError(
            message="Rate limit exceeded",
            code=APIErrorCode.RATE_LIMITED.value,
            http_status=429,
            exchange_message="rate_limit_exceeded",
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_positions()

        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value
        assert exc_info.value.http_status == 429
        assert "Rate limit exceeded" in exc_info.value.message

        await api.close()

    @pytest.mark.asyncio
    async def test_get_account_summary_server_error_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_account_summary propagates SERVER_ERROR correctly."""
        api = bp_api_with_di()

        mock_bp_account_service.get_account_info.side_effect = APIError(
            message="Internal server error occurred",
            code=APIErrorCode.SERVER_ERROR.value,
            http_status=500,
            exchange_message="internal_server_error",
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value.code == APIErrorCode.SERVER_ERROR.value
        assert exc_info.value.http_status == 500
        assert "Internal server error" in exc_info.value.message

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_history_timeout_error_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_order_history propagates TIMEOUT error correctly."""
        api = bp_api_with_di()

        mock_bp_account_service.get_order_history.side_effect = APIError(
            message="Request timeout after 30 seconds",
            code=APIErrorCode.TIMEOUT.value,
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_order_history()

        assert exc_info.value.code == APIErrorCode.TIMEOUT.value
        assert "timeout" in exc_info.value.message.lower()

        await api.close()

    @pytest.mark.asyncio
    async def test_get_trade_history_service_unavailable_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_trade_history propagates SERVICE_UNAVAILABLE error correctly."""
        api = bp_api_with_di()

        mock_bp_account_service.get_trade_history.side_effect = APIError(
            message="Service temporarily unavailable",
            code=APIErrorCode.SERVICE_UNAVAILABLE.value,
            http_status=503,
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_trade_history()

        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == 503

        await api.close()

    # =============================================================================
    # II. TRADING OPERATION ERROR SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_place_order_service_unexpected_exception(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test place_order handles unexpected service exceptions correctly."""
        api = bp_api_with_di()

        # Mock service to raise unexpected exception
        mock_bp_trading_service.place_order.side_effect = RuntimeError("Unexpected service failure")

        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="BTC_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.1"),
                price=Decimal("50000"),
                time_in_force=TimeInForce.GTC,
            )

        # API should wrap unexpected exceptions
        assert exc_info.value.code in [
            APIErrorCode.UNKNOWN.value,
            APIErrorCode.EXCHANGE_SPECIFIC.value,
        ]
        assert isinstance(exc_info.value.original_exception, RuntimeError)
        assert "Unexpected service failure" in str(exc_info.value.original_exception)

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_insufficient_balance_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test cancel_order propagates INSUFFICIENT_FUNDS error correctly."""
        api = bp_api_with_di()

        mock_bp_trading_service.cancel_order.side_effect = APIError(
            message="Insufficient balance for cancellation fee",
            code=APIErrorCode.INSUFFICIENT_FUNDS.value,
        )

        with pytest.raises(APIError) as exc_info:
            await api.cancel_order("order_123", symbol="SOL_USDC")

        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        assert "Insufficient balance" in exc_info.value.message

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_order_not_found_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test get_order propagates ORDER_NOT_FOUND error correctly."""
        api = bp_api_with_di()

        mock_bp_trading_service.get_order.side_effect = APIError(
            message="Order not found",
            code=APIErrorCode.ORDER_NOT_FOUND.value,
            http_status=404,
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_order("nonexistent_order", symbol="BTC_USDC")

        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert exc_info.value.http_status == 404

        await api.close()

    @pytest.mark.asyncio
    async def test_get_open_orders_exchange_specific_error(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test get_open_orders handles exchange-specific errors."""
        api = bp_api_with_di()

        mock_bp_trading_service.get_open_orders.side_effect = APIError(
            message="Exchange maintenance mode",
            code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            exchange_message="MAINTENANCE_MODE",
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_open_orders()

        assert exc_info.value.code == APIErrorCode.EXCHANGE_SPECIFIC.value
        assert "Exchange maintenance mode" in exc_info.value.message

        await api.close()

    # =============================================================================
    # III. INPUT VALIDATION AND BOUNDARY TESTING
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_ticker_none_symbol_input(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_market_data_service: MagicMock
    ) -> None:
        """Test get_ticker behavior with None symbol input."""
        api = bp_api_with_di()

        # Configure mock service to raise TypeError for None input
        # (simulating real service behavior)
        def mock_get_ticker_side_effect(symbol: str | None) -> None:
            if symbol is None:
                raise TypeError("symbol must be a string, not NoneType")
            return None  # This won't be reached for None input

        mock_bp_market_data_service.get_ticker.side_effect = mock_get_ticker_side_effect

        # This should be handled by type hints, but test runtime behavior
        with pytest.raises((APIError, TypeError, ValueError)):
            await api.get_ticker(None)  # type: ignore[arg-type]

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_empty_symbol_input(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_positions behavior with empty symbol input."""
        api = bp_api_with_di()

        # Service might return empty list for empty symbol
        mock_bp_account_service.get_positions.return_value = []

        result = await api.get_positions(symbol="")
        assert result == []
        mock_bp_account_service.get_positions.assert_called_once_with(symbol="")

        await api.close()

    @pytest.mark.asyncio
    async def test_place_order_invalid_quantity_input(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test place_order with invalid quantity input."""
        api = bp_api_with_di()

        # Mock service to validate and reject invalid quantity
        mock_bp_trading_service.place_order.side_effect = APIError(
            message="Invalid order quantity",
            code=APIErrorCode.INVALID_REQUEST.value,
        )

        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="BTC_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("-0.1"),  # Negative quantity
                price=Decimal("50000"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value

        await api.close()

    # =============================================================================
    # IV. SERVICE INTEGRATION ERROR CHAINING
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_balances_full_error_chain_validation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_balances error handling through complete call chain."""
        api = bp_api_with_di()

        # Test scenario where account service raises APIError
        mock_bp_account_service.get_balances.side_effect = APIError(
            message="Account service request failed",
            code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            http_status=503,
            exchange_message="upstream_service_error",
        )

        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value.code == APIErrorCode.EXCHANGE_SPECIFIC.value
        assert exc_info.value.http_status == 503
        assert "Account service request failed" in exc_info.value.message

        await api.close()

    @pytest.mark.asyncio
    async def test_multiple_service_error_isolation(
        self,
        bp_api_with_di: Callable[..., BackpackAPI],
        mock_bp_account_service: MagicMock,
        mock_bp_trading_service: MagicMock,
        mock_bp_market_data_service: MagicMock,
    ) -> None:
        """Test that errors in one service don't affect others."""
        api = bp_api_with_di()

        # Configure different errors for different services
        mock_bp_account_service.get_balances.side_effect = APIError(
            message="Account service error",
            code=APIErrorCode.RATE_LIMITED.value,
        )

        mock_bp_trading_service.get_open_orders.return_value = []  # Success
        mock_bp_market_data_service.get_ticker.return_value = None  # Success (no data)

        # Account service should fail
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()
        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value

        # Trading service should still work
        orders = await api.get_open_orders()
        assert orders == []

        # Market data service should still work
        ticker = await api.get_ticker("BTC_USDC")
        assert ticker is None

        # DEFENSIVE CHECK: Resource cleanup after test. Mypy=[unreachable]
        await api.close()  # type: ignore[unreachable]

    # =============================================================================
    # V. COMPLEX MULTI-STEP OPERATION FAILURES
    # =============================================================================

    @pytest.mark.asyncio
    async def test_complex_operation_partial_failure_simulation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test complex operations with partial failures."""
        api = bp_api_with_di()

        # Simulate a scenario where multiple orders are placed, some succeed, some fail
        def place_order_side_effect(
            symbol: str, **_kwargs: str | OrderSide | OrderType | Decimal | TimeInForce
        ) -> Order:
            # Check the symbol to determine success/failure
            if symbol == "ETH_USDC":
                raise APIError(
                    message="Insufficient balance",
                    code=APIErrorCode.INSUFFICIENT_FUNDS.value,
                )
            elif symbol == "SOL_USDC":
                raise APIError(
                    message="Symbol temporarily suspended",
                    code=APIErrorCode.SYMBOL_NOT_FOUND.value,
                )
            else:
                # Success case
                return Order(
                    exchange="backpack",
                    symbol=symbol,
                    side=OrderSide.BUY,
                    status=OrderStatus.NEW,
                    order_type=OrderType.LIMIT,
                    quantity_requested=Decimal("1.0"),
                    price=Decimal("100.0"),
                    time_in_force=TimeInForce.GTC,
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                    triggered_at=None,
                    strategy_name=None,
                    signal_id=None,
                )

        mock_bp_trading_service.place_order.side_effect = place_order_side_effect

        # Test successful order
        order = await api.place_order(
            symbol="BTC_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000"),
            time_in_force=TimeInForce.GTC,
        )
        assert order.symbol == "BTC_USDC"

        # Test insufficient balance error
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="ETH_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("3000"),
                time_in_force=TimeInForce.GTC,
            )
        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value

        # Test symbol not found error
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10"),
                price=Decimal("100"),
                time_in_force=TimeInForce.GTC,
            )
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value

        await api.close()

    @pytest.mark.asyncio
    async def test_concurrent_operation_error_handling(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test error handling in concurrent operations."""
        api = bp_api_with_di()

        # Configure service to behave differently for concurrent calls
        call_count = 0

        def get_balances_side_effect() -> dict[str, SpotBalance]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {}  # First call succeeds with empty dict
            else:
                raise APIError(
                    message="Concurrent request limit exceeded",
                    code=APIErrorCode.RATE_LIMITED.value,
                )

        mock_bp_account_service.get_balances.side_effect = get_balances_side_effect

        # First call should succeed
        balances1 = await api.get_balances()
        assert not balances1  # Empty dict check instead of comparing to empty list

        # Second call should fail
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()
        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value

        # DEFENSIVE CHECK: Cleanup after pytest.raises context. Mypy=[unreachable]
        await api.close()


class TestBackpackAPIWebSocketOperations:
    """Test WebSocket operations using black-box approach."""

    @pytest.mark.asyncio
    async def test_subscribe_delegates_to_ws_manager(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test that subscribe works through public interface."""
        api = bp_api_with_di()

        # Create a mock handler
        async def mock_handler(data: dict[str, Any], full_message: dict[str, Any]) -> None:
            pass

        # Test subscription (this tests the public interface)
        # The actual WebSocket manager is mocked, so this tests orchestration
        try:
            await api.subscribe("depth:SOL_USDC", mock_handler)
            # If no exception, the subscription interface works
            assert True
        except Exception as e:
            # If there's an exception, it should be from the mocked dependencies
            # not from the API interface itself
            pytest.fail(f"Subscription failed: {e}")

        await api.close()

    def test_subscription_payload_construction_public_behavior(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test subscription payload construction through public behavior."""
        api = bp_api_with_di()

        # We can't directly test the private method, but we can test
        # that the API can be instantiated and has the expected public interface
        assert hasattr(api, "subscribe")
        assert callable(api.subscribe)

    @pytest.mark.asyncio
    async def test_websocket_message_handling_public_behavior(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test WebSocket message handling through public behavior."""
        api = bp_api_with_di()

        # Test that the API can handle subscription setup
        # This indirectly tests the WebSocket message handling setup

        message_received = False

        async def test_handler(data: dict[str, Any], full_message: dict[str, Any]) -> None:
            nonlocal message_received
            message_received = True

        # Subscribe to a topic
        await api.subscribe("ticker:SOL_USDC", test_handler)

        # The WebSocket manager is mocked, so we can't test actual message routing
        # But we can verify the subscription was set up
        assert True  # If we get here, subscription worked

        await api.close()


class TestBackpackAPIDependencyIsolation:
    """Test that dependency injection provides proper isolation."""

    def test_custom_dependency_override(self, bp_api_with_di: Callable[..., BackpackAPI]) -> None:
        """Test that specific dependencies can be overridden."""
        # Create a custom mock trading service
        custom_trading_service = MagicMock()

        # Create API instance with custom dependency
        api = bp_api_with_di(trading_service=custom_trading_service)

        # Verify the custom service is used
        assert api.trading_service is custom_trading_service

    def test_multiple_api_instances_are_isolated(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test that multiple API instances don't share dependencies."""
        # Create separate mock instances for each API
        mock_trading_1 = MagicMock()
        mock_account_1 = MagicMock()
        mock_market_1 = MagicMock()

        mock_trading_2 = MagicMock()
        mock_account_2 = MagicMock()
        mock_market_2 = MagicMock()

        api1 = bp_api_with_di(
            trading_service=mock_trading_1,
            account_service=mock_account_1,
            market_data_service=mock_market_1,
        )
        api2 = bp_api_with_di(
            trading_service=mock_trading_2,
            account_service=mock_account_2,
            market_data_service=mock_market_2,
        )

        # Verify instances are different
        assert api1 is not api2
        assert api1.trading_service is not api2.trading_service
        assert api1.account_service is not api2.account_service
        assert api1.market_data_service is not api2.market_data_service

    def test_dependency_injection_completeness(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test that all expected dependencies are injected."""
        api = bp_api_with_di()

        # Verify all major services are available
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

        # Verify services are not None
        assert api.trading_service is not None
        assert api.account_service is not None
        assert api.market_data_service is not None

        # Verify services have expected methods (they are mocks)
        assert hasattr(api.trading_service, "place_order")
        assert hasattr(api.account_service, "get_balances")
        assert hasattr(api.market_data_service, "get_ticker")


class TestBackpackAPIResourceManagement:
    """Test resource management and cleanup."""

    @pytest.mark.asyncio
    async def test_api_close_cleanup(self, bp_api_with_di: Callable[..., BackpackAPI]) -> None:
        """Test that API close method works correctly."""
        api = bp_api_with_di()

        # Close should not raise an exception
        await api.close()

        # Should be able to call close multiple times
        await api.close()

    @pytest.mark.asyncio
    async def test_context_manager_behavior(
        self, bp_api_with_di: Callable[..., BackpackAPI]
    ) -> None:
        """Test API as context manager."""
        # Test that API can be used in a context manager
        # (if implemented in the future)
        api = bp_api_with_di()

        try:
            # Simulate some operations
            assert api is not None
        finally:
            await api.close()
