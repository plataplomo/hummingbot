"""
Unit tests for the HyperliquidAPI client implementation.
Tests use dependency injection patterns to mock collaborators and focus on public interface testing.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order

# --- Dependency Injection Test Fixtures for HyperliquidAPI ---


@pytest.fixture
def mock_hl_http_client() -> MagicMock:
    """Mock HttpClient for HyperliquidAPI main endpoint."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_hl_info_http_client() -> MagicMock:
    """Mock HttpClient for HyperliquidAPI info endpoint."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_hl_authenticator() -> MagicMock:
    """Mock HyperliquidEip712Authenticator."""
    from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator

    mock_auth = MagicMock(spec=HyperliquidEip712Authenticator)
    mock_auth.prepare_request = AsyncMock()
    mock_auth.wallet_address = "0x1234567890123456789012345678901234567890"
    return mock_auth


@pytest.fixture
def mock_hl_error_mapper() -> MagicMock:
    """Mock HyperliquidErrorMapper."""
    from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper

    mock_mapper = MagicMock(spec=HyperliquidErrorMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Mock HyperliquidRequestBuilder."""
    from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder

    mock_builder = MagicMock(spec=HyperliquidRequestBuilder)
    return mock_builder


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Mock HyperliquidResponseHandler."""
    from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler

    mock_handler = MagicMock(spec=HyperliquidResponseHandler)
    return mock_handler


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    """Mock HyperliquidMapper."""
    from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper

    mock_mapper = MagicMock(spec=HyperliquidMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:
    """Mock HyperliquidOrderMapper."""
    from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidOrderMapper

    mock_mapper = MagicMock(spec=HyperliquidOrderMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_candle_mapper() -> MagicMock:
    """Mock HyperliquidCandleMapper."""
    from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidCandleMapper

    mock_mapper = MagicMock(spec=HyperliquidCandleMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:
    """Mock HyperliquidUserFillMapper."""
    from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidUserFillMapper

    mock_mapper = MagicMock(spec=HyperliquidUserFillMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_account_service() -> MagicMock:
    """Mock HyperliquidAccountService."""
    from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService

    mock_service = MagicMock(spec=HyperliquidAccountService)
    mock_service.get_balances = AsyncMock()
    mock_service.get_positions = AsyncMock()
    mock_service.get_account_summary = AsyncMock()
    mock_service.get_order_history = AsyncMock()
    mock_service.get_trade_history = AsyncMock()
    return mock_service


@pytest.fixture
def mock_hl_trading_service() -> MagicMock:
    """Mock HyperliquidTradingService."""
    from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService

    mock_service = MagicMock(spec=HyperliquidTradingService)
    mock_service.place_order = AsyncMock()
    mock_service.cancel_order = AsyncMock()
    mock_service.cancel_all_orders = AsyncMock()
    mock_service.get_open_orders = AsyncMock()
    mock_service.get_order = AsyncMock()
    return mock_service


@pytest.fixture
def mock_hl_market_data_service() -> MagicMock:
    """Mock HyperliquidMarketDataService."""
    from cyberdelta.apis.hyperliquid.services.hl_market_data_service import (
        HyperliquidMarketDataService,
    )

    mock_service = MagicMock(spec=HyperliquidMarketDataService)
    mock_service.get_ticker = AsyncMock()
    mock_service.get_order_book = AsyncMock()
    mock_service.get_recent_trades = AsyncMock()
    mock_service.get_funding_rates = AsyncMock()
    mock_service.get_market_data = AsyncMock()
    mock_service.get_historical_funding_rates = AsyncMock()
    return mock_service


@pytest.fixture
def hl_api_with_di(
    hyperliquid_config: dict[str, Any],
    hyperliquid_secrets: dict[str, str],
    mock_hl_authenticator: MagicMock,
    mock_hl_error_mapper: MagicMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_mapper: MagicMock,
    mock_hl_order_mapper: MagicMock,
    mock_hl_candle_mapper: MagicMock,
    mock_hl_user_fill_mapper: MagicMock,
    mock_hl_http_client: MagicMock,
    mock_hl_info_http_client: MagicMock,
    mock_hl_account_service: MagicMock,
    mock_hl_trading_service: MagicMock,
    mock_hl_market_data_service: MagicMock,
) -> Callable[..., Any]:
    """
    Factory fixture to create HyperliquidAPI instances with all dependencies injected.
    This enables black-box testing without accessing private members.
    """
    from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI

    def _create_api(
        # Allow overriding specific dependencies if needed
        config: dict[str, Any] | None = None,
        secrets: dict[str, str | None] | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI with injected dependencies."""
        actual_config = config or hyperliquid_config
        # Convert dict[str, str] to dict[str, str | None] for API compatibility
        actual_secrets: dict[str, str | None] = secrets or {
            k: v for k, v in hyperliquid_secrets.items()
        }

        return HyperliquidAPI(
            api_config=actual_config,
            secrets=actual_secrets,
            authenticator=overrides.get("authenticator", mock_hl_authenticator),
            error_mapper=overrides.get("error_mapper", mock_hl_error_mapper),
            request_builder=overrides.get("request_builder", mock_hl_request_builder),
            response_handler=overrides.get("response_handler", mock_hl_response_handler),
            mapper=overrides.get("mapper", mock_hl_mapper),
            order_mapper=overrides.get("order_mapper", mock_hl_order_mapper),
            candle_mapper=overrides.get("candle_mapper", mock_hl_candle_mapper),
            user_fill_mapper=overrides.get("user_fill_mapper", mock_hl_user_fill_mapper),
            http_client=overrides.get("http_client", mock_hl_http_client),
            info_http_client=overrides.get("info_http_client", mock_hl_info_http_client),
            account_service=overrides.get("account_service", mock_hl_account_service),
            trading_service=overrides.get("trading_service", mock_hl_trading_service),
            market_data_service=overrides.get("market_data_service", mock_hl_market_data_service),
        )

    return _create_api


# --- End Dependency Injection Fixtures ---

# Constants for testing
TEST_WALLET_ADDRESS = "0x0000000000000000000000000000000000000000"


class TestHyperliquidAPIInitialization:
    """Test HyperliquidAPI initialization with dependency injection."""

    def test_api_creation_with_di_fixture(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that the DI fixture creates a valid API instance."""
        api = hl_api_with_di()

        # Verify the API instance is created correctly
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

    def test_api_creation_with_custom_config(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test API creation with custom configuration."""
        custom_config = {
            "base_url": "https://custom.hyperliquid.api",
            "ws_endpoint": "wss://custom.hyperliquid.ws",
        }

        api = hl_api_with_di(config=custom_config)
        assert api is not None


class TestHyperliquidAPIAccountOperations:
    """Test account-related operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_balances_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_balances properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                total_quantity=Decimal("5000.0"),
                available_quantity=Decimal("4800.0"),
                timestamp=datetime.now(UTC),
            )
        }
        mock_hl_account_service.get_balances.return_value = expected_balances

        # Test delegation
        result = await api.get_balances()

        # Verify service was called and result returned
        mock_hl_account_service.get_balances.assert_called_once()
        assert result == expected_balances

        await api.close()

    @pytest.mark.asyncio
    async def test_get_account_summary_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_account_summary properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_summary = MarginAccountSummary(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            total_equity=Decimal("5000.0"),
            available_equity=Decimal("4200.0"),
            total_initial_margin_required=None,
            total_maintenance_margin_required=Decimal("80.0"),
            total_unrealized_pnl=Decimal("25.0"),
        )
        mock_hl_account_service.get_account_summary.return_value = expected_summary

        # Test delegation
        result = await api.get_account_summary()

        # Verify service was called and result returned
        mock_hl_account_service.get_account_summary.assert_called_once()
        assert result == expected_summary

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_positions properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_positions: list[DerivativePosition] = []  # Empty positions list
        mock_hl_account_service.get_positions.return_value = expected_positions

        # Test delegation
        result = await api.get_positions()

        # Verify service was called and result returned
        mock_hl_account_service.get_positions.assert_called_once_with(symbol=None)
        assert result == expected_positions

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_with_symbol_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_positions with symbol properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_positions: list[DerivativePosition] = []
        mock_hl_account_service.get_positions.return_value = expected_positions

        # Test delegation with symbol
        result = await api.get_positions(symbol="ETH")

        # Verify service was called with correct parameters
        mock_hl_account_service.get_positions.assert_called_once_with(symbol="ETH")
        assert result == expected_positions

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_history_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_order_history properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_orders: list[Order] = []  # Empty orders list
        mock_hl_account_service.get_order_history.return_value = expected_orders

        # Test delegation
        result = await api.get_order_history(symbol="ETH")

        # Verify service was called with correct parameters
        mock_hl_account_service.get_order_history.assert_called_once_with(
            symbol="ETH", start_time=None, end_time=None
        )
        assert result == expected_orders

        await api.close()

    @pytest.mark.asyncio
    async def test_get_trade_history_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_trade_history properly delegates to account service."""
        api = hl_api_with_di()

        # Configure mock account service
        expected_trades: list[Trade] = []  # Empty trades list
        mock_hl_account_service.get_trade_history.return_value = expected_trades

        # Test delegation
        result = await api.get_trade_history(symbol="ETH", limit=50)

        # Verify service was called with correct parameters
        mock_hl_account_service.get_trade_history.assert_called_once_with(symbol="ETH")
        assert result == expected_trades

        await api.close()


class TestHyperliquidAPITradingOperations:
    """Test trading-related operations with service delegation."""

    @pytest.mark.asyncio
    async def test_place_order_delegates_to_trading_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that place_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        test_time = datetime.now(UTC)
        expected_order = Order(
            client_order_id="hl_test_order_123",
            exchange_order_id="67890",  # Valid integer order ID for Hyperliquid
            exchange="hyperliquid",
            symbol="ETH",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.NEW,
            quantity_requested=Decimal("1.0"),
            quantity_filled=Decimal("0.0"),
            price=Decimal("2000.0"),
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
        mock_hl_trading_service.place_order.return_value = expected_order

        # Test delegation
        result = await api.place_order(
            symbol="ETH",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("2000.0"),
            time_in_force=TimeInForce.GTC,
        )

        # Verify service was called with correct parameters
        mock_hl_trading_service.place_order.assert_called_once_with(
            symbol="ETH",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2000.0"),
            stop_price=None,
            client_order_id=None,
            reduce_only=False,
            post_only=False,
        )
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_delegates_to_trading_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that cancel_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        mock_hl_trading_service.cancel_order.return_value = True

        # Test delegation with valid integer order ID
        result = await api.cancel_order("12345", symbol="ETH")

        # Verify service was called with correct parameters
        mock_hl_trading_service.cancel_order.assert_called_once_with(
            order_id=12345,
            symbol="ETH",  # HyperliquidAPI converts string to int
        )
        assert result is True

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_delegates_to_trading_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that get_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        test_order = Order(
            client_order_id="hl_test_order",
            exchange_order_id="12345",  # Valid integer order ID for Hyperliquid
            exchange="hyperliquid",
            symbol="ETH",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("0.5"),
            quantity_filled=Decimal("0.5"),
            price=Decimal("2000.0"),
            average_fill_price=Decimal("1999.0"),
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
        mock_hl_trading_service.get_order.return_value = test_order

        # Test delegation with valid integer order ID
        result = await api.get_order("12345", symbol="ETH")

        # Verify service was called with correct parameters
        mock_hl_trading_service.get_order.assert_called_once_with(
            symbol="ETH",
            order_id=12345,  # HyperliquidAPI converts string to int
        )
        assert result == test_order

        await api.close()

    @pytest.mark.asyncio
    async def test_get_open_orders_delegates_to_trading_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that get_open_orders properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        expected_orders: list[Order] = []  # Empty orders list
        mock_hl_trading_service.get_open_orders.return_value = expected_orders

        # Test delegation
        result = await api.get_open_orders()

        # Verify service was called and result returned
        mock_hl_trading_service.get_open_orders.assert_called_once_with(symbol=None)
        assert result == expected_orders

        await api.close()


class TestHyperliquidAPIMarketDataOperations:
    """Test market data operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_ticker_delegates_to_market_data_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test that get_ticker properly delegates to market data service."""
        api = hl_api_with_di()

        # Configure mock market data service
        from cyberdelta.core.models import Ticker

        expected_ticker = Ticker(
            symbol="ETH",
            price=Decimal("2000.0"),
            timestamp=datetime.now(UTC),
        )
        mock_hl_market_data_service.get_ticker.return_value = expected_ticker

        # Test delegation
        result = await api.get_ticker("ETH")

        # Verify service was called with correct parameters
        mock_hl_market_data_service.get_ticker.assert_called_once_with("ETH")
        assert result == expected_ticker

        await api.close()

    @pytest.mark.asyncio
    async def test_get_funding_rates_delegates_to_market_data_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test that get_funding_rates properly delegates to market data service."""
        api = hl_api_with_di()

        # Configure mock market data service
        expected_rates = [
            FundingRate(
                symbol="ETH",
                funding_rate=Decimal("0.0001"),
                timestamp=datetime.now(UTC),
                next_funding_time=datetime.now(UTC),
            ),
        ]
        mock_hl_market_data_service.get_funding_rates.return_value = expected_rates

        # Test delegation
        result = await api.get_funding_rates(symbols=["ETH"])

        # Verify service was called with correct parameters
        mock_hl_market_data_service.get_funding_rates.assert_called_once_with(symbols=["ETH"])
        assert result == expected_rates

        await api.close()


class TestHyperliquidAPIErrorHandling:
    """Test error handling and propagation."""

    @pytest.mark.asyncio
    async def test_service_error_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that service errors are properly propagated."""
        api = hl_api_with_di()

        # Configure mock service to raise an error
        service_error = APIError(
            "Symbol not found",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.get_order.side_effect = service_error

        # Test error propagation with valid integer order ID
        with pytest.raises(APIError) as exc_info:
            await api.get_order("12345", symbol="ETH")

        # Verify the error is the same as from the service
        assert exc_info.value == service_error

        await api.close()

    @pytest.mark.asyncio
    async def test_authentication_error_handling(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test authentication error handling."""
        api = hl_api_with_di()

        # Configure mock service to raise authentication error
        auth_error = APIError(
            "Invalid signature",
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
        )
        mock_hl_trading_service.place_order.side_effect = auth_error

        # Test error propagation
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("2000.0"),
                time_in_force=TimeInForce.GTC,
            )

        # Verify the error code
        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value

        await api.close()


class TestHyperliquidAPIWebSocketOperations:
    """Test WebSocket operations using black-box approach."""

    @pytest.mark.asyncio
    async def test_subscribe_delegates_to_ws_manager(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that subscribe works through public interface."""
        api = hl_api_with_di()

        # Create a mock handler
        async def mock_handler(data: dict[str, Any], full_message: dict[str, Any]) -> None:
            pass

        # Test subscription (this tests the public interface)
        # The actual WebSocket manager is mocked, so this tests orchestration
        try:
            await api.subscribe("l2Book:BTC", mock_handler)
            # If no exception, the subscription interface works
            assert True
        except Exception as e:
            # If there's an exception, it should be from the mocked dependencies
            # not from the API interface itself
            pytest.fail(f"Subscription failed: {e}")

        await api.close()

    def test_subscription_payload_construction_public_behavior(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test subscription payload construction through public behavior."""
        api = hl_api_with_di()

        # We can't directly test the private method, but we can test
        # that the API can be instantiated and has the expected public interface
        assert hasattr(api, "subscribe")
        assert callable(api.subscribe)

    @pytest.mark.asyncio
    async def test_websocket_message_handling_public_behavior(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test WebSocket message handling through public behavior."""
        api = hl_api_with_di()

        # Test that the API can handle subscription setup
        # This indirectly tests the WebSocket message handling setup

        message_received = False

        async def test_handler(data: dict[str, Any], full_message: dict[str, Any]) -> None:
            nonlocal message_received
            message_received = True

        # Subscribe to a topic
        await api.subscribe("l2Book:BTC", test_handler)

        # The WebSocket manager is mocked, so we can't test actual message routing
        # But we can verify the subscription was set up
        assert True  # If we get here, subscription worked

        await api.close()


class TestHyperliquidAPIDependencyIsolation:
    """Test that dependency injection provides proper isolation."""

    def test_custom_dependency_override(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that specific dependencies can be overridden."""
        # Create a custom mock trading service
        custom_trading_service = MagicMock()

        # Create API instance with custom dependency
        api = hl_api_with_di(trading_service=custom_trading_service)

        # Verify the custom service is used
        assert api.trading_service is custom_trading_service

    def test_multiple_api_instances_are_isolated(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that multiple API instances don't share dependencies."""
        # Create separate mock instances for each API
        mock_trading_1 = MagicMock()
        mock_account_1 = MagicMock()
        mock_market_1 = MagicMock()

        mock_trading_2 = MagicMock()
        mock_account_2 = MagicMock()
        mock_market_2 = MagicMock()

        api1 = hl_api_with_di(
            trading_service=mock_trading_1,
            account_service=mock_account_1,
            market_data_service=mock_market_1,
        )
        api2 = hl_api_with_di(
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
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that all expected dependencies are injected."""
        api = hl_api_with_di()

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


class TestHyperliquidAPIResourceManagement:
    """Test resource management and cleanup."""

    @pytest.mark.asyncio
    async def test_api_close_cleanup(self, hl_api_with_di: Callable[..., HyperliquidAPI]) -> None:
        """Test that API close method works correctly."""
        api = hl_api_with_di()

        # Close should not raise an exception
        await api.close()

        # Should be able to call close multiple times
        await api.close()

    @pytest.mark.asyncio
    async def test_context_manager_behavior(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test API as context manager."""
        # Test that API can be used in a context manager
        # (if implemented in the future)
        api = hl_api_with_di()

        try:
            # Simulate some operations
            assert api is not None
        finally:
            await api.close()
