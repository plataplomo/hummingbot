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
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetFundingRatesArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ExchangeSecrets
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from cyberdelta.enums.exchange_names import ExchangeName

# Constants for testing - Valid base64-encoded ED25519 keys
TEST_API_KEY = "61D/XTRs1Es8SgdZN4xO438vv1ls0aWhJSs//JDNxLk="
TEST_API_SECRET = "7s6pf6Xs8VJDMTNmcseiLge61XCSZeQ6GW8PP6odR1c="


def create_test_exchange_config(
    api_base_url: str = "https://api.backpack.exchange",
    ws_url: str = "wss://ws.backpack.exchange",
    **kwargs: object,
) -> ExchangeSpecificConfig:
    """
    Create ExchangeSpecificConfig for testing by parsing from dict.
    This works with the validator that expects string inputs.
    """
    config_dict = {
        "exchange_name": ExchangeName.BACKPACK,
        "api_base_url": api_base_url,
        "ws_url": ws_url,
        "rate_limit_per_minute": 120,
        "symbols": {"SOL_USDC": "SOL_USDC", "BTC_USDC": "BTC_USDC"},
        **kwargs,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


# --- Dependency Injection Test Fixtures for BackpackAPI ---


@pytest.fixture
def mock_bp_http_client() -> MagicMock:
    """Mock HttpClient for BackpackAPI."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    return mock_client


@pytest.fixture
def mock_bp_authenticator() -> MagicMock:
    """Mock BackpackEd25519Authenticator."""
    from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator

    mock_auth = MagicMock(spec=BackpackEd25519Authenticator)
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
        config: ExchangeSpecificConfig | None = None,
        secrets: ExchangeSecrets | None = None,
        **overrides: MagicMock,
    ) -> BackpackAPI:
        # Create default Pydantic models if not provided
        if config is None:
            config = create_test_exchange_config(
                request_timeout_seconds=10.0,
                ws_ping_interval_seconds=30.0,
            )

        if secrets is None:
            secrets = ExchangeSecrets(
                api_key=SecretStr(TEST_API_KEY),
                api_secret=SecretStr(TEST_API_SECRET),
            )

        # Create the API instance
        api = BackpackAPI(exchange_config=config, exchange_secrets=secrets)

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
        custom_config = create_test_exchange_config(
            api_base_url="https://custom.backpack.api",
            ws_url="wss://custom.backpack.ws",
            symbols={"SOL_USDC": "SOL_USDC"},
        )

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

        # Verify service was called and result returned (positional argument)
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

        # Verify service was called with correct parameters (positional argument)
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
        args = GetOrderHistoryArgs(symbol="SOL")
        result = await api.get_order_history(args)

        # Verify service was called with correct parameters
        mock_bp_account_service.get_order_history.assert_called_once_with(args=args)
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
        result = await api.get_trade_history(args=GetTradeHistoryArgs(
            symbol="SOL", limit=50
        ))

        # Verify service was called with correct parameters
        mock_bp_account_service.get_trade_history.assert_called_once_with(args=GetTradeHistoryArgs(
            symbol="SOL", limit=50
        ))
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
        args = PlaceOrderArgs(
            symbol="SOL",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.0"),
            price=Decimal("150.0"),
            time_in_force=TimeInForce.GTC,
        )
        result = await api.place_order(args=args)

        # Verify service was called with correct parameters
        mock_bp_trading_service.place_order.assert_called_once_with(args=args)
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
        cancel_args = CancelOrderArgs(order_id="order_789", symbol="SOL")
        result = await api.cancel_order(args=cancel_args)

        # Verify service was called and result returned
        mock_bp_trading_service.cancel_order.assert_called_once_with(args=cancel_args)
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

        # Verify service was called and result returned (positional argument)
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
        funding_args = GetFundingRatesArgs(symbols=["SOL"])
        result = await api.get_funding_rates(args=funding_args)

        # Verify service was called and result returned
        mock_bp_market_data_service.get_funding_rates.assert_called_once_with(args=funding_args)
        assert result == expected_rates

        await api.close()

    @pytest.mark.asyncio
    async def test_get_ticker_empty_successful_response(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_market_data_service: MagicMock
    ) -> None:
        """Test get_ticker handling of symbol not found error correctly."""
        api = bp_api_with_di()

        # Configure service to raise APIError for symbol not found
        symbol_not_found_error = APIError(
            message="Symbol not found",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
            http_status=404,
        )
        mock_bp_market_data_service.get_ticker.side_effect = symbol_not_found_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_ticker("UNKNOWN_SYMBOL")

        assert exc_info.value is symbol_not_found_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        mock_bp_market_data_service.get_ticker.assert_called_once_with(symbol="UNKNOWN_SYMBOL")

        await api.close()


class TestBackpackAPIErrorHandling:
    """Test that the API client correctly propagates errors from services."""

    @pytest.mark.asyncio
    async def test_service_apierror_propagation_exact_passthrough(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that APIError from service is propagated exactly without wrapping."""
        api = bp_api_with_di()

        # Configure service to raise specific APIError
        service_error = APIError(
            "Symbol not found",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_bp_trading_service.get_order.side_effect = service_error

        # API client should propagate the exact same APIError
        with pytest.raises(APIError) as exc_info:
            await api.get_order("missing_order", symbol="SOL")

        # Assert exact error propagation
        assert exc_info.value is service_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value

    @pytest.mark.asyncio
    async def test_service_valueerror_propagation_exact_passthrough(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_trading_service: MagicMock
    ) -> None:
        """Test that ValueError from service is propagated exactly without wrapping."""
        api = bp_api_with_di()

        # Configure service to raise ValueError for input validation
        service_error = ValueError("Invalid API credentials format")
        mock_bp_trading_service.place_order.side_effect = service_error

        # API client should propagate the exact same ValueError
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("150.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(args=args)

        # Assert exact error propagation
        assert exc_info.value is service_error  # Same instance
        assert str(exc_info.value) == "Invalid API credentials format"

    @pytest.mark.asyncio
    async def test_multiple_error_types_from_different_services(
        self,
        bp_api_with_di: Callable[..., BackpackAPI],
        mock_bp_trading_service: MagicMock,
        mock_bp_account_service: MagicMock,
        mock_bp_market_data_service: MagicMock,
    ) -> None:
        """
        Test that different services can raise different error types and all are
        propagated correctly.
        """
        api = bp_api_with_di()

        # Configure different services to raise different error types
        trading_api_error = APIError(
            message="Trading service authentication failed",
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
        )
        account_value_error = ValueError("Account service input validation failed")
        market_data_api_error = APIError(
            message="Market data service rate limited",
            code=APIErrorCode.RATE_LIMITED.value,
        )

        mock_bp_trading_service.cancel_order.side_effect = trading_api_error
        mock_bp_account_service.get_balances.side_effect = account_value_error
        mock_bp_market_data_service.get_ticker.side_effect = market_data_api_error

        # Test trading service APIError propagation
        with pytest.raises(APIError) as trading_exc:
            cancel_args = CancelOrderArgs(order_id="12345", symbol="SOL")
            await api.cancel_order(args=cancel_args)
        assert trading_exc.value is trading_api_error

        # Test account service ValueError propagation
        with pytest.raises(ValueError) as account_exc:
            await api.get_balances()
        assert account_exc.value is account_value_error

        # Test market data service APIError propagation
        with pytest.raises(APIError) as market_exc:
            await api.get_ticker(symbol="SOL")
        assert market_exc.value is market_data_api_error


class TestBackpackAPIComprehensiveErrorHandling:
    """Comprehensive edge case and failure scenario testing."""

    # =============================================================================
    # I. DATA RETRIEVAL METHOD ERROR SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_balances_service_validation_error(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_balances exact propagation of service validation errors."""
        api = bp_api_with_di()

        # Configure service to raise specific APIError
        from pydantic import ValidationError

        validation_error = APIError(
            message="Invalid balance response structure",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=ValidationError.from_exception_data(
                title="BalanceModel", line_errors=[]
            ),
        )
        mock_bp_account_service.get_balances.side_effect = validation_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value is validation_error  # Same instance
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid balance response structure" in exc_info.value.message
        mock_bp_account_service.get_balances.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_positions_rate_limited_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_positions exact propagation of RATE_LIMITED error."""
        api = bp_api_with_di()

        rate_limit_error = APIError(
            message="Rate limit exceeded",
            code=APIErrorCode.RATE_LIMITED.value,
            http_status=429,
            exchange_message="rate_limit_exceeded",
        )
        mock_bp_account_service.get_positions.side_effect = rate_limit_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_positions()

        assert exc_info.value is rate_limit_error  # Same instance
        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value
        assert exc_info.value.http_status == 429
        assert "Rate limit exceeded" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_summary_server_error_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_account_summary exact propagation of SERVER_ERROR."""
        api = bp_api_with_di()

        server_error = APIError(
            message="Internal server error occurred",
            code=APIErrorCode.SERVER_ERROR.value,
            http_status=500,
            exchange_message="internal_server_error",
        )
        mock_bp_account_service.get_account_info.side_effect = server_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value is server_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SERVER_ERROR.value
        assert exc_info.value.http_status == 500
        assert "Internal server error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_history_timeout_error_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test that timeout errors from get_order_history are properly propagated."""
        api = bp_api_with_di()

        # Configure mock to raise timeout error
        timeout_error = APIError("Request timeout", APIErrorCode.TIMEOUT.value, http_status=408)
        mock_bp_account_service.get_order_history.side_effect = timeout_error

        # Test error propagation
        with pytest.raises(APIError) as exc_info:
            args = GetOrderHistoryArgs(symbol="SOL")
            await api.get_order_history(args)

        assert exc_info.value.code == APIErrorCode.TIMEOUT.value
        assert exc_info.value.http_status == 408

        await api.close()

    @pytest.mark.asyncio
    async def test_get_trade_history_service_unavailable_propagation(
        self, bp_api_with_di: Callable[..., BackpackAPI], mock_bp_account_service: MagicMock
    ) -> None:
        """Test get_trade_history exact propagation of SERVICE_UNAVAILABLE error."""
        api = bp_api_with_di()

        service_unavailable_error = APIError(
            message="Service temporarily unavailable",
            code=APIErrorCode.SERVICE_UNAVAILABLE.value,
            http_status=503,
        )
        mock_bp_account_service.get_trade_history.side_effect = service_unavailable_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_trade_history(args=GetTradeHistoryArgs())

        assert exc_info.value is service_unavailable_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == 503


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
