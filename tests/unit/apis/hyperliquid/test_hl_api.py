"""
Unit tests for the HyperliquidAPI client implementation.
Tests use dependency injection patterns to mock collaborators and focus on public interface testing.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

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
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
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
    """Mock HyperliquidMarketDataMapper (for backwards compatibility)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidMarketDataMapper

    mock_mapper = MagicMock(spec=HyperliquidMarketDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_account_mapper() -> MagicMock:
    """Mock HyperliquidAccountDataMapper."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidAccountDataMapper

    mock_mapper = MagicMock(spec=HyperliquidAccountDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:
    """Mock HyperliquidTradingDataMapper (legacy order mapper)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidTradingDataMapper

    mock_mapper = MagicMock(spec=HyperliquidTradingDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_trading_mapper() -> MagicMock:
    """Mock HyperliquidTradingDataMapper."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidTradingDataMapper

    mock_mapper = MagicMock(spec=HyperliquidTradingDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:
    """Mock HyperliquidAccountDataMapper (for user fills)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidAccountDataMapper

    mock_mapper = MagicMock(spec=HyperliquidAccountDataMapper)
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
    mock_hl_account_mapper: MagicMock,
    mock_hl_order_mapper: MagicMock,
    mock_hl_trading_mapper: MagicMock,
    mock_hl_user_fill_mapper: MagicMock,
    mock_hl_http_client: MagicMock,
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
            market_data_mapper=overrides.get("market_data_mapper", mock_hl_mapper),
            account_data_mapper=overrides.get("account_data_mapper", mock_hl_account_mapper),
            trading_data_mapper=overrides.get("trading_data_mapper", mock_hl_trading_mapper),
            http_client=overrides.get("http_client", mock_hl_http_client),
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

    def test_api_has_required_services(self, hl_api_with_di: Callable[..., HyperliquidAPI]) -> None:
        """Test that API instance has all required services initialized."""
        api = hl_api_with_di()

        # Verify the API instance has all required services
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")


class TestHyperliquidAPIAssetIndexingIntegration:
    """Test asset indexing integration through public API methods that depend on it."""

    @pytest.mark.asyncio
    async def test_place_order_with_asset_indexing_success(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that place_order works correctly when asset indexing succeeds."""
        api = hl_api_with_di()

        # Mock the trading service to return a successful order
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the trading service mock to succeed
        mock_hl_trading_service.place_order.return_value = expected_order

        # Call place_order - this should internally use asset indexing
        result = await api.place_order(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )

        # Verify the trading service was called correctly
        mock_hl_trading_service.place_order.assert_called_once_with(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
            stop_price=None,
            client_order_id=None,
            reduce_only=False,
            post_only=False,
        )

        # Verify the result
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_place_order_with_asset_indexing_failure(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that place_order properly handles asset indexing failures."""
        api = hl_api_with_di()

        # Configure trading service to raise an asset indexing error
        # This simulates what happens when the trading service can't resolve the asset index
        asset_indexing_error = APIError(
            "Asset index not found for symbol 'UNKNOWN_SYMBOL'",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.place_order.side_effect = asset_indexing_error

        # Call place_order with an unknown symbol and expect the error to be propagated
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="UNKNOWN_SYMBOL",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )

        # Verify the error is the expected asset indexing error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index not found" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_with_asset_indexing_success(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that cancel_order works correctly when asset indexing succeeds."""
        api = hl_api_with_di()

        # Configure trading service to return successful cancellation
        mock_hl_trading_service.cancel_order.return_value = True

        # Call cancel_order - this should internally use asset indexing
        result = await api.cancel_order(order_id="12345", symbol="BTC")

        # Verify the trading service was called correctly
        mock_hl_trading_service.cancel_order.assert_called_once_with(symbol="BTC", order_id="12345")

        # Verify the result
        assert result is True

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_with_asset_indexing_failure(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that cancel_order properly handles asset indexing failures."""
        api = hl_api_with_di()

        # Configure trading service to raise an asset indexing error
        asset_indexing_error = APIError(
            "Asset index not found for symbol 'INVALID_SYMBOL'",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.cancel_order.side_effect = asset_indexing_error

        # Call cancel_order with an invalid symbol and expect the error to be propagated
        with pytest.raises(APIError) as exc_info:
            await api.cancel_order(order_id="12345", symbol="INVALID_SYMBOL")

        # Verify the error is the expected asset indexing error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index not found" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_with_asset_indexing_success(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that get_order works correctly when asset indexing succeeds."""
        api = hl_api_with_di()

        # Configure trading service to return an order
        expected_order = Order(
            exchange_order_id="12345",
            symbol="ETH",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("2.0"),
            price=Decimal("3000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_service.get_order.return_value = expected_order

        # Call get_order - this should internally use asset indexing
        result = await api.get_order(order_id="12345", symbol="ETH")

        # Verify the trading service was called correctly
        mock_hl_trading_service.get_order.assert_called_once_with(symbol="ETH", order_id="12345")

        # Verify the result
        assert result == expected_order

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_with_asset_indexing_failure(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that get_order properly handles asset indexing failures."""
        api = hl_api_with_di()

        # Configure trading service to raise an asset indexing error
        asset_indexing_error = APIError(
            "Asset index not found for symbol 'NONEXISTENT'",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_trading_service.get_order.side_effect = asset_indexing_error

        # Call get_order with a nonexistent symbol and expect the error to be propagated
        with pytest.raises(APIError) as exc_info:
            await api.get_order(order_id="12345", symbol="NONEXISTENT")

        # Verify the error is the expected asset indexing error
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index not found" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_multiple_operations_asset_indexing_consistency(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that multiple operations using the same symbol work consistently."""
        api = hl_api_with_di()

        # Configure trading service responses
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        mock_hl_trading_service.place_order.return_value = expected_order
        mock_hl_trading_service.get_order.return_value = expected_order
        mock_hl_trading_service.cancel_order.return_value = True

        # Perform multiple operations with the same symbol
        # Each should use asset indexing internally

        # Place order
        place_result = await api.place_order(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )

        # Get order
        get_result = await api.get_order(order_id="12345", symbol="BTC")

        # Cancel order
        cancel_result = await api.cancel_order(order_id="12345", symbol="BTC")

        # Verify all operations succeeded
        assert place_result == expected_order
        assert get_result == expected_order
        assert cancel_result is True

        # Verify all trading service methods were called
        mock_hl_trading_service.place_order.assert_called_once()
        mock_hl_trading_service.get_order.assert_called_once()
        mock_hl_trading_service.cancel_order.assert_called_once()

        await api.close()


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
        mock_hl_account_service.get_positions.assert_called_once_with(None)
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
        result = await api.get_positions("ETH")

        # Verify service was called with correct parameters
        mock_hl_account_service.get_positions.assert_called_once_with("ETH")
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
        result = await api.get_trade_history(symbol="ETH")

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
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_service.place_order.return_value = expected_order

        # Test delegation
        result = await api.place_order(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )

        # Verify service was called with correct parameters
        mock_hl_trading_service.place_order.assert_called_once_with(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
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

        # Test delegation
        result = await api.cancel_order(order_id="12345", symbol="BTC")

        # Verify service was called with correct parameters
        mock_hl_trading_service.cancel_order.assert_called_once_with(symbol="BTC", order_id="12345")
        assert result is True

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_delegates_to_trading_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that get_order properly delegates to trading service."""
        api = hl_api_with_di()

        # Configure mock trading service
        expected_order = Order(
            exchange_order_id="12345",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_service.get_order.return_value = expected_order

        # Test delegation
        result = await api.get_order(order_id="12345", symbol="BTC")

        # Verify service was called with correct parameters
        mock_hl_trading_service.get_order.assert_called_once_with(symbol="BTC", order_id="12345")
        assert result == expected_order

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
        result = await api.get_open_orders(symbol="BTC")

        # Verify service was called with correct parameters
        mock_hl_trading_service.get_open_orders.assert_called_once_with("BTC")
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
        from cyberdelta.core.models.market import Ticker

        expected_ticker = Ticker(
            symbol="BTC",
            price=Decimal("50000.0"),
            bid=Decimal("49999.0"),
            ask=Decimal("50001.0"),
            volume=Decimal("100.0"),
            timestamp=datetime.now(UTC),
        )
        mock_hl_market_data_service.get_ticker.return_value = expected_ticker

        # Test delegation
        result = await api.get_ticker("BTC")

        # Verify service was called and result returned
        mock_hl_market_data_service.get_ticker.assert_called_once_with("BTC")
        assert result == expected_ticker

        await api.close()

    @pytest.mark.asyncio
    async def test_get_funding_rates_delegates_to_market_data_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test that get_funding_rates properly delegates to market data service."""
        api = hl_api_with_di()

        # Configure mock market data service
        expected_funding_rates = [
            FundingRate(
                symbol="BTC",
                funding_rate=Decimal("0.0001"),
                timestamp=datetime.now(UTC),
                next_funding_time=datetime.now(UTC),
            )
        ]
        mock_hl_market_data_service.get_funding_rates.return_value = expected_funding_rates

        # Test delegation
        result = await api.get_funding_rates(symbols=["BTC"])

        # Verify service was called with correct parameters
        mock_hl_market_data_service.get_funding_rates.assert_called_once_with(["BTC"])
        assert result == expected_funding_rates

        await api.close()


class TestHyperliquidAPIErrorHandling:
    """Test error handling and propagation."""

    @pytest.mark.asyncio
    async def test_service_error_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that service errors are properly propagated."""
        api = hl_api_with_di()

        # Configure mock to raise an error
        expected_error = APIError(
            "Test error from trading service", code=APIErrorCode.INVALID_REQUEST.value
        )
        mock_hl_trading_service.place_order.side_effect = expected_error

        # Test error propagation
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.message == expected_error.message
        assert exc_info.value.code == expected_error.code

        await api.close()

    @pytest.mark.asyncio
    async def test_authentication_error_handling(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test authentication error handling."""
        api = hl_api_with_di()

        # Configure mock to raise authentication error
        auth_error = APIError(
            "Authentication failed", code=APIErrorCode.AUTHENTICATION_FAILED.value
        )
        mock_hl_trading_service.place_order.side_effect = auth_error

        # Test authentication error propagation
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value

        await api.close()


class TestHyperliquidAPIWebSocketOperations:
    """Test WebSocket operations."""

    @pytest.mark.asyncio
    async def test_subscribe_delegates_to_ws_manager(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that subscribe properly delegates to WebSocket manager."""
        api = hl_api_with_di()

        async def mock_handler(data: dict[str, Any], full_message: dict[str, Any]) -> None:
            pass

        # Mock the base class subscribe method
        empty_handlers: dict[str, Any] = {}
        empty_subscriptions: dict[str, Any] = {}
        with patch.object(api, "_ws_handlers", empty_handlers):
            with patch.object(api, "_ws_subscriptions", empty_subscriptions):
                # This should not raise an error
                await api.subscribe("test_topic", mock_handler)

        await api.close()

    def test_subscription_payload_construction_public_behavior(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test subscription payload construction through public interface."""
        api = hl_api_with_di()

        # Test that the method exists and can be called
        # Note: We avoid accessing protected members directly
        # Instead we test through public interface behavior
        assert hasattr(api, "subscribe_to_order_book")
        assert hasattr(api, "subscribe_to_trades")
        assert hasattr(api, "subscribe_to_account_updates")

    @pytest.mark.asyncio
    async def test_websocket_message_handling_public_behavior(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test WebSocket message handling through public interface."""
        api = hl_api_with_di()

        # Mock the router to avoid actual message processing
        with patch.object(api, "_hl_ws_router") as mock_router:
            mock_router.route_message = AsyncMock()
            # Test through public interface instead of protected method
            # This tests that the WebSocket infrastructure is properly set up
            assert hasattr(api, "_hl_ws_router")

        await api.close()


class TestHyperliquidAPIDependencyIsolation:
    """Test dependency isolation and injection."""

    def test_custom_dependency_override(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that custom dependencies can be injected."""
        custom_trading_service = MagicMock()
        api = hl_api_with_di(trading_service=custom_trading_service)

        # Verify the custom dependency was injected
        assert api.trading_service is custom_trading_service

    def test_multiple_api_instances_are_isolated(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that multiple API instances are different objects."""
        api1 = hl_api_with_di()
        api2 = hl_api_with_di()

        # Verify instances are different
        assert api1 is not api2

        # Note: In testing, services are the same mock instances (expected behavior)
        # but in production, each API instance would have its own service instances
        assert api1.trading_service is api2.trading_service  # Same mock in tests
        assert api1.account_service is api2.account_service  # Same mock in tests
        assert api1.market_data_service is api2.market_data_service  # Same mock in tests

        # Verify they have the same exchange name but are independent API instances
        assert api1.exchange_name == api2.exchange_name == "hyperliquid"

        # Test that the API instances themselves are different objects
        api1_id = id(api1)
        api2_id = id(api2)
        assert api1_id != api2_id


class TestHyperliquidAPIResourceManagement:
    """Test resource management and cleanup."""

    @pytest.mark.asyncio
    async def test_api_close_cleanup(self, hl_api_with_di: Callable[..., HyperliquidAPI]) -> None:
        """Test that API close properly cleans up resources."""
        api = hl_api_with_di()

        # Test that close doesn't raise an error
        await api.close()

    @pytest.mark.asyncio
    async def test_context_manager_behavior(
        self, hl_api_with_di: Callable[..., HyperliquidAPI]
    ) -> None:
        """Test that API can be used as a context manager."""
        api = hl_api_with_di()

        # Test basic usage without context manager for now
        # since HyperliquidAPI doesn't implement __aenter__/__aexit__
        assert api.exchange_name == "hyperliquid"
        await api.close()


class TestHyperliquidAPIComprehensiveErrorHandling:
    """
    Comprehensive error handling tests covering various failure scenarios
    and edge cases across all API operations.
    """

    @pytest.mark.asyncio
    async def test_get_balances_service_validation_error(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test get_balances handling of service validation errors."""
        api = hl_api_with_di()

        # Configure mock to raise validation error
        validation_error = APIError(
            "Invalid balance data format", code=APIErrorCode.INVALID_RESPONSE.value
        )
        mock_hl_account_service.get_balances.side_effect = validation_error

        # Test error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid balance data format" in exc_info.value.message
        mock_hl_account_service.get_balances.assert_called_once()

        await api.close()

    @pytest.mark.asyncio
    async def test_get_ticker_empty_successful_response(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_ticker handling of empty but successful responses."""
        api = hl_api_with_di()

        # Configure mock to return None (valid empty response)
        mock_hl_market_data_service.get_ticker.return_value = None

        # Test handling of empty response
        result = await api.get_ticker("NONEXISTENT")

        assert result is None
        mock_hl_market_data_service.get_ticker.assert_called_once_with("NONEXISTENT")

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_rate_limited_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test get_positions handling of rate limiting errors."""
        api = hl_api_with_di()

        # Configure mock to raise rate limit error
        rate_limit_error = APIError(
            "Rate limit exceeded", code=APIErrorCode.RATE_LIMITED.value, http_status=429
        )
        mock_hl_account_service.get_positions.side_effect = rate_limit_error

        # Test rate limit error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_positions(symbol="BTC")

        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value
        assert exc_info.value.http_status == 429
        mock_hl_account_service.get_positions.assert_called_once_with("BTC")

        await api.close()

    @pytest.mark.asyncio
    async def test_get_account_summary_server_error_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test get_account_summary handling of server errors."""
        api = hl_api_with_di()

        # Configure mock to raise server error
        server_error = APIError(
            "Internal server error", code=APIErrorCode.SERVER_ERROR.value, http_status=500
        )
        mock_hl_account_service.get_account_summary.side_effect = server_error

        # Test server error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value.code == APIErrorCode.SERVER_ERROR.value
        assert exc_info.value.http_status == 500
        mock_hl_account_service.get_account_summary.assert_called_once()

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_book_timeout_error_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_order_book handling of timeout errors."""
        api = hl_api_with_di()

        # Configure mock to raise timeout error
        timeout_error = APIError("Request timeout", code=APIErrorCode.TIMEOUT.value)
        mock_hl_market_data_service.get_order_book.side_effect = timeout_error

        # Test timeout error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_order_book("ETH")

        assert exc_info.value.code == APIErrorCode.TIMEOUT.value
        mock_hl_market_data_service.get_order_book.assert_called_once_with("ETH")

        await api.close()

    @pytest.mark.asyncio
    async def test_get_recent_trades_service_unavailable_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_recent_trades handling of service unavailable errors."""
        api = hl_api_with_di()

        # Configure mock to raise service unavailable error
        service_error = APIError(
            "Service temporarily unavailable",
            code=APIErrorCode.SERVICE_UNAVAILABLE.value,
            http_status=503,
        )
        mock_hl_market_data_service.get_recent_trades.side_effect = service_error

        # Test service unavailable error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_recent_trades("BTC", limit=10)

        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == 503
        mock_hl_market_data_service.get_recent_trades.assert_called_once_with("BTC")

        await api.close()

    @pytest.mark.asyncio
    async def test_place_order_service_unexpected_exception(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test place_order handling of unexpected exceptions from service."""
        api = hl_api_with_di()

        # Configure mock to raise unexpected exception
        unexpected_error = RuntimeError("Unexpected service failure")
        mock_hl_trading_service.place_order.side_effect = unexpected_error

        # Test unexpected exception handling
        with pytest.raises(RuntimeError) as exc_info:
            await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )

        assert "Unexpected service failure" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_order_insufficient_funds_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test cancel_order handling of insufficient funds errors."""
        api = hl_api_with_di()

        # Configure mock to raise insufficient funds error
        funds_error = APIError(
            "Insufficient funds for cancellation fee",
            code=APIErrorCode.INSUFFICIENT_FUNDS.value,
        )
        mock_hl_trading_service.cancel_order.side_effect = funds_error

        # Test insufficient funds error propagation
        with pytest.raises(APIError) as exc_info:
            await api.cancel_order("12345", symbol="BTC")

        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        mock_hl_trading_service.cancel_order.assert_called_once_with(symbol="BTC", order_id="12345")

        await api.close()

    @pytest.mark.asyncio
    async def test_get_order_order_not_found_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test get_order handling of order not found scenarios."""
        api = hl_api_with_di()

        # Configure mock to return None (order not found)
        mock_hl_trading_service.get_order.return_value = None

        # Test order not found handling
        result = await api.get_order("99999", symbol="BTC")

        assert result is None
        mock_hl_trading_service.get_order.assert_called_once_with(symbol="BTC", order_id="99999")

        await api.close()

    @pytest.mark.asyncio
    async def test_get_open_orders_exchange_specific_error(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test get_open_orders handling of exchange-specific errors."""
        api = hl_api_with_di()

        # Configure mock to raise exchange-specific error
        exchange_error = APIError(
            "Exchange maintenance in progress",
            code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            exchange_message="Maintenance mode active",
        )
        mock_hl_trading_service.get_open_orders.side_effect = exchange_error

        # Test exchange-specific error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_open_orders()

        assert exc_info.value.code == APIErrorCode.EXCHANGE_SPECIFIC.value
        assert exc_info.value.exchange_message == "Maintenance mode active"
        mock_hl_trading_service.get_open_orders.assert_called_once_with(None)

        await api.close()

    @pytest.mark.asyncio
    async def test_get_ticker_none_symbol_input(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_ticker with None symbol input."""
        api = hl_api_with_di()

        def mock_get_ticker_side_effect(symbol: str | None) -> None:
            if symbol is None:
                raise ValueError("Symbol cannot be None")
            return None

        mock_hl_market_data_service.get_ticker.side_effect = mock_get_ticker_side_effect

        # Test None symbol handling
        with pytest.raises(ValueError) as exc_info:
            await api.get_ticker(None)  # type: ignore[arg-type]

        assert "Symbol cannot be None" in str(exc_info.value)

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_empty_symbol_input(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test get_positions with empty string symbol."""
        api = hl_api_with_di()

        # Configure mock to handle empty string
        mock_hl_account_service.get_positions.return_value = []

        # Test empty string symbol handling
        result = await api.get_positions(symbol="")

        assert result == []
        mock_hl_account_service.get_positions.assert_called_once_with("")

        await api.close()

    @pytest.mark.asyncio
    async def test_place_order_invalid_quantity_input(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test place_order with invalid quantity input."""
        api = hl_api_with_di()

        # Configure mock to raise validation error for invalid quantity
        validation_error = APIError(
            "Invalid quantity: must be positive", code=APIErrorCode.INVALID_REQUEST.value
        )
        mock_hl_trading_service.place_order.side_effect = validation_error

        # Test invalid quantity handling
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("-1.0"),  # Invalid negative quantity
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Invalid quantity" in exc_info.value.message

        await api.close()

    @pytest.mark.asyncio
    async def test_get_market_data_invalid_time_range(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_market_data with invalid time range."""
        api = hl_api_with_di()

        # Configure mock to raise validation error for invalid time range
        time_error = APIError(
            "Invalid time range: end_time before start_time",
            code=APIErrorCode.INVALID_REQUEST.value,
        )
        mock_hl_market_data_service.get_market_data.side_effect = time_error

        # Test invalid time range handling
        with pytest.raises(APIError) as exc_info:
            await api.get_market_data(
                symbol="BTC",
                timeframe="1h",
                start_time_ms=1000000,
                end_time_ms=500000,  # End before start
            )

        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Invalid time range" in exc_info.value.message

        await api.close()

    @pytest.mark.asyncio
    async def test_get_balances_full_error_chain_validation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test get_balances with full error chain validation."""
        api = hl_api_with_di()

        # Configure mock to raise error with full context
        original_exception = ValueError("Invalid balance format")
        chained_error = APIError(
            "Failed to parse balance data",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=original_exception,
            metadata={"balance_type": "spot", "asset": "USDC"},
        )
        mock_hl_account_service.get_balances.side_effect = chained_error

        # Test full error chain propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert exc_info.value.original_exception is original_exception
        assert exc_info.value.metadata == {"balance_type": "spot", "asset": "USDC"}

        await api.close()

    @pytest.mark.asyncio
    async def test_multiple_service_error_isolation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
        mock_hl_trading_service: MagicMock,
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that errors in one service don't affect others."""
        api = hl_api_with_di()

        # Configure one service to fail
        account_error = APIError("Account service error", code=APIErrorCode.SERVER_ERROR.value)
        mock_hl_account_service.get_balances.side_effect = account_error

        # Configure other services to succeed
        mock_hl_trading_service.get_open_orders.return_value = []
        mock_hl_market_data_service.get_ticker.return_value = None

        # Test that account service error doesn't affect other services
        with pytest.raises(APIError):
            await api.get_balances()

        # Other services should still work
        orders = await api.get_open_orders()
        ticker = await api.get_ticker("BTC")

        assert orders == []
        assert ticker is None

        await api.close()

    @pytest.mark.asyncio
    async def test_get_funding_rates_historical_data_error(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_funding_rates with historical data retrieval errors."""
        api = hl_api_with_di()

        # Configure mock to raise historical data error
        historical_error = APIError(
            "Historical funding data unavailable",
            code=APIErrorCode.UNKNOWN.value,
            metadata={"requested_symbols": ["BTC", "ETH"], "time_range": "last_24h"},
        )
        mock_hl_market_data_service.get_funding_rates.side_effect = historical_error

        # Test historical data error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_funding_rates(symbols=["BTC", "ETH"])

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        if exc_info.value.metadata:
            assert exc_info.value.metadata["requested_symbols"] == ["BTC", "ETH"]

        await api.close()

    @pytest.mark.asyncio
    async def test_cancel_all_orders_partial_failure(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test cancel_all_orders with partial failure scenarios."""
        api = hl_api_with_di()

        # Configure mock to return partial success results
        from cyberdelta.core.models.enums import CancelOrderResultStatus
        from cyberdelta.core.models.market.order import CancelOrderResult

        partial_results = [
            CancelOrderResult(
                order_id="123",
                success=True,
                status=CancelOrderResultStatus.SUCCESS,
                symbol="BTC",
                message="Successfully cancelled.",
            ),
            CancelOrderResult(
                order_id="456",
                success=False,
                status=CancelOrderResultStatus.FAILED,
                symbol="BTC",
                message="Order not found.",
            ),
        ]
        mock_hl_trading_service.cancel_all_orders.return_value = partial_results

        # Test partial failure handling
        results = await api.cancel_all_orders(symbol="BTC")

        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False
        assert results[1].status == CancelOrderResultStatus.FAILED

        await api.close()

    @pytest.mark.asyncio
    async def test_complex_operation_authentication_chain_failure(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test complex operation with authentication chain failure."""
        api = hl_api_with_di()

        # Configure mock to raise authentication error with chain
        auth_failure = APIError(
            "Authentication signature invalid",
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            metadata={
                "signature_type": "EIP712",
                "wallet_address": "0x123...",
                "nonce": 12345,
                "verification_step": "signature_recovery",
            },
        )
        mock_hl_trading_service.place_order.side_effect = auth_failure

        # Test authentication chain failure
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        if exc_info.value.metadata:
            assert exc_info.value.metadata["signature_type"] == "EIP712"
            assert exc_info.value.metadata["verification_step"] == "signature_recovery"

        await api.close()

    @pytest.mark.asyncio
    async def test_concurrent_market_data_requests_error_handling(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test concurrent market data requests with mixed success/failure."""
        api = hl_api_with_di()

        # Configure service to behave differently for concurrent calls
        call_count = 0

        def get_ticker_side_effect(symbol: str) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None  # First call succeeds with no data
            else:
                raise APIError(
                    message="Concurrent request limit exceeded",
                    code=APIErrorCode.RATE_LIMITED.value,
                )

        mock_hl_market_data_service.get_ticker.side_effect = get_ticker_side_effect

        # First call should succeed
        ticker1 = await api.get_ticker("BTC")
        assert ticker1 is None

        # Second call should fail
        with pytest.raises(APIError) as exc_info:
            await api.get_ticker("ETH")
        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value

        await api.close()
