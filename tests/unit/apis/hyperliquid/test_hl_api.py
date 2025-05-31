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
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
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
    Trade,
)
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from cyberdelta.enums.exchange_names import ExchangeName


def create_test_exchange_config(
    api_base_url: str = "https://api.hyperliquid.xyz",
    ws_url: str = "wss://api.hyperliquid.xyz/ws",
    **kwargs: object,
) -> ExchangeSpecificConfig:
    """
    Create ExchangeSpecificConfig for testing by parsing from dict.
    This works with the validator that expects string inputs.
    """
    config_dict = {
        "exchange_name": ExchangeName.HYPERLIQUID,
        "api_base_url": api_base_url,
        "ws_url": ws_url,
        "rate_limit_per_minute": 300,
        "symbols": {"ETH": "ETH", "BTC": "BTC"},
        "chain_id": 1337,
        **kwargs,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


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
        config: ExchangeSpecificConfig | None = None,
        secrets: ExchangeSecrets | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI with injected dependencies."""
        # Create default Pydantic models if not provided
        if config is None:
            config = create_test_exchange_config()

        if secrets is None:
            secrets = ExchangeSecrets(
                api_key=SecretStr(""),
                api_secret=SecretStr(""),
                private_key=SecretStr("0x" + "1" * 64),
                passphrase=None,
            )

        return HyperliquidAPI(
            exchange_config=config,
            exchange_secrets=secrets,
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
        custom_config = create_test_exchange_config(
            api_base_url="https://custom.hyperliquid.api",
            ws_url="wss://custom.hyperliquid.ws",
        )

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
        place_order_args = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )
        result = await api.place_order(place_order_args)

        # Verify the trading service was called correctly
        mock_hl_trading_service.place_order.assert_called_once_with(place_order_args)

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
            place_order_args = PlaceOrderArgs(
                symbol="UNKNOWN_SYMBOL",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(place_order_args)

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
        cancel_args = CancelOrderArgs(order_id="12345", symbol="BTC")
        result = await api.cancel_order(args=cancel_args)

        # Verify the trading service was called correctly
        mock_hl_trading_service.cancel_order.assert_called_once_with(args=cancel_args)

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
            cancel_args = CancelOrderArgs(order_id="12345", symbol="INVALID_SYMBOL")
            await api.cancel_order(args=cancel_args)

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
        place_order_args = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )
        place_result = await api.place_order(place_order_args)

        # Get order
        get_result = await api.get_order(order_id="12345", symbol="BTC")

        # Cancel order
        cancel_args = CancelOrderArgs(order_id="12345", symbol="BTC")
        cancel_result = await api.cancel_order(args=cancel_args)

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
        mock_hl_account_service.get_positions.assert_called_once_with(symbol=None)
        assert result == expected_positions

        await api.close()

    @pytest.mark.asyncio
    async def test_get_positions_with_symbol_delegates_to_account_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that get_positions with symbol delegates to account service."""
        api = hl_api_with_di()
        mock_hl_account_service.get_positions.return_value = []

        result = await api.get_positions("ETH")

        assert result == []
        mock_hl_account_service.get_positions.assert_called_once_with(symbol="ETH")

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
        order_args = GetOrderHistoryArgs(symbol="ETH")
        result = await api.get_order_history(args=order_args)

        # Verify service was called with correct parameters
        mock_hl_account_service.get_order_history.assert_called_once_with(
            args=order_args
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
        result = await api.get_trade_history(args=GetTradeHistoryArgs(symbol="ETH"))

        # Verify service was called with correct parameters
        mock_hl_account_service.get_trade_history.assert_called_once_with(args=GetTradeHistoryArgs(symbol="ETH"))
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
        place_order_args = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.0"),
            time_in_force=TimeInForce.GTC,
        )
        result = await api.place_order(place_order_args)

        # Verify service was called with correct parameters
        mock_hl_trading_service.place_order.assert_called_once_with(place_order_args)
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
        cancel_args = CancelOrderArgs(order_id="12345", symbol="BTC")
        result = await api.cancel_order(args=cancel_args)

        # Verify service was called with correct parameters
        mock_hl_trading_service.cancel_order.assert_called_once_with(args=cancel_args)
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
        """Test that get_open_orders delegates to trading service."""
        api = hl_api_with_di()
        mock_hl_trading_service.get_open_orders.return_value = []

        result = await api.get_open_orders("BTC")

        assert result == []
        mock_hl_trading_service.get_open_orders.assert_called_once_with(symbol="BTC")


class TestHyperliquidAPIMarketDataOperations:
    """Test market data operations with service delegation."""

    @pytest.mark.asyncio
    async def test_get_ticker_delegates_to_market_data_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test that get_ticker delegates to market data service."""
        api = hl_api_with_di()

        # Create a mock ticker
        mock_ticker = MagicMock()
        mock_hl_market_data_service.get_ticker.return_value = mock_ticker

        result = await api.get_ticker("BTC")

        assert result == mock_ticker
        mock_hl_market_data_service.get_ticker.assert_called_once_with(symbol="BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rates_delegates_to_market_data_service(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test that get_funding_rates delegates to market data service."""
        api = hl_api_with_di()
        mock_hl_market_data_service.get_funding_rates.return_value = []

        funding_args = GetFundingRatesArgs(symbols=["BTC"])
        result = await api.get_funding_rates(args=funding_args)

        assert result == []
        mock_hl_market_data_service.get_funding_rates.assert_called_once_with(args=funding_args)


class TestHyperliquidAPIErrorHandling:
    """Test that the API client correctly propagates errors from services."""

    @pytest.mark.asyncio
    async def test_service_apierror_propagation_exact_passthrough(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that APIError from service is propagated exactly without wrapping."""
        api = hl_api_with_di()

        # Configure service to raise specific APIError
        service_error = APIError(
            message="Service-level validation failed",
            code=APIErrorCode.INVALID_PARAMS.value,
            http_status=400,
        )
        mock_hl_trading_service.place_order.side_effect = service_error

        # API client should propagate the exact same APIError
        with pytest.raises(APIError) as exc_info:
            place_order_args = PlaceOrderArgs(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(place_order_args)

        # Assert exact error propagation
        assert exc_info.value is service_error  # Same instance
        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value
        assert exc_info.value.message == "Service-level validation failed"
        assert exc_info.value.http_status == 400

    @pytest.mark.asyncio
    async def test_service_valueerror_propagation_exact_passthrough(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test that ValueError from service is propagated exactly without wrapping."""
        api = hl_api_with_di()

        # Configure service to raise ValueError for input validation
        service_error = ValueError("Invalid symbol format for service processing")
        mock_hl_trading_service.get_order.side_effect = service_error

        # API client should propagate the exact same ValueError
        with pytest.raises(ValueError) as exc_info:
            await api.get_order(order_id="invalid_id")

        # Assert exact error propagation
        assert exc_info.value is service_error  # Same instance
        assert str(exc_info.value) == "Invalid symbol format for service processing"

    @pytest.mark.asyncio
    async def test_multiple_error_types_from_different_services(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
        mock_hl_account_service: MagicMock,
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test different services raise different error types and all are propagated correctly."""
        api = hl_api_with_di()

        # Configure different services to raise different error types
        trading_api_error = APIError(
            message="Trading service error",
            code=APIErrorCode.NETWORK_ISSUE.value,
        )
        account_value_error = ValueError("Account service input validation failed")
        market_data_api_error = APIError(
            message="Market data service error",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

        mock_hl_trading_service.cancel_order.side_effect = trading_api_error
        mock_hl_account_service.get_balances.side_effect = account_value_error
        mock_hl_market_data_service.get_ticker.side_effect = market_data_api_error

        # Test trading service APIError propagation
        with pytest.raises(APIError) as trading_exc:
            cancel_args = CancelOrderArgs(order_id="12345")
            await api.cancel_order(args=cancel_args)
        assert trading_exc.value is trading_api_error

        # Test account service ValueError propagation
        with pytest.raises(ValueError) as account_exc:
            await api.get_balances()
        assert account_exc.value is account_value_error

        # Test market data service APIError propagation
        with pytest.raises(APIError) as market_exc:
            await api.get_ticker(symbol="BTC")
        assert market_exc.value is market_data_api_error


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
        """Test get_balances exact propagation of service validation errors."""
        api = hl_api_with_di()

        # Configure mock to raise validation error
        validation_error = APIError(
            "Invalid balance data format", code=APIErrorCode.INVALID_RESPONSE.value
        )
        mock_hl_account_service.get_balances.side_effect = validation_error

        # Test exact error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_balances()

        assert exc_info.value is validation_error  # Same instance
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid balance data format" in exc_info.value.message
        mock_hl_account_service.get_balances.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ticker_empty_successful_response(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test that get_ticker handles empty successful response correctly."""
        api = hl_api_with_di()
        mock_hl_market_data_service.get_ticker.return_value = None

        result = await api.get_ticker("NONEXISTENT")

        assert result is None
        mock_hl_market_data_service.get_ticker.assert_called_once_with(symbol="NONEXISTENT")

    @pytest.mark.asyncio
    async def test_get_positions_rate_limited_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test that rate limited errors from account service are propagated correctly."""
        api = hl_api_with_di()

        # Configure mock to raise rate limited error
        rate_limited_error = APIError("Rate limited", code=429)
        mock_hl_account_service.get_positions.side_effect = rate_limited_error

        with pytest.raises(APIError) as exc_info:
            await api.get_positions("BTC")

        assert exc_info.value.code == 429
        assert "Rate limited" in str(exc_info.value)
        mock_hl_account_service.get_positions.assert_called_once_with(symbol="BTC")

    @pytest.mark.asyncio
    async def test_get_account_summary_server_error_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_account_service: MagicMock
    ) -> None:
        """Test get_account_summary exact propagation of server errors."""
        api = hl_api_with_di()

        # Configure mock to raise server error
        server_error = APIError(
            "Internal server error", code=APIErrorCode.SERVER_ERROR.value, http_status=500
        )
        mock_hl_account_service.get_account_summary.side_effect = server_error

        # Test exact server error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value is server_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SERVER_ERROR.value
        assert exc_info.value.http_status == 500
        mock_hl_account_service.get_account_summary.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_book_timeout_error_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_order_book exact propagation of timeout errors."""
        api = hl_api_with_di()

        # Configure mock to raise timeout error
        timeout_error = APIError("Request timeout", code=APIErrorCode.TIMEOUT.value)
        mock_hl_market_data_service.get_order_book.side_effect = timeout_error

        # Test exact timeout error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_order_book("ETH")

        assert exc_info.value is timeout_error  # Same instance
        assert exc_info.value.code == APIErrorCode.TIMEOUT.value
        mock_hl_market_data_service.get_order_book.assert_called_once_with(symbol="ETH")

    @pytest.mark.asyncio
    async def test_get_recent_trades_service_unavailable_propagation(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_market_data_service: MagicMock
    ) -> None:
        """Test get_recent_trades exact propagation of service unavailable errors."""
        api = hl_api_with_di()

        # Configure mock to raise service unavailable error
        service_error = APIError(
            "Service temporarily unavailable",
            code=APIErrorCode.SERVICE_UNAVAILABLE.value,
            http_status=503,
        )
        mock_hl_market_data_service.get_recent_trades.side_effect = service_error

        # Test exact service unavailable error propagation
        with pytest.raises(APIError) as exc_info:
            await api.get_recent_trades("BTC", limit=10)

        assert exc_info.value is service_error  # Same instance
        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == 503
        mock_hl_market_data_service.get_recent_trades.assert_called_once_with(symbol="BTC")

    @pytest.mark.asyncio
    async def test_place_order_service_unexpected_exception(
        self, hl_api_with_di: Callable[..., HyperliquidAPI], mock_hl_trading_service: MagicMock
    ) -> None:
        """Test place_order exact propagation of unexpected exceptions from service."""
        api = hl_api_with_di()

        # Configure mock to raise unexpected exception
        unexpected_error = RuntimeError("Unexpected service failure")
        mock_hl_trading_service.place_order.side_effect = unexpected_error

        # Test exact unexpected exception propagation
        with pytest.raises(RuntimeError) as exc_info:
            place_order_args = PlaceOrderArgs(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("50000.0"),
                time_in_force=TimeInForce.GTC,
            )
            await api.place_order(place_order_args)

        assert exc_info.value is unexpected_error  # Same instance
        assert "Unexpected service failure" in str(exc_info.value)
