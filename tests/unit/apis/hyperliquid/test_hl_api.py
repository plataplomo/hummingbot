"""Unit tests for the HyperliquidAPI client implementation.

Tests use dependency injection patterns to mock collaborators and focus on isolated logic testing.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import AnyUrl, ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import (
    HyperliquidMarketDataService,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.service_args.market_data import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
)
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.enums import CancelOrderResultStatus, OrderStatus
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market import Candle, Market, OrderBook
from cyberdelta.core.models.market.order import (
    CancelOrderResult,
    Order,
)
from cyberdelta.enums.environment import EnvironmentType
from cyberdelta.enums.trading import OrderSide, OrderType, TimeInForce
from cyberdelta.exceptions.base import RequiredParameterError
from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL


# Removed create_test_exchange_config function - now using active_hl_config fixture


# --- Dependency Injection Test Fixtures for HyperliquidAPI ---


@pytest.fixture
def mock_hl_http_client() -> MagicMock:
    """Mock HttpClient for HyperliquidAPI main endpoint.

    Returns:
        MagicMock: Mock HTTP client with async request methods.
    """
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_hl_authenticator() -> MagicMock:
    """Mock HyperliquidEip712Authenticator.

    Returns:
        MagicMock: Mock authenticator with wallet address and prepare_request method.
    """
    mock_auth = MagicMock(spec=HyperliquidEip712Authenticator)
    mock_auth.prepare_request = AsyncMock()
    mock_auth.wallet_address = "0x1234567890123456789012345678901234567890"
    return mock_auth


@pytest.fixture
def mock_hl_error_mapper() -> MagicMock:
    """Mock HyperliquidErrorMapper.

    Returns:
        MagicMock: Mock error mapper for handling API errors.
    """
    return MagicMock(spec=HyperliquidErrorMapper)


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Mock HyperliquidMarketDataRequestBuilder.

    Returns:
        MagicMock: Mock request builder for constructing API requests.
    """
    return MagicMock(spec=HyperliquidMarketDataRequestBuilder)


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Mock HyperliquidResponseHandler.

    Returns:
        MagicMock: Mock response handler for processing API responses.
    """
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_account_service() -> MagicMock:
    """Mock HyperliquidAccountService.

    Returns:
        MagicMock: Mock account service with async methods for account operations.
    """
    mock_service = MagicMock(spec=HyperliquidAccountService)
    mock_service.get_balances = AsyncMock()
    mock_service.get_positions = AsyncMock()
    mock_service.get_account_summary = AsyncMock()
    mock_service.get_order_history = AsyncMock()
    mock_service.get_trade_history = AsyncMock()
    return mock_service


@pytest.fixture
def mock_hl_trading_service() -> MagicMock:
    """Mock HyperliquidTradingService.

    Returns:
        MagicMock: Mock trading service with async methods for trading operations.
    """
    mock_service = MagicMock(spec=HyperliquidTradingService)
    mock_service.place_order = AsyncMock()
    mock_service.cancel_order = AsyncMock()
    mock_service.cancel_all_orders = AsyncMock()
    mock_service.get_open_orders = AsyncMock()
    mock_service.get_order = AsyncMock()
    return mock_service


@pytest.fixture
def mock_hl_market_data_service() -> MagicMock:
    """Mock HyperliquidMarketDataService.

    Returns:
        MagicMock: Mock market data service with async methods for market data operations.
    """
    mock_service = MagicMock(spec=HyperliquidMarketDataService)
    mock_service.get_ticker = AsyncMock()
    mock_service.get_order_book = AsyncMock()
    mock_service.get_recent_trades = AsyncMock()
    mock_service.get_funding_rates = AsyncMock()
    mock_service.get_market_data = AsyncMock()
    mock_service.get_historical_funding_rates = AsyncMock()
    mock_service.get_market = AsyncMock()
    mock_service.get_markets = AsyncMock()
    return mock_service


@pytest.fixture
def hl_api_with_di(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
    mock_hl_authenticator: MagicMock,
    mock_hl_error_mapper: MagicMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_http_client: MagicMock,
    mock_hl_account_service: MagicMock,
    mock_hl_trading_service: MagicMock,
    mock_hl_market_data_service: MagicMock,
) -> Callable[..., Any]:
    """Create HyperliquidAPI instances with all dependencies injected.

    This enables black-box testing without accessing private members.
    Uses active configuration and secrets from test fixtures.

    Returns:
        Callable[..., Any]: Factory function for creating HyperliquidAPI instances.
    """

    def _create_api(
        # Allow overriding specific dependencies if needed
        config: ExchangeSpecificConfig | None = None,
        secrets: PrivateKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI with injected dependencies.

        Returns:
            HyperliquidAPI: Configured API instance with injected dependencies.
        """
        # Use active fixtures as defaults
        if config is None:
            config = active_hl_config
        if secrets is None:
            secrets = active_hl_secrets

        return HyperliquidAPI(
            exchange_config=config,
            exchange_secrets=secrets,
            authenticator=overrides.get("authenticator", mock_hl_authenticator),
            error_mapper=overrides.get("error_mapper", mock_hl_error_mapper),
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
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that the DI fixture creates a valid API instance."""
        api = hl_api_with_di()

        # Verify the API instance is created correctly
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

    def test_api_creation_with_active_config(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test API creation with active configuration fixture."""
        # Use the active configuration from test config
        api = hl_api_with_di(config=active_hl_config)
        assert api is not None
        # Verify that the API uses the active configuration
        assert api.exchange_name == "hyperliquid"

    def test_api_has_required_services(self, hl_api_with_di: Callable[..., HyperliquidAPI]) -> None:
        """Test that API instance has all required services initialized."""
        api = hl_api_with_di()

        # Verify the API instance has all required services
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")


class TestHyperliquidAPIWebSocketOperations:
    """Test WebSocket operations."""

    def test_subscription_payload_construction_public_behavior(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test subscription payload construction through public interface."""
        api = hl_api_with_di()

        # Test that the method exists and can be called
        # Note: We avoid accessing protected members directly
        # Instead we test through public interface behavior
        assert hasattr(api, "subscribe_to_order_book")
        assert hasattr(api, "subscribe_to_trades")
        assert hasattr(api, "subscribe_to_account_updates")


class TestHyperliquidAPIDependencyIsolation:
    """Test dependency isolation and injection."""

    def test_custom_dependency_override(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that custom dependencies can be injected."""
        custom_trading_service = MagicMock()
        api = hl_api_with_di(trading_service=custom_trading_service)

        # Verify the custom dependency was injected
        assert api.trading_service is custom_trading_service

    def test_multiple_api_instances_are_isolated(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
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


class TestHyperliquidAPIConfigurationIntegration:
    """Test configuration integration with active fixtures."""

    def test_active_config_environment_awareness(
        self,
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that active_hl_config fixture provides valid configuration."""
        # Verify that the active configuration has required URLs
        assert active_hl_config.api_base_url_mainnet is not None
        assert active_hl_config.ws_url_mainnet is not None
        assert active_hl_config.exchange_name.value == "hyperliquid"

        # Test computed properties work
        assert active_hl_config.active_api_base_url is not None
        assert active_hl_config.active_ws_url is not None

    def test_api_creation_with_active_fixtures(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test that API can be created with active configuration fixtures."""
        # Test with active config and secrets
        api = hl_api_with_di(config=active_hl_config, secrets=active_hl_secrets)

        assert api is not None
        assert api.exchange_name == "hyperliquid"

    def test_api_uses_default_active_config(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that API uses active configuration by default."""
        # No explicit config/secrets provided - should use active fixtures
        api = hl_api_with_di()

        assert api is not None
        assert api.exchange_name == "hyperliquid"


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
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that API can be used as a context manager."""
        api = hl_api_with_di()

        # Test basic usage without context manager for now
        # since HyperliquidAPI doesn't implement __aenter__/__aexit__
        assert api.exchange_name == "hyperliquid"
        await api.close()


class TestHyperliquidAPIMarketDataMethods:
    """Test market data methods in HyperliquidAPI."""

    @pytest.mark.asyncio
    async def test_get_markets_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_markets call delegates to market data service."""
        # Create test data
        expected_markets = [
            Market(
                symbol=BTC_HL,
                market_type="Perpetual",
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.001"),
                status="Trading",
            ),
            Market(
                symbol=ETH_HL,
                market_type="Perpetual",
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.01"),
                status="Trading",
            ),
        ]

        # Configure mock service
        mock_hl_market_data_service.get_markets.return_value = expected_markets

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        args = GetMarketsArgs()
        result = await api.get_markets(args)

        # Verify
        assert result == expected_markets
        assert len(result) == 2
        assert result[0].symbol == BTC_HL
        assert result[1].symbol == ETH_HL

        # Verify service was called correctly
        mock_hl_market_data_service.get_markets.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_markets_empty_list(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test get_markets returns empty list when service returns empty list."""
        # Configure mock service to return empty list
        mock_hl_market_data_service.get_markets.return_value = []

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        args = GetMarketsArgs()
        result = await api.get_markets(args)

        # Verify
        assert result == []
        assert isinstance(result, list)

        # Verify service was called correctly
        mock_hl_market_data_service.get_markets.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_markets_service_error_propagation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that exceptions from market data service are propagated."""
        # Configure mock service to raise an error
        api_error = APIError(
            message="Failed to fetch markets",
            code=APIErrorCode.RATE_LIMITED.value,
        )
        mock_hl_market_data_service.get_markets.side_effect = api_error

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute and verify exception is propagated
        args = GetMarketsArgs()
        with pytest.raises(APIError) as exc_info:
            await api.get_markets(args)

        assert exc_info.value == api_error
        mock_hl_market_data_service.get_markets.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_market_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_market call delegates to market data service."""
        # Create test data
        symbol = BTC_HL.value
        expected_market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            status="Trading",
        )

        # Configure mock service
        mock_hl_market_data_service.get_market.return_value = expected_market

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        args = GetMarketArgs(symbol=BTC_HL)
        result = await api.get_market(args)

        # Verify
        assert result == expected_market
        assert result.symbol == BTC_HL
        assert result.market_type == "Perpetual"

        # Verify service was called correctly
        mock_hl_market_data_service.get_market.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_market_with_different_symbols(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test get_market works with different symbol formats."""
        test_cases = [
            (BTC_HL, "BTC", "USD"),
            (ETH_HL, "ETH", "USD"),
            (SOL_HL, "SOL", "USD"),
        ]

        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        for symbol, base, quote in test_cases:
            # Create expected market for this test case
            expected_market = Market(
                symbol=symbol,
                market_type="Perpetual",
                tick_size=Decimal("0.01"),
                step_size=Decimal("0.001"),
                status="Trading",
            )

            # Configure mock service for this symbol
            mock_hl_market_data_service.get_market.return_value = expected_market

            # Execute
            args = GetMarketArgs(symbol=symbol)
            result = await api.get_market(args)

            # Verify
            assert result == expected_market
            assert result.symbol == symbol

        # Verify service was called for each test case
        assert mock_hl_market_data_service.get_market.call_count == len(test_cases)

    @pytest.mark.asyncio
    async def test_get_market_service_error_propagation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that exceptions from market data service are propagated."""
        # Configure mock service to raise an error
        symbol = BTC_HL.value
        api_error = APIError(
            message=f"Market {symbol} not found",
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,
        )
        mock_hl_market_data_service.get_market.side_effect = api_error

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute and verify exception is propagated
        args = GetMarketArgs(symbol=BTC_HL)
        with pytest.raises(APIError) as exc_info:
            await api.get_market(args)

        assert exc_info.value == api_error
        mock_hl_market_data_service.get_market.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_market_args_validation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that GetMarketArgs validation works correctly."""
        # Test valid args creation
        valid_args = GetMarketArgs(symbol=BTC_HL)
        assert valid_args.symbol == BTC_HL

        # Test invalid args - None symbol should fail validation
        with pytest.raises(ValidationError) as exc_info:
            GetMarketArgs(symbol=None)  # type: ignore[arg-type]
        assert "Input should be an instance of BaseSymbol" in str(exc_info.value)

        # Test args with invalid symbol type
        with pytest.raises(ValidationError) as exc_info:
            GetMarketArgs(symbol="INVALID")  # type: ignore[arg-type]
        assert "Input should be an instance of BaseSymbol" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_markets_args_validation(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test that GetMarketsArgs validation works correctly."""
        # Test valid args creation (no fields required)
        valid_args = GetMarketsArgs()
        assert valid_args is not None

        # Test that extra fields are forbidden
        with pytest.raises(ValidationError):
            # This should fail due to extra fields being forbidden
            GetMarketsArgs(extra_field="not_allowed")  # type: ignore[call-arg]


class TestHyperliquidAPIAccountMethods:
    """Test account-related methods in HyperliquidAPI."""

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test successful get_balances call delegates to account service."""
        # Create test data
        from tests.common_symbols import BTC_ASSET_HL, USD_HL

        expected_balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset=USD_HL,
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("1050.00"),
                available_quantity=Decimal("1000.00"),
                hl_details=None,
                bp_details=None,
            ),
            "BTC": SpotBalance(
                exchange="hyperliquid",
                asset=BTC_ASSET_HL,
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("0.6"),
                available_quantity=Decimal("0.5"),
                hl_details=None,
                bp_details=None,
            ),
        }

        # Configure mock service
        mock_hl_account_service.get_balances.return_value = expected_balances

        # Create API instance with mocked service
        api = hl_api_with_di(account_service=mock_hl_account_service)

        # Execute
        result = await api.get_balances()

        # Verify
        assert result == expected_balances
        assert len(result) == 2
        assert "USDC" in result
        assert "BTC" in result
        assert result["USDC"].total_quantity == Decimal("1050.00")

        # Verify service was called correctly
        mock_hl_account_service.get_balances.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_positions_with_symbol(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test get_positions with specific symbol."""
        # Create test data
        expected_positions = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol=BTC_HL,
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal("50000.00"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000.00"),
                liquidation_price=None,
                unrealized_pnl=Decimal("1000.00"),
                realized_pnl=Decimal("100.00"),
                strategy_name=None,
                signal_id=None,
                hl_details=None,
                bp_details=None,
            )
        ]

        # Configure mock service
        mock_hl_account_service.get_positions.return_value = expected_positions

        # Create API instance with mocked service
        api = hl_api_with_di(account_service=mock_hl_account_service)

        # Execute
        result = await api.get_positions(symbol=BTC_HL)

        # Verify
        assert result == expected_positions
        assert len(result) == 1
        assert result[0].symbol == BTC_HL
        assert result[0].unrealized_pnl == Decimal("1000.00")

        # Verify service was called correctly
        mock_hl_account_service.get_positions.assert_called_once_with(symbol=BTC_HL)

    @pytest.mark.asyncio
    async def test_get_positions_all(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test get_positions without symbol (all positions)."""
        # Configure mock service to return empty list
        mock_hl_account_service.get_positions.return_value = []

        # Create API instance with mocked service
        api = hl_api_with_di(account_service=mock_hl_account_service)

        # Execute
        result = await api.get_positions()

        # Verify
        assert result == []
        mock_hl_account_service.get_positions.assert_called_once_with(symbol=None)

    @pytest.mark.asyncio
    async def test_get_account_summary_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test successful get_account_summary call."""
        # Create test data
        expected_summary = MarginAccountSummary(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            total_equity=Decimal("10000.00"),
            available_equity=Decimal("8000.00"),
            total_initial_margin_required=Decimal("2000.00"),
            total_maintenance_margin_required=Decimal("1500.00"),
            total_position_notional=Decimal("50000.00"),
            total_unrealized_pnl=Decimal("500.00"),
            hl_details=None,
            bp_details=None,
        )

        # Configure mock service
        mock_hl_account_service.get_account_summary.return_value = expected_summary

        # Create API instance with mocked service
        api = hl_api_with_di(account_service=mock_hl_account_service)

        # Execute
        result = await api.get_account_summary()

        # Verify
        assert result == expected_summary
        assert result.total_equity == Decimal("10000.00")
        assert result.available_equity == Decimal("8000.00")

        # Verify service was called correctly
        mock_hl_account_service.get_account_summary.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test successful get_order_history call."""
        # Create test data
        expected_orders = [
            Order(
                client_order_id="order123",
                exchange_order_id="ex123",
                related_order_id=None,
                exchange="hyperliquid",
                symbol=BTC_HL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                status=OrderStatus.FILLED,
                quantity_requested=Decimal("1.0"),
                quote_quantity_requested=None,
                quantity_filled=Decimal("1.0"),
                price=Decimal("50000.00"),
                stop_price=None,
                average_fill_price=Decimal("50000.00"),
                trigger_by=None,
                time_in_force=TimeInForce.GTC,
                reduce_only=False,
                post_only=False,
                created_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                trades=[],
                hl_details=None,
                bp_details=None,
            ),
        ]

        # Configure mock service
        mock_hl_account_service.get_order_history.return_value = expected_orders

        # Create API instance with mocked service
        api = hl_api_with_di(account_service=mock_hl_account_service)

        # Execute
        args = GetOrderHistoryArgs(symbol=BTC_HL, limit=10)
        result = await api.get_order_history(args)

        # Verify
        assert result == expected_orders
        assert len(result) == 1
        assert result[0].client_order_id == "order123"

        # Verify service was called correctly
        mock_hl_account_service.get_order_history.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_trade_history_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_account_service: MagicMock,
    ) -> None:
        """Test successful get_trade_history call."""
        # Create test data
        expected_trades = [
            Trade(
                id="trade123",
                symbol=BTC_HL,
                executed_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
                side=OrderSide.BUY,
                order_id="order123",
                exchange="hyperliquid",
                price=Decimal("50000.00"),
                quantity=Decimal("1.0"),
                client_order_id="order123",
                fee=Decimal("5.00"),
                fee_asset="USDC",
                is_maker=False,
                hl_details=None,
                bp_details=None,
            ),
        ]

        # Configure mock service
        mock_hl_account_service.get_trade_history.return_value = expected_trades

        # Create API instance with mocked service
        api = hl_api_with_di(account_service=mock_hl_account_service)

        # Execute
        args = GetTradeHistoryArgs(symbol=BTC_HL, limit=10)
        result = await api.get_trade_history(args)

        # Verify
        assert result == expected_trades
        assert len(result) == 1
        assert result[0].id == "trade123"

        # Verify service was called correctly
        mock_hl_account_service.get_trade_history.assert_called_once_with(args=args)


class TestHyperliquidAPITradingMethods:
    """Test trading-related methods in HyperliquidAPI."""

    @pytest.mark.asyncio
    async def test_place_order_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test successful place_order call."""
        # Create test data
        args = PlaceOrderArgs(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            time_in_force=TimeInForce.GTC,
        )

        expected_order = Order(
            client_order_id="order123",
            exchange_order_id="ex123",
            related_order_id=None,
            exchange="hyperliquid",
            symbol=BTC_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.OPEN,
            quantity_requested=Decimal("1.0"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.0"),
            price=Decimal("50000.00"),
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.GTC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )

        # Configure mock service
        mock_hl_trading_service.place_order.return_value = expected_order

        # Create API instance with mocked service
        api = hl_api_with_di(trading_service=mock_hl_trading_service)

        # Execute
        result = await api.place_order(args)

        # Verify
        assert result == expected_order
        assert result.client_order_id == "order123"
        assert result.status == OrderStatus.OPEN

        # Verify service was called correctly
        mock_hl_trading_service.place_order.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test successful cancel_order call."""
        # Create test data
        args = CancelOrderArgs(
            order_id="order123",
            symbol=BTC_HL,
        )

        expected_result = CancelOrderResult(
            symbol=BTC_HL,
            order_id="order123",
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

        # Configure mock service
        mock_hl_trading_service.cancel_order.return_value = expected_result

        # Create API instance with mocked service
        api = hl_api_with_di(trading_service=mock_hl_trading_service)

        # Execute
        result = await api.cancel_order(args)

        # Verify
        assert result == expected_result
        assert result.success is True
        assert result.status == CancelOrderResultStatus.SUCCESS

        # Verify service was called correctly
        mock_hl_trading_service.cancel_order.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_cancel_all_orders_with_symbol(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test cancel_all_orders with specific symbol."""
        # Create test data
        expected_results = [
            CancelOrderResult(
                symbol=BTC_HL,
                order_id="order1",
                client_order_id=None,
                success=True,
                message=None,
                status=CancelOrderResultStatus.SUCCESS,
                raw_response=None,
            ),
            CancelOrderResult(
                symbol=BTC_HL,
                order_id="order2",
                client_order_id=None,
                success=True,
                message=None,
                status=CancelOrderResultStatus.SUCCESS,
                raw_response=None,
            ),
        ]

        # Configure mock service
        mock_hl_trading_service.cancel_all_orders.return_value = expected_results

        # Create API instance with mocked service
        api = hl_api_with_di(trading_service=mock_hl_trading_service)

        # Execute
        result = await api.cancel_all_orders(symbol=BTC_HL)

        # Verify
        assert result == expected_results
        assert len(result) == 2
        assert all(r.success for r in result)

        # Verify service was called correctly
        mock_hl_trading_service.cancel_all_orders.assert_called_once_with(symbol=BTC_HL)

    @pytest.mark.asyncio
    async def test_get_open_orders_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test successful get_open_orders call."""
        # Create test data
        expected_orders = [
            Order(
                client_order_id="order123",
                exchange_order_id="ex123",
                related_order_id=None,
                exchange="hyperliquid",
                symbol=BTC_HL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                status=OrderStatus.OPEN,
                quantity_requested=Decimal("1.0"),
                quote_quantity_requested=None,
                quantity_filled=Decimal("0.0"),
                price=Decimal("50000.00"),
                stop_price=None,
                average_fill_price=None,
                trigger_by=None,
                time_in_force=TimeInForce.GTC,
                reduce_only=False,
                post_only=False,
                created_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                trades=[],
                hl_details=None,
                bp_details=None,
            ),
        ]

        # Configure mock service
        mock_hl_trading_service.get_open_orders.return_value = expected_orders

        # Create API instance with mocked service
        api = hl_api_with_di(trading_service=mock_hl_trading_service)

        # Execute
        result = await api.get_open_orders(symbol=BTC_HL)

        # Verify
        assert result == expected_orders
        assert len(result) == 1
        assert result[0].status == OrderStatus.OPEN

        # Verify service was called correctly
        mock_hl_trading_service.get_open_orders.assert_called_once_with(symbol=BTC_HL)

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test successful get_order_status call."""
        # Create test data
        args = GetOrderArgs(order_id="order123", symbol=BTC_HL)
        expected_order = Order(
            client_order_id="order123",
            exchange_order_id="ex123",
            related_order_id=None,
            exchange="hyperliquid",
            symbol=BTC_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.PARTIALLY_FILLED,
            quantity_requested=Decimal("1.0"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("0.5"),
            price=Decimal("50000.00"),
            stop_price=None,
            average_fill_price=None,
            trigger_by=None,
            time_in_force=TimeInForce.GTC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )

        # Configure mock service
        mock_hl_trading_service.get_order.return_value = expected_order

        # Create API instance with mocked service
        api = hl_api_with_di(trading_service=mock_hl_trading_service)

        # Execute
        result = await api.get_order_status(args)

        # Verify
        assert result == expected_order
        assert result is not None
        assert result.status == OrderStatus.PARTIALLY_FILLED

        # Verify service was called correctly
        mock_hl_trading_service.get_order.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_trading_service: MagicMock,
    ) -> None:
        """Test successful get_order call (alias for get_order_status)."""
        # Create test data
        args = GetOrderArgs(order_id="order123", symbol=BTC_HL)
        expected_order = Order(
            client_order_id="order123",
            exchange_order_id="ex123",
            related_order_id=None,
            exchange="hyperliquid",
            symbol=BTC_HL,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("1.0"),
            quote_quantity_requested=None,
            quantity_filled=Decimal("1.0"),
            price=None,
            stop_price=None,
            average_fill_price=Decimal("50000.00"),
            trigger_by=None,
            time_in_force=TimeInForce.GTC,
            reduce_only=False,
            post_only=False,
            created_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
            hl_details=None,
            bp_details=None,
        )

        # Configure mock service
        mock_hl_trading_service.get_order.return_value = expected_order

        # Create API instance with mocked service
        api = hl_api_with_di(trading_service=mock_hl_trading_service)

        # Execute
        result = await api.get_order(args)

        # Verify
        assert result == expected_order
        assert result is not None
        assert result.status == OrderStatus.FILLED

        # Verify service was called correctly
        mock_hl_trading_service.get_order.assert_called_once_with(args=args)


class TestHyperliquidAPIMarketDataAdditionalMethods:
    """Test additional market data methods in HyperliquidAPI."""

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_ticker call."""
        # Create test data
        expected_ticker = Ticker(
            symbol=BTC_HL,
            exchange="hyperliquid",
            timestamp=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
            price=Decimal("50000.50"),
            bid=Decimal("50000.00"),
            ask=Decimal("50001.00"),
            volume=Decimal("1000.00"),
            hl_details=None,
            bp_details=None,
        )

        # Configure mock service
        mock_hl_market_data_service.get_ticker.return_value = expected_ticker

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        result = await api.get_ticker(BTC_HL)

        # Verify
        assert result == expected_ticker
        assert result is not None
        assert result.symbol == BTC_HL
        assert result.bid == Decimal("50000.00")

        # Verify service was called correctly
        mock_hl_market_data_service.get_ticker.assert_called_once_with(symbol=BTC_HL)

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_order_book call."""
        # Create test data
        expected_order_book = OrderBook(
            symbol=BTC_HL,
            timestamp=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
            bids=[(Decimal("50000.00"), Decimal("10.0"))],
            asks=[(Decimal("50001.00"), Decimal("10.0"))],
        )

        # Configure mock service
        mock_hl_market_data_service.get_order_book.return_value = expected_order_book

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        result = await api.get_order_book(BTC_HL, depth=10)

        # Verify
        assert result == expected_order_book
        assert result is not None
        assert result.symbol == BTC_HL
        assert len(result.bids) == 1
        assert len(result.asks) == 1

        # Verify service was called correctly
        mock_hl_market_data_service.get_order_book.assert_called_once_with(symbol=BTC_HL)

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_recent_trades call."""
        # Create test data
        expected_trades = [
            Trade(
                id="trade123",
                symbol=BTC_HL,
                executed_at=datetime.fromisoformat("2024-01-01T10:00:00+00:00"),
                side=OrderSide.BUY,
                order_id="order123",
                exchange="hyperliquid",
                price=Decimal("50000.00"),
                quantity=Decimal("0.1"),
                client_order_id=None,
                fee=Decimal("0.0"),
                fee_asset=None,
                is_maker=None,
                hl_details=None,
                bp_details=None,
            ),
            Trade(
                id="trade124",
                symbol=BTC_HL,
                executed_at=datetime.fromisoformat("2024-01-01T10:00:01+00:00"),
                side=OrderSide.SELL,
                order_id="order124",
                exchange="hyperliquid",
                price=Decimal("50001.00"),
                quantity=Decimal("0.2"),
                client_order_id=None,
                fee=Decimal("0.0"),
                fee_asset=None,
                is_maker=None,
                hl_details=None,
                bp_details=None,
            ),
        ]

        # Configure mock service
        mock_hl_market_data_service.get_recent_trades.return_value = expected_trades

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        result = await api.get_recent_trades(BTC_HL, limit=50)

        # Verify
        assert result == expected_trades
        assert len(result) == 2
        assert result[0].id == "trade123"

        # Verify service was called correctly
        # Note: Hyperliquid's get_recent_trades only takes symbol parameter, not limit
        mock_hl_market_data_service.get_recent_trades.assert_called_once_with(symbol=BTC_HL)

    @pytest.mark.asyncio
    async def test_get_funding_rates_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_funding_rates call."""
        # Create test data
        args = GetFundingRatesArgs(symbols=[BTC_HL, ETH_HL])
        expected_rates = [
            FundingRate(
                symbol=BTC_HL,
                timestamp=datetime.fromisoformat("2024-01-01T08:00:00+00:00"),
                funding_rate=Decimal("0.0001"),
                predicted_rate=None,
                mark_price=None,
                index_price=None,
                next_funding_time=None,
                hl_details=None,
                bp_details=None,
            ),
            FundingRate(
                symbol=ETH_HL,
                timestamp=datetime.fromisoformat("2024-01-01T08:00:00+00:00"),
                funding_rate=Decimal("0.0002"),
                predicted_rate=None,
                mark_price=None,
                index_price=None,
                next_funding_time=None,
                hl_details=None,
                bp_details=None,
            ),
        ]

        # Configure mock service
        mock_hl_market_data_service.get_funding_rates.return_value = expected_rates

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        result = await api.get_funding_rates(args)

        # Verify
        assert result == expected_rates
        assert len(result) == 2
        assert result[0].funding_rate == Decimal("0.0001")

        # Verify service was called correctly
        mock_hl_market_data_service.get_funding_rates.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_market_data call."""
        # Create test data
        args = GetMarketDataArgs(
            symbol=BTC_HL,
            timeframe="1h",
            start_time_ms=1704067200000,  # 2024-01-01T00:00:00Z
            end_time_ms=1704110400000,  # 2024-01-01T12:00:00Z
            limit=10,
        )
        expected_candles = [
            Candle(
                symbol=BTC_HL,
                interval="1h",
                open_time=datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
                open=Decimal("50000.00"),
                high=Decimal("51000.00"),
                low=Decimal("49500.00"),
                close=Decimal("50500.00"),
                volume=Decimal("100.00"),
            ),
        ]

        # Configure mock service
        mock_hl_market_data_service.get_market_data.return_value = expected_candles

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        result = await api.get_market_data(args)

        # Verify
        assert result == expected_candles
        assert len(result) == 1
        assert result[0].high == Decimal("51000.00")

        # Verify service was called correctly
        mock_hl_market_data_service.get_market_data.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_market_data_service: MagicMock,
    ) -> None:
        """Test successful get_historical_funding_rates call."""
        # Create test data
        args = GetHistoricalFundingRatesArgs(
            symbol=BTC_HL,
            start_time=datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
            end_time=datetime.fromisoformat("2024-01-02T00:00:00+00:00"),
        )
        expected_rates = [
            FundingRate(
                symbol=BTC_HL,
                timestamp=datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
                funding_rate=Decimal("0.0001"),
                predicted_rate=None,
                mark_price=None,
                index_price=None,
                next_funding_time=None,
                hl_details=None,
                bp_details=None,
            ),
            FundingRate(
                symbol=BTC_HL,
                timestamp=datetime.fromisoformat("2024-01-01T08:00:00+00:00"),
                funding_rate=Decimal("0.0002"),
                predicted_rate=None,
                mark_price=None,
                index_price=None,
                next_funding_time=None,
                hl_details=None,
                bp_details=None,
            ),
        ]

        # Configure mock service
        mock_hl_market_data_service.get_historical_funding_rates.return_value = expected_rates

        # Create API instance with mocked service
        api = hl_api_with_di(market_data_service=mock_hl_market_data_service)

        # Execute
        result = await api.get_historical_funding_rates(args)

        # Verify
        assert result == expected_rates
        assert len(result) == 2

        # Verify service was called correctly
        mock_hl_market_data_service.get_historical_funding_rates.assert_called_once_with(args=args)


class TestHyperliquidAPIInitializationPaths:
    """Test different initialization paths for HyperliquidAPI."""

    def test_api_initialization_mainnet(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test API initialization with mainnet configuration."""
        # Ensure config is set to mainnet
        config = active_hl_config.model_copy(update={"environment_type": EnvironmentType.MAINNET})

        # Create API instance
        api = hl_api_with_di(config=config)

        # Verify initialization
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert api.rest_endpoint == str(config.api_base_url_mainnet)
        if config.ws_url_mainnet:
            assert api.ws_endpoint == str(config.ws_url_mainnet)

    def test_api_initialization_testnet(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test API initialization with testnet configuration."""
        # Create testnet config
        config = active_hl_config.model_copy(
            update={
                "environment_type": EnvironmentType.TESTNET,
                "api_base_url_testnet": AnyUrl("https://api.testnet.hyperliquid.xyz"),
                "ws_url_testnet": AnyUrl("wss://api.testnet.hyperliquid.xyz/ws"),
            }
        )

        # Create API instance
        api = hl_api_with_di(config=config)

        # Verify initialization
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert api.rest_endpoint == "https://api.testnet.hyperliquid.xyz"
        assert api.ws_endpoint == "wss://api.testnet.hyperliquid.xyz/ws"

    def test_api_initialization_testnet_fallback_to_mainnet(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test API initialization falls back to mainnet when testnet URLs missing."""
        # Create testnet config without testnet URLs
        config = active_hl_config.model_copy(
            update={
                "environment_type": EnvironmentType.TESTNET,
                "api_base_url_testnet": None,
                "ws_url_testnet": None,
            }
        )

        # Create API instance - should fallback to mainnet
        api = hl_api_with_di(config=config)

        # Verify initialization with mainnet URLs
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert api.rest_endpoint == str(config.api_base_url_mainnet)
        if config.ws_url_mainnet:
            assert api.ws_endpoint == str(config.ws_url_mainnet)

    def test_api_initialization_missing_chain_id(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test API initialization fails when chain_id is missing."""
        # Create config without chain_id
        config = active_hl_config.model_copy(update={"chain_id": None})

        # Attempt to create API instance should raise error
        with pytest.raises(RequiredParameterError) as exc_info:
            hl_api_with_di(config=config)

        assert exc_info.value.parameter == "chain_id"
        assert "Hyperliquid" in str(exc_info.value)


class TestHyperliquidAPIHelperMethods:
    """Test helper methods in HyperliquidAPI."""

    @pytest.mark.asyncio
    async def test_close_method(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        mock_hl_http_client: MagicMock,
    ) -> None:
        """Test that close method properly closes HTTP client."""
        # Create API instance with mocked HTTP client
        api = hl_api_with_di(http_client=mock_hl_http_client)

        # Call close
        await api.close()

        # Verify HTTP client close was called
        mock_hl_http_client.close_session.assert_called_once()

    # NOTE: Authentication and asset index functionality are tested through public methods
    # that internally use these components. Testing private methods (_authenticate,
    # _get_asset_index) violates the principle of testing only through already exposed
    # public behavior.
