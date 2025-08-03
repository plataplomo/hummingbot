"""Improved unit tests for HyperliquidAPI to boost coverage.

Tests all public methods with parametrized tests, smart fixture usage,
and comprehensive success/edge/failure cases. Tests only through public APIs.
"""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.market_data import (
    GetMarketArgs,
    GetMarketsArgs,
)
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderArgs,
)
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from tests.common_symbols import BTC_HL, BTC_USD_HL, ETH_HL, ETH_USD_HL, SOL_HL, SOL_USD_HL


class TestHyperliquidAPIImproved:
    """Improved tests for HyperliquidAPI focusing on coverage increase."""

    @pytest.fixture
    def mock_dependencies(self) -> dict[str, MagicMock]:
        """Create all mock dependencies for API testing.

        Returns:
            Dictionary containing all mocked dependencies keyed by service name
        """
        authenticator = MagicMock()
        authenticator.wallet_address = "0x1234567890123456789012345678901234567890"
        authenticator.prepare_request = AsyncMock()

        http_client = MagicMock()
        http_client.request = AsyncMock()
        http_client.close_session = AsyncMock()

        error_mapper = MagicMock()

        account_service = MagicMock()
        account_service.get_balances = AsyncMock(return_value={})
        account_service.get_positions = AsyncMock(return_value=[])
        account_service.get_account_summary = AsyncMock(return_value={})
        account_service.get_order_history = AsyncMock(return_value=[])
        account_service.get_trade_history = AsyncMock(return_value=[])

        trading_service = MagicMock()
        trading_service.place_order = AsyncMock()
        trading_service.cancel_order = AsyncMock()
        trading_service.cancel_all_orders = AsyncMock(return_value=[])
        trading_service.get_open_orders = AsyncMock(return_value=[])
        trading_service.get_order = AsyncMock()
        trading_service.place_batch_orders = AsyncMock(return_value=[])
        trading_service.cancel_batch_orders = AsyncMock(return_value=[])
        trading_service.get_all_open_orders = AsyncMock(return_value=[])

        market_data_service = MagicMock()
        market_data_service.get_ticker = AsyncMock()
        market_data_service.get_order_book = AsyncMock()
        market_data_service.get_recent_trades = AsyncMock(return_value=[])
        market_data_service.get_funding_rates = AsyncMock(return_value=[])
        market_data_service.get_market_data = AsyncMock(return_value=[])
        market_data_service.get_historical_funding_rates = AsyncMock(return_value=[])
        market_data_service.get_market = AsyncMock()
        market_data_service.get_markets = AsyncMock(return_value=[])

        return {
            "authenticator": authenticator,
            "error_mapper": error_mapper,
            "http_client": http_client,
            "account_service": account_service,
            "trading_service": trading_service,
            "market_data_service": market_data_service,
        }

    @pytest.fixture
    def api_instance(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
        mock_dependencies: dict[str, MagicMock],
    ) -> Callable[..., HyperliquidAPI]:
        """Factory to create HyperliquidAPI instances with mock dependencies.

        Args:
            active_hl_config: Exchange configuration
            active_hl_secrets: Authentication secrets
            mock_dependencies: Dictionary of mocked services

        Returns:
            Factory function that creates HyperliquidAPI instances
        """

        def _create(**overrides: MagicMock) -> HyperliquidAPI:
            deps = {**mock_dependencies, **overrides}
            return HyperliquidAPI(
                exchange_config=active_hl_config,
                exchange_secrets=active_hl_secrets,
                **deps,
            )

        return _create

    # Core Properties Tests

    def test_exchange_name_property(self, api_instance: Callable[[], HyperliquidAPI]) -> None:
        """Test exchange_name property returns correct value."""
        api = api_instance()
        assert api.exchange_name == "hyperliquid"

    # Resource Management Tests

    @pytest.mark.asyncio
    async def test_close_method(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test that close method properly calls http client close."""
        api = api_instance()
        await api.close()
        mock_dependencies["http_client"].close_session.assert_called_once()

    # Account Service Delegation Tests

    @pytest.mark.asyncio
    async def test_get_balances_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test get_balances delegates to account service."""
        # Focus on testing delegation, not exact return types
        api = api_instance()
        await api.get_balances()
        mock_dependencies["account_service"].get_balances.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("symbol", [None, BTC_HL.value, ETH_HL.value])
    async def test_get_positions_delegation_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        symbol: str | None,
    ) -> None:
        """Test get_positions delegates correctly with different symbols."""
        api = api_instance()
        await api.get_positions(symbol)
        mock_dependencies["account_service"].get_positions.assert_called_once_with(symbol=symbol)

    @pytest.mark.asyncio
    async def test_get_account_summary_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test get_account_summary delegates to account service."""
        api = api_instance()
        await api.get_account_summary()
        mock_dependencies["account_service"].get_account_summary.assert_called_once()

    # Trading Service Delegation Tests

    @pytest.mark.asyncio
    @pytest.mark.parametrize("symbol", [None, BTC_HL.value, ETH_HL.value])
    async def test_get_open_orders_delegation_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        symbol: str | None,
    ) -> None:
        """Test get_open_orders delegates correctly with different symbols."""
        api = api_instance()
        await api.get_open_orders(symbol)
        mock_dependencies["trading_service"].get_open_orders.assert_called_once_with(symbol=symbol)

    @pytest.mark.asyncio
    async def test_get_order_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test get_order delegates to trading service."""
        args = GetOrderArgs(order_id="order123")
        api = api_instance()
        await api.get_order(args)
        mock_dependencies["trading_service"].get_order.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_cancel_order_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test cancel_order delegates to trading service."""
        args = CancelOrderArgs(order_id="order123")
        api = api_instance()
        await api.cancel_order(args)
        mock_dependencies["trading_service"].cancel_order.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("symbol", [None, BTC_HL.value])
    async def test_cancel_all_orders_delegation_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        symbol: str | None,
    ) -> None:
        """Test cancel_all_orders delegates correctly with different symbols."""
        api = api_instance()
        await api.cancel_all_orders(symbol)
        mock_dependencies["trading_service"].cancel_all_orders.assert_called_once_with(
            symbol=symbol
        )

    # Market Data Service Delegation Tests

    @pytest.mark.asyncio
    @pytest.mark.parametrize("symbol", [BTC_HL.value, ETH_HL.value, SOL_HL.value])
    async def test_get_ticker_delegation_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        symbol: str,
    ) -> None:
        """Test get_ticker delegates correctly with different symbols."""
        api = api_instance()
        await api.get_ticker(symbol)
        mock_dependencies["market_data_service"].get_ticker.assert_called_once_with(symbol=symbol)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("symbol", "depth"),
        [
            (BTC_USD_HL.value, None),
            (ETH_USD_HL.value, 10),
            (SOL_USD_HL.value, 50),
        ],
    )
    async def test_get_order_book_delegation_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        symbol: str,
        depth: int | None,
    ) -> None:
        """Test get_order_book delegates correctly with different parameters."""
        api = api_instance()
        await api.get_order_book(symbol, depth)
        mock_dependencies["market_data_service"].get_order_book.assert_called_once_with(
            symbol=symbol, depth=depth
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("symbol", "limit"),
        [
            (BTC_USD_HL.value, 50),
            (ETH_USD_HL.value, 100),
            (SOL_USD_HL.value, None),
        ],
    )
    async def test_get_recent_trades_delegation_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        symbol: str,
        limit: int | None,
    ) -> None:
        """Test get_recent_trades delegates correctly with different parameters."""
        api = api_instance()
        await api.get_recent_trades(symbol, limit)
        expected_limit = limit if limit is not None else 50
        mock_dependencies["market_data_service"].get_recent_trades.assert_called_once_with(
            symbol=symbol, limit=expected_limit
        )

    @pytest.mark.asyncio
    async def test_get_market_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test get_market delegates to market data service."""
        args = GetMarketArgs(symbol=BTC_USD_HL.value)
        api = api_instance()
        await api.get_market(args)
        mock_dependencies["market_data_service"].get_market.assert_called_once_with(args=args)

    @pytest.mark.asyncio
    async def test_get_markets_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test get_markets delegates to market data service."""
        args = GetMarketsArgs()
        api = api_instance()
        await api.get_markets(args)
        mock_dependencies["market_data_service"].get_markets.assert_called_once_with(args=args)

    # Error Handling Tests

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error_code", "error_message"),
        [
            (APIErrorCode.RATE_LIMITED, "Rate limit exceeded"),
            (APIErrorCode.SYMBOL_NOT_FOUND, "Symbol not found"),
            (APIErrorCode.INSUFFICIENT_FUNDS, "Insufficient funds"),
        ],
    )
    async def test_error_propagation_from_services_parametrized(
        self,
        api_instance: Callable[[], HyperliquidAPI],
        mock_dependencies: dict[str, MagicMock],
        error_code: APIErrorCode,
        error_message: str,
    ) -> None:
        """Test that API errors are properly propagated from services."""
        api_error = APIError(message=error_message, code=error_code.value)
        mock_dependencies["market_data_service"].get_ticker.side_effect = api_error

        api = api_instance()

        with pytest.raises(APIError) as exc_info:
            await api.get_ticker(BTC_USD_HL.value)

        assert exc_info.value == api_error
        assert exc_info.value.message == error_message
        assert exc_info.value.code == error_code.value

    # Edge Cases Tests

    @pytest.mark.asyncio
    async def test_service_returns_none(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test API handles None responses from services correctly."""
        mock_dependencies["market_data_service"].get_ticker.return_value = None
        mock_dependencies["trading_service"].get_order.return_value = None

        api = api_instance()

        # Test that methods are called (focus on delegation)
        await api.get_ticker(BTC_USD_HL.value)
        await api.get_order(GetOrderArgs(order_id="nonexistent"))

        # Verify delegation occurred
        mock_dependencies["market_data_service"].get_ticker.assert_called_once()
        mock_dependencies["trading_service"].get_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_returns_empty_collections(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test API handles empty collections from services correctly."""
        api = api_instance()

        # Test that methods are called (focus on delegation)
        await api.get_balances()
        await api.get_positions()
        await api.get_markets(GetMarketsArgs())

        # Verify delegation occurred
        mock_dependencies["account_service"].get_balances.assert_called_once()
        mock_dependencies["account_service"].get_positions.assert_called_once()
        mock_dependencies["market_data_service"].get_markets.assert_called_once()

    # WebSocket Methods Tests (Public Interface Only)

    def test_websocket_methods_exist(self, api_instance: Callable[[], HyperliquidAPI]) -> None:
        """Test that WebSocket methods exist on public interface."""
        api = api_instance()

        # Verify WebSocket methods exist
        assert hasattr(api, "subscribe_to_order_book")
        assert hasattr(api, "subscribe_to_trades")
        assert hasattr(api, "subscribe_to_account_updates")
        assert hasattr(api, "subscribe_to_ticker")
        assert hasattr(api, "subscribe")

        # Verify they are callable
        assert callable(api.subscribe_to_order_book)
        assert callable(api.subscribe_to_trades)
        assert callable(api.subscribe_to_account_updates)
        assert callable(api.subscribe_to_ticker)
        assert callable(api.subscribe)

    # Initialization Tests

    def test_api_initialization_with_custom_dependencies(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test API initialization with custom dependencies."""
        custom_authenticator = MagicMock()
        custom_authenticator.wallet_address = "0xcustom"

        custom_http_client = MagicMock()
        custom_error_mapper = MagicMock()
        custom_account_service = MagicMock()
        custom_trading_service = MagicMock()
        custom_market_data_service = MagicMock()

        api = HyperliquidAPI(
            exchange_config=active_hl_config,
            exchange_secrets=active_hl_secrets,
            authenticator=custom_authenticator,
            error_mapper=custom_error_mapper,
            http_client=custom_http_client,
            account_service=custom_account_service,
            trading_service=custom_trading_service,
            market_data_service=custom_market_data_service,
        )

        # Verify custom dependencies are used
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

    def test_multiple_api_instances_are_independent(
        self, api_instance: Callable[[], HyperliquidAPI]
    ) -> None:
        """Test that multiple API instances are independent objects."""
        api1 = api_instance()
        api2 = api_instance()

        # Verify instances are different objects
        assert api1 is not api2

        # But have same exchange name
        assert api1.exchange_name == api2.exchange_name == "hyperliquid"

    # Service Method Coverage Tests

    @pytest.mark.asyncio
    async def test_additional_service_methods_delegation(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test delegation of additional service methods for coverage."""
        api = api_instance()

        # Test funding rates delegation
        funding_args = MagicMock()
        await api.get_funding_rates(funding_args)
        mock_dependencies["market_data_service"].get_funding_rates.assert_called_once_with(
            args=funding_args
        )

        # Test market data delegation
        market_data_args = MagicMock()
        await api.get_market_data(market_data_args)
        mock_dependencies["market_data_service"].get_market_data.assert_called_once_with(
            args=market_data_args
        )

    # Business Logic Validation Tests

    @pytest.mark.asyncio
    async def test_proper_argument_forwarding(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test that arguments are properly forwarded to services."""
        api = api_instance()

        # Test complex argument forwarding
        order_history_args = MagicMock()
        await api.get_order_history(order_history_args)
        mock_dependencies["account_service"].get_order_history.assert_called_once_with(
            args=order_history_args
        )

        # Test trade history forwarding
        trade_history_args = MagicMock()
        await api.get_trade_history(trade_history_args)
        mock_dependencies["account_service"].get_trade_history.assert_called_once_with(
            args=trade_history_args
        )

    @pytest.mark.asyncio
    async def test_service_delegation_consistency(
        self, api_instance: Callable[[], HyperliquidAPI], mock_dependencies: dict[str, MagicMock]
    ) -> None:
        """Test that service delegation is consistent across different methods."""
        api = api_instance()

        # Verify account service methods are consistently delegated
        await api.get_balances()
        await api.get_positions(None)
        await api.get_account_summary()

        assert mock_dependencies["account_service"].get_balances.call_count == 1
        assert mock_dependencies["account_service"].get_positions.call_count == 1
        assert mock_dependencies["account_service"].get_account_summary.call_count == 1

        # Verify trading service methods are consistently delegated
        await api.get_open_orders(None)
        await api.cancel_all_orders(None)

        assert mock_dependencies["trading_service"].get_open_orders.call_count == 1
        assert mock_dependencies["trading_service"].cancel_all_orders.call_count == 1

        # Verify market data service methods are consistently delegated
        await api.get_ticker(BTC_USD_HL.value)
        await api.get_recent_trades(BTC_USD_HL.value)

        assert mock_dependencies["market_data_service"].get_ticker.call_count == 1
        assert mock_dependencies["market_data_service"].get_recent_trades.call_count == 1
