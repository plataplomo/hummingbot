"""Unit tests for the Backpack WebSocket Router."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cyberdelta.apis.backpack.bp_ws_router import BackpackWebSocketRouter
from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackFillMapper
from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.base.infrastructure_config_domain import MemoryOptimizationMode
from cyberdelta.apis.websocket.error_handling.error_handler import WebSocketErrorHandler
from cyberdelta.apis.websocket.exceptions.stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.registry.registry_factory import WebSocketRegistryFactory
from cyberdelta.apis.websocket.ws_context_factory import WebSocketContextFactory


class TestBackpackWebSocketRouter:
    """Test BackpackWebSocketRouter with decomposed mappers."""

    @pytest.fixture
    def error_handler(self) -> MagicMock:
        """Create mock stream error handler.

        Returns:
            MagicMock: Mock WebSocketErrorHandler instance for testing.
        """
        return MagicMock(spec=WebSocketErrorHandler)

    @pytest.fixture
    def order_book_mapper(self) -> MagicMock:
        """Create mock order book mapper.

        Returns:
            MagicMock: Mock BackpackOrderBookMapper instance for testing.
        """
        return MagicMock(spec=BackpackOrderBookMapper)

    @pytest.fixture
    def ticker_mapper(self) -> MagicMock:
        """Create mock ticker mapper.

        Returns:
            MagicMock: Mock BackpackTickerMapper instance for testing.
        """
        return MagicMock(spec=BackpackTickerMapper)

    @pytest.fixture
    def trade_mapper(self) -> MagicMock:
        """Create mock trade mapper.

        Returns:
            MagicMock: Mock BackpackFillMapper instance for testing.
        """
        return MagicMock(spec=BackpackFillMapper)

    @pytest.fixture
    def balance_mapper(self) -> MagicMock:
        """Create mock balance mapper.

        Returns:
            MagicMock: Mock BackpackBalanceMapper instance for testing.
        """
        return MagicMock(spec=BackpackBalanceMapper)

    @pytest.fixture
    def position_mapper(self) -> MagicMock:
        """Create mock position mapper.

        Returns:
            MagicMock: Mock BackpackPositionMapper instance for testing.
        """
        return MagicMock(spec=BackpackPositionMapper)

    @pytest.fixture
    def order_mapper(self) -> MagicMock:
        """Create mock order mapper.

        Returns:
            MagicMock: Mock BackpackOrderMapper instance for testing.
        """
        return MagicMock(spec=BackpackOrderMapper)

    @pytest.fixture
    def transaction_mapper(self) -> MagicMock:
        """Create mock transaction mapper.

        Returns:
            MagicMock: Mock BackpackTransactionMapper instance for testing.
        """
        return MagicMock(spec=BackpackTransactionMapper)

    @pytest.fixture
    def router(
        self,
        error_handler: MagicMock,
        order_book_mapper: MagicMock,
        ticker_mapper: MagicMock,
        trade_mapper: MagicMock,
        balance_mapper: MagicMock,
        position_mapper: MagicMock,
        order_mapper: MagicMock,
        transaction_mapper: MagicMock,
    ) -> BackpackWebSocketRouter:
        """Create router for testing.

        Returns:
            BackpackWebSocketRouter: Configured router instance with mocked dependencies.
        """
        # Create context factory for testing
        registry = WebSocketRegistryFactory.create_registry()
        context_factory = WebSocketContextFactory(registry)

        return BackpackWebSocketRouter(
            stream_error_handler=error_handler,
            context_factory=context_factory,
            order_book_mapper=order_book_mapper,
            ticker_mapper=ticker_mapper,
            trade_mapper=trade_mapper,
            balance_mapper=balance_mapper,
            position_mapper=position_mapper,
            order_mapper=order_mapper,
            transaction_mapper=transaction_mapper,
            memory_optimization_mode=MemoryOptimizationMode.DISABLED,
            memory_pool_size=100,
        )

    def test_initialization(self, router: BackpackWebSocketRouter) -> None:
        """Test router initialization."""
        assert router.exchange_name == "backpack"
        assert len(router.processors) > 0  # Should have processors set up

        # Check that key processors are set up
        expected_processors = {"depth", "ticker", "trade", "trades"}
        for processor_key in expected_processors:
            assert processor_key in router.processors

    def test_construct_subscription_payload(self, router: BackpackWebSocketRouter) -> None:
        """Test subscription payload construction."""
        # Test ticker subscription
        payload = router.construct_subscription_payload("ticker.SOL_USDC")

        assert payload.method == "SUBSCRIBE"
        assert "ticker.SOL_USDC" in payload.params

    def test_construct_subscription_payload_invalid_topic(
        self, router: BackpackWebSocketRouter
    ) -> None:
        """Test subscription with invalid topic format."""
        # Test with invalid topic that doesn't contain dot
        with pytest.raises(WebSocketStreamError):
            router.construct_subscription_payload("invalid_topic")

    def test_processor_setup_completeness(self, router: BackpackWebSocketRouter) -> None:
        """Test that expected processors are properly set up."""
        # Key processors that should be present
        expected_processors = [
            "depth",
            "ticker",
            "trade",
            "trades",  # Compatibility alias
            "orders",
            "positionUpdate",
            "fills",
            "subscriptionResponse",
        ]

        for processor_key in expected_processors:
            assert processor_key in router.processors, f"Missing processor: {processor_key}"
            assert router.processors[processor_key] is not None
