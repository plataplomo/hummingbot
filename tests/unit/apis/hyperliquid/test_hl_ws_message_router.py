"""Unit tests for HyperliquidWsMessageRouter.

Tests the WebSocket message routing logic in isolation with mocked dependencies.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, MessageHandler, TransformationError
from cyberdelta.apis.hyperliquid.hl_ws_message_router import HyperliquidWsMessageRouter
from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import HyperliquidRawWsSubscribeRequest


class TestHyperliquidWsMessageRouter:
    """Test suite for HyperliquidWsMessageRouter."""

    @pytest.fixture
    def mock_market_data_mapper(self) -> Mock:
        """Create a mock market data mapper.

        Returns:
            Mock HyperliquidMarketDataMapper for testing.
        """
        mapper = Mock(spec=HyperliquidMarketDataMapper)
        mapper.transform_ws_book_update_to_internal = Mock(return_value=Mock())
        mapper.transform_ws_trade_event_to_internal = Mock(return_value=Mock())
        return mapper

    @pytest.fixture
    def mock_account_data_mapper(self) -> Mock:
        """Create a mock account data mapper.

        Returns:
            Mock HyperliquidAccountDataMapper for testing.
        """
        mapper = Mock(spec=HyperliquidAccountDataMapper)
        mapper.transform_ws_fill_event_to_internal = Mock(return_value=Mock())
        mapper.transform_ws_position_update_to_internal_position = Mock(return_value=Mock())
        return mapper

    @pytest.fixture
    def mock_trading_data_mapper(self) -> Mock:
        """Create a mock trading data mapper.

        Returns:
            Mock HyperliquidTradingDataMapper for testing.
        """
        mapper = Mock(spec=HyperliquidTradingDataMapper)
        mapper.transform_ws_order_update_to_internal_order = Mock(return_value=Mock())
        return mapper

    @pytest.fixture
    def mock_raw_ws_handler(self) -> Mock:
        """Create a mock raw WebSocket message handler.

        Returns:
            Mock HyperliquidWsRawMessageHandler for testing.
        """
        handler = Mock(spec=HyperliquidWsRawMessageHandler)
        handler.handle_l2book_payload = Mock(return_value=Mock())
        handler.handle_public_trades_payload = Mock(return_value=[Mock()])
        handler.handle_user_fill_event_payload = Mock(return_value=Mock())
        handler.handle_user_order_update_wrapper_payload = Mock(return_value=Mock(data=Mock()))
        handler.handle_user_order_event_payload = Mock(return_value=Mock())
        handler.handle_user_position_update_event_payload = Mock(return_value=Mock())
        handler.handle_all_mids_payload = Mock(return_value=Mock(model_dump=Mock(return_value={})))
        return handler

    @pytest.fixture
    def router(
        self,
        mock_market_data_mapper: Mock,
        mock_account_data_mapper: Mock,
        mock_trading_data_mapper: Mock,
        mock_raw_ws_handler: Mock,
    ) -> HyperliquidWsMessageRouter:
        """Create a HyperliquidWsMessageRouter instance with mocked dependencies.

        Returns:
            HyperliquidWsMessageRouter instance with mock dependencies for testing.
        """
        return HyperliquidWsMessageRouter(
            market_data_mapper=mock_market_data_mapper,
            account_data_mapper=mock_account_data_mapper,
            trading_data_mapper=mock_trading_data_mapper,
            raw_ws_handler=mock_raw_ws_handler,
            exchange_name="Hyperliquid",
        )

    @pytest.fixture
    def mock_app_handler(self) -> AsyncMock:
        """Create a mock application handler.

        Returns:
            AsyncMock: Mock MessageHandler for testing message routing.
        """
        return AsyncMock(spec=MessageHandler)

    def test_init(self, router: HyperliquidWsMessageRouter) -> None:
        """Test router initialization."""
        assert router is not None
        assert hasattr(router, "logger")

    def test_construct_subscription_payload_l2book(
        self,
        router: HyperliquidWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for l2Book."""
        result = router.construct_subscription_payload("l2Book:SOL", None)

        # Verify the result is a HyperliquidRawWsSubscribeRequest
        assert isinstance(result, HyperliquidRawWsSubscribeRequest)
        assert result.method == "subscribe"
        assert result.subscription.type == "l2Book"
        assert result.subscription.coin == "SOL"

    def test_construct_subscription_payload_trades(
        self,
        router: HyperliquidWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for trades."""
        result = router.construct_subscription_payload("trades:BTC", None)

        assert isinstance(result, HyperliquidRawWsSubscribeRequest)
        assert result.method == "subscribe"
        assert result.subscription.type == "trades"
        assert result.subscription.coin == "BTC"

    def test_construct_subscription_payload_user_events(
        self,
        router: HyperliquidWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for userEvents."""
        wallet_address = "0x1234567890abcdef"
        result = router.construct_subscription_payload("userEvents", wallet_address)

        assert isinstance(result, HyperliquidRawWsSubscribeRequest)
        assert result.method == "subscribe"
        assert result.subscription.type == "userEvents"
        assert result.subscription.user == wallet_address

    def test_construct_subscription_payload_user_events_no_wallet(
        self,
        router: HyperliquidWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for userEvents without wallet address."""
        with pytest.raises(
            ValueError,
            match="Cannot subscribe to userEvents without wallet address",
        ):
            router.construct_subscription_payload("userEvents", None)

    def test_construct_subscription_payload_candle(
        self,
        router: HyperliquidWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for candle."""
        result = router.construct_subscription_payload("candle:ETH:1m", None)

        assert isinstance(result, HyperliquidRawWsSubscribeRequest)
        assert result.method == "subscribe"
        assert result.subscription.type == "candle"
        assert result.subscription.coin == "ETH"
        assert result.subscription.interval == "1m"

    def test_construct_subscription_payload_invalid_topic(
        self,
        router: HyperliquidWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for invalid topic."""
        with pytest.raises(APIError, match="Unsupported WebSocket topic"):
            router.construct_subscription_payload("invalid_topic", None)

    @pytest.mark.asyncio
    async def test_route_message_l2book(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing l2Book messages."""
        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {"coin": "SOL", "levels": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book:SOL": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_l2book_payload.assert_called_once_with(
            {"coin": "SOL", "levels": []},
        )
        mock_market_data_mapper.transform_ws_book_update_to_internal.assert_called_once()
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_trades(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing trades messages."""
        message: dict[str, Any] = {
            "channel": "trades",
            "data": [{"coin": "BTC", "px": "50000", "sz": "1.0"}],
        }
        ws_handlers: dict[str, MessageHandler] = {"trades:BTC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_public_trades_payload.assert_called_once_with(
            [{"coin": "BTC", "px": "50000", "sz": "1.0"}],
        )
        mock_market_data_mapper.transform_ws_trade_event_to_internal.assert_called_once()
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_user_events_fill(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_account_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing userEvents fill messages."""
        message: dict[str, Any] = {
            "channel": "userEvents",
            "data": [{"type": "fill", "fillData": {"coin": "SOL", "px": "100"}}],
        }
        ws_handlers: dict[str, MessageHandler] = {"userEvents": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_user_fill_event_payload.assert_called_once()
        mock_account_data_mapper.transform_ws_fill_event_to_internal.assert_called_once()
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_user_events_order(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_trading_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing userEvents order messages."""
        message: dict[str, Any] = {
            "channel": "userEvents",
            "data": [{"type": "order", "orderData": {"oid": "123", "status": "filled"}}],
        }
        ws_handlers: dict[str, MessageHandler] = {"userEvents": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_user_order_update_wrapper_payload.assert_called_once()
        mock_raw_ws_handler.handle_user_order_event_payload.assert_called_once()
        mock_trading_data_mapper.transform_ws_order_update_to_internal_order.assert_called_once()
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_user_events_position_update(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_account_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing userEvents positionUpdate messages."""
        message: dict[str, Any] = {
            "channel": "userEvents",
            "data": [{"type": "positionUpdate", "positionData": {"coin": "SOL", "szi": "10"}}],
        }
        ws_handlers: dict[str, MessageHandler] = {"userEvents": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_user_position_update_event_payload.assert_called_once()
        pos_transform = mock_account_data_mapper.transform_ws_position_update_to_internal_position
        pos_transform.assert_called_once()
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_all_mids(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing allMids messages."""
        message: dict[str, Any] = {
            "channel": "allMids",
            "data": {"mids": {"SOL": "100.5", "BTC": "50000"}},
        }
        ws_handlers: dict[str, MessageHandler] = {"allMids": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_all_mids_payload.assert_called_once_with(
            {"mids": {"SOL": "100.5", "BTC": "50000"}},
        )
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_control_messages(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing control messages (pong, subscriptionResponse)."""
        with patch.object(router, "logger") as mock_logger:
            for channel in ["pong", "subscriptionResponse"]:
                message: dict[str, Any] = {"channel": channel, "data": {"status": "ok"}}
                ws_handlers: dict[str, MessageHandler] = {channel: mock_app_handler}

                await router.route_message(message, ws_handlers)

            # Should be called twice (once for each control message)
            # Business logic uses logger.trace() for control messages, not debug()
            assert mock_logger.trace.call_count == 2

            # Check that structured logging calls were made
            call_args_list = mock_logger.trace.call_args_list

            # Find calls for both channels
            pong_calls = [call for call in call_args_list if call[1].get("channel") == "pong"]
            subscription_calls = [
                call for call in call_args_list if call[1].get("channel") == "subscriptionResponse"
            ]

            assert len(pong_calls) == 1, "Expected one pong control message log"
            assert len(subscription_calls) == 1, (
                "Expected one subscriptionResponse control message log"
            )

            # Check the structure of the calls
            pong_call = pong_calls[0]
            assert pong_call[0][0] == "control_message_received"
            assert pong_call[1]["action"] == "handle_control_message"
            assert pong_call[1]["exchange"] == "Hyperliquid"
            assert pong_call[1]["channel"] == "pong"

            subscription_call = subscription_calls[0]
            assert subscription_call[0][0] == "control_message_received"
            assert subscription_call[1]["action"] == "handle_control_message"
            assert subscription_call[1]["exchange"] == "Hyperliquid"
            assert subscription_call[1]["channel"] == "subscriptionResponse"
        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_no_channel(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing message with no channel - should return early."""
        message: dict[str, Any] = {"data": {"some": "data"}}
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_no_data(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing message with no data - should return early."""
        message: dict[str, Any] = {"channel": "l2Book"}
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_no_handler_registered(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing message with no registered handler - should return early."""
        message: dict[str, Any] = {
            "channel": "unknown_channel",
            "data": {"some": "data"},
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_api_error_in_raw_handler(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling APIError from raw message handler."""
        mock_raw_ws_handler.handle_l2book_payload.side_effect = APIError(
            "Invalid l2Book data",
            code="INVALID_DATA",
        )

        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {"invalid": "data"},
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        # Handler should not be called due to error
        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_transformation_error_in_mapper(
        self,
        router: HyperliquidWsMessageRouter,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling TransformationError from mapper."""
        mock_market_data_mapper.transform_ws_book_update_to_internal.side_effect = (
            TransformationError("Failed to transform book data")
        )

        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {"coin": "SOL", "levels": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        # Handler should not be called due to transformation error
        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_validation_error_in_user_events(
        self,
        router: HyperliquidWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling ValidationError in userEvents processing."""
        # Construct a valid ValidationError instance for Pydantic v2
        validation_error = ValidationError.from_exception_data(
            title="HyperliquidRawUserFillEvent",
            line_errors=[
                {
                    "type": "value_error",  # A standard Pydantic error type string
                    "loc": ("fillData",),  # Location of the error
                    "input": {"invalid": "data"},  # The input data causing the error
                    # No 'msg' here, Pydantic generates it. Add 'ctx' if needed for the error type.
                    "ctx": {"error": "Simulated value error"},  # Added context for value_error
                },
            ],
        )
        mock_raw_ws_handler.handle_user_fill_event_payload.side_effect = validation_error

        message: dict[str, Any] = {
            "channel": "userEvents",
            "data": [{"type": "fill", "invalid": "data"}],
        }
        ws_handlers: dict[str, MessageHandler] = {"userEvents": mock_app_handler}

        # Should not raise exception, just log and continue
        await router.route_message(message, ws_handlers)

        # Handler should not be called for the invalid item
        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_exception_in_app_handler(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling exception in application handler."""
        mock_app_handler.side_effect = Exception("Handler failed")

        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {"coin": "SOL", "levels": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        # Should not raise exception, just log it
        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_topic_key_derivation(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test correct topic key derivation for handler lookup."""
        # Test l2Book topic key derivation
        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {"coin": "SOL", "levels": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book:SOL": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_called_once()
        mock_app_handler.reset_mock()

        # Test trades topic key derivation
        message = {
            "channel": "trades",
            "data": [{"coin": "BTC", "px": "50000"}],
        }
        ws_handlers = {"trades:BTC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_fallback_to_base_handler(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test fallback to base channel handler when specific topic handler not found."""
        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {"coin": "SOL", "levels": []},
        }
        # Only register base channel handler
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_invalid_data_types(
        self,
        router: HyperliquidWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling of invalid data types in messages."""
        # Test l2Book with non-dict data
        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": "invalid_data_type",
        }
        ws_handlers: dict[str, MessageHandler] = {"l2Book": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()
        mock_app_handler.reset_mock()

        # Test trades with non-list data
        message = {
            "channel": "trades",
            "data": {"not": "a_list"},
        }
        ws_handlers = {"trades": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()
        mock_app_handler.reset_mock()

        # Test userEvents with non-list data
        message = {
            "channel": "userEvents",
            "data": "not_a_list",
        }
        ws_handlers = {"userEvents": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()
