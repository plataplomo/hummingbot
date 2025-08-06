"""Unit tests for Hyperliquid Account Service order and trade history operations.

Tests the main HyperliquidAccountService's delegation to specialized services.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderData,
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.models.service_args.trading import GetOrderHistoryArgs, GetTradeHistoryArgs
from cyberdelta.enums import ExchangeName
from cyberdelta.models import Fill, Order, OrderSide, OrderStatus, OrderType, TimeInForce
from tests.common_symbols import ETH_HL


class TestHyperliquidAccountServiceOrderTradeHistory:
    """Tests for the HyperliquidAccountService order and trade history functionality."""

    def create_account_service(self) -> HyperliquidAccountService:
        """Create a minimal account service for testing.

        Returns:
            HyperliquidAccountService: An account service instance for testing.
        """
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        return HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=MagicMock(),
            exchange_name="hyperliquid_test",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )

    @pytest.mark.asyncio
    async def test_get_order_history_success(self) -> None:
        """Test get_order_history successfully retrieves and processes order history data."""
        # Create test arguments
        args = GetOrderHistoryArgs(
            symbol=ETH_HL,
            start_time=datetime(2023, 1, 1, tzinfo=UTC),
            end_time=datetime(2023, 1, 2, tzinfo=UTC),
        )

        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()
        mock_authenticator = MagicMock()

        # Mock HTTP response data that matches the expected API format
        mock_raw_api_response = [
            {
                "order": {
                    "coin": "ETH",
                    "side": "B",
                    "sz": "10.0",
                    "limitPx": "2000.0",
                    "oid": 12345,
                    "timestamp": 1672574400000,
                    "orderType": "Limit",
                    "reduceOnly": False,
                    "origSz": "10.0",
                    "tif": "Gtc",
                },
                "status": "filled",
                "statusTimestamp": 1672574500000,
            }
        ]

        # Mock response from response handler (processed data)

        mock_processed_response = [
            HyperliquidRawHistoricalOrderResponse(
                order=HyperliquidRawHistoricalOrderData(
                    coin="ETH",
                    side="B",
                    sz="10.0",
                    limitPx="2000.0",
                    oid=12345,
                    timestamp=1672574400000,
                    orderType="Limit",
                    reduceOnly=False,
                    origSz="10.0",
                    tif="Gtc",
                    cloid=None,
                    triggerCondition=None,
                    isTrigger=None,
                    triggerPx=None,
                    children=None,
                    isPositionTpsl=None,
                ),
                status="filled",
                statusTimestamp=1672574500000,
            )
        ]

        # Mock the internal Order object that should be returned by the mapper

        mock_internal_order = Order(
            exchange_order_id="12345",
            symbol=ETH_HL,
            side=OrderSide.BUY,
            quantity_requested=Decimal("10.0"),
            price=Decimal("2000.0"),
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.FILLED,
            created_at=datetime(2023, 1, 1, 0, 0, tzinfo=UTC),
            exchange=ExchangeName.HYPERLIQUID,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the mocks
        mock_requester.return_value = (mock_raw_api_response, 200, {})
        mock_builder.build_historical_orders_payload.return_value = {
            "type": "historicalOrders",
            "user": "0x1234567890abcdef1234567890abcdef12345678",
        }
        mock_handler.handle_historical_orders_response.return_value = mock_processed_response

        # Mock the order mapper to return internal Order
        mock_order_mapper = MagicMock()
        mock_order_mapper.transform_raw_historical_order_to_internal.return_value = (
            mock_internal_order
        )

        # Create service with mocked dependencies
        service = HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            order_mapper=mock_order_mapper,
        )

        # Call the service method
        result = await service.get_order_history(args)

        # Verify the HTTP request was made correctly
        mock_requester.assert_called_once()
        call_args = mock_requester.call_args
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/info"

        # Verify the builder was called correctly
        mock_builder.build_historical_orders_payload.assert_called_once_with(
            "0x1234567890abcdef1234567890abcdef12345678"
        )

        # Verify the handler was called correctly
        mock_handler.handle_historical_orders_response.assert_called_once()

        # Verify result structure (we test the public behavior, not exact values)
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], Order)
        assert result[0].symbol == ETH_HL

    @pytest.mark.asyncio
    async def test_get_order_history_none_response_returns_empty_list(self) -> None:
        """Test get_order_history returns empty list when API response is None."""
        # Create test arguments
        args = GetOrderHistoryArgs(
            symbol=ETH_HL,
            start_time=datetime(2023, 1, 1, tzinfo=UTC),
            end_time=datetime(2023, 1, 2, tzinfo=UTC),
        )

        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock None response - this should return empty list per business logic
        mock_requester.return_value = (None, 200, {})
        mock_builder.build_historical_orders_payload.return_value = {
            "type": "historicalOrders",
            "user": "0x1234567890abcdef1234567890abcdef12345678",
        }

        # Create service with mocked dependencies
        service = HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=MagicMock(),
            exchange_name="hyperliquid_test",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )

        # Call the service method
        result = await service.get_order_history(args)

        # Verify that None response returns empty list (per business logic)
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_order_history_missing_authenticator_raises_error(self) -> None:
        """Test get_order_history raises error when authenticator is missing."""
        # Create test arguments
        args = GetOrderHistoryArgs(
            symbol=ETH_HL,
            start_time=datetime(2023, 1, 1, tzinfo=UTC),
            end_time=datetime(2023, 1, 2, tzinfo=UTC),
        )

        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Create service with no authenticator (None) - this should raise APIError
        service = HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=None,  # Missing authenticator should cause error
            exchange_name="hyperliquid_test",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )

        # Call and expect error
        with pytest.raises(APIError) as exc_info:
            await service.get_order_history(args)

        assert "Authentication required" in str(exc_info.value.message)

    @pytest.mark.asyncio
    async def test_get_fill_history_success(self) -> None:
        """Test get_fill_history successfully retrieves and processes trade history data."""
        # Create test arguments
        args = GetTradeHistoryArgs(
            symbol=ETH_HL,
            limit=50,
        )

        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()
        mock_authenticator = MagicMock()

        # Mock HTTP response data that would come from the API
        # Note: Hyperliquid's userFills endpoint returns a list directly
        mock_trade_history_response = [
            {
                "tid": 123456,
                "time": 1672574400000,
                "coin": "ETH",
                "side": "B",
                "sz": "5.0",
                "px": "2000.0",
                "fee": "1.0",
                "oid": 789012,
                "startPosition": "0.0",
                "dir": "Open Long",
                "hash": "0x1234567890abcdef",
                "isMaker": True,
            }
        ]

        # Mock the raw response from response handler (processed data)

        # The HyperliquidRawUserFillsResponse expects a list of HyperliquidRawUserFill objects
        mock_processed_response = HyperliquidRawUserFillsResponse([
            HyperliquidRawUserFill(
                tid=123456,
                time=1672574400000,
                coin="ETH",
                side="B",
                sz="5.0",
                px="2000.0",
                fee="1.0",
                oid=789012,
                startPosition="0.0",
                dir="Open Long",
                hash="0x1234567890abcdef",
                isMaker=True,
                liquidationMarkPx=None,
                cloid=None,
            )
        ])

        # Mock the internal Trade object that should be returned by the mapper

        mock_internal_trade = Fill(
            id="123456",
            symbol=ETH_HL,
            executed_at=datetime(2023, 1, 1, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="789012",
            exchange=ExchangeName.HYPERLIQUID,
            price=Decimal("2000.0"),
            quantity=Decimal("5.0"),
            fee=Decimal("1.0"),
            fee_asset="USD",
            is_maker=True,
        )

        # Configure the mocks
        mock_requester.return_value = (mock_trade_history_response, 200, {})
        mock_builder.build_user_fills_request_payload.return_value = {"type": "userFills"}
        mock_handler.handle_info_user_fills_response.return_value = mock_processed_response

        # Mock the transaction mapper to return internal Trade
        mock_transaction_mapper = MagicMock()
        mock_transaction_mapper.transform_raw_user_fill_to_internal.return_value = (
            mock_internal_trade
        )

        # Create service with mocked dependencies
        service = HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            transaction_mapper=mock_transaction_mapper,
        )

        # Create trade history service mock
        mock_trade_history_service = MagicMock()
        mock_trade_history_service.get_fill_history = AsyncMock(return_value=[mock_internal_trade])

        # Patch the trade history service creation
        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_account_service.HyperliquidTradeHistoryService",
            return_value=mock_trade_history_service,
        ):
            # Re-create the service so it gets the mocked trade history service
            service = HyperliquidAccountService(
                http_client_requester=mock_requester,
                request_builder=mock_builder,
                response_handler=mock_handler,
                authenticator=mock_authenticator,
                exchange_name="hyperliquid_test",
                wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                transaction_mapper=mock_transaction_mapper,
            )

            # Call the service method
            result = await service.get_fill_history(args)

        # Verify the mock trade history service was called correctly
        mock_trade_history_service.get_fill_history.assert_called_once_with(args)

        # Verify result structure (we test the public behavior, not exact values)
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], Fill)
        assert result[0].symbol == ETH_HL

    @pytest.mark.asyncio
    async def test_get_fill_history_none_response_raises_error(self) -> None:
        """Test get_fill_history raises error when API response is None."""
        # Create test arguments
        args = GetTradeHistoryArgs(
            symbol=ETH_HL,
            limit=50,
        )

        # Create mocks for dependencies
        mock_requester = AsyncMock()
        mock_builder = MagicMock()
        mock_handler = MagicMock()

        # Mock None response to trigger error
        mock_requester.return_value = (None, 200, {})
        mock_builder.build_user_fills_request_payload.return_value = {"type": "userFills"}

        # Create service with mocked dependencies
        service = HyperliquidAccountService(
            http_client_requester=mock_requester,
            request_builder=mock_builder,
            response_handler=mock_handler,
            authenticator=MagicMock(),
            exchange_name="hyperliquid_test",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )

        # Call and expect error (trade history service validates None responses as errors)
        with pytest.raises(APIError) as exc_info:
            await service.get_fill_history(args)

        assert "No data received" in str(exc_info.value.message)
