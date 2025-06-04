"""Unit tests for HyperliquidAccountService order and trade history functionality."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import GetOrderHistoryArgs, GetTradeHistoryArgs

# Unit tests for HyperliquidAccountService (moved from mislabeled integration tests)
# These are unit tests because they mock all dependencies and test individual methods

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_account"]


class TestHyperliquidAccountServiceOrderTradeHistory:
    """Tests for the HyperliquidAccountService order and trade history functionality."""

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_trading_mapper: MagicMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test get_order_history returns mapped orders."""
        # wallet_address is already set by the hyperliquid_account_service fixture

        # Patch builder
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_order_history_payload.return_value = mock_payload_model
        # Patch http_client_requester
        mock_http_client_requester.return_value = ([{"order": 1}], 200, {})
        # Patch response handler
        mock_raw_order = MagicMock()
        mock_response_handler.handle_query_order_history_response.return_value = [
            MagicMock(order=mock_raw_order),
        ]
        # Patch order mapper - the trading mapper that actually maps orders
        mapped_order = MagicMock(symbol="BTC")
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.return_value = (
            mapped_order
        )
        result = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol="BTC",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
            ),
        )
        assert result == [mapped_order]

        expected_start_ms = int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        expected_end_ms = int(datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)

        mock_request_builder.build_order_history_payload.assert_called_once_with(
            wallet_address="0xTestWalletAddress",
            start_time_ms=expected_start_ms,
            end_time_ms=expected_end_ms,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data={"foo": "bar"},
            is_signed=True,
        )
        mock_response_handler.handle_query_order_history_response.assert_called_once_with(
            raw_response_content=[{"order": 1}],
            user_address="0xTestWalletAddress",
        )
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.assert_called_once_with(
            raw_historical_order=mock_raw_order,
            trigger=None,
        )

    @pytest.mark.asyncio
    async def test_get_order_history_symbol_filter(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_trading_mapper: MagicMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test get_order_history filters by symbol."""
        # Wallet address is already set by the hyperliquid_account_service fixture
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_order_history_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"order": 1}], 200, {})
        mock_raw_order1 = MagicMock(
            oid=1,
            cloid=None,
            asset="BTC",
            side="B",
            limit_px="10000.0",
            sz="0.001",
            timestamp=1672531200000,
            order_type={"limit": {"tif": "Gtc"}},
            reduce_only=False,
            remaining_sz="0.0",
            status="Filled",
            status_timestamp=1672531200000,
        )
        mock_raw_order2 = MagicMock(
            oid=2,
            cloid=None,
            asset="ETH",
            side="S",
            limit_px="2000.0",
            sz="0.01",
            timestamp=1672531201000,
            order_type={"limit": {"tif": "Gtc"}},
            reduce_only=False,
            remaining_sz="0.0",
            status="Filled",
            status_timestamp=1672531201000,
        )
        mock_response_handler.handle_query_order_history_response.return_value = [
            MagicMock(order=mock_raw_order1),
            MagicMock(order=mock_raw_order2),
        ]
        mapped_order1 = MagicMock(symbol="BTC")
        mapped_order2 = MagicMock(symbol="ETH")

        def map_side_effect(
            raw_historical_order: MagicMock,
            trigger: MagicMock | None = None,
        ) -> MagicMock:
            """Helper function for map side effect."""
            return mapped_order1 if raw_historical_order is mock_raw_order1 else mapped_order2

        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.side_effect = (
            map_side_effect
        )
        result = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol="BTC",
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
            ),
        )
        assert result == [mapped_order1]
        result_all = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol=None,
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
            ),
        )
        assert set(result_all) == {mapped_order1, mapped_order2}

    @pytest.mark.asyncio
    async def test_get_order_history_error_conditions(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_account_mapper: MagicMock,
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test get_order_history error handling for missing wallet, missing times, and APIError
        from requester.
        """
        # No wallet address case: Instantiate service with wallet_address=None
        service_no_wallet = HyperliquidAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test_no_wallet",
            wallet_address=None,  # Key change here
            account_mapper=mock_hl_account_mapper,
            trading_mapper=mock_hl_trading_mapper,
        )
        with pytest.raises(APIError) as excinfo_no_wallet:
            await service_no_wallet.get_order_history(
                GetOrderHistoryArgs(
                    symbol=None,
                    start_time=datetime(2024, 1, 1),
                    end_time=datetime(2024, 1, 2),
                ),
            )
        assert excinfo_no_wallet.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo_no_wallet.value.message

        # Missing times (uses the standard hyperliquid_account_service fixture
        # which has a wallet address)
        with pytest.raises(ValueError) as excinfo_no_times:
            await hyperliquid_account_service.get_order_history(
                GetOrderHistoryArgs(symbol=None, start_time=None, end_time=None),
            )
        assert "'start_time' is required" in str(excinfo_no_times.value)

        # APIError from requester (uses the standard hyperliquid_account_service
        # fixture which has a wallet address)
        mock_request_builder.build_order_history_payload.return_value = MagicMock(
            model_dump=lambda: {"foo": "bar"},
        )
        mock_http_client_requester.side_effect = APIError("fail", 1)
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_order_history(
                GetOrderHistoryArgs(
                    symbol=None,
                    start_time=datetime(2024, 1, 1),
                    end_time=datetime(2024, 1, 2),
                ),
            )

    @pytest.mark.asyncio
    async def test_get_order_history_http_client_returns_none(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_order_mapper: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_order_history when HTTP client returns None content."""
        wallet_address = "0xTestWalletAddress"  # Known fixture value
        symbol: str | None = None
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "queryOrderHistory",
            "user": wallet_address,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
        }

        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_request_builder.build_order_history_payload.return_value = mock_payload_model

        mock_http_client_requester.return_value = (
            None,
            200,
            MagicMock(),
        )  # HTTP client returns None

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_account_service.get_order_history(
                GetOrderHistoryArgs(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                ),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for order history" in exc_info.value.message

        mock_request_builder.build_order_history_payload.assert_called_once_with(
            wallet_address=wallet_address,
            start_time_ms=int(start_time.timestamp() * 1000),
            end_time_ms=int(end_time.timestamp() * 1000),
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_payload_dict,
            is_signed=True,
        )
        mock_response_handler.handle_query_order_history_response.assert_not_called()
        mock_hl_order_mapper.transform_raw_historical_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_trade_history_success(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_account_mapper: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history returns mapped trades."""
        # Wallet address is already set by the hyperliquid_account_service fixture
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"fill": 1}], 200, {})
        mock_raw_fill = MagicMock()
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=[mock_raw_fill],
        )
        mapped_trade = MagicMock(symbol="BTC")
        mock_hl_account_mapper.transform_raw_user_fill_to_internal.return_value = mapped_trade
        result = await hyperliquid_account_service.get_trade_history(
            args=GetTradeHistoryArgs(symbol="BTC"),
        )
        assert result == [mapped_trade]
        mock_hl_account_mapper.transform_raw_user_fill_to_internal.assert_called_once_with(
            mock_raw_fill,
        )

    @pytest.mark.asyncio
    async def test_get_trade_history_symbol_filter(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_account_mapper: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history filters by symbol."""
        # Wallet address is already set by the hyperliquid_account_service fixture
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"foo": "bar"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"fill": 1}], 200, {})
        mock_raw_fill1 = MagicMock(
            oid=1,
            cloid=None,
            asset="BTC",
            side="B",
            limit_px="10000.0",
            sz="0.001",
            timestamp=1672531200000,
            order_type={"limit": {"tif": "Gtc"}},
            reduce_only=False,
            remaining_sz="0.0",
            status="Filled",
            status_timestamp=1672531200000,
        )
        mock_raw_fill2 = MagicMock(
            oid=2,
            cloid=None,
            asset="ETH",
            side="S",
            limit_px="2000.0",
            sz="0.01",
            timestamp=1672531201000,
            order_type={"limit": {"tif": "Gtc"}},
            reduce_only=False,
            remaining_sz="0.0",
            status="Filled",
            status_timestamp=1672531201000,
        )
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=[mock_raw_fill1, mock_raw_fill2],
        )
        mapped_trade1 = MagicMock(symbol="BTC")
        mapped_trade2 = MagicMock(symbol="ETH")

        def map_side_effect_func(raw_fill_arg: MagicMock) -> MagicMock:
            """Helper function for map side effect func."""
            if raw_fill_arg is mock_raw_fill1:
                return mapped_trade1
            if raw_fill_arg is mock_raw_fill2:
                return mapped_trade2
            raise AssertionError(f"Unexpected raw_fill_arg: {raw_fill_arg}")

        mock_hl_account_mapper.transform_raw_user_fill_to_internal.side_effect = (
            map_side_effect_func
        )
        result = await hyperliquid_account_service.get_trade_history(
            args=GetTradeHistoryArgs(symbol="BTC"),
        )
        assert result == [mapped_trade1]
        mock_hl_account_mapper.transform_raw_user_fill_to_internal.assert_any_call(mock_raw_fill1)

        mock_hl_account_mapper.transform_raw_user_fill_to_internal.reset_mock()
        result_all = await hyperliquid_account_service.get_trade_history(
            args=GetTradeHistoryArgs(symbol=None),
        )
        assert set(result_all) == {mapped_trade1, mapped_trade2}
        mock_hl_account_mapper.transform_raw_user_fill_to_internal.assert_any_call(mock_raw_fill1)
        mock_hl_account_mapper.transform_raw_user_fill_to_internal.assert_any_call(mock_raw_fill2)

    @pytest.mark.asyncio
    async def test_get_trade_history_error_conditions(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_account_mapper: MagicMock,
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test get_trade_history error handling for missing wallet and APIError from requester."""
        # No wallet address case: Instantiate service with wallet_address=None
        service_no_wallet_trade_hist = HyperliquidAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test_no_wallet_trade_hist",
            wallet_address=None,  # Key: Instantiate with None
            account_mapper=mock_hl_account_mapper,
            trading_mapper=mock_hl_trading_mapper,
        )
        with pytest.raises(APIError) as excinfo_no_wallet:
            await service_no_wallet_trade_hist.get_trade_history(
                args=GetTradeHistoryArgs(symbol=None),
            )
        assert excinfo_no_wallet.value.code == APIErrorCode.INVALID_REQUEST.value
        assert "Wallet address is required" in excinfo_no_wallet.value.message

        # APIError from requester (uses the standard hyperliquid_account_service fixture which has
        # a wallet address)
        mock_request_builder.build_user_fills_request_payload.return_value = MagicMock(
            model_dump=lambda: {"foo": "bar"},
        )
        mock_http_client_requester.side_effect = APIError("fail", 1)
        with pytest.raises(APIError):
            await hyperliquid_account_service.get_trade_history(
                args=GetTradeHistoryArgs(symbol=None),
            )  # Uses the fixture

    @pytest.mark.asyncio
    async def test_get_trade_history_http_client_returns_none(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_account_mapper: MagicMock,
        mock_hl_user_fill_mapper: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_trade_history when HTTP client returns None content."""
        wallet_address = "0xTestWalletAddress"  # Known fixture value
        symbol: str | None = None

        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "userFills", "user": wallet_address}  # Simplified
        if symbol:
            mock_payload_dict["coin"] = symbol  # Actual payload might differ, adjust if needed

        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model

        mock_http_client_requester.return_value = (
            None,
            200,
            MagicMock(),
        )  # HTTP client returns None

        with pytest.raises(APIError) as exc_info:
            # Using symbol=None to match current build_user_fills_payload simplicity
            await hyperliquid_account_service.get_trade_history(
                args=GetTradeHistoryArgs(symbol=symbol),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for user fills, status: 200" in exc_info.value.message

        mock_request_builder.build_user_fills_request_payload.assert_called_once_with(
            wallet_address,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_payload_dict,
            is_signed=True,
        )
        mock_response_handler.handle_info_user_fills_response.assert_not_called()
        # Note: We expect the account mapper to not be called since HTTP client returned None

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_hl_order_mapper: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_open_orders when the specific HTTP client call for open orders returns None."""
        wallet_address = "0xTestWalletAddress"

        # Setup for the open_orders specific payload and HTTP call
        mock_open_orders_payload_model = MagicMock()
        mock_open_orders_payload_dict = {"type": "openOrders", "user": wallet_address}
        mock_open_orders_payload_model.model_dump.return_value = mock_open_orders_payload_dict

        mock_request_builder.build_open_orders_payload.return_value = mock_open_orders_payload_model

        mock_http_client_requester.return_value = (
            None,
            200,
            MagicMock(),
        )  # HTTP client returns None

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_account_service.get_open_orders()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for open orders, status: 200" in exc_info.value.message

        mock_request_builder.build_open_orders_payload.assert_called_once_with(
            wallet_address=wallet_address,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_open_orders_payload_dict,
            is_signed=True,
        )
        mock_response_handler.handle_query_open_orders_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_history_comprehensive_filtering_and_edge_cases(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test comprehensive order history filtering and edge cases."""
        # Setup basic mocks
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"test": "payload"}
        mock_request_builder.build_order_history_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = ([{"orders": "mock"}], 200, {})

        # Test case 1: Empty order history
        mock_response_handler.handle_query_order_history_response.return_value = []
        result_empty = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol="NONEXISTENT",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
            ),
        )
        assert result_empty == []

        # Test case 2: Multiple orders with complex filtering
        mock_orders = [
            MagicMock(order=MagicMock(asset="BTC")),
            MagicMock(order=MagicMock(asset="ETH")),
            MagicMock(order=MagicMock(asset="BTC")),
        ]
        mock_response_handler.handle_query_order_history_response.return_value = mock_orders

        mock_internal_orders = [
            MagicMock(symbol="BTC", id="order1"),
            MagicMock(symbol="ETH", id="order2"),
            MagicMock(symbol="BTC", id="order3"),
        ]
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.side_effect = (
            mock_internal_orders
        )

        # Test filtering by BTC symbol
        result_btc = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol="BTC",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
            ),
        )
        # Should return only BTC orders (filtered at the application level)
        btc_orders = [order for order in mock_internal_orders if order.symbol == "BTC"]
        assert set(result_btc) == set(btc_orders)

    @pytest.mark.asyncio
    async def test_get_trade_history_comprehensive_edge_cases(
        self,
        hyperliquid_account_service: HyperliquidAccountService,
        mock_request_builder: MagicMock,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_hl_account_mapper: MagicMock,
    ) -> None:
        """Test comprehensive trade history edge cases and validation."""
        # Setup basic mocks
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = {"test": "payload"}
        mock_request_builder.build_user_fills_request_payload.return_value = mock_payload_model

        # Test case 1: Empty trade history
        mock_http_client_requester.return_value = ([{"fills": []}], 200, {})
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(root=[])

        result_empty = await hyperliquid_account_service.get_trade_history(
            args=GetTradeHistoryArgs(symbol="NONEXISTENT"),
        )
        assert result_empty == []

        # Test case 2: Trade history with mapper errors for some trades
        # The service is designed to be resilient and skip invalid fills rather than
        # raising APIError
        mock_fills = [MagicMock(hash=f"fill_{i}") for i in range(3)]
        mock_response_handler.handle_info_user_fills_response.return_value = MagicMock(
            root=mock_fills,
        )

        # Configure mapper to succeed for some, fail for others
        def mapper_side_effect(fill: MagicMock) -> MagicMock:
            """Helper function for mapper side effect."""
            if fill.hash == "fill_1":
                raise ValueError("Invalid fill data")
            return MagicMock(symbol="BTC", id=fill.hash)

        mock_hl_account_mapper.transform_raw_user_fill_to_internal.side_effect = mapper_side_effect

        # The service should handle mapper errors gracefully by skipping invalid fills
        # and returning only the valid ones
        result = await hyperliquid_account_service.get_trade_history(
            args=GetTradeHistoryArgs(symbol=None),
        )

        # Should return 2 valid trades (fill_0 and fill_2), skipping fill_1 which raised an error
        assert len(result) == 2
        assert all(trade.symbol == "BTC" for trade in result)

        # Verify the mapper was called for all fills
        assert mock_hl_account_mapper.transform_raw_user_fill_to_internal.call_count == 3
