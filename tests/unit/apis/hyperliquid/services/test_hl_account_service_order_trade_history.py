"""Unit tests for HyperliquidAccountService order and trade history functionality."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetOpenOrdersArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    GetUserFillsArgs,
)


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
        mock_request_builder.build_historical_orders_payload.return_value = mock_payload_model
        # Patch http_client_requester
        mock_http_client_requester.return_value = (
            [
                {
                    "order": {
                        "oid": 1,
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "30000.0",
                        "sz": "0.001",
                        "timestamp": 1704067200000,
                        "orderType": "limit",
                        "origSz": "0.001",
                        "cloid": None,
                        "reduceOnly": False,
                        "tif": "Gtc",
                    },
                    "status": "filled",
                    "statusTimestamp": 1704067200000,
                },
            ],
            200,
            {},
        )
        # Patch response handler
        mock_raw_order = MagicMock()
        # Make model_dump return the expected dict structure
        mock_raw_order.model_dump.return_value = {
            "oid": 1,
            "coin": "BTC",
            "side": "B",
            "limitPx": "30000.0",
            "sz": "0.001",
            "timestamp": 1704067200000,
            "orderType": "limit",
            "origSz": "0.001",
            "cloid": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }
        mock_response_handler.handle_historical_orders_response.return_value = [
            MagicMock(order=mock_raw_order, status="filled", status_timestamp=1704067200000),
        ]
        # Patch order mapper - the trading mapper that actually maps orders
        mapped_order = MagicMock(symbol="BTC", created_at=datetime(2024, 1, 1, 12, 0, tzinfo=UTC))
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

        # Business logic only passes wallet_address to build_historical_orders_payload
        mock_request_builder.build_historical_orders_payload.assert_called_once_with(
            wallet_address="0xTestWalletAddress",
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data={"foo": "bar"},
            is_signed=False,  # Business logic uses is_signed=False for info endpoints
        )
        mock_response_handler.handle_historical_orders_response.assert_called_once_with(
            raw_response_content=[
                {
                    "order": {
                        "oid": 1,
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "30000.0",
                        "sz": "0.001",
                        "timestamp": 1704067200000,
                        "orderType": "limit",
                        "origSz": "0.001",
                        "cloid": None,
                        "reduceOnly": False,
                        "tif": "Gtc",
                    },
                    "status": "filled",
                    "statusTimestamp": 1704067200000,
                },
            ],
            user_address="0xTestWalletAddress",
        )
        # Verify the mapper was called once with a HyperliquidRawHistoricalOrder object
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.assert_called_once()
        call_args = mock_hl_trading_mapper.transform_raw_historical_order_to_internal.call_args

        # The raw_historical_order should be a HyperliquidRawHistoricalOrder instance
        # with the combined data
        raw_order_arg = call_args.kwargs.get("raw_historical_order") or call_args.args[0]
        assert raw_order_arg.oid == 1
        assert raw_order_arg.coin == "BTC"
        assert raw_order_arg.order_type == "limit"
        assert raw_order_arg.status == "filled"
        assert call_args.kwargs.get("trigger") is None or (
            len(call_args.args) > 1 and call_args.args[1] is None
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
        mock_request_builder.build_historical_orders_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = (
            [
                {
                    "order": {
                        "oid": 1,
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "30000.0",
                        "sz": "0.001",
                        "timestamp": 1704067200000,
                        "orderType": "limit",
                        "origSz": "0.001",
                        "cloid": None,
                        "reduceOnly": False,
                        "tif": "Gtc",
                    },
                    "status": "filled",
                    "statusTimestamp": 1704067200000,
                },
                {
                    "order": {
                        "oid": 2,
                        "coin": "ETH",
                        "side": "A",
                        "limitPx": "2000.0",
                        "sz": "0.01",
                        "timestamp": 1704067201000,
                        "orderType": "limit",
                        "origSz": "0.01",
                        "cloid": None,
                        "reduceOnly": False,
                        "tif": "Gtc",
                    },
                    "status": "filled",
                    "statusTimestamp": 1704067201000,
                },
            ],
            200,
            {},
        )
        mock_raw_order1 = MagicMock(
            oid=1,
            cloid=None,
            coin="BTC",
            side="B",
            limit_px="10000.0",
            sz="0.001",
            timestamp=1672531200000,
            order_type="limit",
            reduce_only=False,
            orig_sz="0.001",
            tif="Gtc",
        )
        mock_raw_order1.model_dump.return_value = {
            "oid": 1,
            "coin": "BTC",
            "side": "B",
            "limitPx": "10000.0",
            "sz": "0.001",
            "timestamp": 1672531200000,
            "orderType": "limit",
            "origSz": "0.001",
            "cloid": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }
        mock_raw_order2 = MagicMock(
            oid=2,
            cloid=None,
            coin="ETH",
            side="A",
            limit_px="2000.0",
            sz="0.01",
            timestamp=1672531201000,
            order_type="limit",
            reduce_only=False,
            orig_sz="0.01",
            tif="Gtc",
        )
        mock_raw_order2.model_dump.return_value = {
            "oid": 2,
            "coin": "ETH",
            "side": "A",
            "limitPx": "2000.0",
            "sz": "0.01",
            "timestamp": 1672531201000,
            "orderType": "limit",
            "origSz": "0.01",
            "cloid": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }
        mock_response_handler.handle_historical_orders_response.return_value = [
            MagicMock(order=mock_raw_order1, status="filled", status_timestamp=1672531200000),
            MagicMock(order=mock_raw_order2, status="filled", status_timestamp=1672531201000),
        ]
        mapped_order1 = MagicMock(symbol="BTC", created_at=datetime(2023, 1, 1, 0, 0, tzinfo=UTC))
        mapped_order2 = MagicMock(
            symbol="ETH",
            created_at=datetime(2023, 1, 1, 0, 0, 1, tzinfo=UTC),
        )

        def map_side_effect(
            raw_historical_order: MagicMock,
            trigger: MagicMock | None = None,
        ) -> MagicMock:
            """Map raw historical orders to internal order objects for testing."""
            # Match based on the oid property since the business logic creates new objects
            if hasattr(raw_historical_order, "oid") and raw_historical_order.oid == 1:
                return mapped_order1
            if hasattr(raw_historical_order, "oid") and raw_historical_order.oid == 2:
                return mapped_order2
            # Default fallback - check coin field
            if hasattr(raw_historical_order, "coin") and raw_historical_order.coin == "BTC":
                return mapped_order1
            return mapped_order2

        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.side_effect = (
            map_side_effect
        )
        result = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol="BTC",
                start_time=datetime(2023, 1, 1),
                end_time=datetime(2023, 1, 2),
            ),
        )
        assert result == [mapped_order1]
        result_all = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol=None,
                start_time=datetime(2023, 1, 1),
                end_time=datetime(2023, 1, 2),
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
        """Test get_order_history error handling for missing wallet, missing times, and APIError.

        Tests error handling when wallet address is missing, time parameters are invalid,
        and when the underlying HTTP requester raises APIError exceptions.
        """
        # Create mock for get_asset_index_callable
        mock_get_asset_index = AsyncMock(return_value=0)

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
            get_asset_index_callable=mock_get_asset_index,
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
        mock_request_builder.build_historical_orders_payload.return_value = MagicMock(
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
        mock_request_builder.build_historical_orders_payload.return_value = mock_payload_model

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

        mock_request_builder.build_historical_orders_payload.assert_called_once_with(
            wallet_address=wallet_address,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_payload_dict,
            is_signed=False,
        )
        mock_response_handler.handle_historical_orders_response.assert_not_called()
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

        expected_args = GetUserFillsArgs(wallet_address="0xTestWalletAddress")
        mock_request_builder.build_user_fills_request_payload.assert_called_once_with(expected_args)

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
            order_type="limit",
            reduce_only=False,
            remaining_sz="0.0",
            status="Filled",
            status_timestamp=1672531200000,
        )
        mock_raw_fill2 = MagicMock(
            oid=2,
            cloid=None,
            asset="ETH",
            side="A",
            limit_px="2000.0",
            sz="0.01",
            timestamp=1672531201000,
            order_type="limit",
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
            """Map raw fill objects to internal trade objects for testing."""
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
        # Create mock for get_asset_index_callable
        mock_get_asset_index = AsyncMock(return_value=0)

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
            get_asset_index_callable=mock_get_asset_index,
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

        expected_args = GetUserFillsArgs(wallet_address=wallet_address)
        mock_request_builder.build_user_fills_request_payload.assert_called_once_with(expected_args)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_payload_dict,
            is_signed=False,
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
        assert "No data received for open orders" in exc_info.value.message

        expected_args = GetOpenOrdersArgs(wallet_address=wallet_address)
        mock_request_builder.build_open_orders_payload.assert_called_once_with(expected_args)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_open_orders_payload_dict,
            is_signed=False,
        )
        mock_response_handler.handle_info_open_orders_response.assert_not_called()

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
        mock_request_builder.build_historical_orders_payload.return_value = mock_payload_model
        mock_http_client_requester.return_value = (
            [
                {
                    "order": {
                        "oid": 1,
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "30000.0",
                        "sz": "0.001",
                        "timestamp": 1704067200000,
                        "orderType": "limit",
                        "origSz": "0.001",
                        "cloid": None,
                        "reduceOnly": False,
                        "tif": "Gtc",
                    },
                    "status": "filled",
                    "statusTimestamp": 1704067200000,
                },
            ],
            200,
            {},
        )

        # Test case 1: Empty order history
        mock_response_handler.handle_historical_orders_response.return_value = []
        result_empty = await hyperliquid_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol="NONEXISTENT",
                start_time=datetime(2024, 1, 1, tzinfo=UTC),
                end_time=datetime(2024, 1, 2, tzinfo=UTC),
            ),
        )
        assert result_empty == []

        # Test case 2: Multiple orders with complex filtering
        mock_order1 = MagicMock(coin="BTC")
        mock_order1.model_dump.return_value = {
            "oid": 1,
            "coin": "BTC",
            "side": "B",
            "limitPx": "30000.0",
            "sz": "0.001",
            "timestamp": 1704067200000,
            "orderType": "limit",
            "origSz": "0.001",
            "cloid": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }
        mock_order2 = MagicMock(coin="ETH")
        mock_order2.model_dump.return_value = {
            "oid": 2,
            "coin": "ETH",
            "side": "A",
            "limitPx": "2000.0",
            "sz": "0.01",
            "timestamp": 1704067201000,
            "orderType": "limit",
            "origSz": "0.01",
            "cloid": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }
        mock_order3 = MagicMock(coin="BTC")
        mock_order3.model_dump.return_value = {
            "oid": 3,
            "coin": "BTC",
            "side": "B",
            "limitPx": "30500.0",
            "sz": "0.002",
            "timestamp": 1704067202000,
            "orderType": "limit",
            "origSz": "0.002",
            "cloid": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }
        mock_orders = [
            MagicMock(order=mock_order1, status="filled", status_timestamp=1704067200000),
            MagicMock(order=mock_order2, status="filled", status_timestamp=1704067201000),
            MagicMock(order=mock_order3, status="filled", status_timestamp=1704067202000),
        ]
        mock_response_handler.handle_historical_orders_response.return_value = mock_orders

        mock_internal_orders = [
            MagicMock(
                symbol="BTC",
                id="order1",
                created_at=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            ),
            MagicMock(
                symbol="ETH",
                id="order2",
                created_at=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            ),
            MagicMock(
                symbol="BTC",
                id="order3",
                created_at=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            ),
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
            """Handle selective mapping success/failure for testing error scenarios."""
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
