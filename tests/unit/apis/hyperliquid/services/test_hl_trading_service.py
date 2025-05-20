from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


# Minimal Fixtures that might be needed (original file likely had more comprehensive ones)
@pytest.fixture
def mock_exchange_http_client_requester() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_info_http_client_requester() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_get_asset_index_callable() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:
    return MagicMock(spec=HyperliquidOrderMapper)


@pytest.fixture
def hl_trading_service_fixture(
    mock_exchange_http_client_requester: AsyncMock,
    mock_info_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_get_asset_index_callable: AsyncMock,
    mock_hl_order_mapper: MagicMock,
) -> HyperliquidTradingService:
    # A minimal authenticator mock for the service
    mock_authenticator = MagicMock(spec=IAuthenticator)
    service = HyperliquidTradingService(
        exchange_http_client_requester=mock_exchange_http_client_requester,
        info_http_client_requester=mock_info_http_client_requester,
        request_builder=mock_hl_request_builder,
        response_handler=MagicMock(spec=HyperliquidResponseHandler),  # Add response_handler mock
        authenticator=mock_authenticator,
        exchange_name="hyperliquid_test_trading",
        wallet_address="0xTestWalletAddrTrading",
        get_asset_index_callable=mock_get_asset_index_callable,
        order_mapper=mock_hl_order_mapper,
    )
    return service


class TestHyperliquidTradingService:
    # Test that was previously added (place_order with None response)
    # This test was lost due to the file overwrite, re-adding its structure
    @pytest.mark.asyncio
    async def test_place_order_http_client_returns_none_in_exchange_action(
        self,
        hl_trading_service_fixture: HyperliquidTradingService,
        mock_exchange_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
    ) -> None:
        """Test place_order when the exchange HTTP client returns None content."""
        symbol = "ETH"
        # order_id was unused
        hl_trading_service_fixture._wallet_address = "0xWallet"
        mock_get_asset_index_callable.return_value = 0

        mock_exchange_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service_fixture.place_order(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("100"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (order) returned no content" in exc_info.value.message
        mock_exchange_http_client_requester.assert_called_once()

    # Test that was previously added (get_order with None response)
    # This test was also lost, re-adding its structure
    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none_in_info_request(
        self,
        hl_trading_service_fixture: HyperliquidTradingService,
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_order when the info HTTP client returns None content."""
        symbol = "ETH"
        order_id = 12345
        wallet_address = "0xWallet"  # Using a local var for clarity in test logic
        hl_trading_service_fixture._wallet_address = wallet_address

        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "orderStatus", "user": wallet_address, "oid": order_id}
        mock_hl_request_builder.build_order_status_payload.return_value = mock_request_payload_model
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service_fixture.get_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Fetching order status returned no content." in exc_info.value.message
        mock_hl_request_builder.build_order_status_payload.assert_called_once_with(
            wallet_address=wallet_address, order_id=order_id
        )
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            authenticator=hl_trading_service_fixture._authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )

    # Test that was previously added (get_open_orders with None response)
    # This test was also lost, re-adding its structure
    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none_in_info_request(
        self,
        hl_trading_service_fixture: HyperliquidTradingService,
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_open_orders when the info HTTP client returns None content."""
        wallet_address = "0xWallet"
        hl_trading_service_fixture._wallet_address = wallet_address

        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "openOrders", "user": wallet_address}
        mock_hl_request_builder.build_user_open_orders_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service_fixture.get_open_orders()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Fetching open orders returned no content." in exc_info.value.message
        mock_hl_request_builder.build_user_open_orders_payload.assert_called_once_with(
            user_address=wallet_address
        )
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            authenticator=hl_trading_service_fixture._authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )

    # Test that was previously added (cancel_order with None response)
    # This test was also lost, re-adding its structure
    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none_in_exchange_action(
        self,
        hl_trading_service_fixture: HyperliquidTradingService,
        mock_exchange_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test cancel_order when the exchange HTTP client returns None content."""
        symbol = "ETH"
        order_id = 12345
        wallet_address = "0xWallet"
        hl_trading_service_fixture._wallet_address = wallet_address
        mock_get_asset_index_callable.return_value = 0

        mock_cancel_action = MagicMock()
        mock_hl_request_builder.build_cancel_order_action.return_value = mock_cancel_action
        mock_exchange_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service_fixture.cancel_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (cancel) returned no content" in exc_info.value.message
        mock_get_asset_index_callable.assert_called_once_with(symbol)
        mock_hl_request_builder.build_cancel_order_action.assert_called_once_with(
            asset_index=0, order_id=order_id
        )
        mock_exchange_http_client_requester.assert_called_once()

    # The problematic test from before, now with type annotations and correct imports
    @pytest.mark.asyncio
    async def test_cancel_all_orders_get_open_orders_returns_none(
        self: "TestHyperliquidTradingService",  # Type annotation for self
        hl_trading_service_fixture: HyperliquidTradingService,
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test cancel_all_orders when _get_open_orders_raw receives None from info HTTP client."""
        wallet_address = "0xWallet"  # Local var for test clarity
        # Ensure service has a wallet address set for the test.
        # This specific test is for when get_open_orders fails, so wallet needs to be set for that part.
        hl_trading_service_fixture._wallet_address = wallet_address

        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "openOrders", "user": wallet_address}
        mock_hl_request_builder.build_user_open_orders_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict

        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service_fixture.cancel_all_orders(symbol="ETH")

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Fetching open orders returned no content." in exc_info.value.message

        mock_hl_request_builder.build_user_open_orders_payload.assert_called_once_with(
            user_address=wallet_address
        )
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            authenticator=hl_trading_service_fixture._authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )
