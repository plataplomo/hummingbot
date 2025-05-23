from collections.abc import Callable
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)
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
def mock_authenticator_fixt() -> MagicMock:
    """Fixture for the authenticator mock."""
    return MagicMock(spec=IAuthenticator)


@pytest.fixture
def mock_hl_response_handler_fixt() -> MagicMock:
    """Fixture for the HyperliquidResponseHandler mock."""
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_error_mapper() -> MagicMock:
    """Fixture for the HyperliquidErrorMapper mock."""
    return MagicMock(spec=HyperliquidErrorMapper)


@pytest.fixture
def make_hl_trading_service(
    mock_exchange_http_client_requester: AsyncMock,
    mock_info_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler_fixt: MagicMock,
    mock_authenticator_fixt: MagicMock,
    mock_get_asset_index_callable: AsyncMock,
    mock_hl_order_mapper: MagicMock,
    mock_hl_error_mapper: MagicMock,
) -> Callable[..., HyperliquidTradingService]:
    """Factory fixture to create HyperliquidTradingService instances."""

    def _factory(wallet_address: str = "0xTestWalletAddrTrading") -> HyperliquidTradingService:
        return HyperliquidTradingService(
            exchange_http_client_requester=mock_exchange_http_client_requester,
            info_http_client_requester=mock_info_http_client_requester,
            request_builder=mock_hl_request_builder,
            response_handler=mock_hl_response_handler_fixt,
            authenticator=mock_authenticator_fixt,
            exchange_name="hyperliquid_test_trading",
            wallet_address=wallet_address,
            get_asset_index_callable=mock_get_asset_index_callable,
            order_mapper=mock_hl_order_mapper,
            error_mapper=mock_hl_error_mapper,
        )

    return _factory


class TestHyperliquidTradingService:
    # Test that was previously added (place_order with None response)
    # This test was lost due to the file overwrite, re-adding its structure
    @pytest.mark.asyncio
    async def test_place_order_http_client_returns_none_in_exchange_action(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_exchange_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
        # mock_authenticator_fixt: MagicMock # Not directly used in this test's assertions
    ) -> None:
        """Test place_order when the exchange HTTP client returns None content."""
        symbol = "ETH"
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = 0

        # Configure the mock to return a payload with the correct type attribute
        mock_payload = MagicMock()
        mock_payload.type = "order"  # Set the expected type value
        mock_hl_request_builder.build_place_order_payload.return_value = mock_payload

        mock_exchange_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.place_order(
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
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator_fixt: MagicMock,  # For assertion
    ) -> None:
        """Test get_order when the info HTTP client returns None content."""
        symbol = "ETH"
        order_id = 12345
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "orderStatus", "user": wallet_address, "oid": order_id}
        mock_hl_request_builder.build_order_status_payload.return_value = mock_request_payload_model
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"No data received for order status for OID {order_id}." in exc_info.value.message
        mock_hl_request_builder.build_order_status_payload.assert_called_once_with(
            wallet_address=wallet_address, order_id=order_id
        )
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_model.model_dump.return_value,
            authenticator=mock_authenticator_fixt,  # Use injected mock authenticator
            rate_limiter_service=None,
            is_signed=True,
        )

    # Test that was previously added (get_open_orders with None response)
    # This test was also lost, re-adding its structure
    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none_in_info_request(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator_fixt: MagicMock,  # For assertion
    ) -> None:
        """Test get_open_orders when the info HTTP client returns None content."""
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        mock_request_payload_model = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_request_payload_model.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_request_payload_model
        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_open_orders()
            mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(
                wallet_address
            )  # Moved inside

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Fetching open orders returned no content." in exc_info.value.message
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_model.model_dump.return_value,
            authenticator=mock_authenticator_fixt,  # Use injected mock authenticator
            rate_limiter_service=None,
            is_signed=True,
        )

    # Test that was previously added (cancel_order with None response)
    # This test was also lost, re-adding its structure
    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none_in_exchange_action(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_exchange_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
        # mock_authenticator_fixt: MagicMock # Not directly used in this test's assertions
    ) -> None:
        """Test cancel_order when the exchange HTTP client returns None content."""
        symbol = "ETH"
        order_id = 12345
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = 0

        mock_exchange_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.cancel_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (cancel) returned no content" in exc_info.value.message
        mock_get_asset_index_callable.assert_called_once_with(symbol)

        mock_exchange_http_client_requester.assert_called_once()
        _args, call_kwargs = mock_exchange_http_client_requester.call_args
        assert call_kwargs.get("method") == "POST"
        assert call_kwargs.get("endpoint") == "/exchange"
        assert call_kwargs.get("is_signed") is True

        # Import HyperliquidApiCancelOrderRequest and HyperliquidRawCancelOrderAction
        from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
            HyperliquidApiCancelOrderRequest,
        )
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
            HyperliquidRawCancelOrderAction,
        )

        sent_data = call_kwargs.get("data")
        assert isinstance(sent_data, HyperliquidApiCancelOrderRequest)
        assert sent_data.type == "cancel"
        assert isinstance(sent_data.action, HyperliquidRawCancelOrderAction)
        assert sent_data.action.asset == mock_get_asset_index_callable.return_value
        assert sent_data.action.oid == order_id

    # The problematic test from before, now with type annotations and correct imports
    @pytest.mark.asyncio
    async def test_cancel_all_orders_get_open_orders_returns_none(
        self: "TestHyperliquidTradingService",  # Type annotation for self
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator_fixt: MagicMock,  # For assertion
    ) -> None:
        """Test cancel_all_orders when _get_open_orders_raw receives None from info HTTP client."""
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        mock_request_payload_model_local = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_request_payload_model_local.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = (
            mock_request_payload_model_local
        )

        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.cancel_all_orders(symbol="ETH")
            mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(
                wallet_address
            )  # Moved inside

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Fetching open orders returned no content." in exc_info.value.message
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_model_local.model_dump.return_value,  # Mocked return value
            authenticator=mock_authenticator_fixt,
            rate_limiter_service=None,
            is_signed=True,
        )
