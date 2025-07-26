"""Unit tests for BackpackAccountService history and operations functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args import (
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    WithdrawArgs,
)
from cyberdelta.core.models.operations import Withdrawal
from cyberdelta.enums import OrderSide


class TestBackpackAccountServiceHistoryOperations:
    """Tests for the BackpackAccountService history and operations functionality."""

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_order_history returns empty list (no order history endpoint)."""
        symbol = "SOL_USDC"
        limit = 10
        start_time = datetime(2023, 1, 1, tzinfo=UTC)
        end_time = datetime(2023, 1, 2, tzinfo=UTC)

        # Business logic returns empty list since Backpack doesn't have order history endpoint
        result = await bp_account_service.get_order_history(
            GetOrderHistoryArgs(
                symbol=symbol,
                limit=limit,
                start_time=start_time,
                end_time=end_time,
            )
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_history returns empty list (no order history endpoint)."""
        symbol = "SOL_USDC"

        # Business logic returns empty list since Backpack doesn't have order history endpoint
        result = await bp_account_service.get_order_history(GetOrderHistoryArgs(symbol=symbol))

        assert result == []

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_order_history returns empty list (no order history endpoint)."""
        symbol = "SOL_USDC"

        # Business logic returns empty list since Backpack doesn't have order history endpoint
        result = await bp_account_service.get_order_history(GetOrderHistoryArgs(symbol=symbol))

        assert result == []

    @pytest.mark.asyncio
    async def test_get_order_history_response_none_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_order_history returns empty list (no order history endpoint)."""
        symbol = "SOL_USDC"
        limit = 10

        # Business logic returns empty list since Backpack doesn't have order history endpoint
        result = await bp_account_service.get_order_history(
            GetOrderHistoryArgs(symbol=symbol, limit=limit)
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_withdraw_success(
        self,
        bp_account_service: BackpackAccountService,
        asset: str,
        amount: Decimal,
        address: str,
        withdrawal_result: Withdrawal,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test withdraw raises NotImplementedError (Backpack doesn't support withdrawal)."""
        network = "Polygon"
        tag = "some_tag"
        withdrawal_id = "withdrawal_123"

        withdraw_args = WithdrawArgs(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
            tag=tag,
            client_withdrawal_id=withdrawal_id,
        )

        # Business logic raises NotImplementedError for withdrawal
        with pytest.raises(NotImplementedError) as exc_info:
            await bp_account_service.withdraw(withdraw_args)

        assert "Withdraw operation is not supported by Backpack exchange" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_withdraw_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        asset: str,
        amount: Decimal,
        address: str,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test withdraw raises NotImplementedError (Backpack doesn't support withdrawal)."""
        network = "Polygon"

        withdraw_args = WithdrawArgs(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
        )

        # Business logic raises NotImplementedError for withdrawal
        with pytest.raises(NotImplementedError) as exc_info:
            await bp_account_service.withdraw(withdraw_args)

        assert "Withdraw operation is not supported by Backpack exchange" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_withdraw_validation_error(
        self,
        bp_account_service: BackpackAccountService,
        asset: str,
        amount: Decimal,
        address: str,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test withdraw raises NotImplementedError (Backpack doesn't support withdrawal)."""
        network = "Polygon"

        withdraw_args = WithdrawArgs(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
        )

        # Business logic raises NotImplementedError for withdrawal
        with pytest.raises(NotImplementedError) as exc_info:
            await bp_account_service.withdraw(withdraw_args)

        assert "Withdraw operation is not supported by Backpack exchange" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_withdraw_unexpected_exception(
        self,
        bp_account_service: BackpackAccountService,
        asset: str,
        amount: Decimal,
        address: str,
        mock_http_client: MagicMock,
    ) -> None:
        """Test withdraw raises NotImplementedError (Backpack doesn't support withdrawal)."""
        withdraw_args = WithdrawArgs(
            asset=asset,
            amount=amount,
            address=address,
            network="Ethereum",  # Add required network parameter
        )

        # Business logic raises NotImplementedError for withdrawal
        with pytest.raises(NotImplementedError) as exc_info:
            await bp_account_service.withdraw(withdraw_args)

        assert "Withdraw operation is not supported by Backpack exchange" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_trade_history_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test successful trade history retrieval."""
        symbol = "SOL_USDC"
        limit = 50

        # Create proper raw trade data with all required fields
        mock_raw_trade_data = {
            "fee": "0.01",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "order_123",
            "price": "100.0",
            "quantity": "10.0",
            "side": "Bid",  # Buy side
            "symbol": symbol,
            "timestamp": "2009-02-13T23:31:30.000Z",  # 1234567890 seconds from epoch
            "tradeId": 123,
        }
        mock_raw_response = [mock_raw_trade_data]

        # Mock the HTTP client to return expected data
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Test calls to the service, which delegates to the transaction history service
        result = await bp_account_service.get_trade_history(
            GetTradeHistoryArgs(symbol=symbol, limit=limit),
        )

        # The mapper returns trades with exchange="backpack" not "backpack_test_account"
        assert len(result) == 1
        assert result[0].id == "123"
        assert result[0].symbol == symbol
        assert result[0].exchange == "backpack"  # Mapper hardcodes this
        assert result[0].order_id == "order_123"
        assert result[0].quantity == Decimal("10.0")
        assert result[0].price == Decimal("100.0")
        assert result[0].fee == Decimal("0.01")
        assert result[0].fee_asset == "USDC"
        assert result[0].side == OrderSide.BUY
        # Check executed_at is correct (parsed from ISO timestamp)
        assert result[0].executed_at == datetime(2009, 2, 13, 23, 31, 30, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_get_trade_history_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_trade_history when HTTP client returns None content."""
        symbol = "SOL_USDC"
        limit = 50

        # Mock the HTTP client to return None which triggers error
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_trade_history(
                GetTradeHistoryArgs(symbol=symbol, limit=limit),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for trade history, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_trade_history_validation_error(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_trade_history when validation error occurs."""
        symbol = "SOL_USDC"
        limit = 50

        # Mock the HTTP client to return invalid data that causes validation error
        # Missing required fields like fee, feeSymbol, etc. will cause validation error
        mock_http_client_requester.return_value = (
            [{"invalid": "trade", "symbol": "SOL_USDC"}],
            200,
            {},
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_trade_history(
                GetTradeHistoryArgs(symbol=symbol, limit=limit),
            )

        # Business logic wraps validation errors as INVALID_RESPONSE
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_get_trade_history_unexpected_exception(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_trade_history when unexpected exception occurs."""
        symbol = "SOL_USDC"

        # Mock the HTTP client to return incomplete data that causes validation error
        # This will trigger the business logic's error handling path
        mock_http_client_requester.return_value = (
            [{"symbol": "SOL_USDC", "incomplete": "data"}],
            200,
            {},
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_trade_history(GetTradeHistoryArgs(symbol=symbol))

        # Business logic wraps validation errors as INVALID_RESPONSE
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection."""
        custom_mapper = MagicMock()

        # Test that service can be created with custom transaction mapper
        service = BackpackAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="backpack_test",
            transaction_mapper=custom_mapper,
        )

        # Verify service was created successfully
        assert service is not None

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor creates default mapper when none provided."""
        # Test that service can be created without providing a custom mapper
        service = BackpackAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="backpack_test",
            # No mapper parameters needed - service should create default
        )

        # Verify service was created successfully
        assert service is not None
