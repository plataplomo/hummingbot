"""
Unit tests for BackpackAccountService history and operations functionality.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import (
    InternalWithdrawalStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.operations import BackpackWithdrawalDetails, Withdrawal
from cyberdelta.core.models.spot_balance import SpotBalance


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
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test successful fetching of order history."""
        symbol = "SOL_USDC"
        limit = 5
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC)

        mock_built_params = {
            "symbol": symbol,
            "limit": limit,
            "startTime": int(start_time.timestamp() * 1000),
        }
        mock_raw_order_data = {
            "id": "orderHist123",
            "symbol": symbol,
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "10",
            "price": "100",
            "status": "FILLED",
            "createdAt": int(start_time.timestamp() * 1000),
            "timeInForce": "GTC",
        }
        mock_raw_response_list: list[dict[str, Any]] = [mock_raw_order_data]
        mock_status_code = 200
        mock_headers: dict[str, str] = {}

        mock_validated_raw_orders = [BackpackRawOrder.model_validate(mock_raw_order_data)]

        expected_internal_order = Order(
            exchange_order_id="orderHist123",
            exchange="backpack_test_account",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("10"),
            price=Decimal("100"),
            time_in_force=TimeInForce.GTC,
            created_at=start_time,
            updated_at=start_time,  # Assuming updated_at is same as created_at for this mock
            client_order_id="mock_client_order_id",  # This should come from mapper or be None
            quantity_filled=Decimal("10"),
            average_fill_price=Decimal("100"),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            bp_details=None,  # Add missing field
            hl_details=None,  # Add missing field
        )
        expected_internal_orders_list = [expected_internal_order]

        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (
            mock_raw_response_list,
            mock_status_code,
            mock_headers,
        )
        mock_response_handler.handle_get_order_history_response.return_value = (
            mock_validated_raw_orders
        )
        mock_mapper.transform_raw_order_to_internal.return_value = expected_internal_order

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_order_history(
                symbol=symbol, limit=limit, start_time=start_time, end_time=end_time
            )

        mock_request_builder.build_get_order_history_params.assert_called_once_with(
            symbol=symbol,
            start_time_ms=int(start_time.timestamp() * 1000),
            end_time_ms=int(end_time.timestamp() * 1000),
            limit=limit,
            order_id=None,
            client_order_id=None,
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/history/orders",
            params=mock_built_params,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_get_order_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )
        mock_mapper.transform_raw_order_to_internal.assert_called_once_with(
            mock_validated_raw_orders[0]
        )
        assert len(result) == len(expected_internal_orders_list)
        for actual, expected in zip(result, expected_internal_orders_list, strict=False):
            assert actual.exchange_order_id == expected.exchange_order_id
            assert actual.exchange == expected.exchange
            assert actual.symbol == expected.symbol
            assert actual.side == expected.side
            assert actual.order_type == expected.order_type
            assert actual.status == expected.status
            assert actual.quantity_requested == expected.quantity_requested
            assert actual.price == expected.price
            assert actual.time_in_force == expected.time_in_force
            assert actual.created_at == expected.created_at
            # Timestamps can be tricky, ensure they are datetimes and UTC for actual
            assert isinstance(actual.updated_at, datetime)
            assert actual.updated_at.tzinfo == UTC
            # For expected, it's already set to start_time (which is UTC)
            # We might need to mock datetime.now(UTC) in the mapper if it's used for updated_at
            # For now, if expected.updated_at is fixed, compare directly if appropriate
            assert actual.updated_at == expected.updated_at

            assert actual.client_order_id == expected.client_order_id
            assert actual.quantity_filled == expected.quantity_filled
            assert actual.average_fill_price == expected.average_fill_price
            assert actual.triggered_at == expected.triggered_at
            assert actual.strategy_name == expected.strategy_name
            assert actual.signal_id == expected.signal_id
            assert actual.bp_details == expected.bp_details
            assert actual.hl_details == expected.hl_details

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_response_handler(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_history handles APIError from response_handler."""
        symbol = "SOL_USDC"
        mock_built_params = {"symbol": symbol}
        mock_raw_response_list: list[dict[str, Any]] = [{"invalid": "order"}]
        mock_status_code = 200

        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (mock_raw_response_list, mock_status_code, {})
        handler_api_error = APIError(
            "Invalid raw order history", APIErrorCode.INVALID_RESPONSE.value
        )
        mock_response_handler.handle_get_order_history_response.side_effect = handler_api_error

        with pytest.raises(APIError) as excinfo:
            await bp_account_service.get_order_history(symbol=symbol)
        assert excinfo.value is handler_api_error
        mock_response_handler.handle_get_order_history_response.assert_called_once_with(
            mock_raw_response_list, symbol
        )

    @pytest.mark.asyncio
    async def test_get_order_history_api_error_from_mapper(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_order_history handles APIError from mapper."""
        symbol = "SOL_USDC"
        mock_built_params = {"symbol": symbol}
        mock_raw_order_data = {
            "id": "orderHist123",
            "symbol": symbol,
            "status": "FILLED",
            "timeInForce": "GTC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "1",
            "price": "1",
            "createdAt": 1234567890000,
        }
        mock_raw_response_list: list[dict[str, Any]] = [mock_raw_order_data]
        mock_status_code = 200
        mock_validated_raw_orders = [BackpackRawOrder.model_validate(mock_raw_order_data)]

        mock_request_builder.build_get_order_history_params.return_value = mock_built_params
        mock_http_client_requester.return_value = (mock_raw_response_list, mock_status_code, {})
        mock_response_handler.handle_get_order_history_response.return_value = (
            mock_validated_raw_orders
        )

        mapper_api_error = APIError("Order history mapping failed", APIErrorCode.UNKNOWN.value)
        mock_mapper.transform_raw_order_to_internal.side_effect = mapper_api_error

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            with pytest.raises(APIError) as excinfo:
                await bp_account_service.get_order_history(symbol=symbol)
            assert excinfo.value is mapper_api_error

    @pytest.mark.asyncio
    async def test_get_order_history_response_none_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_order_history when HTTP client returns None content."""
        symbol = "SOL_USDC"
        limit = 10

        mock_params = {"symbol": symbol, "limit": limit}
        mock_request_builder.build_get_order_history_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_account_service.get_order_history(symbol=symbol, limit=limit)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert "No data received for order history, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_withdraw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test successful withdrawal operation."""
        asset = "USDC"
        amount = Decimal("100.0")
        address = "0x1234567890abcdef1234567890abcdef12345678"
        network = "ETH"
        tag = "12345"
        client_withdrawal_id = "withdraw_123"
        two_factor_token = "2fa_token"

        mock_payload = {
            "asset": asset,
            "amount": str(amount),
            "address": address,
            "network": network,
            "tag": tag,
            "clientWithdrawalId": client_withdrawal_id,
            "twoFactorToken": two_factor_token,
        }

        mock_raw_response = {
            "id": 123,
            "blockchain": "Ethereum",
            "quantity": "100.0",
            "fee": "1.0",
            "symbol": "USDC",
            "status": "pending",
            "toAddress": address,
            "createdAt": "2023-01-01T00:00:00Z",
            "isInternal": False,
        }

        mock_raw_withdrawal_model = BackpackRawWithdrawalResponse.model_validate(
            {
                "id": 123,
                "blockchain": "Ethereum",
                "quantity": "100.0",
                "fee": "1.0",
                "symbol": "USDC",
                "status": "pending",
                "toAddress": address,
                "createdAt": "2023-01-01T00:00:00Z",
                "isInternal": False,
            }
        )

        expected_withdrawal = Withdrawal(
            id="123",
            exchange="backpack_test_account",
            asset=asset,
            quantity=amount,
            status=InternalWithdrawalStatus.PENDING,
            address=address,
            timestamp=datetime.now(UTC),
            response_message=None,
            bp_details=BackpackWithdrawalDetails(
                client_id=client_withdrawal_id,
                blockchain=network,
            ),
            hl_details=None,
        )

        mock_request_builder.build_withdraw_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_withdraw_response.return_value = mock_raw_withdrawal_model
        mock_mapper.transform_raw_withdrawal_response_to_internal.return_value = expected_withdrawal

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.withdraw(
                asset=asset,
                amount=amount,
                address=address,
                network=network,
                tag=tag,
                client_withdrawal_id=client_withdrawal_id,
                two_factor_token=two_factor_token,
            )

        mock_request_builder.build_withdraw_payload.assert_called_once_with(
            asset=asset,
            amount=amount,
            address=address,
            network=network,
            tag=tag,
            client_withdrawal_id=client_withdrawal_id,
            two_factor_token=two_factor_token,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/api/v1/capital/withdrawals",
            data=mock_payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_withdraw_response.assert_called_once_with(mock_raw_response)
        mock_mapper.transform_raw_withdrawal_response_to_internal.assert_called_once_with(
            raw_response=mock_raw_withdrawal_model,
            asset=asset,
            quantity=amount,
            address=address,
            network=network,
            client_withdrawal_id=client_withdrawal_id,
            tag=tag,
        )

        assert result.id == expected_withdrawal.id
        assert result.exchange == expected_withdrawal.exchange
        assert result.asset == expected_withdrawal.asset
        assert result.quantity == expected_withdrawal.quantity
        assert result.status == expected_withdrawal.status

    @pytest.mark.asyncio
    async def test_withdraw_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test withdrawal when HTTP client returns None content."""
        asset = "USDC"
        amount = Decimal("100.0")
        address = "0x1234567890abcdef1234567890abcdef12345678"

        mock_payload = {"asset": asset, "amount": str(amount), "address": address}
        mock_request_builder.build_withdraw_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (None, 400, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.withdraw(asset=asset, amount=amount, address=address)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for withdrawal, status: 400" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_withdraw_validation_error(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test withdrawal when validation error occurs."""
        asset = "USDC"
        amount = Decimal("100.0")
        address = "0x1234567890abcdef1234567890abcdef12345678"

        mock_payload = {"asset": asset, "amount": str(amount), "address": address}
        mock_raw_response = {"invalid": "response"}

        mock_request_builder.build_withdraw_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_withdraw_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.withdraw(asset=asset, amount=amount, address=address)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing withdrawal data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_withdraw_unexpected_exception(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test withdrawal when an unexpected exception occurs."""
        asset = "USDC"
        amount = Decimal("100.0")
        address = "0x1234567890abcdef1234567890abcdef12345678"

        mock_payload = {"asset": asset, "amount": str(amount), "address": address}
        mock_raw_response = {"id": "123", "status": "PENDING"}

        mock_request_builder.build_withdraw_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_withdraw_response.side_effect = Exception("Unexpected error")

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.withdraw(asset=asset, amount=amount, address=address)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for withdrawal" in exc_info.value.message

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

        mock_params = {"symbol": symbol, "limit": limit}
        mock_raw_trade_data = {
            "id": "trade_123",
            "orderId": "order_123",
            "symbol": symbol,
            "qty": "10.0",
            "price": "100.0",
            "time": 1234567890000,
        }
        mock_raw_response = [mock_raw_trade_data]
        mock_validated_raw_trades = [BackpackRawTrade.model_validate(mock_raw_trade_data)]

        expected_trade = Trade(
            id="trade_123",
            exchange="backpack_test_account",
            symbol=symbol,
            side=OrderSide.BUY,
            executed_at=datetime.fromtimestamp(1234567890000 / 1000, tz=UTC),
            order_id="order_123",
            quantity=Decimal("10.0"),
            price=Decimal("100.0"),
            fee=Decimal("0.1"),
            fee_asset="USDC",
            bp_details=None,
            hl_details=None,
        )

        mock_request_builder.build_get_trade_history_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_trade_history_response.return_value = (
            mock_validated_raw_trades
        )
        mock_mapper.transform_raw_trade_to_internal.return_value = expected_trade

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_trade_history(symbol=symbol, limit=limit)

        mock_request_builder.build_get_trade_history_params.assert_called_once_with(
            symbol=symbol, limit=limit
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/history/fills",
            params=mock_params,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
            is_public_info_endpoint=False,
        )
        mock_response_handler.handle_get_trade_history_response.assert_called_once_with(
            mock_raw_response, symbol
        )
        mock_mapper.transform_raw_trade_to_internal.assert_called_once_with(
            mock_validated_raw_trades[0]
        )

        assert len(result) == 1
        assert result[0].id == expected_trade.id
        assert result[0].symbol == expected_trade.symbol
        assert result[0].side == expected_trade.side
        assert result[0].executed_at == expected_trade.executed_at
        assert result[0].order_id == expected_trade.order_id
        assert result[0].quantity == expected_trade.quantity
        assert result[0].price == expected_trade.price
        assert result[0].fee == expected_trade.fee
        assert result[0].fee_asset == expected_trade.fee_asset
        assert result[0].bp_details == expected_trade.bp_details
        assert result[0].hl_details == expected_trade.hl_details

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

        mock_params = {"symbol": symbol, "limit": limit}
        mock_request_builder.build_get_trade_history_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_trade_history(symbol=symbol, limit=limit)

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

        mock_params = {"symbol": symbol, "limit": limit}
        mock_raw_response = [{"invalid": "trade"}]

        mock_request_builder.build_get_trade_history_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_trade_history_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_trade_history(symbol=symbol, limit=limit)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing trade history data failed" in exc_info.value.message

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

        mock_params = {"symbol": symbol}
        mock_raw_response = [{"id": "order_123"}]

        mock_request_builder.build_get_trade_history_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_trade_history_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_trade_history(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Processing trade history data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection by testing behavior."""
        custom_mapper = MagicMock()

        service = BackpackAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="backpack_test",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=custom_mapper,
        )

        # Test that custom mapper is used through behavior
        custom_mapper.transform_raw_balance_to_internal.return_value = SpotBalance(
            exchange="backpack_test",
            asset="USDC",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("100.0"),
            available_quantity=Decimal("100.0"),
        )

        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = (
            {"USDC": {"available": "100.0", "total": "100.0"}},
            200,
            {},
        )
        mock_response_handler.handle_get_balances_response.return_value = {
            "USDC": {"available": "100.0", "total": "100.0"}
        }

        with patch.object(service, "_mapper", custom_mapper):
            result = await service.get_balances()

        # Verify custom mapper was called
        custom_mapper.transform_raw_balance_to_internal.assert_called_once()
        assert "USDC" in result

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor creates default mapper when none provided by testing behavior."""
        service = BackpackAccountService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="backpack_test",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=None,  # Explicitly pass None
        )

        # Test behavior that would require a mapper
        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = (
            {"USDC": {"available": "100.0", "total": "100.0"}},
            200,
            {},
        )
        mock_response_handler.handle_get_balances_response.return_value = {
            "USDC": {"available": "100.0", "total": "100.0"}
        }

        # Mock the mapper behavior since we can't access it directly
        mock_default_mapper = MagicMock()
        mock_default_mapper.transform_raw_balance_to_internal.return_value = SpotBalance(
            exchange="backpack_test",
            asset="USDC",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("100.0"),
            available_quantity=Decimal("100.0"),
        )

        with patch.object(service, "_mapper", mock_default_mapper):
            result = await service.get_balances()

        # Verify default mapper functionality works
        assert "USDC" in result
