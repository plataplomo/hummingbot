"""Unit tests for Backpack Transfer Service.

Tests cover all methods of the BackpackTransferService including:
- Internal transfers between account types
- External withdrawals to blockchain addresses
- Account type validation
- Network validation and support
- Error handling and edge cases
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.mappers.account.bp_transfer_mapper import BackpackTransferMapper
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_transfer_service import (
    BackpackTransferService,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args.account import TransferArgs, WithdrawArgs
from cyberdelta.core.enums import InternalTransferStatus, InternalWithdrawalStatus
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.service_validation import (
    InvalidAccountTypeError,
    NetworkRequiredError,
    UnsupportedNetworkError,
)
from cyberdelta.models.operations import Transfer, Withdrawal


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Create a mock HTTP client requester.

    Returns:
        AsyncMock: Mock HTTP client for testing.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Create a mock request builder.

    Returns:
        MagicMock: Mock BackpackAccountRequestBuilder instance for testing.
    """
    return MagicMock(spec=BackpackAccountRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Create a mock response handler.

    Returns:
        MagicMock: Mock BackpackAccountResponseHandler instance for testing.
    """
    return MagicMock(spec=BackpackAccountResponseHandler)


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Create a mock data mapper.

    Returns:
        MagicMock: Mock BackpackTransferMapper instance for testing.
    """
    return MagicMock(spec=BackpackTransferMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Create a mock authenticator.

    Returns:
        MagicMock: Mock authenticator instance for testing.
    """
    return MagicMock()


@pytest.fixture
def transfer_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackTransferService:
    """Create a transfer service instance with mocks.

    Returns:
        BackpackTransferService: Configured service instance with mocked dependencies.
    """
    return BackpackTransferService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name=ExchangeName.BACKPACK,
    )


@pytest.fixture
def mock_raw_withdrawal_response() -> BackpackRawWithdrawalResponse:
    """Create a mock raw withdrawal response.

    Returns:
        BackpackRawWithdrawalResponse: Mock withdrawal response with test data.
    """
    return BackpackRawWithdrawalResponse(
        id=123,
        status="confirmed",
        blockchain="Solana",
        toAddress="5xoBq7f7CDgZwqHrDBdRWM84ExRetg4gZq93dyJLpiLZ",
        quantity="500.00",
        fee="0.01",
        symbol="USDC",
        createdAt="2024-01-15T10:30:00Z",  # type: ignore[arg-type]
        isInternal=False,
    )


@pytest.fixture
def mock_transfer() -> Transfer:
    """Create a mock transfer.

    Returns:
        Transfer: Mock Transfer instance with test data.
    """
    return Transfer(
        id="transfer_123",
        asset="USDC",
        quantity=Decimal("1000.00"),
        status=InternalTransferStatus.COMPLETED,
        timestamp=datetime.now(UTC),
        exchange=ExchangeName.BACKPACK,
    )


@pytest.fixture
def mock_withdrawal() -> Withdrawal:
    """Create a mock withdrawal.

    Returns:
        Withdrawal: Mock Withdrawal instance with test data.
    """
    return Withdrawal(
        id="withdrawal_123",
        asset="USDC",
        quantity=Decimal("500.00"),
        address="5xoBq7f7CDgZwqHrDBdRWM84ExRetg4gZq93dyJLpiLZ",
        status=InternalWithdrawalStatus.COMPLETED,
        timestamp=datetime.now(UTC),
        exchange=ExchangeName.BACKPACK,
    )


class TestBackpackTransferService:
    """Test suite for BackpackTransferService."""

    @pytest.mark.asyncio
    async def test_transfer_success(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_transfer: Transfer,
    ) -> None:
        """Test successful internal transfer."""
        # Arrange
        args = TransferArgs(
            asset="USDC",
            amount=Decimal("1000.00"),
            from_account_type="SPOT",
            to_account_type="FUTURES",
            client_transfer_id="client_transfer_123",
        )

        mock_payload = {
            "asset": "USDC",
            "amount": "1000.00",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload

        raw_response = {"status": "SUCCESS", "id": "transfer_123"}
        mock_http_client.return_value = (raw_response, 200, {})

        mock_response_handler.handle_transfer_response.return_value = raw_response
        mock_mapper.transform_raw_transfer_to_internal.return_value = mock_transfer

        # Act
        result = await transfer_service.transfer(args)

        # Assert
        assert result == mock_transfer
        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol="USDC",
            amount=Decimal("1000.00"),
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )
        mock_http_client.assert_called_once()
        assert mock_http_client.call_args.kwargs["method"] == "POST"
        assert mock_http_client.call_args.kwargs["endpoint"] == "/api/v1/capital/transfer"

    @pytest.mark.asyncio
    async def test_transfer_invalid_from_account_type(
        self,
        transfer_service: BackpackTransferService,
    ) -> None:
        """Test transfer with invalid from account type."""
        # Arrange
        args = TransferArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            from_account_type="INVALID",
            to_account_type="SPOT",
        )

        # Act & Assert
        with pytest.raises(InvalidAccountTypeError) as exc_info:
            await transfer_service.transfer(args)

        assert exc_info.value.account_type == "INVALID"
        assert exc_info.value.parameter_name == "from_account_type"
        assert "SPOT" in exc_info.value.valid_types
        assert "MARGIN" in exc_info.value.valid_types
        assert "FUTURES" in exc_info.value.valid_types

    @pytest.mark.asyncio
    async def test_transfer_invalid_to_account_type(
        self,
        transfer_service: BackpackTransferService,
    ) -> None:
        """Test transfer with invalid to account type."""
        # Arrange
        args = TransferArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            from_account_type="SPOT",
            to_account_type="INVALID",
        )

        # Act & Assert
        with pytest.raises(InvalidAccountTypeError) as exc_info:
            await transfer_service.transfer(args)

        assert exc_info.value.account_type == "INVALID"
        assert exc_info.value.parameter_name == "to_account_type"

    @pytest.mark.asyncio
    async def test_transfer_http_error(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test transfer with HTTP error."""
        # Arrange
        args = TransferArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            from_account_type="SPOT",
            to_account_type="MARGIN",
        )

        mock_request_builder.build_internal_transfer_payload.return_value = {}
        mock_http_client.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transfer_service.transfer(args)

        # Business logic wraps HTTP errors with generic message
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_transfer_invalid_response_type(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test transfer with invalid response type from handler."""
        # Arrange
        args = TransferArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            from_account_type="SPOT",
            to_account_type="FUTURES",
        )

        mock_request_builder.build_internal_transfer_payload.return_value = {}
        mock_http_client.return_value = ({}, 200, {})

        # Response handler returns wrong type
        mock_response_handler.handle_transfer_response.return_value = []  # Should be dict

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transfer_service.transfer(args)

        assert "unexpected type" in exc_info.value.message
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_withdraw_success(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_withdrawal_response: BackpackRawWithdrawalResponse,
        mock_withdrawal: Withdrawal,
    ) -> None:
        """Test successful withdrawal."""
        # Arrange
        args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("500.00"),
            address="5xoBq7f7CDgZwqHrDBdRWM84ExRetg4gZq93dyJLpiLZ",
            network="Solana",
            client_withdrawal_id="client_withdrawal_123",
        )

        mock_payload = {
            "asset": "USDC",
            "amount": "500.00",
            "address": "5xoBq7f7CDgZwqHrDBdRWM84ExRetg4gZq93dyJLpiLZ",
            "blockchain": "Solana",
        }
        mock_request_builder.build_withdraw_payload.return_value = mock_payload

        raw_response = mock_raw_withdrawal_response.model_dump()
        mock_http_client.return_value = (raw_response, 200, {})

        mock_response_handler.handle_withdraw_response.return_value = mock_raw_withdrawal_response
        mock_mapper.transform_raw_withdrawal_response_to_internal.return_value = mock_withdrawal

        # Act
        result = await transfer_service.withdraw(args)

        # Assert
        assert result == mock_withdrawal
        mock_request_builder.build_withdraw_payload.assert_called_once_with(
            asset_symbol="USDC",
            amount=Decimal("500.00"),
            address="5xoBq7f7CDgZwqHrDBdRWM84ExRetg4gZq93dyJLpiLZ",
            network="Solana",
            tag=None,
            client_withdraw_id="client_withdrawal_123",
        )
        mock_http_client.assert_called_once()
        assert mock_http_client.call_args.kwargs["method"] == "POST"
        assert mock_http_client.call_args.kwargs["endpoint"] == "/api/v1/capital/withdrawals"

    @pytest.mark.asyncio
    async def test_withdraw_missing_network(
        self,
        transfer_service: BackpackTransferService,
    ) -> None:
        """Test withdrawal without network specified."""
        # Arrange
        args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            address="some_address",
            network=None,
        )

        # Act & Assert
        with pytest.raises(NetworkRequiredError) as exc_info:
            await transfer_service.withdraw(args)

        assert exc_info.value.operation == "withdrawal"

    @pytest.mark.asyncio
    async def test_withdraw_unsupported_network(
        self,
        transfer_service: BackpackTransferService,
    ) -> None:
        """Test withdrawal with unsupported network."""
        # Arrange
        args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            address="some_address",
            network="UnsupportedNetwork",
        )

        # Act & Assert
        with pytest.raises(UnsupportedNetworkError) as exc_info:
            await transfer_service.withdraw(args)

        assert exc_info.value.network == "UnsupportedNetwork"
        assert "Solana" in exc_info.value.supported_networks
        assert "Ethereum" in exc_info.value.supported_networks
        assert "Bitcoin" in exc_info.value.supported_networks

    @pytest.mark.asyncio
    async def test_withdraw_with_all_supported_networks(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_withdrawal_response: BackpackRawWithdrawalResponse,
        mock_withdrawal: Withdrawal,
    ) -> None:
        """Test withdrawal with each supported network."""
        # Arrange
        supported_networks = [
            "Arbitrum",
            "Base",
            "Bitcoin",
            "BitcoinCash",
            "BNBSmartChain",
            "Cardano",
            "Dogecoin",
            "Ethereum",
            "Litecoin",
            "Polygon",
            "Solana",
            "Story",
            "Sui",
            "XRP",
        ]

        for network in supported_networks:
            args = WithdrawArgs(
                asset="USDC",
                amount=Decimal("100.00"),
                address="test_address",
                network=network,
            )

            mock_request_builder.build_withdraw_payload.return_value = {}
            mock_http_client.return_value = (mock_raw_withdrawal_response.model_dump(), 200, {})
            mock_response_handler.handle_withdraw_response.return_value = (
                mock_raw_withdrawal_response
            )
            mock_mapper.transform_raw_withdrawal_response_to_internal.return_value = mock_withdrawal

            # Act - Should not raise
            result = await transfer_service.withdraw(args)

            # Assert
            assert result == mock_withdrawal

    @pytest.mark.asyncio
    async def test_withdraw_with_tag(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_withdrawal_response: BackpackRawWithdrawalResponse,
        mock_withdrawal: Withdrawal,
    ) -> None:
        """Test withdrawal with address tag (for XRP, etc.)."""
        # Arrange
        args = WithdrawArgs(
            asset="XRP",
            amount=Decimal("100.00"),
            address="rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH",
            network="XRP",
            tag="12345",
        )

        mock_request_builder.build_withdraw_payload.return_value = {}
        mock_http_client.return_value = (mock_raw_withdrawal_response.model_dump(), 200, {})
        mock_response_handler.handle_withdraw_response.return_value = mock_raw_withdrawal_response
        mock_mapper.transform_raw_withdrawal_response_to_internal.return_value = mock_withdrawal

        # Act
        result = await transfer_service.withdraw(args)

        # Assert
        assert result == mock_withdrawal
        # Verify tag was passed to request builder
        mock_request_builder.build_withdraw_payload.assert_called_once()
        call_args = mock_request_builder.build_withdraw_payload.call_args
        assert call_args.kwargs["tag"] == "12345"

    @pytest.mark.asyncio
    async def test_withdraw_with_2fa(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_withdrawal_response: BackpackRawWithdrawalResponse,
        mock_withdrawal: Withdrawal,
    ) -> None:
        """Test withdrawal with two-factor authentication token."""
        # Arrange
        args = WithdrawArgs(
            asset="BTC",
            amount=Decimal("0.1"),
            address="bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
            network="Bitcoin",
            two_factor_token="123456",
        )

        mock_request_builder.build_withdraw_payload.return_value = {}
        mock_http_client.return_value = (mock_raw_withdrawal_response.model_dump(), 200, {})
        mock_response_handler.handle_withdraw_response.return_value = mock_raw_withdrawal_response
        mock_mapper.transform_raw_withdrawal_response_to_internal.return_value = mock_withdrawal

        # Act
        result = await transfer_service.withdraw(args)

        # Assert
        assert result == mock_withdrawal
        # Verify call was made (2FA token not supported in current business logic)
        mock_request_builder.build_withdraw_payload.assert_called_once()

    @pytest.mark.asyncio
    async def test_withdraw_transformation_error(
        self,
        transfer_service: BackpackTransferService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_withdrawal_response: BackpackRawWithdrawalResponse,
    ) -> None:
        """Test withdrawal with transformation error."""
        # Arrange
        args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            address="test_address",
            network="Solana",
        )

        mock_request_builder.build_withdraw_payload.return_value = {}
        mock_http_client.return_value = (mock_raw_withdrawal_response.model_dump(), 200, {})
        mock_response_handler.handle_withdraw_response.return_value = mock_raw_withdrawal_response

        mock_mapper.transform_raw_withdrawal_response_to_internal.side_effect = TransformationError(
            "Invalid withdrawal data"
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transfer_service.withdraw(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to process/transform" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_transfer_without_authenticator(
        self,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_transfer: Transfer,
    ) -> None:
        """Test transfer when authenticator is None."""
        # Arrange
        service = BackpackTransferService(
            http_client_requester=mock_http_client,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            mapper=mock_mapper,
            authenticator=None,  # No authenticator
            exchange_name=ExchangeName.BACKPACK,
        )

        args = TransferArgs(
            asset="USDC",
            amount=Decimal("100.00"),
            from_account_type="SPOT",
            to_account_type="MARGIN",
        )

        mock_request_builder.build_internal_transfer_payload.return_value = {}
        mock_http_client.return_value = ({"status": "SUCCESS"}, 200, {})
        mock_response_handler.handle_transfer_response.return_value = {"status": "SUCCESS"}
        mock_mapper.transform_raw_transfer_to_internal.return_value = mock_transfer

        # Act
        result = await service.transfer(args)

        # Assert - Should still work, authenticator is optional
        assert result == mock_transfer
