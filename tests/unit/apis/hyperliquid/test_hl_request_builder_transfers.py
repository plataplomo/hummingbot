"""Unit tests for HyperliquidAccountRequestBuilder transfer functionality."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
    HyperliquidApiTokenWithdrawalRequest,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_account_request_builder import (
    HyperliquidAccountRequestBuilder,
)
from cyberdelta.apis.models.service_args_models import (
    TransferL2UsdArgs,
    WithdrawL1Args,
)


class TestHyperliquidAccountRequestBuilder:
    """Tests for HyperliquidAccountRequestBuilder transfer functionality."""

    @pytest.fixture
    def builder(self) -> HyperliquidAccountRequestBuilder:
        """Create a HyperliquidAccountRequestBuilder instance."""
        return HyperliquidAccountRequestBuilder()

    @pytest.fixture
    def valid_wallet_address(self) -> str:
        """Provide a valid wallet address."""
        return "0x1234567890abcdef1234567890abcdef12345678"

    @pytest.fixture
    def valid_destination_address(self) -> str:
        """Provide a valid destination wallet address."""
        return "0xabcdef1234567890abcdef1234567890abcdef12"

    def test_build_withdraw_payload(
        self,
        builder: HyperliquidAccountRequestBuilder,
        valid_destination_address: str,
    ) -> None:
        """Test building withdraw payload."""
        args = WithdrawL1Args(
            asset="USDC",
            amount=Decimal("100.0"),
            destination_address=valid_destination_address,
        )

        payload = builder.build_withdrawal_payload(args)

        assert isinstance(
            payload, (HyperliquidApiEthWithdrawalRequest, HyperliquidApiTokenWithdrawalRequest)
        )
        # Test passes if we can build the payload successfully

    def test_build_usd_transfer_payload(
        self,
        builder: HyperliquidAccountRequestBuilder,
        valid_destination_address: str,
    ) -> None:
        """Test building USD transfer payload."""
        args = TransferL2UsdArgs(
            destination_address=valid_destination_address,
            amount=Decimal("50.0"),
        )

        payload = builder.build_l2_usd_transfer_payload(args)

        assert isinstance(payload, HyperliquidApiL2UsdTransferRequest)
        # Test passes if we can build the payload successfully

    def test_build_withdraw_payload_large_amount(
        self,
        builder: HyperliquidAccountRequestBuilder,
        valid_destination_address: str,
    ) -> None:
        """Test building withdraw payload with large amount."""
        args = WithdrawL1Args(
            asset="USDC",
            destination_address=valid_destination_address,
            amount=Decimal("1000000.123456"),
        )

        payload = builder.build_withdrawal_payload(args)

        assert isinstance(
            payload, (HyperliquidApiEthWithdrawalRequest, HyperliquidApiTokenWithdrawalRequest)
        )
        # Test passes if we can build the payload successfully

    def test_build_usd_transfer_payload_small_amount(
        self,
        builder: HyperliquidAccountRequestBuilder,
        valid_destination_address: str,
    ) -> None:
        """Test building USD transfer payload with small amount."""
        args = TransferL2UsdArgs(
            destination_address=valid_destination_address,
            amount=Decimal("0.01"),
        )

        payload = builder.build_l2_usd_transfer_payload(args)

        assert isinstance(payload, HyperliquidApiL2UsdTransferRequest)
        # Test passes if we can build the payload successfully
