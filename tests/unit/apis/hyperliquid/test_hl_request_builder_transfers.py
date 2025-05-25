"""Unit tests for HyperliquidRequestBuilder transfer and withdrawal functionality."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
    HyperliquidApiTokenWithdrawalRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
)

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_request_builder"]


class TestHyperliquidRequestBuilderTransfers:
    """Tests for HyperliquidRequestBuilder transfer and withdrawal functionality."""

    def test_build_l2_usd_transfer_payload(self, valid_wallet_address: str) -> None:
        """Test build_l2_usd_transfer_payload with valid inputs."""
        request_model = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address=valid_wallet_address, amount=Decimal("100.50")
        )
        assert isinstance(request_model, HyperliquidApiL2UsdTransferRequest)
        assert request_model.type == "usdTransfer"
        action = request_model.action
        assert isinstance(action, HyperliquidRawL2UsdTransferActionDetails)
        assert action.chain == "L2"
        assert isinstance(action.payload, HyperliquidRawL2UsdTransferPayload)
        assert action.payload.destination == valid_wallet_address
        assert action.payload.token == "USDC"
        assert action.payload.amount == "100.50"

    def test_build_l2_usd_transfer_payload_different_amounts(
        self, valid_wallet_address: str
    ) -> None:
        """Test build_l2_usd_transfer_payload with various decimal amounts."""
        # Test with small amount
        request_small = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address=valid_wallet_address, amount=Decimal("0.01")
        )
        assert request_small.action.payload.amount == "0.01"

        # Test with large amount
        request_large = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address=valid_wallet_address, amount=Decimal("999999.999999")
        )
        assert request_large.action.payload.amount == "999999.999999"

        # Test with integer amount
        request_int = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address=valid_wallet_address, amount=Decimal("1000")
        )
        assert request_int.action.payload.amount == "1000"

    def test_build_l2_usd_transfer_payload_invalid_input(self) -> None:
        """Test build_l2_usd_transfer_payload with invalid (empty) address."""
        with pytest.raises(
            ValueError, match="Destination address .* required for Hyperliquid L2 transfer"
        ):
            HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
                destination_address="", amount=Decimal("100")
            )

    def test_build_l2_usd_transfer_payload_invalid_whitespace_address(self) -> None:
        """Test build_l2_usd_transfer_payload with whitespace-only address."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="String cannot be empty"):
            HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
                destination_address="   ", amount=Decimal("100")
            )

    def test_build_withdrawal_payload_eth(self, valid_wallet_address: str) -> None:
        """Test build_withdrawal_payload for ETH withdrawals."""
        request_model = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="ETH", amount=Decimal("1.23"), destination_address=valid_wallet_address
        )
        assert isinstance(request_model, HyperliquidApiEthWithdrawalRequest)
        assert request_model.type == "withdrawEth"
        action = request_model.action
        assert isinstance(action, HyperliquidRawEthWithdrawalActionPayload)
        assert action.destination == valid_wallet_address
        assert action.amount == "1.23"

    def test_build_withdrawal_payload_eth_different_amounts(
        self, valid_wallet_address: str
    ) -> None:
        """Test build_withdrawal_payload for ETH with various amounts."""
        # Test with small ETH amount
        request_small = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="ETH", amount=Decimal("0.001"), destination_address=valid_wallet_address
        )
        assert request_small.action.amount == "0.001"

        # Test with precise ETH amount
        request_precise = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="ETH", amount=Decimal("2.123456789"), destination_address=valid_wallet_address
        )
        assert request_precise.action.amount == "2.123456789"

    def test_build_withdrawal_payload_token(self, valid_wallet_address: str) -> None:
        """Test build_withdrawal_payload for generic token (USDC) withdrawals."""
        request_model = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="USDC", amount=Decimal("500"), destination_address=valid_wallet_address
        )
        assert isinstance(request_model, HyperliquidApiTokenWithdrawalRequest)
        assert request_model.type == "withdraw"
        action = request_model.action
        assert isinstance(action, HyperliquidRawWithdrawalToL1ActionPayload)
        assert action.token == "USDC"
        assert action.amount == "500"
        assert action.destination == valid_wallet_address

    def test_build_withdrawal_payload_various_tokens(self, valid_wallet_address: str) -> None:
        """Test build_withdrawal_payload for various token types."""
        # Test with USDT
        request_usdt = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="USDT", amount=Decimal("1000.50"), destination_address=valid_wallet_address
        )
        assert isinstance(request_usdt.action, HyperliquidRawWithdrawalToL1ActionPayload)
        assert request_usdt.action.token == "USDT"
        assert request_usdt.action.amount == "1000.50"

        # Test with arbitrary token
        request_arb = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="ARB", amount=Decimal("25.75"), destination_address=valid_wallet_address
        )
        assert isinstance(request_arb.action, HyperliquidRawWithdrawalToL1ActionPayload)
        assert request_arb.action.token == "ARB"
        assert request_arb.action.amount == "25.75"

    def test_build_withdrawal_payload_invalid_input(self) -> None:
        """Test build_withdrawal_payload with invalid (empty) address."""
        with pytest.raises(ValueError, match="Destination address is required for withdrawal"):
            HyperliquidRequestBuilder.build_withdrawal_payload(
                asset="USDC", amount=Decimal("100"), destination_address=""
            )

    def test_build_withdrawal_payload_invalid_whitespace_address(self) -> None:
        """Test build_withdrawal_payload with whitespace-only address."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="String cannot be empty"):
            HyperliquidRequestBuilder.build_withdrawal_payload(
                asset="USDC", amount=Decimal("100"), destination_address="   "
            )

    def test_build_withdrawal_payload_case_sensitivity(self, valid_wallet_address: str) -> None:
        """Test that asset case is handled correctly in withdrawal payloads."""
        # Test ETH (uppercase) creates ETH withdrawal
        request_eth_upper = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="ETH", amount=Decimal("1.0"), destination_address=valid_wallet_address
        )
        assert isinstance(request_eth_upper, HyperliquidApiEthWithdrawalRequest)
        assert request_eth_upper.type == "withdrawEth"

        # Test with lowercase eth - the request builder converts to uppercase,
        # so it also creates ETH withdrawal
        request_eth_lower = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="eth", amount=Decimal("1.0"), destination_address=valid_wallet_address
        )
        assert isinstance(request_eth_lower, HyperliquidApiEthWithdrawalRequest)
        assert request_eth_lower.type == "withdrawEth"

        # Test with a different token to ensure token withdrawal works
        request_usdc = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="USDC", amount=Decimal("1.0"), destination_address=valid_wallet_address
        )
        assert isinstance(request_usdc, HyperliquidApiTokenWithdrawalRequest)
        assert request_usdc.type == "withdraw"
        assert isinstance(request_usdc.action, HyperliquidRawWithdrawalToL1ActionPayload)
        assert request_usdc.action.token == "USDC"
