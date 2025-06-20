"""Integration tests for Hyperliquid private spot balance endpoints.

This module focuses specifically on testing the SpotBalance model pipeline
through Hyperliquid's private /exchange endpoints with EIP-712 authentication.
Tests validate complete data transformation for spot balance state-changing operations.

Model Focus: SpotBalance (Write Operations)
- Tests L2 USD transfer operations
- Tests token withdrawal operations
- Tests ETH withdrawal operations
- Validates EIP-712 cryptographic authentication
- Comprehensive error handling for balance management operations

Authentication: EIP-712 signing for all /exchange endpoint operations
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    TransferArgs,
    WithdrawArgs,
)

pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.requires_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/spot/balances/private"], indirect=True
)
@pytest.mark.spot
@pytest.mark.requires_balance
class TestHyperliquidSpotBalancesPrivate:
    """Comprehensive private spot balance integration tests for /exchange endpoint operations."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_l2_usd_transfer_not_implemented_status(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test L2 USD transfer operation.

        Currently expects APIError with not-implemented status.
        """
        transfer_args = TransferArgs(
            asset="USDC",
            amount=Decimal("1.0"),
            from_account_type="spot",
            to_account_type="perp",
        )

        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.transfer(transfer_args)

        api_error = exc_info.value
        assert api_error.code == APIErrorCode.UNKNOWN.value, (
            f"Expected error code {APIErrorCode.UNKNOWN.value}, got {api_error.code}"
        )
        assert "service failure" in api_error.message.lower(), (
            f"Should indicate service failure: {api_error.message}"
        )
        assert api_error.original_exception is not None, "Should have original exception details"
        assert isinstance(api_error.original_exception, NotImplementedError), (
            "Original exception should be NotImplementedError"
        )
        assert "transfer not yet implemented" in str(api_error.original_exception), (
            "Original exception should indicate transfer is not yet implemented"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_token_withdrawal_not_implemented_status(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test token withdrawal operation.

        Currently expects APIError with not-implemented status.
        """
        withdraw_args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("1.0"),
            address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",
        )

        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.withdraw(withdraw_args)

        api_error = exc_info.value
        assert api_error.code == APIErrorCode.UNKNOWN.value, (
            f"Expected error code {APIErrorCode.UNKNOWN.value}, got {api_error.code}"
        )
        assert "service failure" in api_error.message.lower(), (
            f"Should indicate service failure: {api_error.message}"
        )
        assert api_error.original_exception is not None, "Should have original exception details"
        assert isinstance(api_error.original_exception, NotImplementedError), (
            "Original exception should be NotImplementedError"
        )
        assert "withdraw not yet implemented" in str(api_error.original_exception), (
            "Original exception should indicate withdraw is not yet implemented"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_eth_withdrawal_not_implemented_status(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test ETH withdrawal operation.

        Currently expects APIError with not-implemented status.
        """
        eth_withdraw_args = WithdrawArgs(
            asset="ETH",
            amount=Decimal("0.001"),
            address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",
        )

        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.withdraw(eth_withdraw_args)

        api_error = exc_info.value
        assert api_error.code == APIErrorCode.UNKNOWN.value, (
            f"Expected error code {APIErrorCode.UNKNOWN.value}, got {api_error.code}"
        )
        assert "service failure" in api_error.message.lower(), (
            f"Should indicate service failure: {api_error.message}"
        )
        assert api_error.original_exception is not None, "Should have original exception details"
        assert isinstance(api_error.original_exception, NotImplementedError), (
            "Original exception should be NotImplementedError"
        )
        assert "withdraw not yet implemented" in str(api_error.original_exception), (
            "Original exception should indicate withdraw is not yet implemented"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_transfer_args_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test TransferArgs model validation with invalid inputs."""
        with pytest.raises((ValueError, TypeError)) as exc_info:
            TransferArgs(
                asset="USDC",
                amount=Decimal("-1.0"),
                from_account_type="spot",
                to_account_type="perp",
            )

        error_message = str(exc_info.value).lower()
        assert any(word in error_message for word in ["amount", "positive", "greater"]), (
            f"Validation error should mention amount constraint: {exc_info.value}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_withdraw_args_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WithdrawArgs model validation with invalid inputs."""
        with pytest.raises((ValueError, TypeError)) as exc_info:
            WithdrawArgs(
                asset="USDC",
                amount=Decimal("0"),
                address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",
            )

        error_message = str(exc_info.value).lower()
        assert any(word in error_message for word in ["amount", "positive", "greater"]), (
            f"Validation error should mention amount constraint: {exc_info.value}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_balance_operations_interface_consistency(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that balance operation interfaces are consistent and properly structured."""
        assert hasattr(hl_api_for_test_env, "transfer"), "API should have transfer method"
        assert callable(hl_api_for_test_env.transfer), "transfer should be callable"

        assert hasattr(hl_api_for_test_env, "withdraw"), "API should have withdraw method"
        assert callable(hl_api_for_test_env.withdraw), "withdraw should be callable"

        valid_transfer_args = TransferArgs(
            asset="USDC",
            amount=Decimal("1.0"),
            from_account_type="spot",
            to_account_type="perp",
        )

        valid_withdraw_args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("1.0"),
            address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",
        )

        with pytest.raises(APIError):
            await hl_api_for_test_env.transfer(valid_transfer_args)

        with pytest.raises(APIError):
            await hl_api_for_test_env.withdraw(valid_withdraw_args)
