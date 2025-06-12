"""Integration tests for Hyperliquid private balance endpoints.

This module focuses specifically on testing the SpotBalance model pipeline
through Hyperliquid's private /exchange endpoints with EIP-712 authentication.
Tests validate complete data transformation for balance state-changing operations.

Model Focus: SpotBalance (Write Operations)
- Tests L2 USD transfer operations
- Tests token withdrawal operations
- Tests ETH withdrawal operations
- Validates EIP-712 cryptographic authentication
- Comprehensive error handling for balance management operations

Authentication: EIP-712 signing for all /exchange endpoint operations
VCR: Records both success and error responses with sensitive data filtering

Note: Some operations currently raise NotImplementedError pending full implementation
in HyperliquidAccountService. Tests are structured to be ready for when implementation
is completed.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import (
    TransferArgs,
    WithdrawArgs,
)

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/balances_private"], indirect=True
)
class TestHyperliquidBalancesPrivate:
    """Comprehensive private balance integration tests for /exchange endpoint operations.

    This class tests only /exchange endpoint operations (signed with EIP-712) that affect balances:
    - L2 USD transfer operations (between accounts on Hyperliquid L2)
    - Token withdrawal operations (to L1 addresses)
    - ETH withdrawal operations (to L1 addresses)

    These operations require cryptographic authentication and modify balance state.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_l2_usd_transfer_not_implemented_status(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test L2 USD transfer operation - currently expects NotImplementedError.

        This test validates the current status where transfer operations are defined
        but not yet fully implemented in HyperliquidAccountService.
        """
        # Define transfer parameters (testnet address and small amount)
        transfer_args = TransferArgs(
            asset="USDC",
            amount=Decimal("1.0"),  # Small amount for testnet
            from_account_type="spot",
            to_account_type="perp",
        )

        # Currently should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            await hl_api_for_test_env.transfer(transfer_args)

        # Validate the specific error message
        assert "transfer not yet implemented" in str(exc_info.value), (
            "Should indicate transfer is not yet implemented"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_token_withdrawal_not_implemented_status(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test token withdrawal operation - currently expects NotImplementedError.

        This test validates the current status where withdrawal operations are defined
        but not yet fully implemented in HyperliquidAccountService.
        """
        # Define withdrawal parameters (testnet address and small amount)
        withdraw_args = WithdrawArgs(
            asset="USDC",
            amount=Decimal("1.0"),  # Small amount for testnet
            address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",  # Sample testnet address
        )

        # Currently should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            await hl_api_for_test_env.withdraw(withdraw_args)

        # Validate the specific error message
        assert "withdraw not yet implemented" in str(exc_info.value), (
            "Should indicate withdraw is not yet implemented"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_eth_withdrawal_not_implemented_status(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test ETH withdrawal operation - currently expects NotImplementedError.

        This test validates the current status where ETH withdrawal operations are defined
        but not yet fully implemented in HyperliquidAccountService.
        """
        # Define ETH withdrawal parameters
        eth_withdraw_args = WithdrawArgs(
            asset="ETH",
            amount=Decimal("0.001"),  # Small amount for testnet
            address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",  # Sample testnet address
        )

        # Currently should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            await hl_api_for_test_env.withdraw(eth_withdraw_args)

        # Validate the specific error message
        assert "withdraw not yet implemented" in str(exc_info.value), (
            "Should indicate withdraw is not yet implemented"
        )

    # ==============================================================================
    # FUTURE IMPLEMENTATION TESTS (Currently commented out due to NotImplementedError)
    # ==============================================================================
    #
    # The following tests are structured and ready for when the transfer/withdrawal
    # operations are fully implemented in HyperliquidAccountService. They can be
    # uncommented and activated once the NotImplementedError is removed.

    # @pytest.mark.vcr
    # @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Awaiting transfer implementation in HyperliquidAccountService")
    # async def test_l2_usd_transfer_success_comprehensive(
    #     self,
    #     hl_api_for_test_env: HyperliquidAPI,
    #     custom_vcr_config: dict[str, Any],
    # ) -> None:
    #     """Test successful L2 USD transfer with comprehensive SpotBalance validation.
    #
    #     This test validates the complete pipeline from EIP-712 authenticated transfer
    #     to balance modification and SpotBalance model validation.
    #     """
    #     # Get initial balances to establish baseline
    #     initial_balances = await hl_api_for_test_env.get_balances()
    #     initial_usdc_balance = None
    #     for balance in initial_balances:
    #         if balance.asset == "USDC":
    #             initial_usdc_balance = balance
    #             break
    #
    #     # Define transfer parameters
    #     transfer_args = TransferArgs(
    #         asset="USDC",
    #         amount=Decimal("10.0"),  # Moderate amount for testnet
    #         from_account_type="spot",
    #         to_account_type="perp",
    #         client_transfer_id="test_transfer_001",
    #     )
    #
    #     # Execute the transfer
    #     transfer_result = await hl_api_for_test_env.transfer(transfer_args)
    #
    #     # Validate transfer result
    #     assert transfer_result.status in ["completed", "pending"], (
    #         f"Transfer should complete or be pending, got {transfer_result.status}"
    #     )
    #     assert transfer_result.amount == Decimal("10.0"), (
    #         f"Transfer amount should match request, got {transfer_result.amount}"
    #     )
    #     assert transfer_result.asset == "USDC", (
    #         f"Transfer asset should match request, got {transfer_result.asset}"
    #     )
    #
    #     # Get updated balances to validate impact
    #     updated_balances = await hl_api_for_test_env.get_balances()
    #     updated_usdc_balance = None
    #     for balance in updated_balances:
    #         if balance.asset == "USDC":
    #             updated_usdc_balance = balance
    #             break
    #
    #     # Validate SpotBalance model consistency
    #     if updated_usdc_balance is not None:
    #         assert isinstance(updated_usdc_balance, SpotBalance), (
    #             "Balance should be SpotBalance instance"
    #         )
    #         assert updated_usdc_balance.exchange == "hyperliquid", (
    #             f"Exchange should be 'hyperliquid', got {updated_usdc_balance.exchange}"
    #         )
    #         assert isinstance(updated_usdc_balance.available_balance, Decimal), (
    #             f"available_balance must be Decimal, got "
    #             f"{type(updated_usdc_balance.available_balance)}"
    #         )

    # @pytest.mark.vcr
    # @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Awaiting withdrawal implementation in HyperliquidAccountService")
    # async def test_token_withdrawal_success_comprehensive(
    #     self,
    #     hl_api_for_test_env: HyperliquidAPI,
    #     custom_vcr_config: dict[str, Any],
    # ) -> None:
    #     """Test successful token withdrawal with comprehensive SpotBalance validation.
    #
    #     This test validates the complete pipeline from EIP-712 authenticated withdrawal
    #     to balance reduction and SpotBalance model validation.
    #     """
    #     # Get initial balances to establish baseline
    #     initial_balances = await hl_api_for_test_env.get_balances()
    #     initial_usdc_balance = None
    #     for balance in initial_balances:
    #         if balance.asset == "USDC":
    #             initial_usdc_balance = balance
    #             break
    #
    #     # Skip if insufficient balance for withdrawal
    #     if (initial_usdc_balance is None or
    #         initial_usdc_balance.available_balance < Decimal("5.0")):
    #         pytest.skip("Insufficient USDC balance for withdrawal test")
    #
    #     # Define withdrawal parameters
    #     withdraw_args = WithdrawArgs(
    #         asset="USDC",
    #         amount=Decimal("1.0"),  # Small amount for testnet
    #         address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",  # Sample testnet address
    #         client_withdrawal_id="test_withdrawal_001",
    #     )
    #
    #     # Execute the withdrawal
    #     withdrawal_result = await hl_api_for_test_env.withdraw(withdraw_args)
    #
    #     # Validate withdrawal result
    #     assert withdrawal_result.status in ["completed", "pending", "processing"], (
    #         f"Withdrawal should have valid status, got {withdrawal_result.status}"
    #     )
    #     assert withdrawal_result.amount == Decimal("1.0"), (
    #         f"Withdrawal amount should match request, got {withdrawal_result.amount}"
    #     )
    #     assert withdrawal_result.asset == "USDC", (
    #         f"Withdrawal asset should match request, got {withdrawal_result.asset}"
    #     )
    #
    #     # Get updated balances to validate impact
    #     updated_balances = await hl_api_for_test_env.get_balances()
    #     updated_usdc_balance = None
    #     for balance in updated_balances:
    #         if balance.asset == "USDC":
    #             updated_usdc_balance = balance
    #             break
    #
    #     # Validate balance reduction
    #     if updated_usdc_balance is not None and initial_usdc_balance is not None:
    #         expected_balance = initial_usdc_balance.available_balance - Decimal("1.0")
    #         assert updated_usdc_balance.available_balance <= expected_balance, (
    #             f"Balance should be reduced after withdrawal: "
    #             f"initial={initial_usdc_balance.available_balance}, "
    #             f"updated={updated_usdc_balance.available_balance}"
    #         )

    # @pytest.mark.vcr
    # @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Awaiting withdrawal implementation in HyperliquidAccountService")
    # async def test_eth_withdrawal_success_comprehensive(
    #     self,
    #     hl_api_for_test_env: HyperliquidAPI,
    #     custom_vcr_config: dict[str, Any],
    # ) -> None:
    #     """Test successful ETH withdrawal with comprehensive SpotBalance validation."""
    #     # Similar structure to token withdrawal test, but for ETH
    #     # ... implementation details when service methods are ready

    # @pytest.mark.vcr
    # @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Awaiting transfer implementation in HyperliquidAccountService")
    # async def test_l2_transfer_insufficient_balance_error(
    #     self,
    #     hl_api_for_test_env: HyperliquidAPI,
    #     custom_vcr_config: dict[str, Any],
    # ) -> None:
    #     """Test L2 transfer with insufficient balance error."""
    #     # Create transfer with unrealistically large amount to trigger insufficient balance
    #     large_transfer_args = TransferArgs(
    #         asset="USDC",
    #         amount=Decimal("999999999.0"),  # Unrealistically large for testnet
    #         from_account_type="spot",
    #         to_account_type="perp",
    #     )
    #
    #     # Should raise APIError with INSUFFICIENT_FUNDS code
    #     with pytest.raises(APIError) as exc_info:
    #         await hl_api_for_test_env.transfer(large_transfer_args)
    #
    #     # Validate error mapping
    #     api_error = exc_info.value
    #     assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
    #         f"Should map to INSUFFICIENT_FUNDS, got {api_error.code}"
    #     )

    # @pytest.mark.vcr
    # @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Awaiting withdrawal implementation in HyperliquidAccountService")
    # async def test_withdrawal_invalid_address_error(
    #     self,
    #     hl_api_for_test_env: HyperliquidAPI,
    #     custom_vcr_config: dict[str, Any],
    # ) -> None:
    #     """Test withdrawal with invalid destination address error."""
    #     # Create withdrawal with invalid address format
    #     invalid_address_args = WithdrawArgs(
    #         asset="USDC",
    #         amount=Decimal("1.0"),
    #         address="invalid_address_format",  # Invalid Ethereum address
    #     )
    #
    #     # Should raise APIError with appropriate error code
    #     with pytest.raises(APIError) as exc_info:
    #         await hl_api_for_test_env.withdraw(invalid_address_args)
    #
    #     # Validate error structure
    #     api_error = exc_info.value
    #     assert api_error.code in [
    #         APIErrorCode.INVALID_ADDRESS.value,
    #         APIErrorCode.INVALID_REQUEST.value,
    #     ], f"Should map to address-related error code, got {api_error.code}"

    # @pytest.mark.vcr
    # @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Awaiting implementation in HyperliquidAccountService")
    # async def test_balance_precision_edge_cases(
    #     self,
    #     hl_api_for_test_env: HyperliquidAPI,
    #     custom_vcr_config: dict[str, Any],
    # ) -> None:
    #     """Test balance operations with edge cases around decimal precision."""
    #     # Test very small transfer amount
    #     small_transfer_args = TransferArgs(
    #         asset="USDC",
    #         amount=Decimal("0.000001"),  # Very small amount
    #         from_account_type="spot",
    #         to_account_type="perp",
    #     )
    #
    #     try:
    #         # Attempt small transfer
    #         transfer_result = await hl_api_for_test_env.transfer(small_transfer_args)
    #
    #         # If successful, validate precision is maintained
    #         assert transfer_result.amount == Decimal("0.000001"), (
    #             f"Small amount precision should be maintained: {transfer_result.amount}"
    #         )
    #
    #         # Validate that small amounts maintain proper decimal representation
    #         amount_str = str(transfer_result.amount)
    #         assert "E" not in amount_str.upper() or "E-" in amount_str.upper(), (
    #             f"Scientific notation should be negative exponent if used: {amount_str}"
    #         )
    #
    #     except APIError as e:
    #         # If exchange rejects due to minimum transfer amount, that's also valid behavior
    #         if "minimum" in e.message.lower() or "amount" in e.message.lower():
    #             pytest.skip(f"Exchange has minimum transfer amount requirements: {e.message}")
    #         else:
    #             raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_transfer_args_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test TransferArgs model validation with invalid inputs.

        This test validates that the TransferArgs model properly validates
        input parameters before attempting to call the service method.
        """
        # Test with negative amount (should fail validation)
        with pytest.raises((ValueError, TypeError)) as exc_info:
            TransferArgs(
                asset="USDC",
                amount=Decimal("-1.0"),  # Negative amount should fail validation
                from_account_type="spot",
                to_account_type="perp",
            )

        # Validate that validation error is related to amount
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
        """Test WithdrawArgs model validation with invalid inputs.

        This test validates that the WithdrawArgs model properly validates
        input parameters before attempting to call the service method.
        """
        # Test with zero amount (should fail validation)
        with pytest.raises((ValueError, TypeError)) as exc_info:
            WithdrawArgs(
                asset="USDC",
                amount=Decimal("0"),  # Zero amount should fail validation
                address="0x742d35Cc6634C0532925a3b8D8F3b6B4E7c5bD92",
            )

        # Validate that validation error is related to amount
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
        """Test that balance operation interfaces are consistent and properly structured.

        This validates that the API interface methods exist and have the expected
        signatures, even if the implementation is not yet complete.
        """
        # Verify transfer method exists and has correct signature
        assert hasattr(hl_api_for_test_env, "transfer"), "API should have transfer method"
        assert callable(hl_api_for_test_env.transfer), "transfer should be callable"

        # Verify withdraw method exists and has correct signature
        assert hasattr(hl_api_for_test_env, "withdraw"), "API should have withdraw method"
        assert callable(hl_api_for_test_env.withdraw), "withdraw should be callable"

        # Test that methods accept the expected args types (should not raise TypeError)
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

        # These should raise NotImplementedError (not TypeError from wrong signature)
        with pytest.raises(NotImplementedError):
            await hl_api_for_test_env.transfer(valid_transfer_args)

        with pytest.raises(NotImplementedError):
            await hl_api_for_test_env.withdraw(valid_withdraw_args)
