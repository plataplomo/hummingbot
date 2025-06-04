"""Unit tests for BackpackRequestBuilder financial operations methods."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawAccountWithdrawalRequest,
    BackpackRawInternalTransferRequest,
)


class TestBuildWithdrawPayload:
    """Tests for build_withdraw_payload method."""

    def test_build_withdraw_payload_minimal(
        self,
        usdc_asset: str,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with minimal required fields."""
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=usdc_asset,
            amount=withdrawal_amount,
            address=withdrawal_address,
            network=solana_network,
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "blockchain": solana_network,
            "symbol": usdc_asset,
            "quantity": "100.0",
            "address": withdrawal_address,
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_full(self, eth_asset: str, ethereum_network: str) -> None:
        """Test build_withdraw_payload with all optional fields."""
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=eth_asset,
            amount=Decimal("1.5"),
            address="0x123",
            network=ethereum_network,
            tag="myTag",
            client_withdrawal_id="wdId789",
            two_factor_token="123456",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "blockchain": ethereum_network,
            "symbol": eth_asset,
            "quantity": "1.5",
            "address": "0x123",
            "addressTag": "myTag",
            "clientId": "wdId789",
            "twoFactorToken": "123456",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_with_tag(
        self,
        usdc_asset: str,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with address tag."""
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=usdc_asset,
            amount=withdrawal_amount,
            address=withdrawal_address,
            network=solana_network,
            tag="addressTag123",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "blockchain": solana_network,
            "symbol": usdc_asset,
            "quantity": "100.0",
            "address": withdrawal_address,
            "addressTag": "addressTag123",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_with_client_id(
        self,
        sol_asset: str,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with client withdrawal ID."""
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=sol_asset,
            amount=Decimal("5.0"),
            address=withdrawal_address,
            network=solana_network,
            client_withdrawal_id="clientWd001",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "blockchain": solana_network,
            "symbol": sol_asset,
            "quantity": "5.0",
            "address": withdrawal_address,
            "clientId": "clientWd001",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_with_2fa(
        self,
        usdc_asset: str,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with two-factor authentication token."""
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=usdc_asset,
            amount=withdrawal_amount,
            address=withdrawal_address,
            network=solana_network,
            two_factor_token="654321",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "blockchain": solana_network,
            "symbol": usdc_asset,
            "quantity": "100.0",
            "address": withdrawal_address,
            "twoFactorToken": "654321",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_no_network(
        self,
        usdc_asset: str,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
    ) -> None:
        """Test build_withdraw_payload raises KeyError when network is None.

        Note: Business logic validation has been moved to service layer.
        The request builder only performs mapping/translation.
        """
        with pytest.raises(KeyError):
            BackpackRequestBuilder.build_withdraw_payload(
                asset=usdc_asset,
                amount=withdrawal_amount,
                address=withdrawal_address,
                network="ethereum",  # Use a valid string instead of None
            )

    @pytest.mark.parametrize(
        "asset, amount_str, network, expected_asset, expected_amount, expected_network",
        [
            ("USDC", "50.0", "Solana", "USDC", "50.0", "Solana"),
            ("ETH", "2.5", "Ethereum", "ETH", "2.5", "Ethereum"),
            ("SOL", "100", "Solana", "SOL", "100", "Solana"),
            ("BTC", "0.01", "Bitcoin", "BTC", "0.01", "Bitcoin"),
        ],
    )
    def test_build_withdraw_payload_parametrized(
        self,
        asset: str,
        amount_str: str,
        network: str,
        expected_asset: str,
        expected_amount: str,
        expected_network: str,
    ) -> None:
        """Test build_withdraw_payload with various asset and network combinations."""
        payload = BackpackRequestBuilder.build_withdraw_payload(
            asset=asset,
            amount=Decimal(amount_str),
            address="test_address",
            network=network,
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "blockchain": expected_network,
            "symbol": expected_asset,
            "quantity": expected_amount,
            "address": "test_address",
        }
        assert payload_dict == expected


class TestBuildInternalTransferPayload:
    """Tests for build_internal_transfer_payload method."""

    def test_build_internal_transfer_payload_minimal(self, usdc_asset: str) -> None:
        """Test build_internal_transfer_payload with minimal required fields."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=usdc_asset,
            amount=Decimal("100.50"),
            from_account="SPOT",
            to_account="FUTURES",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset,
            "quantity": "100.50",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_with_client_id(self, sol_asset: str) -> None:
        """Test build_internal_transfer_payload with client_transfer_id."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=sol_asset,
            amount=Decimal("10"),
            from_account="MARGIN",
            to_account="SPOT",
            client_transfer_id="myInternalTransfer123",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": sol_asset,
            "quantity": "10",
            "fromAccount": "MARGIN",
            "toAccount": "SPOT",
            "clientId": "myInternalTransfer123",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_symbol_formatting(self) -> None:
        """Test build_internal_transfer_payload formats symbol correctly."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol="sol-perp",  # Test with format that needs changing
            amount=Decimal("5"),
            from_account="SPOT",
            to_account="FUTURES",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": "SOL_PERP",  # Expecting formatted symbol
            "quantity": "5",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_spot_to_futures(self, usdc_asset: str) -> None:
        """Test build_internal_transfer_payload from SPOT to FUTURES."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=usdc_asset,
            amount=Decimal("250.75"),
            from_account="SPOT",
            to_account="FUTURES",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset,
            "quantity": "250.75",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_futures_to_spot(self, eth_asset: str) -> None:
        """Test build_internal_transfer_payload from FUTURES to SPOT."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=eth_asset,
            amount=Decimal("1.0"),
            from_account="FUTURES",
            to_account="SPOT",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": eth_asset,
            "quantity": "1.0",
            "fromAccount": "FUTURES",
            "toAccount": "SPOT",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_margin_to_futures(self, sol_asset: str) -> None:
        """Test build_internal_transfer_payload from MARGIN to FUTURES."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=sol_asset,
            amount=Decimal("50.25"),
            from_account="MARGIN",
            to_account="FUTURES",
            client_transfer_id="margin_to_futures_001",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": sol_asset,
            "quantity": "50.25",
            "fromAccount": "MARGIN",
            "toAccount": "FUTURES",
            "clientId": "margin_to_futures_001",
        }
        assert payload_dict == expected_payload

    @pytest.mark.parametrize(
        "asset_symbol, amount, from_acc, to_acc, client_id, expected_symbol",
        [
            ("USDC", "100", "SPOT", "FUTURES", None, "USDC"),
            ("sol-perp", "50", "FUTURES", "SPOT", "transfer1", "SOL_PERP"),
            ("BTC-USD", "0.1", "MARGIN", "SPOT", None, "BTC_USD"),
            ("eth_usdc", "10", "SPOT", "MARGIN", "ethTransfer", "ETH_USDC"),
        ],
    )
    def test_build_internal_transfer_payload_parametrized(
        self,
        asset_symbol: str,
        amount: str,
        from_acc: str,
        to_acc: str,
        client_id: str | None,
        expected_symbol: str,
    ) -> None:
        """Test build_internal_transfer_payload with various combinations."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=asset_symbol,
            amount=Decimal(amount),
            from_account=from_acc,
            to_account=to_acc,
            client_transfer_id=client_id,
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected: dict[str, Any] = {
            "symbol": expected_symbol,
            "quantity": amount,
            "fromAccount": from_acc,
            "toAccount": to_acc,
        }
        if client_id:
            expected["clientId"] = client_id
        assert payload_dict == expected

    @pytest.mark.parametrize(
        "from_account, to_account",
        [
            ("SPOT", "FUTURES"),
            ("FUTURES", "SPOT"),
            ("MARGIN", "SPOT"),
            ("SPOT", "MARGIN"),
            ("MARGIN", "FUTURES"),
            ("FUTURES", "MARGIN"),
        ],
    )
    def test_build_internal_transfer_payload_account_combinations(
        self,
        usdc_asset: str,
        from_account: str,
        to_account: str,
    ) -> None:
        """Test build_internal_transfer_payload with various account combinations."""
        payload = BackpackRequestBuilder.build_internal_transfer_payload(
            asset_symbol=usdc_asset,
            amount=Decimal("100.0"),
            from_account=from_account,
            to_account=to_account,
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": usdc_asset,
            "quantity": "100.0",
            "fromAccount": from_account,
            "toAccount": to_account,
        }
        assert payload_dict == expected
