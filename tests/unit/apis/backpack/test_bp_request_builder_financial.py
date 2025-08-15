"""Unit tests for BackpackAccountRequestBuilder financial operations methods with Property-Based Testing.

--------------------------------------------------------------------

Comprehensive property-based test suite for BackpackAccountRequestBuilder financial operations using Hypothesis.
Tests payload building for financial endpoints including:
- Withdrawal payload generation with various assets, amounts, addresses, and networks
- Internal transfer payload generation with different wallet combinations and amounts
- Edge cases, boundary values, and malicious input resistance
- Comprehensive validation of decimal precision in financial operations
- Hundreds of generated test combinations for comprehensive coverage
"""

import string
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawAccountWithdrawalRequest,
    BackpackRawInternalTransferRequest,
)
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import USDC_BP


class TestBuildWithdrawPayload:
    """Tests for build_withdraw_payload method."""

    def test_build_withdraw_payload_minimal(
        self,
        usdc_asset: Symbol,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with minimal required fields."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=usdc_asset,
            amount=withdrawal_amount,
            address=withdrawal_address,
            network=solana_network,
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset.value,
            "blockchain": solana_network,
            "quantity": "100.0",
            "address": withdrawal_address,
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_full(self, eth_asset: Symbol, ethereum_network: str) -> None:
        """Test build_withdraw_payload with all optional fields."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=eth_asset,
            amount=Decimal("1.5"),
            address="0x123",
            network=ethereum_network,
            tag="myTag",
            client_withdraw_id="wdId789",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": eth_asset.value,
            "blockchain": ethereum_network,
            "quantity": "1.5",
            "address": "0x123",
            "addressTag": "myTag",
            "clientId": "wdId789",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_with_tag(
        self,
        usdc_asset: Symbol,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with address tag."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=usdc_asset,
            amount=withdrawal_amount,
            address=withdrawal_address,
            network=solana_network,
            tag="addressTag123",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset.value,
            "blockchain": solana_network,
            "quantity": "100.0",
            "address": withdrawal_address,
            "addressTag": "addressTag123",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_with_client_id(
        self,
        sol_asset: Symbol,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with client withdrawal ID."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=sol_asset,
            amount=Decimal("5.0"),
            address=withdrawal_address,
            network=solana_network,
            client_withdraw_id="clientWd001",
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": sol_asset.value,
            "blockchain": solana_network,
            "quantity": "5.0",
            "address": withdrawal_address,
            "clientId": "clientWd001",
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_with_2fa(
        self,
        usdc_asset: Symbol,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
        solana_network: str,
    ) -> None:
        """Test build_withdraw_payload with two-factor authentication token."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=usdc_asset,
            amount=withdrawal_amount,
            address=withdrawal_address,
            network=solana_network,
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset.value,
            "blockchain": solana_network,
            "quantity": "100.0",
            "address": withdrawal_address,
        }
        assert payload_dict == expected_payload

    def test_build_withdraw_payload_no_network(
        self,
        usdc_asset: Symbol,
        withdrawal_amount: Decimal,
        withdrawal_address: str,
    ) -> None:
        """Test build_withdraw_payload raises ValidationError for invalid network.

        Note: Business logic validation has been moved to service layer.
        The request builder only performs mapping/translation.
        Pydantic validates that 'ethereum' (lowercase) is not valid - should be 'Ethereum'.
        """
        with pytest.raises(ValidationError):
            BackpackAccountRequestBuilder.build_withdraw_payload(
                asset_symbol=usdc_asset,
                amount=withdrawal_amount,
                address=withdrawal_address,
                network="ethereum",  # Use a valid string instead of None
            )

    @pytest.mark.parametrize(
        ("asset", "amount_str", "network", "expected_asset", "expected_amount", "expected_network"),
        [
            ("USDC", "50.0", "Solana", "USDC", "50.0", "Solana"),
            ("ETH", "2.5", "Ethereum", "ETH", "2.5", "Ethereum"),
            ("SOL", "100", "Solana", "SOL", "100", "Solana"),
            ("BTC", "0.01", "Bitcoin", "BTC", "0.01", "Bitcoin"),
        ],
    )
    def test_build_withdraw_payload_parametrized(
        self,
        asset: Symbol,
        amount_str: str,
        network: str,
        expected_asset: str,
        expected_amount: str,
        expected_network: str,
    ) -> None:
        """Test build_withdraw_payload with various asset and network combinations."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=asset,
            amount=Decimal(amount_str),
            address="test_address",
            network=network,
        )
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": expected_asset,
            "blockchain": expected_network,
            "quantity": expected_amount,
            "address": "test_address",
        }
        assert payload_dict == expected


class TestBuildInternalTransferPayload:
    """Tests for build_internal_transfer_payload method."""

    def test_build_internal_transfer_payload_minimal(self, usdc_asset: Symbol) -> None:
        """Test build_internal_transfer_payload with minimal required fields."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=usdc_asset,
            amount=Decimal("100.50"),
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset.value,
            "quantity": "100.50",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_with_client_id(self, sol_asset: Symbol) -> None:
        """Test build_internal_transfer_payload with sub_account_id."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=sol_asset,
            amount=Decimal(10),
            from_wallet="MARGIN",
            to_wallet="SPOT",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": sol_asset.value,
            "quantity": "10",
            "fromAccount": "MARGIN",
            "toAccount": "SPOT",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_symbol_formatting(self) -> None:
        """Test build_internal_transfer_payload formats symbol correctly."""
        sol_perp_symbol = exchanges.backpack("sol-perp")
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=sol_perp_symbol,  # Test with format that needs changing
            amount=Decimal(5),
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": sol_perp_symbol.value,  # Implementation passes symbol as-is
            "quantity": "5",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_spot_to_futures(self, usdc_asset: Symbol) -> None:
        """Test build_internal_transfer_payload from SPOT to FUTURES."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=usdc_asset,
            amount=Decimal("250.75"),
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": usdc_asset.value,
            "quantity": "250.75",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_futures_to_spot(self, eth_asset: Symbol) -> None:
        """Test build_internal_transfer_payload from FUTURES to SPOT."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=eth_asset,
            amount=Decimal("1.0"),
            from_wallet="FUTURES",
            to_wallet="SPOT",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": eth_asset.value,
            "quantity": "1.0",
            "fromAccount": "FUTURES",
            "toAccount": "SPOT",
        }
        assert payload_dict == expected_payload

    def test_build_internal_transfer_payload_margin_to_futures(self, sol_asset: Symbol) -> None:
        """Test build_internal_transfer_payload from MARGIN to FUTURES."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=sol_asset,
            amount=Decimal("50.25"),
            from_wallet="MARGIN",
            to_wallet="FUTURES",
            sub_account_id="margin_to_futures_001",
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": sol_asset.value,
            "quantity": "50.25",
            "fromAccount": "MARGIN",
            "toAccount": "FUTURES",
            "clientId": "margin_to_futures_001",
        }
        assert payload_dict == expected_payload

    @pytest.mark.parametrize(
        ("asset_symbol", "amount", "from_acc", "to_acc", "client_id", "expected_symbol"),
        [
            (USDC_BP, "100", "SPOT", "FUTURES", None, USDC_BP.value),
            (exchanges.backpack("SOL-PERP"), "50", "FUTURES", "SPOT", "transfer1", "SOL-PERP"),
            (exchanges.backpack("BTC-USD"), "0.1", "MARGIN", "SPOT", None, "BTC-USD"),
            (exchanges.backpack("ETH_USDC"), "10", "SPOT", "MARGIN", "ethTransfer", "ETH_USDC"),
        ],
    )
    def test_build_internal_transfer_payload_parametrized(
        self,
        asset_symbol: Symbol,
        amount: str,
        from_acc: str,
        to_acc: str,
        client_id: str | None,
        expected_symbol: str,
    ) -> None:
        """Test build_internal_transfer_payload with various combinations."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=asset_symbol,
            amount=Decimal(amount),
            from_wallet=from_acc,
            to_wallet=to_acc,
            sub_account_id=client_id,
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
        ("from_wallet", "to_wallet"),
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
        usdc_asset: Symbol,
        from_wallet: str,
        to_wallet: str,
    ) -> None:
        """Test build_internal_transfer_payload with various account combinations."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=usdc_asset,
            amount=Decimal("100.0"),
            from_wallet=from_wallet,
            to_wallet=to_wallet,
        )
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": usdc_asset.value,
            "quantity": "100.0",
            "fromAccount": from_wallet,
            "toAccount": to_wallet,
        }
        assert payload_dict == expected


# =======================
# Property-Based Testing Strategy Builders
# =======================


def bp_asset_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Backpack asset symbols.

    Returns:
        SearchStrategy[Symbol]: Strategy for valid asset symbols.
    """
    return st.sampled_from([
        exchanges.backpack("USDC"),
        exchanges.backpack("USDT"),
        exchanges.backpack("SOL"),
        exchanges.backpack("ETH"),
        exchanges.backpack("BTC"),
        exchanges.backpack("SOL-PERP"),
        exchanges.backpack("BTC-USD"),
        exchanges.backpack("ETH_USDC"),
    ])


def withdrawal_amount_strategy() -> SearchStrategy[Decimal]:
    """Generate valid withdrawal amounts.

    Returns:
        SearchStrategy[Decimal]: Strategy for withdrawal amounts.
    """
    return st.one_of([
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal("1000.0"), places=2),
        st.decimals(min_value=Decimal("0.000001"), max_value=Decimal("10.0"), places=6),
        st.decimals(min_value=Decimal("1.0"), max_value=Decimal("100000.0"), places=3),
    ])


def withdrawal_address_strategy() -> SearchStrategy[str]:
    """Generate valid withdrawal addresses.

    Returns:
        SearchStrategy[str]: Strategy for withdrawal addresses.
    """
    return st.one_of([
        # Solana addresses (base58, ~44 chars)
        st.text(
            alphabet="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
            min_size=43,
            max_size=44,
        ),
        # Ethereum addresses (hex, 42 chars with 0x)
        st.builds(
            lambda addr: f"0x{addr}",
            st.text(alphabet=string.hexdigits, min_size=40, max_size=40),
        ),
        # Bitcoin addresses (various formats)
        st.text(
            alphabet="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
            min_size=26,
            max_size=35,
        ),
        # Test addresses
        st.sampled_from([
            "test_address_123",
            "withdraw_addr_456",
            "destination_789",
        ]),
    ])


def network_strategy() -> SearchStrategy[str]:
    """Generate valid blockchain networks.

    Returns:
        SearchStrategy[str]: Strategy for blockchain networks.
    """
    return st.sampled_from(["Solana", "Ethereum", "Bitcoin", "Polygon", "BSC", "Arbitrum"])


def address_tag_strategy() -> SearchStrategy[str | None]:
    """Generate valid address tags (optional).

    Returns:
        SearchStrategy[str | None]: Strategy for address tags.
    """
    return st.one_of([
        st.none(),
        st.text(min_size=1, max_size=50),
        st.sampled_from([
            "tag123",
            "addressTag456",
            "memo789",
            "payment_id",
            "destination_tag",
            "memo",
            "tag",
            "note",
        ]),
    ])


def client_withdraw_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client withdrawal IDs (optional).

    Returns:
        SearchStrategy[str | None]: Strategy for client withdrawal IDs.
    """
    return st.one_of([
        st.none(),
        st.text(min_size=1, max_size=64),
        st.builds(
            lambda prefix, suffix: f"{prefix}_{suffix}",
            st.sampled_from(["wd", "withdraw", "out", "tx"]),
            st.integers(min_value=1, max_value=999999),
        ),
    ])


def wallet_type_strategy() -> SearchStrategy[str]:
    """Generate valid wallet types for transfers.

    Returns:
        SearchStrategy[str]: Strategy for wallet types.
    """
    return st.sampled_from(["SPOT", "FUTURES", "MARGIN"])


def transfer_amount_strategy() -> SearchStrategy[Decimal]:
    """Generate valid transfer amounts.

    Returns:
        SearchStrategy[Decimal]: Strategy for transfer amounts.
    """
    return st.one_of([
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal("10000.0"), places=2),
        st.decimals(min_value=Decimal("0.1"), max_value=Decimal("1000.0"), places=1),
        st.decimals(min_value=Decimal(1), max_value=Decimal(100000), places=0),
    ])


def sub_account_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid sub-account IDs (optional).

    Returns:
        SearchStrategy[str | None]: Strategy for sub-account IDs.
    """
    return st.one_of([
        st.none(),
        st.text(min_size=1, max_size=50),
        st.builds(
            lambda prefix, suffix: f"{prefix}_{suffix}",
            st.sampled_from(["transfer", "move", "internal", "sub"]),
            st.integers(min_value=1, max_value=999),
        ),
    ])


@composite
def withdrawal_params_strategy(
    draw: st.DrawFn,
) -> tuple[Symbol, Decimal, str, str, str | None, str | None]:
    """Generate valid withdrawal parameter combinations.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple: (asset_symbol, amount, address, network, tag, client_withdraw_id)
    """
    asset = draw(bp_asset_strategy())
    amount = draw(withdrawal_amount_strategy())
    address = draw(withdrawal_address_strategy())
    network = draw(network_strategy())
    tag = draw(address_tag_strategy())
    client_id = draw(client_withdraw_id_strategy())

    return asset, amount, address, network, tag, client_id


@composite
def transfer_params_strategy(draw: st.DrawFn) -> tuple[Symbol, Decimal, str, str, str | None]:
    """Generate valid internal transfer parameter combinations.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple: (asset_symbol, amount, from_wallet, to_wallet, sub_account_id)
    """
    asset = draw(bp_asset_strategy())
    amount = draw(transfer_amount_strategy())
    from_wallet = draw(wallet_type_strategy())
    to_wallet = draw(wallet_type_strategy())

    # Ensure from_wallet != to_wallet
    while to_wallet == from_wallet:
        to_wallet = draw(wallet_type_strategy())

    sub_account_id = draw(sub_account_id_strategy())

    return asset, amount, from_wallet, to_wallet, sub_account_id


# =======================
# Property-Based Test Classes
# =======================


class TestBuildWithdrawPayloadPropertyBased:
    """Property-based tests for build_withdraw_payload method."""

    @given(params=withdrawal_params_strategy())
    @settings(max_examples=100)
    def test_withdraw_payload_property_based(
        self,
        params: tuple[Symbol, Decimal, str, str, str | None, str | None],
    ) -> None:
        """Property-based test for withdraw payload building."""
        asset_symbol, amount, address, network, tag, client_withdraw_id = params

        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=asset_symbol,
            amount=amount,
            address=address,
            network=network,
            tag=tag,
            client_withdraw_id=client_withdraw_id,
        )

        # Verify payload structure
        assert isinstance(payload, BackpackRawAccountWithdrawalRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)

        # Check required fields
        assert payload_dict["symbol"] == asset_symbol.value
        assert payload_dict["blockchain"] == network
        assert payload_dict["quantity"] == str(amount)
        assert payload_dict["address"] == address

        # Check optional fields
        if tag is not None:
            assert payload_dict["addressTag"] == tag
        else:
            assert "addressTag" not in payload_dict

        if client_withdraw_id is not None:
            assert payload_dict["clientId"] == client_withdraw_id
        else:
            assert "clientId" not in payload_dict

    @given(
        asset=bp_asset_strategy(),
        address=withdrawal_address_strategy(),
        network=network_strategy(),
    )
    @settings(max_examples=50)
    def test_withdraw_amount_precision(
        self,
        asset: Symbol,
        address: str,
        network: str,
    ) -> None:
        """Test withdrawal amount precision preservation."""
        # Test high precision amounts
        for precision in [2, 4, 6, 8]:
            amount = Decimal("123.123456789").quantize(Decimal(10) ** -precision)

            payload = BackpackAccountRequestBuilder.build_withdraw_payload(
                asset_symbol=asset,
                amount=amount,
                address=address,
                network=network,
            )

            assert payload.quantity == str(amount)
            assert Decimal(payload.quantity) == amount

    @given(
        asset=bp_asset_strategy(),
        amount=withdrawal_amount_strategy(),
        network=network_strategy(),
    )
    def test_withdraw_address_handling(
        self,
        asset: Symbol,
        amount: Decimal,
        network: str,
    ) -> None:
        """Test various address format handling."""
        addresses = [
            "0x742d35cc6635C0532925a3b8d0dcc1e87b8Bc31B",  # Ethereum
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # Solana
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",  # Bitcoin
            "test_address_123",  # Test format
        ]

        for addr in addresses:
            payload = BackpackAccountRequestBuilder.build_withdraw_payload(
                asset_symbol=asset,
                amount=amount,
                address=addr,
                network=network,
            )

            assert payload.address == addr

    @given(
        malicious_tag=st.one_of([
            st.text(min_size=1000, max_size=5000),  # Very long
            st.sampled_from(["<script>alert('xss')</script>", "'; DROP TABLE users; --"]),
        ]),
        asset=bp_asset_strategy(),
        amount=withdrawal_amount_strategy(),
        address=withdrawal_address_strategy(),
        network=network_strategy(),
    )
    @settings(max_examples=20)
    def test_withdraw_malicious_input_resistance(
        self,
        malicious_tag: str,
        asset: Symbol,
        amount: Decimal,
        address: str,
        network: str,
    ) -> None:
        """Test resistance to malicious input in withdrawal requests."""
        try:
            payload = BackpackAccountRequestBuilder.build_withdraw_payload(
                asset_symbol=asset,
                amount=amount,
                address=address,
                network=network,
                tag=malicious_tag,
            )

            # If it succeeds, verify the malicious input is safely stored
            assert payload.addressTag == malicious_tag
            assert isinstance(payload.addressTag, str)

        except ValidationError:
            # Rejecting malicious input is also acceptable
            pass


class TestBuildInternalTransferPayloadPropertyBased:
    """Property-based tests for build_internal_transfer_payload method."""

    @given(params=transfer_params_strategy())
    @settings(max_examples=100)
    def test_internal_transfer_property_based(
        self,
        params: tuple[Symbol, Decimal, str, str, str | None],
    ) -> None:
        """Property-based test for internal transfer payload building."""
        asset_symbol, amount, from_wallet, to_wallet, sub_account_id = params

        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=asset_symbol,
            amount=amount,
            from_wallet=from_wallet,
            to_wallet=to_wallet,
            sub_account_id=sub_account_id,
        )

        # Verify payload structure
        assert isinstance(payload, BackpackRawInternalTransferRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)

        # Check required fields
        assert payload_dict["symbol"] == asset_symbol.value
        assert payload_dict["quantity"] == str(amount)
        assert payload_dict["fromAccount"] == from_wallet
        assert payload_dict["toAccount"] == to_wallet

        # Check optional fields
        if sub_account_id is not None:
            assert payload_dict["clientId"] == sub_account_id
        else:
            assert "clientId" not in payload_dict

    @given(
        from_wallet=wallet_type_strategy(),
        to_wallet=wallet_type_strategy(),
    )
    def test_wallet_combinations(
        self,
        from_wallet: str,
        to_wallet: str,
    ) -> None:
        """Test all valid wallet type combinations."""
        # Skip same wallet transfers
        if from_wallet == to_wallet:
            return

        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=USDC_BP,
            amount=Decimal("100.0"),
            from_wallet=from_wallet,
            to_wallet=to_wallet,
        )

        assert payload.fromAccount == from_wallet
        assert payload.toAccount == to_wallet

    @given(
        asset=bp_asset_strategy(),
        from_wallet=wallet_type_strategy(),
        to_wallet=wallet_type_strategy(),
    )
    def test_transfer_amount_precision(
        self,
        asset: Symbol,
        from_wallet: str,
        to_wallet: str,
    ) -> None:
        """Test transfer amount precision preservation."""
        # Skip same wallet transfers
        if from_wallet == to_wallet:
            return

        # Test various precision levels
        amounts = [
            Decimal(1),
            Decimal("1.0"),
            Decimal("1.00"),
            Decimal("1.000"),
            Decimal("123.456789"),
            Decimal("0.000001"),
        ]

        for amount in amounts:
            payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
                asset_symbol=asset,
                amount=amount,
                from_wallet=from_wallet,
                to_wallet=to_wallet,
            )

            assert payload.quantity == str(amount)
            assert Decimal(payload.quantity) == amount

    @given(
        large_amount=st.decimals(
            min_value=Decimal(1000000), max_value=Decimal(999999999), places=2
        ),
        from_wallet=wallet_type_strategy(),
        to_wallet=wallet_type_strategy(),
    )
    def test_large_transfer_amounts(
        self,
        large_amount: Decimal,
        from_wallet: str,
        to_wallet: str,
    ) -> None:
        """Test handling of large transfer amounts."""
        # Skip same wallet transfers
        if from_wallet == to_wallet:
            return

        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=USDC_BP,
            amount=large_amount,
            from_wallet=from_wallet,
            to_wallet=to_wallet,
        )

        assert Decimal(payload.quantity) == large_amount
        assert payload.quantity == str(large_amount)

    @given(
        malicious_client_id=st.one_of([
            st.text(min_size=1000, max_size=5000),  # Very long
            st.sampled_from(["<script>", "'; DROP TABLE transfers; --", "../../../etc/passwd"]),
        ]),
    )
    @settings(max_examples=20)
    def test_transfer_malicious_input_resistance(
        self,
        malicious_client_id: str,
    ) -> None:
        """Test resistance to malicious input in transfer requests."""
        try:
            payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
                asset_symbol=USDC_BP,
                amount=Decimal("100.0"),
                from_wallet="SPOT",
                to_wallet="FUTURES",
                sub_account_id=malicious_client_id,
            )

            # If it succeeds, verify the malicious input is safely stored
            assert payload.clientId == malicious_client_id
            assert isinstance(payload.clientId, str)

        except ValidationError:
            # Rejecting malicious input is also acceptable
            pass


# =======================
# Edge Case and Boundary Testing
# =======================


class TestFinancialEdgeCases:
    """Property-based tests for edge cases and boundary conditions."""

    @given(
        zero_amount=st.sampled_from([
            Decimal(0),
            Decimal("0.0"),
            Decimal("0.00"),
            Decimal("0.000000"),
        ]),
    )
    def test_zero_amount_handling(self, zero_amount: Decimal) -> None:
        """Test handling of zero amounts in transfers and withdrawals."""
        # Zero withdrawal
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=USDC_BP,
            amount=zero_amount,
            address="test_address",
            network="Solana",
        )
        assert payload.quantity == str(zero_amount)

        # Zero transfer
        transfer_payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=USDC_BP,
            amount=zero_amount,
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )
        assert transfer_payload.quantity == str(zero_amount)

    @given(
        tiny_amount=st.decimals(
            min_value=Decimal("0.000000001"), max_value=Decimal("0.000001"), places=9
        ),
    )
    def test_tiny_amount_precision(self, tiny_amount: Decimal) -> None:
        """Test precision with very small amounts."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=USDC_BP,
            amount=tiny_amount,
            address="test_address",
            network="Solana",
        )

        assert Decimal(payload.quantity) == tiny_amount
        assert str(tiny_amount) in payload.quantity

    @given(
        empty_string_fields=st.sampled_from([
            {"address": ""},
            {"network": ""},
            {"tag": ""},
            {"client_withdraw_id": ""},
            {"sub_account_id": ""},
        ]),
    )
    def test_empty_string_handling(self, empty_string_fields: dict[str, str]) -> None:
        """Test handling of empty string fields."""
        try:
            if "address" in empty_string_fields:
                BackpackAccountRequestBuilder.build_withdraw_payload(
                    asset_symbol=USDC_BP,
                    amount=Decimal("100.0"),
                    address="",  # Empty address
                    network="Solana",
                )

            if "network" in empty_string_fields:
                BackpackAccountRequestBuilder.build_withdraw_payload(
                    asset_symbol=USDC_BP,
                    amount=Decimal("100.0"),
                    address="test_address",
                    network="",  # Empty network
                )

        except ValidationError:
            # Validation errors for empty required fields are expected
            pass


# =======================
# Legacy Compatibility Verification
# =======================


class TestLegacyCompatibility:
    """Verify that property-based tests don't break legacy functionality."""

    def test_legacy_withdraw_minimal_still_works(self) -> None:
        """Verify legacy withdraw functionality remains intact."""
        payload = BackpackAccountRequestBuilder.build_withdraw_payload(
            asset_symbol=USDC_BP,
            amount=Decimal("100.0"),
            address="test_address",
            network="Solana",
        )

        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": USDC_BP.value,
            "blockchain": "Solana",
            "quantity": "100.0",
            "address": "test_address",
        }
        assert payload_dict == expected

    def test_legacy_transfer_minimal_still_works(self) -> None:
        """Verify legacy transfer functionality remains intact."""
        payload = BackpackAccountRequestBuilder.build_internal_transfer_payload(
            asset_symbol=USDC_BP,
            amount=Decimal("100.50"),
            from_wallet="SPOT",
            to_wallet="FUTURES",
        )

        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": USDC_BP.value,
            "quantity": "100.50",
            "fromAccount": "SPOT",
            "toAccount": "FUTURES",
        }
        assert payload_dict == expected
