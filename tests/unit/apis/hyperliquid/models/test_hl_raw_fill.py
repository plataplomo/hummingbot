"""Property-based tests for Hyperliquid Raw Fill models.

This module tests the critical Hyperliquid raw fill models for API boundary validation to ensure:
- Robust field validation for all fill (trade execution) data from external APIs
- Financial precision preservation in all price/quantity/fee fields
- Alias mapping consistency for API field naming conventions
- Type safety and boundary validation for all field types
- Serialization round-trip properties for API data integrity
- Edge case handling for malformed or corrupted API responses

SECURITY CRITICAL: Raw fill model errors could allow malformed external data
to enter the trading system, leading to incorrect trade accounting, invalid PnL
calculations, or financial losses.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill


# =============================================================================
# HYPOTHESIS STRATEGIES FOR HYPERLIQUID RAW FILL TESTING
# =============================================================================


def valid_string_strategy(max_length: int = 64) -> SearchStrategy[str]:
    """Generate valid non-empty strings with reasonable length.

    Returns:
        A Hypothesis strategy that generates valid non-empty strings.
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_."),
        min_size=1,
        max_size=max_length,
    ).filter(lambda x: len(x.strip()) > 0)


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts.

    Returns:
        A Hypothesis strategy that generates valid decimal strings for prices and amounts.
    """
    return st.one_of([
        # Normal decimal values
        st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal(1000000),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Common edge cases
        st.just("0.00000001"),
        st.just("999999.99999999"),
        st.just("1.0"),
        st.just("100"),
        st.just("50000.0"),
    ])


def position_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid position sizes (can be negative for shorts).

    Returns:
        A Hypothesis strategy that generates position size strings (positive or negative).
    """
    return st.one_of([
        st.decimals(
            min_value=Decimal(-100000),
            max_value=Decimal(100000),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        st.just("0"),
        st.just("-1.0"),
        st.just("1.0"),
        st.just("-100.5"),
        st.just("100.5"),
    ])


def fee_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid fee amounts (typically smaller positive values).

    Returns:
        A Hypothesis strategy that generates fee amount strings.
    """
    return st.one_of([
        st.decimals(
            min_value=Decimal(0),
            max_value=Decimal(100),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        st.just("0"),
        st.just("0.001"),
        st.just("0.1"),
        st.just("1.0"),
    ])


def trade_id_strategy() -> SearchStrategy[int]:
    """Generate valid trade/order IDs.

    Returns:
        A Hypothesis strategy that generates valid trade/order ID integers.
    """
    return st.integers(min_value=0, max_value=2**31 - 1)


def timestamp_ms_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp in milliseconds.

    Returns:
        A Hypothesis strategy that generates valid millisecond timestamps.
    """
    # Unix timestamps in milliseconds for years 2020-2030
    return st.integers(
        min_value=1577836800000,  # 2020-01-01
        max_value=1893456000000,  # 2030-01-01
    )


def coin_strategy() -> SearchStrategy[str]:
    """Generate valid coin/asset identifiers.

    Returns:
        A Hypothesis strategy that generates valid cryptocurrency symbols.
    """
    return st.sampled_from([
        "BTC",
        "ETH",
        "SOL",
        "MATIC",
        "ARB",
        "OP",
        "AVAX",
        "ATOM",
        "DOGE",
        "LTC",
        "BCH",
    ])


def side_strategy() -> SearchStrategy[str]:
    """Generate valid fill sides (Hyperliquid format).

    Returns:
        A Hypothesis strategy that generates valid order sides ('B' or 'A').
    """
    return st.sampled_from(["B", "A"])  # Buy, Ask/Sell


def direction_strategy() -> SearchStrategy[str]:
    """Generate valid direction descriptions.

    Returns:
        A Hypothesis strategy that generates valid trade direction strings.
    """
    return st.sampled_from([
        "Open Long",
        "Close Long",
        "Open Short",
        "Close Short",
        "Increase Long",
        "Decrease Long",
        "Increase Short",
        "Decrease Short",
    ])


def tx_hash_strategy() -> SearchStrategy[str]:
    """Generate valid transaction hashes.

    Returns:
        A Hypothesis strategy that generates valid blockchain transaction hashes.
    """
    return st.text(alphabet="0123456789abcdef", min_size=32, max_size=32).map(lambda x: f"0x{x}")


def cloid_strategy() -> SearchStrategy[str | None]:
    """Generate valid client order IDs (Hyperliquid format).

    Returns:
        A Hypothesis strategy that generates valid client order IDs or None.
    """
    return st.one_of([
        st.none(),
        # Hex format cloids
        st.text(alphabet="0123456789abcdef", min_size=8, max_size=16).map(lambda x: f"0x{x}"),
        # User-defined string cloids
        valid_string_strategy(max_length=128),
    ])


def required_fill_fields_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate required fields for a valid fill.

    Returns:
        A Hypothesis strategy that generates dictionaries with all required fill fields.
    """
    return st.fixed_dictionaries({
        "tid": trade_id_strategy(),
        "oid": trade_id_strategy(),
        "coin": coin_strategy(),
        "px": financial_decimal_string_strategy(),
        "sz": financial_decimal_string_strategy(),
        "startPosition": position_decimal_string_strategy(),
        "fee": fee_decimal_string_strategy(),
        "time": timestamp_ms_strategy(),
        "side": side_strategy(),
        "dir": direction_strategy(),
        "hash": tx_hash_strategy(),
        "isMaker": st.booleans(),
    })


def optional_fill_fields_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate optional fields for fills.

    Returns:
        A Hypothesis strategy that generates dictionaries with optional fill fields.
    """
    return st.fixed_dictionaries({
        "liquidationMarkPx": st.one_of(st.none(), financial_decimal_string_strategy()),
        "cloid": cloid_strategy(),
    })


def complete_fill_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate complete fill data with both required and optional fields.

    Returns:
        A Hypothesis strategy that generates complete fill data dictionaries.
    """

    def merge_dicts(req: dict[str, Any], opt: dict[str, Any]) -> dict[str, Any]:
        return {**req, **opt}

    return st.builds(
        merge_dicts,
        req=required_fill_fields_strategy(),
        opt=optional_fill_fields_strategy(),
    )


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW FILL
# =============================================================================


class TestHyperliquidRawFillProperties:
    """Property-based tests for HyperliquidRawFill validation."""

    @given(fill_data=complete_fill_data_strategy())
    def test_valid_fill_creation_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Valid data should create valid models."""
        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Required fields should be preserved exactly
        assert fill.tid == fill_data["tid"]
        assert fill.oid == fill_data["oid"]
        assert fill.coin == fill_data["coin"]
        assert fill.px == fill_data["px"]
        assert fill.sz == fill_data["sz"]
        assert fill.start_position == fill_data["startPosition"]
        assert fill.fee == fill_data["fee"]
        assert fill.time == fill_data["time"]
        assert fill.side == fill_data["side"]
        assert fill.dir == fill_data["dir"]
        assert fill.hash == fill_data["hash"]
        assert fill.is_maker == fill_data["isMaker"]

        # Property: Optional fields should handle None correctly
        if fill_data.get("liquidationMarkPx") is None:
            assert fill.liquidation_mark_px is None
        else:
            assert fill.liquidation_mark_px == fill_data["liquidationMarkPx"]

        if fill_data.get("cloid") is None:
            assert fill.cloid is None
        else:
            assert fill.cloid == fill_data["cloid"]

    @given(
        required_fields=required_fill_fields_strategy(),
        missing_field=st.sampled_from([
            "tid",
            "oid",
            "coin",
            "px",
            "sz",
            "startPosition",
            "fee",
            "time",
            "side",
            "dir",
            "hash",
            "isMaker",
        ]),
    )
    def test_missing_required_fields_rejection(
        self, required_fields: dict[str, Any], missing_field: str
    ) -> None:
        """Property: Fills missing required fields should always be rejected."""
        incomplete_data = required_fields.copy()
        del incomplete_data[missing_field]

        # Property: Missing required field should cause validation error
        with pytest.raises(ValidationError):
            HyperliquidRawFill.model_validate(incomplete_data)

    @given(fill_data=complete_fill_data_strategy())
    def test_serialization_roundtrip_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Fills should survive serialization round trip."""
        fill = HyperliquidRawFill.model_validate(fill_data)

        # Serialize to dict
        serialized = fill.model_dump()

        # Property: All fields should be present (snake_case conversion)
        assert "tid" in serialized
        assert "oid" in serialized
        assert "coin" in serialized
        assert "px" in serialized
        assert "sz" in serialized
        assert "start_position" in serialized
        assert "fee" in serialized
        assert "time" in serialized
        assert "side" in serialized
        assert "dir" in serialized
        assert "hash" in serialized
        assert "is_maker" in serialized

        # Property: Re-parsing should produce identical result
        reparsed = HyperliquidRawFill.model_validate(serialized)

        # Check critical fields are identical
        assert reparsed.tid == fill.tid
        assert reparsed.px == fill.px
        assert reparsed.sz == fill.sz
        assert reparsed.fee == fill.fee

    @given(
        invalid_decimal=st.one_of(
            st.just("NaN"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
        )
    )
    def test_invalid_decimal_fields_rejection(self, invalid_decimal: str) -> None:
        """Property: Invalid decimal strings should be consistently rejected."""
        base_data: dict[str, object] = {
            "tid": 12345,
            "oid": 67890,
            "coin": "BTC",
            "startPosition": "0",
            "time": 1234567890000,
            "side": "B",
            "dir": "Open Long",
            "hash": "0x" + "a" * 32,
            "isMaker": True,
        }

        # Test each decimal field
        decimal_fields = ["px", "sz", "fee"]

        for field in decimal_fields:
            test_data = base_data.copy()
            # Add valid values for other decimal fields
            for other_field in decimal_fields:
                if other_field != field:
                    test_data[other_field] = "1.0"
            test_data["startPosition"] = "0"
            test_data[field] = invalid_decimal

            # Property: Invalid decimal should cause validation error
            with pytest.raises((ValidationError, Exception)):
                HyperliquidRawFill.model_validate(test_data)

        # Test startPosition separately (can be negative)
        test_data = base_data.copy()
        test_data["px"] = "100.0"
        test_data["sz"] = "1.0"
        test_data["fee"] = "0.1"
        test_data["startPosition"] = invalid_decimal

        with pytest.raises((ValidationError, Exception)):
            HyperliquidRawFill.model_validate(test_data)

    @given(
        base_data=required_fill_fields_strategy(),
        invalid_side=st.text(alphabet="xyz", min_size=1, max_size=10),
    )
    def test_invalid_side_rejection(self, base_data: dict[str, Any], invalid_side: str) -> None:
        """Property: Invalid side values should be consistently rejected."""
        # Ensure we don't accidentally generate valid sides
        valid_sides = {"B", "A", "b", "a"}
        assume(invalid_side not in valid_sides)

        test_data = base_data.copy()
        test_data["side"] = invalid_side

        # Property: Invalid side should cause validation error
        with pytest.raises((ValidationError, Exception)):
            HyperliquidRawFill.model_validate(test_data)

    @given(negative_id=st.integers(min_value=-1000, max_value=-1))
    def test_negative_ids_rejection(self, negative_id: int) -> None:
        """Property: Negative trade/order IDs should be rejected."""
        fill_data = {
            "tid": negative_id,  # Invalid negative
            "oid": 67890,
            "coin": "BTC",
            "px": "100.0",
            "sz": "1.0",
            "startPosition": "0",
            "fee": "0.1",
            "time": 1234567890000,
            "side": "B",
            "dir": "Open Long",
            "hash": "0x" + "a" * 32,
            "isMaker": True,
        }

        # Property: Negative tid should cause validation error
        with pytest.raises(ValidationError):
            HyperliquidRawFill.model_validate(fill_data)

        # Test negative oid
        fill_data["tid"] = 12345
        fill_data["oid"] = negative_id

        with pytest.raises(ValidationError):
            HyperliquidRawFill.model_validate(fill_data)

    @given(fill_data=complete_fill_data_strategy())
    def test_immutability_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Fill models should be immutable after creation."""
        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises((AttributeError, ValidationError)):
            fill.px = "999.99"

        with pytest.raises((AttributeError, ValidationError)):
            fill.tid = 99999

    @given(fill_data=complete_fill_data_strategy())
    def test_alias_mapping_properties(self, fill_data: dict[str, Any]) -> None:
        """Property: Alias mapping should work consistently."""
        # The model should accept camelCase aliases
        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Field access should use snake_case internally
        assert hasattr(fill, "start_position")
        assert hasattr(fill, "liquidation_mark_px")
        assert hasattr(fill, "is_maker")

        # Property: Values should be correctly mapped
        assert fill.start_position == fill_data["startPosition"]
        assert fill.is_maker == fill_data["isMaker"]
        if "liquidationMarkPx" in fill_data and fill_data["liquidationMarkPx"] is not None:
            assert fill.liquidation_mark_px == fill_data["liquidationMarkPx"]

    @given(fill_data=complete_fill_data_strategy())
    def test_extra_fields_rejection(self, fill_data: dict[str, Any]) -> None:
        """Property: Extra fields should always be rejected."""
        # Add an extra field
        invalid_data = fill_data.copy()
        invalid_data["extraField"] = "should_not_be_allowed"

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawFill.model_validate(invalid_data)

        # Property: Error should mention extra field
        assert "extra" in str(exc_info.value).lower()

    @given(
        px=financial_decimal_string_strategy(),
        sz=financial_decimal_string_strategy(),
        fee=fee_decimal_string_strategy(),
        start_position=position_decimal_string_strategy(),
    )
    def test_financial_precision_preservation(
        self, px: str, sz: str, fee: str, start_position: str
    ) -> None:
        """Property: Financial values should preserve exact string precision."""
        fill_data = {
            "tid": 12345,
            "oid": 67890,
            "coin": "BTC",
            "px": px,
            "sz": sz,
            "startPosition": start_position,
            "fee": fee,
            "time": 1234567890000,
            "side": "B",
            "dir": "Open Long",
            "hash": "0x" + "a" * 32,
            "isMaker": True,
        }

        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Financial values should be preserved exactly as strings
        assert fill.px == px
        assert fill.sz == sz
        assert fill.fee == fee
        assert fill.start_position == start_position

        # Property: If parseable, should be valid decimals
        px_decimal = Decimal(px)
        sz_decimal = Decimal(sz)
        fee_decimal = Decimal(fee)
        start_decimal = Decimal(start_position)

        assert px_decimal.is_finite()
        assert sz_decimal.is_finite()
        assert fee_decimal.is_finite()
        assert start_decimal.is_finite()

        # Property: Notional calculation should be possible
        notional = px_decimal * sz_decimal
        assert notional.is_finite()
        total_cost = notional + fee_decimal
        assert total_cost.is_finite()

    @given(
        invalid_hash=st.one_of(
            st.just("not_a_hash"),
            st.just("0x"),
            st.just("0x" + "g" * 32),  # Invalid hex
            st.just("0x" + "a" * 31),  # Too short
            st.just("0x" + "a" * 33),  # Too long
        )
    )
    def test_invalid_hash_rejection(self, invalid_hash: str) -> None:
        """Property: Invalid transaction hashes should be rejected."""
        fill_data = {
            "tid": 12345,
            "oid": 67890,
            "coin": "BTC",
            "px": "100.0",
            "sz": "1.0",
            "startPosition": "0",
            "fee": "0.1",
            "time": 1234567890000,
            "side": "B",
            "dir": "Open Long",
            "hash": invalid_hash,
            "isMaker": True,
        }

        # Property: Invalid hash should cause validation error
        with pytest.raises((ValidationError, Exception)):
            HyperliquidRawFill.model_validate(fill_data)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestHyperliquidRawFillIntegrationProperties:
    """Integration property tests for Hyperliquid raw fill models."""

    @given(fill_data=complete_fill_data_strategy())
    def test_model_deterministic_creation(self, fill_data: dict[str, Any]) -> None:
        """Property: Model creation should be deterministic for same inputs."""
        fill1 = HyperliquidRawFill.model_validate(fill_data)
        fill2 = HyperliquidRawFill.model_validate(fill_data)

        # Property: All field values should be identical
        assert fill1.tid == fill2.tid
        assert fill1.px == fill2.px
        assert fill1.sz == fill2.sz
        assert fill1.fee == fill2.fee
        assert fill1.hash == fill2.hash
        assert fill1.is_maker == fill2.is_maker

    @given(
        px=financial_decimal_string_strategy(),
        sz=financial_decimal_string_strategy(),
        fee=fee_decimal_string_strategy(),
        side=side_strategy(),
        is_maker=st.booleans(),
    )
    def test_fill_financial_calculations(
        self, px: str, sz: str, fee: str, side: str, is_maker: bool
    ) -> None:
        """Property: Fill financial values should support accurate calculations."""
        fill_data = {
            "tid": 12345,
            "oid": 67890,
            "coin": "BTC",
            "px": px,
            "sz": sz,
            "startPosition": "0",
            "fee": fee,
            "time": 1234567890000,
            "side": side,
            "dir": "Open Long" if side == "B" else "Open Short",
            "hash": "0x" + "a" * 32,
            "isMaker": is_maker,
        }

        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Should be able to calculate notional value
        px_decimal = Decimal(fill.px)
        sz_decimal = Decimal(fill.sz)
        fee_decimal = Decimal(fill.fee)

        notional = px_decimal * sz_decimal
        assert notional.is_finite()

        # Property: Fee should be reasonable (less than notional in most cases)
        if notional > 0:
            fee_percentage = (fee_decimal / notional) * 100
            assert fee_percentage.is_finite()

        # Property: Total cost calculation should work
        total_cost = notional + fee_decimal if fill.side == "B" else notional - fee_decimal
        assert total_cost.is_finite()

    @given(fills=st.lists(complete_fill_data_strategy(), min_size=2, max_size=10))
    def test_multiple_fills_independence(self, fills: list[dict[str, Any]]) -> None:
        """Property: Multiple fills should be processed independently."""
        parsed_fills: list[HyperliquidRawFill] = []

        for fill_data in fills:
            fill = HyperliquidRawFill.model_validate(fill_data)
            parsed_fills.append(fill)

        # Property: Each fill should maintain its individual data
        for i, (original_data, parsed_fill) in enumerate(zip(fills, parsed_fills, strict=False)):
            assert parsed_fill.tid == original_data["tid"]
            assert parsed_fill.oid == original_data["oid"]
            assert parsed_fill.px == original_data["px"]
            assert parsed_fill.sz == original_data["sz"]

            # Property: Fills should not affect each other
            for j, other_fill in enumerate(parsed_fills):
                if i != j and original_data["tid"] != fills[j]["tid"]:
                    # Trade IDs should be independent
                    assert parsed_fill.tid != other_fill.tid

    @given(
        start_position=position_decimal_string_strategy(),
        sz=financial_decimal_string_strategy(),
        side=side_strategy(),
    )
    def test_position_change_calculation(self, start_position: str, sz: str, side: str) -> None:
        """Property: Position changes should be calculable from fill data."""
        fill_data = {
            "tid": 12345,
            "oid": 67890,
            "coin": "BTC",
            "px": "100.0",
            "sz": sz,
            "startPosition": start_position,
            "fee": "0.1",
            "time": 1234567890000,
            "side": side,
            "dir": "Open Long" if side == "B" else "Open Short",
            "hash": "0x" + "a" * 32,
            "isMaker": True,
        }

        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Should be able to calculate position change
        start_pos = Decimal(fill.start_position)
        size = Decimal(fill.sz)

        # Calculate end position based on side
        end_position = start_pos + size if fill.side == "B" else start_pos - size

        assert end_position.is_finite()

        # Property: Position change should be the size
        position_change = abs(end_position - start_pos)
        assert position_change == size

    @settings(max_examples=50)
    @given(time=timestamp_ms_strategy(), tid=trade_id_strategy(), tx_hash=tx_hash_strategy())
    def test_fill_uniqueness_properties(self, time: int, tid: int, tx_hash: str) -> None:
        """Property: Fills should have unique identifiers."""
        fill_data = {
            "tid": tid,
            "oid": 67890,
            "coin": "BTC",
            "px": "100.0",
            "sz": "1.0",
            "startPosition": "0",
            "fee": "0.1",
            "time": time,
            "side": "B",
            "dir": "Open Long",
            "hash": tx_hash,
            "isMaker": True,
        }

        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Trade ID should uniquely identify the fill
        assert fill.tid == tid

        # Property: Hash should be unique
        assert fill.hash == tx_hash

        # Property: Timestamp + tid + hash should form a unique key
        unique_key = f"{fill.time}_{fill.tid}_{fill.hash}"
        assert unique_key == f"{time}_{tid}_{tx_hash}"

        # Property: Trade ID should be immutable
        with pytest.raises((AttributeError, ValidationError)):
            fill.tid = tid + 1

    @given(
        liquidation_px=st.one_of(st.none(), financial_decimal_string_strategy()),
        is_liquidation=st.booleans(),
    )
    def test_liquidation_handling(self, liquidation_px: str | None, is_liquidation: bool) -> None:
        """Property: Liquidation fields should be handled correctly."""
        fill_data = {
            "tid": 12345,
            "oid": 67890,
            "coin": "BTC",
            "px": "100.0",
            "sz": "1.0",
            "startPosition": "-5.0",  # Short position
            "fee": "0.1",
            "time": 1234567890000,
            "side": "B",  # Buying to close short (liquidation)
            "dir": "Close Short",
            "hash": "0x" + "a" * 32,
            "isMaker": False,  # Liquidations are usually taker
        }

        # Add liquidation mark price if this is a liquidation
        if is_liquidation and liquidation_px is not None:
            fill_data["liquidationMarkPx"] = liquidation_px

        fill = HyperliquidRawFill.model_validate(fill_data)

        # Property: Liquidation mark price should be preserved if present
        if is_liquidation and liquidation_px is not None:
            assert fill.liquidation_mark_px == liquidation_px
            # Should be able to parse as decimal
            if fill.liquidation_mark_px is not None:
                liq_px_decimal = Decimal(fill.liquidation_mark_px)
                assert liq_px_decimal.is_finite()
                assert liq_px_decimal > 0
        else:
            assert fill.liquidation_mark_px is None
