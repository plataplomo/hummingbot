"""Property-based tests for the CyberDeltaEngine MarginAccountSummary model.

This module provides comprehensive property-based testing of the MarginAccountSummary Pydantic
model,
which represents immutable snapshots of margin account state for trading positions.

Key Testing Areas:
- Margin account field validation and type safety using property-based input generation
- Financial precision handling for equity, margin requirements, and PnL
- Exchange-specific extension slot validation (Backpack and Hyperliquid details)
- Immutability properties and frozen model behavior
- Margin account constraint validation (non-negative equity, valid ratios)
- Cross-field consistency and business rule validation
- Edge cases and boundary conditions for margin calculations

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete margin account creation flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.margin_account import (
    BackpackMarginDetails,
    HyperliquidMarginDetails,
    MarginAccountSummary,
)


pytestmark = pytest.mark.timing


# =============================================================================
# HYPOTHESIS STRATEGIES FOR MARGIN ACCOUNT DATA
# =============================================================================


@st.composite
def finite_positive_decimal_strategy(
    draw: st.DrawFn, min_value: float = 0.0, max_value: float = 1000000.0
) -> Decimal:
    """Generate finite positive decimal values for margin account fields.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value for generation
        max_value: Maximum value for generation

    Returns:
        Decimal: A finite positive decimal value
    """
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def finite_decimal_strategy(
    draw: st.DrawFn, min_value: float = -100000.0, max_value: float = 100000.0
) -> Decimal:
    """Generate finite decimal values (can be negative) for PnL fields.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value for generation
        max_value: Maximum value for generation

    Returns:
        Decimal: A finite decimal value
    """
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def exchange_name_strategy(draw: st.DrawFn) -> ExchangeName:
    """Generate valid exchange names.

    Args:
        draw: Hypothesis draw function

    Returns:
        ExchangeName: A valid exchange enum value
    """
    return draw(st.sampled_from(list(ExchangeName)))


@st.composite
def valid_timestamp_strategy(draw: st.DrawFn) -> datetime:
    """Generate valid UTC timestamps for margin account data.

    Args:
        draw: Hypothesis draw function

    Returns:
        datetime: A valid UTC timestamp
    """
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 12, 31),
            timezones=st.just(UTC),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def hyperliquid_details_strategy(draw: st.DrawFn) -> HyperliquidMarginDetails:
    """Generate valid HyperliquidMarginDetails.

    Args:
        draw: Hypothesis draw function

    Returns:
        HyperliquidMarginDetails: Valid Hyperliquid margin details
    """
    return HyperliquidMarginDetails(
        cross_maintenance_margin_used=draw(finite_positive_decimal_strategy()),
        isolated_maintenance_margin_used=draw(finite_positive_decimal_strategy()),
    )


@st.composite
def backpack_details_strategy(draw: st.DrawFn) -> BackpackMarginDetails:
    """Generate valid BackpackMarginDetails.

    Args:
        draw: Hypothesis draw function

    Returns:
        BackpackMarginDetails: Valid Backpack margin details
    """
    assets_value = draw(st.one_of(st.none(), finite_positive_decimal_strategy(max_value=1000000.0)))
    liabilities_value = draw(
        st.one_of(st.none(), finite_positive_decimal_strategy(max_value=100000.0))
    )

    return BackpackMarginDetails(
        assets_value=assets_value,
        liabilities_value=liabilities_value,
        locked_equity=draw(
            st.one_of(st.none(), finite_positive_decimal_strategy(max_value=50000.0))
        ),
        borrow_liability=draw(
            st.one_of(st.none(), finite_positive_decimal_strategy(max_value=10000.0))
        ),
        unsettled_equity=draw(
            st.one_of(
                st.none(),
                finite_decimal_strategy(),  # Can be negative
            )
        ),
        margin_fraction=draw(
            st.one_of(
                st.none(),
                st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False).map(
                    lambda x: Decimal(str(x))
                ),
            )
        ),
        net_exposure_futures=draw(
            st.one_of(
                st.none(),
                finite_decimal_strategy(),  # Can be negative
            )
        ),
        imf_raw=draw(
            st.one_of(st.none(), st.text(min_size=1, max_size=100).filter(lambda x: x.strip()))
        ),
        mmf_raw=draw(
            st.one_of(st.none(), st.text(min_size=1, max_size=100).filter(lambda x: x.strip()))
        ),
        leverage_limit=draw(
            st.one_of(
                st.none(),
                st.floats(
                    min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False
                ).map(lambda x: Decimal(str(x))),
            )
        ),
        subaccount_id=draw(st.one_of(st.none(), st.integers(min_value=0, max_value=65535))),
    )


@st.composite
def parseable_decimal_strategy(draw: st.DrawFn) -> Decimal | int | float | str:
    """Generate parseable decimal values in various formats.

    Args:
        draw: Hypothesis draw function

    Returns:
        A value that can be parsed to a Decimal.
    """
    return draw(
        st.one_of(
            # Decimal objects
            finite_positive_decimal_strategy(),
            # Integer values
            st.integers(min_value=0, max_value=1000000),
            # Float values
            st.floats(min_value=0.0, max_value=1000000.0, allow_nan=False, allow_infinity=False),
            # String values
            st.text(alphabet="0123456789.", min_size=1, max_size=20).filter(
                lambda x: (
                    any(c.isdigit() for c in x)
                    and x.count(".") <= 1
                    and _is_valid_decimal_string(x)
                    and float(x) >= 0
                )
            ),
        )
    )


def _is_valid_decimal_string(s: str) -> bool:
    """Check if a string can be parsed as a valid Decimal.

    Returns:
        True if string can be parsed as Decimal, False otherwise.
    """
    try:
        Decimal(s)
    except (ValueError, TypeError, InvalidOperation):
        return False
    else:
        return True


# =============================================================================
# PROPERTY TESTS FOR MARGIN ACCOUNT SUMMARY MODEL
# =============================================================================


class TestMarginAccountSummaryProperties:
    """Property-based tests for the MarginAccountSummary model."""

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_minimal_margin_summary_creation_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
    ) -> None:
        """Property: Minimal MarginAccountSummary with only required fields should be valid.

        always be valid.
        """
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
        )

        # Properties: Required fields should be set correctly
        assert summary.exchange == exchange
        assert summary.timestamp == timestamp
        assert summary.total_equity == total_equity
        assert summary.available_equity == available_equity

        # Properties: Optional fields should be None
        assert summary.total_initial_margin_required is None
        assert summary.total_maintenance_margin_required is None
        assert summary.total_position_notional is None
        assert summary.total_unrealized_pnl is None
        assert summary.hl_details is None
        assert summary.bp_details is None

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
        total_initial_margin_required=st.one_of(st.none(), finite_positive_decimal_strategy()),
        total_maintenance_margin_required=st.one_of(st.none(), finite_positive_decimal_strategy()),
        total_position_notional=st.one_of(st.none(), finite_positive_decimal_strategy()),
        total_unrealized_pnl=st.one_of(
            st.none(),
            finite_decimal_strategy(),  # Can be negative
        ),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_complete_margin_summary_creation_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
        total_initial_margin_required: Decimal | None,
        total_maintenance_margin_required: Decimal | None,
        total_position_notional: Decimal | None,
        total_unrealized_pnl: Decimal | None,
    ) -> None:
        """Property: Complete MarginAccountSummary with all core fields should maintain integrity.

        data integrity.
        """
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
            total_initial_margin_required=total_initial_margin_required,
            total_maintenance_margin_required=total_maintenance_margin_required,
            total_position_notional=total_position_notional,
            total_unrealized_pnl=total_unrealized_pnl,
        )

        # Properties: All fields should be preserved exactly
        assert summary.exchange == exchange
        assert summary.timestamp == timestamp
        assert summary.total_equity == total_equity
        assert summary.available_equity == available_equity
        assert summary.total_initial_margin_required == total_initial_margin_required
        assert summary.total_maintenance_margin_required == total_maintenance_margin_required
        assert summary.total_position_notional == total_position_notional
        assert summary.total_unrealized_pnl == total_unrealized_pnl

    @given(
        exchange=st.just(ExchangeName.HYPERLIQUID),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
        hl_details=hyperliquid_details_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_summary_with_hyperliquid_details_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
        hl_details: HyperliquidMarginDetails,
    ) -> None:
        """Property: MarginAccountSummary with Hyperliquid details should work correctly."""
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
            hl_details=hl_details,
        )

        # Properties: Extension slot should be preserved
        assert summary.exchange == ExchangeName.HYPERLIQUID
        assert summary.hl_details == hl_details
        assert summary.bp_details is None

        # Properties: Details should be accessible
        assert summary.hl_details is not None
        assert summary.hl_details.cross_maintenance_margin_used >= 0
        assert summary.hl_details.isolated_maintenance_margin_used >= 0

    @given(
        exchange=st.just(ExchangeName.BACKPACK),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
        bp_details=backpack_details_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_summary_with_backpack_details_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
        bp_details: BackpackMarginDetails,
    ) -> None:
        """Property: MarginAccountSummary with Backpack details should work correctly."""
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
            bp_details=bp_details,
        )

        # Properties: Extension slot should be preserved
        assert summary.exchange == ExchangeName.BACKPACK
        assert summary.bp_details == bp_details
        assert summary.hl_details is None

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=parseable_decimal_strategy(),
        available_equity=parseable_decimal_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_margin_summary_decimal_parsing_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal | float | str,
        available_equity: Decimal | float | str,
    ) -> None:
        """Property: MarginAccountSummary should correctly parse various numeric types.

        types to Decimal.
        """
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=cast(Decimal, total_equity),
            available_equity=cast(Decimal, available_equity),
        )

        # Property: All financial fields should be converted to Decimal
        assert isinstance(summary.total_equity, Decimal)
        assert isinstance(summary.available_equity, Decimal)

        # Property: Values should be non-negative
        assert summary.total_equity >= 0
        assert summary.available_equity >= 0

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_summary_immutability_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
    ) -> None:
        """Property: MarginAccountSummary should be immutable after creation."""
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
        )

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            summary.total_equity = total_equity + 100

        with pytest.raises(ValidationError, match="Instance is frozen"):
            summary.exchange = ExchangeName.BACKPACK

        with pytest.raises(ValidationError, match="Instance is frozen"):
            summary.timestamp = datetime.now(UTC)

    @given(
        field_name=st.sampled_from(["total_equity", "available_equity"]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.01")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_required_decimal_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Required decimal fields should reject invalid values."""
        base_kwargs: dict[str, Any] = {
            "exchange": ExchangeName.BACKPACK,
            "timestamp": datetime.now(UTC),
            "total_equity": Decimal(10000),
            "available_equity": Decimal(8000),
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid decimal values should be rejected
        with pytest.raises(ValidationError):
            MarginAccountSummary(**kwargs)

    @given(
        field_name=st.sampled_from([
            "total_initial_margin_required",
            "total_maintenance_margin_required",
            "total_position_notional",
        ]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.01")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_optional_positive_decimal_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Optional positive decimal fields should reject negative/invalid values."""
        base_kwargs: dict[str, Any] = {
            "exchange": ExchangeName.BACKPACK,
            "timestamp": datetime.now(UTC),
            "total_equity": Decimal(10000),
            "available_equity": Decimal(8000),
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid values should be rejected
        with pytest.raises(ValidationError):
            MarginAccountSummary(**kwargs)

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_extra_fields_rejection_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
    ) -> None:
        """Property: Extra fields should always be rejected."""
        margin_data: dict[str, Any] = {
            "exchange": exchange,
            "timestamp": timestamp,
            "total_equity": total_equity,
            "available_equity": available_equity,
            "extra_field": "not_allowed",
        }

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            MarginAccountSummary(**margin_data)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID MARGIN DETAILS
# =============================================================================


class TestHyperliquidMarginDetailsProperties:
    """Property-based tests for HyperliquidMarginDetails model."""

    @given(
        cross_maintenance_margin_used=finite_positive_decimal_strategy(),
        isolated_maintenance_margin_used=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_hyperliquid_details_creation_properties(
        self,
        cross_maintenance_margin_used: Decimal,
        isolated_maintenance_margin_used: Decimal,
    ) -> None:
        """Property: HyperliquidMarginDetails with valid fields should always be valid."""
        details = HyperliquidMarginDetails(
            cross_maintenance_margin_used=cross_maintenance_margin_used,
            isolated_maintenance_margin_used=isolated_maintenance_margin_used,
        )

        # Properties: Fields should be preserved exactly
        assert details.cross_maintenance_margin_used == cross_maintenance_margin_used
        assert details.isolated_maintenance_margin_used == isolated_maintenance_margin_used

        # Properties: Values should be non-negative
        assert details.cross_maintenance_margin_used >= 0
        assert details.isolated_maintenance_margin_used >= 0

    @given(
        cross_maintenance_margin_used=finite_positive_decimal_strategy(),
        isolated_maintenance_margin_used=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_hyperliquid_details_immutability_properties(
        self,
        cross_maintenance_margin_used: Decimal,
        isolated_maintenance_margin_used: Decimal,
    ) -> None:
        """Property: HyperliquidMarginDetails should be immutable after creation."""
        details = HyperliquidMarginDetails(
            cross_maintenance_margin_used=cross_maintenance_margin_used,
            isolated_maintenance_margin_used=isolated_maintenance_margin_used,
        )

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.cross_maintenance_margin_used = Decimal(1000)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.isolated_maintenance_margin_used = Decimal(500)

    @given(
        cross_maintenance_margin_used=finite_positive_decimal_strategy(),
        isolated_maintenance_margin_used=finite_positive_decimal_strategy(),
        extra_field=st.text(min_size=1, max_size=20),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_hyperliquid_details_extra_fields_ignored_properties(
        self,
        cross_maintenance_margin_used: Decimal,
        isolated_maintenance_margin_used: Decimal,
        extra_field: str,
    ) -> None:
        """Property: HyperliquidMarginDetails should ignore extra fields (extra='ignore')."""
        details_data: dict[str, Any] = {
            "cross_maintenance_margin_used": cross_maintenance_margin_used,
            "isolated_maintenance_margin_used": isolated_maintenance_margin_used,
            "extra_field_name": extra_field,
        }
        details = HyperliquidMarginDetails(**details_data)

        # Property: Extra fields should be ignored
        assert not hasattr(details, "extra_field_name")
        assert details.cross_maintenance_margin_used == cross_maintenance_margin_used
        assert details.isolated_maintenance_margin_used == isolated_maintenance_margin_used


# =============================================================================
# PROPERTY TESTS FOR BACKPACK MARGIN DETAILS
# =============================================================================


class TestBackpackMarginDetailsProperties:
    """Property-based tests for BackpackMarginDetails model."""

    @given(bp_details=backpack_details_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_backpack_details_creation_properties(self, bp_details: BackpackMarginDetails) -> None:
        """Property: BackpackMarginDetails should maintain all field relationships."""
        # Property: All optional fields should be valid if set
        if bp_details.assets_value is not None:
            assert bp_details.assets_value >= 0
        if bp_details.liabilities_value is not None:
            assert bp_details.liabilities_value >= 0
        if bp_details.locked_equity is not None:
            assert bp_details.locked_equity >= 0
        if bp_details.borrow_liability is not None:
            assert bp_details.borrow_liability >= 0
        if bp_details.margin_fraction is not None:
            assert 0 <= bp_details.margin_fraction <= 1
        if bp_details.leverage_limit is not None:
            assert bp_details.leverage_limit > 0
        if bp_details.subaccount_id is not None:
            assert 0 <= bp_details.subaccount_id <= 65535

    @given(bp_details=backpack_details_strategy())
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_backpack_details_immutability_properties(
        self, bp_details: BackpackMarginDetails
    ) -> None:
        """Property: BackpackMarginDetails should be immutable after creation."""
        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            bp_details.assets_value = Decimal(10000)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            bp_details.margin_fraction = Decimal("0.5")

    @given(
        bp_details=backpack_details_strategy(),
        extra_field=st.text(min_size=1, max_size=20),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_backpack_details_extra_fields_ignored_properties(
        self,
        bp_details: BackpackMarginDetails,
        extra_field: str,
    ) -> None:
        """Property: BackpackMarginDetails should ignore extra fields (extra='ignore')."""
        # Create new details with extra field
        data: dict[str, Any] = {
            "assets_value": bp_details.assets_value,
            "liabilities_value": bp_details.liabilities_value,
            "locked_equity": bp_details.locked_equity,
            "extra_field_name": extra_field,
        }

        details = BackpackMarginDetails(**data)

        # Property: Extra fields should be ignored
        assert not hasattr(details, "extra_field_name")
        assert details.assets_value == bp_details.assets_value
        assert details.liabilities_value == bp_details.liabilities_value


# =============================================================================
# MARGIN ACCOUNT BUSINESS LOGIC PROPERTIES
# =============================================================================


class TestMarginAccountBusinessLogicProperties:
    """Property-based tests for MarginAccountSummary business logic and relationships."""

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(min_value=1000, max_value=100000),
        available_equity_ratio=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_account_equity_relationship_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity_ratio: float,
    ) -> None:
        """Property: Available equity should always be <= total equity."""
        available_equity = total_equity * Decimal(str(available_equity_ratio))

        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
        )

        # Property: Available equity should not exceed total equity
        assert summary.available_equity <= summary.total_equity
        assert summary.available_equity >= 0
        assert summary.total_equity >= 0

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
        total_initial_margin_required=finite_positive_decimal_strategy(),
        total_maintenance_margin_required=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_account_margin_requirements_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
        total_initial_margin_required: Decimal,
        total_maintenance_margin_required: Decimal,
    ) -> None:
        """Property: Margin requirements should maintain logical relationships."""
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
            total_initial_margin_required=total_initial_margin_required,
            total_maintenance_margin_required=total_maintenance_margin_required,
        )

        # Property: All margin values should be non-negative
        if summary.total_initial_margin_required is not None:
            assert summary.total_initial_margin_required >= 0
        if summary.total_maintenance_margin_required is not None:
            assert summary.total_maintenance_margin_required >= 0

        # Note: Initial margin is typically >= maintenance margin in practice,
        # but the model doesn't enforce this constraint

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_account_serialization_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
    ) -> None:
        """Property: MarginAccountSummary should be serializable and deserializable."""
        summary = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
        )

        # Property: Model should be serializable to dict
        summary_dict = summary.model_dump()
        assert isinstance(summary_dict, dict)

        # Property: Essential fields should be present in serialized data
        assert "exchange" in summary_dict
        assert "timestamp" in summary_dict
        assert "total_equity" in summary_dict
        assert "available_equity" in summary_dict

        # Property: Model should be serializable to JSON
        json_str = summary.model_dump_json()
        assert isinstance(json_str, str)
        assert len(json_str) > 0

    @given(
        exchange=exchange_name_strategy(),
        timestamp=valid_timestamp_strategy(),
        total_equity=finite_positive_decimal_strategy(),
        available_equity=finite_positive_decimal_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_account_deterministic_creation_properties(
        self,
        exchange: ExchangeName,
        timestamp: datetime,
        total_equity: Decimal,
        available_equity: Decimal,
    ) -> None:
        """Property: MarginAccountSummary creation should be deterministic for same inputs."""
        summary1 = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
        )
        summary2 = MarginAccountSummary(
            exchange=exchange,
            timestamp=timestamp,
            total_equity=total_equity,
            available_equity=available_equity,
        )

        # Property: All field values should be identical
        assert summary1.exchange == summary2.exchange
        assert summary1.timestamp == summary2.timestamp
        assert summary1.total_equity == summary2.total_equity
        assert summary1.available_equity == summary2.available_equity


# =============================================================================
# EDGE CASE AND INTEGRATION PROPERTIES
# =============================================================================


class TestMarginAccountEdgeCaseProperties:
    """Property-based tests for edge cases and integration scenarios."""

    @given(
        base_time=valid_timestamp_strategy(),
        offset_seconds=st.integers(min_value=-86400, max_value=86400),  # ±1 day
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_margin_account_timing_edge_cases_properties(
        self,
        base_time: datetime,
        offset_seconds: int,
    ) -> None:
        """Property: MarginAccountSummary timing should handle various edge cases correctly."""
        snapshot_time = base_time + timedelta(seconds=offset_seconds)

        summary = MarginAccountSummary(
            exchange=ExchangeName.BACKPACK,
            timestamp=snapshot_time,
            total_equity=Decimal(10000),
            available_equity=Decimal(8000),
        )

        # Property: Timestamp relationships should be preserved
        assert summary.timestamp == snapshot_time
        assert summary.timestamp.tzinfo == UTC

        # Property: Time difference from base should match our offset
        if summary.timestamp and base_time:
            time_diff = summary.timestamp - base_time
            assert time_diff.total_seconds() == offset_seconds

    @given(
        very_small_values=st.floats(
            min_value=1e-8, max_value=1e-6, allow_nan=False, allow_infinity=False
        ),
        very_large_values=st.floats(
            min_value=1e12, max_value=1e15, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_margin_account_extreme_values_properties(
        self,
        very_small_values: float,
        very_large_values: float,
    ) -> None:
        """Property: MarginAccountSummary should handle extreme values correctly."""
        # Property: Very small values should be handled correctly
        small_summary = MarginAccountSummary(
            exchange=ExchangeName.BACKPACK,
            timestamp=datetime.now(UTC),
            total_equity=Decimal(str(very_small_values)),
            available_equity=Decimal(str(very_small_values * 0.8)),
        )

        assert small_summary.total_equity == Decimal(str(very_small_values))
        assert small_summary.available_equity == Decimal(str(very_small_values * 0.8))

        # Property: Very large values should be handled correctly
        large_summary = MarginAccountSummary(
            exchange=ExchangeName.HYPERLIQUID,
            timestamp=datetime.now(UTC),
            total_equity=Decimal(str(very_large_values)),
            available_equity=Decimal(str(very_large_values * 0.9)),
        )

        assert large_summary.total_equity == Decimal(str(very_large_values))
        assert large_summary.available_equity == Decimal(str(very_large_values * 0.9))

    @given(
        summaries=st.lists(
            st.tuples(
                exchange_name_strategy(),
                valid_timestamp_strategy(),
                finite_positive_decimal_strategy(),
                finite_positive_decimal_strategy(),
            ),
            min_size=2,
            max_size=10,
        )
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_multiple_margin_accounts_independence_properties(
        self, summaries: list[tuple[ExchangeName, datetime, Decimal, Decimal]]
    ) -> None:
        """Property: Multiple margin accounts should be processed independently."""
        created_summaries: list[MarginAccountSummary] = []

        for exchange, timestamp, total_equity, available_equity in summaries:
            summary = MarginAccountSummary(
                exchange=exchange,
                timestamp=timestamp,
                total_equity=total_equity,
                available_equity=available_equity,
            )
            created_summaries.append(summary)

        # Property: Each summary should maintain its individual data
        for i, (original_data, created_summary) in enumerate(
            zip(summaries, created_summaries, strict=False)
        ):
            exchange, timestamp, total_equity, available_equity = original_data
            assert created_summary.exchange == exchange
            assert created_summary.timestamp == timestamp
            assert created_summary.total_equity == total_equity
            assert created_summary.available_equity == available_equity

            # Property: Summaries should not affect each other
            for j, other_summary in enumerate(created_summaries):
                if i != j:
                    # Summaries should be independent instances
                    assert created_summary is not other_summary
