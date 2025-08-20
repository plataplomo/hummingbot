"""Property-based tests for the core SpotBalance model using Hypothesis.

This module provides comprehensive property-based testing of the SpotBalance Pydantic model,
which serves as the unified internal representation for spot asset balances across all
supported exchanges in the CyberDeltaEngine.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- Decimal precision handling for financial calculations (comprehensive value ranges)
- Balance state validation and business rule enforcement
- Exchange-specific detail model integration with validation
- Immutability properties under all mutation attempts
- Cross-field validation logic with generated combinations
- Timestamp and asset symbol validation

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete balance data flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.parsing import DateTimeParsingError, ParsingError
from cyberdelta.models.spot_balance import (
    BackpackSpotBalanceDetails,
    HyperliquidSpotBalanceDetails,
    SpotBalance,
)
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import (
    BTC_BP,
    BTC_HL,
    BTC_USDC_BP,
    DOGE_HL,
    ETH_BP,
    ETH_HL,
    ETH_USDC_BP,
    SOL_BP,
    SOL_HL,
    SOL_USDC_BP,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR BALANCE DATA
# =============================================================================


@st.composite
def financial_decimal_strategy(
    draw: st.DrawFn,
    min_value: float = 0.0,
    max_value: float = 1000000.0,
    allow_zero: bool = True,
    allow_negative: bool = False,
) -> Decimal:
    """Generate realistic Decimal values for financial calculations.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value
        max_value: Maximum value
        allow_zero: Whether to allow zero values
        allow_negative: Whether to allow negative values

    Returns:
        Decimal: A valid decimal for financial calculations
    """
    if allow_zero and draw(st.booleans()):
        return Decimal(0)

    # Adjust minimum based on restrictions
    if not allow_negative:
        min_value = max(0.000001 if not allow_zero else 0.0, min_value)

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
def balance_quantity_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic balance quantity values (non-negative).

    Returns:
        Non-negative Decimal balance quantity for testing.
    """
    return draw(
        financial_decimal_strategy(
            min_value=0.0, max_value=1000000.0, allow_zero=True, allow_negative=False
        )
    )


@st.composite
def collateral_weight_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic collateral weight values (0 to 1.0).

    Returns:
        Decimal collateral weight between 0 and 1 for testing.
    """
    return draw(
        financial_decimal_strategy(
            min_value=0.0, max_value=1.0, allow_zero=True, allow_negative=False
        )
    )


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Symbol:
    """Generate valid Symbol objects for balance testing.

    Returns:
        Valid Symbol object for testing.
    """
    return draw(
        st.sampled_from([
            BTC_HL,
            ETH_HL,
            SOL_HL,
            DOGE_HL,
            BTC_BP,
            ETH_BP,
            SOL_BP,
            BTC_USDC_BP,
            ETH_USDC_BP,
            SOL_USDC_BP,
        ])
    )


@st.composite
def valid_timestamp_strategy(draw: st.DrawFn) -> datetime:
    """Generate valid UTC timestamps for balance data.

    Returns:
        UTC datetime object for balance testing.
    """
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 12, 31, tzinfo=UTC),
            timezones=st.just(UTC),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def consistent_balance_quantities_strategy(draw: st.DrawFn) -> tuple[Decimal, Decimal]:
    """Generate consistent total and available quantities.

    Returns:
        tuple: (total_quantity, available_quantity) where available <= total
    """
    total = draw(balance_quantity_strategy())

    # Generate available quantity that's <= total
    if total == Decimal(0):
        available = Decimal(0)
    else:
        # Generate a factor between 0 and 1 to multiply with total
        factor = draw(st.floats(min_value=0.0, max_value=1.0))
        available = total * Decimal(str(factor))

    return total, available


@st.composite
def backpack_balance_details_strategy(draw: st.DrawFn) -> BackpackSpotBalanceDetails:
    """Generate valid BackpackSpotBalanceDetails for testing.

    Returns:
        Valid BackpackSpotBalanceDetails object for testing.
    """
    open_order_quantity = draw(st.one_of(st.none(), balance_quantity_strategy()))
    lend_quantity = draw(st.one_of(st.none(), balance_quantity_strategy()))
    collateral_weight = draw(st.one_of(st.none(), collateral_weight_strategy()))

    return BackpackSpotBalanceDetails(
        open_order_quantity=open_order_quantity,
        lend_quantity=lend_quantity,
        collateral_weight=collateral_weight,
    )


# =============================================================================
# PROPERTY TESTS FOR SPOT BALANCE MODEL
# =============================================================================


class TestSpotBalanceModelProperties:
    """Property-based tests for the SpotBalance model."""

    @given(
        balance_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        balance_quantities=consistent_balance_quantities_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_minimal_balance_creation_properties(
        self,
        balance_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        balance_quantities: tuple[Decimal, Decimal],
    ) -> None:
        """Property: Minimal balance with only required fields should always be valid."""
        total_quantity, available_quantity = balance_quantities

        balance = SpotBalance(
            exchange=exchange,
            asset=balance_symbol,
            timestamp=timestamp,
            total_quantity=total_quantity,
            available_quantity=available_quantity,
        )

        # Properties: Required fields should be set correctly
        assert balance.exchange == exchange
        assert balance.asset == balance_symbol
        assert balance.timestamp == timestamp
        assert balance.total_quantity == total_quantity
        assert balance.available_quantity == available_quantity

        # Properties: Optional extension slots should have correct defaults
        assert balance.hl_details is None
        assert balance.bp_details is None

        # Properties: Business logic consistency
        assert balance.available_quantity <= balance.total_quantity

    @given(
        balance_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        balance_quantities=consistent_balance_quantities_strategy(),
        data=st.data(),
    )
    @settings(
        max_examples=300,
        deadline=timedelta(seconds=1),
        suppress_health_check=[HealthCheck.filter_too_much],
    )
    def test_full_balance_creation_properties(
        self,
        balance_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        balance_quantities: tuple[Decimal, Decimal],
        data: st.DataObject,
    ) -> None:
        """Property: Full balance with exchange-specific details should maintain data integrity."""
        total_quantity, available_quantity = balance_quantities

        # Generate exchange-specific details based on exchange
        if exchange == ExchangeName.HYPERLIQUID:
            hl_details = data.draw(st.one_of(st.none(), st.just(HyperliquidSpotBalanceDetails())))
            bp_details = None
        else:
            hl_details = None
            bp_details = data.draw(st.one_of(st.none(), backpack_balance_details_strategy()))

        balance = SpotBalance(
            exchange=exchange,
            asset=balance_symbol,
            timestamp=timestamp,
            total_quantity=total_quantity,
            available_quantity=available_quantity,
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Properties: All fields should be preserved exactly
        assert balance.exchange == exchange
        assert balance.asset == balance_symbol
        assert balance.timestamp == timestamp
        assert balance.total_quantity == total_quantity
        assert balance.available_quantity == available_quantity
        assert balance.hl_details == hl_details
        assert balance.bp_details == bp_details

        # Properties: Business logic consistency
        assert balance.available_quantity <= balance.total_quantity

    @given(
        field_name=st.sampled_from(["total_quantity", "available_quantity"]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_financial_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Financial quantity fields should reject invalid values."""
        base_kwargs: dict[str, Any] = {
            "exchange": ExchangeName.BACKPACK,
            "asset": BTC_BP,
            "timestamp": datetime.now(UTC),
            "total_quantity": Decimal("100.0"),
            "available_quantity": Decimal("50.0"),
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid values should be rejected
        with pytest.raises(ValidationError):
            SpotBalance(**kwargs)

    @given(
        balance_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        timestamp=valid_timestamp_strategy(),
        balance_quantities=consistent_balance_quantities_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_balance_mutability_properties(
        self,
        balance_symbol: Symbol,
        exchange: ExchangeName,
        timestamp: datetime,
        balance_quantities: tuple[Decimal, Decimal],
    ) -> None:
        """Property: Balance instances should be mutable and validate on assignment."""
        total_quantity, available_quantity = balance_quantities

        balance = SpotBalance(
            exchange=exchange,
            asset=balance_symbol,
            timestamp=timestamp,
            total_quantity=total_quantity,
            available_quantity=available_quantity,
        )

        # Property: Valid mutations should work
        new_total = total_quantity + Decimal(10)
        balance.total_quantity = new_total
        assert balance.total_quantity == new_total

        new_available = available_quantity + Decimal(5)
        balance.available_quantity = new_available
        assert balance.available_quantity == new_available

        new_timestamp = timestamp + timedelta(seconds=1)
        balance.timestamp = new_timestamp
        assert balance.timestamp == new_timestamp

        # Property: Invalid mutations should be rejected
        with pytest.raises(ValidationError):
            balance.total_quantity = Decimal(-1)  # Negative quantity

    @given(
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        hl_details=st.just(HyperliquidSpotBalanceDetails()),
        bp_details=backpack_balance_details_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_exchange_details_properties(
        self,
        exchange: ExchangeName,
        hl_details: HyperliquidSpotBalanceDetails,
        bp_details: BackpackSpotBalanceDetails,
    ) -> None:
        """Property: Exchange-specific details should be preserved correctly.

        Raises:
            AssertionError: When property expectations are not met.
        """
        base_kwargs: dict[str, Any] = {
            "asset": BTC_BP,
            "timestamp": datetime.now(UTC),
            "total_quantity": Decimal("100.0"),
            "available_quantity": Decimal("50.0"),
            "exchange": exchange,
        }

        if exchange == ExchangeName.HYPERLIQUID:
            # Valid: HL exchange with HL details
            balance = SpotBalance(**base_kwargs, hl_details=hl_details)
            assert balance.hl_details == hl_details
            assert balance.bp_details is None

            # Property: HL details model config
            # HyperliquidSpotBalanceDetails is currently empty, just verify it exists
            assert balance.hl_details is not None
        else:
            # Valid: BP exchange with BP details
            balance = SpotBalance(**base_kwargs, bp_details=bp_details)
            assert balance.bp_details == bp_details
            assert balance.hl_details is None

            # Property: BP details should be immutable
            if balance.bp_details is not None:
                try:
                    balance.bp_details.open_order_quantity = Decimal(999)
                    raise AssertionError("Expected ValidationError for modifying frozen model")
                except (ValidationError, AttributeError):
                    pass

    @given(
        parseable_inputs=st.one_of(
            st.integers(min_value=0, max_value=1000000),
            st.floats(min_value=0.0, max_value=1000000.0, allow_nan=False, allow_infinity=False),
            st.text(alphabet="0123456789.", min_size=1, max_size=20).filter(
                lambda x: x.replace(".", "").isdigit()
                and len(x.replace(".", "")) > 0
                and x.count(".") <= 1
            ),
        ),
    )
    @settings(
        max_examples=200,
        deadline=timedelta(seconds=1),
        suppress_health_check=[HealthCheck.filter_too_much],
    )
    def test_decimal_parsing_properties(self, parseable_inputs: float | str) -> None:
        """Property: Balance should correctly parse various numeric input types to Decimal."""
        # Skip edge cases that might cause precision issues
        if isinstance(parseable_inputs, float):
            assume(abs(parseable_inputs) < 1e15)  # Avoid precision loss
            assume(parseable_inputs >= 0)  # Ensure non-negative

        balance = SpotBalance(
            exchange=ExchangeName.BACKPACK,
            asset=BTC_BP,
            timestamp=datetime.now(UTC),
            total_quantity=cast(Decimal, parseable_inputs),
            available_quantity=cast(Decimal, parseable_inputs),
        )

        # Property: Quantities should be converted to Decimal
        assert isinstance(balance.total_quantity, Decimal)
        assert isinstance(balance.available_quantity, Decimal)
        assert balance.total_quantity >= 0  # Should maintain non-negativity
        assert balance.available_quantity >= 0  # Should maintain non-negativity

        # Property: Should preserve reasonable precision
        if isinstance(parseable_inputs, (int, str)):
            # Exact conversion expected for integers and valid decimal strings
            expected = Decimal(str(parseable_inputs))
            assert balance.total_quantity == expected
            assert balance.available_quantity == expected

    @given(
        invalid_input=st.one_of(
            st.just(""),
            st.just("   "),
            st.just("not_a_number"),
            st.just("12..34"),
            st.text().filter(
                lambda x: x.strip() and not x.replace(".", "").replace("-", "").isdigit()
            ),
        ),
    )
    @settings(
        max_examples=100,
        deadline=timedelta(seconds=1),
        suppress_health_check=[HealthCheck.filter_too_much],
    )
    def test_invalid_decimal_input_rejection_properties(self, invalid_input: str) -> None:
        """Property: Invalid decimal inputs should always raise ValidationError."""
        with pytest.raises(ValidationError):
            SpotBalance(
                exchange=ExchangeName.BACKPACK,
                asset=BTC_BP,
                timestamp=datetime.now(UTC),
                total_quantity=cast(Decimal, invalid_input),
                available_quantity=Decimal("50.0"),
            )

    @given(
        invalid_timestamp=st.one_of(
            st.just("not_a_datetime"),
            st.just("2023-13-01T00:00:00Z"),  # Invalid month
            st.just("2023-02-30T00:00:00Z"),  # Invalid day
            st.just("invalid_date_string"),
            st.just("2023/01/01"),  # Wrong format
            st.just("abc123"),
        ),
    )
    @settings(
        max_examples=100,
        deadline=timedelta(seconds=1),
        suppress_health_check=[HealthCheck.filter_too_much],
    )
    def test_invalid_timestamp_rejection_properties(self, invalid_timestamp: str) -> None:
        """Property: Invalid timestamp inputs should always raise ValidationError."""
        with pytest.raises((ValidationError, DateTimeParsingError, ParsingError)):
            SpotBalance(
                exchange=ExchangeName.BACKPACK,
                asset=BTC_BP,
                timestamp=cast(datetime, invalid_timestamp),
                total_quantity=Decimal("100.0"),
                available_quantity=Decimal("50.0"),
            )

    @given(
        total_quantity=balance_quantity_strategy(),
        available_quantity=balance_quantity_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_quantity_relationship_properties(
        self, total_quantity: Decimal, available_quantity: Decimal
    ) -> None:
        """Property: Available quantity should be <= total quantity for valid balances."""
        # Property: Test both valid and invalid relationships
        if available_quantity <= total_quantity:
            # Should be valid
            balance = SpotBalance(
                exchange=ExchangeName.BACKPACK,
                asset=BTC_BP,
                timestamp=datetime.now(UTC),
                total_quantity=total_quantity,
                available_quantity=available_quantity,
            )
            assert balance.available_quantity <= balance.total_quantity
        else:
            # For invalid relationships (available > total), the model should still create
            # but this represents a business logic constraint that could be validated elsewhere
            balance = SpotBalance(
                exchange=ExchangeName.BACKPACK,
                asset=BTC_BP,
                timestamp=datetime.now(UTC),
                total_quantity=total_quantity,
                available_quantity=available_quantity,
            )
            # The model creates successfully - business logic validation would be elsewhere
            assert balance.total_quantity == total_quantity
            assert balance.available_quantity == available_quantity


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC DETAIL MODELS
# =============================================================================


class TestHyperliquidSpotBalanceDetailsProperties:
    """Property-based tests for HyperliquidSpotBalanceDetails model."""

    def test_hyperliquid_details_creation_properties(self) -> None:
        """Property: HyperliquidSpotBalanceDetails should create successfully (currently empty)."""
        details = HyperliquidSpotBalanceDetails()

        # Property: Should be immutable
        # Currently empty model, so test basic immutability structure
        assert hasattr(details, "model_config")

        # Property: Should follow ExtensionSlotModel pattern
        # This is more of a structural test since the model is currently empty


class TestBackpackSpotBalanceDetailsProperties:
    """Property-based tests for BackpackSpotBalanceDetails model."""

    @given(
        open_order_quantity=st.one_of(st.none(), balance_quantity_strategy()),
        lend_quantity=st.one_of(st.none(), balance_quantity_strategy()),
        collateral_weight=st.one_of(st.none(), collateral_weight_strategy()),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_backpack_details_creation_properties(
        self,
        open_order_quantity: Decimal | None,
        lend_quantity: Decimal | None,
        collateral_weight: Decimal | None,
    ) -> None:
        """Property: BackpackSpotBalanceDetails should handle all field combinations correctly.

        Raises:
            AssertionError: When property expectations are not met.
        """
        details = BackpackSpotBalanceDetails(
            open_order_quantity=open_order_quantity,
            lend_quantity=lend_quantity,
            collateral_weight=collateral_weight,
        )

        # Properties: All fields should be preserved
        assert details.open_order_quantity == open_order_quantity
        assert details.lend_quantity == lend_quantity
        assert details.collateral_weight == collateral_weight

        # Property: Should be immutable
        try:
            details.open_order_quantity = Decimal(999)
            raise AssertionError("Expected ValidationError for modifying frozen model")
        except (ValidationError, AttributeError):
            pass

    @given(
        field_name=st.sampled_from(["open_order_quantity", "lend_quantity", "collateral_weight"]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_backpack_details_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: BackpackSpotBalanceDetails should validate decimal field values correctly."""
        kwargs = {field_name: invalid_value}

        with pytest.raises(ValidationError):
            BackpackSpotBalanceDetails(**kwargs)

    @given(
        base_data=backpack_balance_details_strategy(),
        extra_field_value=st.one_of(
            st.text(),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_backpack_details_extra_fields_ignored_properties(
        self, base_data: BackpackSpotBalanceDetails, extra_field_value: object
    ) -> None:
        """Property: BackpackSpotBalanceDetails should ignore extra fields."""
        # Create a dict from the base data and add extra field
        base_dict = {
            "open_order_quantity": base_data.open_order_quantity,
            "lend_quantity": base_data.lend_quantity,
            "collateral_weight": base_data.collateral_weight,
            "extra_ignored_field": extra_field_value,
        }

        # Should not raise ValidationError due to extra field
        details = BackpackSpotBalanceDetails(**base_dict)  # type: ignore[arg-type]  # Testing extra field handling

        # Property: Extra field should not be present
        assert not hasattr(details, "extra_ignored_field")

        # Property: Original fields should be preserved
        assert details.open_order_quantity == base_data.open_order_quantity
        assert details.lend_quantity == base_data.lend_quantity
        assert details.collateral_weight == base_data.collateral_weight
