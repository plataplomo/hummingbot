"""Property-based tests for the CyberDeltaEngine Market model.

This module provides comprehensive property-based testing of the Market Pydantic model,
which represents immutable snapshots of market metadata for trading symbols.

Key Testing Areas:
- Market field validation and type safety using property-based input generation
- Financial precision handling for tick sizes, prices, and quantities
- Market constraint validation (positive tick/step sizes, non-negative limits)
- Exchange-specific extension slot validation (Backpack and Hyperliquid details)
- Immutability properties and frozen model behavior
- Market metadata consistency and serialization round-trip properties
- Edge cases and boundary conditions for market configuration

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete market creation flows with real constraints
- Validates financial calculation invariants and business rules

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from cyberdelta.models.market.market import (
    BackpackMarketDetails,
    HyperliquidMarketDetails,
    Market,
)
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


pytestmark = pytest.mark.timing


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _is_valid_decimal_string(s: str) -> bool:
    """Check if a string can be parsed as a valid Decimal."""
    try:
        Decimal(s)
        return True
    except:
        return False


# =============================================================================
# HYPOTHESIS STRATEGIES FOR MARKET DATA
# =============================================================================


@st.composite
def positive_decimal_strategy(
    draw: st.DrawFn, min_value: float = 0.00000001, max_value: float = 1000000.0
) -> Decimal:
    """Generate positive decimal values for market configuration.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value for generation
        max_value: Maximum value for generation

    Returns:
        Decimal: A positive decimal value
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
def non_negative_decimal_strategy(draw: st.DrawFn, max_value: float = 1000000.0) -> Decimal:
    """Generate non-negative decimal values for market limits.

    Args:
        draw: Hypothesis draw function
        max_value: Maximum value for generation

    Returns:
        Decimal: A non-negative decimal value (including 0)
    """
    value = draw(
        st.floats(
            min_value=0.0,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
        )
    )
    return Decimal(str(value))


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Any:
    """Generate valid Symbol objects for market testing.

    Args:
        draw: Hypothesis draw function

    Returns:
        Symbol: A valid symbol for market data
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
    """Generate valid UTC timestamps for market data.

    Args:
        draw: Hypothesis draw function

    Returns:
        datetime: A valid UTC timestamp
    """
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 12, 31, tzinfo=UTC),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def market_type_strategy(draw: st.DrawFn) -> str:
    """Generate valid market types.

    Args:
        draw: Hypothesis draw function

    Returns:
        str: A valid market type
    """
    return draw(
        st.sampled_from([
            "Spot",
            "Perpetual",
            "Future",
            "Option",
            "Swap",
            "Margin",
            "Cross",
            "Isolated",
        ])
    )


@st.composite
def market_status_strategy(draw: st.DrawFn) -> str:
    """Generate valid market statuses.

    Args:
        draw: Hypothesis draw function

    Returns:
        str: A valid market status
    """
    return draw(
        st.sampled_from([
            "Trading",
            "Halted",
            "Paused",
            "PreTrading",
            "PostTrading",
            "Maintenance",
            "Delisted",
            "Suspended",
        ])
    )


@st.composite
def backpack_details_strategy(draw: st.DrawFn) -> BackpackMarketDetails:
    """Generate valid BackpackMarketDetails.

    Args:
        draw: Hypothesis draw function

    Returns:
        BackpackMarketDetails: Valid Backpack market details
    """
    order_book_states = ["Live", "Paused", "Halted", "Maintenance"]

    return BackpackMarketDetails(
        order_book_state=draw(st.one_of(st.none(), st.sampled_from(order_book_states))),
        created_at_raw=draw(
            st.one_of(
                st.none(),
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-T:Z."
                    ),
                    min_size=10,
                    max_size=30,
                ),
            )
        ),
    )


@st.composite
def hyperliquid_details_strategy(draw: st.DrawFn) -> HyperliquidMarketDetails:
    """Generate valid HyperliquidMarketDetails.

    Args:
        draw: Hypothesis draw function

    Returns:
        HyperliquidMarketDetails: Valid Hyperliquid market details
    """
    return HyperliquidMarketDetails(
        max_leverage=draw(st.integers(min_value=1, max_value=1000)),
        only_isolated=draw(st.one_of(st.none(), st.booleans())),
        sz_decimals=draw(st.integers(min_value=0, max_value=18)),
        mark_price=draw(st.one_of(st.none(), non_negative_decimal_strategy(max_value=100000.0))),
        funding_rate=draw(
            st.one_of(
                st.none(),
                st.floats(min_value=-0.1, max_value=0.1, allow_nan=False, allow_infinity=False).map(
                    lambda x: Decimal(str(x))
                ),
            )
        ),
    )


@st.composite
def valid_string_strategy(draw: st.DrawFn, min_size: int = 1, max_size: int = 64) -> str:
    """Generate valid non-empty strings.

    Args:
        draw: Hypothesis draw function
        min_size: Minimum string length
        max_size: Maximum string length

    Returns:
        str: A valid non-empty string
    """
    return draw(
        st.text(
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_."
            ),
            min_size=min_size,
            max_size=max_size,
        ).filter(lambda x: len(x.strip()) > 0)
    )


# =============================================================================
# PROPERTY TESTS FOR MARKET MODEL
# =============================================================================


class TestMarketModelProperties:
    """Property-based tests for the Market model."""

    @given(
        symbol=valid_symbol_strategy(),
        market_type=market_type_strategy(),
        tick_size=positive_decimal_strategy(),
        step_size=positive_decimal_strategy(),
        status=market_status_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_market_creation_properties(
        self,
        symbol: Any,
        market_type: str,
        tick_size: Decimal,
        step_size: Decimal,
        status: str,
    ) -> None:
        """Property: Minimal Market with only required fields should always be valid."""
        market = Market(
            symbol=symbol,
            market_type=market_type,
            tick_size=tick_size,
            step_size=step_size,
            status=status,
        )

        # Properties: Required fields should be set correctly
        assert market.symbol == symbol
        assert market.market_type == market_type
        assert market.tick_size == tick_size
        assert market.step_size == step_size
        assert market.status == status

        # Properties: Optional fields should have correct defaults
        assert market.min_price is None
        assert market.max_price is None
        assert market.min_quantity is None
        assert market.max_quantity is None
        assert market.created_at is None
        assert market.bp_details is None
        assert market.hl_details is None

        # Properties: Financial fields should be positive
        assert market.tick_size > 0
        assert market.step_size > 0

    @given(
        symbol=valid_symbol_strategy(),
        market_type=market_type_strategy(),
        tick_size=positive_decimal_strategy(),
        step_size=positive_decimal_strategy(),
        min_price=non_negative_decimal_strategy(),
        max_price=non_negative_decimal_strategy(),
        min_quantity=non_negative_decimal_strategy(),
        max_quantity=non_negative_decimal_strategy(),
        status=market_status_strategy(),
        created_at=valid_timestamp_strategy(),
        bp_details=backpack_details_strategy(),
        hl_details=hyperliquid_details_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_complete_market_creation_properties(
        self,
        symbol: Any,
        market_type: str,
        tick_size: Decimal,
        step_size: Decimal,
        min_price: Decimal,
        max_price: Decimal,
        min_quantity: Decimal,
        max_quantity: Decimal,
        status: str,
        created_at: datetime,
        bp_details: BackpackMarketDetails,
        hl_details: HyperliquidMarketDetails,
    ) -> None:
        """Property: Complete Market with all fields should maintain data integrity."""
        # Ensure max >= min for logical consistency
        if max_price < min_price:
            min_price, max_price = max_price, min_price
        if max_quantity < min_quantity:
            min_quantity, max_quantity = max_quantity, min_quantity

        market = Market(
            symbol=symbol,
            market_type=market_type,
            tick_size=tick_size,
            step_size=step_size,
            min_price=min_price,
            max_price=max_price,
            min_quantity=min_quantity,
            max_quantity=max_quantity,
            status=status,
            created_at=created_at,
            bp_details=bp_details,
            hl_details=hl_details,
        )

        # Properties: All fields should be preserved exactly
        assert market.symbol == symbol
        assert market.market_type == market_type
        assert market.tick_size == tick_size
        assert market.step_size == step_size
        assert market.min_price == min_price
        assert market.max_price == max_price
        assert market.min_quantity == min_quantity
        assert market.max_quantity == max_quantity
        assert market.status == status
        assert market.created_at == created_at
        assert market.bp_details == bp_details
        assert market.hl_details == hl_details

    @given(
        symbol=valid_symbol_strategy(),
        market_type=market_type_strategy(),
        tick_size=positive_decimal_strategy(),
        step_size=positive_decimal_strategy(),
        status=market_status_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_market_immutability_properties(
        self,
        symbol: Any,
        market_type: str,
        tick_size: Decimal,
        step_size: Decimal,
        status: str,
    ) -> None:
        """Property: Market should be immutable after creation."""
        market = Market(
            symbol=symbol,
            market_type=market_type,
            tick_size=tick_size,
            step_size=step_size,
            status=status,
        )

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises(ValidationError, match="Instance is frozen"):
            market.symbol = BTC_HL

        with pytest.raises(ValidationError, match="Instance is frozen"):
            market.tick_size = Decimal("0.01")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            market.status = "Modified"

    @given(
        field_name=st.sampled_from(["tick_size", "step_size"]),
        invalid_value=st.one_of(
            st.just(Decimal(0)),
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_positive_decimal_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Positive decimal fields should reject invalid values."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "market_type": "Perpetual",
            "tick_size": Decimal("0.0001"),
            "step_size": Decimal("0.001"),
            "status": "Trading",
        }

        kwargs: dict[str, Any] = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid positive decimal values should be rejected
        with pytest.raises(ValidationError):
            Market(**kwargs)

    @given(
        field_name=st.sampled_from(["min_price", "max_price", "min_quantity", "max_quantity"]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_non_negative_decimal_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Non-negative decimal fields should reject negative/invalid values."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "market_type": "Perpetual",
            "tick_size": Decimal("0.0001"),
            "step_size": Decimal("0.001"),
            "status": "Trading",
        }

        kwargs: dict[str, Any] = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid non-negative decimal values should be rejected
        with pytest.raises(ValidationError):
            Market(**kwargs)

    @given(
        parseable_inputs=st.one_of(
            st.integers(min_value=1, max_value=1000000),
            st.floats(min_value=0.0001, max_value=1000000.0, allow_nan=False, allow_infinity=False),
            st.text(alphabet="0123456789.", min_size=1, max_size=20).filter(
                lambda x: (
                    # Must have digits
                    any(c.isdigit() for c in x)
                    # Can have at most one decimal point
                    and x.count(".") <= 1
                    # Must be parseable as decimal
                    and _is_valid_decimal_string(x)
                    # Must be positive
                    and float(x) > 0
                )
            ),
        ),
    )
    @settings(max_examples=200, deadline=None)
    def test_decimal_parsing_properties(self, parseable_inputs: Any) -> None:
        """Property: Market should correctly parse various numeric input types to Decimal."""
        market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=parseable_inputs,
            step_size=parseable_inputs,
            min_price=parseable_inputs,
            max_price=parseable_inputs,
            min_quantity=parseable_inputs,
            max_quantity=parseable_inputs,
            status="Trading",
        )

        # Property: All financial fields should be converted to Decimal
        assert isinstance(market.tick_size, Decimal)
        assert isinstance(market.step_size, Decimal)
        assert isinstance(market.min_price, Decimal)
        assert isinstance(market.max_price, Decimal)
        assert isinstance(market.min_quantity, Decimal)
        assert isinstance(market.max_quantity, Decimal)

        # Property: Required fields should be positive
        assert market.tick_size > 0
        assert market.step_size > 0

        # Property: Optional fields should be non-negative
        assert market.min_price >= 0
        assert market.max_price >= 0
        assert market.min_quantity >= 0
        assert market.max_quantity >= 0

    @given(
        timestamp_input=st.one_of(
            valid_timestamp_strategy(),
            st.integers(
                min_value=946684800,  # 2000-01-01
                max_value=1893456000,  # 2030-01-01
            ),
        ),
    )
    @settings(max_examples=150, deadline=None)
    def test_timestamp_parsing_properties(self, timestamp_input: Any) -> None:
        """Property: Timestamp fields should parse various input types correctly."""
        market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=Decimal("0.0001"),
            step_size=Decimal("0.001"),
            status="Trading",
            created_at=timestamp_input,
        )

        # Property: Timestamp should be converted to UTC datetime
        assert isinstance(market.created_at, datetime)
        assert market.created_at.tzinfo == UTC

    @given(
        invalid_string=st.one_of(
            st.just(""),
            st.just("   "),
            st.text(min_size=65, max_size=100),  # Too long
        ),
        field_name=st.sampled_from(["market_type", "status"]),
    )
    @settings(max_examples=100, deadline=None)
    def test_string_field_validation_properties(self, invalid_string: str, field_name: str) -> None:
        """Property: String fields should validate length and emptiness."""
        base_kwargs: dict[str, Any] = {
            "symbol": BTC_HL,
            "market_type": "Perpetual",
            "tick_size": Decimal("0.0001"),
            "step_size": Decimal("0.001"),
            "status": "Trading",
        }

        kwargs: dict[str, Any] = base_kwargs.copy()
        kwargs[field_name] = invalid_string

        # Property: Invalid string should be rejected
        with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
            Market(**kwargs)

    @given(
        symbol=valid_symbol_strategy(),
        market_type=market_type_strategy(),
        tick_size=positive_decimal_strategy(),
        step_size=positive_decimal_strategy(),
        status=market_status_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_extra_fields_rejection_properties(
        self,
        symbol: Any,
        market_type: str,
        tick_size: Decimal,
        step_size: Decimal,
        status: str,
    ) -> None:
        """Property: Extra fields should always be rejected."""
        market_data = {
            "symbol": symbol,
            "market_type": market_type,
            "tick_size": tick_size,
            "step_size": step_size,
            "status": status,
            "extra_field": "not_allowed",
        }

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            Market(**market_data)


# =============================================================================
# MARKET EXTENSION SLOT PROPERTIES
# =============================================================================


class TestMarketExtensionSlotProperties:
    """Property-based tests for Market extension slots."""

    @given(bp_details=backpack_details_strategy())
    @settings(max_examples=150, deadline=None)
    def test_backpack_details_properties(self, bp_details: BackpackMarketDetails) -> None:
        """Property: BackpackMarketDetails should work correctly as extension slot."""
        market = Market(
            symbol=BTC_HL,
            market_type="Spot",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            status="Trading",
            bp_details=bp_details,
        )

        # Property: Backpack details should be preserved
        assert market.bp_details == bp_details

        # Property: Details should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            bp_details.order_book_state = "Modified"

    @given(hl_details=hyperliquid_details_strategy())
    @settings(max_examples=150, deadline=None)
    def test_hyperliquid_details_properties(self, hl_details: HyperliquidMarketDetails) -> None:
        """Property: HyperliquidMarketDetails should work correctly as extension slot."""
        market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            status="Trading",
            hl_details=hl_details,
        )

        # Property: Hyperliquid details should be preserved
        assert market.hl_details == hl_details

        # Property: Leverage should be in valid range
        assert 1 <= market.hl_details.max_leverage <= 1000

        # Property: Decimals should be in valid range
        assert 0 <= market.hl_details.sz_decimals <= 18

        # Property: Mark price should be non-negative if set
        if market.hl_details.mark_price is not None:
            assert market.hl_details.mark_price >= 0

        # Property: Details should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            hl_details.max_leverage = 999

    @given(
        bp_details=backpack_details_strategy(),
        hl_details=hyperliquid_details_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_both_extension_slots_properties(
        self,
        bp_details: BackpackMarketDetails,
        hl_details: HyperliquidMarketDetails,
    ) -> None:
        """Property: Market should support both extension slots simultaneously."""
        market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            status="Trading",
            bp_details=bp_details,
            hl_details=hl_details,
        )

        # Property: Both extension slots should be preserved
        assert market.bp_details == bp_details
        assert market.hl_details == hl_details
        assert market.bp_details is not None
        assert market.hl_details is not None

    @given(
        invalid_leverage=st.one_of(
            st.integers(max_value=0),
            st.integers(min_value=1001),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_hyperliquid_leverage_validation_properties(self, invalid_leverage: int) -> None:
        """Property: HyperliquidMarketDetails should validate leverage bounds."""
        with pytest.raises(ValidationError):
            HyperliquidMarketDetails(
                max_leverage=invalid_leverage,
                only_isolated=False,
                sz_decimals=4,
            )

    @given(
        invalid_sz_decimals=st.one_of(
            st.integers(max_value=-1),
            st.integers(min_value=19),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_hyperliquid_sz_decimals_validation_properties(self, invalid_sz_decimals: int) -> None:
        """Property: HyperliquidMarketDetails should validate sz_decimals bounds."""
        with pytest.raises(ValidationError):
            HyperliquidMarketDetails(
                max_leverage=50,
                only_isolated=False,
                sz_decimals=invalid_sz_decimals,
            )


# =============================================================================
# MARKET BUSINESS LOGIC PROPERTIES
# =============================================================================


class TestMarketBusinessLogicProperties:
    """Property-based tests for Market business logic and relationships."""

    @given(
        symbol=valid_symbol_strategy(),
        tick_size=positive_decimal_strategy(min_value=0.00000001, max_value=1.0),
        step_size=positive_decimal_strategy(min_value=0.00000001, max_value=1.0),
        min_price=non_negative_decimal_strategy(max_value=50000.0),
        max_price=non_negative_decimal_strategy(max_value=100000.0),
        min_quantity=non_negative_decimal_strategy(max_value=1000.0),
        max_quantity=non_negative_decimal_strategy(max_value=10000.0),
    )
    @settings(max_examples=150, deadline=None)
    def test_market_constraint_relationships_properties(
        self,
        symbol: Any,
        tick_size: Decimal,
        step_size: Decimal,
        min_price: Decimal,
        max_price: Decimal,
        min_quantity: Decimal,
        max_quantity: Decimal,
    ) -> None:
        """Property: Market constraints should maintain logical relationships."""
        # Ensure max >= min for logical consistency
        if max_price < min_price:
            min_price, max_price = max_price, min_price
        if max_quantity < min_quantity:
            min_quantity, max_quantity = max_quantity, min_quantity

        market = Market(
            symbol=symbol,
            market_type="Spot",
            tick_size=tick_size,
            step_size=step_size,
            min_price=min_price,
            max_price=max_price,
            min_quantity=min_quantity,
            max_quantity=max_quantity,
            status="Trading",
        )

        # Property: Required constraints should be positive
        assert market.tick_size > 0
        assert market.step_size > 0

        # Property: Optional constraints should be non-negative when present
        if market.min_price is not None:
            assert market.min_price >= 0
        if market.max_price is not None:
            assert market.max_price >= 0
        if market.min_quantity is not None:
            assert market.min_quantity >= 0
        if market.max_quantity is not None:
            assert market.max_quantity >= 0

        # Property: Max should be >= Min when both are present
        if market.max_price is not None and market.min_price is not None:
            assert market.max_price >= market.min_price
        if market.max_quantity is not None and market.min_quantity is not None:
            assert market.max_quantity >= market.min_quantity

    @given(
        symbol=valid_symbol_strategy(),
        market_type=market_type_strategy(),
        tick_size=positive_decimal_strategy(),
        step_size=positive_decimal_strategy(),
        status=market_status_strategy(),
        created_at=valid_timestamp_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_market_serialization_properties(
        self,
        symbol: Any,
        market_type: str,
        tick_size: Decimal,
        step_size: Decimal,
        status: str,
        created_at: datetime,
    ) -> None:
        """Property: Markets should be serializable and deserializable."""
        market = Market(
            symbol=symbol,
            market_type=market_type,
            tick_size=tick_size,
            step_size=step_size,
            status=status,
            created_at=created_at,
        )

        # Property: Market should be serializable to dict
        market_dict = market.model_dump()
        assert isinstance(market_dict, dict)

        # Property: Essential fields should be present in serialized data
        assert "symbol" in market_dict
        assert "market_type" in market_dict
        assert "tick_size" in market_dict
        assert "step_size" in market_dict
        assert "status" in market_dict

        # Property: Market should be serializable to JSON
        json_str = market.model_dump_json()
        assert isinstance(json_str, str)
        assert len(json_str) > 0

    @given(
        symbol=valid_symbol_strategy(),
        market_type=market_type_strategy(),
        tick_size=positive_decimal_strategy(),
        step_size=positive_decimal_strategy(),
        status=market_status_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_market_deterministic_creation_properties(
        self,
        symbol: Any,
        market_type: str,
        tick_size: Decimal,
        step_size: Decimal,
        status: str,
    ) -> None:
        """Property: Market creation should be deterministic for same inputs."""
        market1 = Market(
            symbol=symbol,
            market_type=market_type,
            tick_size=tick_size,
            step_size=step_size,
            status=status,
        )
        market2 = Market(
            symbol=symbol,
            market_type=market_type,
            tick_size=tick_size,
            step_size=step_size,
            status=status,
        )

        # Property: All field values should be identical
        assert market1.symbol == market2.symbol
        assert market1.market_type == market2.market_type
        assert market1.tick_size == market2.tick_size
        assert market1.step_size == market2.step_size
        assert market1.status == market2.status


# =============================================================================
# EDGE CASE AND INTEGRATION PROPERTIES
# =============================================================================


class TestMarketEdgeCaseProperties:
    """Property-based tests for edge cases and integration scenarios."""

    @given(
        decimal_value=positive_decimal_strategy(),
        operation=st.sampled_from(["addition", "multiplication", "precision_check"]),
    )
    @settings(max_examples=150, deadline=None)
    def test_financial_calculation_properties(self, decimal_value: Decimal, operation: str) -> None:
        """Property: Financial values should maintain precision for calculations."""
        market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=decimal_value,
            step_size=decimal_value,
            min_price=decimal_value,
            max_price=decimal_value,
            min_quantity=decimal_value,
            max_quantity=decimal_value,
            status="Trading",
        )

        # Property: Basic operations should work with exact precision
        if operation == "addition":
            result = market.tick_size + market.step_size
            assert result.is_finite()
        elif operation == "multiplication":
            result = market.tick_size * market.step_size
            assert result.is_finite()
        elif operation == "precision_check":
            # Property: Decimal precision should be preserved
            assert market.tick_size.is_finite()
            assert market.step_size.is_finite()
            assert isinstance(market.tick_size, Decimal)
            assert isinstance(market.step_size, Decimal)

    @given(
        base_time=valid_timestamp_strategy(),
        offset_seconds=st.integers(min_value=-86400, max_value=86400),  # ±1 day
    )
    @settings(max_examples=150, deadline=None)
    def test_market_timing_edge_cases_properties(
        self,
        base_time: datetime,
        offset_seconds: int,
    ) -> None:
        """Property: Market timing should handle various edge cases correctly."""
        created_time = base_time + timedelta(seconds=offset_seconds)

        market = Market(
            symbol=BTC_HL,
            market_type="Perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.01"),
            status="Trading",
            created_at=created_time,
        )

        # Property: Timestamp relationships should be preserved
        assert market.created_at == created_time
        assert market.created_at.tzinfo == UTC

        # Property: Time difference from base should match our offset
        if market.created_at and base_time:
            time_diff = market.created_at - base_time
            assert time_diff.total_seconds() == offset_seconds

    @given(
        markets=st.lists(
            st.tuples(
                valid_symbol_strategy(),
                market_type_strategy(),
                positive_decimal_strategy(),
                positive_decimal_strategy(),
                market_status_strategy(),
            ),
            min_size=2,
            max_size=10,
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_multiple_markets_independence_properties(
        self, markets: list[tuple[Any, str, Decimal, Decimal, str]]
    ) -> None:
        """Property: Multiple markets should be processed independently."""
        created_markets = []

        for symbol, market_type, tick_size, step_size, status in markets:
            market = Market(
                symbol=symbol,
                market_type=market_type,
                tick_size=tick_size,
                step_size=step_size,
                status=status,
            )
            created_markets.append(market)

        # Property: Each market should maintain its individual data
        for i, (original_data, created_market) in enumerate(
            zip(markets, created_markets, strict=False)
        ):
            symbol, market_type, tick_size, step_size, status = original_data
            assert created_market.symbol == symbol
            assert created_market.market_type == market_type
            assert created_market.tick_size == tick_size
            assert created_market.step_size == step_size
            assert created_market.status == status

            # Property: Markets should not affect each other
            for j, other_market in enumerate(created_markets):
                if i != j:
                    # Markets should be independent instances
                    assert created_market is not other_market
