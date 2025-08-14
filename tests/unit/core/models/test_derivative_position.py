"""Property-based tests for the core DerivativePosition model using Hypothesis.

This module provides comprehensive property-based testing of the DerivativePosition Pydantic model,
which serves as the unified internal representation for leveraged trading positions across all
supported exchanges in the CyberDeltaEngine.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- Complex cross-field validation logic (size/side consistency, entry_price validation)
- Position lifecycle management and business rule enforcement
- Decimal precision handling for financial calculations (comprehensive value ranges)
- Exchange-specific detail model integration with validation
- Position state transitions and PnL calculation properties
- Mutability and assignment validation
- Business rule enforcement across all possible combinations

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete position data flows with real constraints
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
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import PositionLogicError
from cyberdelta.exceptions.field_validation import FieldNameMissingError, TypeFieldError
from cyberdelta.exceptions.parsing import ParsingError
from cyberdelta.models.derivative_position import (
    BackpackPositionDetails,
    DerivativePosition,
    HyperliquidPositionDetails,
)
from tests.common_symbols import (
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
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR POSITION DATA
# =============================================================================


@st.composite
def financial_decimal_strategy(
    draw: st.DrawFn,
    min_value: float = -1000000.0,
    max_value: float = 1000000.0,
    allow_zero: bool = True,
    allow_negative: bool = True,
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
        return Decimal("0")

    # Generate values based on allowed ranges
    if not allow_negative:
        min_value = max(0.000001, min_value)

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
def positive_decimal_strategy(draw: st.DrawFn) -> Decimal:
    """Generate positive decimal values for prices and entry prices."""
    return draw(
        financial_decimal_strategy(
            min_value=0.000001, max_value=100000.0, allow_zero=False, allow_negative=False
        )
    )


@st.composite
def position_size_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic position size values (can be positive, negative, or zero)."""
    return draw(
        financial_decimal_strategy(
            min_value=-10000.0, max_value=10000.0, allow_zero=True, allow_negative=True
        )
    )


@st.composite
def pnl_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic PnL values (can be negative)."""
    return draw(
        financial_decimal_strategy(
            min_value=-100000.0, max_value=100000.0, allow_zero=True, allow_negative=True
        )
    )


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Any:
    """Generate valid Symbol objects for position testing."""
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
    """Generate valid UTC timestamps for position data."""
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 12, 31),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def valid_id_strategy(draw: st.DrawFn) -> str:
    """Generate valid ID strings for position testing."""
    return draw(
        st.text(
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=128,
        ).filter(lambda x: x.strip() and x == x.strip())
    )


@st.composite
def consistent_position_data_strategy(draw: st.DrawFn) -> tuple[Decimal, OrderSide, Decimal | None]:
    """Generate consistent size, side, and entry_price combinations.

    Returns:
        tuple: (size, side, entry_price) that satisfy business rules
    """
    size = draw(position_size_strategy())

    if size == Decimal("0"):
        # Flat position - side can be either, entry_price must be None
        side = draw(st.sampled_from([OrderSide.BUY, OrderSide.SELL]))
        entry_price = None
    elif size > Decimal("0"):
        # Long position - side must be BUY, entry_price must be positive
        side = OrderSide.BUY
        entry_price = draw(positive_decimal_strategy())
    else:
        # Short position - side must be SELL, entry_price must be positive
        side = OrderSide.SELL
        entry_price = draw(positive_decimal_strategy())

    return size, side, entry_price


@st.composite
def hyperliquid_position_details_strategy(draw: st.DrawFn) -> HyperliquidPositionDetails:
    """Generate valid HyperliquidPositionDetails for testing."""
    leverage_type = draw(st.sampled_from(["cross", "isolated"]))
    leverage_value = draw(st.integers(min_value=0, max_value=100))
    max_leverage = draw(st.integers(min_value=leverage_value, max_value=200))  # max >= current
    margin_used = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(
                min_value=0.0, max_value=100000.0, allow_zero=True, allow_negative=False
            ),
        )
    )

    return HyperliquidPositionDetails(
        leverage_type=leverage_type,
        leverage_value=leverage_value,
        max_leverage=max_leverage,
        margin_used=margin_used,
    )


@st.composite
def backpack_position_details_strategy(draw: st.DrawFn) -> BackpackPositionDetails:
    """Generate valid BackpackPositionDetails for testing."""
    leverage = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=100)))
    imf_base = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        )
    )
    imf_factor = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        )
    )
    mmf_base = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        )
    )
    mmf_factor = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        )
    )
    cumulative_funding = draw(st.one_of(st.none(), pnl_strategy()))

    return BackpackPositionDetails(
        leverage=leverage,
        imf_base=imf_base,
        imf_factor=imf_factor,
        mmf_base=mmf_base,
        mmf_factor=mmf_factor,
        cumulative_funding=cumulative_funding,
    )


# =============================================================================
# PROPERTY TESTS FOR DERIVATIVE POSITION MODEL
# =============================================================================


class TestDerivativePositionModelProperties:
    """Property-based tests for the DerivativePosition model."""

    @given(
        position_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        position_data=consistent_position_data_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_position_creation_properties(
        self,
        position_symbol: Any,
        exchange: ExchangeName,
        position_data: tuple[Decimal, OrderSide, Decimal | None],
        timestamp: datetime,
    ) -> None:
        """Property: Minimal position with consistent data should always be valid."""
        size, side, entry_price = position_data

        position = DerivativePosition(
            exchange=exchange,
            symbol=position_symbol,
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=timestamp,
        )

        # Properties: Required fields should be set correctly
        assert position.exchange == exchange
        assert position.symbol == position_symbol
        assert position.side == side
        assert position.size == size
        assert position.entry_price == entry_price
        assert position.timestamp == timestamp

        # Properties: Optional fields should have correct defaults
        assert position.mark_price is None
        assert position.liquidation_price is None
        assert position.unrealized_pnl is None
        assert position.realized_pnl is None
        assert position.strategy_name is None
        assert position.signal_id is None
        assert position.hl_details is None
        assert position.bp_details is None

        # Properties: Business logic consistency
        assert position.is_active() == (size != Decimal("0"))

    @given(
        position_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        position_data=consistent_position_data_strategy(),
        timestamp=valid_timestamp_strategy(),
        data=st.data(),
    )
    @settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_full_position_creation_properties(
        self,
        position_symbol: Any,
        exchange: ExchangeName,
        position_data: tuple[Decimal, OrderSide, Decimal | None],
        timestamp: datetime,
        data: st.DataObject,
    ) -> None:
        """Property: Full position with all fields should maintain data integrity."""
        size, side, entry_price = position_data

        # Generate optional fields
        mark_price = data.draw(st.one_of(st.none(), positive_decimal_strategy()))
        liquidation_price = data.draw(st.one_of(st.none(), positive_decimal_strategy()))
        unrealized_pnl = data.draw(st.one_of(st.none(), pnl_strategy()))
        realized_pnl = data.draw(st.one_of(st.none(), pnl_strategy()))

        strategy_name = data.draw(
            st.one_of(
                st.none(),
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
                    ),
                    min_size=1,
                    max_size=50,
                ).filter(lambda x: x.strip() and x == x.strip()),
            )
        )
        signal_id = data.draw(st.one_of(st.none(), valid_id_strategy()))

        # Generate exchange-specific details based on exchange
        if exchange == ExchangeName.HYPERLIQUID:
            hl_details = data.draw(st.one_of(st.none(), hyperliquid_position_details_strategy()))
            bp_details = None
        else:
            hl_details = None
            bp_details = data.draw(st.one_of(st.none(), backpack_position_details_strategy()))

        position = DerivativePosition(
            exchange=exchange,
            symbol=position_symbol,
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=timestamp,
            mark_price=mark_price,
            liquidation_price=liquidation_price,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=realized_pnl,
            strategy_name=strategy_name,
            signal_id=signal_id,
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Properties: All fields should be preserved exactly
        assert position.exchange == exchange
        assert position.symbol == position_symbol
        assert position.side == side
        assert position.size == size
        assert position.entry_price == entry_price
        assert position.timestamp == timestamp
        assert position.mark_price == mark_price
        assert position.liquidation_price == liquidation_price
        assert position.unrealized_pnl == unrealized_pnl
        assert position.realized_pnl == realized_pnl
        assert position.strategy_name == strategy_name
        assert position.signal_id == signal_id
        assert position.hl_details == hl_details
        assert position.bp_details == bp_details

    @given(
        size=position_size_strategy(),
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
        entry_price=st.one_of(st.none(), positive_decimal_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_position_logic_validation_properties(
        self, size: Decimal, side: OrderSide, entry_price: Decimal | None
    ) -> None:
        """Property: Position logic validation should enforce business rules."""
        base_kwargs = {
            "exchange": ExchangeName.HYPERLIQUID,
            "symbol": BTC_HL,
            "side": side,
            "size": size,
            "entry_price": entry_price,
            "timestamp": datetime.now(UTC),
        }

        # Check if this combination violates business rules
        has_logic_error = False
        error_pattern = ""

        if size == Decimal("0") and entry_price is not None:
            has_logic_error = True
            error_pattern = "entry_price must be None if size is zero"
        elif size != Decimal("0") and entry_price is None:
            has_logic_error = True
            error_pattern = "entry_price must be provided if size is non-zero"
        elif size != Decimal("0") and entry_price is not None and entry_price <= Decimal("0"):
            has_logic_error = True
            error_pattern = "entry_price must be positive .* if size is non-zero"
        elif size > Decimal("0") and side != OrderSide.BUY:
            has_logic_error = True
            error_pattern = "side must be BUY if size is positive"
        elif size < Decimal("0") and side != OrderSide.SELL:
            has_logic_error = True
            error_pattern = "side must be SELL if size is negative"

        if has_logic_error:
            with pytest.raises((ValidationError, PositionLogicError), match=error_pattern):
                DerivativePosition(**base_kwargs)
        else:
            # Should be valid
            position = DerivativePosition(**base_kwargs)
            assert position.size == size
            assert position.side == side
            assert position.entry_price == entry_price

    @given(
        mark_price=positive_decimal_strategy(),
        entry_price=positive_decimal_strategy(),
        size=position_size_strategy().filter(lambda x: x != Decimal("0")),
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
    )
    @settings(max_examples=200, deadline=None)
    def test_unrealized_pnl_calculation_properties(
        self, mark_price: Decimal, entry_price: Decimal, size: Decimal, side: OrderSide
    ) -> None:
        """Property: Unrealized PnL calculation should follow mathematical properties."""
        # Ensure side/size consistency
        if size > 0:
            side = OrderSide.BUY
        else:
            side = OrderSide.SELL

        position = DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            symbol=BTC_HL,
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=datetime.now(UTC),
        )

        unrealized_pnl = position.calculate_unrealized_pnl(mark_price)
        assert unrealized_pnl is not None

        # Properties: Mathematical invariants for PnL calculation
        size_abs = abs(size)

        if side == OrderSide.BUY:
            # Long position: profit when mark > entry, loss when mark < entry
            expected_pnl = size_abs * (mark_price - entry_price)
            assert unrealized_pnl == expected_pnl

            if mark_price > entry_price:
                assert unrealized_pnl > 0  # Profit
            elif mark_price < entry_price:
                assert unrealized_pnl < 0  # Loss
            else:
                assert unrealized_pnl == 0  # Break even
        else:
            # Short position: profit when mark < entry, loss when mark > entry
            expected_pnl = size_abs * (entry_price - mark_price)
            assert unrealized_pnl == expected_pnl

            if mark_price < entry_price:
                assert unrealized_pnl > 0  # Profit
            elif mark_price > entry_price:
                assert unrealized_pnl < 0  # Loss
            else:
                assert unrealized_pnl == 0  # Break even

    @given(
        position_symbol=valid_symbol_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        position_data=consistent_position_data_strategy(),
        timestamp=valid_timestamp_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_position_mutability_properties(
        self,
        position_symbol: Any,
        exchange: ExchangeName,
        position_data: tuple[Decimal, OrderSide, Decimal | None],
        timestamp: datetime,
    ) -> None:
        """Property: Position instances should be mutable with validation on assignment."""
        size, side, entry_price = position_data

        position = DerivativePosition(
            exchange=exchange,
            symbol=position_symbol,
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=timestamp,
        )

        # Property: Valid mutations should work
        new_timestamp = timestamp + timedelta(seconds=1)
        position.timestamp = new_timestamp
        assert position.timestamp == new_timestamp

        if size != Decimal("0"):
            # For active positions, we can update mark price and PnL
            new_mark_price = Decimal("50000.0")
            position.mark_price = new_mark_price
            assert position.mark_price == new_mark_price

            new_unrealized_pnl = Decimal("1000.0")
            position.unrealized_pnl = new_unrealized_pnl
            assert position.unrealized_pnl == new_unrealized_pnl

        # Property: Invalid mutations should be rejected
        with pytest.raises(ValidationError):
            position.mark_price = Decimal("-100.0")  # Negative mark price

    @given(
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        hl_details=hyperliquid_position_details_strategy(),
        bp_details=backpack_position_details_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_exchange_details_validation_properties(
        self,
        exchange: ExchangeName,
        hl_details: HyperliquidPositionDetails,
        bp_details: BackpackPositionDetails,
    ) -> None:
        """Property: Exchange-specific details should only be valid for correct exchange."""
        base_kwargs = {
            "symbol": BTC_HL,
            "side": OrderSide.BUY,
            "size": Decimal("1.0"),
            "entry_price": Decimal("50000.0"),
            "timestamp": datetime.now(UTC),
            "exchange": exchange,
        }

        # Property: Exchange details validation
        if exchange == ExchangeName.HYPERLIQUID:
            # Valid: HL exchange with HL details
            position = DerivativePosition(**base_kwargs, hl_details=hl_details)
            assert position.hl_details == hl_details
            assert position.bp_details is None

            # Invalid: HL exchange with BP details
            with pytest.raises(
                (ValidationError, PositionLogicError),
                match="Backpack details .* must be None for a Hyperliquid position",
            ):
                DerivativePosition(**base_kwargs, bp_details=bp_details)
        else:
            # Valid: BP exchange with BP details
            position = DerivativePosition(**base_kwargs, bp_details=bp_details)
            assert position.bp_details == bp_details
            assert position.hl_details is None

            # Invalid: BP exchange with HL details
            with pytest.raises(
                (ValidationError, PositionLogicError),
                match="Hyperliquid details .* must be None for a Backpack position",
            ):
                DerivativePosition(**base_kwargs, hl_details=hl_details)

    @given(
        field_name=st.sampled_from(["mark_price", "liquidation_price"]),
        invalid_value=st.one_of(
            st.just(Decimal("-0.001")),
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_financial_field_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: Financial fields should reject invalid values."""
        base_kwargs = {
            "exchange": ExchangeName.HYPERLIQUID,
            "symbol": BTC_HL,
            "side": OrderSide.BUY,
            "size": Decimal("1.0"),
            "entry_price": Decimal("50000.0"),
            "timestamp": datetime.now(UTC),
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid values should be rejected
        with pytest.raises(ValidationError):
            DerivativePosition(**kwargs)

    @given(
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
    )
    @settings(max_examples=100, deadline=None)
    def test_flat_position_properties(self, side: OrderSide) -> None:
        """Property: Flat positions should have specific behavior."""
        # Generate flat position directly
        size = Decimal("0")
        entry_price = None

        position = DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            symbol=BTC_HL,
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=datetime.now(UTC),
        )

        # Properties: Flat position behavior
        assert not position.is_active()
        assert position.calculate_unrealized_pnl(Decimal("50000.0")) is None


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC DETAIL MODELS
# =============================================================================


class TestHyperliquidPositionDetailsProperties:
    """Property-based tests for HyperliquidPositionDetails model."""

    @given(
        leverage_type=st.sampled_from(["cross", "isolated"]),
        leverage_value=st.integers(min_value=0, max_value=100),
        max_leverage=st.integers(min_value=0, max_value=200),
        margin_used=st.one_of(
            st.none(),
            financial_decimal_strategy(
                min_value=0.0, max_value=100000.0, allow_zero=True, allow_negative=False
            ),
        ),
    )
    @settings(max_examples=200, deadline=None)
    def test_hyperliquid_details_creation_properties(
        self,
        leverage_type: str,
        leverage_value: int,
        max_leverage: int,
        margin_used: Decimal | None,
    ) -> None:
        """Property: HyperliquidPositionDetails should handle all field combinations correctly."""
        # Ensure max_leverage >= leverage_value for business logic
        if max_leverage < leverage_value:
            max_leverage = leverage_value

        details = HyperliquidPositionDetails(
            leverage_type=leverage_type,
            leverage_value=leverage_value,
            max_leverage=max_leverage,
            margin_used=margin_used,
        )

        # Properties: All fields should be preserved
        assert details.leverage_type == leverage_type
        assert details.leverage_value == leverage_value
        assert details.max_leverage == max_leverage
        assert details.margin_used == margin_used

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.leverage_value = 999

    @given(
        field_name=st.sampled_from(["leverage_value", "max_leverage"]),
        invalid_value=st.one_of(st.just(-1), st.just(-100)),
    )
    @settings(max_examples=50, deadline=None)
    def test_hyperliquid_details_validation_properties(
        self, field_name: str, invalid_value: int
    ) -> None:
        """Property: HyperliquidPositionDetails should validate field values correctly."""
        base_kwargs = {
            "leverage_type": "cross",
            "leverage_value": 10,
            "max_leverage": 20,
        }

        kwargs = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        with pytest.raises((ValidationError, TypeFieldError)):
            HyperliquidPositionDetails(**kwargs)


class TestBackpackPositionDetailsProperties:
    """Property-based tests for BackpackPositionDetails model."""

    @given(
        leverage=st.one_of(st.none(), st.integers(min_value=0, max_value=100)),
        imf_base=st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        ),
        imf_factor=st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        ),
        mmf_base=st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        ),
        mmf_factor=st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=0.0, max_value=1.0, allow_negative=False),
        ),
        cumulative_funding=st.one_of(st.none(), pnl_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_backpack_details_creation_properties(
        self,
        leverage: int | None,
        imf_base: Decimal | None,
        imf_factor: Decimal | None,
        mmf_base: Decimal | None,
        mmf_factor: Decimal | None,
        cumulative_funding: Decimal | None,
    ) -> None:
        """Property: BackpackPositionDetails should handle all field combinations correctly."""
        details = BackpackPositionDetails(
            leverage=leverage,
            imf_base=imf_base,
            imf_factor=imf_factor,
            mmf_base=mmf_base,
            mmf_factor=mmf_factor,
            cumulative_funding=cumulative_funding,
        )

        # Properties: All fields should be preserved
        assert details.leverage == leverage
        assert details.imf_base == imf_base
        assert details.imf_factor == imf_factor
        assert details.mmf_base == mmf_base
        assert details.mmf_factor == mmf_factor
        assert details.cumulative_funding == cumulative_funding

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.leverage = 999

    @given(
        field_name=st.sampled_from([
            "imf_base",
            "imf_factor",
            "mmf_base",
            "mmf_factor",
            "cumulative_funding",
        ]),
        invalid_value=st.one_of(
            st.just(Decimal("NaN")),
            st.just(Decimal("Infinity")),
            st.just(Decimal("-Infinity")),
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_backpack_details_validation_properties(
        self, field_name: str, invalid_value: Decimal
    ) -> None:
        """Property: BackpackPositionDetails should validate decimal field values correctly."""
        kwargs = {field_name: invalid_value}

        with pytest.raises(ValidationError):
            BackpackPositionDetails(**kwargs)
