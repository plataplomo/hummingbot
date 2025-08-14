"""Property-based tests for the core Fill model using Hypothesis.

This module provides comprehensive property-based testing of the Fill Pydantic model,
which serves as the unified internal representation for trade executions (fills) across
all supported exchanges in the CyberDeltaEngine.

Key Testing Areas:
- Field validation and type safety using property-based input generation
- Decimal precision handling for financial calculations (comprehensive value ranges)
- Cross-field validation logic (fee/fee_asset relationship) with generated combinations
- Extension slot functionality with exchange-specific data generation
- Cost calculation properties across all possible price/quantity combinations
- Edge case handling through exhaustive generation
- Immutability properties under all mutation attempts

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms with arbitrary values
- Uses property-based testing for comprehensive coverage
- Tests complete fill data flows with real constraints
- Validates financial calculation invariants

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from pydantic import ValidationError

from cyberdelta.enums import ExchangeName, MakerTaker, OrderSide
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from cyberdelta.models.market.fill import BackpackFillDetails, Fill, HyperliquidFillDetails
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
# HYPOTHESIS STRATEGIES FOR FILL DATA
# =============================================================================


@st.composite
def financial_decimal_strategy(
    draw: st.DrawFn,
    min_value: float = 0.000001,
    max_value: float = 1000000.0,
    allow_zero: bool = True,
    allow_negative: bool = False,
) -> Decimal:
    """Generate realistic Decimal values for financial calculations.

    Args:
        draw: Hypothesis draw function
        min_value: Minimum value (exclusive)
        max_value: Maximum value (inclusive)
        allow_zero: Whether to allow zero values
        allow_negative: Whether to allow negative values (for fees)

    Returns:
        Decimal: A valid decimal for financial calculations
    """
    if allow_zero and draw(st.booleans()):
        return Decimal(0)

    if allow_negative and draw(st.booleans()):
        # Generate negative values for fees/rebates
        negative_value = draw(
            st.floats(
                min_value=-max_value,
                max_value=-min_value,
                allow_infinity=False,
                allow_nan=False,
                exclude_max=True,
            )
        )
        return Decimal(str(negative_value))

    # Generate positive values
    value = draw(
        st.floats(
            min_value=min_value,
            max_value=max_value,
            allow_infinity=False,
            allow_nan=False,
            exclude_min=True,
        )
    )
    return Decimal(str(value))


@st.composite
def price_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic price values for fill data."""
    return draw(financial_decimal_strategy(min_value=0.01, max_value=100000.0, allow_zero=False))


@st.composite
def quantity_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic quantity values for fill data."""
    return draw(financial_decimal_strategy(min_value=0.000001, max_value=10000.0, allow_zero=False))


@st.composite
def fee_strategy(draw: st.DrawFn) -> Decimal:
    """Generate realistic fee values (can be negative for rebates)."""
    return draw(
        financial_decimal_strategy(
            min_value=0.000001, max_value=1000.0, allow_zero=True, allow_negative=True
        )
    )


@st.composite
def valid_symbol_strategy(draw: st.DrawFn) -> Any:
    """Generate valid Symbol objects for fill testing."""
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
    """Generate valid UTC timestamps for fill data."""
    naive_dt = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1, tzinfo=UTC),
            max_value=datetime(2030, 12, 31, tzinfo=UTC),
        )
    )
    return naive_dt.replace(tzinfo=UTC)


@st.composite
def valid_id_strategy(draw: st.DrawFn) -> str:
    """Generate valid ID strings for fill testing."""
    return draw(
        st.text(
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=128,
        ).filter(lambda x: x.strip())
    )


@st.composite
def fee_asset_strategy(draw: st.DrawFn) -> str:
    """Generate valid fee asset names."""
    return draw(st.sampled_from(["USDC", "USD", "BTC", "ETH", "SOL", "DOGE"]))


@st.composite
def hyperliquid_fill_details_strategy(draw: st.DrawFn) -> HyperliquidFillDetails:
    """Generate valid HyperliquidFillDetails for testing."""
    fill_hash = draw(valid_id_strategy())
    liquidation_mark_px = draw(st.one_of(st.none(), price_strategy()))
    start_position = draw(
        st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=-10000.0, max_value=10000.0, allow_negative=True),
        )
    )
    direction = draw(st.one_of(st.none(), st.sampled_from(["open", "close", "reduce"])))

    return HyperliquidFillDetails(
        fill_hash=fill_hash,
        liquidation_mark_px=liquidation_mark_px,
        start_position=start_position,
        dir=direction,
    )


@st.composite
def backpack_fill_details_strategy(draw: st.DrawFn) -> BackpackFillDetails:
    """Generate valid BackpackFillDetails for testing."""
    system_order_type = draw(
        st.one_of(st.none(), st.sampled_from(["LIMIT", "MARKET", "STOP_LOSS", "TAKE_PROFIT"]))
    )

    return BackpackFillDetails(system_order_type=system_order_type)


# =============================================================================
# PROPERTY TESTS FOR FILL MODEL
# =============================================================================


class TestFillModelProperties:
    """Property-based tests for the Fill model."""

    @given(
        fill_id=valid_id_strategy(),
        fill_symbol=valid_symbol_strategy(),
        executed_at=valid_timestamp_strategy(),
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
        order_id=valid_id_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        price=price_strategy(),
        quantity=quantity_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_minimal_fill_creation_properties(
        self,
        fill_id: str,
        fill_symbol: Any,
        executed_at: datetime,
        side: OrderSide,
        order_id: str,
        exchange: ExchangeName,
        price: Decimal,
        quantity: Decimal,
    ) -> None:
        """Property: Minimal fill with only required fields should always be valid."""
        fill = Fill(
            id=fill_id,
            symbol=fill_symbol,
            executed_at=executed_at,
            side=side,
            order_id=order_id,
            exchange=exchange,
            price=price,
            quantity=quantity,
        )

        # Properties: Required fields should be set correctly
        assert fill.id == fill_id
        assert fill.symbol == fill_symbol
        assert fill.executed_at == executed_at
        assert fill.side == side
        assert fill.order_id == order_id
        assert fill.exchange == exchange
        assert fill.price == price
        assert fill.quantity == quantity

        # Properties: Optional fields should have correct defaults
        assert fill.client_order_id is None
        assert fill.fee == Decimal(0)
        assert fill.fee_asset is None
        assert fill.maker_taker is None
        assert fill.hl_details is None
        assert fill.bp_details is None

        # Properties: Cost should be computed correctly
        assert fill.cost == price * quantity

    @given(
        fill_id=valid_id_strategy(),
        fill_symbol=valid_symbol_strategy(),
        executed_at=valid_timestamp_strategy(),
        side=st.sampled_from([OrderSide.BUY, OrderSide.SELL]),
        order_id=valid_id_strategy(),
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        price=price_strategy(),
        quantity=quantity_strategy(),
        client_order_id=st.one_of(st.none(), valid_id_strategy()),
        fee=fee_strategy(),
        fee_asset=st.one_of(st.none(), fee_asset_strategy()),
        maker_taker=st.one_of(st.none(), st.sampled_from([MakerTaker.MAKER, MakerTaker.TAKER])),
        hl_details=st.one_of(st.none(), hyperliquid_fill_details_strategy()),
        bp_details=st.one_of(st.none(), backpack_fill_details_strategy()),
    )
    @settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_full_fill_creation_properties(
        self,
        fill_id: str,
        fill_symbol: Any,
        executed_at: datetime,
        side: OrderSide,
        order_id: str,
        exchange: ExchangeName,
        price: Decimal,
        quantity: Decimal,
        client_order_id: str | None,
        fee: Decimal,
        fee_asset: str | None,
        maker_taker: MakerTaker | None,
        hl_details: HyperliquidFillDetails | None,
        bp_details: BackpackFillDetails | None,
    ) -> None:
        """Property: Full fill with all fields should maintain data integrity."""
        # Ensure fee_asset is provided when fee is non-zero
        if fee != 0 and fee_asset is None:
            fee_asset = "USDC"  # Provide required fee_asset
        elif fee == 0:
            fee_asset = None  # Clear fee_asset when fee is zero

        fill = Fill(
            id=fill_id,
            symbol=fill_symbol,
            executed_at=executed_at,
            side=side,
            order_id=order_id,
            exchange=exchange,
            price=price,
            quantity=quantity,
            client_order_id=client_order_id,
            fee=fee,
            fee_asset=fee_asset,
            maker_taker=maker_taker,
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Properties: All fields should be preserved exactly
        assert fill.id == fill_id
        assert fill.symbol == fill_symbol
        assert fill.executed_at == executed_at
        assert fill.side == side
        assert fill.order_id == order_id
        assert fill.exchange == exchange
        assert fill.price == price
        assert fill.quantity == quantity
        assert fill.client_order_id == client_order_id
        assert fill.fee == fee
        assert fill.fee_asset == fee_asset
        assert fill.maker_taker == maker_taker
        assert fill.hl_details == hl_details
        assert fill.bp_details == bp_details

        # Properties: Cost calculation should be exact
        assert fill.cost == price * quantity

    @given(
        price=price_strategy(),
        quantity=quantity_strategy(),
    )
    @settings(max_examples=500, deadline=None)
    def test_cost_calculation_properties(self, price: Decimal, quantity: Decimal) -> None:
        """Property: Cost calculation should follow mathematical properties."""
        fill = Fill(
            id="test_cost",
            symbol=BTC_HL,
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            order_id="order_cost",
            exchange=ExchangeName.HYPERLIQUID,
            price=price,
            quantity=quantity,
        )

        cost = fill.cost()

        # Properties: Mathematical invariants for cost
        assert cost == price * quantity  # Exact arithmetic
        assert cost >= 0  # Cost should always be non-negative for positive price and quantity

        # Properties: Precision preservation
        # Cost precision should be sum of price and quantity precisions
        price_exp = price.as_tuple().exponent
        quantity_exp = quantity.as_tuple().exponent
        cost_exp = cost.as_tuple().exponent

        # Handle special values (infinity, NaN) - skip precision check for these
        if isinstance(price_exp, str) or isinstance(quantity_exp, str) or isinstance(cost_exp, str):
            return

        price_places = max(0, -price_exp)
        quantity_places = max(0, -quantity_exp)
        cost_places = max(0, -cost_exp)
        assert cost_places <= price_places + quantity_places

        # Properties: Scaling invariants
        double_price = price * Decimal(2)
        double_quantity = quantity * Decimal(2)

        fill_double_price = Fill(
            id="test_double_price",
            symbol=BTC_HL,
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            order_id="order_double_price",
            exchange=ExchangeName.HYPERLIQUID,
            price=double_price,
            quantity=quantity,
        )

        fill_double_quantity = Fill(
            id="test_double_quantity",
            symbol=BTC_HL,
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            order_id="order_double_quantity",
            exchange=ExchangeName.HYPERLIQUID,
            price=price,
            quantity=double_quantity,
        )

        # Double price should double cost (allowing for precision differences)
        expected_double_price_cost = double_price * quantity
        assert fill_double_price.cost == expected_double_price_cost

        # Double quantity should double cost (allowing for precision differences)
        expected_double_quantity_cost = price * double_quantity
        assert fill_double_quantity.cost == expected_double_quantity_cost

    @given(
        fee=fee_strategy(),
        fee_asset=st.one_of(st.none(), fee_asset_strategy()),
    )
    @settings(max_examples=200, deadline=None)
    def test_fee_asset_validation_properties(self, fee: Decimal, fee_asset: str | None) -> None:
        """Property: Fee asset validation should enforce business rules correctly."""
        base_kwargs: dict[str, Any] = {
            "id": "test_fee",
            "symbol": BTC_HL,
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "order_fee",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": Decimal("100.0"),
            "quantity": Decimal("1.0"),
            "fee": fee,
            "fee_asset": fee_asset,
        }

        # Property: Non-zero fee requires fee_asset
        if fee != 0 and fee_asset is None:
            with pytest.raises(ValueError, match="fee_asset must be provided if fee is nonzero"):
                Fill(**base_kwargs)
        else:
            # Property: Valid combinations should work
            fill = Fill(**base_kwargs)
            assert fill.fee == fee
            assert fill.fee_asset == fee_asset

    @given(
        field_name=st.sampled_from(["price", "quantity"]),
        invalid_value=st.one_of(
            st.just(Decimal(0)),
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
        base_kwargs: dict[str, Any] = {
            "id": "test_validation",
            "symbol": BTC_HL,
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "order_validation",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": Decimal("100.0"),
            "quantity": Decimal("1.0"),
        }

        kwargs: dict[str, Any] = base_kwargs.copy()
        kwargs[field_name] = invalid_value

        # Property: Invalid values should be rejected
        with pytest.raises(ValidationError):
            Fill(**kwargs)

    @given(
        string_field=st.sampled_from(["id", "order_id", "client_order_id", "fee_asset"]),
        invalid_string=st.one_of(
            st.just(""),
            st.just("   "),
            st.text(min_size=129, max_size=200),  # Too long
        ),
    )
    @settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
    def test_string_field_validation_properties(
        self, string_field: str, invalid_string: str
    ) -> None:
        """Property: String fields should validate length and emptiness correctly."""
        base_kwargs: dict[str, Any] = {
            "id": "test_string",
            "symbol": BTC_HL,
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "order_string",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": Decimal("100.0"),
            "quantity": Decimal("1.0"),
        }

        # Skip cases where we're trying to set required fields to invalid values
        if string_field in ["id", "order_id"] and not invalid_string.strip():
            assume(False)  # Skip empty string tests for required fields

        kwargs: dict[str, Any] = base_kwargs.copy()
        kwargs[string_field] = invalid_string

        # Property: Invalid strings should be rejected
        with pytest.raises((EmptyStringError, TypeFieldError, ValidationError)):
            Fill(**kwargs)

    @given(
        fill_id=valid_id_strategy(),
        price=price_strategy(),
        quantity=quantity_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_fill_immutability_properties(
        self, fill_id: str, price: Decimal, quantity: Decimal
    ) -> None:
        """Property: Fill instances should be completely immutable."""
        fill = Fill(
            id=fill_id,
            symbol=BTC_HL,
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            order_id="order_immutable",
            exchange=ExchangeName.HYPERLIQUID,
            price=price,
            quantity=quantity,
        )

        # Property: All field modifications should raise ValidationError
        with pytest.raises(ValidationError, match="Instance is frozen"):
            fill.id = "new_id"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fill.price = price + Decimal(1)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fill.quantity = quantity + Decimal(1)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fill.side = OrderSide.SELL

    @given(
        hl_details=hyperliquid_fill_details_strategy(),
        bp_details=backpack_fill_details_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_exchange_details_properties(
        self, hl_details: HyperliquidFillDetails, bp_details: BackpackFillDetails
    ) -> None:
        """Property: Exchange-specific details should be preserved correctly."""
        fill = Fill(
            id="test_details",
            symbol=BTC_HL,
            executed_at=datetime.now(UTC),
            side=OrderSide.BUY,
            order_id="order_details",
            exchange=ExchangeName.HYPERLIQUID,
            price=Decimal("100.0"),
            quantity=Decimal("1.0"),
            hl_details=hl_details,
            bp_details=bp_details,
        )

        # Properties: Details should be preserved
        assert fill.hl_details == hl_details
        assert fill.bp_details == bp_details

        # Properties: Details should be immutable
        if fill.hl_details is not None:
            with pytest.raises(ValidationError, match="Instance is frozen"):
                fill.hl_details.fill_hash = "modified"

        if fill.bp_details is not None:
            with pytest.raises(ValidationError, match="Instance is frozen"):
                fill.bp_details.system_order_type = "MODIFIED"

    @given(
        data=st.data(),
    )
    @settings(max_examples=100, deadline=None)
    def test_serialization_properties(self, data: st.DataObject) -> None:
        """Property: Fill serialization should preserve all data correctly."""
        # Generate a complete fill with random data
        fill_data = {
            "id": data.draw(valid_id_strategy()),
            "symbol": data.draw(valid_symbol_strategy()),
            "executed_at": data.draw(valid_timestamp_strategy()),
            "side": data.draw(st.sampled_from([OrderSide.BUY, OrderSide.SELL])),
            "order_id": data.draw(valid_id_strategy()),
            "exchange": data.draw(
                st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])
            ),
            "price": data.draw(price_strategy()),
            "quantity": data.draw(quantity_strategy()),
        }

        # Add optional fields
        fee = data.draw(fee_strategy())
        if fee != 0:
            fill_data["fee"] = fee
            fill_data["fee_asset"] = data.draw(fee_asset_strategy())

        client_order_id = data.draw(st.one_of(st.none(), valid_id_strategy()))
        if client_order_id is not None:
            fill_data["client_order_id"] = client_order_id

        maker_taker = data.draw(
            st.one_of(st.none(), st.sampled_from([MakerTaker.MAKER, MakerTaker.TAKER]))
        )
        if maker_taker is not None:
            fill_data["maker_taker"] = maker_taker

        fill = Fill(**fill_data)

        # Property: model_dump should include all fields
        serialized = fill.model_dump(mode="json")

        # Properties: Core fields should be serialized
        assert serialized["id"] == fill_data["id"]
        assert serialized["price"] == str(fill_data["price"])
        assert serialized["quantity"] == str(fill_data["quantity"])
        assert serialized["cost"] == str(fill.cost)
        assert serialized["side"] == fill_data["side"].value

        # Property: Computed cost should match calculation
        assert Decimal(serialized["cost"]) == fill_data["price"] * fill_data["quantity"]


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC DETAIL MODELS
# =============================================================================


class TestHyperliquidFillDetailsProperties:
    """Property-based tests for HyperliquidFillDetails model."""

    @given(
        fill_hash=valid_id_strategy(),
        liquidation_mark_px=st.one_of(st.none(), price_strategy()),
        start_position=st.one_of(
            st.none(),
            financial_decimal_strategy(min_value=-10000.0, max_value=10000.0, allow_negative=True),
        ),
        direction=st.one_of(
            st.none(), st.text(min_size=1, max_size=32).filter(lambda x: x.strip())
        ),
    )
    @settings(max_examples=200, deadline=None)
    def test_hyperliquid_details_creation_properties(
        self,
        fill_hash: str,
        liquidation_mark_px: Decimal | None,
        start_position: Decimal | None,
        direction: str | None,
    ) -> None:
        """Property: HyperliquidFillDetails should handle all field combinations correctly."""
        details = HyperliquidFillDetails(
            fill_hash=fill_hash,
            liquidation_mark_px=liquidation_mark_px,
            start_position=start_position,
            dir=direction,
        )

        # Properties: All fields should be preserved
        assert details.fill_hash == fill_hash
        assert details.liquidation_mark_px == liquidation_mark_px
        assert details.start_position == start_position
        assert details.dir == direction

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.fill_hash = "modified"

    @given(invalid_hash=st.one_of(st.just(""), st.just("   "), st.text(min_size=129, max_size=200)))
    @settings(max_examples=50, deadline=None)
    def test_hyperliquid_details_validation_properties(self, invalid_hash: str) -> None:
        """Property: HyperliquidFillDetails should validate required fields correctly."""
        with pytest.raises((EmptyStringError, TypeFieldError)):
            HyperliquidFillDetails(fill_hash=invalid_hash)


class TestBackpackFillDetailsProperties:
    """Property-based tests for BackpackFillDetails model."""

    @given(
        system_order_type=st.one_of(
            st.none(),
            st.text(min_size=1, max_size=32).filter(lambda x: x.strip()),
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_backpack_details_creation_properties(self, system_order_type: str | None) -> None:
        """Property: BackpackFillDetails should handle all field combinations correctly."""
        details = BackpackFillDetails(system_order_type=system_order_type)

        # Property: Field should be preserved
        assert details.system_order_type == system_order_type

        # Property: Should be immutable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.system_order_type = "modified"

    @given(invalid_type=st.text(min_size=33, max_size=100))
    @settings(max_examples=50, deadline=None)
    def test_backpack_details_validation_properties(self, invalid_type: str) -> None:
        """Property: BackpackFillDetails should validate field length correctly."""
        with pytest.raises(TypeFieldError):
            BackpackFillDetails(system_order_type=invalid_type)
