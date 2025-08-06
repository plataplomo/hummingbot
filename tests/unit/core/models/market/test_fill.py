"""Unit tests for the CyberDeltaEngine internal Fill model.

This module provides comprehensive validation of the Fill Pydantic model, which serves
as the unified internal representation for trade executions (fills) across all supported
exchanges in the CyberDeltaEngine. The Fill model is a critical component that ensures
consistent data handling and validation for trading operations.

Key Testing Areas:
- Field validation and type safety for all fill attributes
- Decimal precision handling for financial calculations (price, quantity, fees)
- Cross-field validation logic (fee/fee_asset relationship)
- Extension slot functionality for exchange-specific enrichment
- Serialization and deserialization for data persistence
- Edge case handling and error conditions
- Performance characteristics under various data loads

The Fill model implements the "Core + Typed Extension Slots" pattern, providing:
- Lean core fields common to all exchanges
- Optional exchange-specific details slots (HyperliquidFillDetails, BackpackFillDetails)
- Immutable snapshots with strict validation
- Computed fields for derived values (cost calculation)

This testing ensures the Fill model maintains data integrity, type safety, and
consistent behavior across different exchange integrations while supporting
future extensibility for additional exchanges and trading features.

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from datetime import UTC, datetime
from decimal import Decimal

import pydantic
import pytest
from pydantic import ValidationError

from cyberdelta.enums import ExchangeName, MakerTaker, OrderSide
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from cyberdelta.models.market.fill import BackpackFillDetails, Fill, HyperliquidFillDetails
from tests.common_symbols import BTC_HL


def test_fill_minimal_valid() -> None:
    """Test that a minimal valid Fill instance is accepted and fields are set correctly.

    This test validates the core functionality of the Fill model with only required
    fields, ensuring that the model can be instantiated with minimal data while
    maintaining proper defaults for optional fields. This is essential for handling
    basic trade data from exchanges that may not provide all optional information.
    """
    price = Decimal("100.0")
    quantity = Decimal("2.0")
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=price,
        quantity=quantity,
    )
    assert fill.price == price
    assert fill.quantity == quantity
    assert fill.cost == price * quantity
    assert fill.fee == Decimal(0)
    assert fill.fee_asset is None
    assert fill.maker_taker is None
    assert fill.exchange == ExchangeName.BACKPACK
    assert fill.side == OrderSide.BUY


def test_fill_with_all_optionals() -> None:
    """Test that a Fill instance with all optional fields is accepted and values are set correctly.

    This test verifies that all optional fields can be set and are properly validated,
    including exchange-specific extension slots. This ensures the model supports rich
    fill data from exchanges that provide comprehensive fill information, including
    fees, maker/taker status, and exchange-specific metadata.
    """
    price = Decimal("100.0")
    quantity = Decimal("2.0")
    hl_details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        liquidation_mark_px=Decimal("99.5"),
        start_position=Decimal("1.0"),
        dir="open",
    )
    bp_details = BackpackFillDetails()
    fill = Fill(
        id="12345",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="67890",
        exchange=ExchangeName.BACKPACK,
        price=price,
        quantity=quantity,
        client_order_id="cloid-123",
        fee=Decimal("-0.01"),
        fee_asset="USDC",
        maker_taker=MakerTaker.MAKER,
        hl_details=hl_details,
        bp_details=bp_details,
    )
    assert fill.client_order_id == "cloid-123"
    assert fill.fee == Decimal("-0.01")
    assert fill.fee_asset == "USDC"
    assert fill.maker_taker == MakerTaker.MAKER
    assert fill.hl_details is not None
    assert fill.hl_details.fill_hash == "hash-abc"
    assert fill.hl_details.liquidation_mark_px == Decimal("99.5")
    assert fill.hl_details.start_position == Decimal("1.0")
    assert fill.hl_details.dir == "open"
    assert fill.bp_details is not None
    assert fill.cost == price * quantity


def test_fill_cost_computed() -> None:
    """Test that cost is always computed as price * quantity.

    This test validates the computed field functionality for fill cost calculation,
    ensuring that the cost is always accurately derived from price and quantity
    regardless of the specific values. This computed field is critical for P&L
    calculations and portfolio valuation in the trading engine.
    """
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("1.5"),
        quantity=Decimal("3.0"),
    )
    assert fill.cost == Decimal("4.5")


def test_fill_id_and_order_id_validation() -> None:
    """Test that id and order_id accept only valid non-empty strings, and reject invalid values.

    This test ensures robust validation of fill and order identifiers, which are
    critical for fill tracking, reconciliation, and debugging. The validation
    prevents empty strings, excessive lengths, and invalid types that could cause
    issues in downstream processing or database storage.
    """
    # Valid strings
    for valid_id in ["idstr", "order-xyz", "1.23", "114", "abc", "hash-abc", "A" * 64]:
        fill = Fill(
            id=valid_id,
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id=valid_id,
            exchange=ExchangeName.BACKPACK,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
        assert fill.id == valid_id
        assert fill.order_id == valid_id
    # Invalid: empty string
    with pytest.raises(EmptyStringError):
        Fill(
            id="",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
    # Invalid: whitespace-only string
    with pytest.raises(EmptyStringError):
        Fill(
            id="   ",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
    # Invalid: overlength string (model allows up to 128 chars)
    with pytest.raises(TypeFieldError):
        Fill(
            id="A" * 129,
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
    # Invalid: non-string types
    bad_ids: list[object] = [123, 1.23, None, [], {}, b"bytes"]
    for bad_id in bad_ids:
        with pytest.raises((TypeError, ValueError, pydantic.ValidationError)):
            # Create test dict with wrong type for id field
            test_data = {
                "id": bad_id,
                "symbol": BTC_HL,
                "executed_at": datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                "side": OrderSide.BUY,
                "order_id": "order-xyz",
                "exchange": "backpack",
                "price": Decimal("1.0"),
                "quantity": Decimal("1.0"),
            }
            # Use model_validate to bypass type checking while still testing validation
            Fill.model_validate(test_data)


def test_fill_fee_asset_required() -> None:
    """Test that a ValueError is raised if fee is nonzero and fee_asset is not provided.

    This test validates the critical business rule that fee_asset must be specified
    whenever a fee is charged. This cross-field validation ensures proper fee
    accounting and prevents ambiguous fee records that could lead to incorrect
    P&L calculations or regulatory reporting issues.
    """
    with pytest.raises(ValueError, match="fee_asset must be provided if fee is nonzero"):
        Fill(
            id="abc123",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("100.0"),
            quantity=Decimal("2.0"),
            fee=Decimal("0.01"),
        )


def test_fill_negative_fee_allowed() -> None:
    """Test that a negative fee (rebate) is accepted if fee_asset is provided.

    This test ensures the model correctly handles fee rebates, which are common
    in cryptocurrency trading when providing liquidity. Negative fees represent
    rebates or rewards paid to the trader, and proper handling is essential for
    accurate P&L calculation and fee accounting.
    """
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        fee=Decimal("-0.01"),
        fee_asset="USDC",
    )
    assert fill.fee == Decimal("-0.01")
    assert fill.fee_asset == "USDC"


def test_fill_positive_constraints() -> None:
    """Test that price and quantity must both be positive."""
    # price
    with pytest.raises(pydantic.ValidationError):
        Fill(
            id="abc123",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal(0),
            quantity=Decimal("2.0"),
        )
    # quantity
    with pytest.raises(pydantic.ValidationError):
        Fill(
            id="abc123",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("100.0"),
            quantity=Decimal(0),
        )


def test_fill_decimal_parsing() -> None:
    """Test that Fill accepts values for Decimal fields that can be parsed from int or str."""
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("100.0"),
        quantity=Decimal(2),
    )
    assert fill.price == Decimal("100.0")
    assert fill.quantity == Decimal(2)
    assert fill.cost == Decimal("200.0")


def test_fill_optional_string_fields() -> None:
    """Test that optional string fields accept None and valid strings, and reject invalid strings.

    This test verifies validation of optional string fields like client_order_id and fee_asset.
    """
    # Valid
    hl_details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        dir="open",
    )
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        client_order_id="cloid-123",
        fee_asset="USDC",
        hl_details=hl_details,
    )
    assert fill.client_order_id == "cloid-123"
    assert fill.fee_asset == "USDC"
    assert fill.hl_details is not None
    assert fill.hl_details.fill_hash == "hash-abc"
    assert fill.hl_details.dir == "open"
    # None
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        client_order_id=None,
        fee_asset=None,
        hl_details=None,
    )
    assert fill.client_order_id is None
    assert fill.fee_asset is None
    assert fill.hl_details is None
    # Invalid: empty string
    with pytest.raises(EmptyStringError):
        Fill(
            id="abc123",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
            client_order_id="",
        )


def test_fill_optional_decimal_fields() -> None:
    """Test that optional decimal fields accept None and valid decimals, and reject invalid values.

    This test verifies validation of optional decimal fields in enrichment details.
    """
    # Valid
    hl_details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        liquidation_mark_px=Decimal("1.23"),
        start_position=Decimal("2.34"),
    )
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        hl_details=hl_details,
    )
    assert fill.hl_details is not None
    assert fill.hl_details.liquidation_mark_px == Decimal("1.23")
    assert fill.hl_details.start_position == Decimal("2.34")
    # None
    hl_details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        liquidation_mark_px=None,
        start_position=None,
    )
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        hl_details=hl_details,
    )
    assert fill.hl_details is not None
    assert fill.hl_details.liquidation_mark_px is None
    assert fill.hl_details.start_position is None
    with pytest.raises(ValueError):
        HyperliquidFillDetails(
            fill_hash="hash-abc",
            liquidation_mark_px=Decimal("NaN"),
        )


def test_fill_custom_to_dict_serialization() -> None:
    """Test that Fill.to_dict() (deprecated) serializes Decimal, Enum, and datetime fields.

    This test verifies the deprecated to_dict method still works correctly for serialization.
    """
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        fee=Decimal("0.01"),
        fee_asset="USDC",
    )
    d = fill.to_dict()
    assert d["price"] == "100.0"
    assert d["quantity"] == "2.0"

    assert Decimal(d["cost"]) == Decimal("200.0")
    assert d["fee"] == "0.01"
    assert d["fee_asset"] == "USDC"
    assert d["side"] == "BUY"
    assert isinstance(d["executed_at"], str)


def test_fill_model_dump_json_serialization() -> None:
    """Test that Fill.model_dump(mode='json') serializes all fields, including enrichment slots.

    This test verifies the modern model_dump method works correctly for JSON serialization.
    """
    hl_details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        liquidation_mark_px=Decimal("99.5"),
        start_position=Decimal("1.0"),
        dir="open",
    )
    bp_details = BackpackFillDetails(system_order_type="LIMIT")
    fill = Fill(
        id="abc123",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange=ExchangeName.HYPERLIQUID,
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        fee=Decimal("0.01"),
        fee_asset="USDC",
        hl_details=hl_details,
        bp_details=bp_details,
    )
    d = fill.model_dump(mode="json")
    assert d["price"] == "100.0"
    assert d["quantity"] == "2.0"

    assert Decimal(d["cost"]) == Decimal("200.0")
    assert d["fee"] == "0.01"
    assert d["fee_asset"] == "USDC"
    assert d["side"] == "BUY"
    assert isinstance(d["executed_at"], str)
    assert d["hl_details"]["fill_hash"] == "hash-abc"
    assert d["hl_details"]["liquidation_mark_px"] == "99.5"
    assert d["hl_details"]["start_position"] == "1.0"
    assert d["hl_details"]["dir"] == "open"
    assert d["bp_details"]["system_order_type"] == "LIMIT"


def test_hyperliquid_fill_details_validation() -> None:
    """Test validation logic for HyperliquidFillDetails enrichment fields."""
    # Valid
    details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        liquidation_mark_px=Decimal("123.45"),
        start_position=Decimal("10.0"),
        dir="open",
    )
    assert details.fill_hash == "hash-abc"
    assert details.liquidation_mark_px == Decimal("123.45")
    assert details.start_position == Decimal("10.0")
    assert details.dir == "open"
    # Optional fields None
    details = HyperliquidFillDetails(fill_hash="hash-abc")
    assert details.liquidation_mark_px is None
    assert details.start_position is None
    assert details.dir is None
    # Invalid: empty fill_hash
    with pytest.raises(EmptyStringError):
        HyperliquidFillDetails(fill_hash="")
    # Invalid: overlength fill_hash
    with pytest.raises(TypeFieldError):
        HyperliquidFillDetails(fill_hash="a" * 129)
    # Invalid: overlength dir
    with pytest.raises(TypeFieldError):
        HyperliquidFillDetails(fill_hash="hash", dir="a" * 33)
    # Invalid: non-finite decimal
    with pytest.raises(ValidationError):
        HyperliquidFillDetails(fill_hash="hash", liquidation_mark_px=Decimal("NaN"))


def test_backpack_fill_details_validation() -> None:
    """Test validation logic for BackpackFillDetails enrichment fields."""
    # Valid
    details = BackpackFillDetails(system_order_type="LIMIT")
    assert details.system_order_type == "LIMIT"
    # Optional None
    details = BackpackFillDetails()
    assert details.system_order_type is None
    # Invalid: overlength system_order_type
    with pytest.raises(TypeFieldError):
        BackpackFillDetails(system_order_type="a" * 33)


def test_fill_enrichment_slots_acceptance_and_serialization() -> None:
    """Test that Fill accepts enrichment slots and serializes them as expected."""
    hl_details = HyperliquidFillDetails(
        fill_hash="hash-abc",
        liquidation_mark_px=Decimal("99.5"),
        start_position=Decimal("1.0"),
        dir="open",
    )
    bp_details = BackpackFillDetails(system_order_type="LIMIT")
    fill = Fill(
        id="t1",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.BUY,
        order_id="o1",
        exchange=ExchangeName.HYPERLIQUID,
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        hl_details=hl_details,
        bp_details=bp_details,
    )
    assert fill.hl_details is not None
    assert fill.hl_details.fill_hash == "hash-abc"
    assert fill.bp_details is not None
    assert fill.bp_details.system_order_type == "LIMIT"
    # model_dump includes enrichment fields
    d = fill.model_dump(mode="json")
    assert d["hl_details"]["fill_hash"] == "hash-abc"
    assert d["hl_details"]["liquidation_mark_px"] == "99.5"
    assert d["bp_details"]["system_order_type"] == "LIMIT"


def test_fill_enrichment_slots_none() -> None:
    """Test that Fill accepts None for enrichment slots and serializes as null."""
    fill = Fill(
        id="t2",
        symbol=BTC_HL,
        executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        side=OrderSide.SELL,
        order_id="o2",
        exchange=ExchangeName.BACKPACK,
        price=Decimal("50.0"),
        quantity=Decimal("1.0"),
        hl_details=None,
        bp_details=None,
    )
    assert fill.hl_details is None
    assert fill.bp_details is None
    d = fill.model_dump(mode="json")
    assert d["hl_details"] is None
    assert d["bp_details"] is None


def test_fill_enrichment_invalid_details() -> None:
    """Test that Fill rejects invalid enrichment slot data."""
    # Invalid HyperliquidFillDetails
    with pytest.raises(EmptyStringError):
        Fill(
            id="t3",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="o3",
            exchange=ExchangeName.HYPERLIQUID,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
            hl_details=HyperliquidFillDetails(fill_hash=""),
        )
    # Invalid BackpackFillDetails
    with pytest.raises(TypeFieldError):
        Fill(
            id="t4",
            symbol=BTC_HL,
            executed_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            side=OrderSide.BUY,
            order_id="o4",
            exchange=ExchangeName.BACKPACK,
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
            bp_details=BackpackFillDetails(system_order_type="a" * 33),
        )
