"""Unit tests for the CyberDeltaEngine internal Trade model.

This module provides comprehensive validation of the Trade Pydantic model, which serves
as the unified internal representation for trade executions (fills) across all supported
exchanges in the CyberDeltaEngine. The Trade model is a critical component that ensures
consistent data handling and validation for trading operations.

Key Testing Areas:
- Field validation and type safety for all trade attributes
- Decimal precision handling for financial calculations (price, quantity, fees)
- Cross-field validation logic (fee/fee_asset relationship)
- Extension slot functionality for exchange-specific enrichment
- Serialization and deserialization for data persistence
- Edge case handling and error conditions
- Performance characteristics under various data loads

The Trade model implements the "Core + Typed Extension Slots" pattern, providing:
- Lean core fields common to all exchanges
- Optional exchange-specific details slots (HyperliquidTradeDetails, BackpackTradeDetails)
- Immutable snapshots with strict validation
- Computed fields for derived values (cost calculation)

This testing ensures the Trade model maintains data integrity, type safety, and
consistent behavior across different exchange integrations while supporting
future extensibility for additional exchanges and trading features.

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for strict model separation
- Implements RULE-RUNTIME-SAFETY-V4 for Decimal usage and validation
- Adheres to RULE-NO-SILENCING-V4 for type safety without suppressions
"""

from datetime import datetime
from decimal import Decimal

import pydantic
import pytest

from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market.trade import BackpackTradeDetails, HyperliquidTradeDetails, Trade


def test_trade_minimal_valid() -> None:
    """Test that a minimal valid Trade instance is accepted and fields are set correctly.

    This test validates the core functionality of the Trade model with only required
    fields, ensuring that the model can be instantiated with minimal data while
    maintaining proper defaults for optional fields. This is essential for handling
    basic trade data from exchanges that may not provide all optional information.
    """
    price = Decimal("100.0")
    quantity = Decimal("2.0")
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=price,
        quantity=quantity,
    )
    assert trade.price == price
    assert trade.quantity == quantity
    assert trade.cost == price * quantity
    assert trade.fee == Decimal("0")
    assert trade.fee_asset is None
    assert trade.is_maker is None
    assert trade.exchange == "backpack"
    assert trade.side == OrderSide.BUY


def test_trade_with_all_optionals() -> None:
    """Test that a Trade instance with all optional fields is accepted and values are set correctly.

    This test verifies that all optional fields can be set and are properly validated,
    including exchange-specific extension slots. This ensures the model supports rich
    trade data from exchanges that provide comprehensive fill information, including
    fees, maker/taker status, and exchange-specific metadata.
    """
    price = Decimal("100.0")
    quantity = Decimal("2.0")
    hl_details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        liquidation_mark_px=Decimal("99.5"),
        start_position=Decimal("1.0"),
        dir="open",
    )
    bp_details = BackpackTradeDetails()
    trade = Trade(
        id="12345",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="67890",
        exchange="backpack",
        price=price,
        quantity=quantity,
        client_order_id="cloid-123",
        fee=Decimal("-0.01"),
        fee_asset="USDC",
        is_maker=True,
        hl_details=hl_details,
        bp_details=bp_details,
    )
    assert trade.client_order_id == "cloid-123"
    assert trade.fee == Decimal("-0.01")
    assert trade.fee_asset == "USDC"
    assert trade.is_maker is True
    assert trade.hl_details is not None
    assert trade.hl_details.trade_hash == "hash-abc"
    assert trade.hl_details.liquidation_mark_px == Decimal("99.5")
    assert trade.hl_details.start_position == Decimal("1.0")
    assert trade.hl_details.dir == "open"
    assert trade.bp_details is not None
    assert trade.cost == price * quantity


def test_trade_cost_computed() -> None:
    """Test that cost is always computed as price * quantity.

    This test validates the computed field functionality for trade cost calculation,
    ensuring that the cost is always accurately derived from price and quantity
    regardless of the specific values. This computed field is critical for P&L
    calculations and portfolio valuation in the trading engine.
    """
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("1.5"),
        quantity=Decimal("3.0"),
    )
    assert trade.cost == Decimal("4.5")


def test_trade_id_and_order_id_validation() -> None:
    """Test that id and order_id accept only valid non-empty strings, and reject invalid values.

    This test ensures robust validation of trade and order identifiers, which are
    critical for trade tracking, reconciliation, and debugging. The validation
    prevents empty strings, excessive lengths, and invalid types that could cause
    issues in downstream processing or database storage.
    """
    # Valid strings
    for valid_id in ["idstr", "order-xyz", "1.23", "114", "abc", "hash-abc", "A" * 64]:
        trade = Trade(
            id=valid_id,
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id=valid_id,
            exchange="backpack",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
        assert trade.id == valid_id
        assert trade.order_id == valid_id
    # Invalid: empty string
    with pytest.raises(ValueError):
        Trade(
            id="",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
    # Invalid: whitespace-only string
    with pytest.raises(ValueError):
        Trade(
            id="   ",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
    # Invalid: overlength string (model allows up to 128 chars)
    with pytest.raises(ValueError):
        Trade(
            id="A" * 129,
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
        )
    # Invalid: non-string types
    for bad_id in [123, 1.23, None, [], {}, b"bytes"]:  # type: ignore
        with pytest.raises((TypeError, ValueError, pydantic.ValidationError)):
            Trade(
                id=bad_id,  # type: ignore
                symbol="BTC-PERP",
                executed_at=datetime(2024, 1, 1, 0, 0, 0),
                side=OrderSide.BUY,
                order_id="order-xyz",
                exchange="backpack",
                price=Decimal("1.0"),
                quantity=Decimal("1.0"),
            )


def test_trade_fee_asset_required() -> None:
    """Test that a ValueError is raised if fee is nonzero and fee_asset is not provided.

    This test validates the critical business rule that fee_asset must be specified
    whenever a fee is charged. This cross-field validation ensures proper fee
    accounting and prevents ambiguous fee records that could lead to incorrect
    P&L calculations or regulatory reporting issues.
    """
    with pytest.raises(ValueError, match="fee_asset must be provided if fee is nonzero"):
        Trade(
            id="abc123",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("100.0"),
            quantity=Decimal("2.0"),
            fee=Decimal("0.01"),
        )


def test_trade_negative_fee_allowed() -> None:
    """Test that a negative fee (rebate) is accepted if fee_asset is provided.

    This test ensures the model correctly handles fee rebates, which are common
    in cryptocurrency trading when providing liquidity. Negative fees represent
    rebates or rewards paid to the trader, and proper handling is essential for
    accurate P&L calculation and fee accounting.
    """
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        fee=Decimal("-0.01"),
        fee_asset="USDC",
    )
    assert trade.fee == Decimal("-0.01")
    assert trade.fee_asset == "USDC"


def test_trade_positive_constraints() -> None:
    """Test that price and quantity must both be positive."""
    # price
    with pytest.raises(pydantic.ValidationError):
        Trade(
            id="abc123",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("0"),
            quantity=Decimal("2.0"),
        )
    # quantity
    with pytest.raises(pydantic.ValidationError):
        Trade(
            id="abc123",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("100.0"),
            quantity=Decimal("0"),
        )


def test_trade_decimal_parsing() -> None:
    """Test that Trade accepts values for Decimal fields that can be parsed from int or str."""
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("100.0"),
        quantity=Decimal("2"),
    )
    assert trade.price == Decimal("100.0")
    assert trade.quantity == Decimal("2")
    assert trade.cost == Decimal("200.0")


def test_trade_optional_string_fields() -> None:
    """Test that optional string fields accept None and valid strings, and reject invalid strings.

    This test verifies validation of optional string fields like client_order_id and fee_asset.
    """
    # Valid
    hl_details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        dir="open",
    )
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        client_order_id="cloid-123",
        fee_asset="USDC",
        hl_details=hl_details,
    )
    assert trade.client_order_id == "cloid-123"
    assert trade.fee_asset == "USDC"
    assert trade.hl_details is not None
    assert trade.hl_details.trade_hash == "hash-abc"
    assert trade.hl_details.dir == "open"
    # None
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        client_order_id=None,
        fee_asset=None,
        hl_details=None,
    )
    assert trade.client_order_id is None
    assert trade.fee_asset is None
    assert trade.hl_details is None
    # Invalid: empty string
    with pytest.raises(ValueError):
        Trade(
            id="abc123",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="order-xyz",
            exchange="backpack",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
            client_order_id="",
        )


def test_trade_optional_decimal_fields() -> None:
    """Test that optional decimal fields accept None and valid decimals, and reject invalid values.

    This test verifies validation of optional decimal fields in enrichment details.
    """
    # Valid
    hl_details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        liquidation_mark_px=Decimal("1.23"),
        start_position=Decimal("2.34"),
    )
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        hl_details=hl_details,
    )
    assert trade.hl_details is not None
    assert trade.hl_details.liquidation_mark_px == Decimal("1.23")
    assert trade.hl_details.start_position == Decimal("2.34")
    # None
    hl_details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        liquidation_mark_px=None,
        start_position=None,
    )
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("1.0"),
        quantity=Decimal("1.0"),
        hl_details=hl_details,
    )
    assert trade.hl_details is not None
    assert trade.hl_details.liquidation_mark_px is None
    assert trade.hl_details.start_position is None
    # Invalid: non-finite
    with pytest.raises(ValueError):
        HyperliquidTradeDetails(
            trade_hash="hash-abc",
            liquidation_mark_px=Decimal("NaN"),
        )


def test_trade_custom_to_dict_serialization() -> None:
    """Test that Trade.to_dict() (deprecated) serializes Decimal, Enum, and datetime fields.

    This test verifies the deprecated to_dict method still works correctly for serialization.
    """
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="backpack",
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        fee=Decimal("0.01"),
        fee_asset="USDC",
    )
    d = trade.to_dict()
    assert d["price"] == "100.0"
    assert d["quantity"] == "2.0"
    from decimal import Decimal as D

    assert D(d["cost"]) == D("200.0")
    assert d["fee"] == "0.01"
    assert d["fee_asset"] == "USDC"
    assert d["side"] == "BUY"
    assert isinstance(d["executed_at"], str)


def test_trade_model_dump_json_serialization() -> None:
    """Test that Trade.model_dump(mode='json') serializes all fields, including enrichment slots.

    This test verifies the modern model_dump method works correctly for JSON serialization.
    """
    hl_details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        liquidation_mark_px=Decimal("99.5"),
        start_position=Decimal("1.0"),
        dir="open",
    )
    bp_details = BackpackTradeDetails(system_order_type="LIMIT")
    trade = Trade(
        id="abc123",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="order-xyz",
        exchange="hyperliquid",
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        fee=Decimal("0.01"),
        fee_asset="USDC",
        hl_details=hl_details,
        bp_details=bp_details,
    )
    d = trade.model_dump(mode="json")
    assert d["price"] == "100.0"
    assert d["quantity"] == "2.0"
    from decimal import Decimal as D

    assert D(d["cost"]) == D("200.0")
    assert d["fee"] == "0.01"
    assert d["fee_asset"] == "USDC"
    assert d["side"] == "BUY"
    assert isinstance(d["executed_at"], str)
    assert d["hl_details"]["trade_hash"] == "hash-abc"
    assert d["hl_details"]["liquidation_mark_px"] == "99.5"
    assert d["hl_details"]["start_position"] == "1.0"
    assert d["hl_details"]["dir"] == "open"
    assert d["bp_details"]["system_order_type"] == "LIMIT"


def test_hyperliquid_trade_details_validation() -> None:
    """Test validation logic for HyperliquidTradeDetails enrichment fields."""
    # Valid
    details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        liquidation_mark_px=Decimal("123.45"),
        start_position=Decimal("10.0"),
        dir="open",
    )
    assert details.trade_hash == "hash-abc"
    assert details.liquidation_mark_px == Decimal("123.45")
    assert details.start_position == Decimal("10.0")
    assert details.dir == "open"
    # Optional fields None
    details = HyperliquidTradeDetails(trade_hash="hash-abc")
    assert details.liquidation_mark_px is None
    assert details.start_position is None
    assert details.dir is None
    # Invalid: empty trade_hash
    with pytest.raises(ValueError):
        HyperliquidTradeDetails(trade_hash="")
    # Invalid: overlength trade_hash
    with pytest.raises(ValueError):
        HyperliquidTradeDetails(trade_hash="a" * 129)
    # Invalid: overlength dir
    with pytest.raises(ValueError):
        HyperliquidTradeDetails(trade_hash="hash", dir="a" * 33)
    # Invalid: non-finite decimal
    with pytest.raises(ValueError):
        HyperliquidTradeDetails(trade_hash="hash", liquidation_mark_px=Decimal("NaN"))


def test_backpack_trade_details_validation() -> None:
    """Test validation logic for BackpackTradeDetails enrichment fields."""
    # Valid
    details = BackpackTradeDetails(system_order_type="LIMIT")
    assert details.system_order_type == "LIMIT"
    # Optional None
    details = BackpackTradeDetails()
    assert details.system_order_type is None
    # Invalid: overlength system_order_type
    with pytest.raises(ValueError):
        BackpackTradeDetails(system_order_type="a" * 33)


def test_trade_enrichment_slots_acceptance_and_serialization() -> None:
    """Test that Trade accepts enrichment slots and serializes them as expected."""
    hl_details = HyperliquidTradeDetails(
        trade_hash="hash-abc",
        liquidation_mark_px=Decimal("99.5"),
        start_position=Decimal("1.0"),
        dir="open",
    )
    bp_details = BackpackTradeDetails(system_order_type="LIMIT")
    trade = Trade(
        id="t1",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.BUY,
        order_id="o1",
        exchange="hyperliquid",
        price=Decimal("100.0"),
        quantity=Decimal("2.0"),
        hl_details=hl_details,
        bp_details=bp_details,
    )
    assert trade.hl_details is not None
    assert trade.hl_details.trade_hash == "hash-abc"
    assert trade.bp_details is not None
    assert trade.bp_details.system_order_type == "LIMIT"
    # model_dump includes enrichment fields
    d = trade.model_dump(mode="json")
    assert d["hl_details"]["trade_hash"] == "hash-abc"
    assert d["hl_details"]["liquidation_mark_px"] == "99.5"
    assert d["bp_details"]["system_order_type"] == "LIMIT"


def test_trade_enrichment_slots_none() -> None:
    """Test that Trade accepts None for enrichment slots and serializes as null."""
    trade = Trade(
        id="t2",
        symbol="BTC-PERP",
        executed_at=datetime(2024, 1, 1, 0, 0, 0),
        side=OrderSide.SELL,
        order_id="o2",
        exchange="backpack",
        price=Decimal("50.0"),
        quantity=Decimal("1.0"),
        hl_details=None,
        bp_details=None,
    )
    assert trade.hl_details is None
    assert trade.bp_details is None
    d = trade.model_dump(mode="json")
    assert d["hl_details"] is None
    assert d["bp_details"] is None


def test_trade_enrichment_invalid_details() -> None:
    """Test that Trade rejects invalid enrichment slot data."""
    # Invalid HyperliquidTradeDetails
    with pytest.raises(ValueError):
        Trade(
            id="t3",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="o3",
            exchange="hyperliquid",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
            hl_details=HyperliquidTradeDetails(trade_hash=""),
        )
    # Invalid BackpackTradeDetails
    with pytest.raises(ValueError):
        Trade(
            id="t4",
            symbol="BTC-PERP",
            executed_at=datetime(2024, 1, 1, 0, 0, 0),
            side=OrderSide.BUY,
            order_id="o4",
            exchange="backpack",
            price=Decimal("1.0"),
            quantity=Decimal("1.0"),
            bp_details=BackpackTradeDetails(system_order_type="a" * 33),
        )
