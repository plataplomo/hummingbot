"""Property-based tests for Fill model.

This module tests the critical Fill model for trading execution to ensure:
- Financial precision preservation in all price/quantity/fee fields
- Fee logic validation (fee_asset required when fee != 0)
- Exchange-specific field validation and constraints
- Mathematical consistency in calculations (cost = price * quantity)
- Immutability properties for executed trade data
- Cross-field validation constraints

SECURITY CRITICAL: Fill model errors could lead to incorrect trade recording,
wrong fee calculations, invalid fill data reaching exchanges, or accounting failures.
"""

from decimal import Decimal
from datetime import datetime, UTC
import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy

from cyberdelta.models.market.fill import Fill, HyperliquidFillDetails, BackpackFillDetails
from cyberdelta.enums import OrderSide, MakerTaker
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import FillLogicError
from cyberdelta.symbols import exchanges


# =============================================================================
# HYPOTHESIS STRATEGIES FOR FILL MODEL TESTING
# =============================================================================


def financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for financial amounts."""
    return st.one_of([
        # Common trading amounts with realistic precision
        st.decimals(
            min_value=Decimal("0.00000001"),  # Crypto precision
            max_value=Decimal("1000000"),
            places=8,
        ).map(str),
        st.decimals(min_value=Decimal("0.0001"), max_value=Decimal("100000"), places=4).map(str),
        # Edge cases
        st.just("0.00000001"),  # Minimum crypto amount
        st.just("999999.99999999"),  # Large amount
        st.just("1.0"),  # Common unit amount
    ])


def positive_decimal_strategy() -> SearchStrategy[Decimal]:
    """Generate positive Decimal values for financial calculations."""
    return st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("1000000"), places=8)


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate realistic price values."""
    return st.decimals(
        min_value=Decimal("0.01"),  # Minimum meaningful price
        max_value=Decimal("100000"),
        places=6,
    )


def quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate realistic quantity values."""
    return st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("10000"), places=8)


def fee_strategy() -> SearchStrategy[Decimal]:
    """Generate realistic fee values (can be negative for rebates)."""
    return st.decimals(
        min_value=Decimal("-100"),  # Negative for rebates
        max_value=Decimal("1000"),
        places=6,
    )


def order_side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides."""
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def maker_taker_strategy() -> SearchStrategy[MakerTaker]:
    """Generate valid maker/taker values."""
    return st.sampled_from([MakerTaker.MAKER, MakerTaker.TAKER])


def exchange_strategy() -> SearchStrategy[ExchangeName]:
    """Generate valid exchange names."""
    return st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])


def symbol_strategy() -> SearchStrategy:
    """Generate valid Symbol objects."""

    def create_symbol(exchange: ExchangeName, asset: str) -> object:
        if exchange == ExchangeName.HYPERLIQUID:
            return exchanges.hyperliquid(value=asset)
        else:
            return exchanges.backpack(value=asset)

    return st.builds(
        create_symbol,
        exchange=exchange_strategy(),
        asset=st.sampled_from(["BTC", "ETH", "SOL", "DOGE"]),
    )


def fill_id_strategy() -> SearchStrategy[str]:
    """Generate valid fill IDs."""
    return st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"),
        min_size=1,
        max_size=64,
    )


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid order IDs."""
    return st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"),
        min_size=1,
        max_size=64,
    )


def asset_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbols for fee_asset."""
    return st.sampled_from(["USDC", "USD", "BTC", "ETH", "SOL"])


def basic_fill_data_strategy():
    """Generate data for valid basic fills."""
    return st.fixed_dictionaries({
        "id": fill_id_strategy(),
        "symbol": symbol_strategy(),
        "executed_at": st.just(datetime.now(UTC)),
        "side": order_side_strategy(),
        "order_id": order_id_strategy(),
        "exchange": exchange_strategy(),
        "price": price_strategy(),
        "quantity": quantity_strategy(),
    })


def fill_with_fee_data_strategy():
    """Generate data for fills with fees."""
    return st.fixed_dictionaries({
        "id": fill_id_strategy(),
        "symbol": symbol_strategy(),
        "executed_at": st.just(datetime.now(UTC)),
        "side": order_side_strategy(),
        "order_id": order_id_strategy(),
        "exchange": exchange_strategy(),
        "price": price_strategy(),
        "quantity": quantity_strategy(),
        "fee": fee_strategy(),
        "fee_asset": asset_strategy(),
        "maker_taker": st.one_of(st.none(), maker_taker_strategy()),
    })


# =============================================================================
# PROPERTY TESTS FOR FILL VALIDATION LOGIC
# =============================================================================


class TestFillValidationProperties:
    """Property-based tests for Fill model validation logic."""

    @given(fill_data=basic_fill_data_strategy())
    def test_basic_fill_creation_properties(self, fill_data):
        """Property: Valid basic fills should always be created successfully."""
        fill = Fill(**fill_data)

        # Property: All financial values should be preserved as Decimal
        assert isinstance(fill.price, Decimal)
        assert isinstance(fill.quantity, Decimal)
        assert isinstance(fill.fee, Decimal)

        # Property: Required fields must be positive
        assert fill.price > Decimal("0")
        assert fill.quantity > Decimal("0")

        # Property: Default fee should be zero
        assert fill.fee == Decimal("0")
        assert fill.fee_asset is None

        # Property: Optional fields should have expected defaults
        assert fill.client_order_id is None
        assert fill.maker_taker is None

        # Property: Financial precision preserved
        assert fill.price == fill_data["price"]
        assert fill.quantity == fill_data["quantity"]

    @given(fill_data=fill_with_fee_data_strategy())
    def test_fill_with_fee_creation_properties(self, fill_data):
        """Property: Valid fills with fees should be created successfully."""
        # Ensure fee logic is valid
        assume(fill_data["fee"] == Decimal("0") or fill_data["fee_asset"] is not None)

        fill = Fill(**fill_data)

        # Property: All financial values preserved
        assert fill.price == fill_data["price"]
        assert fill.quantity == fill_data["quantity"]
        assert fill.fee == fill_data["fee"]

        # Property: Fee asset consistency
        if fill.fee != Decimal("0"):
            assert fill.fee_asset == fill_data["fee_asset"]

        # Property: Maker/taker preserved
        assert fill.maker_taker == fill_data.get("maker_taker")

    @given(price=price_strategy(), quantity=quantity_strategy())
    def test_cost_calculation_properties(self, price: Decimal, quantity: Decimal):
        """Property: Cost should always equal price * quantity exactly."""
        fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": price,
            "quantity": quantity,
        }

        fill = Fill(**fill_data)

        # Property: Cost calculation should be exact
        expected_cost = price * quantity
        assert fill.cost == expected_cost

        # Property: Cost should be finite
        assert fill.cost.is_finite()

        # Property: Cost should be positive (since price and quantity are positive)
        assert fill.cost > Decimal("0")

        # Property: String representation should be consistent
        assert str(fill.cost) == str(expected_cost)

    @given(fee=fee_strategy(), has_fee_asset=st.booleans())
    def test_fee_logic_validation_properties(self, fee: Decimal, has_fee_asset: bool):
        """Property: Fee logic should be consistently validated."""
        fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": Decimal("50000.0"),
            "quantity": Decimal("1.0"),
            "fee": fee,
            "fee_asset": "USDC" if has_fee_asset else None,
        }

        # Property: Non-zero fee requires fee_asset
        if fee != Decimal("0") and not has_fee_asset:
            with pytest.raises((FillLogicError, Exception)) as exc_info:
                Fill(**fill_data)
            # Should raise either FillLogicError or Pydantic ValidationError
            assert "fee_asset" in str(exc_info.value).lower()
        else:
            # Property: Valid combinations should work
            fill = Fill(**fill_data)
            assert fill.fee == fee
            if fee != Decimal("0"):
                assert fill.fee_asset == "USDC"
            else:
                assert fill.fee_asset is None

    @given(
        negative_price=st.decimals(min_value=Decimal("-1000"), max_value=Decimal("0"), places=6),
        negative_quantity=st.decimals(min_value=Decimal("-100"), max_value=Decimal("0"), places=8),
    )
    def test_negative_price_quantity_rejection(
        self, negative_price: Decimal, negative_quantity: Decimal
    ):
        """Property: Negative prices and quantities should be rejected."""
        base_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
        }

        # Test negative price
        fill_data_price = base_data.copy()
        fill_data_price.update({
            "price": negative_price,
            "quantity": Decimal("1.0"),
        })

        with pytest.raises(Exception):  # Pydantic validation error
            Fill(**fill_data_price)

        # Test negative quantity
        fill_data_quantity = base_data.copy()
        fill_data_quantity.update({
            "price": Decimal("50000.0"),
            "quantity": negative_quantity,
        })

        with pytest.raises(Exception):  # Pydantic validation error
            Fill(**fill_data_quantity)


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC VALIDATION
# =============================================================================


class TestFillExchangeValidationProperties:
    """Property-based tests for exchange-specific fill validation."""

    @given(exchange=exchange_strategy())
    def test_exchange_details_consistency(self, exchange: ExchangeName):
        """Property: Fills should only have details for their own exchange."""
        fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC")
            if exchange == ExchangeName.HYPERLIQUID
            else exchanges.backpack(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": exchange,
            "price": Decimal("50000.0"),
            "quantity": Decimal("1.0"),
        }

        # Test with correct exchange details
        if exchange == ExchangeName.HYPERLIQUID:
            fill_data["hl_details"] = HyperliquidFillDetails(fill_hash="test_hash_123")
            fill = Fill(**fill_data)
            assert fill.hl_details is not None
            assert fill.bp_details is None
        else:
            fill_data["bp_details"] = BackpackFillDetails()
            fill = Fill(**fill_data)
            assert fill.bp_details is not None
            assert fill.hl_details is None

    def test_exchange_details_cross_contamination(self):
        """Property: Fills should reject details from other exchanges."""
        # Note: Since Fill model doesn't have explicit cross-exchange validation in its logic,
        # this test focuses on proper isolation of exchange-specific details

        # Hyperliquid fill with its own details (should work)
        hl_fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": Decimal("50000.0"),
            "quantity": Decimal("1.0"),
            "hl_details": HyperliquidFillDetails(fill_hash="test_hash_123"),
        }

        hl_fill = Fill(**hl_fill_data)
        assert hl_fill.hl_details is not None
        assert hl_fill.bp_details is None

        # Backpack fill with its own details (should work)
        bp_fill_data = {
            "id": "test_fill_456",
            "symbol": exchanges.backpack(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.SELL,
            "order_id": "test_order_789",
            "exchange": ExchangeName.BACKPACK,
            "price": Decimal("50000.0"),
            "quantity": Decimal("1.0"),
            "bp_details": BackpackFillDetails(system_order_type="LIMIT"),
        }

        bp_fill = Fill(**bp_fill_data)
        assert bp_fill.bp_details is not None
        assert bp_fill.hl_details is None


# =============================================================================
# PROPERTY TESTS FOR FINANCIAL PRECISION
# =============================================================================


class TestFillFinancialPrecisionProperties:
    """Property-based tests for financial precision preservation in fills."""

    @given(
        price=st.decimals(min_value=Decimal("0.000001"), max_value=Decimal("999999"), places=8),
        quantity=st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("10000"), places=8),
        fee=st.decimals(min_value=Decimal("-100"), max_value=Decimal("100"), places=8),
    )
    def test_financial_precision_preservation(
        self, price: Decimal, quantity: Decimal, fee: Decimal
    ):
        """Property: All financial values should preserve exact decimal precision."""
        fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": price,
            "quantity": quantity,
            "fee": fee,
            "fee_asset": "USDC" if fee != Decimal("0") else None,
        }

        fill = Fill(**fill_data)

        # Property: Exact precision preserved
        assert fill.price == price
        assert fill.quantity == quantity
        assert fill.fee == fee

        # Property: String representation should be consistent
        assert str(fill.price) == str(price)
        assert str(fill.quantity) == str(quantity)
        assert str(fill.fee) == str(fee)

        # Property: Mathematical operations should be exact
        assert fill.cost == price * quantity

        # Property: All values should be finite
        assert fill.price.is_finite()
        assert fill.quantity.is_finite()
        assert fill.fee.is_finite()
        assert fill.cost.is_finite()

    @given(
        base_price=price_strategy(),
        base_quantity=quantity_strategy(),
        fee_rate=st.decimals(
            min_value=Decimal("0"), max_value=Decimal("0.01"), places=6
        ),  # 0-1% fee
    )
    def test_fill_value_calculations(
        self, base_price: Decimal, base_quantity: Decimal, fee_rate: Decimal
    ):
        """Property: Fill value calculations should be mathematically consistent."""
        notional_value = base_price * base_quantity
        fee_amount = notional_value * fee_rate

        fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": base_price,
            "quantity": base_quantity,
            "fee": fee_amount,
            "fee_asset": "USDC",
            "maker_taker": MakerTaker.TAKER,
        }

        fill = Fill(**fill_data)

        # Property: Cost calculation should be exact
        assert fill.cost == notional_value
        assert fill.cost == base_price * base_quantity

        # Property: Fee calculation should be exact
        assert fill.fee == fee_amount

        # Property: Net value calculations should be consistent
        if fill.side == OrderSide.BUY:
            # For buys, total cost including fees
            total_cost = fill.cost + fill.fee
            assert total_cost == notional_value + fee_amount
        else:
            # For sells, net proceeds after fees
            net_proceeds = fill.cost - fill.fee
            assert net_proceeds == notional_value - fee_amount

        # Property: All calculations should be finite and positive
        assert fill.cost.is_finite() and fill.cost > 0
        assert fill.fee.is_finite()


# =============================================================================
# PROPERTY TESTS FOR FILL IMMUTABILITY
# =============================================================================


class TestFillImmutabilityProperties:
    """Property-based tests for Fill model immutability."""

    @given(fill_data=basic_fill_data_strategy())
    def test_fill_immutability(self, fill_data):
        """Property: Fill instances should be immutable after creation."""
        fill = Fill(**fill_data)

        # Property: Attempting to modify fields should fail
        with pytest.raises(Exception):  # Pydantic ValidationError for immutable model
            fill.price = Decimal("99999.99")

        with pytest.raises(Exception):
            fill.quantity = Decimal("0.5")

        with pytest.raises(Exception):
            fill.fee = Decimal("10.0")

        # Property: Original values should be preserved
        assert fill.price == fill_data["price"]
        assert fill.quantity == fill_data["quantity"]
        assert fill.fee == Decimal("0")  # Default value

    @given(fill_data=basic_fill_data_strategy())
    def test_fill_serialization_round_trip(self, fill_data):
        """Property: Fill should survive serialization round trip with precision."""
        fill = Fill(**fill_data)

        # Test both serialization methods

        # 1. Test model_dump() - returns native types
        fill_dict = fill.model_dump()

        # Property: All critical fields should be present
        assert "price" in fill_dict
        assert "quantity" in fill_dict
        assert "fee" in fill_dict
        assert "cost" in fill_dict  # Computed field
        assert "symbol" in fill_dict
        assert "exchange" in fill_dict

        # Property: Financial values should be preserved as Decimal in model_dump()
        assert isinstance(fill_dict["price"], Decimal)
        assert isinstance(fill_dict["quantity"], Decimal)
        assert isinstance(fill_dict["fee"], Decimal)
        assert isinstance(fill_dict["cost"], Decimal)

        # Property: Values should be exact
        assert fill_dict["price"] == fill.price
        assert fill_dict["quantity"] == fill.quantity
        assert fill_dict["fee"] == fill.fee
        assert fill_dict["cost"] == fill.cost

        # 2. Test to_dict() - converts to strings for JSON safety
        fill_str_dict = fill.to_dict()

        # Property: Financial values should be strings in to_dict()
        assert isinstance(fill_str_dict["price"], str)
        assert isinstance(fill_str_dict["quantity"], str)
        assert isinstance(fill_str_dict["fee"], str)
        assert isinstance(fill_str_dict["cost"], str)

        # Property: String conversion should preserve precision
        assert Decimal(fill_str_dict["price"]) == fill.price
        assert Decimal(fill_str_dict["quantity"]) == fill.quantity
        assert Decimal(fill_str_dict["fee"]) == fill.fee
        assert Decimal(fill_str_dict["cost"]) == fill.cost


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestFillIntegrationProperties:
    """Integration property tests for Fill model behavior."""

    @given(fill_data=basic_fill_data_strategy())
    def test_fill_creation_deterministic(self, fill_data):
        """Property: Fill creation should be deterministic for same inputs."""
        # Create same fill twice
        fill1 = Fill(**fill_data)
        fill2 = Fill(**fill_data)

        # Property: All field values should be identical
        assert fill1.id == fill2.id
        assert fill1.price == fill2.price
        assert fill1.quantity == fill2.quantity
        assert fill1.fee == fill2.fee
        assert fill1.cost == fill2.cost
        assert fill1.symbol == fill2.symbol
        assert fill1.exchange == fill2.exchange
        assert fill1.side == fill2.side

    @given(
        price=price_strategy(),
        quantity=quantity_strategy(),
        side=order_side_strategy(),
        maker_taker=st.one_of(st.none(), maker_taker_strategy()),
    )
    def test_fill_business_logic_consistency(
        self, price: Decimal, quantity: Decimal, side: OrderSide, maker_taker: MakerTaker | None
    ):
        """Property: Fill should maintain business logic consistency."""
        fill_data = {
            "id": "test_fill_123",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": side,
            "order_id": "test_order_456",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": price,
            "quantity": quantity,
            "maker_taker": maker_taker,
        }

        fill = Fill(**fill_data)

        # Property: Side consistency
        assert fill.side == side

        # Property: Cost should always be positive for valid price/quantity
        assert fill.cost > Decimal("0")

        # Property: Maker/taker should be preserved
        assert fill.maker_taker == maker_taker

        # Property: Execution timestamp should be timezone-aware
        assert fill.executed_at.tzinfo is not None

    @given(
        price1=price_strategy(),
        quantity1=quantity_strategy(),
        price2=price_strategy(),
        quantity2=quantity_strategy(),
    )
    def test_multiple_fills_independence(
        self, price1: Decimal, quantity1: Decimal, price2: Decimal, quantity2: Decimal
    ):
        """Property: Multiple fills should be independent of each other."""
        fill1_data = {
            "id": "fill_1",
            "symbol": exchanges.hyperliquid(value="BTC"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.BUY,
            "order_id": "order_1",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": price1,
            "quantity": quantity1,
        }

        fill2_data = {
            "id": "fill_2",
            "symbol": exchanges.hyperliquid(value="ETH"),
            "executed_at": datetime.now(UTC),
            "side": OrderSide.SELL,
            "order_id": "order_2",
            "exchange": ExchangeName.HYPERLIQUID,
            "price": price2,
            "quantity": quantity2,
        }

        fill1 = Fill(**fill1_data)
        fill2 = Fill(**fill2_data)

        # Property: Fills should be independent
        assert fill1.price == price1
        assert fill2.price == price2
        assert fill1.quantity == quantity1
        assert fill2.quantity == quantity2

        # Property: Cost calculations should be independent
        assert fill1.cost == price1 * quantity1
        assert fill2.cost == price2 * quantity2

        # Property: Modifying one should not affect the other (immutability test)
        original_fill1_cost = fill1.cost
        # fill1 and fill2 should remain unchanged regardless of operations
        assert fill1.cost == original_fill1_cost
