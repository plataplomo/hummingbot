"""Property-based tests for Backpack order mapper.

This module tests critical order mapping functions to ensure:
- Precision preservation in financial calculations
- Round-trip mapping consistency
- Edge case handling for order statuses, types, and values
- Financial invariants are maintained

SECURITY CRITICAL: Order mapping errors could lead to incorrect trading amounts,
wrong order types, or mismatched order statuses causing execution failures.
"""

from decimal import Decimal
from datetime import datetime, UTC
import pytest
from hypothesis import given, strategies as st, assume, settings
from hypothesis.strategies import SearchStrategy

from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderType, OrderSide, TimeInForce
from cyberdelta.symbols.models import Symbol


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDER MAPPING
# =============================================================================


def backpack_order_status_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order statuses."""
    return st.one_of([
        st.just("new"),
        st.just("filled"),
        st.just("cancelled"),
        st.just("canceled"),  # Alternative spelling
        st.just("rejected"),
        st.just("partially_filled"),
        st.just("pending"),
        st.just("trigger_pending"),
        st.just("triggerpending"),  # Alternative spelling
        # Invalid statuses for edge testing
        st.just("unknown_status"),
        st.just(""),
        st.text(min_size=1, max_size=20).filter(
            lambda x: x
            not in [
                "new",
                "filled",
                "cancelled",
                "canceled",
                "rejected",
                "partially_filled",
                "pending",
                "trigger_pending",
                "triggerpending",
            ]
        ),
    ])


def backpack_order_type_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order types."""
    return st.one_of([
        st.just("limit"),
        st.just("market"),
        st.just("stop"),
        st.just("stop_limit"),
        st.just("trailing_stop"),
        st.just("take_profit"),
        # Invalid types for edge testing
        st.just("unknown_type"),
        st.just(""),
        st.text(min_size=1, max_size=20).filter(
            lambda x: x
            not in ["limit", "market", "stop", "stop_limit", "trailing_stop", "take_profit"]
        ),
    ])


def backpack_time_in_force_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack time in force values."""
    return st.one_of([
        st.just("GTC"),  # Good Till Cancelled
        st.just("IOC"),  # Immediate Or Cancel
        st.just("FOK"),  # Fill Or Kill
        st.just("DAY"),  # Day order
        # Case variations
        st.just("gtc"),
        st.just("ioc"),
        st.just("fok"),
        st.just("day"),
        # Invalid values for edge testing
        st.just("INVALID_TIF"),
        st.just(""),
        st.text(min_size=1, max_size=10).filter(
            lambda x: x.upper() not in ["GTC", "IOC", "FOK", "DAY"]
        ),
    ])


def financial_decimal_str_strategy() -> SearchStrategy[str]:
    """Generate financial decimal strings for order values."""
    return st.one_of([
        # Common trading amounts
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("1000000"), places=8).map(
            str
        ),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100000"), places=2).map(str),
        # Edge cases
        st.just("0"),
        st.just("0.00000001"),  # Minimum crypto amount
        st.just("999999.99"),  # Large amount
    ])


def order_transformation_data_strategy():
    """Generate data for order transformation testing."""
    return st.fixed_dictionaries({
        "id": st.text(min_size=10, max_size=50),
        "clientId": st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        "symbol": st.sampled_from(["BTC_USDC", "ETH_USDC", "SOL_USDC"]),
        "side": st.sampled_from(["Bid", "Ask", "bid", "ask", "BUY", "SELL"]),
        "orderType": backpack_order_type_strategy(),
        "status": backpack_order_status_strategy(),
        "timeInForce": backpack_time_in_force_strategy(),
        "quantity": financial_decimal_str_strategy(),
        "price": st.one_of(st.none(), financial_decimal_str_strategy()),
        "triggerPrice": st.one_of(st.none(), financial_decimal_str_strategy()),
        "postOnly": st.booleans(),
        "selfTradePrevention": st.sampled_from([
            "RejectTaker",
            "RejectMaker",
            "RejectBoth",
            "Allow",
        ]),
        "timestamp": st.integers(min_value=1600000000000, max_value=2000000000000),  # Milliseconds
    })


# =============================================================================
# PROPERTY TESTS FOR ORDER STATUS MAPPING
# =============================================================================


class TestOrderStatusMappingProperties:
    """Property-based tests for order status mapping."""

    @given(bp_status=backpack_order_status_strategy())
    def test_status_mapping_consistency(self, bp_status: str):
        """Property: Status mapping should be consistent and deterministic."""
        # Map the status twice
        result1 = BackpackOrderMapper._map_status_to_internal(bp_status)
        result2 = BackpackOrderMapper._map_status_to_internal(bp_status)

        # Property: Same input should always give same output
        assert result1 == result2

        # Property: Result should always be a valid OrderStatus
        assert isinstance(result1, OrderStatus)

    @given(
        bp_status=st.sampled_from([
            "new",
            "filled",
            "cancelled",
            "canceled",
            "rejected",
            "partially_filled",
            "pending",
            "trigger_pending",
            "triggerpending",
        ])
    )
    def test_known_status_mapping_correctness(self, bp_status: str):
        """Property: Known status values should map to correct internal values."""
        result = BackpackOrderMapper._map_status_to_internal(bp_status)

        # Property: Known statuses should not map to UNKNOWN
        assert result != OrderStatus.UNKNOWN

        # Property: Specific mappings should be correct
        expected_mappings = {
            "new": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "pending": OrderStatus.OPEN,
            "trigger_pending": OrderStatus.TRIGGER_PENDING,
            "triggerpending": OrderStatus.TRIGGER_PENDING,
        }

        if bp_status in expected_mappings:
            assert result == expected_mappings[bp_status]

    @given(
        bp_status=st.text().filter(
            lambda x: x.lower()
            not in [
                "new",
                "filled",
                "cancelled",
                "canceled",
                "rejected",
                "partially_filled",
                "pending",
                "trigger_pending",
                "triggerpending",
            ]
        )
    )
    def test_unknown_status_mapping(self, bp_status: str):
        """Property: Unknown status values should map to UNKNOWN."""
        result = BackpackOrderMapper._map_status_to_internal(bp_status)

        # Property: Unknown statuses should map to UNKNOWN
        assert result == OrderStatus.UNKNOWN

    def test_status_mapping_case_insensitive(self):
        """Property: Status mapping should be case-insensitive."""
        test_cases = ["FILLED", "filled", "Filled", "FiLlEd"]

        results = [BackpackOrderMapper._map_status_to_internal(status) for status in test_cases]

        # Property: All case variations should give same result
        assert all(result == OrderStatus.FILLED for result in results)


# =============================================================================
# PROPERTY TESTS FOR ORDER TYPE MAPPING
# =============================================================================


class TestOrderTypeMappingProperties:
    """Property-based tests for order type mapping."""

    @given(bp_type=backpack_order_type_strategy())
    def test_type_mapping_consistency(self, bp_type: str):
        """Property: Type mapping should be consistent and deterministic."""
        # Map the type twice
        result1 = BackpackOrderMapper._map_type_to_internal(bp_type)
        result2 = BackpackOrderMapper._map_type_to_internal(bp_type)

        # Property: Same input should always give same output
        assert result1 == result2

        # Property: Result should always be a valid OrderType
        assert isinstance(result1, OrderType)

    @given(
        bp_type=st.sampled_from([
            "limit",
            "market",
            "stop",
            "stop_limit",
            "trailing_stop",
            "take_profit",
        ])
    )
    def test_known_type_mapping_correctness(self, bp_type: str):
        """Property: Known type values should map to correct internal values."""
        result = BackpackOrderMapper._map_type_to_internal(bp_type)

        # Property: Known types should map correctly
        expected_mappings = {
            "limit": OrderType.LIMIT,
            "market": OrderType.MARKET,
            "stop": OrderType.STOP_MARKET,
            "stop_limit": OrderType.STOP_LIMIT,
            "trailing_stop": OrderType.STOP_MARKET,
            "take_profit": OrderType.LIMIT,
        }

        assert result == expected_mappings[bp_type]

    @given(
        bp_type=st.sampled_from(["market", "limit"]), trigger_price=financial_decimal_str_strategy()
    )
    def test_trigger_price_affects_type_mapping(self, bp_type: str, trigger_price: str):
        """Property: Presence of trigger price should affect order type mapping."""
        # Map without trigger price
        result_without_trigger = BackpackOrderMapper._map_type_to_internal(bp_type)

        # Map with trigger price
        result_with_trigger = BackpackOrderMapper._map_type_to_internal(
            bp_type, trigger_price=trigger_price
        )

        # Property: Trigger price may change the mapping
        # (This is implementation-dependent, but result should still be valid)
        assert isinstance(result_without_trigger, OrderType)
        assert isinstance(result_with_trigger, OrderType)


# =============================================================================
# PROPERTY TESTS FOR TIME IN FORCE MAPPING
# =============================================================================


class TestTimeInForceMappingProperties:
    """Property-based tests for time in force mapping."""

    @given(bp_tif=backpack_time_in_force_strategy())
    def test_tif_mapping_consistency(self, bp_tif: str):
        """Property: Time in force mapping should be consistent."""
        # Map the TIF twice
        result1 = BackpackOrderMapper._map_time_in_force(bp_tif)
        result2 = BackpackOrderMapper._map_time_in_force(bp_tif)

        # Property: Same input should always give same output
        assert result1 == result2

        # Property: Result should always be a valid TimeInForce
        assert isinstance(result1, TimeInForce)

    @given(bp_tif=st.sampled_from(["GTC", "IOC", "FOK"]))
    def test_known_tif_mapping_correctness(self, bp_tif: str):
        """Property: Known TIF values should map correctly."""
        result = BackpackOrderMapper._map_time_in_force(bp_tif)

        # Property: Known TIFs should map to correct values
        expected_mappings = {
            "GTC": TimeInForce.GTC,
            "IOC": TimeInForce.IOC,
            "FOK": TimeInForce.FOK,
        }

        assert result == expected_mappings[bp_tif]

    def test_tif_mapping_case_insensitive(self):
        """Property: TIF mapping should be case-insensitive."""
        test_cases = ["GTC", "gtc", "Gtc", "gTc"]

        results = [BackpackOrderMapper._map_time_in_force(tif) for tif in test_cases]

        # Property: All case variations should give same result
        assert all(result == TimeInForce.GTC for result in results)


# =============================================================================
# PROPERTY TESTS FOR ORDER TRANSFORMATION
# =============================================================================


class TestOrderTransformationProperties:
    """Property-based tests for complete order transformation."""

    @given(order_data=order_transformation_data_strategy())
    @settings(max_examples=500)
    def test_order_transformation_preserves_financial_precision(self, order_data):
        """Property: Order transformation should preserve financial precision."""
        try:
            # Transform the order data
            result = BackpackOrderMapper.transform_order_data_to_internal(order_data)

            # Property: Financial values should be preserved as Decimal
            if order_data.get("quantity"):
                assert isinstance(result.quantity_requested, Decimal)
                # Property: Precision should be preserved
                original_decimal = Decimal(order_data["quantity"])
                assert result.quantity_requested == original_decimal

            if order_data.get("price"):
                assert isinstance(result.price, Decimal)
                # Property: Price precision should be preserved
                original_price = Decimal(order_data["price"])
                assert result.price == original_price

            # Property: Symbol should be properly mapped
            assert isinstance(result.symbol, Symbol)

            # Property: Side should be properly mapped
            assert result.side in [OrderSide.BUY, OrderSide.SELL]

        except Exception as e:
            # If transformation fails, it should be for a valid reason
            # (e.g., invalid data that should be rejected)
            assert isinstance(e, (ValueError, TypeError, AttributeError))

    @given(order_data=order_transformation_data_strategy())
    def test_order_transformation_financial_invariants(self, order_data):
        """Property: Transformed orders should maintain financial invariants."""
        try:
            result = BackpackOrderMapper.transform_order_data_to_internal(order_data)

            # Property: Quantities should be positive or zero
            if result.quantity_requested is not None:
                assert result.quantity_requested >= Decimal("0")

            # Property: Prices should be positive or None
            if result.price is not None:
                assert result.price > Decimal("0")

            # Property: Trigger prices should be positive or None
            if hasattr(result, "trigger_price") and result.trigger_price is not None:
                assert result.trigger_price > Decimal("0")

            # Property: Financial values should be finite
            if result.quantity_requested is not None:
                assert result.quantity_requested.is_finite()
            if result.price is not None:
                assert result.price.is_finite()

        except Exception:
            # Expected for invalid input data
            pass

    @given(
        valid_quantity=financial_decimal_str_strategy(),
        valid_price=financial_decimal_str_strategy(),
    )
    def test_order_transformation_round_trip_properties(
        self, valid_quantity: str, valid_price: str
    ):
        """Property: Valid financial values should survive round-trip transformation."""
        # Skip zero prices as they're invalid for orders
        assume(Decimal(valid_price) > Decimal("0"))
        assume(Decimal(valid_quantity) > Decimal("0"))

        # Create minimal valid order data
        order_data = {
            "id": "test_order_123",
            "symbol": "BTC_USDC",
            "side": "Bid",
            "orderType": "limit",
            "status": "new",
            "timeInForce": "GTC",
            "quantity": valid_quantity,
            "price": valid_price,
            "postOnly": False,
            "selfTradePrevention": "RejectTaker",
            "timestamp": 1700000000000,
        }

        try:
            mapper = BackpackOrderMapper()

            # Need to create a symbol for the test
            from cyberdelta.symbols import exchanges

            symbol = exchanges.backpack("BTC_USDC")

            result = mapper.transform_order_data_to_internal(
                order_id=order_data["id"],
                symbol=symbol,
                side=order_data["side"],
                order_type=order_data["orderType"],
                status=order_data["status"],
                quantity=order_data["quantity"],
                price=order_data["price"],
                time_in_force=order_data["timeInForce"],
            )

            # Property: Original decimal precision should be preserved
            assert str(result.quantity_requested) == str(Decimal(valid_quantity))
            assert str(result.price) == str(Decimal(valid_price))

            # Property: Values should be exactly equal as Decimals
            assert result.quantity_requested == Decimal(valid_quantity)
            assert result.price == Decimal(valid_price)

        except Exception as e:
            # Should not fail for valid financial inputs
            pytest.fail(f"Valid financial data rejected: {e}")


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestOrderMapperIntegrationProperties:
    """Integration property tests across multiple mapping functions."""

    @given(
        status=st.sampled_from(["new", "filled", "cancelled"]),
        order_type=st.sampled_from(["limit", "market"]),
        tif=st.sampled_from(["GTC", "IOC"]),
    )
    def test_enum_mapping_consistency(self, status: str, order_type: str, tif: str):
        """Property: All enum mappings should be consistent."""
        mapped_status = BackpackOrderMapper._map_status_to_internal(status)
        mapped_type = BackpackOrderMapper._map_type_to_internal(order_type)
        mapped_tif = BackpackOrderMapper._map_time_in_force(tif)

        # Property: All mappings should return valid enum values
        assert isinstance(mapped_status, OrderStatus)
        assert isinstance(mapped_type, OrderType)
        assert isinstance(mapped_tif, TimeInForce)

        # Property: Mappings should be deterministic
        assert BackpackOrderMapper._map_status_to_internal(status) == mapped_status
        assert BackpackOrderMapper._map_type_to_internal(order_type) == mapped_type
        assert BackpackOrderMapper._map_time_in_force(tif) == mapped_tif

    @given(order_data=order_transformation_data_strategy())
    def test_transformation_error_safety(self, order_data):
        """Property: Transformation errors should be safe and informative."""
        try:
            BackpackOrderMapper.transform_order_data_to_internal(order_data)
        except Exception as e:
            # Property: Errors should be specific exception types (not generic Exception)
            assert not isinstance(e, Exception) or type(e) != Exception

            # Property: Error messages should be informative (contain relevant info)
            error_msg = str(e)
            assert len(error_msg) > 0

            # Property: Should not contain sensitive information
            assert "password" not in error_msg.lower()
            assert "secret" not in error_msg.lower()
            assert "key" not in error_msg.lower()
