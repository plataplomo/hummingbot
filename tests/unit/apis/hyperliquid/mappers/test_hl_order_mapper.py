"""Property-based tests for Hyperliquid order mapper.

This module tests critical order mapping functions for Hyperliquid exchange to ensure:
- Precision preservation in financial calculations
- Round-trip mapping consistency
- Edge case handling for order statuses, types, and values
- Financial invariants are maintained

SECURITY CRITICAL: Order mapping errors could lead to incorrect trading amounts,
wrong order types, or mismatched order statuses causing execution failures.
"""

from decimal import Decimal
from typing import cast

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.apis.exceptions.data_transformation import UnknownEnumError
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.mappers.trading.hl_trading_enum_mapper import (
    HyperliquidTradingEnumMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


# =============================================================================
# HYPOTHESIS STRATEGIES FOR HYPERLIQUID ORDER MAPPING
# =============================================================================


def hyperliquid_order_status_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid order statuses.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        st.just("open"),
        st.just("filled"),
        st.just("canceled"),
        st.just("rejected"),
        st.just("partiallyFilled"),
        st.just("triggered"),
        st.just("pending"),
        # Case variations
        st.just("OPEN"),
        st.just("FILLED"),
        st.just("CANCELED"),
        # Invalid statuses for edge testing
        st.just("unknown_status"),
        st.just(""),
        st.text(min_size=1, max_size=20).filter(
            lambda x: x.lower()
            not in [
                "open",
                "filled",
                "canceled",
                "rejected",
                "partiallyfilled",
                "triggered",
                "pending",
            ]
        ),
    ])


def hyperliquid_order_type_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid order types.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        st.just("Market"),
        st.just("Limit"),
        st.just("Stop"),
        st.just("StopLimit"),
        st.just("TakeProfit"),
        st.just("TakeProfitLimit"),
        # Case variations
        st.just("market"),
        st.just("limit"),
        st.just("stop"),
        # Invalid types for edge testing
        st.just("unknown_type"),
        st.just(""),
        st.text(min_size=1, max_size=20).filter(
            lambda x: x.lower()
            not in ["market", "limit", "stop", "stoplimit", "takeprofit", "takeprofitlimit"]
        ),
    ])


def hyperliquid_side_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid order sides.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        st.just("B"),  # Buy
        st.just("A"),  # Ask/Sell
        # Invalid sides for testing error handling
        st.just("X"),
        st.just(""),
        st.text(min_size=1, max_size=10),
    ])


def hyperliquid_financial_value_strategy() -> SearchStrategy[str]:
    """Generate financial values typical for Hyperliquid.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        # Common trading amounts for crypto
        st.decimals(min_value=Decimal("0.0001"), max_value=1000000, places=4).map(str),
        st.decimals(min_value=Decimal("0.000001"), max_value=100000, places=6).map(str),
        # Edge cases
        st.just("0"),
        st.just("0.0001"),  # Minimum meaningful amount
        st.just("999999.9999"),  # Large amount
    ])


def hyperliquid_order_data_strategy() -> SearchStrategy[dict[str, object]]:
    """Generate Hyperliquid order data for transformation testing.

    Returns:
        SearchStrategy for dict[str, object] order data.
    """
    return st.fixed_dictionaries({
        "oid": st.integers(min_value=1, max_value=2**63 - 1),  # Order ID
        "user": st.text(min_size=10, max_size=42),  # Ethereum address format
        "coin": st.sampled_from(["BTC", "ETH", "SOL", "DOGE"]),  # Asset
        "side": hyperliquid_side_strategy(),
        "sz": hyperliquid_financial_value_strategy(),  # Size
        "px": st.one_of(
            st.none(), hyperliquid_financial_value_strategy()
        ),  # Price (None for market orders)
        "orderType": hyperliquid_order_type_strategy(),
        "timestamp": st.integers(min_value=1600000000000, max_value=2000000000000),  # Milliseconds
        "triggerCondition": st.one_of(st.none(), st.sampled_from(["above", "below"])),
        "triggerPx": st.one_of(st.none(), hyperliquid_financial_value_strategy()),
        "reduceOnly": st.booleans(),
        "tif": st.sampled_from(["Gtc", "Ioc", "Alo"]),  # Time in force
    })


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID ORDER STATUS MAPPING
# =============================================================================


class TestHyperliquidOrderStatusMappingProperties:
    """Property-based tests for Hyperliquid order status mapping."""

    @given(hl_status=hyperliquid_order_status_strategy())
    def test_status_mapping_consistency(self, hl_status: str) -> None:
        """Property: Status mapping should be consistent and deterministic."""
        # Map the status twice
        result1 = HyperliquidTradingEnumMapper.map_status_to_internal(hl_status)
        result2 = HyperliquidTradingEnumMapper.map_status_to_internal(hl_status)

        # Property: Same input should always give same output
        assert result1 == result2

        # Property: Result should always be a valid OrderStatus
        assert isinstance(result1, OrderStatus)

    @given(hl_status=st.sampled_from(["open", "filled", "canceled", "rejected"]))
    def test_known_status_mapping_correctness(self, hl_status: str) -> None:
        """Property: Known status values should map to correct internal values."""
        result = HyperliquidTradingEnumMapper.map_status_to_internal(hl_status)

        # Property: Known statuses should not map to UNKNOWN
        assert result != OrderStatus.UNKNOWN

        # Property: Specific mappings should be correct
        expected_mappings = {
            "open": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
        }

        if hl_status.lower() in expected_mappings:
            assert result == expected_mappings[hl_status.lower()]

    @given(
        hl_status=st.text().filter(
            lambda x: x.lower() not in ["open", "filled", "canceled", "rejected"]
        )
    )
    def test_unknown_status_mapping(self, hl_status: str) -> None:
        """Property: Unknown status values should map to UNKNOWN."""
        result = HyperliquidTradingEnumMapper.map_status_to_internal(hl_status)

        # Property: Unknown statuses should map to UNKNOWN
        assert result == OrderStatus.UNKNOWN


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID ORDER TYPE MAPPING
# =============================================================================


class TestHyperliquidOrderTypeMappingProperties:
    """Property-based tests for Hyperliquid order type mapping."""

    @given(hl_type=hyperliquid_order_type_strategy())
    def test_type_mapping_consistency(self, hl_type: str) -> None:
        """Property: Type mapping should be consistent and deterministic."""
        # Hyperliquid order types need to be in dict format
        order_type_dict: dict[str, dict[str, object]] = {hl_type.lower(): {}}

        # Map the type twice
        result1 = HyperliquidTradingEnumMapper.map_type_to_internal(order_type_dict, None)
        result2 = HyperliquidTradingEnumMapper.map_type_to_internal(order_type_dict, None)

        # Property: Same input should always give same output
        assert result1 == result2

        # Property: Result should always be a valid OrderType
        assert isinstance(result1, OrderType)

    @given(hl_type=st.sampled_from(["Market", "Limit"]))
    def test_known_type_mapping_correctness(self, hl_type: str) -> None:
        """Property: Known type values should map to correct internal values."""
        # Convert to dict format expected by Hyperliquid mapper
        order_type_dict: dict[str, dict[str, object]] = {hl_type.lower(): {}}
        result = HyperliquidTradingEnumMapper.map_type_to_internal(order_type_dict, None)

        # Property: Known types should map correctly
        expected_mappings = {
            "market": OrderType.MARKET,
            "limit": OrderType.LIMIT,
        }

        if hl_type.lower() in expected_mappings:
            assert result == expected_mappings[hl_type.lower()]


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID SIDE MAPPING
# =============================================================================


class TestHyperliquidSideMappingProperties:
    """Property-based tests for Hyperliquid side mapping."""

    @given(hl_side=hyperliquid_side_strategy())
    def test_side_mapping_consistency(self, hl_side: str) -> None:
        """Property: Side mapping should be consistent and deterministic."""
        try:
            # Map the side twice
            result1 = HyperliquidTradingEnumMapper.map_side_to_internal(hl_side)
            result2 = HyperliquidTradingEnumMapper.map_side_to_internal(hl_side)

            # Property: Same input should always give same output
            assert result1 == result2

            # Property: Result should always be a valid OrderSide
            assert isinstance(result1, OrderSide)

        except UnknownEnumError:
            # Property: Invalid sides should consistently raise UnknownEnumError
            # This is expected for invalid side values
            pass

    def test_known_side_mapping_correctness(self) -> None:
        """Property: Known side values should map correctly."""
        # Test standard Hyperliquid format
        assert HyperliquidTradingEnumMapper.map_side_to_internal("B") == OrderSide.BUY
        assert HyperliquidTradingEnumMapper.map_side_to_internal("A") == OrderSide.SELL

        # Test alternative formats if supported
        try:
            buy_result = HyperliquidTradingEnumMapper.map_side_to_internal("buy")
            assert buy_result == OrderSide.BUY
        except (ValueError, TypeError, KeyError):
            pass  # May not support alternative formats

    @given(hl_side=st.text().filter(lambda x: x not in ["B", "A", "buy", "sell", "BUY", "SELL"]))
    def test_invalid_side_handling(self, hl_side: str) -> None:
        """Property: Invalid side values should be handled consistently."""
        try:
            result = HyperliquidTradingEnumMapper.map_side_to_internal(hl_side)
            # If it doesn't raise, result should be a valid OrderSide
            assert isinstance(result, OrderSide)
        except (ValueError, TypeError, KeyError):
            # Invalid sides should raise exceptions
            pass


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID ORDER TRANSFORMATION
# =============================================================================


class TestHyperliquidOrderTransformationProperties:
    """Property-based tests for complete Hyperliquid order transformation."""

    @given(order_data=hyperliquid_order_data_strategy())
    @settings(max_examples=300)
    def test_order_transformation_preserves_financial_precision(
        self, order_data: dict[str, object]
    ) -> None:
        """Property: Order transformation should preserve financial precision."""
        # Skip obviously invalid data
        assume(order_data["sz"] and Decimal(str(order_data["sz"])) > 0)
        assume(order_data["side"] in ["B", "A"])

        try:
            # Create mock raw order data for testing

            # Skip this test if we can't create valid mock data
            if not order_data.get("sz") or not order_data.get("side"):
                return

            # Create a raw order mock for transformation
            raw_order = HyperliquidRawOrder(
                oid=int(cast(int, order_data["oid"])),
                asset=str(order_data["coin"]),
                side=str(order_data["side"]),
                sz=str(order_data["sz"]),
                limitPx=str(order_data.get("px", "0")),
                remainingSz=str(order_data["sz"]),  # No fills initially
                timestamp=int(cast(int, order_data["timestamp"])),
                orderType={"limit": {"tif": order_data.get("tif", "Gtc")}},
                reduceOnly=bool(order_data.get("reduceOnly")),
                status="open",
                statusTimestamp=int(cast(int, order_data["timestamp"])),
                cloid=None,
            )

            mapper = HyperliquidOrderMapper()
            result = mapper.transform_raw_order_to_internal(raw_order)

            # Property: Financial values should be preserved as Decimal
            assert isinstance(result.quantity_requested, Decimal)

            # Property: Precision should be preserved
            original_size = Decimal(str(order_data["sz"]))
            assert result.quantity_requested == original_size

            if order_data.get("px") and order_data["px"] != "0":
                assert isinstance(result.price, Decimal)
                original_price = Decimal(str(order_data["px"]))
                assert result.price == original_price

            # Property: Order ID should be preserved
            assert result.exchange_order_id == str(order_data["oid"])

            # Property: Side should be properly mapped
            assert result.side in [OrderSide.BUY, OrderSide.SELL]

        except (ValueError, TypeError, AttributeError):
            # Expected exceptions for invalid data
            pass

    @given(order_data=hyperliquid_order_data_strategy())
    def test_order_transformation_financial_invariants(self, order_data: dict[str, object]) -> None:
        """Property: Transformed orders should maintain financial invariants."""
        # Skip invalid data
        assume(order_data["side"] in ["B", "A"])

        try:
            # Skip zero/negative sizes
            size_decimal = Decimal(str(order_data["sz"]))
            assume(size_decimal > 0)

            # Skip this test if we can't create valid mock data
            if not order_data.get("sz") or not order_data.get("side"):
                return

            # Create mock raw order for testing

            raw_order = HyperliquidRawOrder(
                oid=int(cast(int, order_data["oid"])),
                asset=str(order_data["coin"]),
                side=str(order_data["side"]),
                sz=str(order_data["sz"]),
                limitPx=str(order_data.get("px", "0")),
                remainingSz=str(order_data["sz"]),
                timestamp=int(cast(int, order_data["timestamp"])),
                orderType={"limit": {"tif": order_data.get("tif", "Gtc")}},
                reduceOnly=bool(order_data.get("reduceOnly")),
                status="open",
                statusTimestamp=int(cast(int, order_data["timestamp"])),
                cloid=None,
            )

            mapper = HyperliquidOrderMapper()
            result = mapper.transform_raw_order_to_internal(raw_order)

            # Property: Quantities should be positive
            assert result.quantity_requested > Decimal(0)

            # Property: Prices should be positive or None
            if result.price is not None:
                assert result.price > Decimal(0)

            # Property: Stop prices should be positive or None
            if hasattr(result, "stop_price") and result.stop_price is not None:
                assert result.stop_price > Decimal(0)

            # Property: Financial values should be finite
            assert result.quantity_requested.is_finite()
            if result.price is not None:
                assert result.price.is_finite()

        except (ValueError, TypeError, AttributeError):
            # Expected for invalid input data
            pass

    @given(
        valid_size=hyperliquid_financial_value_strategy(),
        valid_price=st.one_of(st.none(), hyperliquid_financial_value_strategy()),
    )
    def test_order_transformation_round_trip_properties(
        self, valid_size: str, valid_price: str | None
    ) -> None:
        """Property: Valid financial values should survive round-trip transformation."""
        # Skip zero sizes and prices
        assume(Decimal(valid_size) > Decimal(0))
        if valid_price:
            assume(Decimal(valid_price) > Decimal(0))

        # Create minimal valid order data
        order_data = {
            "oid": 123456789,
            "user": "0x1234567890123456789012345678901234567890",
            "coin": "BTC",
            "side": "B",
            "sz": valid_size,
            "px": valid_price,
            "orderType": "Limit" if valid_price else "Market",
            "timestamp": 1700000000000,
            "triggerCondition": None,
            "triggerPx": None,
            "reduceOnly": False,
            "tif": "Gtc",
        }

        try:
            # Create mock raw order for testing

            raw_order = HyperliquidRawOrder(
                oid=int(cast(int, order_data["oid"])),
                asset=str(order_data["coin"]),
                side=str(order_data["side"]),
                sz=valid_size,
                limitPx=valid_price or "0",
                remainingSz=valid_size,
                timestamp=int(cast(int, order_data["timestamp"])),
                orderType={"market": {}}
                if valid_price is None
                else {"limit": {"tif": str(order_data["tif"])}},
                reduceOnly=bool(order_data.get("reduceOnly")),
                status="open",
                statusTimestamp=int(cast(int, order_data["timestamp"])),
                cloid=None,
            )

            mapper = HyperliquidOrderMapper()
            result = mapper.transform_raw_order_to_internal(raw_order)

            # Property: Values should be exactly equal as Decimals
            assert result.quantity_requested == Decimal(valid_size)
            if valid_price and valid_price != "0":
                assert result.price == Decimal(valid_price)

            # Property: Values should be finite and positive
            assert result.quantity_requested.is_finite()
            assert result.quantity_requested > 0
            if valid_price and valid_price != "0" and result.price is not None:
                assert result.price.is_finite()
                assert result.price > 0

        except (ValueError, TypeError, AttributeError) as e:
            # Should not fail for valid financial inputs
            pytest.fail(f"Valid financial data rejected: {e}")


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID TIME IN FORCE MAPPING
# =============================================================================


class TestHyperliquidTimeInForceMappingProperties:
    """Property-based tests for Hyperliquid time in force mapping."""

    @given(hl_tif=st.sampled_from(["Gtc", "Ioc", "Alo"]))
    def test_tif_mapping_consistency(self, hl_tif: str) -> None:
        """Property: Time in force mapping should be consistent."""
        # TIF mapping expects order type dict format
        order_type_dict = {"limit": {"tif": hl_tif}}

        result1 = HyperliquidTradingEnumMapper.map_time_in_force(order_type_dict)
        result2 = HyperliquidTradingEnumMapper.map_time_in_force(order_type_dict)

        # Property: Same input should always give same output
        assert result1 == result2

        # Property: Result should always be a valid TimeInForce
        assert isinstance(result1, TimeInForce)

    def test_known_tif_mapping_correctness(self) -> None:
        """Property: Known TIF values should map correctly."""
        expected_mappings = {
            "Gtc": TimeInForce.GTC,
            "Ioc": TimeInForce.IOC,
            "Alo": TimeInForce.ALO,  # Alo maps to ALO (Add Liquidity Only)
        }

        for hl_tif, expected in expected_mappings.items():
            order_type_dict = {"limit": {"tif": hl_tif}}
            result = HyperliquidTradingEnumMapper.map_time_in_force(order_type_dict)
            assert result == expected


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestHyperliquidOrderMapperIntegrationProperties:
    """Integration property tests for Hyperliquid order mapper."""

    @given(
        status=st.sampled_from(["open", "filled", "canceled"]),
        order_type=st.sampled_from(["Market", "Limit"]),
        tif=st.sampled_from(["Gtc", "Ioc"]),
    )
    def test_enum_mapping_consistency(self, status: str, order_type: str, tif: str) -> None:
        """Property: All enum mappings should be consistent."""
        mapped_status = HyperliquidTradingEnumMapper.map_status_to_internal(status)

        order_type_dict = {order_type.lower(): {"tif": tif}}
        mapped_type = HyperliquidTradingEnumMapper.map_type_to_internal(order_type_dict, None)
        mapped_tif = HyperliquidTradingEnumMapper.map_time_in_force(order_type_dict)

        # Property: All mappings should return valid enum values
        assert isinstance(mapped_status, OrderStatus)
        assert isinstance(mapped_type, OrderType)
        assert isinstance(mapped_tif, TimeInForce)

        # Property: Mappings should be deterministic
        assert HyperliquidTradingEnumMapper.map_status_to_internal(status) == mapped_status
        assert (
            HyperliquidTradingEnumMapper.map_type_to_internal(order_type_dict, None) == mapped_type
        )
        assert HyperliquidTradingEnumMapper.map_time_in_force(order_type_dict) == mapped_tif

    @given(order_data=hyperliquid_order_data_strategy())
    def test_transformation_error_safety(self, order_data: dict[str, object]) -> None:
        """Property: Transformation errors should be safe and informative."""
        try:
            # Create mock raw order for testing error handling

            # Use potentially invalid data to test error handling
            raw_order = HyperliquidRawOrder(
                oid=int(cast(int, order_data.get("oid", 0))),
                asset=str(order_data.get("coin", "INVALID")),
                side=str(order_data.get("side", "INVALID")),
                sz=str(order_data.get("sz", "0")),
                limitPx=str(order_data.get("px", "0")),
                remainingSz=str(order_data.get("sz", "0")),
                timestamp=int(cast(int, order_data.get("timestamp", 0))),
                orderType={"limit": {"tif": "Gtc"}},  # Use valid format
                reduceOnly=bool(order_data.get("reduceOnly")),
                status="open",  # Use valid status
                statusTimestamp=int(cast(int, order_data.get("timestamp", 0))),
                cloid=None,
            )

            mapper = HyperliquidOrderMapper()
            mapper.transform_raw_order_to_internal(raw_order)
        except (ValueError, TypeError, AttributeError) as e:
            # Property: Error messages should be informative (contain relevant info)
            error_msg = str(e)
            if len(error_msg) == 0:
                pytest.fail("Error message should not be empty")

            # Property: Should not contain sensitive information
            if "password" in error_msg.lower():
                pytest.fail("Error message should not contain sensitive information")
            if "secret" in error_msg.lower():
                pytest.fail("Error message should not contain sensitive information")
            # Note: Skip 'key' check as it may appear in randomly generated test data
