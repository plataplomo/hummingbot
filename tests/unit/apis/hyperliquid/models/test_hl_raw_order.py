"""Property-based tests for Hyperliquid Raw Order models.

This module tests the critical Hyperliquid raw order models for API boundary validation to ensure:
- Robust field validation for all order data from external APIs
- Financial precision preservation in all price/quantity fields
- Order type structure consistency (limit, market, trigger)
- Type safety and boundary validation for all field types
- Serialization properties for API signing requirements
- Edge case handling for malformed or corrupted API responses

SECURITY CRITICAL: Raw order model errors could allow malformed external data
to enter the trading system, leading to incorrect order processing, invalid trades,
or system instability.
"""

from decimal import Decimal
from typing import Any, Literal, cast

import pytest
from hypothesis import given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawTriggerInfo
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawHistoricalOrdersRequestPayload,
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawPlaceOrderAction,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR HYPERLIQUID RAW ORDER TESTING
# =============================================================================


def valid_string_strategy(max_length: int = 64) -> SearchStrategy[str]:
    """Generate valid non-empty strings with reasonable length.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_."),
        min_size=1,
        max_size=max_length,
    ).filter(lambda x: len(x.strip()) > 0)


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for financial amounts.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        # Normal decimal values
        st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal(1000000),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Common edge cases
        st.just("0.00000001"),
        st.just("999999.99999999"),
        st.just("1.0"),
        st.just("100"),
        st.just("50000.0"),
    ])


def ethereum_address_strategy() -> SearchStrategy[str]:
    """Generate valid Ethereum addresses.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.text(alphabet="0123456789abcdef", min_size=40, max_size=40).map(lambda x: f"0x{x}")


def cloid_strategy() -> SearchStrategy[str | None]:
    """Generate valid client order IDs (Hyperliquid format).

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        st.none(),
        # Hex format cloids
        st.text(alphabet="0123456789abcdef", min_size=8, max_size=16).map(lambda x: f"0x{x}"),
        # User-defined string cloids
        valid_string_strategy(max_length=32),
    ])


def asset_index_strategy() -> SearchStrategy[int]:
    """Generate valid asset indices.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.integers(min_value=0, max_value=1000)


def tif_strategy() -> SearchStrategy[str]:
    """Generate valid time-in-force values for Hyperliquid.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.sampled_from(["Alo", "Ioc", "Gtc"])


def limit_order_type_strategy() -> SearchStrategy[HyperliquidRawLimitOrderTypeDetails]:
    """Generate valid limit order type details.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.builds(HyperliquidRawLimitOrderTypeDetails, tif=tif_strategy())


def market_order_type_strategy() -> SearchStrategy[HyperliquidRawMarketOrderTypeDetails]:
    """Generate valid market order type details.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.builds(HyperliquidRawMarketOrderTypeDetails)


def trigger_info_strategy() -> SearchStrategy[HyperliquidRawTriggerInfo]:
    """Generate valid trigger info for orders.

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.builds(
        HyperliquidRawTriggerInfo,
        isMarket=st.booleans(),
        triggerPx=financial_decimal_string_strategy(),
        tpsl=st.sampled_from(["tp", "sl"]),
    )


def order_type_strategy() -> SearchStrategy[HyperliquidRawOrderType]:
    """Generate valid order types (limit, market, or trigger).

    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        # Limit order type
        st.builds(
            HyperliquidRawOrderType,
            limit=limit_order_type_strategy(),
            market=st.none(),
            trigger=st.none(),
        ),
        # Market order type
        st.builds(
            HyperliquidRawOrderType,
            limit=st.none(),
            market=market_order_type_strategy(),
            trigger=st.none(),
        ),
        # Trigger order type
        st.builds(
            HyperliquidRawOrderType,
            limit=st.none(),
            market=st.none(),
            trigger=trigger_info_strategy(),
        ),
    ])


def place_order_action_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid place order action data.

    Returns:
        SearchStrategy for dict[str, Any] place order action data.
    """
    return st.fixed_dictionaries({
        "asset": asset_index_strategy(),
        "isBuy": st.booleans(),
        "limitPx": financial_decimal_string_strategy(),
        "sz": financial_decimal_string_strategy(),
        "reduceOnly": st.booleans(),
        "orderType": order_type_strategy(),
        "cloid": cloid_strategy(),
    })


def historical_orders_request_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid historical orders request data.

    Returns:
        SearchStrategy for dict[str, Any] historical orders request data.
    """
    return st.fixed_dictionaries({
        "type": st.just("historicalOrders"),
        "user": ethereum_address_strategy(),
    })


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW PLACE ORDER ACTION
# =============================================================================


class TestHyperliquidRawPlaceOrderActionProperties:
    """Property-based tests for HyperliquidRawPlaceOrderAction validation."""

    @given(order_data=place_order_action_strategy())
    def test_valid_order_creation_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid data should create valid models."""
        order = HyperliquidRawPlaceOrderAction(
            asset=cast(int, order_data["asset"]),
            isBuy=cast(bool, order_data["isBuy"]),
            limitPx=cast(str, order_data["limitPx"]),
            sz=cast(str, order_data["sz"]),
            reduceOnly=cast(bool, order_data["reduceOnly"]),
            orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
            cloid=cast(str | None, order_data.get("cloid")),
        )

        # Property: Required fields should be preserved exactly
        assert order.asset == order_data["asset"]
        assert order.isBuy == order_data["isBuy"]
        assert order.limitPx == order_data["limitPx"]
        assert order.sz == order_data["sz"]
        assert order.reduceOnly == order_data["reduceOnly"]

        # Property: Financial fields should preserve exact string values
        assert order.limitPx == order_data["limitPx"]
        assert order.sz == order_data["sz"]

        # Property: Optional cloid should handle None correctly
        assert order.cloid == order_data.get("cloid")

    @given(
        asset=asset_index_strategy(),
        isBuy=st.booleans(),
        limitPx=financial_decimal_string_strategy(),
        sz=financial_decimal_string_strategy(),
        reduceOnly=st.booleans(),
    )
    def test_missing_order_type_rejection(
        self, asset: int, isBuy: bool, limitPx: str, sz: str, reduceOnly: bool
    ) -> None:
        """Property: Orders without orderType should be rejected."""
        incomplete_data = {
            "asset": asset,
            "isBuy": isBuy,
            "limitPx": limitPx,
            "sz": sz,
            "reduceOnly": reduceOnly,
            # Missing orderType
        }

        # Property: Missing required field should cause validation error
        with pytest.raises(ValidationError):
            HyperliquidRawPlaceOrderAction(
                asset=cast(int, incomplete_data["asset"]),
                isBuy=cast(bool, incomplete_data["isBuy"]),
                limitPx=cast(str, incomplete_data["limitPx"]),
                sz=cast(str, incomplete_data["sz"]),
                reduceOnly=cast(bool, incomplete_data["reduceOnly"]),
                # Missing orderType
            )  # type: ignore[call-arg]

    @given(order_data=place_order_action_strategy())
    def test_serialization_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Orders should serialize correctly for API signing."""
        order = HyperliquidRawPlaceOrderAction(
            asset=cast(int, order_data["asset"]),
            isBuy=cast(bool, order_data["isBuy"]),
            limitPx=cast(str, order_data["limitPx"]),
            sz=cast(str, order_data["sz"]),
            reduceOnly=cast(bool, order_data["reduceOnly"]),
            orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
            cloid=cast(str | None, order_data.get("cloid")),
        )

        # Serialize to dict for API
        serialized = order.model_dump(exclude_none=True)

        # Property: All non-None fields should be present
        assert "asset" in serialized
        assert "isBuy" in serialized
        assert "limitPx" in serialized
        assert "sz" in serialized
        assert "reduceOnly" in serialized
        assert "orderType" in serialized

        # Property: cloid should only be present if not None
        if order_data.get("cloid") is not None:
            assert "cloid" in serialized
            assert serialized["cloid"] == order_data["cloid"]
        else:
            assert "cloid" not in serialized or serialized["cloid"] is None

    @given(
        invalid_decimal=st.one_of(
            st.just("NaN"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
        )
    )
    def test_invalid_decimal_fields_rejection(self, invalid_decimal: str) -> None:
        """Property: Invalid decimal strings should be consistently rejected."""
        base_data: dict[str, int | bool | HyperliquidRawOrderType | str] = {
            "asset": 0,
            "isBuy": True,
            "reduceOnly": False,
            "orderType": HyperliquidRawOrderType(market=HyperliquidRawMarketOrderTypeDetails()),
        }

        # Test limitPx with invalid decimal
        test_data = base_data.copy()
        test_data["limitPx"] = invalid_decimal
        test_data["sz"] = "1.0"

        # Property: Invalid decimal should cause validation error
        with pytest.raises((ValidationError, Exception)):
            HyperliquidRawPlaceOrderAction(
                asset=cast(int, test_data["asset"]),
                isBuy=cast(bool, test_data["isBuy"]),
                limitPx=test_data["limitPx"],  # type: ignore[arg-type]
                sz=cast(str, test_data["sz"]),
                reduceOnly=cast(bool, test_data["reduceOnly"]),
                orderType=cast(HyperliquidRawOrderType, test_data["orderType"]),
            )

        # Test sz with invalid decimal
        test_data = base_data.copy()
        test_data["limitPx"] = "100.0"
        test_data["sz"] = invalid_decimal

        with pytest.raises((ValidationError, Exception)):
            HyperliquidRawPlaceOrderAction(
                asset=cast(int, test_data["asset"]),
                isBuy=cast(bool, test_data["isBuy"]),
                limitPx=cast(str, test_data["limitPx"]),
                sz=test_data["sz"],  # type: ignore[arg-type]
                reduceOnly=cast(bool, test_data["reduceOnly"]),
                orderType=cast(HyperliquidRawOrderType, test_data["orderType"]),
            )

    @given(negative_asset=st.integers(min_value=-1000, max_value=-1))
    def test_negative_asset_index_rejection(self, negative_asset: int) -> None:
        """Property: Negative asset indices should be rejected."""
        order_data = {
            "asset": negative_asset,
            "isBuy": True,
            "limitPx": "100.0",
            "sz": "1.0",
            "reduceOnly": False,
            "orderType": HyperliquidRawOrderType(market=HyperliquidRawMarketOrderTypeDetails()),
        }

        # Property: Negative asset index should cause validation error
        with pytest.raises(ValidationError):
            HyperliquidRawPlaceOrderAction(
                asset=cast(int, order_data["asset"]),
                isBuy=cast(bool, order_data["isBuy"]),
                limitPx=cast(str, order_data["limitPx"]),
                sz=cast(str, order_data["sz"]),
                reduceOnly=cast(bool, order_data["reduceOnly"]),
                orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
            )

    @given(order_data=place_order_action_strategy())
    def test_extra_fields_rejection(self, order_data: dict[str, Any]) -> None:
        """Property: Extra fields should always be rejected."""
        # Add an extra field
        invalid_data: dict[str, Any] = order_data.copy()
        invalid_data["extraField"] = "should_not_be_allowed"

        # Property: Extra fields should cause validation error
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawPlaceOrderAction(**invalid_data)

        # Property: Error should mention extra field
        assert "extra" in str(exc_info.value).lower()

    @given(limitPx=financial_decimal_string_strategy(), sz=financial_decimal_string_strategy())
    def test_financial_precision_preservation(self, limitPx: str, sz: str) -> None:
        """Property: Financial values should preserve exact string precision."""
        order_data = {
            "asset": 0,
            "isBuy": True,
            "limitPx": limitPx,
            "sz": sz,
            "reduceOnly": False,
            "orderType": HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc")
            ),
        }

        order = HyperliquidRawPlaceOrderAction(
            asset=cast(int, order_data["asset"]),
            isBuy=cast(bool, order_data["isBuy"]),
            limitPx=cast(str, order_data["limitPx"]),
            sz=cast(str, order_data["sz"]),
            reduceOnly=cast(bool, order_data["reduceOnly"]),
            orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
        )

        # Property: Financial values should be preserved exactly as strings
        assert order.limitPx == limitPx
        assert order.sz == sz

        # Property: If parseable, should be valid decimals
        limit_decimal = Decimal(limitPx)
        sz_decimal = Decimal(sz)
        assert limit_decimal.is_finite()
        assert sz_decimal.is_finite()


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ORDER TYPE
# =============================================================================


class TestHyperliquidRawOrderTypeProperties:
    """Property-based tests for HyperliquidRawOrderType validation."""

    @given(order_type=order_type_strategy())
    def test_order_type_exclusivity(self, order_type: HyperliquidRawOrderType) -> None:
        """Property: Order type should have exactly one non-None field."""
        non_none_count = sum([
            order_type.limit is not None,
            order_type.market is not None,
            order_type.trigger is not None,
        ])

        # Property: Exactly one order type should be set
        assert non_none_count == 1

    @given(order_type=order_type_strategy())
    def test_order_type_serialization(self, order_type: HyperliquidRawOrderType) -> None:
        """Property: Order type serialization should produce clean structure."""
        # Serialize the order type
        serialized = order_type.model_dump(exclude_none=True)

        # Property: Only one key should be present
        assert len(serialized) == 1

        # Property: The key should be one of the valid types
        assert next(iter(serialized.keys())) in ["limit", "market", "trigger"]

        # Property: Market orders should have empty dict value
        if "market" in serialized:
            assert serialized["market"] == {}

    @given(tif=tif_strategy())
    def test_limit_order_type_tif_preservation(self, tif: str) -> None:
        """Property: Limit order type should preserve TIF value."""
        limit_details = HyperliquidRawLimitOrderTypeDetails(tif=tif)
        order_type = HyperliquidRawOrderType(limit=limit_details)

        # Property: TIF should be preserved
        assert order_type.limit is not None
        assert order_type.limit.tif == tif

        # Property: Serialization should include TIF
        serialized = order_type.model_dump(exclude_none=True)
        assert serialized["limit"]["tif"] == tif

    def test_market_order_type_empty_structure(self) -> None:
        """Property: Market order type should serialize to empty dict."""
        market_details = HyperliquidRawMarketOrderTypeDetails()
        order_type = HyperliquidRawOrderType(market=market_details)

        # Property: Market should be empty dict
        serialized = order_type.model_dump(exclude_none=True)
        assert serialized == {"market": {}}


# =============================================================================
# PROPERTY TESTS FOR HISTORICAL ORDERS REQUEST
# =============================================================================


class TestHyperliquidHistoricalOrdersRequestProperties:
    """Property-based tests for HyperliquidRawHistoricalOrdersRequestPayload."""

    @given(request_data=historical_orders_request_strategy())
    def test_valid_request_creation(self, request_data: dict[str, Any]) -> None:
        """Property: Valid request data should create valid models."""
        request = HyperliquidRawHistoricalOrdersRequestPayload(
            type=cast(Literal["historicalOrders"], request_data["type"]),
            user=cast(str, request_data["user"]),
        )

        # Property: Type should always be "historicalOrders"
        assert request.type == "historicalOrders"

        # Property: User address should be preserved
        assert request.user == request_data["user"]

        # Property: Address should be Ethereum format
        assert request.user.startswith("0x")
        assert len(request.user) == 42

    @given(invalid_address=st.text(alphabet="xyz", min_size=1, max_size=10))
    def test_invalid_address_rejection(self, invalid_address: str) -> None:
        """Property: Invalid Ethereum addresses should be rejected."""
        # Property: Invalid address should cause validation error
        with pytest.raises((ValidationError, Exception)):
            HyperliquidRawHistoricalOrdersRequestPayload(
                type="historicalOrders",
                user=invalid_address,
            )

    @given(request_data=historical_orders_request_strategy())
    def test_request_immutability(self, request_data: dict[str, Any]) -> None:
        """Property: Request models should be immutable."""
        request = HyperliquidRawHistoricalOrdersRequestPayload(
            type=cast(Literal["historicalOrders"], request_data["type"]),
            user=cast(str, request_data["user"]),
        )

        # Property: Attempting to modify fields should fail
        with pytest.raises((AttributeError, ValidationError)):
            request.type = "modifiedType"  # type: ignore[assignment]

        with pytest.raises((AttributeError, ValidationError)):
            request.user = "0x" + "a" * 40


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestHyperliquidRawOrderIntegrationProperties:
    """Integration property tests for Hyperliquid raw order models."""

    @given(order_data=place_order_action_strategy())
    def test_model_deterministic_creation(self, order_data: dict[str, Any]) -> None:
        """Property: Model creation should be deterministic for same inputs."""
        order1 = HyperliquidRawPlaceOrderAction(
            asset=order_data["asset"],
            isBuy=order_data["isBuy"],
            limitPx=order_data["limitPx"],
            sz=order_data["sz"],
            reduceOnly=order_data["reduceOnly"],
            orderType=order_data["orderType"],
            cloid=order_data.get("cloid"),
        )
        order2 = HyperliquidRawPlaceOrderAction(
            asset=order_data["asset"],
            isBuy=order_data["isBuy"],
            limitPx=order_data["limitPx"],
            sz=order_data["sz"],
            reduceOnly=order_data["reduceOnly"],
            orderType=order_data["orderType"],
            cloid=order_data.get("cloid"),
        )

        # Property: All field values should be identical
        assert order1.asset == order2.asset
        assert order1.isBuy == order2.isBuy
        assert order1.limitPx == order2.limitPx
        assert order1.sz == order2.sz
        assert order1.reduceOnly == order2.reduceOnly
        assert order1.cloid == order2.cloid

    @given(
        decimal_value=financial_decimal_string_strategy(),
        operation=st.sampled_from(["multiplication", "addition", "comparison"]),
    )
    def test_financial_calculation_properties(self, decimal_value: str, operation: str) -> None:
        """Property: Financial values should maintain precision for calculations."""
        order_data = {
            "asset": 0,
            "isBuy": True,
            "limitPx": decimal_value,
            "sz": decimal_value,
            "reduceOnly": False,
            "orderType": HyperliquidRawOrderType(
                limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc")
            ),
        }

        order = HyperliquidRawPlaceOrderAction(
            asset=cast(int, order_data["asset"]),
            isBuy=cast(bool, order_data["isBuy"]),
            limitPx=cast(str, order_data["limitPx"]),
            sz=cast(str, order_data["sz"]),
            reduceOnly=cast(bool, order_data["reduceOnly"]),
            orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
        )

        # Property: Should be able to reconstruct exact Decimal values
        price_decimal = Decimal(order.limitPx)
        size_decimal = Decimal(order.sz)

        # Property: Basic operations should work with exact precision
        if operation == "multiplication":
            result = price_decimal * size_decimal
            assert result.is_finite()
        elif operation == "addition":
            result = price_decimal + size_decimal
            assert result.is_finite()
        elif operation == "comparison":
            # Property: Comparison should work correctly
            assert price_decimal == Decimal(decimal_value)
            assert size_decimal == Decimal(decimal_value)

    @given(orders=st.lists(place_order_action_strategy(), min_size=2, max_size=5))
    def test_multiple_orders_independence(self, orders: list[dict[str, Any]]) -> None:
        """Property: Multiple orders should be processed independently."""
        parsed_orders: list[HyperliquidRawPlaceOrderAction] = []

        for order_data in orders:
            order = HyperliquidRawPlaceOrderAction(
                asset=cast(int, order_data["asset"]),
                isBuy=cast(bool, order_data["isBuy"]),
                limitPx=cast(str, order_data["limitPx"]),
                sz=cast(str, order_data["sz"]),
                reduceOnly=cast(bool, order_data["reduceOnly"]),
                orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
                cloid=cast(str | None, order_data.get("cloid")),
            )
            parsed_orders.append(order)

        # Property: Each order should maintain its individual data
        for i, (original_data, parsed_order) in enumerate(zip(orders, parsed_orders, strict=False)):
            assert parsed_order.asset == original_data["asset"]
            assert parsed_order.isBuy == original_data["isBuy"]
            assert parsed_order.limitPx == original_data["limitPx"]
            assert parsed_order.sz == original_data["sz"]

            # Property: Orders should not affect each other
            for j, other_order in enumerate(parsed_orders):
                if i != j and original_data["asset"] != orders[j]["asset"]:
                    # Orders with different data should remain independent
                    assert parsed_order.asset != other_order.asset

    @given(order_type_choice=st.sampled_from(["limit", "market", "trigger"]), tif=tif_strategy())
    def test_order_type_consistency(self, order_type_choice: str, tif: str) -> None:
        """Property: Order type should maintain consistency through operations."""
        # Create order type based on choice
        if order_type_choice == "limit":
            order_type = HyperliquidRawOrderType(limit=HyperliquidRawLimitOrderTypeDetails(tif=tif))
        elif order_type_choice == "market":
            order_type = HyperliquidRawOrderType(market=HyperliquidRawMarketOrderTypeDetails())
        else:  # trigger
            order_type = HyperliquidRawOrderType(
                trigger=HyperliquidRawTriggerInfo(isMarket=True, triggerPx="100.0", tpsl="tp")
            )

        # Create order with this type
        order_data = {
            "asset": 0,
            "isBuy": True,
            "limitPx": "100.0",
            "sz": "1.0",
            "reduceOnly": False,
            "orderType": order_type,
        }

        order = HyperliquidRawPlaceOrderAction(
            asset=cast(int, order_data["asset"]),
            isBuy=cast(bool, order_data["isBuy"]),
            limitPx=cast(str, order_data["limitPx"]),
            sz=cast(str, order_data["sz"]),
            reduceOnly=cast(bool, order_data["reduceOnly"]),
            orderType=cast(HyperliquidRawOrderType, order_data["orderType"]),
        )

        # Property: Order type should be preserved
        if order_type_choice == "limit":
            assert order.orderType.limit is not None
            assert order.orderType.market is None
            assert order.orderType.trigger is None
            assert order.orderType.limit.tif == tif
        elif order_type_choice == "market":
            assert order.orderType.market is not None
            assert order.orderType.limit is None
            assert order.orderType.trigger is None
        else:  # trigger
            assert order.orderType.trigger is not None
            assert order.orderType.limit is None
            assert order.orderType.market is None
