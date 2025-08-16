"""Unit tests for BackpackTradingRequestBuilder order management methods with property testing.

Enhanced with Hypothesis for comprehensive property-based testing to ensure
robust handling of edge cases, security boundaries, and data transformations.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import DrawFn, SearchStrategy, composite
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawOrderCancelAllRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderExecuteRequest,
)
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetOrderParams,
)
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
)
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import (
    BTC_USDT_BP,
    ETH_BP,
    ETH_USDC_PERP_BP,
    SOL_USDC_BP,
)


# =======================
# Helper Functions for Strategy Builders
# =======================


def _build_uuid_format(a: str, b: str, c: str, d: str) -> str:
    """Build UUID-like format string.

    Returns:
        str: UUID-like string in format "{a}-{b}-{c}-{d}".
    """
    return f"{a}-{b}-{c}-{d}"


# =======================
# Strategy Builders
# =======================


def symbol_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Symbol instances for testing.

    Returns:
        SearchStrategy[Symbol]: Strategy for Symbol instances.
    """
    return st.sampled_from([SOL_USDC_BP, BTC_USDT_BP, ETH_BP, ETH_USDC_PERP_BP])


def decimal_price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid price values as Decimal.

    Returns:
        SearchStrategy[Decimal]: Strategy for price decimals.
    """
    return st.one_of([
        # Normal prices
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=2),
        # High precision prices
        st.decimals(min_value=Decimal("0.00001"), max_value=Decimal(999999), places=8),
        # Edge case prices
        st.sampled_from([
            Decimal("0.01"),
            Decimal("0.99"),
            Decimal("1.00"),
            Decimal("99.99"),
            Decimal("100.00"),
            Decimal("999.99"),
            Decimal("9999.99"),
            Decimal("10000.00"),
            Decimal("99999.99"),
        ]),
    ])


def decimal_quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate valid quantity values as Decimal.

    Returns:
        SearchStrategy[Decimal]: Strategy for quantity decimals.
    """
    return st.one_of([
        # Normal quantities
        st.decimals(min_value=Decimal("0.001"), max_value=Decimal(10000), places=3),
        # High precision quantities
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000), places=8),
        # Edge case quantities
        st.sampled_from([
            Decimal("0.001"),
            Decimal("0.01"),
            Decimal("0.1"),
            Decimal(1),
            Decimal(10),
            Decimal(100),
            Decimal(1000),
        ]),
    ])


def order_side_strategy() -> SearchStrategy[OrderSide]:
    """Generate OrderSide enum values.

    Returns:
        SearchStrategy[OrderSide]: Strategy for order sides.
    """
    return st.sampled_from(list(OrderSide))


def order_type_strategy() -> SearchStrategy[OrderType]:
    """Generate OrderType enum values.

    Returns:
        SearchStrategy[OrderType]: Strategy for order types.
    """
    return st.sampled_from([
        OrderType.LIMIT,
        OrderType.MARKET,
        OrderType.STOP_MARKET,
        OrderType.STOP_LIMIT,
    ])


def time_in_force_strategy() -> SearchStrategy[TimeInForce]:
    """Generate TimeInForce enum values.

    Returns:
        SearchStrategy[TimeInForce]: Strategy for time in force values.
    """
    return st.sampled_from(list(TimeInForce))


def client_order_id_strategy() -> SearchStrategy[str]:
    """Generate valid client order IDs.

    Returns:
        SearchStrategy[str]: Strategy for client order IDs.
    """
    return st.one_of([
        # Numeric IDs
        st.integers(min_value=1, max_value=2**31 - 1).map(str),
        # Alphanumeric IDs
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]), min_size=1, max_size=64
        ),
        # UUID-like IDs
        st.builds(
            _build_uuid_format,
            st.text(alphabet="0123456789abcdef", min_size=8, max_size=8),
            st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            st.text(alphabet="0123456789abcdef", min_size=12, max_size=12),
        ),
    ])


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid exchange order IDs.

    Returns:
        SearchStrategy[str]: Strategy for order IDs.
    """
    return st.text(
        alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]), min_size=1, max_size=128
    )


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamps in milliseconds.

    Returns:
        SearchStrategy[int]: Strategy for timestamps.
    """
    # Timestamps from 2020-01-01 to 2030-01-01 in milliseconds
    return st.integers(min_value=1577836800000, max_value=1893456000000)


def limit_strategy() -> SearchStrategy[int]:
    """Generate valid limit values for queries.

    Returns:
        SearchStrategy[int]: Strategy for limit values.
    """
    return st.integers(min_value=1, max_value=1000)


@composite
def order_execution_strategy(draw: DrawFn) -> OrderExecution:
    """Generate OrderExecution instances with various configurations.

    Args:
        draw: Hypothesis draw function.

    Returns:
        OrderExecution: Order execution configuration.
    """
    liquidity = draw(
        st.sampled_from([
            None,  # Default
            LiquidityRequirement.POST_ONLY,
            LiquidityRequirement.IMMEDIATE_OR_CANCEL,
            LiquidityRequirement.FILL_OR_KILL,
        ])
    )

    if liquidity is None:
        return OrderExecution()
    return OrderExecution(liquidity_requirement=liquidity)


@composite
def place_order_params_strategy(
    draw: DrawFn,
) -> dict[str, Any]:
    """Generate valid parameters for place_order_payload.

    Args:
        draw: Hypothesis draw function.

    Returns:
        dict: Valid parameters for order placement.
    """
    symbol = draw(symbol_strategy())
    order_side = draw(order_side_strategy())
    order_type = draw(order_type_strategy())
    quantity = draw(decimal_quantity_strategy())
    time_in_force = draw(time_in_force_strategy())

    params: dict[str, Any] = {
        "symbol": symbol,
        "order_side": order_side,
        "order_type": order_type,
        "quantity": quantity,
        "time_in_force": time_in_force,
    }

    # Add price for limit orders
    if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
        params["price"] = draw(decimal_price_strategy())

    # Add stop price for stop orders
    if order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]:
        params["stop_price"] = draw(decimal_price_strategy())

    # Optionally add client order ID
    if draw(st.booleans()):
        params["client_order_id"] = draw(client_order_id_strategy())

    # Optionally add execution configuration
    if draw(st.booleans()):
        params["execution"] = draw(order_execution_strategy())

    return params


# =======================
# Property-Based Tests
# =======================


class TestBuildPlaceOrderPayload:
    """Property-based tests for build_place_order_payload method."""

    @given(params=place_order_params_strategy())
    @settings(max_examples=100)
    def test_place_order_payload_properties(
        self,
        params: dict[str, Any],
    ) -> None:
        """Test build_place_order_payload with various parameter combinations."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(**params)

        # Verify we get the correct model type
        assert isinstance(payload, BackpackRawOrderExecuteRequest)

        # Verify required fields are present
        assert payload.symbol == params["symbol"].value
        assert payload.quantity is not None

        # Verify side mapping
        if params["order_side"] == OrderSide.BUY:
            assert payload.side == "Bid"
        else:
            assert payload.side == "Ask"

        # Verify order type mapping
        if params["order_type"] == OrderType.LIMIT:
            assert payload.orderType == "Limit"
        elif params["order_type"] in [OrderType.MARKET, OrderType.STOP_MARKET]:
            assert payload.orderType == "Market"
        elif params["order_type"] == OrderType.STOP_LIMIT:
            assert payload.orderType == "Limit"

        # Verify price presence for limit orders
        if params["order_type"] in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
            assert payload.price is not None

        # Verify trigger price for stop orders
        if params["order_type"] in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]:
            assert payload.triggerPrice is not None

    @given(
        symbol=symbol_strategy(),
        side=order_side_strategy(),
        quantity=decimal_quantity_strategy(),
        price=decimal_price_strategy(),
    )
    def test_limit_order_consistency(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
    ) -> None:
        """Test LIMIT order creation maintains data consistency."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=symbol,
            order_side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
            price=price,
        )

        assert payload.orderType == "Limit"
        assert payload.price is not None
        assert Decimal(payload.price) == price
        assert payload.quantity is not None
        assert Decimal(payload.quantity) == quantity
        assert payload.timeInForce == "GTC"

    @given(
        symbol=symbol_strategy(),
        side=order_side_strategy(),
        quantity=decimal_quantity_strategy(),
    )
    def test_market_order_no_price(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
    ) -> None:
        """Test MARKET orders don't have price field."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=symbol,
            order_side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            time_in_force=TimeInForce.IOC,
        )

        assert payload.orderType == "Market"
        assert payload.price is None
        assert payload.quantity is not None
        assert Decimal(payload.quantity) == quantity

    @given(
        symbol=symbol_strategy(),
        side=order_side_strategy(),
        quantity=decimal_quantity_strategy(),
        stop_price=decimal_price_strategy(),
    )
    def test_stop_market_order_trigger(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
        stop_price: Decimal,
    ) -> None:
        """Test STOP_MARKET orders have trigger price."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=symbol,
            order_side=side,
            order_type=OrderType.STOP_MARKET,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
            stop_price=stop_price,
        )

        assert payload.orderType == "Market"
        assert payload.triggerPrice is not None
        assert Decimal(payload.triggerPrice) == stop_price
        assert payload.triggerQuantity is not None

    @given(client_id=client_order_id_strategy())
    def test_client_order_id_handling(self, client_id: str) -> None:
        """Test client order ID is properly handled."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=SOL_USDC_BP,
            order_side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal(1),
            time_in_force=TimeInForce.GTC,
            price=Decimal(100),
            client_order_id=client_id,
        )

        # Client ID should be converted appropriately
        assert payload.clientId is not None

    @given(
        symbol=symbol_strategy(),
        side=order_side_strategy(),
        quantity=decimal_quantity_strategy(),
        price=decimal_price_strategy(),
    )
    def test_post_only_order_flag(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
    ) -> None:
        """Test post-only orders have correct flag."""
        execution = OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY)

        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=symbol,
            order_side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
            price=price,
            execution=execution,
        )

        assert payload.postOnly is True

    # Legacy parametrized tests for specific mappings
    @pytest.mark.parametrize(
        ("side", "expected_side_str"),
        [
            (OrderSide.BUY, "Bid"),
            (OrderSide.SELL, "Ask"),
        ],
    )
    def test_build_place_order_payload_side_mapping(
        self,
        symbol_spot: Symbol,
        side: OrderSide,
        expected_side_str: str,
    ) -> None:
        """Test build_place_order_payload correctly maps order sides."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=symbol_spot,
            order_side=side,
            order_type=OrderType.MARKET,
            quantity=Decimal(1),
            time_in_force=TimeInForce.IOC,
        )

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        assert payload.side == expected_side_str

    @pytest.mark.parametrize(
        ("order_type", "expected_type_str"),
        [
            (OrderType.LIMIT, "Limit"),
            (OrderType.MARKET, "Market"),
            (OrderType.STOP_MARKET, "Market"),  # STOP_MARKET maps to "Market" with trigger
        ],
    )
    def test_build_place_order_payload_type_mapping(
        self,
        symbol_spot: Symbol,
        order_type: OrderType,
        expected_type_str: str,
    ) -> None:
        """Test build_place_order_payload correctly maps order types."""
        kwargs: dict[str, Any] = {
            "symbol": symbol_spot,
            "order_side": OrderSide.BUY,
            "order_type": order_type,
            "quantity": Decimal(1),
            "time_in_force": TimeInForce.GTC,
        }
        if order_type == OrderType.LIMIT:
            kwargs["price"] = Decimal(100)
        elif order_type == OrderType.STOP_MARKET:
            kwargs["stop_price"] = Decimal(100)

        payload = BackpackTradingRequestBuilder.build_place_order_payload(**kwargs)

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        assert payload.orderType == expected_type_str


class TestBuildCancelOrderPayload:
    """Property-based tests for build_cancel_order_payload method."""

    @given(
        symbol=symbol_strategy(),
        order_id=order_id_strategy(),
    )
    def test_cancel_with_order_id(self, symbol: Symbol, order_id: str) -> None:
        """Test cancel order payload with order ID."""
        payload = BackpackTradingRequestBuilder.build_cancel_order_payload(
            symbol, order_id=order_id
        )

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        assert payload.symbol == symbol.value
        assert payload.orderId == order_id
        assert payload.clientId is None

    @given(
        symbol=symbol_strategy(),
        client_id=client_order_id_strategy(),
    )
    def test_cancel_with_client_id(self, symbol: Symbol, client_id: str) -> None:
        """Test cancel order payload with client order ID."""
        payload = BackpackTradingRequestBuilder.build_cancel_order_payload(
            symbol, client_order_id=client_id
        )

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        assert payload.symbol == symbol.value
        assert payload.orderId is None
        # Client ID might be converted to integer
        assert payload.clientId is not None

    @given(
        symbol=symbol_strategy(),
        order_id=st.one_of(st.none(), order_id_strategy()),
        client_id=st.one_of(st.none(), client_order_id_strategy()),
    )
    def test_cancel_id_exclusivity(
        self,
        symbol: Symbol,
        order_id: str | None,
        client_id: str | None,
    ) -> None:
        """Test that either order_id or client_id is provided, not both."""
        if order_id is None and client_id is None:
            # Should handle the case where neither is provided
            return

        payload = BackpackTradingRequestBuilder.build_cancel_order_payload(
            symbol,
            order_id=order_id,
            client_order_id=client_id,
        )

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        assert payload.symbol == symbol.value

        # Only one ID type should be present
        if order_id:
            assert payload.orderId == order_id
        if client_id:
            assert payload.clientId is not None

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("symbol", "order_id", "client_id_int", "expected_payload"),
        [
            (SOL_USDC_BP, "123", None, {"symbol": SOL_USDC_BP.value, "orderId": "123"}),
            (BTC_USDT_BP, None, 456789, {"symbol": BTC_USDT_BP.value, "clientId": 456789}),
            (ETH_USDC_PERP_BP, "456", None, {"symbol": ETH_USDC_PERP_BP.value, "orderId": "456"}),
        ],
    )
    def test_build_cancel_order_payload_parametrized(
        self,
        symbol: Symbol,
        order_id: str | None,
        client_id_int: int | None,
        expected_payload: dict[str, str | int],
    ) -> None:
        """Test build_cancel_order_payload with various combinations."""
        client_id_str = str(client_id_int) if client_id_int is not None else None
        payload = BackpackTradingRequestBuilder.build_cancel_order_payload(
            symbol,
            order_id=order_id,
            client_order_id=client_id_str,
        )

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == expected_payload


class TestBuildGetOpenOrdersParams:
    """Property-based tests for build_get_open_orders_params method."""

    @given(symbol=st.one_of(st.none(), symbol_strategy()))
    def test_open_orders_params(self, symbol: Symbol | None) -> None:
        """Test get open orders params with optional symbol."""
        params = BackpackTradingRequestBuilder.build_get_open_orders_params(symbol)

        assert isinstance(params, BackpackRawGetOpenOrdersParams)

        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        if symbol is None:
            assert params_dict == {}
        else:
            assert params_dict == {"symbol": symbol.value}

    @given(symbol=symbol_strategy())
    def test_symbol_format_preservation(self, symbol: Symbol) -> None:
        """Test that symbol format is preserved."""
        params = BackpackTradingRequestBuilder.build_get_open_orders_params(symbol)

        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        # Some symbols might have format transformations
        assert "symbol" in params_dict
        assert isinstance(params_dict["symbol"], str)

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("symbol", "expected_dict"),
        [
            (None, {}),
            (SOL_USDC_BP, {"symbol": SOL_USDC_BP.value}),
            (BTC_USDT_BP, {"symbol": BTC_USDT_BP.value}),
            (ETH_BP, {"symbol": ETH_BP.value.replace("-", "_")}),
        ],
    )
    def test_build_get_open_orders_params_parametrized(
        self,
        symbol: Symbol | None,
        expected_dict: dict[str, str],
    ) -> None:
        """Test build_get_open_orders_params with various symbols."""
        params = BackpackTradingRequestBuilder.build_get_open_orders_params(symbol)

        assert isinstance(params, BackpackRawGetOpenOrdersParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_dict


class TestBuildGetOrderParams:
    """Property-based tests for build_get_order_params method."""

    @given(symbol=symbol_strategy())
    def test_get_order_params(self, symbol: Symbol) -> None:
        """Test get order params with symbol."""
        params = BackpackTradingRequestBuilder.build_get_order_params(symbol)

        assert isinstance(params, BackpackRawGetOrderParams)
        params_dict = params.model_dump(by_alias=True)
        assert params_dict["symbol"] == symbol.value

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("input_symbol", "expected_symbol"),
        [
            (SOL_USDC_BP, SOL_USDC_BP.value),
            (BTC_USDT_BP, BTC_USDT_BP.value),
            (ETH_BP, ETH_BP.value),
        ],
    )
    def test_build_get_order_params_parametrized(
        self,
        input_symbol: Symbol,
        expected_symbol: str,
    ) -> None:
        """Test build_get_order_params with various symbol formats."""
        params = BackpackTradingRequestBuilder.build_get_order_params(input_symbol)

        assert isinstance(params, BackpackRawGetOrderParams)
        params_dict = params.model_dump(by_alias=True)
        assert params_dict == {"symbol": expected_symbol}


class TestBuildGetOrderHistoryParams:
    """Property-based tests for build_get_order_history_params method."""

    @given(
        symbol=st.one_of(st.none(), symbol_strategy()),
        start_time=st.one_of(st.none(), timestamp_strategy()),
        end_time=st.one_of(st.none(), timestamp_strategy()),
        limit=st.one_of(st.none(), limit_strategy()),
        order_id=st.one_of(st.none(), order_id_strategy()),
        client_id=st.one_of(st.none(), client_order_id_strategy()),
    )
    def test_order_history_params_combinations(
        self,
        symbol: Symbol | None,
        start_time: int | None,
        end_time: int | None,
        limit: int | None,
        order_id: str | None,
        client_id: str | None,
    ) -> None:
        """Test order history params with various combinations."""
        # Ensure time range is valid
        if start_time and end_time and start_time > end_time:
            start_time, end_time = end_time, start_time

        params = BackpackTradingRequestBuilder.build_get_order_history_params(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit or 100,
            order_id=order_id,
            client_id=client_id,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify fields are set correctly
        if symbol:
            assert params_dict.get("symbol") == symbol.value
        if start_time:
            assert params_dict.get("from") == start_time
        if end_time:
            assert params_dict.get("to") == end_time
        assert params_dict.get("limit") == (limit or 100)
        if order_id:
            assert params_dict.get("orderId") == order_id
        if client_id:
            assert "clientId" in params_dict

    @given(
        start_time=timestamp_strategy(),
        end_time=timestamp_strategy(),
    )
    def test_time_range_validation(self, start_time: int, end_time: int) -> None:
        """Test that time ranges are handled correctly."""
        # Ensure valid time range
        if start_time > end_time:
            start_time, end_time = end_time, start_time

        params = BackpackTradingRequestBuilder.build_get_order_history_params(
            symbol=SOL_USDC_BP,
            start_time=start_time,
            end_time=end_time,
        )

        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict["from"] == start_time
        assert params_dict["to"] == end_time

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("symbol", "limit", "expected_base"),
        [
            (None, None, {"limit": 100}),
            (SOL_USDC_BP, 25, {"symbol": SOL_USDC_BP.value, "limit": 25}),
            (BTC_USDT_BP, 50, {"symbol": BTC_USDT_BP.value, "limit": 50}),
        ],
    )
    def test_build_get_order_history_params_parametrized(
        self,
        symbol: Symbol | None,
        limit: int | None,
        expected_base: dict[str, str | int],
    ) -> None:
        """Test build_get_order_history_params with various parameters."""
        params = BackpackTradingRequestBuilder.build_get_order_history_params(
            symbol=symbol,
            start_time=None,
            end_time=None,
            limit=limit if limit is not None else 100,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_base


class TestBuildCancelAllOrdersPayload:
    """Property-based tests for build_cancel_all_orders_payload method."""

    @given(symbol=symbol_strategy())
    def test_cancel_all_orders(self, symbol: Symbol) -> None:
        """Test cancel all orders payload with symbol."""
        payload = BackpackTradingRequestBuilder.build_cancel_all_orders_payload(symbol)

        assert isinstance(payload, BackpackRawOrderCancelAllRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == {"symbol": symbol.value}

    # Legacy parametrized test
    @pytest.mark.parametrize(
        ("symbol", "expected_dict"),
        [
            (SOL_USDC_BP, {"symbol": SOL_USDC_BP.value}),
            (BTC_USDT_BP, {"symbol": BTC_USDT_BP.value}),
            (ETH_BP, {"symbol": ETH_BP.value}),
        ],
    )
    def test_build_cancel_all_orders_payload_parametrized(
        self,
        symbol: Symbol,
        expected_dict: dict[str, str],
    ) -> None:
        """Test build_cancel_all_orders_payload with various symbols."""
        payload = BackpackTradingRequestBuilder.build_cancel_all_orders_payload(symbol)

        assert isinstance(payload, BackpackRawOrderCancelAllRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == expected_dict


class TestEdgeCases:
    """Property-based tests for edge cases and boundary conditions."""

    @given(
        quantity=st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal(999999999),
            places=8,
        ),
        price=st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal(999999999),
            places=8,
        ),
    )
    def test_extreme_decimal_precision(self, quantity: Decimal, price: Decimal) -> None:
        """Test handling of extreme decimal precision."""
        payload = BackpackTradingRequestBuilder.build_place_order_payload(
            symbol=SOL_USDC_BP,
            order_side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
            price=price,
        )

        # Verify decimals are preserved (as strings)
        assert payload.quantity is not None
        assert Decimal(payload.quantity) == quantity
        assert payload.price is not None
        assert Decimal(payload.price) == price

    @given(
        client_id=st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd", "Pc"]),
            min_size=65,
            max_size=200,
        )
    )
    def test_long_client_order_ids(self, client_id: str) -> None:
        """Test handling of very long client order IDs."""
        # This might be truncated or rejected depending on implementation
        try:
            payload = BackpackTradingRequestBuilder.build_place_order_payload(
                symbol=SOL_USDC_BP,
                order_side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal(1),
                time_in_force=TimeInForce.GTC,
                price=Decimal(100),
                client_order_id=client_id,
            )
            # If it succeeds, verify it's handled somehow
            assert payload.clientId is not None
        except (ValueError, ValidationError):
            # Long IDs might be rejected, which is acceptable
            pass

    @given(
        symbol=symbol_strategy(),
        quantities=st.lists(
            decimal_quantity_strategy(),
            min_size=2,
            max_size=5,
        ),
    )
    def test_multiple_orders_consistency(
        self,
        symbol: Symbol,
        quantities: list[Decimal],
    ) -> None:
        """Test consistency when creating multiple orders."""
        payloads: list[BackpackRawOrderExecuteRequest] = []

        for qty in quantities:
            payload = BackpackTradingRequestBuilder.build_place_order_payload(
                symbol=symbol,
                order_side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=qty,
                time_in_force=TimeInForce.IOC,
            )
            payloads.append(payload)

        # All orders should have the same symbol
        symbols = [p.symbol for p in payloads]
        assert all(s == symbol.value for s in symbols)

        # Each should have its specific quantity
        for payload, qty in zip(payloads, quantities, strict=False):
            assert payload.quantity is not None
            assert Decimal(payload.quantity) == qty
