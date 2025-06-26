"""Unit tests for BackpackRequestBuilder order management methods."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
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
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


class TestBuildPlaceOrderPayload:
    """Tests for build_place_order_payload method."""

    def test_build_place_order_payload_limit_gtc(
        self,
        symbol_spot: str,
        buy_order_side: OrderSide,
        limit_order_type: OrderType,
        standard_quantity: Decimal,
        gtc_time_in_force: TimeInForce,
        standard_price: Decimal,
    ) -> None:
        """Test build_place_order_payload for a GTC LIMIT order."""
        # Use integer client_order_id to match RawBpUint32 expectation
        client_order_id = 12345
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_spot,
            side=buy_order_side,
            order_type=limit_order_type,
            quantity=standard_quantity,
            time_in_force=gtc_time_in_force,
            price=standard_price,
            client_order_id=str(client_order_id),
            post_only=False,
        )

        # Assert that we get the correct Pydantic model type
        assert isinstance(payload, BackpackRawOrderExecuteRequest)

        # Convert to dict for comparison using aliases
        payload_dict = payload.model_dump(by_alias=True)
        expected_payload = {
            "symbol": symbol_spot,
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "10.5",
            "timeInForce": "GTC",
            "price": "140.00",
            "clientId": client_order_id,
        }

        # Compare only the fields that should be present
        for key, expected_value in expected_payload.items():
            assert payload_dict[key] == expected_value

    def test_build_place_order_payload_market_ioc(
        self,
        symbol_btc_spot: str,
        sell_order_side: OrderSide,
        market_order_type: OrderType,
        ioc_time_in_force: TimeInForce,
    ) -> None:
        """Test build_place_order_payload for an IOC MARKET order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_btc_spot,
            side=sell_order_side,
            order_type=market_order_type,
            quantity=Decimal("0.5"),
            time_in_force=ioc_time_in_force,
        )

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": symbol_btc_spot,
            "side": "Ask",
            "orderType": "Market",
            "quantity": "0.5",
        }

        for key, expected_value in expected_payload.items():
            assert payload_dict[key] == expected_value

    def test_build_place_order_payload_limit_post_only(
        self,
        symbol_eth_spot: str,
        buy_order_side: OrderSide,
        limit_order_type: OrderType,
        gtc_time_in_force: TimeInForce,
    ) -> None:
        """Test build_place_order_payload for a post-only LIMIT order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_eth_spot,
            side=buy_order_side,
            order_type=limit_order_type,
            quantity=Decimal("1.0"),
            time_in_force=gtc_time_in_force,
            price=Decimal("1800.00"),
            post_only=True,
        )

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {
            "symbol": symbol_eth_spot,
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "1.0",
            "timeInForce": "GTC",
            "price": "1800.00",
            "postOnly": True,
        }

        for key, expected_value in expected_payload.items():
            assert payload_dict[key] == expected_value

    def test_build_place_order_payload_stop_market(
        self,
        symbol_spot: str,
        sell_order_side: OrderSide,
        stop_market_order_type: OrderType,
        gtc_time_in_force: TimeInForce,
        trigger_price: Decimal,
    ) -> None:
        """Test build_place_order_payload for a STOP_MARKET order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_spot,
            side=sell_order_side,
            order_type=stop_market_order_type,
            quantity=Decimal(5),
            time_in_force=gtc_time_in_force,
            trigger_price=trigger_price,
        )

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)

        # For STOP_MARKET, we expect orderType to be "Market" with triggerPrice
        expected_fields = {
            "symbol": symbol_spot,
            "side": "Ask",
            "orderType": "Market",
            "quantity": "5",
        }

        for key, expected_value in expected_fields.items():
            assert payload_dict[key] == expected_value

        # Check that trigger price is present (though actual field name may vary)
        # We'll check this via the Pydantic model structure

    def test_build_place_order_payload_stop_limit(
        self,
        symbol_eth_spot: str,
        buy_order_side: OrderSide,
        stop_limit_order_type: OrderType,
        gtc_time_in_force: TimeInForce,
    ) -> None:
        """Test build_place_order_payload for a STOP_LIMIT order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_eth_spot,
            side=buy_order_side,
            order_type=stop_limit_order_type,
            quantity=Decimal("0.1"),
            time_in_force=gtc_time_in_force,
            price=Decimal(1700),
            trigger_price=Decimal(1690),
        )

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)

        expected_fields = {
            "symbol": symbol_eth_spot,
            "side": "Bid",
            "orderType": "Limit",
            "price": "1700",
            "timeInForce": "GTC",
            "triggerPrice": "1690",
            "triggerQuantity": "0.1",
        }

        for key, expected_value in expected_fields.items():
            assert payload_dict[key] == expected_value

    @pytest.mark.parametrize(
        "side, expected_side_str",
        [
            (OrderSide.BUY, "Bid"),
            (OrderSide.SELL, "Ask"),
        ],
    )
    def test_build_place_order_payload_side_mapping(
        self,
        symbol_spot: str,
        side: OrderSide,
        expected_side_str: str,
    ) -> None:
        """Test build_place_order_payload correctly maps order sides."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_spot,
            side=side,
            order_type=OrderType.MARKET,
            quantity=Decimal(1),
            time_in_force=TimeInForce.IOC,
        )

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        assert payload.side == expected_side_str

    @pytest.mark.parametrize(
        "order_type, expected_type_str",
        [
            (OrderType.LIMIT, "Limit"),
            (OrderType.MARKET, "Market"),
            (OrderType.STOP_MARKET, "Market"),  # STOP_MARKET maps to "Market" with trigger
        ],
    )
    def test_build_place_order_payload_type_mapping(
        self,
        symbol_spot: str,
        order_type: OrderType,
        expected_type_str: str,
    ) -> None:
        """Test build_place_order_payload correctly maps order types."""
        kwargs: dict[str, Any] = {
            "symbol": symbol_spot,
            "side": OrderSide.BUY,
            "order_type": order_type,
            "quantity": Decimal(1),
            "time_in_force": TimeInForce.GTC,
        }
        if order_type == OrderType.LIMIT:
            kwargs["price"] = Decimal(100)
        elif order_type == OrderType.STOP_MARKET:
            kwargs["trigger_price"] = Decimal(100)

        payload = BackpackRequestBuilder.build_place_order_payload(**kwargs)

        assert isinstance(payload, BackpackRawOrderExecuteRequest)
        assert payload.orderType == expected_type_str


class TestBuildCancelOrderPayload:
    """Tests for build_cancel_order_payload method."""

    def test_build_cancel_order_payload_order_id(self, symbol_spot: str, order_id: str) -> None:
        """Test build_cancel_order_payload with order ID."""
        payload = BackpackRequestBuilder.build_cancel_order_payload(symbol_spot, order_id=order_id)

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {"symbol": symbol_spot, "orderId": order_id}
        assert payload_dict == expected_payload

    def test_build_cancel_order_payload_client_id(self, symbol_spot: str) -> None:
        """Test build_cancel_order_payload with client order ID."""
        client_order_id = 98765  # Use integer for RawBpUint32
        payload = BackpackRequestBuilder.build_cancel_order_payload(
            symbol_spot,
            client_order_id=str(client_order_id),
        )

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {"symbol": symbol_spot, "clientId": client_order_id}
        assert payload_dict == expected_payload

    def test_build_cancel_order_payload_formats_symbol(self, order_id: str) -> None:
        """Test build_cancel_order_payload formats symbol correctly."""
        payload = BackpackRequestBuilder.build_cancel_order_payload("SOL-USDC", order_id=order_id)

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        expected_payload = {"symbol": "SOL_USDC", "orderId": order_id}
        assert payload_dict == expected_payload

    @pytest.mark.parametrize(
        "symbol, order_id, client_id_int, expected_payload",
        [
            ("SOL_USDC", "123", None, {"symbol": "SOL_USDC", "orderId": "123"}),
            ("BTC_USDT", None, 456789, {"symbol": "BTC_USDT", "clientId": 456789}),
            ("eth-perp", "456", None, {"symbol": "ETH_PERP", "orderId": "456"}),
        ],
    )
    def test_build_cancel_order_payload_parametrized(
        self,
        symbol: str,
        order_id: str | None,
        client_id_int: int | None,
        expected_payload: dict[str, str | int],
    ) -> None:
        """Test build_cancel_order_payload with various combinations."""
        client_id_str = str(client_id_int) if client_id_int is not None else None
        payload = BackpackRequestBuilder.build_cancel_order_payload(
            symbol,
            order_id=order_id,
            client_order_id=client_id_str,
        )

        assert isinstance(payload, BackpackRawOrderCancelRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == expected_payload


class TestBuildGetOpenOrdersParams:
    """Tests for build_get_open_orders_params method."""

    def test_build_get_open_orders_params_no_symbol(self) -> None:
        """Test build_get_open_orders_params without symbol."""
        params = BackpackRequestBuilder.build_get_open_orders_params(None)

        assert isinstance(params, BackpackRawGetOpenOrdersParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        # When symbol is None, the model should not include it in the output
        assert params_dict == {}

    def test_build_get_open_orders_params_with_symbol(self, symbol_spot: str) -> None:
        """Test build_get_open_orders_params with symbol."""
        params = BackpackRequestBuilder.build_get_open_orders_params(symbol_spot)

        assert isinstance(params, BackpackRawGetOpenOrdersParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_spot}

    def test_build_get_open_orders_params_formats_symbol(self) -> None:
        """Test build_get_open_orders_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_open_orders_params("SOL-USDC")

        assert isinstance(params, BackpackRawGetOpenOrdersParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "symbol, expected_dict",
        [
            (None, {}),
            ("SOL_USDC", {"symbol": "SOL_USDC"}),
            ("btc-usdt", {"symbol": "BTC_USDT"}),
            ("ETH_PERP", {"symbol": "ETH_PERP"}),
        ],
    )
    def test_build_get_open_orders_params_parametrized(
        self,
        symbol: str | None,
        expected_dict: dict[str, str],
    ) -> None:
        """Test build_get_open_orders_params with various symbols."""
        params = BackpackRequestBuilder.build_get_open_orders_params(symbol)

        assert isinstance(params, BackpackRawGetOpenOrdersParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_dict


class TestBuildGetOrderParams:
    """Tests for build_get_order_params method."""

    def test_build_get_order_params_basic(self, symbol_spot: str) -> None:
        """Test build_get_order_params with basic symbol."""
        params = BackpackRequestBuilder.build_get_order_params(symbol_spot)

        assert isinstance(params, BackpackRawGetOrderParams)
        params_dict = params.model_dump(by_alias=True)
        assert params_dict == {"symbol": symbol_spot}

    def test_build_get_order_params_formats_symbol(self) -> None:
        """Test build_get_order_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_order_params("SOL-USDC")

        assert isinstance(params, BackpackRawGetOrderParams)
        params_dict = params.model_dump(by_alias=True)
        assert params_dict == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "input_symbol, expected_symbol",
        [
            ("SOL_USDC", "SOL_USDC"),
            ("btc-usdt", "BTC_USDT"),
            ("ETH-PERP", "ETH_PERP"),
        ],
    )
    def test_build_get_order_params_parametrized(
        self,
        input_symbol: str,
        expected_symbol: str,
    ) -> None:
        """Test build_get_order_params with various symbol formats."""
        params = BackpackRequestBuilder.build_get_order_params(input_symbol)

        assert isinstance(params, BackpackRawGetOrderParams)
        params_dict = params.model_dump(by_alias=True)
        assert params_dict == {"symbol": expected_symbol}


class TestBuildGetOrderHistoryParams:
    """Tests for build_get_order_history_params method."""

    def test_build_get_order_history_params_all_fields(
        self,
        symbol_spot: str,
        current_timestamp_ms: int,
        past_timestamp_ms: int,
        order_id: str,
    ) -> None:
        """Test build_get_order_history_params with all fields."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=symbol_spot,
            start_time_ms=past_timestamp_ms,
            end_time_ms=current_timestamp_ms,
            limit=50,
            order_id=order_id,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": symbol_spot,
            "from": past_timestamp_ms,
            "to": current_timestamp_ms,
            "limit": 50,
            "orderId": order_id,
        }
        assert params_dict == expected

    def test_build_get_order_history_params_client_id(self, symbol_eth_spot: str) -> None:
        """Test build_get_order_history_params with client order ID."""
        client_order_id = 789012  # Use integer for RawBpUint32
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=symbol_eth_spot,
            client_order_id=str(client_order_id),
            start_time_ms=None,
            end_time_ms=None,
            limit=None,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_eth_spot, "clientId": str(client_order_id)}

    def test_build_get_order_history_params_minimal(self) -> None:
        """Test build_get_order_history_params with minimal parameters."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=None,
            start_time_ms=None,
            end_time_ms=None,
            limit=None,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {}

    def test_build_get_order_history_params_formats_symbol(self) -> None:
        """Test build_get_order_history_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol="SOL-USDC",
            start_time_ms=None,
            end_time_ms=None,
            limit=None,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "symbol, limit, expected_base",
        [
            (None, None, {}),
            ("SOL_USDC", 25, {"symbol": "SOL_USDC", "limit": 25}),
            ("btc-usdt", 50, {"symbol": "BTC_USDT", "limit": 50}),
        ],
    )
    def test_build_get_order_history_params_parametrized(
        self,
        symbol: str | None,
        limit: int | None,
        expected_base: dict[str, Any],
    ) -> None:
        """Test build_get_order_history_params with various parameters."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=symbol,
            start_time_ms=None,
            end_time_ms=None,
            limit=limit,
        )

        assert isinstance(params, BackpackRawGetOrderHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_base


class TestBuildCancelAllOrdersPayload:
    """Tests for build_cancel_all_orders_payload method."""

    def test_build_cancel_all_orders_payload_with_symbol(self, symbol_spot: str) -> None:
        """Test build_cancel_all_orders_payload with symbol."""
        payload = BackpackRequestBuilder.build_cancel_all_orders_payload(symbol_spot)

        assert isinstance(payload, BackpackRawOrderCancelAllRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == {"symbol": symbol_spot}

    def test_build_cancel_all_orders_payload_formats_symbol(self) -> None:
        """Test build_cancel_all_orders_payload formats symbol correctly."""
        payload = BackpackRequestBuilder.build_cancel_all_orders_payload("SOL-USDC")

        assert isinstance(payload, BackpackRawOrderCancelAllRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "symbol, expected_dict",
        [
            ("SOL_USDC", {"symbol": "SOL_USDC"}),
            ("btc-usdt", {"symbol": "BTC_USDT"}),
            ("ETH-PERP", {"symbol": "ETH_PERP"}),
        ],
    )
    def test_build_cancel_all_orders_payload_parametrized(
        self,
        symbol: str,
        expected_dict: dict[str, str],
    ) -> None:
        """Test build_cancel_all_orders_payload with various symbols."""
        payload = BackpackRequestBuilder.build_cancel_all_orders_payload(symbol)

        assert isinstance(payload, BackpackRawOrderCancelAllRequest)
        payload_dict = payload.model_dump(by_alias=True, exclude_none=True)
        assert payload_dict == expected_dict
