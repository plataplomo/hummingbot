"""Unit tests for BackpackRequestBuilder order management methods."""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
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
        client_order_id: str,
    ) -> None:
        """Test build_place_order_payload for a GTC LIMIT order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_spot,
            side=buy_order_side,
            order_type=limit_order_type,
            quantity=standard_quantity,
            time_in_force=gtc_time_in_force,
            price=standard_price,
            client_order_id=client_order_id,
            post_only=False,
        )
        expected_payload = {
            "symbol": symbol_spot,
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "10.5",
            "timeInForce": "GTC",
            "price": "140.00",
            "clientId": client_order_id,
        }
        assert payload == expected_payload

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
        expected_payload = {
            "symbol": symbol_btc_spot,
            "side": "Ask",
            "orderType": "Market",
            "quantity": "0.5",
        }
        assert payload == expected_payload

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
        expected_payload = {
            "symbol": symbol_eth_spot,
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "1.0",
            "timeInForce": "GTC",
            "price": "1800.00",
            "postOnly": True,
        }
        assert payload == expected_payload

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
            quantity=Decimal("5"),
            time_in_force=gtc_time_in_force,
            trigger_price=trigger_price,
        )
        expected_payload = {
            "symbol": symbol_spot,
            "side": "Ask",
            "orderType": "Stop",
            "quantity": "5",
            "triggerPrice": "28.00",
        }
        assert payload == expected_payload

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
            price=Decimal("1700"),
            trigger_price=Decimal("1690"),
        )
        expected_payload: dict[str, Any] = {
            "symbol": symbol_eth_spot,
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "0.1",
            "price": "1700",
            "triggerPrice": "1690",
            "timeInForce": "GTC",
        }
        assert payload == expected_payload

    def test_build_place_order_payload_invalid_limit_no_price(
        self,
        symbol_spot: str,
        buy_order_side: OrderSide,
        limit_order_type: OrderType,
        gtc_time_in_force: TimeInForce,
    ) -> None:
        """Test build_place_order_payload raises ValueError for LIMIT order without price."""
        with pytest.raises(ValueError, match="Price is required for LIMIT orders"):
            BackpackRequestBuilder.build_place_order_payload(
                symbol=symbol_spot,
                side=buy_order_side,
                order_type=limit_order_type,
                quantity=Decimal("1"),
                time_in_force=gtc_time_in_force,
                price=None,
            )

    def test_build_place_order_payload_invalid_stop_market_no_trigger(
        self,
        symbol_spot: str,
        buy_order_side: OrderSide,
        stop_market_order_type: OrderType,
        gtc_time_in_force: TimeInForce,
    ) -> None:
        """Test build_place_order_payload raises ValueError for STOP_MARKET.

        Tests that ValueError is raised when trigger price is not provided.
        """
        with pytest.raises(ValueError, match="Trigger price is required for STOP_MARKET orders"):
            BackpackRequestBuilder.build_place_order_payload(
                symbol=symbol_spot,
                side=buy_order_side,
                order_type=stop_market_order_type,
                quantity=Decimal("1"),
                time_in_force=gtc_time_in_force,
                trigger_price=None,
            )

    @pytest.mark.parametrize(
        "side, expected_side_str",
        [
            (OrderSide.BUY, "Bid"),
            (OrderSide.SELL, "Ask"),
        ],
    )
    def test_build_place_order_payload_side_mapping(
        self, symbol_spot: str, side: OrderSide, expected_side_str: str
    ) -> None:
        """Test build_place_order_payload correctly maps order sides."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol=symbol_spot,
            side=side,
            order_type=OrderType.MARKET,
            quantity=Decimal("1"),
            time_in_force=TimeInForce.IOC,
        )
        assert payload["side"] == expected_side_str

    @pytest.mark.parametrize(
        "order_type, expected_type_str",
        [
            (OrderType.LIMIT, "Limit"),
            (OrderType.MARKET, "Market"),
            (OrderType.STOP_MARKET, "Stop"),
        ],
    )
    def test_build_place_order_payload_type_mapping(
        self, symbol_spot: str, order_type: OrderType, expected_type_str: str
    ) -> None:
        """Test build_place_order_payload correctly maps order types."""
        kwargs: dict[str, Any] = {
            "symbol": symbol_spot,
            "side": OrderSide.BUY,
            "order_type": order_type,
            "quantity": Decimal("1"),
            "time_in_force": TimeInForce.GTC,
        }
        if order_type == OrderType.LIMIT:
            kwargs["price"] = Decimal("100")
        elif order_type == OrderType.STOP_MARKET:
            kwargs["trigger_price"] = Decimal("100")

        payload = BackpackRequestBuilder.build_place_order_payload(**kwargs)
        assert payload["orderType"] == expected_type_str


class TestBuildCancelOrderPayload:
    """Tests for build_cancel_order_payload method."""

    def test_build_cancel_order_payload_order_id(self, symbol_spot: str, order_id: str) -> None:
        """Test build_cancel_order_payload with order ID."""
        payload = BackpackRequestBuilder.build_cancel_order_payload(symbol_spot, order_id=order_id)
        assert payload == {"symbol": symbol_spot, "orderId": order_id}

    def test_build_cancel_order_payload_client_id(
        self, symbol_spot: str, client_order_id: str
    ) -> None:
        """Test build_cancel_order_payload with client order ID."""
        payload = BackpackRequestBuilder.build_cancel_order_payload(
            symbol_spot, client_order_id=client_order_id
        )
        assert payload == {"symbol": symbol_spot, "clientId": client_order_id}

    def test_build_cancel_order_payload_no_identifiers(self, symbol_spot: str) -> None:
        """Test build_cancel_order_payload raises ValueError when no identifiers provided."""
        with pytest.raises(ValueError, match="Either orderId or clientId must be provided"):
            BackpackRequestBuilder.build_cancel_order_payload(symbol_spot)

    def test_build_cancel_order_payload_formats_symbol(self, order_id: str) -> None:
        """Test build_cancel_order_payload formats symbol correctly."""
        payload = BackpackRequestBuilder.build_cancel_order_payload("SOL-USDC", order_id=order_id)
        assert payload == {"symbol": "SOL_USDC", "orderId": order_id}

    @pytest.mark.parametrize(
        "symbol, order_id, client_id, expected_payload",
        [
            ("SOL_USDC", "123", None, {"symbol": "SOL_USDC", "orderId": "123"}),
            ("BTC_USDT", None, "client123", {"symbol": "BTC_USDT", "clientId": "client123"}),
            ("eth-perp", "456", None, {"symbol": "ETH_PERP", "orderId": "456"}),
        ],
    )
    def test_build_cancel_order_payload_parametrized(
        self,
        symbol: str,
        order_id: str | None,
        client_id: str | None,
        expected_payload: dict[str, str],
    ) -> None:
        """Test build_cancel_order_payload with various combinations."""
        payload = BackpackRequestBuilder.build_cancel_order_payload(
            symbol, order_id=order_id, client_order_id=client_id
        )
        assert payload == expected_payload


class TestBuildGetOpenOrdersParams:
    """Tests for build_get_open_orders_params method."""

    def test_build_get_open_orders_params_no_symbol(self) -> None:
        """Test build_get_open_orders_params without symbol."""
        params = BackpackRequestBuilder.build_get_open_orders_params(None)
        assert params is None

    def test_build_get_open_orders_params_with_symbol(self, symbol_spot: str) -> None:
        """Test build_get_open_orders_params with symbol."""
        params = BackpackRequestBuilder.build_get_open_orders_params(symbol_spot)
        assert params == {"symbol": symbol_spot}

    def test_build_get_open_orders_params_formats_symbol(self) -> None:
        """Test build_get_open_orders_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_open_orders_params("SOL-USDC")
        assert params == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "symbol, expected_params",
        [
            (None, None),
            ("SOL_USDC", {"symbol": "SOL_USDC"}),
            ("btc-usdt", {"symbol": "BTC_USDT"}),
            ("ETH_PERP", {"symbol": "ETH_PERP"}),
        ],
    )
    def test_build_get_open_orders_params_parametrized(
        self, symbol: str | None, expected_params: dict[str, str] | None
    ) -> None:
        """Test build_get_open_orders_params with various symbols."""
        params = BackpackRequestBuilder.build_get_open_orders_params(symbol)
        assert params == expected_params


class TestBuildGetOrderParams:
    """Tests for build_get_order_params method."""

    def test_build_get_order_params_basic(self, symbol_spot: str) -> None:
        """Test build_get_order_params with basic symbol."""
        params = BackpackRequestBuilder.build_get_order_params(symbol_spot)
        assert params == {"symbol": symbol_spot}

    def test_build_get_order_params_formats_symbol(self) -> None:
        """Test build_get_order_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_order_params("SOL-USDC")
        assert params == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "input_symbol, expected_symbol",
        [
            ("SOL_USDC", "SOL_USDC"),
            ("btc-usdt", "BTC_USDT"),
            ("ETH-PERP", "ETH_PERP"),
        ],
    )
    def test_build_get_order_params_parametrized(
        self, input_symbol: str, expected_symbol: str
    ) -> None:
        """Test build_get_order_params with various symbol formats."""
        params = BackpackRequestBuilder.build_get_order_params(input_symbol)
        assert params == {"symbol": expected_symbol}


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
        expected = {
            "symbol": symbol_spot,
            "from": past_timestamp_ms,
            "to": current_timestamp_ms,
            "limit": 50,
            "orderId": order_id,
        }
        assert params == expected

    def test_build_get_order_history_params_client_id(
        self, symbol_eth_spot: str, client_order_id: str
    ) -> None:
        """Test build_get_order_history_params with client order ID."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=symbol_eth_spot,
            client_order_id=client_order_id,
            start_time_ms=None,
            end_time_ms=None,
            limit=None,
        )
        assert params == {"symbol": symbol_eth_spot, "clientId": client_order_id}

    def test_build_get_order_history_params_minimal(self) -> None:
        """Test build_get_order_history_params with minimal parameters."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=None, start_time_ms=None, end_time_ms=None, limit=None
        )
        assert params == {}

    def test_build_get_order_history_params_formats_symbol(self) -> None:
        """Test build_get_order_history_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol="SOL-USDC", start_time_ms=None, end_time_ms=None, limit=100
        )
        assert params == {"symbol": "SOL_USDC", "limit": 100}

    @pytest.mark.parametrize(
        "symbol, limit, expected_base",
        [
            (None, None, {}),
            ("SOL_USDC", 25, {"symbol": "SOL_USDC", "limit": 25}),
            ("btc-usdt", 50, {"symbol": "BTC_USDT", "limit": 50}),
        ],
    )
    def test_build_get_order_history_params_parametrized(
        self, symbol: str | None, limit: int | None, expected_base: dict[str, Any]
    ) -> None:
        """Test build_get_order_history_params with various combinations."""
        params = BackpackRequestBuilder.build_get_order_history_params(
            symbol=symbol, start_time_ms=None, end_time_ms=None, limit=limit
        )
        assert params == expected_base


class TestBuildCancelAllOrdersPayload:
    """Tests for build_cancel_all_orders_payload method."""

    def test_build_cancel_all_orders_payload_with_symbol(self, symbol_spot: str) -> None:
        """Test build_cancel_all_orders_payload with symbol."""
        params = BackpackRequestBuilder.build_cancel_all_orders_payload(symbol_spot)
        assert params == {"symbol": symbol_spot}

    def test_build_cancel_all_orders_payload_no_symbol(self) -> None:
        """Test build_cancel_all_orders_payload without symbol."""
        params = BackpackRequestBuilder.build_cancel_all_orders_payload(None)
        assert params is None

    def test_build_cancel_all_orders_payload_formats_symbol(self) -> None:
        """Test build_cancel_all_orders_payload formats symbol correctly."""
        params = BackpackRequestBuilder.build_cancel_all_orders_payload("SOL-USDC")
        assert params == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "symbol, expected_params",
        [
            (None, None),
            ("SOL_USDC", {"symbol": "SOL_USDC"}),
            ("btc-usdt", {"symbol": "BTC_USDT"}),
            ("ETH-PERP", {"symbol": "ETH_PERP"}),
        ],
    )
    def test_build_cancel_all_orders_payload_parametrized(
        self, symbol: str | None, expected_params: dict[str, str] | None
    ) -> None:
        """Test build_cancel_all_orders_payload with various symbols."""
        params = BackpackRequestBuilder.build_cancel_all_orders_payload(symbol)
        assert params == expected_params
