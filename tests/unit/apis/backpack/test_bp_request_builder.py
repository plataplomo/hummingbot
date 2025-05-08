from __future__ import annotations

import unittest
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


class TestBackpackRequestBuilder(unittest.TestCase):
    """Test suite for BackpackRequestBuilder."""

    def test_format_symbol(self) -> None:
        """Test _format_symbol correctly formats various inputs."""
        self.assertEqual(BackpackRequestBuilder.format_symbol("SOL-USDC"), "SOL_USDC")
        self.assertEqual(BackpackRequestBuilder.format_symbol("sol_usdc"), "SOL_USDC")
        self.assertEqual(BackpackRequestBuilder.format_symbol("ETH_PERP"), "ETH_PERP")

    def test_build_get_ticker_params(self) -> None:
        """Test build_get_ticker_params."""
        params = BackpackRequestBuilder.build_get_ticker_params("SOL-USDC")
        self.assertEqual(params, {"symbol": "SOL_USDC"})

    def test_build_get_order_book_params(self) -> None:
        """Test build_get_order_book_params with and without limit."""
        params_no_limit = BackpackRequestBuilder.build_get_order_book_params("BTC_USDT", None)
        self.assertEqual(params_no_limit, {"symbol": "BTC_USDT"})
        params_with_limit = BackpackRequestBuilder.build_get_order_book_params("BTC_USDT", 10)
        self.assertEqual(params_with_limit, {"symbol": "BTC_USDT", "limit": 10})

    def test_build_get_recent_trades_params(self) -> None:
        """Test build_get_recent_trades_params with and without limit."""
        params_no_limit = BackpackRequestBuilder.build_get_recent_trades_params("ETH_USDC", None)
        self.assertEqual(params_no_limit, {"symbol": "ETH_USDC"})
        params_with_limit = BackpackRequestBuilder.build_get_recent_trades_params("ETH_USDC", 50)
        self.assertEqual(params_with_limit, {"symbol": "ETH_USDC", "limit": 50})

    def test_build_get_balances_params(self) -> None:
        """Test build_get_balances_params."""
        params = BackpackRequestBuilder.build_get_balances_params()
        self.assertIsNone(params)

    def test_build_get_positions_params(self) -> None:
        """Test build_get_positions_params."""
        params_no_symbol = BackpackRequestBuilder.build_get_positions_params(None)
        self.assertIsNone(params_no_symbol)
        params_with_symbol = BackpackRequestBuilder.build_get_positions_params("SOL_USDC")
        self.assertIsNone(params_with_symbol)  # Symbol is in path, not params

    def test_build_place_order_payload_limit_gtc(self) -> None:
        """Test build_place_order_payload for a GTC LIMIT order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.5"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("30.25"),
            client_order_id="myLimitOrder1",
            post_only=False,
        )
        expected_payload = {
            "symbol": "SOL_USDC",
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "10.5",
            "timeInForce": "GTC",
            "price": "30.25",
            "clientId": "myLimitOrder1",
        }
        self.assertEqual(payload, expected_payload)

    def test_build_place_order_payload_market_ioc(self) -> None:
        """Test build_place_order_payload for an IOC MARKET order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol="BTC_USDT",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.5"),
            time_in_force=TimeInForce.IOC,
        )
        expected_payload = {
            "symbol": "BTC_USDT",
            "side": "Ask",
            "orderType": "Market",
            "quantity": "0.5",
            "timeInForce": "IOC",
        }
        self.assertEqual(payload, expected_payload)

    def test_build_place_order_payload_limit_post_only(self) -> None:
        """Test build_place_order_payload for a post-only LIMIT order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol="ETH_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,  # TIF GTC with postOnly=true
            price=Decimal("1800.00"),
            post_only=True,
        )
        expected_payload = {
            "symbol": "ETH_USDC",
            "side": "Bid",
            "orderType": "Limit",
            "quantity": "1.0",
            "timeInForce": "GTC",  # PostOnly is a separate flag
            "price": "1800.00",
            "postOnly": True,
        }
        self.assertEqual(payload, expected_payload)

    def test_build_place_order_payload_stop_market(self) -> None:
        """Test build_place_order_payload for a STOP_MARKET order."""
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol="SOL_USDC",
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("5"),
            time_in_force=TimeInForce.GTC,  # Not typically used by stop_market directly
            trigger_price=Decimal("28.00"),
        )
        expected_payload = {
            "symbol": "SOL_USDC",
            "side": "Ask",
            "orderType": "Stop",  # Backpack uses "Stop" for stop market
            "quantity": "5",
            "triggerPrice": "28.00",
        }
        self.assertEqual(payload, expected_payload)

    def test_build_place_order_payload_stop_limit(self) -> None:
        """Test build_place_order_payload for a STOP_LIMIT order."""
        # Assuming Backpack uses orderType "Stop" and then includes a price for limit part
        # Or it might have a distinct orderType like "StopLimit". This needs Backpack doc
        # verification.
        payload = BackpackRequestBuilder.build_place_order_payload(
            symbol="ETH_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("1700"),  # Limit price for the triggered order
            trigger_price=Decimal("1690"),  # Trigger price
        )
        expected_payload: dict[str, Any] = {
            "symbol": "ETH_USDC",
            "side": "Bid",
            "orderType": "Stop",  # Assuming Stop, and price implies the limit part
            "quantity": "0.1",
            "price": "1700",
            "triggerPrice": "1690",
            "timeInForce": "GTC",  # TIF for the limit order part
        }
        self.assertEqual(payload, expected_payload)

    def test_build_place_order_payload_invalid_input(self) -> None:
        """Test build_place_order_payload raises ValueError for invalid inputs."""
        with self.assertRaisesRegex(ValueError, "Price is required for LIMIT orders"):
            BackpackRequestBuilder.build_place_order_payload(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
                price=None,
            )
        with self.assertRaisesRegex(ValueError, "Trigger price is required for STOP_MARKET orders"):
            BackpackRequestBuilder.build_place_order_payload(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_MARKET,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
                trigger_price=None,
            )

    def test_build_cancel_order_params(self) -> None:
        """Test build_cancel_order_params."""
        params_order_id = BackpackRequestBuilder.build_cancel_order_params(
            "SOL_USDC", order_id="12345"
        )
        self.assertEqual(params_order_id, {"symbol": "SOL_USDC", "orderId": "12345"})

        params_client_id = BackpackRequestBuilder.build_cancel_order_params(
            "SOL_USDC", client_order_id="myOrder1"
        )
        self.assertEqual(params_client_id, {"symbol": "SOL_USDC", "clientId": "myOrder1"})

        with self.assertRaisesRegex(ValueError, "Either orderId or clientId must be provided"):
            BackpackRequestBuilder.build_cancel_order_params("SOL_USDC")

    def test_build_get_open_orders_params(self) -> None:
        """Test build_get_open_orders_params."""
        params_no_symbol = BackpackRequestBuilder.build_get_open_orders_params(None)
        self.assertIsNone(params_no_symbol)
        params_with_symbol = BackpackRequestBuilder.build_get_open_orders_params("SOL_USDC")
        self.assertEqual(params_with_symbol, {"symbol": "SOL_USDC"})

    def test_build_get_funding_rate_params(self) -> None:
        """Test build_get_funding_rate_params."""
        params = BackpackRequestBuilder.build_get_funding_rate_params("SOL_PERP")
        self.assertEqual(params, {})  # Symbol in path, no query params

    def test_build_get_account_info_params(self) -> None:
        """Test build_get_account_info_params."""
        params = BackpackRequestBuilder.build_get_account_info_params()
        self.assertIsNone(params)

    def test_build_withdraw_payload(self) -> None:
        """Test build_withdraw_payload with various optional fields."""
        payload_minimal = BackpackRequestBuilder.build_withdraw_payload(
            asset="USDC", amount=Decimal("100"), address="xyzAddress", network="Solana"
        )
        expected_minimal = {
            "blockchain": "Solana",
            "coin": "USDC",
            "quantity": "100",
            "address": "xyzAddress",
        }
        self.assertEqual(payload_minimal, expected_minimal)

        payload_full = BackpackRequestBuilder.build_withdraw_payload(
            asset="ETH",
            amount=Decimal("1.5"),
            address="0x123",
            network="Ethereum",
            tag="myTag",
            client_withdrawal_id="wdId789",
            two_factor_token="123456",
        )
        expected_full = {
            "blockchain": "Ethereum",
            "coin": "ETH",
            "quantity": "1.5",
            "address": "0x123",
            "addressTag": "myTag",
            "clientId": "wdId789",
            "twoFactorToken": "123456",
        }
        self.assertEqual(payload_full, expected_full)

        with self.assertRaisesRegex(ValueError, "Network is required for withdrawals"):
            BackpackRequestBuilder.build_withdraw_payload(
                asset="USDC", amount=Decimal("50"), address="addr1", network=None
            )

    def test_build_get_order_history_params(self) -> None:
        """Test build_get_order_history_params with various filters."""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        params_all = BackpackRequestBuilder.build_get_order_history_params(
            symbol="SOL_USDC",
            start_time_ms=now_ms - 100000,
            end_time_ms=now_ms,
            limit=50,
            order_id="ord123",
        )
        expected_all = {
            "symbol": "SOL_USDC",
            "from": now_ms - 100000,
            "to": now_ms,
            "limit": 50,
            "orderId": "ord123",
        }
        self.assertEqual(params_all, expected_all)

        params_client_id = BackpackRequestBuilder.build_get_order_history_params(
            symbol="ETH_USDC",
            client_order_id="clientOrdX",
            start_time_ms=None,
            end_time_ms=None,
            limit=None,
        )
        self.assertEqual(params_client_id, {"symbol": "ETH_USDC", "clientId": "clientOrdX"})

        params_minimal = BackpackRequestBuilder.build_get_order_history_params(
            symbol=None, start_time_ms=None, end_time_ms=None, limit=None
        )
        self.assertEqual(params_minimal, {})

    def test_build_get_trade_history_params(self) -> None:
        """Test build_get_trade_history_params (fills)."""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        params = BackpackRequestBuilder.build_get_trade_history_params(
            symbol="BTC_USDT",
            limit=25,
            start_time_ms=now_ms - 200000,
            end_time_ms=None,
            from_id="fillIdStart",
        )
        expected = {
            "symbol": "BTC_USDT",
            "limit": 25,
            "from": now_ms - 200000,
            "fromId": "fillIdStart",
        }
        self.assertEqual(params, expected)

    def test_build_get_market_data_params(self) -> None:
        """Test build_get_market_data_params (klines)."""
        params = BackpackRequestBuilder.build_get_market_data_params(
            symbol="SOL_USDC", timeframe_str="1h", limit=100, start_time_ms=None, end_time_ms=None
        )
        expected = {"symbol": "SOL_USDC", "interval": "1h", "limit": 100}
        self.assertEqual(params, expected)

    def test_build_get_historical_trades_params(self) -> None:
        """Test build_get_historical_trades_params."""
        params = BackpackRequestBuilder.build_get_historical_trades_params(
            symbol="ETH_PERP", limit=50, from_id="trade123"
        )
        expected = {"symbol": "ETH_PERP", "limit": 50, "fromId": "trade123"}
        self.assertEqual(params, expected)

    def test_build_cancel_all_orders_payload(self) -> None:
        """Test build_cancel_all_orders_payload."""
        # Assuming query params for now if symbol is provided
        params_with_symbol = BackpackRequestBuilder.build_cancel_all_orders_payload("SOL_USDC")
        self.assertEqual(params_with_symbol, {"symbol": "SOL_USDC"})
        params_no_symbol = BackpackRequestBuilder.build_cancel_all_orders_payload(None)
        self.assertIsNone(params_no_symbol)

    def test_build_get_order_params(self) -> None:
        """Test build_get_order_params (for GET /orders/{id})."""
        params = BackpackRequestBuilder.build_get_order_params()
        self.assertIsNone(params)  # No query params expected


if __name__ == "__main__":
    unittest.main()
