from __future__ import annotations

import unittest
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce


class TestHyperliquidRequestBuilder(unittest.TestCase):
    """Test suite for HyperliquidRequestBuilder."""

    @classmethod
    def setUpClass(cls) -> None:
        # A valid, but not necessarily real, 42-character address
        cls.VALID_ADDRESS = "0xAbCDeF0123456789AbCDeF0123456789AbCDeF01"

    def test_build_info_request_payload(self) -> None:
        """Test build_info_request_payload."""
        payload = HyperliquidRequestBuilder.build_info_request_payload()
        self.assertIsNone(payload, "Expected None for general info payload")

    def test_build_l2_usd_transfer_payload(self) -> None:
        """Test build_l2_usd_transfer_payload with valid inputs."""
        payload = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address=self.VALID_ADDRESS, amount=Decimal("100.50")
        )
        expected_payload = {
            "type": "usdTransfer",
            "action": {
                "chain": "L2",
                "payload": {
                    "destination": self.VALID_ADDRESS,
                    "token": "USDC",
                    "amount": "100.50",
                },
            },
        }
        self.assertEqual(payload, expected_payload)

    def test_build_l2_usd_transfer_payload_invalid_input(self) -> None:
        """Test build_l2_usd_transfer_payload with invalid (empty) address."""
        with self.assertRaisesRegex(
            ValueError, "Destination address .* required for Hyperliquid L2 transfer"
        ):
            HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
                destination_address="", amount=Decimal("100")
            )

    def test_build_withdrawal_payload_eth(self) -> None:
        """Test build_withdrawal_payload for ETH."""
        payload = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="ETH", amount=Decimal("1.23"), destination_address="0xabc"
        )
        expected_payload = {
            "type": "withdrawEth",
            "action": {"amount": "1.23", "destination": "0xabc"},
        }
        self.assertEqual(payload, expected_payload)

    def test_build_withdrawal_payload_token(self) -> None:
        """Test build_withdrawal_payload for a generic token (USDC)."""
        payload = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset="USDC", amount=Decimal("500"), destination_address=self.VALID_ADDRESS
        )
        expected_payload = {
            "type": "withdraw",
            "action": {
                "token": "USDC",
                "amount": "500",
                "destination": self.VALID_ADDRESS,
            },
        }
        self.assertEqual(payload, expected_payload)

    def test_build_withdrawal_payload_invalid_input(self) -> None:
        """Test build_withdrawal_payload with invalid (empty) address."""
        with self.assertRaisesRegex(ValueError, "Destination address is required for withdrawal"):
            HyperliquidRequestBuilder.build_withdrawal_payload(
                asset="USDC", amount=Decimal("100"), destination_address=""
            )

    def test_build_order_history_payload(self) -> None:
        """Test build_order_history_payload."""
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 2, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        payload = HyperliquidRequestBuilder.build_order_history_payload(
            wallet_address="0xuser",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        expected_payload = {
            "type": "queryOrderHistory",
            "user": "0xuser",
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        self.assertEqual(payload, expected_payload)

    def test_build_candle_snapshot_payload(self) -> None:
        """Test build_candle_snapshot_payload."""
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
        payload = HyperliquidRequestBuilder.build_candle_snapshot_payload(
            symbol="ETH-PERP",
            timeframe="1h",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        expected_payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": "ETH-PERP",
                "interval": "1h",
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        self.assertEqual(payload, expected_payload)

    def test_build_place_order_payload_limit_gtc(self) -> None:
        """Test build_place_order_payload for a GTC LIMIT order."""
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=0,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.5"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2000.50"),
            client_order_id="clOrd123",
            reduce_only=False,
        )
        expected_action = {
            "asset": 0,
            "isBuy": True,
            "sz": "1.5",
            "limitPx": "2000.50",
            "orderType": {"limit": {"tif": "Gtc"}},
            "reduceOnly": False,
            "cloid": "clOrd123",
        }
        self.assertEqual(payload, {"type": "order", "actions": [expected_action]})

    def test_build_place_order_payload_market(self) -> None:
        """Test build_place_order_payload for a MARKET order."""
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=1,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            time_in_force=TimeInForce.IOC,  # Market orders often IOC
            reduce_only=True,
        )
        expected_action: dict[str, Any] = {
            "asset": 1,
            "isBuy": False,
            "sz": "10",
            "limitPx": "0",  # Market orders have limitPx 0
            "orderType": {"market": {}},
            "reduceOnly": True,
        }
        # IOC for market is handled by orderType: market, not TIF in limit
        self.assertEqual(payload, {"type": "order", "actions": [expected_action]})

    def test_build_place_order_payload_limit_alo_post_only(self) -> None:
        """Test build_place_order_payload for ALO LIMIT order (post_only=True)."""
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=0,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,  # TIF might be GTC but post_only makes it ALO
            price=Decimal("2100"),
            post_only=True,
        )
        expected_action = {
            "asset": 0,
            "isBuy": True,
            "sz": "1.0",
            "limitPx": "2100",
            "orderType": {"limit": {"tif": "Alo"}},  # ALO due to post_only
            "reduceOnly": False,
        }
        self.assertEqual(payload, {"type": "order", "actions": [expected_action]})

    def test_build_place_order_payload_stop_market(self) -> None:
        """Test build_place_order_payload for a STOP_MARKET order."""
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=2,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=Decimal("0.5"),
            time_in_force=TimeInForce.GTC,  # TIF for the underlying part
            stop_price=Decimal("1900"),
        )
        expected_action = {
            "asset": 2,
            "isBuy": False,
            "sz": "0.5",
            "limitPx": "0",  # Stop Market triggers into a market order
            "orderType": {"limit": {"tif": "Gtc"}},  # Base order type
            "reduceOnly": False,
            "trigger": {"triggerPx": "1900", "isMarket": True, "tpsl": "sl"},
        }
        self.assertEqual(payload, {"type": "order", "actions": [expected_action]})

    def test_build_place_order_payload_stop_limit(self) -> None:
        """Test build_place_order_payload for a STOP_LIMIT order."""
        payload = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=3,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=Decimal("2"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("2200"),  # Limit price after trigger
            stop_price=Decimal("2150"),  # Trigger price
        )
        expected_action = {
            "asset": 3,
            "isBuy": True,
            "sz": "2",
            "limitPx": "2200",
            "orderType": {"limit": {"tif": "Gtc"}},
            "reduceOnly": False,
            "trigger": {"triggerPx": "2150", "isMarket": False, "tpsl": "sl"},
        }
        self.assertEqual(payload, {"type": "order", "actions": [expected_action]})

    def test_build_place_order_invalid_params(self) -> None:
        """Test build_place_order_payload with missing price for LIMIT order."""
        with self.assertRaisesRegex(ValueError, "Price is required for LIMIT orders"):
            HyperliquidRequestBuilder.build_place_order_payload(
                asset_index=0,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )
        with self.assertRaisesRegex(ValueError, "stop_price is required for STOP_MARKET orders"):
            HyperliquidRequestBuilder.build_place_order_payload(
                asset_index=0,
                side=OrderSide.BUY,
                order_type=OrderType.STOP_MARKET,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )

    def test_build_cancel_order_payload(self) -> None:
        """Test build_cancel_order_payload."""
        payload = HyperliquidRequestBuilder.build_cancel_order_payload(
            asset_index=0, order_id=12345
        )
        expected_payload = {
            "type": "cancel",
            "action": {"asset": 0, "oid": 12345},
        }
        self.assertEqual(payload, expected_payload)

    def test_build_order_status_payload(self) -> None:
        """Test build_order_status_payload."""
        payload = HyperliquidRequestBuilder.build_order_status_payload(
            wallet_address="0xuser", order_id=67890
        )
        expected_payload = {
            "type": "orderStatus",
            "user": "0xuser",
            "oid": 67890,
        }
        self.assertEqual(payload, expected_payload)


if __name__ == "__main__":
    unittest.main()
