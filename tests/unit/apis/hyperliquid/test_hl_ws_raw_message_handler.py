"""
Unit tests for HyperliquidWsRawMessageHandler.
"""

import unittest

# Removed: from decimal import Decimal
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
)  # For user order data
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
    # Removed: HyperliquidRawBookLevel
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


class TestHyperliquidWsRawMessageHandler(unittest.TestCase):
    """Test suite for HyperliquidWsRawMessageHandler."""

    def test_handle_l2book_payload_valid(self) -> None:
        """Test handle_l2book_payload with valid data."""
        valid_payload = {
            "coin": "ETH",
            "levels": [
                [{"px": "1800.0", "sz": "10.5", "n": 2}],  # Bids
                [{"px": "1800.5", "sz": "5.2", "n": 1}],  # Asks
            ],
            "time": 1678886400000,
        }
        expected_model = HyperliquidRawWsBookUpdate.model_validate(valid_payload)
        result = HyperliquidWsRawMessageHandler.handle_l2book_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_l2book_payload_invalid(self) -> None:
        """Test handle_l2book_payload with invalid data (missing 'coin')."""
        invalid_payload = {"levels": [[], []], "time": 1678886400000}
        with self.assertRaises(APIError) as cm:
            HyperliquidWsRawMessageHandler.handle_l2book_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    def test_handle_public_trades_payload_valid(self) -> None:
        """Test handle_public_trades_payload with a list of valid trade data."""
        valid_payload_list = [
            {
                "coin": "BTC",
                "px": "28000.0",
                "sz": "0.5",
                "side": "B",
                "time": 1678886400100,
                "hash": "0xabc",
            },
            {
                "coin": "BTC",
                "px": "28000.5",
                "sz": "0.2",
                "side": "A",
                "time": 1678886400200,
                "hash": "0xdef",
            },
        ]
        expected_models = [HyperliquidRawWsTradeEvent.model_validate(p) for p in valid_payload_list]
        result = HyperliquidWsRawMessageHandler.handle_public_trades_payload(valid_payload_list)
        self.assertEqual(result, expected_models)

    def test_handle_public_trades_payload_empty(self) -> None:
        """Test handle_public_trades_payload with an empty list."""
        result = HyperliquidWsRawMessageHandler.handle_public_trades_payload([])
        self.assertEqual(result, [])

    def test_handle_public_trades_payload_invalid_item(self) -> None:
        """Test handle_public_trades_payload with a list containing one invalid trade."""
        invalid_payload_list = [
            {
                "coin": "BTC",
                "px": "28000.0",
                "sz": "0.5",
                "side": "B",
                "time": 1678886400100,
                "hash": "0xabc",
            },
            {
                "coin": "BTC",
                "sz": "0.2",
                "side": "A",
                "time": 1678886400200,
                "hash": "0xdef",
            },  # Missing 'px'
        ]
        with self.assertRaises(APIError) as cm:
            HyperliquidWsRawMessageHandler.handle_public_trades_payload(invalid_payload_list)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIn("Error in public trades list at index 1", cm.exception.message)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- User Fill Event ---
    def test_handle_user_fill_event_payload_valid(self) -> None:
        """Test handle_user_fill_event_payload with valid data."""
        valid_payload = {
            "coin": "ETH",
            "px": "1800.0",
            "sz": "1.0",
            "side": "B",
            "time": 1678886400000,
            "hash": "0x123",
            "oid": 12345,
            "cloid": "clientOrder1",
            "isMaker": True,
        }
        expected_model = HyperliquidRawWsFillEvent.model_validate(valid_payload)
        result = HyperliquidWsRawMessageHandler.handle_user_fill_event_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_user_fill_event_payload_invalid(self) -> None:
        """Test handle_user_fill_event_payload with invalid data (px not decimal string)."""
        invalid_payload = {
            "coin": "ETH",
            "px": 1800.0,
            "sz": "1.0",
            "side": "B",  # px is float
            "time": 1678886400000,
            "hash": "0x123",
            "oid": 12345,
            "isMaker": False,
        }
        with self.assertRaises(APIError) as cm:
            HyperliquidWsRawMessageHandler.handle_user_fill_event_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- User Order Event (Inner Detail) --- using HyperliquidRawOrder
    def test_handle_user_order_event_payload_valid(self) -> None:
        """Test handle_user_order_event_payload with valid inner order data."""
        # This is the structure HyperliquidRawOrder expects
        valid_payload = {
            "order": {
                "coin": "ETH",
                "side": "A",
                "limitPx": "1900.0",
                "sz": "0.5",
                "origSz": "0.5",
                "reduceOnly": False,
                "timestamp": 1678886400000,
                "tif": "Gtc",
                "cloid": "myOrder123",
            },
            "status": "filled",
            "statusTimestamp": 1678886400100,
            "oid": 54321,
            "totalPx": "1899.5",
            "totalSz": "0.5",
        }
        expected_model = HyperliquidRawOrder.model_validate(valid_payload)
        result = HyperliquidWsRawMessageHandler.handle_user_order_event_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_user_order_event_payload_invalid(self) -> None:
        """Test handle_user_order_event_payload with invalid inner order data."""
        invalid_payload = {
            "order": {"coin": "ETH", "side": "A", "limitPx": "1900.0"},  # Missing sz, etc.
            "status": "open",
        }
        with self.assertRaises(APIError) as cm:
            HyperliquidWsRawMessageHandler.handle_user_order_event_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- User Order Update Wrapper --- using HyperliquidRawWsOrderUpdate
    def test_handle_user_order_update_wrapper_payload_valid(self) -> None:
        """Test handle_user_order_update_wrapper_payload with valid wrapper data."""
        valid_payload = {
            "eventType": "order",  # HyperliquidRawWsOrderUpdate expects 'eventType'
            "data": {  # The inner data for the order
                "order": {
                    "coin": "ETH",
                    "side": "B",
                    "limitPx": "1800",
                    "sz": "1",
                    "origSz": "1",
                    "reduceOnly": False,
                    "timestamp": 1678886400000,
                    "tif": "Gtc",
                },
                "status": "open",
                "oid": 123,
                "statusTimestamp": 1678886400100,
            },
        }
        expected_model = HyperliquidRawWsOrderUpdate.model_validate(valid_payload)
        result = HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload(
            valid_payload
        )
        self.assertEqual(result, expected_model)

    def test_handle_user_order_update_wrapper_payload_invalid(self) -> None:
        """Test handle_user_order_update_wrapper_payload with invalid wrapper (missing data)."""
        invalid_payload = {"eventType": "order"}
        with self.assertRaises(APIError) as cm:
            HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- User Position Update Event ---
    def test_handle_user_position_update_event_payload_valid(self) -> None:
        """Test handle_user_position_update_event_payload with valid data."""
        valid_payload = {
            "asset": "ETH",
            "position": {
                "type": "isolated",
                "amount": "2.5",
                "entryPx": "1750.0",
                "unrealizedPnl": "125.0",
                "liquidationPx": "1600.0",
                "marginUsed": "500.0",
            },
            "time": 1678886400000,
        }
        expected_model = HyperliquidRawWsPositionUpdateEvent.model_validate(valid_payload)
        result = HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload(
            valid_payload
        )
        self.assertEqual(result, expected_model)

    def test_handle_user_position_update_event_payload_invalid(self) -> None:
        """Test handle_user_position_update_event_payload with invalid data (missing position)."""
        invalid_payload = {"asset": "ETH", "time": 1678886400000}
        with self.assertRaises(APIError) as cm:
            HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload(
                invalid_payload
            )
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)


if __name__ == "__main__":
    unittest.main()
