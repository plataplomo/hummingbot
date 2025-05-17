"""
Unit tests for BackpackWsRawMessageHandler.
"""

import unittest

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.models import (
    BackpackRawOrderBook,
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,
    BackpackRawTicker,  # Assuming this is the correct model for WS ticker
    BackpackRawTradeEvent,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


class TestBackpackWsRawMessageHandler(unittest.TestCase):
    """Test suite for BackpackWsRawMessageHandler."""

    def test_handle_depth_payload_valid(self) -> None:
        """Test handle_depth_payload with valid data."""
        # Minimal valid payload for BackpackRawOrderBook - structure may vary
        # Based on BackpackRawMarket, which includes bids/asks/lastUpdateTime
        # And BackpackRawOrderBook (in bp_raw_market.py) has bids, asks, lastUpdateId, timestamp
        valid_payload = {
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "2.0"]],
            "lastUpdateId": "12345",
            "timestamp": 1678886400000,
        }
        expected_model = BackpackRawOrderBook.model_validate(valid_payload)
        result = BackpackWsRawMessageHandler.handle_depth_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_depth_payload_invalid(self) -> None:
        """Test handle_depth_payload with invalid data (missing field)."""
        invalid_payload = {"asks": [["101.0", "2.0"]]}  # Missing bids, lastUpdateId, timestamp
        with self.assertRaises(APIError) as cm:
            BackpackWsRawMessageHandler.handle_depth_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- Ticker --- (Assuming BackpackRawTicker is used for WS)
    def test_handle_ticker_payload_valid(self) -> None:
        """Test handle_ticker_payload with valid data."""
        # Based on BackpackRawTicker model in bp_raw_market.py
        valid_payload = {
            "symbol": "SOL_USDC",
            "price": "150.50",
            "bid": "150.45",
            "ask": "150.55",
            "volume": "10000.0",
            "time": 1678886400000,
        }
        expected_model = BackpackRawTicker.model_validate(valid_payload)
        result = BackpackWsRawMessageHandler.handle_ticker_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_ticker_payload_invalid(self) -> None:
        """Test handle_ticker_payload with invalid data (wrong type)."""
        invalid_payload = {"symbol": "SOL_USDC", "price": 150.50}  # Price should be string
        with self.assertRaises(APIError) as cm:
            BackpackWsRawMessageHandler.handle_ticker_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- Trade Event --- (BackpackRawTradeEvent)
    def test_handle_trade_event_payload_valid(self) -> None:
        """Test handle_trade_event_payload with valid data."""
        # Based on BackpackRawTradeEvent in bp_raw_trade.py
        valid_payload = {
            "e": "trade",  # event_type
            "E": 1678886400000,  # event_time
            "s": "SOL_USDC",  # symbol
            "p": "150.00",  # price
            "q": "1.5",  # quantity
            "b": "buyerOrderId123",  # buyer_order_id
            "a": "sellerOrderId456",  # seller_order_id
            "t": "tradeId789",  # trade_id
            "T": 1678886400001,  # engine_timestamp
            "m": True,  # is_buyer_the_maker
        }
        expected_model = BackpackRawTradeEvent.model_validate(valid_payload)
        result = BackpackWsRawMessageHandler.handle_trade_event_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_trade_event_payload_invalid(self) -> None:
        """Test handle_trade_event_payload with invalid data (missing 'p' price)."""
        invalid_payload = {
            "e": "trade",
            "E": 1678886400000,
            "s": "SOL_USDC",
            "q": "1.5",
            "b": "buyerOrderId123",
            "a": "sellerOrderId456",
            "t": "tradeId789",
            "T": 1678886400001,
            "m": True,
        }
        with self.assertRaises(APIError) as cm:
            BackpackWsRawMessageHandler.handle_trade_event_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- Order Update --- (BackpackRawOrderUpdate)
    def test_handle_order_update_payload_valid(self) -> None:
        """Test handle_order_update_payload with valid data."""
        # Based on BackpackRawOrderUpdate in bp_raw_order.py
        valid_payload = {
            "e": "orderAccepted",  # event_type
            "E": 1678886400000,  # event_time
            "s": "SOL_USDC",  # symbol
            "c": "clientOrderId123",  # client_order_id
            "S": "Bid",  # side
            "o": "LIMIT",  # order_type
            "f": "GTC",  # time_in_force
            "q": "10.0",  # quantity
            "p": "149.00",  # price
            "X": "NEW",  # order_status
        }
        expected_model = BackpackRawOrderUpdate.model_validate(valid_payload)
        result = BackpackWsRawMessageHandler.handle_order_update_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_order_update_payload_invalid(self) -> None:
        """Test handle_order_update_payload with invalid data (status missing)."""
        invalid_payload = {
            "e": "orderAccepted",
            "E": 1678886400000,
            "s": "SOL_USDC",
            "c": "clientOrderId123",
            "S": "Bid",
            "o": "LIMIT",
            "f": "GTC",
            "q": "10.0",
            "p": "149.00",
        }
        with self.assertRaises(APIError) as cm:
            BackpackWsRawMessageHandler.handle_order_update_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)

    # --- Position Update --- (BackpackRawPositionUpdate)
    def test_handle_position_update_payload_valid(self) -> None:
        """Test handle_position_update_payload with valid data."""
        # Based on BackpackRawPositionUpdate in bp_raw_position.py
        # This model expects: event_type, event_time, symbol, break_event_price, entry_price etc.
        # Payload example requires specific aliases like 'e', 'E', 's'
        valid_payload = {
            "e": "positionUpdate",
            "E": 1678886400000,
            "s": "SOL_USDC",
            "b": "150.0",  # break_event_price
            "B": "149.5",  # entry_price
            "l": "140.0",  # liquidation_price
            "f": "0.01",  # initial_margin_fraction
            "M": "150.1",  # mark_price
            "m": "0.005",  # maintenance_margin_fraction
            "q": "5.0",  # net_quantity
            "Q": "5.0",  # net_exposure_quantity
            "n": "750.5",  # net_exposure_notional
        }
        expected_model = BackpackRawPositionUpdate.model_validate(valid_payload)
        result = BackpackWsRawMessageHandler.handle_position_update_payload(valid_payload)
        self.assertEqual(result, expected_model)

    def test_handle_position_update_payload_invalid(self) -> None:
        """Test handle_position_update_payload with invalid data (symbol missing)."""
        invalid_payload = {
            "e": "positionUpdate",
            "E": 1678886400000,
            "b": "150.0",
            "B": "149.5",
            "l": "140.0",
            "f": "0.01",
            "M": "150.1",
            "m": "0.005",
            "q": "5.0",
            "Q": "5.0",
            "n": "750.5",
        }
        with self.assertRaises(APIError) as cm:
            BackpackWsRawMessageHandler.handle_position_update_payload(invalid_payload)
        self.assertEqual(cm.exception.code, APIErrorCode.INVALID_RESPONSE.value)
        self.assertIsInstance(cm.exception.original_exception, ValidationError)


if __name__ == "__main__":
    unittest.main()
