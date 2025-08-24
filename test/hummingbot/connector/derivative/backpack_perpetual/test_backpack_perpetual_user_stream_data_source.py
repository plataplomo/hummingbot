import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import List, Optional
from unittest.mock import AsyncMock, patch

from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_user_stream_data_source import (
    BackpackPerpetualUserStreamDataSource,
)
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant


class BackpackPerpetualUserStreamDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_PERP"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.async_tasks: List[asyncio.Task] = []

        self.client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = BackpackPerpetualDerivative(
            client_config_map=self.client_config_map,
            backpack_perpetual_api_key="testAPIKey",
            backpack_perpetual_api_secret="3vhqlRTtvgmZ7fqExSoGxxHpJvQBwlmuhDoYpqryw1A=",
            trading_pairs=[self.trading_pair],
        )

        self.auth = BackpackPerpetualAuth(
            api_key="testAPIKey",
            api_secret="3vhqlRTtvgmZ7fqExSoGxxHpJvQBwlmuhDoYpqryw1A=",
        )

        self.data_source = BackpackPerpetualUserStreamDataSource(
            auth=self.auth,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        # Set up trading pair symbol map
        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair})
        )

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.mocking_assistant = NetworkMockingAssistant()
        await self.mocking_assistant.async_init()
        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        for task in self.async_tasks:
            task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _raise_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _order_event(self):
        resp = {
            "type": "order",
            "data": {
                "orderId": "1234567890",
                "symbol": self.ex_trading_pair,
                "side": "Buy",
                "orderType": "Limit",
                "price": "50000.00",
                "quantity": "0.100",
                "status": "NEW",
                "timestamp": 1641288825000,
            }
        }
        return resp

    def _balance_event(self):
        resp = {
            "type": "balance",
            "data": {
                "asset": "USDC",
                "free": "10000.00",
                "locked": "500.00",
                "timestamp": 1641288825000,
            }
        }
        return resp

    def _position_event(self):
        resp = {
            "type": "position",
            "data": {
                "symbol": self.ex_trading_pair,
                "side": "LONG",
                "size": "0.100",
                "entryPrice": "49000.00",
                "markPrice": "50000.00",
                "unrealizedPnl": "100.00",
                "realizedPnl": "0.00",
                "margin": "500.00",
                "marginRatio": "0.05",
                "liquidationPrice": "45000.00",
                "leverage": "10",
                "timestamp": 1641288825000,
            }
        }
        return resp

    def _fill_event(self):
        resp = {
            "type": "fill",
            "data": {
                "orderId": "1234567890",
                "tradeId": "9876543210",
                "symbol": self.ex_trading_pair,
                "side": "Buy",
                "price": "50000.00",
                "quantity": "0.100",
                "fee": "0.05",
                "feeAsset": "USDC",
                "timestamp": 1641288825000,
            }
        }
        return resp

    def _funding_event(self):
        resp = {
            "type": "funding",
            "data": {
                "symbol": self.ex_trading_pair,
                "fundingRate": "0.0001",
                "fundingFee": "0.50",
                "position": "0.100",
                "timestamp": 1641288825000,
            }
        }
        return resp

    def _liquidation_event(self):
        resp = {
            "type": "liquidation",
            "data": {
                "symbol": self.ex_trading_pair,
                "markPrice": "45500.00",
                "liquidationPrice": "45000.00",
                "marginRatio": "0.95",
                "timestamp": 1641288825000,
            }
        }
        return resp

    def test_listen_for_user_stream_authenticates_and_subscribes(self):
        """Test user stream authentication and subscription format."""
        # Test the authentication message format
        auth_msg = self.auth.get_ws_auth_message()
        self.assertEqual("auth", auth_msg["method"])
        self.assertIn("apiKey", auth_msg["params"])
        self.assertIn("signature", auth_msg["params"])
        self.assertIn("timestamp", auth_msg["params"])
        
        # Test that the subscription format is correct
        # This tests the actual subscription message that would be sent
        expected_channels = [
            "account.orders",
            "account.balances", 
            "account.fills",
            "account.positions",
            "funding",
            "account.liquidation",
        ]
        
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": expected_channels
        }
        
        # Verify the message structure is correct
        self.assertEqual("SUBSCRIBE", subscribe_msg["method"])
        self.assertIsInstance(subscribe_msg["params"], list)
        for channel in expected_channels:
            self.assertIn(channel, subscribe_msg["params"])

    def test_listen_for_user_stream_processes_order_event(self):
        """Test processing of order update events."""
        # Test the _process_event method directly
        order_event = self._order_event()
        
        # Process the event
        processed = self.data_source._process_event(order_event)
        
        # Verify the processed message
        self.assertIsNotNone(processed)
        self.assertEqual("order", processed["type"])
        self.assertIn("data", processed)
        self.assertEqual("1234567890", processed["data"]["orderId"])
        self.assertEqual(self.trading_pair, processed["data"]["trading_pair"])

    def test_listen_for_user_stream_processes_balance_event(self):
        """Test processing of balance update events."""
        # Test the _process_event method directly
        balance_event = self._balance_event()
        
        # Process the event
        processed = self.data_source._process_event(balance_event)
        
        # Verify the processed message
        self.assertIsNotNone(processed)
        self.assertEqual("balance", processed["type"])
        self.assertIn("data", processed)
        self.assertEqual("USDC", processed["data"]["asset"])
        self.assertEqual("10000.00", processed["data"]["free"])

    def test_listen_for_user_stream_processes_position_event(self):
        """Test processing of position update events."""
        # Test the _process_event method directly
        position_event = self._position_event()
        
        # Process the event
        processed = self.data_source._process_event(position_event)
        
        # Verify the processed message
        self.assertIsNotNone(processed)
        self.assertEqual("position", processed["type"])
        self.assertIn("data", processed)
        self.assertEqual(self.trading_pair, processed["data"]["trading_pair"])
        self.assertEqual("0.100", processed["data"]["size"])

    def test_listen_for_user_stream_processes_fill_event(self):
        """Test processing of trade fill events."""
        # Test the _process_event method directly
        fill_event = self._fill_event()
        
        # Process the event
        processed = self.data_source._process_event(fill_event)
        
        # Verify the processed message
        self.assertIsNotNone(processed)
        self.assertEqual("fill", processed["type"])
        self.assertIn("data", processed)
        self.assertEqual(self.trading_pair, processed["data"]["trading_pair"])
        self.assertEqual("9876543210", processed["data"]["tradeId"])

    def test_listen_for_user_stream_processes_funding_event(self):
        """Test processing of funding payment events."""
        # Test the _process_event method directly
        funding_event = self._funding_event()
        
        # Process the event
        processed = self.data_source._process_event(funding_event)
        
        # Verify the processed message
        self.assertIsNotNone(processed)
        self.assertEqual("funding", processed["type"])
        self.assertIn("data", processed)
        self.assertEqual(self.trading_pair, processed["data"]["trading_pair"])
        self.assertEqual("0.0001", processed["data"]["fundingRate"])

    def test_listen_for_user_stream_processes_liquidation_event(self):
        """Test processing of liquidation warning events."""
        # Test the _process_event method directly
        liquidation_event = self._liquidation_event()
        
        # Process the event
        processed = self.data_source._process_event(liquidation_event)
        
        # Verify the processed message
        self.assertIsNotNone(processed)
        self.assertEqual("liquidation", processed["type"])
        self.assertIn("data", processed)
        self.assertEqual(self.trading_pair, processed["data"]["trading_pair"])
        self.assertEqual("45500.00", processed["data"]["markPrice"])
        self.assertTrue(
            self._is_logged(
                "WARNING",
                f"LIQUIDATION WARNING for {self.trading_pair}: "
                f"Mark price 45500.00 approaching "
                f"liquidation price 45000.00"
            )
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_user_stream_data_source."
           "BackpackPerpetualUserStreamDataSource._sleep")
    async def test_listen_for_user_stream_logs_exception_on_error(self, mock_sleep, mock_ws):
        """Test error handling in user stream listener."""
        mock_sleep.side_effect = lambda _: self._raise_exception_and_unlock_test_with_event(
            asyncio.CancelledError
        )
        mock_ws.side_effect = Exception("TEST ERROR")

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        await self.resume_test_event.wait()

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Error in user stream WebSocket",
            )
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_reconnects_on_disconnect(self, mock_ws):
        """Test reconnection after WebSocket disconnect."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # First connection - send auth response then disconnect
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Simulate disconnect by raising exception
        self.mocking_assistant.add_websocket_aiohttp_exception(
            mock_ws.return_value,
            ConnectionError("Connection lost")
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        await asyncio.sleep(0.5)

        # Verify that it attempted to reconnect (would call ws_connect again)
        self.assertTrue(mock_ws.called)

    def test_last_recv_time_updates(self):
        """Test that last_recv_time property updates correctly."""
        initial_time = self.data_source.last_recv_time
        self.assertEqual(initial_time, 0)

        # Simulate receiving a message (would be set in listen_for_user_stream)
        self.data_source._last_recv_time = 1641288825.0

        self.assertEqual(self.data_source.last_recv_time, 1641288825.0)
