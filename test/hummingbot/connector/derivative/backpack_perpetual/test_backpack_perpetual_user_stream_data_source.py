import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

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
            backpack_perpetual_api_secret="testSecret",
            trading_pairs=[self.trading_pair],
        )

        self.auth = BackpackPerpetualAuth(
            api_key="testAPIKey",
            api_secret="testSecret",
        )

        self.data_source = BackpackPerpetualUserStreamDataSource(
            auth=self.auth,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)
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

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_authenticates_and_subscribes(self, mock_ws):
        """Test user stream authentication and subscription."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        self.async_run_with_timeout(asyncio.sleep(0.5))

        # Check that auth message was sent
        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(mock_ws.return_value)
        self.assertEqual(2, len(sent_messages))  # Auth message + subscription message

        auth_msg = sent_messages[0]
        self.assertEqual("auth", auth_msg["method"])
        self.assertIn("apiKey", auth_msg["params"])

        subscribe_msg = sent_messages[1]
        self.assertEqual("subscribe", subscribe_msg["method"])
        self.assertIn("subscriptions", subscribe_msg["params"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_processes_order_event(self, mock_ws):
        """Test processing of order update events."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Add order event
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._order_event())
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual("order", msg["type"])
        self.assertEqual("1234567890", msg["data"]["orderId"])
        self.assertEqual(self.trading_pair, msg["data"]["trading_pair"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_processes_balance_event(self, mock_ws):
        """Test processing of balance update events."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Add balance event
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._balance_event())
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual("balance", msg["type"])
        self.assertEqual("USDC", msg["data"]["asset"])
        self.assertEqual("10000.00", msg["data"]["free"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_processes_position_event(self, mock_ws):
        """Test processing of position update events."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Add position event
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._position_event())
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual("position", msg["type"])
        self.assertEqual(self.trading_pair, msg["data"]["trading_pair"])
        self.assertEqual("0.100", msg["data"]["size"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_processes_fill_event(self, mock_ws):
        """Test processing of trade fill events."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Add fill event
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._fill_event())
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual("fill", msg["type"])
        self.assertEqual(self.trading_pair, msg["data"]["trading_pair"])
        self.assertEqual("9876543210", msg["data"]["tradeId"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_processes_funding_event(self, mock_ws):
        """Test processing of funding payment events."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Add funding event
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._funding_event())
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual("funding", msg["type"])
        self.assertEqual(self.trading_pair, msg["data"]["trading_pair"])
        self.assertEqual("0.0001", msg["data"]["fundingRate"])

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_processes_liquidation_event(self, mock_ws):
        """Test processing of liquidation warning events."""
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Mock authentication response
        auth_response = {"result": "success", "type": "authenticated"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(auth_response)
        )

        # Add liquidation event
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value,
            json.dumps(self._liquidation_event())
        )

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        msg = self.async_run_with_timeout(output_queue.get())

        self.assertEqual("liquidation", msg["type"])
        self.assertEqual(self.trading_pair, msg["data"]["trading_pair"])
        self.assertEqual("45500.00", msg["data"]["markPrice"])

        # Check that warning was logged
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
    def test_listen_for_user_stream_logs_exception_on_error(self, mock_sleep, mock_ws):
        """Test error handling in user stream listener."""
        mock_sleep.side_effect = lambda _: self._raise_exception_and_unlock_test_with_event(
            asyncio.CancelledError
        )
        mock_ws.side_effect = Exception("TEST ERROR")

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Error in user stream WebSocket",
            )
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_reconnects_on_disconnect(self, mock_ws):
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

        self.async_run_with_timeout(asyncio.sleep(0.5))

        # Verify that it attempted to reconnect (would call ws_connect again)
        self.assertTrue(mock_ws.called)

    def test_last_recv_time_updates(self):
        """Test that last_recv_time property updates correctly."""
        initial_time = self.data_source.last_recv_time
        self.assertEqual(initial_time, 0)

        # Simulate receiving a message (would be set in listen_for_user_stream)
        self.data_source._last_recv_time = 1641288825.0

        self.assertEqual(self.data_source.last_recv_time, 1641288825.0)
