import asyncio
import base64
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_api_user_stream_data_source import BackpackAPIUserStreamDataSource
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange


class BackpackUserStreamDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

        cls.private_key_bytes = bytes(range(32))
        cls.api_secret = base64.b64encode(cls.private_key_bytes).decode("utf-8")

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None

        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        self.auth = BackpackAuth(api_key="TEST_API_KEY", api_secret=self.api_secret, time_provider=self.mock_time_provider)

        self.connector = BackpackExchange(
            backpack_api_key="",
            backpack_api_secret="",
            trading_pairs=[],
            trading_required=False,
            domain=self.domain,
        )
        self.connector._web_assistants_factory._auth = self.auth

        self.data_source = BackpackAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    async def test_subscribe_channels_sends_authenticated_subscription(self):
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        mock_ws.receive = AsyncMock(return_value=None)

        await self.data_source._subscribe_channels(mock_ws)

        sent_request = mock_ws.send.call_args[0][0]
        sent_payload = sent_request.payload

        expected_timestamp = str(int(self.mock_time_provider.time() * 1e3))
        expected_window = str(CONSTANTS.AUTH_WINDOW_MS)
        auth_payload = f"instruction={CONSTANTS.WS_AUTH_INSTRUCTION}&timestamp={expected_timestamp}&window={expected_window}"
        expected_signature = base64.b64encode(
            ed25519.Ed25519PrivateKey.from_private_bytes(self.private_key_bytes).sign(auth_payload.encode("utf-8"))
        ).decode("utf-8")

        self.assertEqual("SUBSCRIBE", sent_payload["method"])
        self.assertEqual([CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL], sent_payload["params"])
        self.assertEqual(["TEST_API_KEY", expected_signature, expected_timestamp, expected_window], sent_payload["signature"])

    async def test_process_user_stream_message_routes_order_update(self):
        output = asyncio.Queue()
        message = {
            "stream": CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,
            "data": {"c": "1"},
        }

        await self.data_source._process_user_stream_message(message, output)

        output_message = await output.get()
        self.assertEqual("order_update", output_message["message_type"])

    async def test_process_user_stream_message_routes_balance_update(self):
        output = asyncio.Queue()
        message = {
            "stream": CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,
            "data": {"balances": {"USDC": {"available": "10"}}},
        }

        await self.data_source._process_user_stream_message(message, output)

        order_message = await output.get()
        balance_message = await output.get()
        self.assertEqual("order_update", order_message["message_type"])
        self.assertEqual("balance_update", balance_message["message_type"])

    async def test_process_user_stream_message_routes_trade_update(self):
        output = asyncio.Queue()
        message = {
            "stream": "account.tradeUpdate",
            "data": {"t": "1"},
        }

        await self.data_source._process_user_stream_message(message, output)

        output_message = await output.get()
        self.assertEqual("trade_update", output_message["message_type"])
