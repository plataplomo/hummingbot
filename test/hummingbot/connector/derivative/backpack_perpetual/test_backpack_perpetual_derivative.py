import asyncio
import base64
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from collections.abc import Callable
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
from aioresponses.core import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils as web_utils
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import BackpackPerpetualDerivative
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.network_iterator import NetworkStatus


class BackpackPerpetualDerivativeUnitTest(IsolatedAsyncioWrapperTestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    start_timestamp: float = pd.Timestamp("2021-01-01", tz="UTC").timestamp()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_PERP"
        cls.symbol = f"{cls.base_asset}_PERP"
        cls.listen_key = "TEST_LISTEN_KEY"
        cls.client_order_id_prefix = "HBOT"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.expected_exchange_order_id = "123456789"

        self.ws_sent_messages = []
        self.ws_incoming_messages = asyncio.Queue()
        self.resume_test_event = asyncio.Event()
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())

        # Use a valid base64 encoded test key (32 bytes)
        test_secret = base64.b64encode(b"0" * 32).decode()

        self.exchange = BackpackPerpetualDerivative(
            client_config_map=self.client_config_map,
            backpack_perpetual_api_key="testAPIKey",
            backpack_perpetual_api_secret=test_secret,
            trading_pairs=[self.trading_pair],
        )

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)
        self.test_task: Optional[asyncio.Task] = None
        self.resume_test_event = asyncio.Event()
        self._initialize_event_loggers()

    @property
    def all_symbols_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_URL)
        return url

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TIME_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.EXCHANGE_INFO_URL)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.BALANCE_URL)
        return url

    @property
    def funding_info_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.TICKER_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    @property
    def funding_payment_url(self):
        url = web_utils.private_rest_url(path_url=CONSTANTS.FUNDING_HISTORY_URL)
        url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        return url

    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        super().tearDown()

    def async_run_with_timeout(self, coroutine, timeout: int = 1):
        """Run async coroutine with timeout."""
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.funding_payment_completed_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.FundingPaymentCompleted, self.funding_payment_completed_logger)]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _get_position_risk_api_endpoint_single_position_list(self) -> List[Dict[str, Any]]:
        positions = [
            {
                "symbol": self.symbol,
                "side": "LONG",
                "size": "1",
                "entryPrice": "50000",
                "markPrice": "51000",
                "unrealizedPnl": "1000",
                "realizedPnl": "0",
                "margin": "500",
                "marginRatio": "0.05",
                "liquidationPrice": "45000",
                "leverage": "10",
                "notional": "51000",
                "updateTime": int(self.start_timestamp * 1000),
            }
        ]
        return positions

    def _get_account_update_ws_event_single_position_dict(self) -> Dict[str, Any]:
        account_update = {
            "type": "position",
            "data": {
                "symbol": self.symbol,
                "side": "LONG",
                "size": "1",
                "entryPrice": "50000",
                "markPrice": "51000",
                "unrealizedPnl": "1000",
                "realizedPnl": "0",
                "margin": "500",
                "marginRatio": "0.05",
                "liquidationPrice": "45000",
                "leverage": "10",
                "timestamp": int(self.start_timestamp * 1000),
            }
        }
        return account_update

    def _get_income_history_dict(self) -> List[Dict[str, Any]]:
        income_history = [
            {
                "symbol": self.symbol,
                "incomeType": "FUNDING_FEE",
                "income": "0.5",
                "asset": "USDC",
                "info": "funding payment",
                "time": int(self.start_timestamp * 1000),
                "tranId": "123456789",
                "tradeId": "",
            }
        ]
        return income_history

    @aioresponses()
    def test_check_network_success(self, mock_api):
        """Test successful network check."""
        url = self.network_status_url
        # Mock the time endpoint response (using TIME_URL instead of PING_URL)
        mock_api.get(url, body=json.dumps({"serverTime": 1234567890000}))

        result = self.async_run_with_timeout(self.exchange.check_network())
        self.assertEqual(result, NetworkStatus.CONNECTED)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        """Test network check failure."""
        url = self.network_status_url
        mock_api.get(url, status=500)

        result = self.async_run_with_timeout(self.exchange.check_network())
        self.assertEqual(result, NetworkStatus.NOT_CONNECTED)

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        """Test updating trading rules from exchange info."""
        url = self.trading_rules_url

        mock_response = {
            "symbols": [
                {
                    "symbol": self.symbol,
                    "baseAsset": self.base_asset,
                    "quoteAsset": self.quote_asset,
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "filters": {
                        "minQuantity": "0.001",
                        "maxQuantity": "1000",
                        "stepSize": "0.001",
                        "minNotional": "10",
                        "tickSize": "0.01",
                    }
                }
            ]
        }
        mock_api.get(url, body=json.dumps(mock_response))

        self.async_run_with_timeout(self.exchange._update_trading_rules())

        trading_rule = self.exchange._trading_rules[self.trading_pair]
        self.assertIsInstance(trading_rule, TradingRule)
        self.assertEqual(trading_rule.trading_pair, self.trading_pair)
        self.assertEqual(trading_rule.min_order_size, Decimal("0.001"))
        self.assertEqual(trading_rule.max_order_size, Decimal("1000"))
        self.assertEqual(trading_rule.min_order_value, Decimal("10"))
        self.assertEqual(trading_rule.min_base_amount_increment, Decimal("0.001"))
        self.assertEqual(trading_rule.min_price_increment, Decimal("0.01"))

    @aioresponses()
    def test_update_balances(self, mock_api):
        """Test updating account balances."""
        url = self.balance_url

        mock_response = {
            "balances": [
                {
                    "asset": "USDC",
                    "free": "10000",
                    "locked": "500",
                }
            ],
            "positions": self._get_position_risk_api_endpoint_single_position_list(),
        }
        mock_api.get(url, body=json.dumps(mock_response))

        self.async_run_with_timeout(self.exchange._update_balances())

        available_balance = self.exchange.get_balance("USDC")
        self.assertEqual(available_balance, Decimal("10000"))

    @aioresponses()
    def test_get_open_orders(self, mock_api):
        """Test fetching open orders."""
        url = web_utils.private_rest_url(path_url=CONSTANTS.OPEN_ORDERS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")

        mock_response = [
            {
                "orderId": "123456",
                "clientOrderId": "HBOT-1",
                "symbol": self.symbol,
                "side": "Buy",
                "orderType": "Limit",
                "price": "50000",
                "quantity": "0.1",
                "executedQuantity": "0",
                "status": "NEW",
                "timestamp": int(self.start_timestamp * 1000),
            }
        ]
        mock_api.get(regex_url, body=json.dumps(mock_response))

        orders = self.async_run_with_timeout(self.exchange.get_open_orders())

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].trading_pair, self.trading_pair)
        self.assertEqual(orders[0].price, Decimal("50000"))
        self.assertEqual(orders[0].amount, Decimal("0.1"))

    @aioresponses()
    def test_place_order_buy_market_order(self, mock_api):
        """Test placing a market buy order."""
        order_id = "HBOT-1"
        amount = Decimal("0.1")

        # Mock order placement
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "orderId": "123456",
            "clientOrderId": order_id,
            "symbol": self.symbol,
            "side": "Buy",
            "orderType": "Market",
            "quantity": str(amount),
            "status": "NEW",
            "timestamp": int(self.start_timestamp * 1000),
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        # Mock balance check
        balance_url = self.balance_url
        balance_response = {
            "balances": [{"asset": "USDC", "free": "10000", "locked": "0"}],
            "positions": [],
        }
        mock_api.get(balance_url, body=json.dumps(balance_response))

        order_id_result = self.async_run_with_timeout(
            self.exchange._place_order(
                order_id=order_id,
                trading_pair=self.trading_pair,
                amount=amount,
                trade_type=TradeType.BUY,
                order_type=OrderType.MARKET,
                price=None,
                position_action=PositionAction.OPEN,
            )
        )

        self.assertEqual(order_id, order_id_result)

        in_flight_order = self.exchange._order_tracker.fetch_order(order_id)
        self.assertIsNotNone(in_flight_order)

    @aioresponses()
    def test_place_order_sell_limit_order(self, mock_api):
        """Test placing a limit sell order."""
        order_id = "HBOT-2"
        amount = Decimal("0.1")
        price = Decimal("51000")

        # Mock order placement
        url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "orderId": "123457",
            "clientOrderId": order_id,
            "symbol": self.symbol,
            "side": "Sell",
            "orderType": "Limit",
            "price": str(price),
            "quantity": str(amount),
            "status": "NEW",
            "timestamp": int(self.start_timestamp * 1000),
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        # Mock balance check
        balance_url = self.balance_url
        balance_response = {
            "balances": [{"asset": "USDC", "free": "10000", "locked": "0"}],
            "positions": self._get_position_risk_api_endpoint_single_position_list(),
        }
        mock_api.get(balance_url, body=json.dumps(balance_response))

        order_id_result = self.async_run_with_timeout(
            self.exchange._place_order(
                order_id=order_id,
                trading_pair=self.trading_pair,
                amount=amount,
                trade_type=TradeType.SELL,
                order_type=OrderType.LIMIT,
                price=price,
                position_action=PositionAction.CLOSE,
            )
        )

        self.assertEqual(order_id, order_id_result)

        in_flight_order = self.exchange._order_tracker.fetch_order(order_id)
        self.assertIsNotNone(in_flight_order)
        self.assertEqual(in_flight_order.price, price)

    @aioresponses()
    def test_cancel_order(self, mock_api):
        """Test cancelling an order."""
        order_id = "HBOT-3"
        exchange_order_id = "123458"

        # Add order to tracker
        self.exchange._order_tracker.start_tracking_order(
            InFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=self.trading_pair,
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("50000"),
                amount=Decimal("0.1"),
                creation_timestamp=self.start_timestamp,
            )
        )

        # Mock cancel request
        url = web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "orderId": exchange_order_id,
            "clientOrderId": order_id,
            "status": "CANCELLED",
        }
        mock_api.delete(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange._execute_cancel(trading_pair=self.trading_pair, order_id=order_id)
        )

        self.assertTrue(result)

        # Order should be marked as cancelled
        cancelled_order = self.exchange._order_tracker.fetch_order(order_id)
        self.assertTrue(cancelled_order.is_cancelled)

    @aioresponses()
    def test_get_funding_info(self, mock_api):
        """Test fetching funding info."""
        url = self.funding_info_url

        mock_response = {
            "symbol": self.symbol,
            "markPrice": "50000",
            "indexPrice": "50001",
            "fundingRate": "0.0001",
            "nextFundingTime": int((self.start_timestamp + 28800) * 1000),
        }
        mock_api.get(url, body=json.dumps(mock_response))

        funding_info = self.async_run_with_timeout(
            self.exchange.get_funding_info(self.trading_pair)
        )

        self.assertEqual(funding_info.trading_pair, self.trading_pair)
        self.assertEqual(funding_info.mark_price, Decimal("50000"))
        self.assertEqual(funding_info.index_price, Decimal("50001"))
        self.assertEqual(funding_info.rate, Decimal("0.0001"))

    @aioresponses()
    def test_get_funding_payment_history(self, mock_api):
        """Test fetching funding payment history."""
        url = self.funding_payment_url

        mock_response = self._get_income_history_dict()
        mock_api.get(url, body=json.dumps(mock_response))

        funding_payments = self.async_run_with_timeout(
            self.exchange.get_funding_payment_history(
                trading_pair=self.trading_pair,
                start_time=self.start_timestamp,
            )
        )

        self.assertEqual(len(funding_payments), 1)
        self.assertEqual(funding_payments[0].trading_pair, self.trading_pair)
        self.assertEqual(funding_payments[0].amount, Decimal("0.5"))

    def test_supported_position_modes(self):
        """Test that only ONE_WAY position mode is supported."""
        supported_modes = self.exchange.supported_position_modes
        self.assertEqual(len(supported_modes), 1)
        self.assertIn(PositionMode.ONEWAY, supported_modes)

    @aioresponses()
    def test_set_leverage(self, mock_api):
        """Test setting leverage."""
        url = web_utils.private_rest_url(path_url=CONSTANTS.LEVERAGE_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        leverage = 5
        mock_response = {
            "symbol": self.symbol,
            "leverage": leverage,
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        result = self.async_run_with_timeout(
            self.exchange.set_leverage(self.trading_pair, leverage)
        )

        self.assertEqual(result, leverage)

    @aioresponses()
    def test_existing_account_position_detected_on_positions_update(self, mock_api):
        """Test that existing positions are properly detected and updated."""
        # Set up initial position - using _perpetual_trading directly like the implementation does
        self.exchange._perpetual_trading.account_positions[self.trading_pair] = MagicMock()

        position_response = {
            "positions": [
                {
                    "symbol": self.symbol,
                    "netQuantity": "0.5",  # Positive for long position
                    "entryPrice": "45000.00",
                    "markPrice": "46000.00",
                    "pnlUnrealized": "500.00",
                    "pnlRealized": "0.00",
                    "estLiquidationPrice": "40000.00",
                    "breakEvenPrice": "45000.00"
                }
            ]
        }

        positions_url = web_utils.private_rest_url(path_url=CONSTANTS.POSITIONS_URL)
        mock_api.get(positions_url, body=json.dumps(position_response))

        self.async_run_with_timeout(self.exchange._update_positions())

        # Verify position was updated - use public property for reading
        self.assertIn(self.trading_pair, self.exchange.account_positions)
        position = self.exchange.account_positions[self.trading_pair]
        # Check position attributes
        self.assertEqual(position.trading_pair, self.trading_pair)
        self.assertEqual(position.position_side, PositionSide.LONG)
        self.assertEqual(position.amount, Decimal("0.5"))
        self.assertEqual(position.entry_price, Decimal("45000"))
        self.assertEqual(position.unrealized_pnl, Decimal("500"))

    @aioresponses()
    def test_closed_account_position_removed_on_positions_update(self, mock_api):
        """Test that closed positions are properly removed."""
        # Set up initial position - using _perpetual_trading directly like the implementation does
        self.exchange._perpetual_trading.account_positions[self.trading_pair] = MagicMock()

        # Empty positions response indicates closed position
        position_response = {"positions": []}

        positions_url = web_utils.private_rest_url(path_url=CONSTANTS.POSITIONS_URL)
        mock_api.get(positions_url, body=json.dumps(position_response))

        self.async_run_with_timeout(self.exchange._update_positions())

        # Verify position was removed - use public property for reading
        self.assertNotIn(self.trading_pair, self.exchange.account_positions)

    @aioresponses()
    def test_order_event_with_cancelled_status_marks_order_as_cancelled(self, mock_api):
        """Test that cancelled order events properly update order status."""
        client_order_id = "test_order_1"
        exchange_order_id = "12345"

        # Create in-flight order
        order = InFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("50000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.start_timestamp,
        )
        self.exchange._order_tracker.start_tracking_order(order)

        # Mock cancelled order status
        order_status = {
            "order": {
                "orderId": exchange_order_id,
                "clientOrderId": client_order_id,
                "symbol": self.symbol,
                "status": "CANCELLED",
                "side": "Buy",
                "orderType": "Limit",
                "price": "50000.00",
                "quantity": "0.1",
                "executedQuantity": "0.0",
                "timestamp": 1640780000000
            }
        }

        order_url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_URL)
        mock_api.get(order_url, body=json.dumps(order_status))

        self.async_run_with_timeout(
            self.exchange._update_order_status()
        )

        # Verify order is marked as cancelled
        self.assertTrue(order.is_cancelled)
        self.assertEqual(order.current_state, OrderState.CANCELED)

    @aioresponses()
    def test_order_fill_event_takes_fee_from_update_event(self, mock_api):
        """Test that order fill events properly extract fee information."""
        client_order_id = "test_order_1"
        exchange_order_id = "12345"

        # Create in-flight order
        order = InFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("50000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.start_timestamp,
        )
        self.exchange._order_tracker.start_tracking_order(order)

        # Mock filled order with fee
        trade_update = {
            "type": "fill",
            "data": {
                "orderId": exchange_order_id,
                "tradeId": "67890",
                "symbol": self.symbol,
                "side": "Buy",
                "price": "50000.00",
                "quantity": "0.1",
                "fee": "0.05",
                "feeAsset": "USDC",
                "timestamp": 1640780000000
            }
        }

        # Process trade update through order message handler
        self.exchange._process_order_message(trade_update)

        # Verify fee was recorded
        self.assertEqual(len(order.order_fills), 1)
        fill = order.order_fills[list(order.order_fills.keys())[0]]
        self.assertEqual(fill.fee.flat_fees[0].amount, Decimal("0.05"))
        self.assertEqual(fill.fee.flat_fees[0].token, "USDC")

    def test_create_order_with_invalid_position_action_raises_value_error(self):
        """Test that creating order with invalid position action raises error."""
        with self.assertRaises(ValueError) as context:
            self.async_run_with_timeout(
                self.exchange._create_order(
                    trade_type=TradeType.BUY,
                    order_id="test_order",
                    trading_pair=self.trading_pair,
                    amount=Decimal("0.1"),
                    order_type=OrderType.LIMIT,
                    price=Decimal("50000"),
                    position_action=PositionAction.NIL,  # Invalid action
                )
            )

        self.assertIn("Invalid position action", str(context.exception))

    @aioresponses()
    def test_user_stream_update_for_new_order(self, mock_api):
        """Test that user stream order updates are properly processed."""
        client_order_id = "test_order_1"

        # Mock order placement
        order_response = {
            "order": {
                "orderId": "12345",
                "clientOrderId": client_order_id,
                "symbol": self.symbol,
                "status": "NEW",
                "side": "Buy",
                "orderType": "Limit",
                "price": "50000.00",
                "quantity": "0.1",
                "timestamp": 1640780000000
            }
        }

        order_url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_URL)
        mock_api.post(order_url, body=json.dumps(order_response))

        # Simulate user stream update
        user_stream_update = {
            "type": "order",
            "data": order_response["order"]
        }

        self.exchange._process_order_message(user_stream_update)

        # Verify order tracking
        tracked_orders = self.exchange.in_flight_orders
        self.assertEqual(len(tracked_orders), 1)

    @aioresponses()
    def test_user_stream_update_for_position_update(self, mock_api):
        """Test that user stream position updates are properly processed."""
        position_update = {
            "type": "position",
            "data": {
                "symbol": self.symbol,
                "side": "LONG",
                "size": "0.5",
                "entryPrice": "45000.00",
                "markPrice": "46000.00",
                "unrealizedPnl": "500.00",
                "realizedPnl": "0.00",
                "margin": "450.00",
                "liquidationPrice": "40000.00",
                "leverage": "10"
            }
        }

        self.exchange._process_position_message(position_update)

        # Verify position was updated
        position_key = self.exchange._perpetual_trading.position_key(self.trading_pair)
        self.assertIn(position_key, self.exchange._perpetual_trading._account_positions)
        position = self.exchange._perpetual_trading._account_positions[position_key]
        self.assertEqual(position.amount, Decimal("0.5"))

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        """Test that lost orders are removed when not found on exchange."""
        client_order_id = "test_order_1"
        exchange_order_id = "12345"

        # Create in-flight order
        order = InFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("50000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.start_timestamp - 1000,  # Old order
        )
        self.exchange._order_tracker.start_tracking_order(order)

        # Mock order not found response
        order_url = web_utils.private_rest_url(path_url=CONSTANTS.ORDER_URL)
        mock_api.get(order_url, status=404, body=json.dumps({"error": "Order not found"}))

        self.async_run_with_timeout(
            self.exchange._update_order_status()
        )

        # Verify order was marked as failed
        self.assertTrue(order.is_failure)

    @aioresponses()
    def test_format_trading_rules(self, mock_api):
        """Test that trading rules are properly formatted from exchange info."""
        exchange_info = {
            "symbols": [
                {
                    "symbol": self.symbol,
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "minPrice": "0.01",
                    "maxPrice": "1000000.00",
                    "tickSize": "0.01",
                    "minQuantity": "0.001",
                    "maxQuantity": "1000.000",
                    "stepSize": "0.001",
                    "minNotional": "10.00"
                }
            ]
        }

        url = self.all_symbols_url
        mock_api.get(url, body=json.dumps(exchange_info))

        self.async_run_with_timeout(self.exchange._update_trading_rules())

        trading_rule = self.exchange._trading_rules[self.trading_pair]

        self.assertEqual(trading_rule.min_order_size, Decimal("0.001"))
        self.assertEqual(trading_rule.max_order_size, Decimal("1000"))
        self.assertEqual(trading_rule.min_price_increment, Decimal("0.01"))
        self.assertEqual(trading_rule.min_base_amount_increment, Decimal("0.001"))
        self.assertEqual(trading_rule.min_notional_size, Decimal("10"))

    @aioresponses()
    def test_get_last_trade_prices(self, mock_api):
        """Test fetching last trade prices."""
        ticker_response = {
            "symbol": self.symbol,
            "lastPrice": "50000.00",
            "markPrice": "50001.00",
            "indexPrice": "50000.50",
            "fundingRate": "0.0001",
            "nextFundingTime": 1640790000000
        }

        url = self.latest_prices_url
        mock_api.get(url, body=json.dumps(ticker_response))

        prices = self.async_run_with_timeout(
            self.exchange.get_last_traded_prices([self.trading_pair])
        )

        self.assertEqual(prices[self.trading_pair], Decimal("50000"))

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        """Configure mock response for order not found during cancellation."""
        url = web_utils.private_rest_url(path_url=CONSTANTS.CANCEL_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?") + ".*")
        
        # Backpack returns error code -2011 for unknown order
        response = {
            "code": CONSTANTS.UNKNOWN_ORDER_ERROR_CODE,
            "msg": CONSTANTS.UNKNOWN_ORDER_MESSAGE
        }
        
        mock_api.delete(
            regex_url,
            body=json.dumps(response),
            callback=callback,
            status=400
        )
        return url

    @aioresponses()
    async def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        """Test that order not found during cancellation is handled correctly."""
        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()
        
        self.exchange.start_tracking_order(
            order_id="HBOT1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        
        self.assertIn("HBOT1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders["HBOT1"]
        
        self.configure_order_not_found_error_cancelation_response(
            order=order, mock_api=mock_api, callback=lambda *args, **kwargs: request_sent_event.set()
        )
        
        self.exchange.cancel(trading_pair=self.trading_pair, client_order_id="HBOT1")
        await request_sent_event.wait()
        
        # When order is not found, it should NOT be marked as cancelled
        # The connector should handle it as per _is_order_not_found_during_cancelation_error
        self.assertFalse(order.is_done)
        self.assertFalse(order.is_failure)
        self.assertFalse(order.is_cancelled)
        self.assertIn(order.client_order_id, self.exchange._order_tracker.all_updatable_orders)
        self.assertEqual(1, self.exchange._order_tracker._order_not_found_records[order.client_order_id])

    def test_get_buy_and_sell_collateral_tokens(self):
        """Test getting collateral tokens for buy and sell."""
        buy_collateral = self.exchange.get_buy_collateral_token(self.trading_pair)
        sell_collateral = self.exchange.get_sell_collateral_token(self.trading_pair)

        # For perpetuals, collateral is typically the quote asset
        self.assertEqual(buy_collateral, self.quote_asset)
        self.assertEqual(sell_collateral, self.quote_asset)

    @aioresponses()
    def test_funding_payment_polling_loop_sends_update_event(self, mock_api):
        """Test that funding payment polling loop sends proper events."""
        funding_rate_response = {
            "symbol": self.symbol,
            "fundingRate": "0.0001",
            "fundingFee": "0.50",
            "timestamp": 1640780000000
        }

        funding_url = web_utils.private_rest_url(path_url=CONSTANTS.FUNDING_HISTORY_URL)
        mock_api.get(funding_url, body=json.dumps([funding_rate_response]))

        # Start funding payment polling
        task = self.local_event_loop.create_task(
            self.exchange._funding_payment_polling_loop()
        )

        # Wait for one iteration
        self.async_run_with_timeout(asyncio.sleep(0.5))

        # Cancel the task
        task.cancel()

        # Verify funding payment was recorded in the event logger
        self.assertEqual(1, len(self.funding_payment_completed_logger.event_log))
        funding_event = self.funding_payment_completed_logger.event_log[0]
        self.assertEqual(self.trading_pair, funding_event.trading_pair)
        self.assertEqual(Decimal("0.5"), funding_event.amount)  # From the mock response

    @aioresponses()
    def test_set_position_mode_initial_mode_is_none(self, mock_api):
        """Test setting position mode when initial mode is None."""
        # Initially position mode is None
        self.assertIsNone(self.exchange.position_mode)

        # Mock API response for setting position mode
        url = self.funding_info_url()
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        mock_response = {
            "success": True,
            "positionMode": "ONE_WAY"
        }
        mock_api.post(regex_url, body=json.dumps(mock_response))

        # Set position mode
        self.async_run_with_timeout(
            self.exchange._set_position_mode(PositionMode.ONEWAY)
        )

        self.assertEqual(self.exchange.position_mode, PositionMode.ONEWAY)

    @aioresponses()
    def test_set_position_mode_unchanged(self, mock_api):
        """Test setting position mode when it's already set to the same mode."""
        # Set initial position mode
        self.exchange._position_mode = PositionMode.ONEWAY

        # No API call should be made
        self.async_run_with_timeout(
            self.exchange._set_position_mode(PositionMode.ONEWAY)
        )

        # Position mode should remain the same
        self.assertEqual(self.exchange.position_mode, PositionMode.ONEWAY)

    @aioresponses()
    def test_margin_call_event(self, mock_api):
        """Test margin call event handling."""
        # Create a position near liquidation
        position_data = {
            "symbol": self.ex_trading_pair,
            "side": "LONG",
            "size": "0.100",
            "entryPrice": "50000.00",
            "markPrice": "45500.00",  # Near liquidation
            "liquidationPrice": "45000.00",
            "marginRatio": "0.95"  # High margin ratio - danger zone
        }

        # Process margin call warning
        self.exchange._process_position_message(position_data)

        # Check if warning was logged
        self.assertTrue(
            self._is_logged(
                "WARNING",
                f"Margin call warning for {self.trading_pair}: Margin ratio at 95.00%"
            )
        )

    @aioresponses()
    def test_order_fill_event_ignored_for_repeated_trade_id(self, mock_api):
        """Test that repeated trade IDs are ignored."""
        # Create an order
        order = InFlightOrder(
            client_order_id="HBOT-123456",
            exchange_order_id="123456789",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("50000"),
            amount=Decimal("0.01"),
            creation_timestamp=1641288825000,
        )
        self.exchange._order_tracker.start_tracking_order(order)

        # First fill event
        fill_data = {
            "orderId": "123456789",
            "tradeId": "9876543210",
            "symbol": self.ex_trading_pair,
            "side": "Buy",
            "price": "50000.00",
            "quantity": "0.005",
            "fee": "0.025",
            "feeAsset": "USDC",
            "timestamp": 1641288826000,
        }

        # Process fill data through order message handler  
        self.exchange._process_order_message(fill_data)

        # Check that fill was recorded
        self.assertEqual(len(order.order_fills), 1)

        # Send the same fill again (duplicate trade ID)
        # Process fill data through order message handler  
        self.exchange._process_order_message(fill_data)

        # Should still have only 1 fill
        self.assertEqual(len(order.order_fills), 1)

    @aioresponses()
    def test_fee_is_zero_when_not_included_in_fill_event(self, mock_api):
        """Test that fee defaults to zero when not included in fill event."""
        # Create an order
        order = InFlightOrder(
            client_order_id="HBOT-123456",
            exchange_order_id="123456789",
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("50000"),
            amount=Decimal("0.01"),
            creation_timestamp=1641288825000,
        )
        self.exchange._order_tracker.start_tracking_order(order)

        # Fill event without fee
        fill_data = {
            "orderId": "123456789",
            "tradeId": "9876543210",
            "symbol": self.ex_trading_pair,
            "side": "Buy",
            "price": "50000.00",
            "quantity": "0.01",
            # No fee field
            "timestamp": 1641288826000,
        }

        # Process fill data through order message handler  
        self.exchange._process_order_message(fill_data)

        # Check that fill was recorded with zero fee
        self.assertEqual(len(order.order_fills), 1)
        fill = list(order.order_fills.values())[0]
        self.assertEqual(fill.fee.flat_fees[0].amount, Decimal("0"))

    @aioresponses()
    def test_user_stream_event_listener_raises_cancelled_error(self, mock_api):
        """Test that cancelled error is properly handled in user stream listener."""
        # Mock a cancelled error during event listening
        self.exchange._user_stream_tracker.user_stream.get = AsyncMock(
            side_effect=asyncio.CancelledError
        )

        # Start listening task
        listening_task = self.local_event_loop.create_task(
            self.exchange._user_stream_event_listener()
        )

        # Should handle cancellation gracefully
        self.async_run_with_timeout(asyncio.sleep(0.1))
        listening_task.cancel()

        # No exception should propagate
        try:
            self.async_run_with_timeout(listening_task)
        except asyncio.CancelledError:
            pass  # Expected

    @aioresponses()
    def test_wrong_symbol_position_detected_on_positions_update(self, mock_api):
        """Test that positions for wrong symbols are ignored."""
        url = self.balance_url
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        positions_data = [
            {
                "symbol": "ETH_PERP",  # Wrong symbol
                "side": "LONG",
                "size": "1.0",
                "entryPrice": "3000.00",
                "markPrice": "3100.00",
            },
            {
                "symbol": self.ex_trading_pair,  # Correct symbol
                "side": "SHORT",
                "size": "0.05",
                "entryPrice": "51000.00",
                "markPrice": "50000.00",
            }
        ]

        mock_api.get(regex_url, body=json.dumps({"positions": positions_data}))

        self.async_run_with_timeout(self.exchange._update_positions())

        # Should only have position for our trading pair
        self.assertEqual(len(self.exchange._perpetual_trading._account_positions), 1)
        position_key = self.exchange._perpetual_trading.position_key(self.trading_pair)
        self.assertIn(position_key, self.exchange._perpetual_trading._account_positions)
        eth_position_key = self.exchange._perpetual_trading.position_key("ETH-USDC")
        self.assertNotIn(eth_position_key, self.exchange._perpetual_trading._account_positions)

    @aioresponses()
    def test_account_position_updated_on_positions_update(self, mock_api):
        """Test that existing positions are updated correctly."""
        # Create initial position
        initial_position = {
            "symbol": self.ex_trading_pair,
            "side": "LONG",
            "size": "0.1",
            "entryPrice": "50000.00",
            "markPrice": "50000.00",
        }

        # Set the position using the perpetual trading object
        position_key = self.exchange._perpetual_trading.position_key(self.trading_pair)
        self.exchange._perpetual_trading._account_positions[position_key] = Position(
            trading_pair=self.trading_pair,
            position_side=PositionSide.LONG,
            unrealized_pnl=Decimal("0"),
            entry_price=Decimal("50000"),
            amount=Decimal("0.1"),
            leverage=Decimal("10"),
        )

        # Update with new data - positions endpoint
        url = web_utils.private_rest_url(path_url=CONSTANTS.POSITIONS_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        updated_position = {
            "symbol": self.ex_trading_pair,
            "side": "Long",  # Backpack uses capitalized sides
            "netQuantity": "0.2",  # Changed from size to netQuantity
            "entryPrice": "49500.00",  # Entry price changed
            "markPrice": "51000.00",  # Mark price changed
            "pnlUnrealized": "300.00",  # Backpack uses pnlUnrealized with capital U
        }

        mock_api.get(regex_url, body=json.dumps({"positions": [updated_position]}))

        self.async_run_with_timeout(self.exchange._update_positions())

        # Check position was updated using proper API
        position_key = self.exchange._perpetual_trading.position_key(self.trading_pair)
        position = self.exchange._perpetual_trading._account_positions[position_key]
        self.assertEqual(position.amount, Decimal("0.2"))
        self.assertEqual(position.entry_price, Decimal("49500"))
        self.assertEqual(position.unrealized_pnl, Decimal("300"))

    @aioresponses()
    def test_new_account_position_detected_on_positions_update(self, mock_api):
        """Test that new positions are detected and added."""
        # Start with no positions
        self.exchange._perpetual_trading._account_positions.clear()

        url = self.balance_url
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        new_position = {
            "symbol": self.ex_trading_pair,
            "side": "SHORT",
            "size": "0.05",
            "entryPrice": "52000.00",
            "markPrice": "51000.00",
            "unrealizedPnl": "50.00",
        }

        mock_api.get(regex_url, body=json.dumps({"positions": [new_position]}))

        self.async_run_with_timeout(self.exchange._update_positions())

        # Check new position was added
        position_key = self.exchange._perpetual_trading.position_key(self.trading_pair)
        self.assertIn(position_key, self.exchange._perpetual_trading._account_positions)
        position = self.exchange._perpetual_trading._account_positions[position_key]
        self.assertEqual(position.position_side, PositionSide.SHORT)
        self.assertEqual(position.amount, Decimal("-0.05"))  # Negative for short
        self.assertEqual(position.unrealized_pnl, Decimal("50"))
