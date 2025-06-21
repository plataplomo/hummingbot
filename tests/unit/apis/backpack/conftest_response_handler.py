"""Shared fixtures for BackpackResponseHandler unit tests."""

from typing import Any

import pytest


@pytest.fixture
def symbol_spot() -> str:
    """Return spot trading symbol for testing."""
    return "SOL_USDC"


@pytest.fixture
def symbol_perp() -> str:
    """Return perpetual trading symbol for testing."""
    return "SOL-PERP"


@pytest.fixture
def symbol_any() -> str:
    """Return generic symbol fixture for tests not specific to spot/perp."""
    return "GENERIC_SYMBOL"


@pytest.fixture
def order_id() -> str:
    """Return order ID for testing."""
    return "987654321"


@pytest.fixture
def client_id() -> str:
    """Return client ID for testing."""
    return "clientOrder001"


@pytest.fixture
def valid_raw_ticker(symbol_spot: str) -> dict[str, Any]:
    """Return valid raw ticker for testing."""
    return {
        "symbol": symbol_spot,
        "firstPrice": "140.00",
        "lastPrice": "140.50",
        "high": "141.00",
        "low": "139.50",
        "priceChange": "0.50",
        "priceChangePercent": "0.36",
        "volume": "500000.0",
        "quoteVolume": "70250000.0",
        "trades": "1250",
    }


@pytest.fixture
def valid_raw_order_book(symbol_spot: str) -> dict[str, Any]:
    """Return valid raw order book for testing."""
    return {
        "bids": [["140.10", "10"], ["140.00", "20"]],
        "asks": [["140.20", "15"], ["140.30", "25"]],
        "lastUpdateId": "update123",
        "timestamp": 1678886401000,
    }


@pytest.fixture
def valid_raw_trade_item(symbol_spot: str) -> dict[str, Any]:
    """Return valid raw trade item for testing."""
    return {
        "id": 1001,
        "isBuyerMaker": False,
        "price": "141.00",
        "quantity": "1.5",
        "quoteQuantity": "211.50",
        "timestamp": 1678886402000,
    }


@pytest.fixture
def valid_raw_recent_trades(valid_raw_trade_item: dict[str, Any]) -> list[dict[str, Any]]:
    """Return valid raw recent trades for testing."""
    item1 = valid_raw_trade_item.copy()
    item2 = valid_raw_trade_item.copy()
    item2["id"] = 1002
    item2["price"] = "141.01"
    item2["quantity"] = "0.5"
    item2["quoteQuantity"] = "70.505"
    item2["timestamp"] = 1678886403000
    return [item1, item2]


@pytest.fixture
def valid_raw_market_data() -> list[list[Any]]:
    """Return valid raw market data for testing."""
    return [
        [
            1678886400000,
            "138.0",
            "139.5",
            "137.5",
            "139.0",
            "1000.0",
            1678886459999,
            "500000.0",
            100,
            "250000.0",
            "125000.0",
            "0",
        ],
        [
            1678886460000,
            "139.0",
            "140.0",
            "138.5",
            "139.8",
            "1200.0",
            1678886519999,
            "600000.0",
            120,
            "300000.0",
            "150000.0",
            "0",
        ],
    ]


@pytest.fixture
def valid_raw_historical_trades(symbol_spot: str) -> list[dict[str, Any]]:
    """Return valid raw historical trades for testing."""
    trade1 = {
        "id": "1001",
        "orderId": "histOrderA",
        "symbol": symbol_spot,
        "price": "135.00",
        "qty": "2.0",
        "time": 1678880000000,
    }
    trade2 = {
        "id": "1002",
        "orderId": "histOrderB",
        "symbol": symbol_spot,
        "price": "135.10",
        "qty": "1.0",
        "time": 1678880100000,
    }
    return [trade1, trade2]


@pytest.fixture
def valid_raw_balance_item() -> dict[str, Any]:
    """Return valid raw balance item for testing."""
    return {
        "available": "10.5",
        "locked": "2.0",
        "staked": "0",
    }


@pytest.fixture
def valid_raw_balances(valid_raw_balance_item: dict[str, Any]) -> dict[str, Any]:
    """Return valid raw balances for testing."""
    usdc_item = {
        "available": "1000.0",
        "locked": "50.0",
        "staked": "0",
    }
    return {"SOL": valid_raw_balance_item, "USDC": usdc_item}


@pytest.fixture
def valid_raw_position_item(symbol_spot: str) -> dict[str, Any]:
    """Return valid raw position item for testing."""
    return {
        "symbol": symbol_spot,
        "breakEvenPrice": "131.00",
        "entryPrice": "130.00",
        "estLiquidationPrice": "120.00",
        "imf": "0.1",
        "imfFunction": {"base": "0.005", "factor": "0.000001"},
        "markPrice": "135.00",
        "mmf": "0.05",
        "mmfFunction": {"base": "0.002", "factor": "0.0000005"},
        "netCost": "325.00",
        "netQuantity": "2.5",
        "netExposureQuantity": "2.5",
        "netExposureNotional": "337.50",
        "pnlRealized": "10.00",
        "pnlUnrealized": "12.50",
        "cumulativeFundingPayment": "-0.50",
        "userId": 1,
        "positionId": "pos123",
        "cumulativeInterest": "0.0",
        "subaccountId": 0,
    }


@pytest.fixture
def valid_raw_positions(valid_raw_position_item: dict[str, Any]) -> list[dict[str, Any]]:
    """Return valid raw positions for testing."""
    item2 = valid_raw_position_item.copy()
    item2["symbol"] = "BTC_USDT"
    item2["breakEvenPrice"] = "54900.00"
    item2["entryPrice"] = "55000.00"
    item2["estLiquidationPrice"] = "60000.00"
    item2["markPrice"] = "54000.00"
    item2["netCost"] = "-5500.00"
    item2["netQuantity"] = "-0.1"
    item2["netExposureQuantity"] = "-0.1"
    item2["netExposureNotional"] = "-5400.00"
    item2["pnlRealized"] = "50.00"
    item2["pnlUnrealized"] = "100.00"
    item2["cumulativeFundingPayment"] = "1.20"
    item2["positionId"] = "pos456"
    return [valid_raw_position_item, item2]


@pytest.fixture
def valid_raw_account_summary() -> dict[str, Any]:
    """Return valid raw account summary for testing."""
    return {
        "autoBorrowSettlements": True,
        "autoLend": False,
        "autoRealizePnl": True,
        "autoRepayBorrows": True,
        "borrowLimit": "100000.0",
        "futuresMakerFee": "0.0002",
        "futuresTakerFee": "0.0005",
        "leverageLimit": "20.0",
        "limitOrders": 50,
        "liquidating": False,
        "positionLimit": "500000.0",
        "spotMakerFee": "0.0008",
        "spotTakerFee": "0.0010",
        "triggerOrders": 20,
    }


@pytest.fixture
def valid_raw_order(order_id: str, client_id: str, symbol_spot: str) -> dict[str, Any]:
    """Return valid raw order for testing."""
    return {
        "id": order_id,
        "clientId": client_id,
        "symbol": symbol_spot,
        "side": "buy",
        "orderType": "LIMIT",
        "quantity": "10.0",
        "price": "140.00",
        "timeInForce": "GTC",
        "status": "NEW",
        "createdAt": 1678886405000,
        "executedQuantity": "0",
        "avgFillPrice": None,
    }


@pytest.fixture
def valid_raw_open_orders(valid_raw_order: dict[str, Any]) -> list[dict[str, Any]]:
    """Return valid raw open orders for testing."""
    item2 = valid_raw_order.copy()
    item2["id"] = "order002"
    item2["symbol"] = "BTC_USDT"
    item2["side"] = "sell"
    item2["quantity"] = "0.1"
    item2["price"] = "56000.00"
    item2["createdAt"] = 1678886411000
    return [valid_raw_order, item2]


@pytest.fixture
def valid_raw_funding_rate(symbol_perp: str) -> dict[str, Any]:
    """Return valid raw funding rate for testing."""
    return {
        "symbol": symbol_perp,
        "rate": "0.000123",
        "markPrice": "140.00",
        "indexPrice": "139.90",
        "time": 1678887000000,
    }


@pytest.fixture
def valid_raw_withdrawal() -> dict[str, Any]:
    """Return valid raw withdrawal for testing."""
    return {
        "id": 12345,
        "blockchain": "Solana",
        "quantity": "100.0",
        "fee": "0.01",
        "symbol": "USDC",
        "status": "confirmed",
        "toAddress": "SOLANA_ADDRESS_HERE",
        "createdAt": "2023-03-15T10:00:00.000Z",
        "isInternal": False,
    }


@pytest.fixture
def valid_raw_order_history(valid_raw_order: dict[str, Any]) -> list[dict[str, Any]]:
    """Return valid raw order history for testing."""
    item1 = valid_raw_order.copy()
    item1["id"] = "histOrder001"
    item1["status"] = "FILLED"
    item1["executedQuantity"] = "10.0"
    item1["avgFillPrice"] = "140.00"
    item1["createdAt"] = 1678886000000

    item2 = valid_raw_order.copy()
    item2["id"] = "histOrder002"
    item2["symbol"] = "BTC_USDT"
    item2["side"] = "sell"
    item2["orderType"] = "MARKET"
    item2["quantity"] = "0.2"
    item2["price"] = None
    item2["timeInForce"] = "IOC"
    item2["status"] = "FILLED"
    item2["executedQuantity"] = "0.2"
    item2["avgFillPrice"] = "55950.00"
    item2["createdAt"] = 1678886100000

    return [item1, item2]


@pytest.fixture
def valid_raw_trade_history(symbol_spot: str) -> list[dict[str, Any]]:
    """Return valid raw trade history for testing."""
    trade1 = {
        "symbol": symbol_spot,
        "price": "141.00",
        "qty": "1.5",
        "time": 1678886000000,
        "id": "tradeHist001",
        "orderId": "histOrderX001",
    }
    trade2 = {
        "symbol": symbol_spot,
        "price": "141.05",
        "qty": "0.75",
        "time": 1678886001000,
        "id": "tradeHist002",
        "orderId": "histOrderX002",
    }
    return [trade1, trade2]


@pytest.fixture
def valid_raw_order_status(valid_raw_order: dict[str, Any]) -> dict[str, Any]:
    """Return valid raw order status for testing."""
    order_copy = valid_raw_order.copy()
    order_copy["id"] = "statusOrder123"
    order_copy["clientId"] = "clientStatus001"
    order_copy["side"] = "sell"
    order_copy["price"] = "142.00"
    order_copy["status"] = "FILLED"
    order_copy["createdAt"] = 1678889000000
    order_copy["executedQuantity"] = "10.0"
    order_copy["avgFillPrice"] = "142.00"
    return order_copy
