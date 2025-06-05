"""Shared fixtures for HyperliquidResponseHandler tests."""

from typing import Any

import pytest


@pytest.fixture
def user_address() -> str:
    """Provide user address for testing."""
    return "0xTestUserAddress1234567890abcdef"


@pytest.fixture
def symbol() -> str:
    """Provide symbol for testing."""
    return "ETH-PERP"


@pytest.fixture
def order_id() -> int:
    """Provide order id for testing."""
    return 98765


@pytest.fixture
def valid_raw_exchange_status_object_resting() -> dict[str, Any]:
    """Return valid raw exchange status object resting for testing."""
    return {"resting": {"oid": 12345}}


@pytest.fixture
def valid_raw_exchange_response(
    valid_raw_exchange_status_object_resting: dict[str, Any],
) -> dict[str, Any]:
    """Return valid raw exchange response for testing."""
    return {
        "status": "ok",
        "data": {
            "type": "order",
            "statuses": [valid_raw_exchange_status_object_resting, "canceled"],
        },
    }


@pytest.fixture
def valid_raw_meta_and_asset_ctxs() -> list[Any]:
    """Return valid raw meta and asset ctxs for testing."""
    return [
        {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 100, "onlyIsolated": False},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 80, "onlyIsolated": False},
            ],
        },
        [
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "55000.0",
                "prevDayPx": "54000.0",
                "dayNtlVlm": "1000000000.0",
                "impactPx": "55010.0",
            },
            {
                "name": "ETH",
                "funding": "0.0002",
                "markPx": "3000.0",
                "prevDayPx": "2950.0",
                "dayNtlVlm": "500000000.0",
                "impactPx": "3005.0",
            },
        ],
    ]


@pytest.fixture
def valid_raw_user_state() -> dict[str, Any]:
    """Return valid raw user state for testing."""
    return {
        "assetPositions": [
            {
                "asset": "ETH-PERP",
                "position": {
                    "coin": "ETH-PERP",
                    "szi": "1.0",
                    "entryPx": "3000.0",
                    "leverage": {"type": "cross", "value": 10},
                    "liquidationPx": "2700.0",
                    "marginUsed": "300.0",
                    "maxLeverage": 50,
                    "positionValue": "3000.0",
                    "returnOnEquity": "0.0",
                    "unrealizedPnl": "0.0",
                },
            },
        ],
        "crossMaintenanceMarginUsed": "30.0",
        "crossMarginSummary": {
            "accountValue": "5000.0",
            "totalMarginUsed": "300.0",
            "totalNtlPos": "3000.0",
            "totalRawUsd": "4700.0",
        },
        "marginSummary": {
            "accountValue": "5000.0",
            "totalMarginUsed": "300.0",
            "totalNtlPos": "3000.0",
            "totalRawUsd": "4700.0",
        },
        "isolatedMaintenanceMarginUsed": "0.0",
        "isolatedMarginSummary": {
            "accountValue": "0.0",
            "totalMarginUsed": "0.0",
            "totalNtlPos": "0.0",
            "totalRawUsd": "0.0",
        },
        "withdrawable": "4700.0",
    }


@pytest.fixture
def valid_raw_open_order_item() -> dict[str, Any]:
    """Return valid raw open order item for testing."""
    return {
        "order": {
            "asset": "ETH-PERP",
            "limitPx": "3000.0",
            "oid": 6001,
            "reduceOnly": False,
            "side": "B",
            "sz": "0.5",
            "timestamp": 1678889600000,
            "orderType": {"limit": {"tif": "Gtc"}},
            "remainingSz": "0.5",
            "status": "open",
            "statusTimestamp": 1678889601000,
            "cloid": "clientOpen1",
        },
        "trigger": None,
    }


@pytest.fixture
def valid_raw_user_fill() -> dict[str, Any]:
    """Return valid raw user fill for testing."""
    return {
        "tid": 1001,
        "coin": "ETH-PERP",
        "px": "3000.1",
        "sz": "0.5",
        "time": 1678889800000,
        "side": "B",
        "oid": 6001,
        "startPosition": "0.0",
        "dir": "Open Long",
        "hash": "0xfillhash1",
        "fee": "1.5",
        "isMaker": False,
        "liquidationMarkPx": None,
        "cloid": "clientFill1",
    }


@pytest.fixture
def valid_raw_asset_ctx() -> dict[str, Any]:
    """Return valid raw asset ctx for testing."""
    return {
        "name": "ETH-PERP",
        "markPx": "3010.00",
        "funding": "0.00015",
        "prevDayPx": "2990.00",
        "dayNtlVlm": "50000000.0",
        "impactPx": "3011.00",
    }


@pytest.fixture
def valid_raw_l2_book() -> dict[str, Any]:
    """Return valid raw l2 book for testing."""
    return {
        "coin": "ETH-PERP",
        "levels": [
            [{"px": "2999.0", "sz": "10.5", "n": 5}, {"px": "2998.0", "sz": "20.0", "n": 8}],
            [{"px": "3001.0", "sz": "5.2", "n": 3}, {"px": "3002.0", "sz": "15.8", "n": 6}],
        ],
        "time": 1678889300000,
    }


@pytest.fixture
def valid_raw_public_trade() -> dict[str, Any]:
    """Return valid raw public trade for testing."""
    return {
        "coin": "ETH-PERP",
        "side": "B",
        "px": "3005.0",
        "sz": "0.1",
        "time": 1678889400000,
        "hash": "0xtradeHashValid",
    }


@pytest.fixture
def valid_raw_candle() -> dict[str, Any]:
    """Return valid raw candle for testing."""
    return {
        "t": 1678889500000,
        "o": "3000.0",
        "h": "3015.0",
        "l": "2995.0",
        "c": "3010.0",
        "v": "100.5",
        "n": 50,
    }


@pytest.fixture
def valid_raw_candle_snapshot() -> dict[str, Any]:
    """Return valid raw candle snapshot for testing."""
    # HyperliquidRawCandleSnapshot expects parallel arrays, not a list of candle dicts
    return {
        "t": [1672531200000, 1672531260000],
        "o": ["1200.0", "1201.0"],
        "h": ["1250.0", "1205.0"],
        "l": ["1190.0", "1198.0"],
        "c": ["1240.0", "1202.0"],
        "v": ["1000.0", "500.0"],
        "s": "ok",
    }


@pytest.fixture
def valid_raw_historical_funding_rates_data() -> list[dict[str, Any]]:
    """Return valid raw historical funding rates data for testing."""
    return [
        {"coin": "ETH", "fundingRate": "0.000123", "premium": "0.0001", "time": 1678886400000},
        {"coin": "BTC", "fundingRate": "-0.00005", "premium": "-0.00003", "time": 1678882800000},
    ]


@pytest.fixture
def valid_raw_historical_order_response() -> dict[str, Any]:
    """Return valid raw historical order response for testing."""
    return {
        "order": {
            "asset": "ETH-PERP",
            "limitPx": "2900.0",
            "oid": 7001,
            "reduceOnly": False,
            "side": "B",
            "sz": "1.0",
            "timestamp": 1678890000000,
            "orderType": {"limit": {"tif": "Gtc"}},
            "remainingSz": "0.0",
            "status": "filled",
            "statusTimestamp": 1678890001000,
            "cloid": "histClient1",
        },
    }


@pytest.fixture
def valid_raw_vault_details() -> dict[str, Any]:
    """Return valid raw vault details for testing."""
    # Placeholder structure - adjust based on actual API/model
    return {
        "name": "Test Vault",
        "totalValueLockedUSD": "1000000.0",
        "sharePrice": "1.05",
        "userBalance": "500.0",
        # ... other expected fields
    }
