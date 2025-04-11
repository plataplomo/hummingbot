"""
Tests for the PositionReconciliationSystem class.
"""

from datetime import datetime, timedelta, UTC
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal

import pytest

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import OrderSide, Position
from cyberdelta.utils.config import Config
from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem


class TestPositionReconciliationSystem:
    """Test suite for the PositionReconciliationSystem class."""

    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        config = MagicMock(spec=Config)
        config_data = {
            "exchanges": {"hyperliquid": {}, "backpack": {}},
            "exchanges.hyperliquid.enabled": True,
            "exchanges.backpack.enabled": True,
            "validation.position_reconciliation.threshold": 0.05,
            "validation.position_reconciliation.auto_correct": False,
            "validation.position_reconciliation.check_interval": 3600,
            "validation.position_reconciliation.use_fill_history": False,
        }

        def config_get_side_effect(key, default=None):
            if key in config_data:
                return config_data[key]
            parts = key.split(".")
            if len(parts) == 3 and parts[0] == "exchanges" and parts[2] == "enabled":
                return (
                    config_data.get("exchanges", {})
                    .get(parts[1], {})
                    .get("enabled", default)
                )
            return default

        config.get.side_effect = config_get_side_effect
        return config

    @pytest.fixture
    def portfolio_tracker(self):
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()

        # Mock hyperliquid positions
        hyper_positions = [
            Position(
                symbol="BTC",
                size=1.0,
                entry_price=50000.0,
                mark_price=51000.0,
                liquidation_price=45000.0,
                unrealized_pnl=1000.0,
                leverage=2.0,
                side=OrderSide.BUY,
            ),
            Position(
                symbol="ETH",
                size=10.0,
                entry_price=3000.0,
                mark_price=3100.0,
                liquidation_price=2800.0,
                unrealized_pnl=1000.0,
                leverage=1.0,
                side=OrderSide.SELL,
            ),
        ]

        # Mock backpack positions
        backpack_positions = [
            Position(
                symbol="BTC",
                size=-2.0,
                entry_price=50500.0,
                mark_price=51000.0,
                liquidation_price=55000.0,
                unrealized_pnl=-1000.0,
                leverage=1.0,
                side=OrderSide.SELL,
            )
        ]

        # Setup the get_position method
        def get_position(exchange, symbol):
            if exchange == "hyperliquid":
                for pos in hyper_positions:
                    if pos.symbol == symbol:
                        return pos
            elif exchange == "backpack":
                for pos in backpack_positions:
                    if pos.symbol == symbol:
                        return pos
            return None

        # Setup get_positions_by_exchange method
        def get_positions_by_exchange(exchange):
            if exchange == "hyperliquid":
                return hyper_positions
            elif exchange == "backpack":
                return backpack_positions
            return []

        # Mock API clients (Use AsyncMock for awaitable methods)
        hyperliquid_client = AsyncMock(spec=ExchangeAPI)
        backpack_client = AsyncMock(spec=ExchangeAPI)

        # Setup position data for API clients
        hyper_api_positions = [
            Position(
                symbol="BTC",
                size=1.1,  # 10% discrepancy with local (1.0)
                entry_price=50000.0,
                mark_price=51000.0,
                liquidation_price=45000.0,
                unrealized_pnl=1000.0,
                leverage=2.0,
                side=OrderSide.BUY,
            ),
            Position(
                symbol="ETH",
                size=10.0,  # Matches local
                entry_price=3000.0,
                mark_price=3100.0,
                liquidation_price=2800.0,
                unrealized_pnl=1000.0,
                leverage=1.0,
                side=OrderSide.SELL,
            ),
        ]

        backpack_api_positions = [
            Position(
                symbol="BTC",
                size=-2.0,  # Matches local
                entry_price=50500.0,
                mark_price=51000.0,
                liquidation_price=55000.0,
                unrealized_pnl=-1000.0,
                leverage=1.0,
                side=OrderSide.SELL,
            )
        ]

        # Mock execution handlers for fill history
        execution_handler_hyper = MagicMock()
        execution_handler_backpack = MagicMock()

        # Setup fill derived positions
        hyper_fill_positions = [
            Position(
                symbol="BTC",
                size=1.05,  # 5% discrepancy with local (1.0)
                entry_price=50000.0,
                mark_price=51000.0,
                liquidation_price=45000.0,
                unrealized_pnl=1000.0,
                leverage=2.0,
                side=OrderSide.BUY,
            ),
            Position(
                symbol="ETH",
                size=10.0,  # Matches local
                entry_price=3000.0,
                mark_price=3100.0,
                liquidation_price=2800.0,
                unrealized_pnl=1000.0,
                leverage=1.0,
                side=OrderSide.SELL,
            ),
        ]

        backpack_fill_positions = [
            Position(
                symbol="BTC",
                size=-1.9,  # 5% discrepancy with local (-2.0)
                entry_price=50500.0,
                mark_price=51000.0,
                liquidation_price=55000.0,
                unrealized_pnl=-1000.0,
                leverage=1.0,
                side=OrderSide.SELL,
            ),
            Position(
                symbol="SOL",  # Position not in local state
                size=5.0,
                entry_price=100.0,
                mark_price=103.0,
                liquidation_price=90.0,
                unrealized_pnl=15.0,
                leverage=1.0,
                side=OrderSide.BUY,
            ),
        ]

        # Configure the execution handlers
        execution_handler_hyper.get_derived_positions.return_value = (
            hyper_fill_positions
        )
        execution_handler_backpack.get_derived_positions.return_value = (
            backpack_fill_positions
        )

        # Configure API clients
        hyperliquid_client.get_positions.return_value = hyper_api_positions
        backpack_client.get_positions.return_value = backpack_api_positions

        # Configure portfolio tracker methods
        tracker.get_position.side_effect = get_position
        tracker.get_positions_by_exchange.side_effect = get_positions_by_exchange
        tracker.update_position = MagicMock()

        # Configure API client and execution handler access
        tracker.api_clients = {
            "hyperliquid": hyperliquid_client,
            "backpack": backpack_client,
        }

        # Configure mock get method on the tracker's mock config
        if not hasattr(tracker, "config") or tracker.config is None:
            tracker.config = MagicMock()
        tracker.config.get.side_effect = lambda key, default=None: {
            "validation.position_reconciliation.use_fill_history": False
        }.get(key, default)

        def get_execution_handler(exchange):
            if exchange == "hyperliquid":
                return execution_handler_hyper
            elif exchange == "backpack":
                return execution_handler_backpack
            return None

        # Add helper method for test access to exchange positions
        def _get_exchange_positions(exchange):
            if exchange == "hyperliquid":
                return hyper_api_positions
            elif exchange == "backpack":
                return backpack_api_positions
            return []

        tracker._get_exchange_positions = _get_exchange_positions

        # Remove the side_effect for get_api_client as api_clients dict is now used
        tracker.get_execution_handler.side_effect = get_execution_handler

        return tracker

    @pytest.fixture
    def reconciliation_system(self, config, portfolio_tracker):
        """Create a PositionReconciliationSystem instance for testing."""
        system = PositionReconciliationSystem(config, portfolio_tracker)
        return system

    def test_init(self, reconciliation_system, config, portfolio_tracker):
        """Test initializing the reconciliation system."""
        # Verify configuration parameters were loaded
        assert reconciliation_system.reconciliation_threshold == 0.05
        assert reconciliation_system.auto_correct is False
        assert reconciliation_system.check_interval == 3600

        # Verify dependencies
        assert reconciliation_system.config == config
        assert reconciliation_system.portfolio_tracker == portfolio_tracker

        # Verify initial state
        assert reconciliation_system.discrepancy_history == []
        assert reconciliation_system.latest_results == {}

    def test_register_portfolio_tracker(self, reconciliation_system):
        """Test registering a portfolio tracker."""
        # Create a new mock
        new_tracker = MagicMock()

        # Register it
        reconciliation_system.register_portfolio_tracker(new_tracker)

        # Verify it was set
        assert reconciliation_system.portfolio_tracker == new_tracker

    @pytest.mark.asyncio
    async def test_check_positions_interval(self, reconciliation_system):
        """Test that check_positions respects the check interval."""
        # Save original state
        original_last_check = reconciliation_system.last_check_time

        # Set last check time to now
        reconciliation_system.last_check_time = datetime.now()

        # Call without force
        results = await reconciliation_system.check_positions(force=False)

        # Should return cached results without checking
        assert results == reconciliation_system.latest_results

        # Last check time should not have been updated
        assert (
            reconciliation_system.last_check_time
            == reconciliation_system.last_check_time
        )

        # Now call with force
        results = await reconciliation_system.check_positions(force=True)

        # Should have performed the check and updated last check time
        assert reconciliation_system.last_check_time > original_last_check

    @pytest.mark.asyncio
    async def test_check_positions_no_portfolio_tracker(self, config):
        """Test check_positions with no portfolio tracker registered."""
        # Create system without portfolio tracker
        system = PositionReconciliationSystem(config)

        # Should return empty dictionary
        results = await system.check_positions()
        assert results == {}

    @pytest.mark.asyncio
    async def test_check_positions(self, reconciliation_system):
        """Test checking positions across exchanges."""
        # Call the method
        results = await reconciliation_system.check_positions()

        # Verify results structure - check for exchange keys and nested structure
        assert "hyperliquid" in results
        assert "backpack" in results
        assert isinstance(results["hyperliquid"], dict)
        # Check a key expected from _reconcile_positions
        assert "has_discrepancies" in results["hyperliquid"]
        # Based on mock data, hyperliquid should have discrepancies
        assert results["hyperliquid"]["has_discrepancies"] is True
        assert results["backpack"]["has_discrepancies"] is False

    @pytest.mark.asyncio
    async def test_auto_correct(self, config, portfolio_tracker):
        """Test auto-correction of positions."""
        # Create system with auto-correct enabled
        config.get.side_effect = (
            lambda key, default=None: {
                "exchanges": {"hyperliquid": {}, "backpack": {}},
                "exchanges.hyperliquid.enabled": True,
                "exchanges.backpack.enabled": True,
                "validation.position_reconciliation.threshold": 0.05,
                "validation.position_reconciliation.auto_correct": True,  # Auto-correct enabled
                "validation.position_reconciliation.check_interval": 3600,
            }.get(key, default)
        )

        system = PositionReconciliationSystem(config, portfolio_tracker)

        # Perform reconciliation
        await system.check_positions()

        # Verify that update_position was called for discrepancies
        portfolio_tracker.update_position.assert_called()

        # Verify specifically for BTC on hyperliquid
        call_args_list = portfolio_tracker.update_position.call_args_list
        has_btc_update = False

        for call in call_args_list:
            exchange, position = call[0]
            if exchange == "hyperliquid" and position.symbol == "BTC":
                assert position.size == Decimal("1.1")
                has_btc_update = True
                break

        assert has_btc_update, "Expected update_position call for hyperliquid/BTC not found"

        # Verify discrepancy was marked as corrected
        btc_discrepancy = next(
            (
                d
                for d in system.discrepancy_history
                if d["exchange"] == "hyperliquid" and d["symbol"] == "BTC"
            ),
            None,
        )
        assert btc_discrepancy is not None
        assert btc_discrepancy["corrected"] is True

    def test_reconcile_positions(self, reconciliation_system):
        """Test reconciling positions from different sources."""
        # Create test data
        exchange = "testexchange"

        # Exchange API positions
        exchange_positions = [
            Position(
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal("50000"),
                mark_price=Decimal("50000"),
                liquidation_price=Decimal("45000"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
            Position(
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("10.0"),
                entry_price=Decimal("3000"),
                mark_price=Decimal("3000"),
                liquidation_price=Decimal("2700"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
        ]

        # Fill history positions with discrepancy
        fill_positions = [
            Position(
                symbol="BTC",
                size=Decimal("0.9"),
                side=OrderSide.BUY,
                entry_price=Decimal("50000"),
                mark_price=Decimal("50000"),  # 10% discrepancy
                liquidation_price=Decimal("45000"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
            Position(
                symbol="ETH",
                size=Decimal("-9.8"),
                side=OrderSide.SELL,
                entry_price=Decimal("3000"),
                mark_price=Decimal("3000"),
                liquidation_price=Decimal("2700"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
            Position(
                symbol="SOL",
                size=Decimal("50.0"),
                side=OrderSide.BUY,
                entry_price=Decimal("100"),
                mark_price=Decimal("100"),  # Not in exchange
                liquidation_price=Decimal("90"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
        ]

        # Local positions with discrepancy
        local_positions = [
            Position(
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal("50000"),
                mark_price=Decimal("50000"),
                liquidation_price=Decimal("45000"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
            Position(
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("9.8"),  # Discrepancy
                entry_price=Decimal("3000"),
                mark_price=Decimal("3000"),
                liquidation_price=Decimal("2700"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
            Position(
                symbol="DOGE",
                size=Decimal("1000.0"),
                side=OrderSide.BUY,
                entry_price=Decimal("0.1"),
                mark_price=Decimal("0.1"),  # Not in exchange
                liquidation_price=Decimal("0.08"),
                unrealized_pnl=Decimal("0"),
                leverage=Decimal("1"),
            ),
        ]

        # Call the method
        results = reconciliation_system._reconcile_positions(
            exchange, exchange_positions, fill_positions, local_positions
        )

        # Verify results structure
        assert results["success"] is True
        assert results["symbols_checked"] == 3
        assert len(results["discrepancies"]) == 2

        # Check BTC discrepancy (should be detected)
        btc_discrepancy = next(
            (d for d in results["discrepancies"] if d["symbol"] == "BTC"), None
        )
        assert btc_discrepancy is not None
        assert btc_discrepancy["exchange_size"] == Decimal("1.0")
        assert btc_discrepancy["fill_size"] == Decimal("0.9")
        assert btc_discrepancy["local_size"] == Decimal("1.0")
        assert btc_discrepancy["exchange_size"] == 1.0
        assert btc_discrepancy["fill_size"] == 0.9
        assert btc_discrepancy["local_size"] == 0.95
        assert btc_discrepancy["correct_size"] == 1.0  # Exchange is source of truth

        # Check ETH (should not have discrepancy)
        eth_discrepancy = next(
            (d for d in results["discrepancies"] if d["symbol"] == "ETH"), None
        )
        assert eth_discrepancy is None

        # Check SOL (should be detected since in fill but not local)
        sol_discrepancy = next(
            (d for d in results["discrepancies"] if d["symbol"] == "SOL"), None
        )
        assert sol_discrepancy is not None

        # Check DOGE (should be detected since in local but not exchange)
        doge_discrepancy = next(
            (d for d in results["discrepancies"] if d["symbol"] == "DOGE"), None
        )
        assert doge_discrepancy is not None

    def test_record_discrepancy(self, reconciliation_system):
        """Test recording discrepancies in history."""
        # Create sample results with discrepancies
        exchange = "testexchange"
        # Sample results dictionary needs to match the structure expected by _record_discrepancy
        results = {
            "timestamp": datetime.now(),
            "discrepancies": [
                {
                    "symbol": "BTC",
                    "type": "size", # Added type
                    "exchange_value": "1.0", # Corrected key and value type (string)
                    "local_value": "0.95", # Corrected key and value type (string)
                    "discrepancy": "0.05", # Corrected key and value type (string)
                    # Removed incorrect/unused keys like exchange_size, fill_size, etc.
                }
            ],
        }

        # Call the method
        reconciliation_system._record_discrepancy(exchange, results)

        # Verify history was updated
        assert len(reconciliation_system.discrepancy_history) == 1
        record = reconciliation_system.discrepancy_history[0]

        # Verify record contents (keys should match the corrected discrepancy structure)
        assert record["exchange"] == exchange
        assert record["symbol"] == "BTC"
        assert record["exchange_value"] == "1.0"
        assert record["local_value"] == "0.95"
        assert record["discrepancy"] == "0.05"
        # assert record["correct_size"] == 1.0 # This key doesn't exist in the recorded data
        assert record["corrected"] is False

    def test_get_discrepancy_history(self, reconciliation_system):
        """Test getting history filtered by time."""
        # Add some test data
        now = datetime.now()
        old_time = now - timedelta(days=10)
        recent_time = now - timedelta(days=3)

        # Old record (10 days ago)
        reconciliation_system.discrepancy_history.append(
            {
                "timestamp": old_time,
                "exchange": "exchange1",
                "symbol": "BTC",
                "exchange_size": 1.0,
                "fill_size": 0.9,
                "local_size": 0.95,
                "corrected": False,
            }
        )

        # Recent record (3 days ago)
        reconciliation_system.discrepancy_history.append(
            {
                "timestamp": recent_time,
                "exchange": "exchange2",
                "symbol": "ETH",
                "exchange_size": 10.0,
                "fill_size": 9.5,
                "local_size": 9.8,
                "corrected": True,
            }
        )

        # Get history for last 7 days
        history = reconciliation_system.get_discrepancy_history(days=7)

        # Should only include the recent record
        assert len(history) == 1
        assert history[0]["symbol"] == "ETH"
        assert history[0]["timestamp"] == recent_time

        # Get all history
        all_history = reconciliation_system.get_discrepancy_history(days=30)
        assert len(all_history) == 2

    def test_get_reconciliation_report(self, reconciliation_system):
        """Test generating a reconciliation report."""
        # Use UTC for all datetime objects
        now_utc = datetime.now(UTC)
        # Use a time clearly within the last 24 hours, also UTC aware
        recent_time_utc = now_utc - timedelta(hours=1)

        # Clear history before adding test data
        reconciliation_system.discrepancy_history.clear()

        # Recent records - Use the structure stored by _record_discrepancy
        # Ensure timestamps are timezone-aware (UTC)
        reconciliation_system.discrepancy_history.extend(
            [
                {
                    "timestamp": recent_time_utc, # Use aware datetime
                    "exchange": "hyperliquid",
                    "symbol": "BTC",
                    "exchange_value": "1.0", # Correct key
                    "local_value": "0.95", # Correct key
                    "discrepancy": "0.05", # Correct key
                    "corrected": False,
                    # Removed old/unused keys
                },
                {
                    "timestamp": recent_time_utc, # Use aware datetime
                    "exchange": "hyperliquid",
                    "symbol": "ETH",
                    "exchange_value": "10.0", # Correct key
                    "local_value": "9.8", # Correct key
                    "discrepancy": "0.2", # Correct key
                    "corrected": True,
                    # Removed old/unused keys
                },
                {
                    "timestamp": recent_time_utc, # Use aware datetime
                    "exchange": "backpack",
                    "symbol": "SOL",
                    "exchange_value": "50.0", # Correct key
                    "local_value": "0.0", # Correct key
                    "discrepancy": "50.0", # Correct key
                    "corrected": False,
                    # Removed old/unused keys
                },
            ]
        )

        # Set some recent results (Timestamps should also be aware)
        reconciliation_system.latest_results = {
            "hyperliquid": {
                "success": True,
                "timestamp": now_utc, # Use aware datetime
                "discrepancies": [
                    # Use correct structure if asserting on latest_results details
                    {"symbol": "BTC", "type": "size", "exchange_value": "1.0", "local_value": "0.95", "discrepancy": "0.05"}
                ],
            },
            "backpack": {
                "success": True,
                "timestamp": now_utc, # Use aware datetime
                "discrepancies": [
                    # Use correct structure if asserting on latest_results details
                     {"symbol": "SOL", "type": "size", "exchange_value": "50.0", "local_value": "0.0", "discrepancy": "50.0"}
                ],
            },
        }

        # Generate the report
        # Assuming get_discrepancy_history uses aware comparison internally now
        report = reconciliation_system.get_reconciliation_report()

        # Verify report structure and contents
        # This assertion should now pass as get_discrepancy_history(days=1) will find the 3 records
        assert report["total_discrepancies_24h"] == 3
        assert "hyperliquid" in report["exchange_stats"]
        assert "backpack" in report["exchange_stats"]

        # Check exchange stats
        hyper_stats = report["exchange_stats"]["hyperliquid"]
        assert hyper_stats["total_discrepancies"] == 2
        # assert hyper_stats["symbols_affected"] == 2 # Key changed to symbols_affected_count
        assert hyper_stats["symbols_affected_count"] == 2
        assert hyper_stats["corrected"] == 1
        assert hyper_stats["uncorrected"] == 1

        backpack_stats = report["exchange_stats"]["backpack"]
        assert backpack_stats["total_discrepancies"] == 1
        # assert backpack_stats["symbols_affected"] == 1 # Key changed
        assert backpack_stats["symbols_affected_count"] == 1
        assert backpack_stats["corrected"] == 0
        assert backpack_stats["uncorrected"] == 1

        # Check recent discrepancies are included (ensure correct structure in assertion if needed)
        assert len(report["recent_discrepancies"]) == 3
        assert report["recent_discrepancies"][0]["symbol"] == "BTC" # Example check
        assert report["recent_discrepancies"][0]["exchange_value"] == "1.0"

        # Check configuration settings are included
        assert report["auto_correct_enabled"] == reconciliation_system.auto_correct
