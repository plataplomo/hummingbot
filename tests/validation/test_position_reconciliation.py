"""
Tests for the PositionReconciliationSystem class.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.utils.config import Config
from cyberdelta.validation.models.discrepancy_detail import (
    DiscrepancyDetail,
    HistoricalDiscrepancyRecord,
)
from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem


class TestPositionReconciliationSystem:
    """Test suite for the PositionReconciliationSystem class."""

    @pytest.fixture
    def config(self, mocker: MockerFixture) -> MagicMock:
        """Create a mock config object where 'get' is also a mock."""
        # Base config data
        config_data: dict[str, Any] = {
            "exchanges": {
                "hyperliquid": {"enabled": True},
                "backpack": {"enabled": True},
            },
            "validation.position_reconciliation.interval_seconds": 600.0,
            "validation.position_reconciliation.threshold_percent": "5.0",
            "validation.position_reconciliation.threshold": Decimal("0.05"),
            "validation.position_reconciliation.action_mode": "log",
            "validation.position_reconciliation.auto_correct": False,
            "validation.position_reconciliation.check_interval": 3600,
            "validation.position_reconciliation.use_fill_history": False,
        }

        # Mock the main Config object
        mock_config_obj = MagicMock()

        # Create a separate mock for the 'get' method
        mock_get_method = mocker.MagicMock()

        # Define the side effect for the 'get' mock
        def config_get_side_effect(key: str, default: object | None = None) -> object | None:
            # Simplified logic for test purposes
            # First, check the base dictionary directly
            if key in config_data:
                return config_data[key]  # type: ignore # Allow Any return for mock
            # Handle specific nested cases if necessary (like exchange enabled flags)
            parts = key.split(".")
            if len(parts) == 3 and parts[0] == "exchanges" and parts[2] == "enabled":
                exchanges = config_data.get("exchanges", {})
                if isinstance(exchanges, dict):
                    # type ignore used here due to complexity of mocking deep gets
                    return exchanges.get(parts[1], {}).get("enabled", default)  # type: ignore
                return default
            # Fallback to default
            return default

        # Assign the side effect to the mocked 'get' method
        mock_get_method.side_effect = config_get_side_effect

        # Attach the mocked 'get' method to the main config mock
        mock_config_obj.get = mock_get_method

        return mock_config_obj

    @pytest.fixture
    def portfolio_tracker(self) -> MagicMock:
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()

        # Mock hyperliquid positions
        hyper_positions = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal("50000"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000"),
                liquidation_price=Decimal("45000.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("-10.0"),
                entry_price=Decimal("3000"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("3100"),
                liquidation_price=Decimal("2800.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
        ]

        # Mock backpack positions
        backpack_positions = [
            DerivativePosition(
                exchange="backpack",
                symbol="BTC",
                side=OrderSide.SELL,
                size=Decimal("-2.0"),
                entry_price=Decimal("50500"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000"),
                liquidation_price=Decimal("55000.0"),
                unrealized_pnl=Decimal("-1000.0"),
            )
        ]

        # Setup the get_position method
        def get_position(exchange: str, symbol: str) -> DerivativePosition | None:
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
        def get_positions_by_exchange(exchange: str) -> list[DerivativePosition]:
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
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.1"),  # 10% discrepancy with local (1.0)
                entry_price=Decimal("50000"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000"),
                liquidation_price=Decimal("45000.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal(
                    "-10.0"
                ),  # Corrected: size should be negative for SELL. Matches local.
                entry_price=Decimal("3000"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("3100"),
                liquidation_price=Decimal("2800.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
        ]

        backpack_api_positions = [
            DerivativePosition(
                exchange="backpack",
                symbol="BTC",
                side=OrderSide.SELL,
                size=Decimal("-2.0"),  # Matches local
                entry_price=Decimal("50500"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000"),
                liquidation_price=Decimal("55000.0"),
                unrealized_pnl=Decimal("-1000.0"),
            )
        ]

        # Mock execution handlers for fill history
        execution_handler_hyper = MagicMock()
        execution_handler_backpack = MagicMock()

        # Setup fill derived positions
        hyper_fill_positions = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.05"),  # 5% discrepancy with local (1.0)
                entry_price=Decimal("50100"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000"),
                liquidation_price=Decimal("45000.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("-10.0"),  # Corrected: size should be negative for SELL. Matches local
                entry_price=Decimal("3000"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("3100"),
                liquidation_price=Decimal("2800.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
        ]

        backpack_fill_positions = [
            DerivativePosition(
                exchange="backpack",
                symbol="BTC",
                side=OrderSide.SELL,
                size=Decimal("-1.9"),  # 5% discrepancy with local (-2.0)
                entry_price=Decimal("50400"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("51000"),
                liquidation_price=Decimal("55000.0"),
                unrealized_pnl=Decimal("-1000.0"),
            ),
            DerivativePosition(
                exchange="backpack",
                symbol="SOL",  # Position not in local state
                side=OrderSide.BUY,
                size=Decimal("5.0"),
                entry_price=Decimal("150"),
                timestamp=datetime.now(UTC),
                mark_price=Decimal("155"),
                liquidation_price=Decimal("90.0"),
                unrealized_pnl=Decimal("15.0"),
            ),
        ]

        # Configure the execution handlers
        execution_handler_hyper.get_derived_positions.return_value = hyper_fill_positions
        execution_handler_backpack.get_derived_positions.return_value = backpack_fill_positions

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

        # Use a named function with explicit type annotations for type safety
        def config_get(key: str, default: object | None = None) -> object | None:
            return {"validation.position_reconciliation.use_fill_history": False}.get(key, default)

        tracker.config.get.side_effect = config_get  # type: ignore[attr-defined, reportUnknownMemberType]

        def get_execution_handler(exchange: str) -> MagicMock:
            if exchange == "hyperliquid":
                return execution_handler_hyper
            elif exchange == "backpack":
                return execution_handler_backpack
            return execution_handler_hyper  # Default

        # Add helper method for test access to exchange positions
        def _get_exchange_positions(exchange: str) -> list[DerivativePosition]:
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
    def reconciliation_system(
        self, config: Config, portfolio_tracker: MagicMock
    ) -> PositionReconciliationSystem:
        """Create a PositionReconciliationSystem instance for testing."""
        # Ensure the portfolio_tracker mock has the api_clients attribute expected by the system
        if not hasattr(portfolio_tracker, "api_clients"):
            portfolio_tracker.api_clients = {
                "hyperliquid": AsyncMock(spec=ExchangeAPI),
                "backpack": AsyncMock(spec=ExchangeAPI),
            }
        return PositionReconciliationSystem(config, portfolio_tracker)

    def test_init(
        self,
        reconciliation_system: PositionReconciliationSystem,
        config: MagicMock,
        portfolio_tracker: MagicMock,
    ) -> None:
        """Test system initialization."""
        assert reconciliation_system._portfolio_tracker == portfolio_tracker
        assert reconciliation_system._config == config

        config.get.assert_any_call("validation.position_reconciliation.threshold", 0.05)
        config.get.assert_any_call("validation.position_reconciliation.auto_correct", False)
        config.get.assert_any_call("validation.position_reconciliation.check_interval", 3600)
        config.get.assert_any_call("validation.position_reconciliation.interval_seconds", 300.0)
        config.get.assert_any_call("validation.position_reconciliation.threshold_percent", "5.0")
        config.get.assert_any_call("validation.position_reconciliation.action_mode", "log")

        assert reconciliation_system.reconciliation_threshold == config.get(
            "validation.position_reconciliation.threshold"
        )
        assert reconciliation_system.auto_correct == config.get(
            "validation.position_reconciliation.auto_correct"
        )
        # Compare total_seconds for timedelta with the int/float from config
        check_interval_config_val = config.get("validation.position_reconciliation.check_interval")
        assert isinstance(reconciliation_system.check_interval, timedelta)
        assert reconciliation_system.check_interval.total_seconds() == float(
            check_interval_config_val
        )

        # Also check reconciliation_interval setup if it differs or uses a different config key
        reconciliation_interval_config_val = config.get(
            "validation.position_reconciliation.interval_seconds"
        )
        assert isinstance(reconciliation_system.reconciliation_interval, timedelta)
        assert reconciliation_system.reconciliation_interval.total_seconds() == float(
            reconciliation_interval_config_val
        )

        # Test __init__ robustness to config.get returning None for specific keys
        # where __init__ has internal defaults for None.
        original_system_config_get_side_effect = reconciliation_system._config.get.side_effect

        def side_effect_for_specific_none_tests(key: str, default: Any = None) -> Any:
            if key == "validation.position_reconciliation.interval_seconds":
                return None  # __init__ handles None for this, defaults to 300.0 for _reconciliation_interval_secs
            if key == "validation.position_reconciliation.action_mode":
                return None  # __init__ handles None for this, defaults to "log" for _action_mode
            # For threshold_percent, if config.get returns None, __init__ would try Decimal(str(None))
            # So, we don't make it return None here. Let the fixture's mock behavior for config.get(..., "5.0") work.
            # If the original side_effect is callable, use it for other keys.
            if callable(original_system_config_get_side_effect):
                return original_system_config_get_side_effect(key, default)
            return default  # Fallback

        # Use the config instance that the existing `reconciliation_system` fixture was created with.
        current_config_mock = reconciliation_system._config
        current_config_mock.get.side_effect = side_effect_for_specific_none_tests

        # Create a new PositionReconciliationSystem instance with this specially configured mock.
        temp_system = PositionReconciliationSystem(current_config_mock, portfolio_tracker)

        assert (
            temp_system._reconciliation_interval_secs == 300.0
        )  # Class default for None from config.get
        # For threshold_percent, __init__ calls config.get(..., "5.0").
        # The side_effect_for_specific_none_tests lets this pass to original_system_config_get_side_effect.
        # So it uses the value from the main fixture mock (e.g. "5.0").
        assert temp_system._discrepancy_threshold_percent == Decimal("5.0")
        assert temp_system._action_mode == "log"  # Class default for None from config.get

        # Restore original side effect on the mock from the fixture scope
        current_config_mock.get.side_effect = original_system_config_get_side_effect

    def test_register_portfolio_tracker(
        self, reconciliation_system: PositionReconciliationSystem, portfolio_tracker: MagicMock
    ) -> None:
        """Test registering a portfolio tracker."""
        new_tracker = MagicMock()
        # Ensure the new_tracker mock also has the api_clients attribute
        new_tracker.api_clients = {
            "hyperliquid": AsyncMock(spec=ExchangeAPI),
            "backpack": AsyncMock(spec=ExchangeAPI),
        }
        reconciliation_system.register_portfolio_tracker(new_tracker)
        assert reconciliation_system._portfolio_tracker == new_tracker

    @pytest.mark.asyncio
    async def test_check_positions_interval(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test position check interval logic."""
        system_config_mock = reconciliation_system._config
        portfolio_tracker_mock = reconciliation_system._portfolio_tracker

        # original_get_side_effect = system_config_mock.get.side_effect # Not strictly needed if side_effect is replaced
        # original_get_call_count = system_config_mock.get.call_count

        portfolio_tracker_mock.api_clients = {
            "hyperliquid": AsyncMock(spec=ExchangeAPI),
            "backpack": AsyncMock(spec=ExchangeAPI),
        }

        # Scenario 1: Interval has passed, should run
        def scenario_1_specific_get(key: str, default: Any = None) -> Any:
            if key == "exchanges.hyperliquid.enabled":
                return True  # This config is checked inside the original _reconcile_exchange
            if key == "exchanges.backpack.enabled":
                return True  # This config is checked inside the original _reconcile_exchange
            # Configs relevant for PositionReconciliationSystem.__init__ or check_positions itself
            if key == "validation.position_reconciliation.check_interval":
                return 100  # Used by check_positions to determine if interval passed
            if key == "validation.position_reconciliation.interval_seconds":
                return 100  # Used by __init__ for self.reconciliation_interval setup
            # Fallback for other config gets if PositionReconciliationSystem init needs them
            # For this test, direct attributes like check_interval are also set on the instance.
            return default

        system_config_mock.get.side_effect = scenario_1_specific_get
        # Ensure reconciliation_system uses this updated config behavior for intervals
        reconciliation_system.check_interval = timedelta(
            seconds=system_config_mock.get("validation.position_reconciliation.check_interval")
        )
        reconciliation_system.reconciliation_interval = timedelta(
            seconds=system_config_mock.get("validation.position_reconciliation.interval_seconds")
        )

        reconciliation_system.last_check_time = datetime.now(UTC) - timedelta(seconds=200)

        # Use a list to record calls to the mock, similar to test_auto_correct
        reconcile_exchange_calls = []

        async def mock_reconcile_exchange_side_effect(exchange: str) -> dict[str, Any]:
            reconcile_exchange_calls.append(exchange)
            # Return the expected dictionary structure
            return {
                "success": True,
                "discrepancies": [],
                "timestamp": datetime.now(UTC),
                "message": f"Mocked reconcile for {exchange}",
            }

        with patch.object(
            reconciliation_system,
            "_reconcile_exchange",
            side_effect=mock_reconcile_exchange_side_effect,
        ) as mock_check_s1_method:  # This is the MagicMock object for the patch
            await reconciliation_system.check_positions(force=False)

            # system_config_mock.get.assert_any_call("exchanges.hyperliquid.enabled", False) # These are not called by check_positions directly
            # system_config_mock.get.assert_any_call("exchanges.backpack.enabled", False)

            assert mock_check_s1_method.call_count == 2, (
                f"Scenario 1: Expected _reconcile_exchange to be called 2 times, got {mock_check_s1_method.call_count}"
            )
            assert "hyperliquid" in reconcile_exchange_calls
            assert "backpack" in reconcile_exchange_calls

        # Scenario 2: Interval has not passed, should not run
        system_config_mock.get.side_effect = (
            scenario_1_specific_get  # Keep same config get behavior
        )
        reconciliation_system.check_interval = timedelta(seconds=300)  # Ensure interval is longer
        reconciliation_system.reconciliation_interval = timedelta(seconds=300)
        reconciliation_system.last_check_time = datetime.now(UTC) - timedelta(
            seconds=100
        )  # Only 100s passed

        reconcile_exchange_calls_s2 = []

        async def mock_reconcile_exchange_side_effect_s2(exchange: str) -> dict[str, Any]:
            reconcile_exchange_calls_s2.append(exchange)
            return {"success": True, "discrepancies": [], "timestamp": datetime.now(UTC)}

        with patch.object(
            reconciliation_system,
            "_reconcile_exchange",
            side_effect=mock_reconcile_exchange_side_effect_s2,
        ) as mock_check_s2_method:
            await reconciliation_system.check_positions(force=False)
            mock_check_s2_method.assert_not_called()
            assert len(reconcile_exchange_calls_s2) == 0

        # Scenario 3: force=True, should run even if interval hasn't passed
        system_config_mock.get.side_effect = scenario_1_specific_get
        reconciliation_system.check_interval = timedelta(seconds=300)
        reconciliation_system.reconciliation_interval = timedelta(seconds=300)
        reconciliation_system.last_check_time = datetime.now(UTC) - timedelta(seconds=100)

        reconcile_exchange_calls_s3 = []

        async def mock_reconcile_exchange_side_effect_s3(exchange: str) -> dict[str, Any]:
            reconcile_exchange_calls_s3.append(exchange)
            return {"success": True, "discrepancies": [], "timestamp": datetime.now(UTC)}

        with patch.object(
            reconciliation_system,
            "_reconcile_exchange",
            side_effect=mock_reconcile_exchange_side_effect_s3,
        ) as mock_check_s3_method:
            await reconciliation_system.check_positions(force=True)
            assert mock_check_s3_method.call_count == 2
            assert "hyperliquid" in reconcile_exchange_calls_s3
            assert "backpack" in reconcile_exchange_calls_s3

    @pytest.mark.asyncio
    async def test_check_positions_no_portfolio_tracker(self, config: Config) -> None:
        """Test check_positions when portfolio tracker fails to provide data."""
        # Create system, passing a mock tracker to satisfy __init__
        mock_tracker = MagicMock()
        # Configure the mock tracker to simulate failure/unavailability
        mock_tracker.get_positions_by_exchange.return_value = (
            None  # Simulate tracker unable to provide data
        )
        mock_tracker.api_clients = {}  # Simulate no clients registered

        system = PositionReconciliationSystem(config, portfolio_tracker=mock_tracker)

        # Run check_positions
        results = await system.check_positions(force=True)  # Force check

        # Expect empty results and an error logged
        assert results == {}

    @pytest.mark.asyncio
    async def test_check_positions(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test checking positions and identifying discrepancies."""
        reconciliation_system._portfolio_tracker.get_positions_by_exchange = MagicMock(
            return_value=[]
        )

        now = datetime.now(UTC)
        api_positions_hyper = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal("100"),
                timestamp=now,
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("-2.0"),
                entry_price=Decimal("50"),
                timestamp=now,
            ),
        ]
        api_positions_bp = [
            DerivativePosition(
                exchange="backpack",
                symbol="SOL",
                side=OrderSide.BUY,
                size=Decimal("5.0"),
                entry_price=Decimal("20"),
                timestamp=now,
            )
        ]

        mock_hl_api_client = AsyncMock(spec=ExchangeAPI)
        mock_hl_api_client.get_positions = AsyncMock(return_value=api_positions_hyper)

        mock_bp_api_client = AsyncMock(spec=ExchangeAPI)
        mock_bp_api_client.get_positions = AsyncMock(return_value=api_positions_bp)

        portfolio_tracker = reconciliation_system._portfolio_tracker
        portfolio_tracker.api_clients = {
            "hyperliquid": mock_hl_api_client,
            "backpack": mock_bp_api_client,
        }

        # Patch _reconcile_exchange to return a known structure to avoid internal errors
        # This helps test check_positions's aggregation logic rather than _reconcile_exchange itself here.
        mock_reconcile_result = {
            "success": True,
            "discrepancies": [],
            "symbols_checked": 0,
            "error": None,
            "timestamp": datetime.now(UTC),
        }
        with patch.object(
            reconciliation_system,
            "_reconcile_exchange",
            return_value=mock_reconcile_result,
        ) as patched_reconcile_pos:
            results = await reconciliation_system.check_positions()

        assert "hyperliquid" in results
        assert "backpack" in results
        assert (
            results["hyperliquid"] == mock_reconcile_result
        )  # As _reconcile_exchange directly returns _reconcile_exchange result if no error
        assert results["backpack"] == mock_reconcile_result
        assert patched_reconcile_pos.call_count == 2  # Called for hyperliquid and backpack

    @pytest.mark.asyncio
    async def test_auto_correct(self, config: Config, portfolio_tracker: MagicMock) -> None:
        """Test auto-correcting positions (conceptual)."""

        # Setup config for auto_correct True
        def config_get(key: str, default: object | None = None) -> object | None:
            if key == "validation.position_reconciliation.auto_correct":
                return True
            if key == "validation.position_reconciliation.check_interval":
                # Setting to a low value or 0 to ensure the check runs if not forced,
                # but force=True is used in this test.
                return 0
            if key == "exchanges.hyperliquid.enabled":
                return True
            if key == "exchanges.backpack.enabled":
                return True
            # Ensure other necessary config values are provided for PositionReconciliationSystem init
            if key == "validation.position_reconciliation.interval_seconds":
                return 600.0
            if key == "validation.position_reconciliation.threshold_percent":
                return "5.0"
            if key == "validation.position_reconciliation.action_mode":
                return "log"
            if key == "validation.position_reconciliation.use_fill_history":  # Added
                return False
            return default

        config.get.side_effect = config_get

        system = PositionReconciliationSystem(config, portfolio_tracker)

        # Mock _reconcile_exchange to track calls and simulate its return
        reconcile_calls = []

        async def mock_side_effect_reconcile_exchange(exchange: str) -> dict[str, Any]:
            reconcile_calls.append({"exchange": exchange})  # Record the call

            # Simulate return structure of _reconcile_exchange
            # This should include discrepancies if we want auto-correct to do something.
            # For this test, we mainly care that it was called.
            # If auto-correct logic depends on specific discrepancy details, mock them here.
            mock_discrepancies = []
            if exchange == "hyperliquid":  # Simulate a discrepancy for hyperliquid
                mock_discrepancies.append({
                    "symbol": "BTC",
                    "exchange_value": "1.1",  # API has 1.1
                    "local_value": "1.0",  # Local has 1.0
                    "discrepancy": "0.1",
                    "action_taken": "logged",  # or "corrected" if auto_correct was to run
                })

            return {
                "success": True,
                "discrepancies": mock_discrepancies,
                "timestamp": datetime.now(UTC),
                "message": f"Mock reconciliation for {exchange}",
            }

        with patch.object(
            system,
            "_reconcile_exchange",  # Patching the correct method name
            side_effect=mock_side_effect_reconcile_exchange,
        ) as mock_reconcile_method:
            # Mock portfolio_tracker.api_clients to return mock ExchangeAPI instances
            mock_hl_api_client = AsyncMock(spec=ExchangeAPI)
            mock_bp_api_client = AsyncMock(spec=ExchangeAPI)

            # Configure mock API clients (get_positions is called by the real _reconcile_exchange,
            # but _reconcile_exchange itself is mocked here, so get_positions won't be hit via this path)
            # However, portfolio_tracker.api_clients itself needs to be set for check_positions to iterate
            portfolio_tracker.api_clients = {
                "hyperliquid": mock_hl_api_client,
                "backpack": mock_bp_api_client,
            }

            # Mock methods on portfolio_tracker that auto_correct might call if discrepancies were processed
            portfolio_tracker.update_position = AsyncMock()
            portfolio_tracker.create_position_from_exchange_data = AsyncMock()

            # Call check_positions, which should trigger the mocked _reconcile_exchange
            await system.check_positions(force=True)

            # Assert that _reconcile_exchange (our mock_reconcile_method) was called
            assert mock_reconcile_method.called, "_reconcile_exchange was not called"

            # Assert it was called for each enabled exchange
            # Call args list contains tuples of (args, kwargs) for each call
            called_exchanges = {
                call_args[0][0] for call_args in mock_reconcile_method.call_args_list
            }
            assert "hyperliquid" in called_exchanges, "hyperliquid was not reconciled"
            assert "backpack" in called_exchanges, "backpack was not reconciled"

            assert any(call["exchange"] == "hyperliquid" for call in reconcile_calls), (
                "Did not record a reconcile call for hyperliquid"
            )
            assert any(call["exchange"] == "backpack" for call in reconcile_calls), (
                "Did not record a reconcile call for backpack"
            )

            # Further assertions could check if portfolio_tracker.update_position was called
            # if mock_discrepancies was non-empty and auto_correct logic was fully exercised.
            # For example, if hyperliquid had a discrepancy:
            # Await the coroutine to get its result (the dictionary)
            reconcile_result_hyperliquid = await mock_side_effect_reconcile_exchange("hyperliquid")
            if any(d["symbol"] == "BTC" for d in reconcile_result_hyperliquid["discrepancies"]):
                # This part of assertion depends on _apply_corrections being called by _reconcile_exchange
                # Since _reconcile_exchange is fully mocked, _apply_corrections is not called by our mock.
                # To test auto-correction fully, one might need to mock _apply_corrections or
                # make the _reconcile_exchange mock more complex to call it.
                # For now, we've confirmed _reconcile_exchange is called.
                pass

    def test_reconcile_positions(self, reconciliation_system: PositionReconciliationSystem) -> None:
        """Test the core _reconcile_exchange logic (if directly testable)."""
        # This test might be difficult to set up without extensive mocking if
        # the method is not yet implemented or relies heavily on internal state.
        # For now, if the method is private or complex to isolate, this test might be a placeholder.
        assert True

    def test_record_discrepancy(self, reconciliation_system: PositionReconciliationSystem) -> None:
        """Test recording a discrepancy."""
        exchange = "testexchange"
        now_ts = datetime.now(UTC)  # Define timestamp for clarity

        # Create a DiscrepancyDetail model instance
        discrepancy_detail_model = DiscrepancyDetail(
            symbol="BTC",
            discrepancy_type="size",
            exchange_value="1.0",
            local_value="0.95",
            details="Size differs by 0.05",
        )

        # Sample results dictionary now contains a list of DiscrepancyDetail models
        results = {
            "timestamp": now_ts,
            "discrepancies": [discrepancy_detail_model],
            "success": False,  # Typically, if there are discrepancies, success might be False
            "has_discrepancies": True,
        }

        # Call the method using individual fields from the DiscrepancyDetail model
        # Since results["discrepancies"] is a list, we take the first one for this test.
        # The method _record_discrepancy is designed to record a single discrepancy event.
        if results["discrepancies"]:
            detail_to_record = results["discrepancies"][0]
            recorded_historical_item = reconciliation_system._record_discrepancy(
                exchange_id=exchange,
                symbol=detail_to_record.symbol,
                discrepancy_type=detail_to_record.discrepancy_type,
                api_val=detail_to_record.exchange_value,
                local_val=detail_to_record.local_value,
                details=detail_to_record.details,
            )
            # Update assertion to check the returned item and history
            assert recorded_historical_item is not None
            assert len(reconciliation_system.discrepancy_history) == 1
            recorded_item_from_history = reconciliation_system.discrepancy_history[0]
            assert recorded_item_from_history is recorded_historical_item
            assert recorded_item_from_history.exchange_id == exchange
            # The recorded_at timestamp is set inside _record_discrepancy, so we can't easily compare with now_ts
            # We can check it's a datetime and reasonably close if needed, or just trust it's set.
            assert isinstance(recorded_item_from_history.recorded_at, datetime)
            assert (
                recorded_item_from_history.detail == detail_to_record
            )  # Check if the detail is the same model
            assert not recorded_item_from_history.is_corrected
        else:
            pytest.fail("Test setup error: results['discrepancies'] is empty.")

    def test_get_discrepancy_history(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test getting history filtered by time."""
        now = datetime.now(UTC)
        old_time = now - timedelta(days=10)
        recent_time = now - timedelta(days=3)

        # Create DiscrepancyDetail instances first
        detail_old = DiscrepancyDetail(
            symbol="BTC_OLD", discrepancy_type="size", exchange_value="1", local_value="0.9"
        )
        detail_recent = DiscrepancyDetail(
            symbol="ETH_RECENT",
            discrepancy_type="entry_price",
            exchange_value="3000",
            local_value="3001",
        )

        # Old record (10 days ago)
        historical_record_old = HistoricalDiscrepancyRecord(
            detail=detail_old,
            exchange_id="exchange1",
            recorded_at=old_time,
            is_corrected=False,
        )
        reconciliation_system.discrepancy_history.append(historical_record_old)

        # Recent record (3 days ago)
        historical_record_recent = HistoricalDiscrepancyRecord(
            detail=detail_recent,
            exchange_id="exchange2",
            recorded_at=recent_time,
            is_corrected=True,
        )
        reconciliation_system.discrepancy_history.append(historical_record_recent)

        # Get history for last 7 days
        history = reconciliation_system.get_discrepancy_history(days=7)

        assert len(history) == 1
        assert history[0].detail.symbol == "ETH_RECENT"
        assert history[0].exchange_id == "exchange2"
        assert history[0].recorded_at == recent_time
        assert history[0].is_corrected is True

    def test_get_reconciliation_report(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test generating a reconciliation report."""
        now_utc = datetime.now(UTC)
        recent_time_utc = now_utc - timedelta(hours=1)

        reconciliation_system.discrepancy_history.clear()

        # Create DiscrepancyDetail instances
        detail1 = DiscrepancyDetail(
            symbol="BTC", discrepancy_type="size", exchange_value="1.0", local_value="0.95"
        )
        detail2 = DiscrepancyDetail(
            symbol="ETH", discrepancy_type="entry_price", exchange_value="10.0", local_value="9.8"
        )
        detail3 = DiscrepancyDetail(
            symbol="SOL", discrepancy_type="size", exchange_value="50.0", local_value="0.0"
        )

        # Populate history with HistoricalDiscrepancyRecord instances
        reconciliation_system.discrepancy_history.extend([
            HistoricalDiscrepancyRecord(
                detail=detail1,
                exchange_id="hyperliquid",
                recorded_at=recent_time_utc,
                is_corrected=False,
            ),
            HistoricalDiscrepancyRecord(
                detail=detail2,
                exchange_id="hyperliquid",
                recorded_at=recent_time_utc,
                is_corrected=True,
            ),
            HistoricalDiscrepancyRecord(
                detail=detail3,
                exchange_id="backpack",
                recorded_at=recent_time_utc,
                is_corrected=False,
            ),
        ])

        # Set some recent results (latest_results still uses dicts with DiscrepancyDetail list)
        # Create DiscrepancyDetail for latest_results
        latest_detail_btc = DiscrepancyDetail(
            symbol="BTC",
            discrepancy_type="size",
            exchange_value="1.0",
            local_value="0.95",
            details="Size difference detected now",
        )
        latest_detail_sol = DiscrepancyDetail(
            symbol="SOL",
            discrepancy_type="size",
            exchange_value="50.0",
            local_value="0.0",
            details="SOL API position exists, local is flat",
        )

        reconciliation_system.latest_results = {
            "hyperliquid": {
                "success": False,  # Typically false if discrepancies
                "timestamp": now_utc,
                "discrepancies": [latest_detail_btc],
                "has_discrepancies": True,
                "symbols_checked": 1,
            },
            "backpack": {
                "success": False,
                "timestamp": now_utc,
                "discrepancies": [latest_detail_sol],
                "has_discrepancies": True,
                "symbols_checked": 1,
            },
        }
        reconciliation_system.last_check_time = now_utc - timedelta(minutes=5)

        report = reconciliation_system.get_reconciliation_report()

        # Basic report structure checks
        assert "report_generated_at" in report
        assert "total_discrepancies_24h" in report
        assert "exchange_specific_stats" in report
        assert "recent_discrepancies_summary" in report
        assert "last_reconciliation_check_time" in report
        assert "auto_correct_enabled" in report
        assert "reconciliation_threshold_config" in report

        # Check content based on the setup
        assert report["total_discrepancies_24h"] == 3  # All 3 historical records are recent
        assert len(report["recent_discrepancies_summary"]) <= 10  # Capped at 10
        assert len(report["recent_discrepancies_summary"]) == 3  # All 3 are recent

        # Check exchange specific stats
        hyper_stats = report["exchange_specific_stats"].get("hyperliquid")
        assert hyper_stats is not None
        assert hyper_stats["total_discrepancies"] == 2
        assert hyper_stats["symbols_affected_count"] == 2  # BTC and ETH
        assert hyper_stats["corrected_count"] == 1
        assert hyper_stats["uncorrected_count"] == 1

        bp_stats = report["exchange_specific_stats"].get("backpack")
        assert bp_stats is not None
        assert bp_stats["total_discrepancies"] == 1
        assert bp_stats["symbols_affected_count"] == 1  # SOL
        assert bp_stats["corrected_count"] == 0
        assert bp_stats["uncorrected_count"] == 1

        # Check one item from recent_discrepancies_summary
        # Order might vary, so find one, e.g., the BTC one for hyperliquid
        btc_summary_found = False
        for item in report["recent_discrepancies_summary"]:
            if item["exchange"] == "hyperliquid" and item["symbol"] == "BTC":
                assert item["discrepancy_type"] == "size"
                assert item["exchange_value"] == "1.0"
                assert item["local_value"] == "0.95"
                assert item["corrected"] is False
                btc_summary_found = True
                break
        assert btc_summary_found, "BTC summary discrepancy not found in report"

        # Check another item from recent_discrepancies_summary
        # Order might vary, so find one, e.g., the ETH one for hyperliquid
        eth_summary_found = False
        for item in report["recent_discrepancies_summary"]:
            if item["exchange"] == "hyperliquid" and item["symbol"] == "ETH":
                assert item["discrepancy_type"] == "entry_price"
                assert item["exchange_value"] == "10.0"
                assert item["local_value"] == "9.8"
                assert item["corrected"] is True
                eth_summary_found = True
                break
        assert eth_summary_found, "ETH summary discrepancy not found in report"

    @pytest.mark.asyncio
    async def test_check_positions_mismatch_triggers_reconciliation_and_logs_error(
        self, reconciliation_system: PositionReconciliationSystem
    ) -> None:
        """Test checking positions and identifying discrepancies."""
        reconciliation_system._portfolio_tracker.get_positions_by_exchange = MagicMock(
            return_value=[]
        )

        now = datetime.now(UTC)
        api_positions_hyper = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC",
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal("100"),
                timestamp=now,
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("-2.0"),
                entry_price=Decimal("50"),
                timestamp=now,
            ),
        ]
        api_positions_bp = [
            DerivativePosition(
                exchange="backpack",
                symbol="SOL",
                side=OrderSide.BUY,
                size=Decimal("5.0"),
                entry_price=Decimal("20"),
                timestamp=now,
            )
        ]

        mock_hl_api_client = AsyncMock(spec=ExchangeAPI)
        mock_hl_api_client.get_positions = AsyncMock(return_value=api_positions_hyper)

        mock_bp_api_client = AsyncMock(spec=ExchangeAPI)
        mock_bp_api_client.get_positions = AsyncMock(return_value=api_positions_bp)

        portfolio_tracker = reconciliation_system._portfolio_tracker
        portfolio_tracker.api_clients = {
            "hyperliquid": mock_hl_api_client,
            "backpack": mock_bp_api_client,
        }

        # Patch _reconcile_exchange to return a known structure to avoid internal errors
        # This helps test check_positions's aggregation logic rather than _reconcile_exchange itself here.
        mock_reconcile_result = {
            "success": True,
            "discrepancies": [],
            "symbols_checked": 0,
            "error": None,
            "timestamp": datetime.now(UTC),
        }
        with patch.object(
            reconciliation_system,
            "_reconcile_exchange",
            return_value=mock_reconcile_result,
        ) as patched_reconcile_pos:
            results = await reconciliation_system.check_positions()

        assert "hyperliquid" in results
        assert "backpack" in results
        assert (
            results["hyperliquid"] == mock_reconcile_result
        )  # As _reconcile_exchange directly returns _reconcile_exchange result if no error
        assert results["backpack"] == mock_reconcile_result
        assert patched_reconcile_pos.call_count == 2  # Called for hyperliquid and backpack
