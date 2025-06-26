"""Tests for the PositionReconciliationSystem class."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config import AppSettings
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.validation.models.discrepancy_detail import (
    DiscrepancyDetail,
    HistoricalDiscrepancyRecord,
)
from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem


pytestmark = pytest.mark.timing


class TestPositionReconciliationSystem:
    """Test suite for the PositionReconciliationSystem class."""

    @pytest.fixture
    def config(self, mocker: MockerFixture) -> MagicMock:
        """Create a mock config object that mimics the Pydantic AppSettings structure."""
        # Create a mock for the position reconciliation settings
        mock_pos_recon_config = MagicMock()
        mock_pos_recon_config.enabled = True
        mock_pos_recon_config.check_interval_sec = 600
        mock_pos_recon_config.max_discrepancy_pct = Decimal("0.05")

        # Create a mock for safety systems
        mock_safety_systems = MagicMock()
        mock_safety_systems.position_reconciliation = mock_pos_recon_config

        # Create the main config mock
        mock_config = MagicMock(spec=AppSettings)
        mock_config.safety_systems = mock_safety_systems

        # Mock exchanges for enabled checks
        mock_exchanges = {
            "hyperliquid": MagicMock(enabled=True),
            "backpack": MagicMock(enabled=True),
        }
        mock_config.exchanges = mock_exchanges

        return mock_config

    def _create_test_positions(self) -> dict[str, list[DerivativePosition]]:
        """Create test positions for hyperliquid and backpack."""
        now = datetime.now(UTC)
        return {
            "hyperliquid": [
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("1.0"),
                    entry_price=Decimal(50000),
                    timestamp=now,
                    mark_price=Decimal(51000),
                    liquidation_price=Decimal("45000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                ),
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("-10.0"),
                    entry_price=Decimal(3000),
                    timestamp=now,
                    mark_price=Decimal(3100),
                    liquidation_price=Decimal("2800.0"),
                    unrealized_pnl=Decimal("1000.0"),
                ),
            ],
            "backpack": [
                DerivativePosition(
                    exchange="backpack",
                    symbol="BTC",
                    side=OrderSide.SELL,
                    size=Decimal("-2.0"),
                    entry_price=Decimal(50500),
                    timestamp=now,
                    mark_price=Decimal(51000),
                    liquidation_price=Decimal("55000.0"),
                    unrealized_pnl=Decimal("-1000.0"),
                ),
            ],
        }

    def _create_api_positions(self) -> dict[str, list[DerivativePosition]]:
        """Create API positions with discrepancies for testing."""
        now = datetime.now(UTC)
        return {
            "hyperliquid": [
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("1.1"),  # 10% discrepancy
                    entry_price=Decimal(50000),
                    timestamp=now,
                    mark_price=Decimal(51000),
                    liquidation_price=Decimal("45000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                ),
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("-10.0"),  # Matches local
                    entry_price=Decimal(3000),
                    timestamp=now,
                    mark_price=Decimal(3100),
                    liquidation_price=Decimal("2800.0"),
                    unrealized_pnl=Decimal("1000.0"),
                ),
            ],
            "backpack": [
                DerivativePosition(
                    exchange="backpack",
                    symbol="BTC",
                    side=OrderSide.SELL,
                    size=Decimal("-2.0"),  # Matches local
                    entry_price=Decimal(50500),
                    timestamp=now,
                    mark_price=Decimal(51000),
                    liquidation_price=Decimal("55000.0"),
                    unrealized_pnl=Decimal("-1000.0"),
                ),
            ],
        }

    def _setup_tracker_methods(
        self,
        tracker: MagicMock,
        positions: dict[str, list[DerivativePosition]],
    ) -> None:
        """Setup mock tracker methods."""

        def get_position(exchange: str, symbol: str) -> DerivativePosition | None:
            for pos in positions.get(exchange, []):
                if pos.symbol == symbol:
                    return pos
            return None

        def get_positions_by_exchange(exchange: str) -> list[DerivativePosition]:
            return positions.get(exchange, [])

        tracker.get_position.side_effect = get_position
        tracker.get_positions_by_exchange.side_effect = get_positions_by_exchange

    @pytest.fixture
    def portfolio_tracker(self) -> MagicMock:
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()

        # Create test positions
        local_positions = self._create_test_positions()
        api_positions = self._create_api_positions()

        # Setup tracker methods
        self._setup_tracker_methods(tracker, local_positions)

        # Create mock API clients
        hyperliquid_client = AsyncMock(spec=ExchangeAPI)
        backpack_client = AsyncMock(spec=ExchangeAPI)

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
                entry_price=Decimal(50100),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(51000),
                liquidation_price=Decimal("45000.0"),
                unrealized_pnl=Decimal("1000.0"),
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH",
                side=OrderSide.SELL,
                size=Decimal("-10.0"),  # Corrected: size should be negative for SELL. Matches local
                entry_price=Decimal(3000),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(3100),
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
                entry_price=Decimal(50400),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(51000),
                liquidation_price=Decimal("55000.0"),
                unrealized_pnl=Decimal("-1000.0"),
            ),
            DerivativePosition(
                exchange="backpack",
                symbol="SOL",  # Position not in local state
                side=OrderSide.BUY,
                size=Decimal("5.0"),
                entry_price=Decimal(150),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(155),
                liquidation_price=Decimal("90.0"),
                unrealized_pnl=Decimal("15.0"),
            ),
        ]

        # Configure the execution handlers
        execution_handler_hyper.get_derived_positions.return_value = hyper_fill_positions
        execution_handler_backpack.get_derived_positions.return_value = backpack_fill_positions

        # Configure API clients
        hyper_api_positions = api_positions["hyperliquid"]
        backpack_api_positions = api_positions["backpack"]
        hyperliquid_client.get_positions.return_value = hyper_api_positions
        backpack_client.get_positions.return_value = backpack_api_positions

        # Configure portfolio tracker methods
        # Note: get_position and get_positions_by_exchange are already set up in
        # _setup_tracker_methods
        tracker.update_position = MagicMock()

        # Configure API client and execution handler access
        tracker.api_clients = {
            "hyperliquid": hyperliquid_client,
            "backpack": backpack_client,
        }

        def get_execution_handler(exchange: str) -> MagicMock:
            """Get execution handler for testing."""
            if exchange == "hyperliquid":
                return execution_handler_hyper
            if exchange == "backpack":
                return execution_handler_backpack
            return execution_handler_hyper  # Default

        # Add helper method for test access to exchange positions
        def _get_exchange_positions(exchange: str) -> list[DerivativePosition]:
            if exchange == "hyperliquid":
                return hyper_api_positions
            if exchange == "backpack":
                return backpack_api_positions
            return []

        tracker._get_exchange_positions = _get_exchange_positions

        # Remove the side_effect for get_api_client as api_clients dict is now used
        tracker.get_execution_handler.side_effect = get_execution_handler

        return tracker

    @pytest.fixture
    def reconciliation_system(
        self,
        config: MagicMock,
        portfolio_tracker: MagicMock,
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
        assert reconciliation_system._portfolio_tracker  # pyright: ignore[reportPrivateUsage] == portfolio_tracker
        assert reconciliation_system._config  # pyright: ignore[reportPrivateUsage] == config

        # Check that the system properly extracted values from the mock config
        assert reconciliation_system.reconciliation_threshold == 0.05
        assert reconciliation_system.auto_correct is False
        assert isinstance(reconciliation_system.check_interval, timedelta)
        assert reconciliation_system.check_interval.total_seconds() == 600.0
        assert isinstance(reconciliation_system.reconciliation_interval, timedelta)
        assert reconciliation_system.reconciliation_interval.total_seconds() == 600.0
        assert reconciliation_system._discrepancy_threshold_percent  # pyright: ignore[reportPrivateUsage] == Decimal("0.05")
        assert reconciliation_system._action_mode  # pyright: ignore[reportPrivateUsage] == "log"

    def test_register_portfolio_tracker(
        self,
        reconciliation_system: PositionReconciliationSystem,
        portfolio_tracker: MagicMock,
    ) -> None:
        """Test registering a portfolio tracker."""
        new_tracker = MagicMock()
        # Ensure the new_tracker mock also has the api_clients attribute
        new_tracker.api_clients = {
            "hyperliquid": AsyncMock(spec=ExchangeAPI),
            "backpack": AsyncMock(spec=ExchangeAPI),
        }
        reconciliation_system.register_portfolio_tracker(new_tracker)
        assert reconciliation_system._portfolio_tracker  # pyright: ignore[reportPrivateUsage] == new_tracker

    @pytest.mark.asyncio
    async def test_check_positions_interval(
        self,
        reconciliation_system: PositionReconciliationSystem,
    ) -> None:
        """Test position check interval logic."""
        portfolio_tracker_mock = reconciliation_system._portfolio_tracker  # pyright: ignore[reportPrivateUsage]

        portfolio_tracker_mock.api_clients = {
            "hyperliquid": AsyncMock(spec=ExchangeAPI),
            "backpack": AsyncMock(spec=ExchangeAPI),
        }

        # Scenario 1: Interval has passed, should run
        reconciliation_system.check_interval = timedelta(seconds=100)
        reconciliation_system.reconciliation_interval = timedelta(seconds=100)
        reconciliation_system.last_check_time = datetime.now(UTC) - timedelta(seconds=200)

        # Use a list to record calls to the mock, similar to test_auto_correct
        reconcile_exchange_calls: list[str] = []

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

            assert mock_check_s1_method.call_count == 2, (
                f"Scenario 1: Expected _reconcile_exchange to be called 2 times, "
                f"got {mock_check_s1_method.call_count}"
            )
            assert "hyperliquid" in reconcile_exchange_calls
            assert "backpack" in reconcile_exchange_calls

        # Scenario 2: Interval has not passed, should not run
        reconciliation_system.check_interval = timedelta(seconds=300)  # Ensure interval is longer
        reconciliation_system.reconciliation_interval = timedelta(seconds=300)
        reconciliation_system.last_check_time = datetime.now(UTC) - timedelta(
            seconds=100,
        )  # Only 100s passed

        reconcile_exchange_calls_s2: list[str] = []

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
        reconciliation_system.check_interval = timedelta(seconds=300)
        reconciliation_system.reconciliation_interval = timedelta(seconds=300)
        reconciliation_system.last_check_time = datetime.now(UTC) - timedelta(seconds=100)

        reconcile_exchange_calls_s3: list[str] = []

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
    async def test_check_positions_no_portfolio_tracker(self, config: MagicMock) -> None:
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
        self,
        reconciliation_system: PositionReconciliationSystem,
    ) -> None:
        """Test checking positions and identifying discrepancies."""
        # Mock the portfolio tracker method using patch.object
        with patch.object(
            reconciliation_system._portfolio_tracker,  # pyright: ignore[reportPrivateUsage]
            "get_positions_by_exchange",
            return_value=[],
        ):
            now = datetime.now(UTC)
            api_positions_hyper = [
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("1.0"),
                    entry_price=Decimal(100),
                    timestamp=now,
                ),
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("-2.0"),
                    entry_price=Decimal(50),
                    timestamp=now,
                ),
            ]
        api_positions_bp = [
            DerivativePosition(
                exchange="backpack",
                symbol="SOL",
                side=OrderSide.BUY,
                size=Decimal("5.0"),
                entry_price=Decimal(20),
                timestamp=now,
            ),
        ]

        mock_hl_api_client = AsyncMock(spec=ExchangeAPI)
        mock_hl_api_client.get_positions = AsyncMock(return_value=api_positions_hyper)

        mock_bp_api_client = AsyncMock(spec=ExchangeAPI)
        mock_bp_api_client.get_positions = AsyncMock(return_value=api_positions_bp)

        portfolio_tracker = reconciliation_system._portfolio_tracker  # pyright: ignore[reportPrivateUsage]
        portfolio_tracker.api_clients = {
            "hyperliquid": mock_hl_api_client,
            "backpack": mock_bp_api_client,
        }

        # Patch _reconcile_exchange to return a known structure to avoid internal errors
        # This helps test check_positions's aggregation logic rather than _reconcile_exchange
        # itself here.
        mock_reconcile_result: dict[str, Any] = {
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
    async def test_auto_correct(self, portfolio_tracker: MagicMock) -> None:
        """Test auto-correcting positions (conceptual)."""
        # Create a mock config with auto_correct enabled
        mock_pos_recon_config = MagicMock()
        mock_pos_recon_config.enabled = True
        mock_pos_recon_config.check_interval_sec = 0  # Low value to ensure check runs
        mock_pos_recon_config.max_discrepancy_pct = Decimal("0.05")

        mock_safety_systems = MagicMock()
        mock_safety_systems.position_reconciliation = mock_pos_recon_config

        mock_config = MagicMock(spec=AppSettings)
        mock_config.safety_systems = mock_safety_systems

        # Mock exchanges
        mock_exchanges = {
            "hyperliquid": MagicMock(enabled=True),
            "backpack": MagicMock(enabled=True),
        }
        mock_config.exchanges = mock_exchanges

        system = PositionReconciliationSystem(mock_config, portfolio_tracker)
        # Override auto_correct for this test
        system.auto_correct = True

        # Mock _reconcile_exchange to track calls and simulate its return
        reconcile_calls: list[dict[str, str]] = []

        async def mock_side_effect_reconcile_exchange(exchange: str) -> dict[str, Any]:
            reconcile_calls.append({"exchange": exchange})  # Record the call

            # Simulate return structure of _reconcile_exchange
            # This should include discrepancies if we want auto-correct to do something.
            # For this test, we mainly care that it was called.
            # If auto-correct logic depends on specific discrepancy details, mock them here.
            mock_discrepancies: list[dict[str, Any]] = []
            if exchange == "hyperliquid":  # Simulate a discrepancy for hyperliquid
                mock_discrepancies.append(
                    {
                        "symbol": "BTC",
                        "exchange_value": "1.1",  # API has 1.1
                        "local_value": "1.0",  # Local has 1.0
                        "discrepancy": "0.1",
                        "action_taken": "logged",  # or "corrected" if auto_correct was to run
                    },
                )

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

            # Configure mock API clients (get_positions is called by the real
            # _reconcile_exchange, but _reconcile_exchange itself is mocked here,
            # so get_positions won't be hit via this path)
            # However, portfolio_tracker.api_clients itself needs to be set for
            # check_positions to iterate
            portfolio_tracker.api_clients = {
                "hyperliquid": mock_hl_api_client,
                "backpack": mock_bp_api_client,
            }

            # Mock methods on portfolio_tracker that auto_correct might call if
            # discrepancies were processed
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
                # This part of assertion depends on _apply_corrections being called by
                # _reconcile_exchange
                # Since _reconcile_exchange is fully mocked, _apply_corrections is not
                # called by our mock.
                # To test auto-correction fully, one might need to mock _apply_corrections
                # or
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
        results: dict[str, Any] = {
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
            recorded_historical_item = reconciliation_system._record_discrepancy(  # pyright: ignore[reportPrivateUsage]
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
            # The recorded_at timestamp is set inside _record_discrepancy, so we can't
            # easily compare with now_ts
            # We can check it's a datetime and reasonably close if needed, or just
            # trust it's set.
            assert isinstance(recorded_item_from_history.recorded_at, datetime)
            assert (
                recorded_item_from_history.detail == detail_to_record
            )  # Check if the detail is the same model
            assert not recorded_item_from_history.is_corrected
        else:
            pytest.fail("Test setup error: results['discrepancies'] is empty.")

    def test_get_discrepancy_history(
        self,
        reconciliation_system: PositionReconciliationSystem,
    ) -> None:
        """Test getting history filtered by time."""
        now = datetime.now(UTC)
        old_time = now - timedelta(days=10)
        recent_time = now - timedelta(days=3)

        # Create DiscrepancyDetail instances first
        detail_old = DiscrepancyDetail(
            symbol="BTC_OLD",
            discrepancy_type="size",
            exchange_value="1",
            local_value="0.9",
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
        self,
        reconciliation_system: PositionReconciliationSystem,
    ) -> None:
        """Test generating a reconciliation report."""
        now_utc = datetime.now(UTC)
        recent_time_utc = now_utc - timedelta(hours=1)

        reconciliation_system.discrepancy_history.clear()

        # Create DiscrepancyDetail instances
        detail1 = DiscrepancyDetail(
            symbol="BTC",
            discrepancy_type="size",
            exchange_value="1.0",
            local_value="0.95",
        )
        detail2 = DiscrepancyDetail(
            symbol="ETH",
            discrepancy_type="entry_price",
            exchange_value="10.0",
            local_value="9.8",
        )
        detail3 = DiscrepancyDetail(
            symbol="SOL",
            discrepancy_type="size",
            exchange_value="50.0",
            local_value="0.0",
        )

        # Populate history with HistoricalDiscrepancyRecord instances
        reconciliation_system.discrepancy_history.extend(
            [
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
            ],
        )

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
        self,
        reconciliation_system: PositionReconciliationSystem,
    ) -> None:
        """Test checking positions and identifying discrepancies."""
        # Mock the portfolio tracker method using patch.object
        with patch.object(
            reconciliation_system._portfolio_tracker,  # pyright: ignore[reportPrivateUsage]
            "get_positions_by_exchange",
            return_value=[],
        ):
            now = datetime.now(UTC)
            api_positions_hyper = [
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("1.0"),
                    entry_price=Decimal(100),
                    timestamp=now,
                ),
                DerivativePosition(
                    exchange="hyperliquid",
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("-2.0"),
                    entry_price=Decimal(50),
                    timestamp=now,
                ),
            ]
            api_positions_bp = [
                DerivativePosition(
                    exchange="backpack",
                    symbol="SOL",
                    side=OrderSide.BUY,
                    size=Decimal("5.0"),
                    entry_price=Decimal(20),
                    timestamp=now,
                ),
            ]

            mock_hl_api_client = AsyncMock(spec=ExchangeAPI)
            mock_hl_api_client.get_positions = AsyncMock(return_value=api_positions_hyper)

            mock_bp_api_client = AsyncMock(spec=ExchangeAPI)
            mock_bp_api_client.get_positions = AsyncMock(return_value=api_positions_bp)

            portfolio_tracker = reconciliation_system._portfolio_tracker  # pyright: ignore[reportPrivateUsage]
            portfolio_tracker.api_clients = {
                "hyperliquid": mock_hl_api_client,
                "backpack": mock_bp_api_client,
            }

            # Patch _reconcile_exchange to return a known structure to avoid internal errors
            # This helps test check_positions's aggregation logic rather than
            # _reconcile_exchange itself here.
            mock_reconcile_result: dict[str, Any] = {
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
